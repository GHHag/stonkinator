import os
import datetime as dt
from typing import Callable

import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier

from trading.data.metadata.trading_system_attributes import (
    TradingSystemAttributes, classproperty
)
from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.price import Price
from trading.position.order import Order, LimitOrder, MarketOrder
from trading.position.position import Position
from trading.trading_system.trading_system import TradingSystem

from persistance.persistance_services.securities_service_pb2 import Instrument
from persistance.persistance_services.dal_grpc import price_data_get
from persistance.persistance_services.securities_grpc_service import SecuritiesGRPCService
from persistance.persistance_meta_classes.trading_systems_persister import (
    TradingSystemsPersisterBase
)
from persistance.stonkinator_mongo_db.systems_mongo_db import TradingSystemsMongoDb

from trading_systems.trading_system_base import MLTradingSystemBase
from trading_systems.trading_system_properties import MLTradingSystemProperties
from trading_systems.trading_system_handler import TradingSystemProcessor
from trading_systems.position_sizer.safe_f_position_sizer import SafeFPositionSizer
from trading_systems.model_creation.model_creation import (
    SKModel, create_backtest_models, create_inference_model
)
from trading_systems.ml_utils.ml_system_utils import serialize_models


class MLTradingSystemExample(MLTradingSystemBase):

    @classproperty
    def name(cls) -> str:
        return 'ml_trading_system_example'

    @classproperty
    def target(cls) -> str:
        return 'target'

    @classproperty
    def target_period(cls) -> 1:
        return 1

    @classproperty
    def entry_args(cls) -> dict:
        target_period = cls.target_period
        return {
            TradingSystemAttributes.REQ_PERIOD_ITERS: target_period, 
            TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK: target_period,
            'target': cls.target,
            'target_period': target_period
        }

    @classproperty
    def exit_args(cls) -> dict:
        return {
            TradingSystemAttributes.EXIT_PERIOD_LOOKBACK: cls.target_period
        }

    @staticmethod
    def entry_signal_logic(
        df: pd.DataFrame, *args, entry_args=None
    ) -> Order | None:
        order = None
        entry_condition = df[TradingSystemAttributes.PRED_COL].iloc[-1] == 1
        if entry_condition == True:
            order = LimitOrder(
                MarketState.ENTRY, df.index[-1], df[Price.CLOSE].iloc[-1], 5,
                direction=TradingSystemAttributes.LONG
            )
        return order

    @staticmethod
    def exit_signal_logic(
        df: pd.DataFrame, position: Position, *args, exit_args=None
    ) -> Order | None:
        order = None
        exit_condition = df[TradingSystemAttributes.PRED_COL].iloc[-1] == 0
        if exit_condition == True:
            order = MarketOrder(MarketState.EXIT, df.index[-1])
        return order

    @staticmethod
    def create_backtest_models(
        data_dict: dict[str, pd.DataFrame], features: list[str], target: str,
        model_class: SKModel, param_grid: dict,
        verbose=False
    ) -> dict[str, pd.DataFrame]:
        models_data_dict = {}
        for symbol, data in data_dict.items():
            models_data_dict[symbol], selected_params = create_backtest_models(
                data, features, target, model_class, param_grid,
                verbose=verbose
            )
            if verbose == True:
                # TODO: do something with selected_params to determine which params to use for inference models
                print('selected_params', selected_params)
        return models_data_dict

    @staticmethod
    def create_inference_models(
        data_dict: dict[str, pd.DataFrame], features: list[str], target: str,
        model_class: SKModel, params: dict
    ) -> dict[str, SKModel]:
        models_dict = {}
        for symbol, data in data_dict.items():
            model = create_inference_model(data, features, target, model_class, params)
            if model:
                models_dict[symbol] = model
        return models_dict

    @classmethod
    def operate_models(
        cls, systems_db: TradingSystemsPersisterBase, 
        data_dict: dict[str, pd.DataFrame], features: list[str], model_class: SKModel, params: dict
    ) -> dict [str, pd.DataFrame]:
        target = cls.target
        models_data_dict = cls.create_backtest_models(data_dict, features, target, model_class, params)
        inference_models_dict = cls.create_inference_models(data_dict, features, target, model_class, params)

        serialized_models = serialize_models(inference_models_dict)
        for symbol, model in serialized_models.items():
            insert_successful = systems_db.insert_ml_model(cls.name, symbol, model)
            if insert_successful == False:
                raise Exception('Failed to insert data.')
        return models_data_dict

    @classmethod
    def make_predictions(
        cls, systems_db: TradingSystemsPersisterBase, 
        data_dict: dict[str, pd.DataFrame], features: list[str]
    ) -> dict[str, pd.DataFrame]:
        for symbol, data in data_dict.items():
            model_pipeline: SKModel = systems_db.get_ml_model(cls.name, symbol)
            if not model_pipeline:
                raise ValueError('Failed to get model pipeline.')

            pred_data = data[features].to_numpy()
            latest_data_point = data.iloc[-1].copy()
            latest_data_point[TradingSystemAttributes.PRED_COL] = (
                model_pipeline.predict(pred_data[-1].reshape(1, -1))[0]
            )
            latest_data_point_df = pd.DataFrame(latest_data_point).transpose()
            data_dict[symbol] = pd.concat(
                [data.iloc[:-1], latest_data_point_df]
            )
        return data_dict

    @staticmethod
    def preprocess_data(
        securities_grpc_service: SecuritiesGRPCService,
        instruments_list: list[Instrument],
        benchmark_instrument: Instrument,
        get_data_function: Callable[[str, dt.datetime, dt.datetime], pd.DataFrame | None],
        entry_args: dict, exit_args: dict,
        start_dt: dt.datetime, end_dt: dt.datetime,
        ts_processor: TradingSystemProcessor | None=None
    ) -> tuple[dict[str, pd.DataFrame], list[str]]:
        target = entry_args.get('target')
        target_period = entry_args.get('target_period')

        data_dict: dict[str, pd.DataFrame] = {}
        for instrument in instruments_list:
            df = get_data_function(securities_grpc_service, instrument.id, start_dt, end_dt)
            if df is None:
                continue
            data_dict[instrument.symbol] = df
        
        benchmark_col_suffix = '_benchmark'
        df_benchmark = get_data_function(securities_grpc_service, benchmark_instrument.id, start_dt, end_dt)
        if df_benchmark is not None:
            df_benchmark = df_benchmark.drop('instrument_id', axis=1)
            df_benchmark = df_benchmark.rename(
                columns={
                    Price.OPEN: f'{Price.OPEN}{benchmark_col_suffix}', 
                    Price.HIGH: f'{Price.HIGH}{benchmark_col_suffix}', 
                    Price.LOW: f'{Price.LOW}{benchmark_col_suffix}', 
                    Price.CLOSE: f'{Price.CLOSE}{benchmark_col_suffix}',
                    Price.VOLUME: f'{Price.VOLUME}{benchmark_col_suffix}', 
                    TradingSystemAttributes.SYMBOL: f'{TradingSystemAttributes.SYMBOL}{benchmark_col_suffix}'
                }
            )
            if ts_processor != None:
                ts_processor.penult_dt = pd.to_datetime(df_benchmark[Price.DT].iloc[-2])
                ts_processor.current_dt = pd.to_datetime(df_benchmark[Price.DT].iloc[-1])

        for symbol, data in data_dict.items():
            if data.empty or len(data) < entry_args.get(TradingSystemAttributes.REQ_PERIOD_ITERS):
                print(symbol, 'DataFrame empty')
                del data_dict[symbol]
            else:
                df_benchmark[Price.DT] = pd.to_datetime(df_benchmark[Price.DT])
                data[Price.DT] = pd.to_datetime(data[Price.DT])

                data = pd.merge_ordered(data, df_benchmark, on=Price.DT, how='inner')
                data = data.ffill()
                data = data.set_index(Price.DT)

                data[Price.VOLUME] = data[Price.VOLUME].astype(int)

                # apply indicators/features to dataframe
                data['Pct_chg'] = data[Price.CLOSE].pct_change().mul(100)
                data['Lag1'] = data['Pct_chg'].shift(1)
                data['Lag2'] = data['Pct_chg'].shift(2)
                data['Lag5'] = data['Pct_chg'].shift(5)

                data['return_shifted'] = (
                    data[Price.CLOSE]
                    .pct_change(periods=target_period)
                    .shift(-target_period)
                    .mul(100)
                )
                data[target] = data['return_shifted'] > 0
                data = data.drop(['return_shifted'], axis=1)
                data = data.dropna()

                data_dict[symbol] = data.dropna()

        pred_features = ['Lag1', 'Lag2', 'Lag5', Price.VOLUME]
        return data_dict, pred_features

    @classmethod
    def get_properties(cls, securities_grpc_service: SecuritiesGRPCService):
        required_runs = 1

        omxs_large_caps_instruments_list = securities_grpc_service.get_market_list_instruments(
            "omxs_large_caps"
        )
        omxs_large_caps_instruments_list = (
            list(omxs_large_caps_instruments_list.instruments)
            if omxs_large_caps_instruments_list
            else None
        )

        benchmark_instrument = securities_grpc_service.get_instrument("^OMX")

        model_class, params = (
            DecisionTreeClassifier, 
            {
                'max_depth': np.array([3, 5, 9])
            }
        )

        entry_args = cls.entry_args
        exit_args = cls.exit_args
        return MLTradingSystemProperties( 
            required_runs, omxs_large_caps_instruments_list,
            (
                benchmark_instrument, price_data_get,
                entry_args, exit_args
            ),
            entry_args, exit_args,
            {
                'plot_fig': False
            },
            SafeFPositionSizer(20, 0.8), (),
            {
                'forecast_data_fraction': 0.7,
                'num_of_sims': 100
            },
            model_class, params
        )


if __name__ == '__main__':
    import trading_systems.env as env
    SYSTEMS_DB = TradingSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.SYSTEMS_DB)
    CLIENT_DB = TradingSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.CLIENT_DB)
    securities_grpc_service = SecuritiesGRPCService(
        f"{os.environ.get('RPC_SERVICE_HOST')}:{os.environ.get('RPC_SERVICE_PORT')}"
    )

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)
    create_inference_models = True
    insert_into_db = True
    target = MLTradingSystemExample.target

    system_props: MLTradingSystemProperties = MLTradingSystemExample.get_properties(
        securities_grpc_service
    )

    data_dict, features = MLTradingSystemExample.preprocess_data(
        securities_grpc_service, system_props.instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt
    )

    models_data_dict = MLTradingSystemExample.create_backtest_models(
        data_dict, features, target, system_props.model_class, system_props.params,
        verbose=True
    )

    # TODO: How will the params for the inference models be determined?
    inference_params = {'max_depth': 9}

    if create_inference_models == True:
        inference_models_dict = MLTradingSystemExample.create_inference_models(
            data_dict, features, target, system_props.model_class, inference_params
        )

    if insert_into_db == True:
        serialized_models = serialize_models(inference_models_dict)
        for symbol, model in serialized_models.items():
            insert_successful = SYSTEMS_DB.insert_ml_model(
                MLTradingSystemExample.name, symbol, model
            )
            if insert_successful == False:
                raise Exception('Failed to insert model.')

    trading_system = TradingSystem(
        MLTradingSystemExample.name,
        MLTradingSystemExample.entry_signal_logic,
        MLTradingSystemExample.exit_signal_logic,
        SYSTEMS_DB, CLIENT_DB
    )

    trading_system.run_trading_system_backtest(
        models_data_dict,
        entry_args=system_props.entry_function_args,
        exit_args=system_props.exit_function_args,
        market_state_null_default=True,
        plot_performance_summary=False,
        save_summary_plot_to_path=None,
        print_data=True,
        insert_data_to_db_bool=insert_into_db,
    )
