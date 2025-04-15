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
from persistance.persistance_meta_classes.securities_service import SecuritiesServiceBase
from persistance.persistance_services.securities_grpc_service import SecuritiesGRPCService
from persistance.persistance_meta_classes.trading_systems_persister import (
    TradingSystemsPersisterBase
)
from persistance.persistance_services.trading_systems_grpc_service import TradingSystemsGRPCService

from trading_systems.trading_system_base import MLTradingSystemBase
from trading_systems.trading_system_properties import MLTradingSystemProperties
from trading_systems.trading_system_handler import TradingSystemProcessor
from trading_systems.position_sizer.safe_f_position_sizer import SafeFPositionSizer
from trading_systems.model_creation.model_creation import (
    SKModel, create_backtest_models, create_inference_model
)


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
        data_dict: dict[tuple[str, str], pd.DataFrame], features: list[str], target: str,
        model_class: SKModel, param_grid: dict,
        verbose=False
    ) -> dict[tuple[str, str], pd.DataFrame]:
        models_data_dict = {}
        for instrument, data in data_dict.items():
            models_data_dict[instrument], selected_params = create_backtest_models(
                data, features, target, model_class, param_grid,
                verbose=verbose
            )
            if verbose == True:
                # TODO: do something with selected_params to determine which params to use for inference models
                print('selected_params', selected_params)
        return models_data_dict

    @staticmethod
    def create_inference_models(
        data_dict: dict[tuple[str, str], pd.DataFrame], features: list[str], target: str,
        model_class: SKModel, params: dict
    ) -> dict[tuple[str, str], SKModel]:
        models_dict = {}
        for instrument, data in data_dict.items():
            model = create_inference_model(data, features, target, model_class, params)
            if model:
                models_dict[instrument] = model
        return models_dict

    @classmethod
    def operate_models(
        cls, trading_system_id: str, trading_systems_persister: TradingSystemsPersisterBase, 
        data_dict: dict[tuple[str, str], pd.DataFrame], features: list[str],
        model_class: SKModel, params: dict
    ) -> dict [tuple[str, str], pd.DataFrame]:
        target = cls.target
        models_data_dict = cls.create_backtest_models(data_dict, features, target, model_class, params)
        inference_models_dict = cls.create_inference_models(data_dict, features, target, model_class, params)
        for (instrument_id, _), model in inference_models_dict.items():
            trading_systems_persister.insert_trading_system_model(trading_system_id, instrument_id, model)
        return models_data_dict

    @classmethod
    def make_predictions(
        cls, trading_system_id: str, trading_systems_persister: TradingSystemsPersisterBase, 
        data_dict: dict[tuple [str, str], pd.DataFrame], features: list[str]
    ) -> dict[tuple[str, str], pd.DataFrame]:
        for instrument, data in data_dict.items():
            instrument_id, _ = instrument
            model_pipeline: SKModel = trading_systems_persister.get_trading_system_model(
                trading_system_id, instrument_id
            )
            if not model_pipeline:
                raise ValueError('Failed to get model pipeline.')

            pred_data = data[features].to_numpy()
            latest_data_point = data.iloc[-1].copy()
            latest_data_point[TradingSystemAttributes.PRED_COL] = (
                model_pipeline.predict(pred_data[-1].reshape(1, -1))[0]
            )
            latest_data_point_df = pd.DataFrame(latest_data_point).transpose()
            data_dict[instrument] = pd.concat(
                [data.iloc[:-1], latest_data_point_df]
            )
        return data_dict

    @staticmethod
    def preprocess_data(
        securities_service: SecuritiesServiceBase,
        instruments_list: list[Instrument],
        benchmark_instrument: Instrument,
        get_data_function: Callable[[str, dt.datetime, dt.datetime], pd.DataFrame | None],
        entry_args: dict, exit_args: dict,
        start_dt: dt.datetime, end_dt: dt.datetime,
        ts_processor: TradingSystemProcessor | None=None
    ) -> tuple[dict[tuple[str, str], pd.DataFrame], list[str]]:
        target = entry_args.get('target')
        target_period = entry_args.get('target_period')

        data_dict: dict[tuple[str, str], pd.DataFrame] = {}
        for instrument in instruments_list:
            df = get_data_function(securities_service, instrument.id, start_dt, end_dt)
            if df is None:
                continue
            data_dict[(instrument.id, instrument.symbol)] = df
        
        benchmark_col_suffix = '_benchmark'
        df_benchmark = get_data_function(securities_service, benchmark_instrument.id, start_dt, end_dt)
        if df_benchmark is not None:
            df_benchmark = df_benchmark.drop(TradingSystemAttributes.INSTRUMENT_ID, axis=1)
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

        instruments_to_remove = []
        for instrument, data in data_dict.items():
            if data.empty or len(data) < entry_args.get(TradingSystemAttributes.REQ_PERIOD_ITERS):
                instruments_to_remove.append(instrument)
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

                data_dict[instrument] = data.dropna()
        for instrument in instruments_to_remove:
            data_dict.pop(instrument)
        pred_features = ['Lag1', 'Lag2', 'Lag5', Price.VOLUME]
        return data_dict, pred_features

    @classmethod
    def get_properties(cls, securities_service: SecuritiesServiceBase):
        required_runs = 1

        omxs_large_caps_instruments_list = securities_service.get_market_list_instruments(
            "omxs_large_caps"
        )
        omxs_large_caps_instruments_list = (
            list(omxs_large_caps_instruments_list.instruments)
            if omxs_large_caps_instruments_list
            else None
        )

        benchmark_instrument = securities_service.get_instrument("^OMX")

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
    securities_grpc_service = SecuritiesGRPCService(
        f"{os.environ.get('RPC_SERVICE_HOST')}:{os.environ.get('RPC_SERVICE_PORT')}"
    )
    trading_systems_grpc_service = TradingSystemsGRPCService(
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

    tsp = lambda: None
    data_dict, features = MLTradingSystemExample.preprocess_data(
        securities_grpc_service, system_props.instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt,
        ts_processor=tsp
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

    trading_system_proto = None
    end_dt = tsp.current_dt
    if insert_into_db == True:
        trading_system_proto = trading_systems_grpc_service.get_or_insert_trading_system(
            MLTradingSystemExample.name, end_dt
        )
        trading_systems_grpc_service.update_current_date_time(
            trading_system_proto.id, tsp.current_dt
        )
        for (instrument_id, _), model in inference_models_dict.items():
            insert_res = trading_systems_grpc_service.insert_trading_system_model(
               trading_system_proto.id, instrument_id, model
            )
            if insert_res is None:
                raise Exception('Failed to insert model.')

    trading_system = TradingSystem(
        '' if not trading_system_proto else trading_system_proto.id,
        MLTradingSystemExample.name,
        MLTradingSystemExample.entry_signal_logic,
        MLTradingSystemExample.exit_signal_logic,
        trading_systems_grpc_service
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
