import datetime as dt
import json
from typing import Callable

import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier

from trading.data.metadata.trading_system_attributes import TradingSystemAttributes, classproperty
from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.price import Price
from trading.position.order import Order, LimitOrder, MarketOrder
from trading.position.position import Position
from trading.trading_system.trading_system import TradingSystem

from persistance.securities_db_py_dal.dal import price_data_get_req
from persistance.persistance_meta_classes.trading_systems_persister import TradingSystemsPersisterBase
from persistance.stonkinator_mongo_db.systems_mongo_db import TradingSystemsMongoDb
from persistance.stonkinator_mongo_db.instruments_mongo_db import InstrumentsMongoDb

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
    def name(cls):
        return 'ml_trading_system_example'

    @classproperty
    def target(cls):
        return 'target'

    @classproperty
    def target_period(cls):
        return 1

    @classproperty
    def entry_args(cls):
        target_period = cls.target_period
        return {
            TradingSystemAttributes.REQ_PERIOD_ITERS: target_period, 
            TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK: target_period,
            'target': cls.target,
            'target_period': target_period
        }

    @classproperty
    def exit_args(cls):
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
        symbols_list, benchmark_symbol, 
        get_data_function: Callable[[str, dt.datetime, dt.datetime], tuple[bytes, int]],
        entry_args: dict, exit_args: dict, start_dt: dt.datetime, end_dt: dt.datetime,
        ts_processor: TradingSystemProcessor=None
    ):
        target = entry_args.get('target')
        target_period = entry_args.get('target_period')

        data_dict: dict[str, pd.DataFrame] = {}
        for symbol in symbols_list:
            try:
                response_data, response_status = get_data_function(symbol, start_dt, end_dt)
            except ValueError:
                continue
            if response_status == 200:
                data_dict[symbol] = pd.json_normalize(json.loads(response_data))
                if 'instrument_id' in data_dict[symbol].columns:
                    data_dict[symbol] = data_dict[symbol].drop('instrument_id', axis=1)
        
        benchmark_col_suffix = '_benchmark'
        response_data, response_status = get_data_function(benchmark_symbol, start_dt, end_dt)
        if response_status == 200:
            df_benchmark = pd.json_normalize(json.loads(response_data))
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
    def get_properties(
        cls, instruments_db: InstrumentsMongoDb,
        import_instruments=False, path=None
    ):
        required_runs = 1
        benchmark_symbol = '^OMX'

        symbols_list = ['SKF_B', 'VOLV_B', 'NDA_SE', 'SCA_B']
        """ symbols_list = json.loads(
            instruments_db.get_market_list_instrument_symbols(
                instruments_db.get_market_list_id('omxs30')
            )
        ) """

        model_class, params = (
            DecisionTreeClassifier, 
            {
                'max_depth': np.array([3, 5, 9])
            }
        )

        return MLTradingSystemProperties( 
            required_runs, symbols_list,
            (
                benchmark_symbol, price_data_get_req,
                cls.entry_args, cls.exit_args
            ),
            cls.entry_args, cls.exit_args,
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
    INSTRUMENTS_DB = InstrumentsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.CLIENT_DB)

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)
    create_inference_models = True
    insert_into_db = True
    target = MLTradingSystemExample.target

    system_props: MLTradingSystemProperties = MLTradingSystemExample.get_properties(INSTRUMENTS_DB)

    data_dict, features = MLTradingSystemExample.preprocess_data(
        system_props.instruments_list,
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
                raise Exception('failed to insert data')

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
