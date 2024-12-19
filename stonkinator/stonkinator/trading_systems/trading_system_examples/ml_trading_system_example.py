import datetime as dt
import json
from typing import Callable

import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.metrics import classification_report, confusion_matrix, precision_score

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
from trading_systems.trading_system_properties import TradingSystemProperties
from trading_systems.trading_system_handler import TradingSystemProcessor
from trading_systems.position_sizer.safe_f_position_sizer import SafeFPositionSizer
from trading_systems.ml_utils.ml_system_utils import serialize_models


class MLTradingSystemExample(MLTradingSystemBase):

    @classproperty
    def name(cls):
        return 'ml_trading_system_example'

    @staticmethod
    def entry_signal_logic(
        df: pd.DataFrame, *args, entry_args=None
    ) -> Order | None:
        order = None
        entry_condition = df['pred'].iloc[-1] == 1
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
        exit_condition = df['pred'].iloc[-1] == 0
        if exit_condition == True:
            order = MarketOrder(MarketState.EXIT, df.index[-1])
        return order

    @staticmethod
    def create_backtest_models(
        data_dict: dict[str, pd.DataFrame], *args, 
        target_col=Price.CLOSE, target_period=1
    ):
        models_data_dict = {}
        for symbol, data in data_dict.items():
            df = data.copy()
            print(symbol)

            # shifted dataframe column
            df['Return_shifted'] = (
                df[target_col]
                .pct_change(periods=target_period)
                .shift(-target_period)
                .mul(100)
            )
            df['Target'] = df['Return_shifted'] > 0
            df = df.drop(['Return_shifted'], axis=1)
            df = df.dropna()

            # assign target column/feature
            y_df = df['Target']

            # copy df and drop columns/features to be excluded from training data...
            X_df = df.copy()
            X_df = X_df.drop(
                [
                    Price.OPEN, Price.HIGH, Price.LOW, Price.CLOSE, 'Pct_chg',
                    f'{Price.OPEN}_benchmark', f'{Price.HIGH}_benchmark', 
                    f'{Price.LOW}_benchmark', f'{Price.CLOSE}_benchmark',
                    f'{Price.VOLUME}_benchmark', 
                    TradingSystemAttributes.SYMBOL, 
                    f'{TradingSystemAttributes.SYMBOL}_benchmark',
                    'Target'
                ], 
                axis=1
            )
            # ... or assign columns/features to use as predictors
            """X_df = df[['Lag1', 'Lag2', 'Lag5']]"""

            # split data into train and test data sets
            X = X_df.to_numpy()
            y = y_df.to_numpy()
            # X_train = X[:int(X.shape[0]*0.7)]
            # y_train = y[:int(X.shape[0]*0.7)]
            # X_test = X[int(X.shape[0]*0.7):]
            # y_test = y[int(X.shape[0]*0.7):]
            ts_split = TimeSeriesSplit(n_splits=3)

            optimizable_params1 = np.array([0])
            optimizable_params2 = np.array([0])
            models_data_dict[symbol] = None
            try:
                for tr_index, val_index in ts_split.split(X):
                    X_train, X_test = X[tr_index], X[val_index]
                    y_train, y_test = y[tr_index], y[val_index]
                    top_model = None
                    top_choice_param = 0
                    for i in optimizable_params1:
                        for n in optimizable_params2:
                            pipeline = make_pipeline(
                                StandardScaler(),
                                DecisionTreeClassifier()
                            )
                            pipeline.fit(X_train, y_train)
                            y_pred = pipeline.predict(X_test)
                            cf_score_dict = {
                                'Accuracy': pipeline.score(X_test, y_test),
                                'Classification report': classification_report(y_test, y_pred),
                                'Confusion matrix': confusion_matrix(y_test, y_pred)
                            }
                            pred_df = df.iloc[val_index].copy()
                            pred_df['pred'] = y_pred.tolist()
                            pred_precision = precision_score(y_test, y_pred)
                            tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
                            print(f'TN: {tn}, FP: {fp}, FN: {fn}, TP: {tp}')
                            print(
                                f'Accuracy: {cf_score_dict["Accuracy"]}\n'
                                f'Classification report: \n'
                                f'{cf_score_dict["Classification report"]}\n'
                                f'Confusion matrix: \n'
                                f'{cf_score_dict["Confusion matrix"]}\n'
                                f'--------------------------------------------------------'
                            )
                            print(pipeline.score(X, y))
                            print(pipeline.score(X_train, y_train))
                            print(pipeline.score(X_test, y_test), '\n')
                            if top_model is None:
                                top_model = pred_df
                                top_choice_param = pred_precision
                            elif pred_precision > top_choice_param:
                                top_model = pred_df
                                top_choice_param = pred_precision
                    if models_data_dict[symbol] is None:
                        models_data_dict[symbol] = top_model
                    else:
                        models_data_dict[symbol] = pd.concat([models_data_dict[symbol], top_model])
            except ValueError as e:
                print('ValueError')
                print(symbol)
                print(len(df))
                print(e)
                input('Enter to proceed')
        return models_data_dict

    @staticmethod
    def create_inference_models(
        data_dict: dict[str, pd.DataFrame], *args,
        target_col=Price.CLOSE, target_period=1
    ):
        models_dict = {}
        for symbol, df in data_dict.items():
            # classification model target
            df['Target_col_shifted'] = (
                df[target_col]
                .pct_change(periods=target_period)
                .shift(-target_period)
                .mul(100)
            )
            df['Target'] = df['Target_col_shifted'] > 0
            df = df.drop(['Target_col_shifted'], axis=1)
            df = df.dropna()

            # assign target column/feature
            y_df = df['Target']

            # copy df and drop columns/features to be excluded from training data...
            X_df = df.copy()
            X_df = X_df.drop(
                [
                    Price.OPEN, Price.HIGH, Price.LOW, Price.CLOSE, 'Pct_chg',
                    f'{Price.OPEN}_benchmark', f'{Price.HIGH}_benchmark', 
                    f'{Price.LOW}_benchmark', f'{Price.CLOSE}_benchmark',
                    f'{Price.VOLUME}_benchmark', 
                    TradingSystemAttributes.SYMBOL, 
                    f'{TradingSystemAttributes.SYMBOL}_benchmark',
                    'Target' 
                ], axis=1
            )
            # ... or assign columns/features to use as predictors
            """X_df = df[['Lag1', 'Lag2', 'Lag5']]"""

            # split data into train and test data sets
            X = X_df.to_numpy()
            y = y_df.to_numpy()

            try:
                model = make_pipeline(
                    StandardScaler(),
                    DecisionTreeClassifier()
                )
                model.fit(X, y)
                models_dict[symbol] = model
            # TODO: Handle this error in a better way, log error message and remove input call
            except ValueError as e:
                print('ValueError')
                print(symbol)
                print(len(df))
                print(e)
                input('Enter to proceed')
        return models_dict

    @classmethod
    def operate_models(
        cls, systems_db: TradingSystemsPersisterBase, 
        data_dict: dict[str, pd.DataFrame],
        target_period=1
    ) -> dict [str, pd.DataFrame]:
        models_data_dict = cls.create_backtest_models(
            data_dict, target_period=target_period
        )

        inference_models_dict = cls.create_inference_models(
            data_dict, target_period=target_period
        )

        serialized_models = serialize_models(inference_models_dict)
        for symbol, model in serialized_models.items():
            insert_successful = systems_db.insert_ml_model(
                cls.name, symbol, model
            )
            if insert_successful == False:
                raise Exception('failed to insert data')
        return models_data_dict

    @classmethod
    def make_predictions(
        cls, systems_db: TradingSystemsPersisterBase, 
        data_dict: dict[str, pd.DataFrame],
        pred_features_data_dict: dict[str, np.ndarray]
    ) -> dict[str, pd.DataFrame]:
        for symbol, data in data_dict.items():
            model_pipeline: Pipeline = systems_db.get_ml_model(cls.name, symbol)
            if not model_pipeline:
                raise ValueError("failed to get model pipeline")

            pred_data = pred_features_data_dict.get(symbol)
            latest_data_point = data.iloc[-1].copy()
            latest_data_point['pred'] = model_pipeline.predict(pred_data[-1].reshape(1, -1))[0]
            latest_data_point_df = pd.DataFrame(latest_data_point).transpose()
            data_dict[symbol] = pd.concat(
                [data.iloc[:-1], latest_data_point_df]
            )
        return data_dict

    @staticmethod
    def preprocess_data(
        symbols_list, benchmark_symbol, 
        get_data_function: Callable[[str, dt.datetime, dt.datetime], tuple[bytes, int]],
        entry_args: dict, exit_args: dict, start_dt, end_dt,
        ts_processor: TradingSystemProcessor=None, target_period=1
    ):
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

        pred_features_data_dict = {}
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

                data_dict[symbol] = data.dropna()

                pred_features_data_dict[symbol] = data[['Lag1', 'Lag2', 'Lag5', Price.VOLUME]].to_numpy()
        return data_dict, pred_features_data_dict 

    @classmethod
    def get_properties(
        cls, instruments_db: InstrumentsMongoDb,
        target_period=1, import_instruments=False, path=None
    ):
        required_runs = 1
        benchmark_symbol = '^OMX'
        entry_args = {
            TradingSystemAttributes.REQ_PERIOD_ITERS: target_period, 
            TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK: target_period
        }
        exit_args = {
            TradingSystemAttributes.EXIT_PERIOD_LOOKBACK: target_period
        }

        symbols_list = ['SKF_B', 'VOLV_B', 'NDA_SE', 'SCA_B']
        """ symbols_list = json.loads(
            instruments_db.get_market_list_instrument_symbols(
                instruments_db.get_market_list_id('omxs30')
            )
        ) """

        return TradingSystemProperties( 
            required_runs, symbols_list,
            (
                benchmark_symbol, price_data_get_req,
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
            }
        )


if __name__ == '__main__':
    import trading_systems.env as env
    SYSTEMS_DB = TradingSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.SYSTEMS_DB)
    CLIENT_DB = TradingSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.CLIENT_DB)
    INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)
    target_period = 1
    create_inference_models = True
    insert_into_db = True

    system_props: TradingSystemProperties = MLTradingSystemExample.get_properties(
        INSTRUMENTS_DB, target_period=target_period
    )

    data_dict, _ = MLTradingSystemExample.preprocess_data(
        system_props.instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt,
        target_period=target_period
    )

    models_data_dict = MLTradingSystemExample.create_backtest_models(
        data_dict, target_period=target_period
    )

    if create_inference_models == True:
        inference_models_dict = MLTradingSystemExample.create_inference_models(
            data_dict, target_period=target_period
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
        save_summary_plot_to_path=None, # '/app/plots/',
        print_data=True,
        insert_data_to_db_bool=insert_into_db,
    )
