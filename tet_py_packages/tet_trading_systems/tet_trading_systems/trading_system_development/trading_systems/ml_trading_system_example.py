import datetime as dt
import json
from typing import Dict

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error, r2_score, classification_report, \
    confusion_matrix, precision_score

from securities_db_py_dal.dal import price_data_get_req

from tet_doc_db.tet_mongo_db.systems_mongo_db import TetSystemsMongoDb
from tet_doc_db.instruments_mongo_db.instruments_mongo_db import InstrumentsMongoDb

from tet_trading_systems.trading_system_development.trading_systems.trading_system_properties.ml_trading_system_properties import MlTradingSystemProperties
from tet_trading_systems.trading_system_development.trading_systems.run_trading_systems import run_trading_system
from tet_trading_systems.trading_system_development.trading_systems.trading_system_handler import handle_ml_trading_system
from tet_trading_systems.trading_system_state_handler.ml_trading_system_state_handler import MlTradingSystemStateHandler 
from tet_trading_systems.trading_system_management.position_sizer.safe_f_position_sizer import SafeFPositionSizer
from tet_trading_systems.trading_system_development.ml_utils.ml_system_utils import serialize_models


def ml_entry_classification(df, *args, entry_args=None):
    return df['pred'].iloc[-1] == 1, 'long'


def ml_entry_regression(df, *args, entry_args=None):
    return df['pred'].iloc[-1] > 0, 'long'


def ml_exit_classification(
    df, trail, trailing_exit_price, entry_price, periods_in_pos,
    *args, exit_args=None
):
    return df['pred'].iloc[-1] == 0, trail, trailing_exit_price


def ml_exit_regression(
    df, trail, trailing_exit_price, entry_price, periods_in_pos,
    *args, exit_args=None
):
    return df['pred'].iloc[-1] < 0, trail, trailing_exit_price


def create_reg_models(
    data_dict: Dict[str, pd.DataFrame], *args, 
    target_col='Close', target_period=1
):
    models_df_dict = {}
    for symbol, df in data_dict.items():
        print(symbol)

        # shifted dataframe column
        df['Target'] = \
            df[target_col].pct_change(periods=target_period).shift(-target_period).mul(100)
        df.dropna(inplace=True)

        # assign target column/feature
        y_df = df['Target']

        # copy df and drop columns/features to be excluded from training data...
        X_df = df.copy()
        X_df.drop(
            [
                'Open', 'High', 'Low', 'Close', 'Pct_chg', 'Date', 
                'Open_benchmark', 'High_benchmark', 'Low_benchmark', 'Close_benchmark',
                'Volume_benchmark', 'symbol', 'symbol_benchmark', 
                'Target'
            ], 
            axis=1, inplace=True
        )
        # ... or assign columns/features to use as predictors
        """X_df = df[['Lag1', 'Lag2', 'Lag5']]"""

        # split data into train and test data sets
        X = X_df.to_numpy()
        y = y_df.to_numpy()
        """X_train = X[:int(X.shape[0]*0.7)]
        X_test = X[int(X.shape[0]*0.7):]
        y_train = y[:int(X.shape[0]*0.7)]
        y_test = y[int(X.shape[0]*0.7):]"""
        ts_split = TimeSeriesSplit(n_splits=3)

        optimizable_params1 = np.array([0])
        optimizable_params2 = np.array([0])
        models_df_dict[symbol] = None
        try:
            for tr_index, val_index in ts_split.split(X):
                X_train, X_test = X[tr_index], X[val_index]
                y_train, y_test = y[tr_index], y[val_index]
                top_model = None
                top_choice_param = 0
                for i in optimizable_params1:
                    for n in optimizable_params2:
                        steps = [
                            ('scaler', StandardScaler()),
                            ('linreg', LinearRegression())
                        ]
                        #steps = [
                        #    ('scaler', StandardScaler()),
                        #    ('dtr', DecisionTreeRegressor(criterion='mse', max_depth=3))
                        #]
                        pipeline = Pipeline(steps)
                        pipeline.fit(X_train, y_train)
                        y_pred = pipeline.predict(X_test)
                        pred_df = df.iloc[-len(X_test):].copy()
                        pred_df['pred'] = y_pred.tolist()
                        r_squared = r2_score(y_test, y_pred)
                        print(f'R^2: {r_squared}')
                        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
                        print(f'Root Mean Squared Error: {rmse}')
                        print(pipeline.score(X, y))
                        print(pipeline.score(X_train, y_train))
                        print(pipeline.score(X_test, y_test), '\n')
                        if top_model is None:
                            top_model = pred_df
                            top_choice_param = rmse
                        elif rmse > top_choice_param:
                            top_model = pred_df
                            top_choice_param = rmse
                    if models_df_dict[symbol] is None:
                        models_df_dict[symbol] = top_model
                    else:
                        models_df_dict[symbol].append(top_model)
        except ValueError:
            print('ValueError')
            print(len(df))
            input('Enter to proceed')
    return models_df_dict


def create_classification_models(
    data_dict: Dict[str, pd.DataFrame], *args, 
    target_col='Close', target_period=1
):
    models_df_dict = {}
    for symbol, df in data_dict.items():
        print(symbol)

        # shifted dataframe column
        df['Return_shifted'] = \
            df[target_col].pct_change(periods=target_period).shift(-target_period).mul(100)
        df['Target'] = df['Return_shifted'] > 0
        df.drop(columns=['Return_shifted'], inplace=True)
        df.dropna(inplace=True)

        # assign target column/feature
        y_df = df['Target']

        # copy df and drop columns/features to be excluded from training data...
        X_df = df.copy()
        X_df.drop(
            [
                'Open', 'High', 'Low', 'Close', 'Pct_chg', 'Date',
                'Open_benchmark', 'High_benchmark', 'Low_benchmark', 'Close_benchmark',
                'Volume_benchmark', 'symbol', 'symbol_benchmark',
                'Target'
            ], 
            axis=1, inplace=True
        )
        # ... or assign columns/features to use as predictors
        """X_df = df[['Lag1', 'Lag2', 'Lag5']]"""

        # split data into train and test data sets
        X = X_df.to_numpy()
        y = y_df.to_numpy()
        """X_train = X[:int(X.shape[0]*0.7)]
        X_test = X[int(X.shape[0]*0.7):]
        y_train = y[:int(X.shape[0]*0.7)]
        y_test = y[int(X.shape[0]*0.7):]"""
        ts_split = TimeSeriesSplit(n_splits=3)

        optimizable_params1 = np.array([0])
        optimizable_params2 = np.array([0])
        models_df_dict[symbol] = None
        try:
            for tr_index, val_index in ts_split.split(X):
                X_train, X_test = X[tr_index], X[val_index]
                y_train, y_test = y[tr_index], y[val_index]
                top_model = None
                top_choice_param = 0
                for i in optimizable_params1:
                    for n in optimizable_params2:
                        steps = [
                            ('scaler', StandardScaler()),
                            ('dt', DecisionTreeClassifier())
                        ]
                        pipeline = Pipeline(steps)
                        pipeline.fit(X_train, y_train)
                        y_pred = pipeline.predict(X_test)
                        cf_score_dict = {
                            'Accuracy': pipeline.score(X_test, y_test),
                            'Classification report': classification_report(y_test, y_pred),
                            'Confusion matrix': confusion_matrix(y_test, y_pred)
                        }
                        pred_df = df.iloc[-len(X_test):].copy()
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
                    if models_df_dict[symbol] is None:
                        models_df_dict[symbol] = top_model
                    else:
                        models_df_dict[symbol].append(top_model)
        except ValueError:
            print('ValueError')
            print(len(df))
            input('Enter to proceed')
    return models_df_dict


def create_production_models(
    db: TetSystemsMongoDb, df_dict: Dict[str, pd.DataFrame], 
    system_name, *args, 
    target_col='Close', target_period=1
):
    models_dict = {}
    for symbol, df in df_dict.items():
        # regression model target
        #df['Target'] = \
        #    df[target_col].pct_change(periods=target_period).shift(-target_period).mul(100)
        # classification model target
        df['Target_col_shifted'] = \
            df[target_col].pct_change(periods=target_period).shift(-target_period).mul(100)
        df['Target'] = df['Target_col_shifted'] > 0
        df.drop(columns=['Target_col_shifted'], inplace=True)
        df.dropna(inplace=True)

        # assign target column/feature
        y_df = df['Target']

        # copy df and drop columns/features to be excluded from training data...
        X_df = df.copy()
        X_df.drop(
            [
                'Open', 'High', 'Low', 'Close', 'Pct_chg', 'Date',
                'Open_benchmark', 'High_benchmark', 'Low_benchmark', 'Close_benchmark',
                'Volume_benchmark', 'symbol', 'symbol_benchmark',
                'Target' 
            ], axis=1, inplace=True
        )
        # ... or assign columns/features to use as predictors
        """X_df = df[['Lag1', 'Lag2', 'Lag5']]"""

        # split data into train and test data sets
        X = X_df.to_numpy()
        y = y_df.to_numpy()

        try:
            """ steps = [
                ('scaler', StandardScaler()),
                ('linreg', LinearRegression())
            ] """
            steps = [
                ('scaler', StandardScaler()),
                ('dt', DecisionTreeClassifier())
            ]
            model = Pipeline(steps)
            model.fit(X, y)
            models_dict[symbol] = model
        except ValueError:
            print('ValueError')
            print(symbol)
            print(len(df))
            input('Enter to proceed')

    binary_models = serialize_models(models_dict)
    for symbol, model in binary_models.items():
        if not db.insert_ml_model(system_name, symbol, model):
            print(symbol)
            raise Exception('Something went wrong while inserting to or updating database.')
    return True


def preprocess_data(
    symbols_list, benchmark_symbol, get_data_function,
    start_dt, end_dt, target_period=1
):
    df_dict = {
        symbol: pd.json_normalize(
            get_data_function(symbol, start_dt, end_dt)['data']
        )
        for symbol in symbols_list
    }

    df_benchmark = pd.json_normalize(
        get_data_function(benchmark_symbol, start_dt, end_dt)['data']
    )
    
    pred_features_df_dict = {}

    for symbol, data in dict(df_dict).items():
        if data.empty or len(data) < target_period:
            print(symbol, 'DataFrame empty')
            del df_dict[symbol]
        else:
            df_dict[symbol] = pd.merge_ordered(
                data, df_benchmark, on='Date', how='inner',
                suffixes=('', '_benchmark')
            )
            df_dict[symbol].fillna(method='ffill', inplace=True)
            df_dict[symbol]['Date'] = pd.to_datetime(df_dict[symbol]['Date'])
            df_dict[symbol].set_index(['Date'], inplace=True)
            #df_dict[symbol].fillna(method='ffill', inplace=True)

            # apply indicators/features to dataframe
            df_dict[symbol]['Pct_chg'] = df_dict[symbol]['Close'].pct_change().mul(100)
            df_dict[symbol]['Lag1'] = df_dict[symbol]['Pct_chg'].shift(1)
            df_dict[symbol]['Lag2'] = df_dict[symbol]['Pct_chg'].shift(2)
            df_dict[symbol]['Lag5'] = df_dict[symbol]['Pct_chg'].shift(5)
            df_dict[symbol].dropna(inplace=True)
            df_dict[symbol].reset_index(inplace=True)
            pred_features_df_dict[symbol] = df_dict[symbol][['Lag1', 'Lag2', 'Lag5', 'Volume']].to_numpy()

    return df_dict, pred_features_df_dict 


def get_example_ml_system_props(instruments_db: InstrumentsMongoDb, target_period=1):
    system_name = 'example_ml_system'
    symbols_list = ['SKF_B', 'VOLV_B']#, 'NDA_SE', 'SCA_B']
    """ symbols_list = json.loads(
        instruments_db.get_market_list_instrument_symbols(
            instruments_db.get_market_list_id('omxs30')
        )
    ) """

    return MlTradingSystemProperties( 
        system_name, 1,
        preprocess_data,
        (
            symbols_list, '^OMX', price_data_get_req
        ),
        handle_ml_trading_system,
        MlTradingSystemStateHandler, (system_name, ),
        (
            #ml_entry_regression, ml_exit_regression,
            ml_entry_classification, ml_exit_classification,
            {'req_period_iters': target_period, 'entry_period_lookback': target_period},
            {'exit_period_lookback': target_period}
        ),
        {},
        None, (), (),
        SafeFPositionSizer, (20, 0.8), (),
        {
            'forecast_data_fraction': 0.7,
            'num_of_sims': 100
        },
        symbols_list
    )


if __name__ == '__main__':
    SYSTEMS_DB = TetSystemsMongoDb('mongodb://localhost:27017/', 'systems_db')
    INSTRUMENTS_DB = InstrumentsMongoDb('mongodb://localhost:27017/', 'instruments_db')

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)

    target_period = 1
    system_props = get_example_ml_system_props(INSTRUMENTS_DB, target_period=target_period)

    df_dict, pred_features = system_props.preprocess_data_function(
        *system_props.preprocess_data_args, start_dt, end_dt,
        target_period=target_period
    )

    #model_data_dict = create_reg_models(df_dict, target_period=target_period)
    model_data_dict = create_classification_models(df_dict, target_period=target_period)

    if not create_production_models(
        SYSTEMS_DB, df_dict, 'example_ml_system', target_period=target_period
    ):
        raise Exception('Failed to create model')

    run_trading_system(
        model_data_dict, 'example_ml_system', 
        #ml_entry_regression, ml_exit_regression, 
        ml_entry_classification, ml_exit_classification, 
        {'req_period_iters': target_period, 'entry_period_lookback': target_period}, 
        {'exit_period_lookback'}, 
        market_state_null_default=True,
        plot_fig=True,
        systems_db=SYSTEMS_DB, client_db=SYSTEMS_DB, insert_into_db=True
    )
