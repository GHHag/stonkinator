import datetime as dt
import json

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

from TETrading.data.metadata.trading_system_attributes import TradingSystemAttributes
from TETrading.trading_system.trading_system import TradingSystem

from tet_doc_db.tet_mongo_db.systems_mongo_db import TetSystemsMongoDb
from tet_doc_db.instruments_mongo_db.instruments_mongo_db import InstrumentsMongoDb

from tet_trading_systems.trading_system_development.trading_systems.trading_system_properties.trading_system_properties \
    import TradingSystemProperties
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
    data_dict: dict[str, pd.DataFrame], *args, 
    target_col='close', target_period=1
):
    models_df_dict = {}
    for symbol, df in data_dict.items():
        print(symbol)

        # shifted dataframe column
        df['Target'] = \
            df[target_col].pct_change(periods=target_period).shift(-target_period).mul(100)
        df = df.dropna()

        # assign target column/feature
        y_df = df['Target']

        # copy df and drop columns/features to be excluded from training data...
        X_df = df.copy()
        X_df = X_df.drop(
            [
                'open', 'high', 'low', 'close', 'Pct_chg', 'date', 
                'open_benchmark', 'high_benchmark', 'low_benchmark', 'close_benchmark',
                'volume_benchmark', 'symbol', 'symbol_benchmark', 
                'Target'
            ], 
            axis=1
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
                        models_df_dict[symbol]._append(top_model)
        except ValueError:
            print('ValueError')
            print(len(df))
            input('Enter to proceed')
    return models_df_dict


def create_classification_models(
    data_dict: dict[str, pd.DataFrame], *args, 
    target_col='close', target_period=1
):
    models_df_dict = {}
    for symbol, df in data_dict.items():
        print(symbol)

        # shifted dataframe column
        df['Return_shifted'] = \
            df[target_col].pct_change(periods=target_period).shift(-target_period).mul(100)
        df['Target'] = df['Return_shifted'] > 0
        df = df.drop(columns=['Return_shifted'])
        df = df.dropna()

        # assign target column/feature
        y_df = df['Target']

        # copy df and drop columns/features to be excluded from training data...
        X_df = df.copy()
        X_df = X_df.drop(
            [
                'open', 'high', 'low', 'close', 'Pct_chg', 'date',
                'open_benchmark', 'high_benchmark', 'low_benchmark', 'close_benchmark',
                'volume_benchmark', 'symbol', 'symbol_benchmark',
                'Target'
            ], 
            axis=1
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
                        models_df_dict[symbol]._append(top_model)
        except ValueError:
            print('ValueError')
            print(len(df))
            input('Enter to proceed')
    return models_df_dict


def create_production_models(
    db: TetSystemsMongoDb, df_dict: dict[str, pd.DataFrame], 
    system_name, *args, 
    target_col='close', target_period=1
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
        df = df.drop(columns=['Target_col_shifted'])
        df = df.dropna()

        # assign target column/feature
        y_df = df['Target']

        # copy df and drop columns/features to be excluded from training data...
        X_df = df.copy()
        X_df = X_df.drop(
            [
                'open', 'high', 'low', 'close', 'Pct_chg', 'date',
                'open_benchmark', 'high_benchmark', 'low_benchmark', 'close_benchmark',
                'volume_benchmark', 'symbol', 'symbol_benchmark',
                'Target' 
            ], axis=1
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
    entry_args, exit_args, start_dt, end_dt, 
    target_period=1
):
    df_dict = {}
    for symbol in symbols_list:
        response_data, response_status = get_data_function(symbol, start_dt, end_dt)
        if response_status == 200:
            df_dict[symbol] = pd.json_normalize(json.loads(response_data))
            if 'instrument_id' in df_dict[symbol].columns:
                df_dict[symbol] = df_dict[symbol].drop('instrument_id', axis=1)
    
    benchmark_col_suffix = '_benchmark'
    response_data, response_status = get_data_function(benchmark_symbol, start_dt, end_dt)
    if response_status == 200:
        df_benchmark = pd.json_normalize(json.loads(response_data))
        df_benchmark = df_benchmark.drop('instrument_id', axis=1)
        df_benchmark = df_benchmark.rename(
            columns={
                'open': f'open{benchmark_col_suffix}',
                'high': f'high{benchmark_col_suffix}',
                'low': f'low{benchmark_col_suffix}',
                'close': f'close{benchmark_col_suffix}',
                'volume': f'volume{benchmark_col_suffix}',
                'symbol': f'symbol{benchmark_col_suffix}'
            }
        )

    pred_features_df_dict = {}
    for symbol, data in dict(df_dict).items():
        if data.empty or len(data) < entry_args.get(TradingSystemAttributes.REQ_PERIOD_ITERS):
            print(symbol, 'DataFrame empty')
            del df_dict[symbol]
        else:
            df_benchmark['date'] = pd.to_datetime(df_benchmark['date'])
            df_dict[symbol]['date'] = pd.to_datetime(df_dict[symbol]['date'])

            df_dict[symbol] = pd.merge_ordered(data, df_benchmark, on='date', how='inner')
            df_dict[symbol] = df_dict[symbol].ffill()
            df_dict[symbol] = df_dict[symbol].set_index('date')

            df_dict[symbol]['volume'] = df_dict[symbol]['volume'].astype(int)

            # apply indicators/features to dataframe
            df_dict[symbol]['Pct_chg'] = df_dict[symbol]['close'].pct_change().mul(100)
            df_dict[symbol]['Lag1'] = df_dict[symbol]['Pct_chg'].shift(1)
            df_dict[symbol]['Lag2'] = df_dict[symbol]['Pct_chg'].shift(2)
            df_dict[symbol]['Lag5'] = df_dict[symbol]['Pct_chg'].shift(5)
            df_dict[symbol] = df_dict[symbol].dropna()
            df_dict[symbol] = df_dict[symbol].reset_index()

            pred_features_df_dict[symbol] = df_dict[symbol][['Lag1', 'Lag2', 'Lag5', 'volume']].to_numpy()

    return df_dict, pred_features_df_dict 


def get_ts_properties(
    instruments_db: InstrumentsMongoDb, target_period=1,
    import_instruments=False, path=None
):
    system_name = 'example_ml_system'
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
        system_name, 1,
        symbols_list,
        preprocess_data,
        (
            benchmark_symbol, price_data_get_req,
            entry_args, exit_args
        ),
        ml_entry_classification, ml_exit_classification,
        entry_args, exit_args,
        SafeFPositionSizer(20, 0.8), (),
        {
            'forecast_data_fraction': 0.7,
            'num_of_sims': 100
        }
    )


if __name__ == '__main__':
    import tet_trading_systems.trading_system_development.trading_systems.env as env
    SYSTEMS_DB = TetSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.SYSTEMS_DB)
    INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)

    target_period = 1
    system_props = get_ts_properties(INSTRUMENTS_DB, target_period=target_period)
    insert_into_db = False

    df_dict, pred_features = system_props.preprocess_data_function(
        system_props.system_instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt,
        target_period=target_period
    )

    #model_data_dict = create_reg_models(df_dict, target_period=target_period)
    model_data_dict = create_classification_models(df_dict, target_period=target_period)

    if insert_into_db is True:
        models_created_bool = create_production_models(
            SYSTEMS_DB, df_dict, system_props.system_name, target_period=target_period
        )
        if not models_created_bool:
            raise Exception('Failed to create model')

    trading_system = TradingSystem(
        system_props.system_name,
        system_props.entry_logic_function,
        system_props.exit_logic_function,
        SYSTEMS_DB, SYSTEMS_DB
    )

    trading_system.run_trading_system_backtest(
        model_data_dict,
        entry_args=system_props.entry_function_args,
        exit_args=system_props.exit_function_args,
        market_state_null_default=True,
        plot_performance_summary=False,
        save_summary_plot_to_path=None, # '/app/plots/',
        print_data=True,
        insert_data_to_db_bool=insert_into_db,
    )
