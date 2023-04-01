import datetime as dt
import json
from typing import Dict, Callable
from pprint import pprint

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier 
from sklearn.decomposition import PCA
from sklearn.model_selection import TimeSeriesSplit, ParameterGrid
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error, r2_score, classification_report, \
    confusion_matrix, precision_score, log_loss, roc_auc_score
from sklearn.inspection import DecisionBoundaryDisplay
import seaborn as sns
import xgboost as xgb

from securities_db_py_dal.dal import price_data_get_req

from tet_doc_db.tet_mongo_db.systems_mongo_db import TetSystemsMongoDb
from tet_doc_db.instruments_mongo_db.instruments_mongo_db import InstrumentsMongoDb

from tet_trading_systems.trading_system_development.trading_systems.trading_system_properties.ml_trading_system_properties import MlTradingSystemProperties
from tet_trading_systems.trading_system_development.trading_systems.run_trading_systems import run_trading_system
from tet_trading_systems.trading_system_development.trading_systems.trading_system_handler import handle_ml_trading_system
from tet_trading_systems.trading_system_state_handler.ml_trading_system_state_handler import MlTradingSystemStateHandler 
from tet_trading_systems.trading_system_management.position_sizer.safe_f_position_sizer import SafeFPositionSizer
from tet_trading_systems.trading_system_development.ml_utils.ml_system_utils import serialize_models
from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.standard_indicators \
    import apply_atr, apply_adr, apply_rsi, apply_comparative_relative_strength
from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.volume_features \
    import apply_vwap, apply_vwap_from_n_period_low, apply_volume_balance
from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.misc_features \
    import apply_percent_rank

"""
applicera fler features på % change data?

multilabel classification
target features: RSI, RSI mean följande n perioder, n period ADR pct change
hyperparameter tuning med ParameterGrid
VotingClassifier, BaggingClassifier, Stacked ensemble, XGBoost, Categorical boosting

trading logik med pred_proba values
trading logik med trailing exit
"""

SYSTEM_NAME = 'kallops_trading'


def ml_entry_classification(df, *args, entry_args=None):
    return df['pred'].iloc[-1] == 1, 'long'


def ml_exit_classification(
    df, trail, trailing_exit_price, entry_price, periods_in_pos,
    *args, exit_args=None
):
    return df['pred'].iloc[-1] == 0, trail, trailing_exit_price


def ml_exit_classification_target_period(
    df, trail, trailing_exit_price, entry_price, periods_in_pos,
    *args, exit_args=None
):
    return df['pred'].iloc[-1] == 0 and periods_in_pos >= exit_args['exit_period_param'] or \
        df['Close'].iloc[-1] >= float(entry_price) + df['ATR'].iloc[-1] * 5.0 or \
        df['Close'].iloc[-1] <= float(entry_price) - df['ATR'].iloc[-1] * 2.0, \
        trail, trailing_exit_price


def ml_exit_classification_half_target_period(
    df, trail, trailing_exit_price, entry_price, periods_in_pos,
    *args, exit_args=None
):
    return df['pred'].iloc[-1] == 0 and periods_in_pos >= exit_args['exit_period_param'] / 2 or \
        df['Close'].iloc[-1] >= float(entry_price) + df['ATR'].iloc[-1] * 5.0 or \
        df['Close'].iloc[-1] <= float(entry_price) - df['ATR'].iloc[-1] * 2.0, \
        trail, trailing_exit_price


def ml_exit_classification_trailing(
    df, trail, trailing_exit_price, entry_price, periods_in_pos,
    *args, exit_args=None
):
    if not trail:
        trail = True
        trailing_exit_price = df['Close'].iloc[-1]

    return df['pred'].iloc[-1] == 0 and periods_in_pos >= exit_args['exit_period_param'] or \
        df['Close'].iloc[-1] >= float(entry_price) + df['ATR'].iloc[-1] * 5.0 or \
        df['Close'].iloc[-1] <= float(entry_price) - df['ATR'].iloc[-1] * 2.0, \
        trail, trailing_exit_price


def create_classification_models(
    df_dict: Dict[str, pd.DataFrame], entry_args, *args, 
    target_col='Close', target_period=1
):
    models_df_dict = {}
    for symbol, df in df_dict.items():
        print(symbol)

        # shifted dataframe column
        df[f'Return_shifted_{target_period}'] = \
            df[target_col].pct_change(periods=target_period).mul(100).shift(-target_period)
        df['target'] = df[f'Return_shifted_{target_period}'] > 0#(df['ADR'] * entry_args['adr_target_multiplier']) 
        df.drop(columns=[f'Return_shifted_{target_period}'], inplace=True)
        df.dropna(inplace=True)

        # assign target column/feature
        y_df = df['target']

        # copy df and drop columns/features to be excluded from training data...
        X_df = df.copy()
        X_df.drop(
            [
                'Open', 'High', 'Low', 'Close', 'Pct_chg', 'Date',
                'Open_benchmark', 'High_benchmark', 'Low_benchmark', 'Close_benchmark',
                'Volume_benchmark', 'symbol', 'symbol_benchmark',
                'sma_fast', 'sma_slow',
                f'VWAP_{entry_args["vwap_period_param_fast"]}_low',
                'crs', 'crs_sma_fast', 'crs_sma_slow',
                'ATR',
                'target'
            ], 
            axis=1, inplace=True
        )
        # ... or assign columns/features to use as predictors
        """X_df = df[['Lag1', 'Lag2', 'Lag5']]"""

        #print(X_df.columns)
        #input()
        """ for col in X_df.columns:
            try:
                X_df[col].plot(title=col)
                plt.show()
            except TypeError:
                print(col)
                input('enter') """

        #corr = X_df.corr()
        #mask = np.triu(np.ones_like(corr, dtype=bool))
        #f, ax = plt.subplots(figsize=(12, 10))
        #cmap = sns.diverging_palette(20, 230, as_cmap=True)
        #sns.heatmap(corr, mask=mask, cmap=cmap, vmin=-1, vmax=1, center=0, square=True, linewidths=.5, cbar_kws={"shrink": .5})
        #plt.show()

        # split data into train and test data sets
        X = X_df.to_numpy()
        y = y_df.to_numpy()
        """X_train = X[:int(X.shape[0]*0.7)]
        X_test = X[int(X.shape[0]*0.7):]
        y_train = y[:int(X.shape[0]*0.7)]
        y_test = y[int(X.shape[0]*0.7):]"""
        ts_split = TimeSeriesSplit(n_splits=3)

        # SVC
        #optimizable_params1 = np.array([0.2, 0.4, 0.8])
        #optimizable_params2 = np.array([0.01, 0.02, 0.04, 0.08, 0.16, 0.32])#, 0.64, 1.28])
        
        # GradientBoostingClassifier
        #optimizable_params1 = np.array([100, 200, 400]) # n_estimators
        #optimizable_params2 = np.array([0.1, 0.2, 0.4, 0.8])#, 1.6, 3.2]) # learning_rate
        #optimizable_params3 = np.array([5])#1, 2, 3, 5, 8]) # max_depth
        #optimizable_params4 = np.array([5, 7, 11])#, 19, 35]) # max_leaf_nodes

        # AdaBoostClassifier 
        #optimizable_params1 = np.array([3, 6, 12, 24, 48]) # n_estimators
        #optimizable_params2 = np.array([0.1, 0.2, 0.4, 0.8, 1])#, 1.6, 3.2]) # learning_rate
        #optimizable_params3 = [SVC(), RandomForestClassifier(max_depth=1), DecisionTreeClassifier(max_depth=1)] # base estimator
        grid = {
            'n_estimators': [3, 6, 12, 24, 48], 
            'learning_rate': [0.1, 0.2, 0.4, 0.8, 1],
            'estimator': [
                DecisionTreeClassifier(max_depth=1), 
                DecisionTreeClassifier(max_depth=2), 
                DecisionTreeClassifier(max_depth=3), 
                RandomForestClassifier(max_depth=1),
                RandomForestClassifier(max_depth=2),
                RandomForestClassifier(max_depth=3)
            ]
        }
        param_grid = ParameterGrid(grid)

        models_df_dict[symbol] = None
        try:
            for tr_index, val_index in ts_split.split(X):
                X_train, X_test = X[tr_index], X[val_index]
                y_train, y_test = y[tr_index], y[val_index]
                top_model = None
                top_choice_param = 0
                test_scores = [] # ParameterGrid test scores
                #for i in optimizable_params1:
                #    for n in optimizable_params2:
                        #for j in optimizable_params3:
                        #    for p in optimizable_params4:
                #for g in ParameterGrid(grid):
                for g in param_grid:
                    steps = [
                        ('scaler', StandardScaler()),
                        ('reducer', PCA(n_components=20)),
                        #('dt', DecisionTreeClassifier())
                        #('svm', SVC(kernel='sigmoid', C=i, gamma=n, degree=4, probability=True))
                        #(
                        #    'gbc', GradientBoostingClassifier(
                        #        loss='exponential', n_estimators=i, 
                        #        learning_rate=n, max_depth=j, 
                        #        max_leaf_nodes=p,
                        #        random_state=0, max_features='sqrt', 
                        #        warm_start=True
                        #    )
                        #)
                        (
                            'abc', AdaBoostClassifier(
                                **g
                                #estimator=RandomForestClassifier(max_depth=1),
                                #DecisionTreeClassifier(max_depth=1),
                                #SVC(kernel='sigmoid'),
                                #n_estimators=i, learning_rate=n
                            )
                        )
                    ]
                    pipeline = Pipeline(steps)
                    pipeline.fit(X_train, y_train)
                    y_pred = pipeline.predict(X_test)
                    y_pred_proba = pipeline.predict_proba(X_test)
                    pred_df = df.iloc[-len(X_test):].copy()
                    pred_df['pred'] = y_pred.tolist()
                    pred_df['pred_proba'] = y_pred_proba.tolist()
                    pred_precision = precision_score(y_test, y_pred)
                    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
                    test_scores.append(pipeline.score(X_test, y_test))
                    cf_score_dict = {
                        'Accuracy': pipeline.score(X_test, y_test),
                        'Classification report': classification_report(y_test, y_pred),
                        'Confusion matrix': confusion_matrix(y_test, y_pred)
                    }
                    print(f'TN: {tn}, FP: {fp}, FN: {fn}, TP: {tp}')
                    print(
                        f'Accuracy: {cf_score_dict["Accuracy"]}\n'
                        f'Classification report: \n'
                        f'{cf_score_dict["Classification report"]}\n'
                        f'Confusion matrix: \n'
                        f'{cf_score_dict["Confusion matrix"]}\n\n'
                    )
                    try:
                        print(pipeline.score(X, y))
                    except:
                        pass
                    print(pipeline.score(X_train, y_train))
                    print(pipeline.score(X_test, y_test), '\n')
                    cross_entropy = log_loss(y_test, y_pred_proba)
                    #pred_precision = log_loss(y_test, y_pred_proba)
                    print(f'Cross entropy: {cross_entropy}')
                    auc = roc_auc_score(y_test, y_pred)
                    print(f'auc: {auc}')
                    if top_model is None:
                        top_model = pred_df
                        top_choice_param = pred_precision
                        pprint(pipeline.get_params())
                        #input('top_model is None')
                    elif pred_precision > top_choice_param:
                    #elif pred_precision < top_choice_param:
                        top_model = pred_df
                        top_choice_param = pred_precision
                        pprint(pipeline.get_params())
                        #input('top_model')
                    print('--------------------------------------------------------')
                if models_df_dict[symbol] is None:
                    models_df_dict[symbol] = top_model
                else:
                    models_df_dict[symbol].append(top_model)
                best_idx = np.argmax(test_scores)
                print(test_scores[best_idx], param_grid[best_idx])
                input('evaluate param grid')
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
                'Volume_benchmark',
                'symbol', 'symbol_benchmark',
                'Target' ,
            ], axis=1, inplace=True
        )
        # ... or assign columns/features to use as predictors
        """X_df = df[['Lag1', 'Lag2', 'Lag5']]"""

        # split data into train and test data sets
        X = X_df.to_numpy()
        y = y_df.to_numpy()

        try:
            steps = [
                ('scaler', StandardScaler()),
                ('svm', SVC(kernel='sigmoid'))
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
    symbols_list, benchmark_symbol, get_data_function: Callable,
    entry_args: Dict, exit_args: Dict, start_dt, end_dt, target_period=1
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
            df_dict[symbol]['Volume'] = df_dict[symbol]['Volume'].astype(float)
            #df_dict[symbol].fillna(method='ffill', inplace=True)

            # apply indicators/features to dataframe
            apply_atr(df_dict[symbol], entry_args['atr_period_param'])
            apply_adr(df_dict[symbol], entry_args['atr_period_param'])
            apply_rsi(df_dict[symbol], period_param=entry_args['rsi_period_param'])
            apply_rsi(df_dict[symbol], period_param=entry_args['rsi_period_param'], col_name='Close_benchmark', suffix='_benchmark')

            #apply_vwap(df_dict[symbol], entry_args['vwap_period_param_fast'])
            #apply_vwap(df_dict[symbol], entry_args['vwap_period_param_slow'])
            apply_vwap_from_n_period_low(df_dict[symbol], entry_args['vwap_period_param_fast'], suffix='_low')
            #df_dict[symbol]['vwap_clf_fast'] = df_dict[symbol]['Close'] >= df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}']
            #df_dict[symbol]['vwap_clf_slow'] = df_dict[symbol]['Close'] >= df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}']
            df_dict[symbol]['vwap_low_clf_fast'] = df_dict[symbol]['Close'] >= df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}_low']
            df_dict[symbol]['vwap_low_price_rel'] = (df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}_low'] - df_dict[symbol]['Close']) / df_dict[symbol]['ATR']
            #df_dict[symbol]['vwap_low_price_rel'] = df_dict[symbol]['Close'] - df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}_low'] / df_dict[symbol]['ATR']
            #df_dict[symbol]['vwap_diff'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}'] - df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}']
            #df_dict[symbol]['atr_vwap_rel'] = (df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}'] - df_dict[symbol]['Close']) / df_dict[symbol]['ATR']
            #apply_comparative_relative_strength(df_dict[symbol], f'VWAP_{entry_args["vwap_period_param_fast"]}', f'VWAP_{entry_args["vwap_period_param_slow"]}', suffix='_vwap')
            apply_volume_balance(df_dict[symbol], period_param=entry_args['vwap_period_param_fast'])
            apply_volume_balance(df_dict[symbol], period_param=entry_args['vwap_period_param_slow'])
            apply_percent_rank(df_dict[symbol], entry_args['pct_rank_period_param_fast'], col_name=f'Volume_balance_{entry_args["vwap_period_param_fast"]}', suffix=f'_vb_{entry_args["vwap_period_param_fast"]}_{entry_args["pct_rank_period_param_fast"]}')
            apply_percent_rank(df_dict[symbol], entry_args['pct_rank_period_param_slow'], col_name=f'Volume_balance_{entry_args["vwap_period_param_fast"]}', suffix=f'_vb_{entry_args["vwap_period_param_fast"]}_{entry_args["pct_rank_period_param_slow"]}')
            apply_percent_rank(df_dict[symbol], entry_args['pct_rank_period_param_fast'], col_name=f'Volume_balance_{entry_args["vwap_period_param_slow"]}', suffix=f'_vb_{entry_args["vwap_period_param_slow"]}_{entry_args["pct_rank_period_param_fast"]}')
            apply_percent_rank(df_dict[symbol], entry_args['pct_rank_period_param_slow'], col_name=f'Volume_balance_{entry_args["vwap_period_param_slow"]}', suffix=f'_vb_{entry_args["vwap_period_param_slow"]}_{entry_args["pct_rank_period_param_slow"]}')

            apply_percent_rank(df_dict[symbol], entry_args['pct_rank_period_param_fast'], suffix=f'_{entry_args["pct_rank_period_param_fast"]}')
            apply_percent_rank(df_dict[symbol], entry_args['pct_rank_period_param_slow'], suffix=f'_{entry_args["pct_rank_period_param_slow"]}')
            df_dict[symbol]['pct_rank_diff'] = df_dict[symbol][f'%_rank_{entry_args["pct_rank_period_param_fast"]}'] - df_dict[symbol][f'%_rank_{entry_args["pct_rank_period_param_slow"]}']

            df_dict[symbol]['adr_lag2'] = df_dict[symbol]['ADR'].shift(2)
            df_dict[symbol]['adr_lag3'] = df_dict[symbol]['ADR'].shift(3)
            df_dict[symbol]['adr_lag4'] = df_dict[symbol]['ADR'].shift(4)
            df_dict[symbol]['adr_lag5'] = df_dict[symbol]['ADR'].shift(5)

            df_dict[symbol]['Pct_chg'] = df_dict[symbol]['Close'].pct_change().mul(100)
            df_dict[symbol]['Lag2'] = df_dict[symbol]['Pct_chg'].shift(2)
            df_dict[symbol]['Lag3'] = df_dict[symbol]['Pct_chg'].shift(3)
            df_dict[symbol]['Lag4'] = df_dict[symbol]['Pct_chg'].shift(4)
            df_dict[symbol]['Lag5'] = df_dict[symbol]['Pct_chg'].shift(5)

            features_to_calculate = [np.min, np.max, np.mean, np.std]
            prices_perc_rolling_fast = df_dict[symbol]['Pct_chg'].rolling(entry_args['sma_period_param_fast'], min_periods=5, closed='right')
            prices_perc_rolling_slow = df_dict[symbol]['Pct_chg'].rolling(entry_args['sma_period_param_slow'], min_periods=5, closed='right')
            features_fast = prices_perc_rolling_fast.agg(features_to_calculate).add_suffix('_fast')
            features_slow = prices_perc_rolling_slow.agg(features_to_calculate).add_suffix('_slow')
            df_dict[symbol] = pd.merge(df_dict[symbol], features_fast, left_index=True, right_index=True)
            df_dict[symbol] = pd.merge(df_dict[symbol], features_slow, left_index=True, right_index=True)

            df_dict[symbol]['sma_fast'] = df_dict[symbol]['Close'].rolling(entry_args['sma_period_param_fast']).mean()
            df_dict[symbol]['sma_slow'] = df_dict[symbol]['Close'].rolling(entry_args['sma_period_param_slow']).mean()
            df_dict[symbol]['sma_diff'] = df_dict[symbol]['sma_fast'] - df_dict[symbol]['sma_slow']
            df_dict[symbol]['atr_sma_fast_rel'] = (df_dict[symbol]['sma_fast'] - df_dict[symbol]['Close']) / df_dict[symbol]['ATR']
            df_dict[symbol]['atr_sma_slow_rel'] = (df_dict[symbol]['sma_slow'] - df_dict[symbol]['Close']) / df_dict[symbol]['ATR']
            df_dict[symbol]['rolling_std_fast'] = df_dict[symbol]['Close'].rolling(entry_args['sma_period_param_fast']).std()
            df_dict[symbol]['rolling_std_slow'] = df_dict[symbol]['Close'].rolling(entry_args['sma_period_param_slow']).std()

            df_dict[symbol]['crs'] = df_dict[symbol]['Close'] / df_dict[symbol]['Close_benchmark']
            df_dict[symbol]['crs_sma_fast'] = df_dict[symbol]['crs'].rolling(entry_args['sma_period_param_fast']).mean()
            df_dict[symbol]['crs_sma_slow'] = df_dict[symbol]['crs'].rolling(entry_args['sma_period_param_slow']).mean()
            df_dict[symbol]['crs_sma_diff'] = df_dict[symbol]['crs_sma_fast'] - df_dict[symbol]['crs_sma_slow']

            df_dict[symbol].dropna(inplace=True)
            df_dict[symbol].reset_index(inplace=True)
            
            #pred_features_df_dict[symbol] = df_dict[symbol][['Lag1', 'Lag2', 'Lag5', 'Volume']].to_numpy()

    return df_dict, None#pred_features_df_dict 


def get_kallops_trading_system_props(instruments_db: InstrumentsMongoDb, target_period=1):
    entry_args = {
        'req_period_iters': target_period, 
        'entry_period_lookback': target_period,
        'atr_period_param': 14,
        'rsi_period_param': 7,
        'sma_period_param_fast': 21,
        'sma_period_param_slow': 63,
        'vwap_period_param_fast': 21,
        'vwap_period_param_slow': 63,
        'pct_rank_period_param_fast': 63,
        'pct_rank_period_param_slow': 126,
        'adr_target_multiplier': 3.0
    }
    exit_args = {
        'exit_period_lookback': target_period,
        'exit_period_param': target_period
    }
    
    symbols_list = ['SKF_B', 'VOLV_B', 'NDA_SE', 'SCA_B']
    """ market_list_ids = [
        instruments_db.get_market_list_id('omxs30')
        #instruments_db.get_market_list_id('omxs_large_caps'),
        #instruments_db.get_market_list_id('omxs_mid_caps')
    ]
    symbols_list = []
    for market_list_id in market_list_ids:
        symbols_list += json.loads(
            instruments_db.get_market_list_instrument_symbols(
                market_list_id
            )
        ) """

    return MlTradingSystemProperties( 
        SYSTEM_NAME, 2,
        preprocess_data,
        (
            symbols_list, '^OMX', price_data_get_req,
            entry_args, exit_args
        ),
        handle_ml_trading_system,
        MlTradingSystemStateHandler, (SYSTEM_NAME, ),
        (
            #ml_entry_regression, ml_exit_regression,
            run_trading_system,
            ml_entry_classification, ml_exit_classification,
            entry_args, exit_args
        ),
        {'plot_fig': True},
        None, (), (),
        SafeFPositionSizer, (20, 0.8), (),
        {
            'forecast_data_fraction': 0.7,
            'num_of_sims': 100
        },
        symbols_list
    )


if __name__ == '__main__':
    import tet_trading_systems.trading_system_development.trading_systems.env as env
    #SYSTEMS_DB = TetSystemsMongoDb('mongodb://localhost:27017/', 'systems_db')
    SYSTEMS_DB = TetSystemsMongoDb(env.ATLAS_MONGO_DB_URL, 'client_db')
    #INSTRUMENTS_DB = InstrumentsMongoDb('mongodb://localhost:27017/', 'instruments_db')
    INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, 'client_db')

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)

    target_period = 10
    system_props = get_kallops_trading_system_props(INSTRUMENTS_DB, target_period=target_period)

    df_dict, pred_features = system_props.preprocess_data_function(
        *system_props.preprocess_data_args, start_dt, end_dt,
        target_period=target_period
    )

    model_df_dict = create_classification_models(
        df_dict, system_props.preprocess_data_args[-2], target_period=target_period
    )

    #if not create_production_models(
    #    SYSTEMS_DB, df_dict, SYSTEM_NAME, target_period=target_period
    #):
    #    raise Exception('Failed to create model')

    run_trading_system(
        model_df_dict, SYSTEM_NAME, 
        ml_entry_classification, 
        ml_exit_classification, 
        #ml_exit_classification_target_period, 
        #ml_exit_classification_half_target_period, 
        #{'req_period_iters': target_period, 'entry_period_lookback': target_period}, 
        system_props.preprocess_data_args[-2],
        #{'exit_period_lookback'}, 
        system_props.preprocess_data_args[-1],
        market_state_null_default=True,
        plot_fig=True,
        plot_positions=False,
        systems_db=SYSTEMS_DB, client_db=SYSTEMS_DB, insert_into_db=False
    )
