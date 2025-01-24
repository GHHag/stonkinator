from typing import Callable, Protocol
from itertools import product

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, confusion_matrix, precision_score, roc_auc_score

from trading.data.metadata.price import Price


class SKModel(Protocol):
    def fit(self, X, y, **kwargs): ...
    def score(self, X, y, **kwargs): ...


def print_classification_model_metrics(estimator: SKModel, X_train, X_test, y_train, y_test, y_pred):
    print(
        f'Accuracy (train): {estimator.score(X_train, y_train):.3f}\n'
        f'Accuracy (test): {estimator.score(X_test, y_test):.3f}\n\n'
        f'Classification report: \n'
        f'{classification_report(y_test, y_pred)}\n\n'
        f'Confusion matrix: \n'
        f'{confusion_matrix(y_test, y_pred)}\n\n'
    )

    if hasattr(estimator, 'predict_proba'):
        y_proba = estimator.predict_proba(X_test)[:, 1]
        print(f'ROC AUC score: {roc_auc_score(y_test, y_proba):.3f}')
    elif hasattr(estimator, 'decision_function'):
        y_scores = estimator.decision_function(X_test)
        print(f'ROC AUC score: {roc_auc_score(y_test, y_scores):.3f}')

    print('--------------------------------------------------------')


def create_classification_backtest_models(
    df: pd.DataFrame, selected_features: list[str], 
    estimator_class: SKModel, param_grid: dict[str, np.array], *args,
    target_col=Price.CLOSE, pipeline_args: tuple[tuple] | None=None, 
    optimization_metric_func: Callable=precision_score, verbose=False
):
    df_copy = df.copy()
    df_copy = df_copy.sort_index()

    X_df = df_copy[selected_features]
    y_df = df_copy[target_col]

    # split data into train and test data sets
    X = X_df.to_numpy()
    y = y_df.to_numpy()
    """X_train = X[:int(X.shape[0]*0.7)]
    X_test = X[int(X.shape[0]*0.7):]
    y_train = y[:int(X.shape[0]*0.7)]
    y_test = y[int(X.shape[0]*0.7):]"""
    ts_split = TimeSeriesSplit(n_splits=3)

    model_params, model_param_values =  zip(*param_grid.items())
    param_combinations = [dict(zip(model_params, v)) for v in product(*model_param_values)]
    selected_params = []

    model_df = None
    try:
        for tr_index, val_index in ts_split.split(X):
            X_train, X_test = X[tr_index], X[val_index]
            y_train, y_test = y[tr_index], y[val_index]
            model_pred_df = None
            optimization_metric_val = -1
            for params in param_combinations:
                estimator = estimator_class(**params)
                if pipeline_args is not None:
                    estimator = Pipeline([*pipeline_args, ('estimator', estimator)])
                estimator.fit(X_train, y_train)
                y_pred = estimator.predict(X_test)
                pred_df = df_copy.iloc[val_index].copy()
                pred_df['pred'] = y_pred.tolist()
                optimization_metric = optimization_metric_func(y_test, y_pred)
                if model_pred_df is None:
                    model_pred_df = pred_df
                    optimization_metric_val = optimization_metric
                elif optimization_metric > optimization_metric_val:
                    model_pred_df = pred_df
                    optimization_metric_val = optimization_metric
            if model_df is None:
                model_df = model_pred_df
            else:
                model_df = pd.concat([model_df, model_pred_df])
            selected_params.append(estimator.get_params())

            if verbose == True:
                print_classification_model_metrics(
                    estimator, X_train, X_test, y_train, y_test, y_pred
                )
    except ValueError as e:
        print(e)
    return model_df


def create_classification_inference_models(
    data: pd.DataFrame, selected_features: list[str], 
    estimator_class: SKModel, param_grid: dict[str, np.array], *args,
    target_col=Price.CLOSE, target_period: int=1,
    pipeline_args: tuple[tuple] | None=None, optimization_func: Callable=precision_score,
    evaluation_df: pd.DataFrame=None
):
    ...