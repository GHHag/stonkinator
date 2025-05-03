from typing import Callable

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller
from sklearn.feature_selection import SelectKBest, mutual_info_classif, RFE, RFECV

from trading_systems.model_creation.model_creation import SKModel


def adf_stationarity_test(df: pd.DataFrame, significance_level=0.05):
    stationary_results = {}
    for column in df.columns:
        series = df[column]
        try:
            result = adfuller(series.dropna())
            _, p_value, *_ = result
            stationary_results[column] = p_value < significance_level
        except ValueError:
            stationary_results[column] = False

    return stationary_results


def encode_categorical_features(df: pd.DataFrame, categorical_features):
    return pd.get_dummies(df, columns=categorical_features, drop_first=True)


def select_k_best_features(
    X: pd.DataFrame, y: pd.Series, number_of_features,
    score_func=mutual_info_classif
) -> list[str]:
    select_k = SelectKBest(score_func=score_func, k=number_of_features)
    selected = select_k.fit(X, y).get_support()
    columns = X.columns[selected]
    
    return columns.to_list()


def feature_importances_selection(
    X: pd.DataFrame, y: pd.Series, model: SKModel,
    feature_metrics_attr_chain=None, fraction=0.5
) -> tuple[list[str], pd.Series]:
    model.fit(X, y)
    
    if feature_metrics_attr_chain:
        attr_target = getattr(model, feature_metrics_attr_chain[0])
        for attr in feature_metrics_attr_chain[1:]:
            attr_target = getattr(attr_target, attr) 

        feature_importances = attr_target
    else:
        if hasattr(model, 'feature_importances_') == False:
            raise AttributeError('model should have a feature_importances_ attribute')
        feature_importances = model.feature_importances_

    split_at = int(len(X.columns) * fraction)
    sorted_index = np.argsort(feature_importances)[::-1]
    selected_features = X.columns[sorted_index[:split_at]]

    importances = pd.Series(data=feature_importances, index=X.columns)
    importances_sorted = importances.sort_values()

    return selected_features.to_list(), importances_sorted[:split_at]


def feature_importances_by_threshold_selection(
    X: pd.DataFrame, y: pd.Series, model: SKModel,
    feature_metrics_attr_chain=None,
    determine_threshold_function: Callable = np.median
) -> tuple[list[str], pd.Series, dict[str, object] | None]:
    model.fit(X, y)
    
    if hasattr(model, 'estimator_') and hasattr(model.estimator_, 'best_params_'):
        estimator_best_params = model.estimator_.best_params_
    elif hasattr(model, 'best_params_'):
        estimator_best_params = model.best_params_
    else:
        estimator_best_params = None
    
    if feature_metrics_attr_chain:
        attr_target = getattr(model, feature_metrics_attr_chain[0])
        for attr in feature_metrics_attr_chain[1:]:
            attr_target = getattr(attr_target, attr) 

        feature_importances = attr_target
    else:
        if hasattr(model, 'feature_importances_') == False:
            raise AttributeError('model should have a feature_importances_ attribute')
        feature_importances = model.feature_importances_

    threshold = determine_threshold_function(feature_importances)
    selected_features = X.loc[:, feature_importances >= threshold].columns

    importances = pd.Series(data=feature_importances, index=X.columns)
    importances_sorted = importances.sort_values()

    return selected_features.to_list(), importances_sorted[:len(selected_features)], estimator_best_params


# n_grid_search_runs = 5
# n_stability_selection_runs = 20
# grid_search_best_params_composite = []
# selected_features_composite = []

# for i in range(n_grid_search_runs):
#     print(f'grid search, iteration {i}')
#     selected_features, feature_importances, estimator_best_params = feature_importances_by_threshold_selection(
#         X_train, y_train, grid_search,
#         feature_metrics_attr_chain=['best_estimator_', 'feature_importances_']
#     )
#     selected_features_composite += selected_features
#     grid_search_best_params_composite.append(estimator_best_params)

#     if i == 0:
#         feature_importances.plot(kind='barh')
#         plt.title('RFE Ranking')

# grid_search_best_params_df = pd.DataFrame(grid_search_best_params_composite)
# grid_search_most_selected_params_dict = {
#     column: grid_search_best_params_df[column].value_counts().idxmax() 
#     for column in grid_search_best_params_df.columns
# }
# gbc = GradientBoostingClassifier(**grid_search_most_selected_params_dict, random_state=42)

# print('grid_search_best_params_composite:', grid_search_best_params_composite)
# print('grid_search_most_selected_params_dict:', grid_search_most_selected_params_dict)

# for i in range(n_stability_selection_runs - n_grid_search_runs):
#     print(f'iteration {i}')
#     selected_features, feature_importances, _ = feature_importances_by_threshold_selection(
#         X_train, y_train, gbc,
#         feature_metrics_attr_chain=['feature_importances_']
#     )
#     selected_features_composite += selected_features

# selected_features_counter = Counter(selected_features_composite)

# # Retain features that are selected in atleast a defined % of the runs
# percentage = 0.85
# # percentage = 0.50
# n_times_selected_threshold = int(n_stability_selection_runs * percentage)
# selected_features = [
#     feature for feature, n_times_selected in selected_features_counter.items() 
#     if n_times_selected >= n_times_selected_threshold
# ]
# X_df_reduced = X_df[selected_features]

# print(f'Extracted features:\n{selected_features}')
# print(f'Excluded features:\n{[feature for feature in X_df.columns.to_list() if feature not in selected_features]}')
# print(f'Number of features before: {len(X_df.columns.to_list())}')
# print(f'Number of features after: {len(selected_features)}')


def recursive_feature_elimination(
    X: pd.DataFrame, y: pd.DataFrame, estimator_model,
    cv=None, feature_metrics_attr_chain=None, **hyperparams
) -> tuple[list[str], pd.Series, dict[str, object] | None]:
    if cv is None:
        rfe = RFE(estimator=estimator_model, **hyperparams)
    else:
        rfe = RFECV(estimator=estimator_model, cv=cv, **hyperparams)

    rfe.fit(X, y)

    selected_features = rfe.get_feature_names_out()

    if hasattr(rfe.estimator_, 'best_params_'):
        estimator_best_params = rfe.estimator_.best_params_
    else:
        estimator_best_params = None

    if feature_metrics_attr_chain:
        attr_target = rfe.estimator_
        for attr in feature_metrics_attr_chain:
            attr_target = getattr(attr_target, attr) 

        feature_importances = attr_target

        importances = pd.Series(data=feature_importances, index=X[selected_features].columns)
        importances_sorted = importances.sort_values()
    else:
        importances_sorted = None

    return list(selected_features), importances_sorted, estimator_best_params


# # Experiment with different values for scoring (accuracy, f1, roc_auc, etc). 
# # Evaluate which scoring function suits the problem the best.
# # How to evaluate the fitted feature selection model? Should that be taken into account for the stability selection?
# rfe_params = {
#     'step': 1,
#     'scoring': 'f1',  # 'accuracy', 'roc_auc',
#     'verbose': 0,
#     'importance_getter': 'best_estimator_.feature_importances_'
# }

# n_grid_search_runs = 1
# n_stability_selection_runs = 6
# grid_search_best_params_composite = []
# selected_features_composite = []

# for i in range(n_grid_search_runs):
#     selected_features, feature_importances, estimator_best_params = recursive_feature_elimination(
#         X_train, y_train, grid_search,
#         cv=ts_split, feature_metrics_attr_chain=rfe_params.get('importance_getter').split('.'),
#         **rfe_params
#     )
#     selected_features_composite += selected_features
#     grid_search_best_params_composite.append(estimator_best_params)

#     if i == 0:
#         feature_importances.plot(kind='barh')
#         plt.title('RFE Ranking')
#         plt.show()

# grid_search_best_params_df = pd.DataFrame(grid_search_best_params_composite)
# grid_search_most_selected_params_dict = {
#     column: grid_search_best_params_df[column].value_counts().idxmax() 
#     for column in grid_search_best_params_df.columns
# }
# gbc = GradientBoostingClassifier(**grid_search_most_selected_params_dict, random_state=42)
# rfe_params['importance_getter'] = 'feature_importances_'

# print('\ngrid_search_best_params_composite:', grid_search_best_params_composite)
# print(f'grid_search_most_selected_params_dict: {grid_search_most_selected_params_dict}\n')

# for i in range(n_stability_selection_runs - n_grid_search_runs):
#     selected_features, feature_importances, _ = recursive_feature_elimination(
#         X_train, y_train, gbc,
#         cv=ts_split, feature_metrics_attr_chain=rfe_params.get('importance_getter').split('.'),
#         **rfe_params
#     )
#     selected_features_composite += selected_features

# selected_features_counter = Counter(selected_features_composite)

# # Retain features that are selected in atleast 85% of the run
# n_times_selected_threshold = int(n_stability_selection_runs * 0.85)
# selected_features = [
#     feature for feature, n_times_selected in selected_features_counter.items() 
#     if n_times_selected >= n_times_selected_threshold
# ]
# X_df_reduced = X_df[selected_features]

# print(f'\nExtracted features:\n{selected_features}')
# print(f'Number of features before: {len(X_df.columns.to_list())}')
# print(f'Number of features after: {len(X_df_reduced.columns.to_list())}')