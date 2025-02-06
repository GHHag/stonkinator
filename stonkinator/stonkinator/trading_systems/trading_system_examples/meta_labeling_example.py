import datetime as dt
import json
from typing import Callable

import pandas as pd
import numpy as np
from sklearn.svm import SVC
from sklearn.metrics import f1_score

from trading.data.metadata.trading_system_attributes import (
    TradingSystemAttributes, classproperty
)
from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.price import Price
from trading.position.order import Order, LimitOrder, MarketOrder
from trading.position.position import Position
from trading.trading_system.trading_system import TradingSystem

from persistance.securities_db_py_dal.dal import price_data_get_req
from persistance.persistance_meta_classes.trading_systems_persister import (
    TradingSystemsPersisterBase
)
from persistance.stonkinator_mongo_db.systems_mongo_db import TradingSystemsMongoDb
from persistance.stonkinator_mongo_db.instruments_mongo_db import InstrumentsMongoDb

from trading_systems.trading_system_base import MLTradingSystemBase
from trading_systems.trading_system_properties import MLTradingSystemProperties
from trading_systems.trading_system_handler import TradingSystemProcessor
from trading_systems.position_sizer.safe_f_position_sizer import SafeFPositionSizer
from trading_systems.model_creation.model_creation import (
    SKModel, create_backtest_models, create_inference_model
)
from trading_systems.ml_utils.ml_system_utils import serialize_model
from trading_systems.data_utils.indicator_feature_workshop.technical_features.standard_indicators import (
    apply_sma, apply_atr, apply_adr, apply_rsi
)
from trading_systems.data_utils.indicator_feature_workshop.technical_features.misc_features import (
    apply_percent_rank
)


TARGET = 'exit_label'


class MetaLabelingExample(MLTradingSystemBase):

    @classproperty
    def name(cls):
        return 'meta_labeling_example'

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
        exit_condition = (
            df[Price.CLOSE]
                .pct_change(position.periods_in_position)
                .mul(100).iloc[-1] >= df['ADR'].iloc[-position.periods_in_position] * 3.5 or
            -(df['ADR'].iloc[-(position.periods_in_position + 1)] * 2.5) > position.unrealised_return or
            position.periods_in_position >= 40
        )

        if exit_condition == True:
            order = MarketOrder(MarketState.EXIT, df.index[-1])
        return order

    @staticmethod
    def create_backtest_models(
        data: pd.DataFrame, features: list[str],
        model_class: SKModel, param_grid: dict,
        optimization_metric_func: Callable=f1_score, verbose=False
    ) -> pd.DataFrame:
        model_data, selected_params = create_backtest_models(
            data, features, TARGET, model_class, param_grid,
            optimization_metric_func=optimization_metric_func,
            verbose=verbose
        )
        if verbose == True:
            # TODO: do something with selected_params to determine which params to use for inference models
            print('selected_params', selected_params)
        return model_data

    @staticmethod
    def create_inference_models(
        data: pd.DataFrame, features: list[str],
        model_class: SKModel, params: dict
    ) -> SKModel:
        return create_inference_model(data, features, TARGET, model_class, params)

    @classmethod
    def operate_models(
        cls, systems_db: TradingSystemsPersisterBase,
        _, data: pd.DataFrame, model_class: SKModel, params: dict
    ) -> pd.DataFrame:
        features = MetaLabelingExample.get_features(data)
        model_data = cls.create_backtest_models(data, features, model_class, params)
        inference_model = cls.create_inference_models(data, features, model_class, params)

        serialized_model = serialize_model(inference_model)
        insert_successful = systems_db.insert_ml_model(cls.name, '', serialized_model)
        if insert_successful == False:
            raise Exception('Failed to insert data.')
        return model_data

    @classmethod
    def make_predictions(
        cls, systems_db: TradingSystemsPersisterBase,
        data_dict: dict[str, pd.DataFrame], data: pd.DataFrame
    ) -> dict[str, pd.DataFrame]:
        model_pipeline: SKModel = systems_db.get_ml_model(cls.name, '')
        if not model_pipeline:
            raise ValueError('Failed to get model pipeline.')

        features = MetaLabelingExample.get_features(data)
        entry_label_true_symbols = data[TradingSystemAttributes.SYMBOL].unique()
        for symbol in data_dict.keys():
            if symbol in entry_label_true_symbols:
                symbol_data = data_dict.get(symbol)
                latest_data_point = symbol_data.iloc[-1].copy()
                pred_data = latest_data_point[features].to_numpy()
                latest_data_point[TradingSystemAttributes.PRED_COL] = (
                    model_pipeline.predict(pred_data.reshape(1, -1))[0]
                )
                latest_data_point_df = pd.DataFrame(latest_data_point).transpose()
                data_dict[symbol] = pd.concat(
                    [data_dict[symbol].iloc[:-1], latest_data_point_df]
                )
            else:
                data_dict[symbol][TradingSystemAttributes.PRED_COL] = False
        return data_dict

    @classmethod
    def add_entry_signal_label(
        cls, data_dict: dict[str, pd.DataFrame], model_data: pd.DataFrame
    ):
        for symbol, data in data_dict.items():
            dates_to_match = model_data[model_data[TradingSystemAttributes.SYMBOL] == symbol].index
            data_dict[symbol][TradingSystemAttributes.PRED_COL] = data.index.isin(dates_to_match)
        return data_dict

    @staticmethod
    def preprocess_data(
        symbols_list, benchmark_symbol,
        get_data_function: Callable[[str, dt.datetime, dt.datetime], tuple[bytes, int]],
        entry_args: dict, exit_args: dict, start_dt: dt.datetime, end_dt: dt.datetime,
        ts_processor: TradingSystemProcessor=None, drop_nan_rows=False
    ) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
        # TODO: Define feature input variables in the same scope, currently defined both here
        # and in get_features method.
        ma_value_1 = 7
        ma_value_2 = 21

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
            if ts_processor is not None:
                ts_processor.penult_dt = pd.to_datetime(df_benchmark[Price.DT].iloc[-2])
                ts_processor.current_dt = pd.to_datetime(df_benchmark[Price.DT].iloc[-1])

        composite_df = pd.DataFrame()
        for symbol, data in data_dict.items():
            if data.empty or len(data) < entry_args.get(TradingSystemAttributes.REQ_PERIOD_ITERS):
                print(symbol, 'DataFrame empty')
                del data_dict[symbol]
            else:
                df_benchmark[Price.DT] = pd.to_datetime(df_benchmark[Price.DT])
                data_dict[symbol][Price.DT] = pd.to_datetime(data_dict[symbol][Price.DT])

                data = pd.merge_ordered(data, df_benchmark, on=Price.DT, how='inner')
                data = data.ffill()
                data = data.set_index(Price.DT)

                data[Price.VOLUME] = data[Price.VOLUME].astype(int)

                apply_atr(data, 14)
                apply_adr(data, 14)

                data['pct_chg'] = data[Price.CLOSE].pct_change().mul(100)
                data['lag_1'] = data['pct_chg'].shift(1)
                data['lag_2'] = data['pct_chg'].shift(2)
                data['lag_4'] = data['pct_chg'].shift(4)
                data['lag_8'] = data['pct_chg'].shift(8)
                data['lag_16'] = data['pct_chg'].shift(16)
                data[f'lag_8_rolling_{ma_value_1}_std'] = data['lag_8'].rolling(ma_value_1).std()
                data[f'lag_8_rolling_{ma_value_2}_std'] = data['lag_8'].rolling(ma_value_2).std()
                data[f'lag_16_rolling_{ma_value_1}_std'] = data['lag_16'].rolling(ma_value_1).std()
                data[f'lag_16_rolling_{ma_value_2}_std'] = data['lag_16'].rolling(ma_value_2).std()
                data[f'lag_8_rolling_{ma_value_1}_std_adr_div'] = data[f'lag_8_rolling_{ma_value_1}_std'] / data['ADR']  
                data[f'lag_8_rolling_{ma_value_2}_std_adr_div'] = data[f'lag_8_rolling_{ma_value_2}_std'] / data['ADR']
                data[f'lag_16_rolling_{ma_value_1}_std_adr_div'] = data[f'lag_16_rolling_{ma_value_1}_std'] / data['ADR']
                data[f'lag_16_rolling_{ma_value_2}_std_adr_div'] = data[f'lag_16_rolling_{ma_value_2}_std'] / data['ADR']

                apply_percent_rank(data, 63, suffix='_63')
                apply_percent_rank(data, 126)
                data['%_rank_diff'] = data['%_rank'] - data['%_rank_63']
                data['%_rank_lag_1'] = data['%_rank'].shift(1)
                data['%_rank_lag_2'] = data['%_rank'].shift(2)
                data['%_rank_lag_4'] = data['%_rank'].shift(4)
                data['%_rank_lag_8'] = data['%_rank'].shift(8)
                data['%_rank_lag_16'] = data['%_rank'].shift(16)
                data['%_rank_63_lag_1'] = data['%_rank_63'].shift(1)
                data['%_rank_63_lag_2'] = data['%_rank_63'].shift(2)
                data['%_rank_63_lag_4'] = data['%_rank_63'].shift(4)
                data['%_rank_63_lag_8'] = data['%_rank_63'].shift(8)
                data['%_rank_63_lag_16'] = data['%_rank_63'].shift(16)

                apply_rsi(data, period_param=7)
                apply_rsi(data, period_param=9)
                apply_rsi(data, period_param=14)
                data['rsi_diff_14_9'] = data['RSI_14'] - data['RSI_9']
                data['rsi_diff_14_7'] = data['RSI_14'] - data['RSI_7']
                data['rsi_diff_9_7'] = data['RSI_9'] - data['RSI_7']
                data[f'rsi_7_rolling_{ma_value_1}_mean'] = data['RSI_7'].rolling(ma_value_1).mean()
                data[f'rsi_7_rolling_{ma_value_2}_mean'] = data['RSI_7'].rolling(ma_value_2).mean()
                data[f'rsi_9_rolling_{ma_value_1}_mean'] = data['RSI_9'].rolling(ma_value_1).mean()
                data[f'rsi_9_rolling_{ma_value_2}_mean'] = data['RSI_9'].rolling(ma_value_2).mean()
                data[f'rsi_rolling_diff_1'] = data[f'rsi_9_rolling_{ma_value_1}_mean'] - data[f'rsi_7_rolling_{ma_value_1}_mean']
                data[f'rsi_rolling_diff_2'] = data[f'rsi_9_rolling_{ma_value_2}_mean'] - data[f'rsi_7_rolling_{ma_value_2}_mean']
                data['rsi_7_lag_1'] = data['RSI_7'].shift(1)
                data['rsi_7_lag_2'] = data['RSI_7'].shift(2)
                data['rsi_7_lag_4'] = data['RSI_7'].shift(4)
                data['rsi_7_lag_8'] = data['RSI_7'].shift(8)
                data['rsi_7_lag_16'] = data['RSI_7'].shift(16)
                data['rsi_9_lag_1'] = data['RSI_9'].shift(1)
                data['rsi_9_lag_2'] = data['RSI_9'].shift(2)
                data['rsi_9_lag_4'] = data['RSI_9'].shift(4)
                data['rsi_9_lag_8'] = data['RSI_9'].shift(8)
                data['rsi_9_lag_16'] = data['RSI_9'].shift(16)
                data['rsi_14_lag_1'] = data['RSI_14'].shift(1)
                data['rsi_14_lag_2'] = data['RSI_14'].shift(2)
                data['rsi_14_lag_4'] = data['RSI_14'].shift(4)
                data['rsi_14_lag_8'] = data['RSI_14'].shift(8)
                data['rsi_14_lag_16'] = data['RSI_14'].shift(16)

                apply_sma(data, ma_value_1)
                apply_sma(data, ma_value_2)
                data[f'sma_{ma_value_1}_std_rolling'] = data[f'SMA_{ma_value_1}'].rolling(ma_value_1).std()
                data[f'sma_{ma_value_2}_std_rolling'] = data[f'SMA_{ma_value_2}'].rolling(ma_value_2).std()
                data[f'sma_{ma_value_1}_std_rolling_lower'] = (data[f'SMA_{ma_value_1}'] - data[f'sma_{ma_value_1}_std_rolling'])
                data[f'sma_{ma_value_1}_std_rolling_upper'] = (data[f'SMA_{ma_value_1}'] + data[f'sma_{ma_value_1}_std_rolling'])
                data[f'sma_{ma_value_2}_std_rolling_lower'] = (data[f'SMA_{ma_value_2}'] - data[f'sma_{ma_value_2}_std_rolling'])
                data[f'sma_{ma_value_2}_std_rolling_upper'] = (data[f'SMA_{ma_value_2}'] + data[f'sma_{ma_value_2}_std_rolling'])
                data[f'sma_{ma_value_1}_std_rolling_lower_atr_rel'] = (data[f'SMA_{ma_value_1}'] - data[f'sma_{ma_value_1}_std_rolling_lower']) / data['ATR']
                data[f'sma_{ma_value_1}_std_rolling_upper_atr_rel'] = (data[f'sma_{ma_value_1}_std_rolling_upper'] - data[f'SMA_{ma_value_1}']) / data['ATR']
                data[f'sma_{ma_value_2}_std_rolling_lower_atr_rel'] = (data[f'SMA_{ma_value_2}'] - data[f'sma_{ma_value_2}_std_rolling_lower']) / data['ATR']
                data[f'sma_{ma_value_2}_std_rolling_upper_atr_rel'] = (data[f'sma_{ma_value_2}_std_rolling_upper'] - data[f'SMA_{ma_value_2}']) / data['ATR']
                data['sma_diff_1'] = data[f'SMA_{ma_value_2}'] - data[f'SMA_{ma_value_1}']
                data['sma_diff_1_atr_rel'] = data['sma_diff_1'] / data['ATR']
                data['sma_diff_1_std_rolling'] = data['sma_diff_1'].rolling(ma_value_1).std()
                data['sma_diff_1_rolling_std_atr_rel'] = data[f'sma_diff_1_std_rolling'] / data['ATR']
                data['sma_diff_1_lag_1'] = data['sma_diff_1'].shift(1)
                data['sma_diff_1_lag_2'] = data['sma_diff_1'].shift(2)
                data['sma_diff_1_lag_4'] = data['sma_diff_1'].shift(4)
                data['sma_diff_1_lag_8'] = data['sma_diff_1'].shift(8)
                data['sma_diff_1_lag_16'] = data['sma_diff_1'].shift(16)

                # entry label
                data['entry_condition'] = data[f'SMA_{ma_value_1}'] > data[f'SMA_{ma_value_2}']
                data['entry_condition_shifted'] = data['entry_condition'].shift(1)
                data['entry_label'] = (
                    (data['entry_condition'] == True) &
                    (data['entry_condition_shifted'] == False) &
                    (data['%_rank'] >= 0.5)
                )

                # exit label
                data['price_shifted'] = data[Price.CLOSE].shift(-target_period)
                data['max_price'] = data['price_shifted'].rolling(window=target_period).max()
                data['min_price'] = data['price_shifted'].rolling(window=target_period).min()
                data['max_pct_chg'] = (data['max_price'] / data[Price.CLOSE] - 1) * 100
                data['min_pct_chg'] = (data['min_price'] / data[Price.CLOSE] - 1) * 100
                data['exit_label'] = (
                    (data['max_pct_chg'] > data['ADR'] * 3.5) & 
                    (data['min_pct_chg'] > -(data['ADR'] * 2.5))
                )

                # TODO: Impute inf values with some derived value from the column where they are found.
                last_row = data.iloc[[-1]]
                data = data.replace([np.inf, -np.inf], 0)
                if drop_nan_rows == True:
                    data = pd.concat([data.iloc[:-1].dropna(), last_row])
                else:
                    data = pd.concat([data.iloc[:-1], last_row])
                data_dict[symbol] = data

                composite_df = pd.concat([composite_df, data[data['entry_label'] == True]])
                composite_df = composite_df.sort_index()
        return data_dict, composite_df

    @staticmethod
    def get_features(composite_df: pd.DataFrame):
        ma_value_1 = 7
        ma_value_2 = 21
        columns_to_drop = [
            Price.OPEN, Price.HIGH, Price.LOW, Price.CLOSE, 'pct_chg', 'ATR',
            f'{Price.OPEN}_benchmark', f'{Price.HIGH}_benchmark',
            f'{Price.LOW}_benchmark', f'{Price.CLOSE}_benchmark',
            f'{Price.VOLUME}_benchmark', 
            TradingSystemAttributes.SYMBOL, f'{TradingSystemAttributes.SYMBOL}_benchmark',
            'entry_condition', 'entry_condition_shifted', 'entry_label',
            'price_shifted', 'max_price', 'min_price',
            'max_pct_chg', 'min_pct_chg', 'exit_label',
            f'SMA_{ma_value_1}', f'SMA_{ma_value_2}',
            f'sma_{ma_value_1}_std_rolling', f'sma_{ma_value_2}_std_rolling', 
            f'sma_{ma_value_1}_std_rolling_lower', f'sma_{ma_value_1}_std_rolling_upper', 
            f'sma_{ma_value_2}_std_rolling_lower', f'sma_{ma_value_2}_std_rolling_upper',
        ]
        return list(set(composite_df.columns.to_list()) ^ set(columns_to_drop))


    @classmethod
    def get_properties(
        cls, instruments_db: InstrumentsMongoDb,
        import_instruments=False, path=None
    ):
        required_runs = 1
        target_period = 40
        benchmark_symbol = '^OMX'
        entry_args = {
            TradingSystemAttributes.REQ_PERIOD_ITERS: target_period,
            TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK: target_period,
            'target_period': target_period
        }
        exit_args = {
            TradingSystemAttributes.EXIT_PERIOD_LOOKBACK: target_period
        }

        market_list_ids = [
            instruments_db.get_market_list_id('omxs30')
            # instruments_db.get_market_list_id('omxs_large_caps'),
            # instruments_db.get_market_list_id('omxs_mid_caps')
        ]
        symbols_list = []
        for market_list_id in market_list_ids:
            symbols_list += json.loads(
                instruments_db.get_market_list_instrument_symbols(
                    market_list_id
                )
            )

        model_class, params = (
            SVC,
            {
                'C': np.array([0.01, 0.01, 1.0, 10]),
                'kernel': np.array(['rbf', 'sigmoid', 'poly']),
                'gamma': np.array(['scale', 'auto']),
                'random_state': np.array([1]),
            }
        )

        return MLTradingSystemProperties(
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
    create_model = True
    run_backtest = True
    insert_into_db = True

    system_props: MLTradingSystemProperties = MetaLabelingExample.get_properties(INSTRUMENTS_DB)

    data_dict, composite_data = MetaLabelingExample.preprocess_data(
        system_props.instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt,
        drop_nan_rows=True
    )

    features = MetaLabelingExample.get_features(composite_data)
    model_data = MetaLabelingExample.create_backtest_models(
        composite_data, features, system_props.model_class, system_props.params,
        optimization_metric_func=f1_score, verbose=True
    )

    # TODO: How will the params for the inference models be determined?
    inference_params = {
        'C': 1.0,
        'kernel': 'sigmoid',
        'gamma': 'auto',
        'random_state': 1,
    }

    if create_model == True:
        inference_model = MetaLabelingExample.create_inference_models(
            composite_data, features, system_props.model_class, inference_params
        )

    model_data = model_data[model_data[TradingSystemAttributes.PRED_COL] == True]
    models_data_dict = MetaLabelingExample.add_entry_signal_label(data_dict, model_data)

    if insert_into_db == True:
        serialized_model = serialize_model(inference_model)
        insert_successful = SYSTEMS_DB.insert_ml_model(MetaLabelingExample.name, '', serialized_model)
        if insert_successful == False:
            raise Exception('failed to insert data')

    if run_backtest == True:
        trading_system = TradingSystem(
            MetaLabelingExample.name,
            MetaLabelingExample.entry_signal_logic,
            MetaLabelingExample.exit_signal_logic,
            SYSTEMS_DB, CLIENT_DB
        )

        trading_system.run_trading_system_backtest(
            models_data_dict,
            entry_args=system_props.entry_function_args,
            exit_args=system_props.exit_function_args,
            market_state_null_default=True,
            plot_performance_summary=False,
            save_summary_plot_to_path=None,
            plot_returns_distribution=False,
            print_data=True,
            insert_data_to_db_bool=insert_into_db,
        )
