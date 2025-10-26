import os
import datetime as dt
from typing import Callable
import pathlib

import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import numpy as np
from sklearn.svm import SVC
from sklearn.metrics import f1_score
from sklearn.preprocessing import StandardScaler

from trading.data.metadata.trading_system_attributes import TradingSystemAttributes, classproperty
from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.price import Price
from trading.position.order import Order, LimitOrder, MarketOrder
from trading.position.position import Position
from trading.trading_system.trading_system import TradingSystem

from data_frame.data_frame_service_client import DataFrameServiceClient
from persistance.persistance_services.securities_service_pb2 import Instrument, Price as PriceProto
from persistance.persistance_meta_classes.securities_service import SecuritiesServiceBase
from persistance.persistance_services.securities_grpc_service import SecuritiesGRPCService
from persistance.persistance_meta_classes.trading_systems_persister import TradingSystemsPersisterBase
from persistance.persistance_services.trading_systems_grpc_service import TradingSystemsGRPCService

from trading_systems.logger import create_timed_rotating_logger
from trading_systems.trading_system_base import MLTradingSystemBase
from trading_systems.trading_system_properties import MLTradingSystemProperties
from trading_systems.trading_system_handler import TradingSystemProcessor
from trading_systems.position_sizer.safe_f_position_sizer import SafeFPositionSizer
from trading_systems.model_creation.model_creation import (
    SKModel, create_backtest_models, create_inference_model
)

from data_frame_service import meta_labeling_example


LOG_DIR_PATH = os.environ.get("LOG_DIR_PATH")
logger_name = pathlib.Path(__file__).stem
logger = create_timed_rotating_logger(LOG_DIR_PATH, logger_name, 1, 14)


class MetaLabelingExample(MLTradingSystemBase):

    @classproperty
    def name(cls) -> str:
        return meta_labeling_example.TRADING_SYSTEM_NAME

    @classproperty
    def minimum_rows(cls) -> int:
        return meta_labeling_example.MINIMUM_ROWS + 2

    @classproperty
    def target(cls) -> str:
        return meta_labeling_example.EXIT_LABEL

    @classproperty
    def target_period(cls) -> int:
        return meta_labeling_example.TARGET_PERIOD

    @classproperty
    def entry_args(cls) -> dict:
        target_period = cls.target_period
        return {
            TradingSystemAttributes.REQ_PERIOD_ITERS: target_period,
            TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK: target_period,
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
        exit_condition = (
            df[Price.CLOSE]
                .pct_change(position.periods_in_position)
                .mul(100).iloc[-1] >= df['adr'].iloc[-1] * 3.5 or
            -(df['adr'].iloc[-1] * 2.5) > position.unrealised_return or
            position.periods_in_position >= MetaLabelingExample.target_period
        )

        if exit_condition == True:
            order = MarketOrder(MarketState.EXIT, df.index[-1])
        return order

    @staticmethod
    def create_backtest_models(
        data: pd.DataFrame, features: list[str], target: str,
        model_class: SKModel, param_grid: dict,
        pipeline_args: tuple[tuple] | None=None, 
        optimization_metric_func: Callable=f1_score,
        verbose=False
    ) -> pd.DataFrame:
        model_data, selected_params = create_backtest_models(
            data, features, target, model_class, param_grid,
            pipeline_args=pipeline_args,
            optimization_metric_func=optimization_metric_func,
            verbose=verbose
        )
        if verbose == True:
            # TODO: do something with selected_params to determine which params to use for inference models
            print('selected_params', selected_params)
        return model_data

    @staticmethod
    def create_inference_models(
        data: pd.DataFrame, features: list[str], target: str,
        model_class: SKModel, params: dict,
        pipeline_args: tuple[tuple] | None=None
    ) -> SKModel:
        return create_inference_model(
            data, features, target, model_class, params,
            pipeline_args=pipeline_args
        )

    @classmethod
    def operate_models(
        cls, trading_system_id: str, trading_systems_persister: TradingSystemsPersisterBase,
        _, data: pd.DataFrame, model_class: SKModel, params: dict
    ) -> pd.DataFrame:
        features = meta_labeling_example.FEATURES
        target = cls.target
        model_data = cls.create_backtest_models(data, features, target, model_class, params)
        inference_model = cls.create_inference_models(data, features, target, model_class, params)
        trading_systems_persister.insert_trading_system_model(trading_system_id, inference_model)
        return model_data

    @classmethod
    def make_predictions(
        cls, trading_system_id: str, trading_systems_persister: TradingSystemsPersisterBase,
        data_dict: dict[tuple[str, str], pd.DataFrame], data: pd.DataFrame
        ) -> dict[tuple[str, str], pd.DataFrame]:
        model_pipeline: SKModel = trading_systems_persister.get_trading_system_model(trading_system_id)
        if not model_pipeline:
            logger.error(
                "MetaLabelingExample.make_predictions - "
                "failed to get model pipeline - "
                f"input: ({trading_system_id})"
            )

        features = meta_labeling_example.FEATURES
        entry_label_true_symbols = data[TradingSystemAttributes.SYMBOL].unique()
        for instrument in data_dict.keys():
            _, symbol = instrument
            if symbol in entry_label_true_symbols:
                symbol_data = data_dict.get(instrument)
                latest_data_point = symbol_data.iloc[-1].copy()
                pred_data = latest_data_point[features].to_numpy()
                latest_data_point[TradingSystemAttributes.PRED_COL] = (
                    model_pipeline.predict(pred_data.reshape(1, -1))[0]
                )
                latest_data_point_df = pd.DataFrame(latest_data_point).transpose()
                data_dict[instrument] = pd.concat(
                    [data_dict[instrument].iloc[:-1], latest_data_point_df]
                )
            else:
                data_dict[instrument][TradingSystemAttributes.PRED_COL] = False
        return data_dict

    @staticmethod
    def add_entry_signal_label(data_dict: dict[tuple[str, str], pd.DataFrame], model_data: pd.DataFrame):
        for instrument, data in data_dict.items():
            _, symbol = instrument
            dates_to_match = model_data[model_data[TradingSystemAttributes.SYMBOL] == symbol].index
            data_dict[instrument][TradingSystemAttributes.PRED_COL] = data.index.isin(dates_to_match)
        return data_dict

    @staticmethod
    def preprocess_data(
        data_frame_service: DataFrameServiceClient,
        securities_service: SecuritiesServiceBase,
        instruments_list: list[Instrument],
        start_dt: dt.datetime, end_dt: dt.datetime,
        ts_processor: TradingSystemProcessor | None=None, drop_nan_rows=False
    ) -> tuple[dict[tuple[str, str], pd.DataFrame], pd.DataFrame]:
        data_dict: dict[tuple[str, str], pd.DataFrame] = {}
        
        dt_set = False
        for instrument in instruments_list:
            price_data: list[PriceProto] = securities_service.get_price_data(instrument.id, start_dt, end_dt)
            push_price_stream_res = data_frame_service.push_price_stream(price_data)
            logger.info(
                "securities_service.get_price_data - "
                f"input: ({instrument.id}, {start_dt}, {end_dt}) - "
                "data_frame_service.push_price_stream - "
                f"length of price_data: {len(price_data)} - "
                f"result: {push_price_stream_res}"
            )

            df = data_frame_service.do_get_df(MetaLabelingExample.name, instrument.id)
            if df is None or df.empty:
                continue

            df[Price.DT] = pd.to_datetime(df[Price.TIMESTAMP], unit="s")
            if ts_processor is not None and dt_set == False:
                ts_processor.penult_dt = df[Price.DT].iloc[-2]
                ts_processor.current_dt = df[Price.DT].iloc[-1]
            elif (
                df[Price.DT].iloc[-2] != ts_processor.penult_dt or
                df[Price.DT].iloc[-1] != ts_processor.current_dt
            ):
                raise ValueError(
                    'datetime mismatch:\n'
                    f'penultimate datetime: {ts_processor.penult_dt}\n'
                    f'evaluated penultimate datetime: {df[Price.DT].iloc[-2]}\n'
                    f'current datetime: {ts_processor.current_dt}\n'
                    f'evaluated current datetime: {df[Price.DT].iloc[-1]}'
                )

            df = df.ffill()
            df = df.dropna()
            if df.empty:
                continue
            df = df.set_index(Price.DT)
            df[TradingSystemAttributes.SYMBOL] = instrument.symbol
            data_dict[(instrument.id, instrument.symbol)] = df

        composite_df = pd.DataFrame()
        for instrument, data in data_dict.items():
            # TODO: Impute inf values with some derived value from the column where they are found.
            last_row = data.iloc[[-1]]
            data = data.replace([np.inf, -np.inf], 0)
            data = (
                pd.concat([data.iloc[:-1].dropna(), last_row]) if drop_nan_rows == True 
                else pd.concat([data.iloc[:-1], last_row])
            )
            data_dict[instrument] = data

            composite_df = pd.concat([composite_df, data[data[meta_labeling_example.ENTRY_CONDITION_COL] == 1]])
            composite_df = composite_df.sort_index()

        return data_dict, composite_df

    @staticmethod
    def reprocess_data(
        data_frame_service: DataFrameServiceClient,
        _: SecuritiesServiceBase,
        instruments_list: list[Instrument], 
        ts_processor: TradingSystemProcessor
    ) -> tuple[dict[tuple[str, str], pd.DataFrame], pd.DataFrame]:
        data_dict: dict[tuple[str, str], pd.DataFrame] = {}

        dt_set = False
        for instrument in instruments_list:
            df = data_frame_service.do_get_df(MetaLabelingExample.name, instrument.id)
            if df is None or MetaLabelingExample.minimum_rows > len(df):
                logger.warning(
                    "reprocess_data - df is None or df.empty - "
                    f"input: ({MetaLabelingExample.name}, {instrument.id})"
                )
                continue

            df[Price.DT] = pd.to_datetime(df[Price.TIMESTAMP], unit="s")
            if dt_set == False:
                ts_processor.penult_dt = df[Price.DT].iloc[-2]
                ts_processor.current_dt = df[Price.DT].iloc[-1]
                dt_set = True
            elif (
                df[Price.DT].iloc[-2] != ts_processor.penult_dt or
                df[Price.DT].iloc[-1] != ts_processor.current_dt
            ):
                raise ValueError(
                    "datetime mismatch:\n"
                    f"penultimate datetime: {ts_processor.penult_dt}\n"
                    f"evaluated penultimate datetime: {df[Price.DT].iloc[-2]}\n"
                    f"current datetime: {ts_processor.current_dt}\n"
                    f"evaluated current datetime: {df[Price.DT].iloc[-1]}"
                )

            df = df.ffill()
            df = df.dropna()
            df = df.set_index(Price.DT)
            df[TradingSystemAttributes.SYMBOL] = instrument.symbol
            data_dict[(instrument.id, instrument.symbol)] = df

        composite_df = pd.DataFrame()
        for instrument, data in data_dict.items():
            # TODO: Impute inf values with some derived value from the column where they are found.
            last_row = data.iloc[[-1]]
            data = data.replace([np.inf, -np.inf], 0)
            data = pd.concat([data.iloc[:-1], last_row])
            data_dict[instrument] = data

            composite_df = pd.concat([composite_df, data[data[meta_labeling_example.ENTRY_CONDITION_COL] == 1]])
            composite_df = composite_df.sort_index()

        return data_dict, composite_df

    @classmethod
    def get_properties(cls, securities_service: SecuritiesServiceBase) -> MLTradingSystemProperties:
        required_runs = 1

        omxs_large_caps_instruments_list = securities_service.get_market_list_instruments(
            "omxs_large_caps"
        )
        omxs_large_caps_instruments_list = (
            list(omxs_large_caps_instruments_list.instruments)
            if omxs_large_caps_instruments_list
            else None
        )
        omxs_mid_caps_instruments_list = securities_service.get_market_list_instruments(
            "omxs_mid_caps"
        )
        omxs_mid_caps_instruments_list = (
            list(omxs_mid_caps_instruments_list.instruments)
            if omxs_mid_caps_instruments_list
            else None
        )
        instruments_list = omxs_large_caps_instruments_list + omxs_mid_caps_instruments_list

        model_class, params, pipeline_args = (
            SVC,
            {
                'C': np.array([0.01, 0.1, 1.0, 10]),
                'kernel': np.array(['rbf', 'sigmoid', 'poly']),
                'gamma': np.array(['scale', 'auto']),
            },
            (('scaler', StandardScaler()),)
        )

        entry_args = cls.entry_args
        exit_args = cls.exit_args
        return MLTradingSystemProperties(
            required_runs, instruments_list,
            (),
            entry_args, exit_args,
            {
                'plot_fig': False
            },
            SafeFPositionSizer(20, 0.8), (),
            {
                'forecast_data_fraction': 0.7,
                'num_of_sims': 100
            },
            model_class, params, pipeline_args
        )


if __name__ == '__main__':
    data_frame_service = DataFrameServiceClient(
        f"{os.environ.get('DF_SERVICE_HOST')}:{os.environ.get('DF_SERVICE_PORT')}"
    )
    securities_grpc_service = SecuritiesGRPCService(
        f"{os.environ.get('RPC_SERVICE_HOST')}:{os.environ.get('RPC_SERVICE_PORT')}"
    )
    trading_systems_grpc_service = TradingSystemsGRPCService(
        f"{os.environ.get('RPC_SERVICE_HOST')}:{os.environ.get('RPC_SERVICE_PORT')}"
    )

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)
    create_model = True
    run_backtest = True
    target = MetaLabelingExample.target

    system_props: MLTradingSystemProperties = MetaLabelingExample.get_properties(
        securities_grpc_service
    )

    eviction_result = data_frame_service.evict(trading_system_id=MetaLabelingExample.name)
    for instrument in system_props.instruments_list:
        presence = data_frame_service.check_presence(
            MetaLabelingExample.name, instrument_id=instrument.id
        )
        if presence.is_present == False:
            map_ts_result = data_frame_service.map_trading_system_instrument(MetaLabelingExample.name, instrument.id)

    tsp = lambda: None
    data_dict, composite_data = MetaLabelingExample.preprocess_data(
        data_frame_service, securities_grpc_service, system_props.instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt,
        ts_processor=tsp, drop_nan_rows=True
    )
    set_minimum_rows_result = data_frame_service.set_minimum_rows(
        MetaLabelingExample.name, MetaLabelingExample.minimum_rows
    )

    features = meta_labeling_example.FEATURES
    model_data = MetaLabelingExample.create_backtest_models(
        composite_data, features, target, system_props.model_class, system_props.params,
        pipeline_args=system_props.pipeline_args, optimization_metric_func=f1_score, verbose=True
    )

    # TODO: How will the params for the inference models be determined?
    inference_params = {
        'C': 10.0,
        'kernel': 'poly',
        'gamma': 'auto',
        'random_state': 1,
    }

    model_data = model_data[model_data[TradingSystemAttributes.PRED_COL] == True]
    models_data_dict = MetaLabelingExample.add_entry_signal_label(data_dict, model_data)

    trading_system_proto = None
    end_dt = tsp.current_dt
    if create_model == True:
        inference_model = MetaLabelingExample.create_inference_models(
            composite_data, features, target, system_props.model_class, inference_params,
            pipeline_args=system_props.pipeline_args
        )
        trading_system_proto = trading_systems_grpc_service.get_or_insert_trading_system(
            MetaLabelingExample.name, end_dt
        )
        trading_system_id = trading_system_proto.id
        trading_systems_grpc_service.remove_trading_system_relations(trading_system_id)
        trading_systems_grpc_service.update_current_date_time(trading_system_id, end_dt)
        insert_res = trading_systems_grpc_service.insert_trading_system_model(
            trading_system_id, inference_model
        )
        if not insert_res or insert_res.num_affected == 0:
            raise Exception('failed to insert model')

    if run_backtest == True:
        trading_system = TradingSystem(
            '' if not trading_system_proto else trading_system_id,
            MetaLabelingExample.name,
            MetaLabelingExample.entry_signal_logic,
            MetaLabelingExample.exit_signal_logic,
            trading_systems_grpc_service
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
            insert_data_to_db_bool=create_model,
        )
