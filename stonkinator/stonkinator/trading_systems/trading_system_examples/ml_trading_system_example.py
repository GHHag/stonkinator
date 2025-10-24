import os
import datetime as dt
import pathlib

import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier

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

from data_frame_service import ml_trading_system_example


LOG_DIR_PATH = os.environ.get("LOG_DIR_PATH")
logger_name = pathlib.Path(__file__).stem
logger = create_timed_rotating_logger(LOG_DIR_PATH, logger_name, 1, 14)


class MLTradingSystemExample(MLTradingSystemBase):

    @classproperty
    def name(cls) -> str:
        return ml_trading_system_example.TRADING_SYSTEM_NAME

    @classproperty
    def minimum_rows(cls) -> int:
        return ml_trading_system_example.MINIMUM_ROWS + 2

    @classproperty
    def target(cls) -> str:
        return ml_trading_system_example.ENTRY_CONDITION_COL

    @classproperty
    def target_period(cls) -> int:
        return ml_trading_system_example.TARGET_PERIOD

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
    ) -> dict[tuple[str, str], pd.DataFrame]:
        target = cls.target
        models_data_dict = cls.create_backtest_models(data_dict, features, target, model_class, params)
        inference_models_dict = cls.create_inference_models(data_dict, features, target, model_class, params)
        for (instrument_id, _), model in inference_models_dict.items():
            trading_systems_persister.insert_trading_system_model(
                trading_system_id, model, optional_identifier=instrument_id
            )
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
                logger.error(
                    "MLTradingSystemExample.make_predictions - "
                    "failed to get model pipeline - "
                    f"input: ({trading_system_id}, {instrument_id})"
                )

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
        data_frame_service: DataFrameServiceClient,
        securities_service: SecuritiesServiceBase,
        instruments_list: list[Instrument],
        start_dt: dt.datetime, end_dt: dt.datetime,
        ts_processor: TradingSystemProcessor | None=None
    ) -> tuple[dict[tuple[str, str], pd.DataFrame], list[str]]:
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

            df = data_frame_service.do_get_df(MLTradingSystemExample.name, instrument.id)
            if df is None or df.empty:
                continue

            df[Price.DT] = pd.to_datetime(df[Price.TIMESTAMP], unit="s")
            if ts_processor is not None and dt_set == False:
                ts_processor.penult_dt = df[Price.DT].iloc[-2]
                ts_processor.current_dt = df[Price.DT].iloc[-1]
                dt_set = True
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
            df = df.set_index(Price.DT)
            data_dict[(instrument.id, instrument.symbol)] = df

        return data_dict, ml_trading_system_example.FEATURES

    @staticmethod
    def reprocess_data(
        data_frame_service: DataFrameServiceClient,
        _: SecuritiesServiceBase,
        instruments_list: list[Instrument], 
        ts_processor: TradingSystemProcessor
    ) -> tuple[dict[tuple[str, str], pd.DataFrame], list[str]]:
        data_dict: dict[tuple[str, str], pd.DataFrame] = {}

        dt_set = False
        for instrument in instruments_list:
            df = data_frame_service.do_get_df(MLTradingSystemExample.name, instrument.id)
            if df is None or df.empty:
                logger.warning(
                    "reprocess_data - df is None or df.empty - "
                    f"input: ({MLTradingSystemExample.name}, {instrument.id})"
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
            data_dict[(instrument.id, instrument.symbol)] = df

        return data_dict, ml_trading_system_example.FEATURES

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
            model_class, params
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
    create_models = True
    target = MLTradingSystemExample.target

    system_props: MLTradingSystemProperties = MLTradingSystemExample.get_properties(
        securities_grpc_service
    )

    eviction_result = data_frame_service.evict(trading_system_id=MLTradingSystemExample.name)
    for instrument in system_props.instruments_list:
        presence = data_frame_service.check_presence(
            MLTradingSystemExample.name, instrument_id=instrument.id
        )
        if presence.is_present == False:
            map_ts_result = data_frame_service.map_trading_system_instrument(MLTradingSystemExample.name, instrument.id)

    tsp = lambda: None
    data_dict, features = MLTradingSystemExample.preprocess_data(
        data_frame_service, securities_grpc_service, system_props.instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt,
        ts_processor=tsp
    )
    set_minimum_rows_result = data_frame_service.set_minimum_rows(
        MLTradingSystemExample.name, MLTradingSystemExample.minimum_rows
    )

    models_data_dict = MLTradingSystemExample.create_backtest_models(
        data_dict, features, target, system_props.model_class, system_props.params,
        verbose=True
    )

    # TODO: How will the params for the inference models be determined?
    inference_params = {'max_depth': 9}

    trading_system_proto = None
    end_dt = tsp.current_dt
    if create_models == True:
        inference_models_dict = MLTradingSystemExample.create_inference_models(
            data_dict, features, target, system_props.model_class, inference_params
        )
        trading_system_proto = trading_systems_grpc_service.get_or_insert_trading_system(
            MLTradingSystemExample.name, end_dt
        )
        trading_system_id = trading_system_proto.id
        trading_systems_grpc_service.remove_trading_system_relations(trading_system_id)
        trading_systems_grpc_service.update_current_date_time(trading_system_id, end_dt)
        for (instrument_id, _), model in inference_models_dict.items():
            insert_res = trading_systems_grpc_service.insert_trading_system_model(
               trading_system_id, model, optional_identifier=instrument_id
            )
            if not insert_res or insert_res.num_affected == 0:
                raise Exception('failed to insert model')

    trading_system = TradingSystem(
        '' if not trading_system_proto else trading_system_id,
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
        insert_data_to_db_bool=create_models,
    )
