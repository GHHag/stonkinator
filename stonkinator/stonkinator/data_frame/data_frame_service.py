import os
import logging
import pathlib

import grpc
import pandas as pd
from pyarrow.flight import FlightCallOptions, FlightClient, Ticket

from data_frame.data_frame_service_pb2_grpc import DataFrameServiceStub
from persistance.persistance_services.securities_grpc_service import grpc_error_handler
from persistance.persistance_services.general_messages_pb2 import (
    CUD,
    OperateOn,
)


LOG_DIR_PATH = os.environ.get("LOG_DIR_PATH")
if not os.path.exists(LOG_DIR_PATH):
    os.makedirs(LOG_DIR_PATH)

logger_name = pathlib.Path(__file__).stem
logger = logging.getLogger(logger_name)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(f"{LOG_DIR_PATH}{logger_name}.log")
logger.addHandler(handler)


def flight_error_handler(logger: logging.Logger, default_return=None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            # except grpc.RpcError as e:
            except Exception as e:
                logger.error(f"error in {func.__name__}\n{e}")
                return default_return        
        return wrapper
    return decorator


class DataFrameService:

    N_ROWS_KEY = b"n-rows"
    EXCLUDE_KEY = b"exclude"
    TRADING_SYSTEM_KEY = "trading_system"
    INSTRUMENT_KEY = "instrument"

    def __init__(self, address: str):
        # df_service_host = os.environ.get("STONKINATOR_DF_SERVICE")
        # df_service_port = os.environ.get("DF_SERVICE_PORT")

        channel = grpc.insecure_channel(address)
        # self.__df_service_client = DataFrameServiceStub(f"{df_service_host}:{df_service_port}")
        # self.__flight_client = FlightClient(f"grpc+tcp://{df_service_host}:{df_service_port}")
        self.__df_service_client = DataFrameServiceStub(channel)
        self.__flight_client = FlightClient(f"grpc+tcp://{address}")

    @grpc_error_handler(logger, default_return=None)
    def add_trading_system(
        self, trading_system_id: str, instrument_id: str
    ) -> CUD:
        req = OperateOn(
            str_identifier=trading_system_id, alt_str_identifier=instrument_id
        )
        res = self.__df_service_client.AddTradingSystem(req)
        return res

    @flight_error_handler(logger, default_return=None)
    def do_get_df(
        self, trading_system_id: str, instrument_id: str,
        n_rows: int | None, exclude: str | None
    ) -> pd.DataFrame:
        ticket = Ticket(
            f"{self.TRADING_SYSTEM_KEY}:{trading_system_id}"
            ":"
            f"{self.INSTRUMENT_KEY}:{instrument_id}"
        )
        headers = [
            (self.N_ROWS_KEY, str(n_rows).encode("utf-8")),
            (self.EXCLUDE_KEY, exclude.encode("utf-8")),
        ]
        call_options = FlightCallOptions(headers=headers)
        reader = self.__flight_client.do_get(ticket, options=call_options)
        table = reader.read_all()
        
        return table.to_pandas()


if __name__ == '__main__':
    data_frame_service = DataFrameService("data_frame_service:50051")

    # add_ts_res = data_frame_service.add_trading_system(
    #     "def456", "00237200-fee8-45ce-81c3-cc7d9e4b5262"
    # )
    # print(add_ts_res)

    # ticket = Ticket(
    #     # b"trading_system:def456:instrument:00237200-fee8-45ce-81c3-cc7d9e4b5262")
    #     b"trading_system:def456:instrument:958070c7-8e18-4e13-9d40-13f7ba8dcaa0")
    # headers = [
    #     (b"n-rows", b"11"),
    #     (b"exclude", b"instrument_id:volume"),
    # ]
    # call_options = FlightCallOptions(headers=headers)
    # reader = data_frame_service.do_get_df(ticket, options=call_options)
    # schema = reader.schema
    # table = reader.read_all()
    # df = table.to_pandas()

    df = data_frame_service.do_get_df(
        "def456", "958070c7-8e18-4e13-9d40-13f7ba8dcaa0", 11, "instrument_id:volume"
    )
    print(df)