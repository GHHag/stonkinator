import os
import logging
import pathlib

import grpc
import pandas as pd
from pyarrow.flight import FlightCallOptions, FlightClient, Ticket

from data_frame.data_frame_service_pb2_grpc import DataFrameServiceStub
from data_frame.data_frame_service_pb2 import (
    MinimumRows,
    Presence,
)
from persistance.persistance_services.general_messages_pb2 import (
    CUD,
    GetBy,
    OperateOn,
)
from persistance.persistance_services.securities_grpc_service import grpc_error_handler
from persistance.persistance_services.securities_service_pb2 import Price


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
            except Exception as e:
                logger.error(f"error in {func.__name__}\n{e}")
                return default_return        
        return wrapper
    return decorator


class DataFrameServiceClient:

    N_ROWS_KEY = b"n-rows"
    EXCLUDE_KEY = b"exclude"
    TRADING_SYSTEM_KEY = "trading_system"
    INSTRUMENT_KEY = "instrument"

    def __init__(self, address: str):
        channel = grpc.insecure_channel(address)
        self.__df_service_client = DataFrameServiceStub(channel)
        self.__flight_client = FlightClient(f"grpc+tcp://{address}")

    @grpc_error_handler(logger, default_return=None)
    def map_trading_system_instrument(
        self, trading_system_id: str, instrument_id: str
    ) -> CUD:
        req = OperateOn(
            str_identifier=trading_system_id, alt_str_identifier=instrument_id
        )
        res = self.__df_service_client.MapTradingSystemInstrument(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def push_price(self, price: Price) -> CUD:
        res = self.__df_service_client.PushPrice(price)
        return res

    @grpc_error_handler(logger, default_return=None)
    def push_price_stream(self, price_data: list[Price]) -> CUD:
        res = self.__df_service_client.PushPriceStream(price for price in price_data)
        return res

    @grpc_error_handler(logger, default_return=None)
    def set_minimum_rows(self, trading_system_id: str, num_rows: int) -> CUD:
        req = MinimumRows(
            operate_on=OperateOn(str_identifier=trading_system_id),
            num_rows=num_rows
        )
        res = self.__df_service_client.SetMinimumRows(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def check_presence(self, trading_system_id: str, instrument_id: str | None = None) -> Presence:
        req = GetBy(
            str_identifier=trading_system_id,
            alt_str_identifier=instrument_id
        )
        res = self.__df_service_client.CheckPresence(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def evict(
        self, 
        trading_system_id: str | None = None, 
        instrument_id: str | None = None
    ) -> CUD:
        req = OperateOn(
            str_identifier=trading_system_id, alt_str_identifier=instrument_id
        )
        res = self.__df_service_client.Evict(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def drop_data_frame_collection_entry(
        self, 
        trading_system_id: str | None = None, 
        instrument_id: str | None = None
    ) -> CUD:
        req = OperateOn(
            str_identifier=trading_system_id, alt_str_identifier=instrument_id
        )
        res = self.__df_service_client.DropDataFrameCollectionEntry(req)
        return res

    @flight_error_handler(logger, default_return=None)
    def do_get_df(
        self, trading_system_id: str, instrument_id: str,
        n_rows: int | None = None, exclude: str | None = None
    ) -> pd.DataFrame:
        ticket = Ticket(
            f"{self.TRADING_SYSTEM_KEY}:{trading_system_id}"
            ":"
            f"{self.INSTRUMENT_KEY}:{instrument_id}"
            .encode("utf-8")
        )

        headers = []
        if n_rows:
            headers.append(self.N_ROWS_KEY, str(n_rows).encode("utf-8"))
        if exclude:
            headers.append(self.EXCLUDE_KEY, exclude.encode("utf-8"))

        call_options = FlightCallOptions(headers=headers)
        reader = self.__flight_client.do_get(ticket, options=call_options)
        table = reader.read_all()
        
        return table.to_pandas()


if __name__ == '__main__':
    data_frame_service = DataFrameServiceClient("data_frame_service:50051")

    add_ts_res = data_frame_service.map_trading_system_instrument(
        "trading_system_example", "66c366f6-d42d-46c9-b6b7-0ba08561ef3e"
    )
    print(add_ts_res)
    add_ts_res = data_frame_service.map_trading_system_instrument(
        "trading_system_example", "0faf6f36-ca72-46c3-a680-5982d44e8b9d"
    )
    print(add_ts_res)
    add_ts_res = data_frame_service.map_trading_system_instrument(
        "trading_system_example", "ab0fa4d8-6ed4-48e1-a693-470b999ba213"
    )
    print(add_ts_res)
    add_ts_res = data_frame_service.map_trading_system_instrument(
        "trading_system_example", "958070c7-8e18-4e13-9d40-13f7ba8dcaa0"
    )
    print(add_ts_res)
    add_ts_res = data_frame_service.map_trading_system_instrument(
        "trading_system_example", "00237200-fee8-45ce-81c3-cc7d9e4b5262"
    )
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
        "trading_system_example", "958070c7-8e18-4e13-9d40-13f7ba8dcaa0"
    )
    print(df)
    df = data_frame_service.do_get_df(
        "trading_system_example", "66c366f6-d42d-46c9-b6b7-0ba08561ef3e"
    )
    print(df)
    df = data_frame_service.do_get_df(
        "trading_system_example", "0faf6f36-ca72-46c3-a680-5982d44e8b9d"
    )
    print(df)
    df = data_frame_service.do_get_df(
        "trading_system_example", "ab0fa4d8-6ed4-48e1-a693-470b999ba213"
    )
    print(df)
    df = data_frame_service.do_get_df(
        "trading_system_example", "00237200-fee8-45ce-81c3-cc7d9e4b5262"
    )
    print(df)