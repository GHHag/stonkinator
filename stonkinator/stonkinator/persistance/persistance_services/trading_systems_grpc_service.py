import os
import datetime as dt
import logging
import pathlib
import json

import grpc

from persistance.persistance_meta_classes.trading_systems_persister import TradingSystemsPersisterBase
from persistance.persistance_services.securities_grpc_service import grpc_error_handler
from persistance.persistance_services.general_messages_pb2 import (
    CUD,
    DateTime,
    GetBy,
)
from persistance.persistance_services.trading_systems_service_pb2 import (
    MarketState,
    MarketStates,
    Order,
    Position,
    Positions,
    TradingSystem,
    TradingSystemModel,
    UpdateCurrentDateTimeRequest,
)
from persistance.persistance_services.trading_systems_service_pb2_grpc import (
    TradingSystemsServiceStub
)


LOG_DIR_PATH = os.environ.get("LOG_DIR_PATH")
if not os.path.exists(LOG_DIR_PATH):
    os.makedirs(LOG_DIR_PATH)

logger_name = pathlib.Path(__file__).stem
logger = logging.getLogger(logger_name)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(f"{LOG_DIR_PATH}{logger_name}.log")
logger.addHandler(handler)


# class TradingSystemsGRPCService(TradingSystemsPersisterBase):
class TradingSystemsGRPCService:

    def __init__(self, channel_address):
        # TODO: Pass file paths into the constructor, read the files here with their paths as env variables?
        with open("/etc/ssl/private/stonkinator.key", "rb") as file:
            key = file.read()
        with open("/etc/ssl/stonkinator.pem", "rb") as file:
            cert = file.read()
        with open("/etc/ssl/ca.pem", "rb") as file:
            ca_cert = file.read()

        creds = grpc.ssl_channel_credentials(ca_cert, key, cert)
        channel = grpc.secure_channel(channel_address, creds)
        self.__client = TradingSystemsServiceStub(channel)

    @grpc_error_handler(default_return=None)
    def insert_trading_system(self, name: str, current_date_time: dt.datetime) -> TradingSystem:
        req = TradingSystem(
            name=name, current_date_time=DateTime(date_time=str(current_date_time))
        )
        res = self.__client.InsertTradingSystem(req)
        return res

    @grpc_error_handler(default_return=None)
    def get_trading_system(self, id: str | None=None, name: str | None=None) -> TradingSystem:
        req = GetBy(str_identifier=id, alt_str_identifier=name)
        res = self.__client.GetTradingSystem(req)
        return res

    @grpc_error_handler(default_return=None)
    def update_trading_system_metrics(self, id: str, metrics: dict) -> CUD:
        # TODO: dump to json here or before its passed to this method?
        req = TradingSystem(id=id, metrics=json.dumps(metrics))
        res = self.__client.UpdateTradingSystemMetrics(req)
        return res

    @grpc_error_handler(default_return=None)
    def upsert_market_state(
        self, instrument_id: str, trading_system_id: str, metrics: dict
    ) -> CUD:
        req = MarketState(
            instrument_id=instrument_id, trading_system_id=trading_system_id,
            # TODO: dump to json here or before its passed to this method?
            metrics=json.dumps(metrics)
        )
        res = self.__client.UpsertMarketState(req)
        return res

    @grpc_error_handler(default_return=None)
    def get_market_state(self, instrument_id: str, trading_system_id: str) -> MarketState:
        req = GetBy(str_identifier=instrument_id, alt_str_identifier=trading_system_id)
        res = self.__client.GetMarketState(req)
        return res

    @grpc_error_handler(default_return=None)
    def get_market_states(self, instrument_id: str) -> MarketStates:
        req = GetBy(str_identifier=instrument_id)
        res = self.__client.GetMarketStates(req)
        return res

    @grpc_error_handler(default_return=None)
    def update_current_date_time(
        self, trading_system_id: str, current_date_time: dt.datetime
    ) -> CUD:
        req = UpdateCurrentDateTimeRequest(
            # TODO: Use a GetBy nested in UpdateCurrentDateTimeRequest instead of trading_system_id as string type?
            trading_system_id=trading_system_id, 
            date_time=DateTime(date_time=str(current_date_time))
        )
        res = self.__client.UpdateCurrentDateTime(req)
        return res

    @grpc_error_handler(default_return=None)
    def get_current_date_time(self, trading_system_id: str) -> DateTime:
        req = GetBy(str_identifier=trading_system_id)
        res = self.__client.GetCurrentDateTime(req)
        return res

    @grpc_error_handler(default_return=None)
    def upsert_order(
        self, instrument_id: str, trading_system_id: str, order_data: dict,
        serialized_order: bytes
    ) -> CUD:
        req = Order(
            instrument_id=instrument_id, trading_system_id=trading_system_id,
            # TODO: dump to json here or before its passed to this method?
            order_data=json.dumps(order_data), serialized_order=serialized_order
        )
        res = self.__client.UpsertOrder(req)
        return res

    @grpc_error_handler(default_return=None)
    def get_order(self, instrument_id: str, trading_system_id: str) -> Order:
        req = GetBy(str_identifier=instrument_id, alt_str_identifier=trading_system_id)
        res = self.__client.GetOrder(req)
        return res

    @grpc_error_handler(default_return=None)
    def insert_position(
        self, instrument_id: str, trading_system_id: str, date_time: dt.datetime,
        position_data: dict, serialized_position: bytes
    ) -> CUD:
        req = Position(
            instrument_id=instrument_id, trading_system_id=trading_system_id,
            # TODO: dump to json here or before its passed to this method?
            date_time=DateTime(date_time=str(date_time)), position_data=json.dumps(position_data),
            serialized_position=serialized_position
        )
        res = self.__client.InsertPosition(req)
        return res

    @grpc_error_handler(default_return=None)
    def insert_positions(self, positions: list[Position]) -> CUD:
        req = Positions(positions=positions)
        res = self.__client.InsertPositions(req)
        return res

    @grpc_error_handler(default_return=None)
    def get_position(self, instrument_id: str, trading_system_id: str) -> Position:
        req = GetBy(str_identifier=instrument_id, alt_str_identifier=trading_system_id)
        res = self.__client.GetPosition(req)
        return res

    @grpc_error_handler(default_return=None)
    def get_positions(self, instrument_id: str, trading_system_id: str) -> Positions:
        req = GetBy(str_identifier=instrument_id, alt_str_identifier=trading_system_id)
        res = self.__client.GetPositions(req)
        return res

    @grpc_error_handler(default_return=None)
    def insert_trading_system_model(
        self, trading_system_id: str, instrument_id: str, serialized_model: bytes
    ) -> CUD:
        req = TradingSystemModel(
            trading_system_id=trading_system_id, instrument_id=instrument_id,
            serialized_model=serialized_model
        )
        res = self.__client.InsertTradingSystemModel(req)
        return res

    @grpc_error_handler(default_return=None)
    def get_trading_system_model(
        self, trading_system_id: str, instrument_id: str | None=None
    ) -> TradingSystemModel:
        if instrument_id is None:
            req = GetBy(str_identifier=trading_system_id)
        else:
            req = GetBy(str_identifier=trading_system_id, alt_str_identifier=instrument_id)
        res = self.__client.GetTradingSystemModel(req)
        return res


if __name__ == '__main__':
    trading_systems_grpc_service = TradingSystemsGRPCService("rpc_service:5001")

    # trading_systems_grpc_service.get_trading_system(id="test", name="abc")
    # trading_systems_grpc_service.get_trading_system(id="test")
    # trading_systems_grpc_service.get_trading_system(name="abc")

    # x = trading_systems_grpc_service.get_trading_system(id="3d53a30f-d824-4a3a-a217-d6ab488afc10", name="trading_system_example")
    # print(x)

    # x = trading_systems_grpc_service.insert_trading_system("trading_system_example", dt.datetime.now())
    # print(x)