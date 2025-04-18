import os
import datetime as dt
import logging
import pathlib
import json
import pickle

import grpc
from pandas import Timestamp

from persistance.persistance_services.securities_grpc_service import grpc_error_handler
from persistance.persistance_meta_classes.trading_systems_persister import TradingSystemsPersisterBase
from persistance.persistance_services.general_messages_pb2 import (
    CUD,
    DateTime,
    GetBy,
    OperateOn,
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
from trading_systems.model_creation.model_creation import SKModel


LOG_DIR_PATH = os.environ.get("LOG_DIR_PATH")
if not os.path.exists(LOG_DIR_PATH):
    os.makedirs(LOG_DIR_PATH)

logger_name = pathlib.Path(__file__).stem
logger = logging.getLogger(logger_name)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(f"{LOG_DIR_PATH}{logger_name}.log")
logger.addHandler(handler)


class TradingSystemsGRPCService(TradingSystemsPersisterBase):

    def __init__(self, channel_address: str):
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

    @grpc_error_handler(logger, default_return=None)
    def get_or_insert_trading_system(
        self, name: str, current_date_time: dt.datetime | Timestamp
    ) -> TradingSystem:
        req = TradingSystem(
            name=name, current_date_time=DateTime(date_time=str(current_date_time))
        )
        res = self.__client.GetOrInsertTradingSystem(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def update_trading_system_metrics(self, id: str, metrics: dict) -> CUD:
        req = TradingSystem(id=id, metrics=json.dumps(metrics))
        res = self.__client.UpdateTradingSystemMetrics(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def remove_trading_system_relations(self, trading_system_id: str) -> CUD:
        req = OperateOn(str_identifier=trading_system_id)
        res = self.__client.RemoveTradingSystemRelations(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def upsert_market_state(
        self, instrument_id: str, trading_system_id: str, metrics: dict, 
        action: str | None=None, signal_date_time: dt.datetime | Timestamp | None=None
    ) -> CUD:
        req = MarketState(
            instrument_id=instrument_id, trading_system_id=trading_system_id,
            metrics=json.dumps(metrics), action=action,
            signal_date_time=DateTime(date_time=str(signal_date_time)) if signal_date_time else None,
        )
        res = self.__client.UpsertMarketState(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def get_market_states(self, instrument_id: str, action: str) -> MarketStates:
        req = GetBy(str_identifier=instrument_id, alt_str_identifier=action)
        res = self.__client.GetMarketStates(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def update_current_date_time(
        self, trading_system_id: str, current_date_time: dt.datetime | Timestamp
    ) -> CUD:
        req = UpdateCurrentDateTimeRequest(
            trading_system_id=trading_system_id, 
            date_time=DateTime(date_time=str(current_date_time))
        )
        res = self.__client.UpdateCurrentDateTime(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def get_current_date_time(self, trading_system_id: str) -> DateTime:
        req = GetBy(str_identifier=trading_system_id)
        res = self.__client.GetCurrentDateTime(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def upsert_order(
        self, instrument_id: str, trading_system_id: str, order_type: str, action: str,
        created_date_time: dt.datetime | Timestamp, active: bool, direction_long: bool, 
        price: float | None=None, max_duration: int | None=None, duration: int | None=None
    ) -> CUD:
        req = Order(
            instrument_id=instrument_id, trading_system_id=trading_system_id,
            order_type=order_type, action=action,
            created_date_time=DateTime(date_time=str(created_date_time)),
            active=active, direction_long=direction_long,
            price=price, max_duration=max_duration, duration=duration
        )
        res = self.__client.UpsertOrder(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def get_order(self, instrument_id: str, trading_system_id: str) -> Order:
        req = GetBy(str_identifier=instrument_id, alt_str_identifier=trading_system_id)
        res = self.__client.GetOrder(req)
        if res.instrument_id and res.trading_system_id:
            return res
        else:
            return None

    @grpc_error_handler(logger, default_return=None)
    def upsert_position(
        self, instrument_id: str, trading_system_id: str, date_time: dt.datetime | Timestamp,
        position_data: dict, position, id: str | None=None
    ) -> CUD:
        req = Position(
            id=id, instrument_id=instrument_id, trading_system_id=trading_system_id,
            date_time=DateTime(date_time=str(date_time)), position_data=json.dumps(position_data),
            serialized_position=pickle.dumps(position)
        )
        res = self.__client.UpsertPosition(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def insert_positions(
        self, instrument_id: str, trading_system_id: str, positions: list
    ) -> CUD:
        positions = [
            Position(
                instrument_id=instrument_id, trading_system_id=trading_system_id,
                date_time=DateTime(date_time=str(position.current_dt)),
                position_data=json.dumps(position.as_dict),
                serialized_position=pickle.dumps(position)
            )
            for position in positions
        ]
        req = Positions(positions=positions)
        res = self.__client.InsertPositions(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def get_position(self, instrument_id: str, trading_system_id: str):
        req = GetBy(str_identifier=instrument_id, alt_str_identifier=trading_system_id)
        res = self.__client.GetPosition(req)
        if res.id and res.serialized_position:
            return res.id, pickle.loads(res.serialized_position)
        else:
            return None, None

    @grpc_error_handler(logger, default_return=None)
    def get_positions(self, instrument_id: str, trading_system_id: str) -> Positions | None:
        req = GetBy(str_identifier=instrument_id, alt_str_identifier=trading_system_id)
        res = self.__client.GetPositions(req)
        if res.positions:
            positions = list(res.positions)
            return [pickle.loads(position.serialized_position) for position in positions]
        else:
            return None

    @grpc_error_handler(logger, default_return=None)
    def get_trading_system_positions(
        self, trading_system_id: str, num_of_positions: int
    ) -> Positions | None:
        req = GetBy(str_identifier=trading_system_id, alt_int_identifier=num_of_positions)
        res = self.__client.GetTradingSystemPositions(req)
        if res.positions:
            positions = list(res.positions)
            return [pickle.loads(position.serialized_position) for position in positions]
        else:
            return None

    @grpc_error_handler(logger, default_return=None)
    def insert_trading_system_model(
        self, trading_system_id: str, model: SKModel, optional_identifier: str=''
    ) -> CUD:
        req = TradingSystemModel(
            trading_system_id=trading_system_id, serialized_model=pickle.dumps(model),
            optional_identifier=optional_identifier
        )
        res = self.__client.InsertTradingSystemModel(req)
        return res

    @grpc_error_handler(logger, default_return=None)
    def get_trading_system_model(
        self, trading_system_id: str, instrument_id: str | None=None
    ) -> SKModel | None:
        if instrument_id is None:
            req = GetBy(str_identifier=trading_system_id)
        else:
            req = GetBy(str_identifier=trading_system_id, alt_str_identifier=instrument_id)
        res = self.__client.GetTradingSystemModel(req)
        if res.serialized_model:
            return pickle.loads(res.serialized_model)
        else:
            return None


if __name__ == '__main__':
    trading_systems_grpc_service = TradingSystemsGRPCService("rpc_service:5001")

    # trading_systems_grpc_service.get_trading_system(id="test", name="abc")
    # trading_systems_grpc_service.get_trading_system(id="test")
    # trading_systems_grpc_service.get_trading_system(name="abc")

    # x = trading_systems_grpc_service.get_trading_system(id="3d53a30f-d824-4a3a-a217-d6ab488afc10", name="trading_system_example")
    # print(x)

    # x = trading_systems_grpc_service.insert_trading_system("trading_system_example", dt.datetime.now())
    # print(x)