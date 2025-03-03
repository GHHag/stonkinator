import datetime as dt

import grpc

import persistance.persistance_services.stonkinator_pb2 as stonkinator_pb2
import persistance.persistance_services.stonkinator_pb2_grpc as stonkinator_pb2_grpc


def grpc_error_handler(default_return=None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except grpc.RpcError:
                return default_return        
        return wrapper
    return decorator
 

class SecuritiesGRPCService:

    def __init__(self, channel_address):
        # TODO: Use secure_channel instead
        channel = grpc.insecure_channel(channel_address)
        self.__client = stonkinator_pb2_grpc.StonkinatorServiceStub(channel)

    @grpc_error_handler(default_return=None)
    def insert_exchange(self, exchange_name: str, currency: str) -> stonkinator_pb2.InsertResponse:
        req = stonkinator_pb2.InsertExchangeRequest(
            exchange_name=exchange_name, currency=currency
        )
        res = self.__client.InsertExchange(req)
        return res

    def update_exchange(self):
        # TODO: Implement
        ...

    @grpc_error_handler(default_return=None)
    def get_exchange(self, exchange_name: str) -> stonkinator_pb2.GetExchangeResponse:
        req = stonkinator_pb2.GetByNameRequest(name=exchange_name)
        res = self.__client.GetExchange(req)
        return res
        
    @grpc_error_handler(default_return=None)
    def get_exchanges(self) -> stonkinator_pb2.GetExchangesResponse:
        req = stonkinator_pb2.GetAllRequest()
        res = self.__client.GetExchanges(req)
        return res
        
    @grpc_error_handler(default_return=None)
    def insert_instrument(
        self, exchange_id: str, instrument_name: str, symbol: str, sector: str
    ) -> stonkinator_pb2.InsertResponse:
        req = stonkinator_pb2.Instrument(
            exchange_id=exchange_id, instrument_name=instrument_name,
            symbol=symbol, sector=sector
        )
        res = self.__client.InsertInstrument(req)
        return res

    def update_instrument(self):
        # TODO: Implement
        ...

    # TODO: Is this method needed or not?
    @grpc_error_handler(default_return=None)
    def get_instrument(self, symbol: str) -> stonkinator_pb2.Instrument:
        req = stonkinator_pb2.GetBySymbolRequest(symbol=symbol)
        res = self.__client.GetInstrument(req)
        return res

    def remove_instrument(self):
        # TODO: Implement
        ...

    @grpc_error_handler(default_return=None)
    def get_date_time(self, instrument_id: str, min=True) -> stonkinator_pb2.DateTime:
        req = stonkinator_pb2.GetDateTimeRequest(instrument_id=instrument_id, min=min)
        res = self.__client.GetDateTime(req)
        return res

    @grpc_error_handler(default_return=None)
    def get_last_date(self, symbol_1: str, symbol_2: str) -> stonkinator_pb2.DateTime:
        req = stonkinator_pb2.GetLastDateRequest(symbol_1=symbol_1, symbol_2=symbol_2)
        res = self.__client.GetLastDate(req)
        return res

    @grpc_error_handler(default_return=None)
    def insert_price_data(
        self, instrument_id: str, open_price: float, high_price: float,
        low_price: float, close_price: float, volume: int, date_time: dt.datetime
    ) -> stonkinator_pb2.InsertResponse:
        req = stonkinator_pb2.PriceData(
            instrument_id=instrument_id, open_price=open_price, high_price=high_price,
            low_price=low_price, close_price=close_price, volume=volume, date_time=str(date_time)
        )
        res = self.__client.InsertPriceData(req)
        return res

    @grpc_error_handler(default_return=None)
    def get_price_data(
        self, instrument_id: str, start_date_time: dt.datetime, end_date_time: dt.datetime
    ) -> stonkinator_pb2.GetPriceDataResponse:
        req = stonkinator_pb2.GetPriceDataRequest(
            instrument_id=instrument_id,
            start_date_time=start_date_time,
            end_date_time=end_date_time
        )
        res = self.__client.GetPriceData(req)
        return res

    def insert_market_list(self):
        # TODO: Implement
        ...

    def update_market_list(self):
        # TODO: Implement
        ...

    @grpc_error_handler(default_return=None)
    def get_exchange_instruments(self, exchange_id: str) -> stonkinator_pb2.Instruments:
        req = stonkinator_pb2.GetByIdRequest(id=exchange_id)
        res = self.__client.GetExchangeInstruments(req)
        return res

    def insert_market_list_instrument(self):
        # TODO: Implement
        ...

    def remove_market_list_instrument(self):
        # TODO: Implement
        ...

    @grpc_error_handler(default_return=None)
    def get_market_list_instruments(self, name: str) -> stonkinator_pb2.Instruments:
        req = stonkinator_pb2.GetByNameRequest(name=name)
        res = self.__client.GetMarketListInstruments(req)
        return res

    def get_market_list_instruments_price_data(self):
        # TODO: Implement
        ...


if __name__ == '__main__':
    grpc_service = SecuritiesGRPCService("rpc_service:5001")

    # insert_exchange_res = grpc_service.insert_exchange("test", "little currency")
    # print(insert_exchange_res)
    # print(type(insert_exchange_res))

    # get_exchange_res = grpc_service.get_exchange("OMXS")
    # print(get_exchange_res)
    # print(type(get_exchange_res))
    # print(get_exchange_res.id)
    # print(get_exchange_res.exchange_name)

    # exchanges_get_res = grpc_service.get_exchanges()
    # print(exchanges_get_res)
    # if exchanges_get_res.exchanges:
    #     print(list(exchanges_get_res.exchanges))
    # print(type(exchanges_get_res))

    # insert_instrument_res = grpc_service.insert_instrument(
    #     get_exchange_res.id, "TEST", "TEST", "TEST"
    # )
    # print(insert_instrument_res)
    # print(type(insert_instrument_res))

    get_instrument_res = grpc_service.get_instrument("ALFA")
    # get_instrument_res = grpc_service.get_instrument("MAERSK_A")
    print(get_instrument_res)
    print(type(get_instrument_res))

    get_first_date_time_res = grpc_service.get_date_time(get_instrument_res.id, min=True)
    print(get_first_date_time_res.date_time)
    print(type(get_first_date_time_res))
    get_last_date_time_res = grpc_service.get_date_time(get_instrument_res.id, min=False)
    print(get_last_date_time_res.date_time)
    print(type(get_last_date_time_res))

    # get_last_date_res = grpc_service.get_last_date("ALFA", "ATCO_A")
    # print()
    # print(get_last_date_res.date_time)
    # print(type(get_last_date_res))

    # insert_price_data_res = grpc_service.insert_price_data(
    #     get_instrument_res.id, 5, 15, 2.5, 10, 9999213, dt.datetime.now().date()
    # )
    # print(insert_price_data_res)
    # print(type(insert_price_data_res))

    # get_price_data_res = grpc_service.get_price_data(
    #     get_instrument_res.id, 
    #     get_first_date_time_res.date_time,
    #     get_last_date_time_res.date_time
    # )
    # try:
    #     print(list(get_price_data_res.price_data))
    # except AttributeError as e:
    #     print(e)

    get_price_data_res = grpc_service.get_price_data(
        get_instrument_res.id,
        get_first_date_time_res.date_time,
        get_last_date_time_res.date_time
    )
    try:
        print(list(get_price_data_res.price_data))
    except AttributeError as e:
        print(e)

    # get_market_list_instruments_res = grpc_service.get_market_list_instruments("omxs30")
    # get_market_list_instruments_res = grpc_service.get_market_list_instruments("omxs_large_caps")
    # print(get_market_list_instruments_res)
    # print(type(get_market_list_instruments_res))

    # for exchange in exchanges_get_res.exchanges:
    #     get_exchange_instruments_res = grpc_service.get_exchange_instruments(exchange.id)
    #     print(len(list(get_exchange_instruments_res.instruments)))
    #     print(type(get_exchange_instruments_res))

    # with_call(req) to test
    # a, b = grpc_service.get_date_time(get_instrument_res.id, min=False)
    # print(b)
    # print(b.code())
    # print(b.details())
    # print(type(b))
    # a, b = grpc_service.get_date_time(get_instrument_res.id, min=False)
    # print(b)
    # print(b.code())
    # print(b.details())
    # print(type(b))