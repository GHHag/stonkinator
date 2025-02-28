import datetime as dt

import grpc

import stonkinator_pb2
import stonkinator_pb2_grpc


class GRPCService:

    def __init__(self, channel_address):
        channel = grpc.insecure_channel(channel_address)
        self.__client = stonkinator_pb2_grpc.StonkinatorServiceStub(channel)
        
    def insert_exchange(self, exchange_name: str, currency: str) -> stonkinator_pb2.InsertResponse:
        req = stonkinator_pb2.InsertExchangeRequest(
            exchange_name=exchange_name, currency=currency
        )
        res = self.__client.InsertExchange(req)
        return res

    def get_exchange(self, exchange_name: str):
        req = stonkinator_pb2.GetByNameRequest(name=exchange_name)
        res = self.__client.GetExchange(req)
        return res
        
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
    def get_instrument(self, symbol: str) -> stonkinator_pb2.Instrument:
        req = stonkinator_pb2.GetBySymbolRequest(symbol=symbol)
        res = self.__client.GetInstrument(req)
        return res

    def get_date_time(self, symbol: str, min=True) -> stonkinator_pb2.DateTime:
        req = stonkinator_pb2.GetDateTimeRequest(symbol=symbol, min=min)
        res = self.__client.GetDateTime(req)
        return res

    def get_last_date(self, symbol_1: str, symbol_2: str) -> stonkinator_pb2.DateTime:
        req = stonkinator_pb2.GetLastDateRequest(symbol_1=symbol_1, symbol_2=symbol_2)
        res = self.__client.GetLastDate(req)
        return res

    def insert_price_data(
        self, instrument_id: str, open_price: float, high_price: float,
        low_price: float, close_price: float, volume: int, date_time: dt.datetime
    ):
        req = stonkinator_pb2.PriceData(
            instrument_id=instrument_id, open_price=open_price, high_price=high_price,
            low_price=low_price, close_price=close_price, volume=volume, date_time=str(date_time)
        )
        res = self.__client.InsertPriceData(req)
        return res

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

    def get_market_list_instruments(self, name: str) -> stonkinator_pb2.Instruments:
        req = stonkinator_pb2.GetByNameRequest(name=name)
        res = self.__client.GetMarketListInstruments(req)
        return res

    def get_market_list_instruments_price_data(self):
        # TODO: Implement
        ...


if __name__ == '__main__':
    securities_grpc_service = GRPCService("stonkinator_rpc_service:5001")

    # insert_exchange_res = securities_grpc_service.insert_exchange("test", "little currency")
    # print(insert_exchange_res)
    # print(type(insert_exchange_res))

    get_exchange_res = securities_grpc_service.get_exchange("OMXS")
    print(get_exchange_res)
    print(type(get_exchange_res))
    print(get_exchange_res.id)
    print(get_exchange_res.exchange_name)

    # insert_instrument_res = securities_grpc_service.insert_instrument(
    #     get_exchange_res.id, "TEST", "TEST", "TEST"
    # )
    # print(insert_instrument_res)
    # print(type(insert_instrument_res))

    get_instrument_res = securities_grpc_service.get_instrument("MAERSK_A")
    print(get_instrument_res)
    print(type(get_instrument_res))

    get_first_date_time_res = securities_grpc_service.get_date_time("ALFA", min=True)
    print(get_first_date_time_res.date_time)
    print(type(get_first_date_time_res))
    get_last_date_time_res = securities_grpc_service.get_date_time("ALFA", min=False)
    print(get_last_date_time_res.date_time)
    print(type(get_last_date_time_res))

    get_last_date_res = securities_grpc_service.get_last_date("ALFA", "ATCO_A")
    print(get_last_date_res.date_time)
    print(type(get_last_date_res))

    # insert_price_data_res = securities_grpc_service.insert_price_data(
    #     get_instrument_res.id, 5, 15, 2.5, 10, 9999213, dt.datetime.now()
    # )
    # print(insert_price_data_res)
    # print(type(insert_price_data_res))

    get_price_data_res = securities_grpc_service.get_price_data(
        get_instrument_res.id, 
        get_first_date_time_res.date_time,
        get_last_date_time_res.date_time
    )
    print(get_price_data_res)
    print(type(get_price_data_res))

    # get_market_list_instruments_res = securities_grpc_service.get_market_list_instruments("omxs30")
    get_market_list_instruments_res = securities_grpc_service.get_market_list_instruments("omxs_large_caps")
    print(get_market_list_instruments_res)
    print(type(get_market_list_instruments_res))