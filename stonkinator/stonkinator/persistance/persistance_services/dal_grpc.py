import datetime as dt
import pytz
import logging
from functools import lru_cache

import requests
from yahooquery import Ticker
import pandas as pd
import grpc

import stonkinator_pb2
import stonkinator_pb2_grpc

from persistance.securities_db_py_dal.market_data import get_stock_indices_symbols_list, get_futures_symbols_list
import persistance.securities_db_py_dal.env as env


class SecuritiesGRPCService:
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
        
    # def insert_instrument(
    #     self, exchange_id: str, instrument_name: str, symbol: str, sector: str
    # ) -> stonkinator_pb2.InsertResponse:
    #     req = stonkinator_pb2.Instrument(
    #         exchange_id=exchange_id, instrument_name=instrument_name,
    #         symbol=symbol, sector=sector
    #     )
    #     res = self.__client.InsertInstrument(req)
    #     return res

    # def get_instrument(self, symbol: str) -> stonkinator_pb2.Instrument:
    #     req = stonkinator_pb2.GetBySymbolRequest(symbol=symbol)
    #     res = self.__client.GetInstrument(req)
    #     return res

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

    def get_market_list_instruments(self, name: str) -> stonkinator_pb2.Instruments:
        req = stonkinator_pb2.GetByNameRequest(name=name)
        res = self.__client.GetMarketListInstruments(req)
        return res


def set_up_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def get_yahooquery_data(
    instrument: str, *args,
    start_date=dt.datetime.now(), end_date=dt.datetime.now(),
    omxs_stock=False
):
    try:
        if omxs_stock:
            if "^" in instrument:
                data = Ticker(
                    instrument.upper()
                ).history(start=start_date, end=end_date)
            else:
                data = Ticker(
                    instrument.upper().replace("_", "-") + ".ST"
                ).history(start=start_date, end=end_date)
        else:
            data = Ticker(instrument.upper()).history(start=start_date, end=end_date)
        data = data.reset_index()
        return data
    except (KeyError, AttributeError, TypeError) as e:
        critical_logger.error(
            f"\n\tERROR while trying to fetch data with yhq. Instrument: {instrument}"
            f"\t{e}"
        )


def price_data_post_req(instrument_id, df_json):
    price_data_post_res = requests.post(
        f"https://{env.API_HOST}:{env.API_PORT}{env.API_URL}/price?id={instrument_id}",
        json=df_json["data"]
    )

    base_logger.info(f"\n\tPRICE DATA POST REQUEST:\n\t{price_data_post_res.content}")
    return price_data_post_res.content, price_data_post_res.status_code


@lru_cache(maxsize=500)
def price_data_get_req(symbol, start_date_time, end_date_time):
    price_data_get_res = requests.get(
        # f"https://{env.API_HOST}:{env.API_PORT}{env.API_URL}/price?symbol={symbol}&start={start_date_time}&end={end_date_time}"
        f"https://{env.API_HOST}:{env.API_PORT}{env.API_URL}/price/{symbol}?start={start_date_time}&end={end_date_time}",
        verify=False
    )
    
    if len(price_data_get_res.content) <= 2:
        raise ValueError("response data empty")

    return price_data_get_res.content, price_data_get_res.status_code


def first_dt_get_req(symbol):
    first_dt_res = requests.get(
        f"https://{env.API_HOST}:{env.API_PORT}{env.API_URL}/price/first-dt?symbol={symbol}"
    )
    return first_dt_res.content, first_dt_res.status_code


def last_dt_get_req(symbol):
    last_dt_res = requests.get(
        f"https://{env.API_HOST}:{env.API_PORT}{env.API_URL}/price/last-dt?symbol={symbol}"
    )
    return last_dt_res.content, last_dt_res.status_code


def post_daily_data(
    securities_grpc_service: SecuritiesGRPCService, instruments_list: list[stonkinator_pb2.Instrument], 
    start_date: dt.datetime, end_date: dt.datetime, omxs_stock=False
):
    for instrument in instruments_list:
        instrument_id = instrument.id
        symbol = instrument.symbol
        df = get_yahooquery_data(
            symbol, start_date=start_date, end_date=end_date, omxs_stock=omxs_stock
        )
        df = df.drop(columns=["adjclose"], errors="ignore")
        # df_json = json.loads(df.to_json(orient="table"))

        for price_data in df.iterrows():
            try:
                price_data_insert_res = securities_grpc_service.insert_price_data(
                    instrument_id, price_data['open'], price_data['high'], price_data['low'], 
                    price_data['close'], price_data['volume'], price_data['date']
                )
                if not price_data_insert_res.num_affected and price_data_insert_res.num_affected > 1:
                    critical_logger.warning(
                        f"\n"
                        f"\tAttempted price data insert for {symbol} had no effect"
                        f"\n"
                    )
                else:
                    base_logger.info(
                        f"\n"
                        f"\tPRICE DATA INESRT REQUEST for {symbol}:\n"
                        f"\tRESULT: {price_data_insert_res.num_affected} rows affected"
                    )

            except Exception as e:
                critical_logger.error(
                    f"\n"
                    f"\tEXCEPTION raised while attempting to POST data for {symbol}"
                    f"\n"
                    f"\t{e}"
                )


# def complete_historic_data(symbol, exchange_name, *args, omxs_stock=False):
#     start_dt = dt.datetime(1995, 1, 1)
#     first_dt_get_res_content, first_dt_get_res_status = first_dt_get_req(symbol)
#     last_dt_get_res_content, last_dt_get_res_status = last_dt_get_req(symbol)

#     if not first_dt_get_res_status == 200 and not last_dt_get_res_status == 200:
#         critical_logger.error(
#             f"\n\tERROR while trying to fetch last/first date. Instrument: {symbol}"
#         )
#         first_dt = start_dt
#         last_dt = dt.datetime.now()
#     else:
#         first_dt = dt.datetime.strptime(
#             json.loads(first_dt_get_res_content).split("T")[0], "%Y-%m-%d"
#         ).date()
#         last_dt = dt.datetime.strptime(
#             json.loads(last_dt_get_res_content).split("T")[0], "%Y-%m-%d"
#         ).date() + dt.timedelta(days=1)

#     try:
#         if omxs_stock:
#             if "^" in symbol:
#                 first_dt_data = Ticker(symbol.upper()).history(start=start_dt, end=first_dt)
#                 last_dt_data = Ticker(symbol.upper()).history(start=last_dt)
#             else:
#                 first_dt_data = Ticker(
#                     symbol.upper().replace("_", "-") + ".ST"
#                 ).history(start=start_dt, end=first_dt)
#                 last_dt_data = Ticker(
#                     symbol.upper().replace("_", "-") + ".ST"
#                 ).history(start=last_dt)
#         else:
#             first_dt_data = Ticker(symbol.upper()).history(start=start_dt, end=first_dt)
#             last_dt_data = Ticker(symbol.upper()).history(start=last_dt)
#         first_dt_data = first_dt_data.reset_index()
#         last_dt_data = last_dt_data.reset_index()
#         if len(first_dt_data):
#             post_daily_data(
#                 [symbol], exchange_name, start_dt, first_dt + dt.timedelta(days=1), 
#                 omxs_stock=omxs_stock
#             )
#         if len(last_dt_data):
#             post_daily_data(
#                 [symbol], exchange_name, last_dt - dt.timedelta(days=1), dt.datetime.now(), 
#                 omxs_stock=omxs_stock
#             )
#     except (KeyError, AttributeError, TypeError):
#         critical_logger.error(
#             f"\n\tERROR while trying to fetch data with yhq. Instrument: {symbol}"
#         )


if __name__ == "__main__":
    base_logger = set_up_logger("base", f"{env.DAL_LOG_FILE_PATH}log.log")
    critical_logger = set_up_logger("critical", f"{env.DAL_LOG_FILE_PATH_CRITICAL}log_critical.log")

    securities_grpc_service = SecuritiesGRPCService("localhost:5000")

    # insert_exchange_res = securities_grpc_service.insert_exchange("test", "little currency")
    # print(insert_exchange_res)
    # print(type(insert_exchange_res))

    # get_exchange_res = securities_grpc_service.get_exchange("OMXS")
    # print(get_exchange_res)
    # print(type(get_exchange_res))
    # print(get_exchange_res.id)
    # print(get_exchange_res.exchange_name)

    # insert_instrument_res = securities_grpc_service.insert_instrument(
    #     get_exchange_res.id, "TEST", "TEST", "TEST"
    # )
    # print(insert_instrument_res)
    # print(type(insert_instrument_res))

    # get_instrument_res = securities_grpc_service.get_instrument("MAERSK_A")
    # print(get_instrument_res)
    # print(type(get_instrument_res))

    # get_first_date_time_res = securities_grpc_service.get_date_time("ALFA", min=True)
    # print(get_first_date_time_res.date_time)
    # print(type(get_first_date_time_res))
    # get_last_date_time_res = securities_grpc_service.get_date_time("ALFA", min=False)
    # print(get_last_date_time_res.date_time)
    # print(type(get_last_date_time_res))

    # get_last_date_res = securities_grpc_service.get_last_date("ALFA", "ATCO_A")
    # print(get_last_date_res.date_time)
    # print(type(get_last_date_res))

    # insert_price_data_res = securities_grpc_service.insert_price_data(
    #     get_instrument_res.id, 5, 15, 2.5, 10, 9999213, dt.datetime.now()
    # )
    # print(insert_price_data_res)
    # print(type(insert_price_data_res))

    # get_price_data_res = securities_grpc_service.get_price_data(
    #     get_instrument_res.id, 
    #     get_first_date_time_res.date_time,
    #     get_last_date_time_res.date_time
    # )
    # print(get_price_data_res)
    # print(type(get_price_data_res))

    # get_market_list_instruments_res = securities_grpc_service.get_market_list_instruments("OMXS")
    # print(get_market_list_instruments_res)
    # print(type(get_market_list_instruments_res))

    stock_indices_symbols_list = get_stock_indices_symbols_list()
    futures_symbols_list = get_futures_symbols_list()

    omxs_market_list_instruments_get_res = securities_grpc_service.get_market_list_instruments("OMXS")
    if omxs_market_list_instruments_get_res.instruments:
        omxs_stock_symbols_list = list(omxs_market_list_instruments_get_res.instruments)
    else:
        # TODO: Log potential errors from calling get_market_list_instruments("OMXS")
        omxs_stock_symbols_list = []
    omxs_stock_symbols_list.append({"symbol": "^OMX"})

    exchanges_dict = {
        "omxs": {
            "name": "OMXS",
            "currency": "SEK",
            "instruments": omxs_stock_symbols_list
        },
        "stock indices": {
            "name": "Stock indices",
            "currency": "USD",
            "instruments": stock_indices_symbols_list
        },
        "futures": {
            "name": "Futures",
            "currency": "USD",
            "instruments": futures_symbols_list
        }
    }

    last_date_get_res = securities_grpc_service.get_last_date(symbol_1="^OMX", symbol_2="^SPX")
    if last_date_get_res.date_time:
        last_inserted_date = pd.Timestamp(last_date_get_res.date_time)
    else:
        last_inserted_date = dt.datetime(1995, 1, 1)
    year = last_inserted_date.year
    month = last_inserted_date.month
    day = last_inserted_date.day
    start_date = dt.datetime(year, month, day, tzinfo=pytz.timezone("Europe/berlin"))
    end_date = dt.datetime.now(tz=pytz.timezone("Europe/Berlin"))
    dt_now = dt.datetime.now(tz=pytz.timezone("Europe/Berlin"))

    base_logger.info(
        f"Insert data\n"
        f"Current datetime: {dt_now}\n"
        f"Start date: {start_date.strftime("%d-%m-%Y")}\n"
        f"End date: {end_date.strftime("%d-%m-%Y")}"
    )
    critical_logger.info(
        f"Insert data\n"
        f"Current datetime: {dt_now}\n"
        f"Start date: {start_date.strftime("%d-%m-%Y")}\n"
        f"End date: {end_date.strftime("%d-%m-%Y")}"
    )

    for exchange, exchange_data in exchanges_dict.items():
        base_logger.info(
            f"\nSeeding data for {exchange} instruments"
        )

        exchange_get_res = securities_grpc_service.get_exchange(exchange_data.get("name"))
        if not exchange_get_res.id:
            exchange_insert_res = securities_grpc_service.insert_exchange(
                exchange_data.get("name"),
                exchange_data.get("currency")
            ) 
            if not exchange_insert_res.num_affected:
                critical_logger.info(
                    f"\nBAD REQUEST - Exchange insert request failed for {exchange}"
                )
                continue
            else:
                exchange_get_res = securities_grpc_service.get_exchange(exchange_data.get("name"))

        end_date_today_check = (
            dt_now.year == end_date.year and
            dt_now.month == end_date.month and
            dt_now.day == end_date.day
        )
        omxs_stock = False
        if exchange == "omxs":
            omxs_stock = True
            if end_date_today_check and dt_now.hour < 18:
                end_date = end_date - dt.timedelta(days=1)
                base_logger.info(
                    f"\n"
                    f"\tDate check: {end_date_today_check}, subtracting one day\n"
                    f"\tNew end date: {end_date}"
                )
        else:
            if end_date_today_check:
                end_date = end_date - dt.timedelta(days=1)
                base_logger.info(
                    f"\n"
                    f"\tDate check: {end_date_today_check}, subtracting one day\n"
                    f"\tNew end date: {end_date}"
                )

        post_daily_data(
            securities_grpc_service, exchange_data.get("instruments"),
            start_date=start_date, end_date=end_date,
            omxs_stock=omxs_stock
        )
        # if dt_now.weekday() == 0 and dt_now.day <= 7:
        #     for symbol in exchange_data.get("instruments"):
        #         complete_historic_data(symbol, exchange, omxs_stock=omxs_stock)

    base_logger.info("\n------------------------------------------------------------------------\n")
    critical_logger.info("\n------------------------------------------------------------------------\n")
