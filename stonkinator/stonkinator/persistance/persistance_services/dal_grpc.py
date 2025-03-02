import pathlib
import sys
import datetime as dt
import pytz
import logging
from functools import lru_cache

from yahooquery import Ticker
import pandas as pd

import stonkinator_pb2
from grpc_service import SecuritiesGRPCService
import persistance.securities_db_py_dal.env as env
from trading.data.metadata.price import Price


def set_up_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def get_yahooquery_data(
    symbol: str, start_date, end_date,
    omxs_stock=False
):
    try:
        if omxs_stock:
            if "^" in symbol:
                data = Ticker(
                    symbol.upper()
                ).history(start=start_date, end=end_date)
            else:
                data = Ticker(
                    symbol.upper().replace("_", "-") + ".ST"
                ).history(start=start_date, end=end_date)
        else:
            data = Ticker(symbol.upper()).history(start=start_date, end=end_date)
        data = data.reset_index()
        return data
    except (KeyError, AttributeError, TypeError) as e:
        critical_logger.error(
            "\n"
            f"\tERROR while trying to fetch data with yhq. Symbol: {symbol}"
            f"\t{e}"
        )


@lru_cache(maxsize=1500)
def price_data_get(
    grpc_service: SecuritiesGRPCService, instrument_id: str,
    start_date_time: dt.datetime, end_date_time: dt.datetime
) -> list[stonkinator_pb2.PriceData] | None:
    price_data_get_res = grpc_service.get_price_data(
        instrument_id, start_date_time, end_date_time
    )

    try:
        price_data_list = list(price_data_get_res.price_data)
        if len(price_data_list) <= 2:
            return None
        else:
            return price_data_list
    except AttributeError as e:
        print(e)
        return None


def insert_daily_data(
    grpc_service: SecuritiesGRPCService, instruments_list: list[stonkinator_pb2.Instrument], 
    start_date: dt.datetime, end_date: dt.datetime, omxs_stock=False
):
    for instrument in instruments_list:
        instrument_id = instrument.id
        symbol = instrument.symbol

        df = get_yahooquery_data(
            symbol, start_date, end_date,
            omxs_stock=omxs_stock
        )
        df = df.drop(columns=["adjclose"], errors="ignore")

        for i, _ in enumerate(df.itertuples()):
            try:
                price_data_insert_res = grpc_service.insert_price_data(
                    instrument_id,
                    df[Price.OPEN].iloc[i], df[Price.HIGH].iloc[i],
                    df[Price.LOW].iloc[i], df[Price.CLOSE].iloc[i],
                    df[Price.VOLUME].iloc[i], df[Price.DT].iloc[i]
                )
                if (
                    not price_data_insert_res or 
                    not price_data_insert_res.num_affected or
                    price_data_insert_res.num_affected < 1
                ):
                    critical_logger.warning(
                        "\n"
                        f"\tPrice data insert had no effect. Symbol: {symbol}"
                        "\n"
                    )
                elif (
                    price_data_insert_res and
                    price_data_insert_res.num_affected
                ):
                    base_logger.info(
                        "\n"
                        f"\tPrice data insert. Symbol: {symbol}\n"
                        f"\tResult: {price_data_insert_res.num_affected} rows affected."
                    )
            except Exception as e:
                critical_logger.error(
                    "\n"
                    f"\tERROR while trying to insert data. Symbol: {symbol}"
                    "\n"
                    f"\t{e}"
                )


def complete_historic_data(
    grpc_service: SecuritiesGRPCService, instrument: stonkinator_pb2.Instrument, 
    exchange_name: str,
    omxs_stock=False
):
    start_dt = dt.datetime(1995, 1, 1)
    symbol = instrument.symbol

    try:
        first_date_time_get_res = grpc_service.get_date_time(symbol, min=True)
        last_date_time_get_res = grpc_service.get_date_time(symbol, min=False)
        first_dt = (
            dt.datetime.strptime(first_date_time_get_res.date_time,
            "%Y-%m-%d %H:%M:%S").date()
        )
        last_dt = (
            dt.datetime.strptime(last_date_time_get_res.date_time,
            "%Y-%m-%d %H:%M:%S").date() + dt.timedelta(days=1)
        )
    except AttributeError as e:
        critical_logger.error(
            "\n"
            f"\tERROR while trying to get last/first date. Symbol: {symbol}"
            "\n"
            f"\t{e}"
        )
        first_dt = start_dt
        last_dt = dt.datetime.now()

    try:
        if omxs_stock:
            if "^" in symbol:
                first_dt_data = Ticker(symbol.upper()).history(start=start_dt, end=first_dt)
                last_dt_data = Ticker(symbol.upper()).history(start=last_dt)
            else:
                first_dt_data = Ticker(
                    symbol.upper().replace("_", "-") + ".ST"
                ).history(start=start_dt, end=first_dt)
                last_dt_data = Ticker(
                    symbol.upper().replace("_", "-") + ".ST"
                ).history(start=last_dt)
        else:
            first_dt_data = Ticker(symbol.upper()).history(start=start_dt, end=first_dt)
            last_dt_data = Ticker(symbol.upper()).history(start=last_dt)
        first_dt_data = first_dt_data.reset_index()
        last_dt_data = last_dt_data.reset_index()
        if len(first_dt_data):
            insert_daily_data(
                grpc_service, [instrument], exchange_name, 
                start_dt, first_dt + dt.timedelta(days=1), 
                omxs_stock=omxs_stock
            )
        if len(last_dt_data):
            insert_daily_data(
                grpc_service, [instrument], exchange_name, 
                last_dt - dt.timedelta(days=1), dt.datetime.now(), 
                omxs_stock=omxs_stock
            )
    except (KeyError, AttributeError, TypeError) as e:
        critical_logger.error(
            "\n"
            f"\tERROR while trying to fetch data with yhq. Symbol: {symbol}"
            "\n"
            f"\t{e}"
        )


if __name__ == "__main__":
    base_logger = set_up_logger(
        "base", f"{env.DAL_LOG_FILE_PATH}{pathlib.Path(__file__).stem}.log"
    )
    critical_logger = set_up_logger(
        "critical", f"{env.DAL_LOG_FILE_PATH_CRITICAL}{pathlib.Path(__file__).stem}_critical.log"
    )

    grpc_service = SecuritiesGRPCService(f"{env.STONKINATOR_RPC_SERVICE}:{env.RPC_SERVICE_PORT}")

    exchanges_get_res = grpc_service.get_exchanges()
    if exchanges_get_res:
        exchanges = list(exchanges_get_res.exchanges)
    else:
        critical_logger.error(
            "\n"
            f"\tNo exchange data available."
        )
        sys.exit(1)

    exchanges_instruments_dict = {}
    for exchange in exchanges:
        exchange_instruments_get_res = grpc_service.get_exchange_instruments(exchange.id)
        if exchange_instruments_get_res:
            exchanges_instruments_dict[exchange.id] = (
                list(exchange_instruments_get_res.instruments)
                if exchange_instruments_get_res.instruments
                else None
            )

    last_date_get_res = grpc_service.get_last_date(symbol_1="^OMX", symbol_2="^SPX")
    if last_date_get_res:
        last_inserted_date = pd.Timestamp(last_date_get_res.date_time)
    else:
        last_inserted_date = dt.datetime(1995, 1, 1)
    year = last_inserted_date.year
    month = last_inserted_date.month
    day = last_inserted_date.day
    start_date = dt.datetime(year, month, day, tzinfo=pytz.timezone("Europe/berlin"))
    end_date = dt.datetime.now(tz=pytz.timezone("Europe/Berlin"))
    dt_now = dt.datetime.now(tz=pytz.timezone("Europe/Berlin"))

    log_message =  (
        "Insert data\n"
        f"Current datetime: {dt_now}\n"
        f"Start date: {start_date.strftime('%d-%m-%Y')}\n"
        f"End date: {end_date.strftime('%d-%m-%Y')}"
    )
    base_logger.info(log_message)
    critical_logger.info(log_message)

    for exchange in exchanges:
        base_logger.info(f"\nSeeding data for {exchange} instruments.")

        instruments = exchanges_instruments_dict.get(exchange.id)
        if instruments is None:
            base_logger.info(f"\nNo instruments found for exchange {exchange}.")
            continue

        end_date_today_check = (
            dt_now.year == end_date.year and
            dt_now.month == end_date.month and
            dt_now.day == end_date.day
        )
        omxs_stock = False
        if exchange.exchange_name == "OMXS":
            omxs_stock = True
            if end_date_today_check and dt_now.hour < 18:
                end_date = end_date - dt.timedelta(days=1)
                base_logger.info(
                    "\n"
                    f"\tDate check: {end_date_today_check}, subtracting one day.\n"
                    f"\tNew end date: {end_date}."
                )
        else:
            if end_date_today_check:
                end_date = end_date - dt.timedelta(days=1)
                base_logger.info(
                    "\n"
                    f"\tDate check: {end_date_today_check}, subtracting one day.\n"
                    f"\tNew end date: {end_date}."
                )

        insert_daily_data(
            grpc_service, instruments, start_date, end_date,
            omxs_stock=omxs_stock
        )
        if dt_now.weekday() == 0 and dt_now.day <= 7:
            base_logger.info(f"\nCompleting data for {exchange} instruments.")
            for instrument in instruments:
                complete_historic_data(
                    grpc_service, instrument, exchange.exchange_name,
                    omxs_stock=omxs_stock
                )

    base_logger.info("\n------------------------------------------------------------------------\n")
    critical_logger.info("\n------------------------------------------------------------------------\n")
