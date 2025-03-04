import pathlib
import sys
import datetime as dt
import pytz
import logging
from functools import lru_cache

from yahooquery import Ticker
import pandas as pd

from persistance.persistance_services.stonkinator_pb2 import (
    DateTime,
    GetExchangesResponse,
    GetPriceDataResponse,
    InsertResponse,
    Instrument,
    Instruments
)
from persistance.persistance_services.grpc_service import SecuritiesGRPCService
import trading_systems.env as env
from trading.data.metadata.price import Price


def set_up_logger(name: str, log_file: str, level=logging.INFO) -> logging.Logger:
    handler = logging.FileHandler(log_file)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def get_yahooquery_data(
    symbol: str, start_date_time: dt.datetime, end_date_time: dt.datetime,
    omxs_stock=False
) -> pd.DataFrame | None:
    try:
        if omxs_stock:
            if "^" in symbol:
                data = Ticker(
                    symbol.upper()
                ).history(start=start_date_time, end=end_date_time)
            else:
                data = Ticker(
                    symbol.upper().replace("_", "-") + ".ST"
                ).history(start=start_date_time, end=end_date_time)
        else:
            data = Ticker(
                symbol.upper()
            ).history(start=start_date_time, end=end_date_time)
        data = data.reset_index()
        return data
    except Exception as e:
        critical_logger.error(
            "\n"
            f"\tERROR while trying to fetch data with yhq. Symbol: {symbol}"
            f"\t{e}"
        )
        return None


@lru_cache(maxsize=1500)
def price_data_get(
    securities_grpc_service: SecuritiesGRPCService, instrument_id: str,
    start_date_time: dt.datetime, end_date_time: dt.datetime
) -> pd.DataFrame | None:
    price_data_get_res: GetPriceDataResponse = securities_grpc_service.get_price_data(
        instrument_id, str(start_date_time), str(end_date_time)
    )

    try:
        if len(price_data_get_res.price_data) <= 2:
            return None
        else:
            return pd.DataFrame(
                [
                    {
                        "instrument_id": price.instrument_id,
                        Price.OPEN: price.open_price, 
                        Price.HIGH: price.high_price, 
                        Price.LOW: price.low_price,
                        Price.CLOSE: price.close_price,
                        Price.VOLUME: price.volume,
                        Price.DT: price.date_time
                    }
                    for price in price_data_get_res.price_data
                ]
            )
    except AttributeError as e:
        print(e)
        return None


def insert_daily_data(
    securities_grpc_service: SecuritiesGRPCService, instruments: list[Instrument], 
    start_date_time: dt.datetime, end_date_time: dt.datetime, omxs_stock=False
):
    for instrument in instruments:
        instrument_id = instrument.id
        symbol = instrument.symbol

        df = get_yahooquery_data(
            symbol, start_date_time, end_date_time,
            omxs_stock=omxs_stock
        )
        if df is None:
            continue
        df = df.drop(columns=["adjclose"], errors="ignore")

        for i, _ in enumerate(df.itertuples()):
            try:
                price_data_insert_res: InsertResponse = securities_grpc_service.insert_price_data(
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
    securities_grpc_service: SecuritiesGRPCService, instrument: Instrument, 
    exchange_name: str,
    omxs_stock=False
):
    start_dt = dt.datetime(1995, 1, 1)
    instrument_id = instrument.id
    symbol = instrument.symbol

    try:
        first_date_time_get_res: DateTime = securities_grpc_service.get_date_time(instrument_id, min=True)
        last_date_time_get_res: DateTime = securities_grpc_service.get_date_time(instrument_id, min=False)
        first_dt = (
            dt.datetime.strptime(
                first_date_time_get_res.date_time, "%Y-%m-%d %H:%M:%S"
            ).date()
        )
        last_dt = (
            dt.datetime.strptime(
                last_date_time_get_res.date_time, "%Y-%m-%d %H:%M:%S"
            ).date() + dt.timedelta(days=1)
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
                securities_grpc_service, [instrument], exchange_name, 
                start_dt, first_dt + dt.timedelta(days=1), 
                omxs_stock=omxs_stock
            )
        if len(last_dt_data):
            insert_daily_data(
                securities_grpc_service, [instrument], exchange_name, 
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
    logger_name = pathlib.Path(__file__).stem
    base_logger = set_up_logger(
        logger_name, f"{env.LOG_DIR_PATH}{pathlib.Path(__file__).stem}.log"
    )
    critical_logger = set_up_logger(
        f"{logger_name}_critical", f"{env.LOG_DIR_PATH}{pathlib.Path(__file__).stem}_critical.log"
    )

    securities_grpc_service = SecuritiesGRPCService(f"{env.STONKINATOR_RPC_SERVICE}:{env.RPC_SERVICE_PORT}")

    exchanges_get_res: GetExchangesResponse = securities_grpc_service.get_exchanges()
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
        exchange_instruments_get_res: Instruments = securities_grpc_service.get_exchange_instruments(exchange.id)
        if exchange_instruments_get_res:
            exchanges_instruments_dict[exchange.id] = (
                list(exchange_instruments_get_res.instruments)
                if exchange_instruments_get_res.instruments
                else None
            )

    last_date_get_res: DateTime = securities_grpc_service.get_last_date(symbol_1="^OMX", symbol_2="^SPX")
    if last_date_get_res:
        last_inserted_date = pd.Timestamp(last_date_get_res.date_time)
    else:
        last_inserted_date = dt.datetime(1995, 1, 1)
    year = last_inserted_date.year
    month = last_inserted_date.month
    day = last_inserted_date.day
    start_date_time = dt.datetime(year, month, day, tzinfo=pytz.timezone("Europe/berlin"))
    end_date_time = dt.datetime.now(tz=pytz.timezone("Europe/Berlin"))
    dt_now = dt.datetime.now(tz=pytz.timezone("Europe/Berlin"))

    log_message =  (
        "Insert data\n"
        f"Current datetime: {dt_now}\n"
        f"Start date: {start_date_time.strftime('%d-%m-%Y')}\n"
        f"End date: {end_date_time.strftime('%d-%m-%Y')}"
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
            dt_now.year == end_date_time.year and
            dt_now.month == end_date_time.month and
            dt_now.day == end_date_time.day
        )
        omxs_stock = False
        if exchange.exchange_name == "OMXS":
            omxs_stock = True
            if end_date_today_check and dt_now.hour < 18:
                end_date_time = end_date_time - dt.timedelta(days=1)
                base_logger.info(
                    "\n"
                    f"\tDate check: {end_date_today_check}, subtracting one day.\n"
                    f"\tNew end date: {end_date_time}."
                )
        else:
            if end_date_today_check:
                end_date_time = end_date_time - dt.timedelta(days=1)
                base_logger.info(
                    "\n"
                    f"\tDate check: {end_date_today_check}, subtracting one day.\n"
                    f"\tNew end date: {end_date_time}."
                )

        insert_daily_data(
            securities_grpc_service, instruments, start_date_time, end_date_time,
            omxs_stock=omxs_stock
        )
        if dt_now.weekday() == 0 and dt_now.day <= 7:
            base_logger.info(f"\nCompleting data for {exchange} instruments.")
            for instrument in instruments:
                complete_historic_data(
                    securities_grpc_service, instrument, exchange.exchange_name,
                    omxs_stock=omxs_stock
                )

    base_logger.info("\n------------------------------------------------------------------------\n")
    critical_logger.info("\n------------------------------------------------------------------------\n")
