import os
import datetime as dt
import pytz

import pandas as pd

from data_frame.data_frame_service_client import DataFrameServiceClient
from persistance.persistance_services.general_messages_pb2 import (
    DateTime,
    CUD,
    Timestamp,
)
from persistance.persistance_services.securities_service_pb2 import  Price
from persistance.persistance_services.securities_grpc_service import SecuritiesGRPCService

from trading_systems.trading_system_examples.trading_system_example import TradingSystemExample


if __name__ == "__main__":
    data_frame_service = DataFrameServiceClient(
        f"{os.environ.get('DF_SERVICE_HOST')}:{os.environ.get('DF_SERVICE_PORT')}"
    )
    securities_grpc_service = SecuritiesGRPCService(
        f"{os.environ.get('RPC_SERVICE_HOST')}:{os.environ.get('RPC_SERVICE_PORT')}"
    )

    symbols = ["ALFA", "HEXA_B", "EVO", "BOL", "CAST"]

    trading_system_name = ""
    dummy_exchange_name = "dummy_exchange"
    insert_exchange_res = securities_grpc_service.insert_exchange(dummy_exchange_name, "SEK")
    exchange = securities_grpc_service.get_exchange(dummy_exchange_name)

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

    for symbol in symbols:
        securities_grpc_service.insert_instrument(exchange.id, f"{symbol}_dummy", f"{symbol}_dummy", "dummy_sector")

    instruments_get_res = securities_grpc_service.get_exchange_instruments(exchange.id)
    if instruments_get_res:
        instruments = list(instruments_get_res.instruments)

    for instrument in instruments:
        instrument_id = instrument.id
        presence = data_frame_service.check_presence(TradingSystemExample.name, instrument_id=instrument.id)
        if presence.is_present == False:
            map_ts_result = data_frame_service.map_trading_system_instrument(TradingSystemExample.name, instrument.id)

        price_data = []
        base_dt = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        for i in range(0, 10):
            current_dt = base_dt + dt.timedelta(days=i)
            unix_timestamp_seconds = int(current_dt.timestamp())
            price_data.append(
                Price(
                    instrument_id=instrument_id,
                    open=100 + i, 
                    high= 105 + i,
                    low= 95 + i, 
                    close=101 + i,
                    volume=100000, 
                    timestamp=Timestamp(unix_timestamp_seconds=unix_timestamp_seconds)
                )
            )

            # price_data_insert_res: CUD = securities_grpc_service.insert_price(
            #     instrument_id, 100 + i, 105 + i, 95 + i, 101 + i, 100000, 
            #     unix_timestamp_seconds
            # )
            # if price_data_insert_res:
            #     print(f"Price data insert.\n\tResult: {price_data_insert_res.num_affected} rows affected")
        try:
            price_data_insert_res: CUD = securities_grpc_service.insert_price_data(price_data)
            if price_data_insert_res:
                print("Price data insert.\n"
                    f"\tResult: {price_data_insert_res.num_affected} rows affected "
                    f"of {len(price_data)} attempted."
                )
        except Exception as e:
            print(f"ERROR while trying to insert data {e}")
            continue

    for instrument in instruments:
        instrument_id = instrument.id
        df = data_frame_service.do_get_df(
            TradingSystemExample.name, instrument_id
        )
        print()
        print(df)
        print(len(df))
