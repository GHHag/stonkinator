import json
import datetime as dt
import pytz
import requests
import logging

from yahooquery import Ticker
import pandas as pd

from tet_doc_db.instruments_mongo_db.instruments_mongo_db import InstrumentsMongoDb

from securities_db_py_dal.market_data import get_stock_indices_symbols_list, \
    get_futures_symbols_list
import securities_db_py_dal.env as env


def setup_logger(name, log_file, level=logging.INFO):
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
            if '^' in instrument:
                data = Ticker(
                    instrument.upper()).history(start=start_date, end=end_date
                )
            else:
                data = Ticker(
                    instrument.upper().replace('_', '-') + '.ST'
                ).history(start=start_date, end=end_date)
        else:
            data = Ticker(instrument.upper()).history(start=start_date, end=end_date)
        data.reset_index(inplace=True)
        return data
    except (KeyError, AttributeError, TypeError):
        critical_logger.error(
            f'\n\tERROR while trying to fetch data with yhq. Instrument: {instrument}'
        )


def exchange_post_req(exchange_data):
    exchange_post_res = requests.post(
        f'http://{env.API_HOST}:{env.API_PORT}{env.API_URL}/exchange',
        json={
            "exchange_name": exchange_data['name'],
            "currency": exchange_data['currency']
        }
    )

    base_logger.info(f'\n\tEXCHANGE POST REQUEST:\n\t{exchange_post_res.content}')
    return exchange_post_res.content


def exchange_get_req(exchange_name):
    return requests.get(
        f'http://{env.API_HOST}:{env.API_PORT}{env.API_URL}/exchange?exchange={exchange_name}',
    ).json()


def instrument_post_req(exchange_id, symbol):
    instrument_post_res = requests.post(
        f'http://{env.API_HOST}:{env.API_PORT}{env.API_URL}/instrument',
        json={"exchange_id": exchange_id, "symbol": symbol}
    )

    base_logger.info(f'\n\tINSTRUMENT POST REQUEST ({symbol}):\n\t{instrument_post_res.content}')
    return instrument_post_res.content


def instrument_get_req(symbol):
    return requests.get(
        f'http://{env.API_HOST}:{env.API_PORT}{env.API_URL}/instrument?symbol={symbol}'
    ).json()


def price_data_post_req(instrument_id, df_json):
    price_data_post_res = requests.post(
        f'http://{env.API_HOST}:{env.API_PORT}{env.API_URL}/price?id={instrument_id}',
        json=df_json['data']
    )

    base_logger.info(f'\n\tPRICE DATA POST REQUEST:\n\t{price_data_post_res.content}')
    return price_data_post_res.content


def price_data_get_req(symbol, start_date_time, end_date_time):
    return requests.get(
        f'http://{env.API_HOST}:{env.API_PORT}{env.API_URL}/price?symbol={symbol}&start={start_date_time}&end={end_date_time}'
    ).json()


def post_daily_data(
    symbols_list, exchange_name, start_date, end_date, omxs_stock=False
):
    exception_none_df_symbols = ''
    incorrect_data_symbols = ''
    for symbol in symbols_list:
        df = get_yahooquery_data(
            symbol, start_date=start_date, end_date=end_date, omxs_stock=omxs_stock
        )

        if df is None or len(df) == 0:
            exception_none_df_symbols += f'{symbol}, '
        else:
            df = df.drop(columns=['adjclose'], errors='ignore')
            df_json = json.loads(df.to_json(orient='table'))

            try:
                exchange_get_res = exchange_get_req(exchange_name)
                exchange_id = exchange_get_res['id']

                instrument_post_req(exchange_id, symbol)

                instrument_get_res = instrument_get_req(symbol)
                instrument_id = instrument_get_res['id']

                price_data_post_res = json.loads(price_data_post_req(instrument_id, df_json))
                if len(price_data_post_res['incorrectData']) > 0:
                    critical_logger.warning(
                        f'\n'
                        f'\tINCORRECT DATA for {symbol}:\n'
                        f'\t{price_data_post_res["incorrectData"]}'
                    )
                    incorrect_data_symbols += f'{symbol}, '
                    print(symbol, price_data_post_res)

            except Exception:
                critical_logger.error(
                    f'\n'
                    f'\tEXCEPTION raised while attempting to POST data for {symbol}'
                )
                exception_none_df_symbols += f'{symbol}, '

    critical_logger.warning(
        f"\n"
        f"\tSymbols where conditional: 'df is None or len(df) == 0:' resulted in True\n"
        f"\tSymbols: {exception_none_df_symbols}\n"
        f"\tSymbols with incorrect data:\n"
        f"\tSymbols: {incorrect_data_symbols}"
    )


def complete_historic_data(symbol, exchange_name, *args, omxs_stock=False):
    start_dt = dt.datetime(1995, 1, 1)
    first_dt = dt.datetime.strptime(
        first_dt_get_req(symbol).get('data').get('min').split('T')[0], '%Y-%m-%d'
    ).date()
    last_dt = dt.datetime.strptime(
        last_dt_get_req(symbol).get('data').get('max').split('T')[0], '%Y-%m-%d'
    ).date() + dt.timedelta(days=1)

    try:
        if omxs_stock:
            if '^' in symbol:
                first_dt_data = Ticker(symbol.upper()).history(start=start_dt, end=first_dt)
                last_dt_data = Ticker(symbol.upper()).history(start=last_dt)
            else:
                first_dt_data = Ticker(
                    symbol.upper().replace('_', '-') + '.ST'
                ).history(start=start_dt, end=first_dt)
                last_dt_data = Ticker(
                    symbol.upper().replace('_', '-') + '.ST'
                ).history(start=last_dt)
        else:
            first_dt_data = Ticker(symbol.upper()).history(start=start_dt, end=first_dt)
            last_dt_data = Ticker(symbol.upper()).history(start=last_dt)
        first_dt_data.reset_index(inplace=True)
        last_dt_data.reset_index(inplace=True)
        if len(first_dt_data):
            post_daily_data(
                [symbol], exchange_name, start_dt, first_dt + dt.timedelta(days=1), 
                omxs_stock=omxs_stock
            )
        if len(last_dt_data):
            post_daily_data(
                [symbol], exchange_name, last_dt - dt.timedelta(days=1), dt.datetime.now(), 
                omxs_stock=omxs_stock
            )
    except (KeyError, AttributeError, TypeError):
        critical_logger.error(
            f'\n\tERROR while trying to fetch data with yhq. Instrument: {symbol}'
        )


def first_dt_get_req(symbol):
    first_dt_res = requests.get(
        f'http://{env.API_HOST}:{env.API_PORT}{env.API_URL}/price/first-dt?symbol={symbol}'
    ).json()
    return first_dt_res


def last_dt_get_req(symbol):
    last_dt_res = requests.get(
        f'http://{env.API_HOST}:{env.API_PORT}{env.API_URL}/price/last-dt?symbol={symbol}'
    ).json()
    return last_dt_res


def last_date_get_req(instrument_one, instrument_two):
    last_date_res = requests.get(
        f'http://{env.API_HOST}:{env.API_PORT}{env.API_URL}/price/date?symbol1={instrument_one}&symbol2={instrument_two}'
    ).json()
    return last_date_res


if __name__ == '__main__':
    base_logger = setup_logger('base', f'{env.DAL_LOG_FILE_PATH}log.log')
    critical_logger = setup_logger('critical', f'{env.DAL_LOG_FILE_PATH_CRITICAL}log_critical.log')

    #INSTRUMENTS_DB = InstrumentsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.INSTRUMENTS_DB)
    INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)

    market_list_ids = [
        INSTRUMENTS_DB.get_market_list_id('omxs_large_caps'),
        INSTRUMENTS_DB.get_market_list_id('omxs_mid_caps')
    ]
    omxs_stock_symbols_list = []
    for market_list_id in market_list_ids:
        omxs_stock_symbols_list += json.loads(
            INSTRUMENTS_DB.get_market_list_instrument_symbols(
                market_list_id
            )
        )
    omxs_stock_symbols_list.append('^OMX')
    stock_indices_symbols_list = get_stock_indices_symbols_list()
    futures_symbols_list = get_futures_symbols_list()

    exchanges_dict = {
        'omxs': {
            'name': 'OMXS',
            'currency': 'SEK',
            'symbols': omxs_stock_symbols_list
        },
        'stock indices': {
            'name': 'Stock indices',
            'currency': 'USD',
            'symbols': stock_indices_symbols_list
        },
        'futures': {
            'name': 'Futures',
            'currency': 'USD',
            'symbols': futures_symbols_list
        }
    }

    last_inserted_date = last_date_get_req('^OMX', '^SPX')
    if not last_inserted_date:
        last_inserted_date = dt.datetime(1995, 1, 1)
    else:
        last_inserted_date = pd.Timestamp(last_inserted_date)
    year = last_inserted_date.year
    month = last_inserted_date.month
    day = last_inserted_date.day
    start_date = dt.datetime(year, month, day, tzinfo=pytz.timezone('Europe/berlin'))
    end_date = dt.datetime.now(tz=pytz.timezone('Europe/Berlin'))
    dt_now = dt.datetime.now(tz=pytz.timezone('Europe/Berlin'))

    base_logger.info(
        f'Insert data\n'
        f'Current datetime: {dt_now}\n'
        f'Start date: {start_date.strftime("%d-%m-%Y")}\n'
        f'End date: {end_date.strftime("%d-%m-%Y")}'
    )
    critical_logger.info(
        f'Insert data\n'
        f'Current datetime: {dt_now}\n'
        f'Start date: {start_date.strftime("%d-%m-%Y")}\n'
        f'End date: {end_date.strftime("%d-%m-%Y")}'
    )

    yes_no_input = 'y' # input('Enter: ')
    if yes_no_input.lower() == 'y':
        for exchange, exchange_data in exchanges_dict.items():
            base_logger.info(
                f'\nSeeding data for {exchange} instruments'
            )
            exchange_post_req(exchange_data)

            end_date_today_check = dt_now.year == end_date.year and \
                dt_now.month == end_date.month and \
                dt_now.day == end_date.day
            omxs_stock = False
            if exchange == 'omxs':
                omxs_stock = True
                if end_date_today_check and dt_now.hour < 18:
                    end_date = end_date - dt.timedelta(days=1)
                    base_logger.info(
                        f'\n'
                        f'\tDate check: {end_date_today_check}, subtracting one day\n'
                        f'\tNew end date: {end_date}'
                    )
            else:
                if end_date_today_check:
                    end_date = end_date - dt.timedelta(days=1)
                    base_logger.info(
                        f'\n'
                        f'\tDate check: {end_date_today_check}, subtracting one day\n'
                        f'\tNew end date: {end_date}'
                    )

            post_daily_data(
                exchange_data.get('symbols'), exchange,
                start_date=start_date, end_date=end_date,
                omxs_stock=omxs_stock
            )
            if dt_now.weekday() == 0 and dt_now.day <= 7:
                for symbol in exchange_data.get('symbols'):
                    complete_historic_data(symbol, exchange, omxs_stock=omxs_stock)

    base_logger.info('\n------------------------------------------------------------------------\n')
    critical_logger.info('\n------------------------------------------------------------------------\n')
