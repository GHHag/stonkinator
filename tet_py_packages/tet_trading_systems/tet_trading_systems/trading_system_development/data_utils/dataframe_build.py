import datetime as dt
import requests

import pandas as pd


def build_dataframe(
    symbols_list, benchmark_symbol, data_retrieve_func, 
    benchmark_data_retrieve_func, *args, 
    write_to_csv_path=None, **kwargs
):
    df_dict = {
        symbol: pd.json_normalize(
            data_retrieve_func(symbol, *args, **kwargs)['data']
        ) 
        for symbol in symbols_list
    }
    
    if benchmark_symbol and benchmark_data_retrieve_func:
        benchmark_df = pd.json_normalize(
            benchmark_data_retrieve_func(benchmark_symbol, *args, **kwargs)['data']
        )
        benchmark_df.drop('symbol', axis=1, inplace=True)
        benchmark_df['Date'] = pd.to_datetime(benchmark_df['Date'])
        benchmark_df.set_index('Date', inplace=True)
        complete_df = benchmark_df
    else:
        complete_df = pd.DataFrame()

    for symbol, symbol_df in dict(df_dict).items():
        if symbol_df is None or symbol_df.empty:
            del df_dict[symbol]
            symbols_list.pop(symbols_list.index(symbol))
            continue
        else:
            symbol_df['Date'] = pd.to_datetime(symbol_df['Date'])
            symbol_df.set_index('Date', inplace=True)
            if 'symbol' in symbol_df:
                symbol_df.drop('symbol', axis=1, inplace=True)
            if benchmark_symbol and benchmark_data_retrieve_func and \
                isinstance(benchmark_df, pd.DataFrame):
                symbol_df = pd.merge_ordered(
                    benchmark_df, symbol_df, on='Date', how='outer', 
                    suffixes=('', f'_{symbol}')
                )
            complete_df = pd.merge_ordered(
                complete_df, symbol_df, 
                on=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'], how='outer'
            )
    complete_df.fillna(method='ffill', inplace=True)

    if write_to_csv_path and write_to_csv_path.endswith('.csv'):
        complete_df.to_csv(write_to_csv_path)

    return complete_df, symbols_list


def get_split_dataframes(
    df, instrument_list, regex=r'^[\w\d\%\_]*', unfiltered_columns=None
):
    split_df_dict = {}

    for ticker in instrument_list:
        split_df_dict[ticker] = df.filter(regex=regex + ticker, axis=1)
        if unfiltered_columns and isinstance(unfiltered_columns, list):
            for col in unfiltered_columns:
                split_df_dict[ticker][col] = df[col]

    return split_df_dict


def get_crypto_data(symbol, start_dt, end_dt, interval='1h', limit=1000):
    url = ''
    df = pd.read_csv(r'')

    start_dt = str(int(start_dt.timestamp() * 1000))
    end_dt = str(int(end_dt.timestamp() * 1000))

    reg_params = {
        'symbol': symbol, 'interval': interval,
        'startTime': start_dt, 'endTime': end_dt,
        'limit': limit
    }

    json_df = pd.read_json(requests.get(url, params=reg_params).text)
    json_df = json_df.iloc[:, 0:6]
    json_df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    json_df.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in json_df.Date]

    return json_df


def build_crypto_dataframe(
    symbol, interval='1h', limit=1000, start_dt=None, end_dt=None
):
    """
    Builds and returns a pandas dataframe for the given symbol 
    with data for the given dates and given interval,
    intervals currently available: 15m, 30m, 1h, 2h, 4h
    """

    assert start_dt is not None
    assert end_dt is not None

    num_of_days = (end_dt - start_dt).days
    periods = 0
    slice_day = 0
    if interval == '15m':
        periods = 24/0.25 * num_of_days
        slice_day = 0.25
    elif interval == '30m':
        periods = 24/0.5 * num_of_days
        slice_day = 0.5
    elif interval == '1h':
        periods = 24/1 * num_of_days
        slice_day = 1
    elif interval == '2h':
        periods = 24/2 * num_of_days
        slice_day = 2
    elif interval == '4h':
        periods = 24/4 * num_of_days
        slice_day = 4

    df_list = []
    requests_to_make = int(periods / limit + 1)
    period_slice = int(periods / requests_to_make + 1)
    slice_start_dt = start_dt
    slice_end_dt = start_dt + dt.timedelta(period_slice / (24 / slice_day))
    for request in range(requests_to_make):
        df = get_crypto_data(
            symbol, slice_start_dt, slice_end_dt, interval=interval, limit=limit
        )
        if request < requests_to_make:
            df_list.append(df.iloc[:-1, :])
        else:
            df_list.append(df)
        slice_start_dt = slice_end_dt
        slice_end_dt = slice_end_dt + dt.timedelta(period_slice / (24 / slice_day))

    full_df = pd.concat(df_list)
    full_df.drop('Date', axis=1, inplace=True)
    full_df.index.name = 'Date'

    return full_df


if __name__ == '__main__':
    #start_dt = dt.datetime(2021, 1, 1)
    #end_dt = dt.datetime(2021, 2, 1)
    #x = get_crypto_data('BTCUSDT', start_dt, end_dt)
    #print(x)

    #start_dt = dt.datetime(2021, 1, 1)
    #end_dt = dt.datetime.now()#(2021, 2, 1)
    #x = build_crypto_df('BTCUSDT', start_dt=start_dt, end_dt=end_dt)
    #print(x.tail(25))
    pass
