import datetime as dt
import json

import pandas as pd

from persistance.securities_db_py_dal.dal import price_data_get_req

from trading.data.metadata.trading_system_attributes import TradingSystemAttributes
from trading.trading_system.trading_system import TradingSystem

from persistance.stonkinator_mongo_db.systems_mongo_db import TetSystemsMongoDb
from persistance.stonkinator_mongo_db.instruments_mongo_db import InstrumentsMongoDb

from trading_systems.trading_system_development.trading_systems.trading_system_properties \
    import TradingSystemProperties
from trading_systems.trading_system_development.trading_systems.trading_system_handler \
    import TradingSystemProcessor
from trading_systems.trading_system_management.position_sizer.safe_f_position_sizer \
    import SafeFPositionSizer
from trading_systems.trading_system_state_handler.instrument_selection.pd_instrument_selector \
    import PdInstrumentSelector


def entry_logic_example(df, *args, entry_args=None):
    """
    An example of an entry logic function. Returns True/False
    depending on the conditional statement.

    Parameters
    ----------
    :param df:
        'Pandas DataFrame' : Data in the form of a Pandas DataFrame
        or a slice of a Pandas DataFrame.
    :param args:
        'tuple' : A tuple with parameters used with the entry logic.
    :param entry_args:
        Keyword arg 'None/dict' : Key-value pairs with parameters used 
        with the entry logic. Default value=None
    :return:
        'bool' : True/False depending on if the entry logic
        condition is met or not.
    """

    entry_period_param = TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK
    return df['close'].iloc[-1] >= max(df['close'].iloc[-entry_args[entry_period_param]:]), \
        'long'


def exit_logic_example(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    """
    An example of an exit logic function. Returns True/False
    depending on the conditional statement.

    Parameters
    ----------
    :param df:
        'Pandas DataFrame' : Data in the form of a Pandas DataFrame
        or a slice of a Pandas DataFrame.
    :param trail:
        'Boolean' : Additional conditions can be used to
        activate a mechanism for using trailing exit logic.
    :param trailing_exit_price:
        'float/Decimal' : Upon activating a trailing exit
        mechanism, this variable could for example be given
        a price to use as a limit for returning the exit
        condition as True if the last price would fall below it.
    :param args:
        'tuple' : A tuple with parameters used with the exit logic.
    :param exit_args:
        Keyword arg 'None/dict' : Key-value pairs with parameters used 
        with the exit logic. Default value=None
    :return:
        'bool' : True/False depending on if the exit logic
        condition is met or not.
    """

    exit_period_param = TradingSystemAttributes.EXIT_PERIOD_LOOKBACK
    return df['close'].iloc[-1] <= min(df['close'].iloc[-exit_args[exit_period_param]:]), \
        trail, trailing_exit_price


def preprocess_data(
    symbols_list, ts_processor: TradingSystemProcessor, 
    benchmark_symbol, get_data_function,
    entry_args, exit_args, start_dt, end_dt
):
    df_dict = {}
    for symbol in symbols_list:
        try:
            response_data, response_status = get_data_function(symbol, start_dt, end_dt)
        except ValueError:
            continue
        if response_status == 200:
            df_dict[symbol] = pd.json_normalize(json.loads(response_data))
            if 'instrument_id' in df_dict[symbol].columns:
                df_dict[symbol] = df_dict[symbol].drop('instrument_id', axis=1)

    benchmark_col_suffix = '_benchmark'
    response_data, response_status = get_data_function(benchmark_symbol, start_dt, end_dt)
    if response_status == 200:
        df_benchmark = pd.json_normalize(json.loads(response_data))
        df_benchmark = df_benchmark.drop('instrument_id', axis=1)
        df_benchmark = df_benchmark.rename(
            columns={
                'open': f'open{benchmark_col_suffix}', 
                'high': f'high{benchmark_col_suffix}', 
                'low': f'low{benchmark_col_suffix}', 
                'close': f'close{benchmark_col_suffix}',
                'volume': f'volume{benchmark_col_suffix}', 
                'symbol': f'symbol{benchmark_col_suffix}'
            }
        )
        if ts_processor != None:
            ts_processor.penult_dt = pd.to_datetime(df_benchmark['date'].iloc[-2])
            ts_processor.current_dt = pd.to_datetime(df_benchmark['date'].iloc[-1])

    for symbol, data in dict(df_dict).items():
        if data.empty or len(data) < entry_args[TradingSystemAttributes.REQ_PERIOD_ITERS]:
            print(symbol, 'DataFrame empty')
            del df_dict[symbol]
        else:
            df_benchmark['date'] = pd.to_datetime(df_benchmark['date'])
            df_dict[symbol]['date'] = pd.to_datetime(df_dict[symbol]['date'])
            
            df_dict[symbol] = pd.merge_ordered(data, df_benchmark, on='date', how='inner')
            df_dict[symbol] = df_dict[symbol].ffill()
            df_dict[symbol] = df_dict[symbol].set_index('date')

            # apply indicators/features to dataframe
            df_dict[symbol]['SMA'] = df_dict[symbol]['close'].rolling(20).mean()

            df_dict[symbol] = df_dict[symbol].dropna()

    return df_dict, None


def get_ts_properties(
    instruments_db: InstrumentsMongoDb,
    import_instruments=False, path=None
):
    system_name = 'example_system'
    benchmark_symbol = '^OMX'
    entry_args = {
        TradingSystemAttributes.REQ_PERIOD_ITERS: 5, 
        TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK: 5
    }
    exit_args = {
        TradingSystemAttributes.EXIT_PERIOD_LOOKBACK: 5
    }

    if import_instruments:
        backtest_df = pd.read_csv(f'{path}/{system_name}.csv') if path else \
            pd.read_csv(f'./backtests/{system_name}')
        instrument_selector = PdInstrumentSelector('sharpe_ratio', backtest_df, 0.9)
        instrument_selector()
        symbols_list = instrument_selector.selected_instruments
    else:
        market_list_ids = [
            #instruments_db.get_market_list_id('omxs30')
            instruments_db.get_market_list_id('omxs_large_caps'),
            instruments_db.get_market_list_id('omxs_mid_caps')
        ]
        symbols_list = []
        for market_list_id in market_list_ids:
            symbols_list += json.loads(
                instruments_db.get_market_list_instrument_symbols(
                    market_list_id
                )
            )

    return TradingSystemProperties(
        system_name, 2, 'regular',
        symbols_list,
        preprocess_data,
        (
            benchmark_symbol, price_data_get_req,
            entry_args, exit_args
        ),
        entry_logic_example, exit_logic_example,
        entry_args, exit_args,
        SafeFPositionSizer(20, 0.8), (),
        {
            'plot_fig': False,
            'num_of_sims': 500
        }
    )


if __name__ == '__main__':
    import trading_systems.trading_system_development.trading_systems.env as env
    SYSTEMS_DB = TetSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.SYSTEMS_DB)
    INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)

    system_props = get_ts_properties(INSTRUMENTS_DB)

    df_dict, features = system_props.preprocess_data_function(
        system_props.system_instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt
    )

    trading_system = TradingSystem(
        system_props.system_name,
        system_props.entry_logic_function,
        system_props.exit_logic_function,
        SYSTEMS_DB, SYSTEMS_DB
    )

    trading_system.run_trading_system_backtest(
        df_dict, 
        entry_args=system_props.entry_function_args,
        exit_args=system_props.exit_function_args,
        market_state_null_default=True,
        plot_performance_summary=False,
        print_data=True,
        insert_data_to_db_bool=False
    )
