import datetime as dt
import json
from typing import Callable

import pandas as pd

from persistance.securities_db_py_dal.dal import price_data_get_req

from trading.data.metadata.trading_system_attributes import TradingSystemAttributes, classproperty
from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.price import Price
from trading.position.order import Order, LimitOrder, MarketOrder
from trading.position.position import Position
from trading.trading_system.trading_system import TradingSystem

from persistance.stonkinator_mongo_db.systems_mongo_db import TradingSystemsMongoDb
from persistance.stonkinator_mongo_db.instruments_mongo_db import InstrumentsMongoDb

from trading_systems.trading_system_base import TradingSystemBase
from trading_systems.trading_system_properties import TradingSystemProperties
from trading_systems.trading_system_handler import TradingSystemProcessor
from trading_systems.position_sizer.safe_f_position_sizer import SafeFPositionSizer
from trading_systems.instrument_selection.pd_instrument_selector import PdInstrumentSelector


class TradingSystemExample(TradingSystemBase):
    
    @classproperty
    def name(cls):
        return 'trading_system_example'

    @staticmethod
    def entry_signal_logic(
        df: pd.DataFrame, *args, entry_args=None
    ) -> Order | None:
        """
        An example of an entry logic function.

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
            'Order' : Returns an Order object if the entry logic
            condition is met, otherwise None.
        """

        entry_period_param = TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK
        order = None
        entry_condition = (
            df[Price.CLOSE].iloc[-1] >= max(df[Price.CLOSE].iloc[-entry_args[entry_period_param]:])
        )
        if entry_condition == True:
            order = LimitOrder(
                MarketState.ENTRY, df.index[-1], df[Price.CLOSE].iloc[-1], 5,
                direction=TradingSystemAttributes.LONG
            )
        return order

    @staticmethod
    def exit_signal_logic(
        df: pd.DataFrame, position: Position, *args, exit_args=None
    ) -> Order | None:
        """
        An example of an exit logic function.

        Parameters
        ----------
        :param df:
            'Pandas DataFrame' : Data in the form of a Pandas DataFrame
            or a slice of a Pandas DataFrame.
        :param position:
            'Position' : The current position of the trading system.
        :param args:
            'tuple' : A tuple with parameters used with the exit logic.
        :param exit_args:
            Keyword arg 'None/dict' : Key-value pairs with parameters used 
            with the exit logic. Default value=None
        :return:
            'Order' : Returns an Order object if the exit logic
            condition is met, otherwise None.
        """

        exit_period_param = TradingSystemAttributes.EXIT_PERIOD_LOOKBACK
        order = None
        exit_condition = (
            df[Price.CLOSE].iloc[-1] <= min(df[Price.CLOSE].iloc[-exit_args[exit_period_param]:])
        )
        if exit_condition == True:
            order = MarketOrder(MarketState.EXIT, df.index[-1])
        return order

    @staticmethod
    def preprocess_data(
        symbols_list, benchmark_symbol,
        get_data_function: Callable[[str, dt.datetime, dt.datetime], tuple[bytes, int]],
        entry_args: dict, exit_args: dict, start_dt, end_dt,
        ts_processor: TradingSystemProcessor=None
    ):
        data_dict: dict[str, pd.DataFrame] = {}
        for symbol in symbols_list:
            try:
                response_data, response_status = get_data_function(symbol, start_dt, end_dt)
            except ValueError:
                continue
            if response_status == 200:
                data_dict[symbol] = pd.json_normalize(json.loads(response_data))
                if 'instrument_id' in data_dict[symbol].columns:
                    data_dict[symbol] = data_dict[symbol].drop('instrument_id', axis=1)

        benchmark_col_suffix = '_benchmark'
        response_data, response_status = get_data_function(benchmark_symbol, start_dt, end_dt)
        if response_status == 200:
            df_benchmark = pd.json_normalize(json.loads(response_data))
            df_benchmark = df_benchmark.drop('instrument_id', axis=1)
            df_benchmark = df_benchmark.rename(
                columns={
                    Price.OPEN: f'{Price.OPEN}{benchmark_col_suffix}', 
                    Price.HIGH: f'{Price.HIGH}{benchmark_col_suffix}', 
                    Price.LOW: f'{Price.LOW}{benchmark_col_suffix}', 
                    Price.CLOSE: f'{Price.CLOSE}{benchmark_col_suffix}',
                    Price.VOLUME: f'{Price.VOLUME}{benchmark_col_suffix}', 
                    TradingSystemAttributes.SYMBOL: f'{TradingSystemAttributes.SYMBOL}{benchmark_col_suffix}'
                }
            )
            if ts_processor != None:
                ts_processor.penult_dt = pd.to_datetime(df_benchmark[Price.DT].iloc[-2])
                ts_processor.current_dt = pd.to_datetime(df_benchmark[Price.DT].iloc[-1])

        for symbol, data in data_dict.items():
            if data.empty or len(data) < entry_args.get(TradingSystemAttributes.REQ_PERIOD_ITERS):
                print(symbol, 'DataFrame empty')
                del data_dict[symbol]
            else:
                df_benchmark[Price.DT] = pd.to_datetime(df_benchmark[Price.DT])
                data[Price.DT] = pd.to_datetime(data[Price.DT])
                
                data = pd.merge_ordered(data, df_benchmark, on=Price.DT, how='inner')
                data = data.ffill()
                data = data.set_index(Price.DT)

                # apply indicators/features to dataframe
                data['SMA'] = data[Price.CLOSE].rolling(20).mean()

                data_dict[symbol] = data.dropna()
        return data_dict, None

    @classmethod
    def get_properties(
        cls, instruments_db: InstrumentsMongoDb,
        import_instruments=False, path=None
    ):
        required_runs = 2
        benchmark_symbol = '^OMX'
        entry_args = {
            TradingSystemAttributes.REQ_PERIOD_ITERS: 5, 
            TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK: 5
        }
        exit_args = {
            TradingSystemAttributes.EXIT_PERIOD_LOOKBACK: 5
        }

        if import_instruments:
            backtest_df = (
                pd.read_csv(f'{path}/{cls.name}.csv') if path else
                pd.read_csv(f'./backtests/{cls.name}')
            )
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
            required_runs, symbols_list,
            (
                benchmark_symbol, price_data_get_req,
                entry_args, exit_args
            ),
            entry_args, exit_args,
            {
                'plot_fig': False
            },
            SafeFPositionSizer(20, 0.8), (),
            {
                'plot_fig': False,
                'num_of_sims': 500
            }
        )


if __name__ == '__main__':
    import trading_systems.env as env
    SYSTEMS_DB = TradingSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.SYSTEMS_DB)
    INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)
    insert_into_db = False

    system_props: TradingSystemProperties = TradingSystemExample.get_properties(INSTRUMENTS_DB)

    data_dict, _ = TradingSystemExample.preprocess_data(
        system_props.instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt
    )

    trading_system = TradingSystem(
        TradingSystemExample.name,
        TradingSystemExample.entry_signal_logic,
        TradingSystemExample.exit_signal_logic,
        SYSTEMS_DB, SYSTEMS_DB
    )

    trading_system.run_trading_system_backtest(
        data_dict, 
        entry_args=system_props.entry_function_args,
        exit_args=system_props.exit_function_args,
        market_state_null_default=True,
        plot_performance_summary=False,
        print_data=True,
        insert_data_to_db_bool=insert_into_db
    )
