import os
import datetime as dt
from typing import Callable

import pandas as pd

from trading.data.metadata.trading_system_attributes import TradingSystemAttributes, classproperty
from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.price import Price
from trading.position.order import Order, LimitOrder, MarketOrder
from trading.position.position import Position
from trading.trading_system.trading_system import TradingSystem

from data_frame.data_frame_service import DataFrameService
from persistance.persistance_services.securities_dal import price_data_get
from persistance.persistance_services.securities_service_pb2 import Instrument, Price as PriceProto
from persistance.persistance_meta_classes.securities_service import SecuritiesServiceBase
from persistance.persistance_services.securities_grpc_service import SecuritiesGRPCService
from persistance.persistance_services.trading_systems_grpc_service import TradingSystemsGRPCService

from trading_systems.trading_system_base import TradingSystemBase
from trading_systems.trading_system_properties import TradingSystemProperties
from trading_systems.trading_system_handler import TradingSystemProcessor
from trading_systems.position_sizer.safe_f_position_sizer import SafeFPositionSizer


class TradingSystemExample(TradingSystemBase):
    
    @classproperty
    def name(cls) -> str:
        return 'trading_system_example'
        
    @classproperty
    def minimum_rows(cls) -> int:
        return 5

    @classproperty
    def entry_args(cls) -> dict:
        return {
            TradingSystemAttributes.REQ_PERIOD_ITERS: cls.minimum_rows, 
        }
        
    @classproperty
    def exit_args(cls) -> dict:
        return {
            TradingSystemAttributes.EXIT_PERIOD_LOOKBACK: cls.minimum_rows
        }

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

        order = None
        if df['5_period_high_close'].iloc[-1] == True:
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
        data_frame_service: DataFrameService,
        securities_service: SecuritiesServiceBase,
        instruments_list: list[Instrument], 
        benchmark_instrument: Instrument,
        get_data_function: Callable[[str, dt.datetime, dt.datetime], pd.DataFrame | None],
        start_dt: dt.datetime, end_dt: dt.datetime,
        ts_processor: TradingSystemProcessor | None=None
    ) -> tuple[dict[tuple[str, str], pd.DataFrame], None]:
        data_dict: dict[tuple[str, str], pd.DataFrame] = {}

        benchmark_col_suffix = '_benchmark'
        df_benchmark = get_data_function(securities_service, benchmark_instrument.id, start_dt, end_dt)
        if df_benchmark is not None:
            df_benchmark = df_benchmark.drop(TradingSystemAttributes.INSTRUMENT_ID, axis=1)
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
            df_benchmark[Price.DT] = pd.to_datetime(df_benchmark[Price.DT])
            if ts_processor != None:
                ts_processor.penult_dt = df_benchmark[Price.DT].iloc[-2]
                ts_processor.current_dt = df_benchmark[Price.DT].iloc[-1]

        for instrument in instruments_list:
            price_data: list[PriceProto] = securities_service.get_price_data(
                instrument.id, start_dt, end_dt
            )
            push_price_stream_res = data_frame_service.push_price_stream(price_data)
            df = data_frame_service.do_get_df(TradingSystemExample.name, instrument.id)
            if df is None or df.empty:
                continue

            df[Price.DT] = pd.to_datetime(df['timestamp'], unit='s')
            df = pd.merge_ordered(df, df_benchmark, on=Price.DT, how='inner')
            df = df.ffill()
            df = df.set_index(Price.DT)
            data_dict[(instrument.id, instrument.symbol)] = df

        return data_dict, None

    @staticmethod
    def reprocess_data(
        data_frame_service: DataFrameService,
        _: SecuritiesServiceBase,
        instruments_list: list[Instrument], 
        ts_processor: TradingSystemProcessor
    ) -> tuple[dict[tuple[str, str], pd.DataFrame], None]:
        data_dict: dict[tuple[str, str], pd.DataFrame] = {}

        dt_set = False
        for instrument in instruments_list:
            df = data_frame_service.do_get_df(TradingSystemExample.name, instrument.id)
            if df is None or df.empty:
                continue

            df[Price.DT] = pd.to_datetime(df['timestamp'], unit='s')

            if dt_set == False:
                ts_processor.penult_dt = df[Price.DT].iloc[-2]
                ts_processor.current_dt = df[Price.DT].iloc[-1]
                dt_set = True
            elif (
                df[Price.DT].iloc[-2] != ts_processor.penult_dt or
                df[Price.DT].iloc[-1] != ts_processor.current_dt
            ):
                raise ValueError(
                    'datetime mismatch:\n'
                    f'penultimate datetime: {ts_processor.penult_dt}\n'
                    f'evaluated penultimate datetime: {df[Price.DT].iloc[-2]}\n'
                    f'current datetime: {ts_processor.current_dt}\n'
                    f'evaluated current datetime: {df[Price.DT].iloc[-1]}'
                )

            df = df.ffill()
            df = df.set_index(Price.DT)
            data_dict[(instrument.id, instrument.symbol)] = df

        return data_dict, None

    @classmethod
    def get_properties(cls, securities_service: SecuritiesServiceBase) -> TradingSystemProperties:
        required_runs = 2

        omxs_large_caps_instruments_list = securities_service.get_market_list_instruments(
            "omxs_large_caps"
        )
        omxs_large_caps_instruments_list = (
            list(omxs_large_caps_instruments_list.instruments)
            if omxs_large_caps_instruments_list
            else None
        )
        omxs_mid_caps_instruments_list = securities_service.get_market_list_instruments(
            "omxs_mid_caps"
        )
        omxs_mid_caps_instruments_list = (
            list(omxs_mid_caps_instruments_list.instruments)
            if omxs_mid_caps_instruments_list
            else None
        )
        instruments_list = omxs_large_caps_instruments_list + omxs_mid_caps_instruments_list

        benchmark_instrument = securities_service.get_instrument("^OMX")

        entry_args = cls.entry_args
        exit_args = cls.exit_args
        return TradingSystemProperties(
            required_runs, instruments_list,
            (benchmark_instrument, price_data_get),
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
    securities_grpc_service = SecuritiesGRPCService(
        f"{os.environ.get('RPC_SERVICE_HOST')}:{os.environ.get('RPC_SERVICE_PORT')}"
    )
    trading_systems_grpc_service = TradingSystemsGRPCService(
        f"{os.environ.get('RPC_SERVICE_HOST')}:{os.environ.get('RPC_SERVICE_PORT')}"
    )

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)
    insert_into_db = False

    trading_system_proto = None
    if insert_into_db == True:
        trading_system_proto = trading_systems_grpc_service.get_or_insert_trading_system(
            TradingSystemExample.name, end_dt
        )

    system_props: TradingSystemProperties = TradingSystemExample.get_properties(
        securities_grpc_service
    )

    data_dict, _ = TradingSystemExample.preprocess_data(
        securities_grpc_service, system_props.instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt
    )

    trading_system = TradingSystem(
        '' if not trading_system_proto else trading_system_proto.id,
        TradingSystemExample.name,
        TradingSystemExample.entry_signal_logic,
        TradingSystemExample.exit_signal_logic,
        trading_systems_grpc_service
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
