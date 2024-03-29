import os
import sys
import importlib
import datetime as dt
import json
import argparse

import pandas as pd
from sterunets.data_handler import FeatureBlueprint, TimeSeriesDataHandler

from TETrading.data.metadata.trading_system_attributes import TradingSystemAttributes
from TETrading.data.metadata.market_state_enum import MarketState
from TETrading.trading_system.trading_system import TradingSystem

from tet_trading_systems.trading_system_development.trading_systems.trading_system_properties.trading_system_properties \
    import TradingSystemProperties
from tet_trading_systems.trading_system_management.position_sizer.position_sizer import IPositionSizer

from tet_doc_db.doc_database_meta_classes.tet_signals_doc_db import ITetSignalsDocumentDatabase
from tet_doc_db.doc_database_meta_classes.tet_systems_doc_db import ITetSystemsDocumentDatabase
from tet_doc_db.doc_database_meta_classes.time_series_doc_db import ITimeSeriesDocumentDatabase
from tet_doc_db.tet_mongo_db.systems_mongo_db import TetSystemsMongoDb
from tet_doc_db.time_series_mongo_db.time_series_mongo_db import TimeSeriesMongoDb
from tet_doc_db.instruments_mongo_db.instruments_mongo_db import InstrumentsMongoDb
import tet_trading_systems.trading_system_development.trading_systems.env as env


#INSTRUMENTS_DB = InstrumentsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.INSTRUMENTS_DB)
TIME_SERIES_DB = TimeSeriesMongoDb(env.LOCALHOST_MONGO_DB_URL, env.TIME_SERIES_DB)
SYSTEMS_DB = TetSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.SYSTEMS_DB)
CLIENT_DB = TetSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.CLIENT_DB)
# CLIENT_DB = TetSystemsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)

INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
#TIME_SERIES_DB = TimeSeriesMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
#SYSTEMS_DB = TetSystemsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
#CLIENT_DB = TetSystemsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)


class TradingSystemProcessor:
    
    # __data: dict[str, TimeSeriesDataHandler] = {}
    __data: dict[str, pd.DataFrame] = {}
    
    def __init__(
        self, ts_properties: TradingSystemProperties, 
        systems_db: ITetSystemsDocumentDatabase, 
        client_db: ITetSystemsDocumentDatabase,
        start_dt: dt.datetime, end_dt: dt.datetime
    ):
        self.__ts_properties = ts_properties
        self.__systems_db: ITetSystemsDocumentDatabase = systems_db
        self.__client_db: ITetSystemsDocumentDatabase = client_db
        self.__trading_system = TradingSystem(
            self.__ts_properties.system_name,
            self.__ts_properties.entry_logic_function, 
            self.__ts_properties.exit_logic_function,
            self.__systems_db, self.__client_db
        )
        self.__start_dt: dt.datetime = start_dt
        self.__end_dt: dt.datetime = end_dt
        self.__preprocess_data(self.__start_dt, self.__end_dt)

    def __preprocess_data(self, start_dt: dt.datetime, end_dt: dt.datetime):
        # self.__data, _ = self.__ts_properties.preprocess_data_function(
        self.__data, pred_features = self.__ts_properties.preprocess_data_function(
            self.__ts_properties.system_instruments_list,
            *self.__ts_properties.preprocess_data_args, start_dt, end_dt
        )

    def reprocess_data(self, end_dt: dt.datetime):
        pass

    def _run_trading_system(
        self, full_run,
        capital=10000, capital_fraction=1.0, avg_yearly_periods=251,  
        market_state_null_default=False, run_monte_carlo_sims=False, num_of_sims=2500,
        print_dataframe=False, plot_fig=False, plot_positions=False, write_to_file_path=None,
        save_summary_plot_to_path=False, system_analysis_to_csv_path=None,
        plot_returns_distribution=False,
        print_data=False,
        insert_into_db=False,
        pos_list_slice_years_est=2,
        **kwargs
    ):
        if full_run:
            self.__trading_system.run_trading_system_backtest(
                self.__data,
                capital=capital,
                capital_fraction=capital_fraction,
                avg_yearly_periods=avg_yearly_periods,
                # market_state_null_default=market_state_null_default,
                market_state_null_default=True,
                plot_performance_summary=plot_fig,
                save_summary_plot_to_path=save_summary_plot_to_path,
                system_analysis_to_csv_path=system_analysis_to_csv_path, 
                plot_returns_distribution=plot_returns_distribution,
                save_returns_distribution_plot_to_path=None, 
                run_monte_carlo_sims=run_monte_carlo_sims,
                num_of_monte_carlo_sims=num_of_sims,
                monte_carlo_data_amount=0.65,
                plot_monte_carlo=plot_fig,
                print_monte_carlo_df=print_dataframe,
                monte_carlo_analysis_to_csv_path=None, 
                print_data=print_data,
                commission_pct_cost=0.0025,
                entry_args=self.__ts_properties.entry_function_args,
                exit_args=self.__ts_properties.exit_function_args,
                fixed_position_size=True,
                generate_signals=True,
                plot_positions=plot_positions,
                save_position_figs_path=None,
                write_signals_to_file_path=write_to_file_path,
                insert_data_to_db_bool=insert_into_db,
                pos_list_slice_years_est=pos_list_slice_years_est
            )
        else:
            self.__trading_system.run_trading_system(
                self.__data,
                capital=capital,
                capital_fraction=capital_fraction,
                commission_pct_cost=0.0025,  # goes into **kwargs
                entry_args=self.__ts_properties.entry_function_args,  # goes into **kwargs
                exit_args=self.__ts_properties.exit_function_args,  # goes into **kwargs
                fixed_position_size=True,  # goes into **kwargs
                print_data=print_data,
                write_signals_to_file_path=write_to_file_path,
                insert_data_to_db_bool=insert_into_db
            )

    def _handle_trading_system(
        self, full_run: bool,
        time_series_db=None, insert_into_db=False, 
        **kwargs
    ):
        for i in range(self.__ts_properties.required_runs):
            if i > 0 and full_run is False:
                break

            insert_data = True if (i + 1) == self.__ts_properties.required_runs and insert_into_db else False
            insert_data = True if full_run is False else insert_data
            self._run_trading_system(
                full_run,
                insert_into_db=insert_data,
                **self.__ts_properties.position_sizer.position_sizer_data_dict
            )
            # not optimal for TradingSystem class to insert market state data and
            # positions to database and then reading it back into the application here
            market_states_data: list[dict] = json.loads(
                self.__client_db.get_market_state_data(
                    self.__ts_properties.system_name, MarketState.ENTRY.value
                )
            )

            for data_dict in market_states_data:
                position_list, num_of_periods = self.__systems_db.get_single_symbol_position_list(
                    self.__ts_properties.system_name, 
                    data_dict.get(TradingSystemAttributes.SYMBOL),
                    serialized_format=True, return_num_of_periods=True
                )
                self.__ts_properties.position_sizer(
                    position_list, num_of_periods,
                    *self.__ts_properties.position_sizer_call_args,
                    symbol=data_dict.get(TradingSystemAttributes.SYMBOL), 
                    **self.__ts_properties.position_sizer_call_kwargs,
                    **self.__ts_properties.position_sizer.position_sizer_data_dict
                )

        pos_sizer_data_dict = self.__ts_properties.position_sizer.get_position_sizer_data_dict()
        self.__client_db.update_market_state_data(
            self.__ts_properties.system_name, json.dumps(pos_sizer_data_dict)
        )

    def _handle_ext_pos_sizer_trading_system(
        self, full_run: bool,
        time_series_db=None, insert_into_db=False, plot_fig=False,
        **kwargs
    ):
        for i in range(self.__ts_properties.required_runs):
            # if i > 0 and full_run is False:
            #     break

            insert_data = True if (i + 1) == self.__ts_properties.required_runs and insert_into_db else False
            insert_data = True if full_run is False else insert_data
            self._run_trading_system(
                full_run,
                insert_into_db=insert_data,
                **self.__ts_properties.position_sizer.position_sizer_data_dict
            )
            position_list, num_of_periods = self.__systems_db.get_position_list(
                self.__ts_properties.system_name, return_num_of_periods=True
            )
            if position_list is None or num_of_periods is None:
                continue
            self.__ts_properties.position_sizer(
                position_list, num_of_periods,
                *self.__ts_properties.position_sizer_call_args,
                **self.__ts_properties.position_sizer_call_kwargs,
                **self.__ts_properties.position_sizer.position_sizer_data_dict
            )

        pos_sizer_data_dict = self.__ts_properties.position_sizer.get_position_sizer_data_dict()
        self.__systems_db.insert_system_metrics(self.__ts_properties.system_name, pos_sizer_data_dict)

    def __call__(
        self, date: dt.datetime, full_run: bool,
        time_series_db=None, insert_into_db=False, **kwargs
    ):
        # get new data here and append it to __data member

        # make some date check on given date against last date of self.__data

        # call reprocess_data if new data is available
        # self.reprocess_data(date)

        self._handle_trading_system(
        # self._handle_ext_pos_sizer_trading_system(
            full_run,
            time_series_db=time_series_db, 
            insert_into_db=insert_into_db,
            **kwargs
        )


class TradingSystemHandler:

    __trading_systems: list[TradingSystemProcessor] = []

    def __init__(
        self, trading_system_properties_list: list[TradingSystemProperties],
        systems_db: ITetSystemsDocumentDatabase, 
        client_db: ITetSystemsDocumentDatabase,
        start_dt: dt.datetime, end_dt: dt.datetime, 
    ):
        for ts_properties in trading_system_properties_list:
            self.__trading_systems.append(
                TradingSystemProcessor(
                    ts_properties, systems_db, client_db, start_dt, end_dt
                )
            )

    def run_trading_systems(
        self, date: dt.datetime, full_run: bool,
        time_series_db=None
    ):
        for trading_system_state_handler in self.__trading_systems:
            trading_system_state_handler(
                date, full_run, 
                time_series_db=time_series_db, insert_into_db=True
            )


def run_trading_system(*args, **kwargs):
    pass


def handle_ml_trading_system(
    system_props: TradingSystemProperties, start_dt, end_dt, 
    from_latest_exit: bool,
    systems_db: ITetSystemsDocumentDatabase, 
    client_db: ITetSignalsDocumentDatabase, 
    time_series_db: ITimeSeriesDocumentDatabase=None,
    insert_into_db=False, plot_fig=False 
):
    data, pred_features_data = system_props.preprocess_data_function(
        system_props.system_instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt
    )

    #if time_series_db:
    #   time_series_db.insert_pandas_time_series_data(data)

    system_state_handler = system_props.system_state_handler(
        # if only pred_features_data is what differs between this and handle_trading_system 
        # function try to make both compatible with all system types
        *system_props.system_state_handler_args, data, pred_features_data, systems_db,
    )
    system_position_sizer: IPositionSizer = system_props.position_sizer(
        *system_props.position_sizer_args
    )

    for _ in range(system_props.required_runs):
        system_state_handler(
            *system_props.system_state_handler_call_args,
            plot_fig=plot_fig, 
            client_db=client_db, insert_into_db=insert_into_db,
            **system_props.system_state_handler_call_kwargs,
            **system_position_sizer.position_sizer_data_dict
        )
        market_states_data: list[dict] = json.loads(
            systems_db.get_market_state_data(
                system_props.system_name, MarketState.ENTRY.value
            )
        )

        for data_dict in market_states_data:
            position_list, num_of_periods = systems_db.get_single_symbol_position_list(
                system_props.system_name, data_dict[TradingSystemAttributes.SYMBOL],
                return_num_of_periods=True
            )
            system_position_sizer(
                position_list, num_of_periods,
                *system_props.position_sizer_call_args,
                symbol=data_dict[TradingSystemAttributes.SYMBOL], 
                **system_props.position_sizer_call_kwargs,
                **system_position_sizer.position_sizer_data_dict
            )

    pos_sizer_data_dict = system_position_sizer.get_position_sizer_data_dict()
    systems_db.insert_market_state_data(
        system_props.system_name, json.dumps(pos_sizer_data_dict)
    )


if __name__ == '__main__':
    import tet_trading_systems.trading_system_development.trading_systems.env as env

    arg_parser = argparse.ArgumentParser(description='trading_system_handler CLI argument parser')
    arg_parser.add_argument(
        '--trading-systems-dir', dest='ts_dir', help='Trading system files directory'
    )
    arg_parser.add_argument(
        '--full-run', action='store_true', dest='full_run',
        help='Run trading systems from the date of the latest exit of each instrument',
    )

    cli_args = arg_parser.parse_args()

    live_systems_dir = cli_args.ts_dir
    full_run = cli_args.full_run

    file_dir = os.path.dirname(os.path.abspath(__file__))
    __globals = globals()
    sys.path.append(os.path.join(sys.path[0], live_systems_dir))
    trading_system_modules = []
    for file in os.listdir(f'{sys.path[0]}/{live_systems_dir}'):
        if file == '__init__.py' or not file.endswith('.py'):
            continue
        module_name = file[:-3]
        try:
            __globals[module_name] = importlib.import_module(module_name)
            trading_system_modules.append(module_name)
        except ModuleNotFoundError as e:
            print(e, module_name)

    trading_system_properties_list = []
    for ts_module in trading_system_modules:
        if ts_module == 'ml_trading_system_example':
        # if ts_module != 'trading_system_example':
            continue
        ts_properties = __globals[ts_module].get_props(
            INSTRUMENTS_DB, import_instruments=False,
            path=f'{file_dir}/{live_systems_dir}/backtests'
        )
        trading_system_properties_list.append(ts_properties)

    #start_dt = dt.datetime(1999, 1, 1)
    #end_dt = dt.datetime(2011, 1, 1)
    start_dt = dt.datetime(2015, 9, 16)
    end_dt = dt.datetime.now()
    # end_dt = dt.datetime(2023, 3, 2)

    ts_handler = TradingSystemHandler(
        trading_system_properties_list, 
        SYSTEMS_DB, CLIENT_DB,
        start_dt, end_dt
    )
    ts_handler.run_trading_systems(end_dt, full_run)