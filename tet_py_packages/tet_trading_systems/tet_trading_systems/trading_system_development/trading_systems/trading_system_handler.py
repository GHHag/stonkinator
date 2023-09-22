import os
import sys
import importlib
import datetime as dt
from typing import List, Dict
import json

from TETrading.data.metadata.trading_system_attributes import TradingSystemAttributes
from TETrading.data.metadata.market_state_enum import MarketState
from TETrading.trading_system.trading_system import TradingSystem

from tet_trading_systems.trading_system_development.trading_systems.trading_system_properties.trading_system_properties \
    import TradingSystemProperties

from tet_trading_systems.trading_system_management.position_sizer.position_sizer import IPositionSizer

from tet_doc_db.doc_database_meta_classes.tet_signals_doc_db import ITetSignalsDocumentDatabase
from tet_doc_db.doc_database_meta_classes.tet_systems_doc_db import ITetSystemsDocumentDatabase
from tet_doc_db.doc_database_meta_classes.tet_portfolio_doc_db import ITetPortfolioDocumentDatabase
from tet_doc_db.doc_database_meta_classes.time_series_doc_db import ITimeSeriesDocumentDatabase
from tet_doc_db.tet_mongo_db.systems_mongo_db import TetSystemsMongoDb
from tet_doc_db.tet_mongo_db.portfolio_mongo_db import TetPortfolioMongoDb
from tet_doc_db.time_series_mongo_db.time_series_mongo_db import TimeSeriesMongoDb
from tet_doc_db.instruments_mongo_db.instruments_mongo_db import InstrumentsMongoDb


def run_trading_system(
    data_dict, system_name, entry_func, exit_func,
    entry_args, exit_args, *args, 
    capital=10000, capital_fraction=1.0, avg_yearly_periods=251,  
    market_state_null_default=False, run_monte_carlo_sims=False, num_of_sims=2500,
    print_dataframe=False, plot_fig=False, plot_positions=False, write_to_file_path=None,
    save_summary_plot_to_path=False, system_analysis_to_csv_path=None,
    plot_returns_distribution=False,
    systems_db: ITetSystemsDocumentDatabase=None, 
    client_db: ITetSystemsDocumentDatabase=None, 
    print_data=False,
    run_from_latest_exit=False,
    insert_into_db=False,
    pos_list_slice_years_est=2,
    save_best_estimate_trades_path=None,
    **kwargs
):
    ts = TradingSystem(system_name, data_dict, entry_func, exit_func, systems_db, client_db)
    ts(
        capital=capital,
        capital_fraction=capital_fraction,
        avg_yearly_periods=avg_yearly_periods,
        market_state_null_default=market_state_null_default,
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
        entry_args=entry_args,
        exit_args=exit_args,
        fixed_position_size=True,
        generate_signals=True,
        plot_positions=plot_positions,
        save_position_figs_path=None,
        write_signals_to_file_path=write_to_file_path,
        run_from_latest_exit=run_from_latest_exit,
        insert_data_to_db_bool=insert_into_db,
        pos_list_slice_years_est=pos_list_slice_years_est
    )


def handle_trading_system(
    system_props: TradingSystemProperties, start_dt, end_dt, 
    from_latest_exit: bool,
    systems_db: ITetSystemsDocumentDatabase, 
    client_db: ITetSignalsDocumentDatabase, 
    time_series_db: ITimeSeriesDocumentDatabase=None,
    insert_into_db=False, plot_fig=False 
):
    if from_latest_exit:
        # make sure exit dates are available to get for the given trading system
        latest_position_dts = json.loads(
            client_db.get_latest_position_dts(
                system_props.system_name, system_props.system_instruments_list
            )
        )

    data, pred_features_data = system_props.preprocess_data_function(
        system_props.system_instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt, 
        latest_position_dts=latest_position_dts if from_latest_exit else from_latest_exit
    )

    #if time_series_db:
    #    time_series_db.insert_pandas_time_series_data(data)

    system_state_handler = system_props.system_state_handler(
        *system_props.system_state_handler_args, systems_db, data
    )
    system_position_sizer: IPositionSizer = system_props.position_sizer(
        *system_props.position_sizer_args
    )

    for i in range(system_props.required_runs):
        system_state_handler(
            *system_props.system_state_handler_call_args, pred_features_data,
            plot_fig=plot_fig, client_db=client_db, 
            insert_into_db=True if (i + 1) == system_props.required_runs and insert_into_db else False,
            **system_props.system_state_handler_call_kwargs,
            run_from_latest_exit=from_latest_exit,
            **system_position_sizer.position_sizer_data_dict
        )
        market_states_data: List[Dict] = json.loads(
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


def handle_ext_pos_sizer_trading_system(
    system_props: TradingSystemProperties, start_dt, end_dt, 
    from_latest_exit: bool,
    systems_db: ITetSystemsDocumentDatabase, 
    client_db: ITetSignalsDocumentDatabase, 
    time_series_db: ITimeSeriesDocumentDatabase=None,
    insert_into_db=False, plot_fig=False 
):
    if from_latest_exit:
        latest_position_dts = json.loads(
            client_db.get_latest_position_dts(
                system_props.system_name, system_props.system_instruments_list
            )
        )

    data, pred_features_data = system_props.preprocess_data_function(
        system_props.system_instruments_list,
        *system_props.preprocess_data_args, start_dt, end_dt,
        latest_position_dts=latest_position_dts if from_latest_exit else from_latest_exit
    )

    #if time_series_db:
    #    time_series_db.insert_pandas_time_series_data(data)

    system_state_handler = system_props.system_state_handler(
        *system_props.system_state_handler_args, systems_db, data
    )
    system_position_sizer: IPositionSizer = system_props.position_sizer(
        *system_props.position_sizer_args
    )

    for i in range(system_props.required_runs):
        system_state_handler(
            *system_props.system_state_handler_call_args, pred_features_data,
            plot_fig=plot_fig, client_db=client_db,
            insert_into_db=True if (i + 1) == system_props.required_runs and insert_into_db else False,
            **system_props.system_state_handler_call_kwargs,
            run_from_latest_exit=from_latest_exit,
            **system_position_sizer.position_sizer_data_dict
        )
        position_list, num_of_periods = systems_db.get_position_list(
            system_props.system_name, return_num_of_periods=True
        )
        system_position_sizer(
            position_list, num_of_periods,
            *system_props.position_sizer_call_args,
            **system_props.position_sizer_call_kwargs,
            **system_position_sizer.position_sizer_data_dict
        )

    pos_sizer_data_dict = system_position_sizer.get_position_sizer_data_dict()
    systems_db.insert_system_metrics(system_props.system_name, pos_sizer_data_dict)
 

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
        market_states_data: List[Dict] = json.loads(
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


def handle_trading_system_portfolio(
    system_props: TradingSystemProperties,
    client_db: ITetSignalsDocumentDatabase,
    portfolio_db: ITetPortfolioDocumentDatabase,
    insert_into_db=False
):
    portfolio = system_props.portfolio(
        *system_props.portfolio_args, 
        client_db, portfolio_db
    )
    portfolio(
        *system_props.portfolio_call_args, 
        insert_into_db=insert_into_db
    )


if __name__ == '__main__':
    import tet_trading_systems.trading_system_development.trading_systems.env as env

    from_latest_exit = '-from-latest-exit' in sys.argv

    LIVE_SYSTEMS_DIR = sys.argv[1]
    file_dir = os.path.dirname(os.path.abspath(__file__))
    __globals = globals()
    sys.path.append(os.path.join(sys.path[0], LIVE_SYSTEMS_DIR))
    trading_system_modules = []
    for file in os.listdir(f'{sys.path[0]}/{LIVE_SYSTEMS_DIR}'):
        if file == '__init__.py' or not file.endswith('.py'):
            continue
        module_name = file[:-3]
        try:
            __globals[module_name] = importlib.import_module(module_name)
            trading_system_modules.append(module_name)
        except ModuleNotFoundError:
            pass

    #INSTRUMENTS_DB = InstrumentsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.INSTRUMENTS_DB)
    TIME_SERIES_DB = TimeSeriesMongoDb(env.LOCALHOST_MONGO_DB_URL, env.TIME_SERIES_DB)
    SYSTEMS_DB = TetSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.SYSTEMS_DB)
    CLIENT_DB = TetSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.CLIENT_DB)

    INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
    #TIME_SERIES_DB = TimeSeriesMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
    #SYSTEMS_DB = TetSystemsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
    #CLIENT_DB = TetSystemsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
    
    PORTFOLIOS_DB = TetPortfolioMongoDb(env.LOCALHOST_MONGO_DB_URL, env.CLIENT_DB)

    #start_dt = dt.datetime(1999, 1, 1)
    #end_dt = dt.datetime(2011, 1, 1)
    start_dt = dt.datetime(2015, 9, 16)
    #end_dt = dt.datetime.now()
    #end_dt = dt.datetime(2023, 2, 24)
    end_dt = dt.datetime(2023, 3, 1)

    systems_props_list: List[TradingSystemProperties] = []

    for trading_system in trading_system_modules:
        #if trading_system == 'ml_trading_system_example':
        if trading_system != 'mean_reversion_stocks':
        #if trading_system != 'trading_system_example':
            continue
        systems_props_list.append(
            __globals[trading_system].get_props(
                INSTRUMENTS_DB, import_instruments=True, 
                path=f'{file_dir}/{LIVE_SYSTEMS_DIR}/backtests'
            )
        )

    for system_props in systems_props_list:
        # TODO: implement protocol for system handler functions
        system_props.system_handler_function(
            system_props, start_dt, end_dt, 
            from_latest_exit,
            SYSTEMS_DB, CLIENT_DB, 
            time_series_db=TIME_SERIES_DB, 
            insert_into_db=True, plot_fig=False
        )
        #if system_props.portfolio_args:
        #    handle_trading_system_portfolio(
        #        system_props, 
        #        CLIENT_DB, PORTFOLIOS_DB, 
        #        insert_into_db=True
        #    )