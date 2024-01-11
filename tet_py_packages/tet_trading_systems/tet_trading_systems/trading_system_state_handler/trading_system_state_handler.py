import datetime as dt
import json

import pandas as pd

from TETrading.data.metadata.trading_system_attributes import TradingSystemAttributes
from TETrading.data.metadata.market_state_enum import MarketState
from TETrading.trading_system.trading_system import TradingSystem

from tet_trading_systems.trading_system_management.position_sizer.position_sizer import IPositionSizer
from tet_trading_systems.trading_system_development.trading_systems.trading_system_properties.trading_system_properties \
    import TradingSystemProperties

from tet_doc_db.doc_database_meta_classes.tet_systems_doc_db import ITetSystemsDocumentDatabase


class TradingSystemStateHandler:
    
    __data: dict[str, pd.DataFrame] = {}
    # __data_handler = nets.DataHandler()
    
    def __init__(
        self, system_name: str, ts_properties: TradingSystemProperties, 
        systems_db: ITetSystemsDocumentDatabase, client_db: ITetSystemsDocumentDatabase,
        start_dt: dt.datetime, end_dt: dt.datetime
    ):
        self.__system_name: str = system_name
        self.__ts_properties = ts_properties
        self.__trading_system = TradingSystem(
            self.__system_name,
            self.__ts_properties.entry_logic_function, 
            self.__ts_properties.exit_logic_function,
            systems_db, client_db
        )
        self.__systems_db: ITetSystemsDocumentDatabase = systems_db
        self.__client_db: ITetSystemsDocumentDatabase = client_db
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

    # new_data, should it be pd.DataFrame or json?
    def add_data(self, new_data: pd.DataFrame):
        # self.__data = nets.DataHandler(new_data)
        pass

    def _run_trading_system(self, *args, **kwargs):
        print(args)
        print(*args)

        # make some date check on given date against last date of self.__data

        self.__trading_system(self.__data, *args, **kwargs)

    def _handle_trading_system(
        self, *args, from_latest_exit: bool=False,
        insert_into_db=False, plot_fig=False, **kwargs
    ):
        # system_state_handler prop has been replaces by this class
        # system_state_handler = self.__ts_properties.system_state_handler(
        #     *self.__ts_properties.system_state_handler_args, self.__systems_db, data
        # )
        system_position_sizer: IPositionSizer = self.__ts_properties.position_sizer(
            *self.__ts_properties.position_sizer_args
        )

        for i in range(self.__ts_properties.required_runs):
            self._run_trading_system(
                *self.__ts_properties.system_state_handler_call_args, pred_features_data,
                plot_fig=plot_fig, client_db=self.__client_db, 
                insert_into_db=True if (i + 1) == self.__ts_properties.required_runs and insert_into_db else False,
                **self.__ts_properties.system_state_handler_call_kwargs,
                run_from_latest_exit=from_latest_exit,
                **system_position_sizer.position_sizer_data_dict
            )
            market_states_data: list[dict] = json.loads(
                self.__systems_db.get_market_state_data(
                    self.__ts_properties.system_name, MarketState.ENTRY.value
                )
            )

            for data_dict in market_states_data:
                position_list, num_of_periods = self.__systems_db.get_single_symbol_position_list(
                    self.__ts_properties.system_name, data_dict[TradingSystemAttributes.SYMBOL],
                    return_num_of_periods=True
                )
                system_position_sizer(
                    position_list, num_of_periods,
                    *self.__ts_properties.position_sizer_call_args,
                    symbol=data_dict[TradingSystemAttributes.SYMBOL], 
                    **self.__ts_properties.position_sizer_call_kwargs,
                    **system_position_sizer.position_sizer_data_dict
                )

        pos_sizer_data_dict = system_position_sizer.get_position_sizer_data_dict()
        self.__systems_db.insert_market_state_data(
            self.__ts_properties.system_name, json.dumps(pos_sizer_data_dict)
        )

    # def __call__(self, date: dt.datetime, *args, **kwargs):
    def __call__(
        self, 
        *args, 
        capital=10000, capital_fraction=1.0, avg_yearly_periods=251,  
        market_state_null_default=False, run_monte_carlo_sims=False, num_of_sims=2500,
        print_dataframe=False, plot_fig=False, plot_positions=False, write_to_file_path=None,
        save_summary_plot_to_path=False, system_analysis_to_csv_path=None,
        plot_returns_distribution=False,
        print_data=True,
        run_from_latest_exit=False,
        insert_into_db=False,
        pos_list_slice_years_est=2,
        **kwargs
    ):
        # make some date check on given date against last date of self.__data
        # self._run_trading_system(*args, **kwargs)

        print(args)
        print(*args)
        self._handle_trading_system(
            *args,
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
            entry_args=self.__ts_properties.entry_function_args,
            exit_args=self.__ts_properties.exit_function_args,
            fixed_position_size=True,
            generate_signals=True,
            plot_positions=plot_positions,
            save_position_figs_path=None,
            write_signals_to_file_path=write_to_file_path,
            run_from_latest_exit=run_from_latest_exit,
            insert_data_to_db_bool=insert_into_db,
            pos_list_slice_years_est=pos_list_slice_years_est,
            **kwargs
        )