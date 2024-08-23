import os
from inspect import isfunction

import pandas as pd
import numpy as np

from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.trading_system_metrics import TradingSystemMetrics
from trading.position.order import Order
from trading.position.position import Position
from trading.position.position_manager import PositionManager
from trading.trading_system.trading_session import TradingSession, BacktestTradingSession
from trading.signal_events.signal_handler import SignalHandler
from trading.utils.monte_carlo_functions import monte_carlo_simulate_returns, \
    monte_carlo_simulation_summary_data
from trading.metrics.metrics_summary_plot import returns_distribution_plot

from persistance.doc_database_meta_classes.tet_systems_doc_db import ITetSystemsDocumentDatabase


class TradingSystem:
    """
    Data together with logic forms the trading system. Objects of this class
    use fields with data and logic to generate historic positions and new signals.

    Parameters
    ----------
    system_name : 'str'
        The name of the system. Will be used to identify it.
    entry_logic_function : 'function'
        The logic used for entering a position.
    exit_logic_function : 'function'
        The logic used to exit a position.
    systems_db : 'ITetSystemsDocumentDatabase'
        Instance of a class that implements the ITetSystemsDocumentDatabase
        meta class. Handles database connection and communication.
    client_db : 'ITetSystemsDocumentDatabase'
        Instance of a class that implements the ITetSystemsDocumentDatabase
        meta class. Handles database connection and communication.
    """

    def __init__(
        self, system_name, 
        entry_logic_function: callable, exit_logic_function: callable,
        systems_db: ITetSystemsDocumentDatabase, client_db: ITetSystemsDocumentDatabase
    ):
        self.__system_name = system_name
        assert isfunction(entry_logic_function), \
            "Parameter 'entry_logic_function' must be a function."
        self.__entry_logic_function = entry_logic_function
        assert isfunction(exit_logic_function), \
            "Parameter 'exit_logic_function' must be a function."
        self.__exit_logic_function = exit_logic_function

        self.__systems_db: ITetSystemsDocumentDatabase = systems_db
        self.__client_db: ITetSystemsDocumentDatabase = client_db
        self.__metrics_df: pd.DataFrame = pd.DataFrame()
        self.__monte_carlo_simulations_df: pd.DataFrame = pd.DataFrame()

    def _print_metrics_df(self):
        print('\nSystem performance summary: \n', self.__metrics_df.to_string())

    def _print_monte_carlo_sims_df(self, num_of_monte_carlo_sims):
        print(
            '\nMonte carlo simulation stats (' + str(num_of_monte_carlo_sims) + ' simulations):\n',
            self.__monte_carlo_simulations_df.to_string()
        )

    def run_trading_system_backtest(
        self, data_dict: dict[str, pd.DataFrame], *args, 
        capital=10000, capital_fraction=None, avg_yearly_periods=251,
        system_evaluation_fields=TradingSystemMetrics.system_evaluation_fields,
        market_state_null_default=False,
        plot_performance_summary=False, save_summary_plot_to_path: str=None, 
        system_analysis_to_csv_path: str=None,
        plot_returns_distribution=False, save_returns_distribution_plot_to_path: str=None,
        run_monte_carlo_sims=False, num_of_monte_carlo_sims=2500, monte_carlo_data_amount=0.4,
        plot_monte_carlo=False, print_monte_carlo_df=False, 
        monte_carlo_analysis_to_csv_path: str=None, write_signals_to_file_path: str=None, 
        print_data=False,
        insert_data_to_db_bool=False,
        pos_list_slice_years_est=2, **kwargs
    ):
        """
        Iterates over data, creates a PositionManager instance and generates
        positions.

        Parameters
        ----------
        :param data_dict:
            'dict' : A dict with key: symbol, value: Pandas DataFrame with data
            for the assets used in the system.
        :param args:
            'tuple' : Args to pass along to PositionManager.generate_positions().
        :param capital:
            Keyword arg 'int/float' : The amount of capital to purchase assets with.
            Default value=10000
        :param capital_fraction:
            Keyword arg 'None/dict/float' : The fraction of the capital that will 
            be used to purchase assets with. Alternatively a dict where the keys 
            are the symbols of the assets in the trading systems and their values 
            are the fraction of the capital. Default value=None
        :param avg_yearly_periods:
            Keyword arg 'int' : The average number of trading periods in a year for 
            the asset class of the system. Default value=251
        :param system_evaluation_fields:
            Keyword arg 'tuple' : A tuple containing strings of metrics used to 
            evaluate a trading system. 
            Default value=TradingSystemMetrics.system_evaluation_fields property
        :param market_state_null_default:
            Keyword arg 'bool' : True/False decides whether the market_state property 
            should be assigned a null value by default or not. Default value=False
        :param plot_performance_summary:
            Keyword arg 'bool' : True/False decides whether to plot summary
            statistics or not. Default value=False
        :param save_summary_plot_to_path:
            Keyword arg 'None/str' : Provide a file path as a str to save the
            summary plot as a file. Default value=None
        :param system_analysis_to_csv_path:
            Keyword arg 'None/str' : Provide a file path as a str to save the
            system analysis Pandas DataFrame as a .csv file. Default value=None
        :param plot_returns_distribution:
            Keyword arg 'bool' : True/False decides whether to plot charts with
            returns, MAE and MFE distributions for the system. Default value=False
        :param save_returns_distribution_plot_to_path:
            Keyword arg 'None/str' : Provide a file path as a str to save the
            returns distribution plot as a file. Default value=None
        :param run_monte_carlo_sims:
            Keyword arg 'bool' : True/False decides whether to run Monte Carlo
            simulations on each assets return sequence. Default value=False
        :param num_of_monte_carlo_sims:
            Keyword arg 'int' : The number of Monte Carlo simulations to run.
            Default value=2500
        :param monte_carlo_data_amount:
            Keyword arg 'float' : The fraction of data to be used in the Monte Carlo
            simulations. Default value=0.4
        :param plot_monte_carlo:
            Keyword arg 'bool' : True/False decides whether to plot the results
            of the Monte Carlo simulations. Default value=False
        :param print_monte_carlo_df:
            Keyword arg 'bool' : True/False decides whether or not to print a Pandas
            DataFrame with stats generated from Monte Carlo simulations to the console.
            Default value=False
        :param monte_carlo_analysis_to_csv_path:
            Keyword arg 'None/str' : Provide a file path as a str to save the
            Monte Carlo simulations Pandas DataFrame as a CSV file. Default value=None
        :param write_signals_to_file_path:
            Keyword arg 'None/str' : Provide a file path as a str to save any signals
            generated by the system. Default value=None
        :param print_data:
            Keyword arg 'bool' : True/False decides if data for positions, trading system
            and trading signals should be printed out to the console or not. 
            Default value=False
        :param insert_data_to_db_bool:
            Keyword arg 'bool' : True/False decides whether or not data should be 
            inserted into database or not. Default value=False
        :param pos_list_slice_years_est:
            Keyword arg 'int' : The number of years to estimate the amount of positions
            to slice the list of positions by. Default value=2
        :param kwargs:
            'dict' : Dictionary with keyword arguments to pass along to
            PositionManager.generate_positions().
        """

        signal_handler = SignalHandler()
        full_pos_list: list[Position] = []
        pos_lists: list[list[Position]] = []
        full_market_to_market_returns_list = np.array([])
        full_mae_list = np.array([])
        full_mfe_list = np.array([])

        for instrument, data in data_dict.items():
            try:
                if 'close' in data:
                    asset_price_series = [float(close) for close in data['close']]
                elif f'close_{instrument}' in data:
                    asset_price_series = [float(close) for close in data[f'close_{instrument}']]
                else:
                    raise Exception(f'Column missing in DataFrame, instrument: {instrument}')
            except TypeError:
                print('TypeError', instrument)
                continue

            if not pd.api.types.is_datetime64_any_dtype(data.index):
                raise ValueError(
                    'Expected index of Pandas DataFrame to have a datetime-like dtype.'
                )

            # if capital_fraction is a dict containing a key with the current value of
            # 'instrument', its value will be assigned to 'capital_f'
            if isinstance(capital_fraction, dict) and instrument in capital_fraction:
                capital_f = capital_fraction[instrument]
            # if capital_fraction is a float its value will be assigned to 'capital_f'
            elif isinstance(capital_fraction, float):
                capital_f = capital_fraction
            else:
                capital_f = 1.0

            pos_manager = PositionManager(
                instrument, len(data), capital, capital_f, 
                asset_price_series=asset_price_series
            )
            trading_session = BacktestTradingSession(
                self.__entry_logic_function, self.__exit_logic_function, data,
                signal_handler, symbol=instrument
            )
            pos_manager.generate_positions(
                trading_session, *args,
                market_state_null_default=market_state_null_default,
                print_data=print_data, **kwargs
            )

            # summary output of the trading system
            if not len(pos_manager.position_list) > 0:
                print(f'\nNo positions generated for {pos_manager.symbol}')
                continue
            else:
                try:
                    if save_summary_plot_to_path:
                        if not os.path.exists(save_summary_plot_to_path):
                            os.makedirs(save_summary_plot_to_path)
                    pos_manager.summarize_performance(
                        print_data=print_data,
                        plot_fig=plot_performance_summary, 
                        save_fig_to_path=save_summary_plot_to_path
                    )
                except ValueError:
                    print('ValueError')

            # add system evaluation data to the SignalHandler
            if signal_handler.entry_signal_given is True or market_state_null_default:
                signal_handler.add_system_evaluation_data(
                    pos_manager.metrics.summary_data_dict, system_evaluation_fields
                )

            if len(pos_manager.position_list) > 0:
                # write trading system data and stats to DataFrame
                df_to_concat = pd.DataFrame([pos_manager.metrics.summary_data_dict])
                if self.__metrics_df.empty:
                    self.__metrics_df = df_to_concat
                else:
                    self.__metrics_df = pd.concat([self.__metrics_df, df_to_concat], ignore_index=True)

                # run Monte Carlo simulations, plot and write stats to DataFrame
                if run_monte_carlo_sims:
                    print('\nRunning Monte Carlo simulations...')
                    monte_carlo_sims_data_dicts_list = monte_carlo_simulate_returns(
                        pos_manager.position_list, pos_manager.symbol, 
                        pos_manager.metrics.num_testing_periods,
                        start_capital=capital, capital_fraction=capital_f,
                        num_of_sims=num_of_monte_carlo_sims, data_amount_used=monte_carlo_data_amount,
                        print_dataframe=print_monte_carlo_df,
                        plot_fig=plot_monte_carlo, save_fig_to_path=save_summary_plot_to_path
                    )
                    if monte_carlo_sims_data_dicts_list:
                        monte_carlo_summary_data_dict = monte_carlo_simulation_summary_data(
                            monte_carlo_sims_data_dicts_list
                        )
                        df_to_concat = pd.DataFrame([monte_carlo_summary_data_dict])
                        if self.__monte_carlo_simulations_df.empty:
                            self.__monte_carlo_simulations_df = df_to_concat
                        else:
                            self.__monte_carlo_simulations_df = pd.concat(
                                [self.__monte_carlo_simulations_df, df_to_concat],
                                ignore_index=True
                            )

                if insert_data_to_db_bool:
                    order, symbol = signal_handler.current_order
                    if order and symbol == instrument:
                        self.__systems_db.insert_current_order(
                            self.__system_name, instrument, order
                        )
                    num_periods = len(data.loc[data.index <= pos_manager.position_list[-1].exit_dt])
                    self.__systems_db.insert_single_symbol_position_list(
                        self.__system_name, instrument, 
                        pos_manager.position_list[:], num_periods,
                        serialized_format=True
                    )
                    self.__client_db.insert_single_symbol_position_list(
                        self.__system_name, instrument,
                        pos_manager.position_list[:], num_periods,
                        json_format=True
                    )

                full_pos_list += pos_manager.position_list[:]
                pos_lists.append(pos_manager.position_list[:])

                if len(pos_manager.metrics.market_to_market_returns_list) > 0:
                    full_market_to_market_returns_list = np.concatenate(
                        (
                            full_market_to_market_returns_list,
                            pos_manager.metrics.market_to_market_returns_list
                        ), axis=0
                    )
                    full_mae_list = np.concatenate(
                        (full_mae_list, pos_manager.metrics.w_mae_list), axis=0
                    )
                    full_mfe_list = np.concatenate(
                        (full_mfe_list, pos_manager.metrics.mfe_list), axis=0                    
                    )

        if print_data: self._print_metrics_df()

        if run_monte_carlo_sims:
            self._print_monte_carlo_sims_df(num_of_monte_carlo_sims)
            if monte_carlo_analysis_to_csv_path and monte_carlo_analysis_to_csv_path.endswith('.csv'):
                self.__monte_carlo_simulations_df.to_csv(monte_carlo_analysis_to_csv_path)

        if system_analysis_to_csv_path and system_analysis_to_csv_path.endswith('.csv'):
            self.__metrics_df.to_csv(system_analysis_to_csv_path)

        if print_data: print(signal_handler)

        if write_signals_to_file_path:
            signal_handler.write_to_csv(write_signals_to_file_path, self.__system_name)

        if insert_data_to_db_bool:
            signal_handler.insert_into_db(self.__client_db, self.__system_name)

        if len(data_dict) > 1:
            num_of_pos_insert_multiplier = pos_list_slice_years_est * 1.5
            sorted_pos_lists = sorted(pos_lists, key=len, reverse=True)
            position_list_lengths = (
                [len(i) for i in sorted_pos_lists[:int(len(pos_lists) / 4 + 0.5)]]
                if len(pos_lists) > 1
                else [len(sorted_pos_lists[0])]
            )
            data_periods = [len(v) for k, v in data_dict.items()][:int(len(data_dict) / 4 + 0.5)]
            avg_yearly_positions = (
                # error prone if data used to calculate is NaN TODO: handle exception
                int(np.mean(position_list_lengths) / (np.mean(data_periods) / avg_yearly_periods) + 0.5)
                * num_of_pos_insert_multiplier
            )
            full_pos_list_slice_param = int(avg_yearly_positions * (pos_list_slice_years_est * 1.5) + 0.5)
            sorted_full_pos_list: list[Position] = sorted(full_pos_list, key=lambda x: x.entry_dt)
            sliced_pos_list: list[Position] = sorted_full_pos_list[-full_pos_list_slice_param:]
            num_of_periods = avg_yearly_periods * pos_list_slice_years_est * num_of_pos_insert_multiplier

        if insert_data_to_db_bool:
            self.__systems_db.insert_position_list(
                self.__system_name, sliced_pos_list, num_of_periods,
                serialized_format=True
            )
            self.__client_db.insert_position_list(
                self.__system_name, sliced_pos_list, num_of_periods, 
                json_format=True
            )

        returns_distribution_plot(
            full_market_to_market_returns_list, full_mae_list, full_mfe_list,
            plot_fig=plot_returns_distribution, save_fig_to_path=save_returns_distribution_plot_to_path
        )


    def run_trading_system(
        self, data_dict: dict[str, pd.DataFrame], *args,
        write_signals_to_file_path: str=None, print_data=False,
        insert_data_to_db_bool=False, **kwargs
    ):
        """
        Iterates over data, creates a TradingSession instance and generates
        positions.

        Parameters
        ----------
        :param data_dict:
            'dict' : A dict with key: symbol, value: Pandas DataFrame with data
            for the assets used in the system.
        :param args:
            'tuple' : A tuple of positional arguments not specified in the method
            signature.
        :param write_signals_to_file_path:
            Keyword arg 'None/str' : Provide a file path as a str to save any signals
            generated by the system. Default value=None
        :param print_data:
            Keyword arg 'bool' : True/False decides if data for positions, trading system
            and trading signals should be printed out to the console or not. 
            Default value=False
        :param insert_data_to_db_bool:
            Keyword arg 'bool' : True/False decides whether or not data should be 
            inserted into database or not. Default value=False
        :param kwargs:
            'dict' : Dictionary with keyword arguments not specified in the method
            signature.
        """

        signal_handler = SignalHandler()

        for instrument, data in data_dict.items():
            if not 'close' in data and not f'close_{instrument}' in data:
                raise ValueError(f'Column missing in DataFrame, instrument: {instrument}')

            if not pd.api.types.is_datetime64_any_dtype(data.index):
                raise ValueError(
                    'Expected index of Pandas DataFrame to have a datetime dtype.'
                )

            order: Order = self.__systems_db.get_current_order(
                self.__system_name, instrument
            )
            if order and order.created_dt >= data.index[-1]:
                continue

            position: Position = self.__systems_db.get_current_position(
                self.__system_name, instrument
            )
            if position and position.current_dt >= data.index[-1]:
                continue

            trading_session = TradingSession(
                self.__entry_logic_function, self.__exit_logic_function,
                signal_handler, symbol=instrument
            )
            order, position = trading_session(
                data, order, position, *args,
                print_data=print_data, **kwargs
            )

            if (
                order and order.active == True and
                order.action == MarketState.ENTRY and
                order.created_dt == data.index[-1]
            ):
                latest_position: Position = self.__systems_db.get_single_symbol_latest_position(
                    self.__system_name, instrument
                )
                num_of_periods = (
                    len(data.loc[data.index > latest_position.exit_dt]) 
                    if latest_position != None else len(data)
                )
                self.__systems_db.increment_num_of_periods(
                    self.__system_name, instrument, num_of_periods
                )
                self.__client_db.increment_num_of_periods(
                    self.__system_name, instrument, num_of_periods
                )

            if insert_data_to_db_bool == True:
                self.__systems_db.insert_current_order(
                    self.__system_name, instrument, order
                )

                self.__systems_db.insert_current_position(
                    self.__system_name, instrument, position
                )
                # also insert position to client db?
                # self.__client_db.insert_current_position(
                #     self.__system_name, instrument, position
                # )

            if (
                position and position.exit_dt == data.index[-1] and 
                insert_data_to_db_bool == True
            ):
                self.__systems_db.insert_single_symbol_position(
                    self.__system_name, instrument, position, len(position.returns_list),
                    serialized_format=True
                )
                self.__client_db.insert_single_symbol_position(
                    self.__system_name, instrument, position, len(position.returns_list),
                    json_format=True
                )
                self.__systems_db.insert_position(
                    self.__system_name, position,
                    serialized_format=True
                )
                self.__client_db.insert_position(
                    self.__system_name, position,
                    json_format=True
                )

        if print_data: print(signal_handler)

        if write_signals_to_file_path:
            signal_handler.write_to_csv(write_signals_to_file_path, self.__system_name)

        if insert_data_to_db_bool:
            signal_handler.insert_into_db(self.__client_db, self.__system_name)