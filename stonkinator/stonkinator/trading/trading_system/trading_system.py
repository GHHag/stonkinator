import os
from inspect import isfunction

import pandas as pd
import numpy as np

from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.trading_system_attributes import TradingSystemAttributes
from trading.data.metadata.trading_system_metrics import TradingSystemMetrics
from trading.data.metadata.price import Price
from trading.position.order import Order
from trading.position.position import Position
from trading.position.position_manager import PositionManager
from trading.trading_system.trading_session import TradingSession, BacktestTradingSession
from trading.signal_events.signal_handler import SignalHandler
from trading.utils.monte_carlo_functions import monte_carlo_simulate_returns, \
    monte_carlo_simulation_summary_data
from trading.metrics.metrics_summary_plot import returns_distribution_plot, \
    composite_system_metrics_summary_plot

from persistance.persistance_meta_classes.trading_systems_persister import TradingSystemsPersisterBase


class TradingSystem:
    """
    Data together with logic forms the trading system. Objects of this class
    use fields with data and logic to generate historic positions and new signals.

    Parameters
    ----------
    trading_system_id : 'str'
        Identifier of a trading system.
    system_name : 'str'
        The name of the system.
    entry_logic_function : 'function'
        The logic used for entering a position.
    exit_logic_function : 'function'
        The logic used to exit a position.
    trading_systems_persister : 'TradingSystemsPersisterBase'
        Instance of a class that implements the TradingSystemsPersisterBase
        meta class. Client for service that handle data persistance.
    """

    def __init__(
        self, trading_system_id, system_name,
        entry_logic_function: callable, exit_logic_function: callable,
        trading_systems_persister: TradingSystemsPersisterBase
    ):
        self.__system_id = trading_system_id
        self.__system_name = system_name
        assert isfunction(entry_logic_function), "Parameter 'entry_logic_function' must be a function."
        self.__entry_logic_function = entry_logic_function
        assert isfunction(exit_logic_function), "Parameter 'exit_logic_function' must be a function."
        self.__exit_logic_function = exit_logic_function
        self.__trading_systems_persister = trading_systems_persister

    def run_trading_system_backtest(
        self, data_dict: dict[tuple[str, str], pd.DataFrame], *args, 
        capital=10000, capital_fraction=None, avg_yearly_periods=251,
        system_evaluation_fields=TradingSystemMetrics.system_evaluation_fields,
        market_state_null_default=False,
        plot_performance_summary=False, save_summary_plot_to_path: str=None, 
        system_analysis_to_csv_path: str=None,
        composite_summary_plot_to_path: str=None,
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
            'dict' : A dict with key: (instrument_id, symbol), value: Pandas DataFrame 
            with data for the assets used in the system.
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
        :param composite_summary_plot_to_path:
            Keyword arg 'None/str' : Provide a file path as a str to save the
            composite trading system summary plot as a file. Default value=None
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
        metrics_df: pd.DataFrame = pd.DataFrame()
        monte_carlo_simulations_df: pd.DataFrame = pd.DataFrame()

        for (instrument_id, symbol), data in data_dict.items():
            try:
                if Price.CLOSE in data:
                    asset_price_series = [float(close) for close in data[Price.CLOSE]]
                elif f'{Price.CLOSE}_{symbol}' in data:
                    asset_price_series = [float(close) for close in data[f'{Price.CLOSE}_{symbol}']]
                else:
                    raise Exception(f'column "{Price.CLOSE}" missing in DataFrame, symbol: {symbol}')
            except TypeError:
                print('TypeError', symbol)
                continue

            if not pd.api.types.is_datetime64_any_dtype(data.index):
                raise ValueError('expected index of Pandas DataFrame to have a datetime-like dtype')

            # if capital_fraction is a dict containing a key with the current value of
            # 'instrument_id', its value will be assigned to 'capital_f'
            if isinstance(capital_fraction, dict) and instrument_id in capital_fraction:
                capital_f = capital_fraction[instrument_id]
            # if capital_fraction is a float its value will be assigned to 'capital_f'
            elif isinstance(capital_fraction, float):
                capital_f = capital_fraction
            else:
                capital_f = 1.0

            pos_manager = PositionManager(
                symbol, len(data), capital, capital_f,
                asset_price_series=asset_price_series
            )
            trading_session = BacktestTradingSession(
                self.__entry_logic_function, self.__exit_logic_function, data,
                signal_handler, instrument_id, symbol=symbol
            )
            pos_manager.generate_positions(
                trading_session, *args,
                market_state_null_default=market_state_null_default,
                print_data=print_data, **kwargs
            )

            # summary output of the trading system
            if not len(pos_manager.position_list) > 0:
                print(f'\nNo positions generated for {pos_manager.identifier}')
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
            if signal_handler.entry_signal_given == True or market_state_null_default:
                summary_data_dict = pos_manager.metrics.summary_data_dict
                num_of_periods = len(data.loc[data.index <= pos_manager.position_list[-1].exit_dt])
                summary_data_dict[TradingSystemAttributes.NUMBER_OF_PERIODS] = num_of_periods
                signal_handler.add_system_evaluation_data(
                    summary_data_dict, (*system_evaluation_fields, TradingSystemAttributes.NUMBER_OF_PERIODS)
                )

            if len(pos_manager.position_list) > 0:
                # write trading system data and stats to DataFrame
                df_to_concat = pd.DataFrame([pos_manager.metrics.summary_data_dict])
                if metrics_df.empty:
                    metrics_df = df_to_concat
                else:
                    metrics_df = pd.concat([metrics_df, df_to_concat], ignore_index=True)

                # run Monte Carlo simulations, plot and write stats to DataFrame
                if run_monte_carlo_sims:
                    print('\nRunning Monte Carlo simulations...')
                    monte_carlo_sims_data_dicts_list = monte_carlo_simulate_returns(
                        pos_manager.position_list, pos_manager.identifier, 
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
                        if monte_carlo_simulations_df.empty:
                            monte_carlo_simulations_df = df_to_concat
                        else:
                            monte_carlo_simulations_df = pd.concat(
                                [monte_carlo_simulations_df, df_to_concat], ignore_index=True
                            )

                if insert_data_to_db_bool:
                    order, order_instrument_id = signal_handler.current_order
                    if order and order_instrument_id == instrument_id:
                        order_direction = (
                            order.direction == TradingSystemAttributes.LONG
                            if order.action == MarketState.ENTRY
                            else None
                        )
                        self.__trading_systems_persister.upsert_order(
                            instrument_id, self.__system_id, order.order_type, order.action.value,
                            order.created_dt, order.active, order_direction,
                            **order.order_properties
                        )
                    position, position_instrument_id = signal_handler.current_position
                    if position and position.active == True and position_instrument_id == instrument_id:
                        self.__trading_systems_persister.upsert_position(
                            instrument_id, self.__system_id, position.current_dt, position.as_dict, position
                        )
                    self.__trading_systems_persister.insert_positions(
                        instrument_id, self.__system_id, pos_manager.position_list
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

        if print_data == True:
            print('\nSystem performance summary: \n', metrics_df.to_string())

        composite_system_metrics_summary_plot(
            metrics_df,
            plot_fig=plot_performance_summary,
            save_fig_to_path=composite_summary_plot_to_path
        )

        if run_monte_carlo_sims:
            print(
                f'\nMonte carlo simulation stats ({num_of_monte_carlo_sims} simulations):\n',
                monte_carlo_simulations_df.to_string()
            )
            if monte_carlo_analysis_to_csv_path and monte_carlo_analysis_to_csv_path.endswith('.csv'):
                monte_carlo_simulations_df.to_csv(monte_carlo_analysis_to_csv_path)

        if system_analysis_to_csv_path and system_analysis_to_csv_path.endswith('.csv'):
            metrics_df.to_csv(system_analysis_to_csv_path)

        if print_data == True: 
            print(signal_handler)

        if write_signals_to_file_path:
            signal_handler.write_to_csv(write_signals_to_file_path, self.__system_name)

        if insert_data_to_db_bool:
            signal_handler.insert_into_db(self.__trading_systems_persister, self.__system_id)

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
            num_of_periods = avg_yearly_periods * pos_list_slice_years_est * num_of_pos_insert_multiplier
            num_of_positons = full_pos_list_slice_param
            self.__trading_systems_persister.update_trading_system_metrics(
                self.__system_id,
                {
                    TradingSystemAttributes.NUMBER_OF_PERIODS: num_of_periods,
                    TradingSystemAttributes.NUMBER_OF_POSITIONS: num_of_positons
                }
            )

        returns_distribution_plot(
            full_market_to_market_returns_list, full_mae_list, full_mfe_list,
            plot_fig=plot_returns_distribution, save_fig_to_path=save_returns_distribution_plot_to_path
        )

    def run_trading_system(
        self, data_dict: dict[tuple[str, str], pd.DataFrame], *args, 
        write_signals_to_file_path: str=None, print_data=False,
        insert_data_to_db_bool=False, **kwargs
    ):
        """
        Iterates over data, creates a TradingSession instance and generates
        positions.

        Parameters
        ----------
        :param data_dict:
            'dict' : A dict with key: (instrument_id, symbol), value: Pandas DataFrame 
            with data for the assets used in the system.
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

        for (instrument_id, symbol), data in data_dict.items():
            if not Price.CLOSE in data and not f'{Price.CLOSE}_{symbol}' in data:
                raise ValueError(f'column missing in DataFrame, symbol: {symbol}')

            if not pd.api.types.is_datetime64_any_dtype(data.index):
                raise ValueError('expected index of Pandas DataFrame to have a datetime dtype')

            order = self.__trading_systems_persister.get_order(instrument_id, self.__system_id)
            order = Order.from_proto(order)
            if order and order.created_dt >= data.index[-1]:
                continue

            position_id: str | None
            position: Position | None
            position_id, position = self.__trading_systems_persister.get_position(
                instrument_id, self.__system_id
            )
            if position and position.current_dt >= data.index[-1]:
                continue

            trading_session = TradingSession(
                self.__entry_logic_function, self.__exit_logic_function,
                signal_handler, instrument_id, symbol=symbol
            )
            current_order, position = trading_session(
                data, order, position, *args,
                print_data=print_data, **kwargs
            )
            order = current_order if current_order else order

            if order and insert_data_to_db_bool:
                order_direction = (
                    order.direction == TradingSystemAttributes.LONG 
                    if order.action == MarketState.ENTRY 
                    else None
                )
                self.__trading_systems_persister.upsert_order(
                    instrument_id, self.__system_id, order.order_type, order.action.value,
                    order.created_dt, order.active, order_direction,
                    **order.order_properties
                )

            if position and insert_data_to_db_bool:
                self.__trading_systems_persister.upsert_position(
                    instrument_id, self.__system_id, position.current_dt, position.as_dict, position, 
                    id=None if position.active == False else position_id
                )

        if print_data == True: 
            print(f'\nTrading System Signals\n{self.__system_name}')
            print(signal_handler)

        if write_signals_to_file_path:
            signal_handler.write_to_csv(write_signals_to_file_path, self.__system_name)

        if insert_data_to_db_bool:
            signal_handler.insert_into_db(self.__trading_systems_persister, self.__system_id)