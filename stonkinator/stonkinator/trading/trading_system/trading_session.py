from decimal import Decimal

import pandas as pd

from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.trading_system_attributes import TradingSystemAttributes
from trading.position.order import Order
from trading.position.position import Position
from trading.signal_events.signal_handler import SignalHandler
from trading.plots.candlestick_plots import candlestick_plot


class TradingSession:
    """
    A class that represents a trading session. By using the given
    dataframe together with the given entry and exit logic a
    TradingSession instance generates Position objects.

    Parameters
    ----------
    entry_logic_function: 'function'
        A function with logic for when to enter a market.
    exit_logic_function: 'function'
        A function with logic for when to exit a market.
    signal_handler: Keyword arg 'None/SignalHandler'
        An instance of the SignalHandler class. Handles
        data from generated events/signals.
        Default value=None
    symbol: Keyword arg 'str'
        The ticker/symbol of the instrument to be traded
        in the current trading session. Default value=''
    """

    def __init__(
        self, entry_logic_function, exit_logic_function,
        signal_handler: SignalHandler, symbol=''
    ):
        self.__entry_logic_function = entry_logic_function
        self.__exit_logic_function = exit_logic_function
        self.__signal_handler = signal_handler
        self.__symbol = symbol
        self.__market_state_column = TradingSystemAttributes.MARKET_STATE

    def __call__(
        self, 
        dataframe: pd.DataFrame, 
        order: Order | None, 
        position: Position | None,
        *args,
        entry_args=None, exit_args=None,
        open_price_col_name='open',
        high_price_col_name='high',
        low_price_col_name='low',
        close_price_col_name='close', 
        volume_col_name='volume', 
        fixed_position_size=True, capital=10000, commission_pct_cost=0.0,
        print_data=False, **kwargs
    ):
        """
        Generates positions using the __entry_logic_function and 
        __exit_logic_function members.

        Parameters
        ----------
        :param dataframe: 
            Pandas.DataFrame : Data in the form of a Pandas DataFrame.
        TODO: Update method comment with order param
        :param position:
            Position : Current or most recent position of an instrument
            of the TradingSystem that creates this TradingSession instance.
        :param args:
            'tuple' : A tuple with arguments.
        :param entry_args:
            Keyword arg 'None/dict' : Key-value pairs with parameters used 
            with the entry logic. Default value=None
        :param exit_args:
            Keyword arg 'None/dict' : Key-value pairs with parameters used 
            with the exit logic. Default value=None
        :param open_price_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for open prices. Default value='open'
        :param high_price_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for high prices. Default value='high'
        :param low_price_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for low prices. Default value='low'
        :param close_price_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for close prices. Default value='close'
        :param volume_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for volume. Default value='volume'
        :param fixed_position_size:
            Keyword arg 'bool' : True/False decides whether the capital
            used for positions generated should be at a fixed amount or not.
            Default value=True
        :param capital:
            Keyword arg 'int/float' : The capital given to purchase assets
            with. Default value=10000
        :param commission_pct_cost:
            Keyword arg 'float' : The transaction cost given as a percentage
            (a float from 0.0 to 1.0) of the total transaction.
            Default value=0.0
        :param print_data:
            Keyword arg 'bool' : True/False decides whether to print data
            of positions and signals or not. Default value=False
        :param kwargs:
            'dict' : A dictionary with keyword arguments.
        TODO: Add return to method documentation
        """

        if position and position.active == True:
            if position.current_dt != dataframe.index[-2]:
                return order, position

            if order and order.active == True or position.exit_signal_given == True:
                capital = order.execute_exit(position, dataframe.iloc[-1], dataframe.index[-1])
            if position.active == False:
                if print_data:
                    position.print_position_stats()
                    print(
                        f'Exit index: {dataframe.index[-1]}: '
                        f'{format(dataframe[open_price_col_name].iloc[-1], ".3f")}, '
                        f'{dataframe.index[-1]}\n'
                        f'Realised return: {position.position_return}'
                    )
                return position
        elif position is None and order and order.active == True:
            position = order.execute_entry(
                capital, 
                dataframe.iloc[-1],
                dataframe.index[-1],
                fixed_position_size=fixed_position_size,
                commission_pct_cost=commission_pct_cost
            )
            if print_data:
                print(f'\nEntry order:\n{order.as_dict}')

        if position and position.active == True:
            position.update(
                Decimal(dataframe[close_price_col_name].iloc[-1]),
                dataframe.index[-1]
            )
            if print_data:
                position.print_position_status()
            self.__signal_handler.handle_active_position(
                self.__symbol, {
                    TradingSystemAttributes.SIGNAL_INDEX: dataframe.index[-1], 
                    TradingSystemAttributes.SIGNAL_DT: dataframe.index[-1],
                    TradingSystemAttributes.SYMBOL: self.__symbol, 
                    TradingSystemAttributes.ORDER: order.as_dict if order else None,
                    TradingSystemAttributes.PERIODS_IN_POSITION: len(position.returns_list), 
                    TradingSystemAttributes.UNREALISED_RETURN: position.unrealised_return,
                    self.__market_state_column: MarketState.ACTIVE.value
                }
            )
            if position.exit_signal_given == False:
                order = self.__exit_logic_function(dataframe, position, exit_args=exit_args)
            if order and order.active == True:
                self.__signal_handler.handle_exit_signal(
                    self.__symbol, {
                        TradingSystemAttributes.SIGNAL_INDEX: dataframe.index[-1], 
                        TradingSystemAttributes.SIGNAL_DT: dataframe.index[-1],
                        TradingSystemAttributes.SYMBOL: self.__symbol, 
                        TradingSystemAttributes.ORDER: order.as_dict,
                        TradingSystemAttributes.PERIODS_IN_POSITION: len(position.returns_list),
                        TradingSystemAttributes.UNREALISED_RETURN: position.unrealised_return,
                        self.__market_state_column: MarketState.EXIT.value
                    }
                )
                if print_data:
                    print(f'\nExit order:\n{order.as_dict}')
        elif order is None or order.active == False:
            order = self.__entry_logic_function(dataframe, entry_args=entry_args)
            if order and order.active == True:
                self.__signal_handler.handle_entry_signal(
                    self.__symbol, {
                        TradingSystemAttributes.SIGNAL_INDEX: dataframe.index[-1], 
                        TradingSystemAttributes.SIGNAL_DT: dataframe.index[-1],
                        TradingSystemAttributes.SYMBOL: self.__symbol,
                        TradingSystemAttributes.ORDER: order.as_dict,
                        TradingSystemAttributes.PERIODS_IN_POSITION: 0,
                        self.__market_state_column: MarketState.ENTRY.value
                    }
                )
                if print_data: 
                    print(f'\nEntry order:\n{order.as_dict}')
        return order, position


class BacktestTradingSession:
    """
    A class that represents a trading session. By using the given
    dataframe together with the given entry and exit logic a
    BacktestTradingSession instance generates Position objects.

    Parameters
    ----------
    entry_logic_function: 'function'
        A function with logic for when to enter a market.
    exit_logic_function: 'function'
        A function with logic for when to exit a market.
    dataframe: 'Pandas.DataFrame'
        Data in the form of a Pandas DataFrame.
    signal_handler: Keyword arg 'None/SignalHandler'
        An instance of the SignalHandler class. Handles
        data from generated events/signals.
        Default value=None
    symbol: Keyword arg 'str'
        The ticker/symbol of the instrument to be traded
        in the current trading session. Default value=''
    """

    def __init__(
        self, entry_logic_function, exit_logic_function, dataframe: pd.DataFrame,
        signal_handler: SignalHandler, symbol=''
    ):
        self.__entry_logic_function = entry_logic_function
        self.__exit_logic_function = exit_logic_function
        self.__dataframe = dataframe
        self.__signal_handler = signal_handler
        self.__symbol = symbol
        self.__market_state_column = TradingSystemAttributes.MARKET_STATE

    def __call__(
        self, *args, 
        entry_args=None, exit_args=None, 
        max_req_periods_feature=TradingSystemAttributes.REQ_PERIOD_ITERS, 
        open_price_col_name='open',
        high_price_col_name='high',
        low_price_col_name='low',
        close_price_col_name='close', 
        volume_price_col_name='volume',
        fixed_position_size=True, capital=10000, commission_pct_cost=0.0,
        market_state_null_default=False,
        generate_signals=False, plot_positions=False, 
        save_position_figs_path=None,
        print_data=False, **kwargs
    ):
        """
        Generates positions using the __entry_logic_function and 
        __exit_logic_function members. If the given value for 
        generate_signals is True, signals will be generated from 
        the most recent data.

        Parameters
        ----------
        :param args:
            'tuple' : A tuple with arguments.
        :param entry_args:
            Keyword arg 'None/dict' : Key-value pairs with parameters used 
            with the entry logic. Default value=None
        :param exit_args:
            Keyword arg 'None/dict' : Key-value pairs with parameters used 
            with the exit logic. Default value=None
        :param max_req_periods_feature:
            Keyword arg 'str' : A key contained in the entry_args dict 
            that should have the value of the number of periods required 
            for all features to be calculated before being able to 
            generate signals from the data. Default value='req_period_iters'
        :param open_price_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for open prices. Default value='open'
        :param high_price_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for high prices. Default value='high'
        :param low_price_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for low prices. Default value='low'
        :param close_price_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for close prices. Default value='close'
        :param volume_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for volume. Default value='volume'
        :param fixed_position_size:
            Keyword arg 'bool' : True/False decides whether the capital
            used for positions generated should be at a fixed amount or not.
            Default value=True
        :param capital:
            Keyword arg 'int/float' : The capital given to purchase assets
            with. Default value=10000
        :param commission_pct_cost:
            Keyword arg 'float' : The transaction cost given as a percentage
            (a float from 0.0 to 1.0) of the total transaction.
            Default value=0.0
        :param market_state_null_default:
            Keyword arg 'bool' : True/False decides whether the market_state
            property should be assigned a null value by default or not.
            Default value=False
        :param generate_signals:
            Keyword arg 'bool' : True/False decides whether or not market
            events/signals should be generated from the most recent data.
            Default value=False
        :param plot_positions:
            Keyword arg 'bool' : True/False decides whether or not a plot of
            a candlestick chart visualizing the points of buying and selling a
            position should be displayed. Default value=False
        :param save_position_figs_path:
            Keyword arg 'None/str' : Provide a file path as a str to save a
            candlestick chart visualizing the points of buying and selling a
            position. Default value=None
        :param print_data:
            Keyword arg 'bool' : True/False decides whether to print data
            of positions and signals or not. Default value=False
        :param kwargs:
            'dict' : A dictionary with keyword arguments.
        """

        order: Order = None
        position: Position = None

        for idx, _ in enumerate(self.__dataframe.itertuples()):
            # entry_args[max_req_periods_feature] is the parameter used 
            # with the longest period lookback required to calculate.
            if idx <= entry_args[max_req_periods_feature]:
                continue

            if position and position.active == True:
                position.update(
                    Decimal(self.__dataframe[close_price_col_name].iloc[idx-1]),
                    self.__dataframe.index[idx-1]
                )
                if position.exit_signal_given == False:
                    order = self.__exit_logic_function(
                        self.__dataframe.iloc[:idx], position, exit_args=exit_args
                    )
                if order and order.active == True or position.exit_signal_given == True:
                    capital = order.execute_exit(
                        position, self.__dataframe.iloc[idx], self.__dataframe.index[idx]
                    )
                if position.active == False:
                    if print_data:
                        position.print_position_stats()
                        print(
                            f'Exit index {idx}: '
                            f'{format(self.__dataframe[open_price_col_name].iloc[idx], ".3f")}, '
                            f'{self.__dataframe.index[idx]}\n'
                            f'Realised return: {position.position_return}'
                        )
                    if plot_positions:
                        if save_position_figs_path is not None:
                            position_figs_path = save_position_figs_path + (
                                fr'\{self.__dataframe.iloc[(idx - len(position.returns_list))].Date.strftime("%Y-%m-%d")}.jpg'
                            )
                        else:
                            position_figs_path = save_position_figs_path
                        candlestick_plot(
                            self.__dataframe.iloc[(idx-len(position.returns_list)-20):(idx+15)],
                            position.entry_dt, position.entry_price, 
                            self.__dataframe.index[idx], 
                            self.__dataframe[open_price_col_name].iloc[idx], 
                            save_fig_to_path=position_figs_path
                        )
                    yield position
                continue
            elif position is None and order and order.active == True:
                position = order.execute_entry(
                    capital,
                    self.__dataframe.iloc[idx],
                    self.__dataframe.index[idx],
                    fixed_position_size=fixed_position_size, 
                    commission_pct_cost=commission_pct_cost
                )
                if print_data:
                    print(f'\nEntry order:\n{order.as_dict}')
            else:
                order = self.__entry_logic_function(
                    self.__dataframe.iloc[:idx], entry_args=entry_args
                )
                if order and order.active == True:
                    position = order.execute_entry(
                        capital,
                        self.__dataframe.iloc[idx],
                        self.__dataframe.index[idx],
                        fixed_position_size=fixed_position_size, 
                        commission_pct_cost=commission_pct_cost
                    )
                    if print_data:
                        print(f'\nEntry order:\n{order.as_dict}')

        # Handle the trading sessions current market state/events/signals.
        if generate_signals:
            if market_state_null_default:
                self.__signal_handler.handle_entry_signal(
                    self.__symbol, {
                        TradingSystemAttributes.SIGNAL_DT: self.__dataframe.index[-1],
                        self.__market_state_column: MarketState.NULL.value
                    }
                )
                return
            if position and position.active == True:
                position.update(
                    Decimal(self.__dataframe[close_price_col_name].iloc[-1]),
                    self.__dataframe.index[-1]
                )
                if print_data:
                    position.print_position_status()
                self.__signal_handler.handle_active_position(
                    self.__symbol, {
                        TradingSystemAttributes.SIGNAL_INDEX: len(self.__dataframe), 
                        TradingSystemAttributes.SIGNAL_DT: self.__dataframe.index[-1], 
                        TradingSystemAttributes.SYMBOL: self.__symbol, 
                        TradingSystemAttributes.ORDER: order.as_dict if order else None,
                        TradingSystemAttributes.PERIODS_IN_POSITION: len(position.returns_list), 
                        TradingSystemAttributes.UNREALISED_RETURN: position.unrealised_return,
                        self.__market_state_column: MarketState.ACTIVE.value
                    }
                )
                if position.exit_signal_given == False:
                    order = self.__exit_logic_function(self.__dataframe, position, exit_args=exit_args)
                if order and order.active == True:
                    self.__signal_handler.handle_exit_signal(
                        self.__symbol, {
                            TradingSystemAttributes.SIGNAL_INDEX: len(self.__dataframe), 
                            TradingSystemAttributes.SIGNAL_DT: self.__dataframe.index[-1], 
                            TradingSystemAttributes.SYMBOL: self.__symbol, 
                            TradingSystemAttributes.ORDER: order.as_dict,
                            TradingSystemAttributes.PERIODS_IN_POSITION: len(position.returns_list),
                            TradingSystemAttributes.UNREALISED_RETURN: position.unrealised_return,
                            self.__market_state_column: MarketState.EXIT.value
                        }
                    )
                    if print_data: 
                        print(f'\nExit order:\n{order.as_dict}')
            elif order is None or order.active == False:
                order = self.__entry_logic_function(self.__dataframe, entry_args=entry_args)
                if order and order.active == True:
                    self.__signal_handler.handle_entry_signal(
                        self.__symbol, {
                            TradingSystemAttributes.SIGNAL_INDEX: len(self.__dataframe), 
                            TradingSystemAttributes.SIGNAL_DT: self.__dataframe.index[-1], 
                            TradingSystemAttributes.SYMBOL: self.__symbol,
                            TradingSystemAttributes.ORDER: order.as_dict,
                            TradingSystemAttributes.PERIODS_IN_POSITION: 0,
                            self.__market_state_column: MarketState.ENTRY.value
                        }
                    )
                    if print_data: 
                        print(f'\nEntry order:\n{order.as_dict}')
            
            self.__signal_handler.current_order = (order, self.__symbol)