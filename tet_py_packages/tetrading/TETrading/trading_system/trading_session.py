from decimal import Decimal

import pandas as pd

from TETrading.data.metadata.market_state_enum import MarketState
from TETrading.data.metadata.trading_system_attributes import TradingSystemAttributes
from TETrading.position.position import Position
from TETrading.signal_events.signal_handler import SignalHandler
from TETrading.plots.candlestick_plots import candlestick_plot


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
        self, dataframe: pd.DataFrame, position: Position, *args,
        entry_args=None, exit_args=None,
        open_price_col_name='open',
        high_price_col_name='high',
        low_price_col_name='low',
        close_price_col_name='close', 
        volume_price_col_name='volume', 
        fixed_position_size=True, capital=10000, commission_pct_cost=0.0,
        print_data=False, **kwargs
    ):
        """
        Generates positions using the __entry_logic_function and 
        __exit_logic_function members. If the given value for 
        generate_signals is True, signals will be generated from 
        the most recent data.

        Parameters
        ----------
        :param dataframe: 
            Pandas.DataFrame : Data in the form of a Pandas DataFrame.
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
        :param volume_price_col_name:
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
        """

        if not position:
            position = Position(-1, None)

        if position.active_position is True and position.exit_signal_dt is not None:
            if position.exit_signal_dt != dataframe.index[-2]:
                return position

            capital = position.exit_market(
                dataframe[open_price_col_name].iloc[-1],
                dataframe.index[-2]
            )
            position.price_data_json = (
                dataframe.iloc[-len(position.returns_list)-15:]
                [[open_price_col_name, high_price_col_name, low_price_col_name, 
                  close_price_col_name, volume_price_col_name]].to_json()
            )
            if print_data:
                position.print_position_stats()
                print(
                    f'Exit index: {dataframe.index[-1]}: '
                    f'{format(dataframe[open_price_col_name].iloc[-1], ".3f")}, '
                    f'{dataframe.index[-1]}\n'
                    f'Realised return: {position.position_return}'
                )
            return position

        if position.active_position is False and position.entry_signal_given is True:
            position.enter_market(
                dataframe[open_price_col_name].iloc[-1],
                dataframe.index[-1]
            )
            if print_data:
                print(
                    f'\nEntry index: {dataframe.index[-1]}: '
                    f'{format(dataframe[open_price_col_name].iloc[-1], ".3f")}, '
                    f'{dataframe.index[-1]}'
                )

        position.print_position_stats()

        if position.active_position is True:
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
                    TradingSystemAttributes.DIRECTION: position.direction,
                    TradingSystemAttributes.PERIODS_IN_POSITION: len(position.returns_list), 
                    TradingSystemAttributes.UNREALISED_RETURN: position.unrealised_return,
                    self.__market_state_column: MarketState.ACTIVE.value
                }
            )
            exit_condition, position.trailing_exit_price, position.trailing_exit = self.__exit_logic_function(
                dataframe, position.trailing_exit, position.trailing_exit_price, 
                position.entry_price, len(position.returns_list), 
                position.unrealised_return, exit_args=exit_args
            )
            if exit_condition == True:
                position.exit_signal_dt = dataframe.index[-1]
                self.__signal_handler.handle_exit_signal(
                    self.__symbol, {
                        TradingSystemAttributes.SIGNAL_INDEX: dataframe.index[-1], 
                        TradingSystemAttributes.SIGNAL_DT: dataframe.index[-1],
                        TradingSystemAttributes.SYMBOL: self.__symbol, 
                        TradingSystemAttributes.DIRECTION: position.direction,
                        TradingSystemAttributes.PERIODS_IN_POSITION: len(position.returns_list),
                        TradingSystemAttributes.UNREALISED_RETURN: position.unrealised_return,
                        self.__market_state_column: MarketState.EXIT.value
                    }
                )
                if print_data: 
                    print(f'\nExit signal, exit next open\nIndex: {dataframe.index[-1]}')
        elif position.active_position is False:
            entry_signal, direction = self.__entry_logic_function(
                dataframe, entry_args=entry_args
            )
            if entry_signal == True:
                position = Position(
                    capital, direction,
                    entry_signal_dt=dataframe.index[-1],
                    fixed_position_size=fixed_position_size, 
                    commission_pct_cost=commission_pct_cost
                )
                position.entry_signal_given = True
                self.__signal_handler.handle_entry_signal(
                    self.__symbol, {
                        TradingSystemAttributes.SIGNAL_INDEX: dataframe.index[-1], 
                        TradingSystemAttributes.SIGNAL_DT: dataframe.index[-1],
                        TradingSystemAttributes.SYMBOL: self.__symbol,
                        TradingSystemAttributes.DIRECTION: direction,
                        self.__market_state_column: MarketState.ENTRY.value
                    }
                )
                if print_data: 
                    print(f'\nEntry signal, buy next open\nIndex {dataframe.index[-1]}')
        return position


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
        self, *args, entry_args=None, exit_args=None, 
        max_req_periods_feature=TradingSystemAttributes.REQ_PERIOD_ITERS, 
        datetime_col_name='date',
        close_price_col_name='close', open_price_col_name='open',
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
        :param datetime_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains time and date data. Default value='date'
        :param close_price_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for close prices. Default value='close'
        :param open_price_col_name:
            Keyword arg 'str' : The column of the objects __dataframe that
            contains values for open prices. Default value='open'
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

        position = Position(-1, None)

        if isinstance(self.__dataframe.index, pd.DatetimeIndex):
            self.__dataframe.reset_index(level=0, inplace=True)

        for index, _ in enumerate(self.__dataframe.itertuples()):
            # entry_args[max_req_periods_feature] is the parameter used 
            # with the longest period lookback required to calculate.
            if index <= entry_args[max_req_periods_feature]:
                continue

            if position.active_position is True:
                position.update(
                    Decimal(self.__dataframe[close_price_col_name].iloc[index-1]),
                    self.__dataframe[datetime_col_name].iloc[index-1]
                )
                exit_condition, position.trailing_exit, position.trailing_exit_price = \
                    self.__exit_logic_function(
                        self.__dataframe.iloc[:index], 
                        position.trailing_exit, position.trailing_exit_price, 
                        position.entry_price, len(position.returns_list), 
                        exit_args=exit_args
                    )
                if exit_condition == True:
                    capital = position.exit_market(
                        self.__dataframe[open_price_col_name].iloc[index], 
                        self.__dataframe[datetime_col_name].iloc[index-1]
                    )
                    position.price_data_json = (
                        self.__dataframe.iloc[(index-len(position.returns_list)-15):(index+15)]
                            [['open', 'high', 'low', 'close', 'volume', 'date']].to_json()
                    )
                    if print_data:
                        position.print_position_stats()
                        print(
                            f'Exit index {index}: '
                            f'{format(self.__dataframe[open_price_col_name].iloc[index], ".3f")}, '
                            f'{self.__dataframe[datetime_col_name].iloc[index]}\n'
                            f'Realised return: {position.position_return}'
                        )
                    if plot_positions:
                        if save_position_figs_path is not None:
                            position_figs_path = save_position_figs_path + (
                                fr'\{self.__dataframe.iloc[(index - len(position.returns_list))].Date.strftime("%Y-%m-%d")}.jpg'
                            )
                        else:
                            position_figs_path = save_position_figs_path
                        candlestick_plot(
                            self.__dataframe.iloc[(index-len(position.returns_list)-20):(index+15)],
                            position.entry_dt, position.entry_price, 
                            self.__dataframe[datetime_col_name].iloc[index], 
                            self.__dataframe[open_price_col_name].iloc[index], 
                            save_fig_to_path=position_figs_path
                        )
                    yield position
                continue

            if position.active_position is False:
                entry_signal, direction = self.__entry_logic_function(
                    self.__dataframe.iloc[:index], entry_args=entry_args
                )
                if entry_signal == True:
                    position = Position(
                        capital, direction,
                        fixed_position_size=fixed_position_size, 
                        commission_pct_cost=commission_pct_cost
                    )
                    position.enter_market(
                        self.__dataframe[open_price_col_name].iloc[index],
                        self.__dataframe[datetime_col_name].iloc[index]
                    )
                    if print_data:
                        print(
                            f'\nEntry index {index}: '
                            f'{format(self.__dataframe[open_price_col_name].iloc[index], ".3f")}, '
                            f'{self.__dataframe[datetime_col_name].iloc[index]}'
                        )

        # Handle the trading sessions current market state/events/signals.
        if market_state_null_default and generate_signals:
            self.__signal_handler.handle_entry_signal(
                self.__symbol, {
                    TradingSystemAttributes.SIGNAL_DT: self.__dataframe[datetime_col_name].iloc[-1],
                    self.__market_state_column: MarketState.NULL.value
                }
            )
            return
        if position.active_position is True and generate_signals:
            position.update(
                Decimal(self.__dataframe[close_price_col_name].iloc[-1]),
                self.__dataframe[datetime_col_name].iloc[-1]
            )
            if print_data:
                position.print_position_status()
            self.__signal_handler.handle_active_position(
                self.__symbol, {
                    TradingSystemAttributes.SIGNAL_INDEX: len(self.__dataframe), 
                    TradingSystemAttributes.SIGNAL_DT: self.__dataframe[datetime_col_name].iloc[-1], 
                    TradingSystemAttributes.SYMBOL: self.__symbol, 
                    TradingSystemAttributes.DIRECTION: position.direction,
                    TradingSystemAttributes.PERIODS_IN_POSITION: len(position.returns_list), 
                    TradingSystemAttributes.UNREALISED_RETURN: position.unrealised_return,
                    self.__market_state_column: MarketState.ACTIVE.value
                }
            )
            exit_condition, position.trailing_exit_price, position.trailing_exit = self.__exit_logic_function(
                self.__dataframe, position.trailing_exit, position.trailing_exit_price, 
                position.entry_price, len(position.returns_list), 
                position.unrealised_return, exit_args=exit_args
            )
            if exit_condition == True:
                self.__signal_handler.handle_exit_signal(
                    self.__symbol, {
                        TradingSystemAttributes.SIGNAL_INDEX: len(self.__dataframe), 
                        TradingSystemAttributes.SIGNAL_DT: self.__dataframe[datetime_col_name].iloc[-1], 
                        TradingSystemAttributes.SYMBOL: self.__symbol, 
                        TradingSystemAttributes.DIRECTION: position.direction,
                        TradingSystemAttributes.PERIODS_IN_POSITION: len(position.returns_list),
                        TradingSystemAttributes.UNREALISED_RETURN: position.unrealised_return,
                        self.__market_state_column: MarketState.EXIT.value
                    }
                )
                if print_data: 
                    print(f'\nExit signal, exit next open\nIndex {len(self.__dataframe)}')
        elif position.active_position is False and generate_signals:
            entry_signal, direction = self.__entry_logic_function(
                self.__dataframe, entry_args=entry_args
            )
            if entry_signal == True:
                self.__signal_handler.handle_entry_signal(
                    self.__symbol, {
                        TradingSystemAttributes.SIGNAL_INDEX: len(self.__dataframe), 
                        TradingSystemAttributes.SIGNAL_DT: self.__dataframe[datetime_col_name].iloc[-1], 
                        TradingSystemAttributes.SYMBOL: self.__symbol,
                        TradingSystemAttributes.DIRECTION: direction,
                        self.__market_state_column: MarketState.ENTRY.value
                    }
                )
                if print_data: 
                    print(f'\nEntry signal, buy next open\nIndex {len(self.__dataframe)}')