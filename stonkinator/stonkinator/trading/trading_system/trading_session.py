import os
from decimal import Decimal

import pandas as pd

from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.trading_system_attributes import TradingSystemAttributes
from trading.data.metadata.price import Price
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
    signal_handler: 'SignalHandler'
        An instance of the SignalHandler class. Handles
        data from generated events/signals.
    instrument_id: 'str'
        Identifier of an instrument.
    symbol: Keyword arg 'str'
        The ticker/symbol of the instrument to be traded
        in the current trading session. Default value=''
    """

    def __init__(
        self, entry_logic_function, exit_logic_function,
        signal_handler: SignalHandler, instrument_id, symbol=''
    ):
        self.__entry_logic_function = entry_logic_function
        self.__exit_logic_function = exit_logic_function
        self.__signal_handler = signal_handler
        self.__instrument_id = instrument_id
        self.__symbol = symbol

    def __call__(
        self, 
        dataframe: pd.DataFrame, 
        order: Order | None, 
        position: Position | None,
        *args,
        entry_args=None, exit_args=None,
        fixed_position_size=True, capital=10000, commission_pct_cost=0.0,
        print_data=False, **kwargs
    ) -> tuple[Order | None, Position | None]:
        """
        Generates positions using the __entry_logic_function and 
        __exit_logic_function members.

        Parameters
        ----------
        :param dataframe: 
            Pandas.DataFrame : Data in the form of a Pandas DataFrame.
        :param order:
            Order : Current or most recent order of an instrument
            of the TradingSystem that creates this TradingSession instance.
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
        :return:
            'tuple[Order, Position]' : Returns the current or most recent
            order and position of the TradingSystem that creates this 
            TradingSession instance in a tuple.
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
                        f'{self.__symbol} exit:\n'
                        f'Date: {dataframe.index[-1]}\n'
                        f'Price: {format(position.exit_price, ".3f")}\n'
                        f'Realised return: {position.position_return}'
                    )
                return order, position
        elif (position is None or position.active == False) and order and order.active == True:
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
                Decimal(dataframe[Price.CLOSE].iloc[-1]),
                dataframe.index[-1]
            )
            if print_data:
                position.print_position_status()
            self.__signal_handler.handle_active_position(
                self.__instrument_id, self.__symbol, {
                    TradingSystemAttributes.SIGNAL_DT: dataframe.index[-1],
                    TradingSystemAttributes.SYMBOL: self.__symbol, 
                    TradingSystemAttributes.ORDER: order.as_dict if order and order.active else None,
                    TradingSystemAttributes.PERIODS_IN_POSITION: position.periods_in_position, 
                    TradingSystemAttributes.UNREALISED_RETURN: float(position.unrealised_return),
                    TradingSystemAttributes.MARKET_STATE: MarketState.ACTIVE.value
                }
            )
            if position.exit_signal_given == False:
                order = self.__exit_logic_function(dataframe, position, exit_args=exit_args)
            if order and order.active == True:
                self.__signal_handler.handle_exit_signal(
                    self.__instrument_id, self.__symbol, {
                        TradingSystemAttributes.SIGNAL_DT: dataframe.index[-1],
                        TradingSystemAttributes.SYMBOL: self.__symbol, 
                        TradingSystemAttributes.ORDER: order.as_dict,
                        TradingSystemAttributes.PERIODS_IN_POSITION: position.periods_in_position,
                        TradingSystemAttributes.UNREALISED_RETURN: float(position.unrealised_return),
                        TradingSystemAttributes.MARKET_STATE: MarketState.EXIT.value
                    }
                )
                if print_data:
                    print(f'\nExit order:\n{order.as_dict}')
        elif order is None or order.active == False:
            order = self.__entry_logic_function(dataframe, entry_args=entry_args)
            if order and order.active == True:
                self.__signal_handler.handle_entry_signal(
                    self.__instrument_id, self.__symbol, {
                        TradingSystemAttributes.SIGNAL_DT: dataframe.index[-1],
                        TradingSystemAttributes.SYMBOL: self.__symbol,
                        TradingSystemAttributes.ORDER: order.as_dict,
                        TradingSystemAttributes.PERIODS_IN_POSITION: 0,
                        TradingSystemAttributes.MARKET_STATE: MarketState.ENTRY.value
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
    signal_handler: 'SignalHandler'
        An instance of the SignalHandler class. Handles
        data from generated events/signals.
    instrument_id: 'str'
        Identifier of an instrument.
    symbol: Keyword arg 'str'
        The ticker/symbol of the instrument to be traded
        in the current trading session. Default value=''
    """

    def __init__(
        self, entry_logic_function, exit_logic_function, dataframe: pd.DataFrame,
        signal_handler: SignalHandler, instrument_id, symbol=''
    ):
        self.__entry_logic_function = entry_logic_function
        self.__exit_logic_function = exit_logic_function
        self.__dataframe = dataframe
        self.__signal_handler = signal_handler
        self.__instrument_id = instrument_id
        self.__symbol = symbol

    def __call__(
        self, *args, 
        entry_args=None, exit_args=None, 
        max_req_periods_feature=TradingSystemAttributes.REQ_PERIOD_ITERS, 
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
                    Decimal(self.__dataframe[Price.CLOSE].iloc[idx-1]),
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
                            f'{self.__symbol} exit:\n'
                            f'Date: {self.__dataframe.index[idx]}\n'
                            f'Price: {format(position.exit_price, ".3f")}\n'
                            f'Realised return: {position.position_return}'
                        )
                    if plot_positions:
                        if save_position_figs_path is not None:
                            position_figs_path = (
                                fr'{save_position_figs_path}/{self.__instrument_id}_{self.__dataframe.index[idx]}.png'
                            )
                        else:
                            position_figs_path = save_position_figs_path
                        if not os.path.exists(save_position_figs_path):
                            os.makedirs(save_position_figs_path)
                        candlestick_plot(
                            self.__dataframe.iloc[(idx-position.periods_in_position-20):(idx+15)],
                            position.entry_dt, position.entry_price, 
                            self.__dataframe.index[idx], 
                            position.exit_price,
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
                        TradingSystemAttributes.MARKET_STATE: MarketState.NULL.value
                    }
                )
                return
            if position and position.active == True:
                position.update(
                    Decimal(self.__dataframe[Price.CLOSE].iloc[-1]),
                    self.__dataframe.index[-1]
                )
                if print_data:
                    position.print_position_status()
                self.__signal_handler.handle_active_position(
                    self.__instrument_id, self.__symbol, {
                        TradingSystemAttributes.SIGNAL_DT: self.__dataframe.index[-1], 
                        TradingSystemAttributes.SYMBOL: self.__symbol, 
                        TradingSystemAttributes.ORDER: order.as_dict if order else None,
                        TradingSystemAttributes.PERIODS_IN_POSITION: position.periods_in_position, 
                        TradingSystemAttributes.UNREALISED_RETURN: float(position.unrealised_return),
                        TradingSystemAttributes.MARKET_STATE: MarketState.ACTIVE.value
                    }
                )
                if position.exit_signal_given == False:
                    order = self.__exit_logic_function(self.__dataframe, position, exit_args=exit_args)
                if order and order.active == True:
                    self.__signal_handler.handle_exit_signal(
                        self.__instrument_id, self.__symbol, {
                            TradingSystemAttributes.SIGNAL_DT: self.__dataframe.index[-1], 
                            TradingSystemAttributes.SYMBOL: self.__symbol, 
                            TradingSystemAttributes.ORDER: order.as_dict,
                            TradingSystemAttributes.PERIODS_IN_POSITION: position.periods_in_position,
                            TradingSystemAttributes.UNREALISED_RETURN: float(position.unrealised_return),
                            TradingSystemAttributes.MARKET_STATE: MarketState.EXIT.value
                        }
                    )
                    if print_data: 
                        print(f'\nExit order:\n{order.as_dict}')
            elif order is None or order.active == False:
                order = self.__entry_logic_function(self.__dataframe, entry_args=entry_args)
                if order and order.active == True:
                    self.__signal_handler.handle_entry_signal(
                        self.__instrument_id, self.__symbol, {
                            TradingSystemAttributes.SIGNAL_DT: self.__dataframe.index[-1], 
                            TradingSystemAttributes.SYMBOL: self.__symbol,
                            TradingSystemAttributes.ORDER: order.as_dict,
                            TradingSystemAttributes.PERIODS_IN_POSITION: 0,
                            TradingSystemAttributes.MARKET_STATE: MarketState.ENTRY.value
                        }
                    )
                    if print_data: 
                        print(f'\nEntry order:\n{order.as_dict}')
            
            self.__signal_handler.current_order = (order, self.__instrument_id)
            self.__signal_handler.current_position = (position, self.__instrument_id)