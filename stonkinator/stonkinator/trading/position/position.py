from decimal import Decimal

import numpy as np


class Position:
    """
    Handles entering, exiting and data of a position.

    Parameters
    ----------
    capital : 'int/float/Decimal'
        The amount of capital to purchase assets with.
    direction : 'str'
        The direction of the Position. Expected value is either
        'long' or 'short'.
    entry_signal_dt : 'Pandas Timestamp/Datetime'
        Time and date of the entry signal that the position was
        initialized upon.
    fixed_position_size : Keyword arg 'bool'
        True/False decides if the position size should be the same
        fixed amount. The variable is used in the class' exit_market
        functions return statement control flow. Default value=True
    commission_pct_cost : Keyword arg 'float'
        The transaction cost given as a percentage
        (a float from 0.0 to 1.0) of the total transaction.
        Default value=0.0
    """

    def __init__(
        self, capital, direction, entry_signal_dt,
        fixed_position_size=True, commission_pct_cost=0.0
    ):
        self.__entry_price, self.__exit_price = None, None
        self.__position_size = None
        self.__entry_dt, self.__exit_dt = None, None
        self.__current_dt = None
        self.__capital = Decimal(capital)
        self.__direction = direction
        self.__entry_signal_dt = entry_signal_dt  # TODO: Only used in datetime_check, remove if datetime check is refactored?
        self.__exit_signal_dt = None  # TODO: Remove if datetime checks are refactored?
        self.__uninvested_capital = 0
        self.__fixed_position_size = fixed_position_size
        self.__commission_pct_cost = Decimal(commission_pct_cost)
        self.__commission = 0
        self.__active_position = False
        self.__last_price = None
        self.__unrealised_return = 0
        self.__unrealised_profit_loss = 0
        self.__returns_list = np.array([])
        self.__market_to_market_returns_list = np.array([])
        self.__position_profit_loss_list = np.array([])
        self.__trailing_exit = False
        self.__trailing_exit_price = None
        self.__exit_signal_given = False

    @property
    def entry_price(self):
        return self.__entry_price

    @property
    def position_size(self):
        return self.__position_size

    @property
    def commission(self):
        return self.__commission

    @property
    def active_position(self):
        return self.__active_position

    @property
    def entry_dt(self):
        return self.__entry_dt

    @property
    def exit_dt(self):
        return self.__exit_dt

    @property
    def current_dt(self):
        return self.__current_dt

    @current_dt.setter
    def current_dt(self, value):
        self.__current_dt = value

    @property
    def entry_signal_dt(self):
        return self.__entry_signal_dt

    @property
    def exit_signal_dt(self):
        return self.__exit_signal_dt

    @exit_signal_dt.setter
    def exit_signal_dt(self, value):
        self.__exit_signal_dt = value

    @property
    def capital(self):
        return self.__capital

    @property
    def direction(self):
        return self.__direction

    @property
    def fixed_position_size(self):
        return self.__fixed_position_size

    @property
    def returns_list(self):
        return self.__returns_list

    @property
    def unrealised_return(self):
        return self.__unrealised_return

    @property
    def unrealised_profit_loss(self):
        return self.__unrealised_profit_loss

    @property
    def position_return(self):
        """
        Calculates the positions return.

        :return:
            'Decimal'
        """

        # TODO: Replace 'long' and 'short' literals with some meta data attribute
        if self.__direction == 'long':
            return Decimal(
                ((self.__exit_price - self.__entry_price) / self.__entry_price) * 100
            ).quantize(Decimal('0.02'))
        elif self.__direction == 'short':
            return Decimal(
                ((self.__entry_price - self.__exit_price) / self.__entry_price) * 100
            ).quantize(Decimal('0.02'))

    @property
    def net_result(self):
        """
        Calculates the positions net result.

        :return:
            'Decimal'
        """

        if self.__direction == 'long':
            return Decimal(
                (self.__position_size * self.__exit_price) - ((self.__position_size * self.__entry_price) + \
                                                                self.__commission)
            ).quantize(Decimal('0.02'))
        elif self.__direction == 'short':
            return Decimal(
                (self.__position_size * self.__entry_price) - ((self.__position_size * self.__exit_price) + \
                                                                self.__commission)
            ).quantize(Decimal('0.02'))

    @property
    def gross_result(self):
        """
        Calculates the positions gross result.

        :return:
            'Decimal'
        """

        if self.__direction == 'long':
            return Decimal(
                (self.__position_size * self.__exit_price) - (self.__position_size * self.__entry_price)
            ).quantize(Decimal('0.02'))
        elif self.__direction == 'short':
            return Decimal(
                (self.__position_size * self.__entry_price) - (self.__position_size * self.__exit_price)
            ).quantize(Decimal('0.02'))

    @property
    def profit_loss(self):
        """
        Calculates the positions P/L.

        :return:
            'Decimal'
        """

        if self.__direction == 'long':
            return Decimal(self.__exit_price - self.__entry_price).quantize(Decimal('0.02'))
        elif self.__direction == 'short':
            return Decimal(self.__entry_price - self.__exit_price).quantize(Decimal('0.02'))

    @property
    def mae(self):
        """
        Maximum adverse excursion, the minimum value from the
        list of unrealised returns.

        :return:
            'int/float/Decimal'
        """

        if np.min(self.__returns_list) >= 0:
            return 0
        else:
            return np.min(self.__returns_list)

    @property
    def mfe(self):
        """
        Maximum favorable excursion, the maximum value from the
        list of unrealised returns.

        :return:
            'int/float/Decimal'
        """

        if np.max(self.__returns_list) > 0:
            return np.max(self.__returns_list)
        else:
            return 0

    @property
    def market_to_market_returns_list(self):
        return self.__market_to_market_returns_list

    @property
    def trailing_exit(self):
        return self.__trailing_exit

    @trailing_exit.setter
    def trailing_exit(self, value):
        self.__trailing_exit = value

    @property
    def trailing_exit_price(self):
        return self.__trailing_exit_price

    @trailing_exit_price.setter
    def trailing_exit_price(self, value):
        self.__trailing_exit_price = value

    @property
    def exit_signal_given(self):
        return self.__exit_signal_given

    @exit_signal_given.setter
    def exit_signal_given(self, value):
        self.__exit_signal_given = value

    @property
    def as_dict(self):
        return {
            'entry_dt': self.entry_dt,
            'exit_signal_dt': self.exit_signal_dt,
            'entry_price': float(self.entry_price),
            'exit_price': float(self.__exit_price),
            'returns_list': [float(x) for x in self.returns_list],
            'mtm_returns_list': [float(x) for x in self.market_to_market_returns_list],
            'position_return': float(self.position_return),
            'net_result': float(self.net_result),
            'gross_result': float(self.gross_result),
            'profit_loss': float(self.profit_loss),
            'mae': float(self.mae),
            'mfe': float(self.mfe)
       }

    def datetime_check(self, input_datetime):
        """
        Checks date and time properties against a given datetime
        value. Useful to make sure data points of the same date
        and time are not processed multiple times.
        
        Parameters
        ----------
        :param input_datetime:
            'Pandas Timestamp/Datetime' : Date and time to check
            against.
        :return:
            'bool' : Returns True/False depending on the result
            of the datetime comparisons.
        """

        if self.__exit_dt is not None:
            check = not self.__exit_dt >= input_datetime
        elif self.__exit_signal_dt is not None:
            check = not self.__exit_signal_dt >= input_datetime
        elif self.__current_dt is not None:
            check = not self.__current_dt >= input_datetime
        elif self.__entry_signal_dt is not None:
            check = not self.__entry_signal_dt >= input_datetime
        else:
            check = True

        return check

    def enter_market(self, entry_price, entry_dt):
        """
        Enters market at the given price.

        Parameters
        ----------
        :param entry_price:
            'int/float/Decimal' : The price of the asset when entering
            the market.
        :param entry_dt:
            'Pandas Timestamp/Datetime' : Time and date when entering
            the market.
        """

        assert (self.__active_position == False), 'A position is already active'

        self.__entry_price = Decimal(entry_price)
        self.__position_size = int(self.__capital / self.__entry_price)
        self.__uninvested_capital = self.__capital - (self.__position_size * self.__entry_price)
        self.__commission = (self.__position_size * self.__entry_price) * self.__commission_pct_cost
        self.__entry_dt = entry_dt
        self.__current_dt = entry_dt
        self.__active_position = True

    def exit_market(self, exit_price, exit_signal_dt, exit_dt):
        """
        Exits the market at the given price.

        Parameters
        ----------
        :param exit_price:
            'int/float/Decimal' : The price of the asset when exiting
            the market.
        :param exit_signal_dt:
            'Pandas Timestamp/Datetime' : Time and date when the signal
            to exit market was given.
        :param exit_dt:
            'Pandas Timestamp/Datetime' : Time and date when the order
            to exit market was made.
        :return:
            'int/float/Decimal' : Returns the capital amount, which is
            the same as the Position was instantiated with if
            fixed_position_size was set to True. Otherwise it returns
            the position size multiplied with the exit price + the
            amount of capital that was left over when entering the
            market.
        """

        # TODO: What to check here?
        # if self.__current_dt != exit_signal_dt or not self.__current_dt < exit_dt:
        if self.__current_dt < exit_dt == False:
            raise ValueError(
                f'Date mismatch.\n'
                f'self.__current_dt != exit_signal_dt: {self.__current_dt != exit_signal_dt}, '
                'should be True\n'
                f'not self.__current_dt < exit_dt: {not self.__current_dt < exit_dt}, '
                'should be True'
            )

        self.__exit_price = Decimal(exit_price)
        self.update(self.__exit_price, exit_signal_dt)
        if self.__exit_signal_dt is None:
            self.__exit_signal_dt = exit_signal_dt
        self.__exit_dt = exit_dt
        self.__active_position = False
        self.__commission += (self.__position_size * self.__exit_price) * self.__commission_pct_cost

        if not self.__fixed_position_size:
            self.__capital = Decimal(
                self.__position_size * self.__exit_price + self.__uninvested_capital
            ).quantize(Decimal('0.02'))
            return self.__capital
        else:
            return self.__capital

    def _unrealised_profit_loss(self, current_price):
        """
        Calculates and assigns the unrealised P/L, appends
        the value to __position_profit_loss_list.

        Parameters
        ----------
        :param current_price:
            'int/float/Decimal' : The assets most recent price.
        """

        if self.__direction == 'long':
            self.__unrealised_profit_loss = Decimal(
                current_price - self.__entry_price
            ).quantize(Decimal('0.02'))
            self.__position_profit_loss_list = np.append(
                self.__position_profit_loss_list, self.__unrealised_profit_loss
            )
        elif self.__direction == 'short':
            self.__unrealised_profit_loss = Decimal(
                self.__entry_price - current_price
            ).quantize(Decimal('0.02'))
            self.__position_profit_loss_list = np.append(
                self.__position_profit_loss_list, self.__unrealised_profit_loss
            )

    def _unrealised_return(self, current_price):
        """
        Calculates and assigns the unrealised return and the
        return from the two last recorded prices. Appends the
        values to __returns_list and __market_to_market_returns_list.

        Parameters
        ----------
        :param current_price:
            'int/float/Decimal' : The assets most recent price.
        """

        if self.__last_price is None:
            self.__last_price = self.__entry_price

        if self.__direction == 'long':
            unrealised_return = Decimal(
                ((current_price - self.__entry_price) / self.__entry_price) * 100
            ).quantize(Decimal('0.02'))
            self.__market_to_market_returns_list = np.append(
                self.__market_to_market_returns_list,
                Decimal(
                    (current_price - self.__last_price) / self.__last_price * 100
                ).quantize(Decimal('0.02'))
            )
            self.__returns_list = np.append(
                self.__returns_list, unrealised_return
            )
            self.__last_price = current_price
            self.__unrealised_return = unrealised_return
        elif self.__direction == 'short':
            unrealised_return = Decimal(
                ((self.__entry_price - current_price) / self.__entry_price) * 100
            ).quantize(Decimal('0.02'))
            self.market_to_market_returns_list = np.append(
                self.__market_to_market_returns_list,
                Decimal(
                    (self.__last_price - current_price) / self.__last_price * 100
                ).quantize(Decimal('0.02'))
            )
            self.__returns_list = np.append(
                self.__returns_list, unrealised_return
            )
            self.__last_price = current_price
            self.__unrealised_return = unrealised_return

    def update(self, price, current_dt):
        """
        Calls methods to update the unrealised return and
        unrealised profit and loss of the Position.

        Parameters
        ----------
        :param price:
            'float/Decimal' : The most recently updated price of the asset.
        :param current_dt:
            'Pandas Timestamp/Datetime' : Time and date of the data point
            the position is updated with.
        """

        self._unrealised_return(price)
        self._unrealised_profit_loss(price)
        self.__current_dt = current_dt

    def print_position_status(self):
        """
        Prints the status of the Position.
        """

        if self.__active_position == True:
            print(
                f'Active position\n'
                f'Periods in position: {len(self.__returns_list)}\n'
                f'Unrealised return sequence: {list(map(float, self.__returns_list))}'
            )

    def print_position_stats(self):
        """
        Prints stats of the Position.
        """

        print(
            f'Unrealised P/L sequence: {list(map(float, self.__position_profit_loss_list))}\n'
            f'Market to market returns: {list(map(float, self.__market_to_market_returns_list))}\n'
            f'Unrealised return sequence: {list(map(float, self.__returns_list))}'
        )