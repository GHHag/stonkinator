from abc import ABCMeta, abstractmethod
from datetime import datetime

from trading.position.position import Position

class BaseOrder(metaclass=ABCMeta):
    
    __metaclass__ = ABCMeta

    @property
    @abstractmethod
    def direction(self):
        ...

    @property
    @abstractmethod
    def action(self):
        ...

    @property
    @abstractmethod
    def created_dt(self):
        ...

    @property
    @abstractmethod
    def active(self):
        ...

    @property
    @abstractmethod
    def as_dict(self):
        ...

    # TODO: Can a single execute implementation support both entry and exit functionality?
    @abstractmethod
    def execute_entry(self):
        ...

    @abstractmethod
    def execute_exit(self):
        ...


class Order(BaseOrder):
    __direction: str
    __action: str
    __created_dt: datetime
    __active: bool

    def __init__(self, direction, action, dt):
        self.__direction = direction
        self.__action = action
        self.__created_dt = dt
        self.__active = True

    @property
    def direction(self):
        return self.__direction

    @property
    def action(self):
        return self.__action

    @property
    def created_dt(self):
        return self.__created_dt

    @property
    def active(self):
        return self.__active

    @active.setter
    def active(self, value):
        self.__active = value

    @property
    def as_dict(self):
        return {
            'direction': self.__direction,
            'action': self.__action,
            'created_dt': self.__created_dt,
            'active': self.__active
        }

    def execute_entry(
        self, capital, price_data_point, fixed_position_size, 
        commission_pct_cost
    ) -> Position:
        position = Position(
            capital, self.__direction,
            entry_signal_dt=self.__created_dt,
            fixed_position_size=fixed_position_size, 
            commission_pct_cost=commission_pct_cost
        )
        position.entry_signal_given = True  # TODO: Handle this with order instead?
        position.enter_market(price_data_point['open'], price_data_point['date'])
        self.__active = False
        return position

    def execute_exit(self, position: Position, price_data_point, prior_dt):
        capital = position.exit_market(
            # TODO: Can I pass self.__created_dt here instead of prior_dt? Does it break
            # current datetime checks?
            price_data_point['open'], prior_dt, price_data_point['date']
        )
        self.__active = False
        return capital


class MarketOrder(Order):

    def __init__(self, direction, action, dt):
        super().__init__(direction, action, dt)


class LimitOrder(Order):
    __price: float
    __max_duration: int
    __duration: int

    def __init__(self, direction, action, dt, price, max_duration):
        super().__init__(direction, action, dt)
        self.__price = price
        self.__max_duration = max_duration
        self.__duration = 0

    # TODO: Is this field needed publicly?
    @property
    def price(self):
        return self.__price

    # TODO: Is this field needed publicly?
    @property
    def max_duration(self):
        return self.__max_duration

    @property
    def as_dict(self):
        return {
            'direction': self.direction,
            'action': self.action,
            'created_dt': self.created_dt,
            'active': self.active,
            'price': self.__price,
            'max_duration': self.__max_duration,
            'duration': self.__duration
        }

    def execute_entry(
        self, capital, price_data_point, fixed_position_size, 
        commission_pct_cost
    ) -> Position | None:
        if self.__price > price_data_point['low']:
            position = Position(
                capital, self.direction,
                entry_signal_dt=self.created_dt,
                fixed_position_size=fixed_position_size, 
                commission_pct_cost=commission_pct_cost
            )
            position.entry_signal_given = True
            position.enter_market(self.__price, price_data_point['date'])
            # TODO: Does this work without defining a setter in super class?
            self.active = False
            return position
        else:
            self.__duration += 1
            if self.__duration >= self.__max_duration:
                self.active = False
            # TODO: Is the current_dt needed? Could it be defined in Order class instead?
            # position.current_dt = price_data_point['date']
            return None


    def execute_exit(self, position: Position, price_data_point, prior_dt):
        position.exit_signal_given = True
        if price_data_point['high'] > self.__price:
            capital = position.exit_market(
                # TODO: Should I pass self.__created_dt here instead of prior_dt? Does it break
                # current datetime checks?
                self.__price, prior_dt, price_data_point['date']
            )
            return capital
        else:
            self.__duration += 1
            position.current_dt = price_data_point['date']
            if self.active == False:
                # TODO: Does this need to be set to False here?
                position.entry_signal_given = False
                position.exit_signal_given = False
                if self.__duration >= self.__max_duration:
                    # TODO: Does this work without defining a setter in super class?
                    self.active = False
            # TODO: How to handle this return?
            return None
