from abc import ABCMeta, abstractmethod
from datetime import datetime
from decimal import Decimal

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
        self, capital, price_data_point,
        fixed_position_size=True, commission_pct_cost=0.0
    ) -> Position:
        position = Position(
            capital, self.__direction, self.__created_dt,
            fixed_position_size=fixed_position_size, 
            commission_pct_cost=commission_pct_cost
        )
        position.enter_market(price_data_point['open'], price_data_point['date'])
        self.__active = False
        return position

    def execute_exit(self, position: Position, price_data_point) -> Decimal:
        capital = position.exit_market(
            price_data_point['open'], self.__created_dt, price_data_point['date']
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
        self, capital, price_data_point,
        fixed_position_size=True, commission_pct_cost=0.0
    ) -> Position | None:
        # TODO: This condition will not work for short positions
        if self.__price > price_data_point['low']:
            position = Position(
                capital, self.direction, self.created_dt,
                fixed_position_size=fixed_position_size, 
                commission_pct_cost=commission_pct_cost
            )
            position.enter_market(self.__price, price_data_point['date'])
            self.active = False
            return position
        else:
            self.__duration += 1
            if self.__duration >= self.__max_duration:
                self.active = False
            return None


    def execute_exit(self, position: Position, price_data_point) -> Decimal | None:
        position.exit_signal_given = True
        # TODO: This condition will not work for short positions
        if price_data_point['high'] > self.__price:
            capital = position.exit_market(
                self.__price, self.created_dt, price_data_point['date']
            )
            self.active = False
            return capital
        else:
            self.__duration += 1
            if self.__duration >= self.__max_duration:
                self.active = False
                position.exit_signal_given = False
            return None