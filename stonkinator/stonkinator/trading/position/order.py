from abc import ABCMeta, abstractmethod
from datetime import datetime
from decimal import Decimal

from trading.position.position import Position
from trading.data.metadata.price import Price
from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.trading_system_attributes import TradingSystemAttributes


class OrderBase(metaclass=ABCMeta):
    
    __metaclass__ = ABCMeta

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
    def direction(self):
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


class Order(OrderBase):

    def __init__(self, action, dt, direction):
        if action == MarketState.ENTRY.value and direction is None:
            raise ValueError(
                'value of direction can not be None '
                'if the given action is entry'
            )
        self.__action: str = action
        self.__created_dt: datetime = dt
        self.__active: bool = True
        self.__direction: str = direction

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
    def direction(self):
        return self.__direction

    @property
    def as_dict(self):
        order_dict = {
            'type': type(self).__name__,
            'action': self.__action,
            'created_dt': self.__created_dt,
            'active': self.__active
        }
        if self.direction:
            order_dict['direction'] = self.direction
        return order_dict

    def execute_entry(
        self, capital, price_data_point, data_point_dt, 
        fixed_position_size=True, commission_pct_cost=0.0
    ) -> Position:
        position = Position(
            capital, self.__direction,
            fixed_position_size=fixed_position_size, 
            commission_pct_cost=commission_pct_cost
        )
        position.enter_market(price_data_point[Price.OPEN], data_point_dt)
        self.__active = False
        return position

    def execute_exit(
        self, position: Position, price_data_point, data_point_dt
    ) -> Decimal:
        capital = position.exit_market(price_data_point[Price.OPEN], data_point_dt)
        self.__active = False
        return capital


class MarketOrder(Order):

    def __init__(self, action, dt, direction=None):
        super().__init__(action, dt, direction)


class LimitOrder(Order):

    def __init__(
        self, action, dt, price, max_duration,
        direction=None
    ):
        super().__init__(action, dt, direction)
        self.__price: float = price
        self.__max_duration: int = max_duration
        self.__duration: int = 0

    @property
    def as_dict(self):
        order_dict = {
            'type': type(self).__name__,
            'action': self.action,
            'created_dt': self.created_dt,
            'active': self.active,
            'price': self.__price,
            'max_duration': self.__max_duration,
            'duration': self.__duration
        }
        if self.direction:
            order_dict['direction'] = self.direction
        return order_dict

    def execute_entry(
        self, capital, price_data_point, data_point_dt,
        fixed_position_size=True, commission_pct_cost=0.0
    ) -> Position | None:
        if (
            self.direction == TradingSystemAttributes.LONG and
            self.__price > price_data_point[Price.LOW] or
            self.direction == TradingSystemAttributes.SHORT and
            self.__price < price_data_point[Price.HIGH]
        ):
            position = Position(
                capital, self.direction,
                fixed_position_size=fixed_position_size, 
                commission_pct_cost=commission_pct_cost
            )
            
            if (
                self.direction == TradingSystemAttributes.LONG and
                self.__price > price_data_point[Price.OPEN] or
                self.direction == TradingSystemAttributes.SHORT and
                self.__price < price_data_point[Price.OPEN]
            ):
                self.__price = price_data_point[Price.OPEN]

            position.enter_market(self.__price, data_point_dt)
            self.active = False
            return position
        else:
            self.__duration += 1
            if self.__duration >= self.__max_duration:
                self.active = False
            return None

    def execute_exit(
        self, position: Position, price_data_point, data_point_dt
    ) -> Decimal | None:
        position.exit_signal_given = True
        if (
            position.direction == TradingSystemAttributes.LONG and
            price_data_point[Price.HIGH] > self.__price or
            position.direction == TradingSystemAttributes.SHORT and 
            price_data_point[Price.LOW] < self.__price
        ):
            if (
                position.direction == TradingSystemAttributes.LONG and
                price_data_point[Price.OPEN] > self.__price or
                position.direction == TradingSystemAttributes.SHORT and
                price_data_point[Price.OPEN] < self.__price
            ):
                self.__price = price_data_point[Price.OPEN] 

            capital = position.exit_market(self.__price, data_point_dt)
            self.active = False
            return capital
        else:
            self.__duration += 1
            if self.__duration >= self.__max_duration:
                self.active = False
                position.exit_signal_given = False
            return None
