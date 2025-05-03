from abc import ABCMeta, abstractmethod
from decimal import Decimal

from pandas import Timestamp

from trading.position.position import Position
from trading.data.metadata.price import Price
from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.trading_system_attributes import (
    TradingSystemAttributes, classproperty
)


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
    def order_properties(self) -> dict:
        ...

    @property
    @abstractmethod
    def as_dict(self) -> dict:
        ...

    @abstractmethod
    def execute_entry(self) -> Position | None:
        ...

    @abstractmethod
    def execute_exit(self) -> Position | None:
        ...


class Order(OrderBase):

    def __init__(self, action: MarketState, created_dt: Timestamp, direction, active):
        if action == MarketState.ENTRY and direction is None:
            raise ValueError(
                'value of direction can not be None if the given action is entry'
            )
        self.__action = action
        self.__created_dt = created_dt
        self.__active = active
        self.__direction = direction

    @classproperty
    def order_type(cls):
        return cls.__name__

    @property
    def action(self) -> MarketState:
        return self.__action

    @property
    def created_dt(self) -> Timestamp:
        return self.__created_dt

    @property
    def direction(self):
        return self.__direction

    @property
    def active(self):
        return self.__active

    @active.setter
    def active(self, value):
        self.__active = value

    @property
    def order_properties(self) -> dict:
        return {}

    @property
    def as_dict(self) -> dict:
        order_dict = {
            'type': self.order_type,
            'action': self.__action.value,
            'created_dt': self.__created_dt,
            'active': self.__active
        }
        if self.__direction:
            order_dict['direction'] = self.__direction
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

    @staticmethod
    def from_proto(order_proto):
        if order_proto is None:
            return

        direction = None
        match order_proto.action: 
            case MarketState.ENTRY.value:
                market_state_action = MarketState.ENTRY
                direction =  (
                    TradingSystemAttributes.LONG
                    if order_proto.direction_long is True
                    else TradingSystemAttributes.SHORT
                )
            case MarketState.ACTIVE.value:
                market_state_action = MarketState.ACTIVE
            case MarketState.EXIT.value:
                market_state_action = MarketState.EXIT

        if order_proto.order_type == MarketOrder.order_type:
            return MarketOrder(
                market_state_action,
                Timestamp(order_proto.created_date_time.date_time),
                direction=direction,
                active=order_proto.active,
            )
        elif order_proto.order_type == LimitOrder.order_type:
            return LimitOrder(
                market_state_action,
                Timestamp(order_proto.created_date_time.date_time),
                order_proto.price,
                order_proto.max_duration, 
                direction=direction,
                active=order_proto.active,
                duration=order_proto.duration,
            )

class MarketOrder(Order):

    def __init__(
        self, action: MarketState, created_dt: Timestamp,
        direction=None, active=True
    ):
        super().__init__(action, created_dt, direction, active)


class LimitOrder(Order):

    def __init__(
        self, action: MarketState, created_dt: Timestamp, price, max_duration,
        direction=None, active=True, duration=0
    ):
        super().__init__(action, created_dt, direction, active)
        self.__price = price
        self.__max_duration = max_duration
        self.__duration = duration

    @property
    def order_properties(self) -> dict:
        return {
            "price": self.__price,
            "max_duration": self.__max_duration,
            "duration": self.__duration
        }

    @property
    def as_dict(self) -> dict:
        order_dict = {
            'type': self.order_type,
            'action': self.action.value,
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
