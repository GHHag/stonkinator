from abc import ABCMeta, abstractmethod
from datetime import datetime

from trading.position.position import Position

class BaseOrder(metaclass=ABCMeta):
    
    __metaclass__ = ABCMeta

    # TODO: Might not be needed for Order base class and MarketOrder
    # @property
    # @abstractmethod
    # def price(self):
    #     ...

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
    def filled_dt(self):
        ...

    @filled_dt.setter
    @abstractmethod
    def filled_dt(self, value):
        ...

    @property
    @abstractmethod
    def as_dict(self):
        ...

    @abstractmethod
    def execute_entry(self) -> Position:
        ...

    @abstractmethod
    def execute_exit(self):
        ...


class Order(BaseOrder):
    # __price: float
    __direction: str
    __action: str
    __created_dt: datetime
    __filled_dt: datetime

    def __init__(self, direction, action, dt):
        self.__direction = direction
        self.__action = action
        self.__created_dt = dt

    # TODO: Might not be needed for Order base class and MarketOrder
    # @property
    # def price(self):
    #     return self.__price

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
    def filled_dt(self):
        return self.__filled_dt

    @filled_dt.setter
    def filled_dt(self, value):
        self.__filled_dt = value

    @property
    def as_dict(self):
        return {
            # 'price': self.__price,
            'direction': self.__direction,
            'action': self.__action,
            'created_dt': self.__created_dt,
            'filled_dt': self.__filled_dt
        }

    # TODO: Define comission_pct_cost somewhere else? Maybe fixed_pos_size too
    def execute_entry(
        self, capital, open_price, dt, fixed_position_size, commission_pct_cost
    ) -> Position:
        position = Position(
            capital, self.__direction,
            entry_signal_dt=self.__created_dt,
            fixed_position_size=fixed_position_size, 
            commission_pct_cost=commission_pct_cost
        )
        # TODO: Is this field needed? Set it in Position instead?
        self.__filled_dt = dt 
        position.enter_market(open_price, dt)
        return position

    def execute_exit(self, position: Position, open_price, prior_dt, dt):
        capital = position.exit_market(open_price, prior_dt, dt)
        return capital


class MarketOrder(Order):

    def __init__(self, direction, action, dt):
        super().__init__(direction, action, dt)


class LimitOrder(Order):
    __price: float
    __duration: int

    def __init__(self, direction, action, dt, price, duration):
        super().__init__(direction, action, dt)
        self.__price = price
        self.__duration = duration

    @property
    def price(self):
        return self.__price

    @property
    def duration(self):
        return self.__duration

    @property
    def as_dict(self):
        return {
            'direction': self.direction,
            'action': self.action,
            'created_dt': self.created_dt,
            'filled_dt': self.filled_dt,
            'price': self.__price,
            'duration': self.__duration
        }

    # TODO: Define comission_pct_cost somewhere else? Maybe fixed_pos_size too
    def execute_entry(
        # TODO: Open price not needed here
        self, capital, open_price, dt, fixed_position_size, commission_pct_cost
    ) -> Position:
        position = Position(
            capital, self.direction,
            entry_signal_dt=self.created_dt,
            fixed_position_size=fixed_position_size, 
            commission_pct_cost=commission_pct_cost
        )
        # TODO: Is this field needed? Set it in Position instead?
        self.filled_dt = dt 
        # TODO: Perform checks if limit order would have been filled before calling enter_market
        position.enter_market(self.__price, dt)
        return position

    def execute_exit(self, position: Position, open_price, prior_dt, dt):
        capital = position.exit_market(open_price, prior_dt, dt)
        return capital
