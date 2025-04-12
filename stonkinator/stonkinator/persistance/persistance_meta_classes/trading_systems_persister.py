from abc import ABCMeta, abstractmethod


class TradingSystemsPersisterBase(metaclass=ABCMeta):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_or_insert_trading_system(self):
        ...

    @abstractmethod
    def update_trading_system_metrics(self):
        ...

    @abstractmethod
    def upsert_market_state(self):
        ...

    @abstractmethod
    def get_market_states(self):
        ...

    @abstractmethod
    def update_current_date_time(self):
        ...

    @abstractmethod
    def get_current_date_time(self):
        ...

    @abstractmethod
    def upsert_order(self):
        ...

    @abstractmethod
    def get_order(self):
        ...

    @abstractmethod
    def upsert_position(self):
        ...

    @abstractmethod
    def insert_positions(self):
        ...

    @abstractmethod
    def get_position(self):
        ...

    @abstractmethod
    def get_positions(self):
        ...

    @abstractmethod
    def get_trading_system_positions(self):
        ...

    @abstractmethod
    def remove_trading_system_relations(self):
        ...

    @abstractmethod
    def insert_trading_system_model(self):
        ...

    @abstractmethod
    def get_trading_system_model(self):
        ...