from abc import ABCMeta, abstractmethod


class SignalsPersisterBase(metaclass=ABCMeta):
    
    __metaclass__ = ABCMeta

    @abstractmethod
    def _insert_system(self):
        ...

    @abstractmethod
    def _get_system_id(self):
        ...

    @abstractmethod
    def get_systems(self):
        ...

    @abstractmethod
    def insert_system_metrics(self):
        ...

    @abstractmethod
    def insert_market_state_data(self):
        ...

    @abstractmethod
    def get_market_state_data(self):
        ...

    @abstractmethod
    def get_market_state_data_for_symbol(self):
        ...

    @abstractmethod
    def update_market_state_data(self):
        ...

    @abstractmethod
    def insert_current_order(self):
        ...

    @abstractmethod
    def get_current_order(self):
        ...