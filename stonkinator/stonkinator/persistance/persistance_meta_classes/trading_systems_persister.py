from abc import ABCMeta, abstractmethod

from persistance.persistance_meta_classes.signals_persister import SignalsPersisterBase


class TradingSystemsPersisterBase(SignalsPersisterBase):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_system_metrics(self):
        ...

    @abstractmethod
    def update_current_datetime(self):
        ...

    @abstractmethod
    def get_current_datetime(self):
        ...

    @abstractmethod
    def insert_position_list(self):
        ...

    @abstractmethod
    def insert_position(self):
        ...

    @abstractmethod
    def get_position_list(self):
        ...

    @abstractmethod
    def insert_single_symbol_position_list(self):
        ...

    @abstractmethod
    def insert_single_symbol_position(self):
        ...

    @abstractmethod
    def get_single_symbol_position_list(self):
        ...

    @abstractmethod
    def get_single_symbol_latest_position(self):
        ...

    @abstractmethod
    def insert_current_position(self):
        ...

    @abstractmethod
    def get_current_position(self):
        ...

    @abstractmethod
    def increment_num_of_periods(self):
        ...

    @abstractmethod
    def insert_ml_model(self):
        ...

    @abstractmethod
    def get_ml_model(self):
        ...