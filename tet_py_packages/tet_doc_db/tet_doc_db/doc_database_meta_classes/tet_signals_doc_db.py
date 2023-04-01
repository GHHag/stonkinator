from abc import ABCMeta, abstractmethod


class ITetSignalsDocumentDatabase(metaclass=ABCMeta):
    
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