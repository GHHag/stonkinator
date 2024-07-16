from abc import ABCMeta, abstractmethod

from persistance.doc_database_meta_classes.tet_signals_doc_db import ITetSignalsDocumentDatabase


class ITetSystemsDocumentDatabase(ITetSignalsDocumentDatabase):

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
    def get_market_state_data(self):
        ...

    @abstractmethod
    def get_market_state_data_for_symbol(self):
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
    def get_historic_data(self):
        ...

    @abstractmethod
    def get_single_symbol_historic_data(self):
        ...
