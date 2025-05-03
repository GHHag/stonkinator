from abc import ABCMeta, abstractmethod


class SecuritiesServiceBase(metaclass=ABCMeta):

    __metaclass__ = ABCMeta

    @abstractmethod
    def insert_exchange(self):
        ...

    @abstractmethod
    def update_exchange(self):
        ...

    @abstractmethod
    def get_exchange(self):
        ...
        
    @abstractmethod
    def get_exchanges(self):
        ...
        
    @abstractmethod
    def insert_instrument(self):
        ...

    @abstractmethod
    def update_instrument(self):
        ...

    @abstractmethod
    def get_instrument(self):
        ...

    @abstractmethod
    def remove_instrument(self):
        ...

    @abstractmethod
    def get_date_time(self):
        ...

    @abstractmethod
    def get_last_date(self):
        ...

    @abstractmethod
    def insert_price(self):
        ...

    @abstractmethod
    def insert_price_data(self):
        ...

    @abstractmethod
    def get_price_data(self):
        ...

    @abstractmethod
    def insert_market_list(self):
        ...

    @abstractmethod
    def update_market_list(self):
        ...

    @abstractmethod
    def get_exchange_instruments(self):
        ...

    @abstractmethod
    def insert_market_list_instrument(self):
        ...

    @abstractmethod
    def remove_market_list_instrument(self):
        ...

    @abstractmethod
    def get_market_list_instruments(self):
        ...

    @abstractmethod
    def get_market_list_instruments_price_data(self):
        ...