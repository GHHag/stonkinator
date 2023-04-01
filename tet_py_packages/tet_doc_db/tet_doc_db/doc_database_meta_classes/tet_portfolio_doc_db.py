from abc import ABCMeta, abstractmethod


class ITetPortfolioDocumentDatabase(metaclass=ABCMeta):

    __metaclass__ = ABCMeta

    @abstractmethod
    def _get_portfolio_id(self):
        ...

    @abstractmethod
    def insert_portfolio(self):
        ...

    @abstractmethod
    def get_portfolio(self):
        ...

    @abstractmethod
    def update_portfolio(self):
        ...

    @abstractmethod
    def get_portfolio_max_positions(self):
        ...

    @abstractmethod
    def get_target_portfolio_positions(self):
        ...
