from abc import ABCMeta, abstractmethod


class ITargetPortfolioCreator(metaclass=ABCMeta):

    __metaclass__ = ABCMeta

    @abstractmethod
    def _process_signals(self):
        ...

    @abstractmethod
    def _make_selection(self):
        ...

    @abstractmethod
    def _create_target_portfolio(self):
        ...
