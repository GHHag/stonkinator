from abc import ABCMeta, abstractmethod

from trading.data.metadata.trading_system_attributes import classproperty


class TradingSystemBase(metaclass=ABCMeta):

    __metaclass__ = ABCMeta

    @classproperty
    @abstractmethod
    def name(cls):
        ...

    @staticmethod
    @abstractmethod
    def entry_signal_logic():
        ...

    @staticmethod
    @abstractmethod
    def exit_signal_logic():
        ...

    @staticmethod
    @abstractmethod
    def preprocess_data():
        ...

    @classmethod
    @abstractmethod
    def get_ts_properties(cls):
        ...


class MLTradingSystemBase(TradingSystemBase):

    @staticmethod
    @abstractmethod
    def create_backtest_models():
        ...

    @staticmethod
    @abstractmethod
    def create_inference_models():
        ...

    @classmethod
    @abstractmethod
    def make_predictions(cls):
        ...
