from abc import ABCMeta, abstractmethod

import pandas as pd
import numpy as np

from trading.data.metadata.trading_system_attributes import classproperty

from persistance.persistance_meta_classes.securities_service import SecuritiesServiceBase
from persistance.persistance_meta_classes.trading_systems_persister import TradingSystemsPersisterBase

from trading_systems.trading_system_properties import TradingSystemProperties


class TradingSystemBase(metaclass=ABCMeta):

    __metaclass__ = ABCMeta

    @classproperty
    @abstractmethod
    def name(cls):
        ...

    @classproperty
    @abstractmethod
    def entry_args(cls):
        ...

    @classproperty
    @abstractmethod
    def exit_args(cls):
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
    def get_properties(
        cls, securities_service: SecuritiesServiceBase
    ) -> TradingSystemProperties:
        ...


class MLTradingSystemBase(TradingSystemBase):

    @classproperty
    @abstractmethod
    def target(cls):
        ...

    @classproperty
    @abstractmethod
    def target_period(cls):
        ...

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
    def operate_models(
        cls, 
        trading_systems_persister: TradingSystemsPersisterBase,
        data_dict: dict[str, pd.DataFrame],
        target_period: int
    ) -> dict[str, pd.DataFrame]:
        ...

    @classmethod
    @abstractmethod
    def make_predictions(
        cls, 
        trading_systems_persister: TradingSystemsPersisterBase,
        data_dict: dict[str, pd.DataFrame],
        pred_features_data_dict: dict[str, np.ndarray]
    ) -> dict[str, pd.DataFrame]:
        ...
