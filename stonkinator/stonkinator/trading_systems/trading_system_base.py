from abc import ABCMeta, abstractmethod

import pandas as pd
import numpy as np

from persistance.persistance_meta_classes.trading_systems_persister import TradingSystemsPersisterBase

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
    def get_properties(cls):
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
    def operate_models(
        cls, 
        systems_db: TradingSystemsPersisterBase,
        data_dict: dict[str, pd.DataFrame],
        target_period: int
    ) -> dict[str, pd.DataFrame]:
        ...

    @classmethod
    @abstractmethod
    def make_predictions(
        cls, 
        systems_db: TradingSystemsPersisterBase,
        data_dict: dict[str, pd.DataFrame],
        pred_features_data_dict: dict[str, np.ndarray]
    ) -> dict[str, pd.DataFrame]:
        ...
