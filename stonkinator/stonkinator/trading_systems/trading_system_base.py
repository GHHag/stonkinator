from abc import ABCMeta, abstractmethod

import pandas as pd

from trading.data.metadata.trading_system_attributes import classproperty
from trading.position.order import Order

from persistance.persistance_meta_classes.securities_service import SecuritiesServiceBase
from persistance.persistance_meta_classes.trading_systems_persister import TradingSystemsPersisterBase

from trading_systems.model_creation.model_creation import SKModel
from trading_systems.trading_system_properties import TradingSystemProperties


class TradingSystemBase(metaclass=ABCMeta):

    __metaclass__ = ABCMeta

    @classproperty
    @abstractmethod
    def name(cls) -> str:
        ...

    @classproperty
    @abstractmethod
    def minimum_rows(cls) -> int:
        ...

    @classproperty
    @abstractmethod
    def entry_args(cls) -> dict:
        ...

    @classproperty
    @abstractmethod
    def exit_args(cls) -> dict:
        ...

    @staticmethod
    @abstractmethod
    def entry_signal_logic() -> Order | None:
        ...

    @staticmethod
    @abstractmethod
    def exit_signal_logic() -> Order | None:
        ...

    @staticmethod
    @abstractmethod
    def preprocess_data() -> tuple[dict[tuple[str, str], pd.DataFrame], None]:
        ...

    @staticmethod
    @abstractmethod
    def reprocess_data() -> tuple[dict[tuple[str, str], pd.DataFrame], None]:
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
    def target(cls) -> str:
        ...

    @classproperty
    @abstractmethod
    def target_period(cls) -> int:
        ...

    @staticmethod
    @abstractmethod
    def create_backtest_models() -> dict[tuple[str, str], pd.DataFrame] | pd.DataFrame:
        ...

    @staticmethod
    @abstractmethod
    def create_inference_models() -> dict[tuple[str, str], SKModel] | SKModel:
        ...

    @classmethod
    @abstractmethod
    def operate_models(
        cls,
        trading_system_id: str,
        trading_systems_persister: TradingSystemsPersisterBase, 
        data_dict: dict[tuple[str, str], pd.DataFrame],
        features: list[str],
        model_class: SKModel,
        params: dict
    ) -> dict[tuple[str, str], pd.DataFrame] | pd.DataFrame:
        ...

    @classmethod
    @abstractmethod
    def make_predictions(
        cls,
        trading_system_id: str,
        trading_systems_persister: TradingSystemsPersisterBase,
        data_dict: dict[tuple [str, str], pd.DataFrame],
        features: list[str]
    ) -> dict[tuple[str, str], pd.DataFrame]:
        ...

    @staticmethod
    @abstractmethod
    def preprocess_data() -> tuple[dict[tuple[str, str], pd.DataFrame], list[str] | pd.DataFrame]:
        ...

    @staticmethod
    @abstractmethod
    def reprocess_data() -> tuple[dict[tuple[str, str], pd.DataFrame], None]:
        ...