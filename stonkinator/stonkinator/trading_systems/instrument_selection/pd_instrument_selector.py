import pandas as pd

from trading_systems.instrument_selection.instrument_selector import IInstrumentSelector
from trading.data.metadata.trading_system_attributes import TradingSystemAttributes


class PdInstrumentSelector(IInstrumentSelector):
    
    def __init__(
        self, selection_metric: str, instruments_data: pd.DataFrame,
        selection_threshold
    ):
        if not isinstance(instruments_data, pd.DataFrame) or \
            not selection_metric in instruments_data.columns:
            raise Exception(
                f'the provided pd.DataFrame is missing the given selection'  \
                f'metric column {selection_metric}'
            )

        self.__selection_metric: str = selection_metric
        self.__instruments_data: pd.DataFrame = instruments_data
        self.__selection_threshold = selection_threshold
        self.__selected_instruments: list[str] = []

    @property
    def selected_instruments(self) -> list[str]:
        return self.__selected_instruments
    
    def __call__(self):
        selected_instruments: pd.DataFrame = self.__instruments_data[
            self.__instruments_data[self.__selection_metric] >= self.__selection_threshold
        ]
        self.__selected_instruments = selected_instruments[TradingSystemAttributes.SYMBOL].to_list()