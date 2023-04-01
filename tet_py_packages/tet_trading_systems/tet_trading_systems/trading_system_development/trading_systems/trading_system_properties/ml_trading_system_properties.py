from dataclasses import dataclass
from typing import List

from tet_trading_systems.trading_system_development.trading_systems.trading_system_properties.trading_system_properties \
    import TradingSystemProperties


@dataclass(frozen=True)
class MlTradingSystemProperties(TradingSystemProperties):
    system_instruments_list: List[str]
