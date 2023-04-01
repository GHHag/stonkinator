from dataclasses import dataclass
from typing import Dict, Tuple, Callable

from tet_trading_systems.trading_system_state_handler.trad_trading_system_state_handler \
    import TradingSystemStateHandler
from tet_trading_systems.trading_system_state_handler.portfolio.portfolio import Portfolio
from tet_trading_systems.trading_system_management.position_sizer.position_sizer import IPositionSizer


@dataclass(frozen=True)
class TradingSystemProperties:
    system_name: str
    required_runs: int
    
    preprocess_data_function: Callable
    preprocess_data_args: Tuple
    
    system_handler_function: Callable
    
    system_state_handler: TradingSystemStateHandler
    system_state_handler_args: Tuple
    system_state_handler_call_args: Tuple
    system_state_handler_call_kwargs: Dict
    
    portfolio: Portfolio
    portfolio_args: Tuple
    portfolio_call_args: Tuple

    position_sizer: IPositionSizer
    position_sizer_args: Tuple
    position_sizer_call_args: Tuple
    position_sizer_call_kwargs: Dict
