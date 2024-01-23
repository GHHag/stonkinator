from dataclasses import dataclass

from tet_trading_systems.trading_system_state_handler.portfolio.portfolio import Portfolio
from tet_trading_systems.trading_system_management.position_sizer.position_sizer import IPositionSizer


@dataclass(frozen=True)
class TradingSystemProperties:
    system_name: str
    required_runs: int
    system_instruments_list: list[str]
    
    preprocess_data_function: callable
    preprocess_data_args: tuple
    
    entry_logic_function: callable
    exit_logic_function: callable
    entry_function_args: dict
    exit_function_args: dict
    
    portfolio: Portfolio
    portfolio_args: tuple
    portfolio_call_args: tuple

    position_sizer: IPositionSizer
    position_sizer_args: tuple
    position_sizer_call_args: tuple
    position_sizer_call_kwargs: dict