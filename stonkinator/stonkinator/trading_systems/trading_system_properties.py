from dataclasses import dataclass

from trading_systems.position_sizer.position_sizer import IPositionSizer


@dataclass(frozen=True)
class TradingSystemProperties:
    name: str
    required_runs: int
    instruments_list: list[str]
    
    preprocess_data_function: callable
    preprocess_data_args: tuple
    
    entry_logic_function: callable
    exit_logic_function: callable
    entry_function_args: dict
    exit_function_args: dict

    position_sizer: IPositionSizer
    position_sizer_call_args: tuple
    position_sizer_call_kwargs: dict