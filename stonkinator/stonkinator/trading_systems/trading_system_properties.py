from dataclasses import dataclass

from trading_systems.position_sizer.position_sizer import PositionSizer
from trading_systems.model_creation.model_creation import SKModel


@dataclass(frozen=True)
class TradingSystemProperties:
    required_runs: int
    instruments_list: list[str]
    
    preprocess_data_args: tuple
    
    entry_function_args: dict
    exit_function_args: dict

    ts_run_kwargs: dict

    position_sizer: PositionSizer
    position_sizer_call_args: tuple
    position_sizer_call_kwargs: dict


@dataclass(frozen=True)
class MLTradingSystemProperties(TradingSystemProperties):
    model_class: SKModel
    param_grid: dict