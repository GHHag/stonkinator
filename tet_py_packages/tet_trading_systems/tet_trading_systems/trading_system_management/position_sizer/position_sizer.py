from abc import ABC, abstractmethod
from typing import Dict, List

from TETrading.position.position import Position


class IPositionSizer(ABC):

    @property
    @abstractmethod
    def position_size_metric_str(self):
        raise NotImplementedError("Should contain a 'position_size_metric_str' property.")

    @property
    @abstractmethod
    def position_sizer_data_dict(self) -> Dict:
        raise NotImplementedError("Should contain a 'position_sizer_data_dict' property.")
    
    @abstractmethod
    def get_position_sizer_data_dict(self) -> Dict:
        raise NotImplementedError("Should implement 'get_position_sizer_data_dict()'")
    
    @abstractmethod
    def __call__(self, position_list: List[Position], num_of_periods: int, **kwargs: Dict):
        raise NotImplementedError('Should implement __call__()')
