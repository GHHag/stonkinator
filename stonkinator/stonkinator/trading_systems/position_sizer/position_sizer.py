from abc import ABC, abstractmethod

from trading.position.position import Position


class PositionSizer(ABC):

    @property
    @abstractmethod
    def position_size_metric_str(self):
        raise NotImplementedError("should contain a 'position_size_metric_str' property")

    @property
    @abstractmethod
    def position_sizer_data_dict(self) -> dict:
        raise NotImplementedError("should contain a 'position_sizer_data_dict' property")
    
    @abstractmethod
    def get_position_sizer_data_dict(self) -> dict:
        raise NotImplementedError("should implement 'get_position_sizer_data_dict()'")
    
    @abstractmethod
    def __call__(self, position_list: list[Position], num_of_periods: int, **kwargs: dict):
        raise NotImplementedError('should implement __call__()')
