from abc import ABCMeta, abstractmethod

class IOrderExecutionHandler(metaclass=ABCMeta):
    
    __metaclass__ = ABCMeta

    @abstractmethod
    def _queue_orders(self):
        ...

    @abstractmethod
    def place_orders(self):
        ...
