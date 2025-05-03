from abc import ABCMeta, abstractmethod


class IInstrumentSelector(metaclass=ABCMeta):

    __metaclass__ = ABCMeta

    @property
    @abstractmethod
    def selected_instruments(self) -> list:
        raise NotImplementedError("should contain a 'selected_instruments' property")

    @abstractmethod
    def __call__(self):
        raise NotImplementedError("should implement '__call__()'")