from abc import ABCMeta, abstractmethod


class ITimeSeriesDocumentDatabase(metaclass=ABCMeta):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_time_series_collection_list(self):
        ...

    @abstractmethod
    def insert_pandas_time_series_data(self):
        ...

    @abstractmethod
    def get_time_series_data(self):
        ...
