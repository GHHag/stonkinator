import json
import datetime as dt

from pymongo.mongo_client import MongoClient

from tet_doc_db.doc_database_meta_classes.time_series_doc_db import ITimeSeriesDocumentDatabase


class TimeSeriesMongoDb(ITimeSeriesDocumentDatabase):

    __ID_FIELD = '_id'

    def __init__(self, client_uri, client_name):
        mongo_client = MongoClient(client_uri)
        self.__client = mongo_client[client_name]

    def get_time_series_collection_list(self):
        return json.dumps(
            [x for x in self.__client.list_collection_names() if not 'system.' in x]
        )

    def insert_pandas_time_series_data(
        self, collection_name, time_series_data, 
        time_field='timestamp', timestamp_key='Date'
    ):
        time_series_data = json.loads(time_series_data)
        self.__client[collection_name].drop()
        self.__client.create_collection(collection_name, timeseries={'timeField': time_field})
        for row in time_series_data['data']:
            row[time_field] = dt.datetime.fromisoformat(row[timestamp_key][:-1])
            del row[timestamp_key]
            self.__client[collection_name].insert_one(row)

    def get_time_series_data(self, collection_name, start_dt=None, end_dt=None):
        if not start_dt or not end_dt:
            return json.dumps(
                list(self.__client[collection_name].find({}, {self.__ID_FIELD: 0})), default=str
            )
        else:
            return json.dumps(
                list(
                    self.__client[collection_name].find(
                        {'timestamp': {'$gte': start_dt, '$lte': end_dt}},
                        {self.__ID_FIELD: 0}
                    )
                ), default=str
            )


if __name__ == '__main__':
    pass
