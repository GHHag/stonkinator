import pickle

from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection

from TETrading.data.metadata.trading_system_attributes import TradingSystemAttributes


class TetMlModelsDb:

    __ID_FIELD = TradingSystemAttributes.ID
    __SYSTEM_ID_FIELD = TradingSystemAttributes.SYSTEM_ID
    __NAME_FIELD = TradingSystemAttributes.NAME
    __SYSTEM_NAME_FIELD = TradingSystemAttributes.SYSTEM_NAME
    __ML_MODEL_FIELD = TradingSystemAttributes.ML_MODEL
    __INSTRUMENT_FIELD = TradingSystemAttributes.INSTRUMENT

    def __init__(self, client_uri, client_name):
        mongo_client = MongoClient(client_uri)
        self.__client = mongo_client[client_name]
        self.__systems: Collection = self.__client.systems
        self.__ml_models: Collection = self.__client.ml_models
    
    def _insert_system(self, system_name):
        self.__systems.insert_one({self.__NAME_FIELD: system_name})
    
    def _get_system_id(self, system_name):
        query = self.__systems.find_one(
            {self.__NAME_FIELD: system_name},
            {self.__ID_FIELD: 1}
        )
        return query[self.__ID_FIELD] if query else None

    def insert_ml_model(self, system_name, instrument, model):
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        self.__ml_models.update_one(
            {
                self.__SYSTEM_ID_FIELD: system_id, 
                self.__SYSTEM_NAME_FIELD: system_name, 
                self.__INSTRUMENT_FIELD: instrument
            },
            {'$set': {self.__ML_MODEL_FIELD: model}}, upsert=True
        )
        return True

    def get_ml_model(self, system_name, instrument):
        system_id = self._get_system_id(system_name)
        query = self.__ml_models.find_one(
            {
                self.__SYSTEM_ID_FIELD: system_id, 
                self.__SYSTEM_NAME_FIELD: system_name, 
                self.__INSTRUMENT_FIELD: instrument
            }, 
            {self.__ID_FIELD: 0, self.__ML_MODEL_FIELD: 1}
        )
        return pickle.loads(query[self.__ML_MODEL_FIELD])
