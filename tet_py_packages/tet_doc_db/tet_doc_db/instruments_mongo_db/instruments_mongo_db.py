import json

from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection
from bson import json_util, objectid

from TETrading.data.metadata.trading_system_attributes import TradingSystemAttributes


class InstrumentsMongoDb:

    __MARKET_LISTS_COLLECTION = 'market_lists'
    __INSTRUMENTS_COLLECTION = 'instruments'

    __ID_FIELD = '_id'
    __MARKET_LIST_FIELD = 'market_list'
    __MARKET_LIST_IDS_FIELD = 'market_list_ids'
    __SYMBOL_FIELD = TradingSystemAttributes.SYMBOL 
    __SECTOR_FIELD = 'industry'

    def __init__(self, client_uri, client_name):
        mongo_client = MongoClient(client_uri)
        self.__client = mongo_client[client_name]
        self.__market_lists: Collection = self.__client[self.__MARKET_LISTS_COLLECTION]
        self.__instruments: Collection = self.__client[self.__INSTRUMENTS_COLLECTION]
    
    def get_market_list_id(self, market_list_name):
        query = self.__market_lists.find_one(
            {self.__MARKET_LIST_FIELD: market_list_name},
            {self.__ID_FIELD: 1}
        )
        return query[self.__ID_FIELD]

    def get_market_lists(self):
        return json.dumps(
            list(self.__market_lists.find()), default=json_util.default
        )

    def get_market_list_by_id(self, market_list_id):
        market_list_id = objectid.ObjectId(market_list_id)
        return json.dumps(
            self.__market_lists.find_one({self.__ID_FIELD: market_list_id}), 
            default=json_util.default
        )

    def get_market_list_by_name(self, market_list_name):
        return json.dumps(
            self.__market_lists.find_one({self.__MARKET_LIST_FIELD: market_list_name}),
            default=json_util.default
        )

    def get_market_list_instruments(self, market_list_id):
        query = self.__market_lists.aggregate(
            [
                {
                    '$match': {
                        self.__ID_FIELD: objectid.ObjectId(market_list_id)
                    }
                },
                {
                    '$lookup': {
                        'from': self.__INSTRUMENTS_COLLECTION,
                        'localField': self.__ID_FIELD,
                        'foreignField': self.__MARKET_LIST_IDS_FIELD,
                        'as': 'market_list_instruments'
                    }
                },
                {
                    '$project': {
                        f'market_list_instruments.{self.__ID_FIELD}': 1,
                        f'market_list_instruments.{self.__SYMBOL_FIELD}': 1
                    },
                }
            ]
        )
        return json.dumps(list(query)[0], default=json_util.default)

    def get_market_list_instrument_symbols(self, market_list_id):
        market_list_instruments = json.loads(
            self.get_market_list_instruments(market_list_id)
        )
        return json.dumps(
            [i[self.__SYMBOL_FIELD] for i in market_list_instruments['market_list_instruments']]
        )

    def get_sectors(self):
        query = self.__instruments.distinct(self.__SECTOR_FIELD)
        return json.dumps(query)

    def get_sector_instruments_for_market_lists(self, market_list_ids, sector):
        query = self.__instruments.aggregate(
            [
                {
                    '$match': { self.__SECTOR_FIELD: sector}
                },
                {
                    '$match': {
                        '$nor': [
                            {
                                self.__MARKET_LIST_IDS_FIELD: {
                                    '$nin': market_list_ids
                                }
                            }
                        ]
                    }
                },
                {
                    '$project': {
                        self.__ID_FIELD: 0,
                        self.__SYMBOL_FIELD: 1
                    }
                }
            ]
        )
        return json.dumps(list(query), default=json_util.default)


if __name__ == '__main__':
    pass
