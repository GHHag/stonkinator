import json

from pymongo.collection import Collection
from bson import json_util

from tet_doc_db.doc_database_meta_classes.tet_portfolio_doc_db import ITetPortfolioDocumentDatabase
from tet_doc_db.tet_mongo_db.systems_mongo_db import TetSystemsMongoDb


class TetPortfolioMongoDb(ITetPortfolioDocumentDatabase, TetSystemsMongoDb):

    __TARGET_PORTFOLIO_POSITIONS_FIELD = 'target_portfolio_positions'

    def __init__(self, client_uri, client_name):
        super().__init__(client_uri, client_name)
        self.__portfolios: Collection = self.client.portfolios

    def insert_portfolio(self, system_name, portfolio_creation_data):
        system_id = self._get_system_id(system_name) 
        self.__portfolios.insert_one(
            {
                self.__SYSTEM_ID_FIELD: system_id,
                self.__SYSTEM_NAME_FIELD: system_name, 
                self.__PORTFOLIO_CREATION_DATA_FIELD: portfolio_creation_data
            }
        )

    def _get_portfolio_id(self, system_name):
        query = self.__portfolios.find_one(
            {self.__SYSTEM_NAME_FIELD: system_name},
            {self.__ID_FIELD: 1}
        )
        return query[self.__ID_FIELD] if query else None

    def get_portfolio(self, system_name):
        portfolio_id = self._get_portfolio_id(system_name)
        system_id = self._get_system_id(system_name)
        query = self.__portfolios.find_one(
            {self.__ID_FIELD: portfolio_id, self.__SYSTEM_ID_FIELD: system_id}
        )
        if not portfolio_id or not query:
            return None
        else:
            return json.dumps(query, default=json_util.default)

    def update_portfolio(self, system_name, target_portfolio_positions):
        portfolio_id = self._get_portfolio_id(system_name)
        system_id = self._get_system_id(system_name)
        result = self.__portfolios.update_one(
            {self.__ID_FIELD: portfolio_id, self.__SYSTEM_ID_FIELD: system_id},
            {
                '$set': {
                    self.__TARGET_PORTFOLIO_POSITIONS_FIELD: target_portfolio_positions
                } 
            }, upsert=True
        )
        return result.modified_count > 0

    def get_portfolio_max_positions(self, system_name):
        portfolio_id = self._get_portfolio_id(system_name)
        system_id = self._get_system_id(system_name)
        query = self.__portfolios.find_one(
            {self.__ID_FIELD: portfolio_id, self.__SYSTEM_ID_FIELD: system_id},
            {self.__PORTFOLIO_CREATION_DATA_FIELD: 1}
        )
        return query[self.__PORTFOLIO_CREATION_DATA_FIELD]['max_positions']

    def get_target_portfolio_positions(self, system_name):
        portfolio_id = self._get_portfolio_id(system_name)
        system_id = self._get_system_id(system_name)
        query = self.__portfolios.aggregate(
            [
                {
                    '$match': {
                        self.__ID_FIELD: portfolio_id, self.__SYSTEM_ID_FIELD: system_id
                    }
                },
                {
                    '$lookup': {
                        'from': 'market_states',
                        'localField': self.__TARGET_PORTFOLIO_POSITIONS_FIELD,
                        'foreignField': self.__ID_FIELD,
                        'as': 'positions_market_state'
                    }
                },
                {
                    '$project': {
                        'positions_market_state': 1
                    }
                }
            ]
        )
        # kan man göra aendring i aggregate för att begraensa haemtning av dokument till 1?
        # och då göra return på renare vis utan index och 'positions_market_state' key specificerad
        return json.dumps(list(query)[0]['positions_market_state'], default=json_util.default)


if __name__ == '__main__':
    pass
