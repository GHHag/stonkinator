import json
import pickle

from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection
from bson import json_util

from tet_doc_db.doc_database_meta_classes.tet_systems_doc_db import ITetSystemsDocumentDatabase

from TETrading.data.metadata.market_state_enum import MarketState
from TETrading.data.metadata.trading_system_attributes import TradingSystemAttributes
from TETrading.position.position import Position
from TETrading.position.position_manager import PositionManager


class TetSystemsMongoDb(ITetSystemsDocumentDatabase):

    __ID_FIELD = TradingSystemAttributes.ID
    __SYSTEM_ID_FIELD = TradingSystemAttributes.SYSTEM_ID
    __NAME_FIELD = TradingSystemAttributes.NAME
    __SYSTEM_NAME_FIELD = TradingSystemAttributes.SYSTEM_NAME
    __SYMBOL_FIELD = TradingSystemAttributes.SYMBOL
    __METRICS_FIELD = TradingSystemAttributes.METRICS
    __MARKET_STATE_FIELD = TradingSystemAttributes.MARKET_STATE
    __NUMBER_OF_PERIODS_FIELD = TradingSystemAttributes.NUMBER_OF_PERIODS
    __POSITION_LIST_FIELD = TradingSystemAttributes.POSITION_LIST
    __CURRENT_POSITION_FIELD = TradingSystemAttributes.CURRENT_POSITION
    __ML_MODEL_FIELD = TradingSystemAttributes.ML_MODEL
    __INSTRUMENT_FIELD = TradingSystemAttributes.INSTRUMENT
    __SIGNAL_DT_FIELD = TradingSystemAttributes.SIGNAL_DT
    __START_DT = TradingSystemAttributes.START_DT
    __END_DT = TradingSystemAttributes.END_DT
    __MARKET_TO_MARKET_RETURNS = TradingSystemAttributes.MARKET_TO_MARKET_RETURNS
    __EQUITY_LIST = TradingSystemAttributes.EQUITY_LIST
    __RETURNS_LIST = TradingSystemAttributes.RETURNS_LIST
    __MAE_LIST = TradingSystemAttributes.MAE_LIST
    __MFE_LIST = TradingSystemAttributes.MFE_LIST
    __POSITION_PERIOD_LENGTHS_LIST = TradingSystemAttributes.POSITION_PERIOD_LENGTHS_LIST

    def __init__(self, client_uri, client_name):
        mongo_client = MongoClient(client_uri)
        self.__client = mongo_client[client_name]
        self.__systems: Collection = self.__client.systems
        self.__market_states: Collection = self.__client.market_states
        self.__positions: Collection = self.__client.positions
        self.__single_symbol_positions: Collection = self.__client.single_symbol_positions
        self.__ml_models: Collection = self.__client.ml_models

    @property
    def client(self):
        return self.__client

    def _insert_system(self, system_name):
        self.__systems.insert_one({self.__NAME_FIELD: system_name})

    def _get_system_id(self, system_name):
        query = self.__systems.find_one(
            {self.__NAME_FIELD: system_name},
            {self.__ID_FIELD: 1}
        )
        return query[self.__ID_FIELD] if query else None

    def get_systems(self):
        query = self.__systems.find(
            {}, 
            {self.__ID_FIELD: 1, self.__NAME_FIELD: 1}
        )
        return json.dumps(list(query), default=json_util.default)

    def insert_system_metrics(self, system_name, metrics: dict):
        system_id = self._get_system_id(system_name)
        if not system_id:
            return False
        else:
            result = self.__systems.update_one(
                {self.__ID_FIELD: system_id, self.__NAME_FIELD: system_name},
                {'$set': {self.__METRICS_FIELD: {k: v for k, v in metrics.items()}}}
            )
            return result.modified_count > 0

    def get_system_metrics(self, system_name):
        system_id = self._get_system_id(system_name)
        query = self.__systems.find_one(
            {self.__ID_FIELD: system_id, self.__NAME_FIELD: system_name},
            {
                self.__ID_FIELD: 1, self.__NAME_FIELD: 1, self.__METRICS_FIELD: 1, 
                self.__NUMBER_OF_PERIODS_FIELD: 1
            }
        )
        return json.dumps(query, default=json_util.default)

    def update_current_datetime(self, system_name, current_datetime):
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        result = self.__systems.update_one(
            {self.__ID_FIELD: system_id, self.__NAME_FIELD: system_name},
            {'$set': {'current_datetime': current_datetime}},
            upsert=True
        )
        return result.modified_count > 0

    def get_current_datetime(self, system_name):
        system_id = self._get_system_id(system_name)
        query = self.__systems.find_one(
            {self.__ID_FIELD: system_id, self.__NAME_FIELD: system_name},
            {'current_datetime': 1}
        )
        return query.get('current_datetime')

    def insert_market_state_data(self, system_name, data):
        data: dict[str, object] = json.loads(data)
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        ts_data = data.get('data')
        if not ts_data:
            return False
        for data_p in ts_data:
            assert isinstance(data_p, dict)
            data_p.update({self.__SYSTEM_ID_FIELD: system_id})
            result = self.__market_states.update_one(
                {
                    self.__SYSTEM_ID_FIELD: system_id, 
                    self.__SYMBOL_FIELD: data_p[TradingSystemAttributes.SYMBOL],
                    '$or': [
                        {
                            self.__SIGNAL_DT_FIELD: {'$lt': data_p.get(self.__SIGNAL_DT_FIELD)}
                        },
                        {
                            '$and': [
                                {self.__SIGNAL_DT_FIELD: {'$lte': data_p.get(self.__SIGNAL_DT_FIELD)}},
                                {self.__MARKET_STATE_FIELD: MarketState.NULL.value}
                            ]
                        },
                        {
                            '$and': [
                                {self.__SIGNAL_DT_FIELD: {'$eq': data_p.get(self.__SIGNAL_DT_FIELD)}},
                                {self.__MARKET_STATE_FIELD: MarketState.ACTIVE.value}
                            ]
                        }
                    ]
                },
                {'$set': data_p},
            )
            if result.modified_count < 1:
                existing_doc = self.__market_states.find_one(
                    {
                        self.__SYSTEM_ID_FIELD: system_id, 
                        self.__SYMBOL_FIELD: data_p[TradingSystemAttributes.SYMBOL]
                    }
                )
                if existing_doc is None:
                    self.__market_states.insert_one(data_p)
        return True

    def update_market_state_data(self, system_name, data):
        data: dict[str, object] = json.loads(data)
        system_id = self._get_system_id(system_name)
        if not system_id:
            return False
        ts_data = data.get('data')
        if not ts_data:
            return False
        successful_updates = 0
        for data_p in ts_data:
            assert isinstance(data_p, dict)
            result = self.__market_states.update_one(
                {
                    self.__SYSTEM_ID_FIELD: system_id, 
                    self.__SYMBOL_FIELD: data_p[TradingSystemAttributes.SYMBOL]
                },
                {'$set': data_p}
            )
            successful_updates += result.modified_count
        return successful_updates == len(ts_data)

    def get_market_state_data(self, system_name, market_state):
        system_id = self._get_system_id(system_name)
        query = self.__market_states.find(
            {self.__SYSTEM_ID_FIELD: system_id, self.__MARKET_STATE_FIELD: market_state}
        )
        return json.dumps(list(query), default=json_util.default)

    def get_market_state_data_for_symbol(self, system_name, symbol):
        system_id = self._get_system_id(system_name)
        query = self.__market_states.find_one(
            {self.__SYSTEM_ID_FIELD: system_id, self.__SYMBOL_FIELD: symbol}
        )
        if not query:
            return json.dumps({self.__MARKET_STATE_FIELD: None, self.__SIGNAL_DT_FIELD: None})
        else:
            return json.dumps(query, default=json_util.default)

    def insert_position_list(
        self, system_name, position_list: list[Position], num_of_periods,
        serialized_format=False, json_format=False
    ):
        system_id = self._get_system_id(system_name)
        if serialized_format:
            result = self.__positions.update_one(
                {self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name},
                {
                    '$set': {
                        self.__POSITION_LIST_FIELD: [pickle.dumps(pos) for pos in position_list],
                        self.__NUMBER_OF_PERIODS_FIELD: num_of_periods
                    }
                }, upsert=True
            )
            return result.modified_count > 0
        if json_format:
            result = self.__positions.update_one(
                {self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name},
                {
                    '$set': {
                        f'{self.__POSITION_LIST_FIELD}_json': [pos.as_dict for pos in position_list],
                        self.__NUMBER_OF_PERIODS_FIELD: num_of_periods
                    }
                }, upsert=True
            )
            return result.modified_count > 0

    def insert_position(
        self, system_name, position: Position,
        serialized_format=False, json_format=False
    ):
        system_id = self._get_system_id(system_name)
        if serialized_format:
            pop_result = self.__positions.update_one(
                {self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name},
                {
                    '$pop': {
                        self.__POSITION_LIST_FIELD: -1
                    }
                }
            )
            push_result = self.__positions.update_one(
                {self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name},
                {
                    '$push': {
                        self.__POSITION_LIST_FIELD: pickle.dumps(position)
                    }
                }, upsert=True
            )
            return pop_result.modified_count + push_result.modified_count >= 2
        if json_format:
            pop_result = self.__positions.update_one(
                {self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name},
                {
                    '$pop': {
                        f'{self.__POSITION_LIST_FIELD}_json': -1
                    }
                }
            )
            push_result = self.__positions.update_one(
                {self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name},
                {
                    '$push': {
                        f'{self.__POSITION_LIST_FIELD}_json': position.as_dict
                    },
                }, upsert=True
            )
            return pop_result.modified_count + push_result.modified_count >= 2

    def get_position_list(
        self, system_name, serialized_format=False, json_format=False, 
        return_num_of_periods=False
    ):
        system_id = self._get_system_id(system_name)
        if serialized_format:
            query = self.__positions.find_one(
                {self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name},
                {
                    self.__ID_FIELD: 0, self.__SYSTEM_ID_FIELD: 1, 
                    self.__POSITION_LIST_FIELD: 1, self.__NUMBER_OF_PERIODS_FIELD: 1
                }
            )
            if query is None:
                raise ValueError('no serialized Position objects found')
            if return_num_of_periods:
                return list(map(pickle.loads, query[self.__POSITION_LIST_FIELD])), \
                    query[self.__NUMBER_OF_PERIODS_FIELD]
            else:
                return list(map(pickle.loads, query[self.__POSITION_LIST_FIELD]))
        if json_format:
            query = self.__positions.find_one(
                {self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name},
                {
                    self.__ID_FIELD: 0, self.__SYSTEM_ID_FIELD: 1, 
                    f'{self.__POSITION_LIST_FIELD}_json': 1, self.__NUMBER_OF_PERIODS_FIELD: 1
                }
            )
            return json.dumps(query, default=json_util.default)

    def insert_single_symbol_position_list(
        self, system_name, symbol, position_list: list[Position], num_of_periods,
        serialized_format=False, json_format=False
    ):
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        if serialized_format:
            result = self.__single_symbol_positions.update_one(
                {
                    self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name, 
                    self.__SYMBOL_FIELD: symbol
                },
                {
                    '$set': {
                        self.__POSITION_LIST_FIELD: [pickle.dumps(pos) for pos in position_list],
                        self.__NUMBER_OF_PERIODS_FIELD: num_of_periods
                    }
                }, upsert=True
            )
            return result.modified_count > 0
        if json_format:
            result = self.__single_symbol_positions.update_one(
                {
                    self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name, 
                    self.__SYMBOL_FIELD: symbol
                },
                {
                    '$set': {
                        f'{self.__POSITION_LIST_FIELD}_json': [pos.as_dict for pos in position_list],
                        self.__NUMBER_OF_PERIODS_FIELD: num_of_periods
                    }
                }, upsert=True
            )
            return result.modified_count > 0

    def insert_single_symbol_position(
        self, system_name, symbol, position: Position, num_of_periods,
        serialized_format=False, json_format=False
    ):
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        if serialized_format:
            result = self.__single_symbol_positions.update_one(
                {
                    self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name,
                    self.__SYMBOL_FIELD: symbol
                },
                {
                    '$push': {
                        self.__POSITION_LIST_FIELD: pickle.dumps(position)
                    },
                    '$inc':{
                        self.__NUMBER_OF_PERIODS_FIELD: num_of_periods
                    }
                }, upsert=True
            )
            return result.modified_count > 0
        if json_format:
            result = self.__single_symbol_positions.update_one(
                {
                    self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name,
                    self.__SYMBOL_FIELD: symbol
                },
                {
                    '$push': {
                        f'{self.__POSITION_LIST_FIELD}_json': position.as_dict
                    },
                    '$inc': {
                        self.__NUMBER_OF_PERIODS_FIELD: num_of_periods
                    }
                }, upsert=True
            )
            return result.modified_count > 0

    def get_single_symbol_position_list(
        self, system_name, symbol, 
        serialized_format=False, json_format=False,
        return_num_of_periods=False
    ):
        system_id = self._get_system_id(system_name)
        if serialized_format:
            query = self.__single_symbol_positions.find_one(
                {
                    self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name, 
                    self.__SYMBOL_FIELD: symbol
                },
                {self.__ID_FIELD: 0, self.__POSITION_LIST_FIELD: 1, self.__NUMBER_OF_PERIODS_FIELD: 1}
            )
            if query is None:
                raise ValueError('no serialized Position objects found')
            if return_num_of_periods:
                return list(map(pickle.loads, query[self.__POSITION_LIST_FIELD])), \
                    query[self.__NUMBER_OF_PERIODS_FIELD]
            else:
                return list(map(pickle.loads, query[self.__POSITION_LIST_FIELD]))
        if json_format:
            query = self.__single_symbol_positions.find_one(
                {
                    self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name, 
                    self.__SYMBOL_FIELD: symbol
                },
                {f'{self.__POSITION_LIST_FIELD}_json': 1, self.__NUMBER_OF_PERIODS_FIELD: 1}
            )
            if return_num_of_periods:
                return json.dumps(query, default=json_util.default), \
                    query[self.__NUMBER_OF_PERIODS_FIELD]
            else:
                return json.dumps(query, default=json_util.default)

    def get_single_symbol_latest_position(self, system_name, symbol):
        system_id = self._get_system_id(system_name)
        query = self.__single_symbol_positions.find_one(
            {
                self.__SYSTEM_ID_FIELD: system_id, self.__SYSTEM_NAME_FIELD: system_name,
                self.__SYMBOL_FIELD: symbol
            },
            {self.__ID_FIELD: 0, self.__POSITION_LIST_FIELD: {'$slice': -1}}
        )
        if query is not None:
            return pickle.loads(query[self.__POSITION_LIST_FIELD][0])

    def insert_current_position(
        self, system_name, symbol, position
    ):
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        result = self.__single_symbol_positions.update_one(
            {
                self.__SYSTEM_ID_FIELD: system_id, 
                self.__SYSTEM_NAME_FIELD: system_name,
                self.__SYMBOL_FIELD: symbol
            },
            {
                '$set': {
                    self.__CURRENT_POSITION_FIELD: pickle.dumps(position)
                }
            }, upsert=True
        )
        return result.modified_count > 0

    def get_current_position(self, system_name, symbol):
        system_id = self._get_system_id(system_name)
        query = self.__single_symbol_positions.find_one(
            {
                self.__SYSTEM_ID_FIELD: system_id, 
                self.__SYSTEM_NAME_FIELD: system_name,
                self.__SYMBOL_FIELD: symbol,
                self.__CURRENT_POSITION_FIELD: {'$exists': True}
            },
            {
                self.__ID_FIELD: 0, 
                self.__SYSTEM_ID_FIELD: 1, 
                self.__SYMBOL_FIELD: 1, 
                self.__CURRENT_POSITION_FIELD: 1
            }
        )
        if query is not None:
            return pickle.loads(query[self.__CURRENT_POSITION_FIELD])

    def increment_num_of_periods(self, system_name, symbol, num_of_periods):
        system_id = self._get_system_id(system_name)
        if not system_id:
            self._insert_system(system_name)
            system_id = self._get_system_id(system_name)
        result = self.__single_symbol_positions.update_one(
            {
                self.__SYSTEM_ID_FIELD: system_id, 
                self.__SYSTEM_NAME_FIELD: system_name,
                self.__SYMBOL_FIELD: symbol
            },
            {
                '$inc':{
                    self.__NUMBER_OF_PERIODS_FIELD: num_of_periods
                }
            }
        )
        return result.modified_count > 0

    def get_historic_data(self, system_name):
        position_list = self.get_position_list(system_name)
        system_metrics = json.loads(self.get_system_metrics(system_name))

        start_dt = position_list[0].entry_dt
        end_dt = position_list[-1].exit_signal_dt if position_list[-1].exit_signal_dt is not None \
            else position_list[-2].exit_signal_dt

        position_manager = PositionManager(
            system_name, system_metrics[self.__NUMBER_OF_PERIODS_FIELD], 10000, 1.0
        )

        def generate_pos_sequence(position_list, **kwargs):
            for pos in position_list:
                yield pos

        if position_list[-1].active_position or position_list[-1].entry_dt is None:
            position_manager.generate_positions(generate_pos_sequence, position_list[:-1])
        else:
            position_manager.generate_positions(generate_pos_sequence, position_list[:])

        return json.dumps(
            {
                self.__START_DT: str(start_dt),
                self.__END_DT: str(end_dt),
                self.__MARKET_TO_MARKET_RETURNS: list(
                        map(float, position_manager.metrics.market_to_market_returns_list)
                    ),
                self.__EQUITY_LIST: list(map(float, position_manager.metrics.equity_list)),
                self.__RETURNS_LIST: list(position_manager.metrics.returns_list),
                self.__MAE_LIST: list(position_manager.metrics.mae_list),
                self.__MFE_LIST: list(position_manager.metrics.mfe_list),
                self.__POSITION_PERIOD_LENGTHS_LIST: list(position_manager.metrics.pos_period_lengths_list)
            }
        )

    def get_single_symbol_historic_data(self, system_name, symbol):
        position_list, num_of_periods = self.get_single_symbol_position_list(
            system_name, symbol, return_num_of_periods=True
        )

        start_dt = position_list[0].entry_dt
        end_dt = position_list[-1].exit_signal_dt if position_list[-1].exit_signal_dt is not None \
            else position_list[-2].exit_signal_dt

        position_manager = PositionManager(symbol, num_of_periods, 10000, 1.0)

        def generate_pos_sequence(position_list, **kwargs):
            for pos in position_list:
                yield pos

        if position_list[-1].active_position or position_list[-1].entry_dt is None:
            position_manager.generate_positions(generate_pos_sequence, position_list[:-1])
        else:
            position_manager.generate_positions(generate_pos_sequence, position_list[:])

        return json.dumps(
            {
                self.__START_DT: str(start_dt),
                self.__END_DT: str(end_dt),
                self.__MARKET_TO_MARKET_RETURNS: list(
                    position_manager.metrics.market_to_market_returns_list.astype(float)
                ),
                self.__EQUITY_LIST: list(position_manager.metrics.equity_list.astype(float)),
                self.__RETURNS_LIST: list(position_manager.metrics.returns_list),
                self.__MAE_LIST: list(position_manager.metrics.mae_list),
                self.__MFE_LIST: list(position_manager.metrics.mfe_list),
                self.__POSITION_PERIOD_LENGTHS_LIST: list(position_manager.metrics.pos_period_lengths_list)
            }
        )

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
        if query is not None:
            return pickle.loads(query[self.__ML_MODEL_FIELD])


if __name__ == '__main__':
    pass
