import json
from typing import Union

from bson import json_util

from TETrading.data.metadata.market_state_enum import MarketState

from tet_doc_db.doc_database_meta_classes.tet_systems_doc_db import ITetSystemsDocumentDatabase
from tet_doc_db.doc_database_meta_classes.tet_portfolio_doc_db import ITetPortfolioDocumentDatabase

from tet_trading_systems.trading_system_state_handler.portfolio.i_target_portfolio_creator \
    import ITargetPortfolioCreator
from tet_trading_systems.trading_system_state_handler.order_execution.i_order_execution_handler \
    import IOrderExecutionHandler


class Portfolio:

    def __init__(
        self, system_name, 
        target_portfolio_creator: ITargetPortfolioCreator,
        order_execution_handler: IOrderExecutionHandler,
        client_db: ITetSystemsDocumentDatabase,
        portfolio_db: Union[ITetPortfolioDocumentDatabase, ITetSystemsDocumentDatabase]
    ):
        self.__system_name = system_name
        self.__target_portfolio_creator = target_portfolio_creator
        #self.__order_execution_handler = order_execution_handler
        self.__client_db = client_db
        self.__portfolio_db = portfolio_db
        self.__system_metrics = json.loads(self.__portfolio_db.get_system_metrics(self.__system_name))
        self.__entry_signals = json.loads(
            self.__client_db.get_market_state_data(self.__system_name, MarketState.ENTRY.value), 
            object_hook=json_util.object_hook
        )
        # skapa dataklass PortfolioData med property funcs för members        
        if not self.__portfolio_db.get_portfolio(self.__system_name):
            self.__portfolio_db.insert_portfolio(
                self.__system_name, 
                json.loads(
                    self.__portfolio_db.get_system_portfolio_creation_data(self.__system_name)
                )
            )
        self.__portfolio_max_positions = self.__portfolio_db.get_portfolio_max_positions(self.__system_name)
        self.__target_positions = json.loads(
            self.__portfolio_db.get_target_portfolio_positions(self.__system_name),
            object_hook=json_util.object_hook
        )
        self.__exit_signals = json.loads(
            self.__client_db.get_market_state_data(self.__system_name, MarketState.EXIT.value),
            object_hook=json_util.object_hook
        )

    @property
    def system_name(self):
        return self.__system_name

    @property
    def portfolio_db(self):
        return self.__portfolio_db

    def _get_system_orders(self):
        # hamta nuvarande ordrar hos broker?
        pass

    def __call__(self, *args, **kwargs):
        target_position_ids = self.__target_portfolio_creator(
            self.__system_metrics, self.__portfolio_max_positions, 
            self.__entry_signals, self.__target_positions, self.__exit_signals
        )

        print('\ntarget_portfolio:')
        print(target_position_ids)
        print()

        # egentligen kan portfolio och order_execution_handler saerkopplas och exec handler lyssnar
        # på DB updates av portfolio collection documents
        #self.__order_execution_handler.place_orders()

        # hur hanteras/separeras entry_signals och target_positions vid insert? 
        self.__portfolio_db.update_portfolio(
           self.__system_name, target_position_ids
        )
