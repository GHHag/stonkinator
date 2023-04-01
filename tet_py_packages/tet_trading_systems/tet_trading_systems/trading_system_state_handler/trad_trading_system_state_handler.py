from tet_doc_db.doc_database_meta_classes.tet_systems_doc_db import ITetSystemsDocumentDatabase


class TradingSystemStateHandler:
    
    def __init__(
        self, system_name, symbol, db: ITetSystemsDocumentDatabase, data
    ):
        self.__system_name = system_name
        self.__symbol = symbol
        self.__systems_db = db
        self.__data = data

    def __call__(self, callback_function, *args, **kwargs):
        callback_function(
            self.__data, self.__system_name, *args,
            systems_db=self.__systems_db, **kwargs
        )
