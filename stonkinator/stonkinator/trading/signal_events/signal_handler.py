from trading.position.order import Order
from trading.position.position import Position
from trading.signal_events.signals.entry_signal import EntrySignals
from trading.signal_events.signals.exit_signal import ExitSignals
from trading.signal_events.signals.active_position import ActivePositions

from persistance.persistance_meta_classes.signals_persister import SignalsPersisterBase


class SignalHandler:
    """
    Handles signals given by the trading system.

    TODO: Implement methods _execute_signals and __call__.
    """

    def __init__(self):
        self.__entry_signals = EntrySignals()
        self.__exit_signals = ExitSignals()
        self.__active_positions = ActivePositions()
        self.__entry_signal_given = False
        self.__current_order = (None, '')
        self.__current_position = (None, '')

    @property
    def entry_signal_given(self):
        return self.__entry_signal_given

    @property
    def current_order(self) -> tuple[Order | None, str]:
        return self.__current_order

    @current_order.setter
    def current_order(self, value: tuple[Order | None, str]):
        self.__current_order = value

    @property
    def current_position(self) -> tuple[Position | None, str]:
        return self.__current_position

    @current_position.setter
    def current_position(self, value: tuple[Position | None, str]):
        self.__current_position = value

    def handle_entry_signal(self, symbol, data_dict):
        """
        Calls the __entry_signals members add_signal_data method,
        passing it the given symbol and data_dict.

        Parameters
        ----------
        :param symbol:
            'str' : The symbol/ticker of an asset.
        :param data_dict:
            'dict' : Data to be handled.
        """

        self.__entry_signal_given = True
        self.__entry_signals.add_data(symbol, data_dict)

    def handle_active_position(self, symbol, data_dict):
        """
        Calls the __active_positions members add_data method,
        passing it the given symbol and data_dict.

        Parameters
        ----------
        :param symbol:
            'str' : The symbol/ticker of an asset.
        :param data_dict:
            'dict' : Data to be handled.
        """

        self.__active_positions.add_data(symbol, data_dict)

    def handle_exit_signal(self, symbol, data_dict):
        """
        Calls the __exit_signals members add_signal_data method,
        passing it the given symbol and data_dict.

        Parameters
        ----------
        :param symbol:
            'str' : The symbol/ticker of an asset.
        :param data_dict:
            'dict' : Data to be handled.
        """

        self.__exit_signals.add_data(symbol, data_dict)

    def _execute_signals(self):
        # TODO: Implement functionality to be able to connect to brokers
        #  and execute orders with the use of an 'ExecutionHandler' class.
        pass

    def add_system_evaluation_data(self, evaluation_dict: dict, evaluation_fields):
        """
        Adds the given evaluation data to the EntrySignals object member
        by calling its add_evaluation_data method.

        Parameters
        ----------
        :param evaluation_dict:
            'dict' : A dict with data generated by a TradingSession object.
        :param evaluation_fields:
            'tuple' : A tuple containing strings that should have corresponding
            fields in the given 'evaluation_dict'
        """

        if evaluation_dict:
            self.__entry_signals.add_evaluation_data(
                {
                    k: evaluation_dict.get(k) for k in evaluation_fields 
                    if evaluation_dict.get(k) is not None
                }
            )
        self.__entry_signal_given = False

    def write_to_csv(self, path, system_name):
        """
        Writes the dataframe field of the __entry_signals and __exit_signals
        members to a CSV file.

        Parameters
        ----------
        :param path:
            'str' : The path to where the CSV file will be written.
        :param system_name:
            'str' : The name of the system that generated the signals.
        """

        with open(path, 'a') as file:
            file.write("\n" + system_name + "\n")
            if self.__entry_signals.dataframe is not None:
                self.__entry_signals.dataframe.to_csv(path, mode='a')
            if self.__exit_signals.dataframe is not None:
                self.__exit_signals.dataframe.to_csv(path, mode='a')

    def insert_into_db(self, db: SignalsPersisterBase, system_name):
        """
        Insert data into database from the dataframes that holds data 
        and stats for signals and positions.

        Parameters
        ----------
        :param db:
            'SignalsPersisterBase' : A database object of a class that
            implements the 'SignalsPersisterBase' meta class.
        :param system_name:
            'str' : The name of a system which it will be identified by in
            in the database.
        """

        if self.__entry_signals.dataframe is not None:
            insert_successful = db.insert_market_state_data(
                system_name, self.__entry_signals.dataframe.to_json(orient='table')
            )

            if not insert_successful:
                raise Exception('DatabaseInsertException, failed to insert to database.')

        if self.__active_positions.dataframe is not None:
            insert_successful = db.insert_market_state_data(
                system_name, self.__active_positions.dataframe.to_json(orient='table')
            )

            if not insert_successful:
                raise Exception('DatabaseInsertException, failed to insert to database.')

        if self.__exit_signals.dataframe is not None:
            insert_successful = db.insert_market_state_data(
                system_name, self.__exit_signals.dataframe.to_json(orient='table')
            )

            if not insert_successful:
                raise Exception('DatabaseInsertException, failed to insert to database.')

    def get_position_sizing_dict(self, position_sizing_metric_str):
        """
        Calls the get_pos_sizer_dict method of the __entry_signals member
        and returns the result.

        :param position_sizing_metric_str:
            'str' : The system's position sizing metric as a string.
        :return:
            'dict'
        """
        
        return self.__entry_signals.get_pos_sizer_dict(position_sizing_metric_str)

    def __str__(self):
        return (
            f'Active positions\n{self.__active_positions}\n\n'
            f'Entry signals\n{self.__entry_signals}\n\n'
            f'Exit signals\n{self.__exit_signals}'
        )

    def __call__(self):
        """
        Execute signals.

        TODO: Fully implement the methods functionality.
        """

        self._execute_signals()
