from decimal import Decimal
import json

import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline

from trading.data.metadata.market_state_enum import MarketState
from trading.data.metadata.trading_system_attributes import TradingSystemAttributes
from trading.position.position import Position
from trading.signal_events.signal_handler import SignalHandler

from persistance.stonkinator_mongo_db.systems_mongo_db import TetSystemsMongoDb


class MlSystemInstrumentData:
    __symbol: str
    __dataframe: pd.DataFrame
    __pred_data: np.ndarray
    __model_pipeline: Pipeline
    __position: Position
    __market_state_data: dict[str, object]

    def __init__(self, symbol, dataframe, pred_data):
        self.__symbol = symbol
        self.__dataframe = dataframe
        self.__pred_data = pred_data
        self.__model_pipeline = None
        self.__position = None
        self.__market_state_data = None

    @property
    def symbol(self):
        return self.__symbol

    @property
    def dataframe(self):
        return self.__dataframe

    @dataframe.setter
    def dataframe(self, dataframe):
        self.__dataframe = dataframe

    @property
    def pred_data(self):
        return self.__pred_data

    @property
    def model_pipeline(self):
        return self.__model_pipeline

    @model_pipeline.setter
    def model_pipeline(self, model_pipeline):
        self.__model_pipeline = model_pipeline

    @property
    def position(self):
        return self.__position

    @position.setter
    def position(self, position):
        self.__position = position

    @property
    def market_state_data(self):
        return self.__market_state_data

    @market_state_data.setter
    def market_state_data(self, market_state_data):
        self.__market_state_data = market_state_data


class MlTradingSystemStateHandler:

    def __init__(
        self, system_name: str, instrument_dicts: dict[str, pd.DataFrame],
        instrument_pred_data_dicts: dict[str, np.ndarray], db: TetSystemsMongoDb,
        date_format='%Y-%m-%d'
    ):
        self.__system_name = system_name
        self.__instrument_dicts_list = []
        self.__systems_db = db
        self.__date_format = date_format

        self.__signal_handler = SignalHandler()
        
        for symbol, dataframe in instrument_dicts.items():
            instrument_data = MlSystemInstrumentData(
                symbol, dataframe, instrument_pred_data_dicts.get(symbol)
            )
            instrument_data.model_pipeline = self.__systems_db.get_ml_model(self.__system_name, instrument_data.symbol)
            instrument_data.position = self.__systems_db.get_current_position(
                self.__system_name, instrument_data.symbol
            )

            if instrument_data.model_pipeline != None:
                instrument_data.market_state_data = json.loads(
                    self.__systems_db.get_market_state_data_for_symbol(self.__system_name, instrument_data.symbol)
                )
            else:
                print(f'{symbol} - failed to get model from the database')
                continue

            self.__instrument_dicts_list.append(instrument_data)

    def _handle_entry_signal(self, instrument_data: MlSystemInstrumentData):
        if (
            instrument_data.market_state_data[TradingSystemAttributes.MARKET_STATE] == MarketState.ENTRY.value and
            instrument_data.dataframe.index[-2] == pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT]) and
            instrument_data.position.active_position == False
        ):
            instrument_data.position.enter_market(
                instrument_data.dataframe['open'].iloc[-1], instrument_data.dataframe.index[-1]
            )
            print(
                f'\nEntry index {len(instrument_data.dataframe)}: {format(instrument_data.dataframe["open"].iloc[-1], ".3f")}, '
                f'{instrument_data.dataframe.index[-1]}'
            )

    def _handle_enter_market_state(
        self, instrument_data: MlSystemInstrumentData, entry_logic_function, entry_args,
        capital=10000
    ):
        entry_signal, direction = entry_logic_function(
            instrument_data.dataframe.iloc[-entry_args[TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK]:], 
            entry_args
        )
        if (
            entry_signal == True and instrument_data.position == None or 
            entry_signal == True and
            instrument_data.position.active_position == False and
            instrument_data.position.exit_signal_dt != instrument_data.dataframe.index[-2]
        ):
            self.__signal_handler.handle_entry_signal(
                instrument_data.symbol, 
                {
                    TradingSystemAttributes.SIGNAL_INDEX: len(instrument_data.dataframe), 
                    TradingSystemAttributes.SIGNAL_DT: instrument_data.dataframe.index[-1], 
                    TradingSystemAttributes.SYMBOL: instrument_data.symbol,
                    TradingSystemAttributes.DIRECTION: direction,
                    TradingSystemAttributes.MARKET_STATE: MarketState.ENTRY.value
                }
            )
            
            instrument_data.position = Position(capital, direction)
            print(f'\nEntry signal, buy next open\nIndex {len(instrument_data.dataframe)}')
            return entry_signal

    def _handle_active_pos_state(self, instrument_data: MlSystemInstrumentData):
        if (
            instrument_data.market_state_data[TradingSystemAttributes.MARKET_STATE] == MarketState.EXIT.value and
            instrument_data.dataframe.index[-2] == pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT])
        ):
            instrument_data.position.exit_market(
                instrument_data.dataframe['open'].iloc[-1],
                pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT]),
                instrument_data.dataframe.index[-1]
            ) 
            print(
                f'Exit index {len(instrument_data.dataframe)}: {format(instrument_data.dataframe["open"].iloc[-1], ".3f")}, '
                f'{instrument_data.dataframe.index[-1]}\n'
                f'Realised return: {instrument_data.position.position_return}'
            )
            return False
        else:
            if instrument_data.dataframe.index[-1] != pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT]):
                instrument_data.position.update(
                    Decimal(instrument_data.dataframe['close'].iloc[-1]),
                    instrument_data.dataframe.index[-1]
                )
            instrument_data.position.print_position_stats()
            self.__signal_handler.handle_active_position(
                instrument_data.symbol, 
                {
                    TradingSystemAttributes.SIGNAL_INDEX: len(instrument_data.dataframe), 
                    TradingSystemAttributes.SIGNAL_DT: instrument_data.dataframe.index[-1], 
                    TradingSystemAttributes.SYMBOL: instrument_data.symbol, 
                    TradingSystemAttributes.PERIODS_IN_POSITION: len(instrument_data.position.returns_list),
                    TradingSystemAttributes.UNREALISED_RETURN: instrument_data.position.unrealised_return,
                    TradingSystemAttributes.MARKET_STATE: MarketState.ACTIVE.value 
                }
            )
            return True

    def _handle_exit_market_state(
        self, instrument_data: MlSystemInstrumentData, exit_logic_function, exit_args
    ):
        exit_condition, _, _ = exit_logic_function(
            instrument_data.dataframe.iloc[-exit_args[TradingSystemAttributes.EXIT_PERIOD_LOOKBACK]:], False, None,
            instrument_data.position.entry_price, exit_args, len(instrument_data.position.returns_list)
        )
        if exit_condition == True:
            self.__signal_handler.handle_exit_signal(
                instrument_data.symbol,
                {
                    TradingSystemAttributes.SIGNAL_INDEX: len(instrument_data.dataframe), 
                    TradingSystemAttributes.SIGNAL_DT: instrument_data.dataframe.index[-1],
                    TradingSystemAttributes.SYMBOL: instrument_data.symbol, 
                    TradingSystemAttributes.PERIODS_IN_POSITION: len(instrument_data.position.returns_list),
                    TradingSystemAttributes.UNREALISED_RETURN: instrument_data.position.unrealised_return,
                    TradingSystemAttributes.MARKET_STATE: MarketState.EXIT.value 
                }
            )
            print(f'\nExit signal, exit next open\nIndex {len(instrument_data.dataframe)}')

    def __call__(
        self, entry_logic_function: callable, exit_logic_function: callable,
        entry_args: dict[str, object], exit_args: dict[str, object],
        date_format='%Y-%m-%d', capital=10000,
        client_db: TetSystemsMongoDb=None, insert_into_db=False,
        **kwargs
    ):
        instrument_data: MlSystemInstrumentData
        for instrument_data in self.__instrument_dicts_list:
            if (
                not TradingSystemAttributes.ENTRY_PERIOD_LOOKBACK in entry_args.keys() or
                not TradingSystemAttributes.EXIT_PERIOD_LOOKBACK in exit_args.keys()
            ):
                raise Exception("given parameter for 'entry_args' or 'exit_args' is missing required key(s)")

            if (
                instrument_data.dataframe.index[-1] > pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT]) or
                type(pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT])) == type(pd.NaT)
            ):
                self._handle_entry_signal(instrument_data)
                latest_data_point = instrument_data.dataframe.iloc[-1].copy()
                latest_data_point['pred'] = instrument_data.model_pipeline.predict(instrument_data.pred_data[-1].reshape(1, -1))[0]
                latest_data_point_df = pd.DataFrame(latest_data_point).transpose()
                latest_data_point_df['pred'] = latest_data_point_df['pred'].astype('boolean')
                instrument_data.dataframe = pd.concat(
                    [instrument_data.dataframe.iloc[:-1], latest_data_point_df]
                )
            
                if isinstance(instrument_data.position, Position) == True and instrument_data.position.active_position == True:
                    active_pos = self._handle_active_pos_state(instrument_data)
                    if active_pos == True:
                        self._handle_exit_market_state(instrument_data, exit_logic_function, exit_args)
                    else:
                        if insert_into_db == True:
                            self.__systems_db.insert_single_symbol_position(
                                self.__system_name, instrument_data.symbol,
                                instrument_data.position, len(instrument_data.position.returns_list),
                                serialized_format=True
                            )
                            client_db.insert_single_symbol_position(
                                self.__system_name, instrument_data.symbol,
                                instrument_data.position, len(instrument_data.position.returns_list),
                                json_format=True
                            )
                else:
                    entry_signal = self._handle_enter_market_state(
                        instrument_data, entry_logic_function, entry_args,
                        capital=capital
                    )

                    if entry_signal == True:
                        latest_position: Position = self.__systems_db.get_single_symbol_latest_position(
                            self.__system_name, instrument_data.symbol
                        )
                        num_of_periods = (
                            len(
                                instrument_data.dataframe.loc[
                                    instrument_data.dataframe.index > latest_position.exit_dt
                                ]
                            )
                            if latest_position != None else len(instrument_data.dataframe)
                        )
                        self.__systems_db.increment_num_of_periods(
                            self.__system_name, instrument_data.symbol, num_of_periods
                        )
                        client_db.increment_num_of_periods(
                            self.__system_name, instrument_data.symbol, num_of_periods
                        )

                result = self.__systems_db.insert_current_position(
                    self.__system_name, instrument_data.symbol, instrument_data.position
                )

                if result == False:
                    print(
                        'List of Position objects were not modified.\n'
                        f'Insert position list result: {str(result)}'
                    )

        print(self.__signal_handler)
        if insert_into_db == True:
            self.__signal_handler.insert_into_db(self.__systems_db, self.__system_name)
            if client_db is not self.__systems_db:
                self.__signal_handler.insert_into_db(client_db, self.__system_name)