from decimal import Decimal
import json
from typing import Callable, Dict

import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline

from TETrading.data.metadata.market_state_enum import MarketState
from TETrading.data.metadata.trading_system_attributes import TradingSystemAttributes
from TETrading.data.metadata.trading_system_metrics import TradingSystemMetrics
from TETrading.position.position import Position
from TETrading.position.position_manager import PositionManager
from TETrading.signal_events.signal_handler import SignalHandler

from tet_doc_db.tet_mongo_db.systems_mongo_db import TetSystemsMongoDb


class MlSystemInstrumentData:
    __symbol: str
    __dataframe: pd.DataFrame
    __pred_data: np.ndarray
    __model: Pipeline
    __position_list: list[Position]
    __market_state_data: Dict[str, object]
    __num_testing_periods: int

    def __init__(self, symbol, dataframe, pred_data):
        self.__symbol = symbol
        self.__dataframe = dataframe
        self.__pred_data = pred_data
        self.__model = None
        self.__position_list = None
        self.__market_state_data = None
        self.__num_testing_periods = None

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
    def model(self):
        return self.__model

    @model.setter
    def model(self, model):
        self.__model = model

    @property
    def position_list(self):
        return self.__position_list

    @position_list.setter
    def position_list(self, position_list):
        self.__position_list = position_list

    @property
    def market_state_data(self):
        return self.__market_state_data

    @market_state_data.setter
    def market_state_data(self, market_state_data):
        self.__market_state_data = market_state_data

    @property
    def num_testing_periods(self):
        return self.__num_testing_periods

    @num_testing_periods.setter
    def num_testing_periods(self, num_testing_periods):
        self.__num_testing_periods = num_testing_periods


class MlTradingSystemStateHandler:

    def __init__(
        self, system_name: str, instrument_dicts: Dict[str, pd.DataFrame],
        instrument_pred_data_dicts: Dict[str, np.ndarray], db: TetSystemsMongoDb,
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
            instrument_data.model = self.__systems_db.get_ml_model(self.__system_name, instrument_data.symbol)
            instrument_data.position_list = self.__systems_db.get_single_symbol_position_list(
                self.__system_name, instrument_data.symbol
            )

            if instrument_data.model:
                instrument_data.market_state_data = json.loads(
                    self.__systems_db.get_market_state_data_for_symbol(self.__system_name, instrument_data.symbol)
                )
            else:
                raise Exception("Something went wrong while getting the model from database.")

            if instrument_data.position_list[-1].exit_signal_dt:
                mask = (instrument_data.dataframe['Date'] > str(instrument_data.position_list[0].entry_dt)) & \
                    (instrument_data.dataframe['Date'] <= str(instrument_data.position_list[-1].exit_signal_dt))
                instrument_data.num_testing_periods = len(instrument_data.dataframe.loc[mask])
            else:
                mask = (instrument_data.dataframe['Date'] > str(instrument_data.position_list[0].entry_dt)) & \
                    (instrument_data.dataframe['Date'] <= str(instrument_data.position_list[-2].exit_signal_dt))
                periods_in_position = instrument_data.market_state_data[TradingSystemAttributes.PERIODS_IN_POSITION] \
                    if TradingSystemAttributes.PERIODS_IN_POSITION in instrument_data.market_state_data else 1
                instrument_data.num_testing_periods = len(instrument_data.dataframe.loc[mask]) + periods_in_position 

            self.__instrument_dicts_list.append(instrument_data)

    def _handle_entry_signal(self, instrument_data: MlSystemInstrumentData):
        if instrument_data.market_state_data[TradingSystemAttributes.MARKET_STATE] == MarketState.ENTRY.value and \
            instrument_data.dataframe['Date'].iloc[-2] == pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT]) and \
                not instrument_data.position_list[-1].active_position:
            instrument_data.position_list[-1].enter_market(
                instrument_data.dataframe['Open'].iloc[-1], 'long', instrument_data.dataframe['Date'].iloc[-1]
            )
            print(
                f'\nEntry index {len(instrument_data.dataframe)}: {format(instrument_data.dataframe["Open"].iloc[-1], ".3f")}, ' + 
                f'{instrument_data.dataframe["Date"].iloc[-1]}'
            )

    def _handle_enter_market_state(
        self, instrument_data: MlSystemInstrumentData, entry_logic_function, entry_args,
        capital=10000, num_of_sims=2500, yearly_periods=251, plot_fig=True, 
        **kwargs
    ):
        def generate_position_sequence(**kwargs):
            for pos in instrument_data.position_list:
                yield pos
    
        entry_signal, direction = entry_logic_function(
            instrument_data.dataframe.iloc[-entry_args['entry_period_lookback']:], entry_args
        )
        if entry_signal and not instrument_data.position_list[-1].active_position and \
            not instrument_data.position_list[-1].exit_signal_dt == instrument_data.dataframe['Date'].iloc[-2]:
            # create mask to filter dataframe where the 'Date' is between the first element of 
            # position_list's entry_dt and the last elements exit_signal_dt
            mask = (instrument_data.dataframe['Date'] > str(instrument_data.position_list[0].entry_dt)) & \
                (instrument_data.dataframe['Date'] <= str(instrument_data.position_list[-1].exit_signal_dt))
            avg_yearly_positions = int(
                len(instrument_data.position_list) / (instrument_data.num_testing_periods / yearly_periods) + 0.5
            )
            
            position_manager = PositionManager(
                instrument_data.symbol, instrument_data.num_testing_periods, capital, 1.0,
                asset_price_series=[float(close) for close in instrument_data.dataframe.loc[mask]['Close']]
            )
            position_manager.generate_positions(generate_position_sequence)
            position_manager.summarize_performance(plot_fig=plot_fig)

            self.__signal_handler.handle_entry_signal(
                instrument_data.symbol, 
                {
                    TradingSystemAttributes.SIGNAL_INDEX: len(instrument_data.dataframe), 
                    TradingSystemAttributes.SIGNAL_DT: instrument_data.dataframe['Date'].iloc[-1], 
                    TradingSystemAttributes.SYMBOL: instrument_data.symbol,
                    TradingSystemAttributes.DIRECTION: direction,
                    TradingSystemAttributes.MARKET_STATE: MarketState.ENTRY.value
                }
            )
            self.__signal_handler.add_system_evaluation_data(
                position_manager.metrics.summary_data_dict,
                TradingSystemMetrics.system_evaluation_fields
            )
            
            instrument_data.position_list.pop(0)
            instrument_data.position_list.append(Position(capital))
            print(f'\nEntry signal, buy next open\nIndex {len(instrument_data.dataframe)}')

    def _handle_active_pos_state(self, instrument_data: MlSystemInstrumentData):
        if instrument_data.dataframe['Date'].iloc[-1] != pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT]):
            instrument_data.position_list[-1].update(Decimal(instrument_data.dataframe['Close'].iloc[-1]))
        instrument_data.position_list[-1].print_position_stats()

        if instrument_data.market_state_data[TradingSystemAttributes.MARKET_STATE] == MarketState.EXIT.value and \
            instrument_data.dataframe['Date'].iloc[-2] == pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT]):
            instrument_data.position_list[-1].exit_market(
                instrument_data.dataframe['Open'].iloc[-1], pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT])
            ) 
            print(
                f'Exit index {len(instrument_data.dataframe)}: {format(instrument_data.dataframe["Open"].iloc[-1], ".3f")}, ' + 
                f'{instrument_data.dataframe["Date"].iloc[-1]}\n'
                f'Realised return: {instrument_data.position_list[-1].position_return}'
            )
            return False
        else:
            self.__signal_handler.handle_active_position(
                instrument_data.symbol, 
                {
                    TradingSystemAttributes.SIGNAL_INDEX: len(instrument_data.dataframe), 
                    TradingSystemAttributes.SIGNAL_DT: instrument_data.dataframe['Date'].iloc[-1], 
                    TradingSystemAttributes.SYMBOL: instrument_data.symbol, 
                    TradingSystemAttributes.PERIODS_IN_POSITION: len(instrument_data.position_list[-1].returns_list),
                    TradingSystemAttributes.UNREALISED_RETURN: instrument_data.position_list[-1].unrealised_return,
                    TradingSystemAttributes.MARKET_STATE: MarketState.ACTIVE.value 
                }
            )
            return True

    def _handle_exit_market_state(
        self, instrument_data: MlSystemInstrumentData, exit_logic_function, exit_args
    ):
        exit_condition, trailing_exit, trailing_exit_price = exit_logic_function(
            instrument_data.dataframe.iloc[-exit_args['exit_period_lookback']:], False, None,
            instrument_data.position_list[-1].entry_price, exit_args, len(instrument_data.position_list[-1].returns_list)
        )
        if exit_condition:
            self.__signal_handler.handle_exit_signal(
                instrument_data.symbol,
                {
                    TradingSystemAttributes.SIGNAL_INDEX: len(instrument_data.dataframe), 
                    TradingSystemAttributes.SIGNAL_DT: instrument_data.dataframe['Date'].iloc[-1],
                    TradingSystemAttributes.SYMBOL: instrument_data.symbol, 
                    TradingSystemAttributes.PERIODS_IN_POSITION: len(instrument_data.position_list[-1].returns_list),
                    TradingSystemAttributes.UNREALISED_RETURN: instrument_data.position_list[-1].unrealised_return,
                    TradingSystemAttributes.MARKET_STATE: MarketState.EXIT.value 
                }
            )
            print(f'\nExit signal, exit next open\nIndex {len(instrument_data.dataframe)}')
            return True
        else:
            return False

    def __call__(
        self, entry_logic_function: Callable, exit_logic_function: Callable, 
        entry_args: Dict[str, object], exit_args: Dict[str, object], 
        date_format='%Y-%m-%d', capital=10000, plot_fig=False, insert_into_db=False, 
        **kwargs
    ):
        instrument_data: MlSystemInstrumentData
        for instrument_data in self.__instrument_dicts_list:
            if not 'entry_period_lookback' in entry_args.keys() or \
                not 'exit_period_lookback' in exit_args.keys():
                raise Exception("Given parameter for 'entry_args' or 'exit_args' is missing required key(s).")

            if instrument_data.dataframe['Date'].iloc[-1] != pd.Timestamp(instrument_data.market_state_data[TradingSystemAttributes.SIGNAL_DT]):
                self._handle_entry_signal(instrument_data)
                latest_data_point = instrument_data.dataframe.iloc[-1].copy()
                latest_data_point['pred'] = instrument_data.model.predict(instrument_data.pred_data[-1].reshape(1, -1))[0]
                instrument_data.dataframe = instrument_data.dataframe.iloc[:-1].append(latest_data_point, ignore_index=True)
            
                if isinstance(instrument_data.position_list[-1], Position) and instrument_data.position_list[-1].active_position:
                    active_pos = self._handle_active_pos_state(instrument_data)
                    if active_pos:
                        self._handle_exit_market_state(instrument_data, exit_logic_function, exit_args)
                    else:
                        if insert_into_db:
                            # Position objects in json format are inserted into database after being exited
                            self.__systems_db.insert_single_symbol_position_list(
                                self.__system_name, instrument_data.symbol, 
                                instrument_data.position_list, instrument_data.num_testing_periods,
                                format='json'
                            )
                else:
                    self._handle_enter_market_state(
                        instrument_data, entry_logic_function, entry_args,
                        capital=capital, plot_fig=plot_fig
                    )

                result = self.__systems_db.insert_single_symbol_position_list(
                    self.__system_name, instrument_data.symbol, 
                    instrument_data.position_list, instrument_data.num_testing_periods
                )

                if not result:
                    print(
                        'List of Position objects were not modified.\n' + 
                        'Insert position list result: ' + str(result)
                    )

        print(self.__signal_handler)
        if insert_into_db:
            self.__signal_handler.insert_into_db(
                {
                    MarketState.ENTRY.value: self.__systems_db.insert_market_state_data,
                    MarketState.ACTIVE.value: self.__systems_db.insert_market_state_data, 
                    MarketState.EXIT.value: self.__systems_db.insert_market_state_data
                }, self.__system_name
            )