import pandas as pd

from trading.data.metadata.trading_system_attributes import TradingSystemAttributes


class TradingSystemSignals:
    """
    Handles data for signals.
    """

    def __init__(self):
        self.__data_list: list[dict] = []

    @property
    def data_list(self) -> list[dict]:
        return self.__data_list

    @property
    def dataframe(self):
        """
        Returns a Pandas DataFrame with data from the __data_list
        member.

        :return:
            'Pandas.DataFrame'
        """

        if self.__data_list:
            return pd.DataFrame(instrument['data'] for instrument in self.__data_list)
        else:
            return None

    def __str__(self):
        if self.__data_list:
            return f'{self.dataframe.to_string()}'
        else:
            return f'{self.__data_list}'

    def get_pos_sizer_dict(self, position_sizing_metric_str):
        """
        Formats and returns the __data_list member.

        :param position_sizing_metric_str:
            'str' : The system's position sizing metric as a string.
        :return:
            'dict'
        """
        
        return {
            (
                x.get(TradingSystemAttributes.INSTRUMENT_ID),
                x.get(TradingSystemAttributes.SYMBOL)
            ): 
                x['data'][position_sizing_metric_str] 
            for x in self.__data_list
        }

    def add_data(self, instrument_id, symbol, data_dict):
        """
        Appends a dict with a symbol/ticker and given data to the
        __data_list member.

        Parameters
        ----------
        :param instrument_id:
            'str' : Identifier of an instrument.
        :param symbol:
            'str' : The symbol/ticker of an asset.
        :param data_dict:
            'dict' : A dict containing data of a position.
        """

        self.__data_list.append(
            {
                TradingSystemAttributes.INSTRUMENT_ID: instrument_id,
                TradingSystemAttributes.SYMBOL: symbol,
                'data': data_dict
            }
        )

    def add_evaluation_data(self, evaluation_data_dict: dict):
        """
        Iterates over the __data_list member and if a value
        of the 'symbol' key matches a value in the given
        evaluation_data_dict that entry will be updated with
        the evaluation_data_dict.

        Parameters
        ----------
        :param evaluation_data_dict:
            'dict' : A dict containing system evaluation data.
        """

        for instrument_dict in self.__data_list:
            if (
                instrument_dict.get(TradingSystemAttributes.SYMBOL) == 
                evaluation_data_dict.get(TradingSystemAttributes.SYMBOL)
            ):
                instrument_data: dict = instrument_dict.get('data')
                if instrument_data is not None:
                    assert (
                        isinstance(instrument_data, dict),
                        "The value mapping to the 'data' key in 'instrument_dict' "
                        "does not have the right type."
                    )
                    instrument_data.update(evaluation_data_dict)
