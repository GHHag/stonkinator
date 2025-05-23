from trading.position.position import Position
from trading.metrics.metrics import Metrics


class PositionManager:
    """
    Manages a collection of Position objects and their metrics.

    Parameters
    ----------
    identifier : 'str'
        Identifier of an asset.
    num_testing_periods : 'int'
        The number of periods in the data set.
    start_capital : 'int/float'
        The initial amount of capital to purchase assets with.
    capital_fraction : 'float'
        The fraction of the capital that will be used.
    asset_price_series : Keyword arg 'list'
        A price series of the asset. Default value=None
    """

    def __init__(
        self, identifier, num_testing_periods, start_capital, capital_fraction, 
        asset_price_series=None
    ):
        self.__identifier = identifier
        self.__asset_price_series = asset_price_series
        self.__num_testing_periods = num_testing_periods
        self.__start_capital = start_capital
        self.__capital_fraction = capital_fraction
        self.__safe_f_capital = self.__start_capital * self.__capital_fraction
        self.__uninvested_capital = self.__start_capital - self.__safe_f_capital

        self.__generated_positions = None
        self.__metrics = None

    @property
    def identifier(self):
        """
        The identifier of an asset.

        :return:
            'str'
        """

        return self.__identifier

    @property
    def position_list(self) -> list[Position]:
        """
        A collection of Position objects.

        :return:
            'list'
        """

        return self.metrics.positions

    @property
    def metrics(self):
        """
        Returns the objects __metrics field if it's
        referable, otherwise returns None.

        :return:
            'Metrics'
        """

        if self.__metrics:
            return self.__metrics
        else:
            return None

    def generate_positions(self, trading_logic, *args, **kwargs):
        """
        Calls the trading_logic function to generate positions.
        Creates an instance of the Metrics class, passing it the
         __generated_positions, __start_capital and
         __num_testing_periods.

        Parameters
        ----------
        :param trading_logic:
            'function' : Logic to generate positions.
        :param args:
            'tuple' : A tuple with arguments to pass to the trade_logic
            function.
        :param kwargs:
            'dict' : A dict with keyword arguments to pass to the
            trade_logic function
        """

        self.__generated_positions = trading_logic(
            *args, capital=self.__safe_f_capital, **kwargs
        )
        if not self.__generated_positions:
            print('No positions generated.')
        else:
            self.__metrics = Metrics(
                self.__identifier, self.__start_capital, self.__num_testing_periods
            )
            self.__metrics.calculate_metrics(
                self.__generated_positions, self.__asset_price_series
            )

    def summarize_performance(self, print_data=False, plot_fig=False, save_fig_to_path=None):
        """
        Summarizes the performance of the managed positions,
        printing a summary of metrics and statistics, and plots
        a sheet with charts.

        Parameters
        ----------
        :param print_data:
            Keyword arg 'bool' : True/False decides whether to print metrics
            of the trading system's generated positions or not. Default value=False
        :param plot_fig:
            Keyword arg 'bool' : True/False decides if the figure
            will be plotted or not. Default value=False
        :param save_fig_to_path:
            Keyword arg 'None/str' : Provide a file path as a string
            to save the plot as a file. Default value=None
        """

        if len(self.__metrics.positions) < 1:
            print('No positions generated.')
        else:
            if print_data: 
                self.__metrics.print_metrics()
            if plot_fig or save_fig_to_path:
                self.__metrics.plot_performance_summary(
                    self.__asset_price_series, plot_fig=plot_fig, 
                    save_fig_to_path=save_fig_to_path
                )
