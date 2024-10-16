import math
from decimal import Decimal, DivisionByZero

import numpy as np
import pandas as pd

from trading.data.metadata.trading_system_metrics import TradingSystemMetrics
from trading.position.position import Position
from trading.metrics.metrics_summary_plot import system_metrics_summary_plot
from trading.utils.metric_functions import calculate_max_drawdown, calculate_sharpe_ratio, \
    calculate_cagr 


class Metrics:
    """
    Iterates over a collection of Position objects, calculates and
    assigns field for different metrics.

    Parameters
    ----------
    symbol : 'str'
        The symbol/ticker of an asset.
    start_capital : 'int/float'
        The amount of capital to purchase assets with.
    num_testing_periods : 'int'
        The number of periods in the dataset that the positions
        was generated from.
    """
    
    def __init__(self, symbol, start_capital, num_testing_periods):
        self.__symbol = symbol
        self.__start_capital = start_capital
        self.__positions: list[Position] = []
        self.__num_testing_periods = num_testing_periods
        self.__equity_list = np.array([self.__start_capital])
        self.__profit_loss_list = np.array([])
        self.__returns_list = np.array([])
        self.__market_to_market_returns_list = np.array([])
        self.__pos_net_results_list = np.array([])
        self.__pos_gross_results_list = np.array([])
        self.__pos_period_lengths_list = np.array([])
        self.__winning_pos_period_lengths_list = np.array([])
        self.__losing_pos_period_lengths_list = np.array([])

        self.__profitable_pos_list = np.array([])
        self.__profitable_pos_returns_list = np.array([])
        self.__net_wins_list = np.array([])
        self.__gross_wins_list = np.array([])
        self.__losing_pos_list = np.array([])
        self.__net_losses_list = np.array([])
        self.__gross_losses_list = np.array([])

        self.__mfe_list = np.array([])
        self.__mae_list = np.array([])
        self.__w_mae_list = np.array([])
        self.__mae_mfe_dataframe = pd.DataFrame()

    @property
    def positions(self):
        return self.__positions

    @property
    def start_capital(self):
        return self.__start_capital

    @property
    def num_testing_periods(self):
        return self.__num_testing_periods

    @property
    def returns_list(self):
        return self.__returns_list

    @property
    def market_to_market_returns_list(self):
        return self.__market_to_market_returns_list

    @property
    def equity_list(self):
        return self.__equity_list

    @property
    def pos_period_lengths_list(self):
        return self.__pos_period_lengths_list

    @property
    def max_drawdown(self):
        return self.__max_drawdown

    @property
    def mae_list(self):
        return self.__mae_list

    @property
    def w_mae_list(self):
        return self.__w_mae_list

    @property
    def mfe_list(self):
        return self.__mfe_list

    @property
    def summary_data_dict(self):
        """
        Returns a dict with statistics and metrics.

        :return:
            'dict'
        """

        try:
            return {
                TradingSystemMetrics.SYMBOL: self.__symbol,
                TradingSystemMetrics.NUM_OF_POSITIONS: len(self.__returns_list),
                TradingSystemMetrics.START_CAPITAL: self.__start_capital,
                TradingSystemMetrics.FINAL_CAPITAL: self.__final_capital,
                TradingSystemMetrics.TOTAL_GROSS_PROFIT: self.__total_gross_profit,
                TradingSystemMetrics.AVG_POS_NET_PROFIT: round(self.__avg_pos_net_result, 3),
                TradingSystemMetrics.PCT_WINS: self.__pct_wins,
                TradingSystemMetrics.PROFIT_FACTOR: round(self.__profit_factor, 3),
                TradingSystemMetrics.EXPECTANCY: round(self.__expectancy, 3),
                TradingSystemMetrics.SHARPE_RATIO: round(float(self.__sharpe_ratio), 3),
                TradingSystemMetrics.RATE_OF_RETURN: self.__rate_of_return,
                TradingSystemMetrics.MEAN_PROFIT_LOSS: round(self.__mean_profit_loss, 3),
                TradingSystemMetrics.MEDIAN_PROFIT_LOSS: round(self.__median_profit_loss, 3),
                TradingSystemMetrics.STD_OF_PROFIT_LOSS: round(self.__std_profit_loss, 3),
                TradingSystemMetrics.MEAN_RETURN: round(self.__mean_return, 3),
                TradingSystemMetrics.MEDIAN_RETURN: round(self.__median_return, 3),
                TradingSystemMetrics.STD_OF_RETURNS: round(self.__std_return, 3),
                TradingSystemMetrics.AVG_MAE: round(np.mean(self.__mae_list), 3),
                TradingSystemMetrics.MIN_MAE: round(np.min(self.__mae_list), 3),
                TradingSystemMetrics.AVG_MFE: round(np.mean(self.__mfe_list), 3),
                TradingSystemMetrics.MAX_MFE: round(np.max(self.__mfe_list), 3),
                TradingSystemMetrics.MAX_DRAWDOWN: float(self.__max_drawdown),
                TradingSystemMetrics.ROMAD: round(self.__return_to_max_drawdown, 3),
                TradingSystemMetrics.CAGR: round(self.__cagr, 3),
                TradingSystemMetrics.AVG_PERIODS_IN_POSITIONS: round(np.mean(self.__pos_period_lengths_list), 3),
                TradingSystemMetrics.AVG_PERIODS_IN_WINNING_POSITIONS:
                    round(np.mean((self.__winning_pos_period_lengths_list)))
                    if len(self.__winning_pos_period_lengths_list) > 0
                    else np.nan,
                TradingSystemMetrics.AVG_PERIODS_IN_LOSING_POSITIONS:
                    round(np.mean((self.__losing_pos_period_lengths_list)))
                    if len(self.__losing_pos_period_lengths_list) > 0
                    else np.nan,
                f'underlying_{TradingSystemMetrics.SHARPE_RATIO}': self.underlying_sharpe,
                f'underlying_{TradingSystemMetrics.MAX_DRAWDOWN}': self.underlying_max_dd,
                f'underlying_{TradingSystemMetrics.CAGR}': self.underlying_cagr
            }
        except AttributeError:
            # TODO: Log error
            return {TradingSystemMetrics.SYMBOL: self.__symbol}

    def _mae_mfe_dataframe_apply(self):
        """
        Adds columns for maximum adverse excursion, maximum favorable
        excursion and return data to the __mae_mfe_dataframe member.
        """

        if len(self.__mae_list) == 0 and len(self.__mfe_list) == 0 and len(self.__returns_list) == 0:
            self.__returns_list = np.append(self.__returns_list, 0)
        if len(self.__mae_list) == 0:
            self.__mae_list = np.append(self.__mae_list, 0)
        if len(self.__mfe_list) == 0:
            self.__mfe_list = np.append(self.__mfe_list, 0)

        self.__mae_mfe_dataframe['mae_data'] = self.__mae_list
        self.__mae_mfe_dataframe['mfe_data'] = self.__mfe_list
        self.__mae_mfe_dataframe['return'] = self.__returns_list

    def _calculate_max_drawdown(self):
        """
        Calculates and returns the maximum drawdown of the
        __equity_list member.

        :return:
            'float'
        """

        max_drawdown = 0
        peak_value = (self.__equity_list[0] - 1)

        for index, equity in enumerate(self.__equity_list):
            if equity > peak_value:
                peak_value = equity
                trough_value = np.min(self.__equity_list[index:])
                drawdown = (trough_value - peak_value) / peak_value

                if drawdown < max_drawdown:
                    max_drawdown = drawdown

        return np.abs(max_drawdown * 100)

    def _calculate_expectancy(self):
        """
        Calculates and returns the expectancy.

        :return:
            'float'
        """

        if not len(self.__positions) > 0:
            return np.nan
        else:
            avg_profit = np.sum(self.__pos_net_results_list) / len(self.__positions)
            avg_loss = np.mean(self.__net_losses_list) if len(self.__net_losses_list) > 0 else 1
            if avg_loss == 0:
                return np.nan
            else:
                return float(avg_profit) / float(np.abs(avg_loss))

    def _calculate_cagr(self, yearly_periods=251):
        """
        Calculates and returns the compound annual growth rate.

        Parameters
        ----------
        :param yearly_periods:
            'int' : The number of periods in a trading year
            for the time frame of the dataset.

        :return:
            'float'
        """

        initial_value, *_, final_value = self.__equity_list
        num_of_periods = self.__num_testing_periods
        years = num_of_periods / yearly_periods

        if final_value < 0:
            final_value += np.abs(final_value)
            initial_value += np.abs(final_value)

        try:
            cagr = math.pow((final_value / initial_value), (1 / years)) - 1
        except (ValueError, ZeroDivisionError):
            return 0

        return cagr * 100

    def _calculate_rate_of_return(self):
        """
        Calculates and returns the rate of return.

        :return:
            'float'
        """

        return ((self.__final_capital - self.__start_capital) / self.__start_capital) * 100

    def _calculate_sharpe_ratio(self, risk_free_rate=Decimal(0.05), yearly_periods=251):
        """
        Calculates and returns the annualized sharpe ratio.

        Parameters
        ----------
        :param risk_free_rate:
            'Decimal' : The yearly return of a risk free asset.
        :param yearly periods:
            'int' : The number of periods in a trading year
            for the time frame of the dataset.

        :return:
            'Decimal'
        """

        excess_returns = (np.array(self.__market_to_market_returns_list) / 100) - \
            risk_free_rate / yearly_periods
        if not len(excess_returns) > 0:
            return np.nan
        else:
            return Decimal(np.sqrt(yearly_periods)) * Decimal(np.mean(excess_returns)) / \
                Decimal(np.std(excess_returns))

    def _calculate_avg_annual_profit(self, yearly_periods=251):
        """
        Calculates and returns the average annual profit.

        Parameters
        ----------
        :param yearly_periods:
            'int' : The number of periods in a trading year
            for the time frame of the dataset.

        :return:
            'float'
        """

        try:
            years = self.__num_testing_periods / yearly_periods
            return self.__total_gross_profit / years
        except ZeroDivisionError:
            print('ZeroDivisionError, Metrics._calculate_annual_avg_profit')

    def _calculate_annual_rate_of_return(self):
        # TODO: Implement method for calculation of annual rate of return.
        pass

    def _calculate_annual_ror_to_dd(self):
        # TODO: Implement method for calculation of annual rate of return to drawdown.
        pass

    def print_metrics(self):
        """
        Prints statistics and metrics to the console.
        """

        print(
            f'\nSymbol: {self.__symbol}'
            f'\nPerformance summary: '
            f'\nNumber of positions: {len(self.__returns_list)}'
            f'\nNumber of profitable positions: {len(self.__profitable_pos_list)}'
            f'\nNumber of losing positions: {len(self.__losing_pos_list)}'
            f'\n% wins: {format(self.__pct_wins, ".2f")}'
            f'\n% losses: {format(self.__pct_losses, ".2f")}'
            f'\nTesting periods: {self.__num_testing_periods}'
            f'\nStart capital: {self.__start_capital}'
            f'\nFinal capital: {format(self.__final_capital, ".3f")}'
            f'\nTotal gross profit: {format(self.__total_gross_profit, ".3f")}'
            f'\nAvg pos net profit: {format(self.__avg_pos_net_result, ".3f")}'
            f'\nMean P/L: {format(self.__mean_profit_loss, ".3f")}'
            f'\nMedian P/L: {format(self.__median_profit_loss, ".3f")}'
            f'\nStd of P/L: {format(self.__std_profit_loss, ".3f")}'
            f'\nMean return: {format(self.__mean_return, ".3f")}'
            f'\nMedian return: {format(self.__median_return, ".3f")}'
            f'\nStd of returns: {format(self.__std_return, ".3f")}'
            f'\nExpectancy: {format(self.__expectancy, ".3f")}'
            f'\nRate of return: {format(self.__rate_of_return, ".2f")}'
            f'\nMax drawdown: {format(self.__max_drawdown, ".2f")}%'
            f'\nRoMad: {format(self.__return_to_max_drawdown, ".3f")}%'
            f'\nProfit factor: {format(self.__profit_factor, ".3f")}'
            f'\nCAGR: {format(self.__cagr, ".2f")}%'
            f'\nSharpe ratio: {format(self.__sharpe_ratio, ".4f")}'
            f'\nAvg annual profit: {format(self.__avg_annual_profit, ".2f")}'
    
            f'\n\nTotal equity market to market:'
            f'\n{list(map(float, self.__equity_list))}'
            f'\nP/L (per contract):'
            f'\n{self.__profit_loss_list}'
            f'\nReturns:'
            f'\n{self.__returns_list}'
        
            f'\n\nProfits (per contract):'
            f'\n{self.__profitable_pos_list}'
            f'\nMean profit (per contract): {format(self.__mean_positive_pos, ".3f")}'
            f'\nMedian profit (per contract): {format(self.__median_positive_pos, ".3f")}'

            f'\n\nLosses (per contract):'
            f'\n{self.__losing_pos_list}'
            f'\nMean loss (per contract): {format(self.__mean_negative_pos, ".3f")}'
            f'\nMedian loss (per contract): {format(self.__median_negative_pos, ".3f")}'
        
            f'\n\nMAE data: {str(self.__mae_list)}'
            f'\nMFE data: {str(self.__mfe_list)}'
            f'\nReturns: {str(self.__returns_list)}'
        )

    def plot_performance_summary(
        self, asset_price_series, plot_fig=False, save_fig_to_path=None
    ):
        """
        Calls function that plots summary statistics and metrics.

        Parameters
        ----------
        :param asset_price_series:
            'list' : A collection of underlying asset/benchmark price series.
        :param plot_fig:
            Keyword arg 'bool' : True/False decides whether the
            plot should be shown during run time or not.
            Default value=False
        :param save_fig_to_path:
            Keyword arg 'None/str' : Provide a file path as a string
            to save the plot as a file. Default value=None
        """

        system_metrics_summary_plot(
            self.__market_to_market_returns_list, self.__equity_list,
            asset_price_series, self.__mae_list, self.__mfe_list,
            self.__returns_list, self.__pos_period_lengths_list,
            self.__symbol, self.summary_data_dict,
            plot_fig=plot_fig, save_fig_to_path=save_fig_to_path
        )

    def calculate_metrics(self, positions: list[Position], asset_price_series):
        """
        Iterates over the given collection of Position objects and calculates metrics
        derived from them.
        
        Parameters
        ----------
        :param positions: 
            'list' : A collection of Position objects.
        """
        
        for pos in iter(positions):
            self.__positions.append(pos)
            tot_entry_cap = pos.entry_price * pos.position_size
            pos_value = tot_entry_cap
            for mtm_return in pos.market_to_market_returns_list:
                self.__equity_list = np.append(
                    self.__equity_list, 
                    round(self.__equity_list[-1] + pos_value * (mtm_return / 100), 2)
                )
                pos_value += pos_value * (mtm_return / 100)
            self.__equity_list[-1] -= pos.commission

            self.__profit_loss_list = np.append(
                self.__profit_loss_list, float(pos.profit_loss)
            )
            self.__returns_list = np.append(
                self.__returns_list, float(pos.position_return)
            )
            self.__market_to_market_returns_list = np.concatenate(
                (self.__market_to_market_returns_list, pos.market_to_market_returns_list), axis=0
            )
            self.__pos_net_results_list = np.append(
                self.__pos_net_results_list, pos.net_result
            )
            self.__pos_gross_results_list = np.append(
                self.__pos_gross_results_list, pos.gross_result
            )
            self.__pos_period_lengths_list = np.append(
                self.__pos_period_lengths_list, len(pos.returns_list)
            )

            if pos.profit_loss > 0:
                self.__profitable_pos_list = np.append(
                    self.__profitable_pos_list, float(pos.profit_loss)
                )
                self.__profitable_pos_returns_list = np.append(
                    self.__profitable_pos_returns_list, pos.position_return
                )
                self.__net_wins_list = np.append(
                    self.__net_wins_list, pos.net_result
                )
                self.__gross_wins_list = np.append(
                    self.__gross_wins_list, pos.gross_result
                )
                self.__w_mae_list = np.append(
                    self.__w_mae_list, float(pos.mae)
                )
                self.__winning_pos_period_lengths_list = np.append(
                    self.__winning_pos_period_lengths_list, len(pos.returns_list)
                )
            if pos.profit_loss <= 0:
                self.__losing_pos_list = np.append(
                    self.__losing_pos_list, float(pos.profit_loss)
                )
                self.__net_losses_list = np.append(
                    self.__net_losses_list, pos.net_result
                )
                self.__gross_losses_list = np.append(
                    self.__gross_losses_list, pos.gross_result
                )
                self.__losing_pos_period_lengths_list = np.append(
                    self.__losing_pos_period_lengths_list, len(pos.returns_list)
                )

            self.__mae_list = np.append(self.__mae_list, float(pos.mae))
            self.__mfe_list = np.append(self.__mfe_list, float(pos.mfe))

        if not len(self.__positions):
            return

        self.__final_capital = int(self.__equity_list[-1])
        if len(self.__profitable_pos_list) == 0:
            self.__pct_wins = 0
        else:
            self.__pct_wins = len(self.__profitable_pos_list) / len(self.__positions) * 100
        if len(self.__losing_pos_list) == 0:
            self.__pct_losses = 0
        else:
            self.__pct_losses = len(self.__losing_pos_list) / len(self.__positions) * 100
        self.__mean_profit_loss = np.mean(self.__profit_loss_list)
        self.__median_profit_loss = np.median(self.__profit_loss_list)
        self.__std_profit_loss = np.std(self.__profit_loss_list)
        self.__mean_return = np.mean(self.__returns_list)
        self.__median_return = np.median(self.__returns_list)
        self.__std_return = np.std(self.__returns_list)

        if len(self.__profitable_pos_list) > 0:
            self.__mean_positive_pos = np.mean(self.__profitable_pos_list)
            self.__median_positive_pos = np.median(self.__profitable_pos_list)
        else:
            self.__mean_positive_pos = np.nan
            self.__median_positive_pos = np.nan
        
        if len(self.__losing_pos_list) > 0:
            self.__mean_negative_pos = np.mean(self.__losing_pos_list)
            self.__median_negative_pos = np.median(self.__losing_pos_list)
        else: 
            self.__mean_negative_pos = np.nan
            self.__median_negative_pos = np.nan

        self.__total_gross_profit = self.__final_capital - self.__start_capital
        self.__avg_pos_net_result = np.mean(self.__pos_net_results_list)

        self.__cagr = self._calculate_cagr()
        self.__max_drawdown = self._calculate_max_drawdown()
        self.__rate_of_return = self._calculate_rate_of_return()
        self.__avg_annual_profit = self._calculate_avg_annual_profit()
        try:
            self.__sharpe_ratio = self._calculate_sharpe_ratio()
        except DivisionByZero:
            self.__sharpe_ratio = np.nan
        self.__expectancy = self._calculate_expectancy()

        if len(self.__gross_wins_list) > 0 and len(self.__gross_losses_list) > 0:
            self.__profit_factor = np.sum(self.__gross_wins_list, dtype=float) / \
                np.abs(np.sum(self.__gross_losses_list, dtype=float))
        elif len(self.__gross_wins_list) > 0 and len(self.__gross_losses_list) < 1:
            self.__profit_factor = np.PINF
        elif len(self.__gross_wins_list) < 1 and len(self.__gross_losses_list) > 0:
            self.__profit_factor = np.NINF
        else:
            self.__profit_factor = np.nan

        try:
            self.__return_to_max_drawdown = self.__rate_of_return / float(self.__max_drawdown)
        except ZeroDivisionError:
            self.__return_to_max_drawdown = 0

        self._mae_mfe_dataframe_apply()

        if asset_price_series:
            underlying_returns = np.array(
                [
                    (asset_price_series[n] - asset_price_series[n-1]) \
                    / asset_price_series[n-1] 
                    for n in range(1, len(asset_price_series))
                ]
            )
            self.underlying_sharpe = calculate_sharpe_ratio(underlying_returns)
            self.underlying_max_dd = calculate_max_drawdown(asset_price_series)
            self.underlying_cagr = calculate_cagr(
                asset_price_series[0], asset_price_series[-1], 
                len(asset_price_series)
            )
        else:
            self.underlying_sharpe = np.nan
            self.underlying_max_dd = np.nan
            self.underlying_cagr = np.nan