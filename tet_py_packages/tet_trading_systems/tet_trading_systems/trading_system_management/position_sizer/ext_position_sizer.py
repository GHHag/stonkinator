import random
from typing import Dict, List

import pandas as pd

from TETrading.data.metadata.trading_system_metrics import TradingSystemMetrics
from TETrading.position.position import Position
from TETrading.position.position_manager import PositionManager
from TETrading.utils.metric_functions import calculate_cagr
from TETrading.utils.monte_carlo_functions import monte_carlo_simulations_plot

from tet_trading_systems.trading_system_management.position_sizer.position_sizer \
    import IPositionSizer


class ExtPositionSizer(IPositionSizer):

    __POSITION_SIZE_METRIC_STR = 'safe-f'
    __CAPITAL_FRACTION = 'capital_fraction'
    __PERSISTANT_SAFE_F = 'persistant_safe_f'
    __CAR25 = 'car25'
    __CAR75 = 'car75'

    def __init__(
        self, tolerated_pct_max_drawdown, max_drawdown_percentile_threshold,
        *args, **kwargs
    ):
        self.__tol_pct_max_dd = tolerated_pct_max_drawdown
        self.__max_dd_pctl_threshold = max_drawdown_percentile_threshold
        self.__position_sizer_data_dict = {}
        self.__args = args
        self.__kwargs = kwargs

    @property
    def position_size_metric_str(self):
        return self.__POSITION_SIZE_METRIC_STR

    @property
    def position_sizer_data_dict(self) -> Dict:
        return self.__position_sizer_data_dict

    def get_position_sizer_data_dict(self) -> Dict:
        return self.__position_sizer_data_dict

    def _monte_carlo_simulate_pos_sequence(
        self, positions: List[Position], num_testing_periods, start_capital,
        capital_fraction=1.0, num_of_sims=1000, data_fraction_used=0.66, 
        symbol='', print_dataframe=False, plot_fig=False, **kwargs
    ):
        monte_carlo_sims_df = pd.DataFrame()
        equity_curves_list = []
        final_equity_list = []
        max_drawdowns_list = []
        sim_positions = None

        def generate_pos_sequence(position_list, **kwargs):
            for pos in position_list[:int(len(position_list) * data_fraction_used + 0.5)]:
                yield pos

        for _ in range(num_of_sims):
            sim_positions = PositionManager(
                symbol, int(num_testing_periods * data_fraction_used + 0.5), start_capital,
                capital_fraction
            )

            pos_list = random.sample(positions, len(positions))
            sim_positions.generate_positions(generate_pos_sequence, pos_list)
            monte_carlo_sims_df = monte_carlo_sims_df.append(
                sim_positions.metrics.summary_data_dict, ignore_index=True
            )
            final_equity_list.append(float(sim_positions.metrics.equity_list[-1]))

            max_drawdowns_list.append(sim_positions.metrics.max_drawdown)

            equity_curves_list.append(sim_positions.metrics.equity_list)

        final_equity_list = sorted(final_equity_list)

        car25 = calculate_cagr(
            sim_positions.metrics.start_capital, 
            final_equity_list[(int(len(final_equity_list) * 0.25))],
            sim_positions.metrics.num_testing_periods
        )
        car75 = calculate_cagr(
            sim_positions.metrics.start_capital,
            final_equity_list[(int(len(final_equity_list) * 0.75))],
            sim_positions.metrics.num_testing_periods
        )

        car_series = pd.Series()
        car_series[self.__CAR25] = car25
        car_series[self.__CAR75] = car75
        monte_carlo_sims_df: pd.DataFrame = monte_carlo_sims_df.append(car_series, ignore_index=True)

        if print_dataframe:
            print(monte_carlo_sims_df.to_string())
        if plot_fig:
            monte_carlo_simulations_plot(
                symbol, equity_curves_list, max_drawdowns_list, final_equity_list,
                capital_fraction, car25, car75
            )

        return monte_carlo_sims_df

    def __call__(
        self, position_list: List[Position], num_of_periods, 
        persistant_safe_f=None, capital=10000, forecast_data_fraction=0.66,
        **kwargs
    ):
        position_list.sort(key=lambda pos: pos.entry_dt)

        monte_carlo_sims_df: pd.DataFrame = self._monte_carlo_simulate_pos_sequence(
            position_list, num_of_periods, capital, 
            data_fraction_used=forecast_data_fraction,
            **kwargs
        )

        max_dds = sorted(monte_carlo_sims_df[TradingSystemMetrics.MAX_DRAWDOWN].to_list())
        dd_at_tolerated_threshold = max_dds[int(len(max_dds) * self.__max_dd_pctl_threshold)]
        if dd_at_tolerated_threshold < 1:
            dd_at_tolerated_threshold = 1

        safe_f = persistant_safe_f if persistant_safe_f else \
            self.__tol_pct_max_dd / dd_at_tolerated_threshold

        self.__position_sizer_data_dict[self.__POSITION_SIZE_METRIC_STR] = safe_f
        self.__position_sizer_data_dict[self.__CAPITAL_FRACTION] = safe_f
        self.__position_sizer_data_dict[self.__PERSISTANT_SAFE_F] = safe_f
        self.__position_sizer_data_dict[self.__CAR25] = monte_carlo_sims_df.iloc[-1][self.__CAR25]
        self.__position_sizer_data_dict[self.__CAR75] = monte_carlo_sims_df.iloc[-1][self.__CAR75]
