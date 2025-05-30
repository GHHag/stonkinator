import random

import pandas as pd

from trading.data.metadata.trading_system_attributes import TradingSystemAttributes
from trading.data.metadata.trading_system_metrics import TradingSystemMetrics
from trading.position.position import Position
from trading.position.position_manager import PositionManager
from trading.utils.metric_functions import calculate_cagr
from trading.utils.monte_carlo_functions import monte_carlo_simulations_plot

from trading_systems.position_sizer.position_sizer import PositionSizer


class SafeFPositionSizer(PositionSizer):

    __POSITION_SIZE_METRIC_STR = 'safe-f'
    __CAPITAL_FRACTION = 'capital_fraction'
    __PERSISTANT_SAFE_F = 'persistant_safe_f'
    __CAR25 = 'car25'
    __CAR75 = 'car75'

    def __init__(self, tolerated_pct_max_drawdown, max_drawdown_percentile_threshold):
        self.__tol_pct_max_dd = tolerated_pct_max_drawdown
        self.__max_dd_pctl_threshold = max_drawdown_percentile_threshold
        self.__position_sizer_data_dict = {
            self.__POSITION_SIZE_METRIC_STR: {},
            self.__CAPITAL_FRACTION: {}, 
            self.__PERSISTANT_SAFE_F: {},
            self.__CAR25: {},
            self.__CAR75: {}
        }

    @property
    def position_size_metric_str(self):
        return self.__POSITION_SIZE_METRIC_STR

    @property
    def position_sizer_data_dict(self) -> dict:
        return self.__position_sizer_data_dict

    def get_position_sizer_data_dict(self) -> dict:
        pos_sizer_data = {}
        for k, v in self.__position_sizer_data_dict.items():
            for ki, vi in v.items():
                if not ki in pos_sizer_data:
                    pos_sizer_data[ki] = {}
                    pos_sizer_data[ki][TradingSystemAttributes.INSTRUMENT_ID] = ki
                pos_sizer_data[ki][k] = vi

        return {TradingSystemAttributes.DATA_KEY: list(pos_sizer_data.values())}

    def _monte_carlo_simulate_pos_sequence(
        self, positions: list[Position], num_testing_periods, start_capital, instrument_id,
        capital_fraction=1.0, num_of_sims=1000, data_fraction_used=0.66,
        print_dataframe=False, plot_fig=False, **kwargs
    ):
        monte_carlo_sims_df = pd.DataFrame()
        final_equity_list = []
        max_drawdowns_list = []
        equity_curves_list = []
        sim_positions = None

        def generate_position_sequence(position_list, **kw):
            for pos in position_list[:int(len(position_list) * data_fraction_used + 0.5)]:
                yield pos

        for _ in range(num_of_sims):
            sim_positions = PositionManager(
                instrument_id, int(num_testing_periods * data_fraction_used + 0.5), start_capital,
                capital_fraction
            )

            pos_list = random.sample(positions, len(positions))
            sim_positions.generate_positions(generate_position_sequence, pos_list)
            monte_carlo_sims_df: pd.DataFrame = pd.concat(
                [monte_carlo_sims_df, pd.DataFrame([sim_positions.metrics.summary_data_dict])], 
                ignore_index=True
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

        car_df = pd.DataFrame.from_dict({'car25': [car25], 'car75': [car75]})
        monte_carlo_sims_df = pd.concat([monte_carlo_sims_df, car_df], ignore_index=True)

        if print_dataframe:
            print(monte_carlo_sims_df.to_string())
        if plot_fig:
            monte_carlo_simulations_plot(
                instrument_id, equity_curves_list, max_drawdowns_list, final_equity_list,
                capital_fraction, car25, car75
            )

        return monte_carlo_sims_df

    def __call__(
        self, position_list: list[Position], num_of_periods, instrument_id,
        avg_yearly_periods=251, years_to_forecast=2, persistant_safe_f=None,
        capital=10000, num_of_sims=2500, plot_fig=False, 
        **kwargs
    ):
        position_list = position_list if position_list[-1].entry_dt else position_list[:-1]

        try:
            avg_yearly_positions = len(position_list) / (num_of_periods / avg_yearly_periods)
            forecast_positions = avg_yearly_positions * (years_to_forecast * 1.5)
            forecast_data_fraction = (avg_yearly_positions * years_to_forecast) / forecast_positions
        except ZeroDivisionError:
            return {}

        # sort positions on date
        position_list.sort(key=lambda pos: pos.entry_dt)

        # simulate sequences of given Position objects
        monte_carlo_sims_df: pd.DataFrame = self._monte_carlo_simulate_pos_sequence(
            position_list, num_of_periods, capital, instrument_id,
            capital_fraction=persistant_safe_f[instrument_id] if instrument_id in persistant_safe_f else 1.0,
            num_of_sims=num_of_sims, data_fraction_used=forecast_data_fraction, plot_fig=plot_fig 
        )

        # sort the Max drawdown column and convert to a list
        max_dds = sorted(monte_carlo_sims_df[TradingSystemMetrics.MAX_DRAWDOWN].to_list())
        # get the drawdown value at the percentile set to be the threshold at which to limit the 
        # probability of getting a max drawdown of that magnitude at when simulating sequences 
        # of the best estimate positions
        dd_at_tolerated_threshold = max_dds[int(len(max_dds) * self.__max_dd_pctl_threshold)]
        if dd_at_tolerated_threshold < 1:
            dd_at_tolerated_threshold = 1

        if not instrument_id in persistant_safe_f:
            safe_f = self.__tol_pct_max_dd / dd_at_tolerated_threshold
        else:
            safe_f = persistant_safe_f[instrument_id]

        self.__position_sizer_data_dict[self.__POSITION_SIZE_METRIC_STR][instrument_id] = safe_f
        self.__position_sizer_data_dict[self.__CAPITAL_FRACTION][instrument_id] = safe_f
        self.__position_sizer_data_dict[self.__PERSISTANT_SAFE_F][instrument_id] = safe_f
        self.__position_sizer_data_dict[self.__CAR25][instrument_id] = monte_carlo_sims_df.iloc[-1][self.__CAR25]
        self.__position_sizer_data_dict[self.__CAR75][instrument_id] = monte_carlo_sims_df.iloc[-1][self.__CAR75]
