import os
import datetime as dt
import json
import argparse

import pandas as pd

from trading.data.metadata.trading_system_attributes import TradingSystemAttributes
from trading.data.metadata.market_state_enum import MarketState
from trading.trading_system.trading_system import TradingSystem

from trading_systems.trading_system_base import TradingSystemBase, MLTradingSystemBase
from trading_systems.trading_system_properties import TradingSystemProperties, MLTradingSystemProperties
from trading_systems.position_sizer.ext_position_sizer import ExtPositionSizer

from persistance.persistance_meta_classes.securities_service import SecuritiesServiceBase
from persistance.persistance_meta_classes.trading_systems_persister import TradingSystemsPersisterBase
from persistance.persistance_services.securities_grpc_service import SecuritiesGRPCService
from persistance.persistance_services.trading_systems_grpc_service import TradingSystemsGRPCService


class TradingSystemProcessor:

    def __init__(
        self, ts_class: TradingSystemBase, 
        securities_service: SecuritiesServiceBase,
        trading_systems_persister: TradingSystemsPersisterBase,
        start_dt: dt.datetime, end_dt: dt.datetime,
        full_run=False
    ):
        self.__system_name = ts_class.name
        self.__ts_properties: TradingSystemProperties = ts_class.get_properties(securities_service)
        self.__trading_systems_persister = trading_systems_persister
        self.__data, features = ts_class.preprocess_data(
            securities_service, self.__ts_properties.instruments_list,
            *self.__ts_properties.preprocess_data_args, start_dt, end_dt,
            ts_processor=self
        )
        trading_system_proto = self.__trading_systems_persister.get_or_insert_trading_system(
            self.__system_name, self.__current_dt
        )
        self.__trading_system_id = trading_system_proto.id
        self.__trading_system = TradingSystem(
            self.__trading_system_id,
            self.__system_name,
            ts_class.entry_signal_logic, 
            ts_class.exit_signal_logic,
            self.__trading_systems_persister
        )

        if issubclass(ts_class, MLTradingSystemBase) == True:
            assert isinstance(self.__ts_properties, MLTradingSystemProperties) == True
            if full_run == True:
                self.__data = ts_class.operate_models(
                    self.__trading_system_id, self.__trading_systems_persister, self.__data, features,
                    self.__ts_properties.model_class, self.__ts_properties.params
                )
            else:
                if isinstance(features, pd.DataFrame):
                    end_dt = pd.to_datetime(self.__current_dt).normalize()
                    features = features[features.index == end_dt] 

                # TODO: Skip making predictions if there is an active position for the instrument.
                self.__data = ts_class.make_predictions(
                    self.__trading_system_id, self.__trading_systems_persister, self.__data, features
                )

    @property
    def system_name(self):
        return self.__system_name

    @property
    def penult_dt(self):
        return self.__penult_dt

    @penult_dt.setter
    def penult_dt(self, value):
        self.__penult_dt = value

    @property
    def current_dt(self):
        return self.__current_dt

    @current_dt.setter
    def current_dt(self, value):
        self.__current_dt = value

    def reprocess_data(self, end_dt: dt.datetime):
        # TODO: Implement this method.
        pass

    def _run_trading_system(
        self, full_run, retain_history,
        capital=10000, capital_fraction=1.0, avg_yearly_periods=251,
        run_monte_carlo_sims=False, num_of_sims=2500,
        print_dataframe=False, plot_fig=False, plot_positions=False, write_to_file_path=None,
        save_summary_plot_to_path=False, system_analysis_to_csv_path=None,
        plot_returns_distribution=False,
        print_data=False,
        insert_into_db=False,
        pos_list_slice_years_est=2,
        **kwargs
    ):
        if full_run:
            self.__trading_system.run_trading_system_backtest(
                self.__data,
                capital=capital,
                capital_fraction=capital_fraction,
                avg_yearly_periods=avg_yearly_periods,
                market_state_null_default=retain_history == False,
                plot_performance_summary=plot_fig,
                save_summary_plot_to_path=save_summary_plot_to_path,
                system_analysis_to_csv_path=system_analysis_to_csv_path, 
                plot_returns_distribution=plot_returns_distribution,
                save_returns_distribution_plot_to_path=None, 
                run_monte_carlo_sims=run_monte_carlo_sims,
                num_of_monte_carlo_sims=num_of_sims,
                monte_carlo_data_amount=0.65,
                plot_monte_carlo=plot_fig,
                print_monte_carlo_df=print_dataframe,
                monte_carlo_analysis_to_csv_path=None, 
                print_data=print_data,
                commission_pct_cost=0.0025,
                entry_args=self.__ts_properties.entry_function_args,
                exit_args=self.__ts_properties.exit_function_args,
                fixed_position_size=True,
                generate_signals=True,
                plot_positions=plot_positions,
                save_position_figs_path=None,
                write_signals_to_file_path=write_to_file_path,
                insert_data_to_db_bool=insert_into_db,
                pos_list_slice_years_est=pos_list_slice_years_est
            )
        else:
            self.__trading_system.run_trading_system(
                self.__data,
                capital=capital,
                capital_fraction=capital_fraction,
                commission_pct_cost=0.0025,
                entry_args=self.__ts_properties.entry_function_args,
                exit_args=self.__ts_properties.exit_function_args,
                fixed_position_size=True,
                print_data=print_data,
                write_signals_to_file_path=write_to_file_path,
                insert_data_to_db_bool=insert_into_db
            )

    def _handle_trading_system(
        self, full_run: bool, retain_history: bool,
        insert_into_db=False, **kwargs
    ):
        for i in range(self.__ts_properties.required_runs):
            if full_run == True and insert_into_db == True:
                self.__trading_systems_persister.remove_trading_system_relations(self.__trading_system_id)

            self._run_trading_system(
                full_run, retain_history,
                insert_into_db=insert_into_db,
                **self.__ts_properties.position_sizer.position_sizer_data_dict,
                **self.__ts_properties.ts_run_kwargs,
                **kwargs
            )

            if isinstance(self.__ts_properties.position_sizer, ExtPositionSizer):
                self._run_ext_pos_sizer()
            else:
                self._run_pos_sizer()

            if full_run == False:
                break

        if insert_into_db == True:
            if isinstance(self.__ts_properties.position_sizer, ExtPositionSizer):
                pos_sizer_data_dict = self.__ts_properties.position_sizer.get_position_sizer_data_dict()
                self.__trading_systems_persister.update_trading_system_metrics(
                    self.__trading_system_id, pos_sizer_data_dict
                )
            else:
                pos_sizer_data_dict = self.__ts_properties.position_sizer.get_position_sizer_data_dict()
                for data_dict in pos_sizer_data_dict.get(TradingSystemAttributes.DATA_KEY):
                    self.__trading_systems_persister.upsert_market_state(
                        data_dict.get(TradingSystemAttributes.INSTRUMENT_ID),
                        self.__trading_system_id, data_dict
                    )

    def _run_pos_sizer(self):
        market_states = self.__trading_systems_persister.get_market_states(
            self.__trading_system_id, MarketState.ENTRY.value
        )
        for market_state in market_states:
            instrument_id = market_state.instrument_id
            positions = self.__trading_systems_persister.get_positions(
                instrument_id, self.__trading_system_id
            )
            num_of_periods = json.loads(market_state.metrics).get(TradingSystemAttributes.NUMBER_OF_PERIODS)
            if positions and num_of_periods:
                self.__ts_properties.position_sizer(
                    positions, num_of_periods, instrument_id,
                    *self.__ts_properties.position_sizer_call_args,
                    **self.__ts_properties.position_sizer_call_kwargs,
                    **self.__ts_properties.position_sizer.position_sizer_data_dict
                )

    def _run_ext_pos_sizer(self):
        trading_system_metrics = self.__trading_systems_persister.get_trading_system_metrics(self.__trading_system_id)
        if trading_system_metrics is None:
            return
        num_of_periods = trading_system_metrics.get(TradingSystemAttributes.NUMBER_OF_PERIODS)
        num_of_positions = trading_system_metrics.get(TradingSystemAttributes.NUMBER_OF_POSITIONS)
        positions = self.__trading_systems_persister.get_trading_system_positions(self.__trading_system_id, num_of_positions)
        if positions:
            self.__ts_properties.position_sizer(
                positions, num_of_periods,
                *self.__ts_properties.position_sizer_call_args,
                **self.__ts_properties.position_sizer_call_kwargs,
                **self.__ts_properties.position_sizer.position_sizer_data_dict
            )

    def _check_end_datetime(self, end_dt: dt.datetime):
        end_dt = pd.to_datetime(end_dt)
        if self.__current_dt != end_dt:
            raise ValueError(
                'datetime mismatch between position and input data:\n'
                f'current datetime found: {self.__current_dt}\n'
                f'input datetime: {end_dt}'
            )

        last_processed_dt = self.__trading_systems_persister.get_current_date_time(self.__trading_system_id)
        last_processed_dt = pd.to_datetime(last_processed_dt.date_time)
        if last_processed_dt != self.__penult_dt:
            raise ValueError(
                'datetime mismatch between position and input data:\n'
                f'penultimate datetime found: {self.__penult_dt}\n'
                f'last processed datetime: {last_processed_dt}'
            )

    def __call__(
        self, end_dt: dt.datetime, full_run: bool, retain_history: bool,
        insert_into_db=False, **kwargs
    ):
        # get new data here and append it to __data member

        # call reprocess_data if new data is available
        # self.reprocess_data(date)

        if full_run != True:
            self._check_end_datetime(end_dt)

        self._handle_trading_system(full_run, retain_history, insert_into_db=insert_into_db, **kwargs)

        if full_run != True or full_run == True and retain_history == True:
            self.__trading_systems_persister.update_current_date_time(
                self.__trading_system_id, self.__current_dt
            )


class TradingSystemHandler:

    def __init__(
        self, trading_system_classes: list[TradingSystemBase],
        securities_service: SecuritiesServiceBase,
        trading_systems_persister: TradingSystemsPersisterBase, 
        start_dt: dt.datetime, end_dt: dt.datetime, 
        full_run=False
    ):
        self.__trading_systems: list[TradingSystemProcessor] = []
        for ts_class in trading_system_classes:
            self.__trading_systems.append(
                TradingSystemProcessor(
                    ts_class, securities_service, trading_systems_persister, start_dt, end_dt,
                    full_run=full_run
                )
            )

    def run_trading_systems(
        self, current_datetime: dt.datetime, full_run: bool, retain_history: bool,
        print_data=False
    ):
        for trading_system_processor in self.__trading_systems:
            try:
                trading_system_processor(
                    current_datetime, full_run, retain_history,
                    insert_into_db=True, print_data=print_data
                )
            except ValueError as e:
                print(f'ValueError - trading system "{trading_system_processor.system_name}"\n{e}')
                continue


if __name__ == '__main__':
    SECURITIES_GRPC_SERVICE = SecuritiesGRPCService(
        f"{os.environ.get('RPC_SERVICE_HOST')}:{os.environ.get('RPC_SERVICE_PORT')}"
    )
    TRADING_SYSTEMS_GRPC_SERVICE = TradingSystemsGRPCService(
        f"{os.environ.get('RPC_SERVICE_HOST')}:{os.environ.get('RPC_SERVICE_PORT')}"
    )

    arg_parser = argparse.ArgumentParser(description='trading_system_handler CLI argument parser')
    arg_parser.add_argument(
        '--full-run', action='store_true', dest='full_run',
        help='Run trading systems from the date of the latest exit of each instrument',
    )
    arg_parser.add_argument(
        '--retain-history', action='store_true', dest='retain_history',
        help='Retain orders and positions from a full run of the trading systems',
    )
    arg_parser.add_argument(
        '--print-data', action='store_true', dest='print_data',
        help='Print position and trading system data while running the program',
    )
    arg_parser.add_argument(
        '--step-through', action='store_true', dest='step_through',
        help='Step through while incrementing the datetime period variable.',
    )

    cli_args = arg_parser.parse_args()
    full_run = cli_args.full_run
    retain_history = cli_args.retain_history
    print_data = cli_args.print_data
    step_through = cli_args.step_through

    from trading_systems.trading_system_examples.trading_system_example import TradingSystemExample
    # from trading_systems.trading_system_examples.ml_trading_system_example import MLTradingSystemExample
    # from trading_systems.trading_system_examples.meta_labeling_example import MetaLabelingExample
    # from trading_systems.live_systems.stonkinator_flagship import StonkinatorFlagship

    TRADING_SYSTEM_CLASSES = [
        TradingSystemExample,
        # MLTradingSystemExample,
        # MetaLabelingExample,
        # StonkinatorFlagship,
    ]

    # start_dt = dt.datetime(1999, 1, 1)
    # end_dt = dt.datetime(2011, 1, 1)
    start_dt = dt.datetime(2015, 9, 16)
    # end_dt = dt.datetime.now()
    end_dt = dt.datetime(2023, 3, 8)

    if step_through:
        periods = (dt.datetime.now() - end_dt).days
        for _ in range(periods):
            ts_handler = TradingSystemHandler(
                TRADING_SYSTEM_CLASSES, SECURITIES_GRPC_SERVICE, TRADING_SYSTEMS_GRPC_SERVICE,
                start_dt, end_dt, full_run=full_run
            )
            ts_handler.run_trading_systems(end_dt, full_run, retain_history, print_data=print_data)
            end_dt += dt.timedelta(days=1)
    else:
        ts_handler = TradingSystemHandler(
            TRADING_SYSTEM_CLASSES, SECURITIES_GRPC_SERVICE, TRADING_SYSTEMS_GRPC_SERVICE,
            start_dt, end_dt, full_run=full_run
        )
        ts_handler.run_trading_systems(end_dt, full_run, retain_history, print_data=print_data)