import datetime as dt
import json
import argparse

import pandas as pd
from sterunets.data_handler import FeatureBlueprint, TimeSeriesDataHandler

from trading.data.metadata.trading_system_attributes import TradingSystemAttributes
from trading.data.metadata.market_state_enum import MarketState
from trading.trading_system.trading_system import TradingSystem

from trading_systems.trading_system_base import TradingSystemBase, MLTradingSystemBase
from trading_systems.trading_system_properties import TradingSystemProperties, MLTradingSystemProperties
from trading_systems.position_sizer.ext_position_sizer import ExtPositionSizer

from persistance.persistance_meta_classes.signals_persister import SignalsPersisterBase
from persistance.persistance_meta_classes.trading_systems_persister import TradingSystemsPersisterBase
from persistance.persistance_meta_classes.time_series_persister import TimeSeriesPersisterBase
from persistance.stonkinator_mongo_db.systems_mongo_db import TradingSystemsMongoDb
from persistance.stonkinator_mongo_db.time_series_mongo_db import TimeSeriesMongoDb
from persistance.stonkinator_mongo_db.instruments_mongo_db import InstrumentsMongoDb
import trading_systems.env as env


# INSTRUMENTS_DB = InstrumentsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.INSTRUMENTS_DB)
INSTRUMENTS_DB = InstrumentsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.CLIENT_DB)
TIME_SERIES_DB = TimeSeriesMongoDb(env.LOCALHOST_MONGO_DB_URL, env.TIME_SERIES_DB)
SYSTEMS_DB = TradingSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.SYSTEMS_DB)
CLIENT_DB = TradingSystemsMongoDb(env.LOCALHOST_MONGO_DB_URL, env.CLIENT_DB)
# CLIENT_DB = TradingSystemsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)

# INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
#TIME_SERIES_DB = TimeSeriesMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
#SYSTEMS_DB = TradingSystemsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
#CLIENT_DB = TradingSystemsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)


class TradingSystemProcessor:

    def __init__(
        self, ts_class: TradingSystemBase, 
        systems_db: TradingSystemsPersisterBase, 
        client_db: SignalsPersisterBase,
        instruments_db: InstrumentsMongoDb,
        start_dt: dt.datetime, end_dt: dt.datetime,
        full_run=False
    ):
        self.__system_name = ts_class.name
        self.__ts_properties: TradingSystemProperties = ts_class.get_properties(instruments_db)
        self.__systems_db: TradingSystemsPersisterBase = systems_db
        self.__client_db: SignalsPersisterBase = client_db
        self.__trading_system = TradingSystem(
            self.system_name,
            ts_class.entry_signal_logic, 
            ts_class.exit_signal_logic,
            self.__systems_db, self.__client_db
        )
        self.__data, features = ts_class.preprocess_data(
            self.__ts_properties.instruments_list,
            *self.__ts_properties.preprocess_data_args, start_dt, end_dt,
            ts_processor=self
        )

        if issubclass(ts_class, MLTradingSystemBase) == True:
            assert isinstance(self.__ts_properties, MLTradingSystemProperties) == True
            if full_run == True:
                self.__data = ts_class.operate_models(
                    self.__systems_db, self.__data, features,
                    self.__ts_properties.model_class, self.__ts_properties.params
                )
            else:
                if isinstance(features, pd.DataFrame):
                    end_dt = pd.Timestamp(end_dt, tz="UTC")
                    features = features[features.index == end_dt] 

                # TODO: Skip making predictions if there is an active position for the instrument.
                self.__data = ts_class.make_predictions(
                    self.__systems_db, self.__data, features
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
        time_series_db: TimeSeriesPersisterBase=None, insert_into_db=False, 
        **kwargs
    ):
        for i in range(self.__ts_properties.required_runs):
            if full_run == True:
                insert_data = i == self.__ts_properties.required_runs - 1 and insert_into_db
            else:
                insert_data = insert_into_db

            self._run_trading_system(
                full_run, retain_history,
                insert_into_db=insert_data,
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
                self.__systems_db.insert_system_metrics(self.system_name, pos_sizer_data_dict)
                self.__client_db.insert_system_metrics(self.system_name, pos_sizer_data_dict)
            else:
                pos_sizer_data_dict = self.__ts_properties.position_sizer.get_position_sizer_data_dict()
                self.__client_db.update_market_state_data(
                    self.system_name, json.dumps(pos_sizer_data_dict)
                )

    def _run_pos_sizer(self):
        market_states_data: list[dict] = json.loads(
            self.__client_db.get_market_state_data(self.system_name, MarketState.ENTRY.value)
        )
        for data_dict in market_states_data:
            try:
                position_list, num_of_periods = self.__systems_db.get_single_symbol_position_list(
                    self.system_name, data_dict.get(TradingSystemAttributes.SYMBOL),
                    serialized_format=True, return_num_of_periods=True
                )
            except ValueError as e:
                print(e)
                continue

            self.__ts_properties.position_sizer(
                position_list, num_of_periods,
                *self.__ts_properties.position_sizer_call_args,
                symbol=data_dict.get(TradingSystemAttributes.SYMBOL), 
                **self.__ts_properties.position_sizer_call_kwargs,
                **self.__ts_properties.position_sizer.position_sizer_data_dict
            )

    def _run_ext_pos_sizer(self):
        try:
            position_list, num_of_periods = self.__systems_db.get_position_list(
                self.system_name,
                serialized_format=True, return_num_of_periods=True
            )
        except ValueError as e:
            print(e)
            return

        self.__ts_properties.position_sizer(
            position_list, num_of_periods,
            *self.__ts_properties.position_sizer_call_args,
            **self.__ts_properties.position_sizer_call_kwargs,
            **self.__ts_properties.position_sizer.position_sizer_data_dict
        )

    def _check_end_datetime(self, end_dt: dt.datetime):
        end_dt = pd.to_datetime(end_dt).tz_localize('UTC')
        if self.__current_dt != end_dt:
            raise ValueError(
                'Datetime mismatch between position and input data:\n'
                f'current datetime found: {self.__current_dt}\n'
                f'input datetime: {end_dt}'
            )

        last_processed_dt = self.__systems_db.get_current_datetime(self.system_name)
        if last_processed_dt is None:
            return

        last_processed_dt = pd.to_datetime(last_processed_dt).tz_localize('UTC')
        if last_processed_dt != self.__penult_dt:
            raise ValueError(
                'Datetime mismatch between position and input data:\n'
                f'penultimate datetime found: {self.__penult_dt}\n'
                f'last processed datetime: {last_processed_dt}'
            )

    def __call__(
        self, end_dt: dt.datetime, full_run: bool, retain_history: bool,
        time_series_db=None, insert_into_db=False, **kwargs
    ):
        # get new data here and append it to __data member

        # make some date check on given date against last date of self.__data

        # call reprocess_data if new data is available
        # self.reprocess_data(date)

        if full_run != True:
            self._check_end_datetime(end_dt)

        self._handle_trading_system(
            full_run, retain_history,
            time_series_db=time_series_db, 
            insert_into_db=insert_into_db,
            **kwargs
        )

        if full_run != True or full_run == True and retain_history == True:
            self.__systems_db.update_current_datetime(self.system_name, self.__current_dt)


class TradingSystemHandler:

    def __init__(
        self, trading_system_classes: list[TradingSystemBase],
        systems_db: TradingSystemsPersisterBase, 
        client_db: SignalsPersisterBase,
        instruments_db: InstrumentsMongoDb,
        start_dt: dt.datetime, end_dt: dt.datetime, 
        full_run=False
    ):
        self.__trading_systems: list[TradingSystemProcessor] = []
        for ts_class in trading_system_classes:
            self.__trading_systems.append(
                TradingSystemProcessor(
                    ts_class, systems_db, client_db, instruments_db, start_dt, end_dt,
                    full_run=full_run
                )
            )

    def run_trading_systems(
        self, current_datetime: dt.datetime, full_run: bool, retain_history: bool,
        time_series_db=None, print_data=False
    ):
        for trading_system_processor in self.__trading_systems:
            try:
                trading_system_processor(
                    current_datetime, full_run, retain_history,
                    time_series_db=time_series_db, insert_into_db=True,
                    print_data=print_data
                )
            except ValueError as e:
                print(f'ValueError - trading system "{trading_system_processor.system_name}"\n{e}')
                continue


if __name__ == '__main__':
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

    if full_run == True:
        SYSTEMS_DB.drop_collections()
        CLIENT_DB.drop_collections()

    from trading_systems.trading_system_examples.trading_system_example import TradingSystemExample
    from trading_systems.trading_system_examples.ml_trading_system_example import MLTradingSystemExample
    from trading_systems.trading_system_examples.meta_labeling_example import MetaLabelingExample
    from trading_systems.live_systems.stonkinator_flagship import StonkinatorFlagship

    TRADING_SYSTEM_CLASSES = [
        TradingSystemExample,
        MLTradingSystemExample,
        MetaLabelingExample,
        StonkinatorFlagship,
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
                TRADING_SYSTEM_CLASSES,
                SYSTEMS_DB, CLIENT_DB, INSTRUMENTS_DB,
                start_dt, end_dt,
                full_run=full_run
            )
            ts_handler.run_trading_systems(end_dt, full_run, retain_history, print_data=print_data)
            end_dt += dt.timedelta(days=1)
    else:
        ts_handler = TradingSystemHandler(
            TRADING_SYSTEM_CLASSES,
            SYSTEMS_DB, CLIENT_DB, INSTRUMENTS_DB,
            start_dt, end_dt,
            full_run=full_run
        )
        ts_handler.run_trading_systems(end_dt, full_run, retain_history, print_data=print_data)