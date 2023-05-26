import datetime as dt
import json

import pandas as pd
import numpy as np

from securities_db_py_dal.dal import price_data_get_req

from tet_doc_db.tet_mongo_db.systems_mongo_db import TetSystemsMongoDb
from tet_doc_db.instruments_mongo_db.instruments_mongo_db import InstrumentsMongoDb

from tet_trading_systems.trading_system_development.trading_systems.trading_system_properties.trading_system_properties import TradingSystemProperties
from tet_trading_systems.trading_system_development.trading_systems.run_trading_systems import run_trading_system
from tet_trading_systems.trading_system_development.trading_systems.trading_system_handler import handle_trading_system
from tet_trading_systems.trading_system_development.trading_systems.trading_system_handler import handle_ext_pos_sizer_trading_system
from tet_trading_systems.trading_system_state_handler.trad_trading_system_state_handler import TradingSystemStateHandler
from tet_trading_systems.trading_system_management.position_sizer.safe_f_position_sizer import SafeFPositionSizer
from tet_trading_systems.trading_system_management.position_sizer.ext_position_sizer import ExtPositionSizer
from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.standard_indicators \
    import apply_rsi, apply_atr, apply_comparative_relative_strength
from tet_trading_systems.trading_system_state_handler.instrument_selection.pd_instrument_selector import PdInstrumentSelector
from tet_trading_systems.trading_system_state_handler.portfolio.portfolio import Portfolio
from tet_trading_systems.trading_system_state_handler.portfolio.metric_ranking_portfolio_creator import MetricRankingPortfolioCreator
from tet_trading_systems.trading_system_state_handler.order_execution.order_execution_handler import AvanzaOrderExecutionHandler


def rsi_divergence_entry(df, *args, entry_args=None):
    return min(df[f'RSI_{entry_args["rsi_period_param"]}'].iloc[-entry_args['divergence_periods']:]) < \
        df[f'RSI_{entry_args["rsi_period_param"]}'].iloc[-1] < entry_args['rsi_entry_param'] and \
        df['Close'].iloc[-1] <= min(df['Close'].iloc[-entry_args['divergence_periods']:]), 'long'


def n_period_rw_rsi_target_trail_atr_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    if not trail and np.mean(df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-2:]) >= 50:
        trail = True
        trailing_exit_price = df['Close'].iloc[-1]

    if trail and trailing_exit_price is not None and trailing_exit_price > df['Close'].iloc[-1] or \
        df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-1] > exit_args['exit_rsi_value'] or \
            df['Close'].iloc[-1] < float(entry_price) - \
                (df['ATR'].iloc[-1] * exit_args['atr_multiplier_trail']) or \
            exit_args['n_period_exit'] == periods_in_pos and \
                df['CRS'].iloc[-1] < df['CRS'].iloc[-exit_args['n_period_exit']]:
        return True, False, None
    else:
        return False, True, trailing_exit_price


def preprocess_data(
    symbols_list, benchmark_symbol, get_data_function, 
    entry_args, exit_args, start_dt, end_dt, 
):
    df_dict = {
        symbol: pd.json_normalize(
            get_data_function(symbol, start_dt, end_dt)['data']
        )
        for symbol in symbols_list
    }

    benchmark_col_suffix = '_benchmark'
    df_benchmark = pd.json_normalize(
        get_data_function(benchmark_symbol, start_dt, end_dt)['data']
    ).rename(
        columns={
            'Open': f'Open{benchmark_col_suffix}', 
            'High': f'High{benchmark_col_suffix}', 
            'Low': f'Low{benchmark_col_suffix}', 
            'Close': f'Close{benchmark_col_suffix}',
            'Volume': f'Volume{benchmark_col_suffix}', 
            'symbol': f'symbol{benchmark_col_suffix}'
        }
    )

    for symbol, data in dict(df_dict).items():
        if data.empty or len(data) < entry_args['req_period_iters']:
            print(symbol, 'DataFrame empty')
            del df_dict[symbol]
        else:
            df_dict[symbol] = pd.merge_ordered(data, df_benchmark, on='Date', how='inner')
            df_dict[symbol].fillna(method='ffill', inplace=True)
            df_dict[symbol]['Date'] = pd.to_datetime(df_dict[symbol]['Date'])
            df_dict[symbol].set_index('Date', inplace=True)

            # apply indicators/features to dataframe
            apply_rsi(df_dict[symbol], period_param=entry_args['rsi_period_param'])
            apply_atr(df_dict[symbol], period_param=exit_args['atr_period_param'])
            apply_comparative_relative_strength(df_dict[symbol], 'Close', 'Close_benchmark')
            df_dict[symbol].dropna(inplace=True)

    return df_dict, None


def get_props(instruments_db: InstrumentsMongoDb, import_instruments=False, path=None):
    system_name = 'mean_reversion_stocks'
    benchmark_symbol = '^OMX'
    entry_args = {
        'req_period_iters': 15, 'rsi_period_param': 5,
        'rsi_entry_param': 35, 'divergence_periods': 15,
        'req_period_iters': 15
    }
    exit_args = {
        'rsi_period_param': 5, 'exit_param_period': 5,
        'n_period_exit': 5, 'exit_rsi_mean': 65,
        'exit_rsi_value': 65, 'atr_period_param': 14,
        'atr_multiplier_trail': 2.5
    }
    
    if import_instruments:
        backtest_df = pd.read_csv(f'{path}/{system_name}.csv') if path else \
            pd.read_csv(f'./backtests/{system_name}')
        instrument_selector = PdInstrumentSelector('sharpe_ratio', backtest_df, 0.9)
        instrument_selector()
        symbols_list = instrument_selector.selected_instruments
    else:
        market_list_ids = [
            #instruments_db.get_market_list_id('omxs30')
            instruments_db.get_market_list_id('omxs_large_caps'),
            instruments_db.get_market_list_id('omxs_mid_caps')
        ]
        symbols_list = []
        #symbols_list = ['VOLV_B', 'HM_B', 'NDA_SE']
        for market_list_id in market_list_ids:
            symbols_list += json.loads(
                instruments_db.get_market_list_instrument_symbols(
                    market_list_id
                )
            )

    return TradingSystemProperties(
        system_name, 2,
        preprocess_data,  
        (
            symbols_list, benchmark_symbol, price_data_get_req, 
            entry_args, exit_args
        ),
        handle_trading_system,
        #handle_ext_pos_sizer_trading_system,
        TradingSystemStateHandler, (system_name, None),
        (
            run_trading_system,
            rsi_divergence_entry, n_period_rw_rsi_target_trail_atr_exit,
            entry_args, exit_args
        ),
        {'run_monte_carlo_sims': False, 'print_dataframe': False},
        None, (), (), #Portfolio
        SafeFPositionSizer, (20, 0.8), (),
        #ExtPositionSizer, (20, 0.8), (),
        {
            'forecast_data_fraction': 0.7,
            'num_of_sims': 100
        }
    )


if __name__ == '__main__':
    import tet_trading_systems.trading_system_development.trading_systems.env as env
    #SYSTEMS_DB = TetSystemsMongoDb('mongodb://localhost:27017/', 'systems_db')
    #INSTRUMENTS_DB = InstrumentsMongoDb('mongodb://localhost:27017/', 'instruments_db')
    SYSTEMS_DB = TetSystemsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)
    INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, env.CLIENT_DB)

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)

    system_props = get_props(INSTRUMENTS_DB, import_instruments=False)
    system_name = 'mean_reversion_stocks'

    df_dict, features = system_props.preprocess_data_function(
        system_props.preprocess_data_args[0], '^OMX', 
        price_data_get_req,
        system_props.preprocess_data_args[-2], 
        system_props.preprocess_data_args[-1], 
        start_dt, end_dt, 
    )

    #run_ext_pos_sizer_trading_system(
    run_trading_system(
        df_dict, system_name, 
        rsi_divergence_entry, n_period_rw_rsi_target_trail_atr_exit,
        system_props.preprocess_data_args[-2], 
        system_props.preprocess_data_args[-1], 
        #ExtPositionSizer('sharpe_ratio'),
        SafeFPositionSizer(20, 0.8),
        plot_fig=False,
        # add check if dir exists before running, create it if not
        system_analysis_to_csv_path=f'./backtests/{system_name}.csv',
        systems_db=SYSTEMS_DB, client_db=SYSTEMS_DB, insert_into_db=False
    )
