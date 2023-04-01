import datetime as dt
import json

import pandas as pd

from securities_db_py_dal.dal import price_data_get_req

from tet_doc_db.tet_mongo_db.systems_mongo_db import TetSystemsMongoDb
from tet_doc_db.instruments_mongo_db.instruments_mongo_db import InstrumentsMongoDb

from tet_trading_systems.trading_system_development.trading_systems.trading_system_properties.trading_system_properties \
    import TradingSystemProperties
from tet_trading_systems.trading_system_development.trading_systems.run_trading_systems import run_trading_system
from tet_trading_systems.trading_system_development.trading_systems.trading_system_handler import handle_trading_system
from tet_trading_systems.trading_system_management.position_sizer.safe_f_position_sizer import SafeFPositionSizer
from tet_trading_systems.trading_system_state_handler.trad_trading_system_state_handler import TradingSystemStateHandler
from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.standard_indicators \
    import apply_comparative_relative_strength, apply_rsi, apply_atr
from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.misc_features \
    import apply_percent_rank
from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.volume_features \
    import apply_vwap, apply_vwap_from_n_period_low


SYSTEM_NAME= 'crs_ma'

"""
definiera marknadsregimer:
volatilt ner, trend upp, sidleds
aggregerade % rank som classifier(vwap, pris, crs?)
"""


def crs_ma_cross_entry(df, *args, entry_args=None):
    return df['CRS_fast_MA'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1], 'long'


def crs_ma_cross_pct_rank_regime_filter_entry(df, *args, entry_args=None):
    return df['CRS_fast_MA'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] and \
        df['%_rank'].iloc[-1] >= entry_args['pct_rank_clf_param'], 'long'# and \
        #df[f'RSI_{entry_args["rsi_period_param"]}'].iloc[-1] < 65, 'long'


#def crs_ma_cross_benchmark_rsi_filter_entry(df, *args, entry_args=None):
#    return df['CRS_fast_MA'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] and \
#           df[f'RSI_{14}_benchmark'].iloc[-1] >= 50, 'long'


def crs_ma_classifier_entry(df, *args, entry_args=None):
    return df['CRS'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1], 'long'


def crs_ma_classifier_pct_rank_regime_filter_entry(df, *args, entry_args=None):
    return df['CRS'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] and \
        df['%_rank'].iloc[-1] >= entry_args['pct_rank_clf_param'] and \
        df[f'RSI_{entry_args["rsi_period_param"]}'].iloc[-1] < 65, 'long'


#def crs_ma_classifier_benchmark_rsi_filter_entry(df, *args, entry_args=None):
#    return df['CRS'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] and \
#           df[f'RSI_{14}_benchmark'].iloc[-1] >= 50, 'long'


def crs_ma_crossover_entry(df, *args, entry_args=None):
    return df['CRS_fast_MA'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] and \
           df['CRS_fast_MA'].iloc[-2] <= df['CRS_slow_MA'].iloc[-2], 'long'


def crs_ma_crossover_pct_rank_regime_filter_entry(df, *args, entry_args=None):
    return df['CRS_fast_MA'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] and \
           df['CRS_fast_MA'].iloc[-2] <= df['CRS_slow_MA'].iloc[-2] and \
           df['%_rank'].iloc[-1] >= entry_args['pct_rank_clf_param'], 'long'


#def crs_ma_crossover_benchmark_rsi_filter_entry(df, *args, entry_args=None):
#    return df['CRS_fast_MA'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] and \
#           df['CRS_fast_MA'].iloc[-2] <= df['CRS_slow_MA'].iloc[-2] and \
#           df[f'RSI_{14}_benchmark'].iloc[-1] >= 50, 'long'


def crs_ma_classifier_cross_entry(df, *args, entry_args=None):
    return df['CRS'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] and \
           df['CRS'].iloc[-2] <= df['CRS_slow_MA'].iloc[-2], 'long'


def crs_ma_classifier_cross_pct_rank_regime_filter_entry(df, *args, entry_args=None):
    return df['CRS'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] and \
           df['CRS'].iloc[-2] <= df['CRS_slow_MA'].iloc[-2] and \
           df['%_rank'].iloc[-1] >= entry_args['pct_rank_clf_param'], 'long'


#def crs_ma_classifier_cross_benchmark_rsi_filter_entry(df, *args, entry_args=None):
#    return df['CRS'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] and \
#           df['CRS'].iloc[-2] <= df['CRS_slow_MA'].iloc[-2] and \
#           df[f'RSI_{14}_benchmark'].iloc[-1] >= 50, 'long'


def n_period_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    return periods_in_pos == exit_args['exit_param_period']


def crs_ma_cross_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    return df['CRS_fast_MA'].iloc[-1] < df['CRS_slow_MA'].iloc[-1], trail, trailing_exit_price


def crs_ma_cross_pct_rank_classifier_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    return df['CRS_fast_MA'].iloc[-1] < df['CRS_slow_MA'].iloc[-1] or \
        df['%_rank'].iloc[-1] < exit_args['pct_rank_clf_param'], trail, trailing_exit_price


def crs_n_period_ma_cross_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    exit_condition = False
    for i in range(exit_args['exit_period_param']):
        if df['CRS_fast_MA'].iloc[-(i+1)] < df['CRS_slow_MA'].iloc[-(i+1)]:
            exit_condition = True
        else:
            exit_condition = False
            break

    return exit_condition, trail, trailing_exit_price


def crs_ma_classifier_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    return df['CRS'].iloc[-1] < df['CRS_slow_MA'].iloc[-1], trail, trailing_exit_price


def crs_ma_pct_rank_classifier_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    return df['CRS'].iloc[-1] < df['CRS_slow_MA'].iloc[-1] or \
        df['%_rank'].iloc[-1] < exit_args['pct_rank_clf_param'],  trail, trailing_exit_price


def crs_n_period_ma_classifier_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    exit_condition = False
    for i in range(exit_args['exit_period_param']):
        if df['CRS'].iloc[-(i+1)] < df['CRS_slow_MA'].iloc[-(i+1)]:
            exit_condition = True
        else:
            exit_condition = False
            break

    return exit_condition or df['%_rank'].iloc[-1] < exit_args['pct_rank_clf_param'], trail, trailing_exit_price


def crs_std_multiplier_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    return df['CRS'].iloc[-1] < df['CRS_slow_MA'].iloc[-1] and \
        df['CRS'].iloc[-1] < df['CRS_slow_MA'].iloc[-1] - \
        df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier'], \
        trail, trailing_exit_price


def crs_std_multiplier_trailing_exit(
    df, trail, trailing_exit, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    if not trail:
        trail = True
        trailing_exit = df['CRS'].iloc[-1] - df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier']

    if trail and trailing_exit > df['CRS'].iloc[-1]:
        return True, False, None
    else:
        trailing_exit = df['CRS'].iloc[-1] - df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier']
        #new_trailing_exit = df['CRS'].iloc[-1] - df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier']
        #trailing_exit = new_trailing_exit if new_trailing_exit > trailing_exit else trailing_exit
        return False, trail, trailing_exit


def crs_fixed_std_multiplier_trailing_exit(
    df, trail, trailing_exit, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    print(df[['Close', 'CRS', 'CRS_slow_MA', f'RSI_{exit_args["rsi_period_param"]}', '%_rank', f'VWAP_{exit_args["vwap_period_param"]}']].tail(periods_in_pos).to_string())
    print(trailing_exit)
    if not trail:
        trail = True
        trailing_exit = df['CRS'].iloc[-1] - df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier']

    if trail and trailing_exit > df['CRS'].iloc[-1] or \
        df['Close'].iloc[-2] < (df[f'VWAP_{exit_args["vwap_period_param"]}'].iloc[-2]) and \
        df['Close'].iloc[-1] < (df[f'VWAP_{exit_args["vwap_period_param"]}'].iloc[-1]) and \
        periods_in_pos > exit_args['exit_period_param'] or \
        df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-1] > 75: #or \
        #df['Close'].iloc[-1] >= max(df['Close'].iloc[-21:]) and \
        #df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-1] < max(df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-21:]) and \
        #df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-1] >= 60:# or \
        #df['%_rank'].iloc[-1] < exit_args['pct_rank_clf_param'] and periods_in_pos > 10:
        #df['Close'].iloc[-1] >= max(df['Close'].iloc[-21:]) and \
        return True, False, None
    else:
        if trailing_exit < df['CRS'].iloc[-1] - df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier']:
            trailing_exit = df['CRS'].iloc[-1] - df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier']
        return False, trail, trailing_exit


def crs_std_price_std_multiplier_trailing_exit(
    df, trail, trailing_exit, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    if not trail:
        trail = True
        trailing_exit = [df['CRS'].iloc[-1] - (df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier']),
                         df['Close'].iloc[-1] - (df['ATR'].iloc[-1] * exit_args['atr_std_exit_multiplier'])]

    if trail and trailing_exit[0] > df['CRS'].iloc[-1] or trail and trailing_exit[1] > df['Close'].iloc[-1] or \
            df['CRS'].iloc[-1] < df['CRS_slow_MA'].iloc[-1] and \
            df['CRS'].iloc[-2] < df['CRS_slow_MA'].iloc[-2] and \
            df['CRS'].iloc[-3] < df['CRS_slow_MA'].iloc[-3]:
        return True, False, None
    else:
        if trailing_exit[0] < df['CRS'].iloc[-1] - (df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier']):
            trailing_exit[0] = df['CRS'].iloc[-1] - (df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier'])
        if trailing_exit[1] < df['Close'].iloc[-1] - (df['ATR'].iloc[-1] * exit_args['atr_std_exit_multiplier']):
            trailing_exit[1] = df['Close'].iloc[-1] - (df['ATR'].iloc[-1] * exit_args['atr_std_exit_multiplier'])
        return False, trail, trailing_exit


def crs_std_price_std_multiplier_trailing_target_exit(
    df, trail, trailing_exit, entry_price, periods_in_pos, *args, 
    exit_args=None
):
    # add pct rank trailing exit condition
    if not trail:
        trail = True
        trailing_exit = [df['CRS'].iloc[-1] - (df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier']),
            df['Close'].iloc[-1] - (df['ATR'].iloc[-1] * exit_args['atr_std_exit_multiplier'])]
        return False, trail, trailing_exit

    if trail and trailing_exit[0] > df['CRS'].iloc[-1] or trail and trailing_exit[1] > df['Close'].iloc[-1] or \
        df['CRS'].iloc[-1] >= df['CRS_slow_MA'].iloc[-1] + df['n_period_crs_std'].iloc[-1] * 2.5 or \
        df['Close'].iloc[-1] < df[f'VWAP_{exit_args["vwap_period_param"]}'].iloc[-1] and \
        periods_in_pos >= 5 or \
        df['Close'].iloc[-1] <= df['Close'].iloc[-2] - df['ATR'].iloc[-1] * 2:
        #df['High'].iloc[-1] >= float(entry_price) + df['ATR'].iloc[-1] * 8:
        return True, False, None
    else:
        if trailing_exit[0] < df['CRS'].iloc[-1] - (df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier']):
            trailing_exit[0] = df['CRS'].iloc[-1] - (df['n_period_crs_std'].iloc[-1] * exit_args['crs_std_exit_multiplier'])
        if trailing_exit[1] < df['Close'].iloc[-1] - (df['ATR'].iloc[-1] * exit_args['atr_std_exit_multiplier']):
            trailing_exit[1] = df['Close'].iloc[-1] - (df['ATR'].iloc[-1] * exit_args['atr_std_exit_multiplier'])
        return False, trail, trailing_exit


def preprocess_data(
    symbols_list, benchmark_symbol, get_data_function,
    entry_args, exit_args, start_dt, end_dt
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
            #df_dict[symbol] = pd.concat([data, df_benchmark], axis=1)
            #df_dict[symbol].fillna(method='ffill', inplace=True)

            df_dict[symbol]['Volume'] = df_dict[symbol]['Volume'].astype(float)
            apply_comparative_relative_strength(df_dict[symbol], 'Close', 'Close_benchmark')
            df_dict[symbol]['CRS'] = df_dict[symbol]['CRS'].mul(100)
            df_dict[symbol]['CRS_fast_MA'] = df_dict[symbol]['CRS'].rolling(entry_args['fast_ma_period_param']).mean()
            df_dict[symbol]['CRS_slow_MA'] = df_dict[symbol]['CRS'].rolling(entry_args['slow_ma_period_param']).mean()
            df_dict[symbol]['n_period_crs_std'] = df_dict[symbol]['CRS'].rolling(entry_args['n_period_crs_std']).std()
            apply_percent_rank(df_dict[symbol], entry_args['pct_rank_period_param'])
            apply_rsi(df_dict[symbol], period_param=entry_args['rsi_period_param'])
            apply_rsi(df_dict[symbol], period_param=entry_args['rsi_period_param'], col_name='Close_benchmark', suffix='_benchmark')
            apply_atr(df_dict[symbol])
            apply_vwap_from_n_period_low(df_dict[symbol], entry_args['vwap_period_param'])
            #apply_vwap(df_dict[symbol], entry_args['vwap_period_param'])

            df_dict[symbol].dropna(inplace=True)

    return df_dict, None


def get_system_props(instruments_db: InstrumentsMongoDb):
    benchmark_symbol = '^OMX'

    rsi_period_param = 18
    fast_ma_period_param = 10
    slow_ma_period_param = 63
    pct_rank_period_param = 126

    entry_args={
        'req_period_iters': pct_rank_period_param, 
        'entry_period_param': 5, #'entry_param_period': 15, 
        'fast_ma_period_param': fast_ma_period_param, 
        'slow_ma_period_param': slow_ma_period_param,
        'rsi_period_param': rsi_period_param, 
        'n_period_crs_std': slow_ma_period_param,
        'vwap_period_param': slow_ma_period_param,
        'pct_rank_period_param': pct_rank_period_param, 'pct_rank_clf_param': 0.5
    }
    exit_args={
        'exit_period_param': 10, 
        'crs_std_exit_multiplier': 3.5, 
        'atr_std_exit_multiplier': 3.0,
        'atr_multiplier': 1.0,
        'slow_ma_period_param': slow_ma_period_param,
        'rsi_period_param': rsi_period_param, 
        'vwap_period_param': slow_ma_period_param,
        'pct_rank_clf_param': 0.5,
    }

    market_list_ids = [
        instruments_db.get_market_list_id('omxs30')
        #instruments_db.get_market_list_id('omxs_large_caps'),
        #instruments_db.get_market_list_id('omxs_mid_caps')
    ]
    symbols_list = []
    for market_list_id in market_list_ids:
        symbols_list += json.loads(
            instruments_db.get_market_list_instrument_symbols(
                market_list_id
            )
        )

    return TradingSystemProperties(
        SYSTEM_NAME, 2,
        preprocess_data,
        (
            symbols_list,
            benchmark_symbol, price_data_get_req,
            entry_args, exit_args
        ),
        handle_trading_system,
        TradingSystemStateHandler, (SYSTEM_NAME, None),
        (
            run_trading_system,
            #entry_logic_example, exit_logic_example,
            entry_args, exit_args
        ),
        {'run_monte_carlo_sims': False, 'num_of_sims': 2000},
        None, (), (),
        SafeFPositionSizer, (20, 0.8), (),
        {
            'plot_fig': False,
            'num_of_sims': 500
        }
    )


if __name__ == '__main__':
    import tet_trading_systems.trading_system_development.trading_systems.env as env
    #SYSTEMS_DB = TetSystemsMongoDb('mongodb://localhost:27017/', 'systems_db')
    SYSTEMS_DB = TetSystemsMongoDb(env.ATLAS_MONGO_DB_URL, 'client_db')
    #INSTRUMENTS_DB = InstrumentsMongoDb('mongodb://localhost:27017/', 'instruments_db')
    INSTRUMENTS_DB = InstrumentsMongoDb(env.ATLAS_MONGO_DB_URL, 'client_db')

    start_dt = dt.datetime(1999, 1, 1)
    end_dt = dt.datetime(2011, 1, 1)

    system_props = get_system_props(INSTRUMENTS_DB)

    df_dict, features = system_props.preprocess_data_function(
        system_props.preprocess_data_args[0], '^OMX',
        price_data_get_req,
        system_props.preprocess_data_args[-2],
        system_props.preprocess_data_args[-1],
        start_dt, end_dt
    )

    run_trading_system(
        df_dict, SYSTEM_NAME,
        # ENTRY
        #crs_ma_cross_entry,
        #crs_ma_cross_pct_rank_regime_filter_entry,

        #crs_ma_classifier_entry,
        crs_ma_classifier_pct_rank_regime_filter_entry,
        
        #crs_ma_crossover_entry,
        #crs_ma_crossover_pct_rank_regime_filter_entry,

        #crs_ma_classifier_cross_entry,
        #crs_ma_classifier_cross_pct_rank_regime_filter_entry,

        # EXIT
        #n_period_exit,
        #crs_ma_cross_exit,
        #crs_ma_cross_pct_rank_classifier_exit,
        #crs_n_period_ma_cross_exit,

        #crs_ma_classifier_exit,
        #crs_ma_pct_rank_classifier_exit,
        #crs_n_period_ma_classifier_exit,

        #crs_std_multiplier_exit,
        #crs_std_multiplier_trailing_exit,
        #crs_fixed_std_multiplier_trailing_exit, 
        #crs_std_price_std_multiplier_trailing_exit,
        crs_std_price_std_multiplier_trailing_target_exit,
        system_props.preprocess_data_args[-2], 
        system_props.preprocess_data_args[-1], 
        plot_fig=True,
        plot_positions=False,
        systems_db=SYSTEMS_DB, client_db=SYSTEMS_DB, insert_into_db=False
    )