import datetime as dt
import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

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
    import apply_sma, apply_rsi, apply_atr, apply_comparative_relative_strength, apply_bollinger_bands
from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.misc_features \
    import apply_percent_rank, apply_alpha_score
from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.volume_features \
    import apply_volume_balance, apply_vwap, apply_vwap_from_n_period_low
from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.breadth_features \
    import apply_ad_line, apply_pct_over_n_sma

"""
Basera logik på vwap i relation till vwap (iaf för vwap_low), inte om pris e över eller under.
Relation mellan vwap i olika tidsperspektiv. CRS mellan vwap och pris -- går pris starkare = bullish

Example logic

Entry:
close price = top of period/n period range
atr increase after low volatility (low volatility ex - contraction in vwap rolling std)
n period higher close price
close price above vwap
close price above vwap from n period low
bullish trend + price at or near vwap
short term CRS > SMA

Exit:
n periods close prices under vwap
close price below vwap
close price ATR * n above n vwap
rsi divergence
n period vwap > n period vwap low

Regime filter:
price above/below long term vwap
price above/below n SMA
benchmark price above/below n SMA
% rank of price > 0.5
CRS above/below n SMA
"""

def entry_logic_example(df, *args, entry_args=None):
    #return df['Close'].iloc[-1] >= max(df['Close'].iloc[-entry_args['entry_period_param']:]), \
        #'long'
    return np.mean(df['BB_distance'].iloc[-5:]) <= np.mean(sorted(df['BB_distance'].iloc[-20:])[:int(20/2)]) and \
        df['High'].iloc[-1] - df['Close'].iloc[-1] >= (df['High'].iloc[-1] - df['Low'].iloc[-1]) * 0.66, 'long'


def over_n_rsi_over_n_crs_pct_rank_entry(df, *args, entry_args=None):
    return df[f'RSI_{entry_args["rsi_period_param"]}'].iloc[-1] > 50 and \
        df[f'RSI_{entry_args["rsi_period_param"]}'].iloc[-2] < 50 and \
        df[f'%_rank_crs'].iloc[-1] > 0.5 and \
        df['Close_benchmark'].iloc[-1] > df[f'SMA_{entry_args["sma_classifier_period_param"]}_benchmark'].iloc[-1], 'long'
        #df['%_rank'].iloc[-1] > 0.5, 'long'


def n_period_high_above_vwap(df, *args, entry_args=None):
    try:
        return df['Close'].iloc[-1] >= max(df['Close'].iloc[-entry_args['n_period_price_param']:]) and \
            df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}_low'].iloc[-1], 'long' 
    except:
        return False, ''


def close_price_above_vwap(df, *args, entry_args=None):
    try:
        return df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}'].iloc[-1] and \
            df['Close'].iloc[-2] >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}'].iloc[-2] and \
            df['Close'].iloc[-3] < df[f'VWAP_{entry_args["vwap_period_param_slow"]}'].iloc[-3], 'long'
    except:
        return False, 'long'


def close_price_above_vwap_low(df, *args, entry_args=None):
    try:
        #return df['Close'].iloc[-1] > df[f'VWAP_{entry_args["vwap_period_param_fast"]}'].iloc[-1] and \
        #    df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}_low'].iloc[-1] and \
        #    df['Close'].iloc[-2] < df[f'VWAP_{entry_args["vwap_period_param_slow"]}_low'].iloc[-2], 'long'
        return df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}_low'].iloc[-1] and \
            df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_m"]}_low'].iloc[-1] and \
            df['Close'].iloc[-1] < df[f'vwap_{entry_args["vwap_period_param_slow"]}_high_atr_mul'].iloc[-1] - df['ATR'].iloc[-1] * 1.5 and \
            df['Close'].iloc[-1] < df[f'VWAP_{entry_args["vwap_period_param_m"]}'].iloc[-1] + df['ATR'].iloc[-1] * 1.5, 'long'
    except:
        return False, 'long'


def close_price_above_vwap_low_pct_rank_clf(df, *args, entry_args=None):
    try:
        #return df['Close'].iloc[-1] > df[f'VWAP_{entry_args["vwap_period_param_fast"]}'].iloc[-1] and \
        #    df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}_low'].iloc[-1] and \
        #    df['Close'].iloc[-2] < df[f'VWAP_{entry_args["vwap_period_param_slow"]}_low'].iloc[-2] and \
        #    df[f'%_rank'].iloc[-1] > 0.5, 'long'
        return df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}_low'].iloc[-1] and \
            df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_m"]}_low'].iloc[-1] and \
            df['Close'].iloc[-1] < df[f'vwap_{entry_args["vwap_period_param_slow"]}_high_atr_mul'].iloc[-1] - df['ATR'].iloc[-1] * 1.5 and \
            df['Close'].iloc[-1] < df[f'VWAP_{entry_args["vwap_period_param_m"]}'].iloc[-1] + df['ATR'].iloc[-1] * 1.5 and \
            df[f'%_rank'].iloc[-1] > 0.5, 'long'
    except:
        return False, 'long'


def dual_vwap_momo(df, *args, entry_args=None):
    try:
        #return df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_m"]}_low'].iloc[-1] and \
        #    df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}'].iloc[-1] and \
        return df['Close'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}'].iloc[-1] and \
            df[f'VWAP_{entry_args["vwap_period_param_m"]}_low'].iloc[-1] >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}_low'].iloc[-1] and \
            df[f'VWAP_{entry_args["vwap_period_param_m"]}_low'].iloc[-1] >= max(df[f'VWAP_{entry_args["vwap_period_param_m"]}_low'].iloc[-entry_args['n_period_vwap_param']:]), 'long'
    except:
        return False, 'long'


def tripple_vwap_momo(df, *args, entry_args=None):
    try:
        return df['Close'].iloc[-entry_args['n_period_vwap_param']:].mean() >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}_low'].iloc[-1] and \
            df['Close'].iloc[-entry_args['n_period_vwap_param']:].mean() >= df[f'VWAP_{entry_args["vwap_period_param_m"]}_low'].iloc[-1] and \
            df['Close'].iloc[-entry_args['n_period_vwap_param']:].mean() >= df[f'VWAP_{entry_args["vwap_period_param_fast"]}_low'].iloc[-1] and \
            df['Close'].iloc[-1] >= max(df['Close'].iloc[-entry_args['n_period_price_param']:]), 'long'
    except:
        return False, 'long'

    
def tripple_vwap_momo_top_range_close(df, *args, entry_args=None):
    try:
        return df['Close'].iloc[-entry_args['n_period_vwap_param']:].mean() >= df[f'VWAP_{entry_args["vwap_period_param_slow"]}_low'].iloc[-1] and \
            df['Close'].iloc[-entry_args['n_period_vwap_param']:].mean() >= df[f'VWAP_{entry_args["vwap_period_param_m"]}_low'].iloc[-1] and \
            df['Close'].iloc[-entry_args['n_period_vwap_param']:].mean() >= df[f'VWAP_{entry_args["vwap_period_param_fast"]}_low'].iloc[-1] and \
            df['Close'].iloc[-1] >= df['High'].iloc[-1] - (df['High'].iloc[-1] - df['Low'].iloc[-1]) / 3, 'long'
    except:
        return False, 'long'


def entry_clf_function(df, *args):
    #return df['agg_rank_mean'].iloc[-1] >= 0.5 and df['crs_fast_ma_clf'].iloc[-1] and \
    return df['agg_rank_mean'].iloc[-1] >= 0.5 and df['crs_slow_ma_clf'].iloc[-1] and \
           df['%_rank'].iloc[-1] >= 0.5 and \
           df['Close'].iloc[-1] >= max(df['Close'].iloc[-args[0]['entry_period_param']:])


def exit_logic_example(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    return periods_in_pos > 20, \
        trail, trailing_exit_price


def n_period_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos,
    *args, exit_args=None
):
    try:
        return periods_in_pos >= exit_args['exit_period_param'], trail, trailing_exit_price
    except:
        return False, trail, trailing_exit_price


def u_rsi_mean_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, 
    *args, exit_args=None
):
    # rsi divergence på 'High' ist för 'Close
    # exita endast på rsi divergence om rsi e över ~60-70
    #
    #printa vilken exit condition som triggas
    rsi_val = df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-1]
    print('rsi mean < 45 condition:', df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-4:].mean() < 45)
    print(
        f'rsi divergence condition (rsi value={rsi_val}):', 
        df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-1] < max(df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-10:]) and 
        df['Close'].iloc[-1] >= max(df['Close'].iloc[-15:]) and periods_in_pos >= 10)
    print('rsi mean > 75:', df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-3:].mean() > 75)
    print('atr multiplier condition:', df['Close'].iloc[-1] < float(entry_price) - df['ATR'].iloc[-1] * 2.5)
    print()
    #input('exit conditions')

    return df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-4:].mean() < 45 and periods_in_pos >= 10 or \
        df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-1] < max(df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-10:]) and \
        df['Close'].iloc[-1] >= max(df['Close'].iloc[-15:]) and periods_in_pos >= 10 or \
        df[f'RSI_{exit_args["rsi_period_param"]}'].iloc[-3:].mean() > 75 or \
        df['Close'].iloc[-1] < float(entry_price) - df['ATR'].iloc[-1] * 3, trail, trailing_exit_price


def n_period_low_below_vwap(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, *args, exit_args=None
):
    return df['Close'].iloc[-1] <= min(df['Close'].iloc[-exit_args['n_period_price_param']:]) and \
        df['Close'].iloc[-1] <= df[f'VWAP_{exit_args["vwap_period_param_slow"]}_low'].iloc[-1], trail, trailing_exit_price


def close_price_below_vwap_slow(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, *args, exit_args=None
):
    return df['Close'].iloc[-1] < df[f'VWAP_{exit_args["vwap_period_param_slow"]}'].iloc[-1] and \
        df['Close'].iloc[-2] < df[f'VWAP_{exit_args["vwap_period_param_slow"]}'].iloc[-2], trail, trailing_exit_price


def close_price_below_vwap_fast(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, *args, exit_args=None
):
    return df['Close'].iloc[-1] < df[f'VWAP_{exit_args["vwap_period_param_fast"]}'].iloc[-1] and \
        df['Close'].iloc[-2] < df[f'VWAP_{exit_args["vwap_period_param_fast"]}'].iloc[-2], trail, trailing_exit_price


def close_price_below_vwap_low(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, *args, exit_args=None
):
    return df['Close'].iloc[-1] < df[f'VWAP_{exit_args["vwap_period_param_slow"]}_low'].iloc[-2] and \
        df['Close'].iloc[-2] < df[f'VWAP_{exit_args["vwap_period_param_slow"]}_low'].iloc[-3], trail, trailing_exit_price


def close_price_below_vwap_low_atr_target(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, *args, exit_args=None
):
    return df['Close'].iloc[-1] < df[f'VWAP_{exit_args["vwap_period_param_slow"]}_low'].iloc[-2] and \
        df['Close'].iloc[-2] < df[f'VWAP_{exit_args["vwap_period_param_slow"]}_low'].iloc[-3] or \
        df['Close'].iloc[-1] < df[f'vwap_{exit_args["vwap_period_param_slow"]}_low_atr_mul'].iloc[-1] and \
        df['Close'].iloc[-2] > df[f'vwap_{exit_args["vwap_period_param_slow"]}_low_atr_mul'].iloc[-2], trail, trailing_exit_price


def close_price_below_vwap_std_mul(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, *args, exit_args=None
):
    return df['Close'].iloc[-1] <= df[f'vwap_{exit_args["vwap_period_param_m"]}_std_rolling_lower_{exit_args["vwap_rolling_std_multiplier1"]}'].iloc[-1], trail, trailing_exit_price

def n_period_vwap_decline_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, *args, exit_args=None
):
    print()
    print(df[f'VWAP_{exit_args["vwap_period_param_m"]}_low'].iloc[-1] <= min(df[f'VWAP_{exit_args["vwap_period_param_m"]}_low'].iloc[-exit_args['n_period_vwap_param']:]))
    print(df['CRS_vwap'].iloc[-exit_args['n_period_vwap_param']:].mean() >= df['CRS_vwap'].iloc[-1])
    return df[f'VWAP_{exit_args["vwap_period_param_m"]}_low'].iloc[-1] <= min(df[f'VWAP_{exit_args["vwap_period_param_m"]}_low'].iloc[-exit_args['n_period_vwap_param']:]) or \
        df['CRS_vwap'].iloc[-exit_args['n_period_vwap_param']:].mean() >= df['CRS_vwap'].iloc[-1] , trail, trailing_exit_price


def adaptive_exit_function(df, trail, trailing_exit_price, *args):
    if df[f'RSI_{args[1]["benchmark_rsi_period_param"]}_benchmark'].iloc[-1] <= 40: 
        atr_multiplier = 2.5
    elif df[f'RSI_{args[1]["benchmark_rsi_period_param"]}_benchmark'].iloc[-1] > 40 and \
         df[f'RSI_{args[1]["benchmark_rsi_period_param"]}_benchmark'].iloc[-1] <= 70:
        atr_multiplier = 3.5
    else:
        atr_multiplier = 4.5

    price_target_condition = df['Close'].iloc[-1] >= float(args[0]) + df['ATR'].iloc[-1] * atr_multiplier
    price_stop_loss_condition = df['Close'].iloc[-1] <= float(args[0]) - df['ATR'].iloc[-1] * atr_multiplier

    condition1 = df['agg_rank_mean'].iloc[-4:].mean() <= 0.5 and df['agg_rank_mean'].iloc[-1] < 0.5
    condition2 = ~df['crs_fast_ma_clf'].iloc[-3:].any()
    condition3 = df['CRS'].iloc[-1] >= df['crs_mean'].iloc[-1] + df['crs_std'].iloc[-1] * 3.5

    return condition1 or condition2 or condition3 or \
           price_target_condition or price_stop_loss_condition, trail, trailing_exit_price


def atr_trailing_target_u_mean_lows_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, *args, exit_args=None
):
    if (df['Close'].iloc[-1]-float(entry_price)) >= (df[f'ATR'].iloc[-1] * exit_args['atr_trail_exit']) and \
            trail is False:
        trail = True
        trailing_exit_price = df['Close'].iloc[-1]

        #periods_in_pos > exit_args['period_exit'] or \
    if trail and trailing_exit_price is not None and trailing_exit_price > df['Close'].iloc[-1] or \
            np.mean(df['Low'].iloc[-3:]) - (df[f'ATR'].iloc[-1]) > df['Close'].iloc[-1] or \
            (df['Close'].iloc[-1]-float(entry_price)) >= \
            (df[f'ATR'].iloc[-1] * exit_args['atr_target_exit']):
        return True, False, None
    else:
        return False, trail, trailing_exit_price


def vwap_std_trailing_exit(
    df, trail, trailing_exit_price, entry_price, periods_in_pos, *args, exit_args=None
):
    if not trail and \
        df['Close'].iloc[-1] >= df[f'vwap_{exit_args["vwap_period_param_slow"]}_high_atr_mul'].iloc[-1]:
        trail = True
        trailing_exit_price = df[f'vwap_{exit_args["vwap_period_param_slow"]}_high_atr_mul'].iloc[-1]

    #print(trail, trailing_exit_price)
    #print(df['Close'].iloc[-1], float(entry_price) - (df['ATR'].iloc[-1] * 2))
    #input('exit')
    if trail and trailing_exit_price >= df['Close'].iloc[-1] or \
        df['Close'].iloc[-1] < float(entry_price) - (df['ATR'].iloc[-1] * 2):# or \
        #(df['Close'].iloc[-1] / float(entry_price) - 1) * 100 < -6:
        #print((df['Close'].iloc[-1] / float(entry_price) - 1) * 100)
        #print(df['Close'].iloc[-1], float(entry_price) - (df['ATR'].iloc[-1] * 2))
        #input('exit True')
        return True, False, None
    else:
        trailing_exit_price = max(trailing_exit_price, min(df[f'VWAP_{exit_args["vwap_period_param_slow"]}'].iloc[-5:])) if trailing_exit_price else None
        return False, trail, trailing_exit_price


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

            # apply indicators/features to dataframe
            df_dict[symbol]['Volume'] = df_dict[symbol]['Volume'].astype(float)
            apply_sma(df_dict[symbol], entry_args['sma_classifier_period_param'])
            apply_rsi(df_dict[symbol], period_param=entry_args['rsi_period_param'])
            apply_rsi(df_dict[symbol], period_param=exit_args['rsi_period_param'])
            apply_atr(df_dict[symbol], period_param=exit_args['atr_period_param'])
            apply_bollinger_bands(df_dict[symbol], ma_period_param=30)

            apply_percent_rank(df_dict[symbol], entry_args['pct_rank_price_period_param'])

            #apply_volume_balance(df_dict[symbol])
            #apply_sma(df_dict[symbol], 10, 'Volume_balance_20', suffix='_vb')
            
            apply_comparative_relative_strength(df_dict[symbol], 'Close', 'Close_benchmark')
            apply_sma(df_dict[symbol], entry_args['crs_sma_classifier_period_param'], col_name='CRS', suffix='_crs')
            apply_percent_rank(df_dict[symbol], entry_args['crs_pct_rank_period_param'], col_name='CRS', suffix='_crs')
            apply_sma(df_dict[symbol], entry_args['sma_classifier_period_param'], col_name='Close_benchmark', suffix='_benchmark')

            apply_vwap(df_dict[symbol], entry_args['vwap_period_param_slow'])
            apply_vwap_from_n_period_low(df_dict[symbol], entry_args['vwap_period_param_slow'], suffix='_low')
            apply_vwap(df_dict[symbol], entry_args['vwap_period_param_m'])
            apply_vwap_from_n_period_low(df_dict[symbol], entry_args['vwap_period_param_m'], suffix='_low')
            apply_comparative_relative_strength(df_dict[symbol], f'VWAP_{entry_args["vwap_period_param_m"]}_low', f'VWAP_{entry_args["vwap_period_param_m"]}', suffix='_vwap')
            apply_vwap(df_dict[symbol], entry_args['vwap_period_param_fast'])
            apply_vwap_from_n_period_low(df_dict[symbol], entry_args['vwap_period_param_fast'], suffix='_low')
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}'].std()
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}'].rolling(entry_args['vwap_rolling_std_period_param']).std()
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier1"]}'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}'] - (df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling'] * entry_args["vwap_rolling_std_multiplier1"])
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}'] + (df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling'] * entry_args["vwap_rolling_std_multiplier1"])
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier2"]}'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}'] - (df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling'] * entry_args["vwap_rolling_std_multiplier2"])
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier2"]}'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}'] + (df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling'] * entry_args["vwap_rolling_std_multiplier2"])
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_m"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier1"]}'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_m"]}'] - (df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling'] * entry_args["vwap_rolling_std_multiplier1"])
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_m"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_m"]}'] + (df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling'] * entry_args["vwap_rolling_std_multiplier1"])
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_m"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier2"]}'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_m"]}'] - (df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling'] * entry_args["vwap_rolling_std_multiplier2"])
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_m"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier2"]}'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_m"]}'] + (df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling'] * entry_args["vwap_rolling_std_multiplier2"])
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}'].std()
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}'].rolling(entry_args['vwap_rolling_std_period_param']).std()
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_lower'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}'] - (df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling'] * entry_args["vwap_rolling_std_multiplier1"])
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_upper'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}'] + (df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling'] * entry_args["vwap_rolling_std_multiplier1"])

            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_high_atr_mul'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}_low'] + df_dict[symbol]['ATR'] * entry_args['vwap_rolling_std_multiplier1']
            df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_low_atr_mul'] = df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}_low'] - df_dict[symbol]['ATR'] * entry_args['vwap_rolling_std_multiplier1']
            #df_dict[symbol]['vwap_ma'] = df_dict[symbol]['VWAP_{entry_args["vwap_period_param_slow"]}'].rolling(20).mean()
            #df_dict[symbol]['']

            #apply_percent_rank(df, entry_args['pct_rank_price_period_param'])
            #apply_comparative_relative_strength(df, 'Close', 'Close_benchmark')
            #df['CRS'] = df['CRS'].mul(100) #apply_percent_rank(df, entry_args['pct_rank_price_period_param'], col_name='CRS', suffix='_crs') #df['CRS_fast_ma'] = df['CRS'].rolling(entry_args['fast_ma']).mean() #df['CRS_slow_ma'] = df['CRS'].rolling(entry_args['slow_ma']).mean() #df['crs_fast_ma_clf'] = df['CRS'] >= df['CRS_fast_ma'] #df['crs_slow_ma_clf'] = df['CRS'] >= df['CRS_slow_ma'] #df['crs_std'] = df['CRS'].rolling(18).std() #df['crs_mean'] = df['CRS'].rolling(18).mean() #apply_volume_balance(df, period_param=entry_args["vb_period_param"]) #apply_percent_rank(df, entry_args['pct_rank_vb_period_param'], col_name=f'Volume_balance_{entry_args["vb_period_param"]}', suffix='_vb') #df['agg_rank_mean'] = df[['%_rank', '%_rank_crs', '%_rank_vb']].mean(axis=1) #for x, y in enumerate(df_dict[symbol].itertuples()):
            #    if x < {entry_args["vwap_period_param_slow"]}:
            #        continue
            #    df_dict[symbol][['Close', 'VWAP_{entry_args["vwap_period_param_slow"]}', 'VWAP_{entry_args["vwap_period_param_slow"]}_low']].iloc[x-{entry_args["vwap_period_param_slow"]}:x].plot()
            #    plt.show()

            #print(df_dict[symbol].columns)
            #print(df_dict[symbol][['VWAP_{entry_args["vwap_period_param_slow"]}', 'VWAP_{entry_args["vwap_period_param_slow"]}_low', 'vwap_{entry_args["vwap_period_param_slow"]}_std', 'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling', 'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower', 'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper']].tail(40).to_string())
            #print(df_dict[symbol][['VWAP_{entry_args["vwap_period_param_fast"]}', 'VWAP_{entry_args["vwap_period_param_fast"]}_low', 'vwap_{entry_args["vwap_period_param_fast"]}_std', 'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling', 'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_lower', 'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_upper']].tail(40).to_string())
            
            """fig, axs = plt.subplots(2, 2)
            axs[0, 0].plot(df_dict[symbol]['Close'], label='Close')
            axs[0, 0].plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}')
            axs[0, 0].plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_lower'], label=f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_lower')
            axs[0, 0].plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_upper'], label=f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_upper')
            axs[0, 1].plot(df_dict[symbol]['Close'], label='Close')
            axs[0, 1].plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}'], label=f'VWAP_{entry_args["vwap_period_param_slow"]}')
            #axs[0, 1].plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier1"]}'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier1"]}')
            #axs[0, 1].plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}')
            #axs[0, 1].plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier2"]}'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier2"]}')
            #axs[0, 1].plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier2"]}'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier2"]}')
            axs[1, 0].plot(df_dict[symbol]['Close'], label='Close')
            axs[1, 0].plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}_low'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_low')
            axs[1, 0].plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}_low'], label=f'VWAP_{entry_args["vwap_period_param_slow"]}_low')
            axs[1, 1].plot(df_dict[symbol]['Close'])
            axs[1, 1].plot(df_dict[symbol][f'SMA_{entry_args["sma_classifier_period_param"]}'])
            axs[0, 0].legend()
            axs[0, 1].legend()
            axs[1, 0].legend()
            x = df_dict[symbol][[f'VWAP_{entry_args["vwap_period_param_slow"]}', f'VWAP_{entry_args["vwap_period_param_slow"]}_low']].corr()
            print(x)
            plt.show()"""

            #plt.plot(df_dict[symbol]['Close'], label='Close')
            #plt.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}'], label=f'VWAP_{entry_args["vwap_period_param_slow"]}')
            #plt.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier1"]}'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier1"]}')
            #plt.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}')
            #plt.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier2"]}'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier2"]}')
            #plt.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier2"]}'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier2"]}')
            #plt.legend()
            #plt.show()

            #plt.plot(df_dict[symbol]['Close'], label='Close')
            #plt.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}_low'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_low')
            #plt.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_low')
            #plt.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}_low'], label=f'VWAP_{entry_args["vwap_period_param_slow"]}_low')
            #plt.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_lower'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_std_rolling_lower_f{entry_args["vwap_rolling_std_multiplier1"]}')
            #plt.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}')
            #plt.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_m"]}'], label=f'VWAP_{entry_args["vwap_period_param_m"]}')
            #plt.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_upper'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}')
            #plt.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_high_atr_mul'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_high_atr_mul_{entry_args["vwap_rolling_std_multiplier1"]}')
            #plt.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_low_atr_mul'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_low_atr_mul_{entry_args["vwap_rolling_std_multiplier1"]}')
            #plt.legend()
            #plt.show()

            """ fig, (axs1, axs2) = plt.subplots(2)
            axs1.plot(df_dict[symbol]['Close'], label='Price')
            #axs1.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}_low'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_low')
            #axs[0, 0].plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_low')
            axs1.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}_low'], label=f'VWAP_{entry_args["vwap_period_param_slow"]}_low')
            axs1.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_m"]}_low'], label=f'VWAP_{entry_args["vwap_period_param_m"]}_low')
            axs1.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_slow"]}'], label=f'VWAP_{entry_args["vwap_period_param_slow"]}')
            #axs[0, 0].plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_lower'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_std_rolling_lower_f{entry_args["vwap_rolling_std_multiplier1"]}')
            #axs[0, 0].plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_upper'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}')
            #axs1.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_slow"]}_low_atr_mul'], label=f'vwap_{entry_args["vwap_period_param_slow"]}_low_atr_mul_{entry_args["vwap_rolling_std_multiplier1"]}')
            axs2.plot(df_dict[symbol]['Close'], label='Price')
            #axs2.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_fast"]}_low'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_low')
            #axs2.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_lower'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier1"]}')
            #axs2.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_fast"]}_std_rolling_upper'], label=f'VWAP_{entry_args["vwap_period_param_fast"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}')
            axs2.plot(df_dict[symbol][f'VWAP_{entry_args["vwap_period_param_m"]}_low'], label=f'VWAP_{entry_args["vwap_period_param_m"]}_low')
            axs2.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_m"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier1"]}'], label=f'VWAP_{entry_args["vwap_period_param_m"]}_std_rolling_lower_{entry_args["vwap_rolling_std_multiplier1"]}')
            axs2.plot(df_dict[symbol][f'vwap_{entry_args["vwap_period_param_m"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}'], label=f'VWAP_{entry_args["vwap_period_param_m"]}_std_rolling_upper_{entry_args["vwap_rolling_std_multiplier1"]}')
            axs1.legend()
            axs2.legend()
            plt.show() """

            #fig, axs = plt.subplots(2, 2)
            #axs[0, 0].plot(df_dict[symbol]['Close'])
            #axs[0, 0].plot(df_dict[symbol][f'SMA_{entry_args["sma_classifier_period_param"]}'])
            #axs[0, 1].plot(df_dict[symbol]['Close_benchmark'])
            #axs[1, 0].plot(df_dict[symbol]['CRS'])
            #axs[1, 0].plot(df_dict[symbol][f'SMA_{entry_args["crs_sma_classifier_period_param"]}_crs'])
            #axs[1, 1].plot(df_dict[symbol]['%_rank_crs'])
            #axs[1, 1].plot(df_dict[symbol]['Close'])
            #axs[1, 1].plot(df_dict[symbol]['VWAP_{entry_args["vwap_period_param_slow"]}'])
            #axs[1, 1].plot(df_dict[symbol]['VWAP_{entry_args["vwap_period_param_slow"]}_low'])
            #plt.show()
            df_dict[symbol].dropna(inplace=True)

    return df_dict, None


def get_system_props(instruments_db: InstrumentsMongoDb):
    system_name = 'example_system'
    benchmark_symbol = '^OMX'

    vwap_period_param_slow = 63 
    vwap_period_param_m = 21 
    vwap_period_param_fast = 5

    entry_args = {
        'req_period_iters': vwap_period_param_slow, 'entry_period_param': 10,
        'vwap_period_param_slow': vwap_period_param_slow, 
        'vwap_period_param_m': vwap_period_param_m,
        'vwap_period_param_fast': vwap_period_param_fast,
        'vwap_rolling_std_period_param': 20,
        'vwap_rolling_std_multiplier1': 3.0, 'vwap_rolling_std_multiplier2': 6.0,
        'n_period_vwap_param': 4,
        'n_period_price_param': 10,
        'rsi_period_param': 5, 'crs_pct_rank_period_param': 63,
        'sma_classifier_period_param': 63, 'crs_sma_classifier_period_param': 63,
        'pct_rank_price_period_param': 126
    }
    exit_args = {
        'exit_period_param': 15, 'atr_period_param': 11,
        'rsi_period_param': 7, 'n_period_price_param': 5,
        'n_period_vwap_param': 10, 
        'period_exit': 10, 'atr_trail_exit': 3, 'atr_target_exit': 5,
        'vwap_period_param_slow': vwap_period_param_slow,
        'vwap_period_param_m': vwap_period_param_m,
        'vwap_period_param_fast': vwap_period_param_fast,
        'vwap_rolling_std_multiplier1': 3.0, 'vwap_rolling_std_multiplier2': 6.0
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
        system_name, 2,
        preprocess_data,
        (
            symbols_list,
            benchmark_symbol, price_data_get_req,
            entry_args, exit_args
        ),
        handle_trading_system,
        TradingSystemStateHandler, (system_name, None),
        (
            run_trading_system,
            entry_logic_example, exit_logic_example,
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
        df_dict, 'example_system',
        # entry function
        entry_logic_example,
        #over_n_rsi_over_n_crs_pct_rank_entry, 
        #n_period_high_above_vwap,
        #close_price_above_vwap,
        #close_price_above_vwap_low,
        #close_price_above_vwap_low_pct_rank_clf,
        #dual_vwap_momo,
        #tripple_vwap_momo,
        #tripple_vwap_momo_top_range_close,
        # exit function
        #u_rsi_mean_exit,
        #n_period_exit,
        #n_period_low_below_vwap,
        #close_price_below_vwap_slow,
        #close_price_below_vwap_fast,
        #close_price_below_vwap_low,
        #close_price_below_vwap_low_atr_target,
        #close_price_below_vwap_std_mul,
        #n_period_vwap_decline_exit,
        #atr_trailing_target_u_mean_lows_exit,
        vwap_std_trailing_exit,
        system_props.preprocess_data_args[-2], 
        system_props.preprocess_data_args[-1], 
        plot_fig=True,
        plot_positions=True,
        systems_db=SYSTEMS_DB, client_db=SYSTEMS_DB, insert_into_db=False
    )
