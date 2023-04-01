import numpy as np
import pandas as pd

from tet_trading_systems.trading_system_development.data_utils.indicator_feature_workshop.technical_features.standard_indicators \
    import apply_sma


def apply_pct_over_n_sma(
    df, sma_period_param, ticker_list, col_name='Close', 
    func_apply_sma=False, suffix=''
):
    """
    Apply percentage of instruments over n SMA to given Pandas DataFrame.
    :param df: Pandas DataFrame object
    :param sma_period_param: Simple Moving Average period parameter
    :param ticker_list: List of tickers which data is contained in the DataFrame
    :param col_name: Name of the column to calculate SMA on
    :param func_apply_sma: If true the SMA column will be applied inside the function. 
        If SMA column has already been applied leave this kwarg as False.
    """

    pct_over_sma_list = []

    if func_apply_sma:
        for ticker in ticker_list:
            apply_sma(
                df, sma_period_param, col_name=f'{col_name}_{ticker}', 
                suffix=f'_{ticker}'
            )

    for index, row in df.iterrows():
        np.seterr(divide='ignore', invalid='ignore')
        over_sma_list = [
            row[f'SMA_{str(sma_period_param)}_{ticker}'] < row[f'{col_name}_{ticker}'] 
            for ticker in ticker_list if not np.isnan(row[f'{col_name}_{ticker}'])
        ]
                         
        if len(over_sma_list) < 1:
            pct_over_sma_list.append(0)
        else:
            pct_over_sma_list.append(sum(over_sma_list) / len(over_sma_list) * 100)

    df[f'%_over_SMA_{sma_period_param}{suffix}'] = pct_over_sma_list


def apply_ad_line(df, ticker_list, col_name='Close', suffix=''):
    """
    Apply advance/decline line indicator to given Pandas DataFrame.
    :param df: Pandas DataFrame object
    :param ticker_list: List of tickers which data the indicator will be calculated from.
    :param col_name: Name of DataFrame column to calculate advancers/decliners on.
    """

    ad = []

    for index, row in enumerate(df.iterrows()):
        if index < 1:
            ad.append(0)
            continue
        else:
            advancers = [
                df[f'{col_name}_{ticker}'].iloc[index] > df[f'{col_name}_{ticker}'].iloc[index-1]
                for ticker in ticker_list
            ]
            decliners = [
                df[f'{col_name}_{ticker}'].iloc[index] < df[f'{col_name}_{ticker}'].iloc[index-1]
                for ticker in ticker_list
            ]
            ad.append(ad[-1] + (sum(advancers) - sum(decliners)))

    df[f'AD_line{suffix}'] = ad


def apply_highs_v_lows(df, ticker_list, period_param=63, col_name='Close', suffix=''):

    highs_v_lows = []

    for index, row in enumerate(df.iterrows()):
        if index < period_param:
            highs_v_lows.append(0)
            continue
        else:
            new_highs = [df[f'{col_name}_{ticker}'].iloc[index] >= df[f'{col_name}_{ticker}'].iloc[index-period_param]
                         for ticker in ticker_list]
            new_lows = [df[f'{col_name}_{ticker}'].iloc[index] <= df[f'{col_name}_{ticker}'].iloc[index-period_param]
                        for ticker in ticker_list]
            highs_v_lows.append(sum(new_highs) - sum(new_lows))

    df[f'New_{period_param}_period_highs_v_lows{suffix}'] = highs_v_lows


def apply_periods_above_v_below_breadth_indicator_value(
    df, period_param, indicator_value, col_name='', suffix=''
):
    """
    Calculates and applies the number of periods of the last 'period_param' periods a feature or an indicator has been
    above or below a given 'indicator_value' value.
    :param df: Pandas DataFrame object
    :param period_param: Number of periods to look back and calculate on
    :param indicator_value: Indicator value to use as classification level for being above or below
    :param col_name: Name of the column to calculate the periods above vs below values on
    """

    periods_above_list = []

    for index, row in enumerate(df.iterrows()):
        if index < period_param:
            periods_above_list.append(np.nan)
            continue
        else:
            periods_above = df[col_name].iloc[index-period_param:index][df.iloc[index-period_param:index][col_name] >=
                                                                        indicator_value].count()
            periods_above_list.append((periods_above / period_param) * 100)

    df[f'Periods_above_{col_name}/{indicator_value}_{period_param}{suffix}'] = periods_above_list
