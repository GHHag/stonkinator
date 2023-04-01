import datetime as dt

import pandas as pd
import numpy as np


def apply_percent_period_return(df, period_param, col_name='Close', suffix=''):
    df[f'{period_param}_p_%_change{suffix}'] = df[col_name].pct_change(periods=period_param).mul(100)


def apply_composite_momentum(df, periods=(21, 42, 63), col_name='Close', suffix=''):
    df['p1_return'] = df[col_name].pct_change(periods=periods[0]).mul(100)
    df['p2_return'] = df[col_name].pct_change(periods=periods[1]).mul(100)
    df['p3_return'] = df[col_name].pct_change(periods=periods[2]).mul(100)

    df[f'Composite_momentum{suffix}'] = df[['p1_return', 'p2_return', 'p3_return']].sum(axis=1) / 3


def apply_percent_rank(df, period_param, col_name='Close', suffix=''):
    pct_rank = []
    for i in range(len(df)):
        if i < period_param:
            pct_rank.append(np.nan)
            continue
        else:
            df_col_list = df[col_name].iloc[i-period_param:i].to_list()
            df_col_list.append(df[col_name].iloc[i])
            df_col_list.sort()
            try:
                period_rank = df_col_list.index(df[col_name].iloc[i])
            except ValueError:
                print('ValueError')
                period_rank = 0
            pct_rank.append(period_rank / period_param)

    df[f'%_rank{suffix}'] = pct_rank


def apply_linreg(df, period_param, col_name_1='CRS', col_name_2='Close', suffix=''):
    linreg = []
    for index, row in enumerate(df.itertuples()):
        if index < period_param:
            linreg.append(np.nan)
        else:
            crs_close_fit = np.polyfit(
                df[col_name_1].iloc[index-period_param:index],
                df[col_name_2].iloc[index-period_param:index], 1
            )
            linreg.append(crs_close_fit[0])

    df[f'Linreg{suffix}'] = linreg


def apply_higher_high_higher_low(df, f_period=20, c_period=10, col_name='Close', suffix=''):
    higher_high_higher_low = []
    for index, row in enumerate(df.itertuples()):
        if index < f_period:
            higher_high_higher_low.append(np.nan)
        else:
            higher_high = max(df[col_name].iloc[index-c_period:index]) > \
                max(df[col_name].iloc[index-f_period:index-c_period])
            lower_low = min(df[col_name].iloc[index-c_period:index]) > \
                min(df[col_name].iloc[index-f_period:index-c_period])
            higher_high_higher_low.append(higher_high and lower_low)

    df[f'Higher_high_higher_low{suffix}'] = higher_high_higher_low


def apply_rolling_corr(df, period_param, col_name1, col_name2, suffix=''):
    df[f'Rolling_corr_{col_name1}_{col_name2}{suffix}'] = \
        df[col_name1].rolling(period_param).corr(df[col_name2])


def apply_alpha_score(df, benchmark_col_suffix, period_param, col_name='Close', suffix=''):
    df[f'A_score{suffix}'] = df[col_name].pct_change() - \
        df[f'{col_name}_{benchmark_col_suffix}'].pct_change()
    df[f'A_score{suffix}'] = df[f'A_score{suffix}'].rolling(period_param).mean().mul(100)
