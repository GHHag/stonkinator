import numpy as np


def apply_sma(df, period_param, col_name='Close', suffix=''):
    df[f'SMA_{period_param}{suffix}'] = df[col_name].rolling(period_param).mean()


def apply_ema(df, period_param, col_name='Close', suffix=''):
    df[f'EMA_{period_param}{suffix}'] = df[col_name].ewm(span=period_param, adjust=False).mean()


def apply_atr(
    df, period_param=14, col_name_high='High', col_name_low='Low', col_name_close='Close', 
    suffix=''
):
    tr = []
    for i, r in df.iterrows():
        if i == df.index[0]:
            tr.append(np.nan)
            continue
        else:
            atr1 = r[col_name_high] - r[col_name_low]
            atr2 = abs(r[col_name_high] - r[col_name_close])
            atr3 = abs(r[col_name_low] - r[col_name_close])
            tr.append(max(atr1, atr2, atr3))

    df[f'TR{suffix}'] = tr
    df[f'ATR{suffix}'] = round(
        df[f'TR{suffix}'].ewm(alpha=1 / period_param, adjust=False).mean(), 4
    )


def apply_adr(
    df, period_param=14, col_name_high='High', col_name_low='Low', col_name_close='Close', 
    func_apply_atr=False, suffix=''
):
    if func_apply_atr:
        apply_atr(
            df, period_param=period_param, col_name_high=col_name_high, 
            col_name_low=col_name_low, col_name_close=col_name_close,
            suffix=suffix
        )

    df[f'ADR{suffix}'] = (df['ATR'] / df[col_name_close]) * 100


def apply_rsi(df, period_param=14, col_name='Close', suffix=''):
    def calc_rsi(array, deltas, avg_gain, avg_loss, n):
        # Use Wilder smoothing method
        up = lambda x:  x if x > 0 else 0
        down = lambda x: -x if x < 0 else 0
        i = n+1
        for d in deltas[n+1:]:
            avg_gain = ((avg_gain * (n-1)) + up(d)) / n
            avg_loss = ((avg_loss * (n-1)) + down(d)) / n
            if avg_loss != 0:
                rs = avg_gain / avg_loss
                array[i] = round(100 - (100 / (1 + rs)), 4)
            else:
                array[i] = 100
            i += 1

        return array

    def get_rsi(array, n):
        deltas = np.append([0], np.diff(array))

        avg_gain = np.sum(deltas[1:n+1].clip(min=0)) / n
        avg_loss = -np.sum(deltas[1:n+1].clip(max=0)) / n

        array = np.empty(deltas.shape[0])
        array.fill(np.nan)

        array = calc_rsi(array, deltas, avg_gain, avg_loss, n)
        return array

    df[f'RSI_{period_param}{suffix}'] = get_rsi(df[col_name], period_param)


def apply_keltner_channels(
    df, ema_period_param=20, atr_period_param=20, multiplier=1,
    col_name_high='High', col_name_low='Low', col_name_close='Close', suffix=''
):
    apply_ema(df, ema_period_param, col_name=col_name_high)
    apply_atr(
        df, period_param=atr_period_param, col_name_high=col_name_high, 
        col_name_low=col_name_low, col_name_close=col_name_close, suffix=suffix
    )

    kelt_upper = []
    kelt_lower = []
    for i, r in df.iterrows():
        kelt_upper.append(r[f'EMA_{ema_period_param}'] + r['ATR'] * multiplier)
        kelt_lower.append(r[f'EMA_{ema_period_param}'] - r['ATR'] * multiplier)

    df[f'Keltner_upper{suffix}'] = kelt_upper
    df[f'Keltner_lower{suffix}'] = kelt_lower


def apply_bollinger_bands(
    df, ma_period_param=20, sd_multiplier=2, col_name='Close', suffix=''
):
    stdevs, bb_upper, bb_lower = [], [], []
    apply_sma(df, ma_period_param, col_name=col_name, suffix=suffix)

    for i in range(len(df)):
        if i <= ma_period_param:
            stdevs.append(np.nan)
            bb_upper.append(np.nan)
            bb_lower.append(np.nan)
            continue
        else:
            prices_close = np.array(df[col_name].iloc[i-ma_period_param+1:i+1])
            stdev = np.std(prices_close, dtype=float)
            stdevs.append(round(stdev, 4))

            if suffix:
                bb_upper.append(
                    df[f'SMA_{ma_period_param}_{suffix}'].iloc[i] + stdev * sd_multiplier
                )
                bb_lower.append(
                    df[f'SMA_{ma_period_param}_{suffix}'].iloc[i] - stdev * sd_multiplier
                )
            else:
                bb_upper.append(
                    df[f'SMA_{ma_period_param}'].iloc[i] + stdev * sd_multiplier
                )
                bb_lower.append(
                    df[f'SMA_{ma_period_param}'].iloc[i] - stdev * sd_multiplier
                )

    df[f'STD_{col_name}{suffix}'] = stdevs
    df[f'BB_upper{suffix}'] = bb_upper
    df[f'BB_lower{suffix}'] = bb_lower
    df[f'BB_distance{suffix}'] = df[f'BB_upper{suffix}'] - df[f'BB_lower{suffix}']


def apply_comparative_relative_strength(df, col_1, col_2, suffix=''):
    df[f'CRS{suffix}'] = df[col_1] / df[col_2]
