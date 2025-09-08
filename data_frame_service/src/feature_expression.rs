#![allow(dead_code)]

use polars::lazy::dsl::{Expr, functions::col, rolling_corr};
use polars::prelude::RollingOptionsFixedWindow;
use polars::prelude::*;

// TODO: Fix import problem
// use polars::prelude::max_horizontal;
fn max_horizontal<E: AsRef<[Expr]>>(exprs: E) -> PolarsResult<Expr> {
    let exprs = exprs.as_ref().to_vec();
    polars_ensure!(!exprs.is_empty(), ComputeError: "cannot return empty fold because the number of output rows is unknown");
    Ok(Expr::n_ary(FunctionExpr::MaxHorizontal, exprs))
}

pub fn apply_rolling_mean(options: RollingOptionsFixedWindow, apply_to: &str, name: &str) -> Expr {
    col(apply_to).rolling_mean(options).alias(name)
}

pub fn apply_rolling_max(apply_to: &str, options: RollingOptionsFixedWindow, name: &str) -> Expr {
    col(apply_to).rolling_max(options).alias(name)
}

pub fn apply_ewm_mean(options: EWMOptions, apply_to: &str, name: &str) -> Expr {
    col(apply_to).ewm_mean(options).alias(name)
}

pub fn apply_atr(
    periods: f64,
    high_col_name: &str,
    low_col_name: &str,
    close_col_name: &str,
    name: &str,
) -> PolarsResult<Expr> {
    let tr = max_horizontal(&[
        col(high_col_name) - col(low_col_name),
        (col(high_col_name) - col(close_col_name).shift(lit(1))).abs(),
        (col(low_col_name) - col(close_col_name).shift(lit(1))).abs(),
    ])?;

    Ok(tr
        .ewm_mean(EWMOptions {
            alpha: 1.0 / periods,
            adjust: false,
            ..Default::default()
        })
        .alias(name))
}

pub fn apply_adr(atr_col_name: &str, close_col_name: &str, name: &str) -> Expr {
    when(col(atr_col_name).is_not_null().first())
        .then((col(atr_col_name) / col(close_col_name)) * lit(100))
        .otherwise(lit(f64::NAN))
        .alias(name)
}

pub fn apply_rsi(periods: f64, close_col_name: &str, name: &str) -> Expr {
    let alpha = 1.0 / periods;

    let price_change = col(close_col_name) - col(close_col_name).shift(lit(1));

    let avg_gain = when(price_change.clone().gt(lit(0.0)))
        .then(price_change.clone())
        .otherwise(lit(0.0))
        .ewm_mean(EWMOptions {
            alpha: alpha,
            adjust: false,
            ..Default::default()
        });

    let avg_loss = when(price_change.clone().lt(lit(0.0)))
        .then(price_change.abs())
        .otherwise(lit(0.0))
        .ewm_mean(EWMOptions {
            alpha: alpha,
            adjust: false,
            ..Default::default()
        });

    when(avg_loss.clone().eq(lit(0.0)))
        .then(lit(100.0))
        .otherwise(lit(100.0) - (lit(100.0) / (lit(1.0) + (avg_gain / avg_loss))))
        .alias(name)
}

pub fn apply_bolllinger_bands(
    options: RollingOptionsFixedWindow,
    sd_multiplier: f32,
    close_col_name: &str,
    ma_col_name: &str,
    name: &str,
) -> Vec<Expr> {
    let bb_upper = (col(ma_col_name)
        + (col(close_col_name).rolling_std(options.clone()) * lit(sd_multiplier)))
    .alias(format!("{name}_upper"));

    let bb_lower = (col(ma_col_name)
        - ((col(close_col_name).rolling_std(options)) * lit(sd_multiplier)))
    .alias(format!("{name}_lower"));

    let bb_distance = (bb_upper.clone() - bb_lower.clone()).alias(format!("{name}_distance"));

    vec![bb_upper, bb_lower, bb_distance]
}

pub fn apply_keltner_channels(
    multiplier: f32,
    ema_col_name: &str,
    atr_col_name: &str,
    name: &str,
) -> Vec<Expr> {
    let keltner_upper =
        (col(ema_col_name) + col(atr_col_name) * lit(multiplier)).alias(format!("{name}_upper"));
    let keltner_lower =
        (col(ema_col_name) - col(atr_col_name) * lit(multiplier)).alias(format!("{name}_lower"));

    vec![keltner_upper, keltner_lower]
}

pub fn apply_comparative_relative_strength(col_1: &str, col_2: &str, name: &str) -> Expr {
    (col(col_1) / col(col_2)).alias(name)
}

pub fn apply_relative_value(apply_to: &str, apply_from: &str, name: &str) -> Expr {
    when(col(apply_to).lt_eq(lit(0.0)))
        .then(lit(0.0))
        .otherwise(col(apply_from) / col(apply_to))
        .alias(name)
}

pub fn apply_value_balance(
    options: RollingOptionsFixedWindow,
    comparison_col: &str,
    balance_col: &str,
    name: &str,
) -> Expr {
    when(col(comparison_col).gt_eq(col(comparison_col).shift(lit(1))))
        .then(col(balance_col))
        .otherwise(col(balance_col) * lit(-1))
        .rolling_mean(options)
        .cast(DataType::Int32)
        .alias(name)
}

// TODO: Implement
// pub fn apply_wa() -> Expr {
//
// }

// TODO: Implement
// pub fn apply_wa_from_n_period_low() -> Expr {
//
// }

pub fn apply_composite_pct_change(periods: &[u32], apply_to: &str, name: &str) -> Expr {
    (((col(apply_to) - col(apply_to).shift(lit(periods[0])))
        / col(apply_to).shift(lit(periods[0]))
        + (col(apply_to) - col(apply_to).shift(lit(periods[1])))
            / col(apply_to).shift(lit(periods[1]))
        + (col(apply_to) - col(apply_to).shift(lit(periods[2])))
            / col(apply_to).shift(lit(periods[2])))
        / lit(3.0))
    .alias(name)
}

pub fn apply_percent_rank(periods: u64, apply_to: &str, name: &str) -> Expr {
    (col(apply_to).rolling_map(
        Arc::new(|s: &Series| -> Series {
            let ranked = s.rank(
                RankOptions {
                    method: RankMethod::Ordinal,
                    descending: false,
                },
                None,
            );

            let last_rank = ranked.get(ranked.len() - 1);

            let last_rank = match last_rank {
                Ok(last_rank) => last_rank.cast(&DataType::Float64),
                Err(_) => return Series::new(PlSmallStr::EMPTY, [Option::<u32>::None]),
            };

            Series::new(PlSmallStr::EMPTY, [last_rank])
        }),
        Default::default(),
        RollingOptionsFixedWindow {
            window_size: periods as usize,
            min_periods: periods as usize,
            ..Default::default()
        },
    ) / lit(periods))
    .alias(name)
}

pub fn apply_higher_high_lower_low(
    options: RollingOptionsFixedWindow,
    apply_to: &str,
    name: &str,
) -> Expr {
    let periods = lit(options.window_size as u32);

    let max = col(apply_to).rolling_max(options.clone());
    let min = col(apply_to).rolling_min(options);
    let shifted_max = max.clone().shift(periods.clone());
    let shifted_min = min.clone().shift(periods);

    (max.gt(shifted_max).and(min.gt(shifted_min))).alias(name)
}

pub fn apply_rolling_correlation(
    options: RollingCovOptions,
    col_x: &str,
    col_y: &str,
    name: &str,
) -> Expr {
    rolling_corr(col(col_x), col(col_y), options).alias(name)
}

pub fn apply_diff_score(
    options: RollingOptionsFixedWindow,
    col_x: &str,
    col_y: &str,
    name: &str,
) -> Expr {
    ((((col(col_x) - col(col_x).shift(lit(1))) / col(col_x).shift(lit(1)))
        - ((col(col_y) - col(col_y).shift(lit(1))) / col(col_y).shift(lit(1))))
        * lit(100))
    .rolling_mean(options)
    .alias(name)
}

// apply_pct_over_n_sma
// pub fn apply_pct_over_rolling_mean() -> Expr {
//
// }

// pub fn apply_ad_line() -> Expr {
//
// }

// pub fn apply_highs_v_lows() -> Expr {
//
// }

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;
    use polars::frame::DataFrame;

    fn create_df() -> DataFrame {
        df!(
            "open" => [
                100.00, 101.50, 102.25, 101.75, 103.00,
                104.25, 103.50, 102.80, 104.10, 105.20,
                106.50, 105.80, 107.25, 108.00, 107.50,
                109.25, 110.50, 109.75, 111.00, 112.25,
                111.50, 113.75, 114.25, 113.00, 115.50
            ],
            "high" => [
                102.50, 103.75, 104.00, 103.25, 105.50,
                106.00, 105.25, 104.50, 106.75, 107.80,
                108.25, 107.50, 109.50, 110.25, 109.00,
                111.75, 112.25, 111.50, 113.25, 114.50,
                113.25, 115.50, 116.00, 115.75, 117.25
            ],
            "low" => [
                99.25,  100.75, 101.50, 100.50, 102.25,
                103.50, 102.25, 101.75, 103.25, 104.50,
                105.25, 104.50, 106.75, 107.25, 106.50,
                108.50, 109.75, 108.25, 110.25, 111.50,
                110.25, 112.50, 113.50, 112.25, 114.75
            ],
            "close" => [
                101.50, 102.25, 101.75, 103.00, 104.25,
                103.50, 102.80, 104.10, 105.20, 106.50,
                105.80, 107.25, 108.00, 107.50, 109.25,
                110.50, 109.75, 111.00, 112.25, 111.50,
                113.75, 114.25, 113.00, 115.50, 116.75
            ],
            "volume" => [
                1250000, 890000,  1100000, 1400000, 950000,
                1200000, 1350000, 1050000, 1600000, 1450000,
                1150000, 1750000, 1300000, 1650000, 1800000,
                1950000, 2100000, 1850000, 2200000, 1700000,
                1900000, 2350000, 2000000, 1550000, 2450000
            ],
        )
        .unwrap()
    }

    // TODO: Add assert statements to the test functions

    #[test]
    fn test_apply_rolling_mean() {
        let df = create_df();

        let options = RollingOptionsFixedWindow {
            window_size: 10,
            min_periods: 10,
            ..Default::default()
        };

        let rolling_mean_expr = apply_rolling_mean(options, "close", "ma_10");

        let df = df.lazy().with_column(rolling_mean_expr).collect().unwrap();

        dbg!(df);
    }

    #[test]
    fn test_apply_ewm_mean() {
        let df = create_df();

        let options = EWMOptions {
            alpha: 1.0 / 20 as f64,
            adjust: false,
            ..Default::default()
        };

        let ewm_mean_expr = apply_ewm_mean(options, "close", "ewm_20");

        let df = df.lazy().with_column(ewm_mean_expr).collect().unwrap();

        dbg!(df);
    }

    #[test]
    fn test_apply_atr() {
        let df = create_df();

        let atr_expr = apply_atr(14_f64, "high", "low", "close", "atr_14").unwrap();

        let df = df.lazy().with_column(atr_expr).collect().unwrap();

        dbg!(df);
    }

    #[test]
    fn test_apply_adr() {
        let df = create_df();

        let atr_expr = apply_atr(14_f64, "high", "low", "close", "atr_14").unwrap();
        let adr_expr = apply_adr("atr_14", "close", "adr_14");

        let df = df
            .lazy()
            .with_column(atr_expr)
            .with_column(adr_expr)
            .collect()
            .unwrap();

        dbg!(df);
    }

    #[test]
    fn test_apply_rsi() {
        let df = create_df();

        let rsi_expr = apply_rsi(14_f64, "close", "rsi_14");

        let df = df.lazy().with_column(rsi_expr).collect().unwrap();

        dbg!(df);
    }

    #[test]
    fn test_apply_bollinger_bands() {
        let df = create_df();

        let options = RollingOptionsFixedWindow {
            window_size: 20,
            min_periods: 20,
            ..Default::default()
        };

        let rolling_mean_expr = apply_rolling_mean(options.clone(), "close", "ma_20");

        let bollinger_bands_expr = apply_bolllinger_bands(options, 2.0, "close", "ma_20", "bb_20");

        let df = df
            .lazy()
            .with_column(rolling_mean_expr)
            .with_columns(bollinger_bands_expr)
            .collect()
            .unwrap();

        dbg!(df);
    }

    #[test]
    fn test_apply_keltner_channels() {
        let df = create_df();

        let options = EWMOptions {
            alpha: 1.0 / (20.0 + 1.0),
            adjust: false,
            ..Default::default()
        };

        let ewm_mean_expr = apply_ewm_mean(options, "close", "ewm_20");

        let atr_expr = apply_atr(14_f64, "high", "low", "close", "atr_14").unwrap();

        let keltner_channels = apply_keltner_channels(1.0, "ewm_20", "atr_14", "keltner");

        let df = df
            .lazy()
            .with_columns([ewm_mean_expr, atr_expr])
            .with_columns(keltner_channels)
            .collect()
            .unwrap();

        dbg!(df);
    }

    #[test]
    fn test_apply_comparative_relative_strength() {
        let df = create_df();

        let crs_expr = apply_comparative_relative_strength("close", "open", "crs");

        let df = df.lazy().with_column(crs_expr).collect().unwrap();

        dbg!(df);
    }

    #[test]
    fn test_apply_relative_value() {
        let df = create_df();

        let options = RollingOptionsFixedWindow {
            window_size: 10,
            min_periods: 10,
            ..Default::default()
        };

        let rolling_mean_expr = apply_rolling_mean(options, "volume", "volume_ma_10");

        let rval_expr = apply_relative_value("volume_ma_10", "volume", "rval");

        let df = df
            .lazy()
            .with_column(rolling_mean_expr)
            .with_column(rval_expr)
            .collect()
            .unwrap();

        dbg!(df);
    }

    #[test]
    fn test_apply_value_balance() {
        let df = create_df();

        let options = RollingOptionsFixedWindow {
            window_size: 5,
            min_periods: 5,
            ..Default::default()
        };

        let value_balance_expr = apply_value_balance(options, "close", "volume", "volume_balance");

        let df = df.lazy().with_column(value_balance_expr).collect().unwrap();

        dbg!(&df);
    }

    #[test]
    fn test_apply_composite_pct_change() {
        let df = create_df();

        let composite_pct_change_expr =
            apply_composite_pct_change(&[1, 3, 5], "close", "composite_pct_change");

        let df = df
            .lazy()
            .with_column(composite_pct_change_expr)
            .collect()
            .unwrap();

        dbg!(df);
    }

    #[test]
    fn test_apply_percent_rank() {
        let df = create_df();

        let pct_rank_expr = apply_percent_rank(10, "close", "pct_rank");

        let df = df.lazy().with_column(pct_rank_expr).collect().unwrap();

        dbg!(&df);
    }

    #[test]
    fn test_apply_higher_high_lower_low() {
        let df = create_df();

        let options = RollingOptionsFixedWindow {
            window_size: 10,
            min_periods: 10,
            ..Default::default()
        };

        let hhhl_expr = apply_higher_high_lower_low(options, "close", "hhhl");

        let df = df.lazy().with_column(hhhl_expr).collect().unwrap();

        dbg!(&df);
    }

    #[test]
    fn test_apply_rolling_correlation() {
        let df = create_df();

        let options = RollingCovOptions {
            window_size: 10,
            min_periods: 10,
            ddof: 1,
        };

        let rolling_corr_expr = apply_rolling_correlation(options, "close", "high", "corr");

        let df = df.lazy().with_column(rolling_corr_expr).collect().unwrap();

        dbg!(&df);
    }

    #[test]
    fn test_apply_diff_score() {
        let df = create_df();

        let options = RollingOptionsFixedWindow {
            window_size: 10,
            min_periods: 10,
            ..Default::default()
        };

        let diff_score_expr = apply_diff_score(options, "close", "high", "diff_score");

        let df = df.lazy().with_column(diff_score_expr).collect().unwrap();

        dbg!(&df);
    }
}
