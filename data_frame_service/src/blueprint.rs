#![allow(unused)]

use polars::datatypes::DataType;
use polars::prelude::datatypes::Field;
use polars::prelude::{EWMOptions, RollingOptionsFixedWindow};

use crate::feature_expression::{
    apply_adr, apply_atr, apply_bolllinger_bands, apply_comparative_relative_strength,
    apply_ewm_mean, apply_higher_high_lower_low, apply_keltner_channels, apply_percent_rank,
    apply_rolling_mean, apply_rsi, apply_value_balance,
};
use crate::grpc_service::proto::Price;
use sterunets::DataFrameSchematic;

pub struct TradingSystemBlueprint {
    pub id: &'static str,
    pub df_schematic: DataFrameSchematic,
}

trait TradingSystem {
    fn new() -> TradingSystemBlueprint;
}

pub fn get_trading_system_blueprints() -> Vec<TradingSystemBlueprint> {
    let trading_system_data_blueprints = vec![
        momentum::TradingSystemExample::new(),
        ml::MLTradingSystemExample::new(),
    ];

    trading_system_data_blueprints
}

//     schema_fields.push(Field::new("ma_20".into(), DataType::Float64));
//     schema_fields.push(Field::new("bb_20_lower".into(), DataType::Float64));
//     schema_fields.push(Field::new("bb_20_upper".into(), DataType::Float64));
//     schema_fields.push(Field::new("bb_20_distance".into(), DataType::Float64));

//     df_schematic
//         .feature_operator
//         .append_expression(apply_rolling_mean(
//             RollingOptionsFixedWindow {
//                 window_size: 20,
//                 min_periods: 20,
//                 weights: None,
//                 center: false,
//                 fn_params: Default::default(),
//             },
//             &Price::CLOSE,
//             "ma_20",
//         ));

//     let bollinger_band_exprs = apply_bolllinger_bands(
//         RollingOptionsFixedWindow {
//             window_size: 20,
//             min_periods: 20,
//             weights: None,
//             center: false,
//             fn_params: Default::default(),
//         },
//         2.0,
//         "close",
//         "ma_20",
//         "bb_20",
//     );
//     for expr in bollinger_band_exprs {
//         df_schematic
//             .feature_operator
//             .append_derived_expression(expr);
//     }

//     schema_fields.push(Field::new("atr_14".into(), DataType::Float64));
//     schema_fields.push(Field::new("ema_20".into(), DataType::Float64));
//     schema_fields.push(Field::new("keltner_upper".into(), DataType::Float64));
//     schema_fields.push(Field::new("keltner_lower".into(), DataType::Float64));

//     df_schematic
//         .feature_operator
//         .append_expression(apply_atr(14_f64, "high", "low", "close", "atr_14").unwrap())
//         .append_expression(apply_ewm_mean(
//             EWMOptions {
//                 alpha: 2.0 / (20.0 + 1.0),
//                 adjust: false,
//                 ..Default::default()
//             },
//             "close",
//             "ema_20",
//         ));

//     let keltner_channels_exprs = apply_keltner_channels(2.0, "ema_20", "atr_14", "keltner");
//     for expr in keltner_channels_exprs {
//         df_schematic
//             .feature_operator
//             .append_derived_expression(expr);
//     }

// mod mean_reversion {
//     use sterunets::DataFrameSchematic;
//
//     use crate::blueprint::TradingSystemBlueprint;
//     use crate::grpc_service::proto::Price;
//
//     pub struct RSIDivergence {
//         blueprint: TradingSystemBlueprint
//     }
//
//     impl RSIDivergence {
//         pub fn new() -> RSIDivergence {
//             let mut schema_fields = Price::schema_fields();
//             schema_fields.push(Field::new("rsi_14".into(), DataType::Float64));
//             schema_fields.push(Field::new("pct_rank_100".into(), DataType::Float64));
//
//             let mut df_schematic = DataFrameSchematic::new(schema_fields);
//             df_schematic
//                 .feature_operator
//                 .append_expression(apply_rsi(14_f64, "close", "rsi_14"))
//                 .append_expression(apply_percent_rank(100, "close", "pct_rank_100"));
//
//             blueprint: TradingSystemBlueprint {
//                 id: "mean_reversion",
//                 df_schematic: df_schematic,
//             };
//
//             RSIDivergence { blueprint }
//         }
//     }
// }

mod momentum {
    use polars::datatypes::DataType;
    use polars::prelude::datatypes::Field;
    use polars::prelude::{PlSmallStr, RollingOptionsFixedWindow};

    use crate::blueprint::{TradingSystem, TradingSystemBlueprint};
    use crate::feature_expression::{apply_n_period_high, apply_rolling_max};
    use crate::grpc_service::proto::Price;
    use sterunets::DataFrameSchematic;

    pub struct TradingSystemExample {}

    impl TradingSystemExample {
        const ID: &str = "trading_system_example";
        const N_PERIOD_HIGH: usize = 5;
        const N_PERIOD_HIGH_COL: PlSmallStr = PlSmallStr::from_static("5_period_high");
    }

    impl TradingSystem for TradingSystemExample {
        fn new() -> TradingSystemBlueprint {
            let mut schema_fields = Price::schema_fields();
            schema_fields.push(Field::new(
                TradingSystemExample::N_PERIOD_HIGH_COL,
                DataType::Float64,
            ));

            let mut df_schematic = DataFrameSchematic::new(schema_fields);
            df_schematic
                .feature_operator
                .append_expression(apply_rolling_max(
                    &Price::CLOSE,
                    RollingOptionsFixedWindow {
                        window_size: TradingSystemExample::N_PERIOD_HIGH,
                        min_periods: TradingSystemExample::N_PERIOD_HIGH,
                        weights: None,
                        center: false,
                        fn_params: Default::default(),
                    },
                    &TradingSystemExample::N_PERIOD_HIGH_COL,
                ))
                .append_derived_expression(apply_n_period_high(
                    &Price::CLOSE,
                    &TradingSystemExample::N_PERIOD_HIGH_COL,
                    "5_period_high_close",
                ));

            TradingSystemBlueprint {
                id: TradingSystemExample::ID,
                df_schematic: df_schematic,
            }
        }
    }
}

mod ml {
    use polars::datatypes::DataType;
    use polars::lazy::dsl::{col, lit};
    use polars::prelude::datatypes::Field;
    use polars::prelude::{PlSmallStr, RollingOptionsFixedWindow};

    use crate::blueprint::{TradingSystem, TradingSystemBlueprint};
    use crate::feature_expression::{apply_pct_change, apply_shift};
    use crate::grpc_service::proto::Price;
    use sterunets::DataFrameSchematic;

    pub struct MLTradingSystemExample {}

    impl MLTradingSystemExample {
        const ID: &str = "ml_trading_system_example";
        const TARGET_PERIOD: i32 = 1;
    }

    impl TradingSystem for MLTradingSystemExample {
        fn new() -> TradingSystemBlueprint {
            let mut schema_fields = Price::schema_fields();
            schema_fields.push(Field::new(PlSmallStr::from("lag_1"), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from("lag_2"), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from("lag_5"), DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from("pct_change_shifted"),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(PlSmallStr::from("target"), DataType::Boolean));

            let mut df_schematic = DataFrameSchematic::new(schema_fields);
            df_schematic
                .feature_operator
                .append_expression(apply_shift(&Price::CLOSE, 1, "lag_1"))
                .append_expression(apply_shift(&Price::CLOSE, 2, "lag_2"))
                .append_expression(apply_shift(&Price::CLOSE, 5, "lag_5"))
                .append_expression(apply_pct_change(
                    &Price::CLOSE,
                    5,
                    -MLTradingSystemExample::TARGET_PERIOD,
                    "pct_change_shifted",
                ));

            df_schematic
                .feature_operator
                .append_derived_expression(col("pct_change_shifted").gt_eq(lit(0)).alias("target"));

            TradingSystemBlueprint {
                id: MLTradingSystemExample::ID,
                df_schematic: df_schematic,
            }
        }
    }
}
