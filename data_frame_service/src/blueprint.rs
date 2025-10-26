use sterunets::DataFrameSchematic;

pub struct TradingSystemBlueprint {
    pub id: &'static str,
    pub df_schematic: DataFrameSchematic,
}

trait TradingSystem {
    fn create_blueprint() -> TradingSystemBlueprint;
}

pub fn create_trading_system_blueprints() -> Vec<TradingSystemBlueprint> {
    let trading_system_data_blueprints = vec![
        momentum::TradingSystemExample::create_blueprint(),
        ml::MLTradingSystemExample::create_blueprint(),
        ml::MetaLabelingExample::create_blueprint(),
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

pub mod momentum {
    use polars::datatypes::DataType;
    use polars::prelude::datatypes::Field;
    use polars::prelude::{PlSmallStr, RollingOptionsFixedWindow};

    use crate::blueprint::{TradingSystem, TradingSystemBlueprint};
    use crate::feature_expression::{apply_n_period_high, apply_rolling_max};
    use crate::grpc_service::proto::Price;
    use sterunets::DataFrameSchematic;

    pub struct TradingSystemExample;

    impl TradingSystemExample {
        pub const TRADING_SYSTEM_NAME: &str = "trading_system_example";
        pub const MINIMUM_ROWS: u8 = Self::N_PERIOD_HIGH as u8;
        pub const ENTRY_CONDITION_COL: &str = "5_period_highest_close";
        const N_PERIOD_HIGH: usize = 5;
        const N_PERIOD_HIGH_COL: PlSmallStr = PlSmallStr::from_static("5_period_high_close");
    }

    impl TradingSystem for TradingSystemExample {
        fn create_blueprint() -> TradingSystemBlueprint {
            let mut schema_fields = Price::schema_fields();

            schema_fields.push(Field::new(Self::N_PERIOD_HIGH_COL, DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from_static(Self::ENTRY_CONDITION_COL),
                DataType::Boolean,
            ));

            let feature_expressions = vec![apply_rolling_max(
                RollingOptionsFixedWindow {
                    window_size: Self::N_PERIOD_HIGH,
                    min_periods: Self::N_PERIOD_HIGH,
                    weights: None,
                    center: false,
                    fn_params: Default::default(),
                },
                &Price::CLOSE,
                &Self::N_PERIOD_HIGH_COL,
            )];
            let derived_feature_expressions = vec![apply_n_period_high(
                &Price::CLOSE,
                &Self::N_PERIOD_HIGH_COL,
                Self::ENTRY_CONDITION_COL,
            )];

            let df_schematic = DataFrameSchematic::new(
                schema_fields,
                vec![feature_expressions, derived_feature_expressions],
            );

            TradingSystemBlueprint {
                id: Self::TRADING_SYSTEM_NAME,
                df_schematic: df_schematic,
            }
        }
    }
}

pub mod ml {
    use polars::datatypes::DataType;
    use polars::lazy::dsl::{col, lit};
    use polars::prelude::datatypes::Field;
    use polars::prelude::{PlSmallStr, RollingOptionsFixedWindow};

    use crate::blueprint::{TradingSystem, TradingSystemBlueprint};
    use crate::feature_expression::{
        apply_adr, apply_atr, apply_pct_change, apply_percent_rank, apply_rolling_max,
        apply_rolling_mean, apply_rolling_min, apply_rolling_std, apply_rsi, apply_shift,
    };
    use crate::grpc_service::proto::Price;
    use sterunets::DataFrameSchematic;

    const PCT_CHANGE: PlSmallStr = PlSmallStr::from_static("pct_change");
    const PCT_CHANGE_SHIFTED: PlSmallStr = PlSmallStr::from_static("pct_change_shifted");
    const PRICE_SHIFTED: &str = "price_shifted";

    // TODO: Wrap &str in PlSmallStr here instead if they are pushed to schema_fields vec?
    const LAG_1: &str = "lag_1";
    const LAG_2: &str = "lag_2";
    const LAG_4: &str = "lag_4";
    const LAG_5: &str = "lag_5";
    const LAG_8: &str = "lag_8";
    const LAG_16: &str = "lag_16";

    const ADR: &str = "adr";
    const ATR: &str = "atr";

    const MA_7: &str = "ma_7";
    const MA_21: &str = "ma_21";

    const LAG_8_ROLLING_7_STD: &str = "lag_8_rolling_7_std";
    const LAG_8_ROLLING_21_STD: &str = "lag_8_rolling_21_std";
    const LAG_16_ROLLING_7_STD: &str = "lag_16_rolling_7_std";
    const LAG_16_ROLLING_21_STD: &str = "lag_16_rolling_21_std";
    const LAG_8_ROLLING_7_STD_ADR_DIV: &str = "lag_8_rolling_7_std_adr_div";
    const LAG_8_ROLLING_21_STD_ADR_DIV: &str = "lag_8_rolling_21_std_adr_div";
    const LAG_16_ROLLING_7_STD_ADR_DIV: &str = "lag_16_rolling_7_std_adr_div";
    const LAG_16_ROLLING_21_STD_ADR_DIV: &str = "lag_16_rolling_21_std_adr_div";

    const PCT_RANK_63: &str = "pct_rank_63";
    const PCT_RANK_126: &str = "pct_rank_126";
    const PCT_RANK_DIFF: &str = "pct_rank_diff";

    const PCT_RANK_63_LAG_1: &str = "pct_rank_63_lag_1";
    const PCT_RANK_63_LAG_2: &str = "pct_rank_63_lag_2";
    const PCT_RANK_63_LAG_4: &str = "pct_rank_63_lag_4";
    const PCT_RANK_63_LAG_8: &str = "pct_rank_63_lag_8";
    const PCT_RANK_63_LAG_16: &str = "pct_rank_63_lag_16";

    const PCT_RANK_126_LAG_1: &str = "pct_rank_126_lag_1";
    const PCT_RANK_126_LAG_2: &str = "pct_rank_126_lag_2";
    const PCT_RANK_126_LAG_4: &str = "pct_rank_126_lag_4";
    const PCT_RANK_126_LAG_8: &str = "pct_rank_126_lag_8";
    const PCT_RANK_126_LAG_16: &str = "pct_rank_126_lag_16";

    const RSI_7: &str = "rsi_7";
    const RSI_9: &str = "rsi_9";
    const RSI_13: &str = "rsi_13";
    const RSI_DIFF_13_7: &str = "rsi_diff_13_7";
    const RSI_DIFF_13_9: &str = "rsi_diff_13_9";
    const RSI_DIFF_9_7: &str = "rsi_diff_9_7";
    const RSI_7_MA_7: &str = "rsi_7_ma_7";
    const RSI_7_MA_21: &str = "rsi_7_ma_21";
    const RSI_9_MA_7: &str = "rsi_9_ma_7";
    const RSI_9_MA_21: &str = "rsi_9_ma_21";
    const RSI_MA_7_DIFF: &str = "rsi_ma_7_diff";
    const RSI_MA_21_DIFF: &str = "rsi_ma_21_diff";
    const RSI_7_LAG_1: &str = "rsi_7_lag_1";
    const RSI_7_LAG_2: &str = "rsi_7_lag_2";
    const RSI_7_LAG_4: &str = "rsi_7_lag_4";
    const RSI_7_LAG_8: &str = "rsi_7_lag_8";
    const RSI_7_LAG_16: &str = "rsi_7_lag_16";
    const RSI_9_LAG_1: &str = "rsi_9_lag_1";
    const RSI_9_LAG_2: &str = "rsi_9_lag_2";
    const RSI_9_LAG_4: &str = "rsi_9_lag_4";
    const RSI_9_LAG_8: &str = "rsi_9_lag_8";
    const RSI_9_LAG_16: &str = "rsi_9_lag_16";
    const RSI_13_LAG_1: &str = "rsi_13_lag_1";
    const RSI_13_LAG_2: &str = "rsi_13_lag_2";
    const RSI_13_LAG_4: &str = "rsi_13_lag_4";
    const RSI_13_LAG_8: &str = "rsi_13_lag_8";
    const RSI_13_LAG_16: &str = "rsi_13_lag_16";

    const MA_7_ROLLING_7_STD: &str = "ma_7_rolling_7_std";
    const MA_21_ROLLING_21_STD: &str = "ma_21_rolling_21_std";
    const MA_7_ROLLING_7_STD_LOWER: &str = "ma_7_rolling_7_std_lower";
    const MA_7_ROLLING_7_STD_UPPER: &str = "ma_7_rolling_7_std_upper";
    const MA_21_ROLLING_21_STD_LOWER: &str = "ma_21_rolling_21_std_lower";
    const MA_21_ROLLING_21_STD_UPPER: &str = "ma_21_rolling_21_std_upper";
    const MA_7_ROLLING_7_STD_LOWER_ATR_REL: &str = "ma_7_rolling_7_std_lower_atr_rel";
    const MA_7_ROLLING_7_STD_UPPER_ATR_REL: &str = "ma_7_rolling_7_std_upper_atr_rel";
    const MA_21_ROLLING_21_STD_LOWER_ATR_REL: &str = "ma_21_rolling_21_std_lower_atr_rel";
    const MA_21_ROLLING_21_STD_UPPER_ATR_REL: &str = "ma_21_rolling_21_std_upper_atr_rel";
    const MA_DIFF_21_7: &str = "ma_diff_21_7";
    const MA_DIFF_21_7_ATR_REL: &str = "ma_diff_21_7_atr_rel";
    const MA_DIFF_21_7_ROLLING_7_STD: &str = "ma_diff_21_7_rolling_7_std";
    const MA_DIFF_21_7_ROLLING_7_STD_ATR_REL: &str = "ma_diff_21_7_rolling_7_std_atr_rel";
    const MA_DIFF_21_7_LAG_1: &str = "ma_diff_21_7_lag_1";
    const MA_DIFF_21_7_LAG_2: &str = "ma_diff_21_7_lag_2";
    const MA_DIFF_21_7_LAG_4: &str = "ma_diff_21_7_lag_4";
    const MA_DIFF_21_7_LAG_8: &str = "ma_diff_21_7_lag_8";
    const MA_DIFF_21_7_LAG_16: &str = "ma_diff_21_7_lag_16";

    pub struct MLTradingSystemExample;

    impl MLTradingSystemExample {
        pub const TRADING_SYSTEM_NAME: &str = "ml_trading_system_example";
        pub const MINIMUM_ROWS: u8 = 5;
        pub const ENTRY_CONDITION_COL: &str = "target";
        pub const TARGET_PERIOD: i32 = 1;
        #[allow(dead_code)]
        pub const FEATURES: &[&str] = &[LAG_1, LAG_2, LAG_5];
    }

    impl TradingSystem for MLTradingSystemExample {
        fn create_blueprint() -> TradingSystemBlueprint {
            let mut schema_fields = Price::schema_fields();

            schema_fields.push(Field::new(PlSmallStr::from(LAG_1), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(LAG_2), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(LAG_5), DataType::Float64));
            schema_fields.push(Field::new(PCT_CHANGE_SHIFTED, DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from(Self::ENTRY_CONDITION_COL),
                DataType::Int8,
            ));

            let feature_expressions = vec![
                apply_shift(&Price::CLOSE, 1, LAG_1),
                apply_shift(&Price::CLOSE, 2, LAG_2),
                apply_shift(&Price::CLOSE, 5, LAG_5),
                apply_pct_change(&Price::CLOSE, 5, -Self::TARGET_PERIOD, &PCT_CHANGE_SHIFTED),
            ];

            let derived_feature_expressions = vec![
                col(PCT_CHANGE_SHIFTED)
                    .gt_eq(lit(0))
                    .cast(DataType::Int8)
                    .alias(Self::ENTRY_CONDITION_COL),
            ];

            let df_schematic = DataFrameSchematic::new(
                schema_fields,
                vec![feature_expressions, derived_feature_expressions],
            );

            TradingSystemBlueprint {
                id: Self::TRADING_SYSTEM_NAME,
                df_schematic: df_schematic,
            }
        }
    }

    pub struct MetaLabelingExample;

    impl MetaLabelingExample {
        pub const TRADING_SYSTEM_NAME: &str = "meta_labeling_example";
        pub const MINIMUM_ROWS: u8 = 126 + 16;
        pub const ENTRY_CONDITION_COL: &str = "entry_label";
        const MA_CROSSOVER: &str = "ma_crossover";
        const MA_CROSSOVER_SHIFTED: &str = "ma_crossover_shifted";
        pub const EXIT_LABEL: &str = "exit_label";
        pub const TARGET_PERIOD: i32 = 40;
        const ROLLING_MAX_PRICE: &str = "rolling_max_price";
        const ROLLING_MIN_PRICE: &str = "rolling_min_price";
        const MAX_PCT_CHANGE: &str = "max_pct_change";
        const MIN_PCT_CHANGE: &str = "min_pct_change";
        #[allow(dead_code)]
        pub const FEATURES: &[&str] = &[
            ADR,
            LAG_1,
            LAG_2,
            LAG_4,
            LAG_8,
            LAG_16,
            LAG_8_ROLLING_7_STD,
            LAG_8_ROLLING_21_STD,
            LAG_16_ROLLING_7_STD,
            LAG_16_ROLLING_21_STD,
            PCT_RANK_63,
            PCT_RANK_126,
            RSI_7,
            RSI_9,
            RSI_13,
            LAG_8_ROLLING_7_STD_ADR_DIV,
            LAG_8_ROLLING_21_STD_ADR_DIV,
            LAG_16_ROLLING_7_STD_ADR_DIV,
            LAG_16_ROLLING_21_STD_ADR_DIV,
            PCT_RANK_DIFF,
            PCT_RANK_63_LAG_1,
            PCT_RANK_63_LAG_2,
            PCT_RANK_63_LAG_4,
            PCT_RANK_63_LAG_8,
            PCT_RANK_63_LAG_16,
            PCT_RANK_126_LAG_1,
            PCT_RANK_126_LAG_2,
            PCT_RANK_126_LAG_4,
            PCT_RANK_126_LAG_8,
            PCT_RANK_126_LAG_16,
            RSI_DIFF_13_7,
            RSI_DIFF_13_9,
            RSI_DIFF_9_7,
            RSI_7_MA_7,
            RSI_7_MA_21,
            RSI_9_MA_7,
            RSI_9_MA_21,
            RSI_MA_7_DIFF,
            RSI_MA_21_DIFF,
            RSI_7_LAG_1,
            RSI_7_LAG_2,
            RSI_7_LAG_4,
            RSI_7_LAG_8,
            RSI_7_LAG_16,
            RSI_9_LAG_1,
            RSI_9_LAG_2,
            RSI_9_LAG_4,
            RSI_9_LAG_8,
            RSI_9_LAG_16,
            RSI_13_LAG_1,
            RSI_13_LAG_2,
            RSI_13_LAG_4,
            RSI_13_LAG_8,
            RSI_13_LAG_16,
            MA_7_ROLLING_7_STD_LOWER_ATR_REL,
            MA_7_ROLLING_7_STD_UPPER_ATR_REL,
            MA_21_ROLLING_21_STD_LOWER_ATR_REL,
            MA_21_ROLLING_21_STD_UPPER_ATR_REL,
            MA_DIFF_21_7,
            MA_DIFF_21_7_ATR_REL,
            MA_DIFF_21_7_ROLLING_7_STD,
            MA_DIFF_21_7_ROLLING_7_STD_ATR_REL,
            MA_DIFF_21_7_LAG_1,
            MA_DIFF_21_7_LAG_2,
            MA_DIFF_21_7_LAG_4,
            MA_DIFF_21_7_LAG_8,
            MA_DIFF_21_7_LAG_16,
        ];
    }

    impl TradingSystem for MetaLabelingExample {
        fn create_blueprint() -> TradingSystemBlueprint {
            let mut schema_fields = Price::schema_fields();

            // Entry label related
            schema_fields.push(Field::new(
                PlSmallStr::from(Self::ENTRY_CONDITION_COL),
                DataType::Int8,
            ));
            schema_fields.push(Field::new(PCT_CHANGE, DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from(Self::MA_CROSSOVER),
                DataType::Boolean,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(Self::MA_CROSSOVER_SHIFTED),
                DataType::Boolean,
            ));

            // Exit label related
            schema_fields.push(Field::new(
                PlSmallStr::from(Self::EXIT_LABEL),
                DataType::Int8,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PRICE_SHIFTED),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(Self::ROLLING_MAX_PRICE),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(Self::ROLLING_MIN_PRICE),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(Self::MAX_PCT_CHANGE),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(Self::MIN_PCT_CHANGE),
                DataType::Float64,
            ));

            // Feature related
            schema_fields.push(Field::new(PlSmallStr::from(ATR), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(ADR), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(LAG_1), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(LAG_2), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(LAG_4), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(LAG_8), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(LAG_16), DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from(LAG_8_ROLLING_7_STD),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(LAG_8_ROLLING_21_STD),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(LAG_16_ROLLING_7_STD),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(LAG_16_ROLLING_21_STD),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(LAG_8_ROLLING_7_STD_ADR_DIV),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(LAG_8_ROLLING_21_STD_ADR_DIV),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(LAG_16_ROLLING_7_STD_ADR_DIV),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(LAG_16_ROLLING_21_STD_ADR_DIV),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(PlSmallStr::from(PCT_RANK_63), DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_126),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_DIFF),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_63_LAG_1),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_63_LAG_2),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_63_LAG_4),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_63_LAG_8),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_63_LAG_16),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_126_LAG_1),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_126_LAG_2),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_126_LAG_4),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_126_LAG_8),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(PCT_RANK_126_LAG_16),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_7), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_9), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_13), DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_DIFF_13_7),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_DIFF_13_9),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_DIFF_9_7),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_7_MA_7), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_7_MA_21), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_9_MA_7), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_9_MA_21), DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_MA_7_DIFF),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_MA_21_DIFF),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_7_LAG_1), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_7_LAG_2), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_7_LAG_4), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_7_LAG_8), DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_7_LAG_16),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_9_LAG_1), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_9_LAG_2), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_9_LAG_4), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(RSI_9_LAG_8), DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_9_LAG_16),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_13_LAG_1),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_13_LAG_2),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_13_LAG_4),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_13_LAG_8),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(RSI_13_LAG_16),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(PlSmallStr::from(MA_7), DataType::Float64));
            schema_fields.push(Field::new(PlSmallStr::from(MA_21), DataType::Float64));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_7_ROLLING_7_STD),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_21_ROLLING_21_STD),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_7_ROLLING_7_STD_LOWER),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_7_ROLLING_7_STD_UPPER),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_21_ROLLING_21_STD_LOWER),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_21_ROLLING_21_STD_UPPER),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_7_ROLLING_7_STD_LOWER_ATR_REL),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_7_ROLLING_7_STD_UPPER_ATR_REL),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_21_ROLLING_21_STD_LOWER_ATR_REL),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_21_ROLLING_21_STD_UPPER_ATR_REL),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_DIFF_21_7),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_DIFF_21_7_ATR_REL),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_DIFF_21_7_ROLLING_7_STD),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_DIFF_21_7_ROLLING_7_STD_ATR_REL),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_DIFF_21_7_LAG_1),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_DIFF_21_7_LAG_2),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_DIFF_21_7_LAG_4),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_DIFF_21_7_LAG_8),
                DataType::Float64,
            ));
            schema_fields.push(Field::new(
                PlSmallStr::from(MA_DIFF_21_7_LAG_16),
                DataType::Float64,
            ));

            let rolling_options_target_period = RollingOptionsFixedWindow {
                window_size: Self::TARGET_PERIOD as usize,
                min_periods: Self::TARGET_PERIOD as usize,
                weights: None,
                center: Default::default(),
                fn_params: None,
            };
            let rolling_options_7 = RollingOptionsFixedWindow {
                window_size: 7,
                min_periods: 7,
                weights: None,
                center: Default::default(),
                fn_params: None,
            };
            let rolling_options_21 = RollingOptionsFixedWindow {
                window_size: 21,
                min_periods: 21,
                weights: None,
                center: Default::default(),
                fn_params: None,
            };

            let feature_expressions = vec![
                apply_pct_change(&Price::CLOSE, 1, 0, &PCT_CHANGE),
                apply_shift(&Price::CLOSE, -Self::TARGET_PERIOD, PRICE_SHIFTED),
                apply_atr(14.0, &Price::HIGH, &Price::LOW, &Price::CLOSE, ATR).unwrap(),
                apply_rolling_mean(rolling_options_7.clone(), &Price::CLOSE, MA_7),
                apply_rolling_mean(rolling_options_21.clone(), &Price::CLOSE, MA_21),
                apply_percent_rank(63, &Price::CLOSE, PCT_RANK_63),
                apply_percent_rank(126, &Price::CLOSE, PCT_RANK_126),
                apply_rsi(7.0, &Price::CLOSE, RSI_7),
                apply_rsi(9.0, &Price::CLOSE, RSI_9),
                apply_rsi(13.0, &Price::CLOSE, RSI_13),
            ];
            let derived_feature_expressions1 = vec![
                // TODO: Consider alternatively computing the pct change of n_target_periods out before
                // shifting, then getting the rolling max/min of the pct change.
                apply_rolling_max(
                    rolling_options_target_period.clone(),
                    PRICE_SHIFTED,
                    Self::ROLLING_MAX_PRICE,
                ),
                apply_rolling_min(
                    rolling_options_target_period,
                    PRICE_SHIFTED,
                    Self::ROLLING_MIN_PRICE,
                ),
                apply_shift(&PCT_CHANGE, 1, LAG_1),
                apply_shift(&PCT_CHANGE, 2, LAG_2),
                apply_shift(&PCT_CHANGE, 4, LAG_4),
                apply_shift(&PCT_CHANGE, 8, LAG_8),
                apply_shift(&PCT_CHANGE, 16, LAG_16),
                apply_adr(ATR, &Price::CLOSE, ADR),
                col(MA_7).gt(MA_21).alias(Self::MA_CROSSOVER),
                (col(PCT_RANK_126) - col(PCT_RANK_63)).alias(PCT_RANK_DIFF),
                apply_shift(PCT_RANK_63, 1, PCT_RANK_63_LAG_1),
                apply_shift(PCT_RANK_63, 2, PCT_RANK_63_LAG_2),
                apply_shift(PCT_RANK_63, 4, PCT_RANK_63_LAG_4),
                apply_shift(PCT_RANK_63, 8, PCT_RANK_63_LAG_8),
                apply_shift(PCT_RANK_63, 16, PCT_RANK_63_LAG_16),
                apply_shift(PCT_RANK_126, 1, PCT_RANK_126_LAG_1),
                apply_shift(PCT_RANK_126, 2, PCT_RANK_126_LAG_2),
                apply_shift(PCT_RANK_126, 4, PCT_RANK_126_LAG_4),
                apply_shift(PCT_RANK_126, 8, PCT_RANK_126_LAG_8),
                apply_shift(PCT_RANK_126, 16, PCT_RANK_126_LAG_16),
                (col(RSI_13) - col(RSI_7)).alias(RSI_DIFF_13_7),
                (col(RSI_13) - col(RSI_9)).alias(RSI_DIFF_13_9),
                (col(RSI_9) - col(RSI_7)).alias(RSI_DIFF_9_7),
                apply_rolling_mean(rolling_options_7.clone(), RSI_7, RSI_7_MA_7),
                apply_rolling_mean(rolling_options_21.clone(), RSI_7, RSI_7_MA_21),
                apply_rolling_mean(rolling_options_7.clone(), RSI_9, RSI_9_MA_7),
                apply_rolling_mean(rolling_options_21.clone(), RSI_9, RSI_9_MA_21),
                apply_shift(RSI_7, 1, RSI_7_LAG_1),
                apply_shift(RSI_7, 2, RSI_7_LAG_2),
                apply_shift(RSI_7, 4, RSI_7_LAG_4),
                apply_shift(RSI_7, 8, RSI_7_LAG_8),
                apply_shift(RSI_7, 16, RSI_7_LAG_16),
                apply_shift(RSI_9, 1, RSI_9_LAG_1),
                apply_shift(RSI_9, 2, RSI_9_LAG_2),
                apply_shift(RSI_9, 4, RSI_9_LAG_4),
                apply_shift(RSI_9, 8, RSI_9_LAG_8),
                apply_shift(RSI_9, 16, RSI_9_LAG_16),
                apply_shift(RSI_13, 1, RSI_13_LAG_1),
                apply_shift(RSI_13, 2, RSI_13_LAG_2),
                apply_shift(RSI_13, 4, RSI_13_LAG_4),
                apply_shift(RSI_13, 8, RSI_13_LAG_8),
                apply_shift(RSI_13, 16, RSI_13_LAG_16),
                apply_rolling_std(rolling_options_7.clone(), MA_7, MA_7_ROLLING_7_STD),
                apply_rolling_std(rolling_options_21.clone(), MA_21, MA_21_ROLLING_21_STD),
                (col(MA_21) - col(MA_7)).alias(MA_DIFF_21_7),
            ];
            let derived_feature_expressions2 = vec![
                apply_shift(Self::MA_CROSSOVER, 1, Self::MA_CROSSOVER_SHIFTED),
                ((col(Self::ROLLING_MAX_PRICE) / col(Price::CLOSE) - lit(1)) * lit(100))
                    .alias(Self::MAX_PCT_CHANGE),
                ((col(Self::ROLLING_MIN_PRICE) / col(Price::CLOSE) - lit(1)) * lit(100))
                    .alias(Self::MIN_PCT_CHANGE),
                apply_rolling_std(rolling_options_7.clone(), LAG_8, LAG_8_ROLLING_7_STD),
                apply_rolling_std(rolling_options_21.clone(), LAG_8, LAG_8_ROLLING_21_STD),
                apply_rolling_std(rolling_options_7.clone(), LAG_16, LAG_16_ROLLING_7_STD),
                apply_rolling_std(rolling_options_21, LAG_16, LAG_16_ROLLING_21_STD),
                (col(RSI_9_MA_7) - col(RSI_7_MA_7)).alias(RSI_MA_7_DIFF),
                (col(RSI_9_MA_21) - col(RSI_7_MA_21)).alias(RSI_MA_21_DIFF),
                (col(MA_7) - col(MA_7_ROLLING_7_STD)).alias(MA_7_ROLLING_7_STD_LOWER),
                (col(MA_7) + col(MA_7_ROLLING_7_STD)).alias(MA_7_ROLLING_7_STD_UPPER),
                (col(MA_21) - col(MA_21_ROLLING_21_STD)).alias(MA_21_ROLLING_21_STD_LOWER),
                (col(MA_21) + col(MA_21_ROLLING_21_STD)).alias(MA_21_ROLLING_21_STD_UPPER),
                (col(MA_DIFF_21_7) / col(ATR)).alias(MA_DIFF_21_7_ATR_REL),
                apply_rolling_std(rolling_options_7, MA_DIFF_21_7, MA_DIFF_21_7_ROLLING_7_STD),
                apply_shift(MA_DIFF_21_7, 1, MA_DIFF_21_7_LAG_1),
                apply_shift(MA_DIFF_21_7, 2, MA_DIFF_21_7_LAG_2),
                apply_shift(MA_DIFF_21_7, 4, MA_DIFF_21_7_LAG_4),
                apply_shift(MA_DIFF_21_7, 8, MA_DIFF_21_7_LAG_8),
                apply_shift(MA_DIFF_21_7, 16, MA_DIFF_21_7_LAG_16),
            ];
            let derived_feature_expressions3 = vec![
                col(Self::MA_CROSSOVER)
                    .and(col(Self::MA_CROSSOVER_SHIFTED).not())
                    .and(col(PCT_RANK_126).gt_eq(lit(0.5)))
                    .cast(DataType::Int8)
                    .alias(Self::ENTRY_CONDITION_COL),
                col(Self::MAX_PCT_CHANGE)
                    .gt(col(ADR) * lit(3.5))
                    .and(col(Self::MIN_PCT_CHANGE).gt(-(col(ADR) * lit(2.5))))
                    .cast(DataType::Int8)
                    .alias(Self::EXIT_LABEL),
                (col(LAG_8_ROLLING_7_STD) / col(ADR)).alias(LAG_8_ROLLING_7_STD_ADR_DIV),
                (col(LAG_8_ROLLING_21_STD) / col(ADR)).alias(LAG_8_ROLLING_21_STD_ADR_DIV),
                (col(LAG_16_ROLLING_7_STD) / col(ADR)).alias(LAG_16_ROLLING_7_STD_ADR_DIV),
                (col(LAG_16_ROLLING_21_STD) / col(ADR)).alias(LAG_16_ROLLING_21_STD_ADR_DIV),
                ((col(MA_7) - col(MA_7_ROLLING_7_STD_LOWER)) / col(ATR))
                    .alias(MA_7_ROLLING_7_STD_LOWER_ATR_REL),
                ((col(MA_7_ROLLING_7_STD_UPPER) - col(MA_7)) / col(ATR))
                    .alias(MA_7_ROLLING_7_STD_UPPER_ATR_REL),
                ((col(MA_21) - col(MA_21_ROLLING_21_STD_LOWER)) / col(ATR))
                    .alias(MA_21_ROLLING_21_STD_LOWER_ATR_REL),
                ((col(MA_21_ROLLING_21_STD_UPPER) - col(MA_21)) / col(ATR))
                    .alias(MA_21_ROLLING_21_STD_UPPER_ATR_REL),
                (col(MA_DIFF_21_7_ROLLING_7_STD) / col(ATR))
                    .alias(MA_DIFF_21_7_ROLLING_7_STD_ATR_REL),
            ];

            let df_schematic = DataFrameSchematic::new(
                schema_fields,
                vec![
                    feature_expressions,
                    derived_feature_expressions1,
                    derived_feature_expressions2,
                    derived_feature_expressions3,
                ],
            );

            TradingSystemBlueprint {
                id: Self::TRADING_SYSTEM_NAME,
                df_schematic: df_schematic,
            }
        }
    }
}

