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

struct TradingSystemBlueprint {
    id: &'static str,
    df_schematic: DataFrameSchematic,
    _minimum_rows: u16,
}

trait TradingSystem {
    fn new() -> TradingSystemBlueprint;
}

pub fn get_trading_system_data_blueprints() -> Vec<(&'static str, DataFrameSchematic)> {
    let mut trading_system_data_blueprints: Vec<(&'static str, DataFrameSchematic)> =
        Vec::new();

    let trading_system_example_blueprint = momentum::TradingSystemExample::new();
    trading_system_data_blueprints.push((
        trading_system_example_blueprint.id,
        trading_system_example_blueprint.df_schematic,
    ));

    trading_system_data_blueprint
}

mod momentum {
    use polars::datatypes::DataType;
    use polars::prelude::datatypes::Field;
    use polars::prelude::{PlSmallStr, RollingOptionsFixedWindow};

    use crate::blueprint::{TradingSystem, TradingSystemBlueprint};
    use crate::feature_expression::apply_rolling_max;
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
                ));

            TradingSystemBlueprint {
                id: TradingSystemExample::ID,
                df_schematic: df_schematic,
                _minimum_rows: TradingSystemExample::N_PERIOD_HIGH as u16,
            }
        }
    }
}
