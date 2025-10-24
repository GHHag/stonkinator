#[cfg(feature = "python-exp")]
use pyo3::prelude::*;

pub mod blueprint;
pub mod command;
pub mod feature_expression;
pub mod flight_service;
pub mod grpc_service;

use blueprint::ml::{MLTradingSystemExample, MetaLabelingExample};
use blueprint::momentum::TradingSystemExample;

#[cfg(feature = "python-exp")]
#[pymodule]
fn data_frame_service(module: &Bound<'_, PyModule>) -> PyResult<()> {
    const TRADING_SYSTEM_NAME: &str = "TRADING_SYSTEM_NAME";
    const MINIMUM_ROWS: &str = "MINIMUM_ROWS";
    const ENTRY_CONDITION_COL: &str = "ENTRY_CONDITION_COL";
    const EXIT_LABEL: &str = "EXIT_LABEL";
    const TARGET_PERIOD: &str = "TARGET_PERIOD";
    const FEATURES: &str = "FEATURES";

    let trading_system_example_module =
        PyModule::new(module.py(), TradingSystemExample::TRADING_SYSTEM_NAME)?;
    trading_system_example_module.add(
        TRADING_SYSTEM_NAME,
        TradingSystemExample::TRADING_SYSTEM_NAME,
    )?;
    trading_system_example_module.add(MINIMUM_ROWS, TradingSystemExample::MINIMUM_ROWS)?;
    trading_system_example_module.add(
        ENTRY_CONDITION_COL,
        TradingSystemExample::ENTRY_CONDITION_COL,
    )?;
    module.add_submodule(&trading_system_example_module)?;

    let ml_trading_system_example_module =
        PyModule::new(module.py(), MLTradingSystemExample::TRADING_SYSTEM_NAME)?;
    ml_trading_system_example_module.add(
        TRADING_SYSTEM_NAME,
        MLTradingSystemExample::TRADING_SYSTEM_NAME,
    )?;
    ml_trading_system_example_module.add(MINIMUM_ROWS, MLTradingSystemExample::MINIMUM_ROWS)?;
    ml_trading_system_example_module.add(
        ENTRY_CONDITION_COL,
        MLTradingSystemExample::ENTRY_CONDITION_COL,
    )?;
    ml_trading_system_example_module.add(TARGET_PERIOD, MLTradingSystemExample::TARGET_PERIOD)?;
    ml_trading_system_example_module.add(FEATURES, MLTradingSystemExample::FEATURES)?;
    module.add_submodule(&ml_trading_system_example_module)?;

    let meta_labeling_example_module =
        PyModule::new(module.py(), MetaLabelingExample::TRADING_SYSTEM_NAME)?;
    meta_labeling_example_module.add(
        TRADING_SYSTEM_NAME,
        MetaLabelingExample::TRADING_SYSTEM_NAME,
    )?;
    meta_labeling_example_module.add(MINIMUM_ROWS, MetaLabelingExample::MINIMUM_ROWS)?;
    meta_labeling_example_module.add(
        ENTRY_CONDITION_COL,
        MetaLabelingExample::ENTRY_CONDITION_COL,
    )?;
    meta_labeling_example_module.add(EXIT_LABEL, MetaLabelingExample::EXIT_LABEL)?;
    meta_labeling_example_module.add(TARGET_PERIOD, MetaLabelingExample::TARGET_PERIOD)?;
    meta_labeling_example_module.add(FEATURES, MetaLabelingExample::FEATURES)?;
    module.add_submodule(&meta_labeling_example_module)?;

    Ok(())
}
