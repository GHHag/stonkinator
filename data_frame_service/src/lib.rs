#[cfg(feature = "python-exp")]
use pyo3::prelude::*;

pub mod blueprint;
pub mod command;
pub mod feature_expression;
pub mod flight_service;
pub mod grpc_service;

use blueprint::ml::MLTradingSystemExample;
use blueprint::momentum::TradingSystemExample;

#[cfg(feature = "python-exp")]
#[pymodule]
fn data_frame_service(module: &Bound<'_, PyModule>) -> PyResult<()> {
    const TRADING_SYSTEM_NAME: &str = "TRADING_SYSTEM_NAME";
    const ENTRY_CONDITION_COL: &str = "ENTRY_CONDITION_COL";
    const FEATURES: &str = "FEATURES";

    let trading_system_example_module =
        PyModule::new(module.py(), TradingSystemExample::TRADING_SYSTEM_NAME)?;
    trading_system_example_module.add(
        TRADING_SYSTEM_NAME,
        TradingSystemExample::TRADING_SYSTEM_NAME,
    )?;
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
    ml_trading_system_example_module.add(
        ENTRY_CONDITION_COL,
        MLTradingSystemExample::ENTRY_CONDITION_COL,
    )?;
    ml_trading_system_example_module.add(FEATURES, MLTradingSystemExample::FEATURES)?;
    module.add_submodule(&ml_trading_system_example_module)?;

    Ok(())
}
