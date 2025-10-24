use arrow::{array::RecordBatch, datatypes::Schema, error::ArrowError};
use std::sync::Arc;

use sterunets::DataFrameCollection;

pub const INSTRUMENT: &str = "instrument";
pub const TRADING_SYSTEM: &str = "trading_system";
pub const DELIMITER: &str = ":";

impl InfoCommand {
    pub fn parse(input: &str) -> InfoCommand {
        let split_input: Vec<&str> = input.split(DELIMITER).collect();

        match split_input.as_slice() {
            [TRADING_SYSTEM] => InfoCommand::TradingSystems,

            [TRADING_SYSTEM, trading_system_id] => {
                InfoCommand::TradingSystem(trading_system_id.to_string())
            }

            [INSTRUMENT, instrument_id] => InfoCommand::Instrument(instrument_id.to_string()),

            [_] => InfoCommand::Unknown,
            [_, ..] => InfoCommand::Unknown,
            [] => InfoCommand::Unknown,
        }
    }

    pub async fn dispatch(&self, df_collection: &DataFrameCollection) -> Vec<String> {
        match self {
            InfoCommand::TradingSystems => {
                let mut tickets: Vec<String> = Vec::new();
                for trading_system_id in df_collection.df_schematic_keys().await {
                    tickets.push(format!("{TRADING_SYSTEM}{DELIMITER}{trading_system_id}"));
                }

                tickets
            }
            InfoCommand::TradingSystem(trading_system_id) => {
                let mut tickets: Vec<String> = Vec::new();

                if let Ok(outer_keys) = df_collection.outer_keys_of_inner(trading_system_id).await {
                    for instrument_id in outer_keys {
                        tickets.push(format!(
                            "{TRADING_SYSTEM}{DELIMITER}{trading_system_id}{DELIMITER}{INSTRUMENT}{DELIMITER}{instrument_id}",
                        ));
                    }
                }

                tickets
            }
            InfoCommand::Instrument(instrument_id) => {
                match df_collection.inner_keys_of_outer(instrument_id).await {
                    Some(inner_keys) => return inner_keys,
                    None => return vec![],
                }
            }
            InfoCommand::Unknown => {
                let tickets: Vec<String> = Vec::new();

                #[cfg(debug_assertions)]
                dbg!("the given command is Unknown to the server");

                tickets
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TicketCommand {
    TradingSystemInstrument {
        trading_system_id: String,
        instrument_id: String,
    },
    TradingSystemInstruments {
        trading_system_id: String,
        instrument_ids: Vec<String>,
    },
    InstrumentTradingSystem {
        instrument_id: String,
        trading_system_id: String,
    },
    InstrumentTradingSystems {
        instrument_id: String,
        trading_system_ids: Vec<String>,
    },
    Unknown,
}

impl TicketCommand {
    pub fn parse(input: &str) -> TicketCommand {
        let split_input: Vec<&str> = input.split(DELIMITER).collect();

        match split_input.as_slice() {
            [TRADING_SYSTEM, trading_system_id, INSTRUMENT, instrument_id] => {
                TicketCommand::TradingSystemInstrument {
                    trading_system_id: trading_system_id.to_string(),
                    instrument_id: instrument_id.to_string(),
                }
            }

            [
                TRADING_SYSTEM,
                trading_system_id,
                INSTRUMENT,
                instrument_ids @ ..,
            ] => TicketCommand::TradingSystemInstruments {
                trading_system_id: trading_system_id.to_string(),
                instrument_ids: instrument_ids.iter().map(|i| i.to_string()).collect(),
            },

            [INSTRUMENT, instrument_id, TRADING_SYSTEM, trading_system_id] => {
                TicketCommand::InstrumentTradingSystem {
                    instrument_id: instrument_id.to_string(),
                    trading_system_id: trading_system_id.to_string(),
                }
            }

            [
                INSTRUMENT,
                instrument_id,
                TRADING_SYSTEM,
                trading_system_ids @ ..,
            ] => TicketCommand::InstrumentTradingSystems {
                instrument_id: instrument_id.to_string(),
                trading_system_ids: trading_system_ids.iter().map(|ts| ts.to_string()).collect(),
            },

            [_] => TicketCommand::Unknown,
            [_, ..] => TicketCommand::Unknown,
            [] => TicketCommand::Unknown,
        }
    }

    pub async fn dispatch(
        &self,
        df_collection: &DataFrameCollection,
        num_rows: Option<u32>,
        exclude_columns: Option<Vec<String>>,
    ) -> Result<(Arc<Schema>, Vec<RecordBatch>), ArrowError> {
        match self {
            TicketCommand::TradingSystemInstrument {
                trading_system_id,
                instrument_id,
            } => {
                return df_collection
                    .df_to_arrow(instrument_id, trading_system_id, num_rows, exclude_columns)
                    .await
                    .map_err(|e| ArrowError::IpcError(format!("IPC write failed, {e}")));
            }
            // TicketCommand::TradingSystemInstruments {
            //     trading_system_id,
            //     instrument_ids,
            // } => {
            //     let tickets: Vec<String> = Vec::new();
            //     tickets
            // }
            // TicketCommand::InstrumentTradingSystem {
            //     instrument_id,
            //     trading_system_id,
            // } => {
            //     let tickets: Vec<String> = Vec::new();
            //     tickets
            // }
            // TicketCommand::InstrumentTradingSystems {
            //     instrument_id,
            //     trading_system_ids,
            // } => {
            //     let tickets: Vec<String> = Vec::new();
            //     tickets
            // }
            // TicketCommand::Unknown => {
            //     let tickets: Vec<String> = Vec::new();
            //     tickets
            // }
            _ => Err(ArrowError::InvalidArgumentError(String::from(
                "no path for the given ticket command",
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ticket_command() {
        assert_eq!(
            TicketCommand::parse(&format!(
                "{}:{}:{}:{}",
                TRADING_SYSTEM, "xyz", INSTRUMENT, "abc"
            ),),
            TicketCommand::TradingSystemInstrument {
                trading_system_id: String::from("xyz"),
                instrument_id: String::from("abc")
            }
        );

        assert_eq!(
            TicketCommand::parse(&format!(
                "{}:{}:{}:{}",
                TRADING_SYSTEM, "xyz", INSTRUMENT, "abc:def:ghi:jkl:mno"
            ),),
            TicketCommand::TradingSystemInstruments {
                trading_system_id: String::from("xyz"),
                instrument_ids: vec![
                    "abc".to_string(),
                    "def".to_string(),
                    "ghi".to_string(),
                    "jkl".to_string(),
                    "mno".to_string()
                ]
            }
        );

        assert_eq!(
            TicketCommand::parse(&format!(
                "{}:{}:{}:{}",
                INSTRUMENT, "abc", TRADING_SYSTEM, "xyz"
            ),),
            TicketCommand::InstrumentTradingSystem {
                instrument_id: String::from("abc"),
                trading_system_id: String::from("xyz")
            }
        );

        assert_eq!(
            TicketCommand::parse(&format!(
                "{}:{}:{}:{}",
                INSTRUMENT, "abc", TRADING_SYSTEM, "xyz:def:ghi:jkl:mno"
            ),),
            TicketCommand::InstrumentTradingSystems {
                instrument_id: String::from("abc"),
                trading_system_ids: vec![
                    "xyz".to_string(),
                    "def".to_string(),
                    "ghi".to_string(),
                    "jkl".to_string(),
                    "mno".to_string()
                ]
            }
        );

        assert_eq!(TicketCommand::parse(""), TicketCommand::Unknown);
        assert_eq!(TicketCommand::parse("blabla"), TicketCommand::Unknown);
        assert_eq!(TicketCommand::parse(":blabla"), TicketCommand::Unknown);
        assert_eq!(TicketCommand::parse("blabla:"), TicketCommand::Unknown);
        assert_eq!(
            TicketCommand::parse(&format!("{}:{}", "blabla", "bla")),
            TicketCommand::Unknown
        );
        assert_eq!(
            TicketCommand::parse(&format!("{}:{}:{}:{}", TRADING_SYSTEM, "bla", "bla", "bla"),),
            TicketCommand::Unknown
        );
        assert_eq!(
            TicketCommand::parse(&format!("{}:{}:{}:{}", "bla", "bla", TRADING_SYSTEM, "bla"),),
            TicketCommand::Unknown
        );
        assert_eq!(
            TicketCommand::parse(&format!("{}:{}:{}:{}", INSTRUMENT, "bla", "bla", "bla"),),
            TicketCommand::Unknown
        );
        assert_eq!(
            TicketCommand::parse(&format!("{}:{}:{}:{}", "bla", "bla", INSTRUMENT, "bla"),),
            TicketCommand::Unknown
        );
        assert_eq!(
            TicketCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                TRADING_SYSTEM, "bla", "bla", "bla", "bla", "bla"
            ),),
            TicketCommand::Unknown
        );
        assert_eq!(
            TicketCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                "bla", "bla", TRADING_SYSTEM, "bla", "bla", "bla"
            ),),
            TicketCommand::Unknown
        );
        assert_eq!(
            TicketCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                "bla", "bla", "bla", "bla", TRADING_SYSTEM, INSTRUMENT
            ),),
            TicketCommand::Unknown
        );
        assert_eq!(
            TicketCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                INSTRUMENT, "bla", "bla", "bla", "bla", "bla"
            ),),
            TicketCommand::Unknown
        );
        assert_eq!(
            TicketCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                "bla", "bla", INSTRUMENT, "bla", "bla", "bla"
            ),),
            TicketCommand::Unknown
        );
        assert_eq!(
            TicketCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                "bla", "bla", "bla", "bla", INSTRUMENT, TRADING_SYSTEM
            ),),
            TicketCommand::Unknown
        );
    }

    #[test]
    fn test_parse_flight_info_command() {
        assert_eq!(
            InfoCommand::parse(TRADING_SYSTEM),
            InfoCommand::TradingSystems
        );

        assert_eq!(
            InfoCommand::parse(&format!("{}:{}", TRADING_SYSTEM, "xyz")),
            InfoCommand::TradingSystem(String::from("xyz"))
        );

        assert_eq!(
            InfoCommand::parse(&format!("{}:{}", INSTRUMENT, "abc")),
            InfoCommand::Instrument(String::from("abc"))
        );

        assert_eq!(
            InfoCommand::parse(&format!(
                "{}:{}:{}:{}",
                TRADING_SYSTEM, "xyz", INSTRUMENT, "abc"
            ),),
            InfoCommand::Unknown
        );

        assert_eq!(
            InfoCommand::parse(&format!(
                "{}:{}:{}:{}",
                TRADING_SYSTEM, "xyz", INSTRUMENT, "abc:def:ghi:jkl:mno"
            ),),
            InfoCommand::Unknown
        );

        assert_eq!(
            InfoCommand::parse(&format!(
                "{}:{}:{}:{}",
                INSTRUMENT, "abc", TRADING_SYSTEM, "xyz"
            ),),
            InfoCommand::Unknown
        );

        assert_eq!(
            InfoCommand::parse(&format!(
                "{}:{}:{}:{}",
                INSTRUMENT, "abc", TRADING_SYSTEM, "xyz:def:ghi:jkl:mno"
            ),),
            InfoCommand::Unknown
        );

        assert_eq!(InfoCommand::parse(""), InfoCommand::Unknown);
        assert_eq!(InfoCommand::parse("blabla"), InfoCommand::Unknown);
        assert_eq!(InfoCommand::parse(":blabla"), InfoCommand::Unknown);
        assert_eq!(InfoCommand::parse("blabla:"), InfoCommand::Unknown);
        assert_eq!(
            InfoCommand::parse(&format!("{}:{}", "blabla", "bla")),
            InfoCommand::Unknown
        );
        assert_eq!(
            InfoCommand::parse(&format!("{}:{}:{}:{}", TRADING_SYSTEM, "bla", "bla", "bla"),),
            InfoCommand::Unknown
        );
        assert_eq!(
            InfoCommand::parse(&format!("{}:{}:{}:{}", "bla", "bla", TRADING_SYSTEM, "bla"),),
            InfoCommand::Unknown
        );
        assert_eq!(
            InfoCommand::parse(&format!("{}:{}:{}:{}", INSTRUMENT, "bla", "bla", "bla"),),
            InfoCommand::Unknown
        );
        assert_eq!(
            InfoCommand::parse(&format!("{}:{}:{}:{}", "bla", "bla", INSTRUMENT, "bla"),),
            InfoCommand::Unknown
        );
        assert_eq!(
            InfoCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                TRADING_SYSTEM, "bla", "bla", "bla", "bla", "bla"
            ),),
            InfoCommand::Unknown
        );
        assert_eq!(
            InfoCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                "bla", "bla", TRADING_SYSTEM, "bla", "bla", "bla"
            ),),
            InfoCommand::Unknown
        );
        assert_eq!(
            InfoCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                "bla", "bla", "bla", "bla", TRADING_SYSTEM, INSTRUMENT
            ),),
            InfoCommand::Unknown
        );
        assert_eq!(
            InfoCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                INSTRUMENT, "bla", "bla", "bla", "bla", "bla"
            ),),
            InfoCommand::Unknown
        );
        assert_eq!(
            InfoCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                "bla", "bla", INSTRUMENT, "bla", "bla", "bla"
            ),),
            InfoCommand::Unknown
        );
        assert_eq!(
            InfoCommand::parse(&format!(
                "{}:{}:{}:{}:{}:{}",
                "bla", "bla", "bla", "bla", INSTRUMENT, TRADING_SYSTEM
            ),),
            InfoCommand::Unknown
        );
    }
}
