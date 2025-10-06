use polars::datatypes::AnyValue;
use polars::prelude::PlSmallStr;
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use proto::data_frame_service_server::DataFrameService;
use proto::get_by::{AltIdentifier as GetByAltId, Identifier as GetById};
use proto::operate_on::{AltIdentifier as OperateOnAltId, Identifier as OperateOnId};
use proto::{Cud, GetBy, MinimumRows, OperateOn, Presence, Price};
use sterunets::{DataFrameCollection, RawData};

pub mod proto {
    // TODO: How to get env variable for the protobuf package into scope here?
    tonic::include_proto!("stonkinator");
}

impl RawData for Price {
    fn validate(&self, df: &DataFrame) -> PolarsResult<bool> {
        if df.height() == 0 {
            return Ok(true);
        }

        let timestamp = match self.timestamp {
            Some(ts) => ts.unix_timestamp_seconds,
            None => {
                return Err(PolarsError::NoData(
                    String::from("failed to parse timestamp").into(),
                ));
            }
        };

        let last_row_index = df.height() - 1;
        let latest_timestamp = match df.column(&Price::TIMESTAMP)?.get(last_row_index)? {
            AnyValue::UInt64(timestamp) => timestamp,
            _ => {
                return Err(PolarsError::NoData(
                    String::from("failed to parse timestamp").into(),
                ));
            }
        };

        Ok(timestamp > latest_timestamp)
    }

    fn validate_series(
        &self,
        _series_data: &HashMap<PlSmallStr, Series>,
        df: &DataFrame,
    ) -> PolarsResult<bool> {
        self.validate(df)
    }

    fn format(&self) -> PolarsResult<Vec<(PlSmallStr, AnyValue<'_>)>> {
        let timestamp = match self.timestamp {
            Some(ts) => ts.unix_timestamp_seconds,
            None => {
                return Err(PolarsError::NoData(
                    String::from("failed to parse timestamp").into(),
                ));
            }
        };

        let data_point: Vec<(PlSmallStr, AnyValue)> = vec![
            (Price::INSTRUMENT_ID, AnyValue::String(&self.instrument_id)),
            (Price::OPEN, AnyValue::Float64(self.open)),
            (Price::HIGH, AnyValue::Float64(self.high)),
            (Price::LOW, AnyValue::Float64(self.low)),
            (Price::CLOSE, AnyValue::Float64(self.close)),
            (Price::VOLUME, AnyValue::UInt64(self.volume)),
            (Price::TIMESTAMP, AnyValue::UInt64(timestamp)),
        ];

        Ok(data_point)
    }
}

impl Price {
    pub const INSTRUMENT_ID: PlSmallStr = PlSmallStr::from_static("instrument_id");
    pub const OPEN: PlSmallStr = PlSmallStr::from_static("open");
    pub const HIGH: PlSmallStr = PlSmallStr::from_static("high");
    pub const LOW: PlSmallStr = PlSmallStr::from_static("low");
    pub const CLOSE: PlSmallStr = PlSmallStr::from_static("close");
    pub const VOLUME: PlSmallStr = PlSmallStr::from_static("volume");
    pub const TIMESTAMP: PlSmallStr = PlSmallStr::from_static("timestamp");

    pub fn schema_fields() -> Vec<Field> {
        vec![
            Field::new(Price::INSTRUMENT_ID, DataType::String),
            Field::new(Price::OPEN, DataType::Float64),
            Field::new(Price::HIGH, DataType::Float64),
            Field::new(Price::LOW, DataType::Float64),
            Field::new(Price::CLOSE, DataType::Float64),
            Field::new(Price::VOLUME, DataType::UInt64),
            Field::new(Price::TIMESTAMP, DataType::UInt64),
        ]
    }

    #[allow(unused)]
    fn pl_map_format(&self) -> Result<HashMap<PlSmallStr, Series>, &'static str> {
        let timestamp = match self.timestamp {
            Some(ts) => ts.unix_timestamp_seconds,
            None => return Err("failed to parse timestamp"),
        };

        let mut data_point: HashMap<PlSmallStr, Series> = HashMap::new();

        data_point.insert(
            Price::INSTRUMENT_ID,
            Series::new(
                Price::INSTRUMENT_ID,
                [AnyValue::String(&self.instrument_id)],
            ),
        );
        data_point.insert(
            Price::OPEN,
            Series::new(Price::OPEN, [AnyValue::Float64(self.open)]),
        );
        data_point.insert(
            Price::HIGH,
            Series::new(Price::HIGH, [AnyValue::Float64(self.high)]),
        );
        data_point.insert(
            Price::LOW,
            Series::new(Price::LOW, [AnyValue::Float64(self.low)]),
        );
        data_point.insert(
            Price::CLOSE,
            Series::new(Price::CLOSE, [AnyValue::Float64(self.close)]),
        );
        data_point.insert(
            Price::VOLUME,
            Series::new(Price::VOLUME, [AnyValue::UInt64(self.volume)]),
        );
        data_point.insert(
            Price::TIMESTAMP,
            Series::new(Price::TIMESTAMP, [AnyValue::UInt64(timestamp)]),
        );

        Ok(data_point)
    }

    // TODO: Try creating a similar method that creates a RecordBatch/RecordBatchT instead
    fn pl_series_format(
        price_data: Vec<Price>,
    ) -> Result<HashMap<PlSmallStr, Series>, &'static str> {
        let price_data_len = price_data.len();
        let mut instrument_id_data: Vec<String> = Vec::with_capacity(price_data_len);
        let mut open_price_data: Vec<f64> = Vec::with_capacity(price_data_len);
        let mut high_price_data: Vec<f64> = Vec::with_capacity(price_data_len);
        let mut low_price_data: Vec<f64> = Vec::with_capacity(price_data_len);
        let mut close_price_data: Vec<f64> = Vec::with_capacity(price_data_len);
        let mut volume_data: Vec<u64> = Vec::with_capacity(price_data_len);
        let mut timestamp_data: Vec<u64> = Vec::with_capacity(price_data_len);

        for price in price_data {
            instrument_id_data.push(price.instrument_id);
            open_price_data.push(price.open);
            high_price_data.push(price.high);
            low_price_data.push(price.low);
            close_price_data.push(price.close);
            volume_data.push(price.volume);

            let timestamp = match price.timestamp {
                Some(ts) => ts.unix_timestamp_seconds,
                None => return Err("failed to parse timestamp"),
            };
            timestamp_data.push(timestamp);
        }

        let mut price_data_series_map: HashMap<PlSmallStr, Series> = HashMap::new();
        price_data_series_map.insert(
            Price::INSTRUMENT_ID,
            Series::new(Price::INSTRUMENT_ID, instrument_id_data),
        );
        price_data_series_map.insert(Price::OPEN, Series::new(Price::OPEN, open_price_data));
        price_data_series_map.insert(Price::HIGH, Series::new(Price::HIGH, high_price_data));
        price_data_series_map.insert(Price::LOW, Series::new(Price::LOW, low_price_data));
        price_data_series_map.insert(Price::CLOSE, Series::new(Price::CLOSE, close_price_data));
        price_data_series_map.insert(Price::VOLUME, Series::new(Price::VOLUME, volume_data));
        price_data_series_map.insert(
            Price::TIMESTAMP,
            Series::new(Price::TIMESTAMP, timestamp_data),
        );

        Ok(price_data_series_map)
    }
}

impl OperateOn {
    pub fn parse(&self) -> Result<(Option<String>, Option<String>), &'static str> {
        let str_id = match &self.identifier {
            Some(OperateOnId::StrIdentifier(str_id)) => Some(String::from(str_id)),
            Some(OperateOnId::IntIdentifier(_)) => {
                return Err("integer id is not supported");
            }
            None => None,
        };

        let alt_str_id = match &self.alt_identifier {
            Some(OperateOnAltId::AltStrIdentifier(alt_str_id)) => Some(String::from(alt_str_id)),
            Some(OperateOnAltId::AltIntIdentifier(_)) => {
                return Err("integer id is not supported");
            }
            None => None,
        };

        Ok((str_id, alt_str_id))
    }
}

impl GetBy {
    pub fn parse(&self) -> Result<(Option<String>, Option<String>), &'static str> {
        let str_id = match &self.identifier {
            Some(GetById::StrIdentifier(str_id)) => Some(String::from(str_id)),
            Some(GetById::IntIdentifier(_)) => {
                return Err("integer id is not supported");
            }
            None => None,
        };

        let alt_str_id = match &self.alt_identifier {
            Some(GetByAltId::AltStrIdentifier(alt_str_id)) => Some(String::from(alt_str_id)),
            Some(GetByAltId::AltIntIdentifier(_)) => {
                return Err("integer id is not supported");
            }
            None => None,
        };

        Ok((str_id, alt_str_id))
    }
}

#[derive(Debug)]
pub struct DataFrameServiceImpl {
    pub df_collection: Arc<DataFrameCollection>,
}

impl DataFrameServiceImpl {
    async fn map_trading_system(&self, identifiers: OperateOn) -> Result<bool, &'static str> {
        let ids = identifiers.parse()?;

        match ids {
            (Some(trading_system_id), Some(instrument_id)) => {
                let successful = self
                    .df_collection
                    .insert_inner_map(instrument_id, trading_system_id)
                    .await;
                Ok(successful)
            }
            _ => return Err("failed to parse identifiers"),
        }
    }

    async fn handle_price(&self, price: Price) -> Result<u32, String> {
        let num_appended = self
            .df_collection
            .append_data_point(&price.instrument_id, &price)
            .await
            .map_err(|e| {
                format!(
                    "failed to append data point for instrument with id: {}, error: {e}",
                    &price.instrument_id
                )
            })?;

        Ok(num_appended)
    }

    async fn handle_price_data(&self, price_data: Vec<Price>) -> Result<u32, String> {
        let price_data_len = price_data.len();
        if price_data_len == 0 {
            return Err(String::from("length of price_data was 0"));
        }
        let price = price_data[0].clone();
        let instrument_id = &price.instrument_id;
        let pl_series = Price::pl_series_format(price_data)?;

        let num_appended = self.df_collection
            .append_series(instrument_id, pl_series, price_data_len, &price)
            .await
            .map_err(|e| {
                format!(
                    "failed to process series data for instrument with id: {instrument_id}, error: {e}"
                )
            })?;

        Ok(num_appended)
    }

    async fn set_minimum_rows_required(
        &self,
        minimum_rows: MinimumRows,
    ) -> Result<bool, &'static str> {
        if let Some(identifiers) = minimum_rows.operate_on {
            let schematic_key = match identifiers.identifier {
                Some(OperateOnId::StrIdentifier(trading_system_id)) => trading_system_id,
                _ => return Err("failed to parse trading_system_id"),
            };

            let successful = self
                .df_collection
                .set_minimum_rows(&schematic_key, minimum_rows.num_rows)
                .await;

            Ok(successful)
        } else {
            Ok(false)
        }
    }

    async fn check_df_collection_presence(&self, identifiers: GetBy) -> Result<bool, &'static str> {
        let ids = identifiers.parse()?;

        match ids {
            (Some(trading_system_id), Some(instrument_id)) => {
                let trading_system_ids =
                    self.df_collection.inner_keys_of_outer(&instrument_id).await;
                match trading_system_ids {
                    Some(trading_system_ids) => Ok(trading_system_ids.contains(&trading_system_id)),
                    None => Ok(false),
                }
            }
            (Some(trading_system_id), None) => {
                let trading_system_ids = self.df_collection.df_schematic_keys().await;
                Ok(trading_system_ids.contains(&trading_system_id))
            }
            _ => return Err("no valid id pattern in input parameters"),
        }
    }

    async fn evict_df_on(&self, identifiers: OperateOn) -> Result<u32, String> {
        let ids = identifiers.parse()?;
        let mut evicted_count = 0_u32;

        match ids {
            (Some(trading_system_id), Some(instrument_id)) => {
                let successful = self.df_collection.evict_df(&instrument_id, &trading_system_id).await.map_err(|e| {
                    format!(
                        "failed to evict data frame with trading_system_id: {trading_system_id}, instrument_id: {instrument_id}, error: {e}"
                    )
                })?;
                if successful == true {
                    evicted_count = 1;
                }
            }
            (Some(trading_system_id), None) => {
                evicted_count += self.df_collection.evict_inner(&trading_system_id).await.map_err(|e| {
                    format!("failed to evict data frames with trading_system_id: {trading_system_id}, error: {e}")
                })?;
            }
            (None, Some(instrument_id)) => {
                let successful = self.df_collection.evict_outer(&instrument_id).await.map_err(|e| {
                    format!("failed to evict data frames with instrument_id: {instrument_id}, error: {e}")
                })?;
                if successful == true {
                    evicted_count = 1;
                }
            }
            _ => return Err(String::from("no valid id pattern in input parameters")),
        }

        Ok(evicted_count)
    }

    async fn remove_df_collection_entry(&self, identifiers: OperateOn) -> Result<bool, String> {
        let ids = identifiers.parse()?;

        match ids {
            (Some(trading_system_id), Some(instrument_id)) => self
                .df_collection
                .remove_df_map_entry(&instrument_id, &trading_system_id).await.map_err(|e| {
                    format!(
                        "failed to remove df collection entry with trading_system_id: {trading_system_id}, instrument_id: {instrument_id}, error: {e}"
                    )
                }),
            _ => return Err(String::from("invalid id input value")),
        }
    }
}

#[tonic::async_trait]
impl DataFrameService for DataFrameServiceImpl {
    async fn map_trading_system_instrument(
        &self,
        request: Request<OperateOn>,
    ) -> Result<Response<Cud>, Status> {
        let input = request.into_inner();
        let mut cud = Cud { num_affected: 0 };

        let map_trading_system_result = self.map_trading_system(input).await;
        match map_trading_system_result {
            Ok(successful) => {
                if successful == true {
                    cud.num_affected = 1;
                }
            }
            Err(err) => return Err(Status::internal(err)),
        }

        Ok(Response::new(cud))
    }

    async fn push_price(&self, request: Request<Price>) -> Result<Response<Cud>, Status> {
        let input = request.into_inner();

        let handle_price_result = self.handle_price(input).await;
        let cud = match handle_price_result {
            Ok(result) => Cud {
                num_affected: result,
            },
            Err(err) => return Err(Status::internal(err)),
        };

        Ok(Response::new(cud))
    }

    async fn push_price_stream(
        &self,
        request: Request<tonic::Streaming<Price>>,
    ) -> Result<Response<Cud>, Status> {
        let mut stream = request.into_inner();
        let mut price_data: Vec<Price> = Vec::new();

        while let Some(price) = stream.next().await {
            let price = price?;
            price_data.push(price);
        }

        let handle_price_data_result = self.handle_price_data(price_data).await;
        let cud = match handle_price_data_result {
            Ok(result) => Cud {
                num_affected: result,
            },
            Err(err) => return Err(Status::internal(err)),
        };

        Ok(Response::new(cud))
    }

    async fn set_minimum_rows(
        &self,
        request: Request<MinimumRows>,
    ) -> Result<Response<Cud>, Status> {
        let input = request.into_inner();
        let mut cud = Cud { num_affected: 0 };

        let set_minimum_rows_result = self.set_minimum_rows_required(input).await;
        match set_minimum_rows_result {
            Ok(successful) => {
                if successful == true {
                    cud.num_affected = 1;
                }
            }
            Err(err) => return Err(Status::internal(err)),
        }

        Ok(Response::new(cud))
    }

    async fn check_presence(&self, request: Request<GetBy>) -> Result<Response<Presence>, Status> {
        let input = request.into_inner();

        let check_df_collection_presence_result = self.check_df_collection_presence(input).await;
        let presence = match check_df_collection_presence_result {
            Ok(presence_check) => Presence {
                is_present: presence_check,
            },
            Err(err) => return Err(Status::internal(err)),
        };

        Ok(Response::new(presence))
    }

    async fn evict(&self, request: Request<OperateOn>) -> Result<Response<Cud>, Status> {
        let input = request.into_inner();

        let eviction_result = self.evict_df_on(input).await;
        let cud = match eviction_result {
            Ok(n_evicted) => Cud {
                num_affected: n_evicted,
            },
            Err(err) => return Err(Status::internal(err)),
        };

        Ok(Response::new(cud))
    }

    async fn drop_data_frame_collection_entry(
        &self,
        request: Request<OperateOn>,
    ) -> Result<Response<Cud>, Status> {
        let input = request.into_inner();
        let mut cud = Cud { num_affected: 0 };

        let entry_removal_result = self.remove_df_collection_entry(input).await;
        match entry_removal_result {
            Ok(successful) => {
                if successful == true {
                    cud.num_affected = 1;
                }
            }
            Err(err) => return Err(Status::internal(err)),
        };

        Ok(Response::new(cud))
    }
}
