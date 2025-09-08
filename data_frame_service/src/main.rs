use arrow_flight::flight_service_server::FlightServiceServer;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tonic::transport::Server;

mod blueprint;
mod command;
mod feature_expression;
mod flight_service;
mod grpc_service;

use blueprint::get_trading_system_data_blueprints;
use flight_service::FlightServiceImpl;
use grpc_service::DataFrameServiceImpl;
use grpc_service::proto::data_frame_service_server::DataFrameServiceServer;
use sterunets::DataFrameCollection;
use sterunets::DataFrameSchematic;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = match env::var("DF_SERVICE_HOST") {
        Ok(port) => port,
        Err(_) => String::from("0.0.0.0"),
    };
    let port = match env::var("DF_SERVICE_PORT") {
        Ok(port) => port,
        Err(_) => String::from("50051"),
    };
    let addr = format!("{host}:{port}").parse()?;

    let trading_system_data_blueprint_tuples = get_trading_system_data_blueprints();
    let mut df_schematics: HashMap<String, DataFrameSchematic> = HashMap::new();
    for (id, df_schematic) in trading_system_data_blueprint_tuples {
        df_schematics
            .entry(String::from(id))
            .or_insert(df_schematic);
    }

    let df_collection = Arc::new(DataFrameCollection::new(df_schematics));

    let data_frame_service = DataFrameServiceImpl {
        df_collection: df_collection.clone(),
    };
    let data_frame_service_server = DataFrameServiceServer::new(data_frame_service);

    let flight_service = FlightServiceImpl {
        df_collection: df_collection,
    };
    let flight_server = FlightServiceServer::new(flight_service);

    Server::builder()
        .add_service(data_frame_service_server)
        .add_service(flight_server)
        .serve(addr)
        .await?;

    Ok(())
}
