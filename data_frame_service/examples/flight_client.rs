use arrow::datatypes::Schema;
use arrow_flight::{
    Criteria, FlightDescriptor, FlightInfo, Ticket, flight_descriptor::DescriptorType,
    flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch,
};
use bytes::Bytes;
use std::{collections::HashMap, env, io, sync::Arc};
use tokio_stream::StreamExt;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::transport::Error as TonicError;

use data_frame_service::blueprint::create_trading_system_blueprints;
use data_frame_service::command::{INSTRUMENT, TRADING_SYSTEM};

struct SterunetsFlightClient {
    pub client: FlightServiceClient<Channel>,
}

impl SterunetsFlightClient {
    pub async fn connect(address: &str) -> Result<SterunetsFlightClient, TonicError> {
        Ok(SterunetsFlightClient {
            client: FlightServiceClient::connect(String::from(address)).await?,
        })
    }

    pub async fn list_flights(&mut self, criteria: Criteria) -> Result<Vec<FlightInfo>, String> {
        let response = self
            .client
            .list_flights(criteria)
            .await
            .map_err(|e| format!("failed to list flights, error: {e}"))?;

        let mut flight_info_stream = response.into_inner();

        let mut flights: Vec<FlightInfo> = Vec::new();
        match flight_info_stream.next().await {
            Some(Ok(flight_info)) => flights.push(flight_info),
            Some(Err(err)) => return Err(format!("failed to stream flight, error: {err}")),
            None => {}
        }

        Ok(flights)
    }

    pub async fn get_flight_info(
        &mut self,
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, String> {
        // FlightDescriptor
        // type: i32
        // cmd: Bytes
        //      Opaque value used to express a command. Should only be defined when type = CMD.
        // path: Vec<String>
        //      List of strings identifying a particular dataset. Should only be defined when type = PATH.

        let response = self.client.get_flight_info(descriptor).await;
        match response {
            Ok(flight_info) => Ok(flight_info.into_inner()),
            Err(err) => return Err(format!("failed to get flight info, error: {err}")),
        }
    }

    pub async fn hit_endpoint(
        &mut self,
        request: Request<Ticket>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = self.client.do_get(request).await?.into_inner();
        let schema_data = stream
            .message()
            .await?
            .ok_or("missing schema from server")?;

        let schema = Arc::new(Schema::try_from(&schema_data)?);

        while let Some(data) = stream.message().await? {
            let batch = flight_data_to_arrow_batch(&data, schema.clone(), &HashMap::new())?;
            dbg!(batch);
        }

        Ok(())
    }
}

fn create_tickets(flight_info: FlightInfo) -> Vec<Ticket> {
    let mut tickets = Vec::with_capacity(flight_info.endpoint.len());
    for endpoint in flight_info.endpoint {
        if let Some(ticket) = endpoint.ticket {
            tickets.push(ticket);
        }
    }

    tickets
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let host = match env::var("DF_SERVICE_HOST") {
        Ok(port) => port,
        Err(_) => String::from("0.0.0.0"),
    };
    let port = match env::var("DF_SERVICE_PORT") {
        Ok(port) => port,
        Err(_) => String::from("50051"),
    };
    let addr: String = format!("http://{host}:{port}").parse().unwrap();
    let mut sterunets_flight_client = SterunetsFlightClient::connect(&addr).await?;

    let instrument_id = "instrument1";
    let mut trading_system_id = "";
    for blueprint in create_trading_system_blueprints() {
        trading_system_id = blueprint.id;
        break;
    }

    let trading_system_command = format!("{}:{}", TRADING_SYSTEM, trading_system_id);
    let instrument_command = format!("{}:{}", INSTRUMENT, instrument_id);

    if args.len() > 1 {
        match args[1].as_str() {
            "list_flights" => {
                let criteria = Criteria {
                    expression: Bytes::from(TRADING_SYSTEM),
                };
                let flight_info = sterunets_flight_client.list_flights(criteria).await?;
                dbg!(flight_info);

                let criteria = Criteria {
                    expression: Bytes::copy_from_slice(&trading_system_command.as_bytes()),
                };
                let flight_info = sterunets_flight_client.list_flights(criteria).await?;
                dbg!(flight_info);

                let criteria = Criteria {
                    expression: Bytes::copy_from_slice(&instrument_command.as_bytes()),
                };
                let flight_info = sterunets_flight_client.list_flights(criteria).await?;
                dbg!(flight_info);
            }
            "get_flight_info" => {
                let descriptor = FlightDescriptor {
                    r#type: DescriptorType::Cmd.into(),
                    cmd: Bytes::copy_from_slice(&trading_system_command.as_bytes()),
                    path: vec![],
                };
                let flight_info = sterunets_flight_client.get_flight_info(descriptor).await?;
                dbg!(&flight_info);
                let tickets = create_tickets(flight_info);
                dbg!(tickets);

                let descriptor = FlightDescriptor {
                    r#type: DescriptorType::Cmd.into(),
                    cmd: Bytes::copy_from_slice(&instrument_command.as_bytes()),
                    path: vec![],
                };
                let flight_info = sterunets_flight_client.get_flight_info(descriptor).await?;
                dbg!(&flight_info);
                let tickets = create_tickets(flight_info);
                dbg!(tickets);
            }
            "do_get" => {
                let descriptor = FlightDescriptor {
                    r#type: DescriptorType::Cmd.into(),
                    cmd: Bytes::copy_from_slice(&trading_system_command.as_bytes()),
                    path: vec![],
                };
                let flight_info = sterunets_flight_client.get_flight_info(descriptor).await?;
                dbg!(&flight_info);
                let tickets = create_tickets(flight_info);
                dbg!(&tickets);

                for ticket in tickets {
                    let request = Request::new(ticket);
                    let response = sterunets_flight_client.hit_endpoint(request).await?;
                    dbg!(response);
                }
            }
            _ => {}
        }
    } else {
        let criteria = Criteria {
            expression: Bytes::from(TRADING_SYSTEM),
        };
        let flights = sterunets_flight_client.list_flights(criteria).await?;
        dbg!(flights);

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let descriptor = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Bytes::from(TRADING_SYSTEM),
            path: vec![],
        };
        let flight_info = sterunets_flight_client.get_flight_info(descriptor).await?;
        dbg!(&flight_info);
        let tickets = create_tickets(flight_info);

        io::stdin().read_line(&mut input).unwrap();

        for ticket in tickets {
            dbg!(&ticket);

            let descriptor = FlightDescriptor {
                r#type: DescriptorType::Cmd.into(),
                cmd: ticket.ticket,
                path: vec![],
            };
            let flight_info = sterunets_flight_client.get_flight_info(descriptor).await?;
            dbg!(&flight_info);
            let inner_tickets = create_tickets(flight_info);
            dbg!(&inner_tickets);

            io::stdin().read_line(&mut input).unwrap();

            for ticket in inner_tickets {
                let mut request = Request::new(ticket);
                request
                    .metadata_mut()
                    .insert("n-rows", MetadataValue::from(10));
                let response = sterunets_flight_client.hit_endpoint(request).await;
                if let Ok(response) = response {
                    dbg!(response);
                }
            }
        }
    }

    Ok(())
}
