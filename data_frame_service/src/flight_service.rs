use arrow::{array::RecordBatch, datatypes::Schema, error::ArrowError};
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as ArrowFlightResult,
    SchemaResult, Ticket, flight_service_server::FlightService, utils::batches_to_flight_data,
};
use bytes::Bytes;
use futures::stream;
use futures::stream::{BoxStream, Stream};
use std::pin::Pin;
use std::sync::Arc;
use tokio::task::JoinError;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::{Request, Response, Status, Streaming};

use crate::command::{DELIMITER, InfoCommand, TicketCommand};
use sterunets::DataFrameCollection;

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

#[derive(Clone)]
pub struct FlightServiceImpl {
    pub df_collection: Arc<DataFrameCollection>,
}

impl FlightServiceImpl {
    async fn create_flight_info(
        &self,
        flight_command: InfoCommand,
    ) -> Result<FlightInfo, JoinError> {
        let df_collection = Arc::clone(&self.df_collection);

        let flight_info = tokio::spawn(async move {
            let tickets = flight_command.dispatch(&df_collection).await;

            let mut endpoints: Vec<FlightEndpoint> = Vec::new();
            for ticket in tickets {
                endpoints.push(FlightEndpoint {
                    ticket: Some(Ticket::new(ticket)),
                    location: vec![],
                    expiration_time: None,
                    app_metadata: Bytes::new(),
                });
            }
            let total_records = endpoints.len();

            FlightInfo {
                schema: "".into(),
                flight_descriptor: None,
                endpoint: endpoints,
                total_records: total_records as i64,
                total_bytes: -1,
                ordered: true,
                app_metadata: Bytes::new(),
            }
        })
        .await?;

        Ok(flight_info)
    }

    async fn hit_endpoint(
        &self,
        flight_command: TicketCommand,
        n_rows: Option<&MetadataValue<Ascii>>,
        exclude_columns: Option<&MetadataValue<Ascii>>,
    ) -> Result<(Arc<Schema>, Vec<RecordBatch>), ArrowError> {
        let df_collection = Arc::clone(&self.df_collection);

        let n_rows = match n_rows {
            Some(n_rows) => match n_rows.to_str() {
                Ok(n_rows) => n_rows.parse::<usize>().ok(),
                Err(_) => None,
            },
            None => None,
        };

        let exclude_columns: Option<Vec<String>> = match exclude_columns {
            Some(exclude_columns) => match exclude_columns.to_str() {
                Ok(exclude_columns) => {
                    Some(exclude_columns.split(DELIMITER).map(String::from).collect())
                }
                Err(_) => None,
            },
            None => None,
        };

        tokio::spawn(async move {
            flight_command
                .dispatch(&df_collection, n_rows, exclude_columns)
                .await
        })
        .await
        .map_err(|e| ArrowError::IpcError(format!("IPC write process failed, error: {e}")))?
    }
}

/// A flight service is an endpoint for retrieving or storing Arrow data.
/// A flight service can expose one or more predefined endpoints that can be
/// accessed using the Arrow Flight Protocol. Additionally, a flight service
/// can expose a set of actions that are available.
#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<ArrowFlightResult, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    /// Handshake between client and server. Depending on the server, the
    /// handshake may be required to determine the token that should be used for
    /// future operations. Both request and response are streams to allow multiple
    /// round-trips depending on auth mechanism.
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("Implement handshake"))
    }

    /// Get a list of available streams given a particular criteria. Most flight
    /// services will expose one or more streams that are readily available for
    /// retrieval. This api allows listing the streams available for
    /// consumption. A user can also provide a criteria. The criteria can limit
    /// the subset of streams that can be listed via this interface. Each flight
    /// service allows its own definition of how to consume criteria.
    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let criteria = request.into_inner();

        let info_command = match str::from_utf8(&criteria.expression) {
            Ok(criteria_expression_str) => InfoCommand::parse(criteria_expression_str),
            Err(err) => {
                return Err(Status::invalid_argument(format!(
                    "failed to parse criteria expression, error: {err}"
                )));
            }
        };

        match self.create_flight_info(info_command).await {
            Ok(flight_info) => {
                let stream: Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>> =
                    Box::pin(stream::iter([Result::Ok(flight_info)]));
                let resp = Response::new(stream);

                Ok(resp)
            }
            Err(err) => {
                return Err(Status::not_found(format!(
                    "failed to create flight info, error: {err}",
                )));
            }
        }
    }

    /// For a given FlightDescriptor, get information about how the flight can be
    /// consumed. This is a useful interface if the consumer of the interface
    /// already can identify the specific flight to consume. This interface can
    /// also allow a consumer to generate a flight stream through a specified
    /// descriptor. For example, a flight descriptor might be something that
    /// includes a SQL statement or a Pickled Python operation that will be
    /// executed. In those cases, the descriptor will not be previously available
    /// within the list of available streams provided by ListFlights but will be
    /// available for consumption for the duration defined by the specific flight
    /// service.
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        match DescriptorType::try_from(descriptor.r#type) {
            Ok(DescriptorType::Unknown) => {
                return Err(Status::unimplemented(
                    "descriptor type Unknown not supported",
                ));
            }
            Ok(DescriptorType::Path) => {
                return Err(Status::unimplemented("descriptor type Path not supported"));
            }
            Ok(DescriptorType::Cmd) => {
                let info_command = match str::from_utf8(&descriptor.cmd) {
                    Ok(command_str) => InfoCommand::parse(command_str),
                    Err(err) => {
                        return Err(Status::invalid_argument(format!(
                            "failed to parse descriptor command, error: {err}"
                        )));
                    }
                };

                match self.create_flight_info(info_command).await {
                    Ok(flight_info) => Ok(Response::new(flight_info)),
                    Err(err) => Err(Status::not_found(format!(
                        "failed to created flight info for the descriptor {descriptor}, error: {err}",
                    ))),
                }
            }
            Err(err) => {
                return Err(Status::invalid_argument(format!(
                    "failed to read descriptor type, error: {err}"
                )));
            }
        }
    }

    /// For a given FlightDescriptor, start a query and get information
    /// to poll its execution status. This is a useful interface if the
    /// query may be a long-running query. The first PollFlightInfo call
    /// should return as quickly as possible. (GetFlightInfo doesn't
    /// return until the query is complete.)
    /// A client can consume any available results before
    /// the query is completed. See PollInfo.info for details.
    /// A client can poll the updated query status by calling
    /// PollFlightInfo() with PollInfo.flight_descriptor. A server
    /// should not respond until the result would be different from last
    /// time. That way, the client can "long poll" for updates
    /// without constantly making requests. Clients can set a short timeout
    /// to avoid blocking calls if desired.
    /// A client can't use PollInfo.flight_descriptor after
    /// PollInfo.expiration_time passes. A server might not accept the
    /// retry descriptor anymore and the query may be cancelled.
    /// A client may use the CancelFlightInfo action with
    /// PollInfo.info to cancel the running query.
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    /// For a given FlightDescriptor, get the Schema as described in Schema.fbs::Schema
    /// This is used when a consumer needs the Schema of flight stream. Similar to
    /// GetFlightInfo this interface may generate a new flight that was not previously
    /// available in ListFlights.
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    /// Retrieve a single stream associated with a particular descriptor
    /// associated with the referenced ticket. A Flight can be composed of one or
    /// more streams where each stream can be retrieved using a separate opaque
    /// ticket that the flight service uses for managing a collection of streams.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let (metadata, _extensions, ticket) = request.into_parts();

        let n_rows = metadata.get("n-rows");
        let exclude_columns = metadata.get("exclude");

        let ticket_command = match str::from_utf8(&ticket.ticket) {
            Ok(ticket_command_str) => TicketCommand::parse(ticket_command_str),
            Err(err) => {
                return Err(Status::invalid_argument(format!(
                    "failed to parse ticket, error: {err}"
                )));
            }
        };

        match self
            .hit_endpoint(ticket_command, n_rows, exclude_columns)
            .await
        {
            Ok((schema, batches)) => {
                let flight_data = batches_to_flight_data(&schema, batches)
                    .map_err(|e| status!("could not convert record batches, error: {}", e))?
                    .into_iter()
                    .map(Ok);
                let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
                    Box::pin(stream::iter(flight_data));

                Ok(Response::new(stream))
            }
            Err(err) => Err(Status::not_found(format!(
                "no flight data found for ticket: {:?}, error: {err}",
                str::from_utf8(&ticket.ticket)
                    .unwrap_or(format!("no flight data found for ticket, error: {err}").as_str())
            ))),
        }
    }

    /// Push a stream to the flight service associated with a particular
    /// flight stream. This allows a client of a flight service to upload a stream
    /// of data. Depending on the particular flight service, a client consumer
    /// could be allowed to upload a single stream per descriptor or an unlimited
    /// number. In the latter, the service might implement a 'seal' action that
    /// can be applied to a descriptor once all streams are uploaded.
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    /// Open a bidirectional data channel for a given descriptor. This
    /// allows clients to send and receive arbitrary Arrow data and
    /// application-specific metadata in a single logical stream. In
    /// contrast to DoGet/DoPut, this is more suited for clients
    /// offloading computation (rather than storage) to a Flight service.
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }

    /// Flight services can support an arbitrary number of simple actions in
    /// addition to the possible ListFlights, GetFlightInfo, DoGet, DoPut
    /// operations that are potentially available. DoAction allows a flight client
    /// to do a specific action against a flight service. An action includes
    /// opaque request and response objects that are specific to the type action
    /// being undertaken.
    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    /// A flight service exposes all of the available action types that it has
    /// along with descriptions. This allows different flight consumers to
    /// understand the capabilities of the flight service.
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }
}
