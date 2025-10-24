use futures::StreamExt;
use futures::stream::FuturesUnordered;
use proto::data_frame_service_client::DataFrameServiceClient;
use rand::{Rng, thread_rng};
use std::env;
use std::error::Error;
use std::io;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tonic::Request;
use tonic::transport::Channel;

use data_frame_service::blueprint::create_trading_system_blueprints;
use proto::Price;

pub mod proto {
    tonic::include_proto!("stonkinator");
}

async fn map_trading_system_instrument_request(
    data_frame_service_client: &mut DataFrameServiceClient<Channel>,
    trading_system_id: &str,
    instrument_id: &str,
) -> Result<(), Box<dyn Error>> {
    let req = proto::OperateOn {
        identifier: Some(proto::operate_on::Identifier::StrIdentifier(String::from(
            trading_system_id,
        ))),
        alt_identifier: Some(proto::operate_on::AltIdentifier::AltStrIdentifier(
            String::from(instrument_id),
        )),
    };
    let request = tonic::Request::new(req);
    let response = data_frame_service_client
        .map_trading_system_instrument(request)
        .await?;

    println!(
        "map_trading_system_instrument response: {:?}",
        response.get_ref()
    );

    Ok(())
}

async fn push_price_request(
    data_frame_service_client: &mut DataFrameServiceClient<Channel>,
    price: Price,
) -> Result<(), Box<dyn Error>> {
    let request = tonic::Request::new(price);
    let response = data_frame_service_client.push_price(request).await?;

    println!("push_price response: {:?}", response.get_ref());

    Ok(())
}

async fn stream_price_request(
    data_frame_service_client: &mut DataFrameServiceClient<Channel>,
    price_data: Vec<Price>,
) -> Result<(), Box<dyn Error>> {
    let request = Request::new(tokio_stream::iter(price_data));

    match data_frame_service_client.push_price_stream(request).await {
        Ok(response) => println!("push_price_stream response: {:?}", response.into_inner()),
        Err(err) => println!("push_price_stream error: {:?}", err),
    }

    Ok(())
}

fn create_price_data(instrument_id: &str, n_items: u32) -> Vec<Price> {
    let mut rng = thread_rng();
    let mut price_data: Vec<Price> = Vec::new();
    let mut timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as u64;

    for _ in 0..n_items {
        let price = Price {
            instrument_id: String::from(instrument_id),
            open: rng.r#gen(),
            high: rng.gen_range(50.0..100.0),
            low: rng.gen_range(50.0..100.0),
            close: rng.gen_range(50.0..100.0),
            volume: rng.gen_range(50000..100000),
            timestamp: Some(proto::Timestamp {
                unix_timestamp_seconds: timestamp,
            }),
        };
        price_data.push(price);
        timestamp += 1;
    }

    price_data
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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
    let mut data_frame_service_client = DataFrameServiceClient::connect(addr).await?;

    let instrument_ids = vec!["instrument1", "instrument2"];
    let mut trading_systems: Vec<&str> = Vec::new();
    for blueprint in create_trading_system_blueprints() {
        trading_systems.push(&blueprint.id);
        println!("{}", blueprint.id);
    }

    for &instrument_id in instrument_ids.iter() {
        for &trading_system in trading_systems.iter() {
            map_trading_system_instrument_request(
                &mut data_frame_service_client,
                trading_system,
                instrument_id,
            )
            .await?;
        }
    }

    let start = Instant::now();
    let push_price_handles = FuturesUnordered::new();
    for instrument_id in instrument_ids {
        let args_clone = args.clone();
        let mut data_frame_service_client_clone = data_frame_service_client.clone();
        let push_price_handle = tokio::spawn(async move {
            let price_data = create_price_data(instrument_id, 1000);

            if args_clone.len() > 1 {
                match args_clone[1].as_str() {
                    "step" => {
                        for (i, price) in price_data.into_iter().enumerate() {
                            let response =
                                push_price_request(&mut data_frame_service_client_clone, price)
                                    .await;
                            if let Err(res) = response {
                                println!("Error: {:?}", res);
                            }

                            println!("{}, step {i}", instrument_id);
                            let _ = io::stdin().read_line(&mut String::new());
                        }
                    }
                    "stream" => {
                        let response =
                            stream_price_request(&mut data_frame_service_client_clone, price_data)
                                .await;
                        if let Err(res) = response {
                            println!("Error: {:?}", res);
                        }
                    }
                    _ => {
                        for price in price_data {
                            let response =
                                push_price_request(&mut data_frame_service_client_clone, price)
                                    .await;
                            if let Err(res) = response {
                                println!("Error: {:?}", res);
                            }
                        }
                    }
                }
            } else {
                for price in price_data {
                    let response =
                        push_price_request(&mut data_frame_service_client_clone, price).await;
                    if let Err(res) = response {
                        println!("Error: {:?}", res);
                    }
                }
            }
        });
        push_price_handles.push(push_price_handle);
    }

    push_price_handles.for_each(|_| async {}).await;
    let elapsed = start.elapsed();
    println!("time elapsed: {:?}", elapsed);
    println!("time elapsed: {} ms", elapsed.as_millis());
    println!("time elapsed: {} seconds", elapsed.as_secs_f64());

    Ok(())
}
