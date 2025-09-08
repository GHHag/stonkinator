use std::error::Error;
use std::fs::read_dir;
use std::{env, process};

use tonic_build::Config;

fn main() -> Result<(), Box<dyn Error>> {
    let proto_dir = env::var("PROTOBUF_DIR").unwrap();
    let proto_package = env::var("PROTOBUF_PACKAGE").unwrap();

    let proto_dir_content = read_dir(&proto_dir)
        .unwrap()
        .map(|res| res.map(|e| e.path()))
        .collect::<Vec<_>>();

    let mut proto_files: Vec<String> = Vec::new();
    for proto_dir_item in proto_dir_content {
        match proto_dir_item {
            Ok(file) => {
                if file.is_file() {
                    proto_files.push(String::from(
                        file.to_str().expect("failed to process file as a str"),
                    ))
                }
            }
            Err(err) => {
                eprintln!("failed to list protobuf directory content, err: {err}");
                process::exit(1);
            }
        }
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .bytes([proto_package])
        .compile_protos_with_config(Config::new(), &proto_files, &[proto_dir])?;

    Ok(())
}
