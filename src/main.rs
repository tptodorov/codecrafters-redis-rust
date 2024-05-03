use std::net::TcpListener;
use std::{env, thread};

use anyhow::bail;
use anyhow::Result;

use resp::{RESP, RESPReader};

use crate::commands::RedisServer;

mod resp;
mod commands;

const DEFAULT_PORT: u32 = 6379;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    println!("all args: {:?}", args);

    let args_str = args
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<&str>>();

    let port: u32 = match args_str.as_slice() {
        ["--port", port] => port.parse::<u32>()?,
        _ => DEFAULT_PORT,
    };

    println!("starting redis server on port {}", port);

    let bind_address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(bind_address).unwrap();

    let server = RedisServer::new();

    for stream in listener.incoming() {
        let server = server.clone(); // cheap op since server contains mostly references
        thread::spawn(move || {
            match stream {
                Ok(_stream) => {
                    println!("accepted new connection");
                    let mut reader = RESPReader::new(_stream);
                    loop {
                        if let Some(command) = reader.next() {
                            println!("sending response to {:?}", command);
                            match handler(&server, &command) {
                                Ok(response) => {
                                    println!("{:?} -> {:?}", command, response);
                                    if let Err(err) = reader.response(&response) {
                                        println!("error while writing response: {}. terminating connection", err);
                                        break;
                                    }
                                    // loop continues to read the next message
                                }
                                Err(err) => {
                                    println!("error while handling command: {}. terminating connection", err);
                                    break;
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        });
    }
    Ok(())
}

fn handler(server: &RedisServer, message: &RESP) -> Result<RESP> {
    println!("message: {:?}", message);
    match message {
        RESP::Array(array) =>
            match &array[..] {
                [RESP::Bulk(command), params @ .. ] => server.command_handler(command.to_uppercase().as_str(), params),
                _ => bail!("Invalid message".to_string()),
            }
        ,
        _ => bail!("Invalid message".to_string()),
    }
}
