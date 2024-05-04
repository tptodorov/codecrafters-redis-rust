use std::{env, thread};
use std::net::TcpListener;

use anyhow::bail;
use anyhow::Result;
use net::{Binding, Port};

use resp::{RESP, RESPReader};

use crate::redis::RedisServer;

mod resp;
mod redis;
mod client;
mod net;


const DEFAULT_PORT: Port = 6379;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();

    let mut args_str = args
        .iter()
        .map(|s| s.as_str());

    // parse options
    let mut port = DEFAULT_PORT;
    let mut replicaof: Option<Binding> = None;
    loop {
        let option = args_str.next();
        if option == None {
            break;
        }
        if let Some("--port") = option {
            port = args_str.next().unwrap().parse()?;
        }
        if let Some("--replicaof") = option {
            let host = args_str.next().unwrap().to_string();
            let port = args_str.next().unwrap().parse::<Port>()?;
            let master = Binding(host, port);
            replicaof = Some(master)
        }
    }


    println!("starting redis server on port {}", port);

    let bind_address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(bind_address).unwrap();

    let server = RedisServer::new(replicaof)?;

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

