use std::{env, thread};
use std::net::TcpListener;

use anyhow::Result;
use net::{Binding, Port};

use crate::redis::RedisServer;

mod resp;
mod redis;
mod client;
mod net;
mod rdb;
mod replication;


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

    if replicaof.is_some() {
        println!("starting redis replica on port {}", port);
    } else {
        println!("starting redis master on port {}", port);
    }
    let bind_address = Binding("127.0.0.1".to_string(), port);
    let listener = TcpListener::bind(bind_address.to_string()).unwrap();

    let server = RedisServer::new(bind_address, replicaof)?;

    for stream in listener.incoming() {
        let server_connection = server.clone(); // cheap op since server contains mostly references
        thread::spawn(move || {
            match stream {
                Ok(_stream) => {
                    println!("accepted new connection");
                    server_connection.handle_connection(_stream).unwrap();
                }
                Err(e) => {
                    println!("connection thread failed: {}", e);
                }
            }
        });
    }
    Ok(())
}

