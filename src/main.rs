use std::{env, thread};
use std::net::TcpListener;

use anyhow::Result;

use net::{Binding, Port};

use crate::connection::ClientConnectionHandler;
use crate::master::{MasterConnection, MasterServer};
use crate::redis::RedisServer;
use crate::replica::{ReplicaConnection, start_replication};

mod resp;
mod redis;
mod client;
mod net;
mod rdb;
mod writer;
mod command;
mod master;
mod replica;
mod connection;


const DEFAULT_PORT: Port = 6379;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();

    let mut args_str = args
        .iter()
        .map(|s| s.as_str());

    // parse options
    let mut port = DEFAULT_PORT;
    let mut replica_of: Option<Binding> = None;
    let mut dir = ".".to_string();
    let mut dbfilename = "rds".to_string();
    loop {
        let option = args_str.next();
        if option.is_none() {
            break;
        }
        if let Some("--port") = option {
            port = args_str.next().unwrap().parse()?;
        }
        if let Some("--replicaof") = option {
            let host_port = args_str.next().unwrap().to_string();
            let mut seq = host_port.split(' ');
            let host = seq.next().unwrap();
            let default_port_str = DEFAULT_PORT.to_string();
            let port = seq.next().unwrap_or(&default_port_str).parse::<Port>()?;
            let master = Binding(host.to_string(), port);
            println!("replicating from master {}", master);
            replica_of = Some(master)
        }
        if let Some("--dir") = option {
            dir = args_str.next().unwrap().to_string();
        }
        if let Some("--dbfilename") = option {
            dbfilename = args_str.next().unwrap().to_string();
        }
    }

    let is_replica = replica_of.is_some();
    let label = if is_replica { "replica" } else { "master" };

    println!("starting redis {} on port {}", label, port);

    let bind_address = Binding("127.0.0.1".to_string(), port);
    let listener = TcpListener::bind(bind_address.to_string()).unwrap();

    let redis = RedisServer::new(bind_address, !is_replica, dir, dbfilename)?;

    if is_replica {
        start_replication(redis.clone(), replica_of.clone().unwrap())?;
    }

    let master: Option<MasterServer> = if !is_replica { Some(MasterServer::new(redis.clone())) } else { None };

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                let redis = redis.clone(); // cheap op since server contains mostly references
                let thread_name = format!("client-{}-{}", label, _stream.peer_addr()?);
                let remote_host = _stream.peer_addr()?;
                let replica_of = replica_of.clone();
                let master = master.clone();
                thread::Builder::new().name(thread_name.clone()).spawn(move || {
                    println!("accepted new connection @{}", thread_name);
                    if is_replica {
                        let mut server = ReplicaConnection::new(redis, replica_of.unwrap());
                        server.handle_connection(_stream).unwrap_or_else(|err|
                            {
                                println!("@{}:connection thread failed: {}", thread_name, err);
                            });
                    } else {
                        let mut server = MasterConnection::new(master.unwrap(), remote_host);
                        server.handle_connection(_stream).unwrap_or_else(|err|
                            {
                                println!("@{}:connection thread failed: {}", thread_name, err);
                            });
                    };
                })?;
            }
            Err(e) => {
                println!("receiving connection failed: {}", e);
            }
        }
    }
    Ok(())
}


