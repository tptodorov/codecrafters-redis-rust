use std::{env, thread};
use std::net::TcpListener;
use std::str::FromStr;

use anyhow::Result;

use crate::connection::ClientConnectionHandler;
use crate::io::net::{Binding, DEFAULT_PORT, Port};
use crate::master::{MasterConnection, MasterServer};
use crate::redis::RedisServer;
use crate::replica::{ReplicaConnection, start_replication};

mod client;
mod connection;
mod io;
mod master;
mod protocol;
mod redis;
mod replica;
mod store;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();

    // parse options
    let port = arg_option::<Port>(&args, "--port")?.unwrap_or(DEFAULT_PORT);
    let replica_of = arg_option::<Binding>(&args, "--replicaof")?;
    let dir = arg_option::<String>(&args, "--dir")?.unwrap_or(".".to_string());
    let dbfilename = arg_option::<String>(&args, "--dbfilename")?.unwrap_or("rds".to_string());

    let is_replica = replica_of.is_some();
    let label = if is_replica { "replica" } else { "master" };

    println!("starting redis {} on port {}", label, port);

    let bind_address = Binding("127.0.0.1".to_string(), port);
    let listener = TcpListener::bind(bind_address.to_string()).unwrap();

    let redis = RedisServer::new(bind_address, !is_replica, dir, dbfilename)?;

    if is_replica {
        start_replication(redis.clone(), replica_of.clone().unwrap())?;
    }

    let master: Option<MasterServer> = if !is_replica {
        Some(MasterServer::new(redis.clone()))
    } else {
        None
    };

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                let redis = redis.clone(); // cheap op since server contains mostly references
                let thread_name = format!("client-{}-{}", label, _stream.peer_addr()?);
                let remote_host = _stream.peer_addr()?;
                let replica_of = replica_of.clone();
                let master = master.clone();
                thread::Builder::new()
                    .name(thread_name.clone())
                    .spawn(move || {
                        println!("accepted new connection @{}", thread_name);
                        if is_replica {
                            let mut server = ReplicaConnection::new(redis, replica_of.unwrap());
                            server.handle_connection(_stream).unwrap_or_else(|err| {
                                println!("@{}:connection thread failed: {}", thread_name, err);
                            });
                        } else {
                            let mut server = MasterConnection::new(master.unwrap(), remote_host);
                            server.handle_connection(_stream).unwrap_or_else(|err| {
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

fn arg_option<R: FromStr>(args: &Vec<String>, name: &str) -> core::result::Result<Option<R>, R::Err> {
    args.iter()
        .position(|a| *a == name)
        .and_then(|i| args.get(i + 1))
        .map(|a| a.parse::<R>())
        .transpose()
}
