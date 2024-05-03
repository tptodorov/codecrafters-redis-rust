use std::net::TcpListener;
use std::thread;

use anyhow::bail;
use anyhow::Result;

use resp::{RESP, RESPReader};

use crate::commands::RedisServer;

mod resp;
mod commands;

fn main() {
    println!("starting redis server");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

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
                            if let Ok(response) = handler(&server, &command) {
                                println!("{:?} -> {:?}", command, response);
                                if let Err(err) = reader.response(&response) {
                                    println!("error while writing response: {}. terminating connection", err);
                                    break;
                                }
                                // loop continues to read the next message
                            } else {
                                break;
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
