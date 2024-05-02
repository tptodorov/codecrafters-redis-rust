use std::io::{BufRead, Read, Write};
use std::net::TcpListener;
use std::thread;

use anyhow::Result;
use anyhow::bail;

use resp::{RESP, RESPReader};

mod resp;
mod commands;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        thread::spawn(move || {
            match stream {
                Ok(_stream) => {
                    println!("accepted new connection");
                    let mut reader = RESPReader::new(_stream);
                    loop {
                        if let Some(command) = reader.next() {
                            println!("sending response to {:?}", command);
                            if let Ok(response) = handler(&command) {
                                println!("{:?} -> {:?}", command, response);
                                if let Err(err) = reader.response(&response) {
                                    println!("error while writing response: {}. terminating connection", err);
                                    break;
                                }
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

fn handler(message: &RESP) -> Result<RESP> {
    println!("message: {:?}", message);
    match message {
        RESP::Array(array) =>
            match &array[..] {
                [RESP::Bulk(command), params @ .. ] => commands::command_handler(command.to_uppercase().as_str(), params),
                _ => bail!("Invalid message".to_string()),
            }
        ,
        _ => bail!("Invalid message".to_string()),
    }
}
