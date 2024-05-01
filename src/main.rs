use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::thread;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        thread::spawn(move || {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                loop {
                    let line = {
                        let mut buf_reader = BufReader::new(&mut _stream);
                        let mut line = String::new();
                        if buf_reader.read_line(&mut line).is_ok() {
                            line
                        } else {
                            break;
                        }
                    };
                    println!("sending response to {}", line);
                    if write!(_stream, "+PONG\r\n").is_err() {
                        break;
                    }
                    _stream.flush().unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
        });
    }
}
