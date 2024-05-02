use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use anyhow::{anyhow, Result};
use anyhow::bail;

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
                    // let mut reader = BufReader::new(_stream);
                    // loop {
                    //     let mut buf = String::new();
                    //     if let Ok(len) = reader.read_line(&mut buf) {
                    //         println!("sending response to {:?}", buf.trim());
                    //         if len == 0 {
                    //             break;
                    //         }
                    //     } else {
                    //         break;
                    //     }
                    // }
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
                [RESP::Bulk(command), params @ .. ] => command_handler(command.to_uppercase().as_str(), params),
                _ => bail!("Invalid message".to_string()),
            }
        ,
        _ => bail!("Invalid message".to_string()),
    }
}

fn command_handler(cmd: &str, params: &[RESP]) -> Result<RESP> {
    match cmd {
        "PING" => Ok(RESP::String("PONG".to_string())),
        "ECHO" => {
            let param1 = params.get(0).unwrap_or(&RESP::Null);

            match param1 {
                RESP::Bulk(param1) => Ok(RESP::Bulk(param1.to_owned())),
                _ => Err(anyhow!("invalid echo  command {:?}", params)),
            }
        }
        _ => Err(anyhow!("Unknown command {}", cmd)),
    }
}

#[derive(Debug)]
enum RESP {
    String(String),
    Error(String),
    Int(i64),
    Bulk(String),
    Array(Vec<RESP>),
    Null,
}

struct RESPReader {
    stream: TcpStream,
}

impl RESPReader {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub fn response(&mut self, response: &RESP) -> Result<()> {
        let mut writer = BufWriter::new(&self.stream);

        write_resp(&mut writer, response)?;
        writer.flush()?;
        Ok(())
    }
}

impl Iterator for RESPReader {
    type Item = RESP;

    fn next(&mut self) -> Option<Self::Item> {
        let mut reader = BufReader::new(&self.stream);

        read_resp(&mut reader).unwrap_or_else(|err| {
            println!("error: {:?}", err);
            None
        })
    }
}


fn write_resp(writer: &mut BufWriter<&TcpStream>, response: &RESP) -> Result<()> {
    match response {
        RESP::String(s) => {
            write!(writer, "+{}\r\n", s)?;
        }
        RESP::Error(s) => {
            write!(writer, "-{}\r\n", s)?;
        }
        RESP::Int(n) => {
            write!(writer, ":{}\r\n", n)?;
        }
        RESP::Bulk(s) => {
            write!(writer, "${}\r\n{}\r\n", s.len(), s)?;
        }
        RESP::Null => {
            write!(writer, "$-1\r\n")?;
        }
        RESP::Array(array) => {
            write!(writer, "*{}\r\n", array.len())?;
            if array.len() > 0 {
                for item in array {
                    write_resp(writer, item)?;
                }
            }
        }
    }
    Ok(())
}

fn read_resp(reader: &mut BufReader<&TcpStream>) -> Result<Option<RESP>> {
    let buf = &mut String::new();
    match reader.read_line(buf) {
        Ok(0) => {
            bail!("connection closed by peer");
        }
        Ok(_len) => {
            let line = buf.trim();
            println!("read line: {}", line);
            if line.is_empty() {
                bail!("empty line");
            } else {
                let type_char = line.chars().nth(0);
                match type_char {
                    Some('+') => Ok(Some(RESP::String(line[1..].to_string()))),
                    Some('-') => Ok(Some(RESP::Error(line[1..].to_string()))),
                    Some(':') => Ok(Some(RESP::Int(line[1..].parse().unwrap()))),
                    Some('$') => {
                        let len: i64 = line[1..].parse().unwrap();
                        if len < 0 {
                            Ok(Some(RESP::Null))
                        } else {
                            let mut buf: Vec<u8> = vec![0; len as usize + 2]; // read also the 2 bytes /r/n after the string which are used as delimiters
                            if reader.read_exact(&mut buf).is_ok() {
                                buf.truncate(len as usize); // drop the 2 bytes at the end since they are only delimiters
                                let bulk_string = String::from_utf8(buf)?;
                                Ok(Some(RESP::Bulk(bulk_string)))
                            } else {
                                Ok(None)
                            }
                        }
                    }
                    Some('*') => {
                        let len: u64 = line[1..].parse().unwrap();
                        if len == 0 {
                            Ok(Some(RESP::Array(vec![])))
                        } else {
                            let mut array = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                let item = read_resp(reader)?.unwrap();
                                println!("adding item: {:?}", item);
                                array.push(item);
                            }
                            Ok(Some(RESP::Array(array)))
                        }
                    }
                    _ => {
                        bail!("unknown command {}", line);
                    }
                }
            }
        }
        _ => {
            bail!("read error");
        }
    }
}
