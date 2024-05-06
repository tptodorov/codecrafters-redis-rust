use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;

use anyhow::bail;
use anyhow::Result;

#[derive(Debug, Clone, PartialEq)]
pub enum RESP {
    String(String),
    Error(String),
    Int(i64),
    Bulk(String),
    Array(Vec<RESP>),
    Null,
    File(Vec<u8>),
}

pub struct RESPConnection {
    buf_reader: BufReader<TcpStream>,
    buf_writer: BufWriter<TcpStream>,
}

impl RESPConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self { buf_reader: BufReader::new(stream.try_clone().unwrap()), buf_writer: BufWriter::new(stream) }
    }

    pub fn response(&mut self, response: &RESP) -> Result<()> {
        self.responses(&[response])
    }

    pub fn responses(&mut self, responses: &[&RESP]) -> Result<()> {
        for response in responses {
            write_resp(&mut self.buf_writer, response)?;
            self.buf_writer.flush()?;
        }
        Ok(())
    }

    // expects the following format:
    // $<len>\r\n<content>
    pub fn read_binary(&mut self) -> Result<RESP> {
        let buf = &mut String::new();
        match self.buf_reader.read_line(buf) {
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
                        Some('$') => {
                            let len: usize = line[1..].parse().unwrap();

                            let mut buf: Vec<u8> = vec![0; len];
                            if self.buf_reader.read_exact(&mut buf).is_ok() {
                                buf.truncate(len);
                                Ok(RESP::File(buf))
                            } else {
                                bail!("invalid file command {}", line);
                            }
                        }
                        _ => {
                            bail!("invalid file command {}", line);
                        }
                    }
                }
            }
            _ => {
                bail!("read error");
            }
        }
    }
}

impl Iterator for RESPConnection {
    type Item = RESP;

    fn next(&mut self) -> Option<Self::Item> {
        read_resp(&mut self.buf_reader).unwrap_or_else(|err| {
            println!("connection error: {:?}", err);
            None
        })
    }
}


fn write_resp(writer: &mut BufWriter<TcpStream>, response: &RESP) -> Result<()> {
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
            println!("write array of {} items", array.len());
            write!(writer, "*{}\r\n", array.len())?;
            if array.len() > 0 {
                for item in array {
                    write_resp(writer, item)?;
                }
            }
        }
        RESP::File(array) => {
            println!("write {} binary: {:?}", array.len(), array);
            write!(writer, "${}\r\n", array.len())?;
            writer.flush()?;
            writer.write_all(array)?;
            writer.flush()?;
        }
    }
    Ok(())
}

fn read_resp(reader: &mut BufReader<TcpStream>) -> Result<Option<RESP>> {
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
                                assert_eq!(buf[buf.len() - 2], b'\r');
                                assert_eq!(buf[buf.len() - 1], b'\n');
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
