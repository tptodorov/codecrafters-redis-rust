use std::net::TcpStream;
use crate::net::Binding;
use anyhow::{bail, Result};
use crate::resp::{RESP, RESPReader};

pub struct RedisClient {
    binding: Binding,
    stream: RESPReader,
}

impl RedisClient {
    pub fn new(binding: &Binding) -> Result<Self> {
        let stream = TcpStream::connect(binding.to_string())?;
        Ok(RedisClient {
            stream: RESPReader::new(stream),
            binding: binding.clone(),
        })
    }

    pub fn ping(&mut self) -> Result<()> {
        self.stream.response(&RESP::Array(vec![RESP::Bulk("PING".to_string())]))?;
        if let Some(RESP::String(str)) = self.stream.next() {
            if str == "PONG" {
                return Ok(());
            }
        }
        bail!("ping failed");
    }

    pub fn replconfig(&mut self, params: &[&str]) -> Result<()> {
        let mut command = vec![RESP::Bulk("REPLCONF".to_string())];

        let mut bulk_params = params.into_iter().map(|param| RESP::Bulk(param.to_string())).collect::<Vec<RESP>>();
        command.append(&mut bulk_params);

        self.stream.response(&RESP::Array(command))?;
        if let Some(RESP::String(str)) = self.stream.next() {
            if str == "OK" {
                return Ok(());
            }
        }
        bail!("replconfig failed");
    }
}