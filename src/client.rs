use std::net::TcpStream;
use crate::net::Binding;
use anyhow::Result;
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
        Ok(())
    }

}