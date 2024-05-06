use std::net::TcpStream;
use crate::net::Binding;
use anyhow::{bail, Result};
use crate::resp::{RESP, RESPConnection};

pub struct RedisClient {
    _binding: Binding,
    stream: RESPConnection,
}

impl RedisClient {
    pub fn new(binding: &Binding) -> Result<Self> {
        let stream = TcpStream::connect(binding.to_string())?;
        println!("connected to: {:?}", binding);
        Ok(RedisClient {
            stream: RESPConnection::new(stream),
            _binding: binding.clone(),
        })
    }

    pub fn ping(&mut self) -> Result<()> {
        self.stream.send_response(&RESP::Array(vec![RESP::Bulk("PING".to_string())]))?;
        if let Some(RESP::String(str)) = self.stream.next() {
            if str.to_uppercase() == "PONG" {
                return Ok(());
            }
        }
        bail!("ping failed");
    }

    pub fn replconfig(&mut self, params: &[&str]) -> Result<()> {
        let mut command = vec![RESP::Bulk("REPLCONF".to_string())];

        let mut bulk_params = params.into_iter().map(|param| RESP::Bulk(param.to_string())).collect::<Vec<RESP>>();
        command.append(&mut bulk_params);

        self.stream.send_response(&RESP::Array(command))?;
        if let Some(RESP::String(str)) = self.stream.next() {
            if str.to_uppercase() == "OK" {
                return Ok(());
            }
        }
        bail!("replconfig failed");
    }
    pub fn psync(&mut self, replication_id: &str, offset: i64) -> Result<Vec<u8>> {
        let command = vec![RESP::Bulk("PSYNC".to_string()),
                           RESP::Bulk(replication_id.to_string()),
                           RESP::Bulk(format!("{}", offset)),
        ];

        self.stream.send_response(&RESP::Array(command))?;

        let psync_response = self.stream.next();
        if let Some(RESP::String(str)) = psync_response {
            if str.to_uppercase().starts_with("FULLRESYNC ") {
                println!("waiting for rds data");
                // expect master to send the RDB in a Bulk like binary
                if let RESP::File(rds) = self.stream.read_binary()? {
                    println!("read binary {} rds: {:?}", rds.len(), rds);
                    return Ok(rds);
                }
            }
            bail!("psync unknown response: {}", str);
        }
        bail!("psync failed: {:?}",psync_response);
    }

    pub fn read_replication_command(&mut self) -> Result<RESP> {
        let psync_response = self.stream.next();
        match psync_response {
            Some(array @ RESP::Array(_)) => Ok(array),
            _ => bail!("replication message must be an array: {:?}", psync_response)
        }
    }

    pub fn respond_replconf_ack(&mut self, offset: i64) -> Result<()> {
        let response = vec![RESP::Bulk("REPLCONF".to_string()),
                            RESP::Bulk("ACK".to_string()),
                            RESP::Bulk(format!("{}", offset)),
        ];
        self.stream.send_response(&RESP::Array(response))?;
        Ok(())
    }
}