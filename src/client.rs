use std::net::TcpStream;
use crate::net::Binding;
use anyhow::{bail, Result};
use crate::resp::{RESP, RESPConnection};

pub struct ReplicaClient {
    _binding: Binding,
    pub(crate) stream: RESPConnection,
}

impl ReplicaClient {
    pub fn new(binding: &Binding) -> Result<Self> {
        let stream = TcpStream::connect(binding.to_string())?;
        println!("connected to: {}", binding);
        Ok(ReplicaClient {
            stream: RESPConnection::new(stream),
            _binding: binding.clone(),
        })
    }

    pub fn ping_pong(&mut self) -> Result<()> {
        self.stream.send_message(&RESP::Array(vec![RESP::Bulk("PING".to_string())]))?;
        if let (_, Some(RESP::String(str))) = self.stream.read_message()? {
            if str.to_uppercase() == "PONG" {
                return Ok(());
            }
        }
        bail!("ping failed");
    }

    pub fn replconf(&mut self, params: &[&str]) -> Result<()> {
        let mut command = vec![RESP::Bulk("REPLCONF".to_string())];

        let mut bulk_params = params.into_iter().map(|param| RESP::Bulk(param.to_string())).collect::<Vec<RESP>>();
        command.append(&mut bulk_params);

        self.stream.send_message(&RESP::Array(command))?;
        if let (_, Some(RESP::String(str))) = self.stream.read_message()? {
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

        self.stream.send_message(&RESP::Array(command))?;

        let (_, psync_response) = self.stream.read_message()?;
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
        bail!("psync failed: {}",psync_response.unwrap());
    }

    pub fn read_replication_command(&mut self) -> Result<(usize, RESP)> {
        let (len, psync_response) = self.stream.read_message()?;
        match psync_response {
            Some(array @ RESP::Array(_)) => Ok((len, array)),
            _ => bail!("replication message must be an array: {}", psync_response.unwrap())
        }
    }

}