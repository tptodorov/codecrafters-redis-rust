use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use crate::client::RedisClient;
use crate::net::{Binding, Port};
use crate::rdb::empty_rdb;

use crate::resp::RESP;

struct StoredValue {
    value: String,
    valid_until: Option<Instant>,
}

impl StoredValue {
    fn value(&self) -> Option<String> {
        if let Some(valid_until) = self.valid_until {
            if valid_until < Instant::now() {
                return None;
            }
        }
        Some(self.value.clone())
    }
}

#[derive(Clone)]
pub struct RedisServer {
    store: Arc<RwLock<HashMap<String, StoredValue>>>,
    binding: Binding,
    replica_of: Option<Binding>,
    master_repl_offset: usize,
    master_replid: String,
}

impl RedisServer {
    pub fn new(binding: Binding, replica_of: Option<Binding>) -> Result<Self> {
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990deep".to_string();

        if let Some(master) = &replica_of {
            replication_protocol(binding.1, master)?;
        }
        Ok(
            RedisServer {
                binding,
                store: Arc::new(RwLock::new(HashMap::new())),
                master_repl_offset: 0,
                master_replid,
                replica_of,
            }
        )
    }

    pub fn handler(&self, message: &RESP) -> Result<Vec<RESP>> {
        println!("message: {:?}", message);
        match message {
            RESP::Array(array) =>
                match &array[..] {
                    [RESP::Bulk(command), params @ .. ] => self.command_handler(command.to_uppercase().as_str(), params),
                    _ => bail!("Invalid message".to_string()),
                }
            ,
            _ => bail!("Invalid message".to_string()),
        }
    }


    pub fn command_handler(&self, cmd: &str, params: &[RESP]) -> Result<Vec<RESP>> {
        match cmd {
            "PING" => Ok(vec![RESP::String("PONG".to_string())]),
            "ECHO" => {
                let param1 = params.get(0).unwrap_or(&RESP::Null);
                match param1 {
                    RESP::Bulk(param1) => Ok(vec![RESP::Bulk(param1.to_owned())]),
                    _ => Err(anyhow!("invalid echo  command {:?}", params)),
                }
            }
            "SET" => {
                // minimal implementation of https://redis.io/docs/latest/commands/set/
                match params {
                    // SET key value
                    [RESP::Bulk(key), RESP::Bulk(value), set_options @ ..] => {
                        let px_expiration_ms = extract_px_expiration(set_options)?;
                        let valid_until = px_expiration_ms
                            .iter()
                            .flat_map(|&expiration_ms| Instant::now().checked_add(Duration::from_millis(expiration_ms)))
                            .next();
                        self.store.write().unwrap().insert(key.clone(), StoredValue { value: value.clone(), valid_until });
                        Ok(vec![RESP::String("OK".to_string())])
                    }
                    _ => Err(anyhow!("invalid set command {:?}", params)),
                }
            }
            "GET" => {
                // minimal implementation of https://redis.io/docs/latest/commands/get/
                // GET key
                match params {
                    [RESP::Bulk(key)] => {
                        Ok(vec![
                            // extract valid value from store
                            self.store.read().unwrap().get(key)
                                .iter().flat_map(|&value| value.value())
                                // wrap it in bulk
                                .map(|v| RESP::Bulk(v))
                                .next()
                                // Null if not found
                                .unwrap_or(RESP::Null)
                        ])
                    }
                    _ => Err(anyhow!("invalid get command {:?}", params)),
                }
            }
            "INFO" => {
                // minimal implementation of https://redis.io/docs/latest/commands/info/
                // INFO replication
                match params {
                    [RESP::Bulk(sub_command)] => {
                        match sub_command.to_ascii_uppercase().as_str() {
                            "REPLICATION" => {
                                let role = if self.replica_of.is_some() { "slave" } else { "master" };
                                let pairs = [
                                    ("role", role),
                                    ("master_replid", &self.master_replid),
                                    ("master_repl_offset", &format!("{}", self.master_repl_offset))
                                ];
                                let info = pairs
                                    .map(|(k, v)| format!("{}:{}", k, v))
                                    .join("\r\n");
                                Ok(vec![RESP::Bulk(info)])
                            }
                            _ => Err(anyhow!("unknown info command {:?}", sub_command)),
                        }
                    }
                    _ => Err(anyhow!("invalid get command {:?}", params)),
                }
            }
            "REPLCONF" => {
                // minimal implementation of https://redis.io/docs/latest/commands/replconf/
                // REPLCONF ...
                Ok(vec![RESP::String("OK".to_string())])
            }
            "PSYNC" => {
                // minimal implementation of https://redis.io/docs/latest/commands/psync/
                // PSYNC replication-id offset
                match params {
                    [RESP::Bulk(repl_id), RESP::Bulk(offset)] => {
                        // replica does not know where to start
                        if repl_id == "?" && offset == "-1" {
                            Ok(vec![RESP::Bulk(format!("FULLRESYNC {} 0", self.master_replid)), RESP::File(empty_rdb())])
                        } else {
                            Err(anyhow!("invalid psync command {:?}", params))
                        }
                    }
                    _ => Err(anyhow!("invalid psync command {:?}", params)),
                }
            }

            _ => Err(anyhow!("Unknown command {}", cmd)),
        }
    }
}

fn extract_px_expiration(params: &[RESP]) -> Result<Option<u64>> {
    let mut set_options = params.iter();
    loop {
        match set_options.next() {
            None => break,
            Some(RESP::Bulk(option)) => {
                if option.to_uppercase() == "PX" {
                    if let Some(RESP::Bulk(ms)) = set_options.next() {
                        let ms = ms.to_string().parse::<u64>()?;
                        return Ok(Some(ms));
                    }
                    bail!("invalid PX option");
                }
            }
            _ => continue,
        }
    }
    Ok(None)
}


fn replication_protocol(this_port: Port, master: &Binding) -> Result<()> {
    let mut master_client = RedisClient::new(master)?;
    master_client.ping()?;
    master_client.replconfig(&vec!["listening-port", &format!("{}", this_port)])?;
    master_client.replconfig(&vec!["capa", "psync2"])?;
    master_client.psync("?", -1)?;
    println!("replication initialised with master: {}", master);
    Ok(())
}
