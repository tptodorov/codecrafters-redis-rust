use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};

use crate::command::Command;
use crate::net::Binding;
use crate::rdb::StoredValue;
use crate::resp::RESP;

#[derive(Default)]
pub struct LogStore {
    pub(crate) log: Vec<RESP>,
    pub(crate) log_bytes: usize,
}

#[derive(Clone)]
pub struct RedisServer {
    pub(crate) binding: Binding,
    store: Arc<RwLock<HashMap<String, StoredValue>>>,
    pub(crate) log_store: Arc<RwLock<LogStore>>,
    pub(crate) master_replid: String,
    is_master: bool,
}

impl RedisServer {
    pub fn new(binding: Binding, is_master: bool) -> Result<Self> {
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990deep".to_string();
        let server = RedisServer {
            binding,
            store: Arc::new(RwLock::new(HashMap::new())),
            master_replid,
            is_master,
            log_store: Arc::new(RwLock::new(LogStore::default())),

        };


        Ok(server)
    }

    pub(crate) fn handle_command(&self, cmd: &Command, params: &[RESP]) -> Result<Vec<RESP>> {
        match cmd {
            Command::PING => Ok(vec![RESP::String("PONG".to_string())]),
            Command::ECHO => {
                let param1 = params.get(0).unwrap_or(&RESP::Null);
                match param1 {
                    RESP::Bulk(param1) => Ok(vec![RESP::Bulk(param1.to_owned())]),
                    _ => Err(anyhow!("invalid echo  command {:?}", params)),
                }
            }
            Command::SET => {
                // minimal implementation of https://redis.io/docs/latest/commands/set/
                match params {
                    // SET key value
                    [RESP::Bulk(key), RESP::Bulk(value), set_options @ ..] => {
                        let px_expiration_ms = extract_px_expiration(set_options)?;
                        let valid_until = px_expiration_ms
                            .iter()
                            .flat_map(|&expiration_ms| Instant::now().checked_add(Duration::from_millis(expiration_ms)))
                            .next();
                        self.store.write().unwrap().insert(key.clone(), StoredValue::new(value.clone(), valid_until));
                        Ok(vec![RESP::String("OK".to_string())])
                    }
                    _ => Err(anyhow!("invalid set command {:?}", params)),
                }
            }
            Command::GET => {
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
            Command::INFO => {
                // minimal implementation of https://redis.io/docs/latest/commands/info/
                // INFO replication
                match params {
                    [RESP::Bulk(sub_command)] => {
                        match sub_command.to_ascii_uppercase().as_str() {
                            "REPLICATION" => {
                                let role = if !self.is_master { "slave" } else { "master" };
                                let pairs = [
                                    ("role", role),
                                    ("master_replid", &self.master_replid),
                                    ("master_repl_offset", &format!("{}", self.log_store.read().unwrap().log_bytes))
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

            _ => Err(anyhow!("Unknown command {:?}", cmd)),
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
