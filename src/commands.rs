use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};

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
}

impl RedisServer {
    pub fn new() -> Self {
        RedisServer {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }


    pub fn command_handler(&self, cmd: &str, params: &[RESP]) -> anyhow::Result<RESP> {
        match cmd {
            "PING" => Ok(RESP::String("PONG".to_string())),
            "ECHO" => {
                let param1 = params.get(0).unwrap_or(&RESP::Null);
                match param1 {
                    RESP::Bulk(param1) => Ok(RESP::Bulk(param1.to_owned())),
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
                            .flat_map(|&expiration_ms| std::time::Instant::now().checked_add(Duration::from_millis(expiration_ms)))
                            .next();
                        self.store.write().unwrap().insert(key.clone(), StoredValue { value: value.clone(), valid_until });
                        Ok(RESP::String("OK".to_string()))
                    }
                    _ => Err(anyhow!("invalid set command {:?}", params)),
                }
            }
            "GET" => {
                // minimal implementation of https://redis.io/docs/latest/commands/get/
                // GET key
                match params {
                    [RESP::Bulk(key)] => {
                        Ok(
                            // extract valid value from store
                            self.store.read().unwrap().get(key)
                                .iter().flat_map(|&value| value.value())
                                // wrap it in bulk
                                .map(|v| RESP::Bulk(v))
                                .next()
                                // Null if not found
                                .unwrap_or(RESP::Null)
                        )
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
                                Ok(RESP::Bulk("role:master".to_string()))
                            }
                            _ => Err(anyhow!("unknown info command {:?}", sub_command)),
                        }
                    }
                    _ => Err(anyhow!("invalid get command {:?}", params)),
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
