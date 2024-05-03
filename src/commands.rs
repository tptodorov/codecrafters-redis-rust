use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::anyhow;

use crate::resp::RESP;

#[derive(Clone)]
pub struct RedisServer {
    store: Arc<RwLock<HashMap<String, String>>>,
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
                // SET key value
                match params {
                    [RESP::Bulk(key), RESP::Bulk(value)] => {
                        self.store.write().unwrap().insert(key.clone(), value.clone());
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
                        match  self.store.read().unwrap().get(key)  {
                            Some(value) => Ok(RESP::Bulk(value.clone())),
                            None => Ok(RESP::Null),
                        }
                    }
                    _ => Err(anyhow!("invalid set command {:?}", params)),
                }
            }
            _ => Err(anyhow!("Unknown command {}", cmd)),
        }
    }
}
