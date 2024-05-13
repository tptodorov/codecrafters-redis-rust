use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Result};

use crate::command::Command;
use crate::net::Binding;
use crate::rdb::{KVStore, StoredValue};
use crate::resp::RESP;

#[derive(Default)]
pub struct LogStore {
    pub(crate) log: Vec<RESP>,
    pub(crate) log_bytes: usize,
}


#[derive(Clone)]
pub struct RedisServer {
    pub(crate) binding: Binding,
    store: Arc<RwLock<KVStore>>,
    pub(crate) log_store: Arc<RwLock<LogStore>>,
    pub(crate) master_replid: String,
    is_master: bool,
    pub dir: String,
    pub dbfilename: String,
}

impl RedisServer {
    pub fn new(binding: Binding, is_master: bool, dir: String, dbfilename: String) -> Result<Self> {
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990deep".to_string();

        let path_dir = Path::new(&dir);
        if !path_dir.exists() {
            bail!("dir {} must exist", dir);
        }

        let server = RedisServer {
            binding,
            store: Arc::new(RwLock::new(KVStore(HashMap::new()))),
            master_replid,
            is_master,
            log_store: Arc::new(RwLock::new(LogStore::default())),
            dir: dir.clone(),
            dbfilename: dbfilename.clone(),
        };

        server.try_load_db()?;

        Ok(server)
    }

    pub(crate) fn handle_command(&self, cmd: &Command, params: &[RESP]) -> Result<Vec<RESP>> {
        match cmd {
            Command::PING => Ok(vec![RESP::String("PONG".to_string())]),
            Command::ECHO => {
                let param1 = params.first().unwrap_or(&RESP::Null);
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
                            .flat_map(|&expiration_ms| SystemTime::now().checked_add(Duration::from_millis(expiration_ms)))
                            .next();
                        self.store.write().unwrap().0.insert(key.clone(), StoredValue::new(value.clone(), valid_until));
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
                            self.store.read().unwrap().0.get(key)
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
            Command::KEYS => {
                // minimal implementation of https://redis.io/docs/latest/commands/keys/
                match params {
                    [RESP::Bulk(_pattern)] => {
                        Ok(vec![
                            RESP::Array(
                                self.store.read().unwrap().0.keys()
                                    // TODO filter keys by pattern
                                    // wrap it in bulk
                                    .map(|v| RESP::Bulk(v.to_string()))
                                    .collect()
                            )
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
            Command::CONFIG => {
                // minimal implementation of https://redis.io/docs/latest/commands/info/
                // INFO replication
                match params {
                    [RESP::Bulk(sub_command), RESP::Bulk(key) ] => {
                        match (sub_command.to_uppercase().as_str(), key.to_lowercase().as_str()) {
                            ("GET", "dir") => {
                                Ok(vec![RESP::Array(vec![RESP::Bulk(key.clone()), RESP::Bulk(self.dir.clone())])])
                            }
                            ("GET", "dbfilename") => {
                                Ok(vec![RESP::Array(vec![RESP::Bulk(key.clone()), RESP::Bulk(self.dbfilename.clone())])])
                            }
                            _ => Err(anyhow!("unknown config command {:?}", sub_command)),
                        }
                    }
                    _ => Err(anyhow!("invalid config command {:?}", params)),
                }
            }

            _ => Err(anyhow!("Unknown command {:?}", cmd)),
        }
    }
    fn try_load_db(&self) -> Result<()> {
        let db_file = Path::new(&self.dir).join(&self.dbfilename);
        if db_file.exists() {
            let file = File::open(&db_file)?;
            self.store.write().unwrap().load(BufReader::new(file))?;
            println!("loaded RDB file: {:?}", db_file);
        } else {
            println!("no db file found to load: {:?}", db_file);
        }
        Ok(())
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
