use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Result};

use crate::command::Command;
use crate::net::Binding;
use crate::rdb::{KVStore, StoredValue, StreamEntryId, StreamEvent};
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
    pub is_master: bool,
    pub db_dir: String,
    pub db_filename: String,
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
            db_dir: dir.clone(),
            db_filename: dbfilename.clone(),
        };

        server.try_load_db()?;

        Ok(server)
    }

    pub(crate) fn handle_command(&self, cmd: &Command, params: &[String]) -> Result<Vec<RESP>> {
        match cmd {
            Command::PING => Ok(vec![RESP::String("PONG".to_string())]),
            Command::ECHO => {
                match params.first() {
                    Some(param1) => Ok(vec![RESP::Bulk(param1.to_owned())]),
                    None => Err(anyhow!("invalid echo  command {:?}", params)),
                }
            }
            Command::SET => {
                // minimal implementation of https://redis.io/docs/latest/commands/set/
                match &params[..] {
                    // SET key value
                    [key, value, set_options @ ..] => {
                        let px_expiration_ms = extract_option_u64(params, "PX")?;
                        let valid_until = px_expiration_ms
                            .iter()
                            .flat_map(|&expiration_ms| SystemTime::now().checked_add(Duration::from_millis(expiration_ms)))
                            .next();
                        self.store.write().unwrap().0.insert(key.clone(), StoredValue::from_string(key, value, valid_until));
                        Ok(vec![RESP::String("OK".to_string())])
                    }
                    _ => Err(anyhow!("invalid set command {:?}", params)),
                }
            }
            Command::GET => {
                // minimal implementation of https://redis.io/docs/latest/commands/get/
                // GET key
                match params {
                    [key] => {
                        Ok(vec![
                            // extract valid value from store
                            self.store.read().unwrap().0.get(key)
                                .iter().flat_map(|&value| value.value())
                                // wrap it in bulk
                                .map(RESP::Bulk)
                                .next()
                                // Null if not found
                                .unwrap_or(RESP::Null)
                        ])
                    }
                    _ => Err(anyhow!("invalid get command {:?}", params)),
                }
            }
            Command::TYPE => {
                // minimal implementation of https://redis.io/docs/latest/commands/type/
                match params {
                    [key] => {
                        Ok(vec![
                            RESP::String(
                                self.store.read().unwrap().0.get(key)
                                    .iter().map(|&value| value.value_type())
                                    .next()
                                    .unwrap_or("none")
                                    .to_string()
                            )
                        ])
                    }
                    _ => Err(anyhow!("invalid get command {:?}", params)),
                }
            }
            Command::XADD => {
                // minimal implementation of https://redis.io/docs/latest/commands/xadd/
                // XADD key id field value [field value ...]
                match params {
                    // SET key value
                    [key, id, key_value_pairs @ ..] => {
                        let mut stream_data = vec![];
                        let mut iter = key_value_pairs.iter();
                        while let Some((key, value)) = iter.next().zip(iter.next()) {
                            stream_data.push((key.to_string(), value.to_string()));
                        }
                        self.store.write().unwrap()
                            .insert_stream(key, id, stream_data)
                            .map_or_else(|err| Ok(vec![RESP::Error(err.to_string())]), |new_id| Ok(vec![RESP::Bulk(new_id)]))
                    }
                    _ => Err(anyhow!("invalid set command {:?}", params)),
                }
            }
            Command::XRANGE => {
                // minimal implementation of https://redis.io/commands/xrange/
                // XRANGE key id-from id-to
                match params {
                    // SET key value
                    [key, from_id, to_id] => {
                        let from_id =
                            if from_id == "-" {
                                StreamEntryId::MIN
                            } else {
                                from_id.parse::<StreamEntryId>().or(from_id.parse::<u64>().map(|v| StreamEntryId::new(v, 0)))?
                            };
                        let to_id =
                            if to_id == "+" {
                                StreamEntryId::MAX
                            } else {
                                to_id.parse::<StreamEntryId>().or(to_id.parse::<u64>().map(|v| StreamEntryId::new(v, u64::MAX)))?
                            };
                        self.store.read().unwrap()
                            .range_stream(key, from_id, to_id).map_or_else(|err| Ok(vec![RESP::Error(err.to_string())]),
                                                                           |results| {
                                                                               let results = results.iter().map(|(id, entries)| {
                                                                                   RESP::Array(vec![
                                                                                       RESP::Bulk(id.clone()),
                                                                                       encode_stream_entries(entries),
                                                                                   ])
                                                                               }).collect();
                                                                               Ok(vec![RESP::Array(results)])
                                                                           })
                    }
                    _ => Err(anyhow!("invalid xrange command {:?}", params)),
                }
            }
            Command::XREAD => {
                let block_ms: Option<u64> = extract_option_u64(params, "BLOCK")?;
                let streams = extract_option_list(params, "streams");
                // minimal implementation of https://redis.io/commands/xread/
                match streams {
                    Some(sub_params) => {
                        // XREAD stream key1 key2 id1 id2
                        let (keys, ids) = sub_params.split_at(sub_params.len() / 2);
                        let key_id_pairs: HashMap<String, StreamEntryId> = {
                            let mut pairs: HashMap<String, StreamEntryId> = HashMap::new();
                            let store = self.store.read().unwrap();
                            for (key, id) in keys.iter().zip(ids.iter()) {
                                let key = key.to_string();
                                let from_id = id.to_string();
                                let from_id = if from_id == "$" {
                                    store.latest_stream(&key)?
                                } else {
                                    from_id.parse::<StreamEntryId>()?
                                };
                                pairs.insert(key, from_id);
                            }
                            pairs
                        };

                        // behaves quite differently depending on the blocking option
                        match block_ms {
                            Some(block_ms) => {
                                // fetch any existing or new data that arrives
                                let existing_values = self.xread_values(keys, &key_id_pairs)?;
                                if existing_values != RESP::Null {
                                    return Ok(vec![existing_values]);
                                }
                                // block until some data arrives
                                if self.block_xread(block_ms, &key_id_pairs)? {
                                    Ok(vec![RESP::Null])
                                } else {
                                    Ok(vec![self.xread_values(keys, &key_id_pairs)?])
                                }
                            }
                            None => {
                                // only fetch existing data
                                Ok(vec![self.xread_values(keys, &key_id_pairs)?])
                            }
                        }
                    }
                    _ => bail!("invalid XREAD command"),
                }
            }

            Command::KEYS => {
                // minimal implementation of https://redis.io/docs/latest/commands/keys/
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

            Command::INFO => {
                // minimal implementation of https://redis.io/docs/latest/commands/info/
                // INFO replication
                match params {
                    [sub_command] => {
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
                    [sub_command, key] => {
                        match (sub_command.to_uppercase().as_str(), key.to_lowercase().as_str()) {
                            ("GET", "dir") => {
                                Ok(vec![RESP::Array(vec![RESP::Bulk(key.clone()), RESP::Bulk(self.db_dir.clone())])])
                            }
                            ("GET", "dbfilename") => {
                                Ok(vec![RESP::Array(vec![RESP::Bulk(key.clone()), RESP::Bulk(self.db_filename.clone())])])
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

    fn xread_values(&self, keys: &[String], key_id_pairs: &HashMap<String, StreamEntryId>) -> Result<RESP> {
        let mut all_results = vec![];
        let guard = self.store.read().unwrap();

        // results should be in the same order as the in the command
        for key in keys {
            let key = key.to_string();
            let from_id = key_id_pairs.get(&key).unwrap();

            let key_results = guard
                .read_stream(&key, from_id.clone(), StreamEntryId::MAX)?;

            let results: Vec<RESP> = key_results.iter().map(|(id, entries)| {
                RESP::Array(vec![
                    RESP::Bulk(id.clone()),
                    encode_stream_entries(entries),
                ])
            }).collect();

            if results.is_empty() {
                continue;
            }

            all_results.push(
                RESP::Array(vec![
                    RESP::Bulk(key.to_string()),
                    RESP::Array(results),
                ])
            )
        }

        Ok(
            if all_results.is_empty() { RESP::Null } else { RESP::Array(all_results) }
        )
    }

    /**
    blocks for until either timeout or new records were added.
    returns true if it timed out.
     */
    fn block_xread(&self, block_ms: u64, key_id_pairs: &HashMap<String, StreamEntryId>) -> Result<bool> {
// wait for any of the keys to be added
        let timeout = if block_ms == 0 { Duration::MAX } else { Duration::from_millis(block_ms) };
        println!("will block for {:?}", timeout);

        let keys = key_id_pairs.keys().collect::<Vec<&String>>();

        let is_acceptable = |stream_event: StreamEvent| {
            key_id_pairs.get(&stream_event.0).map_or(false, |id| stream_event.1 > *id)
        };

        let this_listener = Arc::new((Mutex::new(None), Condvar::new()));

        let _key_results = self.store.write().unwrap().add_listener(&keys, this_listener.clone());

        let (lock, cvar) = this_listener.deref();
        let mut event_guard = lock.lock().unwrap();
        while !event_guard.clone().map_or(false, is_acceptable) {
            let result = cvar.wait_timeout_while(
                event_guard, timeout,
                |event| !event.clone().map_or(false, is_acceptable),
            ).unwrap();
            event_guard = result.0;
            if result.1.timed_out() {
                println!("timeout of the blocked xread");
                // timed-out, meaning no new values are added
                return Ok(true);
            }
        }
        // continue by running the normal non-blocking op
        Ok(false)
    }

    fn try_load_db(&self) -> Result<()> {
        let db_file = Path::new(&self.db_dir).join(&self.db_filename);
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

fn encode_stream_entries(entries: &Vec<(String, String)>) -> RESP {
    let mut array = vec![];
    for (k, v) in entries {
        array.push(RESP::Bulk(k.clone()));
        array.push(RESP::Bulk(v.clone()));
    }
    RESP::Array(array)
}


fn extract_option_u64(params: &[String], option_name: &str) -> Result<Option<u64>> {
    let value = extract_option_string(params, option_name);
    let value = value.map(|v| v.parse::<u64>()).transpose()?;
    Ok(value)
}

fn extract_option_string(params: &[String], option_name: &str) -> Option<String> {
    let option_name = option_name.to_uppercase();
    params.iter()
        .position(|e| e.to_string().to_uppercase() == option_name)
        .map(|i| params[i + 1].clone())
}

fn extract_option_list<'a>(params: &'a [String], option_name: &str) -> Option<&'a [String]> {
    let option_name = option_name.to_uppercase();
    params.iter()
        .position(|e| e.to_string().to_uppercase() == option_name)
        .map(|i| &params[i + 1..])
}
