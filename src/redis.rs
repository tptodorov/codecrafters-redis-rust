use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

use anyhow::{bail, Result};

use crate::args;
use crate::args::named_option;
use crate::io::net::Binding;
use crate::protocol::command::{Command, CommandRequest};
use crate::protocol::resp::RESP;
use crate::store::{KVStore, StreamEntryId, StreamEvent};

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
            store: Arc::new(RwLock::new(KVStore::new())),
            master_replid,
            is_master,
            log_store: Arc::new(RwLock::new(LogStore::default())),
            db_dir: dir.clone(),
            db_filename: dbfilename.clone(),
        };

        server.load_rds()?;

        Ok(server)
    }

    pub(crate) fn handle_command(&self, cmd: &CommandRequest) -> Result<Vec<RESP>> {
        match cmd.as_ref() {
            (Command::PING, []) => Ok(vec![RESP::String("PONG".to_string())]),
            (Command::ECHO, [param1]) => {
                Ok(vec![RESP::bulk(param1)])
            }
            (Command::SET, [key, value, options @ ..]) => {
                // minimal implementation of https://redis.io/docs/latest/commands/set/
                let px_expiration = named_option::<u64>(options, "PX")?.map(Duration::from_millis);
                self.store.write().unwrap().insert_value(&key, &value, px_expiration);
                Ok(vec![RESP::String("OK".to_string())])
            }
            (Command::GET, [key]) => {
                // minimal implementation of https://redis.io/docs/latest/commands/get/
                // GET key
                Ok(vec![
                    // extract valid value from store
                    self.store.read().unwrap().get_value(key)
                        // wrap it in bulk
                        // Null if not found
                        .map_or(RESP::Null, RESP::Bulk)
                ])
            }
            (Command::TYPE, [key]) => {
                // minimal implementation of https://redis.io/docs/latest/commands/type/
                Ok(vec![
                    RESP::String(
                        self.store.read().unwrap().get_type(key).to_string()
                    )
                ])
            }
            (Command::XADD, [key, id, key_value_pairs @ ..]) => {
                // minimal implementation of https://redis.io/docs/latest/commands/xadd/
                // XADD key id field value [field value ...]

                let mut stream_data = vec![];
                let mut iter = key_value_pairs.iter();
                while let Some((key, value)) = iter.next().zip(iter.next()) {
                    stream_data.push((key.to_string(), value.to_string()));
                }
                Ok(vec![
                    self.store.write().unwrap()
                        .insert_stream(key, id, stream_data)
                        .map_or_else(|err| RESP::Error(err.to_string()), |new_id| RESP::bulk(&new_id))
                ])
            }
            (Command::XRANGE, [key, from_id, to_id]) => {
                // minimal implementation of https://redis.io/commands/xrange/
                // XRANGE key id-from id-to

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
                Ok(vec![
                    self.store.read().unwrap()
                        .range_stream(key, from_id, to_id)
                        .map_or_else(|err| RESP::Error(err.to_string()),
                                     |results| {
                                         let results = results.iter().map(encode_stream_entries).collect();
                                         RESP::Array(results)
                                     })
                ])
            }
            (Command::XREAD, params) => {
                let block_ms: Option<u64> = named_option::<u64>(params, "BLOCK")?;
                let streams = args::named_option_list(params, "streams");
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

            (Command::KEYS, _) => {
                // minimal implementation of https://redis.io/docs/latest/commands/keys/
                Ok(vec![
                    RESP::Array(
                        self.store.read().unwrap().keys()
                            // wrap it in bulk
                            .iter()
                            .map(|v| RESP::bulk(v))
                            .collect()
                    )
                ])
            }

            (Command::INFO, [sub_command]) => {
                // minimal implementation of https://redis.io/docs/latest/commands/info/
                // INFO replication

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
                        Ok(vec![RESP::bulk(&info)])
                    }
                    // TODO implement other sub commands
                    _ => bail!("unknown info command {:?}", sub_command),
                }
            }
            (Command::CONFIG, [sub_command, key]) => {
                // minimal implementation of https://redis.io/docs/latest/commands/info/
                // INFO replication

                match (sub_command.to_uppercase().as_str(), key.to_lowercase().as_str()) {
                    ("GET", "dir") => {
                        Ok(vec![RESP::Array(vec![RESP::bulk(key), RESP::bulk(&self.db_dir)])])
                    }
                    ("GET", "dbfilename") => {
                        Ok(vec![RESP::Array(vec![RESP::bulk(key), RESP::bulk(&self.db_filename)])])
                    }
                    _ => bail!("unknown config command {:?}", sub_command),
                }
            }

            _ => bail!("Unknown or invalid command {:?}", cmd),
        }
    }

    /// read all stream values for the keys and minimal ids
    fn xread_values(&self, keys: &[String], key_id_pairs: &HashMap<String, StreamEntryId>) -> Result<RESP> {
        let mut all_results = vec![];
        let store = self.store.read().unwrap();

        // results should be in the same order as the in the command
        for key in keys {
            let from_id = key_id_pairs.get(key).unwrap();

            let key_results = store
                .read_stream(key, from_id.clone(), StreamEntryId::MAX)?;

            let results: Vec<RESP> = key_results.iter().map(encode_stream_entries).collect();

            if results.is_empty() {
                continue;
            }

            all_results.push(
                RESP::Array(vec![
                    RESP::bulk(key),
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

        // listeners will be removed passively
        self.store.write().unwrap().add_listener(&keys, Arc::downgrade(&this_listener))?;

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

    fn load_rds(&self) -> Result<()> {
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

fn encode_stream_entries(entries: &(String, &Vec<(String, String)>)) -> RESP {
    let mut array = vec![];
    for (k, v) in entries.1 {
        array.push(RESP::bulk(k));
        array.push(RESP::bulk(v));
    }
    RESP::Array(vec![
        RESP::bulk(&entries.0),
        RESP::Array(array),
    ])
}
