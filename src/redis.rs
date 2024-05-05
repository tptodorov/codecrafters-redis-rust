use std::collections::HashMap;
use std::net::TcpStream;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};

use crate::net::Binding;
use crate::rdb::{empty_rdb, StoredValue};
use crate::replication;
use crate::resp::{RESP, RESPConnection};
use std::sync::mpsc;
use std::thread;

#[derive(Debug, PartialEq)]
enum Command {
    PING,
    ECHO,
    SET,
    DEL,
    GET,
    PSYNC,
    INFO,
    REPLCONF,
}

impl Command {
    pub fn is_mutation(&self) -> bool {
        matches!(self, Command::SET | Command::DEL )
    }
}


impl FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Command, Self::Err> {
        match input.to_uppercase().as_str() {
            "PING" => Ok(Command::PING),
            "GET" => Ok(Command::GET),
            "SET" => Ok(Command::SET),
            "DEL" => Ok(Command::DEL),
            "PSYNC" => Ok(Command::PSYNC),
            "ECHO" => Ok(Command::ECHO),
            "INFO" => Ok(Command::INFO),
            "REPLCONF" => Ok(Command::REPLCONF),
            _ => bail!("unknown command: {}", input),
        }
    }
}


#[derive(Clone)]
pub struct RedisServer {
    store: Arc<RwLock<HashMap<String, StoredValue>>>,
    // binding: Binding,
    replica_of: Option<Binding>,
    master_repl_offset: usize,
    master_replid: String,
    replica_senders: Arc<RwLock<Vec<mpsc::Sender<RESP>>>>,
}

impl RedisServer {
    pub fn new(binding: Binding, replica_of: Option<Binding>) -> Result<Self> {
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990deep".to_string();

        // start replication thread
        if let Some(master) = replica_of.clone() {
            thread::spawn( move || {
                loop {
                    replication::replication_protocol(binding.1, &master).unwrap_or_else(|err| println!("replication failed: {:?}. will restart replication connection", err));
                    thread::sleep(Duration::from_secs(1));
                }
            });
        }

        Ok(
            RedisServer {
                // binding,
                store: Arc::new(RwLock::new(HashMap::new())),
                master_repl_offset: 0,
                master_replid,
                replica_of,
                replica_senders: Arc::new(RwLock::new(vec![])),
            }
        )
    }

    pub fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let mut connection = RESPConnection::new(stream);
        loop {
            if let Some(command) = connection.next() {
                println!("sending response to {:?}", command);
                match self.handle_message(command, &mut connection) {
                    Ok(_) => {
                        // loop continues to read the next message
                        continue;
                    }
                    Err(err) => {
                        println!("error while handling command: {}.", err);
                        continue;
                    }
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    fn handle_message(&self, message: RESP, connection: &mut RESPConnection) -> Result<()> {
        println!("message: {:?}", message);
        match &message {
            RESP::Array(array) =>
                match &array[..] {
                    [RESP::Bulk(command), params @ .. ] => {
                        let command: Command = command.parse()?;

                        let responses = self.handle_command(&command, params)?;

                        if command.is_mutation() && self.is_master() {
                            // replicate mutations only if you are a master
                            self.replicate(message);
                        }

                        println!("{:?} -> {:?}", command, responses);
                        if let Err(err) = connection.responses(&responses.iter().map(|r| r).collect::<Vec<&RESP>>()) {
                            bail!("error while writing response: {}. terminating connection", err);
                        }

                        if command == Command::PSYNC {
                            // this connection is turning into replication connection
                            println!("after PSYNC, here will be sending replication commands");

                            // register listener for messages
                            let (tx, rx) = mpsc::channel::<RESP>();
                            self.replica_senders.write().unwrap().push(tx);

                            // any received messages will be sent to the current replica connection
                            for received in rx {
                                println!("Replicating: {:?}", received);
                                if let Err(err) = connection.response(&received) {
                                    println!("returned error: {} while replicating command: {:?}", err, received);
                                }
                            }
                        }

                        Ok(())
                    }
                    _ => bail!("Invalid message".to_string()),
                }
            ,
            _ => bail!("Invalid message".to_string()),
        }
    }


    fn handle_command(&self, cmd: &Command, params: &[RESP]) -> Result<Vec<RESP>> {
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
            Command::REPLCONF => {
                // minimal implementation of https://redis.io/docs/latest/commands/replconf/
                // REPLCONF ...
                Ok(vec![RESP::String("OK".to_string())])
            }
            Command::PSYNC => {
                // minimal implementation of https://redis.io/docs/latest/commands/psync/
                // PSYNC replication-id offset
                assert!(self.is_master(), "only master can do psync");
                match params {
                    [RESP::Bulk(repl_id), RESP::Bulk(offset)] => {
                        // replica does not know where to start
                        if repl_id == "?" && offset == "-1" {
                            // this makes the current connection a replication connection
                            Ok(vec![RESP::String(format!("FULLRESYNC {} 0", self.master_replid)), RESP::File(empty_rdb())])
                        } else {
                            Err(anyhow!("invalid psync command {:?}", params))
                        }
                    }
                    _ => Err(anyhow!("invalid psync command {:?}", params)),
                }
            }

            _ => Err(anyhow!("Unknown command {:?}", cmd)),
        }
    }

    fn is_master(&self) -> bool {
        self.replica_of.is_none()
    }

    fn replicate(&self, command_array: RESP) {
        assert!(matches!(&command_array, RESP::Array(_)), "not an array: {:?}", command_array);
        assert!(self.is_master(), "not a master");
        self.replica_senders.read().unwrap().iter().for_each(move |sender| {
            sender.send(command_array.clone()).unwrap()
        });
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
