use std::collections::HashMap;
use std::net::TcpStream;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};

use crate::net::{Binding, Port};
use crate::rdb::{empty_rdb, StoredValue};
use crate::resp::{RESP, RESPConnection};
use std::sync::mpsc;
use std::thread;
use crate::client::RedisClient;

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
    WAIT,
}

impl Command {
    /** command mutates the local storage */
    pub fn is_mutating(&self) -> bool {
        matches!(self, Command::SET | Command::DEL )
    }

    /**
     * command can be sent over the replication connection and supports replication functionality.
     */
    pub fn is_replicating(&self) -> bool {
        matches!(self, Command::SET | Command::DEL | Command::REPLCONF | Command::PING )
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
            "WAIT" => Ok(Command::WAIT),
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
    log: Arc<RwLock<Vec<RESP>>>,
}

impl RedisServer {
    pub fn new(binding: Binding, replica_of: Option<Binding>) -> Result<Self> {
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990deep".to_string();

        let server = RedisServer {
            // binding,
            store: Arc::new(RwLock::new(HashMap::new())),
            master_repl_offset: 0,
            master_replid,
            replica_of: replica_of.clone(),
            replica_senders: Arc::new(RwLock::new(vec![])),
            log: Arc::new(RwLock::new(vec![])),
        };

        // start replication thread
        if let Some(master) = replica_of {
            let replica_redis = server.clone(); // cheap clone, store is shared
            thread::spawn(move || {
                loop {
                    replica_redis.replica_replication_protocol(binding.1, &master).unwrap_or_else(|err| println!("replication failed: {:?}. will restart replication connection", err));
                    thread::sleep(Duration::from_secs(1));
                }
            });
        }

        Ok(
            server
        )
    }

    pub fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let mut connection = RESPConnection::new(stream);
        loop {
            if let (_len, Some(command)) = connection.read_resp()? {
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

                        if command.is_mutating() && self.is_master() {
                            // replicate mutations only if you are a master
                            self.master_replicate(message);
                        }

                        println!("{:?} -> {:?}", command, responses);
                        if let Err(err) = connection.send_responses(&responses.iter().map(|r| r).collect::<Vec<&RESP>>()) {
                            bail!("error while writing response: {}. terminating connection", err);
                        }

                        if command == Command::PSYNC {
                            println!("confirm with replica");
                            // connection.send_command("REPLCONF GETACK *")?;
                            // if let Some(RESP::String(repl_ack_response)) = connection.next() {
                            //     if repl_ack_response.to_uppercase() == "REPLCONF ACK 0" {
                            //         println!("replica accepted the rds: {}", repl_ack_response);
                            //     }
                            // }

                            // this connection is turning into replication connection
                            println!("after PSYNC, here will be sending replication commands");

                            // register listener for messages
                            let (tx, rx) = mpsc::channel::<RESP>();
                            self.replica_senders.write().unwrap().push(tx);
                            // TODO remove the TX from the list

                            // any received messages will be sent to the current replica connection
                            for received in rx {
                                println!("Replicating: {:?}", received);
                                if let Err(err) = connection.send_response(&received) {
                                    println!("returned error: {} while replicating command: {:?}", err, received);
                                    if err.to_string().contains("Broken pipe") {
                                        bail!("client connection dropped");
                                    }
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
            Command::WAIT => {
                // minimal implementation of https://redis.io/docs/latest/commands/wait/
                // WAIT ...
                assert!(self.is_master(), "only master can do {:?}", cmd);
                let active_replicas = self.replica_senders.read().unwrap().len();
                Ok(vec![RESP::Int(active_replicas as i64)])
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
                            let mut sync_response = vec![RESP::String(format!("FULLRESYNC {} 0", self.master_replid)), RESP::File(empty_rdb())];
                            // add current log for replication on top of the rds image
                            let mut current_log = self.log.read().unwrap().clone();
                            sync_response.append(&mut current_log);
                            Ok(sync_response)
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

    fn master_replicate(&self, command_array: RESP) {
        assert!(matches!(&command_array, RESP::Array(_)), "not an array: {:?}", command_array);
        assert!(self.is_master(), "not a master");
        // append to the command log
        self.log.write().unwrap().push(command_array.clone());

        self.replica_senders.read().unwrap().iter().for_each(move |sender| {
            sender.send(command_array.clone()).unwrap()
        });
    }

    pub fn replica_replication_protocol(&self, this_port: Port, master: &Binding) -> Result<()> {
        let mut master_client = RedisClient::new(master)?;

        master_client.ping()?;
        master_client.replconfig(&vec!["listening-port", &format!("{}", this_port)])?;
        master_client.replconfig(&vec!["capa", "psync2"])?;
        master_client.psync("?", -1)?;

        println!("replication connection initialised with master: {}", master);

        // accumulating data sent from master to replica
        let mut read_bytes = 0_u64;

        loop {
            let (len, message) = master_client.read_replication_command()?;
            println!("master sent message over replication connection: {:?}", message);

            match &message {
                RESP::Array(array) =>
                    match &array[..] {
                        [RESP::Bulk(command), params @ .. ] => {
                            let command: Command = command.parse()?;

                            // this is an exception to the normal replication flow of commands
                            if command == Command::REPLCONF {
                                println!("replica ack to master request: {:?}", message);
                                master_client.respond_replconf_ack(read_bytes)?;

                                read_bytes += len;
                                println!("after command {:?} offset is {}", message, read_bytes);

                                continue;
                            }

                            read_bytes += len;
                            println!("after command {:?} offset is {}", message, read_bytes);

                            assert!(command.is_replicating(), "command not acceptable in replication channel");

                            self.handle_command(&command, params)?;
                            // replicas are ignoring the response from successful command
                            println!("replicated command: {:?}", command);
                        }
                        _ => {
                            // ignore other messages
                        }
                    }
                _ => {
                    // ignore other messages
                }
            }
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