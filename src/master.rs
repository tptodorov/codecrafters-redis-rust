use std::net::SocketAddr;
use std::sync::{Arc, mpsc, RwLock};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};

use crate::connection::ClientConnectionHandler;
use crate::io::net::{Binding, Port};
use crate::protocol::command::{Command, CommandRequest};
use crate::protocol::rdb::empty_rdb;
use crate::protocol::resp::{RESP, RESPConnection};
use crate::redis::RedisServer;

type ReplicaResponse = (usize, usize); // offset, replica index
#[derive(Clone, Debug)]
enum ReplicaMessage {
    // message for replication and no response is expected
    Replicate(RESP),
    // command that expects a response from the replica
    Command(RESP, Sender<ReplicaResponse>, usize, Duration),
}

#[derive(Debug)]
struct Replica {
    sender: Sender<ReplicaMessage>,
    offset: usize,
}

#[derive(Clone)]
pub struct MasterServer {
    redis: RedisServer,
    replicas: Arc<RwLock<Vec<Replica>>>,
}


impl MasterServer {
    pub fn new(redis: RedisServer) -> Self {
        MasterServer {
            redis,
            replicas: Arc::new(RwLock::new(vec![])),
        }
    }
}

pub struct MasterConnection {
    master: MasterServer,
    replica_binding: Option<Binding>,
    remote_host: SocketAddr,
}

impl MasterConnection {
    pub fn new(master: MasterServer, remote_host: SocketAddr) -> Self {
        MasterConnection {
            master,
            replica_binding: None,
            remote_host,
        }
    }


    fn handle_client_command(&mut self, cmd: &CommandRequest) -> Result<Vec<RESP>> {
        match cmd.as_ref() {
            (Command::REPLCONF, [sub_command, param1]) => {
                // minimal implementation of https://redis.io/docs/latest/commands/replconf/
                // REPLCONF ...
                match sub_command.to_uppercase().as_str() {
                    "LISTENING-PORT" => {
                        let replica_port = param1.parse::<Port>()?;
                        self.replica_binding = Some(Binding(self.remote_host.ip().to_string(), replica_port));
                    }
                    _ => {
                        // ignore other replconf commands
                    }
                }
                Ok(
                    vec![
                        RESP::String("OK".to_string())
                    ])
            }

            (Command::WAIT, [required_replicas, timeout_ms]) => {
                // minimal implementation of https://redis.io/docs/latest/commands/wait/
                // WAIT ...

                let required_replicas = required_replicas.parse::<i64>().unwrap_or(-1);
                let timeout_ms = timeout_ms.parse::<i64>().unwrap_or(-1);
                if required_replicas >= 0 && timeout_ms >= 0 {
                    if self.master.redis.log_store.read().unwrap().log.is_empty() {
                        // for empty log we don't need to check replicas
                        let active_replicas = self.master.replicas.read().unwrap().len();
                        Ok(vec![RESP::Int(active_replicas as i64)])
                    } else {
                        // ack from all replicas
                        let ack_replicas = self.request_ack(required_replicas.abs() as u32, Duration::from_millis(timeout_ms.abs() as u64))?;
                        Ok(vec![RESP::Int(ack_replicas as i64)])
                    }
                } else {
                    bail!("invalid wait command {:?}", cmd)
                }
            }

            (Command::PSYNC, [repl_id, offset]) => {
                // minimal implementation of https://redis.io/docs/latest/commands/psync/
                // PSYNC replication-id offset

                // replica does not know where to start
                if (repl_id == "?" && offset == "-1") || repl_id == &self.master.redis.master_replid {
                    // this makes the current connection a replication connection
                    let mut sync_response = vec![RESP::String(format!("FULLRESYNC {} 0", self.master.redis.master_replid)), RESP::File(empty_rdb())];
                    // add current log for replication on top of the rds image
                    let mut current_log = self.master.redis.log_store.read().unwrap().log.clone();
                    sync_response.append(&mut current_log);
                    Ok(sync_response)
                } else {
                    bail!("invalid psync command {:?}", cmd)
                }
            }

            _ => self.master.redis.handle_command(cmd),
        }
    }


    fn send_replicas(&self, message_bytes: usize, message: &RESP) -> Result<()> {
        assert!(matches!(&message, RESP::Array(_)), "not an array: {}", message);

        // append to the command log
        {
            let mut log_store = self.master.redis.log_store.write().unwrap();
            log_store.log.push(message.clone());
        }

        let mut failed_indexes = vec![];
        let mut replicas = self.master.replicas.write().unwrap();
        println!("replicating {} to {} replicas", message, replicas.len());
        for (i, replica) in replicas.iter().enumerate() {
            if replica.sender.send(ReplicaMessage::Replicate(message.clone())).is_err() {
                failed_indexes.push(i);
            }
        }
        println!("replica connections failed: {:?}", failed_indexes);
        for (items_removed, i) in failed_indexes.iter().enumerate() {
            replicas.remove(i - items_removed);
        }

        if replicas.len() - failed_indexes.len() > 0 {
            // update stored offset
            self.master.redis.log_store.write().unwrap().log_bytes += message_bytes;
        }

        Ok(())
    }

    fn request_ack(&self, expected_replicas: u32, timeout: Duration) -> Result<u32> {
        println!("sending getack to all replicas ");

        let (tx, rx) = mpsc::channel::<ReplicaResponse>();

        let master_offset = self.master.redis.log_store.read().unwrap().log_bytes;

        let getack = RESP::Array(vec![RESP::bulk("REPLCONF"), RESP::bulk("GETACK"), RESP::bulk("*")]);

        let mut replicas = self.master.replicas.write().unwrap();

        let mut replicated = 0_u32;
        let mut requested_ack = 0;

        for (i, replica) in replicas.iter().enumerate() {
            if replicated >= expected_replicas {
                break;
            }
            // TODO check replica last ack
            if master_offset == replica.offset {
                replicated += 1;
            } else if replica.sender.send(ReplicaMessage::Command(getack.clone(), tx.clone(), i, timeout)).is_ok()
            {
                requested_ack += 1;
            }
        }

        println!("waiting for ack from {} replicas offset {}", requested_ack, master_offset);
        let started_at = Instant::now();
        loop {
            if replicated >= expected_replicas {
                break;
            }
            if let Ok((off, replica_index)) = rx.try_recv() {
                if off >= master_offset {
                    replicated += 1;
                    replicas[replica_index].offset = off;
                }
                continue;
            }
            if Instant::now() - started_at > timeout {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        println!("expected replicas {} but ack replicas {}", expected_replicas, replicated);
        println!("master replicas updated {:?}", replicas);
        Ok(replicated)
    }

    fn master_replica_connection(&mut self, connection: &mut RESPConnection) -> Result<()> {
        let thread_name = format!("master-replica-{}", self.replica_binding.clone().unwrap());
        let thread_name = &thread_name;

        // this connection is turning into replication connection
        println!("@{}: PSYNC completed, this connection is a replication connection to replica {:?}", thread_name, self.replica_binding);

        // register listener for messages
        let (tx, rx) = mpsc::channel();
        {
            let mut replicas = self.master.replicas.write().unwrap();
            replicas.push(Replica { sender: tx, offset: 0 });
            println!("@{}: active replicas now {:?}", thread_name, replicas);
        }

        // TODO remove the TX from the list

        // any received messages will be sent to the current replica connection
        for received in rx {
            println!("@{}: Sending to replica: {:?}", thread_name, received);
            match received {
                ReplicaMessage::Replicate(message) => {
                    if let Err(err) = connection.send_message(&message) {
                        println!("@{}: returned error: {} while replicating command: {:?}", thread_name, err, message);
                        if err.to_string().contains("Broken pipe") {
                            bail!("client connection dropped");
                        }
                    }
                }
                ReplicaMessage::Command(message, tx, replica_index, timeout) => {
                    if let Err(err) = connection.send_message(&message) {
                        println!("@{}: returned error: {} while requesting: {:?}", thread_name, err, &message);
                        if err.to_string().contains("Broken pipe") {
                            bail!("client connection dropped");
                        }
                    }
                    if let Ok(CommandRequest(Command::REPLCONF, _)) = message.try_into() {
                        // expect ack response from replica
                        let current_timeout = connection.read_timeout()?;
                        connection.set_read_timeout(Some(timeout))?;
                        if let Ok((_, Some(replica_ack))) = connection.read_message() {
                            if let Ok(CommandRequest(Command::REPLCONF, ack_params)) = replica_ack.try_into() {
                                if let Some(offset) = ack_params.last() {
                                    let offset = offset.parse::<usize>().unwrap();
                                    println!("@{}: replica ACKED with offset {} ", thread_name, offset);
                                    if tx.send((offset, replica_index)).is_err() {
                                        // channel already off
                                    }
                                }
                            }
                        } else {
                            println!("@{}: gave up waiting ACK from replica", thread_name);
                        }
                        connection.set_read_timeout(current_timeout)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl ClientConnectionHandler for MasterConnection {
    fn handle_request(
        &mut self,
        message_bytes: usize,
        message: RESP,
        command: CommandRequest,
        connection: &mut RESPConnection,
    ) -> Result<()> {
        let responses = self.handle_client_command(&command)?;

        if command.0.is_mutating() {
            // replicate mutations only if you are a master
            self.send_replicas(message_bytes, &message)?;
        }

        connection.send_messages(&responses.iter().map(|r| r).collect::<Vec<&RESP>>())?;

        if command.0 == Command::PSYNC {
            self.master_replica_connection(connection)?;
        }

        Ok(())
    }
}