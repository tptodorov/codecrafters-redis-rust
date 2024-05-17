use std::thread;
use std::time::Duration;

use anyhow::{bail, Result};

use crate::client::ReplicaClient;
use crate::connection::ClientConnectionHandler;
use crate::io::net::Binding;
use crate::protocol::command::Command;
use crate::protocol::resp::{RESP, RESPConnection};
use crate::redis::RedisServer;

#[derive(Clone)]
pub struct ReplicaConnection {
    redis: RedisServer,
    replica_of: Binding,
    replicated_offset: usize,
}


impl ReplicaConnection {
    pub fn new(redis: RedisServer, replica_of: Binding) -> Self {
        Self {
            replica_of: replica_of.clone(),
            redis,
            replicated_offset: 0,
        }
    }

    pub(crate) fn handle_client_command(&self, cmd: &Command, params: &[String]) -> Result<Vec<RESP>> {
        if cmd.is_mutating() {
            bail!("replica can't handle mutating command: {:?}", cmd)
        }
        match cmd {
            _ => self.redis.handle_command(cmd, params)
        }
    }

    pub(crate) fn handle_internal_command(&self, cmd: &Command, params: &[String]) -> Result<Vec<RESP>> {
        match cmd {
            Command::REPLCONF => {
                // minimal implementation of https://redis.io/docs/latest/commands/replconf/
                // REPLCONF ...
                Ok(
                    vec![
                        RESP::Array(vec![
                            RESP::Bulk("REPLCONF".to_string()),
                            RESP::Bulk("ACK".to_string()),
                            RESP::Bulk(format!("{}", self.replicated_offset))])
                    ])
            }
            _ => {
                let _response = self.redis.handle_command(cmd, params)?;
                // when replicating responses are ignored
                Ok(vec![])
            }
        }
    }


    pub fn replica_master_connection(&mut self) -> Result<()> {
        let current = thread::current();
        let thread_name = current.name().unwrap();

        let mut master_client = ReplicaClient::new(&self.replica_of)?;
        let this_port = self.redis.binding.1;

        master_client.ping_pong()?;
        master_client.replconf(&vec!["listening-port", &format!("{}", this_port)])?;
        master_client.replconf(&vec!["capa", "psync2"])?;
        let _rds = master_client.psync("?", -1)?;
        // reset offset after snapshot reset
        self.replicated_offset = 0;

        println!("@{}: replication connection initialised with master: {}", thread_name, self.replica_of);

        // accumulating data sent from master to replica

        loop {
            let (len, message) = master_client.read_replication_command()?;
            println!("@{}: master sent message over replication connection: {:?}", thread_name, message);

            let (command, params) = Command::parse_command(&message)?;

            let responses = self.handle_internal_command(&command, &params)?;
            master_client.stream.send_messages(&responses.iter().map(|r| r).collect::<Vec<&RESP>>())?;
            println!("@{}: replica connection handled {:?} and responded to master: {:?}", thread_name, command, responses);

            self.replicated_offset += len;
            println!("@{}: replica after message {:?} offset is {}", thread_name, message, self.replicated_offset);
        }
    }
}

impl ClientConnectionHandler for ReplicaConnection {
    fn handle_message(&mut self, connection: &mut RESPConnection) -> Result<()> {
        let (_, message) = connection.read_message()?;
        let message = message.expect("message not read");

        println!("message: {:?}", message);
        let (command, params) = Command::parse_command(&message)?;

        let responses = self.handle_client_command(&command, &params)?;

        println!("handled {:?} with: {:?}", command, responses);
        connection.send_messages(&responses.iter().map(|r| r).collect::<Vec<&RESP>>())?;

        Ok(())
    }
}

pub fn start_replication(redis: RedisServer, replica_of: Binding) -> Result<()> {
    let thread_name = format!("replica-master-{}", replica_of);
    thread::Builder::new().name(thread_name.clone()).spawn(move || {
        let mut replica = ReplicaConnection::new(redis.clone(), replica_of.clone());
        loop {
            replica.replica_master_connection().unwrap_or_else(|err| println!("replication failed: {:?}. will restart replication connection", err));
            thread::sleep(Duration::from_secs(2));
        }
    })?;
    Ok(())
}
