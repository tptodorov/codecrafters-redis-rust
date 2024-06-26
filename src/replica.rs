use std::thread;
use std::time::Duration;

use anyhow::{bail, Result};

use crate::client::ReplicaClient;
use crate::connection::ClientConnectionHandler;
use crate::io::net::Binding;
use crate::protocol::command::{Command, CommandRequest};
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

    pub(crate) fn handle_client_command(
        &self,
        cmd: CommandRequest,
    ) -> Result<Vec<RESP>> {
        if cmd.0.is_mutating() {
            bail!("replica can't handle mutating command: {:?}", cmd)
        }
        match cmd {
            _ => self.redis.handle_command(&cmd),
        }
    }

    pub(crate) fn handle_internal_command(
        &self,
        cmd: &CommandRequest,
    ) -> Result<Vec<RESP>> {
        match cmd {
            CommandRequest(Command::REPLCONF, _) => {
                // minimal implementation of https://redis.io/docs/latest/commands/replconf/
                // REPLCONF ...
                Ok(vec![RESP::Array(vec![
                    RESP::bulk("REPLCONF"),
                    RESP::bulk("ACK"),
                    RESP::bulk(&format!("{}", self.replicated_offset)),
                ])])
            }
            _ => {
                let _response = self.redis.handle_command(cmd)?;
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

        println!(
            "@{}: replication connection initialised with master: {}",
            thread_name, self.replica_of
        );

        // accumulating data sent from master to replica

        loop {
            let (len, message) = master_client.read_replication_command()?;
            println!(
                "@{}: master sent message over replication connection: {:?}",
                thread_name, message
            );

            let command: CommandRequest = message.try_into()?;

            let responses = self.handle_internal_command(&command)?;
            master_client
                .stream
                .send_messages(&responses.iter().map(|r| r).collect::<Vec<&RESP>>())?;
            println!(
                "@{}: replica connection handled {:?} and responded to master: {:?}",
                thread_name, command, responses
            );

            self.replicated_offset += len;
            println!(
                "@{}: replica after command {:?} offset is {}",
                thread_name, command, self.replicated_offset
            );
        }
    }
}

impl ClientConnectionHandler for ReplicaConnection {
    fn handle_request(
        &mut self,
        _message_bytes: usize,
        _message: RESP,
        command: CommandRequest,
        connection: &mut RESPConnection,
    ) -> anyhow::Result<()> {
        println!("handled {:?} ", command);
        let responses = self.handle_client_command(command)?;

        println!("responded with: {:?}", responses);
        connection.send_messages(&responses.iter().map(|r| r).collect::<Vec<&RESP>>())?;

        Ok(())
    }
}

pub fn start_replication(redis: RedisServer, replica_of: Binding) -> Result<()> {
    let thread_name = format!("replica-master-{}", replica_of);
    thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
            let mut replica = ReplicaConnection::new(redis.clone(), replica_of.clone());
            loop {
                replica.replica_master_connection().unwrap_or_else(|err| {
                    println!(
                        "replication failed: {:?}. will restart replication connection",
                        err
                    )
                });
                thread::sleep(Duration::from_secs(2));
            }
        })?;
    Ok(())
}
