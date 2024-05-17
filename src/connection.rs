use std::net::TcpStream;
use std::thread;

use anyhow::Result;

use crate::protocol::command::Command;
use crate::protocol::resp::{RESP, RESPConnection};

pub trait ClientConnectionHandler {
    /// processing messages from a tcp stream
    fn handle_connection(&mut self, stream: TcpStream) -> Result<()> {
        let mut connection = RESPConnection::new(stream);
        loop {
            let current = thread::current();
            let thread_name = current.name().unwrap();

            let (message_bytes, message) = connection.read_message()?;
            let message = message.expect("message not read");
            let (command, params) = Command::parse_command(&message)?;

            println!("@{}: received command: {} {:?} ", thread_name, command, params);

            self.handle_message(message_bytes, &message, &command, &params, &mut connection)?;
        }
    }
    fn handle_message(&mut self, message_bytes: usize, message: &RESP, command: &Command, params: &[String], connection: &mut RESPConnection) -> Result<()>;
}