use std::net::TcpStream;
use std::thread;

use anyhow::Result;

use crate::protocol::command::CommandRequest;
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
            let command: CommandRequest = message.clone().try_into()?;

            println!("@{}: received command: {:?} ", thread_name, command);

            self.handle_request(message_bytes, message, command, &mut connection)?;
        }
    }
    fn handle_request(
        &mut self,
        message_bytes: usize,
        message: RESP,
        command: CommandRequest,
        connection: &mut RESPConnection,
    ) -> Result<()>;
}
