use std::net::TcpStream;
use crate::resp::RESPConnection;

pub trait ClientConnectionHandler {
    fn handle_connection(&mut self, stream: TcpStream) -> anyhow::Result<()> {
        let mut connection = RESPConnection::new(stream);
        loop {
            self.handle_message(&mut connection)?;
        }
    }
    fn handle_message(&mut self, connection: &mut RESPConnection) -> anyhow::Result<()>;
}