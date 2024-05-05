use crate::client::RedisClient;
use crate::net::{Binding, Port};

pub fn replication_protocol(this_port: Port, master: &Binding) -> anyhow::Result<()> {
    let mut master_client = RedisClient::new(master)?;

    master_client.ping()?;
    master_client.replconfig(&vec!["listening-port", &format!("{}", this_port)])?;
    master_client.replconfig(&vec!["capa", "psync2"])?;
    master_client.psync("?", -1)?;

    println!("replication initialised with master: {}", master);

    loop {
        let command = master_client.read_replication_command()?;
        println!("master sent command: {:?}", command);
    }

}
