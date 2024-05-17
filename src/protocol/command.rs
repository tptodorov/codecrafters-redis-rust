use std::fmt::Display;
use std::str::FromStr;

use anyhow::bail;

use crate::protocol::resp::RESP;

#[derive(Debug, PartialEq)]
pub enum Command {
    PING,
    ECHO,
    // storage commands
    SET,
    GET,
    TYPE,
    KEYS,
    // replication commands
    PSYNC,
    INFO,
    REPLCONF,
    WAIT,
    CONFIG,
    // stream commands
    XADD,
    XRANGE,
    XREAD,
}

impl Command {
    /** command mutates the local storage */
    pub fn is_mutating(&self) -> bool {
        matches!(self, Command::SET  )
    }

    pub(crate) fn parse_command(message: &RESP) -> anyhow::Result<(Command, Vec<String>)> {
        match message {
            RESP::Array(array) =>
                if array.iter().all(|x| matches!(x, RESP::Bulk(_))) {
                    let strings = array.iter().map(|r| r.to_string()).collect::<Vec<String>>();
                    match &strings[..] {
                        [command, params @ .. ] => {
                            let cmd = command.parse::<Command>()?;
                            return Ok((cmd, params.to_vec()));
                        }
                        _ => {}
                    }
                }
            _ => {}
        }
        bail!("message is not a valid command: {}", message)
    }
}


impl FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> anyhow::Result<Command, Self::Err> {
        match input.to_uppercase().as_str() {
            "PING" => Ok(Command::PING),
            "GET" => Ok(Command::GET),
            "TYPE" => Ok(Command::TYPE),
            "SET" => Ok(Command::SET),
            "KEYS" => Ok(Command::KEYS),
            "PSYNC" => Ok(Command::PSYNC),
            "ECHO" => Ok(Command::ECHO),
            "INFO" => Ok(Command::INFO),
            "REPLCONF" => Ok(Command::REPLCONF),
            "WAIT" => Ok(Command::WAIT),
            "CONFIG" => Ok(Command::CONFIG),
            "XADD" => Ok(Command::XADD),
            "XRANGE" => Ok(Command::XRANGE),
            "XREAD" => Ok(Command::XREAD),
            _ => bail!("unknown command: {}", input),
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::PING => write!(f, "PING"),
            Command::ECHO => write!(f, "ECHO"),
            Command::SET => write!(f, "SET"),
            Command::KEYS => write!(f, "KEYS"),
            Command::GET => write!(f, "GET"),
            Command::TYPE => write!(f, "TYPE"),
            Command::PSYNC => write!(f, "PSYNC"),
            Command::INFO => write!(f, "INFO"),
            Command::REPLCONF => write!(f, "REPLCONF"),
            Command::WAIT => write!(f, "WAIT"),
            Command::CONFIG => write!(f, "CONFIG"),
            Command::XADD => write!(f, "XADD"),
            Command::XRANGE => write!(f, "XRANGE"),
            Command::XREAD => write!(f, "XREAD"),
        }
    }
}