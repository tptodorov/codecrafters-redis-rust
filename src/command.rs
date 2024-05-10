use std::fmt::Display;
use std::str::FromStr;
use anyhow::bail;
use crate::resp::RESP;

#[derive(Debug, PartialEq)]
pub enum Command {
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

    pub(crate) fn parse_command(message: &RESP) -> anyhow::Result<(Command, &[RESP])> {
        match message {
            RESP::Array(array) =>
                match &array[..] {
                    [RESP::Bulk(command), params @ .. ] => {
                        command.parse::<Command>().map(|cmd| (cmd, params))
                    }
                    _ => bail!("Command not found in message: {}", message),
                }
            _ => bail!("Command not found in message: {}", message),
        }
    }

}


impl FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> anyhow::Result<Command, Self::Err> {
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

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::PING => write!(f, "PING"),
            Command::ECHO => write!(f, "ECHO"),
            Command::SET => write!(f, "SET"),
            Command::DEL => write!(f, "DEL"),
            Command::GET => write!(f, "GET"),
            Command::PSYNC => write!(f, "PSYNC"),
            Command::INFO => write!(f, "INFO"),
            Command::REPLCONF => write!(f, "REPLCONF"),
            Command::WAIT => write!(f, "WAIT"),
        }
    }
}