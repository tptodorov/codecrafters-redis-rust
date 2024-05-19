use std::fmt::Display;
use std::str::FromStr;

pub const DEFAULT_PORT: Port = 6379;

pub type Port = u32;
pub type Hostname = String;

#[derive(Debug, Clone)]
pub struct Binding(pub Hostname, pub Port);

impl Display for Binding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{}:{}", self.0, self.1))
    }
}

impl FromStr for Binding {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut seq = s.split(' ');
        let host = seq.next().ok_or(anyhow::format_err!("invalid binding: {}", s))?;
        let default_port_str = DEFAULT_PORT.to_string();
        let port = seq.next().unwrap_or(&default_port_str).parse::<Port>()?;
        let master = Binding(host.to_string(), port);
        Ok(master)
    }
}