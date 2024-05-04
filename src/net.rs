use std::fmt::Display;

pub type Port = u32;
pub type Hostname = String;
#[derive(Debug, Clone)]
pub struct Binding(pub Hostname, pub Port);

impl Display for Binding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{}:{}", self.0, self.1))
    }
}
