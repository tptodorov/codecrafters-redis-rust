use anyhow::anyhow;
use crate::resp::RESP;

pub fn command_handler(cmd: &str, params: &[RESP]) -> anyhow::Result<RESP> {
    match cmd {
        "PING" => Ok(RESP::String("PONG".to_string())),
        "ECHO" => {
            let param1 = params.get(0).unwrap_or(&RESP::Null);

            match param1 {
                RESP::Bulk(param1) => Ok(RESP::Bulk(param1.to_owned())),
                _ => Err(anyhow!("invalid echo  command {:?}", params)),
            }
        }
        _ => Err(anyhow!("Unknown command {}", cmd)),
    }
}
