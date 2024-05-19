use core::time::Duration;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::sync::{Condvar, Mutex, Weak};
use std::time::SystemTime;

use anyhow::{bail, format_err};

use crate::protocol::rdb;
use crate::protocol::rdb::LengthEncoding;
use crate::stream::{Stream, StreamEvent, StreamRecordId};

enum Value {
    String(String),
    Stream(Stream),
}

struct StoreEntry {
    value: Value,
    valid_until: Option<SystemTime>,
}

impl StoreEntry {
    fn from_string(value: &str, valid_until: Option<SystemTime>) -> Self {
        StoreEntry {
            value: Value::String(value.to_string()),
            valid_until,
        }
    }
    fn empty_stream(key: &str) -> Self {
        StoreEntry {
            value: Value::Stream(Stream::new(key.to_string())),
            valid_until: None,
        }
    }

    fn value(&self) -> Option<String> {
        if let Some(valid_until) = self.valid_until {
            if valid_until < SystemTime::now() {
                return None;
            }
        }
        match self.value {
            Value::String(ref value) => Some(value.clone()),
            _ => None,
        }
    }


    fn value_type(&self) -> &str {
        match &self.value {
            Value::String(_) => {
                if self.value().is_none() {
                    "none"
                } else {
                    "string"
                }
            }
            Value::Stream(_) => "stream",
        }
    }

    fn stream(&self) -> Option<&Stream> {
        if let Value::Stream(ref stream) = self.value {
            Some(stream)
        } else {
            None
        }
    }
    fn stream_mut(&mut self) -> Option<&mut Stream> {
        if let Value::Stream(ref mut stream) = self.value {
            Some(stream)
        } else {
            None
        }
    }
}

pub(crate) struct Store(HashMap<String, StoreEntry>);

impl Store {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn get_value(&self, key: &str) -> Option<String> {
        self.0.get(key).and_then(|v| v.value())
    }

    pub fn get_type(&self, key: &str) -> &str {
        self.0.get(key).map_or("none", |v| v.value_type())
    }

    pub fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }

    pub fn insert_value(&mut self, key: &str, value: &str, expiration: Option<Duration>) -> () {
        let valid_until = expiration
            .and_then(|d| SystemTime::now().checked_add(d));
        self.0.insert(key.to_string(), StoreEntry::from_string(value, valid_until));
    }

    pub fn insert_stream(
        &mut self,
        key: &str,
        id_pattern: &str,
        stream_data: Vec<(String, String)>,
    ) -> anyhow::Result<String> {
        if !self.0.contains_key(key) {
            self.0
                .insert(key.to_string(), StoreEntry::empty_stream(key));
        }

        let value = self.0.get_mut(key).and_then(|v| v.stream_mut()).ok_or_else(
            || format_err!("stream not found {}", key))?;

        return value.add_entry(id_pattern.to_string(), stream_data);
    }

    pub fn range_stream(
        &self,
        key: &str,
        from_id: StreamRecordId,
        to_id: StreamRecordId,
    ) -> anyhow::Result<Vec<(String, &Vec<(String, String)>)>> {
        self.0.get(key).and_then(|v| v.stream()).map_or_else(
            || bail!("stream not found {}", key),
            |value| {
                Ok(value
                    .range(&from_id, &to_id, true)?
                    .iter()
                    .map(|&entry| (entry.id.to_string(), &entry.attributes))
                    .collect())
            },
        )
    }

    pub fn read_stream(
        &self,
        key: &str,
        from_id: StreamRecordId,
        to_id: StreamRecordId,
    ) -> anyhow::Result<Vec<(String, &Vec<(String, String)>)>> {
        self.0.get(key).and_then(|v| v.stream()).map_or_else(
            || bail!("stream not found {}", key),
            |value| {
                Ok(value
                    .range(&from_id, &to_id, false)?
                    .iter()
                    .map(|&entry| (entry.id.to_string(), &entry.attributes))
                    .collect())
            },
        )
    }

    pub fn latest_stream(&self, key: &str) -> anyhow::Result<StreamRecordId> {
        self.0.get(key).and_then(|v| v.stream()).map_or_else(
            || bail!("stream not found {}", key),
            |value| value.last_id(),
        )
    }

    pub fn add_listener(
        &mut self,
        keys: &Vec<&String>,
        listener: Weak<(Mutex<Option<StreamEvent>>, Condvar)>,
    ) -> anyhow::Result<()> {
        for &key in keys {
            let value = self.0.get_mut(key).and_then(|v| v.stream_mut()).ok_or_else(
                || format_err!("stream not found {}", key))?;
            value.add_listener(listener.clone())?;
        }
        Ok(())
    }

    /**
    load rdb file into the store.
     */
    pub fn load_rdb(&mut self, mut reader: BufReader<File>) -> anyhow::Result<()> {
        // Loading of the RDB file is based on the https://rdb.fnordig.de/file_format.html
        let mut header = [0x00; 9];
        reader.read_exact(&mut header)?;
        let header = String::from_utf8_lossy(&header);
        if !header.starts_with("REDIS") {
            bail!("invalid header: {}", header);
        }

        let version = header["REDIS".len()..header.len()].to_string();
        println!("rdb version: {}", version);
        let mut valid_until_ms = None;

        while let Ok(op) = rdb::read_byte(&mut reader) {
            match op {
                0xFA => {
                    // AUX fields
                    let key = rdb::read_string(&mut reader)?;
                    let value = rdb::read_string(&mut reader)?;
                    println!("aux: {} {}", key, value);
                    // TODO
                }
                0xFE => {
                    // Database selector
                    let db_number = match rdb::read_length(&mut reader)? {
                        LengthEncoding::Len(len) => len,
                        LengthEncoding::Short(len) => len as u32,
                        LengthEncoding::Byte(len) => len as u32,
                        LengthEncoding::Int(len) => len,
                    };
                    println!("database selector {}", db_number);
                    // TODO
                }
                0xFB => {
                    // resize db field
                    let hash_size = rdb::read_int(&mut reader)?;
                    let expire_size = rdb::read_int(&mut reader)?;
                    println!("sizes {} {}", hash_size, expire_size);
                    // TODO
                }
                0xFD => {
                    // The following expire value is specified in seconds. The following 4 bytes represent the Unix timestamp as an unsigned integer.
                    valid_until_ms = Some((rdb::read_u32(&mut reader)? as u64) * 1000);
                }
                0xFC => {
                    // The following expire value is specified in milliseconds. The following 8 bytes represent the Unix timestamp as an unsigned long.
                    valid_until_ms = Some(rdb::read_u64(&mut reader)?);
                }
                0xFF => {
                    // rdb load finished
                    rdb::read_crc64(&mut reader)?;
                    return Ok(());
                }
                0..=14 => {
                    let key = rdb::read_string(&mut reader)?;
                    let value = rdb::read_string(&mut reader)?;
                    let valid_until = valid_until_ms.map(|epoch_ms| {
                        SystemTime::UNIX_EPOCH + Duration::from_millis(epoch_ms)
                    });
                    self.0.insert(
                        key.clone(),
                        StoreEntry::from_string(
                            &value,
                            valid_until,
                        ),
                    );
                    // reset
                    valid_until_ms = None;
                }

                _ => {
                    bail!("invalid rdb op: {}", op);
                }
            }
        }
        Ok(())
    }
}

