use core::time::Duration;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Condvar, Mutex, Weak};
use std::time::SystemTime;

use anyhow::bail;

use crate::protocol::rdb;
use crate::protocol::rdb::LengthEncoding;

#[derive(Clone, Debug)]
pub struct StreamEntryId(u64, u64);

impl PartialEq<Self> for StreamEntryId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl PartialOrd for StreamEntryId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.0 > other.0 || (self.0 == other.0 && self.1 > other.1) {
            Some(Ordering::Greater)
        } else if self.0 < other.0 || (self.0 == other.0 && self.1 < other.1) {
            Some(Ordering::Less)
        } else if self.0 == other.0 && self.1 == other.1 {
            Some(Ordering::Equal)
        } else {
            None
        }
    }
}

impl FromStr for StreamEntryId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self, Self::Err> {
        let parts = s.split('-').collect::<Vec<&str>>();
        match parts[..] {
            [first, second] => Ok(Self(first.parse()?, second.parse()?)),
            _ => Ok(Self(s.parse()?, 0)),
        }
    }
}

impl Display for StreamEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.0, self.1)
    }
}

impl StreamEntryId {
    pub(crate) const MIN: Self = Self(0, 0);
    pub(crate) const MAX: Self = Self(u64::MAX, u64::MAX);

    pub fn new(time_id: u64, seq_id: u64) -> Self {
        Self(time_id, seq_id)
    }

    pub fn from_pattern(pattern: String, last_id: Option<&StreamEntryId>) -> anyhow::Result<Self> {
        if pattern == "*" {
            Ok(match last_id {
                None => {
                    let now = SystemTime::now();
                    let time_id = now
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    Self(time_id, 0)
                }
                Some(StreamEntryId(last_time_id, last_seq_id)) => {
                    Self(*last_time_id, last_seq_id + 1)
                }
            })
        } else {
            match pattern.split('-').collect::<Vec<&str>>().as_slice() {
                [time_id, "*"] => {
                    let time_id = time_id.parse::<u64>()?;
                    let default_seq_id = if time_id == 0 { 1 } else { 0 };

                    Ok(match last_id {
                        None => {
                            // let now = SystemTime::now();
                            // let time_id = now.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
                            // let seq_id = 0;
                            Self(time_id, default_seq_id)
                        }
                        Some(StreamEntryId(last_time_id, last_seq_id)) => {
                            if *last_time_id == time_id {
                                Self(time_id, last_seq_id + 1)
                            } else {
                                Self(time_id, default_seq_id)
                            }
                        }
                    })
                }
                _ => bail!("invalid id"),
            }
        }
    }
}

#[derive(Clone, Debug)]
struct StreamEntry(StreamEntryId, Vec<(String, String)>);

#[derive(Clone, Debug)]
pub struct StreamEvent(pub(crate) String, pub(crate) StreamEntryId);

#[derive(Clone, Debug)]
struct StreamListener(Weak<(Mutex<Option<StreamEvent>>, Condvar)>);
// TODO the stream listeners should deregister with the store on destruction

enum Value {
    String(String),
    Stream(Vec<StreamEntry>, Vec<StreamListener>),
}

struct StoredValue {
    value: Value,
    valid_until: Option<SystemTime>,
    key: String,
}

impl StoredValue {
    fn from_string(key: &str, value: &str, valid_until: Option<SystemTime>) -> Self {
        StoredValue {
            key: key.to_string(),
            value: Value::String(value.to_string()),
            valid_until,
        }
    }
    fn empty_stream(key: &str) -> Self {
        StoredValue {
            key: key.to_string(),
            value: Value::Stream(Vec::new(), Vec::new()),
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

    fn add_entry(
        &mut self,
        id_pattern: String,
        entry: Vec<(String, String)>,
    ) -> anyhow::Result<String> {
        match &mut self.value {
            Value::Stream(ref mut entries, ref mut listeners) => {
                // new id is either explicit or pattern
                let new_id: StreamEntryId = if id_pattern.contains('*') {
                    StreamEntryId::from_pattern(id_pattern, entries.last().map(|e| &e.0))?
                } else {
                    id_pattern.parse()?
                };

                if new_id <= StreamEntryId(0, 0) {
                    bail!("ERR The ID specified in XADD must be greater than 0-0");
                }

                if let Some(last) = entries.last() {
                    let last_id = &last.0;
                    if &new_id <= last_id {
                        bail!("ERR The ID specified in XADD is equal or smaller than the target stream top item");
                    }
                }
                let new_ids = new_id.to_string();
                let stream_entry = StreamEntry(new_id.clone(), entry);
                entries.push(stream_entry.clone());

                let event = StreamEvent(self.key.clone(), new_id);

                // notify listeners while removing the dropped ones
                listeners.retain_mut(|l| {
                    if let Some(arc) = l.0.upgrade() {
                        let (lock, cvar) = arc.deref();
                        lock.lock().unwrap().replace(event.clone());
                        cvar.notify_one();
                        true
                    } else {
                        false
                    }
                });

                // listeners.retain_mut(|l| l.0.upgrade().is_some());

                Ok(new_ids)
            }
            _ => bail!("not a stream"),
        }
    }

    fn add_listener(
        &mut self,
        listener: Weak<(Mutex<Option<StreamEvent>>, Condvar)>,
    ) -> anyhow::Result<StreamListener> {
        match &mut self.value {
            Value::Stream(_, listeners) => {
                let listener1 = StreamListener(listener);
                listeners.push(listener1.clone());
                Ok(listener1)
            }
            _ => bail!("not a stream"),
        }
    }

    fn range(
        &self,
        from_id: &StreamEntryId,
        to_id: &StreamEntryId,
        inclusive_range: bool,
    ) -> anyhow::Result<Vec<&StreamEntry>> {
        match &self.value {
            Value::Stream(entries, _) => Ok(entries
                .iter()
                .filter(|&e| {
                    let inclusive = e.0 >= *from_id && e.0 <= *to_id;
                    let exclusive = e.0 > *from_id && e.0 < *to_id;
                    if inclusive_range {
                        inclusive
                    } else {
                        exclusive
                    }
                })
                .collect()),
            _ => bail!("not a stream"),
        }
    }

    fn last_id(&self) -> anyhow::Result<StreamEntryId> {
        match &self.value {
            Value::Stream(entries, _) => {
                Ok(entries.last().map_or(StreamEntryId::MIN, |e| e.0.clone()))
            }
            _ => bail!("not a stream"),
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
            Value::Stream(_, _) => "stream",
        }
    }
}

pub struct KVStore(HashMap<String, StoredValue>);

impl KVStore {
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
        self.0.insert(key.to_string(), StoredValue::from_string(key, value, valid_until));
    }

    pub fn insert_stream(
        &mut self,
        key: &str,
        id_pattern: &str,
        stream_data: Vec<(String, String)>,
    ) -> anyhow::Result<String> {
        if !self.0.contains_key(key) {
            self.0
                .insert(key.to_string(), StoredValue::empty_stream(key));
        }

        if let Some(value) = self.0.get_mut(key) {
            return value.add_entry(id_pattern.to_string(), stream_data);
        }

        bail!("stream not found {}", key);
    }

    pub fn range_stream(
        &self,
        key: &str,
        from_id: StreamEntryId,
        to_id: StreamEntryId,
    ) -> anyhow::Result<Vec<(String, &Vec<(String, String)>)>> {
        self.0.get(key).map_or_else(
            || bail!("stream not found {}", key),
            |value| {
                Ok(value
                    .range(&from_id, &to_id, true)?
                    .iter()
                    .map(|&entry| (entry.0.to_string(), &entry.1))
                    .collect())
            },
        )
    }

    pub fn read_stream(
        &self,
        key: &str,
        from_id: StreamEntryId,
        to_id: StreamEntryId,
    ) -> anyhow::Result<Vec<(String, &Vec<(String, String)>)>> {
        self.0.get(key).map_or_else(
            || bail!("stream not found {}", key),
            |value| {
                Ok(value
                    .range(&from_id, &to_id, false)?
                    .iter()
                    .map(|&entry| (entry.0.to_string(), &entry.1))
                    .collect())
            },
        )
    }

    pub fn latest_stream(&self, key: &str) -> anyhow::Result<StreamEntryId> {
        self.0.get(key).map_or_else(
            || bail!("stream not found {}", key),
            |value| value.last_id(),
        )
    }

    pub fn add_listener(
        &mut self,
        keys: &Vec<&String>,
        listener: Weak<(Mutex<Option<StreamEvent>>, Condvar)>,
    ) -> anyhow::Result<()> {
        let mut listeners = vec![];
        for &key in keys {
            if let Some(value) = self.0.get_mut(key) {
                listeners.push(value.add_listener(listener.clone())?);
            }
        }
        Ok(())
    }

    /**
    load rdb file into the store.
     */
    pub fn load(&mut self, mut reader: BufReader<File>) -> anyhow::Result<()> {
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
                        StoredValue::from_string(
                            &key,
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
