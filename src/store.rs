use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, Condvar, Mutex};
use std::time::SystemTime;

use anyhow::bail;

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
            _ => Ok(Self(s.parse()?, 0))
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
                    let time_id = now.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
                    Self(time_id, 0)
                }
                Some(StreamEntryId(last_time_id, last_seq_id)) => {
                    Self(*last_time_id, last_seq_id + 1)
                }
            })
        } else {
            match pattern.split('-').collect::<Vec<&str>>().as_slice() {
                [time_id, "*"] =>
                    {
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
                _ => bail!("invalid id")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct StreamEntry(StreamEntryId, Vec<(String, String)>);

#[derive(Clone, Debug)]
pub struct StreamEvent(pub(crate) String, pub(crate) StreamEntryId);

#[derive(Clone, Debug)]
pub struct StreamListener(Arc<(Mutex<Option<StreamEvent>>, Condvar)>);

impl StreamListener {}

enum Value {
    String(String),
    Stream(Vec<StreamEntry>, Vec<StreamListener>),
}

pub struct StoredValue {
    value: Value,
    valid_until: Option<SystemTime>,
    key: String,
}

impl StoredValue {
    pub fn from_string(key: &str, value: &str, valid_until: Option<SystemTime>) -> Self {
        StoredValue {
            key: key.to_string(),
            value: Value::String(value.to_string()),
            valid_until,
        }
    }
    pub fn empty_stream(key: &str) -> Self {
        StoredValue {
            key: key.to_string(),
            value: Value::Stream(Vec::new(), Vec::new()),
            valid_until: None,
        }
    }

    pub fn value(&self) -> Option<String> {
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

    pub fn add_entry(&mut self, id_pattern: String, entry: Vec<(String, String)>) -> anyhow::Result<String> {
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

                for l in listeners {
                    let (lock, cvar) = l.0.deref();
                    lock.lock().unwrap().replace(event.clone());
                    cvar.notify_one();
                }


                Ok(new_ids)
            }
            _ => bail!("not a stream"),
        }
    }

    fn add_listener(&mut self, listener: Arc<(Mutex<Option<StreamEvent>>, Condvar)>) -> anyhow::Result<StreamListener> {
        match &mut self.value {
            Value::Stream(_, listeners) => {
                let listener1 = StreamListener(listener);
                listeners.push(listener1.clone());
                Ok(listener1)
            }
            _ => bail!("not a stream"),
        }
    }

    pub fn range(&self, from_id: &StreamEntryId, to_id: &StreamEntryId, inclusive_range: bool) -> anyhow::Result<Vec<&StreamEntry>> {
        match &self.value {
            Value::Stream(entries, _) => {
                Ok(entries
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
                    .collect()
                )
            }
            _ => bail!("not a stream"),
        }
    }

    pub fn last_id(&self) -> anyhow::Result<StreamEntryId> {
        match &self.value {
            Value::Stream(entries, _) => {
                Ok(entries
                    .last().map_or(StreamEntryId::MIN, |e| e.0.clone())
                )
            }
            _ => bail!("not a stream"),
        }
    }

    pub fn value_type(&self) -> &str {
        match &self.value {
            Value::String(_) => if self.value().is_none() { "none" } else { "string" },
            Value::Stream(_, _) => { "stream" }
        }
    }
}

pub struct KVStore(pub HashMap<String, StoredValue>);

impl KVStore {
    pub fn insert_stream(&mut self, key: &str, id_pattern: &str, stream_data: Vec<(String, String)>) -> anyhow::Result<String> {
        if !self.0.contains_key(key) {
            self.0.insert(key.to_string(), StoredValue::empty_stream(key));
        }

        if let Some(value) = self.0.get_mut(key) {
            return value.add_entry(id_pattern.to_string(), stream_data);
        }

        bail!("stream not found {}", key);
    }

    pub fn range_stream(&self, key: &str, from_id: StreamEntryId, to_id: StreamEntryId) -> anyhow::Result<Vec<(String, &Vec<(String, String)>)>> {
        self.0.get(key).map_or_else(|| bail!("stream not found {}", key), |value| {
            Ok(value
                .range(&from_id, &to_id, true)?
                .iter()
                .map(|&entry| (entry.0.to_string(), &entry.1))
                .collect())
        })
    }

    pub fn read_stream(&self, key: &str, from_id: StreamEntryId, to_id: StreamEntryId) -> anyhow::Result<Vec<(String, &Vec<(String, String)>)>> {
        self.0.get(key).map_or_else(|| bail!("stream not found {}", key), |value| {
            Ok(value
                .range(&from_id, &to_id, false)?
                .iter()
                .map(|&entry| (entry.0.to_string(), &entry.1))
                .collect())
        })
    }

    pub fn latest_stream(&self, key: &str) -> anyhow::Result<StreamEntryId> {
        self.0.get(key).map_or_else(|| bail!("stream not found {}", key), |value| value.last_id())
    }

    pub fn add_listener(&mut self, keys: &Vec<&String>, listener: Arc<(Mutex<Option<StreamEvent>>, Condvar)>) -> anyhow::Result<Vec<StreamListener>> {
        let mut listeners = vec![];
        for &key in keys {
            if let Some(value) = self.0.get_mut(key) {
                listeners.push(value.add_listener(listener.clone())?);
            }
        }
        Ok(listeners)
    }
}
