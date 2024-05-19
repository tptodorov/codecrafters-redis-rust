use std::cmp::Ordering;
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Condvar, Mutex, Weak};
use std::time::SystemTime;

use anyhow::bail;

#[derive(Clone, Debug)]
pub(crate) struct StreamRecordId(u64, u64);

impl PartialEq<Self> for StreamRecordId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl PartialOrd for StreamRecordId {
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

impl FromStr for StreamRecordId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self, Self::Err> {
        let parts = s.split('-').collect::<Vec<&str>>();
        match parts[..] {
            [first, second] => Ok(Self(first.parse()?, second.parse()?)),
            _ => Ok(Self(s.parse()?, 0)),
        }
    }
}

impl Display for StreamRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.0, self.1)
    }
}

impl StreamRecordId {
    pub(crate) const MIN: Self = Self(0, 0);
    pub(crate) const MAX: Self = Self(u64::MAX, u64::MAX);

    pub fn new(time_id: u64, seq_id: u64) -> Self {
        Self(time_id, seq_id)
    }

    pub fn from_pattern(pattern: String, last_id: Option<&StreamRecordId>) -> anyhow::Result<Self> {
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
                Some(StreamRecordId(last_time_id, last_seq_id)) => {
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
                        Some(StreamRecordId(last_time_id, last_seq_id)) => {
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
pub struct StreamRecord {
    pub(crate) id: StreamRecordId,
    pub(crate) attributes: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub(crate) struct StreamEvent {
    pub(crate) key: String,
    pub(crate) id: StreamRecordId,
}

#[derive(Clone, Debug)]
pub struct StreamListener(Weak<(Mutex<Option<StreamEvent>>, Condvar)>);

pub struct Stream(String, Vec<StreamRecord>, Vec<StreamListener>);

impl Stream {
    pub fn new(key: String) -> Self {
        Stream(key.to_string(), Vec::new(), Vec::new())
    }

    pub(crate) fn add_entry(
        &mut self,
        id_pattern: String,
        entry: Vec<(String, String)>,
    ) -> anyhow::Result<String> {
        let entries = &mut self.1;
        let listeners = &mut self.2;
        // new id is either explicit or pattern
        let new_id: StreamRecordId = if id_pattern.contains('*') {
            StreamRecordId::from_pattern(id_pattern, entries.last().map(|e| &e.id))?
        } else {
            id_pattern.parse()?
        };

        if new_id <= StreamRecordId(0, 0) {
            bail!("ERR The ID specified in XADD must be greater than 0-0");
        }

        if let Some(last) = entries.last() {
            let last_id = &last.id;
            if &new_id <= last_id {
                bail!("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
        let new_ids = new_id.to_string();
        let stream_entry = StreamRecord {
            id: new_id.clone(),
            attributes: entry,
        };
        entries.push(stream_entry.clone());

        let event = StreamEvent {
            key: self.0.clone(),
            id: new_id,
        };

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

        Ok(new_ids)
    }

    pub(crate) fn add_listener(
        &mut self,
        listener: Weak<(Mutex<Option<StreamEvent>>, Condvar)>,
    ) -> anyhow::Result<StreamListener> {
        let listener1 = StreamListener(listener);
        self.2.push(listener1.clone());
        Ok(listener1)
    }

    pub(crate) fn range(
        &self,
        from_id: &StreamRecordId,
        to_id: &StreamRecordId,
        inclusive_range: bool,
    ) -> anyhow::Result<Vec<&StreamRecord>> {
        Ok(self.1
            .iter()
            .filter(|&e| {
                let inclusive = e.id >= *from_id && e.id <= *to_id;
                let exclusive = e.id > *from_id && e.id < *to_id;
                if inclusive_range {
                    inclusive
                } else {
                    exclusive
                }
            })
            .collect())
    }

    pub(crate) fn last_id(&self) -> anyhow::Result<StreamRecordId> {
        Ok(self.1.last().map_or(StreamRecordId::MIN, |e| e.id.clone()))
    }
}
