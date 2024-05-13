use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufReader, Read};
use std::time::{Duration, SystemTime};

use anyhow::{bail, Result};

pub struct StreamEntry(String, Vec<(String, String)>);

pub enum Value {
    String(String),
    Stream(Vec<StreamEntry>),
}

pub struct StoredValue {
    value: Value,
    valid_until: Option<SystemTime>,
}

impl StoredValue {
    pub fn from_string(value: String, valid_until: Option<SystemTime>) -> Self {
        StoredValue {
            value: Value::String(value),
            valid_until,
        }
    }
    pub fn from_entry(entry_id: String, entry: Vec<(String, String)>) -> Self {
        StoredValue {
            value: Value::Stream(vec![StreamEntry(entry_id, entry)]),
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

    pub fn add_entry(&mut self, entry_id: String, entry: Vec<(String, String)>) -> Result<()> {
        match &mut self.value {
            Value::Stream(ref mut entries) => {
                entries.push(StreamEntry(entry_id, entry));
                Ok(())
            }
            _ => bail!("not a stream"),
        }
    }

    pub fn value_type(&self) -> &str {
        match &self.value {
            Value::String(_) => if self.value().is_none() { "none" } else { "string" },
            Value::Stream(_) => { "stream" }
        }
    }
}

pub struct KVStore(pub HashMap<String, StoredValue>);

impl KVStore {

    // Loading of the RDB file is based on the https://rdb.fnordig.de/file_format.html

    pub fn load(&mut self, mut reader: BufReader<File>) -> Result<()> {
        let mut header = [0x00; 9];
        reader.read_exact(&mut header)?;
        let header = String::from_utf8_lossy(&header);
        if !header.starts_with("REDIS") {
            bail!("invalid header: {}", header);
        }
        println!("header: {}", header);
        let version = header["REDIS".len()..header.len()].to_string();
        println!("version: {}", version);
        let mut timestamp = None;

        while let Ok(op) = _read_byte(&mut reader) {
            match op {
                0xFA => { // AUX fields
                    let key = read_string(&mut reader)?;
                    let value = read_string(&mut reader)?;
                    println!("aux: {} {}", key, value);
                }
                0xFE => { // Database selector
                    let db_number = match read_length(&mut reader)? {
                        LengthEncoding::Len(len) => len,
                        LengthEncoding::Short(len) => len as u32,
                        LengthEncoding::Byte(len) => len as u32,
                        LengthEncoding::Int(len) => len,
                    };
                    println!("database selector {}", db_number);
                }
                0xFB => { // resize db field
                    let hash_size = read_int(&mut reader)?;
                    let expire_size = read_int(&mut reader)?;
                    println!("sizes {} {}", hash_size, expire_size);
                }
                0xFD => { // The following expire value is specified in seconds. The following 4 bytes represent the Unix timestamp as an unsigned integer.
                    timestamp = Some((_read_u32(&mut reader)? as u64) * 1000);
                    print!("timestamp {:?}", timestamp);
                }
                0xFC => { // The following expire value is specified in milliseconds. The following 8 bytes represent the Unix timestamp as an unsigned long.
                    timestamp = Some(_read_u64(&mut reader)?);
                    print!("timestamp {:?}", timestamp);
                }
                0xFF => {
                    // rdb load finished
                    read_crc64(&mut reader)?;
                    return Ok(());
                }
                0..=14 => {
                    println!("reading value type {} with timestamp {:?}", op, timestamp);
                    let key = read_string(&mut reader)?;
                    let value = read_string(&mut reader)?;
                    self.0.insert(key, StoredValue::from_string(value, timestamp.map(|epoc_ms| SystemTime::UNIX_EPOCH + Duration::from_millis(epoc_ms))));
                    // reset
                    timestamp = None;
                }

                _ => {
                    bail!("invalid rdb op: {}", op);
                }
            }
        }


        Ok(())
    }
}

enum LengthEncoding {
    Len(u32),
    Byte(u8),
    Short(u16),
    Int(u32),

}

fn read_length(reader: &mut BufReader<File>) -> Result<LengthEncoding> {
    let head = _read_byte(reader)?;
    // This is how length encoding works : Read one byte from the stream, compare the two most significant bits:
    let bits = head & 0b11000000;
    match bits {
        0b00000000 => Ok(LengthEncoding::Len(head as u32)),
        0b01000000 => {
            let first6bits = head & 0b00111111;
            let second = _read_byte(reader)?;
            Ok(LengthEncoding::Len(u16::from_le_bytes([first6bits, second]) as u32))
        }
        0b10000000 => {
            let mut buf = [0; 4];
            reader.read_exact(&mut buf)?;
            Ok(LengthEncoding::Len(u32::from_le_bytes(buf)))
        }
        0b11000000 => {
            let first6bits = head & 0b00111111;
            match first6bits {
                0 => Ok(LengthEncoding::Byte(_read_byte(reader)?)),
                1 => {
                    let mut buf = [0; 2];
                    reader.read_exact(&mut buf)?;
                    Ok(LengthEncoding::Short(u16::from_le_bytes(buf)))
                }
                2 => {
                    Ok(LengthEncoding::Int(_read_u32(reader)?))
                }
                3 => {
                    bail!("compressed encoding follows"); // TODO
                }
                _ => {
                    bail!("unknown encoding: {}", head);
                }
            }
        }
        _ => {
            bail!("invalid length encoding: {}", head);
        }
    }
}

fn read_crc64(reader: &mut BufReader<File>) -> Result<[u8; 8]> {
    let mut buf = [0; 8];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_int(reader: &mut BufReader<File>) -> Result<i32> {
    Ok(match read_length(reader)? {
        LengthEncoding::Len(len) => len as i32,
        LengthEncoding::Short(len) => len as i32,
        LengthEncoding::Byte(len) => len as i32,
        LengthEncoding::Int(len) => len as i32,
    })
}

fn read_string(reader: &mut BufReader<File>) -> Result<String> {
    match read_length(reader)? {
        LengthEncoding::Len(len) => {
            let mut buf = vec![0; len as usize];
            reader.read_exact(&mut buf)?;
            Ok(String::from_utf8(buf)?)
        }
        LengthEncoding::Byte(value) => {
            Ok(value.to_string())
        }
        LengthEncoding::Int(value) => {
            Ok(value.to_string())
        }
        LengthEncoding::Short(value) => {
            Ok(value.to_string())
        }
    }
}

fn _read_byte(reader: &mut BufReader<File>) -> Result<u8> {
    let mut buf = [0; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn _read_u32(reader: &mut BufReader<File>) -> Result<u32> {
    let mut buf = [0; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn _read_u64(reader: &mut BufReader<File>) -> Result<u64> {
    let mut buf = [0; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

const RDB_EMPTY_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";


fn hex_to_bytes(hex: &str) -> Vec<u8> {
    // hex string is of 2 chars per byte
    assert_eq!(hex.len() % 2, 0);
    let mut result = vec![];
    let mut iter = hex.chars();
    while let Some(c1) = iter.next() {
        let c2 = iter.next().unwrap();
        let byte = u8::from_str_radix(&format!("{}{}", c1, c2), 16).unwrap();
        result.push(byte);
    }
    result
}

pub fn empty_rdb() -> Vec<u8> {
    hex_to_bytes(RDB_EMPTY_HEX)
}
