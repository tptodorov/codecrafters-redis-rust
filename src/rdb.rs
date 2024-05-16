use std::fmt::Display;
use std::fs::File;
use std::io::{BufReader, Read};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Result};

use crate::rdb;
use crate::store::{KVStore, StoredValue};

/** Empty Encoded RDB */
pub fn empty_rdb() -> Vec<u8> {
    hex_to_bytes(RDB_EMPTY_HEX)
}

// Loading of the RDB file is based on the https://rdb.fnordig.de/file_format.html
impl KVStore {
    /** Load a RDB file into this store */
    pub fn load(&mut self, mut reader: BufReader<File>) -> anyhow::Result<()> {
        let mut header = [0x00; 9];
        reader.read_exact(&mut header)?;
        let header = String::from_utf8_lossy(&header);
        if !header.starts_with("REDIS") {
            bail!("invalid header: {}", header);
        }

        let version = header["REDIS".len()..header.len()].to_string();
        println!("rdb version: {}", version);
        let mut timestamp = None;

        while let Ok(op) = rdb::read_byte(&mut reader) {
            match op {
                0xFA => { // AUX fields
                    let key = rdb::read_string(&mut reader)?;
                    let value = rdb::read_string(&mut reader)?;
                    println!("aux: {} {}", key, value);
                    // TODO
                }
                0xFE => { // Database selector
                    let db_number = match rdb::read_length(&mut reader)? {
                        LengthEncoding::Len(len) => len,
                        LengthEncoding::Short(len) => len as u32,
                        LengthEncoding::Byte(len) => len as u32,
                        LengthEncoding::Int(len) => len,
                    };
                    println!("database selector {}", db_number);
                    // TODO
                }
                0xFB => { // resize db field
                    let hash_size = rdb::read_int(&mut reader)?;
                    let expire_size = rdb::read_int(&mut reader)?;
                    println!("sizes {} {}", hash_size, expire_size);
                    // TODO
                }
                0xFD => { // The following expire value is specified in seconds. The following 4 bytes represent the Unix timestamp as an unsigned integer.
                    timestamp = Some((rdb::read_u32(&mut reader)? as u64) * 1000);
                }
                0xFC => { // The following expire value is specified in milliseconds. The following 8 bytes represent the Unix timestamp as an unsigned long.
                    timestamp = Some(rdb::read_u64(&mut reader)?);
                }
                0xFF => {
                    // rdb load finished
                    rdb::read_crc64(&mut reader)?;
                    return Ok(());
                }
                0..=14 => {
                    let key = rdb::read_string(&mut reader)?;
                    let value = rdb::read_string(&mut reader)?;
                    self.0.insert(key.clone(), StoredValue::from_string(&key, &value, timestamp.map(|epoc_ms| SystemTime::UNIX_EPOCH + Duration::from_millis(epoc_ms))));
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
    let head = read_byte(reader)?;
    // This is how length encoding works : Read one byte from the stream, compare the two most significant bits:
    let bits = head & 0b11000000;
    match bits {
        0b00000000 => Ok(LengthEncoding::Len(head as u32)),
        0b01000000 => {
            let first6bits = head & 0b00111111;
            let second = read_byte(reader)?;
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
                0 => Ok(LengthEncoding::Byte(read_byte(reader)?)),
                1 => {
                    let mut buf = [0; 2];
                    reader.read_exact(&mut buf)?;
                    Ok(LengthEncoding::Short(u16::from_le_bytes(buf)))
                }
                2 => {
                    Ok(LengthEncoding::Int(read_u32(reader)?))
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

fn read_byte(reader: &mut BufReader<File>) -> Result<u8> {
    let mut buf = [0; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u32(reader: &mut BufReader<File>) -> Result<u32> {
    let mut buf = [0; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(reader: &mut BufReader<File>) -> Result<u64> {
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
