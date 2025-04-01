use anyhow::{Result, anyhow};
use std::{
  convert::TryInto,
  fmt::{self, Display, Formatter},
};

/// # [Record Format](https://www.sqlite.org/fileformat.html#record-format)
///
/// A record contains a header and a body, in that order. The header begins with a single varint which determines the total number of bytes in the header.
/// The varint value is the size of the header in bytes including the size varint itself.
/// Following the size varint are one or more additional varints, one per column.
/// These additional varints are called "serial type" numbers and determine the datatype of each column, according to the following chart:
/// +-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
/// |Serial Type  |Content Size|Meaning                                                                                                                                                                                                                                                                                                     |
/// +-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
/// |0            |0           |Value is a NULL.                                                                                                                                                                                                                                                                                            |
/// |1            |1           |Value is an 8-bit twos-complement integer.                                                                                                                                                                                                                                                                  |
/// |2            |2           |Value is a big-endian 16-bit twos-complement integer.                                                                                                                                                                                                                                                       |
/// |3            |3           |Value is a big-endian 24-bit twos-complement integer.                                                                                                                                                                                                                                                       |
/// |4            |4           |Value is a big-endian 32-bit twos-complement integer.                                                                                                                                                                                                                                                       |
/// |5            |6           |Value is a big-endian 48-bit twos-complement integer.                                                                                                                                                                                                                                                       |
/// |6            |8           |Value is a big-endian 64-bit twos-complement integer.                                                                                                                                                                                                                                                       |
/// |7            |8           |Value is a big-endian IEEE 754-2008 64-bit floating point number.                                                                                                                                                                                                                                           |
/// |8            |0           |Value is the integer 0. (Only available for schema format 4 and higher.)                                                                                                                                                                                                                                    |
/// |9            |0           |Value is the integer 1. (Only available for schema format 4 and higher.)                                                                                                                                                                                                                                    |
/// |10           |11          |variable Reserved for internal use. These serial type codes will never appear in a well-formed database file but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.|
/// |N≥12 and even|(N-12)/2    |Value is a BLOB that is (N-12)/2 bytes in length.                                                                                                                                                                                                                                                           |
/// |N≥13 and odd |(N-13)/2    |Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.                                                                                                                                                                                                      |
/// +-------------+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
#[derive(Debug, Clone)]
pub struct Record {
  pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub enum Value {
  Null,
  Integer(i64),
  Float(f64),
  Blob(Vec<u8>),
  Text(String),
}

impl Value {
  pub fn as_integer(&self) -> i64 {
    match self {
      Value::Integer(value) => *value,
      _ => panic!("Value is not an integer"),
    }
  }

  pub fn as_float(&self) -> f64 {
    match self {
      Value::Float(value) => *value,
      _ => panic!("Value is not a float"),
    }
  }

  pub fn as_blob(&self) -> &[u8] {
    match self {
      Value::Blob(value) => value,
      _ => panic!("Value is not a blob"),
    }
  }

  pub fn as_text(&self) -> &str {
    match self {
      Value::Text(value) => value,
      _ => panic!("Value is not a text"),
    }
  }
}

impl Display for Value {
  fn fmt(&self, f: &mut Formatter) -> fmt::Result {
    match self {
      Value::Null => write!(f, "NULL"),
      Value::Integer(value) => write!(f, "{}", value),
      Value::Float(value) => write!(f, "{}", value),
      Value::Blob(value) => write!(f, "{:?}", value),
      Value::Text(value) => write!(f, "{}", value),
    }
  }
}

impl Record {
  pub fn parse(data: &[u8]) -> Result<Self> {
    let (header_size, header_size_len) = Self::parse_varint(data)?;
    let mut offset = header_size_len;

    let mut serial_types = Vec::new();
    while offset < header_size {
      let (serial_type, serial_type_len) = Self::parse_varint(&data[offset..])?;
      serial_types.push(serial_type);
      offset += serial_type_len;
    }

    let mut values = Vec::new();
    for &serial_type in &serial_types {
      let (value, value_len) = Self::parse_value(serial_type, &data[offset..])?;
      values.push(value);
      offset += value_len;
    }

    Ok(Record { values })
  }

  fn parse_varint(data: &[u8]) -> Result<(usize, usize)> {
    let mut value = 0usize;
    let mut length = 0;

    for &byte in data.iter() {
      value = (value << 7) | (byte & 0x7F) as usize;
      length += 1;
      if byte & 0x80 == 0 {
        return Ok((value, length));
      }
    }

    Err(anyhow!("Invalid varint"))
  }

  fn parse_value(serial_type: usize, data: &[u8]) -> Result<(Value, usize)> {
    match serial_type {
      0 => Ok((Value::Null, 0)),
      1 => Ok((Value::Integer(data[0] as i64), 1)),
      2 => {
        let value = i16::from_be_bytes(data[..2].try_into()?);
        Ok((Value::Integer(value as i64), 2))
      }
      3 => {
        let value = ((data[0] as i32) << 16) | ((data[1] as i32) << 8) | (data[2] as i32);
        Ok((Value::Integer(value as i64), 3))
      }
      4 => {
        let value = i32::from_be_bytes(data[..4].try_into()?);
        Ok((Value::Integer(value as i64), 4))
      }
      5 => {
        let value = ((data[0] as i64) << 40)
          | ((data[1] as i64) << 32)
          | ((data[2] as i64) << 24)
          | ((data[3] as i64) << 16)
          | ((data[4] as i64) << 8)
          | (data[5] as i64);
        Ok((Value::Integer(value), 6))
      }
      6 => {
        let value = i64::from_be_bytes(data[..8].try_into()?);
        Ok((Value::Integer(value), 8))
      }
      7 => {
        let value = f64::from_be_bytes(data[..8].try_into()?);
        Ok((Value::Float(value), 8))
      }
      8 => Ok((Value::Integer(0), 0)),
      9 => Ok((Value::Integer(1), 0)),
      n if n >= 12 && n % 2 == 0 => {
        let size = (n - 12) / 2;
        let value = data[..size].to_vec();
        Ok((Value::Blob(value), size))
      }
      n if n >= 13 && n % 2 == 1 => {
        let size = (n - 13) / 2;
        let value = String::from_utf8(data[..size].to_vec())?;
        Ok((Value::Text(value), size))
      }
      _ => Err(anyhow!("Unknown serial type")),
    }
  }
}
