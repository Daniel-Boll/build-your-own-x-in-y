use std::{
  fs::File,
  io::{Read, Seek, SeekFrom},
  ops::Range,
};

use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Page {
  pub data: Vec<u8>,
  pub offset: usize,
}

impl Page {
  pub fn try_from_file(file: &mut File, page_number: u32, page_size: u16) -> Result<Self> {
    let mut data = vec![0; page_size as usize];
    let offset: usize = 100 + (page_number * page_size as u32) as usize;
    file.seek(SeekFrom::Start(offset as u64))?;
    file.read_exact(&mut data)?;
    Ok(Self { data, offset })
  }

  fn at(&self, offset: usize) -> u8 {
    self.data[offset]
  }

  fn slice(&self, offset_range: Range<usize>) -> &[u8] {
    &self.data[offset_range]
  }

  pub fn read_u8(&self, offset: usize) -> u8 {
    self.at(offset)
  }

  pub fn read_u16(&self, offset: usize) -> u16 {
    u16::from_be_bytes([self.at(offset), self.at(offset + 1)])
  }

  pub fn read_u32(&self, offset: usize) -> u32 {
    u32::from_be_bytes([
      self.at(offset),
      self.at(offset + 1),
      self.at(offset + 2),
      self.at(offset + 3),
    ])
  }

  pub fn read_bytes(&self, offset: usize, size: usize) -> Vec<u8> {
    self.slice(offset..offset + size).to_vec()
  }

  pub fn read_varint(&self, offset: usize) -> (u64, usize) {
    let mut value = 0u64;
    let mut shift = 0;
    let mut size = 0;

    for i in 0..9 {
      let byte = self.at(offset + i);
      size += 1;

      if i == 8 {
        value |= (byte as u64) << shift;
        break;
      } else {
        value |= ((byte & 0x7F) as u64) << shift;
        if (byte & 0x80) == 0 {
          break;
        }
      }
      shift += 7;
    }

    (value, size)
  }
}
