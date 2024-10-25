use anyhow::{bail, Result};
use codecrafters_sqlite::btree_page::{page::Page, BTree};
use codecrafters_sqlite::dbheader::DbHeader;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::str::from_utf8;

fn parse_varint(bytes: &[u8]) -> (u64, usize) {
  let mut result: u64 = 0;
  let mut shift = 0;
  let mut length = 0;

  for &byte in bytes.iter() {
    result |= ((byte & 0x7F) as u64) << shift;
    length += 1;
    if byte & 0x80 == 0 {
      break;
    }
    shift += 7;
  }

  (result, length)
}

fn main() -> Result<()> {
  // Parse arguments
  let args = std::env::args().collect::<Vec<_>>();
  match args.len() {
    0 | 1 => bail!("Missing <database path> and <command>"),
    2 => bail!("Missing <command>"),
    _ => {}
  }

  // Parse command and act accordingly
  let command = &args[2];
  match command.as_str() {
    ".tables" => {
      let mut file = File::open(&args[1])?;
      let mut header = [0; 100];
      file.read_exact(&mut header)?;
      let page_size = u16::from_be_bytes([header[16], header[17]]);
      file.seek(SeekFrom::Start(header.len() as u64))?;
      let mut first_page = vec![0; page_size as usize];
      file.read_exact(&mut first_page)?;
      // start of the cell content area
      let cell_content_area_offset = u16::from_be_bytes([first_page[5], first_page[6]]);

      // jump there
      file.seek(SeekFrom::Start(cell_content_area_offset as u64))?;

      let mut chunk = [0; 1024]; // I don't know really how long should I make this
      file.read_exact(&mut chunk)?;

      let (payload_size, mut offset) = parse_varint(&chunk);
      println!("{offset}");
      let (rowid, varint_length) = parse_varint(&chunk[offset..]);
      offset += varint_length;
      println!("{offset}");

      let mut payload = chunk[offset..(offset + payload_size as usize)].to_vec();
      offset += payload_size as usize;

      println!("{payload:?}");

      // The sequence is now [header (varint)][body (depends on the header)]
      for _ in 0..3 {
        let (serial_type, header_length) = parse_varint(&payload);
        dbg!(serial_type);

        let content_size = match serial_type {
          0 | 8 | 9 => 0,
          1 => 1,
          2 => 2,
          3 => 3,
          4 => 4,
          5 => 6,
          6 => 8,
          7 => 8,
          10 | 11 => panic!(
            "Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next."
          ),
          n if n >= 12 && n % 2 == 0 => (n - 12) / 2, // N >= 12 and even (N-12)/2 [Value is a BLOB that is (N-12)/2 bytes in length.]
          n if n >= 13 && n % 2 != 0 => (n - 13) / 2, // N >= 13 and odd  (N-13)/2 [Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.]
          _ => panic!("Unexpected serial type"),
        };

        if serial_type >= 12 && serial_type % 2 == 0 {
          println!(
            "{blob}",
            blob = from_utf8(&payload[..header_length + content_size as usize])?
          );
        }

        payload = payload[header_length + content_size as usize..].to_vec();
      }
    }
    ".dbinfo" => {
      let mut file = File::open(&args[1])?;
      let db_header = DbHeader::try_from(&mut file)?;
      let btree = BTree::new(Page::try_from_file(&mut file, 0, db_header.page_size)?);

      println!("database page size: {}", db_header.page_size);
      println!("number of tables: {}", btree.header.num_cells);
    }
    _ => bail!("Missing or invalid command passed: {}", command),
  }

  Ok(())
}
