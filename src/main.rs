use anyhow::{bail, Result};
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

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
    ".dbinfo" => {
      let mut file = File::open(&args[1])?;
      let mut header = [0; 100];
      file.read_exact(&mut header)?;

      // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
      let page_size = u16::from_be_bytes([header[16], header[17]]);

      // 1. The sqlite_schema page is always page 1, and it always begins at offset 0. The file header is a part of the page.
      // 2. The sqlite_schema page stores the rows of the sqlite_schema table in chunks of data called "cells." Each cell stores a single row.

      // Done with the header, now move
      file.seek(SeekFrom::Start(header.len() as u64))?;
      let mut first_page = vec![0; page_size as usize];
      file.read_exact(&mut first_page)?;

      // The [B-tree Page Header Format] number of cells on the page is at the 3rd byte offset, using 2 bytes in big-endian order
      let number_of_cells = u16::from_be_bytes([first_page[3], first_page[4]]);

      println!("database page size: {}", page_size);
      println!("number of tables: {}", number_of_cells);
    }
    _ => bail!("Missing or invalid command passed: {}", command),
  }

  Ok(())
}
