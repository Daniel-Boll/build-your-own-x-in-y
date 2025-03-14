use anyhow::{Result, bail};
use codecrafters_sqlite::btree_page::cell::Cell;
use codecrafters_sqlite::btree_page::{BTree, page::Page};
use codecrafters_sqlite::dbheader::DbHeader;
use std::fs::File;

fn main() -> Result<()> {
  // Parse arguments
  let args = std::env::args().collect::<Vec<_>>();
  match args.len() {
    0 | 1 => bail!("Missing <database path> and <command>"),
    2 => bail!("Missing <command>"),
    _ => {}
  }

  // Parse command and act accordingly
  // let args = ["", "sample.db", ".tables"];
  let command = &args[2];
  match command.as_str() {
    ".tables" => {
      let mut file = File::open(&args[1])?;
      let db_header = DbHeader::try_from(&mut file)?;
      let btree = BTree::new(Page::try_from_file(&mut file, 0, db_header.page_size)?);

      for (i, cell) in btree.cells.iter().enumerate() {
        if i == 1 {
          continue;
        }

        if let Cell::TableLeaf { record, .. } = cell {
          print!(
            "{}{}",
            record.values[2].as_text(),
            if i < btree.cells.len() - 1 { " " } else { "\n" }
          );
        }
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
