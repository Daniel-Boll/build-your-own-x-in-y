use crate::btree_page::BTree;
use crate::btree_page::cell::Cell;
use crate::btree_page::page::Page;
use crate::btree_page::schema_layer::Record;
use crate::dbheader::DbHeader;
use std::fs::File;

pub mod btree_page;
pub mod dbheader;
pub mod varint;

pub struct SQLite {
  file: File,
  db_header: DbHeader,
}

impl SQLite {
  pub fn open(path: &str) -> anyhow::Result<Self> {
    let mut file = File::open(path)?;
    let db_header = DbHeader::try_from(&mut file)?;
    Ok(SQLite { file, db_header })
  }

  pub fn load_page(&mut self, page_num: u32) -> anyhow::Result<Page> {
    Page::try_from_file(&mut self.file, page_num, self.db_header.page_size)
      .map_err(|e| anyhow::anyhow!("Failed to load page {}: {}", page_num, e))
  }

  pub fn btree_from_page(&mut self, page_num: u32) -> anyhow::Result<BTree> {
    let page = self.load_page(page_num)?;
    Ok(BTree::new(page))
  }

  // Assemble full payload from a cell, following overflow pages
  pub fn get_full_payload(&mut self, cell: &Cell) -> anyhow::Result<Vec<u8>> {
    match cell {
      Cell::TableLeaf {
        payload,
        overflow_page,
        payload_size,
        ..
      } => {
        let mut full_payload = payload.clone();
        let mut current_overflow = *overflow_page;
        let total_size = *payload_size as usize;

        while let Some(page_num) = current_overflow {
          let overflow_page = self.load_page(page_num)?;
          let remaining_size = total_size - full_payload.len();
          let data_size = std::cmp::min(remaining_size - 4, overflow_page.data.len() - 4);
          full_payload.extend_from_slice(&overflow_page.read_bytes(0, data_size));
          current_overflow = if remaining_size > data_size + 4 {
            Some(overflow_page.read_u32(data_size))
          } else {
            None
          };
        }
        Ok(full_payload)
      }
      // Add cases for IndexLeaf, IndexInterior if needed
      _ => Ok(cell.payload().to_vec()),
    }
  }

  pub fn list_tables(&mut self) -> anyhow::Result<()> {
    let btree = self.btree_from_page(1)?;
    for (i, cell) in btree.cells.iter().enumerate() {
      if let Cell::TableLeaf { .. } = cell {
        let full_payload = self.get_full_payload(cell)?;
        let record = Record::parse(&full_payload)?;
        if record.values[0].as_text() == "table" && record.values[2].as_text() != "sqlite_sequence"
        {
          print!(
            "{}{}",
            record.values[2].as_text(),
            if i < btree.cells.len() - 1 { " " } else { "" }
          );
        }
      }
    }
    println!();
    Ok(())
  }

  pub fn print_db_info(&mut self) -> anyhow::Result<()> {
    let btree = self.btree_from_page(1)?;
    println!("database page size: {}", self.db_header.page_size);
    println!("number of tables: {}", btree.header.num_cells);
    Ok(())
  }

  pub fn count_table_rows(&mut self, query: &str) -> anyhow::Result<usize> {
    let table_name = query
      .split_whitespace()
      .last()
      .ok_or_else(|| anyhow::anyhow!("Invalid query: no table name found"))?;

    // Load sqlite_schema (page 1) to find the table's root page
    let schema_btree = self.btree_from_page(1)?;
    let mut rootpage = None;
    for cell in &schema_btree.cells {
      if let Cell::TableLeaf { .. } = cell {
        let record = self.record_from_cell(cell)?;
        if record.values[2].as_text() == table_name {
          rootpage = Some(record.values[3].as_integer() as u32);
          break;
        }
      }
    }

    let root_page = rootpage.ok_or_else(|| anyhow::anyhow!("Table not found: {}", table_name))?;

    // Count rows by traversing the B-Tree
    let total_rows = self.count_rows_in_btree(root_page)?;
    // println!("Total rows counted: {}", total_rows);
    Ok(total_rows)
  }

  fn record_from_cell(&mut self, cell: &Cell) -> anyhow::Result<Record> {
    let full_payload = self.get_full_payload(cell)?;
    Record::parse(&full_payload)
  }

  fn count_rows_in_btree(&mut self, page_num: u32) -> anyhow::Result<usize> {
    let btree = self.btree_from_page(page_num)?;
    // println!(
    //   "Page {} type: 0x{:02X}, num_cells: {}",
    //   page_num, btree.header.page_type, btree.header.num_cells
    // );

    match btree.header.page_type {
      0x0D => {
        // Leaf page: count TableLeaf cells
        let leaf_count = btree
          .cells
          .iter()
          .filter(|cell| matches!(cell, Cell::TableLeaf { .. }))
          .count();
        // println!("Leaf page {} has {} rows", page_num, leaf_count);
        Ok(leaf_count)
      }
      0x05 => {
        // Interior page: recursively count rows in all child pages
        let mut total = 0;
        for cell in &btree.cells {
          if let Cell::TableInterior {
            left_child_page, ..
          } = cell
          {
            total += self.count_rows_in_btree(*left_child_page)?;
          }
        }
        Ok(total)
      }
      _ => anyhow::bail!(
        "Unexpected page type 0x{:02X} for page {}",
        btree.header.page_type,
        page_num
      ),
    }
  }
}
