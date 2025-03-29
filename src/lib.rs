use itertools::Itertools;
use parser::schema;
use parser::select::{Column, SelectStatement};
use tracing::{debug, trace};

use crate::btree_page::BTree;
use crate::btree_page::cell::Cell;
use crate::btree_page::page::Page;
use crate::btree_page::schema_layer::Record;
use crate::dbheader::DbHeader;
use std::collections::HashMap;
use std::fs::File;

pub mod btree_page;
pub mod dbheader;
pub mod parser;

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

  fn get_table_schema(
    &mut self,
    table_name: &str,
  ) -> anyhow::Result<(HashMap<String, usize>, Option<String>)> {
    let schema_btree = self.btree_from_page(1)?;
    let table_name_upper = table_name.to_uppercase();

    for cell in &schema_btree.cells {
      if let Cell::TableLeaf { .. } = cell {
        let record = self.record_from_cell(cell)?;
        if record.values[2].as_text().to_uppercase() == table_name_upper
          && record.values[0].as_text() == "table"
        {
          let sql = record.values[4].as_text();
          trace!("Parsing schema SQL: {}", sql);
          let schema_stmt =
            schema::parse(sql).map_err(|e| anyhow::anyhow!("Invalid schema: {}", e))?;
          let (column_map, rowid_alias) = schema_stmt.to_column_map();
          debug!(
            "Schema for {}: column_map={:?}, rowid_alias={:?}",
            table_name, column_map, rowid_alias
          );
          return Ok((column_map, rowid_alias));
        }
      }
    }

    anyhow::bail!("Table schema not found for: {}", table_name)
  }

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

  pub fn select_columns(&mut self, stmt: &SelectStatement) -> anyhow::Result<()> {
    let schema_btree = self.btree_from_page(1)?;
    let mut rootpage = None;
    let from_upper = stmt.from.to_uppercase();
    for cell in &schema_btree.cells {
      if let Cell::TableLeaf { .. } = cell {
        let record = self.record_from_cell(cell)?;
        if record.values[2].as_text().to_uppercase() == from_upper {
          rootpage = Some(record.values[3].as_integer() as u32);
          break;
        }
      }
    }

    let root_page = rootpage.ok_or_else(|| anyhow::anyhow!("Table not found: {}", stmt.from))?;
    let btree = self.btree_from_page(root_page)?;
    let (column_map, rowid_alias) = self.get_table_schema(&stmt.from)?;

    match stmt.columns.as_slice() {
      [Column::Count] => {
        let count = self.count_rows_in_btree(root_page)?;
        println!("{count}");
        return Ok(());
      }
      cols if cols.iter().any(|c| matches!(c, Column::Count)) => {
        anyhow::bail!("COUNT(*) must be the only column in the query");
      }
      cols => {
        let has_all = cols.iter().any(|c| matches!(c, Column::All));
        let column_positions: Vec<(String, usize)> = if has_all {
          if cols.len() > 1 {
            anyhow::bail!("SELECT * cannot be combined with other columns");
          }
          // For SELECT *, include rowid_alias if present, then payload columns
          let mut positions = Vec::new();
          if let Some(alias) = &rowid_alias {
            positions.push((alias.clone(), usize::MAX));
          }
          positions.extend(column_map.into_iter().sorted_by_key(|(_, pos)| *pos));
          positions
        } else {
          cols
            .iter()
            .map(|col| match col {
              Column::Named(name) => {
                let name_upper = name.to_uppercase();
                if let Some(alias) = &rowid_alias {
                  if name_upper == alias.to_uppercase() || name_upper == "ROWID" {
                    return Ok((name.clone(), usize::MAX));
                  }
                }
                column_map
                  .iter()
                  .find(|(k, _)| k.to_uppercase() == name_upper)
                  .map(|(k, &pos)| (k.clone(), pos))
                  .ok_or_else(|| anyhow::anyhow!("Unknown column: {}", name))
              }
              _ => unreachable!("Count and All handled above"),
            })
            .collect::<anyhow::Result<Vec<_>>>()?
        };

        debug!("Column positions: {:?}", column_positions);
        self.print_rows(&btree, &column_positions, &rowid_alias)?;
      }
    }

    Ok(())
  }

  fn print_rows(
    &mut self,
    btree: &BTree,
    column_positions: &[(String, usize)],
    _rowid_alias: &Option<String>,
  ) -> anyhow::Result<()> {
    for cell in &btree.cells {
      if let Cell::TableLeaf { row_id, .. } = cell {
        let record = self.record_from_cell(cell)?;
        trace!(
          "Row ID: {}, Record values: {:?}",
          row_id,
          record
            .values
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
        );
        let mut row = Vec::new();

        for (name, pos) in column_positions {
          trace!("Processing column: {} at position: {}", name, pos);
          if *pos == usize::MAX {
            row.push(row_id.to_string());
          } else if *pos < record.values.len() {
            row.push(record.values[*pos + 1].to_string());
          } else {
            row.push("NULL".to_string());
          }
        }
        trace!("Row output: {:?}", row);
        println!("{}", row.join("|"));
      }
    }
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
    let total_rows = self.count_rows_in_btree(root_page)?;
    Ok(total_rows)
  }

  fn record_from_cell(&mut self, cell: &Cell) -> anyhow::Result<Record> {
    let full_payload = self.get_full_payload(cell)?;
    Record::parse(&full_payload)
  }

  fn count_rows_in_btree(&mut self, page_num: u32) -> anyhow::Result<usize> {
    let btree = self.btree_from_page(page_num)?;
    match btree.header.page_type {
      0x0D => {
        let leaf_count = btree
          .cells
          .iter()
          .filter(|cell| matches!(cell, Cell::TableLeaf { .. }))
          .count();
        Ok(leaf_count)
      }
      0x05 => {
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
