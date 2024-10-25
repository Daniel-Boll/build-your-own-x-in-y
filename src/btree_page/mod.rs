pub mod cell;
pub mod page;
pub mod schema_layer;

use cell::Cell;
use page::Page;

/// The b-tree algorithm provides key/data storage with unique and ordered keys on page-oriented storage devices.
/// For background information on b-trees, see Knuth, The Art Of Computer Programming, Volume 3 "Sorting and Searching", pages 471-479.
/// Two variants of b-trees are used by SQLite. "Table b-trees" use a 64-bit signed integer key and store all data in the leaves.
/// "Index b-trees" use arbitrary keys and store no data at all.
///
/// A b-tree page is divided into regions in the following order:
///   1. The 100-byte database file header (found on page 1 only)
///   2. The 8 or 12 byte b-tree page header
///   3. The cell pointer array
///   4. Unallocated space
///   5. The cell content area
///   6. The reserved region.
///
/// The 100-byte database file header is found only on page 1, which is always a table b-tree page. All other b-tree pages in the database file omit this 100-byte header.
/// +------------------------------------------------------------------------+----+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
/// |Offset                                                                  |Size|Description                                                                                                                                                         |
/// +------------------------------------------------------------------------+----+--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
/// |0                                                                       |1   |The one-byte flag at offset 0 indicating the b-tree page type.                                                                                                      |
/// |                                                                        |    |                                                                                                                                                                    |
/// |    A value of 2 (0x02) means the page is an interior index b-tree page.|    |                                                                                                                                                                    |
/// |    A value of 5 (0x05) means the page is an interior table b-tree page.|    |                                                                                                                                                                    |
/// |    A value of 10 (0x0a) means the page is a leaf index b-tree page.    |    |                                                                                                                                                                    |
/// |    A value of 13 (0x0d) means the page is a leaf table b-tree page.    |    |                                                                                                                                                                    |
/// |                                                                        |    |                                                                                                                                                                    |
/// |Any other value for the b-tree page type is an error.                   |    |                                                                                                                                                                    |
/// |1                                                                       |2   |The two-byte integer at offset 1 gives the start of the first freeblock on the page or is zero if there are no freeblocks.                                          |
/// |3                                                                       |2   |The two-byte integer at offset 3 gives the number of cells on the page.                                                                                             |
/// |5                                                                       |2   |The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.                              |
/// |7                                                                       |1   |The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell content area.                                                            |
/// |8                                                                       |4   |The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.|
/// +------------------------------------------------------------------------+----+--------------------------------------------------------------------------------------------------------------------------------------------------------------------+
#[derive(Debug, Clone)]
pub struct BTree {
  pub page: Page,
  pub header: Header,
  pub cells: Vec<Cell>,
}

impl BTree {
  pub fn new(page: Page) -> Self {
    let header = Header::new(page.clone());
    let cells = Cell::new(page.clone(), &header);
    Self {
      page,
      header,
      cells,
    }
  }
}

#[derive(Debug, Clone, Copy)]
pub struct Header {
  pub page_type: u8,
  pub first_freeblock: u16,
  pub num_cells: u16,
  pub start_cell_content: u16,
  pub num_fragmented_free_bytes: u8,
  pub right_most_pointer: u32,
}

impl Header {
  pub fn new(page: Page) -> Self {
    let page_type = page.read_u8(0);
    let first_freeblock = page.read_u16(1);
    let num_cells = page.read_u16(3);
    let start_cell_content = page.read_u16(5);
    let num_fragmented_free_bytes = page.read_u8(7);
    let right_most_pointer = page.read_u32(8);

    println!(
      r#"
      page_type: {page_type}
      first_freeblock: {first_freeblock}
      num_cells: {num_cells}
      start_cell_content: {start_cell_content}
      num_fragmented_free_bytes: {num_fragmented_free_bytes}
      right_most_pointer:{right_most_pointer}
    "#
    );

    Self {
      page_type,
      first_freeblock,
      num_cells,
      start_cell_content,
      num_fragmented_free_bytes,
      right_most_pointer,
    }
  }
}
