use super::{Header, page::Page};

/// Table B-Tree Leaf Cell (header 0x0d):
///
///         A varint which is the total number of bytes of payload, including any overflow
///         A varint which is the integer key, a.k.a. "rowid"
///         The initial portion of the payload that does not spill to overflow pages.
///         A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
///
/// Table B-Tree Interior Cell (header 0x05):
///
///         A 4-byte big-endian page number which is the left child pointer.
///         A varint which is the integer key
///
/// Index B-Tree Leaf Cell (header 0x0a):
///
///         A varint which is the total number of bytes of key payload, including any overflow
///         The initial portion of the payload that does not spill to overflow pages.
///         A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
///
/// Index B-Tree Interior Cell (header 0x02):
///
///         A 4-byte big-endian page number which is the left child pointer.
///         A varint which is the total number of bytes of key payload, including any overflow
///         The initial portion of the payload that does not spill to overflow pages.
///         A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
#[derive(Debug, Clone)]
pub enum Cell {
  TableLeaf {
    payload_size: u64,
    row_id: u64,
    payload: Vec<u8>, // Local payload bytes
    overflow_page: Option<u32>,
  },
  TableInterior {
    left_child_page: u32,
    row_id: u64,
  },
  IndexLeaf {
    payload_size: u64,
    payload: Vec<u8>,
    overflow_page: Option<u32>,
  },
  IndexInterior {
    left_child_page: u32,
    payload_size: u64,
    payload: Vec<u8>,
    overflow_page: Option<u32>,
  },
}

macro_rules! read_varint_and_advance {
  ($page:expr, $offset:expr) => {{
    let (value, varint_offset) = $page.read_varint($offset);
    $offset += varint_offset;
    value
  }};
}

impl Cell {
  pub fn payload(&self) -> &[u8] {
    match self {
      Cell::TableLeaf { payload, .. } => payload,
      Cell::IndexLeaf { payload, .. } => payload,
      Cell::IndexInterior { payload, .. } => payload,
      _ => &[],
    }
  }

  pub fn new(page: Page, header: &Header) -> Vec<Self> {
    let mut cells = Vec::new();
    let offset_adjustment = if page.page_number == 1 { 100 } else { 0 };
    for i in 0..header.num_cells {
      let mut cell_offset = page.read_u16(8 + (i as usize) * 2) as usize - offset_adjustment;
      match header.page_type {
        0x0D => {
          let payload_size = read_varint_and_advance!(page, cell_offset);
          let row_id = read_varint_and_advance!(page, cell_offset);
          let (payload, overflow_page) = Self::read_payload(&page, payload_size, cell_offset);
          cells.push(Cell::TableLeaf {
            payload_size,
            row_id,
            payload,
            overflow_page,
          });
        }
        0x05 => {
          cells.push(Cell::TableInterior {
            left_child_page: page.read_u32(cell_offset),
            row_id: page.read_varint(cell_offset + 4).0,
          });
        }
        0x0A => {
          let payload_size = read_varint_and_advance!(page, cell_offset);
          let (payload, overflow_page) = Self::read_payload(&page, payload_size, cell_offset);
          cells.push(Cell::IndexLeaf {
            payload_size,
            payload,
            overflow_page,
          });
        }
        0x02 => {
          let left_child_page = page.read_u32(cell_offset);
          cell_offset += 4;
          let payload_size = read_varint_and_advance!(page, cell_offset);
          let (payload, overflow_page) = Self::read_payload(&page, payload_size, cell_offset);
          cells.push(Cell::IndexInterior {
            left_child_page,
            payload_size,
            payload,
            overflow_page,
          });
        }
        _ => panic!("Unknown page type: {}", header.page_type),
      }
    }
    cells
  }

  fn read_payload(page: &Page, payload_size: u64, payload_offset: usize) -> (Vec<u8>, Option<u32>) {
    let remaining_space = (page.data.len() - payload_offset) as u64;
    if payload_size <= remaining_space {
      let payload = page.read_bytes(payload_offset, payload_size as usize);
      (payload, None)
    } else {
      if remaining_space < 4 {
        eprintln!("Not enough space for overflow page number");
        return (vec![], None);
      }
      let local_payload_size = (remaining_space - 4) as usize;
      let payload = page.read_bytes(payload_offset, local_payload_size);
      let overflow_offset = payload_offset + local_payload_size;
      if overflow_offset + 4 > page.data.len() {
        eprintln!(
          "Overflow offset {} exceeds page length {}",
          overflow_offset,
          page.data.len()
        );
        return (payload, None);
      }
      let overflow_page = page.read_u32(overflow_offset);
      // Sanity check: SQLite page numbers should be small and positive
      if overflow_page == 0 || overflow_page > 1_000_000 {
        return (payload, None); // Treat as no overflow
      }
      (payload, Some(overflow_page))
    }
  }
}
