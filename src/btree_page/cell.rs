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
    rowid: u64,
    payload: Vec<u8>,
    overflow_page: Option<u32>,
  },
  TableInterior {
    left_child_page: u32,
    rowid: u64,
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

impl Cell {
  pub fn new(page: Page, header: &Header) -> Vec<Self> {
    let mut cells = Vec::new();

    for i in 0..header.num_cells {
      let mut cell_offset = page.read_u16(8 + (i as usize) * 2) as usize;
      match header.page_type {
        0x0D => {
          let (payload_size, varint_offset) = page.read_varint(cell_offset);
          cell_offset += varint_offset;
          let (rowid, varint_offset) = page.read_varint(cell_offset);
          cell_offset += varint_offset;
          let payload_offset = cell_offset;
          let overflow_page = if payload_size > (page.data.len() - payload_offset) as u64 {
            Some(page.read_u32(payload_offset + (page.data.len() - payload_offset)))
          } else {
            None
          };
          let payload = page.read_bytes(payload_offset, payload_size as usize);
          cells.push(Cell::TableLeaf {
            payload_size,
            rowid,
            payload,
            overflow_page,
          });
        }
        0x05 => {
          // Table Interior Cell
          let left_child_page = page.read_u32(cell_offset);
          let (rowid, _) = page.read_varint(cell_offset + 4);
          cells.push(Cell::TableInterior {
            left_child_page,
            rowid,
          });
        }
        0x0A => {
          let (payload_size, varint_offset) = page.read_varint(cell_offset);
          cell_offset += varint_offset;
          let payload_offset = cell_offset;
          let overflow_page = if payload_size > (page.data.len() - payload_offset) as u64 {
            Some(page.read_u32(payload_offset + (page.data.len() - payload_offset)))
          } else {
            None
          };
          let payload = page.read_bytes(payload_offset, payload_size as usize);
          cells.push(Cell::IndexLeaf {
            payload_size,
            payload,
            overflow_page,
          });
        }
        0x02 => {
          let left_child_page = page.read_u32(cell_offset);
          cell_offset += 4;
          let (payload_size, varint_offset) = page.read_varint(cell_offset);
          cell_offset += varint_offset;
          let payload_offset = cell_offset;
          let overflow_page = if payload_size > (page.data.len() - payload_offset) as u64 {
            Some(page.read_u32(payload_offset + (page.data.len() - payload_offset)))
          } else {
            None
          };
          let payload = page.read_bytes(payload_offset, payload_size as usize);

          cells.push(Cell::IndexInterior {
            left_child_page,
            payload_size,
            payload,
            overflow_page,
          })
        }
        _ => {
          panic!("Unknown page type: {}", header.page_type);
        }
      }
    }

    cells
  }
}
