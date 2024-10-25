use std::{fs::File, io::Read};

use anyhow::{Error, Result};

/// Sqlite Database Header
///
/// The first 100 bytes of the database file comprise the database file header. The database file header is divided into fields as shown by the table below.
/// All multibyte fields in the database file header are stored with the most significant byte first (big-endian).
///
/// +------+----+-----------------------------------------------------------------------------------------------------------------------------------------+
/// |Offset|Size|Description                                                                                                                              |
/// +------+----+-----------------------------------------------------------------------------------------------------------------------------------------|
/// |0     |16  |The header string: "SQLite format 3\000"                                                                                                 |
/// |16    |2   |The database page size in bytes. Must be a power of two between 512 and 32768 inclusive or the value 1 representing a page size of 65536.|
/// |18    |1   |File format write version. 1 for legacy; 2 for WAL.                                                                                      |
/// |19    |1   |File format read version. 1 for legacy; 2 for WAL.                                                                                       |
/// |20    |1   |Bytes of unused "reserved" space at the end of each page. Usually 0.                                                                     |
/// |21    |1   |Maximum embedded payload fraction. Must be 64.                                                                                           |
/// |22    |1   |Minimum embedded payload fraction. Must be 32.                                                                                           |
/// |23    |1   |Leaf payload fraction. Must be 32.                                                                                                       |
/// |24    |4   |File change counter.                                                                                                                     |
/// |28    |4   |Size of the database file in pages. The "in-header database size".                                                                       |
/// |32    |4   |Page number of the first freelist trunk page.                                                                                            |
/// |36    |4   |Total number of freelist pages.                                                                                                          |
/// |40    |4   |The schema cookie.                                                                                                                       |
/// |44    |4   |The schema format number. Supported schema formats are 1 2 3 and 4.                                                                      |
/// |48    |4   |Default page cache size.                                                                                                                 |
/// |52    |4   |The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes or zero otherwise.                       |
/// |56    |4   |The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.                          |
/// |60    |4   |The "user version" as read and set by the user_version pragma.                                                                           |
/// |64    |4   |True (non-zero) for incremental-vacuum mode. False (zero) otherwise.                                                                     |
/// |68    |4   |The "Application ID" set by PRAGMA application_id.                                                                                       |
/// |72    |20  |Reserved for expansion. Must be zero.                                                                                                    |
/// |92    |4   |The version-valid-for number.                                                                                                            |
/// |96    |4   |SQLITE_VERSION_NUMBER                                                                                                                    |
/// +------+----+-----------------------------------------------------------------------------------------------------------------------------------------+
pub struct DbHeader {
  pub header: [u8; 16],
  pub page_size: u16,
  pub file_format_write_version: u8,
  pub file_format_read_version: u8,
  pub reserved_space: u8,
  pub max_embedded_payload_fraction: u8,
  pub min_embedded_payload_fraction: u8,
  pub leaf_payload_fraction: u8,
  pub file_change_counter: u32,
  pub database_size: u32,
  pub first_freelist_trunk_page: u32,
  pub total_freelist_pages: u32,
  pub schema_cookie: u32,
  pub schema_format_number: u32,
  pub default_page_cache_size: u32,
  pub largest_root_b_tree_page: u32,
  pub database_text_encoding: u32,
  pub user_version: u32,
  pub incremental_vacuum_mode: u32,
  pub application_id: u32,
  pub reserved_expansion: [u8; 20],
  pub version_valid_for_number: u32,
  pub sqlite_version_number: u32,
}

impl TryFrom<&mut File> for DbHeader {
  type Error = Error;

  fn try_from(file: &mut File) -> Result<Self> {
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    DbHeader::try_from(&header)
  }
}

impl TryFrom<&[u8; 100]> for DbHeader {
  type Error = Error;

  fn try_from(bytes: &[u8; 100]) -> Result<Self> {
    let header = DbHeader {
      header: bytes[0..16].try_into()?,
      page_size: u16::from_be_bytes(bytes[16..18].try_into()?),
      file_format_write_version: bytes[18],
      file_format_read_version: bytes[19],
      reserved_space: bytes[20],
      max_embedded_payload_fraction: bytes[21],
      min_embedded_payload_fraction: bytes[22],
      leaf_payload_fraction: bytes[23],
      file_change_counter: u32::from_be_bytes(bytes[24..28].try_into()?),
      database_size: u32::from_be_bytes(bytes[28..32].try_into()?),
      first_freelist_trunk_page: u32::from_be_bytes(bytes[32..36].try_into()?),
      total_freelist_pages: u32::from_be_bytes(bytes[36..40].try_into()?),
      schema_cookie: u32::from_be_bytes(bytes[40..44].try_into()?),
      schema_format_number: u32::from_be_bytes(bytes[44..48].try_into()?),
      default_page_cache_size: u32::from_be_bytes(bytes[48..52].try_into()?),
      largest_root_b_tree_page: u32::from_be_bytes(bytes[52..56].try_into()?),
      database_text_encoding: u32::from_be_bytes(bytes[56..60].try_into()?),
      user_version: u32::from_be_bytes(bytes[60..64].try_into()?),
      incremental_vacuum_mode: u32::from_be_bytes(bytes[64..68].try_into()?),
      application_id: u32::from_be_bytes(bytes[68..72].try_into()?),
      reserved_expansion: bytes[72..92].try_into()?,
      version_valid_for_number: u32::from_be_bytes(bytes[92..96].try_into()?),
      sqlite_version_number: u32::from_be_bytes(bytes[96..100].try_into()?),
    };

    assert!(header.page_size.is_power_of_two());
    assert!(header.page_size >= 512 && header.page_size <= 32768);
    assert!(header.max_embedded_payload_fraction == 64);
    assert!(header.min_embedded_payload_fraction == 32);
    assert!(header.leaf_payload_fraction == 32);
    assert!(header.reserved_expansion.iter().all(|&x| x == 0));

    Ok(header)
  }
}
