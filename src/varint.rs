#[derive(Debug, Clone)]
pub struct VarInt {
  pub bytes: Vec<u8>,
  pub size: usize,
}

impl VarInt {
  pub fn get_encoded_size(value: u64) -> usize {
    match (value as usize).count_ones() {
      0..=7 => 1,
      8..=15 => 2,
      _ => 5,
    }
  }

  pub fn decode(data: &[u8]) -> u64 {
    let mut value = u64::from_be_bytes([0; 8]);
    for i in data.iter() {
      value *= 128;
      value += *i as u64;

      if (value & !0xFF) == 0 {
        break;
      }
    }

    value
  }

  pub fn encode(value: u64) -> Vec<u8> {
    let mut data = vec![];
    let mut value = value;
    while value > 0 {
      data.push((value & 0x7F) as u8);
      value >>= 7;
    }
    data
  }
}
