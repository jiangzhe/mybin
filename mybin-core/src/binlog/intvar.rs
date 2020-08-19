use bitflags::bitflags;
use bytes_parser::error::{Error, Result};
use bytes_parser::{ReadBytesExt, ReadFromBytes};
use bytes::Bytes;

/// Data of IntvarEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/intvar-event.html
#[derive(Debug, Clone)]
pub struct IntvarData {
    pub key: IntvarKey,
    pub value: u64,
}

impl ReadFromBytes for IntvarData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let key = input.read_u8()?;
        let key = IntvarKey::from_bits(key)
            .ok_or_else(|| Error::ConstraintError(format!("invalid intvar key {}", key)))?;
        let value = input.read_le_u64()?;
        Ok(IntvarData { key, value })
    }
}

bitflags! {
    pub struct IntvarKey: u8 {
        const LAST_INSERT_ID = 0x01;
        const INSERT_ID = 0x02;
    }
}
