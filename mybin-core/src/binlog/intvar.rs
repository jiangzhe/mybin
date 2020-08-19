use bitflags::bitflags;
use bytes_parser::error::{Error, Result};
use bytes_parser::number::ReadNumber;
use bytes_parser::ReadFrom;

/// Data of IntvarEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/intvar-event.html
#[derive(Debug, Clone)]
pub struct IntvarData {
    pub key: IntvarKey,
    pub value: u64,
}

impl ReadFrom<'_, IntvarData> for [u8] {
    fn read_from(&self, offset: usize) -> Result<(usize, IntvarData)> {
        let (offset, key) = self.read_u8(offset)?;
        let key = IntvarKey::from_bits(key)
            .ok_or_else(|| Error::ConstraintError(format!("invalid intvar key {}", key)))?;
        let (offset, value) = self.read_le_u64(offset)?;
        debug_assert_eq!(self.len(), offset);
        Ok((offset, IntvarData { key, value }))
    }
}

bitflags! {
    pub struct IntvarKey: u8 {
        const LAST_INSERT_ID = 0x01;
        const INSERT_ID = 0x02;
    }
}
