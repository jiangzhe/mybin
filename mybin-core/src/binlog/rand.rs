use bytes_parser::error::Result;
use bytes_parser::number::ReadNumber;
use bytes_parser::ReadFrom;

/// Data of RandEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/rand-event.html
#[derive(Debug, Clone)]
pub struct RandData {
    pub seed1: u64,
    pub seed2: u64,
}

impl ReadFrom<'_, RandData> for [u8] {
    fn read_from(&self, offset: usize) -> Result<(usize, RandData)> {
        let (offset, seed1) = self.read_le_u64(offset)?;
        let (offset, seed2) = self.read_le_u64(offset)?;
        Ok((offset, RandData { seed1, seed2 }))
    }
}
