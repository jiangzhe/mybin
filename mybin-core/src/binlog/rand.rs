use bytes::Bytes;
use bytes_parser::error::Result;
use bytes_parser::{ReadBytesExt, ReadFromBytes};

/// Data of RandEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/rand-event.html
#[derive(Debug, Clone)]
pub struct RandData {
    pub seed1: u64,
    pub seed2: u64,
}

impl ReadFromBytes for RandData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let seed1 = input.read_le_u64()?;
        let seed2 = input.read_le_u64()?;
        Ok(RandData { seed1, seed2 })
    }
}
