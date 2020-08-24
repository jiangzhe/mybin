use bytes::{Buf, Bytes};
use bytes_parser::error::Result;
use bytes_parser::{ReadBytesExt, ReadFromBytes};

/// Data of RotateEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/rotate-event.html
#[derive(Debug, Clone)]
pub struct RotateData {
    pub position: u64,
    // below is variable part
    pub next_binlog_filename: Bytes,
}

impl ReadFromBytes for RotateData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let position = input.read_le_u64()?;
        let next_binlog_filename = input.split_to(input.remaining());
        Ok(RotateData {
            position,
            next_binlog_filename,
        })
    }
}
