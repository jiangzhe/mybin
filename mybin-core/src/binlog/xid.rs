use bytes_parser::error::Result;
use bytes_parser::{ReadFromBytes, ReadBytesExt};
use bytes::Bytes;

/// Data of XidEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/xid-event.html
#[derive(Debug, Clone)]
pub struct XidData {
    pub xid: u64,
}

impl ReadFromBytes for XidData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let xid = input.read_le_u64()?;
        Ok(XidData { xid })
    }
}
