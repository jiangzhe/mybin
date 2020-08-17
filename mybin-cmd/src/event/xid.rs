use bytes_parser::error::Result;
use bytes_parser::number::ReadNumber;
use bytes_parser::ReadAs;

/// Data of XidEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/xid-event.html
#[derive(Debug, Clone)]
pub struct XidData {
    pub xid: u64,
}

impl ReadAs<'_, XidData> for [u8] {
    fn read_as(&self, offset: usize) -> Result<(usize, XidData)> {
        let (offset, xid) = self.read_le_u64(offset)?;
        Ok((offset, XidData{xid}))
    }
}
