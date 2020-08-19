//! meaningful data structures and parsing logic of RowsEventV1
use bytes_parser::bytes::ReadBytes;
use bytes_parser::error::Result;
use bytes_parser::number::ReadNumber;
use bytes_parser::ReadFrom;

/// Data of DeleteRowsEventV1, UpdateRowsEventV1, WriteRowsEventV1
///
/// reference: https://dev.mysql.com/doc/internals/en/rows-event.html
/// this struct defines common layout of three v1 row events
/// the detailed row information will be handled by separate module
#[derive(Debug, Clone)]
pub struct RowsDataV1<'a> {
    // actual 6-byte integer
    pub table_id: u64,
    pub flags: u16,
    // below is variable part
    pub payload: &'a [u8],
}

impl<'a> ReadFrom<'a, RowsDataV1<'a>> for [u8] {
    fn read_from(&'a self, offset: usize) -> Result<(usize, RowsDataV1<'a>)> {
        let (offset, table_id) = self.read_le_u48(offset)?;
        let (offset, flags) = self.read_le_u16(offset)?;
        let (offset, payload) = self.take_len(offset, self.len() - offset)?;
        Ok((
            offset,
            RowsDataV1 {
                table_id,
                flags,
                payload,
            },
        ))
    }
}
