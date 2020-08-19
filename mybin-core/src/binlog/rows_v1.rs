//! meaningful data structures and parsing logic of RowsEventV1
use bytes_parser::error::Result;
use bytes_parser::{ReadFromBytes, ReadBytesExt};
use bytes::{Buf, Bytes};

/// Data of WriteRowsEventV1
///
/// reference: https://dev.mysql.com/doc/internals/en/rows-event.html
/// this struct defines common layout of three v1 row events
/// the detailed row information will be handled by separate module
#[derive(Debug, Clone)]
pub struct WriteRowsDataV1 {
    // actual 6-byte integer
    pub table_id: u64,
    pub flags: u16,
    // below is variable part
    pub payload: Bytes,
}

impl ReadFromBytes for WriteRowsDataV1 {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let table_id = input.read_le_u48()?;
        let flags = input.read_le_u16()?;
        let payload = input.split_to(input.remaining());
        Ok(WriteRowsDataV1 {
            table_id,
            flags,
            payload,
        })
    }
}

/// Data of UpdateRowsEventV1
#[derive(Debug, Clone)]
pub struct UpdateRowsDataV1 {
    // actual 6-byte integer
    pub table_id: u64,
    pub flags: u16,
    // below is variable part
    pub payload: Bytes,
}

impl ReadFromBytes for UpdateRowsDataV1 {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let wrd = WriteRowsDataV1::read_from(input)?;
        Ok(UpdateRowsDataV1{
            table_id: wrd.table_id,
            flags: wrd.flags,
            payload: wrd.payload,
        })
    }
}

/// Data of DeleteRowsEventV1
#[derive(Debug, Clone)]
pub struct DeleteRowsDataV1 {
    // actual 6-byte integer
    pub table_id: u64,
    pub flags: u16,
    // below is variable part
    pub payload: Bytes,
}

impl ReadFromBytes for DeleteRowsDataV1 {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let wrd = WriteRowsDataV1::read_from(input)?;
        Ok(DeleteRowsDataV1{
            table_id: wrd.table_id,
            flags: wrd.flags,
            payload: wrd.payload,
        })
    }
}