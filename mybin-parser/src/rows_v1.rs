//! meaningful data structures and parsing logic of RowsEventV1
use serde_derive::*;
use nom::IResult;
use nom::error::ParseError;
use nom::number::streaming::{le_u16, le_u32};
use nom::bytes::streaming::take;
use crate::util::streaming_le_u48;


/// Data of DeleteRowsEventV1, UpdateRowsEventV1, WriteRowsEventV1
///
/// reference: https://dev.mysql.com/doc/internals/en/rows-event.html
/// this struct defines common layout of three v1 row events
/// the detailed row information will be handled by separate module
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowsDataV1<'a> {
    // actual 6-byte integer
    pub table_id: u64,
    pub flags: u16,
    // below is variable part
    pub payload: &'a [u8],
}

pub(crate) fn parse_rows_data_v1<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (RowsDataV1<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(8, post_header_len);
    let (input, table_id) = streaming_le_u48(input)?;
    let (input, flags) = le_u16(input)?;
    let (input, payload, crc32) = if checksum {
        let (input, payload) = take(len - post_header_len as u32 - 4)(input)?;
        let (input, crc32) = le_u32(input)?;
        (input, payload, crc32)
    } else {
        let (input, payload) = take(len - post_header_len as u32)(input)?;
        (input, payload, 0)
    };
    Ok((
        input,
        (
            RowsDataV1 {
                table_id,
                flags,
                payload,
            },
            crc32,
        ),
    ))
}
