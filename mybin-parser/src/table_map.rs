use crate::col::*;
use crate::error::Error;
use crate::util::{len_enc_int, streaming_le_u48};
use nom::bytes::streaming::take;
use nom::error::ParseError;
use nom::multi::length_data;
use nom::number::streaming::{le_u16, le_u32, le_u8};
use nom::IResult;
use serde_derive::*;
use std::convert::TryFrom;

/// Data of TableMapEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/table-map-event.html
/// only support binlog v4
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMapData<'a> {
    // actually 6-bytes integer
    pub table_id: u64,
    pub flags: u16,
    // below is variable part
    // complicated to decode, so leave it as is
    // use specific function to evaluate later
    pub payload: &'a [u8],
}

impl<'a> TableMapData<'a> {
    pub fn raw_table_map(&self) -> Result<RawTableMap<'a>, Error> {
        let (_, rtm) =
            parse_raw_table_map(self.payload).map_err(|e| Error::from((self.payload, e)))?;
        Ok(rtm)
    }

    pub fn table_map(&self) -> Result<TableMap, Error> {
        use std::convert::TryInto;
        self.raw_table_map().and_then(TryInto::try_into)
    }
}

pub(crate) fn parse_table_map_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (TableMapData<'a>, u32), E>
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
            TableMapData {
                table_id,
                flags,
                payload,
            },
            crc32,
        ),
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTableMap<'a> {
    pub schema_name: &'a [u8],
    pub table_name: &'a [u8],
    pub column_count: u64,
    pub column_defs: &'a [u8],
    pub column_meta_defs: &'a [u8],
    pub null_bitmap: &'a [u8],
}

/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/rows_event.h
pub(crate) fn parse_raw_table_map<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], RawTableMap<'a>, E>
where
    E: ParseError<&'a [u8]>,
{
    dbg!(input.len());
    let (input, schema_name) = length_data(le_u8)(input)?;
    let (input, _) = take(1usize)(input)?;
    // 8+1+1
    let (input, table_name) = length_data(le_u8)(input)?;
    let (input, _) = take(1usize)(input)?;
    // 5+1+1
    let (input, column_count) = len_enc_int(input)?;
    let column_count = column_count.to_u64().expect("error column count");
    let (input, column_defs) = take(column_count)(input)?;
    // 1+2
    let (input, column_meta_defs_length) = len_enc_int(input)?;
    let column_meta_defs_length = column_meta_defs_length
        .to_u64()
        .expect("error column meta def length");
    let (input, column_meta_defs) = take(column_meta_defs_length)(input)?;
    // 1+2
    let bitmap_len = (column_count + 7) / 8u64;
    let (input, null_bitmap) = take(bitmap_len)(input)?;
    Ok((
        input,
        RawTableMap {
            schema_name,
            table_name,
            column_count,
            column_defs,
            column_meta_defs,
            null_bitmap,
        },
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMap {
    pub schema_name: String,
    pub table_name: String,
    pub col_metas: Vec<ColumnMetadata>,
}

impl<'a> TryFrom<RawTableMap<'a>> for TableMap {
    type Error = Error;
    fn try_from(raw: RawTableMap<'a>) -> Result<Self, Self::Error> {
        let schema_name = String::from_utf8(Vec::from(raw.schema_name))?;
        let table_name = String::from_utf8(Vec::from(raw.table_name))?;
        let col_metas = parse_col_metas(
            raw.column_count as usize,
            raw.column_defs,
            raw.column_meta_defs,
            raw.null_bitmap,
        )?;
        Ok(TableMap {
            schema_name,
            table_name,
            col_metas,
        })
    }
}
