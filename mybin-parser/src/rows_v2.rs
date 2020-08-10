//! meaningful data structures and parsing logic of RowsEventV2
use crate::error::Error;
use crate::util::{len_enc_int, len_enc_str, LenEncStr, streaming_le_u48, bitmap_index};
use nom::bytes::streaming::take;
use nom::error::ParseError;
use nom::number::streaming::{le_i8, le_i16, le_i32, le_i64, le_f32, le_f64, le_u8, le_u16, le_u32};
use nom::IResult;
use serde_derive::*;
use crate::col::{ColumnMetadata, ColumnValue};

/// Data of DeleteRowEventV2, UpdateRowsEventV2, WriteRowsEventV2
///
/// reference: https://dev.mysql.com/doc/internals/en/rows-event.html
/// similar to v1 row events
/// detailed row information will be handled by separate module
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowsDataV2<'a> {
    // actual 6-byte integer
    pub table_id: u64,
    pub flags: u16,
    pub extra_data_len: u16,
    // below is variable part
    pub payload: &'a [u8],
}

impl<'a> RowsDataV2<'a> {

    pub fn raw_delete_rows(&self) -> Result<RawRowsV2<'a>, Error> {
        self.raw_rows(false)
    }

    pub fn delete_rows(&self, col_metas: &[ColumnMetadata]) -> Result<RowsV2, Error> {
        self.raw_delete_rows().and_then(|rr| rr.delete_rows(col_metas))
    }

    pub fn raw_write_rows(&self) -> Result<RawRowsV2<'a>, Error> {
        self.raw_rows(false)
    }

    pub fn write_rows(&self, col_metas: &[ColumnMetadata]) -> Result<RowsV2, Error> {
        self.raw_write_rows().and_then(|rr| rr.write_rows(col_metas))
    }

    pub fn raw_update_rows(&self) -> Result<RawRowsV2<'a>, Error> {
        self.raw_rows(true)
    }

    pub fn update_rows(&self, col_metas: &[ColumnMetadata]) -> Result<UpdateRowsV2, Error> {
        self.raw_update_rows().and_then(|rr| rr.update_rows(col_metas))
    }

    fn raw_rows(&self, update: bool) -> Result<RawRowsV2<'a>, Error> {
        // extra_data_len - 2 is the length of extra data
        let (input, raw_rows) = parse_raw_rows_v2(self.payload, self.extra_data_len-2, update)
            .map_err(|e| Error::from((self.payload, e)))?;
        Ok(raw_rows)
    }
}

pub(crate) fn parse_rows_data_v2<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (RowsDataV2<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(10, post_header_len);
    let (input, table_id) = streaming_le_u48(input)?;
    let (input, flags) = le_u16(input)?;
    let (input, extra_data_len) = le_u16(input)?;
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
            RowsDataV2 {
                table_id,
                flags,
                extra_data_len,
                payload,
            },
            crc32,
        ),
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawRowsV2<'a> {
    pub extra_data: &'a [u8],
    pub n_cols: u32,
    pub before_col_bitmap: &'a [u8],
    // after col bitmap may share same byte array
    // as before col bitmp
    // only UpdateRowsEventV2 owns different bitmaps
    // of before and after
    pub after_col_bitmap: &'a [u8],
    pub rows_data: &'a [u8],
}

impl<'a> RawRowsV2<'a> {

    pub fn write_rows(&self, col_metas: &[ColumnMetadata]) -> Result<RowsV2, Error> {
        // todo: error handling
        let (_, rows) = self.parse_rows(col_metas, true).map_err(|e| Error::from((self.rows_data, e)))?;
        Ok(rows)
    }

    pub fn delete_rows(&self, col_metas: &[ColumnMetadata]) -> Result<RowsV2, Error> {
        // todo: error handling
        let (_, rows) = self.parse_rows(col_metas, false).map_err(|e| Error::from((self.rows_data, e)))?;
        Ok(rows)
    }

    pub fn update_rows(&self, col_metas: &[ColumnMetadata]) -> Result<UpdateRowsV2, Error> {
        // todo: error handling
        let (_, rows) = self.parse_update_rows(col_metas, ).map_err(|e| Error::from((self.rows_data, e)))?;
        Ok(rows)
    }

    fn parse_rows<E>(&self, col_metas: &[ColumnMetadata], write: bool) -> IResult<&'a [u8], RowsV2, E> 
    where
        E: ParseError<&'a [u8]>,
    {
        
        let mut rows = Vec::new();
        let bm1 = if write {
            self.after_col_bitmap
        } else {
            self.before_col_bitmap
        };
        let n_cols = self.n_cols as usize;
        let bitmap_len = (n_cols + 7) / 8;
        let mut input = self.rows_data;
        while !input.is_empty() {
            let (in1, col_bm) = take(bitmap_len)(input)?;
            let col_bm: Vec<u8> = bm1.iter().zip(col_bm.iter()).map(|(b1, b2)| b1 ^ b2).collect();
            let (in1, row) = parse_row(in1, n_cols, &col_bm, col_metas)?;
            rows.push(row);
            input = in1;
        }
        Ok((input, RowsV2(rows)))
    }

    fn parse_update_rows<E>(&self, col_metas: &[ColumnMetadata]) -> IResult<&'a [u8], UpdateRowsV2, E> 
    where
        E: ParseError<&'a [u8]>,
    {
        let mut rows = Vec::new();
        let n_cols = self.n_cols as usize;
        let bitmap_len = (n_cols + 7) / 8;
        let mut input = self.rows_data;
        dbg!(&input[..]);
        while !input.is_empty() {
            // before row
            let (in1, before_col_bm) = take(bitmap_len)(input)?;
            let before_col_bm: Vec<u8> = self.before_col_bitmap.iter().zip(before_col_bm.iter()).map(|(b1, b2)| b1 ^ b2).collect();
            let (in1, before_row) = parse_row(in1, n_cols, &before_col_bm, col_metas)?;
            // after row
            let (in1, after_col_bm) = take(bitmap_len)(in1)?;
            let after_col_bm: Vec<u8> = self.after_col_bitmap.iter().zip(after_col_bm.iter()).map(|(b1, b2)| b1 ^ b2).collect();
            let (in1, after_row) = parse_row(in1, n_cols, &after_col_bm, col_metas)?;
            rows.push(UpdateRow(before_row.0, after_row.0));
            input = in1;
        }
        Ok((input, UpdateRowsV2(rows)))
    }
}

/// parse raw rows v2, including WriteRows and DeleteRows v2
/// 
/// the extra data length should be real length: len in binlog file minus 2
pub fn parse_raw_rows_v2<'a, E>(input: &'a [u8], extra_data_len: u16, update: bool) -> IResult<&'a [u8], RawRowsV2<'a>, E> 
where
    E: ParseError<&'a [u8]>,
{
    let (input, extra_data) = take(extra_data_len)(input)?;
    let (input, n_cols) = len_enc_int(input)?;
    // todo: assign error to avoid panic
    let n_cols = n_cols.to_u32().expect("number of columns");
    let bitmap_len = (n_cols + 7) >> 3;
    let (input, before_col_bitmap) = take(bitmap_len)(input)?;
    let (input, after_col_bitmap) = if update {
        take(bitmap_len)(input)?
    } else {
        (input, before_col_bitmap)
    };
    let (input, rows_data) = take(input.len())(input)?;
    Ok((input, RawRowsV2{extra_data, n_cols, before_col_bitmap, after_col_bitmap, rows_data}))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowsV2(Vec<Row>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row(Vec<ColumnValue>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateRowsV2(Vec<UpdateRow>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateRow(Vec<ColumnValue>, Vec<ColumnValue>);

fn parse_row<'a, E>(input: &'a [u8], n_cols: usize, col_bm: &[u8], col_metas: &[ColumnMetadata]) -> IResult<&'a [u8], Row, E> 
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(n_cols, col_metas.len());
    let mut cols = Vec::with_capacity(n_cols);
    let mut input = input;
    for i in 0..n_cols {
        if bitmap_index(col_bm, i) {
            let (in1, col_val) = parse_column_value(input, &col_metas[i])?;
            input = in1;
            cols.push(col_val);
        } else {
            cols.push(ColumnValue::Null);
        }
    }
    Ok((input, Row(cols)))
}

/// todo: use error instead of panic
/// reference: https://github.com/mysql/mysql-server/blob/5.7/sql/protocol_classic.cc
fn parse_column_value<'a, E>(input: &'a [u8], col_meta: &ColumnMetadata) -> IResult<&'a [u8], ColumnValue, E> 
where
    E: ParseError<&'a [u8]>,
{
    let (input, col_val) = match col_meta {
        ColumnMetadata::Decimal{..} => {
            let (input, v) = len_enc_str(input)?;
            match v {
                LenEncStr::Null => (input, ColumnValue::Null),
                LenEncStr::Err => panic!("invalid decimal"),
                LenEncStr::Ref(s) => (input, ColumnValue::Decimal(Vec::from(s))),
            }
        },
        ColumnMetadata::Tiny{..} => {
            let (input, v) = le_i8(input)?;
            (input, ColumnValue::Tiny(v))
        },
        ColumnMetadata::Short{..} => {
            let (input, v) = le_i16(input)?;
            (input, ColumnValue::Short(v))
        },
        ColumnMetadata::Long{..} => {
            let (input, v) = le_i32(input)?;
            (input, ColumnValue::Long(v))
        },
        // todo: pack_len not used?
        ColumnMetadata::Float{..} => {
            let (input, v) = le_f32(input)?;
            (input, ColumnValue::Float(v))
        },
        // todo: pack_len not used?
        ColumnMetadata::Double{..} => {
            let (input, v) = le_f64(input)?;
            (input, ColumnValue::Double(v))
        },
        ColumnMetadata::Null{..} => {
            (input, ColumnValue::Null)
        },
        ColumnMetadata::Timestamp{..} => {
            let (input, len) = le_u8(input)?;
            match len {
                0 => (input, ColumnValue::Null),
                7 => {
                    let (input, year) = le_u16(input)?;
                    let (input, month) = le_u8(input)?;
                    let (input, day) = le_u8(input)?;
                    let (input, hour) = le_u8(input)?;
                    let (input, minute) = le_u8(input)?;
                    let (input, second) = le_u8(input)?;
                    (input, ColumnValue::Timestamp{year, month, day, hour, minute, second})
                },
                _ => panic!("invalid length of timestamp"),
            }
        },
        ColumnMetadata::LongLong{..} => {
            let (input, v) = le_i64(input)?;
            (input, ColumnValue::LongLong(v))
        },
        ColumnMetadata::Int24{..} => {
            let (input, v) = le_i32(input)?;
            (input, ColumnValue::Int24(v))
        },
        ColumnMetadata::Date{..} => {
            let (input, len) = le_u8(input)?;
            match len {
                0 => (input, ColumnValue::Null),
                4 => {
                    let (input, year) = le_u16(input)?;
                    let (input, month) = le_u8(input)?;
                    let (input, day) = le_u8(input)?;
                    (input, ColumnValue::Date{year, month, day})
                }
                _ => panic!("invalid length of date"),
            }
        },
        ColumnMetadata::Time{..} => {
            let (input, len) = le_u8(input)?;
            match len {
                0 => (input, ColumnValue::Null),
                8 => {
                    let (input, negative) = le_u8(input)?;
                    let negative = negative == 0x01;
                    let (input, days) = le_u32(input)?;
                    let (input, hours) = le_u8(input)?;
                    let (input, minutes) = le_u8(input)?;
                    let (input, seconds) = le_u8(input)?;
                    (input, ColumnValue::Time{negative, days, hours, minutes, seconds, micro_seconds: 0})
                }
                12 => {
                    let (input, negative) = le_u8(input)?;
                    let negative = negative == 0x01;
                    let (input, days) = le_u32(input)?;
                    let (input, hours) = le_u8(input)?;
                    let (input, minutes) = le_u8(input)?;
                    let (input, seconds) = le_u8(input)?;
                    let (input, micro_seconds) = le_u32(input)?;
                    (input, ColumnValue::Time{negative, days, hours, minutes, seconds, micro_seconds})
                }
                _ => panic!("invalid length of time"),
            }
        },
        ColumnMetadata::DateTime{..} => {
            let (input, len) = le_u8(input)?;
            match len {
                0 => (input, ColumnValue::Null),
                7 => {
                    let (input, year) = le_u16(input)?;
                    let (input, month) = le_u8(input)?;
                    let (input, day) = le_u8(input)?;
                    let (input, hour) = le_u8(input)?;
                    let (input, minute) = le_u8(input)?;
                    let (input, second) = le_u8(input)?;
                    (input, ColumnValue::DateTime{year, month, day, hour, minute, second, micro_second: 0})
                },
                11 => {
                    let (input, year) = le_u16(input)?;
                    let (input, month) = le_u8(input)?;
                    let (input, day) = le_u8(input)?;
                    let (input, hour) = le_u8(input)?;
                    let (input, minute) = le_u8(input)?;
                    let (input, second) = le_u8(input)?;
                    let (input, micro_second) = le_u32(input)?;
                    (input, ColumnValue::DateTime{year, month, day, hour, minute, second, micro_second})
                }
                _ => panic!("invalid length of timestamp"),
            }
        },
        ColumnMetadata::Year{..} => {
            let (input, v) = le_u16(input)?;
            (input, ColumnValue::Year(v))
        },
        // NewDate,
        ColumnMetadata::Varchar{..} => {
            let (input, v) = len_enc_str(input)?;
            match v {
                LenEncStr::Null => (input, ColumnValue::Null),
                LenEncStr::Err => panic!("invalid decimal"),
                LenEncStr::Ref(s) => (input, ColumnValue::Varchar(Vec::from(s))),
            }
        },
        ColumnMetadata::Bit{..} => {
            let (input, v) = len_enc_str(input)?;
            match v {
                LenEncStr::Null => (input, ColumnValue::Null),
                LenEncStr::Err => panic!("invalid bit"),
                LenEncStr::Ref(s) => (input, ColumnValue::Bit(Vec::from(s))),
            }
        },
        // Timestamp2,
        // DateTime2,
        // Time2,
        // Json,
        ColumnMetadata::NewDecimal{..} => {
            let (input, v) = len_enc_str(input)?;
            match v {
                LenEncStr::Null => (input, ColumnValue::Null),
                LenEncStr::Err => panic!("invalid newdecimal"),
                LenEncStr::Ref(s) => (input, ColumnValue::NewDecimal(Vec::from(s))),
            }
        },
        // Enum,
        // Set,
        // TinyBlob,
        // MediumBlob,
        // LongBlob,
        ColumnMetadata::Blob{..} => {
            let (input, v) = len_enc_str(input)?;
            match v {
                LenEncStr::Null => (input, ColumnValue::Null),
                LenEncStr::Err => panic!("invalid newdecimal"),
                LenEncStr::Ref(s) => (input, ColumnValue::Blob(Vec::from(s))),
            }
        },
        ColumnMetadata::VarString{..} => {
            let (input, v) = len_enc_str(input)?;
            match v {
                LenEncStr::Null => (input, ColumnValue::Null),
                LenEncStr::Err => panic!("invalid newdecimal"),
                LenEncStr::Ref(s) => (input, ColumnValue::VarString(Vec::from(s))),
            }
        },
        ColumnMetadata::String{..} => {
            let (input, v) = len_enc_str(input)?;
            match v {
                LenEncStr::Null => (input, ColumnValue::Null),
                LenEncStr::Err => panic!("invalid newdecimal"),
                LenEncStr::Ref(s) => (input, ColumnValue::String(Vec::from(s))),
            }
        },
        ColumnMetadata::Geometry{..} => {
            let (input, v) = len_enc_str(input)?;
            match v {
                LenEncStr::Null => (input, ColumnValue::Null),
                LenEncStr::Err => panic!("invalid newdecimal"),
                LenEncStr::Ref(s) => (input, ColumnValue::Geometry(Vec::from(s))),
            }
        },
    };
    Ok((input, col_val))
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_bit_xor() {
        let bm1 = 255;
        let bm2 = 252;
        let bmx = bm1 ^ bm2;
        dbg!(bmx);
    }

    #[test]
    fn test_field_types() {
        use nom::error::VerboseError;
        let input: [u8;4] = [0x33, 0x33, 0x23, 0x41];
        let (_, v) = nom::number::streaming::le_f32::<VerboseError<_>>(&input).unwrap();
        println!("{}", v);
        let input: [u8;8] = [0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x24, 0x40];
        let (_, v) = nom::number::streaming::le_f64::<VerboseError<_>>(&input).unwrap();
        println!("{}", v);
    }
}