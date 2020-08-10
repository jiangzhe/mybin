//! defines structure and metadata for mysql columns

use crate::error::Error;
use serde_derive::*;
use std::convert::TryFrom;
use crate::util::bitmap_index;

/// ColumnType defined in binlog
/// 
/// the complete types listed in 
/// https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/export/binary_log_types.h
/// 
/// several types are missing in binlog, refer to: 
/// https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/rows_event.h#L174
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ColumnType {
    Decimal,
    Tiny,
    Short,
    Long,
    Float,
    Double,
    Null,
    Timestamp,
    LongLong,
    Int24,
    Date,
    Time,
    DateTime,
    Year,
    // NewDate,
    Varchar,
    Bit,
    // Timestamp2,
    // DateTime2,
    // Time2,
    // Json,
    NewDecimal,
    // Enum,
    // Set,
    // TinyBlob,
    // MediumBlob,
    // LongBlob,
    Blob,
    VarString,
    String,
    Geometry,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ColumnTypeCode(pub u8);

impl TryFrom<u8> for ColumnType {
    type Error = Error;

    fn try_from(code: u8) -> Result<Self, Self::Error> {
        let ct = match code {
            0x00 => ColumnType::Decimal,
            0x01 => ColumnType::Tiny,
            0x02 => ColumnType::Short,
            0x03 => ColumnType::Long,
            0x04 => ColumnType::Float,
            0x05 => ColumnType::Double,
            0x06 => ColumnType::Null,
            0x07 => ColumnType::Timestamp,
            0x08 => ColumnType::LongLong,
            0x09 => ColumnType::Int24,
            0x0a => ColumnType::Date,
            0x0b => ColumnType::Time,
            0x0c => ColumnType::DateTime,
            0x0d => ColumnType::Year,
            // 0x0e => ColumnType::NewDate,
            0x0f => ColumnType::Varchar,
            0x10 => ColumnType::Bit,
            // 0x11 => ColumnType::Timestamp2,
            // 0x12 => ColumnType::DateTime2,
            // 0x13 => ColumnType::Time2,
            // 0xf5 => ColumnType::Json,
            0xf6 => ColumnType::NewDecimal,
            // 0xf7 => ColumnType::Enum,
            // 0xf8 => ColumnType::Set,
            // 0xf9 => ColumnType::TinyBlob,
            // 0xfa => ColumnType::MediumBlob,
            // 0xfb => ColumnType::LongBlob,
            0xfc => ColumnType::Blob,
            0xfd => ColumnType::VarString,
            0xfe => ColumnType::String,
            0xff => ColumnType::Geometry,
            _ => return Err(Error::InvalidColumnTypeCode(code as u32)),
        };
        Ok(ct)
    }
}

impl TryFrom<ColumnTypeCode> for ColumnType {
    type Error = Error;
    fn try_from(code: ColumnTypeCode) -> Result<Self, Self::Error> {
        ColumnType::try_from(code.0)
    }
}

impl From<ColumnType> for ColumnTypeCode {
    fn from(ct: ColumnType) -> ColumnTypeCode {
        match ct {
            ColumnType::Decimal => ColumnTypeCode(0x00),
            ColumnType::Tiny => ColumnTypeCode(0x01),
            ColumnType::Short => ColumnTypeCode(0x02),
            ColumnType::Long => ColumnTypeCode(0x03),
            ColumnType::Float => ColumnTypeCode(0x04),
            ColumnType::Double => ColumnTypeCode(0x05),
            ColumnType::Null => ColumnTypeCode(0x06),
            ColumnType::Timestamp => ColumnTypeCode(0x07),
            ColumnType::LongLong => ColumnTypeCode(0x08),
            ColumnType::Int24 => ColumnTypeCode(0x09),
            ColumnType::Date => ColumnTypeCode(0x0a),
            ColumnType::Time => ColumnTypeCode(0x0b),
            ColumnType::DateTime => ColumnTypeCode(0x0c),
            ColumnType::Year => ColumnTypeCode(0x0d),
            // ColumnType::NewDate => ColumnTypeCode(0x0e),
            ColumnType::Varchar => ColumnTypeCode(0x0f),
            ColumnType::Bit => ColumnTypeCode(0x10),
            // ColumnType::Timestamp2 => ColumnTypeCode(0x11),
            // ColumnType::DateTime2 => ColumnTypeCode(0x12),
            // ColumnType::Time2 => ColumnTypeCode(0x13),
            // ColumnType::Json => ColumnTypeCode(0xf5),
            ColumnType::NewDecimal => ColumnTypeCode(0xf6),
            // ColumnType::Enum => ColumnTypeCode(0xf7),
            // ColumnType::Set => ColumnTypeCode(0xf8),
            // ColumnType::TinyBlob => ColumnTypeCode(0xf9),
            // ColumnType::MediumBlob => ColumnTypeCode(0xfa),
            // ColumnType::LongBlob => ColumnTypeCode(0xfb),
            ColumnType::Blob => ColumnTypeCode(0xfc),
            ColumnType::VarString => ColumnTypeCode(0xfd),
            ColumnType::String => ColumnTypeCode(0xfe),
            ColumnType::Geometry => ColumnTypeCode(0xff),
        }
    }
}

/// flattened column metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnMetadata {
    Decimal {
        null: bool,
    },
    Tiny {
        null: bool,
    },
    Short {
        null: bool,
    },
    Long {
        null: bool,
    },
    Float {
        pack_len: u8,
        null: bool,
    },
    Double {
        pack_len: u8,
        null: bool,
    },
    Null {
        null: bool,
    },
    Timestamp {
        null: bool,
    },
    LongLong {
        null: bool,
    },
    Int24 {
        null: bool,
    },
    Date {
        null: bool,
    },
    Time {
        null: bool,
    },
    DateTime {
        null: bool,
    },
    Year {
        null: bool,
    },
    // NewDate,
    Varchar {
        max_len: u16,
        null: bool,
    },
    Bit {
        bits: u8,
        bytes: u8,
        null: bool,
    },
    // Timestamp2,
    // DateTime2,
    // Time2,
    // Json,
    NewDecimal {
        precision: u8,
        decimals: u8,
        null: bool,
    },
    // Enum,
    // Set,
    // TinyBlob,
    // MediumBlob,
    // LongBlob,
    Blob {
        pack_len: u8,
        null: bool,
    },
    VarString {
        real_type: u8,
        bytes: u8,
        null: bool,
    },
    String {
        str_type: u8,
        bytes: u8,
        null: bool,
    },
    Geometry {
        pack_len: u8,
        null: bool,
    },
}

pub fn parse_col_metas<'a>(
    col_cnt: usize,
    col_defs: &'a [u8],
    col_meta_defs: &'a [u8],
    null_bitmap: &'a [u8],
) -> Result<Vec<ColumnMetadata>, Error> {
    debug_assert_eq!(col_cnt, col_defs.len());
    debug_assert_eq!((col_cnt + 7) >> 3, null_bitmap.len());
    let mut result = Vec::with_capacity(col_cnt);
    let mut offset = 0;
    for i in 0..col_cnt {
        let null = bitmap_index(null_bitmap, i);
        let col_meta = match ColumnType::try_from(col_defs[i])? {
            ColumnType::Decimal => ColumnMetadata::Decimal { null },
            ColumnType::Tiny => ColumnMetadata::Tiny { null },
            ColumnType::Short => ColumnMetadata::Short { null },
            ColumnType::Long => ColumnMetadata::Long { null },
            ColumnType::Float => {
                let pack_len = col_meta_defs[offset];
                offset += 1;
                ColumnMetadata::Float { pack_len, null }
            }
            ColumnType::Double => {
                let pack_len = col_meta_defs[offset];
                offset += 1;
                ColumnMetadata::Double { pack_len, null }
            }
            ColumnType::Null => ColumnMetadata::Null { null },
            ColumnType::Timestamp => ColumnMetadata::Timestamp { null },
            ColumnType::LongLong => ColumnMetadata::LongLong { null },
            ColumnType::Int24 => ColumnMetadata::Int24 { null },
            ColumnType::Date => ColumnMetadata::Date { null },
            ColumnType::Time => ColumnMetadata::Time { null },
            ColumnType::DateTime => ColumnMetadata::DateTime { null },
            ColumnType::Year => ColumnMetadata::Year { null },
            ColumnType::Varchar => {
                let max_len =
                    col_meta_defs[offset] as u16 + ((col_meta_defs[offset + 1] as u16) << 8);
                offset += 2;
                ColumnMetadata::Varchar { max_len, null }
            }
            ColumnType::Bit => {
                let bits = col_meta_defs[offset];
                let bytes = col_meta_defs[offset + 1];
                offset += 2;
                ColumnMetadata::Bit { bits, bytes, null }
            }
            ColumnType::NewDecimal => {
                let precision = col_meta_defs[offset];
                let decimals = col_meta_defs[offset + 1];
                offset += 2;
                ColumnMetadata::NewDecimal {
                    precision,
                    decimals,
                    null,
                }
            }
            ColumnType::Blob => {
                let pack_len = col_meta_defs[offset];
                offset += 1;
                ColumnMetadata::Blob { pack_len, null }
            }
            ColumnType::VarString => {
                let real_type = col_meta_defs[offset];
                let bytes = col_meta_defs[offset + 1];
                offset += 2;
                ColumnMetadata::VarString {
                    real_type,
                    bytes,
                    null,
                }
            }
            ColumnType::String => {
                let str_type = col_meta_defs[offset];
                let bytes = col_meta_defs[offset + 1];
                offset += 2;
                ColumnMetadata::String {
                    str_type,
                    bytes,
                    null,
                }
            }
            ColumnType::Geometry => {
                let pack_len = col_meta_defs[offset];
                offset += 1;
                ColumnMetadata::Geometry { pack_len, null }
            }
            _ => unimplemented!(),
        };
        result.push(col_meta);
    }
    Ok(result)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnValue {
    Null,
    Decimal(Vec<u8>),
    Tiny(i8),
    Short(i16),
    Long(i32),
    Float(f32),
    Double(f64),
    Timestamp{
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
    },
    LongLong(i64),
    Int24(i32),
    Date{
        year: u16,
        month: u8,
        day: u8,
    },
    Time{
        negative: bool,
        days: u32,
        hours: u8,
        minutes: u8,
        seconds: u8,
        micro_seconds: u32,
    },
    DateTime{
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        micro_second: u32,
    },
    Year(u16),
    Varchar(Vec<u8>),
    Bit(Vec<u8>),
    NewDecimal(Vec<u8>),
    Blob(Vec<u8>),
    VarString(Vec<u8>),
    String(Vec<u8>),
    Geometry(Vec<u8>),
}