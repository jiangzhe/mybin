//! defines structure and metadata for mysql columns
use crate::util::bitmap_index;
use bytes::Bytes;
use bytes_parser::error::{Error, Result};
use bytes_parser::my::{LenEncStr, ReadMyEnc};
use bytes_parser::{ReadBytesExt, ReadFromBytesWithContext};
use std::convert::TryFrom;

/// ColumnType defined in binlog
///
/// the complete types listed in
/// https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/export/binary_log_types.h
///
/// several types are missing in binlog, refer to:
/// https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/rows_event.h#L174
#[derive(Debug, Clone, Copy)]
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
    TinyBlob,
    MediumBlob,
    LongBlob,
    Blob,
    VarString,
    String,
    Geometry,
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnTypeCode(pub u8);

impl TryFrom<u8> for ColumnType {
    type Error = Error;

    fn try_from(code: u8) -> Result<Self> {
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
            0xf9 => ColumnType::TinyBlob,
            0xfa => ColumnType::MediumBlob,
            0xfb => ColumnType::LongBlob,
            0xfc => ColumnType::Blob,
            0xfd => ColumnType::VarString,
            0xfe => ColumnType::String,
            0xff => ColumnType::Geometry,
            _ => {
                return Err(Error::ConstraintError(format!(
                    "invalid column type code: {}",
                    code
                )))
            }
        };
        Ok(ct)
    }
}

impl TryFrom<ColumnTypeCode> for ColumnType {
    type Error = Error;
    fn try_from(code: ColumnTypeCode) -> Result<Self> {
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
            ColumnType::TinyBlob => ColumnTypeCode(0xf9),
            ColumnType::MediumBlob => ColumnTypeCode(0xfa),
            ColumnType::LongBlob => ColumnTypeCode(0xfb),
            ColumnType::Blob => ColumnTypeCode(0xfc),
            ColumnType::VarString => ColumnTypeCode(0xfd),
            ColumnType::String => ColumnTypeCode(0xfe),
            ColumnType::Geometry => ColumnTypeCode(0xff),
        }
    }
}

/// flattened column metadata
#[derive(Debug, Clone)]
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

/// will consume all given bytes
pub fn parse_col_metas(
    col_cnt: usize,
    col_meta_defs: &mut Bytes,
    col_defs: &[u8],
    null_bitmap: &[u8],
) -> Result<Vec<ColumnMetadata>> {
    // debug_assert_eq!(col_cnt, col_defs.len());
    // debug_assert_eq!((col_cnt + 7) >> 3, null_bitmap.len());
    let mut result = Vec::with_capacity(col_cnt);
    // let mut offset = 0;
    for i in 0..col_cnt {
        let null = bitmap_index(null_bitmap, i);
        let col_meta = match ColumnType::try_from(col_defs[i])? {
            ColumnType::Decimal => ColumnMetadata::Decimal { null },
            ColumnType::Tiny => ColumnMetadata::Tiny { null },
            ColumnType::Short => ColumnMetadata::Short { null },
            ColumnType::Long => ColumnMetadata::Long { null },
            ColumnType::Float => {
                // let pack_len = col_meta_defs[offset];
                // offset += 1;
                let pack_len = col_meta_defs.read_u8()?;
                ColumnMetadata::Float { pack_len, null }
            }
            ColumnType::Double => {
                // let pack_len = col_meta_defs[offset];
                // offset += 1;
                let pack_len = col_meta_defs.read_u8()?;
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
                let max_len = col_meta_defs.read_le_u16()?;
                ColumnMetadata::Varchar { max_len, null }
            }
            ColumnType::Bit => {
                let bits = col_meta_defs.read_u8()?;
                let bytes = col_meta_defs.read_u8()?;
                ColumnMetadata::Bit { bits, bytes, null }
            }
            ColumnType::NewDecimal => {
                let precision = col_meta_defs.read_u8()?;
                let decimals = col_meta_defs.read_u8()?;
                ColumnMetadata::NewDecimal {
                    precision,
                    decimals,
                    null,
                }
            }
            ColumnType::TinyBlob
            | ColumnType::MediumBlob
            | ColumnType::LongBlob
            | ColumnType::Blob => {
                let pack_len = col_meta_defs.read_u8()?;
                ColumnMetadata::Blob { pack_len, null }
            }
            ColumnType::VarString => {
                let real_type = col_meta_defs.read_u8()?;
                let bytes = col_meta_defs.read_u8()?;
                ColumnMetadata::VarString {
                    real_type,
                    bytes,
                    null,
                }
            }
            ColumnType::String => {
                // let str_type = col_meta_defs[offset];
                // let bytes = col_meta_defs[offset + 1];
                // offset += 2;
                let str_type = col_meta_defs.read_u8()?;
                let bytes = col_meta_defs.read_u8()?;
                ColumnMetadata::String {
                    str_type,
                    bytes,
                    null,
                }
            }
            ColumnType::Geometry => {
                // let pack_len = col_meta_defs[offset];
                // offset += 1;
                let pack_len = col_meta_defs.read_u8()?;
                ColumnMetadata::Geometry { pack_len, null }
            }
        };
        result.push(col_meta);
    }
    Ok(result)
}

#[derive(Debug, Clone)]
pub enum ColumnValue {
    Null,
    Decimal(Bytes),
    Tiny(i8),
    Short(i16),
    Long(i32),
    Float(f32),
    Double(f64),
    Timestamp {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
    },
    LongLong(i64),
    Int24(i32),
    Date {
        year: u16,
        month: u8,
        day: u8,
    },
    Time {
        negative: bool,
        days: u32,
        hours: u8,
        minutes: u8,
        seconds: u8,
        micro_seconds: u32,
    },
    DateTime {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        micro_second: u32,
    },
    Year(u16),
    Varchar(Bytes),
    Bit(Bytes),
    NewDecimal(Bytes),
    Blob(Bytes),
    VarString(Bytes),
    String(Bytes),
    Geometry(Bytes),
}

impl<'c> ReadFromBytesWithContext<'c> for ColumnValue {
    type Context = &'c ColumnMetadata;

    fn read_with_ctx(input: &mut Bytes, col_meta: Self::Context) -> Result<Self> {
        let col_val = match col_meta {
            ColumnMetadata::Decimal { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => ColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error column value".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => ColumnValue::Decimal(bs),
                }
            }
            ColumnMetadata::Tiny { .. } => ColumnValue::Tiny(input.read_i8()?),
            ColumnMetadata::Short { .. } => ColumnValue::Short(input.read_le_i16()?),
            ColumnMetadata::Long { .. } => ColumnValue::Long(input.read_le_i32()?),
            // todo: pack_len not used?
            ColumnMetadata::Float { .. } => ColumnValue::Float(input.read_le_f32()?),
            // todo: pack_len not used?
            ColumnMetadata::Double { .. } => ColumnValue::Double(input.read_le_f64()?),
            ColumnMetadata::Null { .. } => ColumnValue::Null,
            ColumnMetadata::Timestamp { .. } => {
                let len = input.read_u8()?;
                match len {
                    0 => ColumnValue::Null,
                    7 => {
                        let year = input.read_le_u16()?;
                        let month = input.read_u8()?;
                        let day = input.read_u8()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        ColumnValue::Timestamp {
                            year,
                            month,
                            day,
                            hour,
                            minute,
                            second,
                        }
                    }
                    _ => {
                        return Err(Error::ConstraintError(format!(
                            "invalid length of timestamp: {}",
                            len
                        )))
                    }
                }
            }
            ColumnMetadata::LongLong { .. } => ColumnValue::LongLong(input.read_le_i64()?),
            ColumnMetadata::Int24 { .. } => {
                // here i32 represents i24
                ColumnValue::Int24(input.read_le_i32()?)
            }
            ColumnMetadata::Date { .. } => {
                let len = input.read_u8()?;
                match len {
                    0 => ColumnValue::Null,
                    4 => {
                        let year = input.read_le_u16()?;
                        let month = input.read_u8()?;
                        let day = input.read_u8()?;
                        ColumnValue::Date { year, month, day }
                    }
                    _ => {
                        return Err(Error::ConstraintError(format!(
                            "invalid length of date: {}",
                            len
                        )))
                    }
                }
            }
            ColumnMetadata::Time { .. } => {
                let len = input.read_u8()?;
                match len {
                    0 => ColumnValue::Null,
                    8 => {
                        let negative = input.read_u8()?;
                        let negative = negative == 0x01;
                        let days = input.read_le_u32()?;
                        let hours = input.read_u8()?;
                        let minutes = input.read_u8()?;
                        let seconds = input.read_u8()?;
                        ColumnValue::Time {
                            negative,
                            days,
                            hours,
                            minutes,
                            seconds,
                            micro_seconds: 0,
                        }
                    }
                    12 => {
                        let negative = input.read_u8()?;
                        let negative = negative == 0x01;
                        let days = input.read_le_u32()?;
                        let hours = input.read_u8()?;
                        let minutes = input.read_u8()?;
                        let seconds = input.read_u8()?;
                        let micro_seconds = input.read_le_u32()?;
                        ColumnValue::Time {
                            negative,
                            days,
                            hours,
                            minutes,
                            seconds,
                            micro_seconds,
                        }
                    }
                    _ => {
                        return Err(Error::ConstraintError(format!(
                            "invalid length of time: {}",
                            len
                        )))
                    }
                }
            }
            ColumnMetadata::DateTime { .. } => {
                let len = input.read_u8()?;
                match len {
                    0 => ColumnValue::Null,
                    7 => {
                        let year = input.read_le_u16()?;
                        let month = input.read_u8()?;
                        let day = input.read_u8()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        ColumnValue::DateTime {
                            year,
                            month,
                            day,
                            hour,
                            minute,
                            second,
                            micro_second: 0,
                        }
                    }
                    11 => {
                        let year = input.read_le_u16()?;
                        let month = input.read_u8()?;
                        let day = input.read_u8()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        let micro_second = input.read_le_u32()?;
                        ColumnValue::DateTime {
                            year,
                            month,
                            day,
                            hour,
                            minute,
                            second,
                            micro_second,
                        }
                    }
                    _ => {
                        return Err(Error::ConstraintError(format!(
                            "invalid length of timestamp: {}",
                            len
                        )))
                    }
                }
            }
            ColumnMetadata::Year { .. } => ColumnValue::Year(input.read_le_u16()?),
            // NewDate,
            ColumnMetadata::Varchar { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => ColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error decimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => ColumnValue::Varchar(bs),
                }
            }
            ColumnMetadata::Bit { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => ColumnValue::Null,
                    LenEncStr::Err => return Err(Error::ConstraintError("error bit".to_owned())),
                    LenEncStr::Bytes(bs) => ColumnValue::Bit(bs),
                }
            }
            // Timestamp2,
            // DateTime2,
            // Time2,
            // Json,
            ColumnMetadata::NewDecimal { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => ColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error newdecimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => ColumnValue::NewDecimal(bs),
                }
            }
            // Enum,
            // Set,
            // TinyBlob,
            // MediumBlob,
            // LongBlob,
            ColumnMetadata::Blob { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => ColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error newdecimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => ColumnValue::Blob(bs),
                }
            }
            ColumnMetadata::VarString { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => ColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error newdecimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => ColumnValue::VarString(bs),
                }
            }
            ColumnMetadata::String { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => ColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error newdecimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => ColumnValue::String(bs),
                }
            }
            ColumnMetadata::Geometry { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => ColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error newdecimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => ColumnValue::Geometry(bs),
                }
            }
        };
        Ok(col_val)
    }
}

/// Column definition
///
/// reference: https://dev.mysql.com/doc/internals/en/com-query-response.html
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    // len-enc-str
    pub catalog: String,
    // len-enc-str
    pub schema: String,
    // len-enc-str
    pub table: String,
    // len-enc-str
    pub org_table: String,
    // len-enc-str
    pub name: String,
    // len-enc-str
    pub org_name: String,
    // len-enc-int, always 0x0c
    pub charset: u16,
    pub col_len: u32,
    pub col_type: ColumnType,
    pub flags: u16,
    // 0x00, 0x1f, 0x00-0x51
    pub decimals: u8,
    // 2-byte filler
    // len-enc-str, if COM_FIELD_LIST
    pub default_values: String,
}

impl<'c> ReadFromBytesWithContext<'_> for ColumnDefinition {
    type Context = bool;

    fn read_with_ctx(input: &mut Bytes, field_list: bool) -> Result<Self> {
        let catalog = input.read_len_enc_str()?;
        let catalog = catalog.into_string()?;
        let schema = input.read_len_enc_str()?;
        let schema = schema.into_string()?;
        let table = input.read_len_enc_str()?;
        let table = table.into_string()?;
        let org_table = input.read_len_enc_str()?;
        let org_table = org_table.into_string()?;
        let name = input.read_len_enc_str()?;
        let name = name.into_string()?;
        let org_name = input.read_len_enc_str()?;
        let org_name = org_name.into_string()?;
        // always 0x0c
        input.read_len_enc_int()?;
        let charset = input.read_le_u16()?;
        let col_len = input.read_le_u32()?;
        let col_type = input.read_u8()?;
        let col_type = ColumnType::try_from(col_type)?;
        let flags = input.read_le_u16()?;
        let decimals = input.read_u8()?;
        // two bytes filler
        input.read_len(2)?;
        let default_values = if field_list {
            let default_values = input.read_len_enc_str()?;
            default_values.into_string()?
        } else {
            String::new()
        };
        Ok(ColumnDefinition {
            catalog,
            schema,
            table,
            org_table,
            name,
            org_name,
            charset,
            col_len,
            col_type,
            flags,
            decimals,
            default_values,
        })
    }
}
