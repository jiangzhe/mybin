//! defines structure and metadata for mysql columns
use bitflags::bitflags;
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

#[derive(Debug, Clone)]
pub struct ColumnMetas(pub Vec<ColumnMeta>);

impl<'c> ReadFromBytesWithContext<'c> for ColumnMetas {
    // bitmap may be longer than the size
    type Context = (usize, &'c [u8]);
    fn read_with_ctx(input: &mut Bytes, (col_cnt, col_defs): Self::Context) -> Result<Self> {
        let mut col_metas = Vec::with_capacity(col_cnt);
        for i in 0..col_cnt {
            let col_type = ColumnType::try_from(col_defs[i])?;
            let col_meta = ColumnMeta::read_with_ctx(input, col_type)?;
            col_metas.push(col_meta);
        }
        Ok(ColumnMetas(col_metas))
    }
}

impl std::ops::Deref for ColumnMetas {
    type Target = [ColumnMeta];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ColumnMetas {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone)]
pub enum ColumnMeta {
    Decimal,
    Tiny,
    Short,
    Long,
    Float { pack_len: u8 },
    Double { pack_len: u8 },
    Null,
    Timestamp,
    LongLong,
    Int24,
    Date,
    Time,
    DateTime,
    Year,
    // NewDate,
    Varchar { max_len: u16 },
    Bit { bits: u8, bytes: u8 },
    // Timestamp2,
    // DateTime2,
    // Time2,
    // Json,
    NewDecimal { precision: u8, decimals: u8 },
    // Enum,
    // Set,
    // TinyBlob,
    // MediumBlob,
    // LongBlob,
    Blob { pack_len: u8 },
    VarString { real_type: u8, bytes: u8 },
    String { str_type: u8, bytes: u8 },
    Geometry { pack_len: u8 },
}

impl ReadFromBytesWithContext<'_> for ColumnMeta {
    type Context = ColumnType;

    fn read_with_ctx(input: &mut Bytes, col_type: Self::Context) -> Result<Self> {
        let col_meta = match col_type {
            ColumnType::Decimal => ColumnMeta::Decimal,
            ColumnType::Tiny => ColumnMeta::Tiny,
            ColumnType::Short => ColumnMeta::Short,
            ColumnType::Long => ColumnMeta::Long,
            ColumnType::Float => {
                let pack_len = input.read_u8()?;
                ColumnMeta::Float { pack_len }
            }
            ColumnType::Double => {
                let pack_len = input.read_u8()?;
                ColumnMeta::Double { pack_len }
            }
            ColumnType::Null => ColumnMeta::Null,
            ColumnType::Timestamp => ColumnMeta::Timestamp,
            ColumnType::LongLong => ColumnMeta::LongLong,
            ColumnType::Int24 => ColumnMeta::Int24,
            ColumnType::Date => ColumnMeta::Date,
            ColumnType::Time => ColumnMeta::Time,
            ColumnType::DateTime => ColumnMeta::DateTime,
            ColumnType::Year => ColumnMeta::Year,
            ColumnType::Varchar => {
                let max_len = input.read_le_u16()?;
                ColumnMeta::Varchar { max_len }
            }
            ColumnType::Bit => {
                let bits = input.read_u8()?;
                let bytes = input.read_u8()?;
                ColumnMeta::Bit { bits, bytes }
            }
            ColumnType::NewDecimal => {
                let precision = input.read_u8()?;
                let decimals = input.read_u8()?;
                ColumnMeta::NewDecimal {
                    precision,
                    decimals,
                }
            }
            ColumnType::TinyBlob
            | ColumnType::MediumBlob
            | ColumnType::LongBlob
            | ColumnType::Blob => {
                let pack_len = input.read_u8()?;
                ColumnMeta::Blob { pack_len }
            }
            ColumnType::VarString => {
                let real_type = input.read_u8()?;
                let bytes = input.read_u8()?;
                ColumnMeta::VarString { real_type, bytes }
            }
            ColumnType::String => {
                let str_type = input.read_u8()?;
                let bytes = input.read_u8()?;
                ColumnMeta::String { str_type, bytes }
            }
            ColumnType::Geometry => {
                let pack_len = input.read_u8()?;
                ColumnMeta::Geometry { pack_len }
            }
        };
        Ok(col_meta)
    }
}

/// column value parsed from binary protocol
///
/// All numeric columns are treated as unsigned.
/// Use extractor or mapper to get actual value.
#[derive(Debug, Clone)]
pub enum BinaryColumnValue {
    Null,
    Decimal(Bytes),
    Tiny(u8),
    Short(u16),
    Long(u32),
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
    LongLong(u64),
    Int24(u32),
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

impl<'c> ReadFromBytesWithContext<'c> for BinaryColumnValue {
    type Context = &'c ColumnMeta;

    fn read_with_ctx(input: &mut Bytes, col_meta: Self::Context) -> Result<Self> {
        let col_val = match col_meta {
            ColumnMeta::Decimal => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error column value".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::Decimal(bs),
                }
            }
            ColumnMeta::Tiny => BinaryColumnValue::Tiny(input.read_u8()?),
            ColumnMeta::Short => BinaryColumnValue::Short(input.read_le_u16()?),
            ColumnMeta::Long => BinaryColumnValue::Long(input.read_le_u32()?),
            // todo: pack_len not used?
            ColumnMeta::Float { .. } => BinaryColumnValue::Float(input.read_le_f32()?),
            // todo: pack_len not used?
            ColumnMeta::Double { .. } => BinaryColumnValue::Double(input.read_le_f64()?),
            ColumnMeta::Null => BinaryColumnValue::Null,
            ColumnMeta::Timestamp => {
                let len = input.read_u8()?;
                match len {
                    0 => BinaryColumnValue::Null,
                    7 => {
                        let year = input.read_le_u16()?;
                        let month = input.read_u8()?;
                        let day = input.read_u8()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        BinaryColumnValue::Timestamp {
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
            ColumnMeta::LongLong => BinaryColumnValue::LongLong(input.read_le_u64()?),
            ColumnMeta::Int24 => {
                // here i32 represents i24
                BinaryColumnValue::Int24(input.read_le_u32()?)
            }
            ColumnMeta::Date => {
                let len = input.read_u8()?;
                match len {
                    0 => BinaryColumnValue::Null,
                    4 => {
                        let year = input.read_le_u16()?;
                        let month = input.read_u8()?;
                        let day = input.read_u8()?;
                        BinaryColumnValue::Date { year, month, day }
                    }
                    _ => {
                        return Err(Error::ConstraintError(format!(
                            "invalid length of date: {}",
                            len
                        )))
                    }
                }
            }
            ColumnMeta::Time => {
                let len = input.read_u8()?;
                match len {
                    0 => BinaryColumnValue::Null,
                    8 => {
                        let negative = input.read_u8()?;
                        let negative = negative == 0x01;
                        let days = input.read_le_u32()?;
                        let hours = input.read_u8()?;
                        let minutes = input.read_u8()?;
                        let seconds = input.read_u8()?;
                        BinaryColumnValue::Time {
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
                        BinaryColumnValue::Time {
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
            ColumnMeta::DateTime => {
                let len = input.read_u8()?;
                match len {
                    0 => BinaryColumnValue::Null,
                    7 => {
                        let year = input.read_le_u16()?;
                        let month = input.read_u8()?;
                        let day = input.read_u8()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        BinaryColumnValue::DateTime {
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
                        BinaryColumnValue::DateTime {
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
            ColumnMeta::Year => BinaryColumnValue::Year(input.read_le_u16()?),
            // NewDate,
            ColumnMeta::Varchar { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error decimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::Varchar(bs),
                }
            }
            ColumnMeta::Bit { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => return Err(Error::ConstraintError("error bit".to_owned())),
                    LenEncStr::Bytes(bs) => BinaryColumnValue::Bit(bs),
                }
            }
            // Timestamp2,
            // DateTime2,
            // Time2,
            // Json,
            ColumnMeta::NewDecimal { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error newdecimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::NewDecimal(bs),
                }
            }
            // Enum,
            // Set,
            // TinyBlob,
            // MediumBlob,
            // LongBlob,
            ColumnMeta::Blob { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error newdecimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::Blob(bs),
                }
            }
            ColumnMeta::VarString { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error newdecimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::VarString(bs),
                }
            }
            ColumnMeta::String { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error newdecimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::String(bs),
                }
            }
            ColumnMeta::Geometry { .. } => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error newdecimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::Geometry(bs),
                }
            }
        };
        Ok(col_val)
    }
}

/// column value parsed from text protocol
pub type TextColumnValue = Option<Bytes>;

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
    pub flags: ColumnFlags,
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
        let flags = ColumnFlags::from_bits(flags)
            .ok_or_else(|| Error::ConstraintError(format!("invalid column flags {}", flags)))?;
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

bitflags! {
    /// flags of column
    ///
    /// the actual column flags is u32, but truncate to u16 to send to client
    ///
    /// references:
    /// https://github.com/mysql/mysql-server/blob/5.7/sql/field.h#L4504
    /// https://github.com/mysql/mysql-server/blob/5.7/sql/protocol_classic.cc#L1163
    pub struct ColumnFlags: u16 {
        const NOT_NULL      = 0x0001;
        const PRIMARY_KEY   = 0x0002;
        const UNIQUE_KEY    = 0x0004;
        const MULTIPLE_KEY  = 0x0008;
        const BLOB          = 0x0010;
        const UNSIGNED      = 0x0020;
        const ZEROFILL      = 0x0040;
        const BINARY        = 0x0080;
        const ENUM          = 0x0100;
        const AUTO_INCREMENT    = 0x0200;
        const TIMESTAMP     = 0x0400;
        const SET           = 0x0800;
        const NO_DEFAULT_VALUE  = 0x1000;
        const ON_UPDATE_NOW = 0x2000;
        const NUM           = 0x4000;
        const PART_KEY      = 0x8000;
    }
}
