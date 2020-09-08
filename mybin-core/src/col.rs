//! defines structure and metadata for mysql columns
use bitflags::bitflags;
use bytes::{Bytes, BytesMut};
use bytes_parser::error::{Error, Result};
use bytes_parser::my::{LenEncStr, ReadMyEnc};
use bytes_parser::{ReadBytesExt, ReadFromBytesWithContext, WriteBytesExt, WriteToBytes};
use std::convert::TryFrom;

/// ColumnType defined in binlog
///
/// the complete types listed in
/// https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/export/binary_log_types.h
///
/// several types are missing in binlog, refer to:
/// https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/rows_event.h#L174
#[derive(Debug, Clone, Copy, PartialEq)]
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

impl From<ColumnType> for u8 {
    fn from(ct: ColumnType) -> u8 {
        match ct {
            ColumnType::Decimal => 0x00,
            ColumnType::Tiny => 0x01,
            ColumnType::Short => 0x02,
            ColumnType::Long => 0x03,
            ColumnType::Float => 0x04,
            ColumnType::Double => 0x05,
            ColumnType::Null => 0x06,
            ColumnType::Timestamp => 0x07,
            ColumnType::LongLong => 0x08,
            ColumnType::Int24 => 0x09,
            ColumnType::Date => 0x0a,
            ColumnType::Time => 0x0b,
            ColumnType::DateTime => 0x0c,
            ColumnType::Year => 0x0d,
            // ColumnType::NewDate => ColumnTypeCode(0x0e),
            ColumnType::Varchar => 0x0f,
            ColumnType::Bit => 0x10,
            // ColumnType::Timestamp2 => ColumnTypeCode(0x11),
            // ColumnType::DateTime2 => ColumnTypeCode(0x12),
            // ColumnType::Time2 => ColumnTypeCode(0x13),
            // ColumnType::Json => ColumnTypeCode(0xf5),
            ColumnType::NewDecimal => 0xf6,
            // ColumnType::Enum => ColumnTypeCode(0xf7),
            // ColumnType::Set => ColumnTypeCode(0xf8),
            ColumnType::TinyBlob => 0xf9,
            ColumnType::MediumBlob => 0xfa,
            ColumnType::LongBlob => 0xfb,
            ColumnType::Blob => 0xfc,
            ColumnType::VarString => 0xfd,
            ColumnType::String => 0xfe,
            ColumnType::Geometry => 0xff,
        }
    }
}

impl From<&ColumnMeta> for ColumnType {
    fn from(src: &ColumnMeta) -> Self {
        match src {
            ColumnMeta::Decimal => ColumnType::Decimal,
            ColumnMeta::Tiny => ColumnType::Tiny,
            ColumnMeta::Short => ColumnType::Short,
            ColumnMeta::Long => ColumnType::Long,
            // todo: pack_len not used?
            ColumnMeta::Float { .. } => ColumnType::Float,
            // todo: pack_len not used?
            ColumnMeta::Double { .. } => ColumnType::Double,
            ColumnMeta::Null => ColumnType::Null,
            ColumnMeta::Timestamp => ColumnType::Timestamp,
            ColumnMeta::LongLong => ColumnType::LongLong,
            ColumnMeta::Int24 => ColumnType::Int24,
            ColumnMeta::Date => ColumnType::Date,
            ColumnMeta::Time => ColumnType::Time,
            ColumnMeta::DateTime => ColumnType::DateTime,
            ColumnMeta::Year => ColumnType::Year,
            // NewDate,
            ColumnMeta::Varchar { .. } => ColumnType::Varchar,
            ColumnMeta::Bit { .. } => ColumnType::Bit,
            // Timestamp2,
            // DateTime2,
            // Time2,
            // Json,
            ColumnMeta::NewDecimal { .. } => ColumnType::NewDecimal,
            // Enum,
            // Set,
            // TinyBlob,
            // MediumBlob,
            // LongBlob,
            ColumnMeta::Blob { .. } => ColumnType::Blob,
            ColumnMeta::VarString { .. } => ColumnType::VarString,
            ColumnMeta::String { .. } => ColumnType::String,
            ColumnMeta::Geometry { .. } => ColumnType::Geometry,
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
#[derive(Debug, Clone, PartialEq)]
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
        hour: u8,
        minute: u8,
        second: u8,
        micro_second: u32,
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

impl ReadFromBytesWithContext<'_> for BinaryColumnValue {
    type Context = ColumnType;

    fn read_with_ctx(input: &mut Bytes, col_type: Self::Context) -> Result<Self> {
        let col_val = match col_type {
            ColumnType::Decimal => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error column value".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::Decimal(bs),
                }
            }
            ColumnType::Tiny => BinaryColumnValue::Tiny(input.read_u8()?),
            ColumnType::Short => BinaryColumnValue::Short(input.read_le_u16()?),
            ColumnType::Long => BinaryColumnValue::Long(input.read_le_u32()?),
            ColumnType::Float => BinaryColumnValue::Float(input.read_le_f32()?),
            ColumnType::Double => BinaryColumnValue::Double(input.read_le_f64()?),
            ColumnType::Null => BinaryColumnValue::Null,
            ColumnType::Timestamp => {
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
            ColumnType::LongLong => BinaryColumnValue::LongLong(input.read_le_u64()?),
            ColumnType::Int24 => {
                // here i32 represents i24
                BinaryColumnValue::Int24(input.read_le_u32()?)
            }
            ColumnType::Date => {
                let len = input.read_u8()?;
                match len {
                    0 => BinaryColumnValue::Date {
                        year: 0,
                        month: 0,
                        day: 0,
                    },
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
            ColumnType::Time => {
                let len = input.read_u8()?;
                match len {
                    0 => BinaryColumnValue::Time {
                        negative: false,
                        days: 0,
                        hour: 0,
                        minute: 0,
                        second: 0,
                        micro_second: 0,
                    },
                    8 => {
                        let negative = input.read_u8()?;
                        let negative = negative == 0x01;
                        let days = input.read_le_u32()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        BinaryColumnValue::Time {
                            negative,
                            days,
                            hour,
                            minute,
                            second,
                            micro_second: 0,
                        }
                    }
                    12 => {
                        let negative = input.read_u8()?;
                        let negative = negative == 0x01;
                        let days = input.read_le_u32()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        let micro_second = input.read_le_u32()?;
                        BinaryColumnValue::Time {
                            negative,
                            days,
                            hour,
                            minute,
                            second,
                            micro_second,
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
            ColumnType::DateTime => {
                let len = input.read_u8()?;
                match len {
                    0 => BinaryColumnValue::DateTime {
                        year: 0,
                        month: 0,
                        day: 0,
                        hour: 0,
                        minute: 0,
                        second: 0,
                        micro_second: 0,
                    },
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
            ColumnType::Year => BinaryColumnValue::Year(input.read_le_u16()?),
            // NewDate,
            ColumnType::Varchar => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error decimal".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::Varchar(bs),
                }
            }
            ColumnType::Bit => {
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
            ColumnType::NewDecimal => {
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
            ColumnType::TinyBlob
            | ColumnType::MediumBlob
            | ColumnType::LongBlob
            | ColumnType::Blob => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => return Err(Error::ConstraintError("error blob".to_owned())),
                    LenEncStr::Bytes(bs) => BinaryColumnValue::Blob(bs),
                }
            }
            ColumnType::VarString => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error varstring".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::VarString(bs),
                }
            }
            ColumnType::String => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error string".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::String(bs),
                }
            }
            ColumnType::Geometry => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error geometry".to_owned()))
                    }
                    LenEncStr::Bytes(bs) => BinaryColumnValue::Geometry(bs),
                }
            }
        };
        Ok(col_val)
    }
}

impl WriteToBytes for BinaryColumnValue {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        match self {
            BinaryColumnValue::Decimal(bs)
            | BinaryColumnValue::Varchar(bs)
            | BinaryColumnValue::Bit(bs)
            | BinaryColumnValue::NewDecimal(bs)
            | BinaryColumnValue::Blob(bs)
            | BinaryColumnValue::VarString(bs)
            | BinaryColumnValue::String(bs)
            | BinaryColumnValue::Geometry(bs) => {
                let les: LenEncStr = LenEncStr::Bytes(bs);
                len += out.write_bytes(les)?;
            }
            BinaryColumnValue::Tiny(n) => len += out.write_u8(n)?,
            BinaryColumnValue::Short(n) => len += out.write_le_u16(n)?,
            BinaryColumnValue::Long(n) => len += out.write_le_u32(n)?,
            BinaryColumnValue::Float(n) => len += out.write_le_f32(n)?,
            BinaryColumnValue::Double(n) => len += out.write_le_f64(n)?,
            // nothing to append for null value, already indicated in null-bitmap
            BinaryColumnValue::Null => (),
            BinaryColumnValue::Timestamp {
                year,
                month,
                day,
                hour,
                minute,
                second,
            } => {
                // length of 7
                len += out.write_u8(7)?;
                len += out.write_le_u16(year)?;
                len += out.write_u8(month)?;
                len += out.write_u8(day)?;
                len += out.write_u8(hour)?;
                len += out.write_u8(minute)?;
                len += out.write_u8(second)?;
            }
            BinaryColumnValue::LongLong(n) => len += out.write_le_u64(n)?,
            // special handling on int24
            BinaryColumnValue::Int24(n) => len += out.write_le_u32(n)?,
            BinaryColumnValue::Date { year, month, day } => {
                // length of 4
                len += out.write_u8(4)?;
                len += out.write_le_u16(year)?;
                len += out.write_u8(month)?;
                len += out.write_u8(day)?;
            }
            BinaryColumnValue::Time {
                negative,
                days,
                hour,
                minute,
                second,
                micro_second,
            } => {
                if days | hour as u32 | minute as u32 | second as u32 | micro_second == 0 {
                    len += out.write_u8(0)?;
                } else if micro_second == 0 {
                    len += out.write_u8(8)?;
                    len += out.write_u8(if negative { 1 } else { 0 })?;
                    len += out.write_le_u32(days)?;
                    len += out.write_u8(hour)?;
                    len += out.write_u8(minute)?;
                    len += out.write_u8(second)?;
                } else {
                    len += out.write_u8(12)?;
                    len += out.write_u8(if negative { 1 } else { 0 })?;
                    len += out.write_le_u32(days)?;
                    len += out.write_u8(hour)?;
                    len += out.write_u8(minute)?;
                    len += out.write_u8(second)?;
                    len += out.write_le_u32(micro_second)?;
                }
            }
            BinaryColumnValue::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micro_second,
            } => {
                if year as u32
                    | month as u32
                    | day as u32
                    | hour as u32
                    | minute as u32
                    | second as u32
                    | micro_second
                    == 0
                {
                    len += out.write_u8(0)?;
                } else if hour as u32 | minute as u32 | second as u32 | micro_second == 0 {
                    len += out.write_u8(4)?;
                    len += out.write_le_u16(year)?;
                    len += out.write_u8(month)?;
                    len += out.write_u8(day)?;
                } else if micro_second == 0 {
                    len += out.write_u8(7)?;
                    len += out.write_le_u16(year)?;
                    len += out.write_u8(month)?;
                    len += out.write_u8(day)?;
                    len += out.write_u8(hour)?;
                    len += out.write_u8(minute)?;
                    len += out.write_u8(second)?;
                } else {
                    len += out.write_u8(11)?;
                    len += out.write_le_u16(year)?;
                    len += out.write_u8(month)?;
                    len += out.write_u8(day)?;
                    len += out.write_u8(hour)?;
                    len += out.write_u8(minute)?;
                    len += out.write_u8(second)?;
                    len += out.write_le_u32(micro_second)?;
                }
            }
            BinaryColumnValue::Year(n) => len += out.write_le_u16(n)?,
        };
        Ok(len)
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
