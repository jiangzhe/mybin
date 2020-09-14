//! defines structure and metadata for mysql columns
use crate::decimal::MyDecimal;
use crate::time::{MyTime, MyDateTime};
use bitflags::bitflags;
use bytes::{Bytes, BytesMut};
use bytes_parser::error::{Error, Result};
use bytes_parser::my::{LenEncStr, ReadMyEnc};
use bytes_parser::{ReadBytesExt, WriteBytesExt, WriteToBytes};
use smol_str::SmolStr;
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
    Timestamp2,
    DateTime2,
    Time2,
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
            0x11 => ColumnType::Timestamp2,
            0x12 => ColumnType::DateTime2,
            0x13 => ColumnType::Time2,
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
            ColumnType::Timestamp2 => 0x11,
            ColumnType::DateTime2 => 0x12,
            ColumnType::Time2 => 0x13,
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
            ColumnMeta::Timestamp{..} => ColumnType::Timestamp,
            ColumnMeta::LongLong => ColumnType::LongLong,
            ColumnMeta::Int24 => ColumnType::Int24,
            ColumnMeta::Date => ColumnType::Date,
            ColumnMeta::Time => ColumnType::Time,
            ColumnMeta::DateTime { .. } => ColumnType::DateTime,
            ColumnMeta::Year => ColumnType::Year,
            // NewDate,
            // ColumnMeta::Varchar { .. } => ColumnType::Varchar,
            ColumnMeta::Bit { .. } => ColumnType::Bit,
            // Timestamp2,
            // DateTime2,
            ColumnMeta::Time2 { .. } => ColumnType::Time2,
            // Json,
            ColumnMeta::NewDecimal { .. } => ColumnType::NewDecimal,
            ColumnMeta::Enum { .. } => ColumnType::String,
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

impl ColumnMetas {
    // bitmap may be longer than the size
    pub fn read_from(input: &mut Bytes, col_cnt: usize, col_defs: &[u8]) -> Result<Self> {
        let mut col_metas = Vec::with_capacity(col_cnt);
        for i in 0..col_cnt {
            let col_type = ColumnType::try_from(col_defs[i])?;
            let col_meta = ColumnMeta::read_from(input, col_type)?;
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

/// Column meta in TableMapEvent
///
/// Usage in comments:
/// https://github.com/mysql/mysql-server/blob/5.7/sql/log_event.cc#L11716
#[derive(Debug, Clone)]
pub enum ColumnMeta {
    Decimal,
    Tiny,
    Short,
    Long,
    Float { pack_len: u8 },
    Double { pack_len: u8 },
    Null,
    Timestamp { frac: u8 },
    LongLong,
    Int24,
    Date,
    // should be deprecated
    Time,
    DateTime { frac: u8 },
    Year,
    // NewDate,
    // Varchar { max_len: u16 },
    Bit { bits: u8, bytes: u8 },
    // Timestamp2,
    // DateTime2,
    Time2 { frac: u8 },
    // Json,
    NewDecimal { prec: u8, frac: u8 },
    // Enum is acually encoded in real_type of String type
    Enum { pack_len: u8 },
    // Set,
    // TinyBlob,
    // MediumBlob,
    // LongBlob,
    Blob { pack_len: u8 },
    VarString { max_len: u16 },
    // string length < 1024
    // from_len will determine the pack length of actual
    // value is 1 byte(<256) or 2 bytes(>=256)
    String { from_len: u16 },
    Geometry { pack_len: u8 },
}

impl ColumnMeta {
    pub fn read_from(input: &mut Bytes, col_type: ColumnType) -> Result<Self> {
        let col_meta = match col_type {
            ColumnType::Decimal => ColumnMeta::Decimal,
            ColumnType::Tiny => ColumnMeta::Tiny,
            ColumnType::Short => ColumnMeta::Short,
            ColumnType::Long => ColumnMeta::Long,
            ColumnType::Float => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L4771
                let pack_len = input.read_u8()?;
                ColumnMeta::Float { pack_len }
            }
            ColumnType::Double => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L5083
                let pack_len = input.read_u8()?;
                ColumnMeta::Double { pack_len }
            }
            ColumnType::Null => ColumnMeta::Null,
            ColumnType::Timestamp => unimplemented!(),
            ColumnType::Timestamp2 => {
                let frac = input.read_u8()?;
                ColumnMeta::Timestamp{ frac }
            }
            ColumnType::LongLong => ColumnMeta::LongLong,
            ColumnType::Int24 => ColumnMeta::Int24,
            ColumnType::Date => ColumnMeta::Date,
            ColumnType::Time => unimplemented!(),
            ColumnType::Time2 => {
                let frac = input.read_u8()?;
                ColumnMeta::Time2{ frac }
            }
            ColumnType::DateTime => unimplemented!(),
            ColumnType::DateTime2 => {
                let frac = input.read_u8()?;
                ColumnMeta::DateTime{ frac }
            }
            ColumnType::Year => ColumnMeta::Year,
            ColumnType::Bit => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L10093
                // bits = field_length % 8
                // bytes = field_length / 8
                let bits = input.read_u8()?;
                let bytes = input.read_u8()?;
                ColumnMeta::Bit { bits, bytes }
            }
            ColumnType::NewDecimal => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L3251
                let prec = input.read_u8()?;
                let frac = input.read_u8()?;
                ColumnMeta::NewDecimal { prec, frac }
            }
            ColumnType::TinyBlob
            | ColumnType::MediumBlob
            | ColumnType::LongBlob
            | ColumnType::Blob => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L8490
                let pack_len = input.read_u8()?;
                ColumnMeta::Blob { pack_len }
            }
            ColumnType::Varchar | ColumnType::VarString => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L7583
                let max_len = input.read_le_u16()?;
                ColumnMeta::VarString { max_len }
            }
            ColumnType::String => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L7419
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L7487
                // very tricky encoding
                let real_type = input.read_u8()?;
                let field_len = input.read_u8()?;
                match real_type | 0xf0 {
                    0xf7 => ColumnMeta::Enum {
                        pack_len: field_len,
                    },
                    0xfe => {
                        let from_len =
                            (((((real_type >> 4) & 0x03) ^ 0x03) as u16) << 8) + field_len as u16;
                        ColumnMeta::String { from_len }
                    }
                    _ => unimplemented!("real type 0x{:x} is not implemented", real_type | 0xf0),
                }
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
    // Decimal(Bytes),
    Tiny(u8),
    Short(u16),
    Long(u32),
    Float(f32),
    Double(f64),
    Timestamp(MyDateTime),
    LongLong(u64),
    Int24(u32),
    Date {
        year: u16,
        month: u8,
        day: u8,
    },
    Time(MyTime),
    DateTime(MyDateTime),
    Year(u16),
    // Varchar(Bytes),
    Bit(Bytes),
    NewDecimal(Bytes),
    // Enum(Bytes),
    Blob(Bytes),
    VarString(Bytes),
    String(Bytes),
    Geometry(Bytes),
}

/// binary protocol
impl BinaryColumnValue {
    /// input may be longer than the column value
    pub fn read_from(input: &mut Bytes, col_type: ColumnType) -> Result<Self> {
        let col_val = match col_type {
            ColumnType::Decimal => {
                let v = input.read_len_enc_str()?;
                match v {
                    LenEncStr::Null => BinaryColumnValue::Null,
                    LenEncStr::Err => {
                        return Err(Error::ConstraintError("error column value".to_owned()))
                    }
                    // prefer NewDecimal to Decimal
                    LenEncStr::Bytes(bs) => BinaryColumnValue::NewDecimal(bs),
                }
            }
            ColumnType::Tiny => BinaryColumnValue::Tiny(input.read_u8()?),
            ColumnType::Short => BinaryColumnValue::Short(input.read_le_u16()?),
            ColumnType::Long => BinaryColumnValue::Long(input.read_le_u32()?),
            ColumnType::Float => BinaryColumnValue::Float(input.read_le_f32()?),
            ColumnType::Double => BinaryColumnValue::Double(input.read_le_f64()?),
            ColumnType::Null => BinaryColumnValue::Null,
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
            ColumnType::Time | ColumnType::Time2 => {
                let len = input.read_u8()?;
                match len {
                    0 => BinaryColumnValue::Time(MyTime{
                        negative: false,
                        days: 0,
                        hour: 0,
                        minute: 0,
                        second: 0,
                        micro_second: 0,
                    }),
                    8 => {
                        let negative = input.read_u8()?;
                        let negative = negative == 0x01;
                        let days = input.read_le_u32()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        BinaryColumnValue::Time(MyTime{
                            negative,
                            days,
                            hour,
                            minute,
                            second,
                            micro_second: 0,
                        })
                    }
                    12 => {
                        let negative = input.read_u8()?;
                        let negative = negative == 0x01;
                        let days = input.read_le_u32()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        let micro_second = input.read_le_u32()?;
                        BinaryColumnValue::Time(MyTime{
                            negative,
                            days,
                            hour,
                            minute,
                            second,
                            micro_second,
                        })
                    }
                    _ => {
                        return Err(Error::ConstraintError(format!(
                            "invalid length of time: {}",
                            len
                        )))
                    }
                }
            }
            ColumnType::Timestamp | ColumnType::Timestamp2 | ColumnType::DateTime | ColumnType::DateTime2 => {
                let len = input.read_u8()?;
                match len {
                    0 => BinaryColumnValue::DateTime(MyDateTime{
                        year: 0,
                        month: 0,
                        day: 0,
                        hour: 0,
                        minute: 0,
                        second: 0,
                        micro_second: 0,
                    }),
                    4 => {
                        let year = input.read_le_u16()?;
                        let month = input.read_u8()?;
                        let day = input.read_u8()?;
                        BinaryColumnValue::DateTime(MyDateTime{
                            year,
                            month,
                            day,
                            hour: 0,
                            minute: 0,
                            second: 0,
                            micro_second: 0,
                        })
                    }
                    7 => {
                        let year = input.read_le_u16()?;
                        let month = input.read_u8()?;
                        let day = input.read_u8()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        BinaryColumnValue::DateTime(MyDateTime{
                            year,
                            month,
                            day,
                            hour,
                            minute,
                            second,
                            micro_second: 0,
                        })
                    }
                    11 => {
                        let year = input.read_le_u16()?;
                        let month = input.read_u8()?;
                        let day = input.read_u8()?;
                        let hour = input.read_u8()?;
                        let minute = input.read_u8()?;
                        let second = input.read_u8()?;
                        let micro_second = input.read_le_u32()?;
                        BinaryColumnValue::DateTime(MyDateTime{
                            year,
                            month,
                            day,
                            hour,
                            minute,
                            second,
                            micro_second,
                        })
                    }
                    _ => {
                        return Err(Error::ConstraintError(format!(
                            "invalid length of datetime: {}",
                            len
                        )))
                    }
                }
            }
            ColumnType::Year => BinaryColumnValue::Year(input.read_le_u16()?),
            // NewDate,
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
            ColumnType::Varchar | ColumnType::VarString => {
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
            // BinaryColumnValue::Decimal(bs)
            BinaryColumnValue::Bit(bs)
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
            BinaryColumnValue::Timestamp(MyDateTime{
                year,
                month,
                day,
                hour,
                minute,
                second,
                ..
            }) => {
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
            BinaryColumnValue::Time(MyTime{
                negative,
                days,
                hour,
                minute,
                second,
                micro_second,
            }) => {
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
            BinaryColumnValue::DateTime(MyDateTime{
                year,
                month,
                day,
                hour,
                minute,
                second,
                micro_second,
            }) => {
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

/// column value parsed from binlog protocol
#[derive(Debug, Clone)]
pub enum BinlogColumnValue {
    Null,
    // Decimal(Bytes),
    Tiny(u8),
    Short(u16),
    Long(u32),
    Float(f32),
    Double(f64),
    // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L5657
    Timestamp(u32),
    LongLong(u64),
    Int24(u32),
    Date {
        year: u16,
        month: u8,
        day: u8,
    },
    Time(MyTime),
    DateTime(MyDateTime),
    Year(u16),
    // Varchar(Bytes),
    Bit(Bytes),
    NewDecimal(MyDecimal),
    Enum(MyEnum),
    Blob(Bytes),
    VarString(Bytes),
    String(Bytes),
    Geometry(Bytes),
}

#[derive(Debug, Clone)]
pub enum MyEnum {
    Pack1(u8),
    Pack2(u16),
    Pack3(u32),
    Pack4(u32),
    Pack8(u64),
}

impl BinlogColumnValue {
    /// read bytes based on binlog protocol
    ///
    /// binlog protocol use separate column meta to distinguish different types
    pub fn read_from(input: &mut Bytes, col_meta: &ColumnMeta) -> Result<Self> {
        let col_val = match col_meta {
            ColumnMeta::Decimal => unimplemented!(),
            ColumnMeta::Tiny => BinlogColumnValue::Tiny(input.read_u8()?),
            ColumnMeta::Short => BinlogColumnValue::Short(input.read_le_u16()?),
            ColumnMeta::Long => BinlogColumnValue::Long(input.read_le_u32()?),
            ColumnMeta::Float { .. } => BinlogColumnValue::Float(input.read_le_f32()?),
            ColumnMeta::Double { .. } => BinlogColumnValue::Double(input.read_le_f64()?),
            ColumnMeta::Null => BinlogColumnValue::Null,
            ColumnMeta::Timestamp{..} => {
                let secs = input.read_le_u32()?;
                BinlogColumnValue::Timestamp(secs)
            }
            ColumnMeta::LongLong => BinlogColumnValue::LongLong(input.read_le_u64()?),
            // in binlog int24 is stored as 3-byte integer
            ColumnMeta::Int24 => BinlogColumnValue::Int24(input.read_le_u24()?),
            ColumnMeta::Date => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L6519
                let n = input.read_le_u24()?;
                // day: 5 bits, month: 4 bits, year: the rest
                let day = (n & 31) as u8;
                let month = ((n >> 5) & 15) as u8;
                let year = (n >> 9) as u16;
                BinlogColumnValue::Date{
                    year,
                    month,
                    day
                }
            }
            ColumnMeta::Time => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L6173
                let mut n = input.read_le_u24()?;
                let negative = n & 0x80_0000 == 0x80_0000;
                if negative {
                    n = (-((n | 0xff00_0000) as i32)) as u32;
                }
                let second = (n % 100) as u8;
                let minute = ((n / 100) % 100) as u8;
                let hours = n / 10000;
                let days = hours / 24;
                let hour = (hours - days * 24) as u8;
                BinlogColumnValue::Time(MyTime{
                    negative,
                    days,
                    hour,
                    minute,
                    second,
                    micro_second: 0,
                })
            }
            ColumnMeta::Time2{ frac } => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c#L1654
                // https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c#L1640
                Self::Time(MyTime::from_binlog(input, *frac as usize)?)                
            }
            ColumnMeta::DateTime { frac } => {
                Self::DateTime(MyDateTime::from_binlog(input, *frac as usize)?)
            }
            ColumnMeta::Year => BinlogColumnValue::Year(input.read_le_u16()?),
            // NewDate,
            // Varchar,
            ColumnMeta::Bit { bits, bytes } => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L10093
                let len = *bytes + if *bits > 0 { 1 } else { 0 };
                let bs = input.read_len(len as usize)?;
                BinlogColumnValue::Bit(bs)
            }
            // Timestamp2,
            // DateTime2,
            // Time2,
            // Json,
            ColumnMeta::NewDecimal { prec, frac } => {
                // https://github.com/mysql/mysql-server/blob/5.7/strings/decimal.c#L1273
                debug_assert!(prec >= frac);
                let intg = prec - frac;
                let d = MyDecimal::read_from(input, intg, *frac)?;
                BinlogColumnValue::NewDecimal(d)
            }
            ColumnMeta::Enum { pack_len } => {
                let me = match pack_len {
                    1 => MyEnum::Pack1(input.read_u8()?),
                    2 => MyEnum::Pack2(input.read_le_u16()?),
                    3 => MyEnum::Pack3(input.read_le_u24()?),
                    4 => MyEnum::Pack4(input.read_le_u32()?),
                    8 => MyEnum::Pack8(input.read_le_u64()?),
                    _ => return Err(Error::ConstraintError(format!(
                        "invalid length of enum: {}",
                        pack_len
                    )))
                };
                BinlogColumnValue::Enum(me)
            },
            // Set,
            // TinyBlob,
            // MediumBlob,
            // LongBlob,
            ColumnMeta::Blob { pack_len } => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L8571
                // https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html
                let len = match *pack_len {
                    // tinyblob, tinytext
                    1 => input.read_u8()? as u32,
                    // blob, text
                    2 => input.read_le_u16()? as u32,
                    // mediumblob, mediumtext
                    3 => input.read_le_u24()?,
                    // longblob, longtext
                    4 => input.read_le_u32()?,
                    _ => unreachable!("unexpected blob length {}", *pack_len),
                };
                let bs = input.read_len(len as usize)?;
                BinlogColumnValue::Blob(bs)
            }
            ColumnMeta::VarString { max_len } => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L7839
                let actual_len = if *max_len < 256 {
                    input.read_u8()? as u16
                } else {
                    input.read_le_u16()?
                };
                debug_assert!(actual_len <= *max_len);
                let bs = input.read_len(actual_len as usize)?;
                BinlogColumnValue::VarString(bs)
            }
            ColumnMeta::String { from_len } => {
                // https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L7361
                let len = if *from_len > 0xff {
                    input.read_le_u16()?
                } else {
                    input.read_u8()? as u16
                };
                let bs = input.read_len(len as usize)?;
                BinlogColumnValue::String(bs)
            }
            ColumnMeta::Geometry { .. } => unimplemented!(),
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
    pub catalog: SmolStr,
    // len-enc-str
    pub schema: SmolStr,
    // len-enc-str
    pub table: SmolStr,
    // len-enc-str
    pub org_table: SmolStr,
    // len-enc-str
    pub name: SmolStr,
    // len-enc-str
    pub org_name: SmolStr,
    // len-enc-int, always 0x0c
    pub charset: u16,
    pub col_len: u32,
    pub col_type: ColumnType,
    pub flags: ColumnFlags,
    // 0x00, 0x1f, 0x00-0x51
    pub decimals: u8,
    // 2-byte filler
    // len-enc-str, if COM_FIELD_LIST
    pub default_values: SmolStr,
}

impl ColumnDefinition {
    pub fn unsigned(&self) -> bool {
        self.flags.contains(ColumnFlags::UNSIGNED)
    }

    pub fn read_from(input: &mut Bytes, field_list: bool) -> Result<Self> {
        let catalog = input.read_len_enc_str()?;
        let catalog = SmolStr::from(catalog.into_string()?);
        let schema = input.read_len_enc_str()?;
        let schema = SmolStr::from(schema.into_string()?);
        let table = input.read_len_enc_str()?;
        let table = SmolStr::from(table.into_string()?);
        let org_table = input.read_len_enc_str()?;
        let org_table = SmolStr::from(org_table.into_string()?);
        let name = input.read_len_enc_str()?;
        let name = SmolStr::from(name.into_string()?);
        let org_name = input.read_len_enc_str()?;
        let org_name = SmolStr::from(org_name.into_string()?);
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
            SmolStr::from(default_values.into_string()?)
        } else {
            SmolStr::new("")
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stmt::StmtColumnValue;
    #[test]
    fn test_read_binlog_int24_negative() {
        let input = vec![78, 160, 254];
        let bin_val = BinlogColumnValue::read_from(&mut Bytes::from(input), &ColumnMeta::Int24).unwrap();
        let stmt_val = StmtColumnValue::from((bin_val, false));
        if let BinaryColumnValue::Long(n) = stmt_val.val {
            assert_eq!(-90034, n as i32);
        } else {
            panic!("type mismatch");
        }
    }

    #[test]
    fn test_read_binlog_int24_positive() {
        let input = vec![186, 84, 1];
        let bin_val = BinlogColumnValue::read_from(&mut Bytes::from(input), &ColumnMeta::Int24).unwrap();
        let stmt_val = StmtColumnValue::from((bin_val, true));
        if let BinaryColumnValue::Long(n) = stmt_val.val {
            assert_eq!(87226, n);
        } else {
            panic!("type mismatch");
        }
    }

    #[test]
    fn test_read_binlog_date() {
        let input = vec![159, 201, 15];
        let mut input = Bytes::from(input);
        let bin_val = BinlogColumnValue::read_from(&mut input, &ColumnMeta::Date).unwrap();
        if let BinlogColumnValue::Date{year, month, day} = bin_val {
            assert_eq!(2020, year);
            assert_eq!(12, month);
            assert_eq!(31, day);
        } else {
            panic!("type mismatch");
        }
    }

    #[test]
    fn test_read_binlog_datetime0() {
        let input = vec![153_u8, 165, 66, 16, 131];
        let mut input = Bytes::from(input);
        let bin_val = BinlogColumnValue::read_from(&mut input, &ColumnMeta::DateTime{frac: 0}).unwrap();
        if let BinlogColumnValue::DateTime(MyDateTime{year, month, day, hour, minute, second, micro_second}) = bin_val {
            assert_eq!(2020, year);
            assert_eq!(1, month);
            assert_eq!(1, day);
            assert_eq!(1, hour);
            assert_eq!(2, minute);
            assert_eq!(3, second);
            assert_eq!(0, micro_second);
        } else {
            panic!("type mismatch");
        }
    }

    #[test]
    fn test_read_binlog_datetime3() {
        let input = vec![153, 165, 66, 16, 131, 1, 194];
        let mut input = Bytes::from(input);
        let bin_val = BinlogColumnValue::read_from(&mut input, &ColumnMeta::DateTime{frac: 3}).unwrap();
        if let BinlogColumnValue::DateTime(MyDateTime{year, month, day, hour, minute, second, micro_second}) = bin_val {
            assert_eq!(2020, year);
            assert_eq!(1, month);
            assert_eq!(1, day);
            assert_eq!(1, hour);
            assert_eq!(2, minute);
            assert_eq!(3, second);
            assert_eq!(45000, micro_second);
        } else {
            panic!("type mismatch");
        }
    }

    #[test]
    fn test_read_binlog_datetime6() {
        let input = vec![153, 165, 66, 16, 131, 0, 176, 11];
        let mut input = Bytes::from(input);
        let bin_val = BinlogColumnValue::read_from(&mut input, &ColumnMeta::DateTime{frac: 6}).unwrap();
        if let BinlogColumnValue::DateTime(MyDateTime{year, month, day, hour, minute, second, micro_second}) = bin_val {
            assert_eq!(2020, year);
            assert_eq!(1, month);
            assert_eq!(1, day);
            assert_eq!(1, hour);
            assert_eq!(2, minute);
            assert_eq!(3, second);
            assert_eq!(45067, micro_second);
        } else {
            panic!("type mismatch");
        }
    }

    #[test]
    fn test_read_binlog_time0() {
        let input = vec![128, 16, 131];
        let mut input = Bytes::from(input);
        let bin_val = BinlogColumnValue::read_from(&mut input, &ColumnMeta::Time2{frac: 0}).unwrap();
        if let BinlogColumnValue::Time(MyTime{negative, days, hour, minute, second, micro_second}) = bin_val {
            assert_eq!(negative, false);
            assert_eq!(0, days);
            assert_eq!(1, hour);
            assert_eq!(2, minute);
            assert_eq!(3, second);
            assert_eq!(0, micro_second);
        } else {
            panic!("type mismatch");
        }
    }

    #[test]
    fn test_read_binlog_time3() {
        let input = vec![128, 16, 131, 1, 194];
        let mut input = Bytes::from(input);
        let bin_val = BinlogColumnValue::read_from(&mut input, &ColumnMeta::Time2{frac: 3}).unwrap();
        if let BinlogColumnValue::Time(MyTime{negative, days, hour, minute, second, micro_second}) = bin_val {
            assert_eq!(negative, false);
            assert_eq!(0, days);
            assert_eq!(1, hour);
            assert_eq!(2, minute);
            assert_eq!(3, second);
            assert_eq!(45000, micro_second);
        } else {
            panic!("type mismatch");
        }
    }

    #[test]
    fn test_read_binlog_time6() {
        let input = vec![127, 239, 124, 255, 79, 245];
        let mut input = Bytes::from(input);
        let bin_val = BinlogColumnValue::read_from(&mut input, &ColumnMeta::Time2{frac: 6}).unwrap();
        if let BinlogColumnValue::Time(MyTime{negative, days, hour, minute, second, micro_second}) = bin_val {
            assert_eq!(negative, true);
            assert_eq!(0, days);
            assert_eq!(1, hour);
            assert_eq!(2, minute);
            assert_eq!(3, second);
            assert_eq!(45067, micro_second);
        } else {
            panic!("type mismatch");
        }
    }

    #[test]
    fn test_read_write_binary_time() {
        let tm = BinaryColumnValue::Time(MyTime{
            negative: true,
            days: 1,
            hour: 2,
            minute: 3,
            second: 4,
            micro_second: 5,
        });
        let mut bs = BytesMut::new();
        tm.clone().write_to(&mut bs).unwrap();
        let mut input = bs.freeze();
        let output = BinaryColumnValue::read_from(&mut input, ColumnType::Time2).unwrap();
        assert_eq!(tm, output);
    }

    #[test]
    fn test_read_write_binary_datetime() {
        // only date
        let tm = BinaryColumnValue::DateTime(MyDateTime{
            year: 2020,
            month: 8,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            micro_second: 0,
        });
        let mut bs = BytesMut::new();
        tm.clone().write_to(&mut bs).unwrap();
        let mut input = bs.freeze();
        let output = BinaryColumnValue::read_from(&mut input, ColumnType::DateTime2).unwrap();
        assert_eq!(tm, output);

        // only timestamp without fraction
        let tm = BinaryColumnValue::DateTime(MyDateTime{
            year: 2020,
            month: 8,
            day: 1,
            hour: 2,
            minute: 3,
            second: 4,
            micro_second: 0,
        });
        let mut bs = BytesMut::new();
        tm.clone().write_to(&mut bs).unwrap();
        let mut input = bs.freeze();
        let output = BinaryColumnValue::read_from(&mut input, ColumnType::DateTime2).unwrap();
        assert_eq!(tm, output);
    }
}