use crate::bitmap;
use crate::col::{BinaryColumnValue, ColumnType};
use crate::resultset::MyTime;
use crate::Command;
use bigdecimal::BigDecimal;
use bitflags::bitflags;
use bytes::{Bytes, BytesMut};
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

#[derive(Debug, Clone)]
pub struct ComStmtExecute {
    pub cmd: Command,
    pub stmt_id: u32,
    pub flags: CursorTypeFlags,
    pub iter_cnt: u32,
    pub null_bitmap: Vec<u8>,
    // for first statement to execute, new_params_bound should be true
    // for any batch execution, if column type is different from
    // the previous one, this flag should be true
    pub new_params_bound: bool,
    pub params: Vec<StmtExecValue>,
}

impl ComStmtExecute {
    pub fn single(stmt_id: u32, params: Vec<StmtExecValue>) -> Self {
        let null_bitmap = bitmap::from_iter(params.iter().map(|p| p.is_null()), 0);
        Self {
            cmd: Command::StmtExecute,
            stmt_id,
            // currently not support cursor
            flags: CursorTypeFlags::empty(),
            iter_cnt: 1,
            null_bitmap,
            new_params_bound: true,
            params,
        }
    }
}

impl WriteToBytes for ComStmtExecute {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_le_u32(self.stmt_id)?;
        len += out.write_u8(self.flags.bits())?;
        len += out.write_le_u32(1)?;
        if !self.params.is_empty() {
            let null_bitmap = bitmap::from_iter(self.params.iter().map(|c| c.is_null()), 0);
            len += out.write_bytes(&null_bitmap[..])?;
            len += out.write_u8(if self.new_params_bound { 0x01 } else { 0x00 })?;
            if self.new_params_bound {
                for param in &self.params {
                    len += out.write_u8(param.col_type.into())?;
                    len += out.write_u8(if param.unsigned { 0x80 } else { 0x00 })?;
                }
            }
            for param in self.params {
                len += out.write_bytes(param.val)?;
            }
        }
        Ok(len)
    }
}

#[derive(Debug, Clone)]
pub struct StmtExecValue {
    pub col_type: ColumnType,
    pub unsigned: bool,
    pub val: BinaryColumnValue,
}

impl StmtExecValue {
    pub fn is_null(&self) -> bool {
        match self.col_type {
            ColumnType::Null => true,
            _ => false,
        }
    }

    pub fn new_null() -> Self {
        Self {
            col_type: ColumnType::Null,
            unsigned: false,
            val: BinaryColumnValue::Null,
        }
    }

    pub fn new_decimal(decimal: BigDecimal) -> Self {
        Self {
            // prefer NewDecimal to Decimal
            col_type: ColumnType::NewDecimal,
            unsigned: false,
            val: BinaryColumnValue::NewDecimal(Bytes::from(decimal.to_string())),
        }
    }

    pub fn new_tinyint(n: i8) -> Self {
        Self {
            col_type: ColumnType::Tiny,
            unsigned: false,
            val: BinaryColumnValue::Tiny(n as u8),
        }
    }

    pub fn new_unsigned_tinyint(n: u8) -> Self {
        Self {
            col_type: ColumnType::Tiny,
            unsigned: true,
            val: BinaryColumnValue::Tiny(n),
        }
    }

    pub fn new_smallint(n: i16) -> Self {
        Self {
            col_type: ColumnType::Short,
            unsigned: false,
            val: BinaryColumnValue::Short(n as u16),
        }
    }

    pub fn new_unsigned_smallint(n: u16) -> Self {
        Self {
            col_type: ColumnType::Short,
            unsigned: true,
            val: BinaryColumnValue::Short(n),
        }
    }

    pub fn new_int(n: i32) -> Self {
        Self {
            col_type: ColumnType::Long,
            unsigned: false,
            val: BinaryColumnValue::Long(n as u32),
        }
    }

    pub fn new_unsigned_int(n: u32) -> Self {
        Self {
            col_type: ColumnType::Long,
            unsigned: true,
            val: BinaryColumnValue::Long(n),
        }
    }

    pub fn new_float(n: f32) -> Self {
        Self {
            col_type: ColumnType::Float,
            unsigned: false,
            val: BinaryColumnValue::Float(n),
        }
    }

    pub fn new_double(n: f64) -> Self {
        Self {
            col_type: ColumnType::Double,
            unsigned: false,
            val: BinaryColumnValue::Double(n),
        }
    }

    pub fn new_timestamp(ts: NaiveDateTime) -> Self {
        Self {
            col_type: ColumnType::Timestamp,
            unsigned: false,
            val: BinaryColumnValue::Timestamp {
                year: ts.year() as u16,
                month: ts.month() as u8,
                day: ts.day() as u8,
                hour: ts.hour() as u8,
                minute: ts.minute() as u8,
                second: ts.second() as u8,
            },
        }
    }

    pub fn new_bigint(n: i64) -> Self {
        Self {
            col_type: ColumnType::LongLong,
            unsigned: false,
            val: BinaryColumnValue::LongLong(n as u64),
        }
    }

    pub fn new_unsigned_bigint(n: u64) -> Self {
        Self {
            col_type: ColumnType::LongLong,
            unsigned: true,
            val: BinaryColumnValue::LongLong(n),
        }
    }

    pub fn new_date(dt: NaiveDate) -> Self {
        Self {
            col_type: ColumnType::Date,
            unsigned: false,
            val: BinaryColumnValue::Date {
                year: dt.year() as u16,
                month: dt.month() as u8,
                day: dt.day() as u8,
            },
        }
    }

    pub fn new_time(negative: bool, days: u32, tm: NaiveTime) -> Self {
        Self {
            col_type: ColumnType::Time,
            unsigned: false,
            val: BinaryColumnValue::Time {
                negative,
                days,
                hour: tm.hour() as u8,
                minute: tm.minute() as u8,
                second: tm.second() as u8,
                // handle leap second
                micro_second: (tm.nanosecond() % 1_000_000_000) / 1000,
            },
        }
    }

    pub fn new_mytime(tm: MyTime) -> Self {
        Self::new_time(tm.negative, tm.days, tm.time)
    }

    pub fn new_datetime(ts: NaiveDateTime) -> Self {
        Self {
            col_type: ColumnType::DateTime,
            unsigned: false,
            val: BinaryColumnValue::DateTime {
                year: ts.year() as u16,
                month: ts.month() as u8,
                day: ts.day() as u8,
                hour: ts.hour() as u8,
                minute: ts.minute() as u8,
                second: ts.second() as u8,
                micro_second: (ts.nanosecond() % 1_000_000_000) / 1000,
            },
        }
    }

    pub fn new_year(n: u16) -> Self {
        // use short instead of year
        Self {
            col_type: ColumnType::Short,
            unsigned: true,
            val: BinaryColumnValue::Short(n),
        }
    }

    pub fn new_varchar<S: Into<String>>(s: S) -> Self {
        Self {
            col_type: ColumnType::VarString,
            unsigned: false,
            val: BinaryColumnValue::VarString(Bytes::from(s.into())),
        }
    }

    /// fixed length string
    pub fn new_char<S: Into<String>>(s: S) -> Self {
        Self {
            col_type: ColumnType::String,
            unsigned: false,
            val: BinaryColumnValue::String(Bytes::from(s.into())),
        }
    }

    pub fn new_text<S: Into<String>>(s: S) -> Self {
        Self {
            col_type: ColumnType::Blob,
            unsigned: false,
            val: BinaryColumnValue::Blob(Bytes::from(s.into())),
        }
    }

    pub fn new_bit<T: Into<Vec<u8>>>(bits: T) -> Self {
        Self {
            col_type: ColumnType::Bit,
            unsigned: false,
            val: BinaryColumnValue::Bit(Bytes::from(bits.into())),
        }
    }

    pub fn new_blob<T: Into<Vec<u8>>>(bs: T) -> Self {
        Self {
            col_type: ColumnType::Blob,
            unsigned: false,
            val: BinaryColumnValue::Blob(Bytes::from(bs.into())),
        }
    }

    pub fn new_geometry(bs: Vec<u8>) -> Self {
        Self {
            col_type: ColumnType::Geometry,
            unsigned: false,
            val: BinaryColumnValue::Geometry(Bytes::from(bs)),
        }
    }

    pub fn new_bool(b: bool) -> Self {
        Self {
            col_type: ColumnType::Tiny,
            unsigned: false,
            val: BinaryColumnValue::Tiny(if b { 0x01 } else { 0x00 }),
        }
    }
}

bitflags! {
    pub struct CursorTypeFlags: u8 {
        const READ_ONLY     = 0x01;
        const FOR_UPDATE    = 0x02;
        const SCROLLABLE    = 0x04;
    }
}

#[derive(Debug)]
pub struct StmtResultSetState {}
