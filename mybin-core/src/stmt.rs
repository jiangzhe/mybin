use crate::col::{BinaryColumnValue, BinlogColumnValue, ColumnType};
use crate::decimal::MyDecimal;
use crate::resultset::{MyBit, MyYear};
use crate::time::{MyTime, MyDateTime};
use crate::{to_opt_stmt_column_value, to_stmt_column_value};
use bigdecimal::BigDecimal;
use bytes::{Buf, Bytes};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use std::borrow::Cow;

const SQL_NULL: &'static str = "null";

/// define types that can be converted to StmtColumnValue
pub trait ToColumnValue {
    fn to_col(self) -> StmtColumnValue;
}

impl ToColumnValue for StmtColumnValue {
    fn to_col(self) -> Self {
        self
    }
}

to_stmt_column_value!(i8, new_tinyint);
to_stmt_column_value!(u8, new_unsigned_tinyint);
to_stmt_column_value!(i16, new_smallint);
to_stmt_column_value!(u16, new_unsigned_smallint);
to_stmt_column_value!(i32, new_int);
to_stmt_column_value!(u32, new_unsigned_int);
to_stmt_column_value!(i64, new_bigint);
to_stmt_column_value!(u64, new_unsigned_bigint);
to_stmt_column_value!(BigDecimal, new_decimal);
to_stmt_column_value!(f32, new_float);
to_stmt_column_value!(f64, new_double);
to_stmt_column_value!(NaiveDate, new_date);
to_stmt_column_value!(MyTime, new_mytime);
to_stmt_column_value!(NaiveDateTime, new_datetime);
to_stmt_column_value!(MyYear, new_myyear);
to_stmt_column_value!(MyBit, new_mybit);
to_stmt_column_value!(bool, new_bool);

impl ToColumnValue for String {
    fn to_col(self) -> StmtColumnValue {
        StmtColumnValue::new_varstring(Bytes::from(self))
    }
}

impl ToColumnValue for (bool, u32, NaiveTime) {
    fn to_col(self) -> StmtColumnValue {
        StmtColumnValue::new_time(self.0, self.1, self.2)
    }
}

impl ToColumnValue for Option<(bool, u32, NaiveTime)> {
    fn to_col(self) -> StmtColumnValue {
        match self {
            Some(val) => val.to_col(),
            None => StmtColumnValue::new_null(),
        }
    }
}

impl ToColumnValue for Vec<u8> {
    fn to_col(self) -> StmtColumnValue {
        StmtColumnValue::new_blob(Bytes::from(self))
    }
}

impl ToColumnValue for Option<Vec<u8>> {
    fn to_col(self) -> StmtColumnValue {
        match self {
            Some(val) => val.to_col(),
            None => StmtColumnValue::new_null(),
        }
    }
}

impl ToColumnValue for MyDateTime {
    fn to_col(self) -> StmtColumnValue {
        StmtColumnValue::new_mydatetime(self)
    }
}

to_opt_stmt_column_value!(
    i8,
    u8,
    i16,
    u16,
    i32,
    u32,
    i64,
    u64,
    BigDecimal,
    f32,
    f64,
    NaiveDate,
    MyTime,
    NaiveDateTime,
    MyYear,
    String,
    MyBit,
    bool,
    MyDateTime
);

#[derive(Debug, Clone, PartialEq)]
pub struct StmtColumnValue {
    pub col_type: ColumnType,
    pub unsigned: bool,
    pub val: BinaryColumnValue,
}

impl StmtColumnValue {
    pub fn is_null(&self) -> bool {
        match self.col_type {
            ColumnType::Null => true,
            _ => false,
        }
    }

    pub fn to_sql_literal<'a>(&'a self) -> (Cow<'a, str>, bool) {
        match &self.val {
            BinaryColumnValue::Null => (Cow::Borrowed(SQL_NULL), false),
            BinaryColumnValue::NewDecimal(bs) => {
                let s = std::str::from_utf8(bs.bytes()).unwrap();
                (Cow::Borrowed(s), false)
            }
            BinaryColumnValue::Tiny(n) => {
                if self.unsigned {
                    (Cow::Owned(n.to_string()), false)
                } else {
                    (Cow::Owned((*n as i8).to_string()), false)
                }
            }
            BinaryColumnValue::Short(n) => {
                if self.unsigned {
                    (Cow::Owned(n.to_string()), false)
                } else {
                    (Cow::Owned((*n as i16).to_string()), false)
                }
            }
            BinaryColumnValue::Long(n) => {
                if self.unsigned {
                    (Cow::Owned(n.to_string()), false)
                } else {
                    (Cow::Owned((*n as i32).to_string()), false)
                }
            }
            BinaryColumnValue::Float(n) => (Cow::Owned(n.to_string()), false),
            BinaryColumnValue::Double(n) => (Cow::Owned(n.to_string()), false),
            BinaryColumnValue::Timestamp(MyDateTime{
                year,
                month,
                day,
                hour,
                minute,
                second,
                ..
            }) => (
                Cow::Owned(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    year, month, day, hour, minute, second
                )),
                true,
            ),
            BinaryColumnValue::LongLong(n) => {
                if self.unsigned {
                    (Cow::Owned(n.to_string()), false)
                } else {
                    (Cow::Owned((*n as i64).to_string()), false)
                }
            }
            BinaryColumnValue::Date { year, month, day } => (
                Cow::Owned(format!("{:04}-{:02}-{:02}", year, month, day)),
                true,
            ),
            BinaryColumnValue::Time(MyTime {
                negative,
                days,
                hour,
                minute,
                second,
                micro_second,
            }) => {
                let mut s = String::new();
                if *negative {
                    s.push('-');
                }
                let hour = *days as i32 * 24 + *hour as i32;
                if hour >= 100 {
                    s.push_str(&hour.to_string());
                } else {
                    s.push_str(&format!("{:02}", hour));
                }
                s.push_str(&format!(":{:02}:{:02}", minute, second));
                if *micro_second != 0 {
                    s.push_str(&format!(".{:06}", micro_second));
                }
                (Cow::Owned(s), true)
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
                let mut s = format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    year, month, day, hour, minute, second
                );
                if *micro_second != 0 {
                    s.push_str(&format!(".{:06}", micro_second));
                }
                (Cow::Owned(s), true)
            }
            BinaryColumnValue::Year(n) => (Cow::Owned(format!("{:04}", n)), false),
            BinaryColumnValue::VarString(bs) | BinaryColumnValue::String(bs) => {
                if !bs.bytes().contains(&b'\'') {
                    (
                        Cow::Borrowed(std::str::from_utf8(bs.bytes()).unwrap()),
                        true,
                    )
                } else {
                    let mut s = String::new();
                    for &b in bs.bytes() {
                        if b == b'\'' {
                            s.push('\'');
                            s.push('\'');
                        } else {
                            s.push(b as char);
                        }
                    }
                    (Cow::Owned(s), true)
                }
            }
            BinaryColumnValue::Bit(bs) => {
                // bs at most 8 bytes
                let mut n = 0_u64;
                for (i, &b) in bs.bytes().iter().enumerate() {
                    let offset = i << 3;
                    n += (b as u64) << offset;
                }
                (Cow::Owned(n.to_string()), false)
            }
            BinaryColumnValue::Blob(bs) | BinaryColumnValue::Geometry(bs) => {
                // hex encoded
                let mut encoded = vec![0; bs.remaining() * 2 + 3];
                encoded[0] = b'x';
                encoded[1] = b'\'';
                let last = encoded.len() - 1;
                hex::encode_to_slice(bs.bytes(), &mut encoded[2..last]).unwrap();
                encoded[last] = b'\'';
                // won't fail to be converted to string
                (Cow::Owned(String::from_utf8(encoded).unwrap()), false)
            }
            BinaryColumnValue::Int24(n) => {
                if self.unsigned || n & 0x80_0000 == 0 {
                    (Cow::Owned(n.to_string()), false)
                } else {
                    (Cow::Owned(((n | 0xff80_0000) as i32).to_string()), false)
                }
            }
        }
    }

    pub fn new(col_type: ColumnType, unsigned: bool, val: BinaryColumnValue) -> Self {
        match col_type {
            ColumnType::Decimal | ColumnType::NewDecimal => Self {
                col_type: ColumnType::NewDecimal,
                unsigned: false,
                val,
            },
            _ => Self {
                col_type,
                unsigned,
                val,
            },
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

    pub fn new_mydecimal(decimal: MyDecimal) -> Self {
        Self {
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
            val: BinaryColumnValue::Timestamp(MyDateTime{
                year: ts.year() as u16,
                month: ts.month() as u8,
                day: ts.day() as u8,
                hour: ts.hour() as u8,
                minute: ts.minute() as u8,
                second: ts.second() as u8,
                micro_second: 0,
            }),
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
        let tm = MyTime{
            negative,
            days,
            hour: tm.hour() as u8,
            minute: tm.minute() as u8,
            second: tm.second() as u8,
            // handle leap second
            micro_second: (tm.nanosecond() % 1_000_000_000) / 1000,
        };
        Self::new_mytime(tm)
    }

    pub fn new_mytime(tm: MyTime) -> Self {
        // Self::new_time(tm.negative, tm.days, tm.time)
        Self {
            col_type: ColumnType::Time,
            unsigned: false,
            val: BinaryColumnValue::Time(tm),
        }
    }

    pub fn new_datetime(ts: NaiveDateTime) -> Self {
        let ts = MyDateTime{
            year: ts.year() as u16,
            month: ts.month() as u8,
            day: ts.day() as u8,
            hour: ts.hour() as u8,
            minute: ts.minute() as u8,
            second: ts.second() as u8,
            micro_second: (ts.nanosecond() % 1_000_000_000) / 1000,
        };
        Self::new_mydatetime(ts)
    }

    pub fn new_mydatetime(ts: MyDateTime) -> Self {
        Self {
            col_type: ColumnType::DateTime,
            unsigned: false,
            val: BinaryColumnValue::DateTime(ts),
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

    pub fn new_myyear(y: MyYear) -> Self {
        Self {
            col_type: ColumnType::Year,
            unsigned: true,
            val: BinaryColumnValue::Year(y.0),
        }
    }

    pub fn new_varstring(bs: impl Into<Bytes>) -> Self {
        Self {
            col_type: ColumnType::VarString,
            unsigned: false,
            val: BinaryColumnValue::VarString(bs.into()),
        }
    }

    /// fixed length string
    pub fn new_string(s: impl Into<Bytes>) -> Self {
        Self {
            col_type: ColumnType::String,
            unsigned: false,
            val: BinaryColumnValue::String(s.into()),
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

    pub fn new_mybit(bits: MyBit) -> Self {
        Self::new_bit(Vec::from(bits.0.bytes()))
    }

    pub fn new_blob(bs: impl Into<Bytes>) -> Self {
        Self {
            col_type: ColumnType::Blob,
            unsigned: false,
            val: BinaryColumnValue::Blob(bs.into()),
        }
    }

    pub fn new_geometry(bs: impl Into<Bytes>) -> Self {
        Self {
            col_type: ColumnType::Geometry,
            unsigned: false,
            val: BinaryColumnValue::Geometry(bs.into()),
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

impl<'c> From<(BinlogColumnValue, bool)> for StmtColumnValue {
    fn from((val, unsigned): (BinlogColumnValue, bool)) -> Self {
        match val {
            BinlogColumnValue::Null => Self::new_null(),
            BinlogColumnValue::Tiny(n) => {
                if unsigned {
                    Self::new_unsigned_tinyint(n)
                } else {
                    Self::new_tinyint(n as i8)
                }
            }
            BinlogColumnValue::Short(n) => {
                if unsigned {
                    Self::new_unsigned_smallint(n)
                } else {
                    Self::new_smallint(n as i16)
                }
            }
            BinlogColumnValue::Long(n) => {
                if unsigned {
                    Self::new_unsigned_int(n)
                } else {
                    Self::new_int(n as i32)
                }
            }
            BinlogColumnValue::Float(n) => Self::new_float(n),
            BinlogColumnValue::Double(n) => Self::new_double(n),
            BinlogColumnValue::Timestamp(secs) => Self::new_timestamp(
                NaiveDateTime::from_timestamp(secs as i64, 0),
            ),
            BinlogColumnValue::LongLong(n) => {
                if unsigned {
                    Self::new_unsigned_bigint(n)
                } else {
                    Self::new_bigint(n as i64)
                }
            }
            BinlogColumnValue::Int24(n) => {
                if unsigned {
                    Self::new_unsigned_int(n)
                } else if n & 0x80_0000 == 0x80_0000 {
                    Self::new_int((n | 0xff00_0000) as i32)
                } else {
                    Self::new_int(n as i32)
                }
            }
            BinlogColumnValue::Date { year, month, day } => {
                Self::new_date(NaiveDate::from_ymd(year as i32, month as u32, day as u32))
            }
            BinlogColumnValue::Time(tm) => {
                Self::new_mytime(tm)
            }
            BinlogColumnValue::DateTime(MyDateTime{
                year,
                month,
                day,
                hour,
                minute,
                second,
                micro_second,
            }) => Self::new_timestamp(
                NaiveDate::from_ymd(year as i32, month as u32, day as u32).and_hms_micro(
                    hour as u32,
                    minute as u32,
                    second as u32,
                    micro_second,
                ),
            ),
            BinlogColumnValue::Year(n) => Self::new_year(n),
            // Varchar(Bytes),
            BinlogColumnValue::Bit(bs) => Self::new_bit(Vec::from(bs.bytes())),
            BinlogColumnValue::NewDecimal(d) => Self::new_mydecimal(d),
            BinlogColumnValue::Enum(e) => Self::new_unsigned_bigint(e.to_u64()),
            BinlogColumnValue::Blob(bs) => Self::new_blob(bs),
            BinlogColumnValue::VarString(bs) => Self::new_varstring(bs),
            BinlogColumnValue::String(bs) => Self::new_varstring(bs),
            BinlogColumnValue::Geometry(..) => unimplemented!("geometry conversion"),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_stmt_column_values() {
        let col1 = (-1 as i8).to_col();
        assert_eq!(BinaryColumnValue::Tiny(-1_i8 as u8), col1.val);
        let (lit, quote) = col1.to_sql_literal();
        assert_eq!("-1", &lit);
        assert!(!quote);
        let col2 = (1 as u8).to_col();
        assert_eq!(BinaryColumnValue::Tiny(1), col2.val);
        let (lit, quote) = col2.to_sql_literal();
        assert_eq!("1", &lit);
        assert!(!quote);
        let col3 = (-1 as i16).to_col();
        assert_eq!(BinaryColumnValue::Short(-1_i16 as u16), col3.val);
        let (lit, quote) = col3.to_sql_literal();
        assert_eq!("-1", &lit);
        assert!(!quote);
        let col4 = (1 as u16).to_col();
        assert_eq!(BinaryColumnValue::Short(1), col4.val);
        let (lit, quote) = col4.to_sql_literal();
        assert_eq!("1", &lit);
        assert!(!quote);
        let col5 = (-1 as i32).to_col();
        assert_eq!(BinaryColumnValue::Long(-1_i32 as u32), col5.val);
        let (lit, quote) = col5.to_sql_literal();
        assert_eq!("-1", &lit);
        assert!(!quote);
        let col6 = (1 as u32).to_col();
        assert_eq!(BinaryColumnValue::Long(1 as u32), col6.val);
        let (lit, quote) = col6.to_sql_literal();
        assert_eq!("1", &lit);
        assert!(!quote);
        let col7 = (-1 as i64).to_col();
        assert_eq!(BinaryColumnValue::LongLong((-1_i64) as u64), col7.val);
        let (lit, quote) = col7.to_sql_literal();
        assert_eq!("-1", &lit);
        assert!(!quote);
        let col8 = (1 as u64).to_col();
        assert_eq!(BinaryColumnValue::LongLong(1), col8.val);
        let (lit, quote) = col8.to_sql_literal();
        assert_eq!("1", &lit);
        assert!(!quote);
        let col9 = BigDecimal::from(1).to_col();
        assert_eq!(
            BinaryColumnValue::NewDecimal(Bytes::from(vec![b'1'])),
            col9.val
        );
        let (lit, quote) = col9.to_sql_literal();
        assert_eq!("1", &lit);
        assert!(!quote);
        let col10 = (1 as f32).to_col();
        assert_eq!(BinaryColumnValue::Float(1f32), col10.val);
        let (lit, quote) = col10.to_sql_literal();
        assert_eq!("1", &lit);
        assert!(!quote);
        let col11 = (1 as f64).to_col();
        assert_eq!(BinaryColumnValue::Double(1f64), col11.val);
        let (lit, quote) = col11.to_sql_literal();
        assert_eq!("1", &lit);
        assert!(!quote);
        let col12 = NaiveDate::from_ymd(2020, 12, 31).to_col();
        assert_eq!(
            BinaryColumnValue::Date {
                year: 2020,
                month: 12,
                day: 31
            },
            col12.val
        );
        let (lit, quote) = col12.to_sql_literal();
        assert_eq!("2020-12-31", &lit);
        assert!(quote);
        let col13 = MyTime {
            negative: false,
            days: 0,
            hour: 1,
            minute: 2,
            second: 3,
            micro_second: 4,
        }
        .to_col();
        assert_eq!(
            BinaryColumnValue::Time(MyTime{
                negative: false,
                days: 0,
                hour: 1,
                minute: 2,
                second: 3,
                micro_second: 4
            }),
            col13.val
        );
        let (lit, quote) = col13.to_sql_literal();
        assert_eq!("01:02:03.000004", &lit);
        assert!(quote);
        let col14 = NaiveDate::from_ymd(2020, 12, 31)
            .and_hms_micro(1, 2, 3, 4)
            .to_col();
        assert_eq!(
            BinaryColumnValue::DateTime(MyDateTime{
                year: 2020,
                month: 12,
                day: 31,
                hour: 1,
                minute: 2,
                second: 3,
                micro_second: 4
            }),
            col14.val
        );
        let (lit, quote) = col14.to_sql_literal();
        assert_eq!("2020-12-31 01:02:03.000004", &lit);
        assert!(quote);
        let col15 = MyYear(2020).to_col();
        assert_eq!(BinaryColumnValue::Year(2020), col15.val);
        let (lit, quote) = col15.to_sql_literal();
        assert_eq!("2020", &lit);
        assert!(!quote);
        let col16 = "hello, world".to_owned().to_col();
        assert_eq!(
            BinaryColumnValue::VarString(Bytes::from("hello, world".as_bytes())),
            col16.val
        );
        let (lit, quote) = col16.to_sql_literal();
        assert_eq!("hello, world", &lit);
        assert!(quote);
        let col17 = MyBit(Bytes::from(vec![1u8])).to_col();
        assert_eq!(BinaryColumnValue::Bit(Bytes::from(vec![1u8])), col17.val);
        let (lit, quote) = col17.to_sql_literal();
        assert_eq!("1", &lit);
        assert!(!quote);
        let col18 = true.to_col();
        assert_eq!(BinaryColumnValue::Tiny(1), col18.val);
        let (lit, quote) = col18.to_sql_literal();
        assert_eq!("1", &lit);
        assert!(!quote);
        let col19 = 
        MyDateTime{
            year: 2020, month: 12, day: 31, hour: 1, minute: 2, second: 3, micro_second: 0
        }.to_col();
        // replace Timestamp with DateTime
        assert_eq!(
            BinaryColumnValue::DateTime(MyDateTime{
                year: 2020,
                month: 12,
                day: 31,
                hour: 1,
                minute: 2,
                second: 3,
                micro_second: 0,
            }),
            col19.val
        );
        let (lit, quote) = col19.to_sql_literal();
        assert_eq!("2020-12-31 01:02:03", &lit);
        assert!(quote);
        let col20 = vec![1, 2, 3].to_col();
        assert_eq!(
            BinaryColumnValue::Blob(Bytes::from(vec![1, 2, 3])),
            col20.val
        );
        let (lit, quote) = col20.to_sql_literal();
        assert_eq!("x'010203'", &lit);
        assert!(!quote);
    }
}
