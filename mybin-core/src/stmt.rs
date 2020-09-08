use crate::col::{BinaryColumnValue, ColumnType};
use crate::resultset::{MyBit, MyTime, MyTimestamp, MyYear};
use crate::{to_opt_stmt_column_value, to_stmt_column_value};
use bigdecimal::BigDecimal;
use bytes::Bytes;
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

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
to_stmt_column_value!(String, new_varchar);
to_stmt_column_value!(MyBit, new_mybit);
to_stmt_column_value!(bool, new_bool);

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
        StmtColumnValue::new_blob(self)
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

impl ToColumnValue for MyTimestamp {
    fn to_col(self) -> StmtColumnValue {
        StmtColumnValue::new_timestamp(self.0)
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
    MyTimestamp
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

    pub fn new_myyear(y: MyYear) -> Self {
        Self {
            col_type: ColumnType::Year,
            unsigned: true,
            val: BinaryColumnValue::Year(y.0),
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

    pub fn new_mybit(bits: MyBit) -> Self {
        use bytes::Buf;
        Self::new_bit(Vec::from(bits.0.bytes()))
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_to_stmt_column_values() {
        let col1 = (-1 as i8).to_col();
        assert_eq!(BinaryColumnValue::Tiny(-1_i8 as u8), col1.val);
        let col2 = (1 as u8).to_col();
        assert_eq!(BinaryColumnValue::Tiny(1), col2.val);
        let col3 = (-1 as i16).to_col();
        assert_eq!(BinaryColumnValue::Short(-1_i16 as u16), col3.val);
        let col4 = (1 as u16).to_col();
        assert_eq!(BinaryColumnValue::Short(1), col4.val);
        let col5 = (-1 as i32).to_col();
        assert_eq!(BinaryColumnValue::Long(-1_i32 as u32), col5.val);
        let col6 = (1 as u32).to_col();
        assert_eq!(BinaryColumnValue::Long(1 as u32), col6.val);
        let col7 = (-1_i64 as u64).to_col();
        assert_eq!(BinaryColumnValue::LongLong(-1_i64 as u64), col7.val);
        let col8 = (1 as u64).to_col();
        assert_eq!(BinaryColumnValue::LongLong(1), col8.val);
        let col9 = BigDecimal::from(1).to_col();
        assert_eq!(
            BinaryColumnValue::NewDecimal(Bytes::from(vec![b'1'])),
            col9.val
        );
        let col10 = (1 as f32).to_col();
        assert_eq!(BinaryColumnValue::Float(1f32), col10.val);
        let col11 = (1 as f64).to_col();
        assert_eq!(BinaryColumnValue::Double(1f64), col11.val);
        let col12 = NaiveDate::from_ymd(2020, 12, 31).to_col();
        assert_eq!(
            BinaryColumnValue::Date {
                year: 2020,
                month: 12,
                day: 31
            },
            col12.val
        );
        let col13 = MyTime {
            negative: false,
            days: 0,
            time: NaiveTime::from_hms_micro(1, 2, 3, 4),
        }
        .to_col();
        assert_eq!(
            BinaryColumnValue::Time {
                negative: false,
                days: 0,
                hour: 1,
                minute: 2,
                second: 3,
                micro_second: 4
            },
            col13.val
        );
        let col14 = NaiveDate::from_ymd(2020, 12, 31)
            .and_hms_micro(1, 2, 3, 4)
            .to_col();
        assert_eq!(
            BinaryColumnValue::DateTime {
                year: 2020,
                month: 12,
                day: 31,
                hour: 1,
                minute: 2,
                second: 3,
                micro_second: 4
            },
            col14.val
        );
        let col15 = MyYear(2020).to_col();
        assert_eq!(BinaryColumnValue::Year(2020), col15.val);
        let col16 = "hello, world".to_owned().to_col();
        assert_eq!(
            BinaryColumnValue::VarString(Bytes::from("hello, world".as_bytes())),
            col16.val
        );
        let col17 = MyBit(Bytes::from(vec![1u8])).to_col();
        assert_eq!(BinaryColumnValue::Bit(Bytes::from(vec![1u8])), col17.val);
        let col18 = true.to_col();
        assert_eq!(BinaryColumnValue::Tiny(1), col18.val);
        let col19 = MyTimestamp(NaiveDate::from_ymd(2020, 12, 31).and_hms(1, 2, 3)).to_col();
        assert_eq!(
            BinaryColumnValue::Timestamp {
                year: 2020,
                month: 12,
                day: 31,
                hour: 1,
                minute: 2,
                second: 3
            },
            col19.val
        );
        let col20 = vec![1, 2, 3].to_col();
        assert_eq!(
            BinaryColumnValue::Blob(Bytes::from(vec![1, 2, 3])),
            col20.val
        );
    }
}
