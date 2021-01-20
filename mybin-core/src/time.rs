//! Decoding of mysql time/timestamp/datetime data types
//!
//! Uses BigEndian and bit operations

use crate::col::{BinaryColumnValue, TextColumnValue};
use crate::error::{Error, Result};
use crate::resultset::FromColumnValue;
use crate::try_non_null_column_value;
use bytes::{Buf, Bytes};
use bytes_parser::error::{Error as BError, Result as BResult};
use bytes_parser::ReadBytesExt;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};

#[derive(Debug, Clone, PartialEq)]
pub struct MyTime {
    pub negative: bool,
    pub days: u32,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub micro_second: u32,
}

impl MyTime {
    /// read time with given fraction in binlog
    ///
    /// https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c#L1689
    pub fn from_binlog(input: &mut Bytes, frac: usize) -> BResult<Self> {
        let (packed, negative) = packed_from_time_binary(input, frac)?;
        let hms = (packed >> 24) & 0xff_ffff;
        let hours = ((hms >> 12) % (1 << 10)) as u32;
        let days = hours / 24;
        let hour = (hours - days * 24) as u8;
        let minute = ((hms >> 6) % (1 << 6)) as u8;
        let second = (hms % (1 << 6)) as u8;
        let micro_second = (packed & 0xff_ffff) as u32;
        Ok(Self {
            negative,
            days,
            hour,
            minute,
            second,
            micro_second,
        })
    }
}

impl FromColumnValue<BinaryColumnValue> for Option<MyTime> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Time(v) => Ok(Some(v)),
            _ => Err(Error::column_type_mismatch("bool", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => MyTime);

impl FromColumnValue<TextColumnValue> for Option<MyTime> {
    fn from_col(value: TextColumnValue) -> Result<Self> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.chunk())?;
                let s = s.trim_start();
                // because MySQL time can be negative and more than 24 hours,
                // cannot use NaiveTime::parse_from_str()
                let splits: Vec<&str> = s.split(":").collect();
                if splits.len() != 3 {
                    return Err(Error::ParseMyTimeError(format!("invalid string {}", s)));
                }
                let hours: i64 = splits[0].parse()?;
                let negative = hours < 0;
                let hours = i64::abs(hours) as u32;
                let days = hours / 24;
                let hour = (hours - days * 24) as u8;
                let minute: u8 = splits[1].parse()?;
                // handle micro seconds if exists
                let sec_splits: Vec<&str> = splits[2].split('.').collect();
                if sec_splits.len() > 2 {
                    return Err(Error::ParseMyTimeError(format!(
                        "invalid seconds {}",
                        splits[2]
                    )));
                }
                let second: u8 = sec_splits[0].parse()?;
                let micro_second = if sec_splits.len() == 2 {
                    if sec_splits[1].len() == 3 {
                        let milliseconds: u32 = sec_splits[1].parse()?;
                        milliseconds * 1000
                    } else {
                        sec_splits[1].parse()?
                    }
                } else {
                    0
                };
                Ok(Some(MyTime {
                    negative,
                    days,
                    hour,
                    minute,
                    second,
                    micro_second,
                }))
            }
        }
    }
}

try_non_null_column_value!(TextColumnValue => MyTime);

#[derive(Debug, Clone, PartialEq)]
pub struct MyDateTime {
    pub year: u16,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub micro_second: u32,
}

impl MyDateTime {
    /// read datetime with given fraction from binlog
    ///
    /// https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c#L1820
    pub fn from_binlog(input: &mut Bytes, frac: usize) -> BResult<Self> {
        let packed = packed_from_datetime_binary(input, frac)?;
        let ymdhms = (packed >> 24) & 0xff_ffff_ffff;
        let ymd = ymdhms >> 17;
        let ym = ymd >> 5;
        let hms = ymdhms % (1 << 17);
        let day = (ymd % (1 << 5)) as u8;
        let month = (ym % 13) as u8;
        let year = (ym / 13) as u16;
        let hour = (hms >> 12) as u8;
        let minute = ((hms >> 6) % (1 << 6)) as u8;
        let second = (hms % (1 << 6)) as u8;
        let micro_second = (packed & 0xff_ffff) as u32;
        Ok(Self {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micro_second,
        })
    }
}

impl From<NaiveDateTime> for MyDateTime {
    fn from(src: NaiveDateTime) -> Self {
        Self {
            year: src.year() as u16,
            month: src.month() as u8,
            day: src.day() as u8,
            hour: src.hour() as u8,
            minute: src.minute() as u8,
            second: src.second() as u8,
            micro_second: (src.nanosecond() % 1_000_000_000) / 1000,
        }
    }
}

impl From<MyDateTime> for NaiveDateTime {
    fn from(src: MyDateTime) -> Self {
        NaiveDate::from_ymd(src.year as i32, src.month as u32, src.day as u32).and_hms_micro(
            src.hour as u32,
            src.minute as u32,
            src.second as u32,
            src.micro_second as u32,
        )
    }
}

impl FromColumnValue<BinaryColumnValue> for Option<MyDateTime> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Timestamp(ts) => Ok(Some(ts)),
            _ => Err(Error::column_type_mismatch("datetime", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => MyDateTime);

impl FromColumnValue<TextColumnValue> for Option<MyDateTime> {
    fn from_col(value: TextColumnValue) -> Result<Self> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.chunk())?;
                dbg!(s);
                let ts = if s.len() > 19 {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")?
                } else {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")?
                };
                Ok(Some(ts.into()))
            }
        }
    }
}

try_non_null_column_value!(TextColumnValue => MyDateTime);

/// convert binary representation of time to packed u64
///
/// will consume 3 ~ 6 bytes from input according to certain fraction
fn packed_from_time_binary(input: &mut Bytes, frac: usize) -> BResult<(u64, bool)> {
    let hms = input.read_be_u24()?;
    let negative = hms & 0x80_0000 != 0x80_0000;
    // todo: negative fraction
    let packed = match frac {
        0 => {
            let int_part = hms.overflowing_sub(0x80_0000).0;
            (int_part as u64) << 24
        }
        1 | 2 => {
            let int_part = hms.overflowing_sub(0x80_0000).0;
            let frac_part = input.read_u8()?;
            ((int_part as u64) << 24) + (frac_part as u64) * 10000
        }
        3 | 4 => {
            let int_part = hms.overflowing_sub(0x80_0000).0;
            let frac_part = input.read_be_u16()?;
            ((int_part as u64) << 24) + (frac_part as u64) * 100
        }
        5 | 6 => {
            let frac_part = input.read_be_u24()?;
            let p = ((hms as u64) << 24) + (frac_part as u64);
            p.overflowing_sub(0x8000_0000_0000).0
        }
        _ => {
            return Err(BError::ConstraintError(format!(
                "invalid fractional length of time {}",
                frac
            )))
        }
    };
    let packed = if negative {
        (-(packed as i64)) as u64
    } else {
        packed
    };
    Ok((packed, negative))
}

// convert binary representation of datetime to packed u64
fn packed_from_datetime_binary(input: &mut Bytes, frac: usize) -> BResult<u64> {
    // https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c#L1762
    let ymdhms = input.read_be_u40()?;
    // https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c#L1905
    let negative = ymdhms & 0x80_0000_0000 != 0x80_0000_0000;
    let packed = match frac {
        0 => {
            let int_part = ymdhms.overflowing_sub(0x80_0000_0000).0;
            int_part << 24
        }
        1 | 2 => {
            let int_part = ymdhms.overflowing_sub(0x80_0000_0000).0;
            let frac_part = input.read_u8()?;
            (int_part << 24) + (frac_part as u64) * 10000
        }
        3 | 4 => {
            let int_part = ymdhms.overflowing_sub(0x80_0000_0000).0;
            let frac_part = input.read_be_u16()?;
            (int_part << 24) + (frac_part as u64) * 100
        }
        5 | 6 => {
            let int_part = ymdhms.overflowing_sub(0x80_0000_0000).0;
            let frac_part = input.read_be_u24()?;
            (int_part << 24) + (frac_part as u64)
        }
        _ => {
            return Err(BError::ConstraintError(format!(
                "invalid fractional length of datetime {}",
                frac
            )))
        }
    };
    let packed = if negative {
        (-(packed as i64)) as u64
    } else {
        packed
    };
    Ok(packed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_binlog_time0() {
        let input = vec![128, 16, 131];
        let mut input = Bytes::from(input);
        let tm = MyTime::from_binlog(&mut input, 0).unwrap();
        // println!("{:?}", tm);
        assert_eq!(
            MyTime {
                negative: false,
                days: 0,
                hour: 1,
                minute: 2,
                second: 3,
                micro_second: 0
            },
            tm
        );
    }

    #[test]
    fn test_read_binlog_time3() {
        let input = vec![128, 16, 131, 1, 194];
        let mut input = Bytes::from(input);
        let tm = MyTime::from_binlog(&mut input, 3).unwrap();
        assert_eq!(
            MyTime {
                negative: false,
                days: 0,
                hour: 1,
                minute: 2,
                second: 3,
                micro_second: 45000
            },
            tm
        );
    }

    #[test]
    fn test_read_binlog_time6() {
        // negative value
        let input = vec![127, 239, 124, 255, 79, 245];
        let mut input = Bytes::from(input);
        let tm = MyTime::from_binlog(&mut input, 6).unwrap();
        assert_eq!(
            MyTime {
                negative: true,
                days: 0,
                hour: 1,
                minute: 2,
                second: 3,
                micro_second: 45067
            },
            tm
        );
    }

    #[test]
    fn test_time_from_text_value() {
        let input = Some(Bytes::from("01:02:03.004"));
        let tm = MyTime::from_col(input).unwrap();
        println!("{:?}", tm);
    }
}
