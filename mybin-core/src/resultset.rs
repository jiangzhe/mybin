use crate::col::{BinaryColumnValue, ColumnDefinition, ColumnType, TextColumnValue};
use crate::error::{Error, Result};
use crate::try_from_text_column_value;
use crate::try_number_from_binary_column_value;
use bigdecimal::BigDecimal;
use bytes::{Buf, Bytes};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use smol_str::SmolStr;
use std::collections::HashMap;

/// RowMapper convert single row to its output
///
/// for simpicity, the conversion must not fail,
/// that means user should make sure the data types
/// of each column meets precondition of the conversion
pub trait RowMapper<T> {
    type Output;

    fn map_row(&self, extractor: &ResultSetColExtractor, row: Vec<T>) -> Self::Output;
}

/// generic impls for function
impl<T, F> RowMapper<TextColumnValue> for F
where
    F: Fn(&ResultSetColExtractor, Vec<TextColumnValue>) -> T,
{
    type Output = T;

    fn map_row(
        &self,
        extractor: &ResultSetColExtractor,
        row: Vec<TextColumnValue>,
    ) -> Self::Output {
        self(extractor, row)
    }
}

/// generic impls for function
impl<T, F> RowMapper<BinaryColumnValue> for F
where
    F: Fn(&ResultSetColExtractor, Vec<BinaryColumnValue>) -> T,
{
    type Output = T;

    fn map_row(
        &self,
        extractor: &ResultSetColExtractor,
        row: Vec<BinaryColumnValue>,
    ) -> Self::Output {
        self(extractor, row)
    }
}

#[derive(Debug, Clone)]
pub struct ResultSetColExtractor {
    meta_by_name: HashMap<SmolStr, ResultSetColMeta>,
    meta_by_lc_name: HashMap<SmolStr, ResultSetColMeta>,
}

#[derive(Debug, Clone)]
struct ResultSetColMeta {
    pub idx: usize,
    pub col_type: ColumnType,
}

impl ResultSetColExtractor {
    pub fn new(col_defs: &[ColumnDefinition]) -> Self {
        let mut meta_by_name = HashMap::new();
        let mut meta_by_lc_name = HashMap::new();
        for (idx, col_def) in col_defs.iter().enumerate() {
            let name = SmolStr::new(&col_def.name);
            meta_by_name.insert(
                name,
                ResultSetColMeta {
                    idx,
                    col_type: col_def.col_type,
                },
            );
            let lc_name = SmolStr::new(&col_def.name.to_lowercase());
            meta_by_lc_name.insert(
                lc_name,
                ResultSetColMeta {
                    idx,
                    col_type: col_def.col_type,
                },
            );
        }
        ResultSetColExtractor {
            meta_by_name,
            meta_by_lc_name,
        }
    }

    pub fn get_col<C, V>(&self, row: &[C], idx: usize) -> Result<Option<V>>
    where
        C: Clone,
        V: FromColumnValue<C>,
    {
        if idx >= row.len() {
            return Err(Error::ColumnIndexOutOfBound(format!(
                "column index {} / {}",
                idx,
                row.len()
            )));
        }
        let col = row[idx].clone();
        V::from_value(col)
    }

    pub fn get_named_col<C, V, N>(&self, row: &[C], name: N) -> Result<Option<V>>
    where
        C: Clone,
        V: FromColumnValue<C>,
        N: AsRef<str>,
    {
        let meta = if let Some(meta) = self.meta_by_name.get(name.as_ref()) {
            meta
        } else if let Some(meta) = self.meta_by_lc_name.get(&name.as_ref().to_lowercase()[..]) {
            meta
        } else {
            return Err(Error::ColumnNameNotFound(name.as_ref().to_owned()));
        };
        self.get_col(row, meta.idx)
    }
}

pub trait FromColumnValue<T>
where
    Self: Sized,
{
    fn from_value(value: T) -> Result<Option<Self>>;
}

impl FromColumnValue<TextColumnValue> for NaiveDateTime {
    fn from_value(value: TextColumnValue) -> Result<Option<Self>> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.bytes())?;
                let ts = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))?;
                Ok(Some(ts))
            }
        }
    }
}

impl FromColumnValue<TextColumnValue> for bool {
    fn from_value(value: TextColumnValue) -> Result<Option<Self>> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.bytes())?;
                let v: u8 = s.parse()?;
                Ok(Some(v == 1))
            }
        }
    }
}

try_from_text_column_value!(
    i8, u8, i16, u16, u32, i32, i64, u64, i128, u128, f32, f64, BigDecimal, NaiveDate
);

impl FromColumnValue<BinaryColumnValue> for Bytes {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Blob(bs)
            | BinaryColumnValue::VarString(bs)
            | BinaryColumnValue::String(bs)
            | BinaryColumnValue::Geometry(bs) => Ok(Some(bs)),
            _ => Err(Error::column_type_mismatch("Bytes", &value)),
        }
    }
}

impl FromColumnValue<BinaryColumnValue> for bool {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Tiny(v) => Ok(Some(v == 1)),
            _ => Err(Error::column_type_mismatch("bool", &value)),
        }
    }
}

impl FromColumnValue<BinaryColumnValue> for NaiveDate {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Date { year, month, day } => Ok(Some(NaiveDate::from_ymd(
                year as i32,
                month as u32,
                day as u32,
            ))),
            _ => Err(Error::column_type_mismatch("bool", &value)),
        }
    }
}

impl FromColumnValue<BinaryColumnValue> for BigDecimal {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Decimal(bs) | BinaryColumnValue::NewDecimal(bs) => {
                let s = std::str::from_utf8(bs.bytes())?;
                Ok(Some(s.parse()?))
            }
            _ => Err(Error::column_type_mismatch("BigDecimal", &value)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MyTime {
    pub negative: bool,
    pub days: u32,
    pub time: NaiveTime,
}

impl FromColumnValue<BinaryColumnValue> for MyTime {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Time {
                negative,
                days,
                hours,
                minutes,
                seconds,
                micro_seconds,
            } => {
                let time = NaiveTime::from_hms_micro(
                    hours as u32,
                    minutes as u32,
                    seconds as u32,
                    micro_seconds,
                );
                Ok(Some(MyTime {
                    negative,
                    days,
                    time,
                }))
            }
            _ => Err(Error::column_type_mismatch("bool", &value)),
        }
    }
}

impl FromColumnValue<TextColumnValue> for MyTime {
    fn from_value(value: TextColumnValue) -> Result<Option<Self>> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.bytes())?;
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
                let (days, hours) = if hours >= 24 {
                    (hours / 24, hours % 24)
                } else {
                    (0, hours)
                };
                let minutes: u8 = splits[1].parse()?;
                // handle micro seconds if exists
                let sec_splits: Vec<&str> = splits[2].split('.').collect();
                if sec_splits.len() > 2 {
                    return Err(Error::ParseMyTimeError(format!(
                        "invalid seconds {}",
                        splits[2]
                    )));
                }
                let seconds: u8 = sec_splits[0].parse()?;
                let micro_seconds = if sec_splits.len() == 2 {
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
                    time: NaiveTime::from_hms_micro(
                        hours,
                        minutes as u32,
                        seconds as u32,
                        micro_seconds,
                    ),
                }))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MyI24(pub i32);

impl FromColumnValue<BinaryColumnValue> for MyI24 {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Tiny(v) => Ok(Some(MyI24(v as i8 as i32))),
            BinaryColumnValue::Short(v) => Ok(Some(MyI24(v as i16 as i32))),
            BinaryColumnValue::Int24(v) => {
                if v & 0x80_0000 != 0 {
                    Ok(Some(MyI24((v | 0xff80_0000) as i32)))
                } else {
                    Ok(Some(MyI24(v as i32)))
                }
            }
            _ => Err(Error::column_type_mismatch("MyI24", &value)),
        }
    }
}

impl FromColumnValue<TextColumnValue> for MyI24 {
    fn from_value(value: TextColumnValue) -> Result<Option<Self>> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.bytes())?;
                let n: i32 = s.parse()?;
                Ok(Some(MyI24(n)))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MyU24(pub u32);

impl FromColumnValue<BinaryColumnValue> for MyU24 {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Tiny(v) => Ok(Some(MyU24(v as u32))),
            BinaryColumnValue::Short(v) => Ok(Some(MyU24(v as u32))),
            BinaryColumnValue::Int24(v) => Ok(Some(MyU24(v as u32))),
            _ => Err(Error::column_type_mismatch("MyI24", &value)),
        }
    }
}

impl FromColumnValue<TextColumnValue> for MyU24 {
    fn from_value(value: TextColumnValue) -> Result<Option<Self>> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.bytes())?;
                let n: u32 = s.parse()?;
                Ok(Some(MyU24(n)))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MyYear(pub u16);

impl FromColumnValue<BinaryColumnValue> for MyYear {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Year(n) => Ok(Some(MyYear(n))),
            _ => Err(Error::column_type_mismatch("MyYear", &value)),
        }
    }
}

impl FromColumnValue<TextColumnValue> for MyYear {
    fn from_value(value: TextColumnValue) -> Result<Option<Self>> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.bytes())?;
                let n: u16 = s.parse()?;
                Ok(Some(MyYear(n)))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MyString(pub Bytes);

impl FromColumnValue<BinaryColumnValue> for MyString {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Varchar(bs) | BinaryColumnValue::VarString(bs) => {
                Ok(Some(MyString(bs)))
            }
            _ => Err(Error::column_type_mismatch("MyString", &value)),
        }
    }
}

impl FromColumnValue<TextColumnValue> for MyString {
    fn from_value(value: TextColumnValue) -> Result<Option<Self>> {
        match value {
            None => Ok(None),
            Some(bs) => Ok(Some(MyString(bs))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MyBit(pub Bytes);

impl FromColumnValue<BinaryColumnValue> for MyBit {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Bit(bs) => Ok(Some(MyBit(bs))),
            _ => Err(Error::column_type_mismatch("MyBit", &value)),
        }
    }
}

impl FromColumnValue<TextColumnValue> for MyBit {
    fn from_value(value: TextColumnValue) -> Result<Option<Self>> {
        match value {
            None => Ok(None),
            Some(bs) => Ok(Some(MyBit(bs))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MyBytes(pub Bytes);

impl FromColumnValue<BinaryColumnValue> for MyBytes {
    fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Blob(bs) | BinaryColumnValue::Geometry(bs) => Ok(Some(MyBytes(bs))),
            _ => Err(Error::column_type_mismatch("MyBytes", &value)),
        }
    }
}

impl FromColumnValue<TextColumnValue> for MyBytes {
    fn from_value(value: TextColumnValue) -> Result<Option<Self>> {
        match value {
            None => Ok(None),
            Some(bs) => Ok(Some(MyBytes(bs))),
        }
    }
}

try_number_from_binary_column_value!(i8, Tiny => i8);

try_number_from_binary_column_value!(u8, Tiny => u8);

try_number_from_binary_column_value!(i16, Tiny => i8, Short => i16);

try_number_from_binary_column_value!(u16, Tiny => u8, Short => u16);

try_number_from_binary_column_value!(i32, Tiny => i8, Short => i16, Long => i32);

try_number_from_binary_column_value!(u32, Tiny => u8, Short => u16, Long => u32);

try_number_from_binary_column_value!(i64, Tiny => i8, Short => i16, Long => i32, LongLong => i64);

try_number_from_binary_column_value!(u64, Tiny => u8, Short => u16, Long => u32, LongLong => u64);

#[cfg(test)]
mod tests {

    #[test]
    fn test_num() {
        let i: i8 = -2;
        let j = i as u16;
        println!("{}", j);
        let j = i as u8 as u16;
        println!("{}", j);
    }
}
