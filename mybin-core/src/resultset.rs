use crate::col::{BinaryColumnValue, ColumnDefinition, ColumnType, TextColumnValue};
use crate::error::{Error, Result};
use crate::try_from_text_column_value;
use crate::try_non_null_column_value;
use crate::try_number_from_binary_column_value;
use bigdecimal::BigDecimal;
use bytes::{Buf, Bytes};
use chrono::{NaiveDate, NaiveDateTime};
use smol_str::SmolStr;
use std::collections::HashMap;

/// define types that can be converted from column value
pub trait FromColumnValue<T>
where
    Self: Sized,
{
    fn from_col(value: T) -> Result<Self>;
}

/// RowMapper convert single row to its output
///
/// for simpicity, the conversion must not fail,
/// that means user should make sure the data types
/// of each column meets precondition of the conversion
pub trait RowMapper<T> {
    type Output;

    fn map_row(&self, extractor: &ColumnExtractor, row: Vec<T>) -> Self::Output;
}

/// generic impls for function
impl<T, F> RowMapper<TextColumnValue> for F
where
    F: Fn(&ColumnExtractor, Vec<TextColumnValue>) -> T,
{
    type Output = T;

    fn map_row(&self, extractor: &ColumnExtractor, row: Vec<TextColumnValue>) -> Self::Output {
        self(extractor, row)
    }
}

/// generic impls for function
impl<T, F> RowMapper<BinaryColumnValue> for F
where
    F: Fn(&ColumnExtractor, Vec<BinaryColumnValue>) -> T,
{
    type Output = T;

    fn map_row(&self, extractor: &ColumnExtractor, row: Vec<BinaryColumnValue>) -> Self::Output {
        self(extractor, row)
    }
}

#[derive(Debug, Clone)]
pub struct ColumnExtractor {
    meta_by_name: HashMap<SmolStr, ResultSetColMeta>,
    meta_by_lc_name: HashMap<SmolStr, ResultSetColMeta>,
}

#[derive(Debug, Clone)]
struct ResultSetColMeta {
    pub idx: usize,
    pub col_type: ColumnType,
}

impl ColumnExtractor {
    pub fn new(col_defs: &[ColumnDefinition]) -> Self {
        let mut meta_by_name = HashMap::new();
        let mut meta_by_lc_name = HashMap::new();
        for (idx, col_def) in col_defs.iter().enumerate() {
            let name = col_def.name.clone();
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
        ColumnExtractor {
            meta_by_name,
            meta_by_lc_name,
        }
    }

    pub fn get_col<C, V>(&self, row: &[C], idx: usize) -> Result<V>
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
        V::from_col(col)
    }

    pub fn get_named_col<C, V, N>(&self, row: &[C], name: N) -> Result<V>
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

impl FromColumnValue<TextColumnValue> for Option<NaiveDateTime> {
    fn from_col(value: TextColumnValue) -> Result<Self> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.chunk())?;
                let ts = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))?;
                Ok(Some(ts))
            }
        }
    }
}

try_non_null_column_value!(TextColumnValue => NaiveDateTime);

impl FromColumnValue<TextColumnValue> for Option<bool> {
    fn from_col(value: TextColumnValue) -> Result<Self> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.chunk())?;
                let v: u8 = s.parse()?;
                Ok(Some(v == 1))
            }
        }
    }
}

try_non_null_column_value!(TextColumnValue => bool);

try_from_text_column_value!(
    i8, u8, i16, u16, u32, i32, i64, u64, i128, u128, f32, f64, BigDecimal, NaiveDate
);

try_non_null_column_value!(TextColumnValue =>
    i8, u8, i16, u16, u32, i32, i64, u64, i128, u128, f32, f64, BigDecimal, NaiveDate
);

impl FromColumnValue<BinaryColumnValue> for Option<Bytes> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
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

try_non_null_column_value!(BinaryColumnValue => Bytes);

impl FromColumnValue<TextColumnValue> for Option<Bytes> {
    fn from_col(value: TextColumnValue) -> Result<Self> {
        Ok(value)
    }
}

try_non_null_column_value!(TextColumnValue => Bytes);

impl FromColumnValue<BinaryColumnValue> for Option<bool> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Tiny(v) => Ok(Some(v == 1)),
            _ => Err(Error::column_type_mismatch("bool", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => bool);

impl FromColumnValue<BinaryColumnValue> for Option<NaiveDate> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
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

try_non_null_column_value!(BinaryColumnValue => NaiveDate);

impl FromColumnValue<BinaryColumnValue> for Option<NaiveDateTime> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::DateTime(ts) | BinaryColumnValue::Timestamp(ts) => {
                Ok(Some(ts.into()))
            }
            _ => Err(Error::column_type_mismatch("datetime", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => NaiveDateTime);

impl FromColumnValue<BinaryColumnValue> for Option<BigDecimal> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::NewDecimal(bs) => {
                let s = std::str::from_utf8(bs.chunk())?;
                Ok(Some(s.parse()?))
            }
            _ => Err(Error::column_type_mismatch("BigDecimal", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => BigDecimal);

#[derive(Debug, Clone)]
pub struct MyI24(pub i32);

impl FromColumnValue<BinaryColumnValue> for Option<MyI24> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
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

try_non_null_column_value!(BinaryColumnValue => MyI24);

impl FromColumnValue<TextColumnValue> for Option<MyI24> {
    fn from_col(value: TextColumnValue) -> Result<Self> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.chunk())?;
                let n: i32 = s.parse()?;
                Ok(Some(MyI24(n)))
            }
        }
    }
}

try_non_null_column_value!(TextColumnValue => MyI24);

#[derive(Debug, Clone)]
pub struct MyU24(pub u32);

impl FromColumnValue<BinaryColumnValue> for Option<MyU24> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Tiny(v) => Ok(Some(MyU24(v as u32))),
            BinaryColumnValue::Short(v) => Ok(Some(MyU24(v as u32))),
            BinaryColumnValue::Int24(v) => Ok(Some(MyU24(v as u32))),
            _ => Err(Error::column_type_mismatch("MyI24", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => MyU24);

impl FromColumnValue<TextColumnValue> for Option<MyU24> {
    fn from_col(value: TextColumnValue) -> Result<Self> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.chunk())?;
                let n: u32 = s.parse()?;
                Ok(Some(MyU24(n)))
            }
        }
    }
}

try_non_null_column_value!(TextColumnValue => MyU24);

#[derive(Debug, Clone)]
pub struct MyYear(pub u16);

impl FromColumnValue<BinaryColumnValue> for Option<MyYear> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Year(n) => Ok(Some(MyYear(n))),
            _ => Err(Error::column_type_mismatch("MyYear", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => MyYear);

impl FromColumnValue<TextColumnValue> for Option<MyYear> {
    fn from_col(value: TextColumnValue) -> Result<Self> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = std::str::from_utf8(bs.chunk())?;
                let n: u16 = s.parse()?;
                Ok(Some(MyYear(n)))
            }
        }
    }
}

try_non_null_column_value!(TextColumnValue => MyYear);

impl FromColumnValue<BinaryColumnValue> for Option<String> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Blob(bs)
            | BinaryColumnValue::VarString(bs)
            | BinaryColumnValue::String(bs) => {
                let s = String::from_utf8(Vec::from(bs.chunk()))?;
                Ok(Some(s))
            }
            _ => Err(Error::column_type_mismatch("String", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => String);

impl FromColumnValue<TextColumnValue> for Option<String> {
    fn from_col(value: TextColumnValue) -> Result<Self> {
        match value {
            None => Ok(None),
            Some(bs) => {
                let s = String::from_utf8(Vec::from(bs.chunk()))?;
                Ok(Some(s))
            }
        }
    }
}

try_non_null_column_value!(TextColumnValue => String);

#[derive(Debug, Clone)]
pub struct MyBit(pub Bytes);

impl FromColumnValue<BinaryColumnValue> for Option<MyBit> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Bit(bs) => Ok(Some(MyBit(bs))),
            _ => Err(Error::column_type_mismatch("MyBit", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => MyBit);

// todo: fix it
impl FromColumnValue<TextColumnValue> for Option<MyBit> {
    fn from_col(value: TextColumnValue) -> Result<Self> {
        match value {
            None => Ok(None),
            Some(bs) => Ok(Some(MyBit(bs))),
        }
    }
}

try_non_null_column_value!(TextColumnValue => MyBit);

impl FromColumnValue<BinaryColumnValue> for Option<f32> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Float(n) => Ok(Some(n)),
            _ => Err(Error::column_type_mismatch("f32", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => f32);

impl FromColumnValue<BinaryColumnValue> for Option<f64> {
    fn from_col(value: BinaryColumnValue) -> Result<Self> {
        match value {
            BinaryColumnValue::Null => Ok(None),
            BinaryColumnValue::Double(n) => Ok(Some(n)),
            _ => Err(Error::column_type_mismatch("f64", &value)),
        }
    }
}

try_non_null_column_value!(BinaryColumnValue => f64);

try_number_from_binary_column_value!(i8, Tiny => i8);

try_number_from_binary_column_value!(u8, Tiny => u8);

try_number_from_binary_column_value!(i16, Tiny => i8, Short => i16);

try_number_from_binary_column_value!(u16, Tiny => u8, Short => u16);

try_number_from_binary_column_value!(i32, Tiny => i8, Short => i16, Long => i32);

try_number_from_binary_column_value!(u32, Tiny => u8, Short => u16, Long => u32);

try_number_from_binary_column_value!(i64, Tiny => i8, Short => i16, Long => i32, LongLong => i64);

try_number_from_binary_column_value!(u64, Tiny => u8, Short => u16, Long => u32, LongLong => u64);

try_non_null_column_value!(BinaryColumnValue => i8, u8, i16, u16, i32, u32, i64, u64);

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
