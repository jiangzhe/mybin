use crate::binlog::rows_v2::{RowsV2, UpdateRowsV2};
use crate::binlog::transform::{filter_col_defs, FromRowsV2};
use crate::col::{BinaryColumnValue, ColumnDefinition};
use crate::stmt::StmtColumnValue;
use bytes::Buf;
use serde_derive::*;
use serde_json::{Map, Number, Value};
use smol_str::SmolStr;

#[derive(Debug, Serialize)]
pub struct JsonRows(Vec<JsonRow>);

impl FromRowsV2 for JsonRows {
    fn from_insert(
        db: SmolStr,
        tbl: SmolStr,
        rowsv2: RowsV2,
        col_defs: &[ColumnDefinition],
    ) -> Self {
        let col_defs = filter_col_defs(rowsv2.present_bitmap.chunk(), col_defs);
        let mut rows = Vec::with_capacity(rowsv2.rows.len());
        for cols in rowsv2.rows {
            let mut map = Map::with_capacity(cols.0.len());
            let mut base64_encoded = vec![];
            for (def, col) in col_defs.iter().zip(cols.0.into_iter()) {
                let sv = StmtColumnValue::from((col, def.unsigned));
                let (jv, enc) = to_json_value(sv);
                map.insert(def.name.to_string(), jv);
                if enc {
                    base64_encoded.push(def.name.clone());
                }
            }
            let row = JsonRow {
                ty: "insert",
                base64_encoded,
                db: db.clone(),
                tbl: tbl.clone(),
                before: None,
                after: Some(Value::Object(map)),
            };
            rows.push(row);
        }
        JsonRows(rows)
    }

    fn from_delete(
        db: SmolStr,
        tbl: SmolStr,
        rowsv2: RowsV2,
        col_defs: &[ColumnDefinition],
    ) -> Self {
        let col_defs = filter_col_defs(rowsv2.present_bitmap.chunk(), col_defs);
        let mut rows = Vec::with_capacity(rowsv2.rows.len());
        for cols in rowsv2.rows {
            let mut map = Map::with_capacity(cols.0.len());
            let mut base64_encoded = vec![];
            for (def, col) in col_defs.iter().zip(cols.0.into_iter()) {
                let sv = StmtColumnValue::from((col, def.unsigned));
                let (jv, enc) = to_json_value(sv);
                map.insert(def.name.to_string(), jv);
                if enc {
                    base64_encoded.push(def.name.clone());
                }
            }
            let row = JsonRow {
                ty: "delete",
                base64_encoded,
                db: db.clone(),
                tbl: tbl.clone(),
                before: Some(Value::Object(map)),
                after: None,
            };
            rows.push(row);
        }
        JsonRows(rows)
    }

    fn from_update(
        db: SmolStr,
        tbl: SmolStr,
        rowsv2: UpdateRowsV2,
        col_defs: &[ColumnDefinition],
    ) -> Self {
        let before_col_defs = filter_col_defs(rowsv2.before_present_bitmap.chunk(), col_defs);
        let after_col_defs = filter_col_defs(rowsv2.after_present_bitmap.chunk(), col_defs);
        let mut rows = Vec::with_capacity(rowsv2.rows.len());
        for cols in rowsv2.rows {
            let mut before_map = Map::with_capacity(cols.0.len());
            let mut after_map = Map::with_capacity(cols.1.len());
            let mut base64_encoded = vec![];
            for (def, col) in before_col_defs.iter().zip(cols.0.into_iter()) {
                let sv = StmtColumnValue::from((col, def.unsigned));
                let (jv, enc) = to_json_value(sv);
                before_map.insert(def.name.to_string(), jv);
                if enc {
                    base64_encoded.push(def.name.clone());
                }
            }
            for (def, col) in after_col_defs.iter().zip(cols.1.into_iter()) {
                let sv = StmtColumnValue::from((col, def.unsigned));
                let (jv, enc) = to_json_value(sv);
                after_map.insert(def.name.to_string(), jv);
                if enc && !base64_encoded.contains(&def.name) {
                    base64_encoded.push(def.name.clone());
                }
            }
            let row = JsonRow {
                ty: "update",
                base64_encoded,
                db: db.clone(),
                tbl: tbl.clone(),
                before: Some(Value::Object(before_map)),
                after: Some(Value::Object(after_map)),
            };
            rows.push(row);
        }
        JsonRows(rows)
    }
}

fn to_json_value(sv: StmtColumnValue) -> (Value, bool) {
    let v = match sv.val {
        BinaryColumnValue::Tiny(v) => {
            if sv.unsigned {
                Value::Number(v.into())
            } else {
                Value::Number((v as i8).into())
            }
        }
        BinaryColumnValue::Short(v) | BinaryColumnValue::Year(v) => {
            if sv.unsigned {
                Value::Number(v.into())
            } else {
                Value::Number((v as i16).into())
            }
        }
        BinaryColumnValue::Long(v) => {
            if sv.unsigned {
                Value::Number(v.into())
            } else {
                Value::Number((v as i32).into())
            }
        }
        BinaryColumnValue::Float(v) => Value::Number(Number::from_f64(v as f64).unwrap()),
        BinaryColumnValue::Double(v) => Value::Number(Number::from_f64(v).unwrap()),
        BinaryColumnValue::Null => Value::Null,
        BinaryColumnValue::Timestamp(ts) | BinaryColumnValue::DateTime(ts) => {
            if ts.micro_second == 0 {
                Value::String(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second
                ))
            } else {
                Value::String(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                    ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second, ts.micro_second
                ))
            }
        }
        BinaryColumnValue::LongLong(v) => {
            if sv.unsigned {
                Value::Number(v.into())
            } else {
                Value::Number((v as i64).into())
            }
        }
        BinaryColumnValue::Int24(v) => {
            if sv.unsigned {
                Value::Number(v.into())
            } else {
                Value::Number((v as i32).into())
            }
        }
        BinaryColumnValue::Date { year, month, day } => {
            Value::String(format!("{:04}-{:02}-{:02}", year, month, day))
        }
        BinaryColumnValue::Time(tm) => match (tm.negative, tm.micro_second == 0) {
            (true, true) => Value::String(format!(
                "-{:02}:{:02}:{:02}",
                tm.days * 24 + tm.hour as u32,
                tm.minute,
                tm.second
            )),
            (true, false) => Value::String(format!(
                "-{:02}:{:02}:{:02}.{:06}",
                tm.days * 24 + tm.hour as u32,
                tm.minute,
                tm.second,
                tm.micro_second
            )),
            (false, true) => Value::String(format!(
                "{:02}:{:02}:{:02}",
                tm.days * 24 + tm.hour as u32,
                tm.minute,
                tm.second
            )),
            (false, false) => Value::String(format!(
                "{:02}:{:02}:{:02}.{:06}",
                tm.days * 24 + tm.hour as u32,
                tm.minute,
                tm.second,
                tm.micro_second
            )),
        },
        BinaryColumnValue::Bit(bs) => {
            // convert bitmap to u64
            let mut n = 0_u64;
            for (i, b) in bs.to_vec().into_iter().enumerate() {
                n += (b as u64) << (i * 8);
            }
            Value::Number(n.into())
        }
        BinaryColumnValue::NewDecimal(bs)
        | BinaryColumnValue::VarString(bs)
        | BinaryColumnValue::String(bs) => Value::String(String::from_utf8(bs.to_vec()).unwrap()),
        // encode with base64
        BinaryColumnValue::Blob(bs) | BinaryColumnValue::Geometry(bs) => {
            return (Value::String(base64::encode(&bs)), true)
        }
    };
    (v, false)
}

#[derive(Debug, Serialize)]
pub struct JsonRow {
    #[serde(rename = "type")]
    pub ty: &'static str,
    // binary values are encoded in base64 format
    // and store names in this field
    pub base64_encoded: Vec<SmolStr>,
    pub db: SmolStr,
    pub tbl: SmolStr,
    pub before: Option<Value>,
    pub after: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_stmt_value_to_json() {
        let sv1 = StmtColumnValue::new_decimal("1.23".parse().unwrap());
        let (jv, enc) = to_json_value(sv1);
        assert_eq!(Value::String("1.23".to_owned()), jv);
        assert!(!enc);
    }
}
