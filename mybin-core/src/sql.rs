use crate::binlog::rows_v2::{RowsV2, UpdateRowsV2};
use crate::bitmap;
use crate::col::{ColumnDefinition, ColumnFlags, ColumnType};
use crate::stmt::StmtColumnValue;
use bytes::Buf;
use smol_str::SmolStr;
use std::borrow::Cow;

/// marker trait of sql collection
///
/// also provide function to get string list
pub trait SqlCollection {
    /// list sql as string list
    fn sql_list(&self) -> Vec<Cow<str>>;
}

#[derive(Debug, Clone)]
pub struct PlainSql {
    pub dbname: SmolStr,
    pub sql: String,
    pub ddl: bool,
}

impl PlainSql {
    pub fn new(dbname: SmolStr, sql: impl Into<String>, ddl: bool) -> Self {
        Self {
            dbname,
            sql: sql.into(),
            ddl,
        }
    }
}

impl SqlCollection for PlainSql {
    fn sql_list(&self) -> Vec<Cow<str>> {
        vec![Cow::Borrowed(&self.sql)]
    }
}

#[derive(Debug, Clone)]
pub struct PreparedSql {
    pub dbname: SmolStr,
    pub sql_fragments: Vec<String>,
    pub params: Vec<Vec<StmtColumnValue>>,
}

impl PreparedSql {
    pub fn new(
        dbname: SmolStr,
        sql_fragments: Vec<String>,
        params: Vec<Vec<StmtColumnValue>>,
    ) -> Self {
        Self {
            dbname,
            sql_fragments,
            params,
        }
    }

    pub fn sql_stmt(&self) -> Cow<String> {
        Cow::Owned(self.sql_fragments.concat())
    }

    pub fn delete(
        db: SmolStr,
        tbl: SmolStr,
        rowsv2: RowsV2,
        col_defs: &[ColumnDefinition],
    ) -> PreparedSql {
        let col_defs = filter_col_defs(rowsv2.present_bitmap.chunk(), col_defs);
        let sql_fragments = delete_sql_fragments(&db, &tbl, &col_defs);
        let mut params = Vec::with_capacity(rowsv2.rows.len());
        for cols in rowsv2.rows {
            let param: Vec<StmtColumnValue> = col_defs
                .iter()
                .zip(cols.0.into_iter())
                .filter(|(cn, _)| cn.key)
                .map(|(cn, row)| StmtColumnValue::from((row, cn.unsigned)))
                .collect();
            params.push(param);
        }
        Self::new(db, sql_fragments, params)
    }

    pub fn insert(
        db: SmolStr,
        tbl: SmolStr,
        rowsv2: RowsV2,
        col_defs: &[ColumnDefinition],
    ) -> PreparedSql {
        let col_defs = filter_col_defs(rowsv2.present_bitmap.chunk(), col_defs);
        let sql_fragments = insert_sql_fragments(&db, &tbl, &col_defs);
        let mut params = Vec::with_capacity(rowsv2.rows.len());
        for cols in rowsv2.rows {
            let param: Vec<StmtColumnValue> = col_defs
                .iter()
                .zip(cols.0.into_iter())
                .map(|(cn, row)| StmtColumnValue::from((row, cn.unsigned)))
                .collect();
            params.push(param);
        }
        Self::new(db, sql_fragments, params)
    }

    /// only return sql if key column exists
    pub fn update(
        db: SmolStr,
        tbl: SmolStr,
        rowsv2: UpdateRowsV2,
        col_defs: &[ColumnDefinition],
    ) -> Option<PreparedSql> {
        if col_defs.iter().all(|def| {
            !def.flags.contains(ColumnFlags::PRIMARY_KEY)
                && !def.flags.contains(ColumnFlags::UNIQUE_KEY)
        }) {
            return None;
        }
        let before_col_defs = filter_col_defs(rowsv2.before_present_bitmap.chunk(), col_defs);
        let after_col_defs = filter_col_defs(rowsv2.after_present_bitmap.chunk(), col_defs);
        let sql_fragments = update_sql_fragments(&db, &tbl, &before_col_defs, &after_col_defs);
        let mut params = Vec::new();
        for cols in rowsv2.rows {
            let mut param = Vec::new();
            for (def, row) in after_col_defs.iter().zip(cols.1.into_iter()) {
                param.push(StmtColumnValue::from((row, def.unsigned)));
            }
            for (def, row) in before_col_defs.iter().zip(cols.0.into_iter()) {
                if def.key {
                    param.push(StmtColumnValue::from((row, def.unsigned)));
                }
            }
            params.push(param);
        }
        Some(Self::new(db, sql_fragments, params))
    }
}

impl SqlCollection for PreparedSql {
    fn sql_list(&self) -> Vec<Cow<str>> {
        let mut list = Vec::with_capacity(self.params.len());
        for cols in &self.params {
            let mut sql = String::new();
            let mut param_iter = cols.iter();
            for f in &self.sql_fragments {
                if f == "?" {
                    if let Some(param) = param_iter.next() {
                        let (lit, quote) = param.to_sql_literal();
                        if quote {
                            sql.push('\'');
                        }
                        sql.push_str(&lit);
                        if quote {
                            sql.push('\'');
                        }
                    }
                } else {
                    sql.push_str(f);
                }
            }
            list.push(Cow::Owned(sql));
        }
        list
    }
}

fn delete_sql_fragments(db: &SmolStr, tbl: &SmolStr, col_defs: &[ColDef]) -> Vec<String> {
    let mut sql_fragments = Vec::new();
    sql_fragments.push(format!("DELETE FROM `{}`.`{}` WHERE ", db, tbl));
    let mut idx = 0;
    for cf in col_defs {
        if cf.key {
            if idx > 0 {
                sql_fragments.push(format!(" AND `{}` = ", cf.name));
            } else {
                sql_fragments
                    .last_mut()
                    .unwrap()
                    .push_str(&format!("`{}` = ", cf.name));
            }
            sql_fragments.push("?".to_owned());
            idx += 1;
        }
    }
    sql_fragments
}

fn update_sql_fragments(
    db: &SmolStr,
    tbl: &SmolStr,
    before_col_defs: &[ColDef],
    after_col_defs: &[ColDef],
) -> Vec<String> {
    let mut sql_fragments = Vec::new();
    sql_fragments.push(format!("UPDATE `{}`.`{}` SET ", db, tbl));
    for (idx, cf) in after_col_defs.iter().enumerate() {
        if idx > 0 {
            sql_fragments.push(format!(", `{}` = ", cf.name));
        } else {
            sql_fragments
                .last_mut()
                .unwrap()
                .push_str(&format!("`{}` = ", cf.name));
        }
        sql_fragments.push("?".to_owned());
    }
    sql_fragments.push(format!(" WHERE "));
    let mut idx = 0;
    for cf in before_col_defs {
        if cf.key {
            if idx > 0 {
                sql_fragments.push(format!(" AND `{}` = ", cf.name));
            } else {
                sql_fragments
                    .last_mut()
                    .unwrap()
                    .push_str(&format!("`{}` = ", cf.name));
            }
            sql_fragments.push("?".to_owned());
            idx += 1;
        }
    }
    sql_fragments
}

fn insert_sql_fragments(db: &SmolStr, tbl: &SmolStr, col_defs: &[ColDef]) -> Vec<String> {
    let mut sql_fragments = Vec::new();
    let mut s = format!("INSERT INTO `{}`.`{}` (", db, tbl);
    for (idx, cf) in col_defs.iter().enumerate() {
        if idx > 0 {
            s.push(',');
        }
        s.push('`');
        s.push_str(&cf.name);
        s.push('`');
    }
    s.push_str(") VALUES (");
    sql_fragments.push(s);
    for (idx, _) in col_defs.iter().enumerate() {
        if idx > 0 {
            sql_fragments.push(",".to_owned());
        }
        sql_fragments.push("?".to_owned());
    }
    sql_fragments.push(")".to_owned());
    sql_fragments
}

fn filter_col_defs(present_bitmap: &[u8], col_defs: &[ColumnDefinition]) -> Vec<ColDef> {
    bitmap::to_iter(present_bitmap, 0)
        .zip(col_defs.iter())
        .filter(|(present, _)| *present)
        .map(|(_, def)| ColDef {
            name: def.name.clone(),
            col_type: def.col_type,
            unsigned: def.flags.contains(ColumnFlags::UNSIGNED),
            key: def.flags.contains(ColumnFlags::PRIMARY_KEY)
                || def.flags.contains(ColumnFlags::UNIQUE_KEY),
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct ColDef {
    pub name: SmolStr,
    pub col_type: ColumnType,
    pub unsigned: bool,
    pub key: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stmt::ToColumnValue;

    #[test]
    fn test_plain_sql() {
        let ps = PlainSql::new("bintest1".into(), "create table plain1 (id int)", true);
        assert_eq!(
            vec!["create table plain1 (id int)".to_owned()],
            ps.sql_list()
        );
    }

    #[test]
    fn test_prepared_sql() {
        let ps = PreparedSql::new(
            "bintest1".into(),
            vec!["insert into plain1 (id) values (", "?", ")"]
                .into_iter()
                .map(|s| s.to_owned())
                .collect(),
            vec![vec![1u32.to_col()]],
        );
        assert_eq!(vec!["insert into plain1 (id) values (1)"], ps.sql_list());
    }
}
