pub mod json;
pub mod sql;

use crate::binlog::rows_v2::{RowsV2, UpdateRowsV2};
use crate::bitmap;
use crate::col::{ColumnDefinition, ColumnFlags, ColumnType};
use smol_str::SmolStr;
use std::fmt::Debug;

/// Base trait to convert rows v2 to certain types
pub trait FromRowsV2: Debug + Sized {
    fn from_insert(
        db: SmolStr,
        tbl: SmolStr,
        rowsv2: RowsV2,
        col_defs: &[ColumnDefinition],
    ) -> Self;

    fn from_delete(
        db: SmolStr,
        tbl: SmolStr,
        rowsv2: RowsV2,
        col_defs: &[ColumnDefinition],
    ) -> Self;

    fn from_update(
        db: SmolStr,
        tbl: SmolStr,
        rowsv2: UpdateRowsV2,
        col_defs: &[ColumnDefinition],
    ) -> Self;
}

pub(self) fn filter_col_defs(present_bitmap: &[u8], col_defs: &[ColumnDefinition]) -> Vec<ColDef> {
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
pub(self) struct ColDef {
    pub name: SmolStr,
    pub col_type: ColumnType,
    pub unsigned: bool,
    pub key: bool,
}
