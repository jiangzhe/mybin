use crate::bitmap;
use crate::col::{BinaryColumnValue, ColumnMeta, ColumnType, TextColumnValue};
use bytes::Bytes;
use bytes_parser::error::{Error, Result};
use bytes_parser::my::{LenEncStr, ReadMyEnc};
use bytes_parser::{ReadBytesExt, ReadFromBytesWithContext};

/// used for text result set of query execution
#[derive(Debug, Clone)]
pub struct TextRow(pub Vec<TextColumnValue>);

impl<'c> ReadFromBytesWithContext<'c> for TextRow {
    type Context = usize;

    fn read_with_ctx(input: &mut Bytes, col_cnt: usize) -> Result<Self> {
        let mut tcvs = Vec::with_capacity(col_cnt as usize);
        for _ in 0..col_cnt {
            let s = input.read_len_enc_str()?;
            match s {
                LenEncStr::Null => tcvs.push(None),
                LenEncStr::Bytes(bs) => tcvs.push(Some(bs)),
                LenEncStr::Err => {
                    return Err(Error::ConstraintError(
                        "invalid text column value".to_owned(),
                    ))
                }
            }
        }
        Ok(TextRow(tcvs))
    }
}

/// used for binary result set of statement execution
#[derive(Debug, Clone)]
pub struct BinaryRow(pub Vec<BinaryColumnValue>);

impl<'c> ReadFromBytesWithContext<'c> for BinaryRow {
    type Context = &'c [ColumnType];

    fn read_with_ctx(input: &mut Bytes, col_types: Self::Context) -> Result<Self> {
        // header 0x00
        input.read_u8()?;
        // null bitmap with offset 2
        let bitmap_len = (col_types.len() + 7 + 2) >> 3;
        let null_bitmap = input.read_len(bitmap_len)?;
        let mut cols = Vec::with_capacity(col_types.len());
        for (col_type, null) in col_types.iter().zip(bitmap::to_iter(&null_bitmap, 2)) {
            if null {
                cols.push(BinaryColumnValue::Null);
            } else {
                let col = BinaryColumnValue::read_with_ctx(input, *col_type)?;
                cols.push(col);
            }
        }
        Ok(BinaryRow(cols))
    }
}

/// used for binlog
#[derive(Debug, Clone)]
pub struct LogRow(pub Vec<BinaryColumnValue>);

impl<'c> ReadFromBytesWithContext<'c> for LogRow {
    type Context = (usize, &'c [u8], &'c [ColumnMeta]);

    fn read_with_ctx(
        input: &mut Bytes,
        (n_cols, col_bm, col_metas): Self::Context,
    ) -> Result<Self> {
        let mut cols = Vec::with_capacity(n_cols);
        for i in 0..n_cols {
            if bitmap::index(col_bm, i) {
                let col_meta = &col_metas[i];
                let col_val = BinaryColumnValue::read_with_ctx(input, col_meta.into())?;
                cols.push(col_val);
            } else {
                cols.push(BinaryColumnValue::Null);
            }
        }
        Ok(LogRow(cols))
    }
}
