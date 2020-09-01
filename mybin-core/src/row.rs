use crate::col::{BinaryColumnValue, ColumnMeta, TextColumnValue};
use crate::util::bitmap_index;
use bytes::Bytes;
use bytes_parser::error::{Error, Result};
use bytes_parser::my::{LenEncStr, ReadMyEnc};
use bytes_parser::ReadFromBytesWithContext;

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

#[derive(Debug, Clone)]
pub struct Row(pub Vec<BinaryColumnValue>);

impl<'c> ReadFromBytesWithContext<'c> for Row {
    type Context = (usize, &'c [u8], &'c [ColumnMeta]);

    fn read_with_ctx(input: &mut Bytes, (n_cols, col_bm, col_metas): Self::Context) -> Result<Row> {
        let mut cols = Vec::with_capacity(n_cols);
        for i in 0..n_cols {
            if bitmap_index(col_bm, i) {
                let col_val = BinaryColumnValue::read_with_ctx(input, &col_metas[i])?;
                cols.push(col_val);
            } else {
                cols.push(BinaryColumnValue::Null);
            }
        }
        Ok(Row(cols))
    }
}
