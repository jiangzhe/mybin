//! meaningful data structures and parsing logic of RowsEventV2
use crate::col::{ColumnMetadata, ColumnValue};
use crate::util::{bitmap_index, bitmap_iter, bitmap_mark};
use bytes::{Buf, Bytes};
use bytes_parser::error::{Error, Result};
use bytes_parser::my::ReadMyEnc;
use bytes_parser::{ReadBytesExt, ReadFromBytes, ReadFromBytesWithContext};

/// Data of WriteRowsEventV2
///
/// reference: https://dev.mysql.com/doc/internals/en/rows-event.html
/// similar to v1 row events
/// detailed row information will be handled by separate module
#[derive(Debug, Clone)]
pub struct WriteRowsDataV2 {
    // actual 6-byte integer
    pub table_id: u64,
    pub flags: u16,
    pub extra_data_len: u16,
    // below is variable part
    pub payload: Bytes,
}

impl WriteRowsDataV2 {
    pub fn rows(&self, col_metas: &[ColumnMetadata]) -> Result<RowsV2> {
        RowsV2::read_with_ctx(
            &mut self.payload.clone(),
            (self.extra_data_len as usize, col_metas),
        )
    }

    pub fn into_rows(mut self, col_metas: &[ColumnMetadata]) -> Result<RowsV2> {
        RowsV2::read_with_ctx(&mut self.payload, (self.extra_data_len as usize, col_metas))
    }
}

impl ReadFromBytes for WriteRowsDataV2 {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let table_id = input.read_le_u48()?;
        let flags = input.read_le_u16()?;
        let extra_data_len = input.read_le_u16()?;
        let payload = input.split_to(input.remaining());
        Ok(WriteRowsDataV2 {
            table_id,
            flags,
            extra_data_len,
            payload,
        })
    }
}

/// Data of UpdateRowsEventV2
#[derive(Debug, Clone)]
pub struct UpdateRowsDataV2 {
    // actual 6-byte integer
    pub table_id: u64,
    pub flags: u16,
    pub extra_data_len: u16,
    // below is variable part
    pub payload: Bytes,
}

impl UpdateRowsDataV2 {
    pub fn rows(&self, col_metas: &[ColumnMetadata]) -> Result<UpdateRowsV2> {
        UpdateRowsV2::read_with_ctx(
            &mut self.payload.clone(),
            (self.extra_data_len as usize, col_metas),
        )
    }

    pub fn into_rows(mut self, col_metas: &[ColumnMetadata]) -> Result<UpdateRowsV2> {
        UpdateRowsV2::read_with_ctx(&mut self.payload, (self.extra_data_len as usize, col_metas))
    }
}

impl ReadFromBytes for UpdateRowsDataV2 {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let wrd = WriteRowsDataV2::read_from(input)?;
        Ok(UpdateRowsDataV2 {
            table_id: wrd.table_id,
            flags: wrd.flags,
            extra_data_len: wrd.extra_data_len,
            payload: wrd.payload,
        })
    }
}

/// Data of DeleteRowsEventV2
#[derive(Debug, Clone)]
pub struct DeleteRowsDataV2 {
    // actual 6-byte integer
    pub table_id: u64,
    pub flags: u16,
    pub extra_data_len: u16,
    // below is variable part
    pub payload: Bytes,
}

impl DeleteRowsDataV2 {
    pub fn rows(&self, col_metas: &[ColumnMetadata]) -> Result<RowsV2> {
        RowsV2::read_with_ctx(
            &mut self.payload.clone(),
            (self.extra_data_len as usize, col_metas),
        )
    }

    pub fn into_rows(mut self, col_metas: &[ColumnMetadata]) -> Result<RowsV2> {
        RowsV2::read_with_ctx(&mut self.payload, (self.extra_data_len as usize, col_metas))
    }
}

impl ReadFromBytes for DeleteRowsDataV2 {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let wrd = WriteRowsDataV2::read_from(input)?;
        Ok(DeleteRowsDataV2 {
            table_id: wrd.table_id,
            flags: wrd.flags,
            extra_data_len: wrd.extra_data_len,
            payload: wrd.payload,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RowsV2 {
    pub extra_data: Bytes,
    pub n_cols: u32,
    // represent before if DELETE, represent after if WRITE
    pub present_bitmap: Bytes,
    pub rows: Vec<Row>,
}

impl<'c> ReadFromBytesWithContext<'c> for RowsV2 {
    type Context = (usize, &'c [ColumnMetadata]);

    fn read_with_ctx(
        input: &mut Bytes,
        (extra_data_len, col_metas): Self::Context,
    ) -> Result<RowsV2> {
        let extra_data = input.read_len(extra_data_len - 2)?;
        // all columns
        let n_cols = input.read_len_enc_int()?;
        let n_cols = n_cols
            .to_u32()
            .ok_or_else(|| Error::ConstraintError(format!("invalid n_cols: {:?}", n_cols)))?;
        let bitmap_len = (n_cols + 7) >> 3;
        let present_bitmap = input.read_len(bitmap_len as usize)?;
        // present columns
        let present_cols = bitmap_iter(present_bitmap.as_ref())
            .take(n_cols as usize)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        let null_bitmap_len = (present_cols + 7) >> 3;
        let mut rows = Vec::new();
        while input.has_remaining() {
            let null_bitmap = input.read_len(null_bitmap_len as usize)?;
            // use present_bitmap as base and mark null using null_bitmap
            let mut col_bitmap = Vec::from(present_bitmap.as_ref());
            let mut j = 0;
            for i in 0..present_cols {
                while !bitmap_index(&col_bitmap, j) {
                    j += 1;
                }
                bitmap_mark(
                    &mut col_bitmap,
                    j,
                    !bitmap_index(null_bitmap.as_ref(), i as usize),
                );
            }
            let row = Row::read_with_ctx(input, (n_cols as usize, &col_bitmap[..], col_metas))?;
            rows.push(row);
        }
        Ok(RowsV2 {
            extra_data,
            n_cols,
            present_bitmap,
            rows,
        })
    }
}

#[derive(Debug, Clone)]
pub struct UpdateRowsV2 {
    pub extra_data: Bytes,
    pub n_cols: u32,
    pub before_present_bitmap: Bytes,
    pub after_present_bitmap: Bytes,
    pub rows: Vec<UpdateRow>,
}

impl<'c> ReadFromBytesWithContext<'c> for UpdateRowsV2 {
    type Context = (usize, &'c [ColumnMetadata]);

    fn read_with_ctx(
        input: &mut Bytes,
        (extra_data_len, col_metas): Self::Context,
    ) -> Result<UpdateRowsV2> {
        let extra_data = input.read_len(extra_data_len - 2)?;
        // all columns
        let n_cols = input.read_len_enc_int()?;
        let n_cols = n_cols
            .to_u32()
            .ok_or_else(|| Error::ConstraintError(format!("invalid n_cols: {:?}", n_cols)))?;
        let bitmap_len = (n_cols + 7) >> 3;
        let before_present_bitmap = input.read_len(bitmap_len as usize)?;
        let after_present_bitmap = input.read_len(bitmap_len as usize)?;
        // before present columns
        let before_present_cols = bitmap_iter(before_present_bitmap.as_ref())
            .take(n_cols as usize)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        let before_null_bitmap_len = (before_present_cols + 7) >> 3;
        let after_present_cols = bitmap_iter(after_present_bitmap.as_ref())
            .take(n_cols as usize)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        let after_null_bitmap_len = (after_present_cols + 7) >> 3;
        let mut rows = Vec::new();
        while input.has_remaining() {
            // before row processing
            let before_null_bitmap = input.read_len(before_null_bitmap_len as usize)?;
            // use present_bitmap as base and mark null using null_bitmap
            let mut before_col_bitmap = Vec::from(before_present_bitmap.as_ref());
            let mut j = 0;
            for i in 0..before_present_cols {
                while !bitmap_index(&before_col_bitmap, j) {
                    j += 1;
                }
                bitmap_mark(
                    &mut before_col_bitmap,
                    j,
                    !bitmap_index(before_null_bitmap.as_ref(), i as usize),
                );
            }
            let before_row =
                Row::read_with_ctx(input, (n_cols as usize, &before_col_bitmap[..], col_metas))?;

            // after row processing
            let after_null_bitmap = input.read_len(after_null_bitmap_len as usize)?;
            // use present_bitmap as base and mark null using null_bitmap
            let mut after_col_bitmap = Vec::from(after_present_bitmap.as_ref());
            let mut j = 0;
            for i in 0..after_present_cols {
                while !bitmap_index(&after_col_bitmap, j) {
                    j += 1;
                }
                bitmap_mark(
                    &mut after_col_bitmap,
                    j,
                    !bitmap_index(after_null_bitmap.as_ref(), i as usize),
                );
            }
            let after_row =
                Row::read_with_ctx(input, (n_cols as usize, &after_col_bitmap[..], col_metas))?;
            rows.push(UpdateRow(before_row.0, after_row.0));
        }
        Ok(UpdateRowsV2 {
            extra_data,
            n_cols,
            before_present_bitmap,
            after_present_bitmap,
            rows,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Row(Vec<ColumnValue>);

impl<'c> ReadFromBytesWithContext<'c> for Row {
    type Context = (usize, &'c [u8], &'c [ColumnMetadata]);

    fn read_with_ctx(input: &mut Bytes, (n_cols, col_bm, col_metas): Self::Context) -> Result<Row> {
        let mut cols = Vec::with_capacity(n_cols);
        for i in 0..n_cols {
            if bitmap_index(col_bm, i) {
                let col_val = ColumnValue::read_with_ctx(input, &col_metas[i])?;
                cols.push(col_val);
            } else {
                cols.push(ColumnValue::Null);
            }
        }
        Ok(Row(cols))
    }
}

#[derive(Debug, Clone)]
pub struct UpdateRow(Vec<ColumnValue>, Vec<ColumnValue>);

#[cfg(test)]
mod tests {
    #[test]
    fn test_bit_xor() {
        let bm1 = 255;
        let bm2 = 252;
        let bmx = bm1 ^ bm2;
        dbg!(bmx);
    }
}
