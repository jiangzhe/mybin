//! meaningful data structures and parsing logic of RowsEventV2
use crate::bitmap;
use crate::col::{BinlogColumnValue, ColumnMeta};
use crate::row::LogRow;
use bytes::{Buf, Bytes};
use bytes_parser::error::{Error, Result};
use bytes_parser::my::ReadMyEnc;
use bytes_parser::{ReadBytesExt, ReadFromBytes};

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
    pub fn rows(&self, col_metas: &[ColumnMeta]) -> Result<RowsV2> {
        RowsV2::read_from(
            &mut self.payload.clone(),
            self.extra_data_len as usize,
            col_metas,
        )
    }

    pub fn into_rows(mut self, col_metas: &[ColumnMeta]) -> Result<RowsV2> {
        RowsV2::read_from(&mut self.payload, self.extra_data_len as usize, col_metas)
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
    pub fn rows(&self, col_metas: &[ColumnMeta]) -> Result<UpdateRowsV2> {
        UpdateRowsV2::read_from(
            &mut self.payload.clone(),
            self.extra_data_len as usize,
            col_metas,
        )
    }

    pub fn into_rows(mut self, col_metas: &[ColumnMeta]) -> Result<UpdateRowsV2> {
        UpdateRowsV2::read_from(&mut self.payload, self.extra_data_len as usize, col_metas)
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
    pub fn rows(&self, col_metas: &[ColumnMeta]) -> Result<RowsV2> {
        RowsV2::read_from(
            &mut self.payload.clone(),
            self.extra_data_len as usize,
            col_metas,
        )
    }

    pub fn into_rows(mut self, col_metas: &[ColumnMeta]) -> Result<RowsV2> {
        RowsV2::read_from(&mut self.payload, self.extra_data_len as usize, col_metas)
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
    pub rows: Vec<LogRow>,
}

impl RowsV2 {
    pub fn read_from(
        input: &mut Bytes,
        extra_data_len: usize,
        col_metas: &[ColumnMeta],
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
        let present_cols = bitmap::to_iter(present_bitmap.as_ref(), 0)
            .take(n_cols as usize)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        let null_bitmap_len = (present_cols + 7) >> 3;
        let mut rows = Vec::new();
        while input.has_remaining() {
            let null_bitmap = input.read_len(null_bitmap_len as usize)?;
            // use present_bitmap as base and mark null using null_bitmap
            let mut col_bitmap = Vec::from(present_bitmap.bytes());
            let mut j = 0;
            for i in 0..present_cols {
                while !bitmap::index(&col_bitmap, j) {
                    j += 1;
                }
                bitmap::mark(
                    &mut col_bitmap,
                    j,
                    !bitmap::index(null_bitmap.as_ref(), i as usize),
                );
            }
            let row = LogRow::read_from(input, n_cols as usize, &col_bitmap[..], col_metas)?;
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

impl UpdateRowsV2 {
    fn read_from(
        input: &mut Bytes,
        extra_data_len: usize,
        col_metas: &[ColumnMeta],
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
        let before_present_cols = bitmap::to_iter(before_present_bitmap.as_ref(), 0)
            .take(n_cols as usize)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        let before_null_bitmap_len = (before_present_cols + 7) >> 3;
        let after_present_cols = bitmap::to_iter(after_present_bitmap.as_ref(), 0)
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
                while !bitmap::index(&before_col_bitmap, j) {
                    j += 1;
                }
                bitmap::mark(
                    &mut before_col_bitmap,
                    j,
                    !bitmap::index(before_null_bitmap.as_ref(), i as usize),
                );
            }
            let before_row =
                LogRow::read_from(input, n_cols as usize, &before_col_bitmap[..], col_metas)?;

            // after row processing
            let after_null_bitmap = input.read_len(after_null_bitmap_len as usize)?;
            // use present_bitmap as base and mark null using null_bitmap
            let mut after_col_bitmap = Vec::from(after_present_bitmap.as_ref());
            let mut j = 0;
            for i in 0..after_present_cols {
                while !bitmap::index(&after_col_bitmap, j) {
                    j += 1;
                }
                bitmap::mark(
                    &mut after_col_bitmap,
                    j,
                    !bitmap::index(after_null_bitmap.as_ref(), i as usize),
                );
            }
            let after_row =
                LogRow::read_from(input, n_cols as usize, &after_col_bitmap[..], col_metas)?;
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
pub struct UpdateRow(pub Vec<BinlogColumnValue>, pub Vec<BinlogColumnValue>);

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
