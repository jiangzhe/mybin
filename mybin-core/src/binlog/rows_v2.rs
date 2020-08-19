//! meaningful data structures and parsing logic of RowsEventV2
use crate::col::{ColumnMetadata, ColumnValue};
// use crate::error::Error;
use crate::util::bitmap_index;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::error::{Error, Result};
use bytes_parser::my::ReadMyEncoding;
use bytes_parser::number::ReadNumber;
use bytes_parser::{ReadFrom, ReadWithContext};

/// Data of DeleteRowEventV2, UpdateRowsEventV2, WriteRowsEventV2
///
/// reference: https://dev.mysql.com/doc/internals/en/rows-event.html
/// similar to v1 row events
/// detailed row information will be handled by separate module
#[derive(Debug, Clone)]
pub struct RowsDataV2<'a> {
    // actual 6-byte integer
    pub table_id: u64,
    pub flags: u16,
    pub extra_data_len: u16,
    // below is variable part
    pub payload: &'a [u8],
}

impl<'a> RowsDataV2<'a> {
    pub fn raw_delete_rows(&self) -> Result<RawRowsV2<'a>> {
        self.raw_rows(false)
    }

    pub fn delete_rows(&self, col_metas: &[ColumnMetadata]) -> Result<RowsV2> {
        self.raw_delete_rows()
            .and_then(|rr| rr.delete_rows(col_metas))
    }

    pub fn raw_write_rows(&self) -> Result<RawRowsV2<'a>> {
        self.raw_rows(false)
    }

    pub fn write_rows(&self, col_metas: &[ColumnMetadata]) -> Result<RowsV2> {
        self.raw_write_rows()
            .and_then(|rr| rr.write_rows(col_metas))
    }

    pub fn raw_update_rows(&self) -> Result<RawRowsV2<'a>> {
        self.raw_rows(true)
    }

    pub fn update_rows(&self, col_metas: &[ColumnMetadata]) -> Result<UpdateRowsV2> {
        self.raw_update_rows()
            .and_then(|rr| rr.update_rows(col_metas))
    }

    fn raw_rows(&self, update: bool) -> Result<RawRowsV2<'a>> {
        // extra_data_len - 2 is the length of extra data
        let (_, raw_rows) = self
            .payload
            .read_with_ctx(0, (self.extra_data_len - 2, update))?;
        Ok(raw_rows)
    }
}

impl<'a> ReadFrom<'a, RowsDataV2<'a>> for [u8] {
    fn read_from(&'a self, offset: usize) -> Result<(usize, RowsDataV2<'a>)> {
        let (offset, table_id) = self.read_le_u48(offset)?;
        let (offset, flags) = self.read_le_u16(offset)?;
        let (offset, extra_data_len) = self.read_le_u16(offset)?;
        let (offset, payload) = self.take_len(offset, self.len() - offset)?;
        Ok((
            offset,
            RowsDataV2 {
                table_id,
                flags,
                extra_data_len,
                payload,
            },
        ))
    }
}

/// parse raw rows v2, including WriteRows and DeleteRows v2
///
/// the extra data length should be real length: len in binlog file minus 2
#[derive(Debug, Clone)]
pub struct RawRowsV2<'a> {
    pub extra_data: &'a [u8],
    pub n_cols: u32,
    pub before_col_bitmap: &'a [u8],
    // after col bitmap may share same byte array
    // as before col bitmp
    // only UpdateRowsEventV2 owns different bitmaps
    // of before and after
    pub after_col_bitmap: &'a [u8],
    pub rows_data: &'a [u8],
}

impl<'a> ReadWithContext<'a, '_, RawRowsV2<'a>> for [u8] {
    type Context = (u16, bool);

    fn read_with_ctx(
        &'a self,
        offset: usize,
        (extra_data_len, update): Self::Context,
    ) -> Result<(usize, RawRowsV2<'a>)> {
        let (offset, extra_data) = self.take_len(offset, extra_data_len as usize)?;
        let (offset, n_cols) = self.read_len_enc_int(offset)?;
        // todo: assign error to avoid panic
        let n_cols = n_cols
            .to_u32()
            .ok_or_else(|| Error::ConstraintError(format!("invalid n_cols: {:?}", n_cols)))?;
        let bitmap_len = (n_cols + 7) >> 3;
        let (offset, before_col_bitmap) = self.take_len(offset, bitmap_len as usize)?;
        let (offset, after_col_bitmap) = if update {
            self.take_len(offset, bitmap_len as usize)?
        } else {
            (offset, before_col_bitmap)
        };
        let (offset, rows_data) = self.take_len(offset, self.len() - offset)?;
        Ok((
            offset,
            RawRowsV2 {
                extra_data,
                n_cols,
                before_col_bitmap,
                after_col_bitmap,
                rows_data,
            },
        ))
    }
}

impl<'a> RawRowsV2<'a> {
    pub fn write_rows(&self, col_metas: &[ColumnMetadata]) -> Result<RowsV2> {
        // todo: error handling
        let (_, rows) = self.parse_rows(col_metas, true)?;
        Ok(rows)
    }

    pub fn delete_rows(&self, col_metas: &[ColumnMetadata]) -> Result<RowsV2> {
        // todo: error handling
        let (_, rows) = self.parse_rows(col_metas, false)?;
        Ok(rows)
    }

    pub fn update_rows(&self, col_metas: &[ColumnMetadata]) -> Result<UpdateRowsV2> {
        // todo: error handling
        let (_, rows) = self.parse_update_rows(col_metas)?;
        Ok(rows)
    }

    fn parse_rows(&'a self, col_metas: &[ColumnMetadata], write: bool) -> Result<(usize, RowsV2)> {
        let mut rows = Vec::new();
        let bm1 = if write {
            self.after_col_bitmap
        } else {
            self.before_col_bitmap
        };
        let n_cols = self.n_cols as usize;
        let bitmap_len = (n_cols + 7) / 8;
        let mut offset = 0;
        while !offset < self.rows_data.len() {
            let (os1, col_bm) = self.rows_data.take_len(offset, bitmap_len)?;
            let col_bm: Vec<u8> = bm1
                .iter()
                .zip(col_bm.iter())
                .map(|(b1, b2)| b1 ^ b2)
                .collect();
            let (os1, row): (_, Row) = self
                .rows_data
                .read_with_ctx(os1, (n_cols, &col_bm[..], col_metas))?;
            rows.push(row);
            offset = os1;
        }
        Ok((offset, RowsV2(rows)))
    }

    fn parse_update_rows(&'a self, col_metas: &[ColumnMetadata]) -> Result<(usize, UpdateRowsV2)> {
        let mut rows = Vec::new();
        let n_cols = self.n_cols as usize;
        let bitmap_len = (n_cols + 7) / 8;
        let mut offset = 0;
        while offset < self.rows_data.len() {
            // before row
            let (os1, before_col_bm) = self.rows_data.take_len(offset, bitmap_len)?;
            let before_col_bm: Vec<u8> = self
                .before_col_bitmap
                .iter()
                .zip(before_col_bm.iter())
                .map(|(b1, b2)| b1 ^ b2)
                .collect();
            let (os1, before_row): (_, Row) = self
                .rows_data
                .read_with_ctx(os1, (n_cols, &before_col_bm[..], col_metas))?;
            // after row
            let (os1, after_col_bm) = self.rows_data.take_len(os1, bitmap_len)?;
            let after_col_bm: Vec<u8> = self
                .after_col_bitmap
                .iter()
                .zip(after_col_bm.iter())
                .map(|(b1, b2)| b1 ^ b2)
                .collect();
            let (os1, after_row): (_, Row) = self
                .rows_data
                .read_with_ctx(os1, (n_cols, &after_col_bm[..], col_metas))?;
            rows.push(UpdateRow(before_row.0, after_row.0));
            offset = os1;
        }
        Ok((offset, UpdateRowsV2(rows)))
    }
}

#[derive(Debug, Clone)]
pub struct RowsV2(Vec<Row>);

#[derive(Debug, Clone)]
pub struct Row(Vec<ColumnValue>);

impl<'c> ReadWithContext<'_, 'c, Row> for [u8] {
    type Context = (usize, &'c [u8], &'c [ColumnMetadata]);

    fn read_with_ctx(
        &self,
        offset: usize,
        (n_cols, col_bm, col_metas): Self::Context,
    ) -> Result<(usize, Row)> {
        debug_assert_eq!(n_cols, col_metas.len());
        let mut cols = Vec::with_capacity(n_cols);
        let mut offset = offset;
        for i in 0..n_cols {
            if bitmap_index(col_bm, i) {
                let (os1, col_val) = self.read_with_ctx(offset, &col_metas[i])?;
                offset = os1;
                cols.push(col_val);
            } else {
                cols.push(ColumnValue::Null);
            }
        }
        Ok((offset, Row(cols)))
    }
}

#[derive(Debug, Clone)]
pub struct UpdateRowsV2(Vec<UpdateRow>);

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
