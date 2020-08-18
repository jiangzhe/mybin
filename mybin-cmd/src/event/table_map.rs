use crate::col::*;
use bytes_parser::ReadFrom;
use bytes_parser::number::ReadNumber;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::my::ReadMyEncoding;
use bytes_parser::error::{Result, Error};
use std::convert::TryFrom;


/// Data of TableMapEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/table-map-event.html
/// only support binlog v4
#[derive(Debug, Clone)]
pub struct TableMapData<'a> {
    // actually 6-bytes integer
    pub table_id: u64,
    pub flags: u16,
    // below is variable part
    // complicated to decode, so leave it as is
    // use specific function to evaluate later
    pub payload: &'a [u8],
}

impl<'a> ReadFrom<'a, TableMapData<'a>> for [u8] {
    fn read_from(&'a self, offset: usize) -> Result<(usize, TableMapData<'a>)> {
        let (offset, table_id) = self.read_le_u48(offset)?;
        let (offset, flags) = self.read_le_u16(offset)?;
        let (offset, payload) = self.take_len(offset, self.len() - offset as usize)?;
        Ok((
            offset,
            TableMapData {
                table_id,
                flags,
                payload,
            },
        ))
    }
}

impl<'a> TableMapData<'a> {
    pub fn raw_table_map(&self) -> crate::error::Result<RawTableMap<'a>> {
        let (_, rtm) = self.payload.read_from(0)?;
        Ok(rtm)
    }

    pub fn table_map(&self) -> crate::error::Result<TableMap> {
        use std::convert::TryInto;
        self.raw_table_map().and_then(TryInto::try_into)
    }
}

#[derive(Debug, Clone)]
pub struct RawTableMap<'a> {
    pub schema_name: &'a [u8],
    pub table_name: &'a [u8],
    pub column_count: u64,
    pub column_defs: &'a [u8],
    pub column_meta_defs: &'a [u8],
    pub null_bitmap: &'a [u8],
}

/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/rows_event.h
impl<'a> ReadFrom<'a, RawTableMap<'a>> for [u8] {
    fn read_from(&'a self, offset: usize) -> Result<(usize, RawTableMap<'a>)> {
        let (offset, schema_name_len) = self.read_u8(offset)?;
        let (offset, schema_name) = self.take_len(offset, schema_name_len as usize)?;
        let (offset, _) = self.take_len(offset, 1)?;
        // 8+1+1
        let (offset, table_name_len) = self.read_u8(offset)?;
        let (offset, table_name) = self.take_len(offset, table_name_len as usize)?;
        let (offset, _) = self.take_len(offset, 1)?;
        // 5+1+1
        let (offset, column_count) = self.read_len_enc_int(offset)?;
        let column_count = column_count.to_u64().ok_or_else(|| Error::ConstraintError("error column count".to_owned()))?;
        let (offset, column_defs) = self.take_len(offset, column_count as usize)?;
        // 1+2
        let (offset, column_meta_defs_length) = self.read_len_enc_int(offset)?;
        let column_meta_defs_length = column_meta_defs_length
            .to_u64()
            .ok_or_else(|| Error::ConstraintError("error column meta def length".to_owned()))?;
        let (offset, column_meta_defs) = self.take_len(offset, column_meta_defs_length as usize)?;
        // 1+2
        let bitmap_len = (column_count + 7) / 8u64;
        let (offset, null_bitmap) = self.take_len(offset, bitmap_len as usize)?;
        Ok((
            offset,
            RawTableMap {
                schema_name,
                table_name,
                column_count,
                column_defs,
                column_meta_defs,
                null_bitmap,
            },
        ))
    }
}

#[derive(Debug, Clone)]
pub struct TableMap {
    pub schema_name: String,
    pub table_name: String,
    pub col_metas: Vec<ColumnMetadata>,
}

impl<'a> TryFrom<RawTableMap<'a>> for TableMap {
    type Error = crate::error::Error;
    fn try_from(raw: RawTableMap<'a>) -> crate::error::Result<Self> {
        let schema_name = String::from_utf8(Vec::from(raw.schema_name))?;
        let table_name = String::from_utf8(Vec::from(raw.table_name))?;
        let col_metas = parse_col_metas(
            raw.column_count as usize,
            raw.column_defs,
            raw.column_meta_defs,
            raw.null_bitmap,
        )?;
        Ok(TableMap {
            schema_name,
            table_name,
            col_metas,
        })
    }
}
