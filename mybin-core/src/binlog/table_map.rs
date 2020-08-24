use crate::col::*;
use bytes::{Buf, Bytes};
use bytes_parser::error::{Error, Result};
use bytes_parser::my::ReadMyEnc;
use bytes_parser::{ReadBytesExt, ReadFromBytes};
use std::convert::TryFrom;

/// Data of TableMapEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/table-map-event.html
/// only support binlog v4
#[derive(Debug, Clone)]
pub struct TableMapData {
    // actually 6-bytes integer
    pub table_id: u64,
    pub flags: u16,
    // below is variable part
    // complicated to decode, so leave it as is
    // use specific function to evaluate later
    pub payload: Bytes,
}

impl ReadFromBytes for TableMapData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let table_id = input.read_le_u48()?;
        let flags = input.read_le_u16()?;
        let payload = input.split_to(input.remaining());
        Ok(TableMapData {
            table_id,
            flags,
            payload,
        })
    }
}

impl<'a> TableMapData {
    pub fn table_map(&self) -> crate::error::Result<TableMap> {
        use std::convert::TryInto;
        // make copy of payload
        let mut payload = self.payload.clone();
        let rtm = RawTableMap::read_from(&mut payload)?;
        rtm.try_into()
    }

    pub fn into_table_map(mut self) -> crate::error::Result<TableMap> {
        use std::convert::TryInto;
        let rtm = RawTableMap::read_from(&mut self.payload)?;
        rtm.try_into()
    }
}

#[derive(Debug, Clone)]
struct RawTableMap {
    pub schema_name: Bytes,
    pub table_name: Bytes,
    pub column_count: u64,
    pub column_defs: Bytes,
    pub column_meta_defs: Bytes,
    pub null_bitmap: Bytes,
}

/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/rows_event.h
impl ReadFromBytes for RawTableMap {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let schema_name_len = input.read_u8()?;
        let schema_name = input.read_len(schema_name_len as usize)?;
        input.read_len(1)?;
        // 8+1+1
        let table_name_len = input.read_u8()?;
        let table_name = input.read_len(table_name_len as usize)?;
        input.read_len(1)?;
        // 5+1+1
        let column_count = input.read_len_enc_int()?;
        let column_count = column_count
            .to_u64()
            .ok_or_else(|| Error::ConstraintError("error column count".to_owned()))?;
        let column_defs = input.read_len(column_count as usize)?;
        // 1+2
        let column_meta_defs_length = input.read_len_enc_int()?;
        let column_meta_defs_length = column_meta_defs_length
            .to_u64()
            .ok_or_else(|| Error::ConstraintError("error column meta def length".to_owned()))?;
        let column_meta_defs = input.read_len(column_meta_defs_length as usize)?;
        // 1+2
        let bitmap_len = (column_count + 7) / 8u64;
        let null_bitmap = input.read_len(bitmap_len as usize)?;
        Ok(RawTableMap {
            schema_name,
            table_name,
            column_count,
            column_defs,
            column_meta_defs,
            null_bitmap,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TableMap {
    pub schema_name: String,
    pub table_name: String,
    pub col_metas: Vec<ColumnMetadata>,
}

impl TryFrom<RawTableMap> for TableMap {
    type Error = crate::error::Error;
    fn try_from(mut raw: RawTableMap) -> crate::error::Result<Self> {
        let schema_name = String::from_utf8(Vec::from(raw.schema_name.as_ref()))?;
        let table_name = String::from_utf8(Vec::from(raw.table_name.as_ref()))?;
        let col_metas = parse_col_metas(
            raw.column_count as usize,
            &mut raw.column_meta_defs,
            raw.column_defs.as_ref(),
            raw.null_bitmap.as_ref(),
        )?;
        Ok(TableMap {
            schema_name,
            table_name,
            col_metas,
        })
    }
}
