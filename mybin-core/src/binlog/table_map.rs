use crate::col::*;
use bytes::{Buf, Bytes};
use bytes_parser::error::{Error, Result};
use bytes_parser::my::ReadMyEnc;
use bytes_parser::{ReadBytesExt, ReadFromBytes};
use smol_str::SmolStr;
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
    pub col_cnt: u64,
    pub col_defs: Bytes,
    pub col_meta_defs: Bytes,
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
        let col_cnt = input.read_len_enc_int()?;
        let col_cnt = col_cnt
            .to_u64()
            .ok_or_else(|| Error::ConstraintError("error column count".to_owned()))?;
        let col_defs = input.read_len(col_cnt as usize)?;
        // 1+2
        let col_meta_defs_len = input.read_len_enc_int()?;
        let col_meta_defs_len = col_meta_defs_len
            .to_u64()
            .ok_or_else(|| Error::ConstraintError("error column meta def length".to_owned()))?;
        let col_meta_defs = input.read_len(col_meta_defs_len as usize)?;
        // 1+2
        let bitmap_len = (col_cnt + 7) / 8u64;
        let null_bitmap = input.read_len(bitmap_len as usize)?;
        Ok(RawTableMap {
            schema_name,
            table_name,
            col_cnt,
            col_defs,
            col_meta_defs,
            null_bitmap,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TableMap {
    pub schema_name: SmolStr,
    pub table_name: SmolStr,
    pub col_metas: ColumnMetas,
    pub null_bitmap: Vec<u8>,
}

impl TryFrom<RawTableMap> for TableMap {
    type Error = crate::error::Error;
    fn try_from(raw: RawTableMap) -> crate::error::Result<Self> {
        let schema_name = SmolStr::from(String::from_utf8(Vec::from(raw.schema_name.as_ref()))?);
        let table_name = SmolStr::from(String::from_utf8(Vec::from(raw.table_name.as_ref()))?);
        let null_bitmap = Vec::from(raw.null_bitmap.bytes());
        let col_metas = ColumnMetas::read_from(
            &mut raw.col_meta_defs.clone(),
            raw.col_cnt as usize,
            raw.col_defs.bytes(),
        )?;
        Ok(TableMap {
            schema_name,
            table_name,
            col_metas,
            null_bitmap,
        })
    }
}
