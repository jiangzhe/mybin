use crate::bitmap;
use crate::stmt::StmtColumnValue;
use crate::Command;
use bitflags::bitflags;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComStmtExecute {
    pub cmd: Command,
    pub stmt_id: u32,
    pub flags: CursorTypeFlags,
    pub iter_cnt: u32,
    pub null_bitmap: Vec<u8>,
    // for first statement to execute, new_params_bound should be true
    // for any batch execution, if column type is different from
    // the previous one, this flag should be true
    pub new_params_bound: bool,
    pub params: Vec<StmtColumnValue>,
}

impl ComStmtExecute {
    pub fn single(stmt_id: u32, params: Vec<StmtColumnValue>) -> Self {
        let null_bitmap = bitmap::from_iter(params.iter().map(|p| p.is_null()), 0);
        Self {
            cmd: Command::StmtExecute,
            stmt_id,
            // currently not support cursor
            flags: CursorTypeFlags::empty(),
            iter_cnt: 1,
            null_bitmap,
            new_params_bound: true,
            params,
        }
    }
}

impl WriteToBytes for ComStmtExecute {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_le_u32(self.stmt_id)?;
        len += out.write_u8(self.flags.bits())?;
        len += out.write_le_u32(1)?;
        if !self.params.is_empty() {
            let null_bitmap = bitmap::from_iter(self.params.iter().map(|c| c.is_null()), 0);
            len += out.write_bytes(&null_bitmap[..])?;
            len += out.write_u8(if self.new_params_bound { 0x01 } else { 0x00 })?;
            if self.new_params_bound {
                for param in &self.params {
                    len += out.write_u8(param.col_type.into())?;
                    len += out.write_u8(if param.unsigned { 0x80 } else { 0x00 })?;
                }
            }
            for param in self.params {
                len += out.write_bytes(param.val)?;
            }
        }
        Ok(len)
    }
}

bitflags! {
    pub struct CursorTypeFlags: u8 {
        const READ_ONLY     = 0x01;
        const FOR_UPDATE    = 0x02;
        const SCROLLABLE    = 0x04;
    }
}
