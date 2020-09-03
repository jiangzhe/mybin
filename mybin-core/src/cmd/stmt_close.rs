use crate::Command;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComStmtClose {
    pub cmd: Command,
    pub stmt_id: u32,
}

impl ComStmtClose {
    pub fn new(stmt_id: u32) -> Self {
        Self {
            cmd: Command::StmtClose,
            stmt_id,
        }
    }
}

impl WriteToBytes for ComStmtClose {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        out.write_u8(self.cmd.to_byte())?;
        out.write_le_u32(self.stmt_id)?;
        Ok(5)
    }
}
