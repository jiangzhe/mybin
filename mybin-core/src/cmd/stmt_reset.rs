use crate::resp::ComResponse;
use crate::Command;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

pub struct ComStmtReset {
    pub cmd: Command,
    pub stmt_id: u32,
}

impl ComStmtReset {
    pub fn new(stmt_id: u32) -> Self {
        Self {
            cmd: Command::StmtReset,
            stmt_id,
        }
    }
}

impl WriteToBytes for ComStmtReset {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        out.write_u8(self.cmd.to_byte())?;
        out.write_le_u32(self.stmt_id)?;
        Ok(5)
    }
}

pub type ComStmtResetResponse = ComResponse;
