use crate::Command;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

// multi-resultset currently not supported
#[derive(Debug, Clone)]
pub struct ComStmtFetch {
    pub cmd: Command,
    pub stmt_id: u32,
    pub n_rows: u32,
}

impl ComStmtFetch {
    pub fn new(stmt_id: u32, n_rows: u32) -> Self {
        Self {
            cmd: Command::StmtFetch,
            stmt_id,
            n_rows,
        }
    }
}

impl WriteToBytes for ComStmtFetch {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        out.write_u8(self.cmd.to_byte())?;
        out.write_le_u32(self.stmt_id)?;
        out.write_le_u32(self.n_rows)?;
        Ok(9)
    }
}
