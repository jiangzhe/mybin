use crate::resp::ComResponse;
use crate::Command;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComProcessKill {
    pub cmd: Command,
    pub conn_id: u32,
}

impl ComProcessKill {
    pub fn new(conn_id: u32) -> Self {
        Self {
            cmd: Command::ProcessKill,
            conn_id,
        }
    }
}

impl WriteToBytes for ComProcessKill {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        out.write_u8(self.cmd.to_byte())?;
        out.write_le_u32(self.conn_id)?;
        Ok(5)
    }
}

pub type ComProcessKillResponse = ComResponse;
