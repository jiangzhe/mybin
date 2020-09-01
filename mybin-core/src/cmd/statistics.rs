use bytes::BytesMut;
use crate::Command;
use bytes_parser::error::Result;
use bytes_parser::{WriteToBytes, WriteBytesExt};

#[derive(Debug, Clone)]
pub struct ComStatistics {
    pub cmd: Command,
}

impl ComStatistics {
    pub fn new() -> Self {
        ComStatistics{
            cmd: Command::Statistics,
        }
    }
}

impl WriteToBytes for ComStatistics {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        out.write_u8(self.cmd.to_byte())
    }
}
