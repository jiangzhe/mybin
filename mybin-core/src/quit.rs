use crate::Command;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComQuit {
    pub cmd: Command,
}

impl ComQuit {
    pub fn new() -> Self {
        ComQuit { cmd: Command::Quit }
    }
}

impl WriteToBytes for ComQuit {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        out.write_u8(self.cmd.to_byte())
    }
}
