use crate::flag::CapabilityFlags;
use crate::packet::{EofPacket, ErrPacket};
use crate::Command;
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::error::{Error, Needed, Result};
use bytes_parser::{ReadFromBytesWithContext, WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComSetOption {
    pub cmd: Command,
    pub multi_stmts: bool,
}

impl ComSetOption {
    pub fn new(multi_stmts: bool) -> Self {
        Self {
            cmd: Command::SetOption,
            multi_stmts,
        }
    }
}

impl WriteToBytes for ComSetOption {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        out.write_u8(self.cmd.to_byte())?;
        out.write_le_u16(if self.multi_stmts { 0x0001 } else { 0x0000 })?;
        Ok(3)
    }
}

#[derive(Debug, Clone)]
pub enum ComSetOptionResponse {
    Eof(EofPacket),
    Err(ErrPacket),
}

impl<'c> ReadFromBytesWithContext<'c> for ComSetOptionResponse {
    type Context = &'c CapabilityFlags;

    fn read_with_ctx(input: &mut Bytes, cap_flags: Self::Context) -> Result<Self> {
        if !input.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match input[0] {
            0xff => {
                let err = ErrPacket::read_with_ctx(input, (cap_flags, true))?;
                Ok(Self::Err(err))
            }
            0xfe => {
                let eof = EofPacket::read_with_ctx(input, cap_flags)?;
                Ok(Self::Eof(eof))
            }
            _ => Err(Error::ConstraintError(format!(
                "invalid packet header {:02x}",
                input[0]
            ))),
        }
    }
}
