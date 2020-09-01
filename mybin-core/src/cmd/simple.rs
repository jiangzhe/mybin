use crate::flag::CapabilityFlags;
use crate::packet::{EofPacket, ErrPacket};
use crate::resp::ComResponse;
use crate::single_byte_cmd;
use bytes::{Buf, Bytes};
use bytes_parser::error::{Error, Needed, Result};
use bytes_parser::ReadFromBytesWithContext;

single_byte_cmd!(ComStatistics, Statistics);

single_byte_cmd!(ComProcessInfo, ProcessInfo);

single_byte_cmd!(ComDebug, Debug);

#[derive(Debug, Clone)]
pub enum ComDebugResponse {
    Eof(EofPacket),
    Err(ErrPacket),
}

impl<'c> ReadFromBytesWithContext<'c> for ComDebugResponse {
    type Context = &'c CapabilityFlags;
    fn read_with_ctx(input: &mut Bytes, cap_flags: Self::Context) -> Result<Self> {
        if !input.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match input[0] {
            0xfe => {
                let eof = EofPacket::read_with_ctx(input, cap_flags)?;
                Ok(Self::Eof(eof))
            }
            0xff => {
                let err = ErrPacket::read_with_ctx(input, (cap_flags, true))?;
                Ok(Self::Err(err))
            }
            _ => Err(Error::ConstraintError(format!(
                "invalid packet code {:02x}",
                input[0]
            ))),
        }
    }
}

single_byte_cmd!(ComPing, Ping);

single_byte_cmd!(ComResetConnection, ResetConnection);

pub type ComResetConnectionResponse = ComResponse;
