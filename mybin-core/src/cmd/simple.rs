use crate::flag::CapabilityFlags;
use crate::packet::{EofPacket, ErrPacket};
use crate::resp::ComResponse;
use crate::single_byte_cmd;
use bytes::{Buf, Bytes};
use bytes_parser::error::{Error, Needed, Result};

single_byte_cmd!(ComStatistics, Statistics);

single_byte_cmd!(ComProcessInfo, ProcessInfo);

single_byte_cmd!(ComDebug, Debug);

#[derive(Debug, Clone)]
pub enum ComDebugResponse {
    Eof(EofPacket),
    Err(ErrPacket),
}

impl ComDebugResponse {
    pub fn read_from(input: &mut Bytes, cap_flags: &CapabilityFlags) -> Result<Self> {
        if !input.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match input[0] {
            0xfe => {
                let eof = EofPacket::read_from(input, cap_flags)?;
                Ok(Self::Eof(eof))
            }
            0xff => {
                let err = ErrPacket::read_from(input, cap_flags, true)?;
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
