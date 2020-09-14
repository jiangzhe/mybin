use crate::flag::CapabilityFlags;
use crate::packet::{ErrPacket, OkPacket};
use bytes::{Buf, Bytes};
use bytes_parser::error::{Error, Needed, Result};

#[derive(Debug, Clone)]
pub enum ComResponse {
    Ok(OkPacket),
    Err(ErrPacket),
}

impl ComResponse {
    pub fn read_from(input: &mut Bytes, cap_flags: &CapabilityFlags) -> Result<Self> {
        if !input.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match input[0] {
            0x00 => {
                let ok = OkPacket::read_from(input, cap_flags)?;
                Ok(ComResponse::Ok(ok))
            }
            0xff => {
                let err = ErrPacket::read_from(input, cap_flags, true)?;
                Ok(ComResponse::Err(err))
            }
            c => Err(Error::ConstraintError(format!("invalid packet code {}", c))),
        }
    }
}
