use crate::packet::{OkPacket, ErrPacket};
use crate::flag::CapabilityFlags;
use bytes_parser::error::{Result, Error, Needed};
use bytes_parser::ReadFromBytesWithContext;
use bytes::{Buf, Bytes};


#[derive(Debug, Clone)]
pub enum ComResponse {
    Ok(OkPacket),
    Err(ErrPacket),
}

impl<'c> ReadFromBytesWithContext<'c> for ComResponse {
    type Context = &'c CapabilityFlags;

    fn read_with_ctx(input: &mut Bytes, cap_flags: Self::Context) -> Result<Self> {
        if !input.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match input[0] {
            0x00 => {
                let ok = OkPacket::read_with_ctx(input, cap_flags)?;
                Ok(ComResponse::Ok(ok))
            }
            0xff => {
                let err = ErrPacket::read_with_ctx(input, (cap_flags, true))?;
                Ok(ComResponse::Err(err))
            }
            c => Err(Error::ConstraintError(format!("invalid packet code {}", c))),
        }
    }
}