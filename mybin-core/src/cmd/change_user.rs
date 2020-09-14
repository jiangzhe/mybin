use crate::flag::CapabilityFlags;
use crate::handshake::ConnectAttr;
use crate::handshake::{AuthMoreData, AuthSwitchRequest};
use crate::packet::{ErrPacket, OkPacket};
use crate::Command;
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::error::{Error, Needed, Result};
use bytes_parser::my::LenEncStr;
use bytes_parser::{ReadFromBytes, WriteBytesExt, WriteToBytesWithContext};

#[derive(Debug, Clone)]
pub struct ComChangeUser {
    pub cmd: Command,
    pub user: String,
    pub auth_response: Vec<u8>,
    pub schema_name: String,
    pub charset: u16,
    pub auth_plugin_name: String,
    pub connect_attrs: Vec<ConnectAttr>,
}

impl ComChangeUser {
    pub fn new<U, S, P>(
        user: U,
        auth_response: Vec<u8>,
        db_name: S,
        auth_plugin_name: P,
        connect_attrs: Vec<ConnectAttr>,
    ) -> Self
    where
        U: Into<String>,
        S: Into<String>,
        P: Into<String>,
    {
        Self {
            cmd: Command::ChangeUser,
            user: user.into(),
            auth_response,
            schema_name: db_name.into(),
            charset: 33,
            auth_plugin_name: auth_plugin_name.into(),
            connect_attrs,
        }
    }
}

impl<'c> WriteToBytesWithContext<'c> for ComChangeUser {
    type Context = &'c CapabilityFlags;

    fn write_with_ctx(self, out: &mut BytesMut, cap_flags: Self::Context) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_bytes(self.user.as_bytes())?;
        len += out.write_u8(0)?;
        if cap_flags.contains(CapabilityFlags::SECURE_CONNECTION) {
            let auth_response_len = self.auth_response.len() as u8;
            len += out.write_u8(auth_response_len)?;
            len += out.write_bytes(&self.auth_response[..])?;
        } else {
            len += out.write_bytes(&self.auth_response[..])?;
            len += out.write_u8(0)?;
        }
        len += out.write_bytes(self.schema_name.as_bytes())?;
        len += out.write_u8(0)?;
        len += out.write_le_u16(self.charset)?;
        if cap_flags.contains(CapabilityFlags::PLUGIN_AUTH) {
            len += out.write_bytes(self.auth_plugin_name.as_bytes())?;
            len += out.write_u8(0)?;
        }
        if cap_flags.contains(CapabilityFlags::CONNECT_ATTRS) {
            let mut buf = BytesMut::new();
            for attr in self.connect_attrs {
                let key = LenEncStr::Bytes(Bytes::from(attr.key));
                buf.write_bytes(key)?;
                let val = LenEncStr::Bytes(Bytes::from(attr.value));
                buf.write_bytes(val)?;
            }
            let kvs = LenEncStr::Bytes(buf.freeze());
            len += out.write_bytes(kvs)?;
        }
        Ok(len)
    }
}

#[derive(Debug, Clone)]
pub enum ComChangeUserResponse {
    Err(ErrPacket),
    Switch(AuthSwitchRequest),
    Ok(OkPacket),
    More(AuthMoreData),
}

impl ComChangeUserResponse {
    pub fn read_from(input: &mut Bytes, cap_flags: &CapabilityFlags) -> Result<Self> {
        if !input.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match input[0] {
            0xff => {
                let err = ErrPacket::read_from(input, cap_flags, true)?;
                Ok(Self::Err(err))
            }
            0x00 => {
                let ok = OkPacket::read_from(input, cap_flags)?;
                Ok(Self::Ok(ok))
            }
            0x01 => {
                let more = AuthMoreData::read_from(input)?;
                Ok(Self::More(more))
            }
            0xfe => {
                let switch = AuthSwitchRequest::read_from(input)?;
                Ok(Self::Switch(switch))
            }
            _ => Err(Error::ConstraintError(format!(
                "invalid packet header {:02x}",
                input[0]
            ))),
        }
    }
}
