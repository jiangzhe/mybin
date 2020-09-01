use crate::flag::*;
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::error::{Error, Result};
use bytes_parser::my::LenEncInt;
use bytes_parser::{ReadBytesExt, ReadFromBytes, WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct InitialHandshake {
    pub protocol_version: u8,
    pub server_version: Bytes,
    pub connection_id: u32,
    pub auth_plugin_data_1: Bytes,
    // filler 0x00
    // pub capability_flags_lower: u16,
    pub charset: u8,
    pub status_flags: u16,
    // pub capability_flags_upper: u16,
    pub capability_flags: u32,
    pub auth_plugin_data_length: u8,
    // reserved 10 bytes
    pub auth_plugin_data_2: Bytes,
    pub auth_plugin_name: Bytes,
}

impl ReadFromBytes for InitialHandshake {
    fn read_from(input: &mut Bytes) -> Result<InitialHandshake> {
        let protocol_version = input.read_u8()?;
        let server_version = input.read_until(0, false)?;
        let connection_id = input.read_le_u32()?;
        let auth_plugin_data_1 = input.read_len(8)?;
        input.read_len(1)?;
        let capability_flags_lower = input.read_le_u16()?;
        let charset = input.read_u8()?;
        let status_flags = input.read_le_u16()?;
        let capability_flags_upper = input.read_le_u16()?;
        let auth_plugin_data_length = input.read_u8()?;
        input.read_len(10)?;
        // construct complete capability_flags
        let capability_flags =
            (capability_flags_lower as u32) | ((capability_flags_upper as u32) << 16);
        let cap_flags = CapabilityFlags::from_bits_truncate(capability_flags);
        let auth_plugin_data_2 = if cap_flags.contains(CapabilityFlags::SECURE_CONNECTION) {
            let len = std::cmp::max(13, auth_plugin_data_length - 8);
            input.read_len(len as usize)?
        } else {
            Bytes::new()
        };
        let auth_plugin_name = if cap_flags.contains(CapabilityFlags::PLUGIN_AUTH) {
            input.read_until(0, false)?
        } else {
            Bytes::new()
        };
        Ok(InitialHandshake {
            protocol_version,
            server_version,
            connection_id,
            auth_plugin_data_1,
            charset,
            status_flags,
            capability_flags,
            auth_plugin_data_length,
            auth_plugin_data_2,
            auth_plugin_name,
        })
    }
}

/// handshake response of client protocol 41
///
/// reference: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
/// this struct should be constructed by user and will be sent to
/// MySQL server to finish handshake process
#[derive(Debug, Clone)]
pub struct HandshakeClientResponse41 {
    pub capability_flags: CapabilityFlags,
    pub max_packet_size: u32,
    pub charset: u8,
    // 23 bytes of 0x00, reserved
    pub username: String,
    // vary according to capability flags and auth setting
    pub auth_response: Vec<u8>,
    // not empty if db is specified
    pub database: String,
    // not empty if plugin auth
    pub auth_plugin_name: String,
    // connect attributes
    pub connect_attrs: Vec<ConnectAttr>,
}

impl WriteToBytes for HandshakeClientResponse41 {
    /// generate response bytes to send to server
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        // capability falgs 0:4
        len += out.write_le_u32(self.capability_flags.bits())?;
        // max packet size 4:8
        len += out.write_le_u32(self.max_packet_size)?;
        // character set 8:9
        len += out.write_u8(self.charset)?;
        // reserved 23 bytes 9:32
        len += out.write_bytes(&[0u8; 23][..])?;
        // null-terminated username
        len += out.write_bytes(self.username.as_bytes())?;
        len += out.write_u8(0)?;
        // len-encoded auth response
        let auth_response_len = LenEncInt::from(self.auth_response.len() as u64);
        len += auth_response_len.write_to(out)?;
        len += out.write_bytes(&self.auth_response[..])?;
        // null-terminated database if connect with db
        if self
            .capability_flags
            .contains(CapabilityFlags::CONNECT_WITH_DB)
        {
            len += out.write_bytes(self.database.as_bytes())?;
            len += out.write_u8(0)?;
        }
        // null-terminated plugin name
        if self.capability_flags.contains(CapabilityFlags::PLUGIN_AUTH) {
            len += out.write_bytes(self.auth_plugin_name.as_bytes())?;
            len += out.write_u8(0)?;
        }
        if self
            .capability_flags
            .contains(CapabilityFlags::CONNECT_ATTRS)
        {
            let mut lb = BytesMut::new();
            for attr in &self.connect_attrs {
                let klen = LenEncInt::from(attr.key.len() as u64);
                klen.write_to(&mut lb)?;
                lb.write_bytes(attr.key.as_bytes())?;
                let vlen = LenEncInt::from(attr.value.len() as u64);
                vlen.write_to(&mut lb)?;
                lb.write_bytes(attr.value.as_bytes())?;
            }
            // use len-enc-int here
            let lblen = LenEncInt::from(lb.len() as u64);
            len += lblen.write_to(out)?;
            len += out.write_bytes(lb.bytes())?;
        }
        Ok(len)
    }
}

impl Default for HandshakeClientResponse41 {
    fn default() -> Self {
        HandshakeClientResponse41 {
            capability_flags: CapabilityFlags::default(),
            // max length of three-byte word
            max_packet_size: 0xffffff,
            // by default use utf-8
            charset: 33,
            username: String::new(),
            auth_response: Vec::new(),
            database: String::new(),
            auth_plugin_name: String::new(),
            connect_attrs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectAttr {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct AuthSwitchRequest {
    pub header: u8,
    // null terminated string
    pub plugin_name: Bytes,
    // EOF terminated string
    pub auth_plugin_data: Bytes,
}

impl ReadFromBytes for AuthSwitchRequest {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let header = input.read_u8()?;
        if header != 0xfe {
            return Err(Error::ConstraintError(format!(
                "message header mismatch: expected=0xfe, actual={:02x}",
                header
            )));
        }
        let plugin_name = input.read_until(0, false)?;
        let auth_plugin_data = input.split_to(input.remaining());
        Ok(AuthSwitchRequest {
            header,
            plugin_name,
            auth_plugin_data,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AuthMoreData {
    pub header: u8,
    pub plugin_data: Bytes,
}

impl ReadFromBytes for AuthMoreData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let header = input.read_u8()?;
        if header != 0x01 {
            return Err(Error::ConstraintError(format!(
                "message header mismatch: expected=0x01, actual={:02x}",
                header
            )));
        }
        let plugin_data = input.split_to(input.remaining());
        Ok(AuthMoreData {
            header,
            plugin_data,
        })
    }
}

#[cfg(test)]
mod tests {

    const PACKET_DATA: &[u8] = include_bytes!("../data/packet.dat");

    use super::*;
    use crate::packet::Packet;

    #[test]
    fn test_read_handshake_packet() {
        let input = &mut PACKET_DATA.to_bytes();
        let pkt = Packet::read_from(input).unwrap();
        let handshake = InitialHandshake::read_from(&mut pkt.payload.clone()).unwrap();
        dbg!(&handshake);
        println!(
            "server_version={}",
            String::from_utf8_lossy(handshake.server_version.bytes())
        );
        println!(
            "auth_plugin_name={}",
            String::from_utf8_lossy(handshake.auth_plugin_name.bytes())
        );
        let capability_flags = CapabilityFlags::from_bits(handshake.capability_flags).unwrap();
        println!("capability_flags={:#?}", capability_flags);
    }

    #[test]
    fn test_read_bytes_handshake_packet() {
        let mut input = PACKET_DATA.to_bytes();
        let pkt = Packet::read_from(&mut input).unwrap();
        dbg!(pkt);
    }
}
