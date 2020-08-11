use crate::error::Error;
use crate::flag::*;
use crate::util::LenEncInt;
use nom::bytes::streaming::{take, take_till};
use nom::error::ParseError;
use nom::number::streaming::{le_u16, le_u32, le_u8};
use nom::IResult;
use serde_derive::*;

/// used for placeholder for optional part in payload
const EMPTY_BYTE_ARRAY: [u8; 0] = [];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitialHandshake<'a> {
    pub protocol_version: u8,
    pub server_version: &'a [u8],
    pub connection_id: u32,
    pub auth_plugin_data_1: &'a [u8],
    // filler 0x00
    // pub capability_flags_lower: u16,
    pub character_set: u8,
    pub status_flags: u16,
    // pub capability_flags_upper: u16,
    pub capability_flags: u32,
    pub auth_plugin_data_length: u8,
    // reserved 10 bytes
    pub auth_plugin_data_2: &'a [u8],
    pub auth_plugin_name: &'a [u8],
}

pub fn initial_handshake(input: &[u8]) -> Result<InitialHandshake, Error> {
    let (_, handshake) = parse_initial_handshake(input).map_err(|e| Error::from((input, e)))?;
    Ok(handshake)
}

pub fn parse_initial_handshake<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], InitialHandshake, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, protocol_version) = le_u8(input)?;
    let (input, server_version) = take_till(|b| b == 0x00)(input)?;
    let (input, _) = take(1usize)(input)?;
    let (input, connection_id) = le_u32(input)?;
    let (input, auth_plugin_data_1) = take(8usize)(input)?;
    let (input, _) = take(1usize)(input)?;
    let (input, capability_flags_lower) = le_u16(input)?;
    let (input, character_set) = le_u8(input)?;
    let (input, status_flags) = le_u16(input)?;
    let (input, capability_flags_upper) = le_u16(input)?;
    let (input, auth_plugin_data_length) = le_u8(input)?;
    let (input, _) = take(10usize)(input)?;
    // construct complete capability_flags
    let capability_flags =
        (capability_flags_lower as u32) | ((capability_flags_upper as u32) << 16);
    let cap_flags = CapabilityFlags::from_bits_truncate(capability_flags);
    let (input, auth_plugin_data_2) = if cap_flags.contains(CapabilityFlags::SECURE_CONNECTION) {
        let len = std::cmp::max(13, auth_plugin_data_length - 8);
        take(len)(input)?
    } else {
        (input, &EMPTY_BYTE_ARRAY[..])
    };
    let (input, auth_plugin_name) = if cap_flags.contains(CapabilityFlags::PLUGIN_AUTH) {
        let (input, auth_plugin_name) = take_till(|b| b == 0x00)(input)?;
        let (input, _) = take(1usize)(input)?;
        (input, auth_plugin_name)
    } else {
        (input, &EMPTY_BYTE_ARRAY[..])
    };
    Ok((
        input,
        InitialHandshake {
            protocol_version,
            server_version,
            connection_id,
            auth_plugin_data_1,
            character_set,
            status_flags,
            capability_flags,
            auth_plugin_data_length,
            auth_plugin_data_2,
            auth_plugin_name,
        },
    ))
}

/// handshake response of client protocol 41
///
/// reference: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
/// this struct should be constructed by user and will be sent to
/// MySQL server to finish handshake process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeClientResponse41 {
    pub capability_flags: CapabilityFlags,
    pub max_packet_size: u32,
    pub character_set: u8,
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

impl HandshakeClientResponse41 {
    /// generate response bytes to send to server
    pub fn to_bytes(self) -> Vec<u8> {
        let mut rst = Vec::new();
        // capability falgs 0:4
        rst.extend(&self.capability_flags.bits().to_le_bytes()[..]);
        // max packet size 4:8
        rst.extend(&self.max_packet_size.to_le_bytes()[..]);
        // character set 8:9
        rst.push(self.character_set);
        // reserved 23 bytes 9:32
        rst.extend(std::iter::repeat(0u8).take(23));
        // null-terminated username
        rst.extend(self.username.as_bytes());
        rst.push(0);
        // len-encoded auth response
        if self.capability_flags.contains(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            let auth_response_len = LenEncInt::from(self.auth_response.len() as u64);
            rst.extend(auth_response_len.to_bytes());
            rst.extend(&self.auth_response[..]);
        } else if self.capability_flags.contains(CapabilityFlags::SECURE_CONNECTION) {
            rst.push(self.auth_response.len() as u8);
            rst.extend(&self.auth_response[..]);
        } else {
            rst.extend(&self.auth_response[..]);
            rst.push(0);
        }
        
        // null-terminated database if connect with db
        if self
            .capability_flags
            .contains(CapabilityFlags::CONNECT_WITH_DB)
        {
            rst.extend(self.database.as_bytes());
            rst.push(0);
        }
        // null-terminated plugin name
        if self.capability_flags.contains(CapabilityFlags::PLUGIN_AUTH) {
            rst.extend(self.auth_plugin_name.as_bytes());
            rst.push(0);
        }
        if self
            .capability_flags
            .contains(CapabilityFlags::CONNECT_ATTRS)
        {
            let mut lb = Vec::new();
            for attr in &self.connect_attrs {
                let klen = LenEncInt::from(attr.key.len() as u64);
                lb.extend(klen.to_bytes());
                lb.extend(attr.key.as_bytes());
                let vlen = LenEncInt::from(attr.value.len() as u64);
                lb.extend(vlen.to_bytes());
                lb.extend(attr.value.as_bytes());
            }
            // use len-enc-int here
            let lblen = LenEncInt::from(lb.len() as u64);
            rst.extend(lblen.to_bytes());
            rst.extend(lb);
        }
        rst
    }
}

impl Default for HandshakeClientResponse41 {
    fn default() -> Self {
        HandshakeClientResponse41 {
            capability_flags: CapabilityFlags::default(),
            max_packet_size: 1024 * 1024 * 1024,
            // by default use utf-8
            character_set: 33,
            username: String::new(),
            auth_response: Vec::new(),
            database: String::new(),
            auth_plugin_name: String::new(),
            connect_attrs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectAttr {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSwitchRequest<'a> {
    pub header: u8,
    // null terminated string
    pub plugin_name: &'a [u8],
    // EOF terminated string
    pub auth_plugin_data: &'a [u8],
}

pub fn parse_auth_switch_request<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], AuthSwitchRequest<'a>, E>
where 
    E: ParseError<&'a [u8]>,
{
    let (input, header) = le_u8(input)?;
    debug_assert_eq!(0xfe, header);
    let (input, plugin_name) = take_till(|b| b == 0x00)(input)?;
    let (input, _) = take(1usize)(input)?;
    let (input, auth_plugin_data) = take(input.len())(input)?;
    Ok((input, AuthSwitchRequest{
        header,
        plugin_name,
        auth_plugin_data,
    }))
}

#[cfg(test)]
mod tests {

    const packet_data: &[u8] = include_bytes!("../data/packet.dat");

    use super::*;
    use crate::packet::*;
    use nom::error::VerboseError;

    #[test]
    fn test_read_handshake_packet() {
        let (_, pkt) = parse_packet::<VerboseError<_>>(packet_data).unwrap();
        let (input, handshake) = parse_initial_handshake::<VerboseError<_>>(pkt.payload).unwrap();
        assert!(input.is_empty());
        dbg!(&handshake);
        println!(
            "server_version={}",
            String::from_utf8_lossy(handshake.server_version)
        );
        println!(
            "auth_plugin_name={}",
            String::from_utf8_lossy(handshake.auth_plugin_name)
        );
        let capability_flags = CapabilityFlags::from_bits(handshake.capability_flags).unwrap();
        println!("capability_flags={:#?}", capability_flags);
    }
}
