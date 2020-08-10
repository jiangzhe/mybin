use serde_derive::*;
use nom::IResult;
use nom::error::ParseError;
use nom::number::streaming::{le_u8, le_u16, le_u32};
use nom::bytes::streaming::{take, take_till};
use crate::error::Error;
use bitflags::bitflags;

/// used for placeholder for optional part in payload
const EMPTY_BYTE_ARRAY: [u8;0] = [];

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
    let capability_flags = (capability_flags_lower as u32) | ((capability_flags_upper as u32) << 16);
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
    Ok((input, InitialHandshake{
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
    }))
}

bitflags! {
    pub struct CapabilityFlags: u32 {
        const LONG_PASSWORD     = 0x0000_0001;
        const FOUND_ROWS        = 0x0000_0002;
        const LONG_FLAG         = 0x0000_0004;
        const CONNECT_WITH_DB   = 0x0000_0008;
        const NO_SCHEMA         = 0x0000_0010;
        const COMPRESS          = 0x0000_0020;
        const ODBC              = 0x0000_0040;
        const LOCAL_FILES       = 0x0000_0080;
        const IGNORE_SPACE      = 0x0000_0100;
        const PROTOCOL_41       = 0x0000_0200;
        const INTERACTIVE       = 0x0000_0400;
        const SSL               = 0x0000_0800;
        const IGNORE_SIGPIPE    = 0x0000_1000;
        const TRANSACTIONS      = 0x0000_2000;
        const RESERVED          = 0x0000_4000;
        const SECURE_CONNECTION = 0x0000_8000;
        const MULTI_STATEMENTS  = 0x0001_0000;
        const MULTI_RESULTS     = 0x0002_0000;
        const PS_MULTI_RESULTS  = 0x0004_0000;
        const PLUGIN_AUTH       = 0x0008_0000;
        const CONNECT_ATTRS     = 0x0010_0000;
        const PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x0020_0000;
        const CAN_HANDLE_EXPIRED_PASSWORDS = 0x0040_0000;
        const SESSION_TRACK     = 0x0080_0000;
        const DEPRECATE_EOF     = 0x0100_0000;
        const SSL_VERITY_SERVER_CERT = 0x4000_0000;
        const REMEMBER_OPTIONS  = 0x8000_0000;
    }
}

impl Default for CapabilityFlags {
    fn default() -> Self {
        Self::empty()
        | CapabilityFlags::LONG_PASSWORD
        | CapabilityFlags::FOUND_ROWS
        | CapabilityFlags::LONG_FLAG
        // | CapabilityFlags::CONNECT_WITH_DB
        // | CapabilityFlags::NO_SCHEMA
        // | CapabilityFlags::COMPRESS
        // | CapabilityFlags::ODBC 
        // | CapabilityFlags::LOCAL_FILES
        // | CapabilityFlags::IGNORE_SPACE
        | CapabilityFlags::PROTOCOL_41
        // | CapabilityFlags::INTERACTIVE 
        // | CapabilityFlags::SSL 
        // | CapabilityFlags::IGNORE_SIGPIPE 
        | CapabilityFlags::TRANSACTIONS 
        | CapabilityFlags::RESERVED 
        // | CapabilityFlags::SECURE_CONNECTION 
        // | CapabilityFlags::MULTI_STATEMENTS 
        | CapabilityFlags::MULTI_RESULTS 
        | CapabilityFlags::PS_MULTI_RESULTS 
        | CapabilityFlags::PLUGIN_AUTH 
        | CapabilityFlags::CONNECT_ATTRS 
        | CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA 
        // | CapabilityFlags::CAN_HANDLE_EXPIRED_PASSWORDS 
        | CapabilityFlags::SESSION_TRACK 
        | CapabilityFlags::DEPRECATE_EOF 
        // | CapabilityFlags::SSL_VERITY_SERVER_CERT
        // | CapabilityFlags::REMEMBER_OPTIONS
    }
}

bitflags! {
    pub struct StatusFlags: u16 {
        const STATUS_IN_TRANS           = 0x0001;
        const STATUS_AUTOCOMMIT         = 0x0002;
        const MORE_RESULTS_EXISTS       = 0x0008;
        const STATUS_NO_GOOD_INDEX_USED = 0x0010;
        const STATUS_NO_INDEX_USED      = 0x0020;
        const STATUS_CURSOR_EXISTS      = 0x0040;
        const STATUS_LAST_ROW_SENT      = 0x0080;
        const STATUS_DB_DROPPED         = 0x0100;
        const STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
        const STATUS_METADATA_CHANGED   = 0x0400;
        const QUERY_WAS_SLOW            = 0x0800;
        const PS_OUT_PARAMS             = 0x1000;
        const STATUS_IN_TRANS_READONLY  = 0x2000;
        const SESSION_STATE_CHANGED     = 0x4000;
    }
}

/// handshake response of client protocol 41
/// 
/// reference: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html
/// this struct should be constructed by user and will be sent to
/// MySQL server to finish handshake process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeClientResponse41 {
    pub capability_flags: u32,
    pub max_packet_size: u32,
    pub character_set: u8,
    // 23 bytes of 0x00, reserved
    pub username: String,
    // vary according to capability flags and auth setting
    pub auth_response: Vec<u8>,
    // exists if db is specified
    pub database: Option<String>,
    // exists if plugin auth
    pub auth_plugin_name: Option<String>,
    // connect attributes
    pub connect_attrs: Vec<ConnectAttr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectAttr {
    pub key: String,
    pub value: String,
}

impl HandshakeClientResponse41 {

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
        println!("server_version={}", String::from_utf8_lossy(handshake.server_version));
        println!("auth_plugin_name={}", String::from_utf8_lossy(handshake.auth_plugin_name));
        let capability_flags = CapabilityFlags::from_bits(handshake.capability_flags).unwrap();
        println!("capability_flags={:#?}", capability_flags);
    }
}