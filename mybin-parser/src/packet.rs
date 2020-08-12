use crate::error::Error;
use crate::flag::*;
use crate::util::{len_enc_int, len_enc_str};
use crate::handshake::{AuthSwitchRequest, parse_auth_switch_request};
use nom::bytes::streaming::{take, take_till};
use nom::error::ParseError;
use nom::number::streaming::{le_u16, le_u24, le_u8};
use nom::IResult;
use serde_derive::*;

const EMPTY_BYTE_ARRAY: [u8; 0] = [];

/// MySQL packet
///
/// reference: https://dev.mysql.com/doc/internals/en/mysql-packet.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Packet<'a> {
    pub payload_length: u32,
    pub sequence_id: u8,
    pub payload: &'a [u8],
}

/// parse packet
///
/// this method requires a fixed input, so may not
/// suitable to parse a real packet from network
pub fn parse_packet<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], Packet<'a>, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, payload_length) = le_u24(input)?;
    let (input, sequence_id) = le_u8(input)?;
    let (input, payload) = take(payload_length)(input)?;
    Ok((
        input,
        Packet {
            payload_length,
            sequence_id,
            payload,
        },
    ))
}

/// one or more packet payloads can combine to one full message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message<'a> {
    Ok(#[serde(borrow)] OkPacket<'a>),
    Err(#[serde(borrow)] ErrPacket<'a>),
    Eof(EofPacket),
}


pub fn parse_message<'a>(
    input: &'a [u8],
    cap_flags: &CapabilityFlags,
) -> Result<Message<'a>, Error> {
    parse_message_internal(input, cap_flags, true)
}

/// handshake message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HandshakeMessage<'a> {
    Ok(#[serde(borrow)] OkPacket<'a>),
    Err(#[serde(borrow)] ErrPacket<'a>),
    Switch(#[serde(borrow)] AuthSwitchRequest<'a>),
}

/// special handle handshake message
/// because err packet does not contain
/// sql state
/// reference: https://dev.mysql.com/doc/internals/en/connection-phase.html
pub fn parse_handshake_message<'a>(
    input: &'a [u8],
    cap_flags: &CapabilityFlags,
) -> Result<HandshakeMessage<'a>, Error> {
    if input.is_empty() {
        return Err(Error::Incomplete(nom::Needed::Unknown));
    }
    match input[0] {
        0x00 => {
            let (_, ok) = parse_ok_packet(input, cap_flags).map_err(|e| Error::from((input, e)))?;
            Ok(HandshakeMessage::Ok(ok))
        }
        0xff => {
            let (_, err) =
                parse_err_packet(input, cap_flags, false).map_err(|e| Error::from((input, e)))?;
            Ok(HandshakeMessage::Err(err))
        }
        0xfe => {
            let (_, switch) =
                parse_auth_switch_request(input).map_err(|e| Error::from((input, e)))?;
            Ok(HandshakeMessage::Switch(switch))
        }
        c => Err(Error::InvalidPacketCode(c)),
    }
}

fn parse_message_internal<'a>(
    input: &'a [u8],
    cap_flags: &CapabilityFlags,
    sql: bool,
) -> Result<Message<'a>, Error> {
    if input.is_empty() {
        return Err(Error::Incomplete(nom::Needed::Unknown));
    }
    match input[0] {
        0x00 => {
            let (_, ok) = parse_ok_packet(input, cap_flags).map_err(|e| Error::from((input, e)))?;
            Ok(Message::Ok(ok))
        }
        0xff => {
            let (_, err) =
                parse_err_packet(input, cap_flags, sql).map_err(|e| Error::from((input, e)))?;
            Ok(Message::Err(err))
        }
        0xfe => {
            let (_, eof) =
                parse_eof_packet(input, cap_flags).map_err(|e| Error::from((input, e)))?;
            Ok(Message::Eof(eof))
        }
        c => Err(Error::InvalidPacketCode(c)),
    }
}

/// Ok Packet
///
/// reference: https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OkPacket<'a> {
    pub header: u8,
    // actually len-enc-int
    pub affected_rows: u64,
    // actually len-enc-int
    pub last_insert_id: u64,
    // if PROTOCOL_41 or TRANSACTIONS enabled
    pub status_flags: StatusFlags,
    // if PROTOCOL_41 enabled
    pub warnings: u16,
    // if SESSION_TRACK enabled: len-enc-str
    // else: EOF-terminated string
    pub info: &'a [u8],
    // if SESSION_TRACK and SESSION_STATE_CHANGED enabled
    pub session_state_changes: &'a [u8],
}

pub fn parse_ok_packet<'a, E>(
    input: &'a [u8],
    cap_flags: &CapabilityFlags,
) -> IResult<&'a [u8], OkPacket<'a>, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, header) = le_u8(input)?;
    debug_assert_eq!(0x00, header);
    let (input, affected_rows) = len_enc_int(input)?;
    let affected_rows = affected_rows.to_u64().expect("invalid affected rows");
    let (input, last_insert_id) = len_enc_int(input)?;
    let last_insert_id = last_insert_id.to_u64().expect("invalid last insert id");
    let (input, status_flags) = if cap_flags.contains(CapabilityFlags::PROTOCOL_41)
        || cap_flags.contains(CapabilityFlags::TRANSACTIONS)
    {
        let (input, status_flags) = le_u16(input)?;
        let status_flags = StatusFlags::from_bits(status_flags).expect("invalid status flags");
        (input, status_flags)
    } else {
        (input, StatusFlags::empty())
    };
    let (input, warnings) = if cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
        le_u16(input)?
    } else {
        (input, 0)
    };
    let (input, info) = if cap_flags.contains(CapabilityFlags::SESSION_TRACK) {
        let (input, info) = len_enc_str(input)?;
        (input, info.as_bytes().expect("invalid info"))
    } else {
        // EOF terminated string
        let (input, info) = take(input.len())(input)?;
        (input, info)
    };
    let (input, session_state_changes) = if cap_flags.contains(CapabilityFlags::SESSION_TRACK)
        && status_flags.contains(StatusFlags::SESSION_STATE_CHANGED)
    {
        let (input, session_state_changes) = len_enc_str(input)?;
        (
            input,
            session_state_changes
                .as_bytes()
                .expect("invalid session state changes"),
        )
    } else {
        (input, &EMPTY_BYTE_ARRAY[..])
    };
    Ok((
        input,
        OkPacket {
            header,
            affected_rows,
            last_insert_id,
            status_flags,
            warnings,
            info,
            session_state_changes,
        },
    ))
}

/// Err Packet
///
/// reference: https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrPacket<'a> {
    pub header: u8,
    pub error_code: u16,
    // if PROTOCOL_41 enabled: string[1]
    pub sql_state_marker: u8,
    // if PROTOCOL_41 enabled: string[5]
    pub sql_state: &'a [u8],
    // EOF-terminated string
    pub error_message: &'a [u8],
}

pub fn parse_err_packet<'a, E>(
    input: &'a [u8],
    cap_flags: &CapabilityFlags,
    sql: bool,
) -> IResult<&'a [u8], ErrPacket<'a>, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, header) = le_u8(input)?;
    debug_assert_eq!(0xff, header);
    let (input, error_code) = le_u16(input)?;
    let (input, sql_state_marker, sql_state) = if sql && cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
        let (input, sql_state_marker) = le_u8(input)?;
        let (input, sql_state) = take(5usize)(input)?;
        (input, sql_state_marker, sql_state)
    } else {
        (input, 0u8, &EMPTY_BYTE_ARRAY[..])
    };
    let (input, error_message) = take(input.len())(input)?;
    Ok((
        input,
        ErrPacket {
            header,
            error_code,
            sql_state_marker,
            sql_state,
            error_message,
        },
    ))
}

/// EOF Packet
///
/// reference: https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EofPacket {
    pub header: u8,
    // if PROTOCOL_41 enabled
    pub warnings: u16,
    // if PROTOCOL_41 enabled
    pub status_flags: StatusFlags,
}

pub fn parse_eof_packet<'a, E>(
    input: &'a [u8],
    cap_flags: &CapabilityFlags,
) -> IResult<&'a [u8], EofPacket, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, header) = le_u8(input)?;
    debug_assert_eq!(0xfe, header);
    let (input, warnings, status_flags) = if cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
        let (input, warnings) = le_u16(input)?;
        let (input, status_flags) = le_u16(input)?;
        let status_flags = StatusFlags::from_bits(status_flags).expect("invalid status flags");
        (input, warnings, status_flags)
    } else {
        (input, 0, StatusFlags::empty())
    };
    Ok((
        input,
        EofPacket {
            header,
            warnings,
            status_flags,
        },
    ))
}

#[cfg(test)]
mod tests {

    const packet_data: &[u8] = include_bytes!("../data/packet.dat");

    use super::*;
    use nom::error::VerboseError;

    #[test]
    fn test_packet() {
        let (input, pkt) = parse_packet::<VerboseError<_>>(packet_data).unwrap();
        assert!(input.is_empty());
        dbg!(pkt);
    }
}
