use crate::flag::*;
use crate::handshake::AuthSwitchRequest;
use bytes::{Buf, Bytes};
use bytes_parser::error::{Error, Needed, Result};
use bytes_parser::my::ReadMyEnc;
use bytes_parser::{ReadBytesExt, ReadFromBytes, ReadFromBytesWithContext};

/// MySQL packet
///
/// reference: https://dev.mysql.com/doc/internals/en/mysql-packet.html
#[derive(Debug, Clone)]
pub struct Packet {
    pub payload_len: u32,
    pub seq_id: u8,
    pub payload: Bytes,
}

impl ReadFromBytes for Packet {
    fn read_from(input: &mut Bytes) -> Result<Packet> {
        let payload_len = input.read_le_u24()?;
        let seq_id = input.read_u8()?;
        let payload = input.read_len(payload_len as usize)?;
        Ok(Packet {
            payload_len,
            seq_id,
            payload,
        })
    }
}

/// one or more packet payloads can combine to one full message
#[derive(Debug, Clone)]
pub enum Message {
    Ok(OkPacket),
    Err(ErrPacket),
    Eof(EofPacket),
}

/// handshake message
#[derive(Debug, Clone)]
pub enum HandshakeMessage {
    Ok(OkPacket),
    Err(ErrPacket),
    Switch(AuthSwitchRequest),
}

impl<'c> ReadFromBytesWithContext<'c> for HandshakeMessage {
    type Context = &'c CapabilityFlags;

    fn read_with_ctx(input: &mut Bytes, cap_flags: Self::Context) -> Result<Self> {
        if !input.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match input[0] {
            0x00 => {
                let ok = OkPacket::read_with_ctx(input, cap_flags)?;
                Ok(HandshakeMessage::Ok(ok))
            }
            0xff => {
                let err = ErrPacket::read_with_ctx(input, (cap_flags, false))?;
                Ok(HandshakeMessage::Err(err))
            }
            0xfe => {
                let switch = AuthSwitchRequest::read_from(input)?;
                Ok(HandshakeMessage::Switch(switch))
            }
            c => Err(Error::ConstraintError(format!("invalid packet code {}", c))),
        }
    }
}

/// Ok Packet
///
/// reference: https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
#[derive(Debug, Clone)]
pub struct OkPacket {
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
    pub info: Bytes,
    // if SESSION_TRACK and SESSION_STATE_CHANGED enabled
    pub session_state_changes: Bytes,
}

impl<'c> ReadFromBytesWithContext<'c> for OkPacket {
    type Context = &'c CapabilityFlags;

    fn read_with_ctx(input: &mut Bytes, cap_flags: Self::Context) -> Result<OkPacket> {
        // header can be either 0x00 or 0xfe
        let header = input.read_u8()?;
        let affected_rows = input.read_len_enc_int()?;
        let affected_rows = affected_rows
            .to_u64()
            .ok_or_else(|| Error::ConstraintError("invalid affected rows".to_owned()))?;
        let last_insert_id = input.read_len_enc_int()?;
        let last_insert_id = last_insert_id
            .to_u64()
            .ok_or_else(|| Error::ConstraintError("invalid last insert id".to_owned()))?;
        let status_flags = if cap_flags.contains(CapabilityFlags::PROTOCOL_41)
            || cap_flags.contains(CapabilityFlags::TRANSACTIONS)
        {
            let status_flags = input.read_le_u16()?;
            StatusFlags::from_bits(status_flags).ok_or_else(|| {
                Error::ConstraintError(format!("invalid status flags {:b}", status_flags))
            })?
        } else {
            StatusFlags::empty()
        };
        let warnings = if cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            input.read_le_u16()?
        } else {
            0
        };
        let info = if cap_flags.contains(CapabilityFlags::SESSION_TRACK) {
            let info = input.read_len_enc_str()?;
            info.into_bytes()
                .ok_or_else(|| Error::ConstraintError("invalid info".to_owned()))?
        } else {
            input.split_to(input.remaining())
        };
        let session_state_changes = if cap_flags.contains(CapabilityFlags::SESSION_TRACK)
            && status_flags.contains(StatusFlags::SESSION_STATE_CHANGED)
        {
            let session_state_changes = input.read_len_enc_str()?;
            session_state_changes
                .into_bytes()
                .ok_or_else(|| Error::ConstraintError("invalid session state changes".to_owned()))?
        } else {
            Bytes::new()
        };
        Ok(OkPacket {
            header,
            affected_rows,
            last_insert_id,
            status_flags,
            warnings,
            info,
            session_state_changes,
        })
    }
}

/// Err Packet
///
/// reference: https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
#[derive(Debug, Clone)]
pub struct ErrPacket {
    pub header: u8,
    pub error_code: u16,
    // if PROTOCOL_41 enabled: string[1]
    pub sql_state_marker: u8,
    // if PROTOCOL_41 enabled: string[5]
    pub sql_state: Bytes,
    // EOF-terminated string
    pub error_message: Bytes,
}

impl<'c> ReadFromBytesWithContext<'c> for ErrPacket {
    type Context = (&'c CapabilityFlags, bool);

    fn read_with_ctx(input: &mut Bytes, (cap_flags, sql): Self::Context) -> Result<ErrPacket> {
        let header = input.read_u8()?;
        let error_code = input.read_le_u16()?;
        let (sql_state_marker, sql_state) =
            if sql && cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
                let sql_state_marker = input.read_u8()?;
                let sql_state = input.read_len(5usize)?;
                (sql_state_marker, sql_state)
            } else {
                (0u8, Bytes::new())
            };
        let error_message = input.split_to(input.remaining());
        Ok(ErrPacket {
            header,
            error_code,
            sql_state_marker,
            sql_state,
            error_message,
        })
    }
}

/// EOF Packet
///
/// reference: https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
#[derive(Debug, Clone)]
pub struct EofPacket {
    pub header: u8,
    // if PROTOCOL_41 enabled
    pub warnings: u16,
    // if PROTOCOL_41 enabled
    pub status_flags: StatusFlags,
}

impl<'c> ReadFromBytesWithContext<'c> for EofPacket {
    type Context = &'c CapabilityFlags;

    fn read_with_ctx(input: &mut Bytes, cap_flags: Self::Context) -> Result<EofPacket> {
        let header = input.read_u8()?;
        let (warnings, status_flags) = if cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            let warnings = input.read_le_u16()?;
            let status_flags = input.read_le_u16()?;
            let status_flags = StatusFlags::from_bits(status_flags).ok_or_else(|| {
                Error::ConstraintError(format!("invalid status flags {:b}", status_flags))
            })?;
            (warnings, status_flags)
        } else {
            (0, StatusFlags::empty())
        };
        Ok(EofPacket {
            header,
            warnings,
            status_flags,
        })
    }
}

#[cfg(test)]
mod tests {

    const PACKET_DATA: &[u8] = include_bytes!("../data/packet.dat");

    use super::*;

    #[test]
    fn test_packet() {
        let mut input = (&PACKET_DATA[..]).to_bytes();
        let pkt = Packet::read_from(&mut input).unwrap();
        dbg!(pkt);
    }

    #[test]
    fn test_ok_packet() {
        let input: Vec<u8> = vec![0, 0, 0, 2, 0, 0, 0];
        let mut input = (&input[..]).to_bytes();
        let ok = OkPacket::read_with_ctx(&mut input, &CapabilityFlags::PROTOCOL_41).unwrap();
        dbg!(ok);
    }

    #[test]
    fn test_err_packet() {
        let input: Vec<u8> = vec![
            255, 212, 4, 35, 72, 89, 48, 48, 48, 83, 108, 97, 118, 101, 32, 99, 97, 110, 32, 110,
            111, 116, 32, 104, 97, 110, 100, 108, 101, 32, 114, 101, 112, 108, 105, 99, 97, 116,
            105, 111, 110, 32, 101, 118, 101, 110, 116, 115, 32, 119, 105, 116, 104, 32, 116, 104,
            101, 32, 99, 104, 101, 99, 107, 115, 117, 109, 32, 116, 104, 97, 116, 32, 109, 97, 115,
            116, 101, 114, 32, 105, 115, 32, 99, 111, 110, 102, 105, 103, 117, 114, 101, 100, 32,
            116, 111, 32, 108, 111, 103, 59, 32, 116, 104, 101, 32, 102, 105, 114, 115, 116, 32,
            101, 118, 101, 110, 116, 32, 39, 109, 121, 115, 113, 108, 45, 98, 105, 110, 46, 48, 48,
            48, 48, 48, 49, 39, 32, 97, 116, 32, 52, 44, 32, 116, 104, 101, 32, 108, 97, 115, 116,
            32, 101, 118, 101, 110, 116, 32, 114, 101, 97, 100, 32, 102, 114, 111, 109, 32, 39, 46,
            47, 109, 121, 115, 113, 108, 45, 98, 105, 110, 46, 48, 48, 48, 48, 48, 49, 39, 32, 97,
            116, 32, 49, 50, 51, 44, 32, 116, 104, 101, 32, 108, 97, 115, 116, 32, 98, 121, 116,
            101, 32, 114, 101, 97, 100, 32, 102, 114, 111, 109, 32, 39, 46, 47, 109, 121, 115, 113,
            108, 45, 98, 105, 110, 46, 48, 48, 48, 48, 48, 49, 39, 32, 97, 116, 32, 49, 50, 51, 46,
        ];
        let mut input = (&input[..]).to_bytes();
        let err =
            ErrPacket::read_with_ctx(&mut input, (&CapabilityFlags::PROTOCOL_41, true)).unwrap();
        dbg!(err);
    }
}
