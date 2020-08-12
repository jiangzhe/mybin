use crate::flag::*;
use crate::handshake::AuthSwitchRequest;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::error::{Error, Needed, Result};
use bytes_parser::my::ReadMyEncoding;
use bytes_parser::number::ReadNumber;
use bytes_parser::EMPTY_BYTE_ARRAY;
use bytes_parser::{ReadFrom, ReadWithContext};

/// MySQL packet
///
/// reference: https://dev.mysql.com/doc/internals/en/mysql-packet.html
#[derive(Debug, Clone)]
pub struct Packet<'a> {
    pub payload_len: u32,
    pub seq_id: u8,
    pub payload: &'a [u8],
}

impl<'a> ReadFrom<'a, Packet<'a>> for [u8] {
    fn read_from(&'a self, offset: usize) -> Result<(usize, Packet<'a>)> {
        let (offset, payload_len) = self.read_le_u24(offset)?;
        let (offset, seq_id) = self.read_u8(offset)?;
        let (offset, payload) = self.take_len(offset, payload_len as usize)?;
        Ok((
            offset,
            Packet {
                payload_len,
                seq_id,
                payload,
            },
        ))
    }
}

/// one or more packet payloads can combine to one full message
#[derive(Debug, Clone)]
pub enum Message<'a> {
    Ok(OkPacket<'a>),
    Err(ErrPacket<'a>),
    Eof(EofPacket),
}

impl<'a, 'c> ReadWithContext<'a, 'c, Message<'a>> for [u8] {
    type Context = &'c CapabilityFlags;

    fn read_with_ctx(
        &'a self,
        offset: usize,
        cap_flags: Self::Context,
    ) -> Result<(usize, Message)> {
        if self.len() <= offset {
            return Err(Error::InputIncomplete(Needed::Unknown));
        }
        match self[0] {
            0x00 => {
                let (offset, ok) = self.read_with_ctx(offset, cap_flags)?;
                Ok((offset, Message::Ok(ok)))
            }
            0xff => {
                let (offset, err) = self.read_with_ctx(offset, (cap_flags, true))?;
                Ok((offset, Message::Err(err)))
            }
            0xfe => {
                let (offset, eof) = self.read_with_ctx(offset, cap_flags)?;
                Ok((offset, Message::Eof(eof)))
            }
            c => Err(Error::ConstraintError(format!("invalid packet code {}", c))),
        }
    }
}

/// handshake message
#[derive(Debug, Clone)]
pub enum HandshakeMessage<'a> {
    Ok(OkPacket<'a>),
    Err(ErrPacket<'a>),
    Switch(AuthSwitchRequest<'a>),
}

impl<'a, 'c> ReadWithContext<'a, 'c, HandshakeMessage<'a>> for [u8] {
    type Context = &'c CapabilityFlags;

    fn read_with_ctx(
        &'a self,
        offset: usize,
        cap_flags: Self::Context,
    ) -> Result<(usize, HandshakeMessage<'a>)> {
        if self.len() <= offset {
            return Err(Error::InputIncomplete(Needed::Unknown));
        }
        match self[0] {
            0x00 => {
                let (offset, ok) = self.read_with_ctx(offset, cap_flags)?;
                Ok((offset, HandshakeMessage::Ok(ok)))
            }
            0xff => {
                let (offset, err) = self.read_with_ctx(offset, (cap_flags, false))?;
                Ok((offset, HandshakeMessage::Err(err)))
            }
            0xfe => {
                let (offset, switch) = self.read_from(offset)?;
                Ok((offset, HandshakeMessage::Switch(switch)))
            }
            c => Err(Error::ConstraintError(format!("invalid packet code {}", c))),
        }
    }
}

/// Ok Packet
///
/// reference: https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
#[derive(Debug, Clone)]
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

impl<'a, 'c> ReadWithContext<'a, 'c, OkPacket<'a>> for [u8] {
    type Context = &'c CapabilityFlags;

    fn read_with_ctx(
        &'a self,
        offset: usize,
        cap_flags: &'c CapabilityFlags,
    ) -> Result<(usize, OkPacket<'a>)> {
        let (offset, header) = self.read_u8(offset)?;
        debug_assert_eq!(0x00, header);
        let (offset, affected_rows) = self.read_len_enc_int(offset)?;
        let affected_rows = affected_rows.to_u64().expect("invalid affected rows");
        let (offset, last_insert_id) = self.read_len_enc_int(offset)?;
        let last_insert_id = last_insert_id.to_u64().expect("invalid last insert id");
        let (offset, status_flags) = if cap_flags.contains(CapabilityFlags::PROTOCOL_41)
            || cap_flags.contains(CapabilityFlags::TRANSACTIONS)
        {
            let (offset, status_flags) = self.read_le_u16(offset)?;
            let status_flags = StatusFlags::from_bits(status_flags).expect("invalid status flags");
            (offset, status_flags)
        } else {
            (offset, StatusFlags::empty())
        };
        let (offset, warnings) = if cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            self.read_le_u16(offset)?
        } else {
            (offset, 0)
        };
        let (offset, info) = if cap_flags.contains(CapabilityFlags::SESSION_TRACK) {
            let (offset, info) = self.read_len_enc_str(offset)?;
            (offset, info.into_ref().expect("invalid info"))
        } else {
            let (offset, info): (_, &'a [u8]) = self.take_len(offset, self.len() - offset)?;
            (offset, info)
        };
        let (offset, session_state_changes) = if cap_flags.contains(CapabilityFlags::SESSION_TRACK)
            && status_flags.contains(StatusFlags::SESSION_STATE_CHANGED)
        {
            let (offset, session_state_changes) = self.read_len_enc_str(offset)?;
            (
                offset,
                session_state_changes
                    .into_ref()
                    .expect("invalid session state changes"),
            )
        } else {
            (offset, &EMPTY_BYTE_ARRAY[..])
        };
        Ok((
            offset,
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
}

/// Err Packet
///
/// reference: https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
#[derive(Debug, Clone)]
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

impl<'a, 'c> ReadWithContext<'a, 'c, ErrPacket<'a>> for [u8] {
    type Context = (&'c CapabilityFlags, bool);

    fn read_with_ctx(
        &'a self,
        offset: usize,
        (cap_flags, sql): Self::Context,
    ) -> Result<(usize, ErrPacket<'a>)> {
        let (offset, header) = self.read_u8(offset)?;
        debug_assert_eq!(0xff, header);
        let (offset, error_code) = self.read_le_u16(offset)?;
        let (offset, sql_state_marker, sql_state) =
            if sql && cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
                let (offset, sql_state_marker) = self.read_u8(offset)?;
                let (offset, sql_state) = self.take_len(offset, 5usize)?;
                (offset, sql_state_marker, sql_state)
            } else {
                (offset, 0u8, &EMPTY_BYTE_ARRAY[..])
            };
        let (offset, error_message) = self.take_len(offset, self.len() - offset)?;
        Ok((
            offset,
            ErrPacket {
                header,
                error_code,
                sql_state_marker,
                sql_state,
                error_message,
            },
        ))
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

impl<'a, 'c> ReadWithContext<'a, 'c, EofPacket> for [u8] {
    type Context = &'c CapabilityFlags;

    fn read_with_ctx(&self, offset: usize, cap_flags: Self::Context) -> Result<(usize, EofPacket)> {
        let (offset, header) = self.read_u8(offset)?;
        debug_assert_eq!(0xfe, header);
        let (offset, warnings, status_flags) = if cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            let (offset, warnings) = self.read_le_u16(offset)?;
            let (offset, status_flags) = self.read_le_u16(offset)?;
            let status_flags = StatusFlags::from_bits(status_flags).expect("invalid status flags");
            (offset, warnings, status_flags)
        } else {
            (offset, 0, StatusFlags::empty())
        };
        Ok((
            offset,
            EofPacket {
                header,
                warnings,
                status_flags,
            },
        ))
    }
}

#[cfg(test)]
mod tests {

    const PACKET_DATA: &[u8] = include_bytes!("../data/packet.dat");

    use super::*;
    use bytes_parser::ReadFrom;

    #[test]
    fn test_packet() {
        let (offset, pkt): (_, Packet<'_>) = PACKET_DATA.read_from(0).unwrap();
        assert_eq!(PACKET_DATA.len(), offset);
        dbg!(pkt);
    }

    #[test]
    fn test_ok_packet() {
        let input: Vec<u8> = vec![0, 0, 0, 2, 0, 0, 0];
        let (_, pkt): (_, OkPacket) = input
            .read_with_ctx(0, &CapabilityFlags::PROTOCOL_41)
            .unwrap();
        dbg!(pkt);
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
        let (_, pkt): (_, ErrPacket) = input
            .read_with_ctx(0, (&CapabilityFlags::PROTOCOL_41, true))
            .unwrap();
        // dbg!(pkt);
        println!("{}", pkt.error_code);
        println!("{:?}", pkt.sql_state_marker);
        println!("{}", String::from_utf8_lossy(pkt.sql_state));
        println!("{}", String::from_utf8_lossy(pkt.error_message));
    }
}
