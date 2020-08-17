use crate::flag::*;
use bytes_parser::error::{Result, Error, Needed};
use bytes_parser::{ReadAs, ReadWithContext};
use bytes_parser::bytes::ReadBytes;
use bytes_parser::number::ReadNumber;
use bytes_parser::my::ReadMyEncoding;
use bytes_parser::EMPTY_BYTE_ARRAY;

/// MySQL packet
///
/// reference: https://dev.mysql.com/doc/internals/en/mysql-packet.html
#[derive(Debug, Clone)]
pub struct Packet<'a> {
    pub payload_len: u32,
    pub seq_id: u8,
    pub payload: &'a [u8],
}

impl<'a> ReadAs<'a, Packet<'a>> for [u8] {
    fn read_as(&'a self, offset: usize) -> Result<(usize, Packet<'a>)> {
        let (offset, payload_len) = self.read_le_u24(offset)?;
        let (offset, seq_id) = self.read_u8(offset)?;
        let (offset, payload) = self.take_len(offset, payload_len as usize)?;
        Ok((offset, Packet{
            payload_len,
            seq_id,
            payload,
        }))
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

    fn read_with_ctx(&'a self, offset: usize, cap_flags: Self::Context) -> Result<(usize, Message)> {
        parse_message(self, offset, cap_flags, true)
    }
}

fn parse_message<'a, 'c>(input: &'a [u8], offset: usize, 
    cap_flags: &'c CapabilityFlags, sql: bool) -> Result<(usize, Message<'a>)> 
{
    if input.len() <= offset {
        return Err(Error::InputIncomplete(Needed::Unknown));
    }
    match input[0] {
        0x00 => {
            let (offset, ok) = input.read_with_ctx(offset, cap_flags)?;
            Ok((offset, Message::Ok(ok)))
        }
        0xff => {
            let (offset, err) = input.read_with_ctx(offset, (cap_flags, sql))?;
            Ok((offset, Message::Err(err)))
        }
        0xfe => {
            let (offset, eof) = input.read_with_ctx(offset, cap_flags)?;
            Ok((offset, Message::Eof(eof)))
        }
        c => Err(Error::ConstraintError(format!("invalid packet code {}", c))),
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

    fn read_with_ctx(&'a self, offset: usize, cap_flags: &'c CapabilityFlags) -> Result<(usize, OkPacket<'a>)> {
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
            let (offset, info): (_, &'a [u8]) = self.take_until(offset, 0, false)?;
            let (offset, _) = self.take_len(offset, 1usize)?;
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

    fn read_with_ctx(&'a self, offset: usize, (cap_flags, sql): Self::Context) -> Result<(usize, ErrPacket<'a>)> {
        let (offset, header) = self.read_u8(offset)?;
        debug_assert_eq!(0xff, header);
        let (offset, error_code) = self.read_le_u16(offset)?;
        let (offset, sql_state_marker, sql_state) = if sql && cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
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

    const packet_data: &[u8] = include_bytes!("../data/packet.dat");

    use super::*;
    use bytes_parser::ReadAs;

    #[test]
    fn test_packet() {
        let (offset, pkt): (_, Packet<'_>) = packet_data.read_as(0).unwrap();
        assert_eq!(packet_data.len(), offset);
        dbg!(pkt);
    }
}
