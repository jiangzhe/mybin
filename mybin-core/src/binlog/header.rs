use super::LogEventTypeCode;
use bitflags::bitflags;
use bytes::Bytes;
use bytes_parser::error::Result;
use bytes_parser::{ReadBytesExt, ReadFromBytes};

#[derive(Debug, Clone)]
pub struct EventHeaderV1 {
    pub timestamp: u32,
    pub type_code: LogEventTypeCode,
    pub server_id: u32,
    pub event_len: u32,
}

impl EventHeaderV1 {
    /// always equals event_length - 13
    /// NOTE: do not count START_EVENT_V3 and FORMAT_DESCRIPTION_EVENT
    /// because they use EventHeader, not EventHeaderV1
    #[allow(dead_code)]
    fn data_len(&self) -> u32 {
        self.event_len - 13
    }
}

/// parse common header of v1 start event, v3 start event and v4 format description event
///
/// the header includes 4 fields:
/// timestamp 0:4, type_code 4:1, server_id: 5:4, event_length: 9:4
impl ReadFromBytes for EventHeaderV1 {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let timestamp = input.read_le_u32()?;
        let type_code = input.read_u8()?;
        let server_id = input.read_le_u32()?;
        let event_len = input.read_le_u32()?;
        Ok(EventHeaderV1 {
            timestamp,
            type_code: LogEventTypeCode(type_code),
            server_id,
            event_len,
        })
    }
}

bitflags! {
    pub struct EventHeaderFlags: u16 {
        const BINLOG_IN_USE         = 0x0001;
        const FORCED_ROTATE         = 0x0002;
        const THREAD_SPECIFIC       = 0x0004;
        const SUPRESS_USE           = 0x0008;
        const UPDATE_TABLE_MAP_VERSION  = 0x0010;
        const ARTIFICIAL            = 0x0020;
        const RELAY_LOG             = 0x0040;
        const IGNORABLE             = 0x0080;
        const NO_FILTER             = 0x0100;
        const MTS_ISOLATE           = 0x0200;
    }
}

#[derive(Debug, Clone)]
pub struct EventHeader {
    pub timestamp: u32,
    pub type_code: LogEventTypeCode,
    pub server_id: u32,
    pub event_len: u32,
    pub next_pos: u32,
    pub flags: EventHeaderFlags,
}

impl EventHeader {
    /// always equals event_length - 19
    pub fn data_len(&self) -> u32 {
        self.event_len - 19
    }
}

/// parse common header of v3 start event and v4 format description event
///
/// thie common header includes 6 fields:
/// timestamp 0:4, type_code 4:1, server_id: 5:4,
/// event_length: 9:4, next_position: 13:4, flags 17:2
impl ReadFromBytes for EventHeader {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let EventHeaderV1 {
            timestamp,
            type_code,
            server_id,
            event_len,
        } = EventHeaderV1::read_from(input)?;
        let next_pos = input.read_le_u32()?;
        let flags = input.read_le_u16()?;
        Ok(EventHeader {
            timestamp,
            type_code,
            server_id,
            event_len,
            next_pos,
            flags: EventHeaderFlags::from_bits_truncate(flags),
        })
    }
}
