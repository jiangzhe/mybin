use crate::event::{LogEventType, LogEventTypeCode};
use chrono::NaiveDateTime;
use nom::error::ParseError;
use nom::number::streaming::{le_u16, le_u32, le_u8};
use nom::IResult;
use serde_derive::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventHeaderV1 {
    pub timestamp: u32,
    pub type_code: LogEventTypeCode,
    pub server_id: u32,
    pub event_length: u32,
}

impl EventHeaderV1 {
    /// always equals event_length - 13
    /// NOTE: do not count START_EVENT_V3 and FORMAT_DESCRIPTION_EVENT
    /// because they use EventHeader, not EventHeaderV1
    fn data_len(&self) -> u32 {
        self.event_length - 13
    }
}

/// parse common header of v1 start event, v3 start event and v4 format description event
///
/// the header includes 4 fields:
/// timestamp 0:4, type_code 4:1, server_id: 5:4, event_length: 9:4
pub(crate) fn parse_event_header_v1<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], EventHeaderV1, E>
where
    E: ParseError<&'a [u8]>,
{
    // timestamp 0:4
    let (input, timestamp) = le_u32(input)?;
    // time_code 4:1
    let (input, type_code) = le_u8(input)?;
    // server_id 5:4
    let (input, server_id) = le_u32(input)?;
    // event_length 9:4
    let (input, event_length) = le_u32(input)?;
    Ok((
        input,
        EventHeaderV1 {
            timestamp,
            type_code: LogEventTypeCode(type_code),
            server_id,
            event_length,
        },
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventHeaderFlags(pub u16);

/// flag decoding
/// reference: https://dev.mysql.com/doc/internals/en/binlog-event-flag.html
impl EventHeaderFlags {
    /// gets unset in FORMAT_DESCRIPTION_EVENT when the file gets
    /// closed to detect broken binlogs
    pub fn is_binlog_in_use(&self) -> bool {
        (self.0 & 0x0001) == 0x0001
    }

    /// unused
    pub fn is_forced_rotate(&self) -> bool {
        (self.0 & 0x0002) == 0x0002
    }

    /// event is thread specific,
    /// e.g. CREATE TEMPORARY TABLE
    pub fn is_thread_specific(&self) -> bool {
        (self.0 & 0x0004) == 0x0004
    }

    /// event doesn't need default database to be updated,
    /// e.g. CREATE DATABASE
    pub fn is_suppress_use(&self) -> bool {
        (self.0 & 0x0008) == 0x0008
    }

    /// unused
    pub fn is_update_table_map_version(&self) -> bool {
        (self.0 & 0x0010) == 0x0010
    }

    /// event is created by the slaves SQL-thread and shouldn't
    /// update the master-log pos
    pub fn is_artificial(&self) -> bool {
        (self.0 & 0x0020) == 0x0020
    }

    /// event is created by the slaves IO-thread when written
    /// to the relay log
    pub fn is_relay_log(&self) -> bool {
        (self.0 & 0x0040) == 0x0040
    }

    pub fn is_ignorable(&self) -> bool {
        (self.0 & 0x0080) == 0x0080
    }

    pub fn is_no_filter(&self) -> bool {
        (self.0 & 0x0100) == 0x0100
    }

    pub fn is_mts_isolate(&self) -> bool {
        (self.0 & 0x0200) == 0x0200
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventHeader {
    pub timestamp: u32,
    pub type_code: LogEventTypeCode,
    pub server_id: u32,
    pub event_length: u32,
    pub next_position: u32,
    pub flags: EventHeaderFlags,
}

impl EventHeader {
    /// always equals event_length - 19
    pub fn data_len(&self) -> u32 {
        self.event_length - 19
    }
}

/// parse common header of v3 start event and v4 format description event
///
/// thie common header includes 6 fields:
/// timestamp 0:4, type_code 4:1, server_id: 5:4,
/// event_length: 9:4, next_position: 13:4, flags 17:2
pub(crate) fn parse_event_header<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], EventHeader, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, ehv1) = parse_event_header_v1(input)?;
    let (input, next_position) = le_u32(input)?;
    let (input, flags) = le_u16(input)?;
    Ok((
        input,
        EventHeader {
            timestamp: ehv1.timestamp,
            type_code: ehv1.type_code,
            server_id: ehv1.server_id,
            event_length: ehv1.event_length,
            next_position,
            flags: EventHeaderFlags(flags),
        },
    ))
}
