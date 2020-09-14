//! start event and format description event
use super::LogEventType;
use bytes::{Buf, Bytes};
use bytes_parser::Result;
use bytes_parser::{ReadBytesExt, ReadFromBytes};

/// Data of StartEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/start-event-v3.html
#[derive(Debug, Clone)]
pub struct StartData {
    pub binlog_version: u16,
    pub server_version: String,
    pub create_timestamp: u32,
}

impl ReadFromBytes for StartData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let binlog_version = input.read_le_u16()?;
        let mut server_version = input.read_len(50)?;
        // remove tail \x00
        let server_version = server_version.read_until(0, false)?;
        let server_version = String::from_utf8(Vec::from(server_version.as_ref()))?;
        let create_timestamp = input.read_le_u32()?;
        Ok(StartData {
            binlog_version,
            server_version,
            create_timestamp,
        })
    }
}

/// Data of FormatDescriptionEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/format-description-event.html
#[derive(Debug, Clone)]
pub struct FormatDescriptionData {
    pub binlog_version: u16,
    pub server_version: String,
    pub create_timestamp: u32,
    pub header_length: u8,
    pub post_header_lengths: Vec<u8>,
    // only record checksum flag, should be 0 or 1 after mysql 5.6.1
    // in case of earlier version, set 0
    pub checksum_flag: u8,
}

/// because FDE is the first event in binlog, we do not know its post header length,
/// so we need the total data size as input argument,
/// which can be calculated by event_length - 19
impl ReadFromBytes for FormatDescriptionData {
    fn read_from(input: &mut Bytes) -> Result<FormatDescriptionData> {
        let StartData {
            binlog_version,
            server_version,
            create_timestamp,
        } = StartData::read_from(input)?;
        let header_length = input.read_u8()?;
        // 57(2+50+4+1) bytes consumed
        // actually there are several random-value bytes at end of the payload
        // but that does not affect post header lengths of existing events

        // before mysql 5.6.1, there is no checksum so the data len is
        // same as total size of all 5 fields
        // but from 5.6.1, there is 5 additional bytes at end of the
        // post header lengths field if checksum is enabled
        // we use self contained FDE post header len to check if
        // the checksum flag and checksum value exist
        let fde_type_code = LogEventType::FormatDescriptionEvent;
        let fde_post_header_len = input[u8::from(fde_type_code) as usize - 1] - 57;
        if input.remaining() == fde_post_header_len as usize {
            // version not support checksum
            let post_header_lengths = input.split_to(input.remaining());
            let post_header_lengths = Vec::from(post_header_lengths.bytes());
            return Ok(FormatDescriptionData {
                binlog_version,
                server_version,
                create_timestamp,
                header_length,
                post_header_lengths,
                checksum_flag: 0,
            });
        }
        // version supports checksum
        let post_header_lengths = input.split_to(fde_post_header_len as usize);
        let post_header_lengths = Vec::from(post_header_lengths.bytes());
        let checksum_flag = input.read_u8()?;
        // there may be remaining 4-byte crc32 checksum at last or not
        Ok(FormatDescriptionData {
            binlog_version,
            server_version,
            create_timestamp,
            header_length,
            post_header_lengths,
            checksum_flag,
        })
    }
}
