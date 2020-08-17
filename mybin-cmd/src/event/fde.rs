//! start event and format description event
use bytes_parser::ReadAs;
use bytes_parser::number::ReadNumber;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::Result;
use super::{LogEventType, LogEventTypeCode};

/// Data of StartEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/start-event-v3.html
#[derive(Debug, Clone)]
pub struct StartData<'a> {
    pub binlog_version: u16,
    pub server_version: &'a [u8],
    pub create_timestamp: u32,
}

impl<'a> ReadAs<'a, StartData<'a>> for [u8] {
    fn read_as(&'a self, offset: usize) -> Result<(usize, StartData<'a>)> {
        let (offset, binlog_version) = self.read_le_u16(offset)?;
        let (offset, server_version) = self.take_len(offset, 50)?;
        // remove tail \x00
        let (_, server_version) = server_version.take_until(0, 0, false)?;
        let (offset, create_timestamp) = self.read_le_u32(offset)?;
        Ok((
            offset,
            StartData {
                binlog_version,
                server_version,
                create_timestamp,
            },
        ))
    }
}

/// Data of FormatDescriptionEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/format-description-event.html
#[derive(Debug, Clone)]
pub struct FormatDescriptionData<'a> {
    pub binlog_version: u16,
    pub server_version: &'a [u8],
    pub create_timestamp: u32,
    pub header_length: u8,
    pub post_header_lengths: &'a [u8],
    // only record checksum flag, should be 0 or 1 after mysql 5.6.1
    // in case of earlier version, set 0
    pub checksum_flag: u8,
    // including 4-byte checksum in FDE
    pub crc32: u32,
}

/// because FDE is the first event in binlog, we do not know its post header length,
/// so we need the total data size as input argument,
/// which can be calculated by event_length - 19
impl<'a> ReadAs<'a, FormatDescriptionData<'a>> for [u8] {

    fn read_as(&'a self, offset: usize) -> Result<(usize, FormatDescriptionData<'a>)> {
        let (offset, StartData{binlog_version, server_version, create_timestamp}) = self.read_as(offset)?;
        let (offset, header_length) = self.read_u8(offset)?;
        // 57(2+50+4+1) bytes consumed
        // actually there are several random-value bytes at end of the payload
        // but that does not affect post header lengths of existing events
        let (offset, post_header_lengths) = self.take_len(offset, self.len() - offset)?;
        // before mysql 5.6.1, there is no checksum so the data len is
        // same as total size of all 5 fields
        // but from 5.6.1, there is 5 additional bytes at end of the
        // post header lengths field if checksum is enabled
        // we use self contained FDE post header len to check if
        // the checksum flag and checksum value exist
        let fde_type_code = LogEventTypeCode::from(LogEventType::FormatDescriptionEvent);
        let fde_post_header_len = post_header_lengths[fde_type_code.0 as usize - 1];
        if self.len() == fde_post_header_len as usize {
            // version not support checksum
            return Ok((
                offset,
                FormatDescriptionData {
                    binlog_version,
                    server_version,
                    create_timestamp,
                    header_length,
                    post_header_lengths,
                    checksum_flag: 0,
                    crc32: 0,
                },
            ));
        }
        // version supports checksum
        // split checksum
        let checksum_len = self.len() - fde_post_header_len as usize;
        let (post_header_lengths, checksum_in) =
            post_header_lengths.split_at(post_header_lengths.len() - checksum_len);
        let (_, checksum_flag) = checksum_in.read_u8(0)?;
        let (_, crc32) = checksum_in.read_le_u32(1)?;
        Ok((
            offset,
            FormatDescriptionData {
                binlog_version,
                server_version,
                create_timestamp,
                header_length,
                post_header_lengths,
                checksum_flag,
                crc32,
            },
        ))
    }
}
