use super::{LogEventType, FormatDescriptionEvent};
use super::header::EventHeader;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::ReadFrom;
use bytes_parser::error::{Result, Error};

#[derive(Debug, Clone, PartialEq)]
pub enum BinlogVersion {
    V1,
    V3,
    V4,
}

/// parse binlog version
///
/// consume preceding 4-byte magic word
/// and determine the binlog version based on the first event.
/// NOTE: some old versions of mysql are not supported and will panic in this method.
/// reference: https://dev.mysql.com/doc/internals/en/binary-log-versions.html
impl ReadFrom<'_, BinlogVersion> for [u8] {
    fn read_from(&self, offset: usize) -> Result<(usize, BinlogVersion)> {
        let (offset, magic) = self.take_len(offset, 4)?;
        if magic != b"\xfebin" {
            return Err(Error::ConstraintError(format!("invalid magic number: {:?}", magic)));
        }
        let (_, header): (_, EventHeader) = self.read_from(offset)?;
        match LogEventType::from(header.type_code) {
            LogEventType::StartEventV3 => {
                if header.event_len < 75 {
                    Ok((offset, BinlogVersion::V1))
                } else {
                    Ok((offset, BinlogVersion::V3))
                }
            }
            LogEventType::FormatDescriptionEvent => Ok((offset, BinlogVersion::V4)),
            et => Err(Error::ConstraintError(format!("invalid event type: {:?}", et))),
        }
    }
}

#[derive(Debug)]
pub struct ParserV4 {
    post_header_lengths: Vec<u8>,
    // whether the crc32 checksum is enabled
    // if enabled, will validate the tail 4-byte checksum of all events
    checksum: bool,
}

impl ParserV4 {
    /// create new parser by given post header lengths and checksum flag
    pub fn new(post_header_lengths: Vec<u8>, checksum: bool) -> Self {
        ParserV4 {
            post_header_lengths,
            checksum,
        }
    }

    /// create parser from given format description event
    pub fn from_fde(fde: &FormatDescriptionEvent) -> Self {
        let post_header_lengths = post_header_lengths_from_raw(fde.post_header_lengths);
        let checksum = fde.checksum_flag == 1;
        ParserV4::new(post_header_lengths, checksum)
    }

    // this function will verify binlog version to be v4
    // and consume FDE to get post header lengths for all
    // following events
    pub fn from_binlog_file(input: &[u8]) -> Result<(usize, Self)> {
        let (offset, binlog_version): (_, BinlogVersion) = input.read_from(0)?;
        if binlog_version != BinlogVersion::V4 {
            return Err(Error::ConstraintError(
                format!("Unsupported binlog version: {:?}", binlog_version)));
        }
        let (offset, fde) = input.read_from(offset)?;
        Ok((offset, Self::from_fde(&fde)))
    }
    
}

// raw lengths originated from FDE in binlog file/stream does not include
// length on UnknownEvent(code=0),
// we need to push 0 at first position
fn post_header_lengths_from_raw(raw_lengths: &[u8]) -> Vec<u8> {
    let mut post_header_lengths: Vec<u8> = Vec::with_capacity(raw_lengths.len() + 1);
    post_header_lengths.push(0);
    post_header_lengths.extend_from_slice(raw_lengths);
    post_header_lengths
}