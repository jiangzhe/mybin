use super::header::EventHeader;
use super::*;
use crate::util::checksum_crc32;
use bytes::{Buf, Bytes};
use bytes_parser::{ReadBytesExt, ReadFromBytes};
// use bytes_parser::error::{Result, Error};
use crate::error::{Error, Result};

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
impl ReadFromBytes for BinlogVersion {
    fn read_from(input: &mut Bytes) -> bytes_parser::error::Result<Self> {
        let magic = input.read_len(4)?;
        if magic.as_ref() != b"\xfebin" {
            return Err(bytes_parser::error::Error::ConstraintError(format!(
                "invalid magic number: {:?}",
                magic
            )));
        }
        // clone to avoid consuming event
        let header = EventHeader::read_from(&mut input.clone())?;
        match LogEventType::from(header.type_code) {
            LogEventType::StartEventV3 => {
                if header.event_len < 75 {
                    Ok(BinlogVersion::V1)
                } else {
                    Ok(BinlogVersion::V3)
                }
            }
            LogEventType::FormatDescriptionEvent => Ok(BinlogVersion::V4),
            et => Err(bytes_parser::error::Error::ConstraintError(format!(
                "invalid event type: {:?}",
                et
            ))),
        }
    }
}

#[derive(Debug)]
pub struct ParserV4 {
    // post header lengths of all events
    post_header_lengths: Vec<u8>,
    // whether the crc32 checksum is enabled
    // if enabled, will validate the tail 4-byte checksum of all events
    checksum: bool,
}

#[allow(dead_code)]
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
        let post_header_lengths =
            post_header_lengths_from_raw(fde.data.post_header_lengths.as_ref());
        let checksum = fde.data.checksum_flag == 1;
        ParserV4::new(post_header_lengths, checksum)
    }

    // this function will verify binlog version to be v4
    // and consume FDE to get post header lengths for all
    // following events
    pub fn from_binlog_file(input: &mut Bytes) -> Result<Self> {
        let binlog_version = BinlogVersion::read_from(input)?;
        if binlog_version != BinlogVersion::V4 {
            return Err(Error::InvalidBinlogFormat(format!(
                "unsupported binlog version: {:?}",
                binlog_version
            )));
        }
        let (pv4, _) = Self::from_fde_bytes(input)?;
        Ok(pv4)
    }

    /// create parser from bytes of format description event
    ///
    /// this function will additional crc32 code if checksum is enabled
    pub fn from_fde_bytes(input: &mut Bytes) -> Result<(Self, Option<u32>)> {
        let header = EventHeader::read_from(input)?;
        // raw data may contains 4 bytes checksum at end
        let mut raw_data = input.read_len(header.data_len() as usize)?;
        let data = FormatDescriptionData::read_from(&mut raw_data)?;
        let crc32 = if data.checksum_flag == 1 {
            if raw_data.remaining() < 4 {
                return Err(Error::BinlogEventError(
                    "FDE does not have 4-byte checksum but flag enabled".to_owned(),
                ));
            }
            Some(raw_data.read_le_u32()?)
        } else {
            None
        };
        Ok((
            Self::from_fde(&FormatDescriptionEvent { header, data }),
            crc32,
        ))
    }

    // parse the event starting from given offset
    // if validate_checksum is set to true, will
    // verify crc32 checksum if possible
    // for any non-supported event, returns None
    pub fn parse_event(&self, input: &mut Bytes, validate_checksum: bool) -> Result<Option<Event>> {
        if self.checksum && validate_checksum {
            // do not consume original input for checksum
            let header = EventHeader::read_from(&mut input.clone())?;
            let mut raw_data = (&mut input.clone()).read_len(header.event_len as usize)?;
            let mut checksum_data = raw_data.split_off(raw_data.remaining() - 4);
            let expected = checksum_data.read_le_u32()?;
            let actual = checksum_crc32(raw_data.as_ref());
            if expected != actual {
                return Err(Error::BinlogChecksumMismatch(expected, actual));
            }
        }

        let header = EventHeader::read_from(input)?;
        if header.type_code == LogEventType::HeartbeatLogEvent {
            println!("read data len = {}", header.data_len());
            println!("input bytes len = {}", input.remaining());
            println!("{:?}", input.bytes());
        }
        let mut raw_data = input.read_len(header.data_len() as usize)?;
        if self.checksum {
            // need to remove 4-byte crc32 code at end
            raw_data.truncate(raw_data.remaining() - 4);
        }
        let event = match LogEventType::from(header.type_code) {
            // UnknownEvent not supported
            LogEventType::StartEventV3 => Event::StartEventV3(RawEvent {
                header: header,
                data: StartData::read_from(&mut raw_data)?,
            }),
            LogEventType::QueryEvent => Event::QueryEvent(RawEvent {
                header: header,
                data: QueryData::read_from(&mut raw_data)?,
            }),
            LogEventType::StopEvent => Event::StopEvent(RawEvent {
                header: header,
                data: (),
            }),
            LogEventType::RotateEvent => Event::RotateEvent(RawEvent {
                header: header,
                data: RotateData::read_from(&mut raw_data)?,
            }),
            LogEventType::IntvarEvent => Event::IntvarEvent(RawEvent {
                header: header,
                data: IntvarData::read_from(&mut raw_data)?,
            }),
            LogEventType::LoadEvent => Event::LoadEvent(RawEvent {
                header: header,
                data: LoadData::read_from(&mut raw_data)?,
            }),
            // SlaveEvent not supported
            LogEventType::CreateFileEvent => Event::CreateFileEvent(RawEvent {
                header: header,
                data: CreateFileData::read_from(&mut raw_data)?,
            }),
            LogEventType::AppendBlockEvent => Event::AppendBlockEvent(RawEvent {
                header: header,
                data: AppendBlockData::read_from(&mut raw_data)?,
            }),
            LogEventType::ExecLoadEvent => Event::ExecLoadEvent(RawEvent {
                header: header,
                data: ExecLoadData::read_from(&mut raw_data)?,
            }),
            LogEventType::DeleteFileEvent => Event::DeleteFileEvent(RawEvent {
                header: header,
                data: DeleteFileData::read_from(&mut raw_data)?,
            }),
            LogEventType::NewLoadEvent => Event::NewLoadEvent(RawEvent {
                header: header,
                data: NewLoadData::read_from(&mut raw_data)?,
            }),
            LogEventType::RandEvent => Event::RandEvent(RawEvent {
                header: header,
                data: RandData::read_from(&mut raw_data)?,
            }),
            LogEventType::UserVarEvent => Event::UserVarEvent(RawEvent {
                header: header,
                data: UserVarData::read_from(&mut raw_data)?,
            }),
            LogEventType::FormatDescriptionEvent => Event::FormatDescriptionEvent(RawEvent {
                header: header,
                data: FormatDescriptionData::read_from(&mut raw_data)?,
            }),
            LogEventType::XidEvent => Event::XidEvent(RawEvent {
                header: header,
                data: XidData::read_from(&mut raw_data)?,
            }),
            LogEventType::BeginLoadQueryEvent => Event::BeginLoadQueryEvent(RawEvent {
                header: header,
                data: BeginLoadQueryData::read_from(&mut raw_data)?,
            }),
            LogEventType::ExecuteLoadQueryEvent => Event::ExecuteLoadQueryEvent(RawEvent {
                header: header,
                data: ExecuteLoadQueryData::read_from(&mut raw_data)?,
            }),
            LogEventType::TableMapEvent => Event::TableMapEvent(RawEvent {
                header: header,
                data: TableMapData::read_from(&mut raw_data)?,
            }),
            // WriteRowsEventV0 not supported
            // UpdateRowsEventV0 not supported
            // DeleteRowsEventV0 not supported
            LogEventType::WriteRowsEventV1 => Event::WriteRowsEventV1(RawEvent {
                header: header,
                data: WriteRowsDataV1::read_from(&mut raw_data)?,
            }),
            LogEventType::UpdateRowsEventV1 => Event::UpdateRowsEventV1(RawEvent {
                header: header,
                data: UpdateRowsDataV1::read_from(&mut raw_data)?,
            }),
            LogEventType::DeleteRowsEventV1 => Event::DeleteRowsEventV1(RawEvent {
                header: header,
                data: DeleteRowsDataV1::read_from(&mut raw_data)?,
            }),
            LogEventType::IncidentEvent => Event::IncidentEvent(RawEvent {
                header: header,
                data: IncidentData::read_from(&mut raw_data)?,
            }),
            LogEventType::HeartbeatLogEvent => Event::HeartbeatLogEvent(RawEvent {
                header: header,
                data: (),
            }),
            // IgnorableLogEvent not supported
            // RowsQueryLogEvent not supported
            LogEventType::WriteRowsEventV2 => Event::WriteRowsEventV2(RawEvent {
                header: header,
                data: WriteRowsDataV2::read_from(&mut raw_data)?,
            }),
            LogEventType::UpdateRowsEventV2 => Event::UpdateRowsEventV2(RawEvent {
                header: header,
                data: UpdateRowsDataV2::read_from(&mut raw_data)?,
            }),
            LogEventType::DeleteRowsEventV2 => Event::DeleteRowsEventV2(RawEvent {
                header: header,
                data: DeleteRowsDataV2::read_from(&mut raw_data)?,
            }),
            LogEventType::GtidLogEvent => Event::GtidLogEvent(RawEvent {
                header: header,
                data: GtidLogData::read_from(&mut raw_data)?,
            }),
            LogEventType::AnonymousGtidLogEvent => Event::AnonymousGtidLogEvent(RawEvent {
                header: header,
                data: AnonymousGtidLogData::read_from(&mut raw_data)?,
            }),
            LogEventType::PreviousGtidsLogEvent => Event::PreviousGtidsLogEvent(RawEvent {
                header: header,
                data: PreviousGtidsLogData::read_from(&mut raw_data)?,
            }),
            // TransactionContextEvent not supported
            // ViewChangeEvent not supported
            // XaPrepareLogEvent not supported
            _ => return Ok(None),
        };
        Ok(Some(event))
    }

    pub fn skip_event(&self, input: &mut Bytes) -> Result<()> {
        let header = EventHeader::read_from(input)?;
        input.read_len(header.data_len() as usize)?;
        Ok(())
    }

    pub fn checksum_event(&self, input: &Bytes) -> Result<()> {
        if !self.checksum {
            return Err(Error::InvalidBinlogFormat(
                "binlog checksum not enabled".to_owned(),
            ));
        }
        let mut input = input.clone();
        let header = EventHeader::read_from(&mut input.clone())?;
        let mut raw_data = (&mut input).read_len(header.event_len as usize)?;
        let mut checksum_data = raw_data.split_off(raw_data.remaining() - 4);
        let expected = checksum_data.read_le_u32()?;
        let actual = checksum_crc32(raw_data.as_ref());
        if expected != actual {
            return Err(Error::BinlogChecksumMismatch(expected, actual));
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use std::convert::TryInto;

    const BINLOG_5_5_50: &[u8] = include_bytes!("../../data/mysql-bin.5.5.50.StartEvent");
    const BINLOG_5_7_30: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.StartEvent");
    const BINLOG_NO_CHECKSUM: &[u8] =
        include_bytes!("../../data/mysql-bin.5.7.30.StartEventNoChecksum");
    const BINLOG_QUERY_EVENT: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.QueryEvent");
    const BINLOG_ROTATE_EVENT: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.RotateEvent");
    const BINLOG_ROWS_EVENT_V1: &[u8] = include_bytes!("../../data/mysql-bin.5.5.50.RowsEventV1");
    const BINLOG_ROWS_EVENT_V2: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.RowsEventV2");
    const BINLOG_BEGIN_LOAD_QUERY_EVENT: &[u8] =
        include_bytes!("../../data/mysql-bin.5.7.30.BeginLoadQueryEvent");
    const BINLOG_RAND_EVENT: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.RandEvent");
    const BINLOG_USER_VAR_EVENT: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.UserVarEvent");
    const BINLOG_GTID_EVENT: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.GtidEvent");
    const BINLOG_TIME_DATA: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.Time");
    const BINLOG_YEAR_DATA: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.Year");
    const BINLOG_TIMESTAMP_DATA: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.Timestamp");
    const BINLOG_ENUM_DATA: &[u8] = include_bytes!("../../data/mysql-bin.5.7.30.Enum");

    #[test]
    fn test_binlog_version() -> Result<()> {
        let mut input = BINLOG_5_7_30;
        let input = &mut input.to_bytes();
        let bv = BinlogVersion::read_from(input)?;
        assert_eq!(BinlogVersion::V4, bv);

        let mut input = &b"\xfebin"[..];
        let input = &mut input.to_bytes();
        let fail = BinlogVersion::read_from(input);
        dbg!(fail.unwrap_err());
        Ok(())
    }

    #[test]
    fn test_binlog_no_checksum() -> Result<()> {
        let mut input = BINLOG_NO_CHECKSUM;
        let input = &mut input.to_bytes();
        BinlogVersion::read_from(input)?;
        EventHeader::read_from(input)?;
        let fdd = FormatDescriptionData::read_from(input)?;
        println!("{:#?}", fdd);
        Ok(())
    }

    #[test]
    fn test_format_description_event_5_5() -> Result<()> {
        let mut input = BINLOG_5_5_50;
        let input = &mut input.to_bytes();
        BinlogVersion::read_from(input)?;
        let header = EventHeader::read_from(input)?;
        let fdd = FormatDescriptionData::read_from(input)?;
        assert_eq!(
            LogEventType::FormatDescriptionEvent,
            LogEventType::from(header.type_code)
        );
        println!("post header lengths: {}", fdd.post_header_lengths.len());
        for i in 0..fdd.post_header_lengths.len() {
            println!(
                "{:?}: {}",
                LogEventType::try_from(i as u8 + 1)?,
                fdd.post_header_lengths[i]
            );
        }
        // reference: https://dev.mysql.com/doc/internals/en/format-description-event.html
        // binlog: mysql 5.5.50
        assert_eq!(56, post_header_length(&fdd, LogEventType::StartEventV3));
        assert_eq!(13, post_header_length(&fdd, LogEventType::QueryEvent));
        assert_eq!(0, post_header_length(&fdd, LogEventType::StopEvent));
        assert_eq!(8, post_header_length(&fdd, LogEventType::RotateEvent));
        assert_eq!(0, post_header_length(&fdd, LogEventType::IntvarEvent));
        assert_eq!(18, post_header_length(&fdd, LogEventType::LoadEvent));
        assert_eq!(0, post_header_length(&fdd, LogEventType::SlaveEvent));
        assert_eq!(4, post_header_length(&fdd, LogEventType::CreateFileEvent));
        assert_eq!(4, post_header_length(&fdd, LogEventType::AppendBlockEvent));
        assert_eq!(4, post_header_length(&fdd, LogEventType::ExecLoadEvent));
        assert_eq!(4, post_header_length(&fdd, LogEventType::DeleteFileEvent));
        assert_eq!(18, post_header_length(&fdd, LogEventType::NewLoadEvent));
        assert_eq!(0, post_header_length(&fdd, LogEventType::RandEvent));
        assert_eq!(0, post_header_length(&fdd, LogEventType::UserVarEvent));
        assert_eq!(
            84,
            post_header_length(&fdd, LogEventType::FormatDescriptionEvent)
        );
        assert_eq!(0, post_header_length(&fdd, LogEventType::XidEvent));
        assert_eq!(
            4,
            post_header_length(&fdd, LogEventType::BeginLoadQueryEvent)
        );
        assert_eq!(
            26,
            post_header_length(&fdd, LogEventType::ExecuteLoadQueryEvent)
        );
        assert_eq!(8, post_header_length(&fdd, LogEventType::TableMapEvent));
        assert_eq!(0, post_header_length(&fdd, LogEventType::DeleteRowsEventV0));
        assert_eq!(0, post_header_length(&fdd, LogEventType::UpdateRowsEventV0));
        assert_eq!(0, post_header_length(&fdd, LogEventType::WriteRowsEventV0));
        assert_eq!(8, post_header_length(&fdd, LogEventType::DeleteRowsEventV1));
        assert_eq!(8, post_header_length(&fdd, LogEventType::UpdateRowsEventV1));
        assert_eq!(8, post_header_length(&fdd, LogEventType::WriteRowsEventV1));
        assert_eq!(2, post_header_length(&fdd, LogEventType::IncidentEvent));
        assert_eq!(0, post_header_length(&fdd, LogEventType::HeartbeatLogEvent));
        // 5.5 does not have v2 row events
        // assert_eq!(10, post_header_length(&event, LogEventType::WriteRowsEventV2));
        // assert_eq!(10, post_header_length(&event, LogEventType::DeleteRowsEventV2));
        // assert_eq!(10, post_header_length(&event, LogEventType::UpdateRowsEventV2));

        println!("{:#?}", fdd);
        Ok(())
    }

    #[test]
    fn test_format_description_event_5_7() -> Result<()> {
        let mut input = BINLOG_5_7_30;
        let input = &mut input.to_bytes();
        BinlogVersion::read_from(input)?;
        let header = EventHeader::read_from(input)?;
        let fdd = FormatDescriptionData::read_from(input)?;
        assert_eq!(
            LogEventType::FormatDescriptionEvent,
            LogEventType::from(header.type_code)
        );
        println!("post header lengths: {}", fdd.post_header_lengths.len());
        for i in 1..fdd.post_header_lengths.len() {
            println!(
                "{:?}: {}",
                LogEventType::try_from(i as u8 + 1)?,
                fdd.post_header_lengths[i]
            );
        }
        // below is the event post header lengths of mysql 5.7.30
        // 1
        assert_eq!(56, post_header_length(&fdd, LogEventType::StartEventV3));
        // 2
        assert_eq!(13, post_header_length(&fdd, LogEventType::QueryEvent));
        // 3
        assert_eq!(0, post_header_length(&fdd, LogEventType::StopEvent));
        // 4
        assert_eq!(8, post_header_length(&fdd, LogEventType::RotateEvent));
        // 5
        assert_eq!(0, post_header_length(&fdd, LogEventType::IntvarEvent));
        // 6
        assert_eq!(18, post_header_length(&fdd, LogEventType::LoadEvent));
        // 7
        assert_eq!(0, post_header_length(&fdd, LogEventType::SlaveEvent));
        // 8
        assert_eq!(4, post_header_length(&fdd, LogEventType::CreateFileEvent));
        // 9
        assert_eq!(4, post_header_length(&fdd, LogEventType::AppendBlockEvent));
        // 10
        assert_eq!(4, post_header_length(&fdd, LogEventType::ExecLoadEvent));
        // 11
        assert_eq!(4, post_header_length(&fdd, LogEventType::DeleteFileEvent));
        // 12
        assert_eq!(18, post_header_length(&fdd, LogEventType::NewLoadEvent));
        // 13
        assert_eq!(0, post_header_length(&fdd, LogEventType::RandEvent));
        // 14
        assert_eq!(0, post_header_length(&fdd, LogEventType::UserVarEvent));
        // 15
        // length of StartEventV3 + 1 + number of LogEventType = 56 + 1 + 38
        // NOTE: FDE may contains additional 1-byte of checksum flag at end,
        //       followed by a 4-byte checksum value
        assert_eq!(
            95,
            post_header_length(&fdd, LogEventType::FormatDescriptionEvent)
        );
        // 16
        assert_eq!(0, post_header_length(&fdd, LogEventType::XidEvent));
        // 17
        assert_eq!(
            4,
            post_header_length(&fdd, LogEventType::BeginLoadQueryEvent)
        );
        // 18
        assert_eq!(
            26,
            post_header_length(&fdd, LogEventType::ExecuteLoadQueryEvent)
        );
        // 19
        assert_eq!(8, post_header_length(&fdd, LogEventType::TableMapEvent));
        // 20
        assert_eq!(0, post_header_length(&fdd, LogEventType::WriteRowsEventV0));
        // 21
        assert_eq!(0, post_header_length(&fdd, LogEventType::UpdateRowsEventV0));
        // 22
        assert_eq!(0, post_header_length(&fdd, LogEventType::DeleteRowsEventV0));
        // 23
        assert_eq!(8, post_header_length(&fdd, LogEventType::WriteRowsEventV1));
        // 24
        assert_eq!(8, post_header_length(&fdd, LogEventType::UpdateRowsEventV1));
        // 25
        assert_eq!(8, post_header_length(&fdd, LogEventType::DeleteRowsEventV1));
        // 26
        assert_eq!(2, post_header_length(&fdd, LogEventType::IncidentEvent));
        // 27
        assert_eq!(0, post_header_length(&fdd, LogEventType::HeartbeatLogEvent));
        // 28
        assert_eq!(0, post_header_length(&fdd, LogEventType::IgnorableLogEvent));
        // 29
        assert_eq!(0, post_header_length(&fdd, LogEventType::RowsQueryLogEvent));
        // 30
        assert_eq!(10, post_header_length(&fdd, LogEventType::WriteRowsEventV2));
        // 31
        assert_eq!(
            10,
            post_header_length(&fdd, LogEventType::UpdateRowsEventV2)
        );
        // 32
        assert_eq!(
            10,
            post_header_length(&fdd, LogEventType::DeleteRowsEventV2)
        );
        // 33
        assert_eq!(42, post_header_length(&fdd, LogEventType::GtidLogEvent));
        // 34
        assert_eq!(
            42,
            post_header_length(&fdd, LogEventType::AnonymousGtidLogEvent)
        );
        // 35
        assert_eq!(
            0,
            post_header_length(&fdd, LogEventType::PreviousGtidsLogEvent)
        );
        // 36
        assert_eq!(
            18,
            post_header_length(&fdd, LogEventType::TransactionContextEvent)
        );
        // 37
        assert_eq!(52, post_header_length(&fdd, LogEventType::ViewChangeEvent));
        // 38
        assert_eq!(0, post_header_length(&fdd, LogEventType::XaPrepareLogEvent));

        println!("{:#?}", fdd);
        Ok(())
    }

    // binlog-query-event contains 4 events:
    // FDE, PreviousGtid, AnonymousGtid, Query
    #[test]
    fn test_query_event() -> Result<()> {
        use crate::binlog::query::{Flags2Code, QueryStatusVar, QueryStatusVars, SqlModeCode};
        let mut input = BINLOG_QUERY_EVENT;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..2 {
            pv4.skip_event(input)?;
        }
        // the 3rd event is QueryEvent
        let qe = pv4.parse_event(input, true)?;
        let qe: QueryEvent = qe.unwrap().try_into()?;
        println!("{:#?}", qe);
        dbg!(std::str::from_utf8(&qe.data.schema)?);
        dbg!(std::str::from_utf8(&qe.data.query)?);
        let vars = QueryStatusVars::read_from(&mut qe.data.status_vars.clone())?;
        println!("{:#?}", vars);
        vars.iter().for_each(|v| match v {
            QueryStatusVar::Flags2Code(n) => {
                let f2c = Flags2Code::from_bits(*n).unwrap();
                dbg!(f2c);
            }
            QueryStatusVar::SqlModeCode(n) => {
                let smc = SqlModeCode::from_bits(*n).unwrap();
                dbg!(smc);
            }
            _ => (),
        });
        Ok(())
    }

    // 3 events:
    // FDE, PreviousGtid, Stop
    #[test]
    fn test_stop_event() -> Result<()> {
        let mut input = BINLOG_5_7_30;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        pv4.skip_event(input)?;
        // third event is StopEvent
        let se = pv4.parse_event(input, true)?.unwrap();
        println!("{:#?}", se);
        Ok(())
    }

    // 3 events:
    // FDE, PreviousGtid, Rotate
    #[test]
    fn test_rotate_event() -> Result<()> {
        let mut input = BINLOG_ROTATE_EVENT;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        pv4.skip_event(input)?;
        // 2nd event is RotateEvent
        let re = pv4.parse_event(input, true)?;
        let re: RotateEvent = re.unwrap().try_into()?;
        println!("{:#?}", re);
        dbg!(String::from_utf8_lossy(
            re.data.next_binlog_filename.as_ref()
        ));
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_intvar_event() -> Result<()> {
        let mut input = BINLOG_RAND_EVENT;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..3 {
            pv4.skip_event(input)?;
        }
        // 4th event
        let ive = pv4.parse_event(input, true)?;
        let ive: IntvarEvent = ive.unwrap().try_into()?;
        println!("{:#?}", ive);
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_load_event_unimplemented() -> Result<()> {
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_create_file_event_unimplemented() -> Result<()> {
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_exec_load_event_unimplemented() -> Result<()> {
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_delete_file_event_unimplemented() -> Result<()> {
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_new_load_event_unimplemented() -> Result<()> {
        Ok(())
    }

    // FDE, PreviousGtids, AnonymousGtid, Query,
    // BeginLoadQueryEvent, ExecuteLoadQueryEvent,
    // Xid
    #[test]
    fn test_begin_load_query_event() -> Result<()> {
        let mut input = BINLOG_BEGIN_LOAD_QUERY_EVENT;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..3 {
            pv4.skip_event(input)?;
        }
        // 4th event
        let blqe = pv4.parse_event(input, true)?;
        let blqe: BeginLoadQueryEvent = blqe.unwrap().try_into()?;
        println!("{:#?}", blqe);
        dbg!(String::from_utf8_lossy(blqe.data.block_data.as_ref()));
        Ok(())
    }

    #[test]
    fn test_execute_load_query_event() -> Result<()> {
        let mut input = BINLOG_BEGIN_LOAD_QUERY_EVENT;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..4 {
            pv4.skip_event(input)?;
        }
        // 5th event
        let elqe = pv4.parse_event(input, true)?;
        let elqe: ExecuteLoadQueryEvent = elqe.unwrap().try_into()?;
        println!("{:#?}", elqe);
        Ok(())
    }

    #[test]
    fn test_rand_event() -> Result<()> {
        let mut input = BINLOG_RAND_EVENT;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..4 {
            pv4.skip_event(input)?;
        }
        // 5th event
        let re = pv4.parse_event(input, true)?;
        println!("{:#?}", re);
        Ok(())
    }

    #[test]
    fn test_xid_event() -> Result<()> {
        let mut input = BINLOG_ROWS_EVENT_V2;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..9 {
            pv4.skip_event(input)?;
        }
        // 10th is Xid Event
        let xe = pv4.parse_event(input, true)?;
        let xe: XidEvent = xe.unwrap().try_into()?;
        println!("{:#?}", xe);
        Ok(())
    }

    #[test]
    fn test_user_var_event() -> Result<()> {
        use crate::binlog::user_var::UserVarValue;

        let mut input = BINLOG_USER_VAR_EVENT;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        while input.has_remaining() {
            // not consume the real bytes
            let event_type = LogEventType::read_from(&mut input.clone())?;
            match event_type {
                LogEventType::UserVarEvent => {
                    let uve = pv4.parse_event(input, true)?;
                    let mut uve: UserVarEvent = uve.unwrap().try_into()?;
                    println!("{:#?}", uve);
                    let uvv = UserVarValue::read_from(&mut uve.data.value)?;
                    println!("{:#?}", uvv);
                }
                _ => pv4.skip_event(input)?,
            }
        }
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_incident_event_unimplemented() -> Result<()> {
        Ok(())
    }

    // BINLOG_ROWS_EVENT_V2 contains below events in order:
    // FDE,
    // PreviousGtid, AnonymousGtid,
    // Query(BEGIN),
    // TableMap, WriteRows,
    // TableMap, UpdateRows,
    // TableMap, DeleteRows,
    // Xid(COMMIT)
    #[test]
    fn test_table_map_event() -> Result<()> {
        let mut input = BINLOG_ROWS_EVENT_V2;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..3 {
            pv4.skip_event(input)?;
        }
        // 4th event
        let tme = pv4.parse_event(input, true)?;
        let tme: TableMapEvent = tme.unwrap().try_into()?;
        println!("{:#?}", tme);
        let tm = tme.data.into_table_map().unwrap();
        println!("{:#?}", tm);
        Ok(())
    }

    #[test]
    fn test_delete_rows_event_v1() -> Result<()> {
        let mut input = BINLOG_ROWS_EVENT_V1;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..6 {
            pv4.skip_event(input)?;
        }
        // 7th event
        let dre = pv4.parse_event(input, true)?;
        let dre: DeleteRowsEventV1 = dre.unwrap().try_into()?;
        println!("{:#?}", dre);
        Ok(())
    }

    #[test]
    fn test_update_rows_event_v1() -> Result<()> {
        let mut input = BINLOG_ROWS_EVENT_V1;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..4 {
            pv4.skip_event(input)?;
        }
        // 5th event
        let ure = pv4.parse_event(input, true)?;
        let ure: UpdateRowsEventV1 = ure.unwrap().try_into()?;
        println!("{:#?}", ure);
        Ok(())
    }

    #[test]
    fn test_write_rows_event_v1() -> Result<()> {
        let mut input = BINLOG_ROWS_EVENT_V1;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..2 {
            pv4.skip_event(input)?;
        }
        // 3th event
        let wre = pv4.parse_event(input, true)?;
        let wre: WriteRowsEventV1 = wre.unwrap().try_into()?;
        println!("{:#?}", wre);
        Ok(())
    }

    #[test]
    fn test_delete_rows_event_v2() -> Result<()> {
        let mut input = BINLOG_ROWS_EVENT_V2;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..7 {
            pv4.skip_event(input)?;
        }
        // 8th is TableMapEvent
        let tme = pv4.parse_event(input, true)?;
        let tme: TableMapEvent = tme.unwrap().try_into()?;
        let tm = tme.data.table_map().unwrap();
        // 9th event is DeleteRowsEventV2
        let dre = pv4.parse_event(input, true)?;
        let dre: DeleteRowsEventV2 = dre.unwrap().try_into()?;
        println!("{:#?}", dre);
        let delete_rows = dre.data.rows(&tm.col_metas).unwrap();
        dbg!(delete_rows);
        Ok(())
    }

    #[test]
    fn test_update_rows_event_v2() -> Result<()> {
        let mut input = BINLOG_ROWS_EVENT_V2;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..5 {
            pv4.skip_event(input)?;
        }
        // 6th is TableMapEvent
        let tme = pv4.parse_event(input, true)?;
        let tme: TableMapEvent = tme.unwrap().try_into()?;
        let tm = tme.data.table_map().unwrap();
        // 7th event is UpdateRowsEventV2
        let ure = pv4.parse_event(input, true)?;
        let ure: UpdateRowsEventV2 = ure.unwrap().try_into()?;
        println!("{:#?}", ure);
        let update_rows = ure.data.rows(&tm.col_metas).unwrap();
        dbg!(update_rows);
        Ok(())
    }

    #[test]
    fn test_write_rows_event_v2() -> Result<()> {
        let mut input = BINLOG_ROWS_EVENT_V2;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        for _ in 0..3 {
            pv4.skip_event(input)?;
        }
        // 4th is TableMapEvent
        let tme = pv4.parse_event(input, true)?;
        let tme: TableMapEvent = tme.unwrap().try_into()?;
        let tm = tme.data.table_map().unwrap();
        // 5th event is WriteRowsEventV2
        let wre = pv4.parse_event(input, true)?;
        let wre: WriteRowsEventV2 = wre.unwrap().try_into()?;
        println!("{:#?}", wre);
        let write_rows = wre.data.rows(&tm.col_metas).unwrap();
        dbg!(write_rows);
        Ok(())
    }

    #[test]
    fn test_gtid_log_event() -> Result<()> {
        let mut input = BINLOG_GTID_EVENT;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        pv4.skip_event(input)?;
        // 2nd event
        let gle = pv4.parse_event(input, true)?;
        let gle: GtidLogEvent = gle.unwrap().try_into()?;
        println!("{:#?}", gle);
        Ok(())
    }

    #[test]
    fn test_anonymous_gtid_log_event() -> Result<()> {
        let mut input = BINLOG_RAND_EVENT;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        pv4.skip_event(input)?;
        // 2nd event
        let agle = pv4.parse_event(input, true)?;
        let agle: AnonymousGtidLogEvent = agle.unwrap().try_into()?;
        println!("{:#?}", agle);
        Ok(())
    }

    #[test]
    fn test_previous_gtids_log_event() -> Result<()> {
        let mut input = BINLOG_GTID_EVENT;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        // 1st event
        let pgle = pv4.parse_event(input, true)?;
        let pgle: PreviousGtidsLogEvent = pgle.unwrap().try_into()?;
        println!("{:#?}", pgle);
        dbg!(pgle.data.gtid_set().unwrap());
        Ok(())
    }

    #[test]
    fn test_checksum_all_files() -> Result<()> {
        let files = vec![
            BINLOG_5_7_30,
            BINLOG_QUERY_EVENT,
            BINLOG_ROTATE_EVENT,
            // BINLOG_ROWS_EVENT_V1,
            BINLOG_ROWS_EVENT_V2,
            BINLOG_BEGIN_LOAD_QUERY_EVENT,
            BINLOG_RAND_EVENT,
        ];
        for mut f in files.into_iter().map(|mut f| f.to_bytes()) {
            let pv4 = ParserV4::from_binlog_file(&mut f)?;
            while f.has_remaining() {
                pv4.checksum_event(&f)?;
                pv4.skip_event(&mut f)?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_binlog_time_data() -> Result<()> {
        let mut input = BINLOG_TIME_DATA;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        // 5th event is insert
        for _ in 0..3 {
            pv4.skip_event(input)?;
        }
        let tme = pv4.parse_event(input, false)?;
        let tme: TableMapEvent = tme.unwrap().try_into()?;
        let tm = tme.data.into_table_map()?;
        dbg!(&tm);
        let wre = pv4.parse_event(input, false)?;
        let wre: WriteRowsEventV2 = wre.unwrap().try_into()?;
        dbg!(&wre);
        let rows = wre.data.into_rows(&tm.col_metas)?;
        dbg!(rows);
        Ok(())
    }

    #[test]
    fn test_binlog_year_data() -> Result<()> {
        let mut input = BINLOG_YEAR_DATA;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        // 5th event is insert
        for _ in 0..3 {
            pv4.skip_event(input)?;
        }
        let tme = pv4.parse_event(input, false)?;
        let tme: TableMapEvent = tme.unwrap().try_into()?;
        let tm = tme.data.into_table_map()?;
        dbg!(&tm);
        let wre = pv4.parse_event(input, false)?;
        let wre: WriteRowsEventV2 = wre.unwrap().try_into()?;
        dbg!(&wre);
        let rows = wre.data.into_rows(&tm.col_metas)?;
        dbg!(rows);
        Ok(())
    }

    #[test]
    fn test_binlog_timestamp_data() -> Result<()> {
        let mut input = BINLOG_TIMESTAMP_DATA;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        // 5th event is insert
        for _ in 0..3 {
            pv4.skip_event(input)?;
        }
        let tme = pv4.parse_event(input, false)?;
        let tme: TableMapEvent = tme.unwrap().try_into()?;
        let tm = tme.data.into_table_map()?;
        dbg!(&tm);
        let wre = pv4.parse_event(input, false)?;
        let wre: WriteRowsEventV2 = wre.unwrap().try_into()?;
        dbg!(&wre);
        let rows = wre.data.into_rows(&tm.col_metas)?;
        dbg!(rows);
        Ok(())
    }

    #[test]
    fn test_binlog_enum_data() -> Result<()> {
        let mut input = BINLOG_ENUM_DATA;
        let input = &mut input.to_bytes();
        let pv4 = ParserV4::from_binlog_file(input)?;
        // 5th event is insert
        for _ in 0..3 {
            pv4.skip_event(input)?;
        }
        let tme = pv4.parse_event(input, false)?;
        let tme: TableMapEvent = tme.unwrap().try_into()?;
        let tm = tme.data.into_table_map()?;
        dbg!(&tm);
        let wre = pv4.parse_event(input, false)?;
        let wre: WriteRowsEventV2 = wre.unwrap().try_into()?;
        dbg!(&wre);
        let rows = wre.data.into_rows(&tm.col_metas)?;
        dbg!(rows);
        Ok(())
    }

    fn post_header_length(fdd: &FormatDescriptionData, event_type: LogEventType) -> u8 {
        let idx = u8::from(event_type) as usize - 1;
        fdd.post_header_lengths[idx]
    }
}
