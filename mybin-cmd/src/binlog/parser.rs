use super::*;
use super::header::EventHeader;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::number::ReadNumber;
use crate::util::checksum_crc32;
use bytes_parser::ReadFrom;
// use bytes_parser::error::{Result, Error};
use crate::error::{Result, Error};

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
    fn read_from(&self, offset: usize) -> bytes_parser::error::Result<(usize, BinlogVersion)> {
        let (offset, magic) = self.take_len(offset, 4)?;
        if magic != b"\xfebin" {
            return Err(bytes_parser::error::Error::ConstraintError(format!("invalid magic number: {:?}", magic)));
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
            et => Err(bytes_parser::error::Error::ConstraintError(format!("invalid event type: {:?}", et))),
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
        let post_header_lengths = post_header_lengths_from_raw(fde.data.post_header_lengths);
        let checksum = fde.data.checksum_flag == 1;
        ParserV4::new(post_header_lengths, checksum)
    }

    // this function will verify binlog version to be v4
    // and consume FDE to get post header lengths for all
    // following events
    pub fn from_binlog_file(input: &[u8]) -> Result<(usize, Self)> {
        let (offset, binlog_version): (_, BinlogVersion) = input.read_from(0)?;
        if binlog_version != BinlogVersion::V4 {
            return Err(Error::InvalidBinlogFormat(
                format!("unsupported binlog version: {:?}", binlog_version)));
        }
        let (offset, fde) = input.read_from(offset)?;
        Ok((offset, Self::from_fde(&fde)))
    }

    pub fn parse_event<'a>(&self, input: &'a [u8], offset: usize, validate_checksum: bool, ignore_unsupported: bool) -> Result<(usize, Option<Event<'a>>)> {
        let start = offset;
        let (_, header): (_, EventHeader) = input.read_from(offset)?;
        let (offset, raw_data) = input.take_len(offset, header.data_len() as usize)?;
        let event = match LogEventType::from(header.type_code) {
            // UnknownEvent not supported
            LogEventType::StartEventV3 => Event::StartEventV3(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::QueryEvent => Event::QueryEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::StopEvent => Event::StopEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::RotateEvent => Event::RotateEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::IntvarEvent => Event::IntvarEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::LoadEvent => Event::LoadEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            // SlaveEvent not supported
            LogEventType::CreateFileEvent => Event::CreateFileEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::AppendBlockEvent => Event::AppendBlockEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::ExecLoadEvent => Event::ExecLoadEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::DeleteFileEvent => Event::DeleteFileEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::NewLoadEvent => Event::NewLoadEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::RandEvent => Event::RandEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::UserVarEvent => Event::UserVarEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::FormatDescriptionEvent => Event::FormatDescriptionEvent(raw_data.read_from(0)?.1),
            LogEventType::XidEvent => Event::XidEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::BeginLoadQueryEvent => Event::BeginLoadQueryEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::ExecuteLoadQueryEvent => Event::ExecuteLoadQueryEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::TableMapEvent => Event::TableMapEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            // WriteRowsEventV0 not supported
            // UpdateRowsEventV0 not supported
            // DeleteRowsEventV0 not supported
            LogEventType::WriteRowsEventV1 => Event::WriteRowsEventV1(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::UpdateRowsEventV1 => Event::UpdateRowsEventV1(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::DeleteRowsEventV1 => Event::DeleteRowsEventV1(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::IncidentEvent => Event::IncidentEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::HeartbeatLogEvent => Event::HeartbeatLogEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            // IgnorableLogEvent not supported
            // RowsQueryLogEvent not supported
            LogEventType::WriteRowsEventV2 => Event::WriteRowsEventV2(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::UpdateRowsEventV2 => Event::UpdateRowsEventV2(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::DeleteRowsEventV2 => Event::DeleteRowsEventV2(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::GtidLogEvent => Event::GtidLogEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::AnonymousGtidLogEvent => Event::AnonymousGtidLogEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            LogEventType::PreviousGtidsLogEvent => Event::PreviousGtidsLogEvent(raw_data.read_with_ctx(0, self.checksum)?.1),
            // TransactionContextEvent not supported
            // ViewChangeEvent not supported
            // XaPrepareLogEvent not supported
            et => if ignore_unsupported {
                return Ok((offset, None));
            } else {
                return Err(Error::UnsupportedBinlogEvent(format!("unspported binlog event {:?}", et)));
            }
        };
        if self.checksum && validate_checksum {
            let expected = event.crc32();
            let actual = checksum_crc32(&input[start..offset - 4]);
            if expected != actual {
                return Err(Error::BinlogChecksumMismatch(expected, actual));
            }
        }
        Ok((offset, Some(event)))
    }

    pub fn skip_event(&self, input: &[u8], offset: usize) -> Result<usize> {
        let (offset, header): (_, EventHeader) = input.read_from(offset)?;
        let (offset, _) = input.take_len(offset, header.data_len() as usize)?;
        Ok(offset)
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
    use bytes_parser::error::Result as PResult;

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

    #[test]
    fn test_binlog_version() -> Result<()> {
        let (_, bv): (_, BinlogVersion) = BINLOG_5_7_30.read_from(0)?;
        assert_eq!(BinlogVersion::V4, bv);
        
        let fail: PResult<(_, BinlogVersion)> = b"\xfebin".read_from(0);
        assert!(fail.is_err());
        dbg!(fail);
        Ok(())
    }

    #[test]
    fn test_binlog_no_checksum() -> Result<()> {
        let input = BINLOG_NO_CHECKSUM;
        let (offset, _): (_, BinlogVersion) = input.read_from(0)?;
        let (_, fde): (_, FormatDescriptionEvent) = input.read_from(offset)?;
        println!("{:#?}", fde);
        Ok(())
    }

    #[test]
    fn test_format_description_event_5_5() -> Result<()> {
        let input = BINLOG_5_5_50;
        let (offset, _): (_, BinlogVersion) = input.read_from(0)?;
        let (_, event): (_, FormatDescriptionEvent) = input.read_from(offset)?;
        assert_eq!(
            LogEventType::FormatDescriptionEvent,
            LogEventType::from(event.header.type_code)
        );
        println!(
            "post header lengths: {}",
            event.data.post_header_lengths.len()
        );
        for i in 0..event.data.post_header_lengths.len() {
            println!(
                "{:?}: {}",
                LogEventType::from(i as u8 + 1),
                event.data.post_header_lengths[i]
            );
        }
        // reference: https://dev.mysql.com/doc/internals/en/format-description-event.html
        // binlog: mysql 5.5.50
        assert_eq!(56, post_header_length(&event, LogEventType::StartEventV3));
        assert_eq!(13, post_header_length(&event, LogEventType::QueryEvent));
        assert_eq!(0, post_header_length(&event, LogEventType::StopEvent));
        assert_eq!(8, post_header_length(&event, LogEventType::RotateEvent));
        assert_eq!(0, post_header_length(&event, LogEventType::IntvarEvent));
        assert_eq!(18, post_header_length(&event, LogEventType::LoadEvent));
        assert_eq!(0, post_header_length(&event, LogEventType::SlaveEvent));
        assert_eq!(4, post_header_length(&event, LogEventType::CreateFileEvent));
        assert_eq!(
            4,
            post_header_length(&event, LogEventType::AppendBlockEvent)
        );
        assert_eq!(4, post_header_length(&event, LogEventType::ExecLoadEvent));
        assert_eq!(4, post_header_length(&event, LogEventType::DeleteFileEvent));
        assert_eq!(18, post_header_length(&event, LogEventType::NewLoadEvent));
        assert_eq!(0, post_header_length(&event, LogEventType::RandEvent));
        assert_eq!(0, post_header_length(&event, LogEventType::UserVarEvent));
        assert_eq!(
            84,
            post_header_length(&event, LogEventType::FormatDescriptionEvent)
        );
        assert_eq!(0, post_header_length(&event, LogEventType::XidEvent));
        assert_eq!(
            4,
            post_header_length(&event, LogEventType::BeginLoadQueryEvent)
        );
        assert_eq!(
            26,
            post_header_length(&event, LogEventType::ExecuteLoadQueryEvent)
        );
        assert_eq!(8, post_header_length(&event, LogEventType::TableMapEvent));
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::DeleteRowsEventV0)
        );
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::UpdateRowsEventV0)
        );
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::WriteRowsEventV0)
        );
        assert_eq!(
            8,
            post_header_length(&event, LogEventType::DeleteRowsEventV1)
        );
        assert_eq!(
            8,
            post_header_length(&event, LogEventType::UpdateRowsEventV1)
        );
        assert_eq!(
            8,
            post_header_length(&event, LogEventType::WriteRowsEventV1)
        );
        assert_eq!(2, post_header_length(&event, LogEventType::IncidentEvent));
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::HeartbeatLogEvent)
        );
        // 5.5 does not have v2 row events
        // assert_eq!(10, post_header_length(&event, LogEventType::WriteRowsEventV2));
        // assert_eq!(10, post_header_length(&event, LogEventType::DeleteRowsEventV2));
        // assert_eq!(10, post_header_length(&event, LogEventType::UpdateRowsEventV2));

        println!("{:#?}", event);
        Ok(())
    }

    #[test]
    fn test_format_description_event_5_7() -> Result<()> {
        let input = BINLOG_5_7_30;
        let (offset, _): (_, BinlogVersion) = input.read_from(0)?;
        let (_, event): (_, FormatDescriptionEvent) = input.read_from(offset)?;
        assert_eq!(
            LogEventType::FormatDescriptionEvent,
            LogEventType::from(event.header.type_code)
        );
        println!(
            "post header lengths: {}",
            event.data.post_header_lengths.len()
        );
        for i in 1..event.data.post_header_lengths.len() {
            println!(
                "{:?}: {}",
                LogEventType::from(i as u8 + 1),
                event.data.post_header_lengths[i]
            );
        }
        // below is the event post header lengths of mysql 5.7.30
        // 1
        assert_eq!(56, post_header_length(&event, LogEventType::StartEventV3));
        // 2
        assert_eq!(13, post_header_length(&event, LogEventType::QueryEvent));
        // 3
        assert_eq!(0, post_header_length(&event, LogEventType::StopEvent));
        // 4
        assert_eq!(8, post_header_length(&event, LogEventType::RotateEvent));
        // 5
        assert_eq!(0, post_header_length(&event, LogEventType::IntvarEvent));
        // 6
        assert_eq!(18, post_header_length(&event, LogEventType::LoadEvent));
        // 7
        assert_eq!(0, post_header_length(&event, LogEventType::SlaveEvent));
        // 8
        assert_eq!(4, post_header_length(&event, LogEventType::CreateFileEvent));
        // 9
        assert_eq!(
            4,
            post_header_length(&event, LogEventType::AppendBlockEvent)
        );
        // 10
        assert_eq!(4, post_header_length(&event, LogEventType::ExecLoadEvent));
        // 11
        assert_eq!(4, post_header_length(&event, LogEventType::DeleteFileEvent));
        // 12
        assert_eq!(18, post_header_length(&event, LogEventType::NewLoadEvent));
        // 13
        assert_eq!(0, post_header_length(&event, LogEventType::RandEvent));
        // 14
        assert_eq!(0, post_header_length(&event, LogEventType::UserVarEvent));
        // 15
        // length of StartEventV3 + 1 + number of LogEventType = 56 + 1 + 38
        // NOTE: FDE may contains additional 1-byte of checksum flag at end,
        //       followed by a 4-byte checksum value
        assert_eq!(
            95,
            post_header_length(&event, LogEventType::FormatDescriptionEvent)
        );
        // 16
        assert_eq!(0, post_header_length(&event, LogEventType::XidEvent));
        // 17
        assert_eq!(
            4,
            post_header_length(&event, LogEventType::BeginLoadQueryEvent)
        );
        // 18
        assert_eq!(
            26,
            post_header_length(&event, LogEventType::ExecuteLoadQueryEvent)
        );
        // 19
        assert_eq!(8, post_header_length(&event, LogEventType::TableMapEvent));
        // 20
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::WriteRowsEventV0)
        );
        // 21
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::UpdateRowsEventV0)
        );
        // 22
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::DeleteRowsEventV0)
        );
        // 23
        assert_eq!(
            8,
            post_header_length(&event, LogEventType::WriteRowsEventV1)
        );
        // 24
        assert_eq!(
            8,
            post_header_length(&event, LogEventType::UpdateRowsEventV1)
        );
        // 25
        assert_eq!(
            8,
            post_header_length(&event, LogEventType::DeleteRowsEventV1)
        );
        // 26
        assert_eq!(2, post_header_length(&event, LogEventType::IncidentEvent));
        // 27
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::HeartbeatLogEvent)
        );
        // 28
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::IgnorableLogEvent)
        );
        // 29
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::RowsQueryLogEvent)
        );
        // 30
        assert_eq!(
            10,
            post_header_length(&event, LogEventType::WriteRowsEventV2)
        );
        // 31
        assert_eq!(
            10,
            post_header_length(&event, LogEventType::UpdateRowsEventV2)
        );
        // 32
        assert_eq!(
            10,
            post_header_length(&event, LogEventType::DeleteRowsEventV2)
        );
        // 33
        assert_eq!(42, post_header_length(&event, LogEventType::GtidLogEvent));
        // 34
        assert_eq!(
            42,
            post_header_length(&event, LogEventType::AnonymousGtidLogEvent)
        );
        // 35
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::PreviousGtidsLogEvent)
        );
        // 36
        assert_eq!(
            18,
            post_header_length(&event, LogEventType::TransactionContextEvent)
        );
        // 37
        assert_eq!(
            52,
            post_header_length(&event, LogEventType::ViewChangeEvent)
        );
        // 38
        assert_eq!(
            0,
            post_header_length(&event, LogEventType::XaPrepareLogEvent)
        );

        println!("{:#?}", event);
        Ok(())
    }

    // binlog-query-event contains 4 events:
    // FDE, PreviousGtid, AnonymousGtid, Query
    #[test]
    fn test_query_event() -> Result<()> {
        use crate::query::{Flags2Code, QueryStatusVar, SqlModeCode};
        let (input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_QUERY_EVENT)?;
        let input = pv4.skip_event::<VerboseError<_>>(input)?;
        let input = pv4.skip_event::<VerboseError<_>>(input)?;
        // the fourth event is QueryEvent
        let (_, qe) = pv4.parse_query_event::<VerboseError<_>>(input)?;
        println!("{:#?}", qe);
        dbg!(String::from_utf8_lossy(qe.data.schema));

        let (_, vars) =
            crate::query::parse_query_status_vars::<VerboseError<_>>(qe.data.status_vars)?;
        println!("{:#?}", vars);
        vars.into_iter().for_each(|v| match v {
            QueryStatusVar::Flags2Code(c) => {
                let f2c = Flags2Code(c);
                dbg!(f2c.auto_is_null());
                dbg!(f2c.not_autocommit());
                dbg!(f2c.no_foreign_key_checks());
                dbg!(f2c.relaxed_unique_checks());
            }
            QueryStatusVar::SqlModeCode(c) => {
                let smc = SqlModeCode(c);
                dbg!(smc.real_as_float());
                dbg!(smc.pipes_as_concat());
                dbg!(smc.ansi_quotes());
                dbg!(smc.ignore_space());
                dbg!(smc.not_used());
                dbg!(smc.only_full_group_by());
                dbg!(smc.no_unsigned_subtraction());
                dbg!(smc.no_dir_in_create());
                dbg!(smc.postgresql());
                dbg!(smc.oracle());
                dbg!(smc.mssql());
                dbg!(smc.db2());
                dbg!(smc.maxdb());
                dbg!(smc.no_key_options());
                dbg!(smc.no_table_options());
                dbg!(smc.no_field_options());
                dbg!(smc.mysql323());
                dbg!(smc.mysql40());
                dbg!(smc.ansi());
                dbg!(smc.no_auto_value_on_zero());
                dbg!(smc.no_backslash_escapes());
                dbg!(smc.strict_trans_tables());
                dbg!(smc.strict_all_tables());
                dbg!(smc.no_zero_in_date());
                dbg!(smc.no_zero_date());
                dbg!(smc.invalid_dates());
                dbg!(smc.error_for_division_by_zero());
                dbg!(smc.tranditional());
                dbg!(smc.no_auto_create_user());
                dbg!(smc.high_not_precedence());
                dbg!(smc.no_engine_substitution());
                dbg!(smc.pad_char_to_full_length());
            }
            _ => (),
        });
        Ok(())
    }

    // 3 events:
    // FDE, PreviousGtid, Stop
    #[test]
    fn test_stop_event() -> TResult {
        let (input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_5_7_30)?;
        let input = pv4.skip_event::<VerboseError<_>>(input)?;
        // third event is StopEvent
        let (_, se) = pv4.parse_stop_event::<VerboseError<_>>(input)?;
        println!("{:#?}", se);
        Ok(())
    }

    // 3 events:
    // FDE, PreviousGtid, Rotate
    #[test]
    fn test_rotate_event() -> TResult {
        let (input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_ROTATE_EVENT)?;
        let input = pv4.skip_event::<VerboseError<_>>(input)?;
        // third event is RotateEvent
        let (_, re) = pv4.parse_rotate_event::<VerboseError<_>>(input)?;
        println!("{:#?}", re);
        dbg!(String::from_utf8_lossy(re.data.next_binlog_filename));
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_intvar_event() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_RAND_EVENT)?;
        for _ in 0..3 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 4th event
        let (_, ive) = pv4.parse_intvar_event::<VerboseError<_>>(input)?;
        println!("{:#?}", ive);
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_load_event_unimplemented() -> TResult {
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_create_file_event_unimplemented() -> TResult {
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_exec_load_event_unimplemented() -> TResult {
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_delete_file_event_unimplemented() -> TResult {
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_new_load_event_unimplemented() -> TResult {
        Ok(())
    }

    // FDE, PreviousGtids, AnonymousGtid, Query,
    // BeginLoadQueryEvent, ExecuteLoadQueryEvent,
    // Xid
    #[test]
    fn test_begin_load_query_event() -> TResult {
        let (mut input, pv4) =
            ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_BEGIN_LOAD_QUERY_EVENT)?;
        for _ in 0..3 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 4th event
        let (_, blqe) = pv4.parse_begin_load_query_event::<VerboseError<_>>(input)?;
        println!("{:#?}", blqe);
        dbg!(String::from_utf8_lossy(blqe.data.block_data));
        Ok(())
    }

    #[test]
    fn test_execute_load_query_event() -> TResult {
        let (mut input, pv4) =
            ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_BEGIN_LOAD_QUERY_EVENT)?;
        for _ in 0..4 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 5th event
        let (_, elqe) = pv4.parse_execute_load_query_event::<VerboseError<_>>(input)?;
        println!("{:#?}", elqe);
        Ok(())
    }

    #[test]
    fn test_rand_event() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_RAND_EVENT)?;
        for _ in 0..4 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 5th event
        let (_, re) = pv4.parse_rand_event::<VerboseError<_>>(input)?;
        println!("{:#?}", re);
        Ok(())
    }

    #[test]
    fn test_xid_event() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_ROWS_EVENT_V2)?;
        for _ in 0..9 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 10th is Xid Event
        let (_, xe) = pv4.parse_xid_event::<VerboseError<_>>(input)?;
        println!("{:#?}", xe);
        Ok(())
    }

    #[test]
    fn test_user_var_event() -> TResult {
        let (mut input, pv4) =
            ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_USER_VAR_EVENT)?;
        // for _ in 0..3 {
        //     input = pv4.skip_event::<VerboseError<_>>(input)?;
        // }
        while !input.is_empty() {
            let event_type = fast_event_type::<VerboseError<_>>(input)?;
            match event_type {
                LogEventType::UserVarEvent => {
                    let (in0, uve) = pv4.parse_user_var_event::<VerboseError<_>>(input)?;
                    println!("{:#?}", uve);
                    let uvv = uve.data.parse_value().expect("parse value");
                    println!("{:#?}", uvv);
                    input = in0;
                }
                _ => input = pv4.skip_event::<VerboseError<_>>(input)?,
            }
        }
        Ok(())
    }

    // rename after implementation
    #[test]
    fn test_incident_event_unimplemented() -> TResult {
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
    fn test_table_map_event() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_ROWS_EVENT_V2)?;
        for _ in 0..3 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 4th event
        let (_, tme) = pv4.parse_table_map_event::<VerboseError<_>>(input)?;
        println!("{:#?}", tme);
        // let (in1, rtm) = parse_raw_table_map::<VerboseError<_>>(tme.data.payload)?;
        // println!("table_map={:#?}", rtm);
        // println!("in1={:?}", in1);
        // let tm = rtm.table_map.unwrap();
        let rtm = tme.data.raw_table_map().unwrap();
        println!("{:#?}", rtm);
        let tm = tme.data.table_map().unwrap();
        println!("{:#?}", tm);
        Ok(())
    }

    #[test]
    fn test_delete_rows_event_v1() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_ROWS_EVENT_V1)?;
        for _ in 0..6 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 7th event
        let (_, dre) = pv4.parse_delete_rows_event_v1::<VerboseError<_>>(input)?;
        println!("{:#?}", dre);
        Ok(())
    }

    #[test]
    fn test_update_rows_event_v1() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_ROWS_EVENT_V1)?;
        for _ in 0..4 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 5th event
        let (_, ure) = pv4.parse_update_rows_event_v1::<VerboseError<_>>(input)?;
        println!("{:#?}", ure);
        Ok(())
    }

    #[test]
    fn test_write_rows_event_v1() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_ROWS_EVENT_V1)?;
        for _ in 0..2 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 3th event
        let (_, wre) = pv4.parse_write_rows_event_v1::<VerboseError<_>>(input)?;
        println!("{:#?}", wre);
        Ok(())
    }

    #[test]
    fn test_delete_rows_event_v2() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_ROWS_EVENT_V2)?;
        for _ in 0..7 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 8th is TableMapEvent
        let (input, tme) = pv4.parse_table_map_event::<VerboseError<_>>(input)?;
        let tm = tme.data.table_map().unwrap();
        // 9th event is DeleteRowsEventV2
        let (_, dre) = pv4.parse_delete_rows_event_v2::<VerboseError<_>>(input)?;
        println!("{:#?}", dre);
        let delete_rows = dre.data.delete_rows(&tm.col_metas).unwrap();
        dbg!(delete_rows);
        Ok(())
    }

    #[test]
    fn test_update_rows_event_v2() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_ROWS_EVENT_V2)?;
        for _ in 0..5 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 6th is TableMapEvent
        let (input, tme) = pv4.parse_table_map_event::<VerboseError<_>>(input)?;
        let tm = tme.data.table_map().unwrap();
        // 7th event is UpdateRowsEventV2
        let (_, ure) = pv4.parse_update_rows_event_v2::<VerboseError<_>>(input)?;
        println!("{:#?}", ure);
        let update_rows = ure.data.update_rows(&tm.col_metas).unwrap();
        dbg!(update_rows);
        Ok(())
    }

    #[test]
    fn test_write_rows_event_v2() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_ROWS_EVENT_V2)?;
        for _ in 0..3 {
            input = pv4.skip_event::<VerboseError<_>>(input)?;
        }
        // 4th is TableMapEvent
        let (input, tme) = pv4.parse_table_map_event::<VerboseError<_>>(input)?;
        let tm = tme.data.table_map().unwrap();
        // 5th event is WriteRowsEventV2
        let (_, wre) = pv4.parse_write_rows_event_v2::<VerboseError<_>>(input)?;
        println!("{:#?}", wre);
        let write_rows = wre.data.write_rows(&tm.col_metas).unwrap();
        dbg!(write_rows);
        Ok(())
    }

    #[test]
    fn test_gtid_event() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_GTID_EVENT)?;
        input = pv4.skip_event::<VerboseError<_>>(input)?;
        // 2nd event
        let (_, ge) = pv4.parse_gtid_event::<VerboseError<_>>(input)?;
        println!("{:#?}", ge);
        Ok(())
    }

    #[test]
    fn test_anonymous_gtid_event() -> TResult {
        let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_RAND_EVENT)?;
        input = pv4.skip_event::<VerboseError<_>>(input)?;
        // 2nd event
        let (_, age) = pv4.parse_anonymous_gtid_event::<VerboseError<_>>(input)?;
        println!("{:#?}", age);
        Ok(())
    }

    #[test]
    fn test_previous_gtids_event() -> TResult {
        let (input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(BINLOG_GTID_EVENT)?;
        // 1st event
        let (_, pge) = pv4.parse_previous_gtids_event::<VerboseError<_>>(input)?;
        println!("{:#?}", pge);
        dbg!(pge.data.gtid_set().unwrap());
        Ok(())
    }

    // #[test]
    // fn test_checksum_all_files() -> TResult {
    //     let mut chk = Checksum::new();
    //     let files = vec![
    //         BINLOG_5_7_30,
    //         BINLOG_QUERY_EVENT,
    //         BINLOG_ROTATE_EVENT,
    //         BINLOG_ROWS_EVENT_V1,
    //         BINLOG_ROWS_EVENT_V2,
    //         BINLOG_BEGIN_LOAD_QUERY_EVENT,
    //         BINLOG_RAND_EVENT,
    //     ];
    //     for f in files {
    //         let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(f)?;
    //         while !input.is_empty() {
    //             input = pv4.checksum_event(&mut chk, input)?;
    //         }
    //     }
    //     Ok(())
    // }

    fn post_header_length(event: &FormatDescriptionEvent, event_type: LogEventType) -> u8 {
        let idx = LogEventTypeCode::from(event_type).0 as usize - 1;
        event.data.post_header_lengths[idx]
    }
}

