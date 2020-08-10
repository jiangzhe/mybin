use crate::checksum::Checksum;
use crate::data::*;
use crate::error::{fmt_nom_err, Error};
use crate::event::*;
use crate::gtid::*;
use crate::header::*;
use crate::rows_v1::*;
use crate::rows_v2::*;
use crate::table_map::*;
use crate::user_var::*;
use nom::bytes::streaming::{tag, take};
use nom::combinator::{cut, verify};
use nom::error::convert_error;
use nom::error::{ParseError, VerboseError};
use nom::number::streaming::{le_u32, le_u8};
use nom::IResult;

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
pub fn parse_binlog_version<'a, E>(input: &'a [u8]) -> IResult<&[u8], BinlogVersion, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, _) = cut(tag(b"\xfebin"))(input)?;
    let (_, eh) = parse_event_header_v1(input)?;
    match LogEventType::from(eh.type_code) {
        LogEventType::StartEventV3 => {
            if eh.event_length < 75 {
                Ok((input, BinlogVersion::V1))
            } else {
                Ok((input, BinlogVersion::V3))
            }
        }
        LogEventType::FormatDescriptionEvent => Ok((input, BinlogVersion::V4)),
        _ => panic!("unsupported mysql binlog version"),
    }
}

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
        let post_header_lengths = post_header_lengths_from_raw(fde.data.post_header_lengths);
        let checksum = fde.data.checksum_flag == 1;
        ParserV4::new(post_header_lengths, checksum)
    }

    // this function will verify binlog version to be v4
    // and consume FDE to get post header lengths for all
    // following events
    pub fn from_binlog_file<'a, E>(input: &'a [u8]) -> IResult<&[u8], Self, E>
    where
        E: ParseError<&'a [u8]>,
    {
        let (input, _) = verify(parse_binlog_version, |bv| bv == &BinlogVersion::V4)(input)?;
        let (input, fde) = parse_start_fde(input)?;
        Ok((input, Self::from_fde(&fde)))
    }

    pub fn post_header_length(&self, event_type: LogEventType) -> u8 {
        let idx: usize = LogEventTypeCode::from(event_type).0 as usize;
        if idx >= self.post_header_lengths.len() {
            return 0;
        }
        self.post_header_lengths[idx]
    }

    pub fn parse_event<'a>(&self, input: &'a [u8]) -> Result<(&'a [u8], Event<'a>), Error> {
        let (input, header) = parse_event_header(input).map_err(|e| Error::from((input, e)))?;
        let event_type = LogEventType::from(header.type_code);
        let post_header_len = self.post_header_length(event_type);
        match event_type {
            LogEventType::Unknown => unreachable!(),
            LogEventType::StartEventV3 => unimplemented!(),
            LogEventType::QueryEvent => {
                let (input, (data, crc32)) =
                    parse_query_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::QueryEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::StopEvent => {
                let (input, (data, crc32)) =
                    parse_stop_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::StopEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::RotateEvent => {
                let (input, (data, crc32)) =
                    parse_rotate_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::RotateEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::IntvarEvent => {
                let (input, (data, crc32)) =
                    parse_intvar_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::IntvarEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::LoadEvent => {
                let (input, (data, crc32)) =
                    parse_load_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::LoadEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::SlaveEvent => unimplemented!(),
            LogEventType::CreateFileEvent => {
                let (input, (data, crc32)) = parse_create_file_data(
                    input,
                    header.data_len(),
                    post_header_len,
                    self.checksum,
                )
                .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::CreateFileEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::AppendBlockEvent => {
                let (input, (data, crc32)) = parse_append_block_data(
                    input,
                    header.data_len(),
                    post_header_len,
                    self.checksum,
                )
                .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::AppendBlockEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::ExecLoadEvent => {
                let (input, (data, crc32)) =
                    parse_exec_load_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::ExecLoadEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::DeleteFileEvent => {
                let (input, (data, crc32)) = parse_delete_file_data(
                    input,
                    header.data_len(),
                    post_header_len,
                    self.checksum,
                )
                .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::DeleteFileEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::NewLoadEvent => {
                let (input, (data, crc32)) =
                    parse_new_load_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::NewLoadEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::RandEvent => {
                let (input, (data, crc32)) =
                    parse_rand_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::RandEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::UserVarEvent => {
                let (input, (data, crc32)) =
                    parse_user_var_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::UserVarEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::FormatDescriptionEvent => {
                // todo: may need to add strict condition
                let (input, (data, crc32)) =
                    parse_format_description_data(input, header.data_len())
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::FormatDescriptionEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::XidEvent => {
                let (input, (data, crc32)) =
                    parse_xid_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::XidEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::BeginLoadQueryEvent => {
                let (input, (data, crc32)) = parse_begin_load_query_data(
                    input,
                    header.data_len(),
                    post_header_len,
                    self.checksum,
                )
                .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::BeginLoadQueryEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::ExecuteLoadQueryEvent => {
                let (input, (data, crc32)) = parse_execute_load_query_data(
                    input,
                    header.data_len(),
                    post_header_len,
                    self.checksum,
                )
                .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::ExecuteLoadQueryEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::TableMapEvent => {
                let (input, (data, crc32)) =
                    parse_table_map_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::TableMapEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::WriteRowsEventV0 => unimplemented!(),
            LogEventType::UpdateRowsEventV0 => unimplemented!(),
            LogEventType::DeleteRowsEventV0 => unimplemented!(),
            LogEventType::WriteRowsEventV1 => {
                let (input, (data, crc32)) =
                    parse_rows_data_v1(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::WriteRowsEventV1(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::UpdateRowsEventV1 => {
                let (input, (data, crc32)) =
                    parse_rows_data_v1(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::UpdateRowsEventV1(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::DeleteRowsEventV1 => {
                let (input, (data, crc32)) =
                    parse_rows_data_v1(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::DeleteRowsEventV1(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::IncidentEvent => {
                let (input, (data, crc32)) =
                    parse_incident_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::IncidentEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::HeartbeatLogEvent => {
                let (input, (data, crc32)) = parse_heartbeat_log_data(
                    input,
                    header.data_len(),
                    post_header_len,
                    self.checksum,
                )
                .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::HeartbeatEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::IgnorableLogEvent => unimplemented!(),
            LogEventType::RowsQueryLogEvent => unimplemented!(),
            LogEventType::WriteRowsEventV2 => {
                let (input, (data, crc32)) =
                    parse_rows_data_v2(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::WriteRowsEventV2(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::UpdateRowsEventV2 => {
                let (input, (data, crc32)) =
                    parse_rows_data_v2(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::UpdateRowsEventV2(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::DeleteRowsEventV2 => {
                let (input, (data, crc32)) =
                    parse_rows_data_v2(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::DeleteRowsEventV2(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::GtidLogEvent => {
                let (input, (data, crc32)) =
                    parse_gtid_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::GtidEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::AnonymousGtidLogEvent => {
                let (input, (data, crc32)) =
                    parse_gtid_data(input, header.data_len(), post_header_len, self.checksum)
                        .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::AnonymousGtidEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::PreviousGtidsLogEvent => {
                let (input, (data, crc32)) = parse_previous_gtids_data(
                    input,
                    header.data_len(),
                    post_header_len,
                    self.checksum,
                )
                .map_err(|e| Error::from((input, e)))?;
                Ok((
                    input,
                    Event::PreviousGtidsEvent(RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
            LogEventType::TransactionContextEvent => unimplemented!(),
            LogEventType::ViewChangeEvent => unimplemented!(),
            LogEventType::XaPrepareLogEvent => unimplemented!(),
            // // pseudo invalid code
            LogEventType::Invalid => unreachable!(),
        }
    }

    pub fn parse_format_description_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], FormatDescriptionEvent<'a>, E> {
        let (input, header) = parse_event_header(input)?;
        debug_assert_eq!(
            LogEventType::FormatDescriptionEvent,
            LogEventType::from(header.type_code)
        );
        let (input, (data, crc32)) = parse_format_description_data(input, header.data_len())?;
        Ok((
            input,
            FormatDescriptionEvent {
                header,
                data,
                crc32,
            },
        ))
    }

    pub fn parse_query_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], QueryEvent<'a>, E> {
        self.parse_raw_event(LogEventType::QueryEvent, parse_query_data, input)
    }

    pub fn parse_stop_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], StopEvent, E> {
        self.parse_raw_event(LogEventType::StopEvent, parse_stop_data, input)
    }

    pub fn parse_rotate_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], RotateEvent<'a>, E> {
        self.parse_raw_event(LogEventType::RotateEvent, parse_rotate_data, input)
    }

    pub fn parse_intvar_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], IntvarEvent, E> {
        self.parse_raw_event(LogEventType::IntvarEvent, parse_intvar_data, input)
    }

    pub fn parse_load_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], LoadEvent<'a>, E> {
        self.parse_raw_event(LogEventType::LoadEvent, parse_load_data, input)
    }

    pub fn parse_create_file_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], CreateFileEvent<'a>, E> {
        self.parse_raw_event(LogEventType::CreateFileEvent, parse_create_file_data, input)
    }

    pub fn parse_append_block_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], AppendBlockEvent<'a>, E> {
        self.parse_raw_event(
            LogEventType::AppendBlockEvent,
            parse_append_block_data,
            input,
        )
    }

    pub fn parse_exec_load_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], ExecLoadEvent, E> {
        self.parse_raw_event(LogEventType::ExecLoadEvent, parse_exec_load_data, input)
    }

    pub fn parse_delete_file_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], DeleteFileEvent, E> {
        self.parse_raw_event(LogEventType::DeleteFileEvent, parse_delete_file_data, input)
    }

    pub fn parse_new_load_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], NewLoadEvent<'a>, E> {
        self.parse_raw_event(LogEventType::NewLoadEvent, parse_new_load_data, input)
    }

    pub fn parse_begin_load_query_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], BeginLoadQueryEvent<'a>, E> {
        self.parse_raw_event(
            LogEventType::BeginLoadQueryEvent,
            parse_begin_load_query_data,
            input,
        )
    }

    pub fn parse_execute_load_query_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], ExecuteLoadQueryEvent<'a>, E> {
        self.parse_raw_event(
            LogEventType::ExecuteLoadQueryEvent,
            parse_execute_load_query_data,
            input,
        )
    }

    pub fn parse_rand_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], RandEvent, E> {
        self.parse_raw_event(LogEventType::RandEvent, parse_rand_data, input)
    }

    pub fn parse_xid_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], XidEvent, E> {
        self.parse_raw_event(LogEventType::XidEvent, parse_xid_data, input)
    }

    pub fn parse_user_var_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], UserVarEvent<'a>, E> {
        self.parse_raw_event(LogEventType::UserVarEvent, parse_user_var_data, input)
    }

    pub fn parse_incident_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], IncidentEvent<'a>, E> {
        self.parse_raw_event(LogEventType::IncidentEvent, parse_incident_data, input)
    }

    pub fn parse_heartbeat_log_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], HeartbeatEvent, E> {
        self.parse_raw_event(
            LogEventType::HeartbeatLogEvent,
            parse_heartbeat_log_data,
            input,
        )
    }

    pub fn parse_table_map_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], TableMapEvent<'a>, E> {
        self.parse_raw_event(LogEventType::TableMapEvent, parse_table_map_data, input)
    }

    pub fn parse_delete_rows_event_v1<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], DeleteRowsEventV1<'a>, E> {
        self.parse_raw_event(LogEventType::DeleteRowsEventV1, parse_rows_data_v1, input)
    }

    pub fn parse_update_rows_event_v1<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], UpdateRowsEventV1<'a>, E> {
        self.parse_raw_event(LogEventType::UpdateRowsEventV1, parse_rows_data_v1, input)
    }

    pub fn parse_write_rows_event_v1<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], WriteRowsEventV1<'a>, E> {
        self.parse_raw_event(LogEventType::WriteRowsEventV1, parse_rows_data_v1, input)
    }

    pub fn parse_delete_rows_event_v2<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], DeleteRowsEventV2<'a>, E> {
        self.parse_raw_event(LogEventType::DeleteRowsEventV2, parse_rows_data_v2, input)
    }

    pub fn parse_update_rows_event_v2<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], UpdateRowsEventV2<'a>, E> {
        self.parse_raw_event(LogEventType::UpdateRowsEventV2, parse_rows_data_v2, input)
    }

    pub fn parse_write_rows_event_v2<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], WriteRowsEventV2<'a>, E> {
        self.parse_raw_event(LogEventType::WriteRowsEventV2, parse_rows_data_v2, input)
    }

    pub fn parse_gtid_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], GtidEvent, E> {
        self.parse_raw_event(LogEventType::GtidLogEvent, parse_gtid_data, input)
    }

    pub fn parse_anonymous_gtid_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], GtidEvent, E> {
        self.parse_raw_event(LogEventType::AnonymousGtidLogEvent, parse_gtid_data, input)
    }

    pub fn parse_previous_gtids_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> IResult<&'a [u8], PreviousGtidsEvent<'a>, E> {
        self.parse_raw_event(
            LogEventType::PreviousGtidsLogEvent,
            parse_previous_gtids_data,
            input,
        )
    }

    /// skip current event
    pub fn skip_event<'a, E: ParseError<&'a [u8]>>(
        &self,
        input: &'a [u8],
    ) -> Result<&'a [u8], nom::Err<E>> {
        let (input, header) = parse_event_header(input)?;
        let (input, _) = take(header.data_len())(input)?;
        Ok(input)
    }

    fn parse_raw_event<'b, P, D, E>(
        &self,
        event_type: LogEventType,
        p: P,
        input: &'b [u8],
    ) -> IResult<&'b [u8], RawEvent<D>, E>
    where
        P: Fn(&'b [u8], u32, u8, bool) -> IResult<&'b [u8], (D, u32), E>,
        D: 'b,
        E: ParseError<&'b [u8]>,
    {
        let (input, header) = parse_event_header(input)?;
        debug_assert_eq!(event_type, LogEventType::from(header.type_code));
        let post_header_len = self.post_header_length(event_type);
        let (input, (data, crc32)) = p(input, header.data_len(), post_header_len, self.checksum)?;
        Ok((
            input,
            RawEvent {
                header,
                data,
                crc32,
            },
        ))
    }

    /// checksum the current event
    /// return input starting from next event if success
    pub fn checksum_event<'b>(
        &self,
        checksum: &mut Checksum,
        input: &'b [u8],
    ) -> Result<&'b [u8], Error> {
        if !self.checksum {
            // checksum is not enabled
            let (input, header) = parse_event_header::<'_, VerboseError<_>>(input)
                .map_err(|e| Error::from((input, e)))?;
            let (input, _) = take(header.data_len())(input).map_err(|e| Error::from((input, e)))?;
            return Ok(input);
        }
        // verbose mode to collect error information
        match self.find_checksum_base::<VerboseError<_>>(input) {
            // todo
            Err(e) => Err(Error::from((input, e))),
            // Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Err(Error::ParseErr(fmt_nom_err(input, e))),
            // Err(nom::Err::Incomplete(n)) => Err(Error::ParseErr(format!("more input required: {:?}", n))),
            Ok((input, (base, expected))) => {
                let actual = checksum.checksum(base);
                if expected == actual {
                    Ok(input)
                } else {
                    Err(Error::InconsistentChecksum(expected, actual))
                }
            }
        }
    }

    fn find_checksum_base<'a, E>(&self, input: &'a [u8]) -> IResult<&'a [u8], (&'a [u8], u32), E>
    where
        E: ParseError<&'a [u8]>,
    {
        debug_assert!(self.checksum);
        let event_length = fast_event_length(input)?;
        let (input, base) = take(event_length - 4)(input)?;
        let (input, crc32) = le_u32(input)?;
        Ok((input, (base, crc32)))
    }
}

/// parse v1 start event
///
/// v1 start event has same payload as v3 start event
/// but header is different.
#[allow(dead_code)]
fn parse_start_event_v1<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], StartEventV1, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, header) = parse_event_header_v1(input)?;
    let (input, data) = parse_start_data(input)?;
    Ok((input, StartEventV1 { header, data }))
}

/// parse v3 start event
fn parse_start_event_v3<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], StartEventV3, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, header) = parse_event_header(input)?;
    let (input, data) = parse_start_data(input)?;
    // start_event_v3 does not have crc32 checksum
    Ok((
        input,
        StartEventV3 {
            header,
            data,
            crc32: 0,
        },
    ))
}

/// parse format description event
///
/// this event has same header as v3 start event,
/// but has two additional fields in data.
/// includes:
/// binlog_version 2,
/// server_version 50,
/// create_timestamp 4,
/// header_length 1,
/// event_payload_header_lengths: n
fn parse_start_fde<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], FormatDescriptionEvent<'a>, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, header) = parse_event_header(input)?;
    let (input, (data, crc32)) = parse_format_description_data(input, header.data_len())?;
    Ok((
        input,
        FormatDescriptionEvent {
            header,
            data,
            crc32,
        },
    ))
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
    use nom::error::VerboseError;

    const BINLOG_5_5_50: &[u8] = include_bytes!("../data/mysql-bin.5.5.50.StartEvent");
    const BINLOG_5_7_30: &[u8] = include_bytes!("../data/mysql-bin.5.7.30.StartEvent");
    const BINLOG_NO_CHECKSUM: &[u8] =
        include_bytes!("../data/mysql-bin.5.7.30.StartEventNoChecksum");
    const BINLOG_QUERY_EVENT: &[u8] = include_bytes!("../data/mysql-bin.5.7.30.QueryEvent");
    const BINLOG_ROTATE_EVENT: &[u8] = include_bytes!("../data/mysql-bin.5.7.30.RotateEvent");
    const BINLOG_ROWS_EVENT_V1: &[u8] = include_bytes!("../data/mysql-bin.5.5.50.RowsEventV1");
    const BINLOG_ROWS_EVENT_V2: &[u8] = include_bytes!("../data/mysql-bin.5.7.30.RowsEventV2");
    const BINLOG_BEGIN_LOAD_QUERY_EVENT: &[u8] =
        include_bytes!("../data/mysql-bin.5.7.30.BeginLoadQueryEvent");
    const BINLOG_RAND_EVENT: &[u8] = include_bytes!("../data/mysql-bin.5.7.30.RandEvent");
    const BINLOG_USER_VAR_EVENT: &[u8] = include_bytes!("../data/mysql-bin.5.7.30.UserVarEvent");
    const BINLOG_GTID_EVENT: &[u8] = include_bytes!("../data/mysql-bin.5.7.30.GtidEvent");

    type DynError = Box<dyn std::error::Error>;
    type TResult = Result<(), DynError>;

    #[test]
    fn test_binlog_version() {
        let (_, bv) = parse_binlog_version::<VerboseError<_>>(BINLOG_5_7_30).unwrap();
        assert_eq!(BinlogVersion::V4, bv);
        assert!(parse_binlog_version::<VerboseError<_>>(b"\xfebin")
            .unwrap_err()
            .is_incomplete())
    }

    #[test]
    fn test_binlog_no_checksum() -> TResult {
        let (input, _) = parse_binlog_version::<VerboseError<_>>(BINLOG_NO_CHECKSUM)?;
        let (_, fde) = parse_start_fde::<VerboseError<_>>(input)?;
        println!("{:#?}", fde);
        Ok(())
    }

    #[test]
    fn test_format_description_event_5_5() -> TResult {
        let (input, _) = parse_binlog_version::<VerboseError<_>>(BINLOG_5_5_50)?;
        let (_, event) = parse_start_fde::<VerboseError<_>>(input)?;
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
    fn test_format_description_event_5_7() -> TResult {
        let (input, _) = parse_binlog_version::<VerboseError<_>>(BINLOG_5_7_30)?;
        let (_, event) = parse_start_fde::<VerboseError<_>>(input)?;
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
    fn test_query_event() -> TResult {
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

    #[test]
    fn test_checksum_all_files() -> TResult {
        let mut chk = Checksum::new();
        let files = vec![
            BINLOG_5_7_30,
            BINLOG_QUERY_EVENT,
            BINLOG_ROTATE_EVENT,
            BINLOG_ROWS_EVENT_V1,
            BINLOG_ROWS_EVENT_V2,
            BINLOG_BEGIN_LOAD_QUERY_EVENT,
            BINLOG_RAND_EVENT,
        ];
        for f in files {
            let (mut input, pv4) = ParserV4::from_binlog_file::<VerboseError<_>>(f)?;
            while !input.is_empty() {
                input = pv4.checksum_event(&mut chk, input)?;
            }
        }
        Ok(())
    }

    fn post_header_length(event: &FormatDescriptionEvent, event_type: LogEventType) -> u8 {
        let idx = LogEventTypeCode::from(event_type).0 as usize - 1;
        event.data.post_header_lengths[idx]
    }
}
