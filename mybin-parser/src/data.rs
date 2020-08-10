//! defines event specific data: post headers and payloads

use crate::error::Error;
use crate::event::{LogEventType, LogEventTypeCode};
use crate::user_var::*;
use crate::util::streaming_le_u48;
use nom::bytes::streaming::{take, take_till};
use nom::error::ParseError;
use nom::number::streaming::{le_u128, le_u16, le_u32, le_u64, le_u8};
use nom::IResult;
use serde_derive::*;

/// Data of StartEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/start-event-v3.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartData<'a> {
    pub binlog_version: u16,
    pub server_version: &'a [u8],
    pub create_timestamp: u32,
}

pub(crate) fn parse_start_data<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], StartData<'a>, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, binlog_version) = le_u16(input)?;
    let (input, server_version) = take(50usize)(input)?;
    // remove tail \x00
    let (_, server_version) = take_till(|b| b == 0x00)(server_version)?;
    let (input, create_timestamp) = le_u32(input)?;
    Ok((
        input,
        StartData {
            binlog_version,
            server_version,
            create_timestamp,
        },
    ))
}

/// Data of FormatDescriptionEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/format-description-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatDescriptionData<'a> {
    pub binlog_version: u16,
    pub server_version: &'a [u8],
    pub create_timestamp: u32,
    pub header_length: u8,
    pub post_header_lengths: &'a [u8],
    // only record checksum flag, should be 0 or 1 after mysql 5.6.1
    // in case of earlier version, set 0
    pub checksum_flag: u8,
}

/// because FDE is the first event in binlog, we do not know its post header length,
/// so we need the total data size as input argument,
/// which can be calculated by event_length - 19
pub(crate) fn parse_format_description_data<'a, E>(
    input: &'a [u8],
    len: u32,
) -> IResult<&'a [u8], (FormatDescriptionData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, sep) = parse_start_data(input)?;
    let (input, header_length) = le_u8(input)?;
    // 57(2+50+4+1) bytes consumed
    // actually there are several random-value bytes at end of the payload
    // but that does not affect post header lengths of existing events
    let (input, post_header_lengths) = take(len - 57)(input)?;
    // before mysql 5.6.1, there is no checksum so the data len is
    // same as total size of all 5 fields
    // but from 5.6.1, there is 5 additional bytes at end of the
    // post header lengths field if checksum is enabled
    // we use self contained FDE post header len to check if
    // the checksum flag and checksum value exist
    let fde_type_code = LogEventTypeCode::from(LogEventType::FormatDescriptionEvent);
    let fde_post_header_len = post_header_lengths[fde_type_code.0 as usize - 1];
    if len == fde_post_header_len as u32 {
        // version not support checksum
        return Ok((
            input,
            (
                FormatDescriptionData {
                    binlog_version: sep.binlog_version,
                    server_version: sep.server_version,
                    create_timestamp: sep.create_timestamp,
                    header_length,
                    post_header_lengths,
                    checksum_flag: 0,
                },
                0,
            ),
        ));
    }
    // version supports checksum
    // split checksum
    let checksum_len = len as usize - fde_post_header_len as usize;
    let (post_header_lengths, checksum_in) =
        post_header_lengths.split_at(post_header_lengths.len() - checksum_len);
    let (checksum_in, checksum_flag) = le_u8(checksum_in)?;
    let (_, crc32) = le_u32(checksum_in)?;
    Ok((
        input,
        (
            FormatDescriptionData {
                binlog_version: sep.binlog_version,
                server_version: sep.server_version,
                create_timestamp: sep.create_timestamp,
                header_length,
                post_header_lengths,
                checksum_flag,
            },
            crc32,
        ),
    ))
}

/// Data of QueryEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/query-event.html
/// only support binlog v4 (with status_vars_length at end of post header)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryData<'a> {
    pub slave_proxy_id: u32,
    pub execution_time: u32,
    pub schema_length: u8,
    pub error_code: u16,
    // if binlog version >= 4
    pub status_vars_length: u16,
    // below is variable part
    pub status_vars: &'a [u8],
    pub schema: &'a [u8],
    pub query: &'a [u8],
}

pub(crate) fn parse_query_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (QueryData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(13, post_header_len);
    let (input, slave_proxy_id) = le_u32(input)?;
    let (input, execution_time) = le_u32(input)?;
    let (input, schema_length) = le_u8(input)?;
    let (input, error_code) = le_u16(input)?;
    let (input, status_vars_length) = le_u16(input)?;
    // 13(4+4+1+2+2) bytes consumed
    // do not parse status_vars in this stage
    let (input, status_vars) = take(status_vars_length)(input)?;
    let (input, schema) = take(schema_length)(input)?;
    let (input, _) = take(1usize)(input)?;
    let query_length =
        len - post_header_len as u32 - status_vars_length as u32 - schema_length as u32 - 1;
    let (input, query, crc32) = if checksum {
        let query_length = query_length - 4;
        let (input, query) = take(query_length)(input)?;
        let (input, crc32) = le_u32(input)?;
        (input, query, crc32)
    } else {
        let (input, query) = take(query_length)(input)?;
        (input, query, 0)
    };
    Ok((
        input,
        (
            QueryData {
                slave_proxy_id,
                execution_time,
                schema_length,
                error_code,
                status_vars_length,
                status_vars,
                schema,
                query,
            },
            crc32,
        ),
    ))
}

/// stop event has no data, but 4-byte integer if checksum enabled
pub(crate) fn parse_stop_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], ((), u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(0, post_header_len);
    let (input, crc32) = if checksum {
        debug_assert_eq!(4, len);
        le_u32(input)?
    } else {
        debug_assert_eq!(0, len);
        (input, 0)
    };
    Ok((input, ((), crc32)))
}

/// Data of RotateEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/rotate-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotateData<'a> {
    pub position: u64,
    // below is variable part
    pub next_binlog_filename: &'a [u8],
}

pub(crate) fn parse_rotate_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (RotateData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(8, post_header_len);
    let (input, position) = le_u64(input)?;
    // let (input, s) = take(len as usize - post_header_len as usize)(input)?;

    let (input, next_binlog_filename, crc32) = if checksum {
        // let (tail, next_binlog_filename) = take_till(|b| b == 0x00)(s)?;
        // remove 0x00
        // let (tail, _) = take(1usize)(tail)?;
        let (input, next_binlog_filename) =
            take(len as usize - post_header_len as usize - 4)(input)?;
        let (input, crc32) = le_u32(input)?;
        (input, next_binlog_filename, crc32)
    } else {
        let (input, next_binlog_filename) = take(len as usize - post_header_len as usize)(input)?;
        (input, next_binlog_filename, 0)
    };
    Ok((
        input,
        (
            RotateData {
                position,
                next_binlog_filename,
            },
            crc32,
        ),
    ))
}

/// Data of IntvarEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/intvar-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntvarData {
    pub key: u8,
    pub value: u64,
}

impl IntvarData {
    pub fn invalid(&self) -> bool {
        self.key == 0x00
    }

    pub fn last_insert_id(&self) -> bool {
        self.key == 0x01
    }

    pub fn insert_id(&self) -> bool {
        self.key == 0x02
    }
}

pub(crate) fn parse_intvar_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (IntvarData, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(0, post_header_len);
    let (input, key) = le_u8(input)?;
    let (input, value) = le_u64(input)?;
    let (input, crc32) = if checksum {
        debug_assert_eq!(9 + 4, len);
        le_u32(input)?
    } else {
        debug_assert_eq!(9, len);
        (input, 0)
    };
    Ok((input, (IntvarData { key, value }, crc32)))
}

/// Data of LoadEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/load-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadData<'a> {
    pub slave_proxy_id: u32,
    pub exec_time: u32,
    pub skip_lines: u32,
    pub table_name_len: u8,
    pub schema_len: u8,
    pub num_fields: u32,
    // below is variable part
    pub field_term: u8,
    pub enclosed_by: u8,
    pub line_term: u8,
    pub line_start: u8,
    pub escaped_by: u8,
    pub opt_flags: u8,
    pub empty_flags: u8,
    pub field_name_lengths: &'a [u8],
    pub field_names: &'a [u8],
    pub table_name: &'a [u8],
    pub schema_name: &'a [u8],
    pub file_name: &'a [u8],
}

pub(crate) fn parse_load_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (LoadData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(18, post_header_len);
    let mut consumed = 0u32;
    let (input, slave_proxy_id) = le_u32(input)?;
    let (input, exec_time) = le_u32(input)?;
    let (input, skip_lines) = le_u32(input)?;
    let (input, table_name_len) = le_u8(input)?;
    let (input, schema_len) = le_u8(input)?;
    let (input, num_fields) = le_u32(input)?;
    consumed += post_header_len as u32;
    // below is variable part
    let (input, field_term) = le_u8(input)?;
    let (input, enclosed_by) = le_u8(input)?;
    let (input, line_term) = le_u8(input)?;
    let (input, line_start) = le_u8(input)?;
    let (input, escaped_by) = le_u8(input)?;
    let (input, opt_flags) = le_u8(input)?;
    let (input, empty_flags) = le_u8(input)?;
    consumed += 7;
    let (input, field_name_lengths) = take(num_fields)(input)?;
    consumed += num_fields;
    let field_name_total_length =
        field_name_lengths.iter().map(|l| *l as u32).sum::<u32>() + num_fields as u32;
    let (input, field_names) = take(field_name_total_length)(input)?;
    consumed += field_name_total_length;
    let (input, tn) = take(table_name_len as u32 + 1)(input)?;
    let (_, table_name) = take_till(|b| b == 0x00)(tn)?;
    consumed += table_name_len as u32 + 1;
    let (input, sn) = take(schema_len as u32 + 1)(input)?;
    let (_, schema_name) = take_till(|b| b == 0x00)(sn)?;
    consumed += schema_len as u32 + 1;
    // let (input, fname) = take(len - consumed)(input)?;
    // let (_, file_name) = take_till(|b| b == 0x00)(fname)?;
    let (input, file_name, crc32) = if checksum {
        let (input, fn_in) = take(len - consumed)(input)?;
        let (fn_in, file_name) = take_till(|b| b == 0x00)(fn_in)?;
        // remove 0x00
        let (fn_in, _) = take(1usize)(fn_in)?;
        debug_assert_eq!(4, fn_in.len());
        let (_, crc32) = le_u32(fn_in)?;
        (input, file_name, crc32)
    } else {
        let (input, fn_in) = take(len - consumed)(input)?;
        let (fn_in, file_name) = take_till(|b| b == 0x00)(fn_in)?;
        // remove 0x00
        let (fn_in, _) = take(1usize)(fn_in)?;
        debug_assert!(fn_in.is_empty());
        (input, file_name, 0)
    };
    Ok((
        input,
        (
            LoadData {
                slave_proxy_id,
                exec_time,
                skip_lines,
                table_name_len,
                schema_len,
                num_fields,
                field_term,
                enclosed_by,
                line_term,
                line_start,
                escaped_by,
                opt_flags,
                empty_flags,
                field_name_lengths,
                field_names,
                table_name,
                schema_name,
                file_name,
            },
            crc32,
        ),
    ))
}

/// Data of CreateFileEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/create-file-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateFileData<'a> {
    pub file_id: u32,
    // below is variable part
    pub block_data: &'a [u8],
}

pub(crate) fn parse_create_file_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (CreateFileData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(4, post_header_len);
    let (input, file_id) = le_u32(input)?;
    let (input, bd) = take(len - post_header_len as u32)(input)?;
    let (tail, block_data) = take_till(|b| b == 0x00)(bd)?;
    // remove 0x00
    let (tail, _) = take(1usize)(tail)?;

    let crc32 = if checksum {
        debug_assert_eq!(4, tail.len());
        let (_, crc32) = le_u32(tail)?;
        crc32
    } else {
        debug_assert!(tail.is_empty());
        0
    };
    Ok((
        input,
        (
            CreateFileData {
                file_id,
                block_data,
            },
            crc32,
        ),
    ))
}

/// Data of AppendBlockEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/append-block-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendBlockData<'a> {
    pub file_id: u32,
    // below is variable part
    pub block_data: &'a [u8],
}

pub(crate) fn parse_append_block_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (AppendBlockData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(4, post_header_len);
    let (input, file_id) = le_u32(input)?;
    let (input, bd) = take(len - post_header_len as u32)(input)?;
    let (tail, block_data) = take_till(|b| b == 0x00)(bd)?;
    // remove 0x00
    let (tail, _) = take(1usize)(tail)?;

    let crc32 = if checksum {
        debug_assert_eq!(4, tail.len());
        let (_, crc32) = le_u32(tail)?;
        crc32
    } else {
        debug_assert!(tail.is_empty());
        0
    };
    Ok((
        input,
        (
            AppendBlockData {
                file_id,
                block_data,
            },
            crc32,
        ),
    ))
}

/// Data of ExecLoadEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/exec-load-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecLoadData {
    pub file_id: u32,
}

pub(crate) fn parse_exec_load_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (ExecLoadData, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(4, post_header_len);
    let (input, file_id) = le_u32(input)?;
    let (input, crc32) = if checksum {
        debug_assert_eq!(4, len - post_header_len as u32);
        le_u32(input)?
    } else {
        debug_assert_eq!(0, len - post_header_len as u32);
        (input, 0)
    };
    Ok((input, (ExecLoadData { file_id }, crc32)))
}

/// Data of DeleteFileEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/delete-file-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteFileData {
    pub file_id: u32,
}

pub(crate) fn parse_delete_file_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (DeleteFileData, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(4, post_header_len);
    let (input, file_id) = le_u32(input)?;
    let (input, crc32) = if checksum {
        debug_assert_eq!(4, len - post_header_len as u32);
        le_u32(input)?
    } else {
        debug_assert_eq!(0, len - post_header_len as u32);
        (input, 0)
    };
    Ok((input, (DeleteFileData { file_id }, crc32)))
}

/// Data of NewLoadEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/new-load-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewLoadData<'a> {
    pub slave_proxy_id: u32,
    pub exec_time: u32,
    pub skip_lines: u32,
    pub table_name_len: u8,
    pub schema_len: u8,
    pub num_fields: u32,
    //below is variable part
    pub field_term_len: u8,
    pub field_term: &'a [u8],
    pub enclosed_by_len: u8,
    pub enclosed_by: &'a [u8],
    pub line_term_len: u8,
    pub line_term: &'a [u8],
    pub line_start_len: u8,
    pub line_start: &'a [u8],
    pub escaped_by_len: u8,
    pub escaped_by: &'a [u8],
    pub opt_flags: u8,
    pub field_name_lengths: &'a [u8],
    pub field_names: &'a [u8],
    pub table_name: &'a [u8],
    pub schema_name: &'a [u8],
    pub file_name: &'a [u8],
}

pub(crate) fn parse_new_load_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (NewLoadData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(18, post_header_len);
    let mut consumed = 0u32;
    let (input, slave_proxy_id) = le_u32(input)?;
    let (input, exec_time) = le_u32(input)?;
    let (input, skip_lines) = le_u32(input)?;
    let (input, table_name_len) = le_u8(input)?;
    let (input, schema_len) = le_u8(input)?;
    let (input, num_fields) = le_u32(input)?;
    consumed += post_header_len as u32;
    // below is variable part
    let (input, field_term_len) = le_u8(input)?;
    let (input, field_term) = take(field_term_len)(input)?;
    consumed += field_term_len as u32 + 1;
    let (input, enclosed_by_len) = le_u8(input)?;
    let (input, enclosed_by) = take(enclosed_by_len)(input)?;
    consumed += enclosed_by_len as u32 + 1;
    let (input, line_term_len) = le_u8(input)?;
    let (input, line_term) = take(line_term_len)(input)?;
    consumed += line_term_len as u32 + 1;
    let (input, line_start_len) = le_u8(input)?;
    let (input, line_start) = take(line_start_len)(input)?;
    consumed += line_start_len as u32 + 1;
    let (input, escaped_by_len) = le_u8(input)?;
    let (input, escaped_by) = take(escaped_by_len)(input)?;
    consumed += escaped_by_len as u32 + 1;
    let (input, opt_flags) = le_u8(input)?;
    consumed += 1;
    let (input, field_name_lengths) = take(num_fields)(input)?;
    consumed += num_fields;
    let field_name_total_length =
        field_name_lengths.iter().map(|l| *l as u32).sum::<u32>() + num_fields as u32;
    let (input, field_names) = take(field_name_total_length)(input)?;
    consumed += field_name_total_length;
    let (input, tn) = take(table_name_len as u32 + 1)(input)?;
    let (_, table_name) = take_till(|b| b == 0x00)(tn)?;
    consumed += table_name_len as u32 + 1;
    let (input, sn) = take(schema_len as u32 + 1)(input)?;
    let (_, schema_name) = take_till(|b| b == 0x00)(sn)?;
    consumed += schema_len as u32 + 1;
    // let (input, fname) = take(len - consumed)(input)?;
    // let (_, file_name) = take_till(|b| b == 0x00)(fname)?;
    let (input, file_name, crc32) = if checksum {
        let (input, fn_in) = take(len - consumed)(input)?;
        let (fn_in, file_name) = take_till(|b| b == 0x00)(fn_in)?;
        // remove 0x00
        let (fn_in, _) = take(1usize)(fn_in)?;
        debug_assert_eq!(4, fn_in.len());
        let (_, crc32) = le_u32(fn_in)?;
        (input, file_name, crc32)
    } else {
        let (input, fn_in) = take(len - consumed)(input)?;
        let (fn_in, file_name) = take_till(|b| b == 0x00)(fn_in)?;
        // remove 0x00
        let (fn_in, _) = take(1usize)(fn_in)?;
        debug_assert!(fn_in.is_empty());
        (input, file_name, 0)
    };
    Ok((
        input,
        (
            NewLoadData {
                slave_proxy_id,
                exec_time,
                skip_lines,
                table_name_len,
                schema_len,
                num_fields,
                field_term_len,
                field_term,
                enclosed_by_len,
                enclosed_by,
                line_term_len,
                line_term,
                line_start_len,
                line_start,
                escaped_by_len,
                escaped_by,
                opt_flags,
                field_name_lengths,
                field_names,
                table_name,
                schema_name,
                file_name,
            },
            crc32,
        ),
    ))
}

/// Data of BeginLoadQueryEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/begin-load-query-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeginLoadQueryData<'a> {
    pub file_id: u32,
    // below is variable part
    pub block_data: &'a [u8],
}

pub(crate) fn parse_begin_load_query_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (BeginLoadQueryData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(4, post_header_len);
    let (input, file_id) = le_u32(input)?;
    // let (input, bd) = take(len - post_header_len as u32)(input)?;
    let (input, block_data, crc32) = if checksum {
        let (input, block_data) = take(len - post_header_len as u32 - 4)(input)?;
        let (input, crc32) = le_u32(input)?;
        (input, block_data, crc32)
    } else {
        let (input, block_data) = take(len - post_header_len as u32)(input)?;
        (input, block_data, 0)
    };
    Ok((
        input,
        (
            BeginLoadQueryData {
                file_id,
                block_data,
            },
            crc32,
        ),
    ))
}

/// Data of ExecuteLoadQueryEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/execute-load-query-event.html
/// there is conflicts compared to another resource,
/// https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
/// after checking real data in binlog, the second resource seems correct
/// payload will be lazy evaluated via separate module
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteLoadQueryData<'a> {
    pub slave_proxy_id: u32,
    pub execution_time: u32,
    pub schema_length: u8,
    pub error_code: u16,
    pub status_vars_length: u16,
    pub file_id: u32,
    pub start_pos: u32,
    pub end_pos: u32,
    pub dup_handling_flags: u8,
    // below is variable part
    pub payload: &'a [u8],
}

pub(crate) fn parse_execute_load_query_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (ExecuteLoadQueryData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(26, post_header_len);
    let (input, slave_proxy_id) = le_u32(input)?;
    let (input, execution_time) = le_u32(input)?;
    let (input, schema_length) = le_u8(input)?;
    let (input, error_code) = le_u16(input)?;
    let (input, status_vars_length) = le_u16(input)?;
    let (input, file_id) = le_u32(input)?;
    let (input, start_pos) = le_u32(input)?;
    let (input, end_pos) = le_u32(input)?;
    let (input, dup_handling_flags) = le_u8(input)?;
    // let (input, payload) = take(len - post_header_len as u32)(input)?;
    let (input, payload, crc32) = if checksum {
        let (input, payload) = take(len - post_header_len as u32 - 4)(input)?;
        let (input, crc32) = le_u32(input)?;
        (input, payload, crc32)
    } else {
        let (input, payload) = take(len - post_header_len as u32)(input)?;
        (input, payload, 0)
    };
    Ok((
        input,
        (
            ExecuteLoadQueryData {
                slave_proxy_id,
                execution_time,
                schema_length,
                error_code,
                status_vars_length,
                file_id,
                start_pos,
                end_pos,
                dup_handling_flags,
                payload,
            },
            crc32,
        ),
    ))
}

/// Data of RandEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/rand-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RandData {
    pub seed1: u64,
    pub seed2: u64,
}

pub(crate) fn parse_rand_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (RandData, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    // debug_assert_eq!(16, len);
    debug_assert_eq!(0, post_header_len);
    let (input, seed1) = le_u64(input)?;
    let (input, seed2) = le_u64(input)?;
    let (input, crc32) = if checksum {
        debug_assert_eq!(16 + 4, len);
        le_u32(input)?
    } else {
        debug_assert_eq!(16, len);
        (input, 0)
    };
    Ok((input, (RandData { seed1, seed2 }, crc32)))
}

/// Data of XidEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/xid-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XidData {
    pub xid: u64,
}

pub(crate) fn parse_xid_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (XidData, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(0, post_header_len);
    let (input, xid, crc32) = if checksum {
        debug_assert_eq!(8 + 4, len);
        let (input, xid) = le_u64(input)?;
        let (input, crc32) = le_u32(input)?;
        (input, xid, crc32)
    } else {
        let (input, xid) = le_u64(input)?;
        debug_assert_eq!(8, len);
        (input, xid, 0)
    };
    Ok((input, (XidData { xid }, crc32)))
}


/// Data of IncidentEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/incident-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncidentData<'a> {
    // https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/control_events.h
    pub incident_type: u16,
    // below is variable part
    pub message_length: u8,
    pub message: &'a [u8],
}

pub(crate) fn parse_incident_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (IncidentData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(2, post_header_len);
    let (input, incident_type) = le_u16(input)?;
    let (input, message_length) = le_u8(input)?;
    let (input, message) = take(message_length)(input)?;
    let (input, crc32) = if checksum {
        debug_assert_eq!(len, message_length as u32 + 3 + 4);
        le_u32(input)?
    } else {
        debug_assert_eq!(len, message_length as u32 + 3);
        (input, 0)
    };
    Ok((
        input,
        (
            IncidentData {
                incident_type,
                message_length,
                message,
            },
            crc32,
        ),
    ))
}

/// heartbeat event has no data, but 4-byte integer if checksum enabled
pub(crate) fn parse_heartbeat_log_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], ((), u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(0, post_header_len);
    let (input, crc32) = if checksum {
        debug_assert_eq!(4, len);
        le_u32(input)?
    } else {
        debug_assert_eq!(0, len);
        (input, 0)
    };
    Ok((input, ((), crc32)))
}
