use bytes_parser::error::Result;
use bytes_parser::number::ReadNumber;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::ReadAs;

/// Data of LoadEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/load-event.html
#[derive(Debug, Clone)]
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

impl<'a> ReadAs<'a, LoadData<'a>> for [u8] {
    fn read_as(&'a self, offset: usize) -> Result<(usize, LoadData<'a>)> {
        let (offset, slave_proxy_id) = self.read_le_u32(offset)?;
        let (offset, exec_time) = self.read_le_u32(offset)?;
        let (offset, skip_lines) = self.read_le_u32(offset)?;
        let (offset, table_name_len) = self.read_u8(offset)?;
        let (offset, schema_len) = self.read_u8(offset)?;
        let (offset, num_fields) = self.read_le_u32(offset)?;
        // below is variable part
        let (offset, field_term) = self.read_u8(offset)?;
        let (offset, enclosed_by) = self.read_u8(offset)?;
        let (offset, line_term) = self.read_u8(offset)?;
        let (offset, line_start) = self.read_u8(offset)?;
        let (offset, escaped_by) = self.read_u8(offset)?;
        let (offset, opt_flags) = self.read_u8(offset)?;
        let (offset, empty_flags) = self.read_u8(offset)?;
        let (offset, field_name_lengths) = self.take_len(offset, num_fields as usize)?;
        let field_name_total_length =
            field_name_lengths.iter().map(|l| *l as u32).sum::<u32>() + num_fields as u32;
        let (offset, field_names) = self.take_len(offset, field_name_total_length as usize)?;
        let (offset, tn) = self.take_len(offset, table_name_len as usize + 1)?;
        let (_, table_name) = tn.take_until(0, 0, false)?;
        let (offset, sn) = self.take_len(offset, schema_len as usize + 1)?;
        let (_, schema_name) = sn.take_until(0, 0, false)?;
        let (offset, fn_in) = self.take_len(offset, self.len() - offset)?;
        let (_, file_name) = fn_in.take_until(0, 0, false)?;
        Ok((
            offset,
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
        ))
    }
}

/// Data of CreateFileEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/create-file-event.html
#[derive(Debug, Clone)]
pub struct CreateFileData<'a> {
    pub file_id: u32,
    // below is variable part
    pub block_data: &'a [u8],
}

impl<'a> ReadAs<'a, CreateFileData<'a>> for [u8] {
    fn read_as(&'a self, offset: usize) -> Result<(usize, CreateFileData<'a>)> {
        let (offset, file_id) = self.read_le_u32(offset)?;
        let (offset, bd) = self.take_len(offset, self.len() - offset)?;
        let (_, block_data) = bd.take_until(0, 0, false)?;
        Ok((
            offset,
            CreateFileData {
                file_id,
                block_data,
            },
        ))
    }
}

/// Data of AppendBlockEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/append-block-event.html
#[derive(Debug, Clone)]
pub struct AppendBlockData<'a> {
    pub file_id: u32,
    // below is variable part
    pub block_data: &'a [u8],
}

impl<'a> ReadAs<'a, AppendBlockData<'a>> for [u8] {
    fn read_as(&'a self, offset: usize) -> Result<(usize, AppendBlockData<'a>)> {
        let (offset, file_id) = self.read_le_u32(offset)?;
        let (offset, bd) = self.take_len(offset, self.len() - offset)?;
        let (_, block_data) = bd.take_until(0, 0, false)?;
        Ok((
            offset,
            AppendBlockData {
                file_id,
                block_data,
            },
        ))
    }
}

/// Data of ExecLoadEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/exec-load-event.html
#[derive(Debug, Clone)]
pub struct ExecLoadData {
    pub file_id: u32,
}

impl ReadAs<'_, ExecLoadData> for [u8] {
    fn read_as(&self, offset: usize) -> Result<(usize, ExecLoadData)> {
        let (offset, file_id) = self.read_le_u32(offset)?;
        debug_assert_eq!(self.len(), offset);
        Ok((offset, ExecLoadData { file_id }))
    }
}

/// Data of DeleteFileEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/delete-file-event.html
#[derive(Debug, Clone)]
pub struct DeleteFileData {
    pub file_id: u32,
}

impl ReadAs<'_, DeleteFileData> for [u8] {
    fn read_as(&self, offset: usize) -> Result<(usize, DeleteFileData)> {
        let (offset, file_id) = self.read_le_u32(offset)?;
        debug_assert_eq!(self.len(), offset);
        Ok((offset, DeleteFileData { file_id }))
    }
}

/// Data of NewLoadEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/new-load-event.html
#[derive(Debug, Clone)]
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

impl<'a> ReadAs<'a, NewLoadData<'a>> for [u8] {
    fn read_as(&'a self, offset: usize) -> Result<(usize, NewLoadData<'a>)> {
        let (offset, slave_proxy_id) = self.read_le_u32(offset)?;
        let (offset, exec_time) = self.read_le_u32(offset)?;
        let (offset, skip_lines) = self.read_le_u32(offset)?;
        let (offset, table_name_len) = self.read_u8(offset)?;
        let (offset, schema_len) = self.read_u8(offset)?;
        let (offset, num_fields) = self.read_le_u32(offset)?;
        // below is variable part
        let (offset, field_term_len) = self.read_u8(offset)?;
        let (offset, field_term) = self.take_len(offset, field_term_len as usize)?;
        let (offset, enclosed_by_len) = self.read_u8(offset)?;
        let (offset, enclosed_by) = self.take_len(offset, enclosed_by_len as usize)?;
        let (offset, line_term_len) = self.read_u8(offset)?;
        let (offset, line_term) = self.take_len(offset, line_term_len as usize)?;
        let (offset, line_start_len) = self.read_u8(offset)?;
        let (offset, line_start) = self.take_len(offset, line_start_len as usize)?;
        let (offset, escaped_by_len) = self.read_u8(offset)?;
        let (offset, escaped_by) = self.take_len(offset, escaped_by_len as usize)?;
        let (offset, opt_flags) = self.read_u8(offset)?;
        let (offset, field_name_lengths) = self.take_len(offset, num_fields as usize)?;
        let field_name_total_length =
            field_name_lengths.iter().map(|l| *l as u32).sum::<u32>() + num_fields as u32;
        let (offset, field_names) = self.take_len(offset, field_name_total_length as usize)?;
        let (offset, tn) = self.take_len(offset, table_name_len as usize + 1)?;
        let (_, table_name) = tn.take_until(0, 0, false)?;
        let (offset, sn) = self.take_len(offset, schema_len as usize + 1)?;
        let (_, schema_name) = sn.take_until(0, 0, false)?;

        let (input, fn_in) = self.take_len(offset, self.len() - offset)?;
        let (_, file_name) = fn_in.take_until(0, 0, false)?;
        Ok((
            input,
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
        ))
    }
}

/// Data of BeginLoadQueryEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/begin-load-query-event.html
#[derive(Debug, Clone)]
pub struct BeginLoadQueryData<'a> {
    pub file_id: u32,
    // below is variable part
    pub block_data: &'a [u8],
}

impl<'a> ReadAs<'a, BeginLoadQueryData<'a>> for [u8] {
    fn read_as(&'a self, offset: usize) -> Result<(usize, BeginLoadQueryData<'a>)> {
        let (offset, file_id) = self.read_le_u32(offset)?;
        let (offset, block_data) = self.take_len(offset, self.len() - offset)?;
        Ok((
            offset,
            BeginLoadQueryData {
                file_id,
                block_data,
            },
        ))
    }
} 

/// Data of ExecuteLoadQueryEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/execute-load-query-event.html
/// there is conflicts compared to another resource,
/// https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
/// after checking real data in binlog, the second resource seems correct
/// payload will be lazy evaluated via separate module
#[derive(Debug, Clone)]
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

impl<'a> ReadAs<'a, ExecuteLoadQueryData<'a>> for [u8] {
    fn read_as(&'a self, offset: usize) -> Result<(usize, ExecuteLoadQueryData<'a>)> {
        let (offset, slave_proxy_id) = self.read_le_u32(offset)?;
        let (offset, execution_time) = self.read_le_u32(offset)?;
        let (offset, schema_length) = self.read_u8(offset)?;
        let (offset, error_code) = self.read_le_u16(offset)?;
        let (offset, status_vars_length) = self.read_le_u16(offset)?;
        let (offset, file_id) = self.read_le_u32(offset)?;
        let (offset, start_pos) = self.read_le_u32(offset)?;
        let (offset, end_pos) = self.read_le_u32(offset)?;
        let (offset, dup_handling_flags) = self.read_u8(offset)?;
        let (input, payload) = self.take_len(offset, self.len() - offset)?;
        Ok((
            input,
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
        ))
    }
}
