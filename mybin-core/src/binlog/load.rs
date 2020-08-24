use bytes::{Buf, Bytes};
use bytes_parser::error::Result;
use bytes_parser::{ReadBytesExt, ReadFromBytes};

/// Data of LoadEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/load-event.html
#[derive(Debug, Clone)]
pub struct LoadData {
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
    pub field_name_lengths: Bytes,
    pub field_names: Bytes,
    pub table_name: Bytes,
    pub schema_name: Bytes,
    pub file_name: Bytes,
}

impl ReadFromBytes for LoadData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let slave_proxy_id = input.read_le_u32()?;
        let exec_time = input.read_le_u32()?;
        let skip_lines = input.read_le_u32()?;
        let table_name_len = input.read_u8()?;
        let schema_len = input.read_u8()?;
        let num_fields = input.read_le_u32()?;
        // below is variable part
        let field_term = input.read_u8()?;
        let enclosed_by = input.read_u8()?;
        let line_term = input.read_u8()?;
        let line_start = input.read_u8()?;
        let escaped_by = input.read_u8()?;
        let opt_flags = input.read_u8()?;
        let empty_flags = input.read_u8()?;
        let field_name_lengths = input.read_len(num_fields as usize)?;
        let field_name_total_length =
            field_name_lengths.iter().map(|l| *l as u32).sum::<u32>() + num_fields as u32;
        let field_names = input.read_len(field_name_total_length as usize)?;
        let mut table_name = input.read_len(table_name_len as usize + 1)?;
        let table_name = table_name.read_until(0, false)?;
        let mut schema_name = input.read_len(schema_len as usize + 1)?;
        let schema_name = schema_name.read_until(0, false)?;
        let mut file_name = input.split_to(input.remaining());
        let file_name = file_name.read_until(0, false)?;
        Ok(LoadData {
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
        })
    }
}

/// Data of CreateFileEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/create-file-event.html
#[derive(Debug, Clone)]
pub struct CreateFileData {
    pub file_id: u32,
    // below is variable part
    pub block_data: Bytes,
}

impl ReadFromBytes for CreateFileData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let file_id = input.read_le_u32()?;
        let mut block_data = input.split_to(input.remaining());
        let block_data = block_data.read_until(0, false)?;
        Ok(CreateFileData {
            file_id,
            block_data,
        })
    }
}

/// Data of AppendBlockEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/append-block-event.html
#[derive(Debug, Clone)]
pub struct AppendBlockData {
    pub file_id: u32,
    // below is variable part
    pub block_data: Bytes,
}

impl ReadFromBytes for AppendBlockData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let file_id = input.read_le_u32()?;
        let mut block_data = input.split_to(input.remaining());
        let block_data = block_data.read_until(0, false)?;
        Ok(AppendBlockData {
            file_id,
            block_data,
        })
    }
}

/// Data of ExecLoadEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/exec-load-event.html
#[derive(Debug, Clone)]
pub struct ExecLoadData {
    pub file_id: u32,
}

impl ReadFromBytes for ExecLoadData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let file_id = input.read_le_u32()?;
        Ok(ExecLoadData { file_id })
    }
}

/// Data of DeleteFileEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/delete-file-event.html
#[derive(Debug, Clone)]
pub struct DeleteFileData {
    pub file_id: u32,
}

impl ReadFromBytes for DeleteFileData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let file_id = input.read_le_u32()?;
        Ok(DeleteFileData { file_id })
    }
}

/// Data of NewLoadEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/new-load-event.html
#[derive(Debug, Clone)]
pub struct NewLoadData {
    pub slave_proxy_id: u32,
    pub exec_time: u32,
    pub skip_lines: u32,
    pub table_name_len: u8,
    pub schema_len: u8,
    pub num_fields: u32,
    //below is variable part
    pub field_term_len: u8,
    pub field_term: Bytes,
    pub enclosed_by_len: u8,
    pub enclosed_by: Bytes,
    pub line_term_len: u8,
    pub line_term: Bytes,
    pub line_start_len: u8,
    pub line_start: Bytes,
    pub escaped_by_len: u8,
    pub escaped_by: Bytes,
    pub opt_flags: u8,
    pub field_name_lengths: Bytes,
    pub field_names: Bytes,
    pub table_name: Bytes,
    pub schema_name: Bytes,
    pub file_name: Bytes,
}

impl ReadFromBytes for NewLoadData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let slave_proxy_id = input.read_le_u32()?;
        let exec_time = input.read_le_u32()?;
        let skip_lines = input.read_le_u32()?;
        let table_name_len = input.read_u8()?;
        let schema_len = input.read_u8()?;
        let num_fields = input.read_le_u32()?;
        // below is variable part
        let field_term_len = input.read_u8()?;
        let field_term = input.read_len(field_term_len as usize)?;
        let enclosed_by_len = input.read_u8()?;
        let enclosed_by = input.read_len(enclosed_by_len as usize)?;
        let line_term_len = input.read_u8()?;
        let line_term = input.read_len(line_term_len as usize)?;
        let line_start_len = input.read_u8()?;
        let line_start = input.read_len(line_start_len as usize)?;
        let escaped_by_len = input.read_u8()?;
        let escaped_by = input.read_len(escaped_by_len as usize)?;
        let opt_flags = input.read_u8()?;
        let field_name_lengths = input.read_len(num_fields as usize)?;
        let field_name_total_length =
            field_name_lengths.iter().map(|l| *l as u32).sum::<u32>() + num_fields as u32;
        let field_names = input.read_len(field_name_total_length as usize)?;
        let mut table_name = input.read_len(table_name_len as usize + 1)?;
        let table_name = table_name.read_until(0, false)?;
        let mut schema_name = input.read_len(schema_len as usize + 1)?;
        let schema_name = schema_name.read_until(0, false)?;
        let mut file_name = input.split_to(input.remaining());
        let file_name = file_name.read_until(0, false)?;
        Ok(NewLoadData {
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
        })
    }
}

/// Data of BeginLoadQueryEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/begin-load-query-event.html
#[derive(Debug, Clone)]
pub struct BeginLoadQueryData {
    pub file_id: u32,
    // below is variable part
    pub block_data: Bytes,
}

impl ReadFromBytes for BeginLoadQueryData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let file_id = input.read_le_u32()?;
        let block_data = input.split_to(input.remaining());
        Ok(BeginLoadQueryData {
            file_id,
            block_data,
        })
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
pub struct ExecuteLoadQueryData {
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
    pub payload: Bytes,
}

impl ReadFromBytes for ExecuteLoadQueryData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let slave_proxy_id = input.read_le_u32()?;
        let execution_time = input.read_le_u32()?;
        let schema_length = input.read_u8()?;
        let error_code = input.read_le_u16()?;
        let status_vars_length = input.read_le_u16()?;
        let file_id = input.read_le_u32()?;
        let start_pos = input.read_le_u32()?;
        let end_pos = input.read_le_u32()?;
        let dup_handling_flags = input.read_u8()?;
        let payload = input.split_to(input.remaining());
        Ok(ExecuteLoadQueryData {
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
        })
    }
}
