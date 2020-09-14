use crate::col::ColumnDefinition;
use crate::Command;
use bytes::{Bytes, BytesMut};
use bytes_parser::error::{Error, Result};
use bytes_parser::{ReadBytesExt, ReadFromBytes, WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComStmtPrepare {
    pub cmd: Command,
    pub query: String,
}

impl ComStmtPrepare {
    pub fn new<S: Into<String>>(query: S) -> Self {
        Self {
            cmd: Command::StmtPrepare,
            query: query.into(),
        }
    }
}

impl WriteToBytes for ComStmtPrepare {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_bytes(self.query.as_bytes())?;
        Ok(len)
    }
}

#[derive(Debug, Clone)]
pub struct PreparedStmt {
    pub stmt_id: u32,
    pub col_defs: Vec<ColumnDefinition>,
    pub param_defs: Vec<ColumnDefinition>,
    pub n_warnings: u16,
}

#[derive(Debug, Clone)]
pub struct StmtPrepareOk {
    pub status: u8,
    pub stmt_id: u32,
    pub n_cols: u16,
    pub n_params: u16,
    // 1-byte filler: 0x00
    pub n_warnings: u16,
}

impl ReadFromBytes for StmtPrepareOk {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let status = input.read_u8()?;
        if status != 0 {
            return Err(Error::ConstraintError(format!(
                "invalid StmtPrepareOk status {:02x}",
                status
            )));
        }
        let stmt_id = input.read_le_u32()?;
        let n_cols = input.read_le_u16()?;
        let n_params = input.read_le_u16()?;
        input.read_u8()?;
        let n_warnings = input.read_le_u16()?;
        Ok(StmtPrepareOk {
            status,
            stmt_id,
            n_cols,
            n_params,
            n_warnings,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StmtPrepareColDefs(pub Vec<ColumnDefinition>);

impl StmtPrepareColDefs {
    pub fn read_from(input: &mut Bytes, cnt: usize) -> Result<Self> {
        let mut col_defs = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            let col_def = ColumnDefinition::read_from(input, false)?;
            col_defs.push(col_def);
        }
        Ok(Self(col_defs))
    }
}

#[derive(Debug, Clone)]
pub struct StmtPrepareParamDefs(pub Vec<ColumnDefinition>);

impl StmtPrepareParamDefs {
    pub fn read_from(input: &mut Bytes, cnt: usize) -> Result<Self> {
        let mut param_defs = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            let param_def = ColumnDefinition::read_from(input, false)?;
            param_defs.push(param_def);
        }
        Ok(Self(param_defs))
    }
}
