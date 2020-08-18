//! meaningful data structures and parsing logic of QueryEvent
use bytes_parser::ReadFrom;
use bytes_parser::number::ReadNumber;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::error::{Error, Result};
use bitflags::bitflags;

/// Data of QueryEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/query-event.html
/// only support binlog v4 (with status_vars_length at end of post header)
#[derive(Debug, Clone)]
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

impl<'a> ReadFrom<'a, QueryData<'a>> for [u8] {
    
    fn read_from(&'a self, offset: usize) -> Result<(usize, QueryData<'a>)> {
        let (offset, slave_proxy_id) = self.read_le_u32(offset)?;
        let (offset, execution_time) = self.read_le_u32(offset)?;
        let (offset, schema_length) = self.read_u8(offset)?;
        let (offset, error_code) = self.read_le_u16(offset)?;
        let (offset, status_vars_length) = self.read_le_u16(offset)?;
        // 13(4+4+1+2+2) bytes consumed
        // do not parse status_vars in this stage
        let (offset, status_vars) = self.take_len(offset, status_vars_length as usize)?;
        let (offset, schema) = self.take_len(offset, schema_length as usize)?;
        let (offset, _) = self.take_len(offset, 1usize)?;
        let (offset, query) = self.take_len(offset, self.len() - offset)?;
        Ok((
            offset,
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
        ))
    }
}

#[derive(Debug, Clone)]
pub enum QueryStatusVar {
    Flags2Code(u32),
    SqlModeCode(u64),
    Catalog(String),
    AutoIncrement { inc: u16, offset: u16 },
    // https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html
    CharsetCode { client: u16, conn: u16, server: u16 },
    TimeZoneCode(String),
    CatalogNzCode(String),
    LcTimeNamesCode(u16),
    CharsetDatabaseCode(u16),
    TableMapForUpdateCode(u64),
    MasterDataWrittenCode(u32),
    Invokers { username: String, hostname: String },
    UpdatedDbNames(Vec<String>),
    // actually is 3-byte int, but Rust only has 4-byte int
    MicroSeconds(u32),
}

#[derive(Debug, Clone)]
pub struct QueryStatusVars(Vec<QueryStatusVar>);

impl std::ops::Deref for QueryStatusVars {
    type Target = Vec<QueryStatusVar>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReadFrom<'_, QueryStatusVars> for [u8] {
    fn read_from(&self, offset: usize) -> Result<(usize, QueryStatusVars)> {
        let mut vars = Vec::new();
        let mut offset = offset;
        while offset < self.len() {
            let (os1, key) = self.read_u8(offset)?;
            let (os1, var) = match key {
                0x00 => {
                    // 4 bytes
                    let (os1, flags2code) = self.read_le_u32(os1)?;
                    (os1, QueryStatusVar::Flags2Code(flags2code))
                }
                0x01 => {
                    // 8 bytes
                    let (os1, sqlmodecode) = self.read_le_u64(os1)?;
                    (os1, QueryStatusVar::SqlModeCode(sqlmodecode))
                }
                0x02 => {
                    // 1 byte length + str + '\0'
                    let (os1, len) = self.read_u8(os1)?;
                    let (os1, s) = self.take_len(os1, len as usize)?;
                    let (os1, _) = self.take_len(os1, 1)?;
                    (os1, QueryStatusVar::Catalog(String::from_utf8_lossy(s).to_string()))
                }
                0x03 => {
                    // 2 + 2
                    let (os1, inc) = self.read_le_u16(os1)?;
                    let (os1, offset) = self.read_le_u16(os1)?;
                    (os1, QueryStatusVar::AutoIncrement { inc, offset })
                }
                0x04 => {
                    // 2 + 2 + 2
                    let (os1, client) = self.read_le_u16(os1)?;
                    let (os1, conn) = self.read_le_u16(os1)?;
                    let (os1, server) = self.read_le_u16(os1)?;
                    (
                        os1,
                        QueryStatusVar::CharsetCode {
                            client,
                            conn,
                            server,
                        },
                    )
                }
                0x05 => {
                    // 1 + n
                    let (os1, len) = self.read_u8(os1)?;
                    let (os1, s) = self.take_len(os1, len as usize)?;
                    (
                        os1,
                        QueryStatusVar::TimeZoneCode(String::from_utf8_lossy(s).to_string()),
                    )
                }
                0x06 => {
                    // 1 + n
                    let (os1, len) = self.read_u8(os1)?;
                    let (os1, s) = self.take_len(os1, len as usize)?;
                    (
                        os1,
                        QueryStatusVar::CatalogNzCode(String::from_utf8_lossy(s).to_string()),
                    )
                }
                0x07 => {
                    // 2 bytes
                    let (os1, code) = self.read_le_u16(os1)?;
                    (os1, QueryStatusVar::LcTimeNamesCode(code))
                }
                0x08 => {
                    // 2 bytes
                    let (os1, code) = self.read_le_u16(os1)?;
                    (os1, QueryStatusVar::CharsetDatabaseCode(code))
                }
                0x09 => {
                    // 8 bytes
                    let (os1, code) = self.read_le_u64(os1)?;
                    (os1, QueryStatusVar::TableMapForUpdateCode(code))
                }
                0x0a => {
                    // 4 bytes
                    let (os1, code) = self.read_le_u32(os1)?;
                    (os1, QueryStatusVar::MasterDataWrittenCode(code))
                }
                0x0b => {
                    // 1 + n + 1 + n
                    let (os1, lun) = self.read_u8(os1)?; 
                    let (os1, un) = self.take_len(os1, lun as usize)?;
                    let (os1, lhn) = self.read_u8(os1)?;
                    let (in1, hn) = self.take_len(os1, lhn as usize)?;
                    (
                        in1,
                        QueryStatusVar::Invokers {
                            username: String::from_utf8_lossy(un).to_string(),
                            hostname: String::from_utf8_lossy(hn).to_string(),
                        },
                    )
                }
                0x0c => {
                    // 1 + n (null-term string)
                    let (mut os1, count) = self.read_u8(os1)?;
                    let mut names = Vec::new();
                    for _ in 0..count {
                        let (os2, s) = self.take_until(os1, 0, false)?;
                        names.push(String::from_utf8_lossy(s).to_string());
                        os1 = os2;
                    }
                    (os1, QueryStatusVar::UpdatedDbNames(names))
                }
                0x0d => {
                    // 3 bytes
                    let (os1, ms) = self.read_le_u24(os1)?;
                    (os1, QueryStatusVar::MicroSeconds(ms))
                }
                _ => return Err(Error::ConstraintError(format!("invalid key of query status var: {}", key))),
            };
            vars.push(var);
            offset = os1;
        }
        Ok((offset, QueryStatusVars(vars)))
    }
}

bitflags! {
    pub struct Flags2Code: u32 {
        const AUTO_IS_NULL      = 0x0000_4000;
        const NOT_AUTOCOMMIT    = 0x0008_0000;
        const NO_FOREIGN_KEY_CHECKS = 0x0400_0000;
        const RELAXED_UNIQUE_CHECKS = 0x0800_0000;
    }
}

bitflags! {
    pub struct SqlModeCode: u64 {
        const REAL_AS_FLOAT     = 0x0000_0001;
        const PIPES_AS_CONCAT   = 0x0000_0002;
        const ANSI_QUOTES       = 0x0000_0004;
        const IGNORE_SPACE      = 0x0000_0008;
        const NOT_USED          = 0x0000_0010;
        const ONLY_FULL_GROUP_BY    = 0x0000_0020;
        const NO_UNSIGNED_SUBTRACTION   = 0x0000_0040;
        const NO_DIR_IN_CREATE  = 0x0000_0080;
        const POSTGRESQL        = 0x0000_0100;
        const ORACLE            = 0x0000_0200;
        const MSSQL             = 0x0000_0400;
        const DB2               = 0x0000_0800;
        const MAXDB             = 0x0000_1000;
        const NO_KEY_OPTIONS    = 0x0000_2000;
        const NO_TABLE_OPTIONS  = 0x0000_4000;
        const NO_FIELD_OPTIONS  = 0x0000_8000;
        const MYSQL323          = 0x0001_0000;
        const MYSQL40           = 0x0002_0000;
        const ANSI              = 0x0004_0000;
        const NO_AUTO_VALUE_ON_ZERO = 0x0008_0000;
        const NO_BACKSLASH_ESCAPES  = 0x0010_0000;
        const STRICT_TRANS_TABLES   = 0x0020_0000;
        const STRICT_ALL_TABLES = 0x0040_0000;
        const NO_ZERO_IN_DATE   = 0x0080_0000;
        const NO_ZERO_DATE      = 0x0100_0000;
        const INVALID_DATES     = 0x0200_0000;
        const ERROR_FOR_DIVISION_BY_ZERO    = 0x0400_0000;
        const TRANDITIONAL      = 0x0800_0000;
        const NO_AUTO_CREATE_USER   = 0x1000_0000;
        const HIGH_NOT_PRECEDENCE   = 0x2000_0000;
        const NO_ENGINE_SUBSTITUTION    = 0x4000_0000;
        const PAD_CHAR_TO_FULL_LENGTH   = 0x8000_0000;
    }
}
