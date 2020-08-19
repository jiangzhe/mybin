//! meaningful data structures and parsing logic of QueryEvent
use bitflags::bitflags;
use bytes_parser::error::{Error, Result};
use bytes_parser::{ReadBytesExt, ReadFromBytes};
use bytes::{Buf, Bytes};

/// Data of QueryEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/query-event.html
/// only support binlog v4 (with status_vars_length at end of post header)
#[derive(Debug, Clone)]
pub struct QueryData {
    pub slave_proxy_id: u32,
    pub exec_time: u32,
    pub schema_len: u8,
    pub error_code: u16,
    // if binlog version >= 4
    pub status_vars_len: u16,
    // below is variable part
    pub status_vars: Bytes,
    pub schema: Bytes,
    pub query: Bytes,
}

impl ReadFromBytes for QueryData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let slave_proxy_id = input.read_le_u32()?;
        let exec_time = input.read_le_u32()?;
        let schema_len = input.read_u8()?;
        let error_code = input.read_le_u16()?;
        let status_vars_len = input.read_le_u16()?;
        // 13(4+4+1+2+2) bytes consumed
        // do not parse status_vars in this stage
        let status_vars = input.read_len(status_vars_len as usize)?;
        let schema = input.read_len(schema_len as usize)?;
        input.read_len(1)?;
        let query = input.split_to(input.remaining());
        Ok(QueryData {
            slave_proxy_id,
            exec_time,
            schema_len,
            error_code,
            status_vars_len,
            status_vars,
            schema,
            query,
        })
    }
}

#[derive(Debug, Clone)]
pub enum QueryStatusVar {
    Flags2Code(u32),
    SqlModeCode(u64),
    Catalog(Bytes),
    AutoIncrement { inc: u16, offset: u16 },
    // https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html
    CharsetCode { client: u16, conn: u16, server: u16 },
    TimeZoneCode(Bytes),
    CatalogNzCode(Bytes),
    LcTimeNamesCode(u16),
    CharsetDatabaseCode(u16),
    TableMapForUpdateCode(u64),
    MasterDataWrittenCode(u32),
    Invokers { username: Bytes, hostname: Bytes },
    UpdatedDbNames(Vec<Bytes>),
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

impl ReadFromBytes for QueryStatusVars {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let mut vars = Vec::new();
        while input.has_remaining() {
            let key = input.read_u8()?;
            let var = match key {
                0x00 => {
                    // 4 bytes
                    QueryStatusVar::Flags2Code(input.read_le_u32()?)
                }
                0x01 => {
                    // 8 bytes
                    QueryStatusVar::SqlModeCode(input.read_le_u64()?)
                }
                0x02 => {
                    // 1 byte length + str + '\0'
                    let len = input.read_u8()?;
                    let s = input.read_len(len as usize)?;
                    input.read_len(1)?;
                    QueryStatusVar::Catalog(s)
                }
                0x03 => {
                    // 2 + 2
                    let inc = input.read_le_u16()?;
                    let offset = input.read_le_u16()?;
                    QueryStatusVar::AutoIncrement { inc, offset }
                }
                0x04 => {
                    // 2 + 2 + 2
                    let client = input.read_le_u16()?;
                    let conn = input.read_le_u16()?;
                    let server = input.read_le_u16()?;
                    QueryStatusVar::CharsetCode {
                        client,
                        conn,
                        server,
                    }
                }
                0x05 => {
                    // 1 + n
                    let len = input.read_u8()?;
                    let s = input.read_len(len as usize)?;
                    QueryStatusVar::TimeZoneCode(s)
                }
                0x06 => {
                    // 1 + n
                    let len = input.read_u8()?;
                    let s = input.read_len(len as usize)?;
                    QueryStatusVar::CatalogNzCode(s)
                }
                0x07 => {
                    // 2 bytes
                    QueryStatusVar::LcTimeNamesCode(input.read_le_u16()?)
                }
                0x08 => {
                    // 2 bytes
                    QueryStatusVar::CharsetDatabaseCode(input.read_le_u16()?)
                }
                0x09 => {
                    // 8 bytes
                    QueryStatusVar::TableMapForUpdateCode(input.read_le_u64()?)
                }
                0x0a => {
                    // 4 bytes
                    QueryStatusVar::MasterDataWrittenCode(input.read_le_u32()?)
                }
                0x0b => {
                    // 1 + n + 1 + n
                    let lun = input.read_u8()?;
                    let username = input.read_len(lun as usize)?;
                    let lhn = input.read_u8()?;
                    let hostname = input.read_len(lhn as usize)?;
                    QueryStatusVar::Invokers {
                        username,
                        hostname,
                    }
                }
                0x0c => {
                    // 1 + n (null-term string)
                    let cnt = input.read_u8()?;
                    let mut names = Vec::new();
                    for _ in 0..cnt {
                        let s = input.read_until(0, false)?;
                        names.push(s);
                    }
                    QueryStatusVar::UpdatedDbNames(names)
                }
                0x0d => {
                    // 3 bytes
                    let ms = input.read_le_u24()?;
                    QueryStatusVar::MicroSeconds(ms)
                }
                _ => {
                    return Err(Error::ConstraintError(format!(
                        "invalid key of query status var: {}",
                        key
                    )))
                }
            };
            vars.push(var);
        }
        Ok(QueryStatusVars(vars))
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
