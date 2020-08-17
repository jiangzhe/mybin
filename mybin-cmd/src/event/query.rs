//! meaningful data structures and parsing logic of QueryEvent
use bytes_parser::ReadAs;
use bytes_parser::number::ReadNumber;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::error::{Error, Result};


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

impl<'a> ReadAs<'a, QueryData<'a>> for [u8] {
    
    fn read_as(&'a self, offset: usize) -> Result<(usize, QueryData<'a>)> {
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

impl ReadAs<'_, QueryStatusVars> for [u8] {
    fn read_as(&self, offset: usize) -> Result<(usize, QueryStatusVars)> {
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

#[derive(Debug, Clone, Copy)]
pub struct Flags2Code(pub u32);

impl Flags2Code {
    pub fn auto_is_null(self) -> bool {
        (self.0 & 0x00004000) == 0x00004000
    }

    pub fn not_autocommit(self) -> bool {
        (self.0 & 0x00080000) == 0x00080000
    }

    pub fn no_foreign_key_checks(self) -> bool {
        (self.0 & 0x04000000) == 0x04000000
    }

    pub fn relaxed_unique_checks(self) -> bool {
        (self.0 & 0x08000000) == 0x08000000
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SqlModeCode(pub u64);

impl SqlModeCode {
    pub fn real_as_float(self) -> bool {
        (self.0 & 0x00000001) == 0x00000001
    }

    pub fn pipes_as_concat(self) -> bool {
        (self.0 & 0x00000002) == 0x00000002
    }

    pub fn ansi_quotes(self) -> bool {
        (self.0 & 0x00000004) == 0x00000004
    }

    pub fn ignore_space(self) -> bool {
        (self.0 & 0x00000008) == 0x00000008
    }

    pub fn not_used(self) -> bool {
        (self.0 & 0x00000010) == 0x00000010
    }

    pub fn only_full_group_by(self) -> bool {
        (self.0 & 0x00000020) == 0x00000020
    }

    pub fn no_unsigned_subtraction(self) -> bool {
        (self.0 & 0x00000040) == 0x00000040
    }

    pub fn no_dir_in_create(self) -> bool {
        (self.0 & 0x00000080) == 0x00000080
    }

    pub fn postgresql(self) -> bool {
        (self.0 & 0x00000100) == 0x00000100
    }

    pub fn oracle(self) -> bool {
        (self.0 & 0x00000200) == 0x00000200
    }

    pub fn mssql(self) -> bool {
        (self.0 & 0x00000400) == 0x00000400
    }

    pub fn db2(self) -> bool {
        (self.0 & 0x00000800) == 0x00000800
    }

    pub fn maxdb(self) -> bool {
        (self.0 & 0x00001000) == 0x00001000
    }

    pub fn no_key_options(self) -> bool {
        (self.0 & 0x00002000) == 0x00002000
    }

    pub fn no_table_options(self) -> bool {
        (self.0 & 0x00004000) == 0x00004000
    }

    pub fn no_field_options(self) -> bool {
        (self.0 & 0x00008000) == 0x00008000
    }

    pub fn mysql323(self) -> bool {
        (self.0 & 0x00010000) == 0x00010000
    }

    pub fn mysql40(self) -> bool {
        (self.0 & 0x00020000) == 0x00020000
    }

    pub fn ansi(self) -> bool {
        (self.0 & 0x00040000) == 0x00040000
    }

    pub fn no_auto_value_on_zero(self) -> bool {
        (self.0 & 0x00080000) == 0x00080000
    }

    pub fn no_backslash_escapes(self) -> bool {
        (self.0 & 0x00100000) == 0x00100000
    }

    pub fn strict_trans_tables(self) -> bool {
        (self.0 & 0x00200000) == 0x00200000
    }

    pub fn strict_all_tables(self) -> bool {
        (self.0 & 0x00400000) == 0x00400000
    }

    pub fn no_zero_in_date(self) -> bool {
        (self.0 & 0x00800000) == 0x00800000
    }

    pub fn no_zero_date(self) -> bool {
        (self.0 & 0x01000000) == 0x01000000
    }

    pub fn invalid_dates(self) -> bool {
        (self.0 & 0x02000000) == 0x02000000
    }

    pub fn error_for_division_by_zero(self) -> bool {
        (self.0 & 0x04000000) == 0x04000000
    }

    pub fn tranditional(self) -> bool {
        (self.0 & 0x08000000) == 0x08000000
    }

    pub fn no_auto_create_user(self) -> bool {
        (self.0 & 0x10000000) == 0x10000000
    }

    pub fn high_not_precedence(self) -> bool {
        (self.0 & 0x20000000) == 0x20000000
    }

    pub fn no_engine_substitution(self) -> bool {
        (self.0 & 0x40000000) == 0x40000000
    }

    pub fn pad_char_to_full_length(self) -> bool {
        (self.0 & 0x80000000) == 0x80000000
    }
}
