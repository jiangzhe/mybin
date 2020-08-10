//! meaningful data structures and parsing logic of QueryEvent
use nom::bytes::streaming::{take, take_till};
use nom::error::ParseError;
use nom::multi::length_data;
use nom::number::streaming::{le_u16, le_u24, le_u32, le_u64, le_u8};
use nom::IResult;
use serde_derive::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

pub(crate) fn parse_query_status_vars<'a, E>(
    input: &'a [u8],
) -> IResult<&'a [u8], Vec<QueryStatusVar>, E>
where
    E: ParseError<&'a [u8]>,
{
    let mut vars = Vec::new();
    let mut input = input;
    while !input.is_empty() {
        let (in1, key) = le_u8(input)?;
        let (in2, var) = match key {
            0x00 => {
                // 4 bytes
                let (in1, flags2code) = le_u32(in1)?;
                (in1, QueryStatusVar::Flags2Code(flags2code))
            }
            0x01 => {
                // 8 bytes
                let (in1, sqlmodecode) = le_u64(in1)?;
                (in1, QueryStatusVar::SqlModeCode(sqlmodecode))
            }
            0x02 => {
                // 1 byte length + str + '\0'
                let (in1, s) = length_data(le_u8)(in1)?;
                let (in1, _) = take(1usize)(in1)?;
                (
                    in1,
                    QueryStatusVar::Catalog(String::from_utf8_lossy(s).to_string()),
                )
            }
            0x03 => {
                // 2 + 2
                let (in1, inc) = le_u16(in1)?;
                let (in1, offset) = le_u16(in1)?;
                (in1, QueryStatusVar::AutoIncrement { inc, offset })
            }
            0x04 => {
                // 2 + 2 + 2
                let (in1, client) = le_u16(in1)?;
                let (in1, conn) = le_u16(in1)?;
                let (in1, server) = le_u16(in1)?;
                (
                    in1,
                    QueryStatusVar::CharsetCode {
                        client,
                        conn,
                        server,
                    },
                )
            }
            0x05 => {
                // 1 + n
                let (in1, s) = length_data(le_u8)(in1)?;
                (
                    in1,
                    QueryStatusVar::TimeZoneCode(String::from_utf8_lossy(s).to_string()),
                )
            }
            0x06 => {
                // 1 + n
                let (in1, s) = length_data(le_u8)(in1)?;
                (
                    in1,
                    QueryStatusVar::CatalogNzCode(String::from_utf8_lossy(s).to_string()),
                )
            }
            0x07 => {
                // 2 bytes
                let (in1, code) = le_u16(in1)?;
                (in1, QueryStatusVar::LcTimeNamesCode(code))
            }
            0x08 => {
                // 2 bytes
                let (in1, code) = le_u16(in1)?;
                (in1, QueryStatusVar::CharsetDatabaseCode(code))
            }
            0x09 => {
                // 8 bytes
                let (in1, code) = le_u64(in1)?;
                (in1, QueryStatusVar::TableMapForUpdateCode(code))
            }
            0x0a => {
                // 4 bytes
                let (in1, code) = le_u32(in1)?;
                (in1, QueryStatusVar::MasterDataWrittenCode(code))
            }
            0x0b => {
                // 1 + n + 1 + n
                let (in1, un) = length_data(le_u8)(in1)?;
                let (in1, hn) = length_data(le_u8)(in1)?;
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
                let (mut in1, count) = le_u8(in1)?;
                let mut names = Vec::new();
                for _ in 0..count {
                    let (in2, s) = take_till(|b| b == 0x00)(in1)?;
                    names.push(String::from_utf8_lossy(s).to_string());
                    // eat '\0'
                    in1 = take(1usize)(in2)?.0;
                }
                (in1, QueryStatusVar::UpdatedDbNames(names))
            }
            0x0d => {
                // 3 bytes
                let (in1, ms) = le_u24(in1)?;
                (in1, QueryStatusVar::MicroSeconds(ms))
            }
            _ => panic!("unexpected key of QueryStatusVar"),
        };
        vars.push(var);
        input = in2;
    }
    Ok((input, vars))
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
