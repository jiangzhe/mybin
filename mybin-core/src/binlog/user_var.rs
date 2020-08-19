use bytes_parser::error::{Result, Error};
use bytes_parser::{ReadBytesExt, ReadFromBytes};
use bytes::{Buf, Bytes};
use bitflags::bitflags;

/// Data of UserVarEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/user-var-event.html
#[derive(Debug, Clone)]
pub struct UserVarData {
    pub name_length: u32,
    pub name: Bytes,
    pub is_null: u8,
    // value is lazy evaluated
    pub value: Bytes,
}

impl ReadFromBytes for UserVarData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let name_length = input.read_le_u32()?;
        let name = input.read_len(name_length as usize)?;
        let is_null = input.read_u8()?;
        let value = input.split_to(input.remaining());
        Ok(UserVarData {
            name_length,
            name,
            is_null,
            value,
        })
    }
}

/// value part of UserVarEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/statement_events.h#L824
#[derive(Debug, Clone)]
pub struct UserVarValue {
    pub value_type: u8,
    pub charset_num: u32,
    pub value: Bytes,
    pub flags: UserVarFlags,
}

// todo: extract meaningful value from value byte arrary
impl ReadFromBytes for UserVarValue {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let value_type = input.read_u8()?;
        let charset_num = input.read_le_u32()?;
        let value_len = input.read_le_u32()?;
        let value = input.read_len(value_len as usize)?;
        let flags = if input.has_remaining() {
            input.read_u8()?
        } else {
            0
        };
        let flags = UserVarFlags::from_bits(flags)
            .ok_or_else(|| Error::ConstraintError(format!("invalid user var flags {:02x}", flags)))?;
        Ok(UserVarValue {
            value_type,
            charset_num,
            value,
            flags,
        })
    }
}

bitflags! {
    pub struct UserVarFlags: u8 {
        const UNSIGNED = 0x01;
    }
}
