use bytes_parser::bytes::ReadBytes;
use bytes_parser::error::{Result, Error};
use bytes_parser::number::ReadNumber;
use bytes_parser::ReadFrom;
use bitflags::bitflags;

/// Data of UserVarEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/user-var-event.html
#[derive(Debug, Clone)]
pub struct UserVarData<'a> {
    pub name_length: u32,
    pub name: &'a [u8],
    pub is_null: u8,
    // value is lazy evaluated
    pub value: &'a [u8],
}

impl<'a> ReadFrom<'a, UserVarData<'a>> for [u8] {
    fn read_from(&'a self, offset: usize) -> Result<(usize, UserVarData<'a>)> {
        let (offset, name_length) = self.read_le_u32(offset)?;
        let (offset, name) = self.take_len(offset, name_length as usize)?;
        let (offset, is_null) = self.read_u8(offset)?;
        let (offset, value) = self.take_len(offset, self.len() - offset)?;
        Ok((
            offset,
            UserVarData {
                name_length,
                name,
                is_null,
                value,
            },
        ))
    }
}

/// value part of UserVarEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/statement_events.h#L824
#[derive(Debug, Clone)]
pub struct UserVarValue<'a> {
    pub value_type: u8,
    pub charset_num: u32,
    pub value: &'a [u8],
    pub flags: UserVarFlags,
}

// todo: extract meaningful value from value byte arrary
impl<'a> ReadFrom<'a, UserVarValue<'a>> for [u8] {
    fn read_from(&'a self, offset: usize) -> Result<(usize, UserVarValue<'a>)> {
        let (offset, value_type) = self.read_u8(offset)?;
        let (offset, charset_num) = self.read_le_u32(offset)?;
        let (offset, value_len) = self.read_le_u32(offset)?;
        let (offset, value) = self.take_len(offset, value_len as usize)?;
        let (offset, flags) = if offset < self.len() {
            self.read_u8(offset)?
        } else {
            (offset, 0)
        };
        let flags = UserVarFlags::from_bits(flags)
            .ok_or_else(|| Error::ConstraintError(format!("invalid user var flags {:02x}", flags)))?;
        Ok((
            offset,
            UserVarValue {
                value_type,
                charset_num,
                value,
                flags,
            },
        ))
    }
}

bitflags! {
    pub struct UserVarFlags: u8 {
        const UNSIGNED = 0x01;
    }
}
