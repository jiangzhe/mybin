use nom::bytes::streaming::take;
use nom::error::ParseError;
use nom::number::streaming::{le_u32, le_u8};
use nom::IResult;
use serde_derive::*;
use crate::error::Error;

/// Data of UserVarEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/user-var-event.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserVarData<'a> {
    pub name_length: u32,
    pub name: &'a [u8],
    pub is_null: u8,
    // value is lazy evaluated
    pub value: &'a [u8],
}

impl<'a> UserVarData<'a> {
    pub fn parse_value(&self) -> Result<UserVarValue<'a>, Error> {
        parse_user_var_value(self.value).map_err(|e| Error::from((self.value, e)))
    }
}

pub(crate) fn parse_user_var_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (UserVarData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(0, post_header_len);
    let (input, name_length) = le_u32(input)?;
    let (input, name) = take(name_length)(input)?;
    let (input, is_null) = le_u8(input)?;
    // let (input, value) = take(len - name_length - 5)(input)?;
    let (input, value, crc32) = if checksum {
        let (input, value) = take(len - name_length - 5 - 4)(input)?;
        let (input, crc32) = le_u32(input)?;
        (input, value, crc32)
    } else {
        let (input, value) = take(len - name_length - 5)(input)?;
        (input, value, 0)
    };
    Ok((
        input,
        (
            UserVarData {
                name_length,
                name,
                is_null,
                value,
            },
            crc32,
        ),
    ))
}

/// value part of UserVarEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/statement_events.h#L824
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserVarValue<'a> {
    pub value_type: u8,
    pub charset_num: u32,
    pub value: &'a [u8],
}

// todo: extract meaningful value from value byte arrary

pub fn parse_user_var_value<'a, E>(input: &'a [u8]) -> Result<UserVarValue<'a>, nom::Err<E>>
where
    E: ParseError<&'a [u8]>,
{
    let (input, value_type) = le_u8(input)?;
    let (input, charset_num) = le_u32(input)?;
    let (input, value_len) = le_u32(input)?;
    let (input, value) = take(value_len)(input)?;
    Ok(UserVarValue {
        value_type,
        charset_num,
        value,
    })
}
