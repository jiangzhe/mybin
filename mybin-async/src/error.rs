use bytes::Bytes;
use mybin_core::packet::ErrPacket;
use std::convert::TryFrom;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("incomplete input: {0:?}")]
    InputIncomplete(Bytes, Needed),
    #[error("io: {0}")]
    IO(#[from] std::io::Error),
    #[error("address not found")]
    AddrNotFound,
    #[error("unavailable output")]
    OutputUnavailable,
    #[error("parse error: {0}")]
    ParseError(#[from] bytes_parser::error::Error),
    #[error("packet error: {0}")]
    PacketError(String),
    #[error("sql error: {0:?}")]
    SqlError(SqlError),
    #[error("utf8 error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
    #[error("binlog stream not ended")]
    BinlogStreamNotEnded,
}

impl TryFrom<ErrPacket> for Error {
    type Error = Error;

    fn try_from(err: ErrPacket) -> Result<Error> {
        use bytes::Buf;
        let e = Error::SqlError(SqlError {
            error_code: err.error_code,
            sql_state_marker: err.sql_state_marker,
            sql_state: String::from_utf8(Vec::from(err.sql_state.bytes()))?,
            error_message: String::from_utf8(Vec::from(err.error_message.bytes()))?,
        });
        Ok(e)
    }
}

#[derive(Debug, Clone)]
pub enum Needed {
    Unknown,
    Size(usize),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct SqlError {
    pub error_code: u16,
    pub sql_state_marker: u8,
    pub sql_state: String,
    pub error_message: String,
}
