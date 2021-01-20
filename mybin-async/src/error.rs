use bytes::Bytes;
use mybin_core::packet::ErrPacket;
use thiserror::*;

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
    #[error("empty result set")]
    EmptyResultSet,
    #[error("core error {0}")]
    CoreError(#[from] mybin_core::error::Error),
    #[error("custom error {0}")]
    CustomError(String),
}

impl From<ErrPacket> for Error {
    fn from(err: ErrPacket) -> Error {
        use bytes::Buf;
        let sql_state = match String::from_utf8(Vec::from(err.sql_state.chunk())) {
            Ok(sql_state) => sql_state,
            Err(e) => return e.into(),
        };
        let error_message = match String::from_utf8(Vec::from(err.error_message.chunk())) {
            Ok(error_message) => error_message,
            Err(e) => return e.into(),
        };
        Error::SqlError(SqlError {
            error_code: err.error_code,
            sql_state_marker: err.sql_state_marker,
            sql_state,
            error_message,
        })
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
