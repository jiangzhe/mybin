use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid command code: {0}")]
    InvalidCommandCode(u8),
    #[error("invalid column type code: {0}")]
    InvalidColumnTypeCode(u32),
    #[error("invalid binlog format: {0}")]
    InvalidBinlogFormat(String),
    #[error("binlog event error: {0}")]
    BinlogEventError(String),
    #[error("binlog checksum mismatch: expected={0}, actual={1}")]
    BinlogChecksumMismatch(u32, u32),
    #[error("utf8 string error: {0}")]
    Utf8StringError(#[from] std::string::FromUtf8Error),
    #[error("utf8 str error: {0}")]
    Utf8StrError(#[from] std::str::Utf8Error),
    #[error("parse error: {0}")]
    ParseError(#[from] bytes_parser::error::Error),
}
