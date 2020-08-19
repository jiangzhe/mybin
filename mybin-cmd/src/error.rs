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
    #[error("unsupported binlog event: {0}")]
    UnsupportedBinlogEvent(String),
    #[error("binlog checksum mismatch: expected={0}, actual={1}")]
    BinlogChecksumMismatch(u32, u32),
    #[error("utf8 error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
    #[error("parse error: {0}")]
    ParseError(#[from] bytes_parser::error::Error),
}