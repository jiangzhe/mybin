use crate::col::BinaryColumnValue;
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
    #[error("parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error("parse bool error: {0}")]
    ParseBoolError(#[from] std::str::ParseBoolError),
    #[error("parse bigdecimal error: {0}")]
    ParseBigDecimalError(#[from] bigdecimal::ParseBigDecimalError),
    #[error("parse datetime error: {0}")]
    ParseDateTimeError(#[from] chrono::ParseError),
    #[error("parse mysql time error: {0}")]
    ParseMyTimeError(String),
    #[error("column type mismatch: {0}")]
    ColumnTypeMismatch(String),
    #[error("column index out of bound: {0}")]
    ColumnIndexOutOfBound(String),
    #[error("column name not found: {0}")]
    ColumnNameNotFound(String),
}

impl Error {
    pub fn column_type_mismatch<T: AsRef<str>>(expected: T, actual: &BinaryColumnValue) -> Self {
        Error::ColumnTypeMismatch(format!(
            "expected={}, actual={:?}",
            expected.as_ref(),
            actual
        ))
    }
}
