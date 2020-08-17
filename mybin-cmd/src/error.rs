use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid command code: {0}")]
    InvalidCommandCode(u8),
    #[error("invalid column type code: {0}")]
    InvalidColumnTypeCode(u32),
    // #[error("invalid gtid range: start={0}, end={1}, last={2}")]
    // InvalidGtidRange(u64, u64, u64),
    #[error("parse error: {0}")]
    ParseError(#[from] bytes_parser::error::Error),
}