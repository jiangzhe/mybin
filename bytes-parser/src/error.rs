use bytes::Bytes;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("incomplete input: {0:?}")]
    InputIncomplete(Bytes, Needed),
    #[error("unavailable output")]
    OutputUnavailable,
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("constraint error: {0}")]
    ConstraintError(String),
    #[error("utf8 error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

#[derive(Debug)]
pub enum Needed {
    Unknown,
    Size(usize),
}
