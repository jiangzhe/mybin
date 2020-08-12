use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("incomplete input: {0:?}")]
    InputIncomplete(Vec<u8>, Needed),
    #[error("io: {0}")]
    IO(#[from] std::io::Error),
    #[error("address not found")]
    AddrNotFound,
    #[error("unavailable output")]
    OutputUnavailable,
    #[error("parse error: {0}")]
    ParseError(#[from] mybin_parser::error::Error),
    #[error("packet error: {0}")]
    PacketError(String),
    #[error("invalid command code: {0}")]
    InvalidCommandCode(u8),
}

#[derive(Debug, Clone)]
pub enum Needed {
    Unknown,
    Size(usize),
}

pub type Result<T> = std::result::Result<T, Error>;
