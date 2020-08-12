use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("incomplete input: {0:?}")]
    InputIncomplete(Needed),
    #[error("unavailable output")]
    OutputUnavailable,
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("constraint error: {0}")]
    ConstraintError(String),
}

#[derive(Debug)]
pub enum Needed {
    Unknown,
    Size(usize),
}
