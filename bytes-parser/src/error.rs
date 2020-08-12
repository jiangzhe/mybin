use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("incomplete input: {0:?}")]
    Incomplete(Needed),
}

#[derive(Debug)]
pub enum Needed {
    Unknown,
    Size(usize),
}
