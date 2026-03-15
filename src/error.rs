use std::fmt;

#[derive(Debug)]
#[allow(dead_code)]
pub enum Error {
    S3(String),
    Decompress(String),
    Deserialize(String),
    InvalidBlock(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::S3(msg) => write!(f, "S3 error: {msg}"),
            Error::Decompress(msg) => write!(f, "Decompression error: {msg}"),
            Error::Deserialize(msg) => write!(f, "Deserialization error: {msg}"),
            Error::InvalidBlock(msg) => write!(f, "Invalid block: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, eyre::Report>;
