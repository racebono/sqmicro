use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Sqlite { code: i32, message: String },
    Corrupt(String),
    Zstd(String),
    InvalidConfig(String),
    NotASqmicroDb,
    UnsupportedVersion(u32),
    InvalidPath(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Sqlite { code, message } => write!(f, "SQLite error ({code}): {message}"),
            Error::Corrupt(msg) => write!(f, "corrupt sqmicro database: {msg}"),
            Error::Zstd(msg) => write!(f, "zstd error: {msg}"),
            Error::InvalidConfig(msg) => write!(f, "invalid sqmicro configuration: {msg}"),
            Error::NotASqmicroDb => write!(f, "file is not a sqmicro database"),
            Error::UnsupportedVersion(v) => {
                write!(f, "unsupported sqmicro format version {v}")
            }
            Error::InvalidPath(msg) => write!(f, "invalid database path: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
