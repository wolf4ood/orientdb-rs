pub mod protocol;
pub mod types;

pub use crate::common::types::error::OrientError;

#[derive(Debug)]
pub enum DatabaseType {
    Memory,
    PLocal,
}

impl DatabaseType {
    pub fn as_str(&self) -> &str {
        match self {
            DatabaseType::Memory => "memory",
            DatabaseType::PLocal => "plocal",
        }
    }
}

pub type OrientResult<T> = Result<T, OrientError>;
