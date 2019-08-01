pub mod client;
mod common;
pub mod network;
pub mod protocol;
pub mod session;
pub mod statement;
pub mod types;

pub use client::OrientDB;
pub use session::OSession;
pub use types::error::OrientError;

pub type OrientResult<T> = Result<T, OrientError>;

pub use common::DatabaseType;
