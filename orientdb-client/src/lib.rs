

pub mod sync;
pub mod common;
// pub mod network;
// pub mod protocol;
// pub mod types;

#[cfg(feature = "async")]
pub mod asynchronous;

pub use sync::client::OrientDB;
pub use sync::session::OSession;
pub use common::types::error::OrientError;

pub type OrientResult<T> = Result<T, OrientError>;

pub use common::DatabaseType;



pub mod types {
    pub use super::common::types::*;
}