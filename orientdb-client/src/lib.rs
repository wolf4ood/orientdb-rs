pub mod common;
pub mod sync;
// pub mod network;
// pub mod protocol;
// pub mod types;

#[cfg(feature = "async")]
pub mod asynchronous;

pub use common::types::error::OrientError;
pub use sync::client::OrientDB;
pub use sync::session::{OSession, SessionPool};

pub type OrientResult<T> = Result<T, OrientError>;

pub use common::DatabaseType;

pub mod types {
    pub use super::common::types::*;
}

#[cfg(feature = "async")]
pub mod aio {
    pub use crate::asynchronous::session::{OSession, SessionPool};
    pub use crate::asynchronous::OrientDB;
}
