//! Experimental Rust client for OrientDB.
//! The driver supports sync and async.
//!
//!
//! You can use orientdb-client this lines in your `Cargo.toml`
//!
//! ```toml
//! [dependencies]
//! orientdb-client = "*"
//! ```
//!
//! Here it is an usage example:
//!
//! ```rust,no_run
//!
//! use orientdb_client::{OrientDB};
//!
//! fn main() -> Result<(), Box<std::error::Error>> {
//!    let client = OrientDB::connect("localhost",2424)?;
//!
//!    let session = client.session("demodb","admin","admin")?;
//!
//!    let results : Vec<_> = session.query("select from V where id = :param").named(&[("param", &1)]).run()?.collect();
//!
//!
//!    println!("{:?}", results);
//!
//!    Ok(())
//!}
//!
//!
//! ```
//!

pub mod common;
pub mod sync;

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
