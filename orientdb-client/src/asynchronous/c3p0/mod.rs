mod builder;
mod config;
mod error;
mod pool;

pub use builder::PoolBuilder;
pub use error::C3p0Error;
pub use pool::{ConnectionManger, Pool, PooledConnection};

pub type C3p0Result<T> = Result<T, C3p0Error>;
