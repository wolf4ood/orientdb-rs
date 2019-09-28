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

#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) pool_size: u32,
}

impl Default for ConnectionOptions {
    fn default() -> ConnectionOptions {
        ConnectionOptions {
            host: String::from("localhost"),
            port: 2424,
            pool_size: 10,
        }
    }
}

impl ConnectionOptions {
    pub fn builder() -> ConnectionOptionsBuilder {
        ConnectionOptionsBuilder(ConnectionOptions::default())
    }
}

impl Into<ConnectionOptions> for (&str, u16) {
    fn into(self) -> ConnectionOptions {
        ConnectionOptions {
            host: String::from(self.0),
            port: self.1,
            ..Default::default()
        }
    }
}

impl Into<ConnectionOptions> for (String, u16) {
    fn into(self) -> ConnectionOptions {
        ConnectionOptions {
            host: self.0,
            port: self.1,
            ..Default::default()
        }
    }
}

impl Into<ConnectionOptions> for &str {
    fn into(self) -> ConnectionOptions {
        ConnectionOptions {
            host: String::from(self),
            ..Default::default()
        }
    }
}

pub struct ConnectionOptionsBuilder(ConnectionOptions);

impl ConnectionOptionsBuilder {
    pub fn host<T>(mut self, host: T) -> Self
    where
        T: Into<String>,
    {
        self.0.host = host.into();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.0.port = port;
        self
    }

    pub fn pool_size(mut self, pool_size: u32) -> Self {
        self.0.pool_size = pool_size;
        self
    }

    pub fn build(self) -> ConnectionOptions {
        self.0
    }
}
