use super::conn::Connection;

use r2d2::{ManageConnection, Pool, PooledConnection};

use crate::{OrientError, OrientResult};
use std::net::SocketAddr;
use std::sync::Arc;

pub type SyncConnection = PooledConnection<ServerConnectionManager>;

pub struct Cluster {
    servers: Vec<Arc<Server>>,
}

impl Cluster {
    pub(crate) fn builder() -> ClusterBuilder {
        ClusterBuilder::default()
    }

    pub(crate) fn connection(&self) -> OrientResult<(SyncConnection, Arc<Server>)> {
        let conn = self.servers[0].connection()?;
        Ok((conn, self.servers[0].clone()))
    }

    pub(crate) fn select(&self) -> Arc<Server> {
        self.servers[0].clone()
    }
}
pub struct ClusterBuilder {
    pool_max: u32,
    servers: Vec<SocketAddr>,
}

impl ClusterBuilder {
    pub fn build(self) -> Cluster {
        let pool_max = self.pool_max;
        let servers = self
            .servers
            .into_iter()
            .map(|s| {
                // handle unreachable servers
                Arc::new(Server::new(s, pool_max).unwrap())
            })
            .collect();
        Cluster { servers }
    }

    pub fn pool_max(mut self, pool_max: u32) -> Self {
        self.pool_max = pool_max;
        self
    }

    pub fn add_server<T: Into<SocketAddr>>(mut self, address: T) -> Self {
        self.servers.push(address.into());
        self
    }
}
impl Default for ClusterBuilder {
    fn default() -> ClusterBuilder {
        ClusterBuilder {
            pool_max: 20,
            servers: vec![],
        }
    }
}
pub struct Server {
    pool: Pool<ServerConnectionManager>,
}

impl Server {
    fn new(address: SocketAddr, pool_max: u32) -> OrientResult<Server> {
        let manager = ServerConnectionManager { address };
        let pool = Pool::builder().max_size(pool_max).build(manager)?;

        Ok(Server { pool })
    }

    pub(crate) fn connection(&self) -> OrientResult<PooledConnection<ServerConnectionManager>> {
        self.pool.get().map_err(OrientError::from)
    }
}
pub struct ServerConnectionManager {
    address: SocketAddr,
}

impl ManageConnection for ServerConnectionManager {
    type Connection = Connection;
    type Error = OrientError;

    fn connect(&self) -> OrientResult<Connection> {
        Connection::connect(&self.address)
    }

    fn is_valid(&self, _conn: &mut Connection) -> OrientResult<()> {
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Connection) -> bool {
        false
    }
}
