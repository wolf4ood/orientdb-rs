use super::conn::Connection;

use crate::{OrientError, OrientResult};
use async_trait::async_trait;
use mobc::{Connection as PooledConnection, Manager, Pool};
use std::net::SocketAddr;
use std::sync::Arc;

pub type AsyncConnection = PooledConnection<ServerConnectionManager>;

#[derive(Debug)]
pub struct Cluster {
    servers: Vec<Arc<Server>>,
}

impl Cluster {
    pub(crate) fn builder() -> ClusterBuilder {
        ClusterBuilder::default()
    }

    pub(crate) async fn connection(&self) -> OrientResult<(AsyncConnection, Arc<Server>)> {
        let conn = self.servers[0]
            .connection()
            .await
            .map_err(OrientError::from)?;
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
    pub async fn build(self) -> OrientResult<Cluster> {
        let pool_max = self.pool_max;

        let mut servers = vec![];

        for server in self.servers {
            let s = Server::connect(server, pool_max).await?;
            servers.push(Arc::new(s));
        }

        Ok(Cluster { servers })
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

impl std::fmt::Debug for Server {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("Server").finish()
    }
}

impl Server {
    async fn connect(address: SocketAddr, pool_max: u32) -> OrientResult<Server> {
        let manager = ServerConnectionManager { address };
        let pool = Pool::builder().max_open(pool_max as u64).build(manager);

        Ok(Server { pool })
    }

    pub(crate) async fn connection(
        &self,
    ) -> OrientResult<PooledConnection<ServerConnectionManager>> {
        self.pool.get().await.map_err(OrientError::from)
    }
}

#[derive(Debug)]
pub struct ServerConnectionManager {
    address: SocketAddr,
}

#[async_trait]
impl Manager for ServerConnectionManager {
    type Connection = Connection;
    type Error = OrientError;

    async fn connect(&self) -> Result<Connection, OrientError> {
        Connection::connect(&self.address).await
    }

    async fn check(&self, conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
        Ok(conn)
    }
}
