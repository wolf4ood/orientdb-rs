use super::conn::Connection;

use super::super::c3p0::{ConnectionManger, Pool, PooledConnection};

use crate::{OrientError, OrientResult};
use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;
use futures;
use futures::Future;
use crate::asynchronous::c3p0::{C3p0Result, C3p0Error};

pub type SyncConnection = PooledConnection<ServerConnectionManager>;

pub struct Cluster {
    servers: Vec<Arc<Server>>,
}

impl Cluster {
    pub(crate) fn builder() -> ClusterBuilder {
        ClusterBuilder::default()
    }

    pub(crate) async fn connection(&self) -> OrientResult<(SyncConnection, Arc<Server>)> {
        let conn = self.servers[0].connection().await.map_err(OrientError::from)?;
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

impl Server {
    async fn connect(address: SocketAddr, pool_max: u32) -> OrientResult<Server> {
        let manager = ServerConnectionManager { address };
        let pool = Pool::builder().max_size(pool_max).build(manager).await.map_err(OrientError::from)?;

        Ok(Server { pool })
    }

    pub(crate) async fn connection(&self) -> OrientResult<PooledConnection<ServerConnectionManager>> {
        self.pool.get().await.map_err(OrientError::from)
    }
}

pub struct ServerConnectionManager {
    address: SocketAddr,
}

#[async_trait]
impl ConnectionManger for ServerConnectionManager {
    type Connection = Connection;


    async fn connect(&self) -> C3p0Result<Connection> {
        Connection::connect(&self.address).await.map_err(|e| C3p0Error::User(Box::new(e)))
    }
}
