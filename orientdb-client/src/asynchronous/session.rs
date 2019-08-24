use super::network::cluster::{Cluster, Server};

use super::c3p0::{ConnectionManger, Pool, PooledConnection};
use super::client::OrientDBClientInternal;
use super::statement::Statement;
use crate::common::protocol::messages::request::{Close, Query};
use crate::common::protocol::messages::response;
use crate::{OrientError, OrientResult};
use async_trait::async_trait;
use std::sync::Arc;

use super::types::resultset::PagedResultSet;

use crate::asynchronous::c3p0::{C3p0Error, C3p0Result};
use crate::common::types::OResult;
use async_std::stream::Stream;

pub struct OSession {
    pub client_id: i32,
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
    // Unused for now. After can be used to switch server in case of failure
    #[allow(unused_variables)]
    #[allow(dead_code)]
    cluster: Arc<Cluster>,
    server: Arc<Server>,
    pooled: bool,
}

impl OSession {
    pub(crate) fn new(
        client_id: i32,
        session_id: i32,
        token: Option<Vec<u8>>,
        cluster: Arc<Cluster>,
        server: Arc<Server>,
        pooled: bool,
    ) -> OSession {
        OSession {
            client_id,
            session_id,
            token,
            cluster,
            server,
            pooled,
        }
    }

    pub fn query<T: Into<String>>(&self, query: T) -> Statement {
        Statement::new(self, query.into())
    }

    pub fn command<T: Into<String>>(&self, command: T) -> Statement {
        Statement::new(self, command.into()).mode(0)
    }

    pub fn script_sql<T: Into<String>>(&self, script: T) -> Statement {
        Statement::new(self, script.into())
            .mode(2)
            .language(String::from("SQL"))
    }
    pub fn script<T: Into<String>, S: Into<String>>(&self, script: T, language: S) -> Statement {
        Statement::new(self, script.into())
            .mode(2)
            .language(language.into())
    }

    pub(crate) async fn run(&self, query: Query) -> OrientResult<impl Stream<Item = OResult>> {
        let mut conn = self.server.connection().await?;
        let page_size = query.page_size;
        let q: response::Query = conn.send(query.into()).await?.payload();

        Ok(PagedResultSet::new(
            self.server.clone(),
            q,
            self.session_id,
            self.token.clone(),
            page_size,
        ))
    }
    /// Close a session
    pub async fn close(self) -> OrientResult<()> {
        if !self.pooled {
            return self.force_close().await;
        }
        Ok(())
    }

    async fn force_close(mut self) -> OrientResult<()> {
        let mut conn = self.server.connection().await?;
        self.session_id = -1;
        self.token = None;
        conn.send_and_forget(Close::new(self.session_id, self.token).into())
            .await?;
        Ok(())
    }
}

pub struct SessionPoolManager {
    db: String,
    user: String,
    password: String,
    server: Arc<Server>,
    client: OrientDBClientInternal,
}

impl SessionPoolManager {
    pub(crate) fn new(
        client: OrientDBClientInternal,
        server: Arc<Server>,
        db_name: &str,
        user: &str,
        password: &str,
    ) -> SessionPoolManager {
        SessionPoolManager {
            db: String::from(db_name),
            user: String::from(user),
            password: String::from(password),
            server,
            client,
        }
    }

    pub(crate) async fn managed(self, size: Option<u32>) -> OrientResult<SessionPool> {
        let pool = Pool::builder()
            .max_size(size.unwrap_or(20))
            .build(self)
            .await?;

        Ok(SessionPool(pool))
    }
}

#[async_trait]
impl ConnectionManger for SessionPoolManager {
    type Connection = OSession;

    async fn connect(&self) -> C3p0Result<OSession> {
        self.client
            ._server_session(
                self.server.clone(),
                &self.db,
                &self.user,
                &self.password,
                true,
            )
            .await
            .map_err(|e| C3p0Error::User(Box::new(e)))
    }
}

#[derive(Clone)]
pub struct SessionPool(Pool<SessionPoolManager>);

pub type SessionPooled = PooledConnection<SessionPoolManager>;

impl SessionPool {
    pub async fn get(&self) -> OrientResult<SessionPooled> {
        self.0.get().await.map_err(OrientError::from)
    }

    pub async fn size(&self) -> u32 {
        self.0.state().await.unwrap().connections
    }
    pub async fn idle(&self) -> u32 {
        self.0.state().await.unwrap().idle_connections
    }
}
