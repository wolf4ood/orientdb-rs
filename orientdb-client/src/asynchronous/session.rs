use super::network::cluster::{Cluster, Server};

use super::client::OrientDBClientInternal;
use super::live_statement::LiveStatement;
use super::statement::Statement;
use crate::common::protocol::messages::request::{Close, LiveQuery, Query};
use crate::common::protocol::messages::response;
use crate::{OrientError, OrientResult};
use std::future::Future;
use mobc::{async_trait, Connection, Manager, Pool};
use std::convert::From;
use std::sync::Arc;

use super::live::Unsubscriber;
use super::types::resultset::PagedResultSet;
use crate::common::types::OResult;
use crate::types::LiveResult;
use futures::Stream;

use futures::channel::mpsc::channel;

#[derive(Debug)]
pub struct OSessionRetry<'session>(&'session OSession);

impl<'session> OSessionRetry<'session> {
    pub fn new(session: &'session OSession) -> Self {
        OSessionRetry(session)
    }

    pub fn query<QUERY: Into<String>>(&self, query: QUERY) -> Statement {
        Statement::new(self.0, query.into())
    }

    pub fn command<COMMAND: Into<String>>(&self, command: COMMAND) -> Statement {
        Statement::new(self.0, command.into()).mode(0)
    }

    pub fn script_sql<SCRIPT: Into<String>>(&self, script: SCRIPT) -> Statement {
        Statement::new(self.0, script.into())
            .mode(2)
            .language(String::from("SQL"))
    }

    pub fn script<SCRIPT, LANGUAGE>(&self, script: SCRIPT, language: LANGUAGE) -> Statement
    where
        SCRIPT: Into<String>,
        LANGUAGE: Into<String>,
    {
        Statement::new(self.0, script.into())
            .mode(2)
            .language(language.into())
    }
}

#[derive(Debug)]
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

    pub async fn transaction<'session, FN, T, RETURN>(
        &'session self,
        retry: u32,
        f: FN,
    ) -> OrientResult<T>
    where
        RETURN: Future<Output = OrientResult<T>>,
        FN: Fn(OSessionRetry<'session>) -> RETURN,
    {
        self.with_retry(retry, |s| async {
            let _ = self.command("begin").run().await?;
            let results = f(s).await;
            let _ = self.command("commit");
            results
        })
        .await
    }
    pub async fn with_retry<'session, FN, T, RETURN>(
        &'session self,
        mut n: u32,
        f: FN,
    ) -> OrientResult<T>
    where
        RETURN: Future<Output = OrientResult<T>>,
        FN: Fn(OSessionRetry<'session>) -> RETURN,
    {
        if n == 0 {
            panic!("retry must be called with a number greater than 0")
        };
        loop {
            let retry_session = OSessionRetry::new(self);
            let result: OrientResult<T> = f(retry_session).await;
            match result {
                Ok(t) => return Ok(t),
                Err(e) => match &e {
                    OrientError::Request(r) => {
                        if n > 0 && r.code == 3 {
                            n -= 1;
                        } else {
                            return Err(e);
                        };
                        continue;
                    }
                    _ => return Err(e),
                },
            }
        }
    }

    pub fn live_query<'a, T: Into<String>>(&'a self, query: T) -> LiveStatement<'a> {
        LiveStatement::new(self, query.into())
    }

    pub(crate) async fn live_run(
        &self,
        live_query: LiveQuery,
    ) -> OrientResult<(Unsubscriber, impl Stream<Item = OrientResult<LiveResult>>)> {
        let mut conn = self.server.connection().await?;

        let (sender, receiver) = channel(10);

        let q: response::LiveQuery = conn.send(live_query.into()).await?.payload();

        conn.register_handler(q.monitor_id, sender).await?;

        let unsubscriber = Unsubscriber::new(
            q.monitor_id,
            self.session_id,
            self.token.clone(),
            self.server.clone(),
        );

        Ok((unsubscriber, receiver))
    }
    pub(crate) async fn run(
        &self,
        query: Query,
    ) -> OrientResult<impl Stream<Item = OrientResult<OResult>>> {
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

#[derive(Debug)]
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

    pub(crate) fn managed(
        self,
        _min_size: Option<u32>,
        max_size: Option<u32>,
    ) -> OrientResult<SessionPool> {
        let pool = Pool::builder()
            .max_open(max_size.unwrap_or(20) as u64)
            .build(self);

        Ok(SessionPool(pool))
    }
}

#[async_trait]
impl Manager for SessionPoolManager {
    type Connection = OSession;
    type Error = OrientError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client
            ._server_session(
                self.server.clone(),
                &self.db,
                &self.user,
                &self.password,
                true,
            )
            .await
    }

    async fn check(&self, conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
        Ok(conn)
    }
}

#[derive(Clone)]
pub struct SessionPool(Pool<SessionPoolManager>);

pub type SessionPooled = Connection<SessionPoolManager>;

impl SessionPool {
    pub async fn get(&self) -> OrientResult<SessionPooled> {
        self.0.get().await.map_err(OrientError::from)
    }

    pub async fn max(&self) -> u64 {
        self.0.state().await.max_open
    }

    pub async fn size(&self) -> u64 {
        self.0.state().await.connections
    }

    pub async fn idle(&self) -> u64 {
        self.0.state().await.idle
    }

    pub async fn used(&self) -> u64 {
        self.0.state().await.in_use
    }
}

impl From<mobc::Error<OrientError>> for OrientError {
    fn from(e: mobc::Error<OrientError>) -> OrientError {
        match e {
            mobc::Error::Inner(e) => e,
            mobc::Error::BadConn => OrientError::Generic(String::from("Async pool bad connection")),
            mobc::Error::Timeout => OrientError::Generic(String::from("Async pool timeout")),
        }
    }
}
