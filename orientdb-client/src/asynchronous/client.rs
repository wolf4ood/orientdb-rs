use super::network::cluster::AsyncConnection;
use super::network::cluster::{Cluster, Server};
use super::session::{OSession, SessionPool, SessionPoolManager};
use crate::asynchronous::server_statement::ServerStatement;
use crate::asynchronous::types::resultset::ServerResultSet;
use crate::common::protocol::messages::request::{
    Close, Connect, CreateDB, DropDB, ExistDB, MsgHeader, Open,
};
use crate::common::protocol::messages::response;
use crate::common::types::result::OResult;
use crate::common::ConnectionOptions;
use crate::{DatabaseType, OrientResult};
use futures::Stream;
use std::future::Future;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone)]
pub struct OrientDB {
    internal: OrientDBClientInternal,
}

impl OrientDB {
    pub async fn connect<T: Into<ConnectionOptions>>(options: T) -> OrientResult<OrientDB> {
        let opts = options.into();
        let addr: SocketAddr = format!("{}:{}", opts.host, opts.port)
            .to_socket_addrs()?
            .next()
            .expect("Cannot parse socket address");

        let cluster = Cluster::builder().add_server(addr).build().await?;

        let internal = OrientDBClientInternal {
            cluster: Arc::new(cluster),
        };

        Ok(OrientDB { internal })
    }
}

#[derive(Clone, Debug)]
pub struct OrientDBClientInternal {
    cluster: Arc<Cluster>,
}

impl Deref for OrientDB {
    type Target = OrientDBClientInternal;

    fn deref(&self) -> &OrientDBClientInternal {
        &self.internal
    }
}

struct AdminSession {
    session_id: i32,
    token: Option<Vec<u8>>,
}

impl OrientDBClientInternal {
    pub async fn sessions(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        min_size: Option<u32>,
        max_size: Option<u32>,
    ) -> OrientResult<SessionPool> {
        let server = self.cluster.select();
        SessionPoolManager::new(self.clone(), server, db_name, user, password)
            .managed(min_size, max_size)
    }
    pub async fn session(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
    ) -> OrientResult<OSession> {
        self._session(db_name, user, password, false).await
    }
    pub(crate) async fn _session(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        pooled: bool,
    ) -> OrientResult<OSession> {
        let server = self.cluster.select();
        self._server_session(server, db_name, user, password, pooled)
            .await
    }
    pub(crate) async fn _server_session(
        &self,
        server: Arc<Server>,
        db_name: &str,
        user: &str,
        password: &str,
        pooled: bool,
    ) -> OrientResult<OSession> {
        let mut conn = server.connection().await?;

        let response: response::Open = conn
            .send(Open::new(db_name, user, password).into())
            .await?
            .payload();

        Ok(OSession::new(
            -1,
            response.session_id,
            response.token,
            self.cluster.clone(),
            server.clone(),
            pooled,
        ))
    }

    async fn run_as_admin<R, W, T>(&self, user: &str, password: &str, work: W) -> OrientResult<R>
    where
        W: FnOnce(AdminSession, AsyncConnection) -> T,
        T: Future<Output = OrientResult<(AsyncConnection, R)>>,
    {
        let pooled = self.cluster.connection().await?;
        let mut conn = pooled.0;
        let response: response::Connect = conn
            .send(Connect::new(user, password).into())
            .await?
            .payload();
        let admin = AdminSession {
            session_id: response.session_id,
            token: response.token.clone(),
        };
        let (mut conn, result) = work(admin, conn).await?;

        conn.send_and_forget(Close::new(response.session_id, response.token).into())
            .await?;

        Ok(result)
    }

    pub async fn create_database(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        db_mode: DatabaseType,
    ) -> OrientResult<()> {
        self.run_as_admin(user, password, move |session, mut conn| async move {
            let _open: response::CreateDB = conn
                .send(
                    CreateDB::new(
                        MsgHeader::new(session.session_id, session.token),
                        db_name,
                        db_mode,
                    )
                    .into(),
                )
                .await?
                .payload();
            Ok((conn, ()))
        })
        .await
    }

    pub async fn exist_database(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        db_type: DatabaseType,
    ) -> OrientResult<bool> {
        self.run_as_admin(user, password, move |session, mut conn| async move {
            let exist: response::ExistDB = conn
                .send(
                    ExistDB::new(
                        MsgHeader::new(session.session_id, session.token),
                        db_name,
                        db_type,
                    )
                    .into(),
                )
                .await?
                .payload();
            Ok((conn, exist.exist))
        })
        .await
    }

    pub async fn drop_database(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        db_type: DatabaseType,
    ) -> OrientResult<()> {
        self.run_as_admin(user, password, move |session, mut conn| async move {
            let _drop: response::DropDB = conn
                .send(
                    DropDB::new(
                        MsgHeader::new(session.session_id, session.token),
                        db_name,
                        db_type,
                    )
                    .into(),
                )
                .await?
                .payload();
            Ok((conn, ()))
        })
        .await
    }

    pub async fn execute(
        &self,
        user: &str,
        password: &str,
        query: &str,
    ) -> OrientResult<ServerStatement> {
        Ok(ServerStatement::new(
            self,
            user.to_string(),
            password.to_string(),
            query.to_string(),
        ))
    }

    pub(crate) async fn run(
        &self,
        stmt: ServerStatement<'_>,
    ) -> OrientResult<impl Stream<Item = OrientResult<OResult>>> {
        let user = stmt.user.clone();
        let pwd = stmt.password.clone();
        self.run_as_admin(&user, &pwd, move |session, mut conn| async move {
            let response: response::ServerQuery = conn
                .send(stmt.into_query(session.session_id, session.token).into())
                .await?
                .payload();
            Ok((conn, ServerResultSet::new(response)))
        })
        .await
    }
}
