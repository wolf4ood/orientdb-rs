use super::network::cluster::SyncConnection;
use super::network::cluster::{Cluster, Server};
use crate::common::protocol::messages::request::{
    Close, Connect, CreateDB, DropDB, ExistDB, MsgHeader, Open, ServerQuery,
};
use crate::common::protocol::messages::response;
use crate::common::ConnectionOptions;
use crate::sync::server_statement::ServerStatement;
use crate::sync::session::{OSession, SessionPool, SessionPoolManager};
use crate::sync::types::resultset::{ResultSet, ServerResultSet};
use crate::{DatabaseType, OrientResult};
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone)]
pub struct OrientDB {
    internal: OrientDBClientInternal,
}

impl OrientDB {
    pub fn connect<T: Into<ConnectionOptions>>(options: T) -> OrientResult<OrientDB> {
        let opts = options.into();

        let addr: SocketAddr = format!("{}:{}", opts.host, opts.port)
            .to_socket_addrs()?
            .next()
            .expect("Cannot parse socket address");

        let cluster = Cluster::builder()
            .add_server(addr)
            .pool_max(opts.pool_size)
            .build();

        let internal = OrientDBClientInternal {
            cluster: Arc::new(cluster),
        };

        Ok(OrientDB { internal })
    }
}

#[derive(Clone)]
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
    pub fn sessions(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        size: Option<u32>,
    ) -> OrientResult<SessionPool> {
        let server = self.cluster.select();
        SessionPoolManager::new(self.clone(), server, db_name, user, password).managed(size)
    }
    pub fn session(&self, db_name: &str, user: &str, password: &str) -> OrientResult<OSession> {
        self._session(db_name, user, password, false)
    }
    pub(crate) fn _session(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        pooled: bool,
    ) -> OrientResult<OSession> {
        let server = self.cluster.select();
        self._server_session(server, db_name, user, password, pooled)
    }
    pub(crate) fn _server_session(
        &self,
        server: Arc<Server>,
        db_name: &str,
        user: &str,
        password: &str,
        pooled: bool,
    ) -> OrientResult<OSession> {
        let mut conn = server.connection()?;

        let response: response::Open = conn
            .send(Open::new(db_name, user, password).into())?
            .payload();

        Ok(OSession::new(
            -1,
            response.session_id,
            response.token,
            self.cluster.clone(),
            server,
            pooled,
        ))
    }

    fn run_as_admin<R, W>(&self, user: &str, password: &str, work: W) -> OrientResult<R>
    where
        W: FnOnce(AdminSession, &mut SyncConnection) -> OrientResult<R>,
    {
        let pooled = self.cluster.connection()?;
        let mut conn = pooled.0;
        let response: response::Connect = conn.send(Connect::new(user, password).into())?.payload();
        let admin = AdminSession {
            session_id: response.session_id,
            token: response.token.clone(),
        };
        let result = work(admin, &mut conn);

        conn.send_and_forget(Close::new(response.session_id, response.token).into())?;

        result
    }
    pub fn create_database(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        db_mode: DatabaseType,
    ) -> OrientResult<()> {
        self.run_as_admin(user, password, move |session, conn| {
            let _open: response::CreateDB = conn
                .send(
                    CreateDB::new(
                        MsgHeader::new(session.session_id, session.token),
                        db_name,
                        db_mode,
                    )
                    .into(),
                )?
                .payload();
            Ok(())
        })
    }

    pub fn exist_database(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        db_type: DatabaseType,
    ) -> OrientResult<bool> {
        self.run_as_admin(user, password, move |session, conn| {
            let exist: response::ExistDB = conn
                .send(
                    ExistDB::new(
                        MsgHeader::new(session.session_id, session.token),
                        db_name,
                        db_type,
                    )
                    .into(),
                )?
                .payload();
            Ok(exist.exist)
        })
    }

    pub fn drop_database(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        db_type: DatabaseType,
    ) -> OrientResult<()> {
        self.run_as_admin(user, password, move |session, conn| {
            let _drop: response::DropDB = conn
                .send(
                    DropDB::new(
                        MsgHeader::new(session.session_id, session.token),
                        db_name,
                        db_type,
                    )
                    .into(),
                )?
                .payload();
            Ok(())
        })
    }

    pub fn execute(
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

    pub(crate) fn run(&self, stmt: ServerStatement) -> OrientResult<impl ResultSet> {
        let user = stmt.user.clone();
        let pwd = stmt.password.clone();
        self.run_as_admin(&user, &pwd, move |session, conn| {
            let response: response::ServerQuery = conn
                .send(stmt.into_query(session.session_id, session.token).into())?
                .payload();
            Ok(ServerResultSet::new(response))
        })
    }
}
