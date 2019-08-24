use super::network::cluster::SyncConnection;
use super::network::cluster::{Cluster, Server};
use crate::common::protocol::messages::request::{
    Close, Connect, CreateDB, DropDB, ExistDB, MsgHeader, Open,
};
use crate::common::protocol::messages::response;
use super::session::{OSession, SessionPool, SessionPoolManager};
use crate::{DatabaseType, OrientResult};
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::ops::Deref;
use std::sync::Arc;
use std::process::Output;
use std::future::Future;
use async_std::task;

#[derive(Clone)]
pub struct OrientDB {
    internal: OrientDBClientInternal,
}

impl OrientDB {
    pub async fn connect<T: Into<String>>(host: T, port: u16) -> OrientResult<OrientDB> {
        let addr: SocketAddr = format!("{}:{}", host.into(), port)
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
    //    pub async fn sessions(
//        &self,
//        db_name: &str,
//        user: &str,
//        password: &str,
//        size: Option<u32>,
//    ) -> OrientResult<SessionPool> {
//        let server = self.cluster.select();
//        SessionPoolManager::new(self.clone(), server, db_name, user, password).managed(size)
//    }
    pub async fn session(&self, db_name: &str, user: &str, password: &str) -> OrientResult<OSession> {
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
        self._server_session(server, db_name, user, password, pooled).await
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
            .send(Open::new(db_name, user, password).into()).await?
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
            W: FnOnce(AdminSession, SyncConnection) -> T,
            T: Future<Output=OrientResult<(SyncConnection, R)>>
    {
        let pooled = self.cluster.connection().await?;
        let mut conn = pooled.0;
        let response: response::Connect = conn.send(Connect::new(user, password).into()).await?.payload();
        let admin = AdminSession {
            session_id: response.session_id,
            token: response.token.clone(),
        };
        let (mut conn, result) = work(admin, conn).await?;


        conn.send_and_forget(Close::new(response.session_id, response.token).into()).await?;

        Ok(result)
    }


    pub async fn create_database(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        db_mode: DatabaseType,
    ) -> OrientResult<()> {
        self.run_as_admin(user, password, move |session, mut conn| {
            async move {
                let _open: response::CreateDB = conn
                    .send(
                        CreateDB::new(
                            MsgHeader::new(session.session_id, session.token),
                            db_name,
                            db_mode,
                        )
                            .into(),
                    ).await?
                    .payload();
                Ok((conn, ()))
            }
        }).await
    }

    pub async fn exist_database(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        db_type: DatabaseType,
    ) -> OrientResult<bool> {
        self.run_as_admin(user, password, move |session, mut conn| {
            async move {
                let exist: response::ExistDB = conn
                    .send(
                        ExistDB::new(
                            MsgHeader::new(session.session_id, session.token),
                            db_name,
                            db_type,
                        )
                            .into(),
                    ).await?
                    .payload();
                Ok((conn, exist.exist))
            }
        }).await
    }

    pub async fn drop_database(
        &self,
        db_name: &str,
        user: &str,
        password: &str,
        db_type: DatabaseType,
    ) -> OrientResult<()> {
        self.run_as_admin(user, password, move |session, mut conn| {
            async move {
                let _drop: response::DropDB = conn
                    .send(
                        DropDB::new(
                            MsgHeader::new(session.session_id, session.token),
                            db_name,
                            db_type,
                        )
                            .into(),
                    ).await?
                    .payload();
                Ok((conn, ()))
            }
        }).await
    }
}
