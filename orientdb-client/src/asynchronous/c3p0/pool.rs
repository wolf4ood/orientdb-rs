use super::config::Config;
use super::C3p0Result;
use super::PoolBuilder;
use async_std::sync::Mutex;
use async_std::{io, task};
use async_trait::async_trait;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use futures::channel::oneshot;
use std::collections::VecDeque;

#[async_trait]
pub trait ConnectionManger: Send + Sync + Debug + 'static {
    type Connection: Send + Debug + 'static;

    async fn connect(&self) -> C3p0Result<Self::Connection>;
}

#[derive(Debug)]
struct Connection<C> {
    conn: C,
}

#[derive(Debug)]
pub struct PooledConnection<M>
where
    M: ConnectionManger,
{
    conn: Option<Connection<M::Connection>>,
    pool: Pool<M>,
}

impl<M: ConnectionManger> Deref for PooledConnection<M> {
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn.as_ref().unwrap().conn
    }
}

impl<M: ConnectionManger> DerefMut for PooledConnection<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn.as_mut().unwrap().conn
    }
}

impl<M: ConnectionManger> Drop for PooledConnection<M> {
    fn drop(&mut self) {
        task::block_on(self.pool.push_back(self.conn.take().unwrap())).unwrap();
    }
}

struct PoolInternals<C> {
    waiting: VecDeque<oneshot::Sender<Connection<C>>>,
    conns: Vec<Connection<C>>,
    state: State,
}

struct SharedPool<M: ConnectionManger> {
    manager: M,
    config: Config,
    internals: Mutex<PoolInternals<M::Connection>>,
}

impl<M: ConnectionManger> Clone for Pool<M> {
    fn clone(&self) -> Self {
        Pool(self.0.clone())
    }
}

pub struct Pool<M: ConnectionManger>(Arc<SharedPool<M>>);

impl<M> std::fmt::Debug for Pool<M>
where
    M: ConnectionManger,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("Pool").finish()
    }
}

impl<M: ConnectionManger> Pool<M> {
    pub fn builder() -> PoolBuilder<M> {
        PoolBuilder::default()
    }

    pub(crate) fn new(manager: M, config: Config) -> Pool<M> {
        let internals = PoolInternals {
            waiting: VecDeque::new(),
            conns: Vec::with_capacity(config.max as usize),
            state: State {
                connections: 0,
                idle_connections: 0,
                pending_connections: 0,
            },
        };

        Pool(Arc::new(SharedPool {
            manager,
            config,
            internals: Mutex::new(internals),
        }))
    }

    pub(crate) async fn establish_idle_connections(&self) -> C3p0Result<()> {
        let min = self.0.config.min;
        let internals = self.0.internals.lock().await;
        let idle = internals.state.idle_connections;

        drop(internals);

        for _ in idle..min {
            add_connection(self.clone()).await?;
        }

        Ok(())
    }

    pub async fn get(&self) -> C3p0Result<PooledConnection<M>> {
        loop {
            let mut internal = self.0.internals.lock().await;
            match internal.conns.pop() {
                Some(c) => {
                    internal.state.idle_connections -= 1;
                    let pool = self.clone();
                    return Ok(PooledConnection {
                        conn: Some(c),
                        pool,
                    });
                }
                None => {
                    drop(internal);

                    if !add_connection(self.clone()).await? {
                        let (tx, rx) = oneshot::channel();
                        let mut internal = self.0.internals.lock().await;

                        internal.waiting.push_back(tx);

                        drop(internal);

                        let result = task::spawn(io::timeout(self.0.config.timeout, async move {
                            Ok(rx.await.map_err(|_| {
                                io::Error::new(io::ErrorKind::TimedOut, "Future was canceled")
                            })?)
                        }))
                        .await?;

                        return Ok(PooledConnection {
                            conn: Some(result),
                            pool: self.clone(),
                        });
                    }
                }
            }
        }
    }

    async fn push_back(&self, conn: Connection<M::Connection>) -> C3p0Result<()> {
        let mut internals = self.0.internals.lock().await;

        match internals.waiting.pop_front() {
            Some(t) => {
                t.send(conn).unwrap();
            }
            None => {
                internals.conns.push(conn);
                internals.state.idle_connections += 1;
            }
        }

        Ok(())
    }

    pub async fn state(&self) -> State {
        self.0.internals.lock().await.state.clone()
    }
}

async fn add_connection<M: ConnectionManger>(pool: Pool<M>) -> C3p0Result<bool> {
    let mut internal = pool.0.internals.lock().await;

    if internal.state.connections + internal.state.pending_connections < pool.0.config.max {
        internal.state.pending_connections += 1;

        drop(internal);

        let conn = pool.0.manager.connect().await?;

        let mut internal = pool.0.internals.lock().await;

        internal.conns.push(Connection { conn });
        internal.state.connections += 1;
        internal.state.idle_connections += 1;
        internal.state.pending_connections -= 1;

        Ok(true)
    } else {
        Ok(false)
    }
}

#[derive(Clone, Debug)]
pub struct State {
    pub connections: u32,
    pub idle_connections: u32,
    pub pending_connections: u32,
}
