use super::network::cluster::Server;
use crate::types::OResult;
use async_std::sync::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use crate::common::protocol::messages::request::UnsubscribeLiveQuery;
use crate::common::protocol::messages::response::LiveQueryResult;

use crate::OrientResult;
use futures::channel::mpsc::UnboundedSender;

#[derive(Debug)]
pub enum LiveResult {
    Created(OResult),
    Updated((OResult, OResult)),
    Deleted(OResult),
}

pub struct Unsubscriber {
    monitor_id: i32,
    session_id: i32,
    token: Option<Vec<u8>>,
    server: Arc<Server>,
}

impl Unsubscriber {
    pub fn new(
        monitor_id: i32,
        session_id: i32,
        token: Option<Vec<u8>>,
        server: Arc<Server>,
    ) -> Self {
        Unsubscriber {
            monitor_id,
            session_id,
            token,
            server,
        }
    }

    pub async fn unsubscribe(self) -> OrientResult<()> {
        let mut conn = self.server.connection().await?;

        conn.send_and_forget(
            UnsubscribeLiveQuery::new(self.session_id, self.token, self.monitor_id).into(),
        )
        .await?;

        Ok(())
    }
}

pub struct LiveQueryManager {
    live_queries: Mutex<HashMap<i32, UnboundedSender<OrientResult<LiveResult>>>>,
}

impl LiveQueryManager {
    pub async fn register_handler(
        &self,
        monitor_id: i32,
        sender: UnboundedSender<OrientResult<LiveResult>>,
    ) -> OrientResult<()> {
        let mut guard = self.live_queries.lock().await;
        guard.insert(monitor_id, sender);
        Ok(())
    }

    pub async fn fire_event(&self, mut evt: LiveQueryResult) -> OrientResult<()> {
        let mut guard = self.live_queries.lock().await;

        if evt.ended {
            if let Some(handler) = guard.remove(&evt.monitor_id) {
                while let Some(e) = evt.events.pop() {
                    UnboundedSender::unbounded_send(&handler, Ok(e)).unwrap();
                }
                drop(handler);
            }
        } else {
            if let Some(handler) = guard.get(&evt.monitor_id) {
                while let Some(e) = evt.events.pop() {
                    UnboundedSender::unbounded_send(handler, Ok(e)).unwrap();
                }
            }
        }

        Ok(())
    }
}
impl Default for LiveQueryManager {
    fn default() -> Self {
        LiveQueryManager {
            live_queries: Mutex::new(HashMap::new()),
        }
    }
}
