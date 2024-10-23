use crate::asynchronous::network::cluster::Server;
use crate::common::protocol::messages::request::{QueryClose, QueryNext};
use crate::common::protocol::messages::response::{Query, ServerQuery};
use crate::common::types::result::OResult;
use crate::OrientResult;
#[cfg(feature = "async-std-runtime")]
use async_std::task;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
#[cfg(feature = "tokio-runtime")]
use tokio::task;

pub struct PagedResultSet {
    server: Arc<Server>,
    response: Query,
    session_id: i32,
    token: Option<Vec<u8>>,
    page_size: i32,
    state: ResultState,
}

pub enum ResultState {
    Looping,
    NextPage(Box<dyn Future<Output = OrientResult<Query>> + Send>),
}

impl PagedResultSet {
    pub(crate) fn new(
        server: Arc<Server>,
        response: Query,
        session_id: i32,
        token: Option<Vec<u8>>,
        page_size: i32,
    ) -> PagedResultSet {
        PagedResultSet {
            server,
            response,
            session_id,
            token,
            page_size,
            state: ResultState::Looping,
        }
    }
}

impl futures::Stream for PagedResultSet {
    type Item = OrientResult<OResult>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                ResultState::Looping => match self.response.records.pop_front() {
                    Some(r) => return Poll::Ready(Some(Ok(r))),
                    None => {
                        if self.response.has_next {
                            let server = self.server.clone();

                            let next = QueryNext {
                                session_id: self.session_id,
                                token: self.token.clone(),
                                query_id: self.response.query_id.clone(),
                                page_size: self.page_size,
                            };
                            let response = async move {
                                let mut conn = server.connection().await?;
                                let response: Query = conn.send(next.into()).await?.payload();

                                Ok(response)
                            };

                            self.state = ResultState::NextPage(Box::new(response));
                        } else {
                            return Poll::Ready(None);
                        }
                    }
                },
                ResultState::NextPage(p) => {
                    let response =
                        futures::ready!(unsafe { Pin::new_unchecked(p.as_mut()) }.poll(cx));
                    self.response = response.unwrap();
                    self.state = ResultState::Looping;
                }
            };
        }
    }
}

impl Drop for PagedResultSet {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        // #[cfg(feature = "async-std-runtime")]
        // task::block_on(self.close_result());

        // #[cfg(feature = "tokio-runtime")]
        spawn_drop(self);
    }
}

// #[cfg(feature = "tokio-runtime")]
fn spawn_drop(resultset: &mut PagedResultSet) {
    let has_next = resultset.response.has_next;
    let server = resultset.server.clone();
    let query_id = resultset.response.query_id.clone();
    let session_id = resultset.session_id.clone();
    let token = resultset.token.clone();

    if has_next {
        task::spawn(async move { close_result(server, query_id, session_id, token).await });
    }
}

// #[cfg(feature = "tokio-runtime")]
async fn close_result(
    server: Arc<Server>,
    query_id: String,
    session_id: i32,
    token: Option<Vec<u8>>,
) -> OrientResult<()> {
    let mut conn = server.connection().await?;
    let msg = QueryClose::new(session_id, token, query_id);
    conn.send(msg.into()).await?;
    Ok(())
}

pub struct ServerResultSet {
    response: ServerQuery,
}

impl ServerResultSet {
    pub(crate) fn new(response: ServerQuery) -> ServerResultSet {
        ServerResultSet { response }
    }
}

impl futures::Stream for ServerResultSet {
    type Item = OrientResult<OResult>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.response.records.pop_front().map(|x| Ok(x)))
    }
}
