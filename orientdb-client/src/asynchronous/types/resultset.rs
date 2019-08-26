use crate::asynchronous::c3p0::PooledConnection;
use crate::asynchronous::network::cluster::{Server, ServerConnectionManager};
use crate::asynchronous::network::Connection;
use crate::common::protocol::messages::request::{QueryClose, QueryNext};
use crate::common::protocol::messages::response::Query;
use crate::common::protocol::messages::Response;
use crate::common::types::result::OResult;
use crate::OrientResult;
use async_std::prelude::Stream;
use futures::future::ready;
use futures::{FutureExt, TryFutureExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct PagedResultSet {
    server: Arc<Server>,
    response: Query,
    session_id: i32,
    token: Option<Vec<u8>>,
    page_size: i32,
    state: ResultState,
}

pub struct ConnFuture {
    server: Arc<Server>,
    future: Option<Box<Future<Output = OrientResult<PooledConnection<ServerConnectionManager>>>>>,
}

unsafe impl Send for ConnFuture {}

impl ConnFuture {
    fn new(server: Arc<Server>) -> ConnFuture {
        ConnFuture {
            server,
            future: None,
        }
    }
}

impl Future for ConnFuture {
    type Output = PooledConnection<ServerConnectionManager>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match &self.future {
            Some(f) => Poll::Pending,
            None => {
                let server = self.server.clone();
                let unchecked = async move { server.connection().await };

                self.future = Some(Box::new(unchecked));
                //                self.future = Some(Box::new(self.server.connection()));
                Poll::Pending
            }
        }
    }
}

pub enum ResultState {
    Looping,
    NextPage(Box<Future<Output = OrientResult<Query>> + Send>),
    //    Connecting(ConnFuture),
    //    Sending,
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
    type Item = OResult;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                ResultState::Looping => match self.response.records.pop_front() {
                    Some(r) => return Poll::Ready(Some(r)),
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
