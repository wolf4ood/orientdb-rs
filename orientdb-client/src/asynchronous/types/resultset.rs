use crate::asynchronous::network::cluster::Server;
use crate::common::protocol::messages::request::{QueryClose, QueryNext};
use crate::common::protocol::messages::response::Query;
use crate::common::types::result::OResult;
use crate::OrientResult;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use async_std::task;
use std::task::{Context, Poll};

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

    async fn close_result(&mut self) -> OrientResult<()> {
        if self.response.has_next {
            let mut conn = self.server.connection().await?;
            let msg = QueryClose::new(
                self.session_id,
                self.token.clone(),
                self.response.query_id.as_str(),
            );
            conn.send(msg.into()).await?;
            self.response.has_next = false;
        }
        Ok(())
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

impl Drop for PagedResultSet {
    #[allow(unused_must_use)]
    fn drop(&mut self) {

        task::block_on(self.close_result());

    }
}
