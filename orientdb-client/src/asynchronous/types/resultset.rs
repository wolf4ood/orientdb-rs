use crate::asynchronous::network::cluster::Server;
use crate::common::protocol::messages::request::{QueryClose, QueryNext};
use crate::common::protocol::messages::response::Query;
use crate::common::types::result::OResult;
use crate::OrientResult;
use async_std::prelude::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct PagedResultSet {
    server: Arc<Server>,
    response: Query,
    session_id: i32,
    token: Option<Vec<u8>>,
    page_size: i32,
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
        }
    }
}

impl futures::Stream for PagedResultSet {
    type Item = OResult;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.response.records.pop_front() {
            Some(r) => Poll::Ready(Some(r)),
            None => Poll::Ready(None),
        }
    }
}
