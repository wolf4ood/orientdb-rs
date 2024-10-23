use crate::common::protocol::messages::request::{QueryClose, QueryNext};
use crate::common::protocol::messages::response::{Query, ServerQuery};
use crate::common::types::result::OResult;
use crate::sync::network::cluster::Server;
use crate::OrientResult;
use std::sync::Arc;

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

    fn fetch_next(&mut self) -> OrientResult<Query> {
        let mut conn = self.server.connection()?;

        let msg = QueryNext::new(
            self.session_id,
            self.token.clone(),
            self.response.query_id.clone(),
            self.page_size,
        );
        let response: Query = conn.send(msg.into())?.payload();
        Ok(response)
    }

    fn close_result(&mut self) -> OrientResult<()> {
        if self.response.has_next {
            if let Ok(mut conn) = self.server.connection() {
                let msg = QueryClose::new(
                    self.session_id,
                    self.token.clone(),
                    self.response.query_id.as_str(),
                );
                conn.send(msg.into())?;
                self.response.has_next = false;
            }
        }
        Ok(())
    }
}

impl ResultSet for PagedResultSet {
    fn close(mut self) -> OrientResult<()> {
        self.close_result()?;
        Ok(())
    }
}

impl Iterator for PagedResultSet {
    type Item = OrientResult<OResult>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.response.records.pop_front() {
                Some(result) => {
                    return Some(Ok(result));
                }
                None => {
                    if self.response.has_next {
                        match self.fetch_next() {
                            Ok(result) => self.response = result,
                            Err(e) => return Some(Err(e)),
                        }
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

pub trait ResultSet: Iterator<Item = OrientResult<OResult>> {
    fn close(self) -> OrientResult<()>;
}

impl Drop for PagedResultSet {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.close_result();
    }
}

pub struct ServerResultSet {
    response: ServerQuery,
}

impl ServerResultSet {
    pub(crate) fn new(response: ServerQuery) -> ServerResultSet {
        ServerResultSet { response }
    }
}

impl ResultSet for ServerResultSet {
    fn close(self) -> OrientResult<()> {
        Ok(())
    }
}

impl Iterator for ServerResultSet {
    type Item = OrientResult<OResult>;

    fn next(&mut self) -> Option<Self::Item> {
        self.response.records.pop_front().map(|x| Ok(x))
    }
}
