use crate::common::types::live::LiveResult;
use crate::common::types::result::OResult;
use std::collections::HashMap;
use std::collections::VecDeque;

#[derive(Debug)]
pub struct Response {
    pub header: Header,
    pub payload: ResponseType,
}

pub trait Payload {
    fn consume(payload: &mut ResponseType) -> Self;
}

impl Response {
    pub fn new(header: Header, payload: ResponseType) -> Response {
        Response { header, payload }
    }
    pub fn empty() -> Response {
        Response {
            header: Header {
                status: Status::OK,
                client_id: None,
                session_id: -1,
                token: None,
                op: -1,
            },
            payload: ResponseType::Empty,
        }
    }

    pub fn payload<T>(&mut self) -> T
    where
        T: Payload,
    {
        T::consume(&mut self.payload)
    }
}

#[derive(Debug)]
pub struct Open {
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
}

impl Open {
    pub fn new(session_id: i32, token: Option<Vec<u8>>) -> Open {
        Open { session_id, token }
    }
}

impl From<Open> for ResponseType {
    fn from(input: Open) -> ResponseType {
        ResponseType::Open(Some(input))
    }
}

#[derive(Debug)]
pub struct Connect {
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
}

impl Connect {
    pub fn new(session_id: i32, token: Option<Vec<u8>>) -> Connect {
        Connect { session_id, token }
    }
}

impl From<Connect> for ResponseType {
    fn from(input: Connect) -> ResponseType {
        ResponseType::Connect(Some(input))
    }
}

#[derive(Debug)]
pub struct LiveQueryResult {
    pub monitor_id: i32,
    pub ended: bool,
    pub events: Vec<LiveResult>,
}

impl LiveQueryResult {
    pub fn new(monitor_id: i32, ended: bool, events: Vec<LiveResult>) -> LiveQueryResult {
        LiveQueryResult {
            monitor_id,
            ended,
            events,
        }
    }
}

impl From<LiveQueryResult> for ResponseType {
    fn from(input: LiveQueryResult) -> ResponseType {
        ResponseType::LiveQueryResult(Some(input))
    }
}

#[derive(Debug)]
pub struct LiveQuery {
    pub monitor_id: i32,
}

impl From<LiveQuery> for ResponseType {
    fn from(input: LiveQuery) -> ResponseType {
        ResponseType::LiveQuery(Some(input))
    }
}
#[derive(Debug)]
pub struct Query {
    pub query_id: String,
    pub tx_changes: bool,
    pub execution_plan: Option<OResult>,
    pub records: VecDeque<OResult>,
    pub has_next: bool,
    pub stats: HashMap<String, i64>,
}

impl Query {
    pub fn new<T>(
        query_id: T,
        tx_changes: bool,
        execution_plan: Option<OResult>,
        records: VecDeque<OResult>,
        has_next: bool,
        stats: HashMap<String, i64>,
    ) -> Query
    where
        T: Into<String>,
    {
        Query {
            query_id: query_id.into(),
            tx_changes,
            execution_plan,
            records,
            has_next,
            stats,
        }
    }
}

impl From<Query> for ResponseType {
    fn from(input: Query) -> ResponseType {
        ResponseType::Query(Some(input))
    }
}

#[derive(Debug)]
pub struct CreateDB {}

impl From<CreateDB> for ResponseType {
    fn from(input: CreateDB) -> ResponseType {
        ResponseType::CreateDB(Some(input))
    }
}

#[derive(Debug)]
pub struct DropDB {}

impl From<DropDB> for ResponseType {
    fn from(input: DropDB) -> ResponseType {
        ResponseType::DropDB(Some(input))
    }
}

#[derive(Debug)]
pub struct ExistDB {
    pub exist: bool,
}

impl ExistDB {
    pub fn new(exist: bool) -> Self {
        ExistDB { exist }
    }
}

impl From<ExistDB> for ResponseType {
    fn from(input: ExistDB) -> ResponseType {
        ResponseType::ExistDB(Some(input))
    }
}

#[derive(Debug)]
pub struct QueryClose {}

impl From<QueryClose> for ResponseType {
    fn from(input: QueryClose) -> ResponseType {
        ResponseType::QueryClose(Some(input))
    }
}

#[derive(Debug)]
pub struct ServerQuery {
    pub query_id: String,
    pub tx_changes: bool,
    pub execution_plan: Option<OResult>,
    pub records: VecDeque<OResult>,
    pub has_next: bool,
    pub stats: HashMap<String, i64>,
}

impl ServerQuery {
    pub fn new<T>(
        query_id: T,
        tx_changes: bool,
        execution_plan: Option<OResult>,
        records: VecDeque<OResult>,
        has_next: bool,
        stats: HashMap<String, i64>,
    ) -> ServerQuery
    where
        T: Into<String>,
    {
        ServerQuery {
            query_id: query_id.into(),
            tx_changes,
            execution_plan,
            records,
            has_next,
            stats,
        }
    }
}

impl From<ServerQuery> for ResponseType {
    fn from(input: ServerQuery) -> ResponseType {
        ResponseType::ServerQuery(Some(input))
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ResponseType {
    Empty,
    Open(Option<Open>),
    Connect(Option<Connect>),
    Query(Option<Query>),
    CreateDB(Option<CreateDB>),
    ExistDB(Option<ExistDB>),
    DropDB(Option<DropDB>),
    ServerQuery(Option<ServerQuery>),
    LiveQuery(Option<LiveQuery>),
    LiveQueryResult(Option<LiveQueryResult>),
    QueryClose(Option<QueryClose>),
}

#[derive(Debug, PartialEq)]
pub enum Status {
    OK,
    ERROR,
    PUSH,
}

impl From<i8> for Status {
    fn from(status: i8) -> Self {
        match status {
            0 => Status::OK,
            3 => Status::PUSH,
            _ => Status::ERROR,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Header {
    pub status: Status,
    pub client_id: Option<i32>,
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
    pub op: i8,
}

macro_rules! impl_payload {
    ($s:ident) => {
        impl Payload for $s {
            fn consume(payload: &mut ResponseType) -> $s {
                match payload {
                    ResponseType::$s(ref mut this) => this
                        .take()
                        .expect(&format!("Response does not contain {:?}", this)),
                    _ => panic!(
                        "Cannot get an {} response from {:?}",
                        stringify!($s),
                        payload
                    ),
                }
            }
        }
    };
}

impl_payload!(Open);
impl_payload!(Query);
impl_payload!(QueryClose);
impl_payload!(CreateDB);
impl_payload!(DropDB);
impl_payload!(ExistDB);
impl_payload!(Connect);
impl_payload!(LiveQuery);
impl_payload!(LiveQueryResult);
impl_payload!(ServerQuery);
