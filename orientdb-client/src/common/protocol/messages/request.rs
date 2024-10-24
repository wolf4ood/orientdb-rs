use crate::common::types::value::OValue;
use crate::common::DatabaseType;
use std::collections::HashMap;

#[derive(Debug)]
pub struct HandShake {
    pub p_version: i16,
    pub name: String,
    pub version: String,
}

impl HandShake {
    pub fn new<T>(p_version: i16, name: T, version: T) -> HandShake
    where
        T: Into<String>,
    {
        HandShake {
            p_version,
            name: name.into(),
            version: version.into(),
        }
    }
}

impl From<HandShake> for Request {
    fn from(input: HandShake) -> Request {
        Request::HandShake(input)
    }
}

#[derive(Debug)]
pub struct MsgHeader {
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
}

impl MsgHeader {
    pub fn new(session_id: i32, token: Option<Vec<u8>>) -> MsgHeader {
        MsgHeader { session_id, token }
    }
}
// Connect Message
#[derive(Debug)]
pub struct Connect {
    pub username: String,
    pub password: String,
}

impl Connect {
    pub fn new<T>(username: T, password: T) -> Connect
    where
        T: Into<String>,
    {
        Connect {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl From<Connect> for Request {
    fn from(input: Connect) -> Request {
        Request::Connect(input)
    }
}

// Open Message
#[derive(Debug)]
pub struct Open {
    pub db: String,
    pub username: String,
    pub password: String,
}

impl Open {
    pub fn new<T>(db: T, username: T, password: T) -> Open
    where
        T: Into<String>,
    {
        Open {
            db: db.into(),
            username: username.into(),
            password: password.into(),
        }
    }
}

impl From<Open> for Request {
    fn from(input: Open) -> Request {
        Request::Open(input)
    }
}

#[derive(Debug)]
pub struct LiveQuery {
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
    pub query: String,
    pub parameters: HashMap<String, OValue>,
    pub named: bool,
}

impl LiveQuery {
    pub fn new<T: Into<String>>(
        session_id: i32,
        token: Option<Vec<u8>>,
        query: T,
        parameters: HashMap<String, OValue>,
        named: bool,
    ) -> LiveQuery {
        LiveQuery {
            session_id,
            token,
            query: query.into(),
            parameters,
            named,
        }
    }
}

impl From<LiveQuery> for Request {
    fn from(input: LiveQuery) -> Request {
        Request::LiveQuery(input)
    }
}

#[derive(Debug)]
pub struct UnsubscribeLiveQuery {
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
    pub monitor_id: i32,
}

impl UnsubscribeLiveQuery {
    pub fn new(session_id: i32, token: Option<Vec<u8>>, monitor_id: i32) -> UnsubscribeLiveQuery {
        UnsubscribeLiveQuery {
            session_id,
            token,
            monitor_id,
        }
    }
}
impl From<UnsubscribeLiveQuery> for Request {
    fn from(input: UnsubscribeLiveQuery) -> Request {
        Request::UnsubscribeLiveQuery(input)
    }
}
// Query Message
#[derive(Debug)]
pub struct Query {
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
    pub query: String,
    pub parameters: HashMap<String, OValue>,
    pub named: bool,
    pub language: String,
    pub mode: i8,
    pub page_size: i32,
}

impl Query {
    #[allow(clippy::too_many_arguments)]
    pub fn new<T>(
        session_id: i32,
        token: Option<Vec<u8>>,
        query: T,
        parameters: HashMap<String, OValue>,
        named: bool,
        language: T,
        mode: i8,
        page_size: i32,
    ) -> Query
    where
        T: Into<String>,
    {
        Query {
            session_id,
            token,
            query: query.into(),
            parameters,
            named,
            language: language.into(),
            mode,
            page_size,
        }
    }
}

impl From<Query> for Request {
    fn from(input: Query) -> Request {
        Request::Query(input)
    }
}

#[derive(Debug)]
pub struct Close {
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
}

impl Close {
    pub fn new(session_id: i32, token: Option<Vec<u8>>) -> Close {
        Close { session_id, token }
    }
}

impl From<Close> for Request {
    fn from(input: Close) -> Request {
        Request::Close(input)
    }
}

#[derive(Debug)]
pub struct QueryNext {
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
    pub query_id: String,
    pub page_size: i32,
}

impl QueryNext {
    pub fn new<T>(session_id: i32, token: Option<Vec<u8>>, query_id: T, page_size: i32) -> QueryNext
    where
        T: Into<String>,
    {
        QueryNext {
            session_id,
            token,
            query_id: query_id.into(),
            page_size,
        }
    }
}

impl From<QueryNext> for Request {
    fn from(input: QueryNext) -> Request {
        Request::QueryNext(input)
    }
}

#[derive(Debug)]
pub struct QueryClose {
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
    pub query_id: String,
}

impl QueryClose {
    pub fn new<T>(session_id: i32, token: Option<Vec<u8>>, query_id: T) -> QueryClose
    where
        T: Into<String>,
    {
        QueryClose {
            session_id,
            token,
            query_id: query_id.into(),
        }
    }
}

impl From<QueryClose> for Request {
    fn from(input: QueryClose) -> Request {
        Request::QueryClose(input)
    }
}

// CreateDB Message
#[derive(Debug)]
pub struct CreateDB {
    pub header: MsgHeader,
    pub name: String,
    pub db_mode: DatabaseType,
    pub backup: Option<String>,
}

impl CreateDB {
    pub fn new<T>(header: MsgHeader, name: T, db_mode: DatabaseType) -> CreateDB
    where
        T: Into<String>,
    {
        CreateDB {
            header,
            name: name.into(),
            db_mode,
            backup: None,
        }
    }
}

impl From<CreateDB> for Request {
    fn from(input: CreateDB) -> Request {
        Request::CreateDB(input)
    }
}

// ExistDB Message
#[derive(Debug)]
pub struct ExistDB {
    pub header: MsgHeader,
    pub name: String,
    pub db_mode: DatabaseType,
}

impl ExistDB {
    pub fn new<T>(header: MsgHeader, name: T, db_mode: DatabaseType) -> ExistDB
    where
        T: Into<String>,
    {
        ExistDB {
            header,
            name: name.into(),
            db_mode,
        }
    }
}

impl From<ExistDB> for Request {
    fn from(input: ExistDB) -> Request {
        Request::ExistDB(input)
    }
}

// DropDB Message
#[derive(Debug)]
pub struct DropDB {
    pub header: MsgHeader,
    pub name: String,
    pub db_mode: DatabaseType,
}

impl DropDB {
    pub fn new<T>(header: MsgHeader, name: T, db_mode: DatabaseType) -> DropDB
    where
        T: Into<String>,
    {
        DropDB {
            header,
            name: name.into(),
            db_mode,
        }
    }
}

impl From<DropDB> for Request {
    fn from(input: DropDB) -> Request {
        Request::DropDB(input)
    }
}

// Server Query Message
#[derive(Debug)]
pub struct ServerQuery {
    pub session_id: i32,
    pub token: Option<Vec<u8>>,
    pub query: String,
    pub parameters: HashMap<String, OValue>,
    pub named: bool,
    pub language: String,
    pub mode: i8,
    pub page_size: i32,
}

impl ServerQuery {
    #[allow(clippy::too_many_arguments)]
    pub fn new<T>(
        session_id: i32,
        token: Option<Vec<u8>>,
        query: T,
        parameters: HashMap<String, OValue>,
        named: bool,
        language: T,
        mode: i8,
        page_size: i32,
    ) -> ServerQuery
    where
        T: Into<String>,
    {
        ServerQuery {
            session_id,
            token,
            query: query.into(),
            parameters,
            named,
            language: language.into(),
            mode,
            page_size,
        }
    }
}

impl From<ServerQuery> for Request {
    fn from(input: ServerQuery) -> Request {
        Request::ServerQuery(input)
    }
}

#[derive(Debug)]
pub enum Request {
    HandShake(HandShake),
    Connect(Connect),
    CreateDB(CreateDB),
    ExistDB(ExistDB),
    DropDB(DropDB),
    ServerQuery(ServerQuery),
    Open(Open),
    Query(Query),
    LiveQuery(LiveQuery),
    UnsubscribeLiveQuery(UnsubscribeLiveQuery),
    QueryNext(QueryNext),
    QueryClose(QueryClose),
    Close(Close),
}

impl Request {
    pub fn need_response(&self) -> bool {
        match self {
            Request::Close(_) => false,
            _ => true,
        }
    }
}
