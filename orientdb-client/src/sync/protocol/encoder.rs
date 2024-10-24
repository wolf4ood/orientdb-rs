use crate::common::protocol::buffer::OBuffer;
use crate::common::protocol::messages::request::{
    Close as ReqClose, Connect, CreateDB, DropDB, ExistDB, HandShake, LiveQuery, Open as ReqOpen,
    Query as ReqQuery, QueryClose, QueryNext, ServerQuery, UnsubscribeLiveQuery,
};

use crate::OrientError;

pub trait VersionedEncoder {
    fn encode_handshake(buf: &mut OBuffer, handshake: HandShake) -> Result<(), OrientError>;
    fn encode_open(buf: &mut OBuffer, open: ReqOpen) -> Result<(), OrientError>;
    fn encode_close(buf: &mut OBuffer, close: ReqClose) -> Result<(), OrientError>;
    fn encode_query(buf: &mut OBuffer, query: ReqQuery) -> Result<(), OrientError>;
    fn encode_live_query(buf: &mut OBuffer, query: LiveQuery) -> Result<(), OrientError>;
    fn encode_unsubscribe_live_query(
        buf: &mut OBuffer,
        unsubscribe: UnsubscribeLiveQuery,
    ) -> Result<(), OrientError>;
    fn encode_query_next(buf: &mut OBuffer, next: QueryNext) -> Result<(), OrientError>;
    fn encode_query_close(buf: &mut OBuffer, close: QueryClose) -> Result<(), OrientError>;
    fn encode_connect(buf: &mut OBuffer, close: Connect) -> Result<(), OrientError>;
    fn encode_create_db(buf: &mut OBuffer, close: CreateDB) -> Result<(), OrientError>;
    fn encode_exist_db(buf: &mut OBuffer, close: ExistDB) -> Result<(), OrientError>;
    fn encode_drop_db(buf: &mut OBuffer, close: DropDB) -> Result<(), OrientError>;
    fn encode_server_query(buf: &mut OBuffer, query: ServerQuery) -> Result<(), OrientError>;
}
