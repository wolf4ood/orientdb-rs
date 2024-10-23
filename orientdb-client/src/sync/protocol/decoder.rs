use crate::common::protocol::messages::response::{
    Connect, CreateDB, DropDB, ExistDB, Header, Open, Query, QueryClose, ServerQuery,
};
use crate::common::types::error::RequestError;
use crate::OrientResult;
use std::io::Read;

pub trait VersionedDecoder {
    fn decode_header<R: Read>(buf: &mut R) -> OrientResult<Header>;
    fn decode_open<R: Read>(buf: &mut R) -> OrientResult<Open>;
    fn decode_errors<R: Read>(buf: &mut R) -> OrientResult<RequestError>;
    fn decode_query<R: Read>(buf: &mut R) -> OrientResult<Query>;
    fn decode_connect<R: Read>(buf: &mut R) -> OrientResult<Connect>;
    fn decode_exist<R: Read>(buf: &mut R) -> OrientResult<ExistDB>;
    fn decode_drop_db<R: Read>(_buf: &mut R) -> OrientResult<DropDB> {
        Ok(DropDB {})
    }
    fn decode_create_db<R: Read>(_buf: &mut R) -> OrientResult<CreateDB> {
        Ok(CreateDB {})
    }
    fn decode_query_close<R: Read>(_buf: &mut R) -> OrientResult<QueryClose> {
        Ok(QueryClose {})
    }

    fn decode_server_query<R: Read>(buf: &mut R) -> OrientResult<ServerQuery>;
}
