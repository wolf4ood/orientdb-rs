use super::super::v37::Protocol37;
use crate::common::protocol::deserializer::DocumentDeserializer;
use crate::common::protocol::messages::response::{
    Connect, ExistDB, Header, Open, Query, ServerQuery, Status,
};
use crate::common::types::error::{OError, RequestError};
use crate::common::types::OResult;
use crate::sync::protocol::decoder::VersionedDecoder;
use crate::OrientResult;
use std::collections::{HashMap, VecDeque};
use std::io::Read;

use crate::sync::protocol::io::reader;

impl VersionedDecoder for Protocol37 {
    fn decode_header<R: Read>(buf: &mut R) -> OrientResult<Header> {
        let status = reader::read_i8(buf)?;
        let session_id = reader::read_i32(buf)?;
        let token = reader::read_optional_bytes(buf)?;
        let op = reader::read_i8(buf)?;
        Ok(Header {
            status: Status::from(status),
            client_id: None,
            session_id,
            token,
            op,
        })
    }
    fn decode_open<R: Read>(buf: &mut R) -> OrientResult<Open> {
        let session_id = reader::read_i32(buf)?;
        let token = reader::read_optional_bytes(buf)?;
        Ok(Open::new(session_id, token))
    }

    fn decode_connect<R: Read>(buf: &mut R) -> OrientResult<Connect> {
        let session_id = reader::read_i32(buf)?;
        let token = reader::read_optional_bytes(buf)?;
        Ok(Connect::new(session_id, token))
    }
    fn decode_exist<R: Read>(buf: &mut R) -> OrientResult<ExistDB> {
        let exist = reader::read_bool(buf)?;
        Ok(ExistDB::new(exist))
    }

    fn decode_query<R: Read>(buf: &mut R) -> OrientResult<Query> {
        let query_id = reader::read_string(buf)?;
        let changes = reader::read_bool(buf)?;
        let has_plan = reader::read_bool(buf)?;

        let execution_plan = if has_plan {
            Some(read_result(buf)?)
        } else {
            None
        };

        let _prefetched = reader::read_i32(buf)?;
        let records = read_result_set(buf)?;
        let has_next = reader::read_bool(buf)?;
        let stats = read_query_stats(buf)?;
        let _reaload_metadata = reader::read_bool(buf)?;

        Ok(Query::new(
            query_id,
            changes,
            execution_plan,
            records,
            has_next,
            stats,
        ))
    }

    fn decode_server_query<R: Read>(buf: &mut R) -> OrientResult<ServerQuery> {
        let query_id = reader::read_string(buf)?;
        let changes = reader::read_bool(buf)?;
        let has_plan = reader::read_bool(buf)?;

        let execution_plan = if has_plan {
            Some(read_result(buf)?)
        } else {
            None
        };

        let _prefetched = reader::read_i32(buf)?;
        let records = read_result_set(buf)?;
        let has_next = reader::read_bool(buf)?;
        let stats = read_query_stats(buf)?;
        let _reaload_metadata = reader::read_bool(buf)?;

        Ok(ServerQuery::new(
            query_id,
            changes,
            execution_plan,
            records,
            has_next,
            stats,
        ))
    }

    fn decode_errors<R: Read>(buf: &mut R) -> OrientResult<RequestError> {
        let code = reader::read_i32(buf)?;
        let identifier = reader::read_i32(buf)?;
        let mut errors = vec![];
        loop {
            let more = reader::read_bool(buf)?;
            if more {
                let err_t = reader::read_string(buf)?;
                let err_m = reader::read_string(buf)?;
                let err = OError::new(err_t, err_m);
                errors.push(err);
            } else {
                break;
            }
        }
        let _serialized_exception = reader::read_bytes(buf)?;
        Ok(RequestError {
            session_id: -1,
            code,
            identifier,
            errors,
            serialized: _serialized_exception,
        })
    }
}

fn read_result<R: Read>(buf: &mut R) -> OrientResult<OResult> {
    let r_type = reader::read_i8(buf)?;
    match r_type {
        4 => {
            let buffer = reader::read_bytes(buf)?;
            let projection = Protocol37::decode_projection(&buffer)?;
            Ok(OResult::from(projection))
        }
        1 | 2 | 3 => {
            let _val = reader::read_i16(buf)?;
            let _d_type = reader::read_i8(buf)?;
            let identity = reader::read_identity(buf)?;
            let version = reader::read_i32(buf)?;

            let buffer = reader::read_bytes(buf)?;
            let mut document = Protocol37::decode_document(&buffer)?;

            document.set_record_id(identity);
            document.set_version(version);

            Ok(OResult::from((r_type, document)))
        }
        _ => panic!("Unsupported result type {}", r_type),
    }
}

fn read_result_set<R: Read>(buf: &mut R) -> OrientResult<VecDeque<OResult>> {
    let size = reader::read_i32(buf)?;
    let mut records = VecDeque::new();
    for _ in 0..size {
        let result = read_result(buf)?;
        records.push_back(result);
    }

    Ok(records)
}

fn read_query_stats<R: Read>(buf: &mut R) -> OrientResult<HashMap<String, i64>> {
    let size = reader::read_i32(buf)?;
    let stats = HashMap::new();
    for _ in 0..size {}
    Ok(stats)
}
