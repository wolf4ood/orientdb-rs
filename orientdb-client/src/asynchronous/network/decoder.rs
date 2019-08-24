use crate::common::protocol::messages::response::Response;
use crate::common::protocol::messages::response::Status;
use crate::{OrientError, OrientResult};
use async_std::io::Read;
use async_std::net::TcpStream;
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

use super::reader;
use async_std::prelude::*;

use crate::common::protocol::messages::response::{
    Connect, CreateDB, DropDB, ExistDB, Header, Open, Query, QueryClose,
};
use crate::common::types::error::{OError, RequestError};

pub async fn decode(version: i16, buf: &mut TcpStream) -> OrientResult<Response> {
    if version >= 37 {
        return decode_with::<Protocol37>(buf).await;
    }
    Err(OrientError::Protocol(format!(
        "Protocol {} not supported",
        version
    )))
}

struct Protocol37 {}

#[async_trait]
impl VersionedDecoder for Protocol37 {
    async fn decode_header(buf: &mut TcpStream) -> OrientResult<Header> {
        let status = reader::read_i8(buf).await?;
        let session_id = reader::read_i32(buf).await?;

        let token = reader::read_optional_bytes(buf).await?;
        let op = reader::read_i8(buf).await?;

        Ok(Header {
            status: Status::from(status),
            client_id: None,
            session_id,
            token,
            op,
        })
    }
    async fn decode_open(buf: &mut TcpStream) -> OrientResult<Open> {
        let session_id = reader::read_i32(buf).await?;
        let token = reader::read_optional_bytes(buf).await?;
        Ok(Open::new(session_id, token))
    }

    async fn decode_errors(buf: &mut TcpStream) -> OrientResult<RequestError> {
        let code = reader::read_i32(buf).await?;
        let identifier = reader::read_i32(buf).await?;
        let mut errors = vec![];
        loop {
            let more = reader::read_bool(buf).await?;
            if more {
                let err_t = reader::read_string(buf).await?;
                let err_m = reader::read_string(buf).await?;
                let err = OError::new(err_t, err_m);
                errors.push(err);
            } else {
                break;
            }
        }
        let _serialized_exception = reader::read_bytes(buf).await?;
        Ok(RequestError {
            session_id: -1,
            code,
            identifier,
            errors,
            serialized: _serialized_exception,
        })
    }
    async fn decode_query<R: Read>(buf: &mut R) -> OrientResult<Query>
    where
        R: std::marker::Send,
    {
        unimplemented!()
    }
    async fn decode_connect(buf: &mut TcpStream) -> OrientResult<Connect> {
        let session_id = reader::read_i32(buf).await?;
        let token = reader::read_optional_bytes(buf).await?;
        Ok(Connect::new(session_id, token))
    }
    async fn decode_exist(buf: &mut TcpStream) -> OrientResult<ExistDB> {
        let exist = reader::read_bool(buf).await?;
        Ok(ExistDB::new(exist))
    }
}

#[async_trait]
pub trait VersionedDecoder {
    async fn decode_header(buf: &mut TcpStream) -> OrientResult<Header>;

    async fn decode_open(buf: &mut TcpStream) -> OrientResult<Open>;

    async fn decode_errors(buf: &mut TcpStream) -> OrientResult<RequestError>;

    async fn decode_query<R: Read>(buf: &mut R) -> OrientResult<Query>
    where
        R: std::marker::Send;
    async fn decode_connect(buf: &mut TcpStream) -> OrientResult<Connect>;

    async fn decode_exist(buf: &mut TcpStream) -> OrientResult<ExistDB>;

    async fn decode_drop_db(_buf: &mut TcpStream) -> OrientResult<DropDB> {
        Ok(DropDB {})
    }
    async fn decode_create_db(_buf: &mut TcpStream) -> OrientResult<CreateDB> {
        Ok(CreateDB {})
    }
    async fn decode_query_close<R: Read>(_buf: &mut R) -> OrientResult<QueryClose>
    where
        R: std::marker::Send,
    {
        Ok(QueryClose {})
    }
}

pub async fn decode_with<T: VersionedDecoder>(buf: &mut TcpStream) -> OrientResult<Response> {
    let header = T::decode_header(buf).await?;

    let payload = match header.status {
        Status::ERROR => return Err(OrientError::Request(T::decode_errors(buf).await?)),
        _ => match header.op {
            2 => T::decode_connect(buf).await?.into(),
            3 => T::decode_open(buf).await?.into(),
            4 => T::decode_create_db(buf).await?.into(),
            6 => T::decode_exist(buf).await?.into(),
            7 => T::decode_drop_db(buf).await?.into(),
            45 => T::decode_query(buf).await?.into(),
            46 => T::decode_query_close(buf).await?.into(),
            47 => T::decode_query(buf).await?.into(),
            _ => panic!("Request {} not supported", header.op),
        },
    };
    Ok(Response::new(header, payload))
}
