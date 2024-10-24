mod decoder;
mod encoder;
pub(crate) mod io;

pub(crate) mod v37;
use self::decoder::VersionedDecoder;
use self::encoder::VersionedEncoder;
use crate::common::protocol::buffer::OBuffer;

use crate::common::protocol::messages::response::Status;
use crate::common::protocol::messages::{Request, Response};

use crate::{OrientError, OrientResult};

pub mod messages {

    pub mod request {
        pub use crate::common::protocol::messages::request::*;
    }
}
use crate::sync::protocol::v37::Protocol37;
use std::io::Read;

#[derive(Clone)]
pub(crate) struct WiredProtocol {
    pub version: i16,
}

impl WiredProtocol {
    pub fn from_version(version: i16) -> OrientResult<WiredProtocol> {
        if version >= 37 {
            Ok(WiredProtocol { version: 37 })
        } else {
            Err(OrientError::Protocol(format!(
                "Protocol {} not supported",
                version
            )))
        }
    }

    pub fn encode(&mut self, item: Request) -> OrientResult<OBuffer> {
        if self.version >= 37 {
            return self.encode_with::<Protocol37>(item);
        }
        Err(OrientError::Protocol(format!(
            "Protocol {} not supported",
            self.version
        )))
    }

    fn encode_with<T: VersionedEncoder>(&mut self, item: Request) -> OrientResult<OBuffer> {
        let mut buffer = OBuffer::new();
        match item {
            Request::HandShake(handshake) => T::encode_handshake(&mut buffer, handshake),
            Request::Connect(connect) => T::encode_connect(&mut buffer, connect),
            Request::Open(open) => T::encode_open(&mut buffer, open),
            Request::CreateDB(create) => T::encode_create_db(&mut buffer, create),
            Request::ExistDB(exist) => T::encode_exist_db(&mut buffer, exist),
            Request::DropDB(drop) => T::encode_drop_db(&mut buffer, drop),
            Request::ServerQuery(query) => T::encode_server_query(&mut buffer, query),
            Request::Close(close) => T::encode_close(&mut buffer, close),
            Request::Query(query) => T::encode_query(&mut buffer, query),
            Request::LiveQuery(query) => T::encode_live_query(&mut buffer, query),
            Request::QueryNext(next) => T::encode_query_next(&mut buffer, next),
            Request::QueryClose(query_close) => T::encode_query_close(&mut buffer, query_close),
            Request::UnsubscribeLiveQuery(unsubscribe) => {
                T::encode_unsubscribe_live_query(&mut buffer, unsubscribe)
            }
        }?;

        Ok(buffer)
    }

    pub fn decode<R: Read>(&mut self, buf: &mut R) -> OrientResult<Response> {
        if self.version >= 37 {
            return self.decode_with::<R, Protocol37>(buf);
        }
        Err(OrientError::Protocol(format!(
            "Protocol {} not supported",
            self.version
        )))
    }

    pub fn decode_with<R: Read, T: VersionedDecoder>(
        &mut self,
        buf: &mut R,
    ) -> OrientResult<Response> {
        let header = T::decode_header(buf)?;

        let payload = match header.status {
            Status::ERROR => return Err(OrientError::Request(T::decode_errors(buf)?)),
            _ => match header.op {
                2 => T::decode_connect(buf)?.into(),
                3 => T::decode_open(buf)?.into(),
                4 => T::decode_create_db(buf)?.into(),
                6 => T::decode_exist(buf)?.into(),
                7 => T::decode_drop_db(buf)?.into(),
                45 => T::decode_query(buf)?.into(),
                46 => T::decode_query_close(buf)?.into(),
                47 => T::decode_query(buf)?.into(),
                50 => T::decode_server_query(buf)?.into(),
                _ => panic!("Request {} not supported", header.op),
            },
        };
        Ok(Response::new(header, payload))
    }
}
