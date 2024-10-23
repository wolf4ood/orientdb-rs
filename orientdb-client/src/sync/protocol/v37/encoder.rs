use super::super::v37::Protocol37;
use crate::common::protocol::buffer::OBuffer;
use crate::common::protocol::messages::request::{
    Close, Connect, CreateDB, DropDB, ExistDB, HandShake, LiveQuery, Open, Query, QueryClose,
    QueryNext, ServerQuery, UnsubscribeLiveQuery,
};
use crate::common::protocol::serializer::DocumentSerializer;
use crate::common::types::document::ODocument;
use crate::sync::protocol::encoder::VersionedEncoder;
use crate::OrientResult;

impl VersionedEncoder for Protocol37 {
    fn encode_handshake(buf: &mut OBuffer, handshake: HandShake) -> OrientResult<()> {
        buf.put_i8(20)?;
        buf.put_i16(handshake.p_version)?;
        buf.write_str(&handshake.name)?;
        buf.write_str(&handshake.version)?;
        buf.put_i8(0)?;
        buf.put_i8(1)?;
        Ok(())
    }

    fn encode_connect(buf: &mut OBuffer, open: Connect) -> OrientResult<()> {
        buf.put_i8(2)?;
        buf.put_i32(-1)?;
        buf.write_slice(&[])?;
        buf.write_str(&open.username)?;
        buf.write_str(&open.password)?;
        Ok(())
    }
    fn encode_open(buf: &mut OBuffer, open: Open) -> OrientResult<()> {
        buf.put_i8(3)?;
        buf.put_i32(-1)?;
        buf.write_slice(&[])?;
        buf.write_str(&open.db)?;
        buf.write_str(&open.username)?;
        buf.write_str(&open.password)?;
        Ok(())
    }

    fn encode_create_db(buf: &mut OBuffer, create: CreateDB) -> OrientResult<()> {
        buf.put_i8(4)?;
        buf.put_i32(create.header.session_id)?;
        if let Some(t) = create.header.token {
            buf.write_slice(&t)?;
        }
        buf.write_str(&create.name)?;
        buf.write_str("")?;
        buf.write_str(create.db_mode.as_str())?;

        match create.backup {
            Some(ref bck) => buf.write_str(bck)?,
            None => buf.put_i32(-1)?,
        };
        Ok(())
    }

    fn encode_close(buf: &mut OBuffer, close: Close) -> OrientResult<()> {
        buf.put_i8(5)?;
        buf.put_i32(close.session_id)?;

        if let Some(t) = close.token {
            buf.write_slice(&t)?;
        }
        Ok(())
    }

    fn encode_exist_db(buf: &mut OBuffer, exist: ExistDB) -> OrientResult<()> {
        buf.put_i8(6)?;
        buf.put_i32(exist.header.session_id)?;
        if let Some(t) = exist.header.token {
            buf.write_slice(&t)?;
        }
        buf.write_str(&exist.name)?;
        buf.write_str(exist.db_mode.as_str())?;
        Ok(())
    }
    fn encode_drop_db(buf: &mut OBuffer, drop: DropDB) -> OrientResult<()> {
        buf.put_i8(7)?;
        buf.put_i32(drop.header.session_id)?;
        if let Some(t) = drop.header.token {
            buf.write_slice(&t)?;
        }
        buf.write_str(&drop.name)?;
        buf.write_str(drop.db_mode.as_str())?;
        Ok(())
    }

    fn encode_unsubscribe_live_query(
        buf: &mut OBuffer,
        query: UnsubscribeLiveQuery,
    ) -> OrientResult<()> {
        buf.put_i8(101)?;
        buf.put_i32(query.session_id)?;
        if let Some(t) = query.token {
            buf.write_slice(&t)?;
        }
        buf.put_i8(2)?;
        buf.put_i32(query.monitor_id)?;

        Ok(())
    }

    fn encode_live_query(buf: &mut OBuffer, query: LiveQuery) -> OrientResult<()> {
        buf.put_i8(100)?;
        buf.put_i32(query.session_id)?;
        if let Some(t) = query.token {
            buf.write_slice(&t)?;
        }
        buf.put_i8(2)?;

        buf.write_str(&query.query)?;
        let mut document = ODocument::empty();
        document.set("params", query.parameters);
        let encoded = Protocol37::encode_document(&document)?;
        buf.write_slice(encoded.as_slice())?;
        buf.write_bool(query.named)?;

        Ok(())
    }
    fn encode_query(buf: &mut OBuffer, query: Query) -> OrientResult<()> {
        buf.put_i8(45)?;
        buf.put_i32(query.session_id)?;

        if let Some(t) = query.token {
            buf.write_slice(&t)?;
        }
        buf.write_str(&query.language)?;
        buf.write_str(&query.query)?;
        buf.put_i8(query.mode)?;
        buf.put_i32(query.page_size)?;
        buf.write_str("")?;
        let mut document = ODocument::empty();
        document.set("params", query.parameters);
        let encoded = Protocol37::encode_document(&document)?;
        buf.write_slice(encoded.as_slice())?;
        buf.write_bool(query.named)?;

        Ok(())
    }

    fn encode_server_query(buf: &mut OBuffer, query: ServerQuery) -> OrientResult<()> {
        buf.put_i8(50)?;
        buf.put_i32(query.session_id)?;

        if let Some(t) = query.token {
            buf.write_slice(&t)?;
        }
        buf.write_str(&query.language)?;
        buf.write_str(&query.query)?;
        buf.put_i8(query.mode)?;
        buf.put_i32(query.page_size)?;
        buf.write_str("")?;
        let mut document = ODocument::empty();
        document.set("params", query.parameters);
        let encoded = Protocol37::encode_document(&document)?;
        buf.write_slice(encoded.as_slice())?;
        buf.write_bool(query.named)?;

        Ok(())
    }

    fn encode_query_next(buf: &mut OBuffer, next: QueryNext) -> OrientResult<()> {
        buf.put_i8(47)?;
        buf.put_i32(next.session_id)?;
        if let Some(t) = next.token {
            buf.write_slice(&t)?;
        }
        buf.write_str(&next.query_id)?;
        buf.put_i32(next.page_size)?;
        Ok(())
    }

    fn encode_query_close(buf: &mut OBuffer, next: QueryClose) -> OrientResult<()> {
        buf.put_i8(46)?;
        buf.put_i32(next.session_id)?;
        if let Some(t) = next.token {
            buf.write_slice(&t)?;
        }
        buf.write_str(&next.query_id)?;
        Ok(())
    }
}
