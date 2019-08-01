use crate::common::types::document::ODocument;
use crate::common::types::projection::Projection;
use crate::common::OrientCommonResult;

pub trait DocumentDeserializer {
    fn decode_document(src: &[u8]) -> OrientCommonResult<ODocument>;
    fn decode_projection(src: &[u8]) -> OrientCommonResult<Projection>;
}
