use super::buffer::OBuffer;
use crate::common::types::document::ODocument;
use crate::common::OrientCommonResult;

pub trait DocumentSerializer {
    fn encode_document(doc: &ODocument) -> OrientCommonResult<OBuffer>;
}
