use super::buffer::OBuffer;
use crate::common::types::document::ODocument;
use crate::common::OrientResult;

pub trait DocumentSerializer {
    fn encode_document(doc: &ODocument) -> OrientResult<OBuffer>;
}
