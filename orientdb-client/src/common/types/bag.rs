use crate::common::types::rid::ORecordID;

#[derive(Debug, PartialEq, Clone)]
pub enum RidBag {
    Embedded(Vec<ORecordID>),
    Tree(i32),
}
