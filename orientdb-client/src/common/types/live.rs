use crate::types::OResult;

#[derive(Debug)]
pub enum LiveResult {
    Created(OResult),
    Updated((OResult, OResult)),
    Deleted(OResult),
}
