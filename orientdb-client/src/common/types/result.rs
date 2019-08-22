use super::document::ODocument;
use super::projection::Projection;
use crate::common::types::value::{FromOValue, OValue};
use crate::common::{OrientError, OrientResult};

#[derive(Debug)]
pub enum ResultType {
    Document(ODocument),
    Projection(Projection),
}

impl ResultType {
    pub fn get(&self, name: &str) -> Option<&OValue> {
        match self {
            ResultType::Document(ref d) => d.get_raw(name),
            ResultType::Projection(ref p) => p.get(name),
        }
    }

    pub fn get_str(&self, name: &str) -> Option<&str> {
        match self {
            ResultType::Document(ref d) => d.get_str(name),
            ResultType::Projection(ref p) => p.as_str(name),
        }
    }
}

#[derive(Debug)]
pub struct OResult {
    inner: ResultType,
}

impl OResult {
    pub fn empty() -> OResult {
        OResult {
            inner: ResultType::Projection(Projection::default()),
        }
    }

    pub fn get_checked<T>(&self, name: &str) -> OrientResult<T>
    where
        T: FromOValue,
    {
        match self.get_raw(name) {
            Some(val) => T::from_value(val),
            None => Err(OrientError::Field(format!(
                "Field {} not found.",
                name
            ))),
        }
    }

    pub fn get<T>(&self, name: &str) -> T
    where
        T: FromOValue,
    {
        match self.get_checked(name) {
            Ok(val) => val,
            Err(err) => panic!("Error : {:?}", err),
        }
    }
    pub fn get_raw(&self, name: &str) -> Option<&OValue> {
        self.inner.get(name)
    }
}

impl From<(i8, ODocument)> for OResult {
    fn from(val: (i8, ODocument)) -> Self {
        match val.0 {
            1 | 2 | 3 => OResult {
                inner: ResultType::Document(val.1),
            },
            _ => panic!("Unsupported record type {}", val.0),
        }
    }
}

impl From<Projection> for OResult {
    fn from(projection: Projection) -> Self {
        OResult {
            inner: ResultType::Projection(projection),
        }
    }
}
