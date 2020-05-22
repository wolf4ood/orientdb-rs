use chrono;
use chrono::offset;
use std::collections::HashMap;

use crate::common::protocol::constants;
use crate::common::types::bag::RidBag;
use crate::common::types::document::ODocument;
use crate::common::types::rid::ORecordID;
use crate::common::{OrientError, OrientResult};

#[cfg(feature = "uuid")]
use uuid::Uuid;

pub type DateTime = chrono::DateTime<offset::Utc>;
pub type Date = chrono::Date<offset::Utc>;
pub type EmbeddedMap = HashMap<String, OValue>;
pub type EmbeddedList = Vec<OValue>;
pub type EmbeddedSet = Vec<OValue>;

#[derive(Debug, PartialEq, Clone)]
pub struct LinkList {
    pub(crate) links: Vec<ORecordID>,
}

impl From<Vec<ORecordID>> for LinkList {
    fn from(links: Vec<ORecordID>) -> Self {
        LinkList { links }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum OValue {
    Null,
    String(String),
    Boolean(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    I32(i32),
    Document(ODocument),
    I64(i64),
    F32(f32),
    F64(f64),
    DateTime(DateTime),
    Date(Date),
    Link(ORecordID),
    LinkList(LinkList),
    LinkSet(LinkList),
    EmbeddedMap(EmbeddedMap),
    EmbeddedList(EmbeddedList),
    EmbeddedSet(EmbeddedSet),
    RidBag(RidBag),
    #[cfg(feature = "uuid")]
    Uuid(Uuid),
}

impl<'a> From<&'a str> for OValue {
    fn from(x: &'a str) -> Self {
        OValue::String(String::from(x))
    }
}

pub trait IntoOValue: Send + Sync {
    fn into_ovalue(&self) -> OValue;
}

impl<'a> IntoOValue for &'a str {
    fn into_ovalue(&self) -> OValue {
        OValue::String(String::from(*self))
    }
}

impl IntoOValue for String {
    fn into_ovalue(&self) -> OValue {
        OValue::String(self.clone())
    }
}

impl IntoOValue for bool {
    fn into_ovalue(&self) -> OValue {
        OValue::Boolean(*self)
    }
}

impl IntoOValue for i64 {
    fn into_ovalue(&self) -> OValue {
        OValue::I64(*self)
    }
}

impl IntoOValue for i32 {
    fn into_ovalue(&self) -> OValue {
        OValue::I32(*self)
    }
}

impl IntoOValue for i16 {
    fn into_ovalue(&self) -> OValue {
        OValue::I16(*self)
    }
}

impl IntoOValue for ORecordID {
    fn into_ovalue(&self) -> OValue {
        OValue::Link((*self).clone())
    }
}

impl IntoOValue for EmbeddedMap {
    fn into_ovalue(&self) -> OValue {
        let value = (*self).clone();
        OValue::EmbeddedMap(value)
    }
}

impl<T> IntoOValue for Vec<T>
where
    T: IntoOValue,
{
    fn into_ovalue(&self) -> OValue {
        OValue::EmbeddedList(self.iter().map(|v| v.into_ovalue()).collect())
    }
}

impl<T, S: std::hash::BuildHasher + Send + Sync> IntoOValue for HashMap<String, T, S>
where
    T: IntoOValue,
{
    fn into_ovalue(&self) -> OValue {
        let map: HashMap<String, OValue> = self
            .iter()
            .map(|(k, v)| (k.clone(), v.into_ovalue()))
            .collect();
        OValue::EmbeddedMap(map)
    }
}

impl IntoOValue for ODocument {
    fn into_ovalue(&self) -> OValue {
        OValue::Document(self.clone())
    }
}

impl IntoOValue for LinkList {
    fn into_ovalue(&self) -> OValue {
        OValue::LinkList(self.clone())
    }
}

#[cfg(feature = "uuid")]
impl IntoOValue for Uuid {
    fn into_ovalue(&self) -> OValue {
        OValue::Uuid(self.clone())
    }
}
impl OValue {
    pub fn get_type_id(&self) -> i8 {
        match self {
            OValue::EmbeddedMap(_) => constants::EMBEDDEDMAP,
            OValue::String(_) => constants::STRING,
            OValue::I8(_) => constants::BYTE,
            OValue::I16(_) => constants::SHORT,
            OValue::I32(_) => constants::INTEGER,
            OValue::I64(_) => constants::LONG,
            OValue::Link(_) => constants::LINK,
            OValue::Boolean(_) => constants::BOOLEAN,
            OValue::Document(_) => constants::EMBEDDED,
            OValue::EmbeddedList(_) => constants::EMBEDDEDLIST,
            OValue::EmbeddedSet(_) => constants::EMBEDDEDSET,
            OValue::LinkList(_) => constants::LINKLIST,
            #[cfg(feature = "uuid")]
            OValue::Uuid(_) => constants::STRING,

            _ => panic!("Type id not supported {:?}", self),
        }
    }
}

pub trait FromOValue {
    fn from_value(ty: &OValue) -> OrientResult<Self>
    where
        Self: Sized;
}

impl FromOValue for i32 {
    fn from_value(ty: &OValue) -> OrientResult<Self>
    where
        Self: Sized,
    {
        match ty {
            OValue::I32(val) => Ok(*val),
            _ => Err(OrientError::Conversion(format!(
                "Cannot convert {:?} to i32",
                ty
            ))),
        }
    }
}

impl FromOValue for i64 {
    fn from_value(ty: &OValue) -> OrientResult<Self>
    where
        Self: Sized,
    {
        match ty {
            OValue::I64(val) => Ok(*val),
            _ => Err(OrientError::Conversion(format!(
                "Cannot convert {:?} to i64",
                ty
            ))),
        }
    }
}

impl FromOValue for String {
    fn from_value(ty: &OValue) -> OrientResult<Self>
    where
        Self: Sized,
    {
        match ty {
            OValue::String(val) => Ok(val.clone()),
            _ => Err(OrientError::Conversion(format!(
                "Cannot convert {:?} to String",
                ty
            ))),
        }
    }
}

impl FromOValue for ODocument {
    fn from_value(ty: &OValue) -> OrientResult<Self>
    where
        Self: Sized,
    {
        match ty {
            OValue::Document(val) => Ok(val.clone()),
            _ => Err(OrientError::Conversion(format!(
                "Cannot convert {:?} to ODocument",
                ty
            ))),
        }
    }
}

impl FromOValue for EmbeddedMap {
    fn from_value(ty: &OValue) -> OrientResult<Self>
    where
        Self: Sized,
    {
        match ty {
            OValue::EmbeddedMap(val) => Ok(val.clone()),
            _ => Err(OrientError::Conversion(format!(
                "Cannot convert {:?} to EmbeddedMap",
                ty
            ))),
        }
    }
}

impl FromOValue for bool {
    fn from_value(ty: &OValue) -> OrientResult<Self>
    where
        Self: Sized,
    {
        match ty {
            OValue::Boolean(val) => Ok(*val),
            _ => Err(OrientError::Conversion(format!(
                "Cannot convert {:?} to &str",
                ty
            ))),
        }
    }
}

#[cfg(feature = "uuid")]
impl FromOValue for Uuid {
    fn from_value(ty: &OValue) -> OrientResult<Self>
    where
        Self: Sized,
    {
        match ty {
            OValue::Uuid(val) => Ok(*val),
            OValue::String(val) => Ok(Uuid::parse_str(val).map_err(|_e| {
                OrientError::Conversion(format!("Cannot parse {:?} to Uuid", val))
            })?),
            _ => Err(OrientError::Conversion(format!(
                "Cannot convert {:?} to Uuid",
                ty
            ))),
        }
    }
}

impl<T: FromOValue> FromOValue for Option<T> {
    fn from_value(ty: &OValue) -> OrientResult<Self>
    where
        Self: Sized,
    {
        match ty {
            OValue::Null => Ok(None),
            _ => Ok(Some(T::from_value(ty)?)),
        }
    }
}
