use crate::common::types::rid::ORecordID;
use crate::common::types::value::{FromOValue, IntoOValue, OValue};
use crate::common::{OrientError, OrientResult};
use std::collections::hash_map;
use std::collections::HashMap;
use std::iter::Iterator;

#[derive(Debug, PartialEq, Clone)]
pub struct ODocument {
    record_id: ORecordID,
    class_name: String,
    version: i32,
    fields: HashMap<String, OValue>,
}

#[derive(Debug)]
pub struct DocumentBuilder {
    doc: ODocument,
}

impl DocumentBuilder {
    pub fn set<KT, T>(mut self, name: KT, value: T) -> Self
    where
        KT: Into<String>,
        T: IntoOValue,
    {
        self.doc.set(name, value);
        self
    }

    pub fn set_class_name<T>(mut self, class_name: T) -> Self
    where
        T: Into<String>,
    {
        self.doc.class_name = class_name.into();
        self
    }
    pub fn build(self) -> ODocument {
        self.doc
    }
}
impl ODocument {
    pub fn builder() -> DocumentBuilder {
        DocumentBuilder {
            doc: ODocument::empty(),
        }
    }
    pub fn new<S>(class_name: S) -> ODocument
    where
        S: Into<String>,
    {
        ODocument::new_with_rid_version(class_name, ORecordID::empty(), 0)
    }
    pub(crate) fn new_with_rid_version<S>(
        class_name: S,
        record_id: ORecordID,
        version: i32,
    ) -> ODocument
    where
        S: Into<String>,
    {
        ODocument {
            version,
            record_id,
            class_name: class_name.into(),
            fields: HashMap::new(),
        }
    }

    pub fn set_record_id(&mut self, record_id: ORecordID) {
        self.record_id = record_id;
    }
    pub fn set_version(&mut self, version: i32) {
        self.version = version
    }
    pub fn empty() -> ODocument {
        ODocument::new("")
    }

    pub fn class_name(&self) -> &str {
        &self.class_name
    }
    pub fn set<KT, T>(&mut self, name: KT, value: T)
    where
        KT: Into<String>,
        T: IntoOValue,
    {
        self.fields.insert(name.into(), value.into_ovalue());
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
        self.fields.get(name)
    }
    pub fn set_raw<T>(&mut self, name: T, value: OValue)
    where
        T: Into<String>,
    {
        self.fields.insert(name.into(), value);
    }
    pub fn get_i32(&self, name: &str) -> Option<i32> {
        match self.fields.get(name) {
            Some(&OValue::I32(v)) => Some(v),
            Some(_) => None,
            None => None,
        }
    }

    // typed getters

    pub fn get_str(&self, name: &str) -> Option<&str> {
        match self.fields.get(name) {
            Some(&OValue::String(ref v)) => Some(v),
            Some(_) => None,
            None => None,
        }
    }
    pub fn get_bool(&self, name: &str) -> Option<bool> {
        match self.fields.get(name) {
            Some(&OValue::Boolean(v)) => Some(v),
            Some(_) => None,
            None => None,
        }
    }
    pub fn get_i8(&self, name: &str) -> Option<i8> {
        match self.fields.get(name) {
            Some(&OValue::I8(v)) => Some(v),
            Some(_) => None,
            None => None,
        }
    }

    pub fn iter(&self) -> Iter<String, OValue> {
        Iter {
            inner: self.fields.iter(),
        }
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

pub struct Iter<'a, K: 'a, V: 'a> {
    inner: hash_map::Iter<'a, K, V>,
}

impl<'a, K: 'a, V: 'a> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::types::document::ODocument;
    use crate::common::types::value::OValue;
    use crate::common::OrientResult;
    use std::error::Error;

    #[test]
    fn doc_simple_test() {
        // Doc build
        let mut doc = ODocument::new("Test");
        doc.set("age", 20);
        doc.set("confirmed", true);
        doc.set("name", String::from("John"));
        doc.set("surname", String::from("Cage"));

        // Doc test
        assert_eq!("Test", doc.class_name);
        assert_eq!(Some(20), doc.get_i32("age"));
        assert_eq!(Some("John"), doc.get_str("name"));
        assert_eq!(Some("Cage"), doc.get_str("surname"));
        assert_eq!(Some(true), doc.get_bool("confirmed"));

        // Checked
        assert_eq!(20, doc.get_checked("age").unwrap());
        assert_eq!(
            String::from("John"),
            doc.get_checked::<String>("name").unwrap()
        );
        assert_eq!(true, doc.get_checked("confirmed").unwrap());

        let checked: OrientResult<bool> = doc.get_checked("confirmed1");

        assert!(checked.is_err());

        assert_eq!(
            format!("Field confirmed1 not found."),
            checked.err().unwrap().description()
        );

        // Unchecked

        assert_eq!(20, doc.get("age"));
        assert_eq!(String::from("John"), doc.get::<String>("name"));
        assert_eq!(true, doc.get("confirmed"));
    }

    #[test]
    fn doc_inner_test() {
        let mut doc = ODocument::new("Test");
        doc.set("age", 20);
        doc.set("confirmed", true);
        doc.set("name", String::from("John"));
        doc.set("surname", String::from("Cage"));
        assert_eq!("Test", doc.class_name);
        assert_eq!(Some(&OValue::I32(20)), doc.get_raw("age"));
        assert_eq!(Some(&OValue::Boolean(true)), doc.get_raw("confirmed"));
        assert_eq!(
            Some(&OValue::String(String::from("John"))),
            doc.get_raw("name")
        );
        assert_eq!(
            Some(&OValue::String(String::from("Cage"))),
            doc.get_raw("surname")
        );
    }
}
