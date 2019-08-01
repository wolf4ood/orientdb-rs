use crate::common::types::value::OValue;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

#[derive(Debug, PartialEq, Clone, Default)]
pub struct Projection {
    fields: HashMap<String, OValue>,
}

impl Projection {
    pub fn as_str(&self, name: &str) -> Option<&str> {
        match self.fields.get(name) {
            Some(&OValue::String(ref v)) => Some(v),
            Some(_) => None,
            None => None,
        }
    }

    pub fn take_map(self) -> HashMap<String, OValue> {
        self.fields
    }
}

impl Deref for Projection {
    type Target = HashMap<String, OValue>;

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl DerefMut for Projection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fields
    }
}
