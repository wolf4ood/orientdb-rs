use super::super::buffer::OBuffer;
use super::Protocol37;
use crate::common::protocol::serializer::DocumentSerializer;
use crate::common::types::document::ODocument;
use crate::common::types::value::OValue;
use crate::common::OrientCommonResult;

impl DocumentSerializer for Protocol37 {
    fn encode_document(doc: &ODocument) -> OrientCommonResult<OBuffer> {
        let mut doc_buf = OBuffer::new();
        encode_document(doc, &mut doc_buf)?;
        Ok(doc_buf)
    }
}

fn encode_document(doc: &ODocument, buf: &mut OBuffer) -> OrientCommonResult<()> {
    buf.write_string(doc.class_name())?;
    buf.write_varint(doc.len() as i64)?;

    for (k, v) in doc.iter() {
        write_field(buf, k, v)?;
    }
    Ok(())
}
fn write_field<'a>(buf: &mut OBuffer, name: &'a str, value: &'a OValue) -> OrientCommonResult<()> {
    buf.write_string(name)?;

    buf.put_i8(value.get_type_id())?;

    write_value(buf, name, value)?;

    Ok(())
}

fn write_value<'a>(buf: &mut OBuffer, owner: &'a str, value: &'a OValue) -> OrientCommonResult<()> {
    match value {
        OValue::String(ref s) => buf.write_string(s),
        OValue::Boolean(v) => {
            buf.write_bool(*v)?;
            Ok(())
        }
        OValue::I16(v) => buf.write_varint(i64::from(*v)),
        OValue::I32(v) => buf.write_varint(i64::from(*v)),
        OValue::I64(v) => buf.write_varint(*v),
        OValue::Link(ref v) => {
            buf.write_varint(i64::from(v.cluster))?;
            buf.write_varint(v.position)?;
            Ok(())
        }
        OValue::EmbeddedList(ref v) => {
            buf.write_varint(v.len() as i64)?;

            for (idx, elem) in v.iter().enumerate() {
                buf.put_i8(elem.get_type_id())?;
                write_value(buf, &format!("{}_{}", owner, idx), elem)?;
            }

            Ok(())
        }
        OValue::Document(ref doc) => {
            encode_document(doc, buf)?;
            Ok(())
        }

        OValue::LinkList(ref list) => {
            buf.write_varint(list.links.len() as i64)?;

            for link in list.links.iter() {
                buf.write_varint(i64::from(link.cluster))?;
                buf.write_varint(link.position)?;
            }
            Ok(())
        }
        OValue::EmbeddedMap(ref map) => {
            buf.write_varint(map.len() as i64)?;
            for (k, v) in map {
                write_field(buf, k, v)?;
            }
            Ok(())
        }
        _ => panic!("Field {} not supported", owner),
    }?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::Protocol37;
    use crate::common::protocol::deserializer::DocumentDeserializer;
    use crate::common::protocol::serializer::DocumentSerializer;
    use crate::common::types::document::ODocument;
    use crate::common::types::rid::ORecordID;
    use crate::common::types::value::{IntoOValue, LinkList, OValue};
    use std::collections::HashMap;

    #[test]
    fn doc_ser_test() {
        let mut doc = ODocument::new("Test");

        // Simple field
        doc.set("age", 20 as i16);
        doc.set("year", 1983);
        doc.set("balance", 10000 as i64);
        doc.set("confirmed", true);
        doc.set("name", String::from("John"));

        // Complex Field
        doc.set("city", ORecordID::new(10, 0));

        let map: HashMap<String, i32> = [("Norway", 100), ("Denmark", 50), ("Iceland", 10)]
            .iter()
            .map(|e| (String::from(e.0), e.1))
            .collect();

        let test_map = map.clone();

        doc.set("tags", vec!["name", "happy", "friends"]);
        doc.set("map", map);

        let mut doc_embedded = ODocument::new("EmbeddedClass");
        doc_embedded.set("name", "Foo");

        let doc_embedded_cloned = doc_embedded.clone();

        doc.set("embedded", doc_embedded);

        let links: LinkList = vec![ORecordID::new(10, 10)].into();
        doc.set("links", links);

        let encoded = Protocol37::encode_document(&doc).unwrap();
        let doc = Protocol37::decode_document(encoded.as_slice()).unwrap();

        assert_eq!("Test", doc.class_name());
        assert_eq!(Some(&OValue::I16(20)), doc.get_raw("age"));
        assert_eq!(Some(&OValue::I32(1983)), doc.get_raw("year"));
        assert_eq!(Some(&OValue::I64(10000)), doc.get_raw("balance"));
        assert_eq!(Some(&OValue::Boolean(true)), doc.get_raw("confirmed"));
        assert_eq!(
            Some(&OValue::String(String::from("John"))),
            doc.get_raw("name")
        );

        assert_eq!(
            Some(&OValue::Link(ORecordID::new(10, 0))),
            doc.get_raw("city")
        );

        let tags = OValue::EmbeddedList(vec![
            OValue::String(String::from("name")),
            OValue::String(String::from("happy")),
            OValue::String(String::from("friends")),
        ]);
        assert_eq!(Some(&tags), doc.get_raw("tags"));

        let embedded = OValue::EmbeddedMap(
            test_map
                .iter()
                .map(|(k, v)| (k.clone(), v.into_ovalue()))
                .collect(),
        );

        assert_eq!(Some(&embedded), doc.get_raw("map"));

        assert_eq!(
            Some(&OValue::Document(doc_embedded_cloned)),
            doc.get_raw("embedded")
        );

        assert_eq!(
            Some(&OValue::LinkList(vec![ORecordID::new(10, 10)].into())),
            doc.get_raw("links")
        );
    }
}
