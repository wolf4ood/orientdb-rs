use super::super::parser::{
    parse_bool, parse_optimized_identity, parse_string_varint, parse_varint,
};

use super::Protocol37;
use crate::common::protocol::constants;
use crate::common::protocol::deserializer::DocumentDeserializer;
use crate::common::types::bag::RidBag;
use crate::common::types::document::ODocument;
use crate::common::types::projection::Projection;
use crate::common::types::value::OValue;

use crate::{OrientError, OrientResult};

use chrono::TimeZone;
use chrono::Utc;
use nom::IResult;

use nom::number::streaming::{be_f32, be_f64, be_i64, be_i8, be_u8};
use nom::{cond, do_parse, many_m_n, named, try_parse};
use std::collections::HashMap;

impl DocumentDeserializer for Protocol37 {
    fn decode_document(input: &[u8]) -> OrientResult<ODocument> {
        let (rm, doc) = parse_document(input)
            .map_err(|e| OrientError::Decoder(format!("Error decoding document: {:?}", e)))?;
        assert_eq!(rm.len(), 0);
        Ok(doc)
    }
    fn decode_projection(input: &[u8]) -> OrientResult<Projection> {
        let (rm, projection) = parse_projection(input)
            .map_err(|e| OrientError::Decoder(format!("Error decoding projection: {:?}", e)))?;

        assert_eq!(rm.len(), 0);
        Ok(projection)
    }
}

fn parse_document(input: &[u8]) -> IResult<&[u8], ODocument> {
    let (remaining, class_name) = try_parse!(input, parse_string_varint);
    let mut doc = ODocument::new(class_name);
    let (mut remaining, fields) = try_parse!(remaining, parse_varint);
    for _ in 0..fields {
        let (rm, (field, value)) = parse_field(remaining, &|e| {
            let (remaining, value) = try_parse!(e, parse_document);
            Ok((remaining, OValue::Document(value)))
        })?;
        doc.set_raw(field, value);
        remaining = rm;
    }
    Ok((remaining, doc))
}

fn parse_projection(input: &[u8]) -> IResult<&[u8], Projection> {
    let (mut remaining, fields) = try_parse!(input, parse_varint);

    let mut projection = Projection::default();
    for _ in 0..fields {
        let (rm, (field, value)) = parse_field(remaining, &|e| {
            let (remaining, value) = try_parse!(e, parse_projection);
            Ok((remaining, OValue::EmbeddedMap(value.take_map())))
        })?;
        projection.insert(field, value);
        remaining = rm;
    }
    let (mut remaining, m_fields) = try_parse!(remaining, parse_varint);

    // metadata
    for _ in 0..m_fields {
        let (rm, (_field, _value)) = parse_field(remaining, &|e| {
            let (remaining, value) = try_parse!(e, parse_projection);
            Ok((remaining, OValue::EmbeddedMap(value.take_map())))
        })?;
        remaining = rm;
    }

    Ok((remaining, projection))
}

fn parse_value<'a, F>(remaining: &'a [u8], embedded: &F) -> IResult<&'a [u8], OValue>
where
    F: Fn(&[u8]) -> IResult<&[u8], OValue>,
{
    let (remaining, f_type) = try_parse!(remaining, be_i8);
    let (remaining, value) = match f_type {
        constants::BYTE => {
            let (remaining, value) = try_parse!(remaining, be_i8);
            (remaining, OValue::I8(value))
        }
        constants::BOOLEAN => {
            let (remaining, value) = try_parse!(remaining, parse_bool);
            (remaining, OValue::Boolean(value))
        }
        constants::LONG => {
            let (remaining, value) = try_parse!(remaining, parse_varint);
            (remaining, OValue::I64(value))
        }
        constants::INTEGER => {
            let (remaining, value) = try_parse!(remaining, parse_varint);
            (remaining, OValue::I32(value as i32))
        }
        constants::SHORT => {
            let (remaining, value) = try_parse!(remaining, parse_varint);
            (remaining, OValue::I16(value as i16))
        }
        constants::FLOAT => {
            let (remaining, value) = try_parse!(remaining, be_f32);
            (remaining, OValue::F32(value))
        }
        constants::DOUBLE => {
            let (remaining, value) = try_parse!(remaining, be_f64);
            (remaining, OValue::F64(value))
        }
        constants::STRING => {
            let (remaining, value) = try_parse!(remaining, parse_string_varint);
            (remaining, OValue::String(value))
        }
        constants::LINK => {
            let (remaining, value) = try_parse!(remaining, parse_optimized_identity);
            (remaining, OValue::Link(value))
        }
        constants::EMBEDDED => embedded(remaining)?,
        constants::LINKLIST | constants::LINKSET => {
            let (mut remaining, size) = try_parse!(remaining, parse_varint);
            let mut links = Vec::new();
            for _ in 0..size {
                let (rm, rid) = try_parse!(remaining, parse_optimized_identity);
                remaining = rm;
                links.push(rid);
            }
            (remaining, OValue::LinkList(links.into()))
        }
        constants::EMBEDDEDLIST | constants::EMBEDDEDSET => {
            let (mut remaining, size) = try_parse!(remaining, parse_varint);
            let mut elements = Vec::new();
            for _ in 0..size {
                let (rm, elem) = parse_value(remaining, embedded)?;
                remaining = rm;
                elements.push(elem)
            }
            (remaining, OValue::EmbeddedList(elements))
        }
        constants::EMBEDDEDMAP => {
            let (mut remaining, size) = try_parse!(remaining, parse_varint);
            let mut elements = HashMap::new();
            for _ in 0..size {
                let (rm, (k, v)) = parse_field(remaining, embedded)?;
                remaining = rm;
                elements.insert(k, v);
            }
            (remaining, OValue::EmbeddedMap(elements))
        }
        constants::DATE => {
            let (remaining, timestamp) = try_parse!(remaining, parse_varint);
            (
                remaining,
                OValue::Date(Utc.timestamp(timestamp * 86400, 0).date()),
            )
        }
        constants::DATETIME => {
            let (remaining, timestamp) = try_parse!(remaining, parse_varint);
            (
                remaining,
                OValue::DateTime(
                    Utc.timestamp(timestamp / 1000, (timestamp % 1000) as u32 * 1_000_000),
                ),
            )
        }
        constants::LINKBAG => {
            // just read for now and skip it
            let (remaining, (embedded, tree)) = try_parse!(remaining, parse_bags);
            let bag = embedded.unwrap_or_else(|| tree.expect("Cannot find tree or embedded bags"));
            (remaining, OValue::RidBag(bag))
        }
        constants::NULL => (remaining, OValue::Null),
        _ => panic!("Unsupported field type {}", f_type),
    };

    Ok((remaining, value))
}

fn parse_field<'a, F>(remaining: &'a [u8], embedded: &F) -> IResult<&'a [u8], (String, OValue)>
where
    F: Fn(&[u8]) -> IResult<&[u8], OValue>,
{
    let (remaining, name) = try_parse!(remaining, parse_string_varint);
    let (remaining, value) = parse_value(remaining, embedded)?;
    Ok((remaining, (name, value)))
}

// fn parse_field(remaining: &[u8]) -> IResult<&[u8], (String, OValue)> {
//     let (remaining, name) = try_parse!(remaining, parse_string_varint);
//     let (remaining, value) = try_parse!(remaining, parse_value);
//     Ok((remaining, (name, value)))
// }

named!(pub parse_bags<&[u8],(Option<RidBag>,Option<RidBag>)>,
    do_parse!(
        _uuid1 : be_i64 >>
        _uuid2 : be_i64 >>
        bag_type : be_u8 >>
        embedded : cond!(bag_type == 1,parse_bags_embedded ) >>
        tree : cond!(bag_type == 2,parse_bags_tree ) >>
        (embedded,tree)
    )
);

named!(pub parse_bags_embedded<&[u8],RidBag>,
    do_parse!(
        size : parse_varint >>
        bags : many_m_n!(0 as usize ,size as usize,parse_optimized_identity) >>
        (RidBag::Embedded(bags))
    )
);

// // TODO read changes
named!(pub parse_bags_tree<&[u8],RidBag>,
    do_parse!(
        _file_id : parse_varint >>
        _page_index : parse_varint >>
        _page_offset : parse_varint >>
        bag_size : parse_varint >>
        _changes_size : parse_varint >>
        (RidBag::Tree(bag_size as i32))
    )
);
