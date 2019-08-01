use crate::common::types::rid::ORecordID;
use nom::number::streaming::{be_i8, be_u8};
use nom::IResult;
use nom::{do_parse, named, take, try_parse};

named!(pub parse_bool<&[u8],bool>,
    do_parse!(
        val : be_i8 >>
        (val == 1)
    )
);

named!(pub parse_optimized_identity<&[u8],ORecordID>,
    do_parse!(
        cluster_id : parse_varint >>
        cluster_position : parse_varint >>
        (ORecordID::new(cluster_id as i16,cluster_position))
    )
);

named!(pub parse_string_varint<&[u8],String>,
  do_parse!(
    length : parse_varint >>
    bytes:  take!(length) >>
    (String::from_utf8(bytes.to_vec()).unwrap())
  )
);

pub fn parse_varint(input: &[u8]) -> IResult<&[u8], i64> {
    let mut value: u64 = 0;
    let mut i: i64 = 0;
    let mut b: u64;
    let mut inc = 0;
    loop {
        let (_, parsed) = try_parse!(&input[inc..], be_u8);
        inc += 1;
        b = u64::from(parsed);
        if (b & 0x80) != 0 {
            value |= (b & 0x7F) << i;
            i += 7;
            if i > 63 {
                panic!("Error deserializing varint")
            }
        }
        if (b & 0x80) == 0 {
            break;
        }
    }
    value |= b << i;
    Ok((
        &input[inc..],
        (((value >> 1) as i64) ^ (-((value & 1) as i64))),
    ))
}

#[cfg(test)]
mod tests {
    use super::super::buffer::OBuffer;
    use super::{parse_string_varint, parse_varint};

    #[test]
    fn test_parse_varint() {
        let mut buf = OBuffer::new();

        buf.write_varint(20).unwrap();

        let result = parse_varint(buf.as_slice());

        assert_eq!(result, Ok((&b""[..], 20)));
    }

    #[test]
    fn test_parse_string_varint() {
        let mut buf = OBuffer::new();

        buf.write_string("text").unwrap();
        let result = parse_string_varint(buf.as_slice());
        assert_eq!(result, Ok((&b""[..], String::from("text"))));
    }

    #[test]
    fn test_read_write_varint() {
        fn write_read_varint(val: i64) -> i64 {
            let mut buf = OBuffer::new();
            buf.write_varint(val).unwrap();
            let (remaining, read) = parse_varint(buf.as_slice()).unwrap();
            assert_eq!(remaining.len(), 0);
            read
        }
        assert_eq!(write_read_varint(12), 12);
        assert_eq!(write_read_varint(234), 234);
        assert_eq!(write_read_varint(43234), 43234);
        assert_eq!(write_read_varint(46576443234), 46576443234);
        assert_eq!(write_read_varint(534), 534);
        assert_eq!(write_read_varint(-1), -1);
        assert_eq!(write_read_varint(-534), -534);
    }
}
