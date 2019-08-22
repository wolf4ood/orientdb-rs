pub mod reader {

    use crate::common::types::rid::ORecordID;
    use crate::OrientResult;
    use byteorder::{BigEndian, ReadBytesExt};
    use std::io::Read;

    pub fn read_i8<T: Read>(buf: &mut T) -> OrientResult<i8> {
        let res = buf.read_i8()?;
        Ok(res)
    }

    pub fn read_i32<T: Read>(buf: &mut T) -> OrientResult<i32> {
        let res = buf.read_i32::<BigEndian>()?;
        Ok(res)
    }

    pub fn read_i64<T: Read>(buf: &mut T) -> OrientResult<i64> {
        let res = buf.read_i64::<BigEndian>()?;
        Ok(res)
    }

    pub fn read_identity<T: Read>(buf: &mut T) -> OrientResult<ORecordID> {
        let cluster_id = read_i16(buf)?;
        let cluster_position = read_i64(buf)?;
        Ok(ORecordID::new(cluster_id, cluster_position))
    }

    pub fn read_i16<T: Read>(buf: &mut T) -> OrientResult<i16> {
        let res = buf.read_i16::<BigEndian>()?;
        Ok(res)
    }

    pub fn read_bool<T: Read>(buf: &mut T) -> OrientResult<bool> {
        let e = buf.read_i8()?;
        let exhists = match e {
            0 => false,
            1 => true,
            _ => panic!("Cannot convert value to bool"),
        };
        Ok(exhists)
    }

    pub fn read_optional_bytes<T: Read>(buf: &mut T) -> OrientResult<Option<Vec<u8>>> {
        let size = read_i32(buf)?;
        let mut buff;
        if size == -1 {
            return Ok(None);
        } else {
            buff = vec![0; size as usize];
            let mut handle = buf.take(size as u64);
            handle.read_exact(&mut buff)?;
        }
        Ok(Some(buff))
    }
    pub fn read_bytes<T: Read>(buf: &mut T) -> OrientResult<Vec<u8>> {
        let size = read_i32(buf)?;
        let mut buff;
        if size == -1 {
            buff = vec![];
        } else {
            buff = vec![0; size as usize];
            let mut handle = buf.take(size as u64);
            handle.read_exact(&mut buff)?;
        }
        Ok(buff)
    }

    pub fn read_string<T: Read>(buf: &mut T) -> OrientResult<String> {
        let bytes = read_bytes(buf)?;
        let res = String::from_utf8(bytes)?;
        Ok(res)
    }
}
