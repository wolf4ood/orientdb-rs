use crate::common::types::rid::ORecordID;
use crate::OrientResult;
use byteorder::{BigEndian, ReadBytesExt};
use futures::io::{AsyncRead, AsyncReadExt};
use std::io::Cursor;

pub async fn read_i8<T>(buf: &mut T) -> OrientResult<i8>
where
    T: AsyncRead + Unpin,
{
    let mut buffer = vec![0u8; 1];
    buf.read(&mut buffer).await?;
    let mut cursor = Cursor::new(buffer);
    let res = cursor.read_i8()?;
    Ok(res)
}

pub async fn read_i32<T>(buf: &mut T) -> OrientResult<i32>
where
    T: AsyncRead + Unpin,
{
    let mut buffer = vec![0u8; 4];
    buf.read(&mut buffer).await?;
    let mut cursor = Cursor::new(buffer);
    let res = cursor.read_i32::<BigEndian>()?;
    Ok(res)
}

pub async fn read_i64<T>(buf: &mut T) -> OrientResult<i64>
where
    T: AsyncRead + Unpin,
{
    let mut buffer = vec![0u8; 8];
    buf.read(&mut buffer).await?;
    let mut cursor = Cursor::new(buffer);
    let res = cursor.read_i64::<BigEndian>()?;
    Ok(res)
}

pub async fn read_identity<T>(buf: &mut T) -> OrientResult<ORecordID>
where
    T: AsyncRead + Unpin,
{
    let cluster_id = read_i16(buf).await?;
    let cluster_position = read_i64(buf).await?;
    Ok(ORecordID::new(cluster_id, cluster_position))
}

pub async fn read_i16<T>(buf: &mut T) -> OrientResult<i16>
where
    T: AsyncRead + Unpin,
{
    let mut buffer = vec![0u8; 2];
    buf.read(&mut buffer).await?;
    let mut cursor = Cursor::new(buffer);
    let res = cursor.read_i16::<BigEndian>()?;
    Ok(res)
}

pub async fn read_bool<T>(buf: &mut T) -> OrientResult<bool>
where
    T: AsyncRead + Unpin,
{
    let e = read_i8(buf).await?;
    let val = match e {
        0 => false,
        1 => true,
        _ => panic!("Cannot convert value to bool"),
    };
    Ok(val)
}

pub async fn read_optional_bytes<T>(buf: &mut T) -> OrientResult<Option<Vec<u8>>>
where
    T: AsyncRead + Unpin,
{
    let size = read_i32(buf).await?;
    let mut buff;
    if size == -1 {
        return Ok(None);
    } else {
        buff = vec![0; size as usize];
        buf.read(&mut buff).await?;
    }
    Ok(Some(buff))
}

pub async fn read_bytes<T>(buf: &mut T) -> OrientResult<Vec<u8>>
where
    T: AsyncRead + Unpin,
{
    let size = read_i32(buf).await?;
    let mut buff;
    if size == -1 {
        buff = vec![];
    } else {
        buff = vec![0; size as usize];
        buf.read(&mut buff).await?;
    }
    Ok(buff)
}

pub async fn read_string<T>(buf: &mut T) -> OrientResult<String>
where
    T: AsyncRead + Unpin,
{
    let bytes = read_bytes(buf).await?;
    let res = String::from_utf8(bytes)?;
    Ok(res)
}
