use crate::common::types::rid::ORecordID;
use crate::OrientResult;
use async_std::prelude::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;

use async_std::io::BufReader;
use async_std::net::TcpStream;

pub async fn read_i8(buf: &mut BufReader<&TcpStream>) -> OrientResult<i8> {
    let mut buffer = vec![0u8; 1];
    buf.read(&mut buffer).await?;
    let mut cursor = Cursor::new(buffer);
    let res = cursor.read_i8()?;
    Ok(res)
}

pub async fn read_i32(buf: &mut BufReader<&TcpStream>) -> OrientResult<i32> {
    let mut buffer = vec![0u8; 4];
    buf.read(&mut buffer).await?;
    let mut cursor = Cursor::new(buffer);
    let res = cursor.read_i32::<BigEndian>()?;
    Ok(res)
}

pub async fn read_i64(buf: &mut BufReader<&TcpStream>) -> OrientResult<i64> {
    let mut buffer = vec![0u8; 8];
    buf.read(&mut buffer).await?;
    let mut cursor = Cursor::new(buffer);
    let res = cursor.read_i64::<BigEndian>()?;
    Ok(res)
}

pub async fn read_identity(buf: &mut BufReader<&TcpStream>) -> OrientResult<ORecordID> {
    let cluster_id = read_i16(buf).await?;
    let cluster_position = read_i64(buf).await?;
    Ok(ORecordID::new(cluster_id, cluster_position))
}

pub async fn read_i16(buf: &mut BufReader<&TcpStream>) -> OrientResult<i16> {
    let mut buffer = vec![0u8; 2];
    buf.read(&mut buffer).await?;
    let mut cursor = Cursor::new(buffer);
    let res = cursor.read_i16::<BigEndian>()?;
    Ok(res)
}

pub async fn read_bool(buf: &mut BufReader<&TcpStream>) -> OrientResult<bool> {
    let e = read_i8(buf).await?;
    let val = match e {
        0 => false,
        1 => true,
        _ => panic!("Cannot convert value to bool"),
    };
    Ok(val)
}

pub async fn read_optional_bytes(buf: &mut BufReader<&TcpStream>) -> OrientResult<Option<Vec<u8>>> {
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

pub async fn read_bytes(buf: &mut BufReader<&TcpStream>) -> OrientResult<Vec<u8>> {
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

pub async fn read_string(buf: &mut BufReader<&TcpStream>) -> OrientResult<String> {
    let bytes = read_bytes(buf).await?;
    let res = String::from_utf8(bytes)?;
    Ok(res)
}
