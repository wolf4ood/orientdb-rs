use crate::common::OrientCommonResult;
use byteorder::{BigEndian, WriteBytesExt};
use std::io::Write;

pub struct OBuffer {
    inner: Vec<u8>,
}

impl OBuffer {
    pub fn new() -> OBuffer {
        OBuffer { inner: Vec::new() }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }

    pub fn put_i8(&mut self, n: i8) -> OrientCommonResult<()> {
        self.inner.write_i8(n)?;
        Ok(())
    }

    pub fn put_u8(&mut self, n: u8) -> OrientCommonResult<()> {
        self.inner.write_u8(n)?;
        Ok(())
    }

    pub fn put_i32(&mut self, n: i32) -> OrientCommonResult<()> {
        self.inner.write_i32::<BigEndian>(n)?;
        Ok(())
    }
    pub fn put_i16(&mut self, n: i16) -> OrientCommonResult<()> {
        self.inner.write_i16::<BigEndian>(n)?;
        Ok(())
    }

    pub fn put_slice(&mut self, src: &[u8]) -> OrientCommonResult<()> {
        self.inner.write_all(src)?;
        Ok(())
    }

    pub fn write_str(&mut self, str: &str) -> OrientCommonResult<()> {
        let bytes = str.as_bytes();
        let size = bytes.len();
        self.put_i32(size as i32)?;
        self.put_slice(bytes)?;
        Ok(())
    }
    pub fn write_slice(&mut self, bytes: &[u8]) -> OrientCommonResult<()> {
        let size = bytes.len();
        self.put_i32(size as i32)?;
        self.put_slice(bytes)?;
        Ok(())
    }

    pub fn write_bool(&mut self, boolean: bool) -> OrientCommonResult<()> {
        if boolean {
            self.put_i8(1)
        } else {
            self.put_i8(0)
        }?;
        Ok(())
    }

    pub fn write_varint(&mut self, number: i64) -> OrientCommonResult<()> {
        let mut real_value: u64 = ((number << 1) ^ (number >> 63)) as u64;
        while real_value & 0xFFFF_FFFF_FFFF_FF80 != 0 {
            self.put_u8(((real_value & 0x7F) | 0x80) as u8)?;
            real_value >>= 7;
        }
        self.put_u8((real_value & 0x7F) as u8)?;
        Ok(())
    }

    pub fn write_string(&mut self, val: &str) -> OrientCommonResult<()> {
        self.write_varint(val.len() as i64)?;
        self.put_slice(val.as_bytes())?;
        Ok(())
    }
}
