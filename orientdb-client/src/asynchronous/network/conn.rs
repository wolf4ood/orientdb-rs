use crate::common::protocol::messages::request::HandShake;
use crate::common::protocol::messages::{Request, Response};
use async_std::net::TcpStream;
use async_std::prelude::*;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Cursor;
use std::net::Shutdown;

use crate::OrientResult;
use std::net::SocketAddr;

use super::decoder::decode;
use super::reader;
use crate::sync::protocol::WiredProtocol;

pub struct Connection {
    stream: TcpStream,
    protocol: WiredProtocol,
}

impl Connection {
    pub async fn connect(addr: &SocketAddr) -> OrientResult<Self> {
        let mut stream = TcpStream::connect(addr).await?;

        let p = reader::read_i16(&mut stream).await?;

        let protocol = WiredProtocol::from_version(p)?;

        let conn = Connection { stream, protocol };

        conn.handshake().await
    }

    async fn handshake(mut self) -> OrientResult<Connection> {
        let handshake = HandShake {
            p_version: self.protocol.version,
            name: String::from("Rust Driver"),
            version: String::from("0.1"),
        };
        self.send_and_forget(handshake.into()).await?;
        Ok(self)
    }

    async fn send_and_forget(&mut self, request: Request) -> OrientResult<()> {
        let buf = self.protocol.encode(request)?;
        self.stream.write_all(buf.as_slice()).await?;
        Ok(())
    }

    pub async fn send(&mut self, request: Request) -> OrientResult<Response> {
        self.send_and_forget(request).await?;
        decode(self.protocol.version, &mut self.stream).await
    }

    pub fn close(self) -> OrientResult<()> {
        self.stream.shutdown(Shutdown::Both)?;

        Ok(())
    }
}
