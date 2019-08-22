use crate::common::protocol::messages::request::HandShake;
use crate::common::protocol::messages::{Request, Response};
use crate::sync::protocol::WiredProtocol;
use crate::OrientResult;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::Write;
use std::net::Shutdown;
use std::net::{SocketAddr, TcpStream};

pub struct Connection {
    stream: TcpStream,
    protocol: WiredProtocol,
}

impl Connection {
    pub fn connect(addr: &SocketAddr) -> OrientResult<Self> {
        let mut stream = TcpStream::connect(addr)?;
        let p = stream.read_i16::<BigEndian>()?;
        let protocol = WiredProtocol::from_version(p)?;
        let conn = Connection { stream, protocol };
        conn.handshake()
    }

    fn handshake(mut self) -> OrientResult<Connection> {
        let handshake = HandShake {
            p_version: self.protocol.version,
            name: String::from("Rust Driver"),
            version: String::from("0.1"),
        };
        self.send_and_forget(handshake.into())?;
        Ok(self)
    }

    pub fn close(&mut self) -> OrientResult<()> {
        self.stream.shutdown(Shutdown::Both)?;
        Ok(())
    }

    pub fn send_and_forget(&mut self, request: Request) -> OrientResult<()> {
        let buf = self.protocol.encode(request)?;
        self.stream.write_all(buf.as_slice())?;
        Ok(())
    }
    pub fn send(&mut self, request: Request) -> OrientResult<Response> {
        self.send_and_forget(request)?;
        self.protocol.decode(&mut self.stream)
    }

    
}
