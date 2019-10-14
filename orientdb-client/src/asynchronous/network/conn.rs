use std::collections::VecDeque;
use std::net::Shutdown;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_std::io::BufReader;
use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::sync::Mutex;
use async_std::task;
use futures::channel::mpsc;
use futures::channel::oneshot;

use crate::common::protocol::messages::request::HandShake;
use crate::common::protocol::messages::{Request, Response};
use crate::sync::protocol::WiredProtocol;
use crate::{OrientError, OrientResult};

use super::decoder::decode;
use super::reader;

pub type ChannelMsg = Cmd;

pub type ResponseChannel = oneshot::Sender<OrientResult<Response>>;

#[derive(Debug)]
pub enum Cmd {
    Msg((oneshot::Sender<OrientResult<Response>>, Request)),
    MsgNoResponse((oneshot::Sender<OrientResult<()>>, Request)),
    Shutdown,
}

pub struct Connection {
    sender: mpsc::UnboundedSender<ChannelMsg>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("Connection").finish()
    }
}

fn sender_loop(
    stream: Arc<TcpStream>,
    mut channel: mpsc::UnboundedReceiver<ChannelMsg>,
    queue: Arc<Mutex<VecDeque<ResponseChannel>>>,
    mut protocol: WiredProtocol,
    shutdown_flag: Arc<AtomicBool>,
) {
    task::spawn(async move {
        let mut stream = &*stream;
        loop {
            match channel.next().await {
                Some(msg) => match msg {
                    Cmd::Msg(m) => {
                        let mut guard = queue.lock().await;
                        guard.push_back(m.0);
                        drop(guard);
                        let buf = protocol.encode(m.1).unwrap();
                        stream.write_all(buf.as_slice()).await.unwrap();
                    }
                    Cmd::MsgNoResponse(m) => {
                        let buf = protocol.encode(m.1).unwrap();
                        stream.write_all(buf.as_slice()).await.unwrap();
                        m.0.send(Ok(())).unwrap();
                    }
                    Cmd::Shutdown => {
                        shutdown_flag.store(true, Ordering::SeqCst);
                        stream.shutdown(Shutdown::Both).unwrap();
                    }
                },
                None => {
                    shutdown_flag.store(true, Ordering::SeqCst);
                    stream.shutdown(Shutdown::Both).unwrap();
                    break;
                }
            }
        }
    });
}

fn responder_loop(
    stream: Arc<TcpStream>,
    queue: Arc<Mutex<VecDeque<ResponseChannel>>>,
    protocol: WiredProtocol,
    shutdown_flag: Arc<AtomicBool>,
) {
    task::spawn(async move {
        let stream = &*stream;

        let mut buf = BufReader::new(stream);

        loop {
            let response = decode(protocol.version, &mut buf).await;

            if let Err(e) = &response {
                match e {
                    OrientError::Protocol(_p) => {
                        if shutdown_flag.load(Ordering::SeqCst) {
                            break;
                        }
                    }
                    _ => {}
                }
            }

            let mut guard = queue.lock().await;

            match guard.pop_front() {
                Some(s) => {
                    drop(guard);
                    s.send(response).unwrap();
                }
                None => {
                    
                }
            }
        }
    });
}

impl Connection {
    pub async fn connect(addr: &SocketAddr) -> OrientResult<Self> {
        let stream = TcpStream::connect(addr).await?;

        let mut buf = BufReader::new(&stream);

        let p = reader::read_i16(&mut buf).await?;
        let protocol = WiredProtocol::from_version(p)?;

        let (sender, receiver) = mpsc::unbounded();

        let conn = Connection { sender };

        let shared = Arc::new(stream);
        let shutdown_flag = Arc::new(AtomicBool::new(false));

        let queue = Arc::new(Mutex::new(VecDeque::new()));

        let p_version = protocol.version;

        sender_loop(
            shared.clone(),
            receiver,
            queue.clone(),
            protocol.clone(),
            shutdown_flag.clone(),
        );

        responder_loop(shared, queue, protocol, shutdown_flag);

        conn.handshake(p_version).await
    }

    async fn handshake(mut self, p_version: i16) -> OrientResult<Connection> {
        let handshake = HandShake {
            p_version,
            name: String::from("Rust Driver"),
            version: String::from("0.1"),
        };
        self.send_and_forget(handshake.into()).await?;
        Ok(self)
    }

    pub(crate) async fn send_and_forget(&mut self, request: Request) -> OrientResult<()> {
        let (sender, receiver) = oneshot::channel();

        mpsc::UnboundedSender::unbounded_send(&self.sender, Cmd::MsgNoResponse((sender, request)))
            .unwrap();

        let result = receiver.await.unwrap();

        return result;
    }

    pub async fn send(&mut self, request: Request) -> OrientResult<Response> {
        let (sender, receiver) = oneshot::channel();
        mpsc::UnboundedSender::unbounded_send(&self.sender, Cmd::Msg((sender, request))).unwrap();
        receiver.await.unwrap()
    }

    pub fn close(self) -> OrientResult<()> {
        mpsc::UnboundedSender::unbounded_send(&self.sender, Cmd::Shutdown).unwrap();
        Ok(())
    }
}
