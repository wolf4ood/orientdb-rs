use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::collections::VecDeque;
use std::net::Shutdown;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::common::protocol::messages::request::HandShake;
use crate::common::protocol::messages::{
    response::LiveQueryResult, response::Status, Request, Response,
};
use crate::sync::protocol::WiredProtocol;
use crate::{OrientError, OrientResult};

use super::super::live::LiveQueryManager;
use super::decoder::decode;
use super::reader;
use crate::asynchronous::network::stream::{split, ShutdownStream};
use crate::types::LiveResult;

#[cfg(feature = "async-std-runtime")]
mod async_std_use {
    pub use async_std::io::BufReader;
    pub use async_std::net::TcpStream;
    pub use async_std::sync::Mutex;
    pub use async_std::task;
}
#[cfg(feature = "async-std-runtime")]
use async_std_use::*;

#[cfg(feature = "tokio-runtime")]
mod tokio_use {
    pub use tokio::io::BufReader;
    pub use tokio::net::TcpStream;
    pub use tokio::sync::Mutex;
    pub use tokio::task;
}

use futures::channel::mpsc::{channel, Receiver, Sender};

#[cfg(feature = "tokio-runtime")]
pub use tokio_use::*;

pub type ChannelMsg = Cmd;
pub type ResponseChannel = Sender<OrientResult<Response>>;

#[derive(Debug)]
pub enum Cmd {
    Msg((Sender<OrientResult<Response>>, Request)),
    MsgNoResponse((Sender<OrientResult<()>>, Request)),
    Shutdown,
}

pub struct Connection {
    sender: Sender<ChannelMsg>,
    live_query_manager: Arc<LiveQueryManager>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("Connection").finish()
    }
}

async fn encode_and_write<T>(
    stream: &mut T,
    protocol: &mut WiredProtocol,
    request: Request,
) -> OrientResult<()>
where
    T: AsyncWrite + Unpin,
{
    let buf = protocol.encode(request)?;
    stream.write_all(buf.as_slice()).await?;

    Ok(())
}
fn sender_loop<T>(
    mut stream: T,
    mut channel: Receiver<ChannelMsg>,
    queue: Arc<Mutex<VecDeque<ResponseChannel>>>,
    mut protocol: WiredProtocol,
    shutdown_flag: Arc<AtomicBool>,
) where
    T: AsyncWrite + Send + ShutdownStream + Unpin + 'static,
{
    task::spawn(async move {
        loop {
            match channel.next().await {
                Some(msg) => match msg {
                    Cmd::Msg(mut m) => {
                        let mut guard = queue.lock().await;

                        match encode_and_write(&mut stream, &mut protocol, m.1).await {
                            Ok(()) => {
                                guard.push_back(m.0);
                                drop(guard);
                            }
                            Err(e) => {
                                drop(guard);
                                match m.0.send(Err(e)).await {
                                    Ok(_) => {}
                                    Err(_e) => {}
                                };
                            }
                        }
                    }
                    Cmd::MsgNoResponse(mut m) => {
                        match encode_and_write(&mut stream, &mut protocol, m.1).await {
                            Ok(()) => match m.0.send(Ok(())).await {
                                Ok(_e) => {}
                                Err(_e) => {}
                            },
                            Err(e) => match m.0.send(Err(e)).await {
                                Ok(_e) => {}
                                Err(_e) => {}
                            },
                        }
                    }
                    Cmd::Shutdown => {
                        shutdown_flag.store(true, Ordering::SeqCst);
                        stream
                            .shutdown(Shutdown::Both)
                            .expect("Failed to shutdown the socket");
                    }
                },
                None => {
                    shutdown_flag.store(true, Ordering::SeqCst);

                    match stream.shutdown(Shutdown::Both) {
                        Ok(_e) => {}
                        Err(_e) => {}
                    }
                    break;
                }
            }
        }
    });
}

fn responder_loop<T>(
    mut stream: T,
    queue: Arc<Mutex<VecDeque<ResponseChannel>>>,
    protocol: WiredProtocol,
    live_manager: Arc<LiveQueryManager>,
    shutdown_flag: Arc<AtomicBool>,
) where
    T: AsyncRead + Send + Unpin + 'static,
{
    task::spawn(async move {
        loop {
            let response = decode(protocol.version, &mut stream).await;

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

            let result = match response {
                Ok(mut r) => match r.header.status {
                    Status::PUSH => {
                        let live_result: LiveQueryResult = r.payload::<LiveQueryResult>();
                        match live_manager.fire_event(live_result).await {
                            Ok(_) => {}
                            Err(_e) => {}
                        }
                        None
                    }
                    _ => Some(Ok(r)),
                },
                Err(e) => Some(Err(e)),
            };

            if let Some(response) = result {
                let mut guard = queue.lock().await;

                match guard.pop_front() {
                    Some(mut s) => {
                        drop(guard);
                        match s.send(response).await {
                            Ok(_m) => {}
                            Err(_e) => {}
                        }
                    }
                    None => {}
                }
            }
        }
    });
}

impl Connection {
    pub async fn connect(addr: &SocketAddr) -> OrientResult<Self> {
        let stream = TcpStream::connect(addr).await?;

        let (mut reader, writer) = split(stream);

        let p = reader::read_i16(&mut reader).await?;

        let protocol = WiredProtocol::from_version(p)?;

        let (sender, receiver) = channel(20);

        let live_query_manager = Arc::new(LiveQueryManager::default());

        let conn = Connection {
            sender,
            live_query_manager: live_query_manager.clone(),
        };

        let shutdown_flag = Arc::new(AtomicBool::new(false));

        let queue = Arc::new(Mutex::new(VecDeque::new()));

        let p_version = protocol.version;

        sender_loop(
            writer,
            receiver,
            queue.clone(),
            protocol.clone(),
            shutdown_flag.clone(),
        );

        responder_loop(reader, queue, protocol, live_query_manager, shutdown_flag);

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
        let (sender, mut receiver) = channel(1);

        self.sender
            .send(Cmd::MsgNoResponse((sender, request)))
            .await?;

        let result = receiver
            .next()
            .await
            .expect("It should contain the response");

        return result;
    }

    pub async fn register_handler(
        &self,
        monitor_id: i32,
        sender: Sender<OrientResult<LiveResult>>,
    ) -> OrientResult<()> {
        self.live_query_manager
            .register_handler(monitor_id, sender)
            .await
    }

    pub async fn send(&mut self, request: Request) -> OrientResult<Response> {
        let (sender, mut receiver) = channel(1);
        self.sender.send(Cmd::Msg((sender, request))).await?;
        receiver
            .next()
            .await
            .expect("It should contain the response")
    }

    pub async fn close(mut self) -> OrientResult<()> {
        &self.sender.send(Cmd::Shutdown).await;
        Ok(())
    }
}
