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

use crate::common::protocol::messages::request::HandShake;
use crate::common::protocol::messages::{
    response::LiveQueryResult, response::Status, Request, Response,
};
use crate::sync::protocol::WiredProtocol;
use crate::{OrientError, OrientResult};

use super::super::live::LiveQueryManager;
use super::decoder::decode;
use super::reader;
use crate::types::LiveResult;

pub type ChannelMsg = Cmd;

pub type ResponseChannel = Sender<OrientResult<Response>>;

use async_std::sync::{channel, Receiver, Sender};

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

async fn encode_and_write(
    stream: &Arc<TcpStream>,
    protocol: &mut WiredProtocol,
    request: Request,
) -> OrientResult<()> {
    let mut stream = &**stream;

    let buf = protocol.encode(request)?;
    stream.write_all(buf.as_slice()).await?;

    Ok(())
}
fn sender_loop(
    stream: Arc<TcpStream>,
    mut channel: Receiver<ChannelMsg>,
    queue: Arc<Mutex<VecDeque<ResponseChannel>>>,
    mut protocol: WiredProtocol,
    shutdown_flag: Arc<AtomicBool>,
) {
    task::spawn(async move {
        loop {
            match channel.next().await {
                Some(msg) => match msg {
                    Cmd::Msg(m) => {
                        let mut guard = queue.lock().await;

                        match encode_and_write(&stream, &mut protocol, m.1).await {
                            Ok(()) => {
                                guard.push_back(m.0);
                                drop(guard);
                            }
                            Err(e) => {
                                drop(guard);
                                m.0.send(Err(e)).await;
                            }
                        }
                    }
                    Cmd::MsgNoResponse(m) => {
                        match encode_and_write(&stream, &mut protocol, m.1).await {
                            Ok(()) => {
                                m.0.send(Ok(())).await;
                            }
                            Err(e) => {
                                m.0.send(Err(e)).await;
                            }
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

fn responder_loop(
    stream: Arc<TcpStream>,
    queue: Arc<Mutex<VecDeque<ResponseChannel>>>,
    protocol: WiredProtocol,
    live_manager: Arc<LiveQueryManager>,
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
                    Some(s) => {
                        drop(guard);
                        s.send(response).await;
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

        let mut buf = BufReader::new(&stream);

        let p = reader::read_i16(&mut buf).await?;
        let protocol = WiredProtocol::from_version(p)?;

        let (sender, receiver) = channel(20);

        let live_query_manager = Arc::new(LiveQueryManager::default());

        let conn = Connection {
            sender,
            live_query_manager: live_query_manager.clone(),
        };

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

        responder_loop(shared, queue, protocol, live_query_manager, shutdown_flag);

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
        let (sender, receiver) = channel(1);

        self.sender
            .send(Cmd::MsgNoResponse((sender, request)))
            .await;

        let result = receiver
            .recv()
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
        let (sender, receiver) = channel(1);
        self.sender.send(Cmd::Msg((sender, request))).await;
        receiver
            .recv()
            .await
            .expect("It should contain the response")
    }

    pub async fn close(self) -> OrientResult<()> {
        &self.sender.send(Cmd::Shutdown).await;
        Ok(())
    }
}
