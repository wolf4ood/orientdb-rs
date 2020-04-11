use core::task::Context;
use core::task::Poll;
use futures::io;
use std::net::Shutdown;
use std::pin::Pin;

use crate::OrientResult;

// async-std impl

#[cfg(feature = "async-std-runtime")]
mod async_std_use {
    pub use async_std::io::BufReader;
    pub use async_std::net::TcpStream;
    pub use async_std::sync::{channel, Mutex, Receiver, Sender};
    pub use async_std::task;
    pub use std::sync::Arc;
}
#[cfg(feature = "async-std-runtime")]
use async_std_use::*;

#[cfg(feature = "tokio-runtime")]
mod tokio_use {
    pub use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
    pub use tokio::net::TcpStream;
    pub use tokio::sync::mpsc::{channel, Receiver, Sender};
    pub use tokio::sync::Mutex;
    pub use tokio::task;
}

#[cfg(feature = "tokio-runtime")]
pub use tokio_use::*;

#[cfg(feature = "async-std-runtime")]
pub(crate) struct StreamReader(Arc<TcpStream>);

#[cfg(feature = "async-std-runtime")]
impl io::AsyncRead for StreamReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self.0).poll_read(cx, buf)
    }
}

#[cfg(feature = "async-std-runtime")]
pub(crate) struct StreamWriter(Arc<TcpStream>);

#[cfg(feature = "async-std-runtime")]
impl ShutdownStream for StreamWriter {
    fn shutdown(&self, mode: Shutdown) -> OrientResult<()> {
        self.0.shutdown(mode)?;

        Ok(())
    }
}

#[cfg(feature = "async-std-runtime")]
pub(crate) fn split(stream: TcpStream) -> (StreamReader, StreamWriter) {
    let shared = Arc::new(stream);

    (StreamReader(shared.clone()), StreamWriter(shared))
}

#[cfg(feature = "async-std-runtime")]
impl io::AsyncWrite for StreamWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut &*self.0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.0).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut &*self.0).poll_close(cx)
    }
}

// tokio impl

#[cfg(feature = "tokio-runtime")]
pub(crate) fn split(stream: TcpStream) -> (StreamReader, StreamWriter) {
    use tokio::io::split as split_stream;

    let (reader, writer) = split_stream(stream);

    (StreamReader(reader), StreamWriter(writer))
}

#[cfg(feature = "tokio-runtime")]
pub(crate) struct StreamReader(ReadHalf<TcpStream>);

#[cfg(feature = "tokio-runtime")]
impl io::AsyncRead for StreamReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<tokio::io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

#[cfg(feature = "tokio-runtime")]
pub(crate) struct StreamWriter(WriteHalf<TcpStream>);

#[cfg(feature = "tokio-runtime")]
impl io::AsyncWrite for StreamWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

#[cfg(feature = "tokio-runtime")]
impl ShutdownStream for StreamWriter {
    fn shutdown(&self, _mode: Shutdown) -> OrientResult<()> {
        Ok(())
    }
}

pub(crate) trait ShutdownStream {
    fn shutdown(&self, mode: Shutdown) -> OrientResult<()>;
}
