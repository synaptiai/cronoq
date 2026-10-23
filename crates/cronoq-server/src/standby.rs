//! Standbys close client connections, so that clients reach the leader.
//!
//! A standby answers every call with `UNAVAILABLE`. Behind a load balancer that sends
//! connections to every replica (a Kubernetes Service whose pods are all ready, for example), a
//! gRPC client keeps its HTTP/2 connection and would keep reaching the same standby. So while
//! the server stands by, each connection reads end-of-file once it is [`GRACE`] old: the client
//! gets its `UNAVAILABLE` answer, then has to reconnect, and the balancer picks a replica again.
//! A leader that steps down closes its connections the same way. During a graceful shutdown
//! connections stay open, so running attempts can still report.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Instant, Sleep};

use crate::state::Shared;

/// How long a standby keeps a connection, and how often a leader's connections check whether
/// it still leads.
pub const GRACE: Duration = Duration::from_secs(1);

/// A connection that ends (reads end-of-file) while the server is a standby.
pub struct CloseOnStandby<IO> {
    io: IO,
    shared: Arc<Shared>,
    check: Pin<Box<Sleep>>,
    closed: bool,
}

impl<IO> CloseOnStandby<IO> {
    /// Wrap a connection accepted now.
    pub fn new(io: IO, shared: Arc<Shared>) -> Self {
        CloseOnStandby {
            io,
            shared,
            check: Box::pin(tokio::time::sleep(GRACE)),
            closed: false,
        }
    }

    /// Whether the connection should end now; arms the next check otherwise.
    fn should_close(&mut self, cx: &mut Context<'_>) -> bool {
        while !self.closed && self.check.as_mut().poll(cx).is_ready() {
            if self.shared.is_standby() {
                self.closed = true;
            } else {
                self.check.as_mut().reset(Instant::now() + GRACE);
            }
        }
        self.closed
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for CloseOnStandby<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.should_close(cx) {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for CloseOnStandby<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
}

impl<IO: tonic::transport::server::Connected> tonic::transport::server::Connected
    for CloseOnStandby<IO>
{
    type ConnectInfo = IO::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}
