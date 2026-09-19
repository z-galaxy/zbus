//! The socket a connection does its I/O through.
//!
//! Every socket a connection owns is registered on that connection's runtime once and then read,
//! written, shut down and asked for peer credentials through that one registration. What tells a
//! unix socket apart from a TCP or VSOCK stream, or from a pipe to a helper process, is a
//! [`SocketOps`], so the readiness machinery itself exists once rather than once per transport and
//! per runtime.

#[cfg(any(unix, windows))]
mod unix;

// Only a helper process is talked to over pipes.
#[cfg(all(unix, any(feature = "unixexec", feature = "ibus", target_os = "macos")))]
mod pipe;

pub(crate) mod tcp;
#[cfg(feature = "vsock")]
mod vsock;

mod connect;
pub(crate) use connect::connect;

// `RefusedPort`, which reserves the port the connection tests aim at, lives here.
#[cfg(all(test, any(unix, windows)))]
pub(crate) mod tests;

#[cfg(unix)]
use std::os::fd::BorrowedFd;
use std::{
    fmt,
    future::{Future, poll_fn},
    io,
    net::Shutdown,
    sync::Arc,
    task::{Context, Poll},
};

use socket2::SockRef;

#[cfg(all(unix, any(feature = "unixexec", feature = "ibus", target_os = "macos")))]
pub(crate) use pipe::PipeOps;
pub(crate) use tcp::TcpOps;
#[cfg(any(unix, windows))]
pub(crate) use unix::UnixOps;
#[cfg(feature = "vsock")]
pub(crate) use vsock::VsockOps;

use super::{Interest, IoSource, Runtime, erased::ErasedRegistration, traits};
use crate::{
    conn::AuthMechanism,
    connection::socket::{ReadHalf, RecvmsgResult, Socket, Split, WriteHalf},
    fdo::ConnectionCredentials,
};

/// A socket the connection's runtime watches, together with the one path its I/O takes.
#[derive(Debug)]
pub(crate) struct RegisteredIo<O> {
    // Fields drop in the order they are declared, and a registration has to stop watching a
    // descriptor before the last owner of that descriptor lets go of it.
    registration: Registration,
    source: IoSource,
    // So that the ops can hand the blocking part of an operation, such as the peer's
    // supplementary groups, to the same runtime the rest of the connection runs on.
    runtime: Runtime,
    ops: O,
}

impl<O> RegisteredIo<O>
where
    O: SocketOps,
{
    /// Registers `source` on `runtime` and drives it with `ops`.
    pub(crate) fn new(runtime: &Runtime, source: IoSource, ops: O) -> io::Result<Self> {
        Ok(Self {
            registration: runtime.register_io_source(source.clone())?,
            source,
            runtime: runtime.clone(),
            ops,
        })
    }

    /// Runs `operation` as soon as the socket is readable.
    pub(crate) async fn read_with<T>(
        &self,
        operation: impl FnMut() -> io::Result<T>,
    ) -> io::Result<T> {
        self.io(Interest::Readable, operation).await
    }

    /// Runs `operation` as soon as the socket is writable.
    pub(crate) async fn write_with<T>(
        &self,
        operation: impl FnMut() -> io::Result<T>,
    ) -> io::Result<T> {
        self.io(Interest::Writable, operation).await
    }

    /// Runs `operation` as soon as the socket is ready for `interest`.
    ///
    /// A call the kernel interrupts is made again straight away: that is not a readiness
    /// question, so it never reaches the runtime.
    async fn io<T>(
        &self,
        interest: Interest,
        mut operation: impl FnMut() -> io::Result<T>,
    ) -> io::Result<T> {
        self.registration
            .io(interest, move || {
                loop {
                    match operation() {
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        result => return result,
                    }
                }
            })
            .await
    }
}

/// Registers a descriptor zbus was handed, after switching it to non-blocking mode.
///
/// The streams a builder is given and both pipes of a helper process come through here. The
/// sockets zbus opens for a `unix:`, `tcp:` or `vsock:` address do not: `connect` creates those
/// non-blocking and registers them itself. A descriptor zbus did not open has to be switched
/// over before it is watched: a runtime waits for readiness and then runs the operation, which
/// would hold up the thread it is polled on if the descriptor still blocked.
///
/// The bound is the standard library's own conversion into what a platform owns a descriptor as,
/// which every stream involved has — bar one, `uds_windows::UnixStream`, handled where it is
/// taken over.
pub(crate) fn registered<S, O>(runtime: &Runtime, stream: S, ops: O) -> io::Result<RegisteredIo<O>>
where
    S: Into<Owned>,
    O: SocketOps,
{
    let source = IoSource::from(stream.into());
    // socket2 is what gives one call for both platforms here: `fcntl` on a unix descriptor,
    // which a pipe takes as readily as a socket does, and `ioctlsocket` on a Windows one.
    SockRef::from(&source).set_nonblocking(true)?;

    RegisteredIo::new(runtime, source, ops)
}

/// What this platform owns a socket or pipe as.
#[cfg(unix)]
type Owned = std::os::fd::OwnedFd;
#[cfg(windows)]
type Owned = std::os::windows::io::OwnedSocket;

/// The non-blocking operations of one family of socket: unix, TCP, VSOCK or a pipe.
///
/// Every method here is one attempt at a syscall on a descriptor that is already non-blocking,
/// and it is [`RegisteredIo`] that turns an attempt into the asynchronous operation the
/// connection sees: it runs the attempt under the runtime's readiness and tries again on
/// `WouldBlock`. That is why this is not a second [`ReadHalf`]/[`WriteHalf`]. Those are the
/// public, async, split halves of a socket; this is what differs between the families underneath
/// them, which is how bytes and descriptors move and what can be asked about the peer.
/// `RegisteredIo<O>` implements the public halves once for every family by wrapping these
/// attempts in the readiness loop.
///
/// The questions the public halves ask that only the family can answer, such as whether
/// descriptors can travel or which mechanism to authenticate with, are forwarded to it from there
/// rather than answered twice.
pub(crate) trait SocketOps: fmt::Debug + Send + Sync + 'static {
    /// Receives bytes, and on a unix socket the file descriptors that came with them.
    fn recv(&self, source: &IoSource, buffer: &mut [u8]) -> RecvmsgResult;

    /// Sends bytes, and on a unix socket `fds` along with them.
    fn send(
        &self,
        source: &IoSource,
        buffer: &[u8],
        #[cfg(unix)] fds: &[BorrowedFd<'_>],
    ) -> io::Result<usize>;

    /// Whether file descriptors can travel over this socket.
    fn can_pass_unix_fd(&self) -> bool {
        false
    }

    /// The mechanism a connection over this socket authenticates with.
    fn auth_mechanism(&self) -> AuthMechanism {
        AuthMechanism::External
    }

    /// The credentials of the peer at the other end.
    ///
    /// Anything here that has to block runs through `runtime`, so a socket never holds up the
    /// thread the connection is polled on.
    fn peer_credentials(
        &self,
        source: &IoSource,
        runtime: &Runtime,
    ) -> impl Future<Output = io::Result<ConnectionCredentials>> + Send {
        let _ = (source, runtime);

        std::future::ready(Ok(ConnectionCredentials::default()))
    }

    /// Whether the zero byte that opens the `EXTERNAL` handshake carries credentials of its own.
    ///
    /// The D-Bus daemon on FreeBSD and DragonFly only reads a peer's credentials off a message
    /// that was sent with `SCM_CREDS`, so a unix socket there sends that byte on its own.
    #[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
    fn sends_credentials_byte(&self) -> bool {
        false
    }

    /// Shuts both directions of the socket down.
    fn shutdown(&self, source: &IoSource) -> io::Result<()> {
        SockRef::from(source).shutdown(Shutdown::Both)
    }
}

/// The readiness handle for one registered socket: one variant per runtime a connection runs on.
#[derive(Debug)]
pub(crate) enum Registration {
    #[cfg(feature = "async-io")]
    Builtin(super::builtin::RegisteredIoSource),
    #[cfg(feature = "tokio")]
    Tokio(super::tokio_rt::Registration),
    External(Box<dyn ErasedRegistration>),
}

impl Registration {
    /// Runs `operation` every time the source is ready for `interest`, until it no longer blocks.
    pub(crate) async fn io<T>(
        &self,
        interest: Interest,
        mut operation: impl FnMut() -> io::Result<T>,
    ) -> io::Result<T> {
        poll_fn(|cx| self.poll_io(cx, interest, &mut operation)).await
    }

    /// [`Registration::poll_io`] on the runtime this registration was made on.
    pub(crate) fn poll_io<T>(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        operation: impl FnMut() -> io::Result<T>,
    ) -> Poll<io::Result<T>> {
        match self {
            #[cfg(feature = "async-io")]
            Self::Builtin(registration) => {
                traits::PollIo::poll_io(registration, cx, interest, operation)
            }
            #[cfg(feature = "tokio")]
            Self::Tokio(registration) => {
                traits::PollIo::poll_io(registration, cx, interest, operation)
            }
            Self::External(registration) => {
                traits::PollIo::poll_io(registration, cx, interest, operation)
            }
        }
    }
}

#[async_trait::async_trait]
impl<O> ReadHalf for Arc<RegisteredIo<O>>
where
    O: SocketOps,
{
    async fn recvmsg(&mut self, buf: &mut [u8]) -> RecvmsgResult {
        let socket = &**self;

        socket
            .read_with(|| socket.ops.recv(&socket.source, buf))
            .await
    }

    fn can_pass_unix_fd(&self) -> bool {
        self.ops.can_pass_unix_fd()
    }

    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        self.ops.peer_credentials(&self.source, &self.runtime).await
    }

    fn auth_mechanism(&self) -> AuthMechanism {
        self.ops.auth_mechanism()
    }
}

#[async_trait::async_trait]
impl<O> WriteHalf for Arc<RegisteredIo<O>>
where
    O: SocketOps,
{
    async fn sendmsg(
        &mut self,
        buffer: &[u8],
        #[cfg(unix)] fds: &[BorrowedFd<'_>],
    ) -> io::Result<usize> {
        let socket = &**self;

        socket
            .write_with(|| {
                socket.ops.send(
                    &socket.source,
                    buffer,
                    #[cfg(unix)]
                    fds,
                )
            })
            .await
    }

    #[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
    async fn send_zero_byte(&mut self) -> io::Result<Option<usize>> {
        if !self.ops.sends_credentials_byte() {
            return Ok(None);
        }

        let socket = &**self;

        socket
            .write_with(|| {
                unix::send_credentials_byte(std::os::fd::AsRawFd::as_raw_fd(&socket.source))
            })
            .await
            .map(Some)
    }

    async fn close(&mut self) -> io::Result<()> {
        self.ops.shutdown(&self.source)
    }

    fn can_pass_unix_fd(&self) -> bool {
        self.ops.can_pass_unix_fd()
    }

    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        self.ops.peer_credentials(&self.source, &self.runtime).await
    }
}

impl<O> Socket for RegisteredIo<O>
where
    O: SocketOps,
{
    type ReadHalf = Arc<Self>;
    type WriteHalf = Arc<Self>;

    fn split(self) -> Split<Arc<Self>, Arc<Self>> {
        let socket = Arc::new(self);

        Split::new(socket.clone(), socket)
    }
}

/// Receives bytes from a socket that carries no ancillary data of its own.
fn recv(source: &IoSource, buffer: &mut [u8]) -> RecvmsgResult {
    #[cfg(unix)]
    {
        let (read, _) = rustix::net::recv(source, buffer, rustix::net::RecvFlags::empty())?;

        Ok((read, vec![]))
    }
    #[cfg(windows)]
    {
        let socket = SockRef::from(source);

        std::io::Read::read(&mut &*socket, buffer)
    }
}

/// Sends bytes over a socket that carries no ancillary data of its own.
fn send(source: &IoSource, buffer: &[u8]) -> io::Result<usize> {
    #[cfg(unix)]
    {
        Ok(rustix::net::send(source, buffer, SEND_FLAGS)?)
    }
    #[cfg(windows)]
    {
        let socket = SockRef::from(source);

        std::io::Write::write(&mut &*socket, buffer)
    }
}

/// The flags every send zbus makes on a socket carries.
///
/// `MSG_NOSIGNAL` makes a write to a peer that has gone away an error rather than a signal.
/// Apple's platforms and Redox have no such flag, so a send there goes without it.
#[cfg(all(
    unix,
    not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
        target_os = "redox"
    ))
))]
const SEND_FLAGS: rustix::net::SendFlags = rustix::net::SendFlags::NOSIGNAL;
#[cfg(all(
    unix,
    any(
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
        target_os = "redox"
    )
))]
const SEND_FLAGS: rustix::net::SendFlags = rustix::net::SendFlags::empty();
