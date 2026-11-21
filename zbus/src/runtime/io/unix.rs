//! The operations of a unix socket, the only kind of socket that carries file descriptors.

#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};
use std::{
    future::Future,
    io::{self, IoSlice},
};

#[cfg(unix)]
use rustix::net::{
    RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SendAncillaryBuffer,
    SendAncillaryMessage, recvmsg, sendmsg,
};

#[cfg(unix)]
use super::SEND_FLAGS;
use super::SocketOps;
use crate::{
    connection::socket::RecvmsgResult,
    fdo::ConnectionCredentials,
    runtime::{IoSource, Runtime},
};
#[cfg(unix)]
use crate::{fdo::GroupLookup, utils::FDS_MAX};

/// The operations of a unix socket: `AF_UNIX` on unix, and on Windows the socket `uds_windows`
/// hands out.
#[derive(Debug)]
pub(crate) struct UnixOps;

impl SocketOps for UnixOps {
    #[cfg(unix)]
    fn recv(&self, source: &IoSource, buffer: &mut [u8]) -> RecvmsgResult {
        fd_recvmsg(source.as_fd(), buffer)
    }

    #[cfg(windows)]
    fn recv(&self, source: &IoSource, buffer: &mut [u8]) -> RecvmsgResult {
        super::recv(source, buffer)
    }

    #[cfg(unix)]
    fn send(
        &self,
        source: &IoSource,
        buffers: &[IoSlice<'_>],
        fds: &[BorrowedFd<'_>],
    ) -> io::Result<usize> {
        fd_sendmsg(source.as_fd(), buffers, fds)
    }

    #[cfg(windows)]
    fn send(&self, source: &IoSource, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
        super::send(source, buffers)
    }

    /// Only a real `AF_UNIX` socket passes file descriptors; the Windows one cannot.
    fn can_pass_unix_fd(&self) -> bool {
        cfg!(unix)
    }

    #[cfg(unix)]
    fn peer_credentials(
        &self,
        source: &IoSource,
        runtime: &Runtime,
    ) -> impl Future<Output = io::Result<ConnectionCredentials>> + Send {
        peer_credentials(source.as_fd(), Some(runtime))
    }

    #[cfg(windows)]
    fn peer_credentials(
        &self,
        source: &IoSource,
        runtime: &Runtime,
    ) -> impl Future<Output = io::Result<ConnectionCredentials>> + Send {
        peer_credentials(source, runtime)
    }

    #[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
    fn sends_credentials_byte(&self) -> bool {
        true
    }
}

/// The address of the unix-domain socket that `path` names, as the standard library has it.
///
/// This is the form a runtime's own connect takes, where [`Runtime::connect_unix`] is handed a
/// path. On Linux and Android, a `path` whose first byte is zero does not name a file: the rest of
/// it is a name in the abstract namespace, and may hold zero bytes of its own. Any other `path` is
/// the path of the file the socket is bound to, and holds no zero byte. A `path` that does, and
/// one that is too long for an address, is an [`InvalidInput`](io::ErrorKind::InvalidInput) error.
///
/// [`Runtime::connect_unix`]: crate::runtime::traits::Runtime::connect_unix
#[cfg(all(unix, any(feature = "default-rt", feature = "tokio")))]
pub(crate) fn unix_socket_address(
    path: &std::path::Path,
) -> io::Result<std::os::unix::net::SocketAddr> {
    use std::os::unix::net::SocketAddr;

    #[cfg(any(target_os = "android", target_os = "linux"))]
    {
        #[cfg(target_os = "android")]
        use std::os::android::net::SocketAddrExt;
        #[cfg(target_os = "linux")]
        use std::os::linux::net::SocketAddrExt;
        use std::os::unix::ffi::OsStrExt;

        if let Some(name) = path.as_os_str().as_bytes().strip_prefix(b"\0") {
            return SocketAddr::from_abstract_name(name);
        }
    }

    SocketAddr::from_pathname(path)
}

/// The credentials of the peer of the unix socket `fd`.
///
/// The socket itself is asked on the calling thread: those are plain socket options. The
/// supplementary groups the D-Bus specification asks for come out of NSS where the socket doesn't
/// tell them, and NSS may go to a file, a daemon or the network, so that part goes off the calling
/// thread, as `complete` tells.
#[cfg(unix)]
pub(crate) async fn peer_credentials(
    fd: BorrowedFd<'_>,
    runtime: Option<&Runtime>,
) -> io::Result<ConnectionCredentials> {
    let (credentials, lookup) = ConnectionCredentials::from_socket(fd)?;

    Ok(complete(credentials, lookup, runtime).await)
}

/// `credentials`, with what `lookup` looks up added to them, if there is a lookup to make.
///
/// The lookup blocks, rather than being work for the thread that the caller is polled on, so it
/// goes to `runtime`, that of the connection over the socket, if there is one, and to `unblock`
/// otherwise. Nothing goes anywhere if there is nothing to look up.
#[cfg(unix)]
async fn complete(
    credentials: ConnectionCredentials,
    lookup: Option<GroupLookup>,
    runtime: Option<&Runtime>,
) -> ConnectionCredentials {
    let Some(lookup) = lookup else {
        return credentials;
    };
    let complete = move || lookup.complete(credentials);

    match runtime {
        Some(runtime) => runtime.spawn_blocking(complete).await,
        None => unblock(complete).await,
    }
}

/// The credentials of the peer of a Windows unix socket.
///
/// Asking the socket and then opening the peer's process token are both trips into the kernel
/// that can take a while to come back, so the whole lookup goes to `runtime`.
#[cfg(windows)]
async fn peer_credentials(
    source: &IoSource,
    runtime: &Runtime,
) -> io::Result<ConnectionCredentials> {
    let source = source.clone();

    runtime
        .spawn_blocking(move || credentials_from_socket(&source))
        .await
}

/// The credentials the peer of a Windows unix socket reports.
#[cfg(windows)]
fn credentials_from_socket(
    socket: &impl std::os::windows::io::AsRawSocket,
) -> io::Result<ConnectionCredentials> {
    use crate::win32::{ProcessToken, unix_stream_get_peer_pid};

    let pid = unix_stream_get_peer_pid(socket)? as _;
    let sid = ProcessToken::open(if pid != 0 { Some(pid as _) } else { None })
        .and_then(|process_token| process_token.sid())?;

    Ok(ConnectionCredentials::default()
        .set_process_id(pid)
        .set_windows_sid(sid))
}

#[cfg(unix)]
fn fd_recvmsg(fd: BorrowedFd<'_>, buffer: &mut [u8]) -> RecvmsgResult {
    use std::{io::IoSliceMut, mem::MaybeUninit};

    let mut iov = [IoSliceMut::new(buffer)];
    let mut cmsg_buffer = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(FDS_MAX))];
    let mut ancillary = RecvAncillaryBuffer::new(&mut cmsg_buffer);

    // A read of nothing is the end of the input, as with any other socket, which the reader tells
    // apart from an error. On Linux, a peer that closes its socket while bytes that it was sent
    // are still unread in it makes a read fail with ECONNRESET instead, but only once the reads
    // before it took all that the peer sent, and the reads after it read nothing: that is the
    // end of the input too, with nothing lost.
    let msg = match recvmsg(fd, &mut iov, &mut ancillary, RecvFlags::empty()) {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        Err(e) if e == rustix::io::Errno::CONNRESET => return Ok((0, vec![])),
        msg => msg?,
    };
    let mut fds = vec![];
    for msg in ancillary.drain() {
        match msg {
            RecvAncillaryMessage::ScmRights(iter) => {
                fds.extend(iter);
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            RecvAncillaryMessage::ScmCredentials(_) => {
                // On Linux, credentials might be received. This shouldn't normally happen
                // in our use case since we don't request them, but ignore if present.
                continue;
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected CMSG kind",
                ));
            }
        }
    }

    Ok((msg.bytes, fds))
}

#[cfg(unix)]
fn fd_sendmsg(
    fd: BorrowedFd<'_>,
    buffers: &[IoSlice<'_>],
    fds: &[BorrowedFd<'_>],
) -> io::Result<usize> {
    use std::mem::MaybeUninit;

    let mut cmsg_buffer = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(FDS_MAX))];
    let mut ancillary = SendAncillaryBuffer::new(&mut cmsg_buffer);

    if !fds.is_empty() && !ancillary.push(SendAncillaryMessage::ScmRights(fds)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "too many file descriptors",
        ));
    }

    let sent = sendmsg(fd, buffers, &mut ancillary, SEND_FLAGS)?;
    if sent == 0 {
        // can it really happen?
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "failed to write to buffer",
        ));
    }

    Ok(sent)
}

/// Runs `work` off the calling thread, with no connection to run it on the runtime of: on the
/// blocking pool of Tokio if the `tokio` feature is enabled and a Tokio runtime is current, as for
/// a connection built there, and on the threads that zbus keeps for blocking work otherwise, which
/// need no runtime to be running.
#[cfg(unix)]
async fn unblock<T>(work: impl FnOnce() -> T + Send + 'static) -> T
where
    T: Send + 'static,
{
    #[cfg(feature = "tokio")]
    if let Some(runtime) = crate::runtime::Tokio::current() {
        return crate::runtime::traits::Runtime::spawn_blocking(&runtime, work).await;
    }

    zruntime::unblock(work).await
}

/// Sends the zero byte that opens the `EXTERNAL` handshake as an `SCM_CREDS` message of its own.
///
/// This is one attempt at a syscall like the rest of the family's operations: the descriptor is
/// already non-blocking and the call runs inside a write the runtime has found the socket ready
/// for, so a `WouldBlock` from here is waited on the same way any other send's is.
#[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
pub(super) fn send_credentials_byte(fd: std::os::fd::RawFd) -> io::Result<usize> {
    // FIXME: Replace with rustix API when it provides SCM_CREDS support for BSD.
    // For now, use libc directly since rustix doesn't support sending SCM_CREDS on BSD.
    use std::mem::MaybeUninit;

    let mut iov = libc::iovec {
        iov_base: c"".as_ptr() as *mut libc::c_void,
        iov_len: 1,
    };

    let mut msg: libc::msghdr = unsafe { MaybeUninit::zeroed().assume_init() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;

    // SCM_CREDS on BSD doesn't actually send data in the control message.
    // Instead, it tells the kernel to attach credentials when receiving.
    // We just need to allocate space for the cmsg header with no data.
    let cmsg_space = unsafe { libc::CMSG_SPACE(0) as usize };
    let mut cmsg_buf = vec![0u8; cmsg_space];

    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_space as _;

    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if !cmsg.is_null() {
        unsafe {
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_CREDS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(0) as _;
        }
    }

    let ret = unsafe { libc::sendmsg(fd, &msg, 0) };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret as usize)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use rustix::process::{getegid, geteuid};

    use crate::{
        block_on,
        fdo::{ConnectionCredentials, GroupLookup},
    };

    /// The groups of a peer that the socket doesn't tell are looked up in NSS off the calling
    /// thread, with no connection to look them up on the runtime of too.
    #[test]
    fn groups_are_looked_up_with_no_connection() {
        let (uid, gid) = (geteuid().as_raw(), getegid().as_raw());
        let lookup = || GroupLookup { user: (uid, gid) };

        let credentials = block_on(super::complete(
            ConnectionCredentials::default(),
            Some(lookup()),
            None,
        ));

        let expected = lookup().complete(ConnectionCredentials::default());
        assert_eq!(credentials, expected);
    }
}
