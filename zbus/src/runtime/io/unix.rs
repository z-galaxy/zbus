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
#[cfg(unix)]
use crate::utils::FDS_MAX;
use crate::{
    connection::socket::RecvmsgResult,
    fdo::ConnectionCredentials,
    runtime::{IoSource, Runtime},
};

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

/// The credentials of the peer of a unix socket.
///
/// The socket itself is asked on the calling thread: those are plain socket options. The
/// supplementary groups the D-Bus specification asks for come out of NSS, which may go to a file,
/// a daemon or the network, so that part goes to `runtime` — on the platforms that have such a
/// lookup to make at all.
#[cfg(unix)]
async fn peer_credentials(
    source: &IoSource,
    runtime: &Runtime,
) -> io::Result<ConnectionCredentials> {
    let (credentials, lookup) = socket_credentials(source.as_fd())?;

    Ok(lookup.complete(credentials, runtime).await)
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

    let msg = recvmsg(fd, &mut iov, &mut ancillary, RecvFlags::empty())?;
    if msg.bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "failed to read from socket",
        ));
    }
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

/// Everything about a peer that the socket itself reports.
///
/// The [`GroupLookup`] alongside carries the identity the peer's groups are looked up from, where
/// zbus makes that lookup; it is a step of its own because it blocks.
#[cfg(unix)]
fn socket_credentials(fd: BorrowedFd<'_>) -> io::Result<(ConnectionCredentials, GroupLookup)> {
    let mut creds = ConnectionCredentials::default();
    #[cfg(any(target_os = "android", target_os = "linux"))]
    let uid;
    #[cfg(any(target_os = "android", target_os = "linux"))]
    let gid;

    #[cfg(any(target_os = "android", target_os = "linux"))]
    {
        let ucred = peer_ucred(fd)?;
        uid = ucred.uid;
        gid = ucred.gid;
        creds = creds.set_unix_user_id(uid);
        // The kernel gives 0 for a peer whose process has no ID in the PID namespace of the caller,
        // which names no process here. Its process descriptor, below, still does.
        if let Some(pid) = u32::try_from(ucred.pid).ok().filter(|&pid| pid != 0) {
            creds = creds.set_process_id(pid);
        }

        #[cfg(target_os = "linux")]
        {
            // FIXME: Replace with rustix API when it provides SO_PEERPIDFD sockopt:
            // https://github.com/bytecodealliance/rustix/pull/1474
            use libc::{c_int, socklen_t};
            use std::{
                mem::{MaybeUninit, size_of},
                os::fd::{AsRawFd, FromRawFd, OwnedFd},
            };

            let mut pidfd = MaybeUninit::<c_int>::zeroed();
            let mut len = size_of::<c_int>() as socklen_t;

            let ret = unsafe {
                libc::getsockopt(
                    fd.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_PEERPIDFD,
                    pidfd.as_mut_ptr().cast(),
                    &mut len,
                )
            };

            if ret == 0 {
                let pidfd = unsafe { pidfd.assume_init() };
                creds = creds.set_process_fd(unsafe { OwnedFd::from_raw_fd(pidfd).into() });
            } else if ret < 0 {
                let err = io::Error::last_os_error();
                // ENOPROTOOPT means the kernel doesn't support this feature.
                if err.raw_os_error() != Some(libc::ENOPROTOOPT) {
                    return Err(err);
                }
            }
        }
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    {
        use std::os::fd::AsRawFd;

        // FIXME: Replace with rustix API when it provides the require API:
        // https://github.com/bytecodealliance/rustix/issues/1533
        let mut uid: libc::uid_t = 0;
        let mut gid: libc::gid_t = 0;

        let ret = unsafe { libc::getpeereid(fd.as_raw_fd(), &mut uid, &mut gid) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        creds = creds.set_unix_user_id(uid);

        // FIXME: Handle pid fetching too
    }

    Ok((
        creds,
        GroupLookup {
            #[cfg(any(target_os = "android", target_os = "linux"))]
            uid,
            #[cfg(any(target_os = "android", target_os = "linux"))]
            gid,
        },
    ))
}

/// The credentials that the peer of the socket `fd` had as it connected, from `SO_PEERCRED`.
///
/// rustix has this call, but it holds the process ID in a type that has no 0, which the kernel
/// gives for a peer in another PID namespace, and fails for such a peer.
#[cfg(any(target_os = "android", target_os = "linux"))]
fn peer_ucred(fd: BorrowedFd<'_>) -> io::Result<libc::ucred> {
    use std::os::fd::AsRawFd;

    let mut ucred = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: `ucred` has room for the `len` bytes that `SO_PEERCRED` fills in, and `fd` stays open
    // for as long as the call lasts.
    let ret = unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            std::ptr::from_mut(&mut ucred).cast(),
            &mut len,
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(ucred)
}

/// The identity a peer's supplementary groups are looked up from.
///
/// Linux and Android report a peer's user and group through `SO_PEERCRED`, macOS and the BSDs
/// through `getpeereid`. Only on the first two does zbus go on to look that identity up for the
/// supplementary groups the D-Bus specification asks for, which is why there is nothing to carry
/// here anywhere else.
#[cfg(unix)]
#[derive(Debug)]
struct GroupLookup {
    #[cfg(any(target_os = "android", target_os = "linux"))]
    uid: libc::uid_t,
    #[cfg(any(target_os = "android", target_os = "linux"))]
    gid: libc::gid_t,
}

#[cfg(unix)]
impl GroupLookup {
    /// `credentials`, with the peer's groups looked up on `runtime` and added to them.
    ///
    /// The groups come out of NSS, which may go to a file, a daemon or the network, so the lookup
    /// is blocking work for the runtime rather than for the thread the connection is polled on.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    async fn complete(
        self,
        mut credentials: ConnectionCredentials,
        runtime: &Runtime,
    ) -> ConnectionCredentials {
        for group in runtime.spawn_blocking(move || self.groups()).await {
            credentials = credentials.add_unix_group_id(group);
        }

        credentials
    }

    /// `credentials`, with nothing added to them.
    ///
    /// zbus looks supplementary groups up on Linux and Android only, so there is nothing to hand
    /// the runtime here: a default blocking hook would start a thread per call to find nothing.
    #[cfg(not(any(target_os = "android", target_os = "linux")))]
    async fn complete(
        self,
        credentials: ConnectionCredentials,
        _runtime: &Runtime,
    ) -> ConnectionCredentials {
        credentials
    }

    /// The peer's primary and supplementary groups, numerically sorted as the D-Bus
    /// specification requires them.
    ///
    /// The lookup goes through NSS, which blocks.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    fn groups(self) -> Vec<u32> {
        use crate::log::debug;

        let Self { uid, gid } = self;

        // The dbus spec requires groups to be either absent or complete (primary +
        // secondary groups).

        // FIXME: rustix does not and [will not] provide `getpwuid_r` and `getgrouplist` so
        // we're left with no choice but to use libc directly. We could consider using
        // `sysinfo` crate though.
        //
        // [will not]: https://docs.rs/rustix/latest/rustix/not_implemented/higher_level/index.html
        let mut passwd: libc::passwd = unsafe { std::mem::zeroed() };
        let mut buf = vec![0u8; 16384];
        let mut result: *mut libc::passwd = std::ptr::null_mut();

        unsafe {
            libc::getpwuid_r(
                uid,
                &mut passwd,
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
                &mut result,
            );
        }

        if result.is_null() {
            return vec![];
        }

        let username = unsafe { std::ffi::CStr::from_ptr((*result).pw_name) };
        let mut groups = vec![0u32; INITIAL_GROUPS];
        loop {
            let mut ngroups = libc::c_int::try_from(groups.len()).unwrap_or(libc::c_int::MAX);
            // SAFETY: `username` ends with a 0, and `groups` has room for the `ngroups` groups that
            // `getgrouplist` is given.
            let found = unsafe {
                libc::getgrouplist(
                    username.as_ptr(),
                    gid,
                    groups.as_mut_ptr() as *mut libc::gid_t,
                    &mut ngroups,
                )
            };
            let ngroups = usize::try_from(ngroups).unwrap_or(0);
            if found >= 0 {
                groups.truncate(ngroups);

                break;
            }
            // The user has more groups than there is room for, and `ngroups` is how many it has.
            // Anything else is a failure, which leaves the groups out, as they must be complete.
            if ngroups <= groups.len() {
                debug!("Group lookup failed for user {:?}", username);

                return vec![];
            }
            groups.resize(ngroups, 0);
        }
        groups.sort();
        groups.dedup();

        groups
    }
}

/// How many groups a lookup of the groups of a peer makes room for at first, which is enough for
/// most users: it makes room for more if the user has more.
#[cfg(any(target_os = "android", target_os = "linux"))]
const INITIAL_GROUPS: usize = 64;

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
