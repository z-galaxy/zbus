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
/// supplementary groups the D-Bus specification asks for come out of NSS, which may go to a file,
/// a daemon or the network, so that part goes off the calling thread, on the platforms that have
/// such a lookup to make at all: to `runtime`, that of the connection over the socket, if there is
/// one, and as `unblock` tells otherwise.
#[cfg(unix)]
pub(crate) async fn peer_credentials(
    fd: BorrowedFd<'_>,
    runtime: Option<&Runtime>,
) -> io::Result<ConnectionCredentials> {
    let (credentials, lookup) = socket_credentials(fd)?;

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

/// Everything about a peer that the socket itself reports.
///
/// The [`GroupLookup`] alongside carries the identity the peer's groups are looked up from, where
/// zbus makes that lookup; it is a step of its own because it blocks.
#[cfg(unix)]
fn socket_credentials(fd: BorrowedFd<'_>) -> io::Result<(ConnectionCredentials, GroupLookup)> {
    let mut creds = ConnectionCredentials::default();
    #[cfg(any(target_os = "android", target_os = "linux"))]
    let nss;

    #[cfg(any(target_os = "android", target_os = "linux"))]
    {
        let ucred = peer_ucred(fd)?;
        let (uid, gid) = (ucred.uid, ucred.gid);
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
                // The kernel may have no descriptor to give, which leaves it out, as dbus-broker
                // does: one older than 6.5 has no such option (ENOPROTOOPT), the socket may have
                // no peer process (ENODATA), and one older than 6.16 has none for a process that
                // has exited (EINVAL or ESRCH), as one that connected and went may have.
                if !matches!(
                    err.raw_os_error(),
                    Some(libc::ENOPROTOOPT | libc::ENODATA | libc::EINVAL | libc::ESRCH)
                ) {
                    return Err(err);
                }
            }
        }

        // The groups that the process of the peer had as it connected, which the socket tells on
        // Linux, rather than those that NSS has for its user now, which zbus looks up otherwise.
        #[cfg(target_os = "linux")]
        match peer_groups(fd, gid)? {
            Some(groups) => {
                for group in groups {
                    creds = creds.add_unix_group_id(group);
                }
                nss = None;
            }
            None => nss = Some((uid, gid)),
        }
        #[cfg(target_os = "android")]
        {
            nss = Some((uid, gid));
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
            nss,
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

/// The identity a peer's supplementary groups are looked up from, if they are to be looked up.
///
/// Linux and Android report a peer's user and group through `SO_PEERCRED`, macOS and the BSDs
/// through `getpeereid`. On Linux, the socket also tells the groups, through `SO_PEERGROUPS`,
/// from Linux 4.13 on. Elsewhere, on older kernels and on Android, zbus goes on to look that
/// identity up in NSS for the supplementary groups the D-Bus specification asks for, which is why
/// there is nothing to carry here on the other platforms.
#[cfg(unix)]
#[derive(Debug)]
struct GroupLookup {
    /// The user and the primary group of the peer, to look its groups up for, unless the socket
    /// told them.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    nss: Option<(libc::uid_t, libc::gid_t)>,
}

#[cfg(unix)]
impl GroupLookup {
    /// `credentials`, with the peer's groups looked up and added to them.
    ///
    /// The groups come out of NSS, which may go to a file, a daemon or the network, so the lookup
    /// is blocking work, rather than work for the thread that the caller is polled on. It goes to
    /// `runtime`, that of the connection over the socket, if there is one, and to `unblock`
    /// otherwise. Nothing goes anywhere if there is nothing to look up.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    async fn complete(
        self,
        mut credentials: ConnectionCredentials,
        runtime: Option<&Runtime>,
    ) -> ConnectionCredentials {
        let Some((uid, gid)) = self.nss else {
            return credentials;
        };
        let lookup = move || Self::groups(uid, gid);
        let groups = match runtime {
            Some(runtime) => runtime.spawn_blocking(lookup).await,
            None => unblock(lookup).await,
        };
        for group in groups {
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
        _runtime: Option<&Runtime>,
    ) -> ConnectionCredentials {
        credentials
    }

    /// The primary and supplementary groups of the user `uid`, whose primary group is `gid`,
    /// numerically sorted as the D-Bus specification requires them.
    ///
    /// The lookup goes through NSS, which blocks.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    fn groups(uid: libc::uid_t, gid: libc::gid_t) -> Vec<u32> {
        use crate::log::debug;

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

/// Runs `work` off the calling thread, with no connection to run it on the runtime of: on the
/// blocking pool of Tokio if the `tokio` feature is enabled and a Tokio runtime is current, as for
/// a connection built there, and on the threads that zbus keeps for blocking work otherwise, which
/// need no runtime to be running.
#[cfg(any(target_os = "android", target_os = "linux"))]
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

/// The groups that the process of the peer of the socket `fd` had as it connected, from
/// `SO_PEERGROUPS`, with `gid`, its primary group, numerically sorted as the D-Bus specification
/// requires them: none if the kernel is older than 4.13, which has no such option, or holds no
/// credentials of the peer.
#[cfg(target_os = "linux")]
fn peer_groups(fd: BorrowedFd<'_>, gid: libc::gid_t) -> io::Result<Option<Vec<u32>>> {
    use std::{mem::size_of, os::fd::AsRawFd};

    let mut groups: Vec<libc::gid_t> = vec![0; INITIAL_GROUPS];
    loop {
        let room = groups.len() * size_of::<libc::gid_t>();
        let mut len = libc::socklen_t::try_from(room).unwrap_or(libc::socklen_t::MAX);
        // SAFETY: `groups` has room for the `len` bytes that `SO_PEERGROUPS` fills in, and `fd`
        // stays open for as long as the call lasts.
        let ret = unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERGROUPS,
                groups.as_mut_ptr().cast(),
                &mut len,
            )
        };
        let len = usize::try_from(len).unwrap_or(usize::MAX);
        if ret == 0 {
            groups.truncate(len / size_of::<libc::gid_t>());

            break;
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            // The peer has more groups than there is room for, and `len` is the room they take.
            Some(libc::ERANGE) if len > room => {
                groups.resize(len.div_ceil(size_of::<libc::gid_t>()), 0);
            }
            Some(libc::ENOPROTOOPT | libc::ENODATA) => return Ok(None),
            _ => return Err(err),
        }
    }
    // The kernel gives the supplementary groups alone.
    groups.push(gid);
    groups.sort_unstable();
    groups.dedup();

    Ok(Some(groups))
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::os::{fd::AsFd, unix::net::UnixStream};

    use rustix::process::{getegid, geteuid, getgroups};

    use super::GroupLookup;
    #[cfg(feature = "bus-impl")]
    use crate::Error;
    use crate::{block_on, fdo::ConnectionCredentials};

    /// The credentials of a peer come from its socket: its user, its process, and the groups that
    /// its process has, along with its primary group, with nothing to look up in NSS.
    #[test]
    fn credentials_of_a_peer() {
        let (socket, _peer) = UnixStream::pair().unwrap();

        let (credentials, lookup) = super::socket_credentials(socket.as_fd()).unwrap();
        assert_ours(&credentials);
        assert!(lookup.nss.is_none());
    }

    /// The groups of a peer that the socket doesn't tell are looked up in NSS off the calling
    /// thread, with no connection to look them up on the runtime of too.
    #[test]
    fn groups_are_looked_up_with_no_connection() {
        let (uid, gid) = (geteuid().as_raw(), getegid().as_raw());
        let lookup = GroupLookup {
            nss: Some((uid, gid)),
        };

        let credentials = block_on(lookup.complete(ConnectionCredentials::default(), None));

        let groups = credentials.unix_group_ids().cloned().unwrap_or_default();
        assert_eq!(groups, GroupLookup::groups(uid, gid));
    }

    /// The credentials of the peer of a socket that no connection runs over are those that a
    /// connection over it would have.
    #[cfg(feature = "bus-impl")]
    #[test]
    fn credentials_of_the_peer_of_a_socket_of_its_own() {
        let (socket, _peer) = UnixStream::pair().unwrap();

        let credentials = block_on(ConnectionCredentials::of_peer(&socket)).unwrap();

        assert_ours(&credentials);
    }

    /// A descriptor that is not a socket has no peer to tell the credentials of.
    #[cfg(feature = "bus-impl")]
    #[test]
    fn a_descriptor_that_is_no_socket_has_no_credentials() {
        let (read, _write) = std::io::pipe().unwrap();

        let credentials = block_on(ConnectionCredentials::of_peer(&read));

        let Err(Error::InputOutput(e)) = &credentials else {
            panic!("a pipe has credentials: {credentials:?}");
        };
        assert_eq!(e.raw_os_error(), Some(libc::ENOTSOCK), "{e}");
    }

    /// Asserts that `credentials` are those of this process, as the peer of a socket pair that it
    /// made has them.
    fn assert_ours(credentials: &ConnectionCredentials) {
        assert_eq!(credentials.unix_user_id(), Some(geteuid().as_raw()));
        assert_eq!(credentials.process_id(), Some(std::process::id()));
        let mut groups: Vec<_> = getgroups()
            .unwrap()
            .into_iter()
            .map(|group| group.as_raw())
            .chain([getegid().as_raw()])
            .collect();
        groups.sort_unstable();
        groups.dedup();
        assert_eq!(credentials.unix_group_ids(), Some(&groups));
    }
}
