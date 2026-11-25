//! The credentials of the peer of a unix socket.

use std::{
    io,
    os::fd::{AsFd, BorrowedFd},
};

use super::ConnectionCredentials;

impl ConnectionCredentials {
    /// The credentials of the peer of the unix-domain socket `socket`, as a connection over the
    /// socket has them.
    ///
    /// A program that accepts connections on its own sockets, such as a bus, uses this to know who
    /// connected, with no connection over the socket and no runtime. The credentials are:
    ///
    /// * the user ID of the peer,
    /// * on Linux and Android, its process ID, unless its process has none in the PID namespace of
    ///   this one,
    /// * on Linux 6.5 and later, a file descriptor that pins its process ([`Self::process_fd`]),
    ///   unless the kernel has none to give, as before Linux 6.16 for a process that has exited,
    /// * on Linux and Android, its groups: on Linux 4.13 and later, those that its process had as
    ///   it connected, which the socket tells, and otherwise those that the name service (NSS) has
    ///   for its user.
    ///
    /// On macOS and the BSDs, the socket tells the user ID alone.
    ///
    /// Where the socket doesn't tell the groups, as on Linux before 4.13 and on Android, this looks
    /// them up in the name service. The name service may go to a file, a daemon or the network, so
    /// the call blocks until it answers. Elsewhere, the call only asks the socket, and doesn't
    /// block.
    ///
    /// This method is only available on Unix platforms, when the `comms` or the `core-p2p` feature
    /// is enabled.
    ///
    /// # Errors
    ///
    /// An I/O error if the socket can't tell the credentials, such as when `socket` isn't a
    /// socket.
    ///
    /// # Example
    ///
    /// ```
    /// use std::os::unix::net::UnixStream;
    /// use zbus::fdo::ConnectionCredentials;
    ///
    /// let (socket, _peer) = UnixStream::pair()?;
    ///
    /// let credentials = ConnectionCredentials::from_socket(&socket)?;
    /// assert!(credentials.unix_user_id().is_some());
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn from_socket(socket: impl AsFd) -> io::Result<Self> {
        let (credentials, lookup) = socket_credentials(socket.as_fd())?;

        Ok(lookup.complete_blocking(credentials))
    }
}

/// Everything about a peer that the socket itself reports.
///
/// The [`GroupLookup`] alongside carries the identity the peer's groups are looked up from, where
/// zbus makes that lookup; it is a step of its own because it blocks.
pub(crate) fn socket_credentials(
    fd: BorrowedFd<'_>,
) -> io::Result<(ConnectionCredentials, GroupLookup)> {
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

            // SAFETY: `pidfd` has room for the `len` bytes of the descriptor that `SO_PEERPIDFD`
            // fills in, and `fd` stays open for as long as the call lasts.
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
                // SAFETY: `pidfd` was zeroed, and any bytes make a valid `c_int`.
                let pidfd = unsafe { pidfd.assume_init() };
                // SAFETY: The call opened `pidfd` for this process, and nothing else owns it.
                let pidfd = unsafe { OwnedFd::from_raw_fd(pidfd) };
                creds = creds.set_process_fd(pidfd.into());
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

        // SAFETY: `uid` and `gid` are valid for the writes of the call, and `fd` stays open for as
        // long as it lasts.
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

/// The identity a peer's supplementary groups are looked up from, if they are to be looked up.
///
/// Linux and Android report a peer's user and group through `SO_PEERCRED`, macOS and the BSDs
/// through `getpeereid`. On Linux, the socket also tells the groups, through `SO_PEERGROUPS`,
/// from Linux 4.13 on. Elsewhere, on older kernels and on Android, zbus goes on to look that
/// identity up in NSS for the supplementary groups the D-Bus specification asks for, which is why
/// there is nothing to carry here on the other platforms.
#[derive(Debug)]
pub(crate) struct GroupLookup {
    /// The user and the primary group of the peer, to look its groups up for, unless the socket
    /// told them.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    pub(crate) nss: Option<(libc::uid_t, libc::gid_t)>,
}

impl GroupLookup {
    /// `credentials`, with the peer's groups looked up and added to them.
    ///
    /// The groups come out of NSS, which may go to a file, a daemon or the network, so the lookup
    /// blocks. Where the socket told the groups, there is nothing to look up, and this doesn't
    /// block.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    pub(crate) fn complete_blocking(
        self,
        mut credentials: ConnectionCredentials,
    ) -> ConnectionCredentials {
        let Some((uid, gid)) = self.nss else {
            return credentials;
        };
        for group in Self::groups(uid, gid) {
            credentials = credentials.add_unix_group_id(group);
        }

        credentials
    }

    /// `credentials`, with nothing added to them.
    ///
    /// zbus looks supplementary groups up on Linux and Android only.
    #[cfg(not(any(target_os = "android", target_os = "linux")))]
    pub(crate) fn complete_blocking(
        self,
        credentials: ConnectionCredentials,
    ) -> ConnectionCredentials {
        credentials
    }

    /// The primary and supplementary groups of the user `uid`, whose primary group is `gid`,
    /// numerically sorted as the D-Bus specification requires them.
    ///
    /// The lookup goes through NSS, which blocks.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    pub(crate) fn groups(uid: libc::uid_t, gid: libc::gid_t) -> Vec<u32> {
        // The dbus spec requires groups to be either absent or complete (primary +
        // secondary groups).

        // FIXME: rustix does not and [will not] provide `getpwuid_r` and `getgrouplist` so
        // we're left with no choice but to use libc directly. We could consider using
        // `sysinfo` crate though.
        //
        // [will not]: https://docs.rs/rustix/latest/rustix/not_implemented/higher_level/index.html
        // SAFETY: `passwd` holds integers and pointers, which zeroes make valid.
        let mut passwd: libc::passwd = unsafe { std::mem::zeroed() };
        let mut buf = vec![0u8; 16384];
        let mut result: *mut libc::passwd = std::ptr::null_mut();

        // SAFETY: `passwd` and `result` are valid for the writes of the call, and `buf` has room
        // for the `buf.len()` bytes of the strings of the entry that it writes there.
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

        // SAFETY: A `result` that isn't null points to `passwd`, whose name is a string in `buf`
        // that a 0 ends, and `buf` outlives `username`.
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
                #[cfg(feature = "tracing")]
                tracing::debug!("Group lookup failed for user {:?}", username);

                return vec![];
            }
            groups.resize(ngroups, 0);
        }
        groups.sort();
        groups.dedup();

        groups
    }
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::os::{fd::AsFd, unix::net::UnixStream};

    use rustix::process::{getegid, geteuid, getgroups};

    use super::GroupLookup;
    use crate::fdo::ConnectionCredentials;

    /// The credentials of a peer come from its socket: its user, its process, and the groups that
    /// its process has, along with its primary group, with nothing to look up in NSS.
    #[test]
    fn credentials_of_a_peer() {
        let (socket, _peer) = UnixStream::pair().unwrap();

        let (credentials, lookup) = super::socket_credentials(socket.as_fd()).unwrap();

        assert_ours(&credentials);
        assert!(lookup.nss.is_none());
    }

    /// `ConnectionCredentials::from_socket` gives the credentials of a peer whole: its user, its
    /// process, and its groups.
    #[test]
    fn credentials_of_a_peer_include_its_groups() {
        let (socket, _peer) = UnixStream::pair().unwrap();

        let credentials = ConnectionCredentials::from_socket(&socket).unwrap();

        assert_ours(&credentials);
    }

    /// The groups of a peer that the socket doesn't tell are those that NSS has for its user.
    #[test]
    fn groups_of_a_peer_are_looked_up() {
        let (uid, gid) = (geteuid().as_raw(), getegid().as_raw());
        let lookup = GroupLookup {
            nss: Some((uid, gid)),
        };

        let credentials =
            lookup.complete_blocking(ConnectionCredentials::default().set_unix_user_id(uid));

        assert_eq!(credentials.unix_user_id(), Some(uid));
        let looked_up = credentials.unix_group_ids().cloned().unwrap_or_default();
        assert_eq!(looked_up, GroupLookup::groups(uid, gid));
    }

    /// A descriptor that is not a socket has no peer to tell the credentials of.
    #[test]
    fn a_descriptor_that_is_no_socket_has_no_credentials() {
        let (read, _write) = std::io::pipe().unwrap();

        let error = ConnectionCredentials::from_socket(&read).unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTSOCK), "{error}");
    }

    /// The credentials of the peer of a socket that no connection runs over are those that a
    /// connection over it would have.
    #[cfg(feature = "comms")]
    #[test]
    fn credentials_of_the_peer_of_a_socket_of_its_own() {
        let (socket, _peer) = UnixStream::pair().unwrap();

        let credentials = crate::block_on(ConnectionCredentials::of_peer(&socket)).unwrap();

        assert_ours(&credentials);
    }

    /// `ConnectionCredentials::of_peer` fails on a descriptor that is not a socket, as
    /// `ConnectionCredentials::from_socket` does.
    #[cfg(feature = "comms")]
    #[test]
    fn a_descriptor_that_is_no_socket_has_no_peer_credentials() {
        let (read, _write) = std::io::pipe().unwrap();

        let credentials = crate::block_on(ConnectionCredentials::of_peer(&read));

        let Err(crate::Error::InputOutput(e)) = &credentials else {
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
