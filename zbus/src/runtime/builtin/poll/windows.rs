//! The wait on Windows: `select` over the sources' sockets and a socket pair that breaks the
//! wait.
//!
//! Winsock asks that all the sockets one `select` is given come from a single service provider,
//! which it settles by the `providerId` of the protocol each of them speaks. The channel that
//! breaks a wait is a loopback TCP connection, and each call puts it in the read set beside the
//! sockets a connection registered, an `AF_UNIX` one among them wherever the bus address is a
//! `unix:path=` one. The runtime's own
//! `an_af_unix_socket_is_watched_beside_the_tcp_wake_socket` is the test that pairs those two
//! families in one wait.

use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    os::windows::io::AsRawSocket,
    ptr,
    time::Duration,
};

use windows_sys::Win32::Networking::WinSock::{
    FD_SET, SOCKET, SOCKET_ERROR, TIMEVAL, WSAGetLastError, select,
};

use super::{Ready, Want};
use crate::runtime::IoSource;

/// How many sockets one set holds.
///
/// Winsock's own `FD_SET` holds `FD_SETSIZE` of them, sixty-four, and lets a program raise that
/// number before it includes the headers that declare the set; this is the number the backend
/// raises it to. A wait costs `select` a pass over each set, so the number is kept to what a
/// process with a great many connections needs and no more.
const SET_SIZE: usize = 1024;

/// How many sources one wait can take in.
///
/// A set holds `SET_SIZE` sockets, and the read set keeps one of those places for the socket a
/// `notify` writes to.
pub(in crate::runtime::builtin) const MAX_SOURCES: usize = SET_SIZE - 1;

/// A socket set with room for `SET_SIZE` sockets.
///
/// `select` reads a set as a count followed by that many sockets, and takes the count from the
/// set rather than any length from the type it was declared with; this is the set Winsock's
/// headers declare once `FD_SETSIZE` is raised, laid out as `FD_SET` is with a longer array.
#[repr(C)]
struct FdSet {
    fd_count: u32,
    fd_array: [SOCKET; SET_SIZE],
}

impl FdSet {
    /// An empty set.
    fn new() -> Self {
        Self {
            fd_count: 0,
            fd_array: [0; SET_SIZE],
        }
    }
}

pub(in crate::runtime::builtin) struct Poller {
    /// The half a wait watches, and drains whenever it holds anything.
    ///
    /// Both halves belong to the poller for as long as it lives, so neither closes while the
    /// other is watched: a closed one would sit in the read set from then on and every wait
    /// would return at once.
    wake_read: TcpStream,
    /// The half a `notify` writes one byte to.
    wake_write: TcpStream,
}

impl Poller {
    pub(in crate::runtime::builtin) fn new() -> io::Result<Self> {
        // Winsock has no socket pair, so the pair is a connection a listener of our own accepts
        // from the loopback address and then has no further use for.
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let wake_write = TcpStream::connect(listener.local_addr()?)?;
        let wake_read = loop {
            let (accepted, _) = listener.accept()?;
            // A loopback listener is reachable by anything else on the machine, so a connection
            // that is not the one made just above is turned away rather than taken for it.
            if accepted.peer_addr()? == wake_write.local_addr()? {
                break accepted;
            }
        };
        wake_read.set_nonblocking(true)?;
        wake_write.set_nonblocking(true)?;
        // A second wake-up byte is not to wait in the sender for the peer's delayed
        // acknowledgement of the first, which is what send coalescing would have it do.
        wake_read.set_nodelay(true)?;
        wake_write.set_nodelay(true)?;

        Ok(Self {
            wake_read,
            wake_write,
        })
    }

    /// Breaks a `wait` in progress or the next one; callable from any thread.
    pub(in crate::runtime::builtin) fn notify(&self) -> io::Result<()> {
        match (&self.wake_write).write(&[1]) {
            Ok(_) => Ok(()),
            // A socket with no room left holds a wake-up already.
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Waits until a wanted source is ready, `notify` is called or `timeout` passes; `None`
    /// waits without limit. The sources are held for the whole call, so no socket in the set
    /// can close under it.
    ///
    /// What Winsock undertakes to report is narrow. The read set comes back holding a socket
    /// with data to read or one whose connection was closed, reset or terminated; the write set
    /// one that can take data or whose connect has been made; and the except set one whose
    /// connect failed or that has urgent data waiting. What else a provider may report in the
    /// except set is left open, which is why every source is offered for that set and a source
    /// found there is reported readable and writable both: whichever of the two a waiter parked
    /// for, it then retries its own operation and reads the error off that.
    pub(in crate::runtime::builtin) fn wait(
        &self,
        sources: &[(IoSource, Want)],
        timeout: Option<Duration>,
    ) -> io::Result<Vec<Ready>> {
        debug_assert!(sources.len() <= MAX_SOURCES);
        let mut readable = FdSet::new();
        let mut writable = FdSet::new();
        // Winsock reports a connect that failed here rather than in the write set, and this is
        // also where it puts whatever else it has to say about a socket.
        let mut excepted = FdSet::new();
        let wake = self.wake_read.as_raw_socket() as SOCKET;
        push(&mut readable, wake);
        for (source, want) in sources {
            let socket = source.as_raw_socket() as SOCKET;
            if want.readable {
                push(&mut readable, socket);
            }
            if want.writable {
                push(&mut writable, socket);
            }
            push(&mut excepted, socket);
        }
        let timeout = timeout.map(timeval);

        // SAFETY: `select` reads and writes the three sets for the length of the call and no
        // longer, and each of them is a live local of this frame across it, in the shape
        // `select` expects: a count of the entries `push` wrote against those entries, and no
        // more entries than `SET_SIZE`, which is what `MAX_SOURCES` holds the caller to. Each
        // set is an `FdSet`: `repr(C)`, a `u32` count followed by an array of `SOCKET`, the
        // layout `FD_SET` has, and `select` reads and writes only the entries the count names,
        // which lie within the array. Every socket in them is held open across the call: the
        // wake socket by `self`, which outlives the call, and each of the others by an
        // `IoSource` the caller holds. The timeout is a live local of this frame as well, or a
        // null pointer, which is how a wait without limit is asked for. The first argument is
        // ignored on Winsock, and zero is passed for it.
        let ready = unsafe {
            select(
                0,
                ptr::from_mut(&mut readable).cast(),
                offered(&mut writable),
                offered(&mut excepted),
                timeout.as_ref().map_or(ptr::null(), ptr::from_ref),
            )
        };
        if ready == SOCKET_ERROR {
            // SAFETY: `WSAGetLastError` takes nothing and reads this thread's last Winsock error.
            return Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }));
        }
        if holds(&readable, wake) {
            let mut buf = [0u8; 64];
            while (&self.wake_read)
                .read(&mut buf)
                .is_ok_and(|n| n == buf.len())
            {}
        }

        Ok(sources
            .iter()
            .filter_map(|(source, want)| {
                let socket = source.as_raw_socket() as SOCKET;
                let is_excepted = holds(&excepted, socket);
                let is_readable = holds(&readable, socket) || is_excepted;
                let is_writable = holds(&writable, socket) || is_excepted;

                (is_readable || is_writable).then_some(Ready {
                    key: want.key,
                    readable: is_readable,
                    writable: is_writable,
                })
            })
            .collect())
    }
}

/// Adds `socket` to `set`.
///
/// A set is a counted array, and the macro that fills one in C is not among what `windows-sys`
/// offers, so the entry is written by hand. A caller never offers more than `MAX_SOURCES`
/// sources, which is what keeps every set within its length.
fn push(set: &mut FdSet, socket: SOCKET) {
    set.fd_array[set.fd_count as usize] = socket;
    set.fd_count += 1;
}

/// A pointer to `set` for `select`, or a null one where nothing was put in it.
///
/// Winsock reads a null set as one it is asked nothing about, while a set it is handed has to
/// hold at least one socket.
fn offered(set: &mut FdSet) -> *mut FD_SET {
    if set.fd_count == 0 {
        return ptr::null_mut();
    }

    ptr::from_mut(set).cast()
}

/// Whether `set` holds `socket` once `select` has left behind what it found.
///
/// `select` rewrites each set it is given as the sockets of that set its answer applies to, so
/// the count read here is the one it wrote and the entries beyond that count are whatever `push`
/// left behind.
fn holds(set: &FdSet, socket: SOCKET) -> bool {
    set.fd_array[..set.fd_count as usize].contains(&socket)
}

/// `duration` in the shape `select` takes its timeout in.
///
/// The sub-second part is rounded up to whole microseconds, so a deadline less than a microsecond
/// ahead is waited for rather than turned into a wait of no length at all. A rounding that
/// reaches a whole second belongs in the seconds, which is where it is carried to.
fn timeval(duration: Duration) -> TIMEVAL {
    let (seconds, microseconds) = match duration.subsec_nanos().div_ceil(1_000) {
        1_000_000 => (duration.as_secs().saturating_add(1), 0),
        microseconds => (duration.as_secs(), microseconds),
    };

    TIMEVAL {
        tv_sec: seconds.try_into().unwrap_or(i32::MAX),
        tv_usec: i32::try_from(microseconds).unwrap_or(i32::MAX),
    }
}
