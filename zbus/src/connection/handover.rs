//! Handing a connection over, for a connection in this process or in another one to go on from
//! where it stopped, as a service or a bus that restarts does.
//!
//! This is behind the `handover` cargo feature, which is off by default.
//!
//! # How it goes
//!
//! A connection is handed over through the stream that has had every one of its messages, from the
//! first: the one that [`Builder::build_message_stream`] builds along with the connection.
//!
//! 1. Take the messages of the connection off that stream, and deal with them, for as long as the
//!    connection is yours.
//! 2. Once it is time to hand the connection over, stop taking messages off the stream, and finish
//!    what you do with those that you took. The replies and the other messages that you send go out
//!    until the hand-over commits.
//! 3. Call [`MessageStream::hand_over`]. It stops the connection reading, wherever the peer is in
//!    sending a message, and takes the connection apart. The [`Handover`] that it gives holds the
//!    socket, the messages that were queued on the stream and that it did not yield, what the
//!    connection read off the socket after them and made no message of yet, the file descriptors
//!    that came with all of those, and what a connection that goes on needs to know of this one.
//!
//! Nothing that the peer sent is lost on the way, and nothing is written to the socket once the
//! hand-over commits, so the peer sees nothing but a pause. A peer that closed its end of the
//! connection, such as one that sent a last message and went, is no exception, even once a send to
//! it failed: what it sent is handed over, and the connection that goes on comes to its end. For
//! that, the connection keeps what it read of such a peer and made no message of, the file
//! descriptors that came with it included, until it is handed over or dropped.
//!
//! A hand-over that fails, or that is given up on, such as by a timeout, loses nothing either: the
//! stream keeps the messages that the hand-over took off it, and yields them before the rest. The
//! connection has stopped reading by then, for good. Hand it over again through the same stream, or
//! take the messages off the stream, deal with them, and close the connection.
//! [`MessageStream::hand_over`] tells more.
//!
//! # Other consumers of the messages
//!
//! The object server, proxies, other [`MessageStream`]s of the connection and its method calls
//! that wait for their replies take its messages too. A message that one of them took, and that
//! the stream did not yield, is handled twice: by them, and again by the connection that goes on.
//! The stream that `build_message_stream` gives, on a connection with no object server and no other
//! stream, is what a hand-over is for.
//!
//! # Example
//!
//! ```
//! use zbus::{block_on, connection::Builder};
//!
//! # fn main() -> zbus::Result<()> {
//! block_on(async {
//!     let mut stream = Builder::session().build_message_stream().await?;
//!     // Take the messages of the connection off `stream` and deal with them, until it is time to
//!     // hand the connection over.
//!
//!     let handover = stream.hand_over().await?;
//!     assert!(handover.unique_name.is_some());
//!
//!     Ok(())
//! })
//! # }
//! ```
//!
//! [`Builder::build_message_stream`]: super::Builder::build_message_stream
//! [`MessageStream`]: crate::MessageStream
//! [`MessageStream::hand_over`]: crate::MessageStream::hand_over

#[cfg(windows)]
use std::os::windows::io::{AsSocket, FromRawSocket, IntoRawSocket, OwnedSocket};
#[cfg(unix)]
use std::os::{
    fd::{AsFd, OwnedFd},
    unix::net::UnixStream,
};
use std::{
    collections::VecDeque,
    fmt,
    future::{Future, poll_fn},
    io, mem,
    net::TcpStream,
    num::NonZeroU32,
    pin::{Pin, pin},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

use futures_core::Stream;
#[cfg(windows)]
use uds_windows::UnixStream;
use zruntime::{
    Event, EventListener,
    broadcast::{Receiver, TryRecvError},
};

#[cfg(any(unix, windows))]
use crate::runtime::IoSource;
use crate::{
    Connection, Error, Message, OwnedGuid, Result,
    connection::socket::{BoxedSplit, ReadHalf, Split, WriteHalf},
    fdo::ConnectionCredentials,
    message,
    names::OwnedUniqueName,
};

/// A connection taken apart by [`MessageStream::hand_over`], for a connection to go on from where
/// it stopped.
///
/// The connection that goes on can be in this process. It can also be in another process that the
/// hand-over is passed to, such as the next instance of a service that restarts, or a helper
/// process.
///
/// This type is only available when the `handover` feature is enabled.
///
/// # Passing it to another process
///
/// Pass on the descriptors: the socket's and, on unix, those of `unread_fds`, in their order, such
/// as over a unix socket or through systemd's file descriptor store. Pass the other fields on as
/// data.
///
/// `unread_bytes` holds the messages that were queued on the stream, and the start of one that the
/// peer was in the middle of sending, which can be as long as the longest message that D-Bus
/// allows (128 MiB).
///
/// [`MessageStream::hand_over`]: crate::MessageStream::hand_over
#[non_exhaustive]
#[must_use = "dropping a `Handover` closes the socket that it holds, which disconnects the peer"]
pub struct Handover {
    /// The socket of the connection.
    pub socket: HandoverSocket,
    /// The GUID of the server end of the connection.
    pub server_guid: OwnedGuid,
    /// The unique name of the connection, if it has one.
    ///
    /// For a connection to a bus, it is the name that the bus assigned it, which the bus goes on
    /// knowing the connection by. The connection that goes on keeps it, rather than say `Hello`
    /// again. For a peer-to-peer connection, it is the one set on it.
    pub unique_name: Option<OwnedUniqueName>,
    /// The bytes read off the socket that are yet to be taken as messages.
    ///
    /// They start at the beginning of a message, and hold any number of messages, the last of
    /// them possibly in part. Whatever the socket still holds follows them.
    pub unread_bytes: Vec<u8>,
    /// The file descriptors of the messages that were queued on the stream, followed by those that
    /// came in with the rest of the bytes read off the socket and that no message has taken yet,
    /// in their order.
    ///
    /// The messages of `unread_bytes`, and those after them, take them, as many as each one's
    /// header says. Those of the messages that were queued are duplicates, which are
    /// close-on-exec, as std makes them, while the others are not, as zbus receives them. To pass
    /// them on through an `exec`, clear the close-on-exec flag of each.
    #[cfg(unix)]
    pub unread_fds: Vec<OwnedFd>,
    /// Whether the two ends of the connection agreed to pass file descriptors.
    #[cfg(unix)]
    pub unix_fd_passing: bool,
    /// Whether the connection is a peer-to-peer one rather than one to a bus, as
    /// [`Builder::p2p`] made it.
    ///
    /// [`Builder::p2p`]: super::Builder::p2p
    #[cfg(feature = "p2p")]
    pub p2p: bool,
    /// The serial number that the process was to give the next message that it builds.
    ///
    /// The connection that goes on is to give its messages serial numbers from this one on.
    /// Otherwise a reply to a method call made before the hand-over could be taken for the reply
    /// to one made after it.
    pub next_serial: NonZeroU32,
}

/// Shows how many bytes and file descriptors were left unread rather than which: the bytes can be
/// as many as the longest message that D-Bus allows.
impl fmt::Debug for Handover {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut handover = f.debug_struct("Handover");
        handover
            .field("socket", &self.socket)
            .field("server_guid", &self.server_guid)
            .field("unique_name", &self.unique_name)
            .field("unread_bytes", &self.unread_bytes.len());
        #[cfg(unix)]
        handover
            .field("unread_fds", &self.unread_fds.len())
            .field("unix_fd_passing", &self.unix_fd_passing);
        #[cfg(feature = "p2p")]
        handover.field("p2p", &self.p2p);

        handover.field("next_serial", &self.next_serial).finish()
    }
}

/// The socket of a connection that was handed over ([`MessageStream::hand_over`]).
///
/// This type is only available when the `handover` feature is enabled.
///
/// The first two variants hold the only descriptor of the socket that zbus has kept. Dropping one
/// of them closes the socket, and the peer sees the connection go, unless the descriptor was
/// passed on first.
///
/// [`MessageStream::hand_over`]: crate::MessageStream::hand_over
#[derive(Debug)]
#[non_exhaustive]
pub enum HandoverSocket {
    /// A unix-domain stream. On Windows it is that of `uds_windows`, as [`Builder::unix_stream`]
    /// takes.
    ///
    /// This is for a connection over a unix socket that zbus drives itself: one built with
    /// [`Builder::unix_stream`], or one to a `unix:` address, such as that of the session or
    /// system bus. The hand-over gives a new descriptor of the socket, which is close-on-exec, as
    /// std makes them. The socket is in non-blocking mode, as zbus switched it to that, so a read
    /// that finds nothing to read fails with an error of kind `WouldBlock` rather than wait. Call
    /// `set_nonblocking(false)` on the stream to read it with calls that wait.
    ///
    /// To pass it to another process, pass the descriptor on, or clear its close-on-exec flag for
    /// an `exec` to keep it. On Windows, duplicate it for the other process with
    /// `WSADuplicateSocketW`.
    ///
    /// [`Builder::unix_stream`]: super::Builder::unix_stream
    #[cfg(any(unix, windows))]
    UnixStream(UnixStream),
    /// A TCP stream, for a connection over a TCP socket that zbus drives itself: one built with
    /// [`Builder::tcp_stream`], or one to a `tcp:` address. Otherwise as for `UnixStream`.
    ///
    /// [`Builder::tcp_stream`]: super::Builder::tcp_stream
    TcpStream(TcpStream),
    /// The halves of a socket, for any other connection: one over a [`Socket`] of your own,
    #[cfg_attr(
        feature = "p2p",
        doc = "such as a [`Channel`](super::socket::Channel), one over the pipes of a `unixexec:`"
    )]
    #[cfg_attr(
        not(feature = "p2p"),
        doc = "such as a `Channel`, one over the pipes of a `unixexec:`"
    )]
    /// address, or, on WASI, which can't duplicate a descriptor, any connection.
    ///
    /// The halves can only go on in this process. Those that zbus made run on the runtime of the
    /// connection that they came from, so they need it to keep running. Dropping them is what
    /// dropping a connection does to its socket.
    ///
    /// [`Socket`]: super::Socket
    Socket(BoxedSplit),
}

impl Connection {
    /// Hand this connection over, through the stream whose queue is `queue` and whose backlog is
    /// `backlog`: the steps that [`MessageStream::hand_over`] documents.
    ///
    /// What the hand-over takes off the queue goes to the backlog, which the stream yields before
    /// its queue, so that a hand-over that fails, or whose future is dropped, loses none of it.
    ///
    /// [`MessageStream::hand_over`]: crate::MessageStream::hand_over
    pub(crate) async fn hand_over(
        &self,
        queue: &mut Receiver<Result<Message>>,
        backlog: &mut Backlog,
    ) -> Result<Handover> {
        self.ensure_whole().await?;
        let stream = backlog.stream.get_or_insert_with(Default::default);
        self.stop_reading(stream)?;
        self.take_queue_until_stopped(queue, &mut backlog.items)
            .await;

        self.take_apart(&mut backlog.items).await
    }

    /// Fail if a send was cut off midway, as `take_apart` then does, but before the connection
    /// stops reading, so that it reads on. A send under way holds the socket, and only `take_apart`
    /// finds out how it went, once it is through.
    async fn ensure_whole(&self) -> Result<()> {
        let mut lock = pin!(self.inner.socket_write.lock());
        let write = poll_fn(|cx| Poll::Ready(lock.as_mut().poll(cx))).await;
        if matches!(write, Poll::Ready(write) if write.torn) {
            return Err(torn());
        }

        Ok(())
    }

    /// Ask the reader to stop reading the socket, wherever it is in a message, for a hand-over
    /// through the stream that `stream` stands for.
    ///
    /// This applies to every clone of the connection, and does nothing the second time, or on a
    /// connection that is closed already, which goes on ending as a closed one does, rather than
    /// be taken for one that stopped. The first hand-over that gets here claims the connection for
    /// its stream, which a hand-over through another stream then fails on.
    fn stop_reading(&self, stream: &Arc<()>) -> Result<()> {
        let stop = &self.inner.socket_status.stop;
        if !stop.can_stop.load(Ordering::Acquire) {
            return Err(Error::Unsupported);
        }
        stop.claim(stream)?;
        if self.inner.is_closed() {
            return Ok(());
        }
        if !stop.asked.swap(true, Ordering::AcqRel) {
            stop.asked_event.notify(usize::MAX);
        }

        Ok(())
    }

    /// Wait for the reader to have stopped, or for the connection to have ended, while taking what
    /// is queued on `queue` off it, oldest first, to the end of `taken`. Then take the rest.
    ///
    /// Each message taken off makes room for one that the reader waits to queue, so a reader that
    /// waits for room in the stream gets it, and stops: nobody else takes messages off the stream
    /// now.
    async fn take_queue_until_stopped(
        &self,
        queue: &mut Receiver<Result<Message>>,
        taken: &mut VecDeque<Result<Message>>,
    ) {
        let inner = &*self.inner;
        let stop = &inner.socket_status.stop;
        loop {
            let mut ended = stop.reader_end_event.listen();
            let mut closed = inner.closed_listener();
            if !stop.reading() || inner.is_closed() {
                break;
            }
            poll_fn(|cx| {
                while let Poll::Ready(Some(msg)) = Pin::new(&mut *queue).poll_next(cx) {
                    taken.push_back(msg);
                }
                if Pin::new(&mut ended).poll(cx).is_ready()
                    || Pin::new(&mut closed).poll(cx).is_ready()
                {
                    return Poll::Ready(());
                }

                Poll::Pending
            })
            .await;
        }
        // Nothing more is queued once the reader ended.
        loop {
            match queue.try_recv() {
                Ok(msg) => taken.push_back(msg),
                // As the stream itself would, this goes on from the oldest message still queued.
                Err(TryRecvError::Overflowed(_)) => continue,
                Err(TryRecvError::Empty | TryRecvError::Closed) => break,
            }
        }
    }

    /// Take the connection apart, once its reader stopped or came to the end of the input, with
    /// the messages of `taken`, which the hand-over took off its stream, ahead of what the reader
    /// read.
    ///
    /// The messages stay in `taken` unless the connection is handed over. An error in it is that
    /// which the reader ended on, as it came to the end of the input, and goes with them then.
    async fn take_apart(&self, taken: &mut VecDeque<Result<Message>>) -> Result<Handover> {
        let inner = &*self.inner;
        let stop = &inner.socket_status.stop;
        // Under the lock that every send, shutdown and hand-over takes, and which a send holds
        // until it is through.
        let mut guard = inner.socket_write.lock().await;
        let write = &mut *guard;
        let mut end = stop
            .reader_end
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if matches!(*end, ReaderEnd::HandedOver) {
            return Err(not_connected("the connection was handed over already").into());
        }
        // Set before the socket is shut down, under the lock that is held here, so a closed
        // connection whose reader then came to the end of the input is not taken for one whose
        // peer closed its end.
        if write.shut_down {
            return Err(not_connected("the connection was closed").into());
        }
        if !matches!(*end, ReaderEnd::Stopped(_) | ReaderEnd::Ended(_)) {
            return Err(not_connected("the connection ended before it stopped reading").into());
        }
        if write.torn {
            return Err(torn());
        }
        // The last steps that can fail, so that a failure commits nothing. The messages may be
        // queued for other streams too, so their descriptors are duplicated rather than taken.
        #[cfg(unix)]
        let mut unread_fds = taken
            .iter()
            .flatten()
            .flat_map(|msg| msg.data().fds())
            .map(|fd| fd.as_fd().try_clone_to_owned())
            .collect::<io::Result<Vec<_>>>()?;
        #[cfg(any(unix, windows))]
        let duplicate = write
            .source
            .as_ref()
            .map(|(source, kind)| dup(source).map(|socket| (socket, *kind)))
            .transpose()?;

        // Nothing fails or waits from here on, so the connection is either handed over whole, or
        // not at all.
        let (ReaderEnd::Stopped(parts) | ReaderEnd::Ended(parts)) =
            mem::replace(&mut *end, ReaderEnd::HandedOver)
        else {
            unreachable!("the reader stopped or ended, as the lock that is still held says");
        };
        drop(end);
        stop.reader_end_event.notify(usize::MAX);
        let write_half = write
            .half
            .take()
            .expect("the write half is only taken by a hand-over, which there was none of");
        #[cfg(any(unix, windows))]
        {
            write.source = None;
        }
        inner.socket_status.closed.store(true, Ordering::Release);
        inner.socket_status.closed_event.notify(usize::MAX);
        drop(guard);

        let ReadParts {
            half: read_half,
            unread_bytes: read,
            #[cfg(unix)]
                unread_fds: read_fds,
        } = parts;
        // The error that the reader ended on, if any, is no message to hand over.
        let queued: Vec<Message> = taken.drain(..).flatten().collect();
        let unread_bytes = if queued.is_empty() {
            // What was read can be as long as the longest message, so it is moved rather than
            // copied where nothing comes before it.
            read
        } else {
            let len = queued.iter().map(|msg| msg.data().len()).sum::<usize>() + read.len();
            let mut unread_bytes = Vec::with_capacity(len);
            for msg in &queued {
                unread_bytes.extend_from_slice(msg.data());
            }
            unread_bytes.extend_from_slice(&read);

            unread_bytes
        };
        #[cfg(unix)]
        unread_fds.extend(read_fds);
        #[cfg(any(unix, windows))]
        let socket = match duplicate {
            Some((socket, kind)) => {
                // The runtime stops watching the socket as the halves go, while their descriptor
                // of it is still open, and then that descriptor is closed.
                drop(read_half);
                drop(write_half);

                kind.stream(socket)
            }
            None => HandoverSocket::Socket(Split::new(read_half, write_half)),
        };
        // WASI can't duplicate a descriptor.
        #[cfg(not(any(unix, windows)))]
        let socket = HandoverSocket::Socket(Split::new(read_half, write_half));

        Ok(Handover {
            socket,
            server_guid: inner.server_guid.clone(),
            unique_name: self.unique_name().cloned(),
            unread_bytes,
            #[cfg(unix)]
            unread_fds,
            #[cfg(unix)]
            unix_fd_passing: inner.cap_unix_fd,
            #[cfg(feature = "p2p")]
            p2p: !self.is_bus(),
            next_serial: message::header::next_serial(),
        })
    }
}

/// What a hand-over through a stream took off the queue of the stream, and the stream is yet to
/// yield, for the stream to keep, whatever comes of the hand-over.
#[derive(Debug, Default)]
pub(crate) struct Backlog {
    /// What was taken off the queue, oldest first: messages, and the error that the reader ended
    /// on, if it came to the end of the input.
    items: VecDeque<Result<Message>>,
    /// What tells the stream apart from the other streams of its connection, from its first
    /// hand-over on.
    stream: Option<Arc<()>>,
}

impl Backlog {
    /// Take the oldest of what is left, for the stream to yield it ahead of its queue.
    pub(crate) fn pop(&mut self) -> Option<Result<Message>> {
        self.items.pop_front()
    }

    /// Whether nothing is left.
    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// A clone of a stream yields what is left in its backlog too, but it is another stream, which a
/// hand-over through it tells apart from this one.
impl Clone for Backlog {
    fn clone(&self) -> Self {
        Self {
            items: self.items.clone(),
            stream: None,
        }
    }
}

/// What a connection writes its socket through, which those that write it, shut it down or hand it
/// over take in turn.
#[derive(Debug)]
pub(super) struct SocketWrite {
    /// The write half of the socket, until the connection is handed over.
    half: Option<Box<dyn WriteHalf>>,
    /// The socket itself, if zbus drives it and so can give a descriptor of it for a hand-over.
    #[cfg(any(unix, windows))]
    source: Option<(IoSource, SocketKind)>,
    /// Whether a send failed or was given up on midway, which can leave part of a message in the
    /// socket. The peer can then make no sense of anything sent after it, so the connection can't
    /// be handed over any more. A send that failed as the peer receives nothing more leaves this
    /// as it was, as nothing sent after it reaches the peer.
    torn: bool,
    /// Whether the connection was closed, which shuts the socket down. It is set before the
    /// socket is shut down, so it also tells a reader that came to the end of the input as the
    /// connection was closed apart from one whose peer closed its end.
    shut_down: bool,
}

impl SocketWrite {
    pub(super) fn new(
        half: Box<dyn WriteHalf>,
        #[cfg(any(unix, windows))] source: Option<(IoSource, SocketKind)>,
    ) -> Self {
        Self {
            half: Some(half),
            #[cfg(any(unix, windows))]
            source,
            torn: false,
            shut_down: false,
        }
    }

    /// Send `msg`, as [`WriteHalf::send_message`] does, unless the connection was handed over.
    pub(super) async fn send_message(&mut self, msg: &Message) -> Result<()> {
        let half = self.half.as_mut().ok_or_else(handed_over)?;
        // Until the message is through, the socket may hold a part of it.
        let torn = mem::replace(&mut self.torn, true);
        let sent = half.send_message(msg).await;
        self.through(torn, &sent);

        sent
    }

    /// Send `msgs`, as [`WriteHalf::send_messages`] does, unless the connection was handed over.
    pub(super) async fn send_messages(&mut self, msgs: &[Message]) -> Result<()> {
        let half = self.half.as_mut().ok_or_else(handed_over)?;
        // Until the messages are through, the socket may hold a part of one.
        let torn = mem::replace(&mut self.torn, true);
        let sent = half.send_messages(msgs).await;
        self.through(torn, &sent);

        sent
    }

    /// The credentials of the peer, as [`WriteHalf::peer_credentials`] gives them, unless the
    /// connection was handed over.
    pub(super) async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        let half = self.half.as_mut().ok_or_else(handed_over)?;

        half.peer_credentials().await
    }

    /// Shut the socket down, as [`WriteHalf::close`] does, unless the connection was handed over,
    /// which leaves the socket alone: it is no longer the connection's.
    pub(super) async fn close(&mut self) -> io::Result<()> {
        let Some(half) = self.half.as_mut() else {
            return Ok(());
        };
        // Set before the socket is shut down, as the shutdown can be given up on midway, and as
        // the reader can come to the end of the input as soon as it is.
        self.shut_down = true;

        half.close().await
    }

    /// Put back `torn`, whether the socket was torn before a send, now that the send is through
    /// with `sent`, unless the send failed while the peer may still receive what follows it.
    fn through(&mut self, torn: bool, sent: &Result<()>) {
        if sent.as_ref().is_err_and(|e| !receives_nothing_more(e)) {
            return;
        }
        self.torn = torn;
    }
}

/// The kind of a socket that zbus drives itself, and so can give a descriptor of for a hand-over.
#[cfg(any(unix, windows))]
#[derive(Clone, Copy, Debug)]
pub(crate) enum SocketKind {
    Unix,
    Tcp,
}

#[cfg(any(unix, windows))]
impl SocketKind {
    /// The stream of this kind over `socket`.
    fn stream(self, socket: Owned) -> HandoverSocket {
        match self {
            #[cfg(unix)]
            Self::Unix => HandoverSocket::UnixStream(UnixStream::from(socket)),
            #[cfg(windows)]
            Self::Unix => {
                let raw = socket.into_raw_socket();
                // SAFETY: `raw` is a unix-domain socket that zbus owns, since it duplicated it,
                // and that nothing else closes, as `socket` gave it up. The stream takes it over.
                let stream = unsafe { UnixStream::from_raw_socket(raw) };

                HandoverSocket::UnixStream(stream)
            }
            Self::Tcp => HandoverSocket::TcpStream(TcpStream::from(socket)),
        }
    }
}

/// Whether the reader of a connection is to stop, or did, for the connection to be handed over.
#[derive(Debug)]
pub(super) struct StopStatus {
    /// Whether the reader can stop, which is whether its read half can take out what it read off
    /// the socket and is yet to return in a message. It is set before the reader starts.
    pub can_stop: AtomicBool,
    /// Whether the connection was asked to stop reading.
    asked: AtomicBool,
    /// Notified when the connection is asked to stop reading.
    asked_event: Event,
    /// The stream of the first hand-over that asked the connection to stop reading, and which may
    /// have taken messages off that stream since. A hand-over through another stream would miss
    /// those, and fails.
    stream: Mutex<Option<Arc<()>>>,
    /// Whether the reader still reads, and how it ended if not.
    reader_end: Mutex<ReaderEnd>,
    /// Notified when the reader ends.
    reader_end_event: Event,
}

impl StopStatus {
    pub(super) fn new() -> Self {
        Self {
            can_stop: AtomicBool::new(false),
            asked: AtomicBool::new(false),
            asked_event: Event::new(),
            stream: Mutex::new(None),
            reader_end: Mutex::new(ReaderEnd::Reading),
            reader_end_event: Event::new(),
        }
    }

    /// A listener for the connection to be asked to stop reading, if the reader can stop.
    pub(super) fn listener(&self) -> Option<EventListener> {
        self.can_stop
            .load(Ordering::Acquire)
            .then(|| self.asked_event.listen())
    }

    /// Fails with the error of a connection that stopped reading, once it was asked to.
    pub(super) fn ensure_reading(&self) -> Result<()> {
        if self.asked() {
            return Err(stopped_error());
        }

        Ok(())
    }

    /// Whether the connection was asked to stop reading.
    pub(super) fn asked(&self) -> bool {
        self.asked.load(Ordering::Acquire)
    }

    /// Record how the reader ended, unless it ended already, and tell whoever waits for it.
    pub(super) fn end_reading(&self, end: ReaderEnd) {
        let mut slot = self
            .reader_end
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !matches!(*slot, ReaderEnd::Reading) {
            // How it ended first stands. `end` is dropped once the lock is let go of.
            return;
        }
        *slot = end;
        drop(slot);
        self.reader_end_event.notify(usize::MAX);
    }

    /// Claim the connection for the hand-overs through the stream that `stream` stands for,
    /// unless a hand-over through another stream claimed it first.
    fn claim(&self, stream: &Arc<()>) -> Result<()> {
        let mut claimed = self.stream.lock().unwrap_or_else(PoisonError::into_inner);
        match &*claimed {
            Some(claimed) if !Arc::ptr_eq(claimed, stream) => Err(not_connected(
                "a hand-over through another stream of the connection may have taken messages off \
                 that stream",
            )
            .into()),
            Some(_) => Ok(()),
            None => {
                *claimed = Some(stream.clone());

                Ok(())
            }
        }
    }

    /// Whether the reader still reads.
    fn reading(&self) -> bool {
        matches!(
            *self
                .reader_end
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            ReaderEnd::Reading
        )
    }
}

/// Whether the reader of a connection still reads its socket, and how it ended if not.
pub(super) enum ReaderEnd {
    /// The reader still reads.
    Reading,
    /// The connection stopped reading, and the reader left it the read half and what it read.
    Stopped(ReadParts),
    /// The reader came to the end of the input, which the read half reports as an I/O error of
    /// kind `UnexpectedEof` (`ends_input`), and left the connection the read half and what it
    /// read, as `Stopped` does. The connection is closed then, and its streams ended on that
    /// error. What the reader left stays here, file descriptors included, until the connection is
    /// handed over or dropped, as nothing tells whether a hand-over is to come.
    ///
    /// The peer closed its end of the socket, unless the connection itself was closed, which shuts
    /// the socket down, so that the reader then comes to the end of the input too.
    /// `SocketWrite::shut_down` tells which: a close sets it before it shuts the socket down,
    /// under the lock that a hand-over takes before it looks at how the reader ended.
    Ended(ReadParts),
    /// The reader ended on any other error, or was let go of before it ended on its own.
    Failed,
    /// The connection stopped reading and was handed over, which took what the reader left.
    HandedOver,
}

/// Shows how much was left unread rather than what, which can be as long as the longest message
/// that D-Bus allows, as this is part of what a connection shows of itself.
impl fmt::Debug for ReaderEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (name, parts) = match self {
            Self::Reading => return f.write_str("Reading"),
            Self::Stopped(parts) => ("Stopped", parts),
            Self::Ended(parts) => ("Ended", parts),
            Self::Failed => return f.write_str("Failed"),
            Self::HandedOver => return f.write_str("HandedOver"),
        };
        let mut left = f.debug_struct(name);
        left.field("half", &parts.half)
            .field("unread_bytes", &parts.unread_bytes.len());
        #[cfg(unix)]
        left.field("unread_fds", &parts.unread_fds.len());

        left.finish()
    }
}

/// What the reader of a connection leaves for it when the connection stops reading, or the reader
/// comes to the end of the input.
pub(super) struct ReadParts {
    /// The read half of the socket.
    pub half: Box<dyn ReadHalf>,
    /// What was read off the socket and is yet to make a message, from the start of a message.
    pub unread_bytes: Vec<u8>,
    /// The file descriptors that came in with what was read, and that no message took, in the
    /// order that they came in.
    #[cfg(unix)]
    pub unread_fds: Vec<OwnedFd>,
}

impl ReadParts {
    /// What the reader leaves: `half`, with what it read and is yet to return in a message taken
    /// out of it, after `unread_bytes` and `unread_fds`, which the reader kept for its next read.
    ///
    /// The last call to receive a message is over, or was dropped wherever it waited, so the half
    /// has all that it read in hand.
    pub(super) fn new(
        mut half: Box<dyn ReadHalf>,
        mut unread_bytes: Vec<u8>,
        #[cfg(unix)] mut unread_fds: Vec<OwnedFd>,
    ) -> Self {
        half.take_unread(
            &mut unread_bytes,
            #[cfg(unix)]
            &mut unread_fds,
        );

        Self {
            half,
            unread_bytes,
            #[cfg(unix)]
            unread_fds,
        }
    }
}

/// Whether `error`, which a read half failed with, is the end of the input: an I/O error of kind
/// `UnexpectedEof`, as a half that can take out what it read reports it
/// ([`ReadHalf::can_take_unread`]).
pub(super) fn ends_input(error: &Error) -> bool {
    matches!(error, Error::InputOutput(e) if e.kind() == io::ErrorKind::UnexpectedEof)
}

/// The error of what a connection that stopped reading can no longer do.
pub(super) fn stopped_error() -> Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "the connection stopped reading",
    )
    .into()
}

/// A new descriptor of `source`, for the socket to outlive the one that zbus closes.
#[cfg(any(unix, windows))]
fn dup(source: &IoSource) -> io::Result<Owned> {
    #[cfg(unix)]
    let borrowed = source.as_fd();
    #[cfg(windows)]
    let borrowed = source.as_socket();

    borrowed.try_clone_to_owned()
}

/// What this platform owns a socket as.
#[cfg(unix)]
type Owned = OwnedFd;
#[cfg(windows)]
type Owned = OwnedSocket;

/// The error of a hand-over of a connection whose socket may hold part of a message.
fn torn() -> Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "a send was cut off midway: the socket may hold part of a message",
    )
    .into()
}

/// Whether `error`, which a send failed with, says that the peer receives nothing more, as one
/// that closed its end of the socket does: an I/O error of kind `BrokenPipe`, `ConnectionReset`
/// or `ConnectionAborted`. Nothing sent from then on reaches the peer, so part of a message that
/// the send left in the socket is followed by nothing that the peer could misread.
///
/// A connection that was closed fails its sends with `BrokenPipe` too, as the close shuts the
/// socket down, but `SocketWrite::shut_down` keeps it from being handed over anyway.
fn receives_nothing_more(error: &Error) -> bool {
    matches!(
        error,
        Error::InputOutput(e) if matches!(
            e.kind(),
            io::ErrorKind::BrokenPipe
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
        )
    )
}

/// The error of what a connection that was handed over can no longer do.
fn handed_over() -> io::Error {
    not_connected("the connection was handed over")
}

/// The error, for `reason`, of what a connection can no longer do, or of a hand-over that the
/// connection is not in a state for.
fn not_connected(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, reason)
}

#[cfg(all(test, feature = "p2p"))]
mod tests {
    use std::{fmt::Debug, io};
    #[cfg(unix)]
    use std::{
        future::Future,
        io::{ErrorKind, IoSlice, Read, Write},
        mem::MaybeUninit,
        net::{Ipv4Addr, Shutdown, TcpListener, TcpStream},
        num::NonZeroUsize,
        os::{
            fd::{AsFd, BorrowedFd, OwnedFd},
            unix::net::{UnixListener, UnixStream},
        },
        pin::{Pin, pin},
        sync::Arc,
        thread,
        time::Duration,
    };

    use futures_util::StreamExt;
    use ntest::timeout;
    use test_log::test;

    use super::HandoverSocket;
    #[cfg(unix)]
    use super::ReaderEnd;
    #[cfg(unix)]
    use crate::{
        Address,
        address::transport::{Transport, Unix, UnixSocket},
        connection::{
            AuthMechanism,
            socket::{BoxedSplit, RecvmsgResult, Split, WriteHalf},
        },
        runtime::io::{RegisteredIo, UnixOps, registered},
    };
    use crate::{
        Connection, Error, Guid, MatchRule, Message, MessageStream, Result,
        connection::{
            Builder,
            socket::{Channel, ReadHalf, read_primary_header},
        },
        message::Type,
        runtime::{
            Runtime,
            test_runtime::{AbortingRuntime, TestRuntime, under_every_runtime},
        },
        wire::{
            Endian,
            serialized::{Context, Data},
        },
    };

    /// A method call that waits for its reply fails once a hand-over stops the connection reading,
    /// and once the connection is handed over, calls fail before they are sent, and so does a
    /// stream for a match rule.
    #[test]
    #[timeout(15000)]
    fn a_call_that_waits_for_its_reply_fails_once_the_connection_stops() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = channel_pair(&runtime).await;
            let conn = Connection::from(&stream);
            let mut peer_stream = MessageStream::from(&peer);

            // Nothing ever answers this call, so only the stop can end the wait for its reply. The
            // peer seeing the call is what says it is registered and away.
            let calling = conn.call_method(None::<()>, PATH, Some(INTERFACE), "NoReply", &());
            let handing_over = async {
                let call = peer_stream.next().await.unwrap().unwrap();
                assert_eq!(call.header().member().unwrap().as_str(), "NoReply");

                stream.hand_over().await.unwrap()
            };
            let (reply, _handover) = futures_util::future::join(calling, handing_over).await;
            assert_not_connected(reply);

            let late = conn
                .call_method(None::<()>, PATH, Some(INTERFACE), "Late", &())
                .await;
            assert_not_connected(late);
            assert_not_connected(MessageStream::for_match_rule(signals(), &conn, None).await);
        });
    }

    /// A connection whose peer closed its end, once it sent a last message, is handed over with
    /// that message, which the stream did not yield, though it came to the end of the input before
    /// the hand-over. Its streams end on that end then, and so do the calls made since.
    #[test]
    #[timeout(15000)]
    fn a_connection_whose_peer_went_is_handed_over_with_what_it_sent() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = channel_pair(&runtime).await;
            let conn = Connection::from(&stream);
            let mut other = MessageStream::from(&conn);
            let msg = signal(1);

            peer.send(&msg).await.unwrap();
            peer.close().await.unwrap();

            let received = other.next().await.unwrap().unwrap();
            assert_eq!(&received.data()[..], &msg.data()[..]);
            assert_end_of_input(other.next().await.unwrap());
            assert!(other.next().await.is_none(), "the stream outlived its end");
            let call = conn
                .call_method(None::<()>, PATH, Some(INTERFACE), "Late", &())
                .await;
            assert_end_of_input(call);
            assert_broken_pipe(MessageStream::for_match_rule(signals(), &conn, None).await);
            let handover = stream.hand_over().await.unwrap();
            assert_eq!(handover.unread_bytes, &msg.data()[..]);
            assert!(
                stream.next().await.is_none(),
                "the stream outlived the hand-over"
            );
        });
    }

    /// A connection whose peer closed its end of the socket is handed over with what it read, the
    /// part of a message included, and the socket, which holds the rest up to that end, whether
    /// the connection came to the end before the hand-over or as the hand-over stopped it, and
    /// whatever the peer sent before.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_connection_whose_peer_closed_its_end_is_handed_over() {
        under_every_runtime(|runtime| async move {
            for ended in [true, false] {
                for (whole, cut) in [(true, 0), (true, 10), (false, 10), (false, 0)] {
                    let (mut stream, peer, _probe) = served(&runtime).await;
                    let conn = Connection::from(&stream);
                    let (last, next) = (signal(1), signal(2));
                    let mut sent = if whole { last.data().to_vec() } else { vec![] };
                    sent.extend_from_slice(&next.data()[..cut]);

                    (&peer).write_all(&sent).unwrap();
                    peer.shutdown(Shutdown::Write).unwrap();
                    if ended {
                        conn.closed().await;
                    }
                    let handover = stream.hand_over().await.unwrap();

                    let case = format!("ended: {ended}, whole: {whole}, cut: {cut}");
                    assert!(stream.next().await.is_none(), "{case}");
                    let socket = match handover.socket {
                        HandoverSocket::UnixStream(socket) => socket,
                        socket => panic!("a unix socket was handed over as {socket:?}"),
                    };
                    // What the connection didn't read is still in the socket, up to its end.
                    let mut unread = handover.unread_bytes;
                    (&socket).read_to_end(&mut unread).unwrap();
                    assert_eq!(unread, sent, "{case}");
                }
            }
        });
    }

    /// A connection whose peer went, once it sent a last message, is handed over with that
    /// message and the socket, though a send to the peer failed since: the peer receives nothing
    /// more, so nothing follows what the send may have left in the socket.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_connection_whose_peer_went_is_handed_over_once_a_send_to_it_failed() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer, _probe) = served(&runtime).await;
            let conn = Connection::from(&stream);
            let last = signal(1);

            // The peer read all that the connection sent it, the answer to its handshake, so it
            // ends what it sends as it goes.
            (&peer).write_all(&last.data()[..]).unwrap();
            drop(peer);
            conn.closed().await;
            assert_broken_pipe(conn.send(&signal(2)).await);
            let handover = stream.hand_over().await.unwrap();

            assert_eq!(handover.unread_bytes, &last.data()[..]);
            let socket = match handover.socket {
                HandoverSocket::UnixStream(socket) => socket,
                socket => panic!("a unix socket was handed over as {socket:?}"),
            };
            // The socket is at the end that the connection came to.
            let mut rest = vec![];
            (&socket).read_to_end(&mut rest).unwrap();
            assert!(rest.is_empty(), "the socket holds more: {rest:?}");
        });
    }

    /// A connection whose peer went without reading what the connection sent it, once it sent a
    /// last message, is handed over with that message: Linux tells the connection that the peer
    /// went with a reset, but only once it read all that the peer sent.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    #[timeout(15000)]
    fn a_connection_whose_peer_went_with_messages_unread_is_handed_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer, _probe) = served(&runtime).await;
            let conn = Connection::from(&stream);
            let last = signal(1);

            conn.send(&signal(2)).await.unwrap();
            (&peer).write_all(&last.data()[..]).unwrap();
            drop(peer);
            conn.closed().await;
            let handover = stream.hand_over().await.unwrap();

            assert_eq!(handover.unread_bytes, &last.data()[..]);
        });
    }

    /// A connection over a read half that can't take out what it read isn't handed over, and
    /// reads on as if no hand-over had been asked for.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_connection_that_cant_stop_reading_reads_on() {
        under_every_runtime(|runtime| async move {
            let (peer, theirs) = UnixStream::pair().unwrap();
            let split = recvmsg_only(&runtime, theirs);
            let mut stream = on(
                Builder::authenticated_socket(split, Guid::generate()),
                &runtime,
            )
            .p2p()
            .build_message_stream()
            .await
            .unwrap();
            let conn = Connection::from(&stream);
            let mut other = MessageStream::from(&conn);

            let handed_over = stream.hand_over().await;

            assert!(
                matches!(handed_over, Err(Error::Unsupported)),
                "{handed_over:?}"
            );
            let mut matching = MessageStream::for_match_rule(signals(), &conn, None)
                .await
                .unwrap();
            let msg = signal(1);
            (&peer).write_all(&msg.data()[..]).unwrap();
            for stream in [&mut other, &mut matching] {
                let received = stream.next().await.unwrap().unwrap();
                assert_eq!(&received.data()[..], &msg.data()[..]);
            }
        });
    }

    /// A stream for a match rule never had the messages that didn't match it, so it isn't handed
    /// over, and the connection reads on.
    #[test]
    #[timeout(15000)]
    fn a_stream_for_a_match_rule_is_not_handed_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = channel_pair(&runtime).await;
            let conn = Connection::from(&stream);
            let mut matching = MessageStream::for_match_rule(signals(), &conn, None)
                .await
                .unwrap();

            let handed_over = matching.hand_over().await;

            assert!(
                matches!(handed_over, Err(Error::Unsupported)),
                "{handed_over:?}"
            );
            let msg = signal(1);
            peer.send(&msg).await.unwrap();
            let received = stream.next().await.unwrap().unwrap();
            assert_eq!(&received.data()[..], &msg.data()[..]);
        });
    }

    /// The messages queued on a full stream, which a reader that read more waits for room in, are
    /// taken off it by the hand-over itself, which lets the reader stop. They are handed over,
    /// those that the reader read after them too, and nothing is written to the socket.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_full_stream_is_handed_over_with_what_it_queued() {
        under_every_runtime(|runtime| async move {
            // However many of the messages the stream yielded before the hand-over.
            for taken in [0, 1] {
                let (peer, theirs) = UnixStream::pair().unwrap();
                // What the reader is yet to take off the socket is read through a descriptor of
                // its own.
                let probe = theirs.try_clone().unwrap();
                let socket = registered(&runtime, theirs, UnixOps).unwrap();
                let mut stream = on(
                    Builder::authenticated_socket(socket, Guid::generate()),
                    &runtime,
                )
                .p2p()
                .max_queued(NonZeroUsize::MIN)
                .build_message_stream()
                .await
                .unwrap();
                let conn = Connection::from(&stream);
                let msgs = [1, 2, 3, 4].map(signal);

                // The first message fills the queue of the stream.
                (&peer).write_all(&msgs[0].data()[..]).unwrap();
                wait_until(|| conn.inner.msg_receiver.is_full());
                // The reader takes in the others at once, and waits for room for the second.
                let rest: Vec<u8> = msgs[1..]
                    .iter()
                    .flat_map(|msg| msg.data().to_vec())
                    .collect();
                (&peer).write_all(&rest).unwrap();
                wait_until(|| rustix::io::ioctl_fionread(&probe).unwrap() == 0);
                for sent in &msgs[..taken] {
                    let received = stream.next().await.unwrap().unwrap();
                    assert_eq!(&received.data()[..], &sent.data()[..]);
                }

                let handover = stream.hand_over().await.unwrap();

                let left: Vec<u8> = msgs[taken..]
                    .iter()
                    .flat_map(|msg| msg.data().to_vec())
                    .collect();
                assert_eq!(handover.unread_bytes, left, "taken: {taken}");
                peer.set_nonblocking(true).unwrap();
                let read = (&peer).read(&mut [0; 1]);
                assert!(
                    matches!(&read, Err(e) if e.kind() == ErrorKind::WouldBlock),
                    "the peer got something: {read:?}",
                );
            }
        });
    }

    /// The messages queued on the stream, which it didn't yield, are handed over with their file
    /// descriptors, in their order, ahead of the part of a message that the reader read, and of
    /// the descriptors that came with that.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn messages_queued_on_the_stream_are_handed_over_with_their_descriptors() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer, probe) = served(&runtime).await;
            let conn = Connection::from(&stream);
            let pipes = [pipe(), pipe(), pipe(), pipe()];
            let fd = |i: usize| crate::Fd::from(&pipes[i]);
            let msgs = [
                Message::signal(PATH, INTERFACE, "WithFds")
                    .build(&fd(0))
                    .unwrap(),
                Message::signal(PATH, INTERFACE, "WithFds")
                    .build(&(fd(1), fd(2)))
                    .unwrap(),
                Message::signal(PATH, INTERFACE, "WithFds")
                    .build(&fd(3))
                    .unwrap(),
            ];
            let fds: Vec<Vec<_>> = msgs
                .iter()
                .map(|msg| msg.data().fds().iter().map(AsFd::as_fd).collect())
                .collect();

            send_with_fds(&peer, &msgs[0].data()[..], &fds[0]);
            send_with_fds(&peer, &msgs[1].data()[..], &fds[1]);
            wait_until(|| conn.inner.msg_receiver.len() == 2);
            // The descriptors go with the first bytes of a message, as D-Bus has it.
            send_with_fds(&peer, &msgs[2].data()[..20], &fds[2]);
            wait_until(|| rustix::io::ioctl_fionread(&probe).unwrap() == 0);

            let handover = stream.hand_over().await.unwrap();

            let sent = [
                &msgs[0].data()[..],
                &msgs[1].data()[..],
                &msgs[2].data()[..20],
            ]
            .concat();
            assert_eq!(handover.unread_bytes, sent);
            let inodes: Vec<_> = handover.unread_fds.iter().map(inode).collect();
            assert_eq!(inodes, pipes.each_ref().map(inode));
        });
    }

    /// A connection that stopped in the middle of a message hands over its socket with the part
    /// of the message that it read, and the socket carries the rest.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_message_cut_short_is_handed_over() {
        under_every_runtime(|runtime| async move {
            hand_over_in_the_middle_of(&runtime, &signal(2), 10).await;
        });
    }

    /// As for a message that fits in the buffer of the reader, but for one that doesn't, of which
    /// as much as came in is handed over, with no room for the rest.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_long_message_cut_short_is_handed_over() {
        under_every_runtime(|runtime| async move {
            let long = Message::signal(PATH, INTERFACE, "Long")
                .build(&vec![7u8; 10 * 1024])
                .unwrap();

            let unread = hand_over_in_the_middle_of(&runtime, &long, 6000).await;

            assert!(
                unread.capacity() < long.data().len(),
                "the hand-over keeps room for the rest of the message",
            );
        });
    }

    /// The file descriptors that came in with the part of a message that was read are handed over
    /// with it, in the order that they came in.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn descriptors_that_came_in_are_handed_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer, probe) = served(&runtime).await;
            let pipes = [pipe(), pipe()];
            let msg = Message::signal(PATH, INTERFACE, "WithFds")
                .build(&(crate::Fd::from(&pipes[0]), crate::Fd::from(&pipes[1])))
                .unwrap();
            let fds: Vec<_> = msg.data().fds().iter().map(AsFd::as_fd).collect();

            // The descriptors go with the first bytes of the message, as D-Bus has it.
            send_with_fds(&peer, &msg.data()[..20], &fds);
            wait_until(|| rustix::io::ioctl_fionread(&probe).unwrap() == 0);
            let handover = stream.hand_over().await.unwrap();

            assert_eq!(handover.unread_bytes, &msg.data()[..20]);
            let inodes: Vec<_> = handover.unread_fds.iter().map(inode).collect();
            assert_eq!(inodes, pipes.each_ref().map(inode));
        });
    }

    /// Once a connection is handed over, nothing is written to its socket through it or through
    /// any clone of it, and closing it leaves the socket alone: the peer sees the connection go
    /// only once the socket that was handed over is dropped.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn nothing_is_written_once_the_connection_is_handed_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer, probe) = served(&runtime).await;
            // Only the socket that is handed over is to keep the connection open.
            drop(probe);
            let conn = Connection::from(&stream);
            let clone = conn.clone();

            let handover = stream.hand_over().await.unwrap();

            let msg = signal(1);
            assert_not_connected(conn.send(&msg).await);
            assert_not_connected(clone.send(&msg).await);
            assert_not_connected(clone.send_all(&[msg]).await);
            // The server of a connection has the credentials of its peer from the handshake.
            conn.peer_creds().await.unwrap();
            clone.close().await.unwrap();
            peer.set_nonblocking(true).unwrap();
            let read = (&peer).read(&mut [0; 1]);
            assert!(
                matches!(&read, Err(e) if e.kind() == ErrorKind::WouldBlock),
                "the peer got {read:?}",
            );
            drop(handover);
            let read = (&peer).read(&mut [0; 1]);
            assert!(matches!(read, Ok(0)), "the peer got {read:?}");
        });
    }

    /// A connection over a TCP socket that zbus drives itself, built with `Builder::tcp_stream`,
    /// hands over a new descriptor of the socket as a `TcpStream`.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_tcp_stream_is_handed_over_as_one() {
        under_every_runtime(|runtime| async move {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let ours = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let theirs = listener.accept().unwrap().0;
            let guid = Guid::generate();
            let (mut stream, peer) = futures_util::try_join!(
                on(Builder::tcp_stream(ours).p2p(), &runtime).build_message_stream(),
                on(
                    Builder::tcp_stream(theirs)
                        .server(&guid)
                        .p2p()
                        .auth_mechanism(AuthMechanism::Anonymous),
                    &runtime,
                )
                .build(),
            )
            .unwrap();

            let handover = stream.hand_over().await.unwrap();

            match handover.socket {
                HandoverSocket::TcpStream(socket) => carries_what_is_sent(socket, &peer).await,
                socket => panic!("a TCP socket was handed over as {socket:?}"),
            }
        });
    }

    /// A connection to a `unix:` address hands over a new descriptor of its socket as a
    /// `UnixStream`.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_connection_to_a_unix_address_is_handed_over_as_a_unix_stream() {
        under_every_runtime(|runtime| async move {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("socket");
            let listener = UnixListener::bind(&path).unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = Address::from(Transport::Unix(Unix::new(UnixSocket::File(path))));
            let guid = Guid::generate();
            // The connection connects to the listener as it is built, which may be after this is
            // first polled.
            let serving = async {
                let theirs = loop {
                    match listener.accept() {
                        Ok((theirs, _)) => break theirs,
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            runtime.sleep(Duration::from_millis(1)).await;
                        }
                        Err(e) => panic!("the listener failed: {e}"),
                    }
                };

                on(Builder::unix_stream(theirs).server(&guid).p2p(), &runtime)
                    .build()
                    .await
            };
            let (mut stream, peer) = futures_util::try_join!(
                on(Builder::address(address).p2p(), &runtime).build_message_stream(),
                serving,
            )
            .unwrap();

            let handover = stream.hand_over().await.unwrap();

            match handover.socket {
                HandoverSocket::UnixStream(socket) => carries_what_is_sent(socket, &peer).await,
                socket => panic!("a unix socket was handed over as {socket:?}"),
            }
        });
    }

    /// A send cut off midway keeps a connection from being handed over, as the socket may hold a
    /// part of a message, of which the peer could make no sense. The hand-over finds that out
    /// before it stops the connection reading, so the connection reads on. It still sends, and
    /// closing it shuts the socket down.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_send_cut_off_midway_keeps_the_connection_from_being_handed_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = over_a_socket_pair(&runtime).await;
            let conn = Connection::from(&stream);
            let big = big_signal();
            {
                let sending = pin!(conn.send(&big));
                stuck(&runtime, sending, &peer).await;
            }

            let handed_over = stream.hand_over().await;

            let Err(Error::InputOutput(e)) = &handed_over else {
                panic!("a connection with a torn send was handed over: {handed_over:?}");
            };
            assert_eq!(e.kind(), ErrorKind::InvalidData, "{e}");
            assert!(
                !conn.is_closed(),
                "the refused hand-over closed the connection"
            );
            let reading = MessageStream::for_match_rule(signals(), &conn, None).await;
            assert!(
                reading.is_ok(),
                "the connection stopped reading: {reading:?}"
            );
            let sent = futures_lite::future::poll_once(pin!(conn.send(&signal(1)))).await;
            assert!(
                !matches!(sent, Some(Err(_))),
                "the send was refused: {sent:?}"
            );
            conn.clone().close().await.unwrap();
            // The peer gets what the socket holds, then sees it end.
            let mut received = vec![];
            (&peer).read_to_end(&mut received).unwrap();
            assert!(received.len() < big.data().len());
        });
    }

    /// A hand-over given up on while a send holds the socket leaves the connection stopped: calls
    /// fail before they are sent, and so does a stream for a match rule. The send goes on once the
    /// peer reads, and messages that expect no reply still go out. The messages that the hand-over
    /// took off its stream stay with the stream, so a hand-over through another stream, which
    /// would miss them, fails, while one through the same stream hands the connection over with
    /// them.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_hand_over_given_up_on_goes_on_through_its_stream() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = over_a_socket_pair(&runtime).await;
            let conn = Connection::from(&stream);
            let queued = queue_two(&conn, &peer);
            let big = big_signal();
            let mut sending = pin!(conn.send(&big));
            stuck(&runtime, sending.as_mut(), &peer).await;

            let given_up = runtime
                .timeout(stream.hand_over(), Duration::from_millis(100))
                .await;

            let Err(Error::InputOutput(e)) = &given_up else {
                panic!("the hand-over didn't wait for the send: {given_up:?}");
            };
            assert_eq!(e.kind(), ErrorKind::TimedOut, "{e}");
            assert!(
                !conn.is_closed(),
                "the given up hand-over closed the connection"
            );
            let late = conn
                .call_method(None::<()>, PATH, Some(INTERFACE), "Late", &())
                .await;
            assert_not_connected(late);
            assert_not_connected(MessageStream::for_match_rule(signals(), &conn, None).await);
            assert_not_connected(MessageStream::from(&conn).hand_over().await);
            let len = big.data().len();
            let reading = thread::spawn(move || {
                let mut received = vec![0; len];
                (&peer).read_exact(&mut received).unwrap();

                (received, peer)
            });
            sending.await.unwrap();
            let (received, peer) = reading.join().unwrap();
            assert_eq!(received, &big.data()[..]);
            // The late call never went out, so this is the next message that the peer gets.
            let msg = signal(3);
            conn.send(&msg).await.unwrap();
            let mut received = vec![0; msg.data().len()];
            (&peer).read_exact(&mut received).unwrap();
            assert_eq!(received, &msg.data()[..]);

            let handover = stream.hand_over().await.unwrap();

            assert_eq!(handover.unread_bytes, queued);
            assert!(
                stream.next().await.is_none(),
                "the stream outlived the hand-over"
            );
        });
    }

    /// A hand-over that fails once it stopped the connection reading, as a send under way is cut
    /// off midway, leaves the messages that it took off its stream with the stream, which yields
    /// them, then ends.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_failed_hand_over_leaves_the_messages_on_its_stream() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = over_a_socket_pair(&runtime).await;
            let conn = Connection::from(&stream);
            let queued = queue_two(&conn, &peer);
            let big = big_signal();
            let mut sending = Box::pin(conn.send(&big));
            stuck(&runtime, sending.as_mut(), &peer).await;

            // The hand-over is polled first, so it has stopped the connection reading, and taken
            // the messages off the stream, by the time that the send is cut off.
            let cutting = async { drop(sending) };
            let (handed_over, ()) = futures_util::future::join(stream.hand_over(), cutting).await;

            let Err(Error::InputOutput(e)) = &handed_over else {
                panic!("a connection with a torn send was handed over: {handed_over:?}");
            };
            assert_eq!(e.kind(), ErrorKind::InvalidData, "{e}");
            let mut received = vec![];
            while let Some(msg) = stream.next().await {
                received.extend_from_slice(&msg.unwrap().data()[..]);
            }
            assert_eq!(received, queued);
        });
    }

    /// A send under way as the hand-over starts, which fails midway as the peer goes, fails no
    /// hand-over, any more than one that goes through: the peer receives nothing more, so nothing
    /// follows the part of the message in the socket.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_send_cut_off_midway_as_the_peer_goes_fails_no_hand_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = over_a_socket_pair(&runtime).await;
            let conn = Connection::from(&stream);
            let queued = queue_two(&conn, &peer);
            let big = big_signal();
            let mut sending = pin!(conn.send(&big));
            stuck(&runtime, sending.as_mut(), &peer).await;
            let mut handing_over = pin!(stream.hand_over());
            let waiting = futures_lite::future::poll_once(handing_over.as_mut()).await;
            assert!(waiting.is_none(), "the hand-over didn't wait for the send");
            wait_until(|| !conn.inner.socket_status.stop.reading());

            drop(peer);
            let (handed_over, sent) = futures_util::future::join(handing_over, sending).await;

            // The peer went with part of the message unread, which Linux reports as a reset.
            let Err(Error::InputOutput(e)) = &sent else {
                panic!("the send went through: {sent:?}");
            };
            assert!(
                matches!(e.kind(), ErrorKind::BrokenPipe | ErrorKind::ConnectionReset),
                "{e}"
            );
            assert_eq!(handed_over.unwrap().unread_bytes, queued);
        });
    }

    /// A connection that was closed isn't handed over.
    #[test]
    #[timeout(15000)]
    fn a_closed_connection_is_not_handed_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, _peer) = channel_pair(&runtime).await;
            Connection::from(&stream).close().await.unwrap();

            // The reader of a channel reads on after a close, as long as the peer is there.
            assert_not_connected(stream.hand_over().await);
        });
    }

    /// A connection over a socket that was closed isn't handed over, though its reader came to the
    /// end of the input as the socket was shut down, as it would have had the peer closed its end.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_connection_closed_over_a_socket_is_not_handed_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, _peer) = over_a_socket_pair(&runtime).await;
            let conn = Connection::from(&stream);

            conn.clone().close().await.unwrap();
            wait_until(|| !conn.inner.socket_status.stop.reading());

            {
                let end = conn.inner.socket_status.stop.reader_end.lock().unwrap();
                assert!(matches!(*end, ReaderEnd::Ended(_)), "the reader {end:?}");
            }
            assert_not_connected(stream.hand_over().await);
        });
    }

    /// A connection whose reader the runtime took away before it stopped isn't handed over.
    #[test]
    #[timeout(15000)]
    fn a_connection_whose_reader_was_taken_away_is_not_handed_over() {
        let runtime = AbortingRuntime::new();
        let (ours, theirs) = Channel::pair();

        futures_lite::future::block_on(async {
            let guid = Guid::generate();
            let (mut stream, _peer) = futures_util::try_join!(
                Builder::authenticated_socket(ours, guid.clone())
                    .p2p()
                    .runtime(runtime.clone())
                    .build_message_stream(),
                Builder::authenticated_socket(theirs, guid)
                    .p2p()
                    .runtime(TestRuntime::new())
                    .build(),
            )
            .unwrap();

            runtime.abort_tasks();
            Connection::from(&stream).closed().await;

            assert_not_connected(stream.hand_over().await);
        });
    }

    /// Of two hand-overs of a connection at once, through two streams, one hands it over and the
    /// other finds it handed over already.
    #[test]
    #[timeout(15000)]
    fn a_connection_is_handed_over_once() {
        under_every_runtime(|runtime| async move {
            let (mut stream, _peer) = channel_pair(&runtime).await;
            let conn = Connection::from(&stream);
            let mut other = MessageStream::from(&conn);

            let handovers = futures_util::future::join(stream.hand_over(), other.hand_over()).await;

            let (handover, refused) = match handovers {
                (Ok(handover), refused) | (refused, Ok(handover)) => (handover, refused),
                handovers => panic!("the connection wasn't handed over: {handovers:?}"),
            };
            assert!(
                matches!(handover.socket, HandoverSocket::Socket(_)),
                "a channel was handed over as {handover:?}",
            );
            assert_not_connected(refused);
            // The connection never asked for the credentials of its peer, and can't any more.
            let credentials = conn.peer_creds().await;
            assert!(
                matches!(&credentials, Err(e) if e.kind() == io::ErrorKind::NotConnected),
                "{credentials:?}",
            );
        });
    }

    /// A connection over a channel hands over its halves, through which come the messages that it
    /// is yet to read, while its peer goes on sending.
    #[test]
    #[timeout(15000)]
    fn a_channel_is_handed_over_with_the_messages_that_it_is_yet_to_read() {
        under_every_runtime(|runtime| async move {
            // However many of the messages the stream yielded before the hand-over.
            for taken in [0, 7, 50] {
                let (mut stream, peer) = channel_pair(&runtime).await;
                let msgs: Vec<_> = (0..50).map(signal).collect();

                let sending = async {
                    for msg in &msgs {
                        peer.send(msg).await.unwrap();
                    }
                };
                let receiving = async {
                    let mut received = vec![];
                    for _ in 0..taken {
                        received.push(stream.next().await.unwrap().unwrap());
                    }
                    let handover = stream.hand_over().await.unwrap();
                    #[cfg(unix)]
                    assert!(handover.unread_fds.is_empty());
                    let mut split = match handover.socket {
                        HandoverSocket::Socket(split) => split,
                        socket => panic!("a channel was handed over as {socket:?}"),
                    };
                    // The messages that the stream had queued.
                    received.extend(messages_of(&handover.unread_bytes));
                    // The peer is still sending the rest.
                    while received.len() < msgs.len() {
                        let msg = split
                            .read_mut()
                            .receive_message(
                                0,
                                &mut vec![],
                                #[cfg(unix)]
                                &mut vec![],
                            )
                            .await
                            .unwrap();
                        received.push(msg);
                    }

                    received
                };
                let ((), received) = futures_util::future::join(sending, receiving).await;

                let numbers: Vec<u32> = received
                    .iter()
                    .map(|msg| msg.body().deserialize().unwrap())
                    .collect();
                assert_eq!(numbers, (0..50).collect::<Vec<_>>(), "taken: {taken}");
            }
        });
    }

    /// Hands over a connection that yielded a whole message, and read `cut` up to `at`, and
    /// checks what it handed over. Gives the bytes that it handed over.
    #[cfg(unix)]
    async fn hand_over_in_the_middle_of(runtime: &Runtime, cut: &Message, at: usize) -> Vec<u8> {
        let (mut stream, peer, probe) = served(runtime).await;
        let conn = Connection::from(&stream);
        let (whole, last) = (signal(1), signal(3));

        let mut sent = whole.data().to_vec();
        sent.extend_from_slice(&cut.data()[..at]);
        (&peer).write_all(&sent).unwrap();
        let received = stream.next().await.unwrap().unwrap();
        assert_eq!(&received.data()[..], &whole.data()[..]);
        // The reader waits for the rest of the message.
        wait_until(|| rustix::io::ioctl_fionread(&probe).unwrap() == 0);

        let handover = stream.hand_over().await.unwrap();

        assert!(
            conn.is_closed(),
            "the connection is open after its hand-over"
        );
        assert_eq!(handover.unread_bytes, &cut.data()[..at]);
        assert!(handover.unread_fds.is_empty());
        assert_eq!(&handover.server_guid, conn.server_guid());
        assert_eq!(handover.unique_name.as_ref(), conn.unique_name());
        assert!(handover.unix_fd_passing);
        assert!(handover.p2p);
        assert!(handover.next_serial > last.primary_header().serial_num());
        let socket = match handover.socket {
            HandoverSocket::UnixStream(socket) => socket,
            socket => panic!("a unix socket was handed over as {socket:?}"),
        };
        // The peer goes on where it was, and the socket that was handed over carries the rest.
        let mut rest = cut.data()[at..].to_vec();
        rest.extend_from_slice(&last.data()[..]);
        (&peer).write_all(&rest).unwrap();
        // It is non-blocking as the connection left it.
        socket.set_nonblocking(false).unwrap();
        let mut received = vec![0; rest.len()];
        (&socket).read_exact(&mut received).unwrap();
        assert_eq!(received, rest);

        handover.unread_bytes
    }

    /// The halves of `socket`, registered on `runtime`, of which the read half only receives
    /// bytes.
    #[cfg(unix)]
    fn recvmsg_only(runtime: &Runtime, socket: UnixStream) -> BoxedSplit {
        let socket = Arc::new(registered(runtime, socket, UnixOps).unwrap());
        let read = RecvmsgOnly {
            socket: socket.clone(),
        };

        Split::new(Box::new(read), Box::new(socket) as Box<dyn WriteHalf>)
    }

    /// A read half that only receives bytes, and leaves everything else to the defaults of
    /// `ReadHalf`.
    #[cfg(unix)]
    #[derive(Debug)]
    struct RecvmsgOnly {
        socket: Arc<RegisteredIo<UnixOps>>,
    }

    #[cfg(unix)]
    #[async_trait::async_trait]
    impl ReadHalf for RecvmsgOnly {
        async fn recvmsg(&mut self, buffer: &mut [u8]) -> RecvmsgResult {
            self.socket.recv(buffer).await
        }
    }

    /// The stream of the server end of a peer-to-peer connection over a unix socket pair, on
    /// `runtime`, and the other end of the pair, through which the test plays the client. Also a
    /// descriptor of the server's end of the pair, which tells how much of what the client sent
    /// the connection is yet to read.
    #[cfg(unix)]
    async fn served(runtime: &Runtime) -> (MessageStream, UnixStream, UnixStream) {
        let (client, server) = UnixStream::pair().unwrap();
        let probe = server.try_clone().unwrap();
        // The client's side of the handshake, all at once, which the server takes as it comes.
        let id = hex::encode(rustix::process::geteuid().as_raw().to_string());
        let commands = format!("\0AUTH EXTERNAL {id}\r\nNEGOTIATE_UNIX_FD\r\nBEGIN\r\n");
        (&client).write_all(commands.as_bytes()).unwrap();
        let guid = Guid::generate();

        let stream = on(Builder::unix_stream(server).server(&guid).p2p(), runtime)
            .build_message_stream()
            .await
            .unwrap();

        let answer = format!("OK {guid}\r\nAGREE_UNIX_FD\r\n");
        let mut answered = vec![0; answer.len()];
        (&client).read_exact(&mut answered).unwrap();
        assert_eq!(answered, answer.as_bytes());

        (stream, client, probe)
    }

    /// The stream of a connection over one end of a unix socket pair, on `runtime`, with no
    /// handshake, and the other end of the pair, through which the test plays its peer.
    #[cfg(unix)]
    async fn over_a_socket_pair(runtime: &Runtime) -> (MessageStream, UnixStream) {
        let (peer, theirs) = UnixStream::pair().unwrap();
        let socket = registered(runtime, theirs, UnixOps).unwrap();
        let stream = on(
            Builder::authenticated_socket(socket, Guid::generate()),
            runtime,
        )
        .p2p()
        .build_message_stream()
        .await
        .unwrap();

        (stream, peer)
    }

    /// Has `peer` send two signals to the connection of `conn`, and waits for them to be queued
    /// on its streams. Gives their bytes.
    #[cfg(unix)]
    fn queue_two(conn: &Connection, peer: &UnixStream) -> Vec<u8> {
        let queued: Vec<u8> = [signal(1), signal(2)]
            .iter()
            .flat_map(|msg| msg.data().to_vec())
            .collect();
        (&*peer).write_all(&queued).unwrap();
        wait_until(|| conn.inner.msg_receiver.len() == 2);

        queued
    }

    /// Polls `sending` until it wrote part of its message to the socket of `peer`, which doesn't
    /// read, and waits for room for the rest.
    #[cfg(unix)]
    async fn stuck<F>(runtime: &Runtime, mut sending: Pin<&mut F>, peer: &UnixStream)
    where
        F: Future<Output = Result<()>>,
    {
        while rustix::io::ioctl_fionread(peer).unwrap() == 0 {
            let sent = futures_lite::future::poll_once(sending.as_mut()).await;
            assert!(
                sent.is_none(),
                "the socket took the whole message: {sent:?}"
            );
            // For the runtime to see the socket writable, if it waits for that first.
            runtime.sleep(Duration::from_millis(1)).await;
        }
    }

    /// Writes `bytes` to `socket`, with `fds` along with them.
    #[cfg(unix)]
    fn send_with_fds(socket: &UnixStream, bytes: &[u8], fds: &[BorrowedFd<'_>]) {
        use rustix::net::{SendAncillaryBuffer, SendAncillaryMessage, SendFlags, sendmsg};

        let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(2))];
        let mut ancillary = SendAncillaryBuffer::new(&mut space);
        assert!(ancillary.push(SendAncillaryMessage::ScmRights(fds)));
        let sent = sendmsg(
            socket,
            &[IoSlice::new(bytes)],
            &mut ancillary,
            SendFlags::empty(),
        )
        .unwrap();
        assert_eq!(sent, bytes.len());
    }

    /// The read end of a pipe of its own.
    #[cfg(unix)]
    fn pipe() -> OwnedFd {
        io::pipe().unwrap().0.into()
    }

    /// The inode of the file that `fd` is open on.
    #[cfg(unix)]
    fn inode(fd: &impl AsFd) -> u64 {
        use std::os::unix::fs::MetadataExt;

        std::fs::File::from(fd.as_fd().try_clone_to_owned().unwrap())
            .metadata()
            .unwrap()
            .ino()
    }

    /// Checks that `socket`, which a connection to `peer` handed over, is in non-blocking mode, as
    /// the connection left it, and gets what `peer` sends.
    #[cfg(unix)]
    async fn carries_what_is_sent<S>(socket: S, peer: &Connection)
    where
        S: AsFd,
        for<'s> &'s S: Read,
    {
        let read = (&socket).read(&mut [0; 1]);
        assert!(
            matches!(&read, Err(e) if e.kind() == ErrorKind::WouldBlock),
            "the read didn't find the socket empty: {read:?}",
        );
        let msg = signal(1);
        peer.send(&msg).await.unwrap();
        rustix::io::ioctl_fionbio(&socket, false).unwrap();
        let mut received = vec![0; msg.data().len()];
        (&socket).read_exact(&mut received).unwrap();
        assert_eq!(received, &msg.data()[..]);
    }

    /// The messages that `bytes` hold, one after the other.
    fn messages_of(mut bytes: &[u8]) -> Vec<Message> {
        let mut msgs = vec![];
        while !bytes.is_empty() {
            let (primary_header, len) = read_primary_header(bytes).unwrap();
            let ctxt = Context::new(Endian::from(primary_header.endian_sig()), 0);
            let data = Data::new(bytes[..len].to_vec(), ctxt);
            msgs.push(Message::from_raw_parts(data, 0).unwrap());
            bytes = &bytes[len..];
        }

        msgs
    }

    /// The stream of a connection and its peer, over a channel, both on `runtime`.
    async fn channel_pair(runtime: &Runtime) -> (MessageStream, Connection) {
        let guid = Guid::generate();
        let (ours, theirs) = Channel::pair();

        futures_util::try_join!(
            on(Builder::authenticated_socket(ours, guid.clone()), runtime)
                .p2p()
                .build_message_stream(),
            on(Builder::authenticated_socket(theirs, guid), runtime)
                .p2p()
                .build(),
        )
        .unwrap()
    }

    /// `builder`, to build its connection on `runtime`.
    fn on<'a>(builder: Builder<'a>, runtime: &Runtime) -> Builder<'a> {
        match runtime {
            // A builder that is given no runtime takes the one that the test runs on: the one of
            // zruntime for this thread, or the Tokio runtime that the test is polled in.
            #[cfg(feature = "default-rt")]
            Runtime::ZRuntime(_) => builder,
            #[cfg(feature = "tokio")]
            Runtime::Tokio(_) => builder,
            Runtime::External(runtime) => builder.runtime(runtime.clone()),
        }
    }

    /// Asserts that `result` is the error of a connection that stopped reading, was closed or was
    /// handed over.
    fn assert_not_connected<T: Debug>(result: Result<T>) {
        let Err(Error::InputOutput(e)) = &result else {
            panic!("expected the error of a connection that is not connected: {result:?}");
        };
        assert_eq!(e.kind(), io::ErrorKind::NotConnected, "{e}");
    }

    /// Asserts that `result` is the error of a stream that can't be made, as the reader of the
    /// connection ended, or of a send to a peer that went.
    fn assert_broken_pipe<T: Debug>(result: Result<T>) {
        let Err(Error::InputOutput(e)) = &result else {
            panic!("expected the error of a broken pipe: {result:?}");
        };
        assert_eq!(e.kind(), io::ErrorKind::BrokenPipe, "{e}");
    }

    /// Asserts that `result` is the error of a connection that came to the end of the input, as
    /// its peer closed its end.
    fn assert_end_of_input<T: Debug>(result: Result<T>) {
        let Err(Error::InputOutput(e)) = &result else {
            panic!("expected the error of the end of the input: {result:?}");
        };
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof, "{e}");
    }

    /// A match rule for every signal.
    fn signals() -> MatchRule<'static> {
        MatchRule::builder().msg_type(Type::Signal).build().unwrap()
    }

    /// A signal that carries `n`.
    fn signal(n: u32) -> Message {
        Message::signal(PATH, INTERFACE, "Test").build(&n).unwrap()
    }

    /// A signal longer than the buffer of a socket holds.
    #[cfg(unix)]
    fn big_signal() -> Message {
        Message::signal(PATH, INTERFACE, "Big")
            .build(&vec![0u8; 1024 * 1024])
            .unwrap()
    }

    /// Waits for `condition` to hold, which a task on another thread is to bring about.
    #[cfg(unix)]
    fn wait_until(mut condition: impl FnMut() -> bool) {
        while !condition() {
            thread::sleep(Duration::from_millis(1));
        }
    }

    const PATH: &str = "/org/zbus/Test";
    const INTERFACE: &str = "org.zbus.Test";
}
