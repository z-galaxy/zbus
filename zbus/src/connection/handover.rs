//! Handing a connection over, for a connection in this process or in another one to go on from
//! where it stopped, as a service or a bus that restarts does.
//!
//! This is behind the `handover` cargo feature, which is off by default, and only on unix.
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
//! 4. To pass the hand-over to another process, or to keep it while this one restarts, such as in
//!    the file descriptor store of systemd, turn it into bytes and file descriptors with
//!    [`Handover::encode`], and back with [`Handover::decode`].
//! 5. Build the connection that goes on, in this process or in another one that the hand-over is
//!    passed to, with [`Builder::take_over`], and its stream with `build_message_stream`. It yields
//!    the messages of the hand-over first, then goes on reading the socket where the other one
//!    stopped, and it can be handed over in turn.
//!
//! Nothing that the peer sent is lost on the way, and nothing is written to the socket from the
//! commit of the hand-over until the connection that goes on sends, so the peer sees nothing but a
//! pause. A peer that closed its end of the connection, such as one that sent a last message and
//! went, is no exception, even once a send to it failed: what it sent is handed over, and the
//! connection that goes on comes to its end. For that, the connection keeps what it read of such a
//! peer and made no message of, the file descriptors that came with it included, until it is handed
//! over or dropped.
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
//! use zbus::{
//!     Connection, block_on,
//!     connection::{Builder, Handover},
//! };
//!
//! # fn main() -> zbus::Result<()> {
//! block_on(async {
//!     let mut stream = Builder::session().build_message_stream().await?;
//!     let name = Connection::from(&stream).unique_name().cloned();
//!     // Take the messages of the connection off `stream` and deal with them, until it is time to
//!     // hand the connection over.
//!
//!     let handover = stream.hand_over().await?;
//!     let (bytes, fds) = handover.encode()?;
//!
//!     // Pass `bytes` and `fds` on, such as to the file descriptor store of systemd, and get them
//!     // back, such as in the next instance of the service.
//!     let handover = Handover::decode(&bytes, fds)?;
//!
//!     // The connection goes on, with the name that the bus knows it by.
//!     let stream = Builder::take_over(handover).build_message_stream().await?;
//!     let conn = Connection::from(&stream);
//!     assert_eq!(conn.unique_name().cloned(), name);
//!     conn.call_method(
//!         Some("org.freedesktop.DBus"),
//!         "/org/freedesktop/DBus",
//!         Some("org.freedesktop.DBus.Peer"),
//!         "Ping",
//!         &(),
//!     )
//!     .await?;
//!
//!     Ok(())
//! })
//! # }
//! ```
//!
//! [`Builder::build_message_stream`]: super::Builder::build_message_stream
//! [`Builder::take_over`]: super::Builder::take_over
//! [`MessageStream`]: crate::MessageStream
//! [`MessageStream::hand_over`]: crate::MessageStream::hand_over

use std::{
    collections::VecDeque,
    fmt,
    future::{Future, poll_fn},
    io, mem,
    net::TcpStream,
    num::NonZeroU32,
    os::{
        fd::{AsFd, OwnedFd},
        unix::net::UnixStream,
    },
    pin::{Pin, pin},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

use futures_core::Stream;
use serde::{Serialize, Serializer};
use zruntime::{
    Event, EventListener,
    broadcast::{Receiver, TryRecvError},
};

use crate::{
    Connection, Error, Guid, Message, OwnedGuid, Result,
    connection::socket::{BoxedSplit, ReadHalf, Split},
    message,
    names::OwnedUniqueName,
    runtime::IoSource,
    wire::{
        self, LE, Signature, Type,
        serialized::{Context, Data},
    },
};

/// A connection taken apart by [`MessageStream::hand_over`], for [`Builder::take_over`] to build a
/// connection that goes on from where it stopped.
///
/// The connection that goes on can be in this process. It can also be in another process that the
/// hand-over is passed to, such as the next instance of a service that restarts, or a helper
/// process.
///
/// This type is only available on unix, with the `handover` feature.
///
/// # Passing it to another process
///
/// [`Handover::encode`] turns it into bytes and file descriptors, which can be passed on over a
/// unix socket or kept in the file descriptor store of systemd, and [`Handover::decode`] turns
/// those back into a hand-over.
///
/// `unread_bytes` holds the messages that were queued on the stream, and the start of one that the
/// peer was in the middle of sending, which can be as long as the longest message that D-Bus
/// allows (128 MiB).
///
/// [`Builder::take_over`]: super::Builder::take_over
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
    pub unread_fds: Vec<OwnedFd>,
    /// Whether the two ends of the connection agreed to pass file descriptors.
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

impl Handover {
    /// A hand-over of a connection over `socket` to the server `server_guid`.
    ///
    /// The hand-over is of a connection to a bus, with no unique name, nothing read ahead and no
    /// passing of file descriptors, and its serial numbers start at 1. Set the fields to those of
    /// the connection that was handed over.
    pub fn new(socket: HandoverSocket, server_guid: OwnedGuid) -> Self {
        Self {
            socket,
            server_guid,
            unique_name: None,
            unread_bytes: Vec::new(),
            unread_fds: Vec::new(),
            unix_fd_passing: false,
            #[cfg(feature = "p2p")]
            p2p: false,
            next_serial: NonZeroU32::MIN,
        }
    }

    /// Turn the hand-over into bytes and file descriptors, to pass it to another process, such as
    /// over a unix socket, or to keep it while this one restarts, such as in the file descriptor
    /// store of systemd.
    ///
    /// The file descriptors are the socket's, followed by those of `unread_fds`, in their order.
    /// Pass them all on, in this order, along with the bytes, and turn them back into a hand-over
    /// with [`Handover::decode`].
    ///
    /// The bytes are a format version, in a byte of its own, followed by the other fields in the
    /// D-Bus wire format, little-endian. The format is kept stable: a later version of zbus decodes
    /// the bytes that this one encodes, as a service that restarts after an update needs. A version
    /// of zbus that doesn't know the format version of the bytes, as an earlier one may not,
    /// refuses them.
    ///
    /// # Errors
    ///
    /// * [`Error::Unsupported`] for the hand-over of a connection over a socket of your own
    ///   ([`HandoverSocket::Socket`]), which has no descriptor to pass on.
    /// * An I/O error ([`Error::InputOutput`]) of kind `InvalidInput` for more unread bytes than
    ///   the wire format can hold (4 GiB).
    ///
    /// As this takes the hand-over, a failure drops it, which closes its socket. Check its socket
    /// first if it may be a socket of your own.
    pub fn encode(self) -> Result<(Vec<u8>, Vec<OwnedFd>)> {
        let socket = match self.socket {
            HandoverSocket::UnixStream(stream) => (ENCODED_UNIX_STREAM, OwnedFd::from(stream)),
            HandoverSocket::TcpStream(stream) => (ENCODED_TCP_STREAM, OwnedFd::from(stream)),
            HandoverSocket::Socket(_) => return Err(Error::Unsupported),
        };
        let (kind, socket) = socket;
        if u32::try_from(self.unread_bytes.len()).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the unread bytes are too many for the wire format",
            )
            .into());
        }
        let fields = (
            kind,
            self.server_guid.as_str(),
            self.unique_name.as_deref().map_or("", |name| name.as_str()),
            Bytes(&self.unread_bytes),
            self.unread_fds.len() as u32,
            self.unix_fd_passing,
            // Encoded whatever the features of this build, so that every build decodes it.
            #[cfg(feature = "p2p")]
            self.p2p,
            #[cfg(not(feature = "p2p"))]
            false,
            self.next_serial.get(),
        );
        let encoded = wire::to_bytes(Context::new(LE, 0), &fields)?;
        let mut bytes = Vec::with_capacity(1 + encoded.len());
        bytes.push(ENCODING_VERSION);
        bytes.extend_from_slice(&encoded);
        let mut fds = Vec::with_capacity(1 + self.unread_fds.len());
        fds.push(socket);
        fds.extend(self.unread_fds);

        Ok((bytes, fds))
    }

    /// Turn `bytes` and `fds`, which [`Handover::encode`] gave, back into a hand-over, in this
    /// process or in another one.
    ///
    /// # Errors
    ///
    /// * [`Error::Unsupported`] for bytes of a format version that this version of zbus doesn't
    ///   know, as one that a later version encoded may be, and for the hand-over of a peer-to-peer
    ///   connection in a build without the `p2p` feature.
    /// * An I/O error ([`Error::InputOutput`]) of kind `InvalidData` for bytes that are not what
    ///   `encode` gives, or for a number of file descriptors other than the one that they count.
    ///
    /// The file descriptors are closed on an error.
    pub fn decode(bytes: &[u8], mut fds: Vec<OwnedFd>) -> Result<Handover> {
        let (version, encoded) = bytes.split_first().ok_or_else(malformed)?;
        if *version != ENCODING_VERSION {
            return Err(Error::Unsupported);
        }
        let encoded = Data::new(encoded, Context::new(LE, 0));
        let (fields, len) = encoded
            .deserialize::<(u8, &str, &str, &[u8], u32, bool, bool, u32)>()
            .map_err(|_| malformed())?;
        let (kind, server_guid, unique_name, unread_bytes, fd_count, unix_fd_passing, p2p, next) =
            fields;
        // The socket's descriptor comes first. Counted without adding to `fd_count`, which can
        // overflow on a 32-bit target.
        if len != encoded.len() || fds.len().checked_sub(1) != Some(fd_count as usize) {
            return Err(malformed());
        }
        let server_guid = Guid::try_from(server_guid).map_err(|_| malformed())?;
        let unique_name = match unique_name {
            "" => None,
            name => Some(OwnedUniqueName::try_from(name).map_err(|_| malformed())?),
        };
        let next_serial = NonZeroU32::new(next).ok_or_else(malformed)?;
        #[cfg(not(feature = "p2p"))]
        if p2p {
            return Err(Error::Unsupported);
        }
        let socket = fds.remove(0);
        let socket = match kind {
            ENCODED_UNIX_STREAM => HandoverSocket::UnixStream(UnixStream::from(socket)),
            ENCODED_TCP_STREAM => HandoverSocket::TcpStream(TcpStream::from(socket)),
            _ => return Err(malformed()),
        };

        Ok(Handover {
            socket,
            server_guid: server_guid.to_owned().into(),
            unique_name,
            unread_bytes: unread_bytes.to_vec(),
            unread_fds: fds,
            unix_fd_passing,
            #[cfg(feature = "p2p")]
            p2p,
            next_serial,
        })
    }
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
        handover
            .field("unread_fds", &self.unread_fds.len())
            .field("unix_fd_passing", &self.unix_fd_passing);
        #[cfg(feature = "p2p")]
        handover.field("p2p", &self.p2p);

        handover.field("next_serial", &self.next_serial).finish()
    }
}

/// The socket of a connection that was handed over ([`MessageStream::hand_over`]), for
/// [`Builder::take_over`] to go on over.
///
/// This type is only available on unix, with the `handover` feature.
///
/// The first two variants hold the only descriptor of the socket that zbus has kept. Dropping one
/// of them closes the socket, and the peer sees the connection go, unless the descriptor was
/// passed on first.
///
/// [`Builder::take_over`]: super::Builder::take_over
/// [`MessageStream::hand_over`]: crate::MessageStream::hand_over
#[derive(Debug)]
#[non_exhaustive]
pub enum HandoverSocket {
    /// A unix-domain stream.
    ///
    /// This is for a connection over a unix socket that zbus drives itself: one built with
    /// [`Builder::unix_stream`], or one to a `unix:` address, such as that of the session or
    /// system bus. The hand-over gives a new descriptor of the socket, which is close-on-exec, as
    /// std makes them. The socket is in non-blocking mode, as zbus switched it to that, so a read
    /// that finds nothing to read fails with an error of kind `WouldBlock` rather than wait. Call
    /// `set_nonblocking(false)` on the stream to read it with calls that wait.
    ///
    /// To pass it to another process, pass the descriptor on, or clear its close-on-exec flag for
    /// an `exec` to keep it.
    ///
    /// [`Builder::unix_stream`]: super::Builder::unix_stream
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
    /// address.
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

    /// Fail if a send that was cut off midway left part of a message in the socket, as `take_apart`
    /// then does, but before the connection stops reading, so that it reads on. A send under way
    /// holds the socket, and only `take_apart` finds out how it went, once it is through.
    async fn ensure_whole(&self) -> Result<()> {
        let mut lock = pin!(self.inner.socket_write.lock());
        let write = poll_fn(|cx| Poll::Ready(lock.as_mut().poll(cx))).await;
        if matches!(write, Poll::Ready(write) if write.is_torn()) {
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
        {
            let end = stop
                .reader_end
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if matches!(*end, ReaderEnd::HandedOver) {
                return Err(not_connected("the connection was handed over already").into());
            }
            // Set before the socket is shut down, under the lock that is held here, so a closed
            // connection whose reader then came to the end of the input is not taken for one
            // whose peer closed its end.
            if write.is_shut_down() {
                return Err(not_connected("the connection was closed").into());
            }
            if !matches!(*end, ReaderEnd::Stopped(_) | ReaderEnd::Ended(_)) {
                return Err(not_connected("the connection ended before it stopped reading").into());
            }
            if write.is_torn() {
                return Err(torn());
            }
        }
        // What the connection has left to write goes out first: what is left of a message that a
        // send was dropped, or failed, in the middle of, as the peer could make no sense of what
        // the connection that goes on sends after part of a message, and the messages that sends
        // kept once the hand-over was prepared, which the connection that goes on knows nothing
        // of. The lock on how the reader ended is let go of meanwhile, as no wait is to hold it.
        // Once the reader stopped or ended, only a hand-over changes that, under the lock that is
        // held here, so it holds all the same once this is through.
        write.write_unwritten_for_hand_over().await?;
        let mut end = stop
            .reader_end
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // The last steps that can fail, so that a failure commits nothing. The messages may be
        // queued for other streams too, so their descriptors are duplicated rather than taken.
        let mut unread_fds = taken
            .iter()
            .flatten()
            .flat_map(|msg| msg.data().fds())
            .map(|fd| fd.as_fd().try_clone_to_owned())
            .collect::<io::Result<Vec<_>>>()?;
        let duplicate = write
            .source()
            .map(|(source, kind)| dup(source).map(|socket| (socket, *kind)))
            .transpose()?;

        // Nothing fails or waits from here on, so the connection is either handed over whole, or
        // not at all.
        let (ReaderEnd::Stopped(parts) | ReaderEnd::Ended(parts)) =
            mem::replace(&mut *end, ReaderEnd::HandedOver)
        else {
            unreachable!("the reader stopped or ended, as the hand-over found under its lock");
        };
        drop(end);
        stop.reader_end_event.notify(usize::MAX);
        let (write_half, writes) = write
            .take_half()
            .expect("the write half is only taken by a hand-over, which there was none of");
        inner.socket_status.closed.store(true, Ordering::Release);
        inner.socket_status.closed_event.notify(usize::MAX);
        drop(guard);

        let ReadParts {
            half: read_half,
            unread_bytes: read,
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
        unread_fds.extend(read_fds);
        let socket = match duplicate {
            Some((socket, kind)) => {
                // The runtime stops watching the socket as the halves, and the handle that the
                // connection wrote it directly through, go, while their descriptor of it is still
                // open, and then that descriptor is closed.
                drop(read_half);
                drop(write_half);
                drop(writes);

                kind.stream(socket)
            }
            // A connection that goes on over the halves writes the socket as this one did.
            None => HandoverSocket::Socket(Split::with_writes(read_half, write_half, writes)),
        };

        Ok(Handover {
            socket,
            server_guid: inner.server_guid.clone(),
            unique_name: self.unique_name().cloned(),
            unread_bytes,
            unread_fds,
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

/// The kind of a socket that zbus drives itself, and so can give a descriptor of for a hand-over.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SocketKind {
    Unix,
    Tcp,
}

impl SocketKind {
    /// The stream of this kind over `socket`.
    fn stream(self, socket: OwnedFd) -> HandoverSocket {
        match self {
            Self::Unix => HandoverSocket::UnixStream(UnixStream::from(socket)),
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
        mut unread_fds: Vec<OwnedFd>,
    ) -> Self {
        half.take_unread(&mut unread_bytes, &mut unread_fds);

        Self {
            half,
            unread_bytes,
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
fn dup(source: &IoSource) -> io::Result<OwnedFd> {
    source.as_fd().try_clone_to_owned()
}

/// Bytes that the wire format takes in one go, rather than one byte at a time, as an array of
/// bytes.
struct Bytes<'b>(&'b [u8]);

impl Serialize for Bytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

impl Type for Bytes<'_> {
    const SIGNATURE: &'static Signature = <[u8]>::SIGNATURE;
}

/// The error of bytes or file descriptors that [`Handover::decode`] can make no hand-over of.
fn malformed() -> Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "not the bytes and file descriptors of a hand-over",
    )
    .into()
}

/// The version of the format that [`Handover::encode`] gives, which only ever goes up, for a format
/// that a version of zbus that knows it decodes.
const ENCODING_VERSION: u8 = 1;
/// How [`Handover::encode`] gives a [`HandoverSocket::UnixStream`].
const ENCODED_UNIX_STREAM: u8 = 0;
/// How [`Handover::encode`] gives a [`HandoverSocket::TcpStream`].
const ENCODED_TCP_STREAM: u8 = 1;

/// The error of a hand-over of a connection whose socket may hold part of a message.
fn torn() -> Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "a send was cut off midway: the socket may hold part of a message",
    )
    .into()
}

/// The error, for `reason`, of what a connection can no longer do, or of a hand-over that the
/// connection is not in a state for.
fn not_connected(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, reason)
}

#[cfg(all(test, feature = "p2p"))]
mod tests {
    use std::{
        fmt::Debug,
        future::Future,
        io,
        io::{ErrorKind, IoSlice, Read, Write},
        mem::MaybeUninit,
        net::{Ipv4Addr, Shutdown, TcpListener, TcpStream},
        num::{NonZeroU32, NonZeroUsize},
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

    use super::{Handover, HandoverSocket, ReaderEnd};
    use crate::{
        Address, Connection, Error, Guid, MatchRule, Message, MessageStream, Result,
        address::transport::{Transport, Unix, UnixSocket},
        connection::{
            AuthMechanism, Builder,
            socket::{BoxedSplit, Channel, ReadHalf, RecvmsgResult, Split, WriteHalf},
        },
        message::{Type, header::next_serial},
        runtime::{
            Runtime,
            io::{Reader, RegisteredIo, UnixOps, registered},
            test_runtime::{AbortingRuntime, TestRuntime, under_every_runtime},
        },
        wire::{self, LE, serialized::Context},
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
    /// the hand-over. Its streams end on that end then, and so do the calls made since. A
    /// connection that takes over yields the message, then comes to the same end.
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
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            let received = stream.next().await.unwrap().unwrap();
            assert_eq!(&received.data()[..], &msg.data()[..]);
            assert_end_of_input(stream.next().await.unwrap());
            assert!(stream.next().await.is_none(), "the stream outlived its end");
        });
    }

    /// A connection whose peer closed its end of the socket is handed over with what it read, the
    /// part of a message included, and the socket, which holds the rest up to that end, whether
    /// the connection came to the end before the hand-over or as the hand-over stopped it, and
    /// whatever the peer sent before. A connection that takes over yields the message that the
    /// peer sent whole, then comes to the same end.
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
                    assert!(
                        matches!(handover.socket, HandoverSocket::UnixStream(_)),
                        "{case}: a unix socket was handed over as {:?}",
                        handover.socket,
                    );
                    // What the connection didn't read is still in the socket, up to its end.
                    assert!(sent.starts_with(&handover.unread_bytes), "{case}");
                    let mut stream = on(Builder::take_over(handover), &runtime)
                        .build_message_stream()
                        .await
                        .unwrap();
                    if whole {
                        let received = stream.next().await.unwrap().unwrap();
                        assert_eq!(&received.data()[..], &last.data()[..], "{case}");
                    }
                    assert_end_of_input(stream.next().await.unwrap());
                    assert!(stream.next().await.is_none(), "{case}");
                }
            }
        });
    }

    /// A connection whose peer went, once it sent a last message, is handed over with that
    /// message and the socket, though a send to the peer failed since: the peer receives nothing
    /// more, so nothing follows what the send may have left in the socket. A connection that takes
    /// over yields the message, then comes to the same end.
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
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            let received = stream.next().await.unwrap().unwrap();
            assert_eq!(&received.data()[..], &last.data()[..]);
            assert_end_of_input(stream.next().await.unwrap());
            assert!(stream.next().await.is_none(), "the stream outlived its end");
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
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            let received = stream.next().await.unwrap().unwrap();
            assert_eq!(&received.data()[..], &last.data()[..]);
            assert_end_of_input(stream.next().await.unwrap());
            assert!(stream.next().await.is_none(), "the stream outlived its end");
        });
    }

    /// A connection over a read half that can't take out what it read isn't handed over, and
    /// reads on as if no hand-over had been asked for.
    #[test]
    #[timeout(15000)]
    fn a_connection_that_cant_stop_reading_reads_on() {
        under_every_runtime(|runtime| async move {
            let (peer, theirs) = UnixStream::pair().unwrap();
            let split = recvmsg_only(&runtime, theirs, false);
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
    /// those that the reader read after them too, and nothing is written to the socket. A
    /// connection that takes over yields them first.
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
                // Only a stream set up before the connection is built is sure to get the messages
                // that it takes over whole.
                let mut stream = on(Builder::take_over(handover), &runtime)
                    .build_message_stream()
                    .await
                    .unwrap();
                for sent in &msgs[taken..] {
                    let received = stream.next().await.unwrap().unwrap();
                    assert_eq!(&received.data()[..], &sent.data()[..]);
                }
            }
        });
    }

    /// The messages queued on the stream, which it didn't yield, are handed over with their file
    /// descriptors, in their order, ahead of the part of a message that the reader read, and of
    /// the descriptors that came with that. A connection that takes over yields each message with
    /// its own descriptors.
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
            drop(conn);
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            (&peer).write_all(&msgs[2].data()[20..]).unwrap();
            for sent in &msgs {
                let received = stream.next().await.unwrap().unwrap();
                assert_eq!(&received.data()[..], &sent.data()[..]);
                let inodes: Vec<_> = received.data().fds().iter().map(inode).collect();
                let sent: Vec<_> = sent.data().fds().iter().map(inode).collect();
                assert_eq!(inodes, sent);
            }
        });
    }

    /// A connection that stopped in the middle of a message hands over its socket with the part
    /// of the message that it read, and a connection that takes over goes on with that message.
    #[test]
    #[timeout(15000)]
    fn a_message_cut_short_is_handed_over() {
        under_every_runtime(|runtime| async move {
            hand_over_in_the_middle_of(&runtime, &signal(2), 10).await;
        });
    }

    /// As for a message that fits in the buffer of the reader, but for one that doesn't, of which
    /// as much as came in is handed over, with no room for the rest.
    #[test]
    #[timeout(15000)]
    fn a_long_message_cut_short_is_handed_over() {
        under_every_runtime(|runtime| async move {
            let long = Message::signal(PATH, INTERFACE, "Long")
                .build(&vec![7u8; 10 * 1024])
                .unwrap();

            let room = hand_over_in_the_middle_of(&runtime, &long, 6000).await;

            assert!(
                room < long.data().len(),
                "the hand-over keeps room for the rest of the message",
            );
        });
    }

    /// The file descriptors that came in with the part of a message that was read are handed over
    /// with it, in the order that they came in, and the message carries them once a connection
    /// that takes over has the rest of it.
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
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            (&peer).write_all(&msg.data()[20..]).unwrap();
            let received = stream.next().await.unwrap().unwrap();
            assert_eq!(&received.data()[..], &msg.data()[..]);
            let inodes: Vec<_> = received.data().fds().iter().map(inode).collect();
            assert_eq!(inodes, pipes.each_ref().map(inode));
        });
    }

    /// Once a connection is handed over, nothing is written to its socket through it or through
    /// any clone of it, and closing it leaves the socket alone: the peer sees the connection go
    /// only once the socket that was handed over is dropped.
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
            assert_not_connected(conn.flush().await);
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

    /// A send through a write half of your own that can't resume writes, cut off midway, keeps a
    /// connection from being handed over, as the socket may hold a part of a message, of which the
    /// peer could make no sense. The hand-over finds that out before it stops the connection
    /// reading, so the connection reads on. It still sends, and closing it shuts the socket down.
    #[test]
    #[timeout(15000)]
    fn a_send_cut_off_midway_keeps_the_connection_from_being_handed_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = over_a_socket_pair_of_your_own(&runtime).await;
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

    /// A hand-over that fails once it stopped the connection reading, as a send under way through
    /// a write half of your own that can't resume writes is cut off midway, leaves the messages
    /// that it took off its stream with the stream, which yields them, then ends.
    #[test]
    #[timeout(15000)]
    fn a_failed_hand_over_leaves_the_messages_on_its_stream() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = over_a_socket_pair_of_your_own(&runtime).await;
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

    /// A send over a socket that zbus drives, cut off midway, fails no hand-over: the hand-over
    /// writes the rest of its message first, once the peer reads, and the connection that takes
    /// over goes on from there.
    #[test]
    #[timeout(15000)]
    fn the_rest_of_a_send_cut_off_midway_goes_out_before_the_hand_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer, _probe) = served(&runtime).await;
            let conn = Connection::from(&stream);
            let (big, next) = (big_signal(), signal(1));
            stuck(&runtime, pin!(conn.send(&big)), &peer).await;
            assert!(conn.unwritten_len() > 0, "the send left nothing to write");

            let len = big.data().len();
            let reading = thread::spawn(move || {
                let mut received = vec![0; len];
                (&peer).read_exact(&mut received).unwrap();

                (received, peer)
            });
            let handover = stream.hand_over().await.unwrap();

            let (received, peer) = reading.join().unwrap();
            assert_eq!(received, &big.data()[..]);
            assert_eq!(conn.unwritten_len(), 0);
            let conn = on(Builder::take_over(handover), &runtime)
                .build()
                .await
                .unwrap();
            conn.send(&next).await.unwrap();
            let mut received = vec![0; next.data().len()];
            (&peer).read_exact(&mut received).unwrap();
            assert_eq!(received, &next.data()[..]);
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

    /// A connection over a channel hands over its halves, with the messages that its stream had
    /// queued, and a connection that takes over gets those, then those that it is yet to read,
    /// while its peer goes on sending.
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
                    assert!(handover.unread_fds.is_empty());
                    assert!(
                        matches!(handover.socket, HandoverSocket::Socket(_)),
                        "a channel was handed over as {:?}",
                        handover.socket,
                    );
                    let mut stream = on(Builder::take_over(handover), &runtime)
                        .build_message_stream()
                        .await
                        .unwrap();
                    // The peer is still sending the rest.
                    while received.len() < msgs.len() {
                        received.push(stream.next().await.unwrap().unwrap());
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

    /// A connection that takes over a channel puts the messages that come over the channel after
    /// those that it was handed, so its streams, joined in the order of the positions of their
    /// messages, yield them in the order that they came in.
    #[test]
    #[timeout(15000)]
    fn messages_after_a_take_over_are_ordered_after_those_handed_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = channel_pair(&runtime).await;
            let mut probe = stream.clone();
            let [handed, live] = ["Handed", "Live"]
                .map(|member| Message::signal(PATH, INTERFACE, member).build(&()).unwrap());

            peer.send(&handed).await.unwrap();
            // Queued on the stream, which hands it over.
            probe.next().await.unwrap().unwrap();
            let handover = stream.hand_over().await.unwrap();
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            // A clone has what the stream has queued, and gets what the stream gets.
            let only = |member: &'static str| {
                let is_member = move |msg: &Result<Message>| {
                    msg.as_ref()
                        .is_ok_and(|msg| msg.header().member().unwrap().as_str() == member)
                };

                ordered_stream::OrderedStreamExt::filter(stream.clone(), is_member)
            };
            let joined = ordered_stream::join(only("Handed"), only("Live"));
            peer.send(&live).await.unwrap();
            let received = [
                stream.next().await.unwrap().unwrap(),
                stream.next().await.unwrap().unwrap(),
            ];

            assert_eq!(members(&received), ["Handed", "Live"]);
            assert!(
                received[0].recv_position() < received[1].recv_position(),
                "{received:?}"
            );
            // Both are queued on the clones by now.
            let joined: Vec<_> = ordered_stream::OrderedStreamExt::into_stream(joined)
                .take(2)
                .map(Result::unwrap)
                .collect()
                .await;
            assert_eq!(members(&joined), ["Handed", "Live"]);
        });
    }

    /// A connection over halves of your own, whose read half only receives bytes and says that it
    /// can take out what it read, is handed over in the middle of a message as its halves, and a
    /// connection that takes over goes on with that message.
    #[test]
    #[timeout(15000)]
    fn a_connection_over_halves_of_your_own_is_taken_over() {
        under_every_runtime(|runtime| async move {
            let (peer, theirs) = UnixStream::pair().unwrap();
            let probe = theirs.try_clone().unwrap();
            let split = recvmsg_only(&runtime, theirs, true);
            let mut stream = on(
                Builder::authenticated_socket(split, Guid::generate()),
                &runtime,
            )
            .p2p()
            .build_message_stream()
            .await
            .unwrap();
            let msgs = [1, 2, 3].map(signal);

            let mut sent = msgs[0].data().to_vec();
            sent.extend_from_slice(&msgs[1].data()[..10]);
            (&peer).write_all(&sent).unwrap();
            let received = stream.next().await.unwrap().unwrap();
            assert_eq!(&received.data()[..], &msgs[0].data()[..]);
            wait_until(|| rustix::io::ioctl_fionread(&probe).unwrap() == 0);
            let handover = stream.hand_over().await.unwrap();

            assert_eq!(handover.unread_bytes, &msgs[1].data()[..10]);
            assert!(
                matches!(handover.socket, HandoverSocket::Socket(_)),
                "halves of your own were handed over as {:?}",
                handover.socket,
            );
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            let mut rest = msgs[1].data()[10..].to_vec();
            rest.extend_from_slice(&msgs[2].data()[..]);
            (&peer).write_all(&rest).unwrap();
            for sent in &msgs[1..] {
                let received = stream.next().await.unwrap().unwrap();
                assert_eq!(&received.data()[..], &sent.data()[..]);
            }
        });
    }

    /// A hand-over that left bytes or file descriptors unread isn't taken over through a read half
    /// that can't take them, while one that left nothing unread is.
    #[test]
    #[timeout(15000)]
    fn what_was_left_unread_isnt_taken_over_through_a_half_that_cant_take_it() {
        under_every_runtime(|runtime| async move {
            let msg = signal(1);
            for (bytes, fds) in [(false, false), (true, false), (false, true)] {
                let (peer, theirs) = UnixStream::pair().unwrap();
                let split = recvmsg_only(&runtime, theirs, false);
                let mut handover = p2p_handover(HandoverSocket::Socket(split));
                if bytes {
                    handover.unread_bytes = msg.data()[..10].to_vec();
                }
                if fds {
                    handover.unread_fds = vec![pipe()];
                }

                let built = on(Builder::take_over(handover), &runtime).build().await;

                if bytes || fds {
                    assert!(matches!(built, Err(Error::Unsupported)), "{built:?}");
                    continue;
                }
                let conn = built.unwrap();
                let mut stream = MessageStream::from(&conn);
                (&peer).write_all(&msg.data()[..]).unwrap();
                let received = stream.next().await.unwrap().unwrap();
                assert_eq!(&received.data()[..], &msg.data()[..]);
            }
        });
    }

    /// The read half of a channel says that it can take what a hand-over left unread, while it
    /// takes whole messages only, so a connection that takes over a channel with part of a message
    /// left unread builds, and then ends on it.
    #[test]
    #[timeout(15000)]
    fn a_channel_taken_over_with_part_of_a_message_left_unread_ends_on_it() {
        under_every_runtime(|runtime| async move {
            let (ours, _theirs) = Channel::pair();
            let mut handover = p2p_handover(HandoverSocket::Socket(ours.into()));
            handover.unread_bytes = signal(1).data()[..10].to_vec();

            let conn = on(Builder::take_over(handover), &runtime)
                .build()
                .await
                .unwrap();

            // A call fails on what ended the connection, whether it is made before the end or
            // after.
            let call = conn
                .call_method(None::<()>, PATH, Some(INTERFACE), "Late", &())
                .await;
            let Err(Error::InputOutput(e)) = &call else {
                panic!("the call didn't fail on the part of a message left unread: {call:?}");
            };
            assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{e}");
        });
    }

    /// A hand-over of a connection to a bus isn't taken over as a peer-to-peer connection.
    #[test]
    #[timeout(15000)]
    fn a_bus_connection_isnt_taken_over_as_a_peer_to_peer_one() {
        under_every_runtime(|runtime| async move {
            let (ours, _theirs) = Channel::pair();
            let mut handover = p2p_handover(HandoverSocket::Socket(ours.into()));
            handover.p2p = false;

            let built = on(Builder::take_over(handover), &runtime)
                .p2p()
                .build()
                .await;

            assert!(matches!(built, Err(Error::Unsupported)), "{built:?}");
        });
    }

    /// Once a connection took over, the messages that the process builds get serial numbers from
    /// the one that the hand-over gives on. A hand-over that gives a lower one doesn't lower them.
    #[test]
    #[timeout(15000)]
    fn serial_numbers_go_on_from_that_of_the_hand_over() {
        under_every_runtime(|runtime| async move {
            // Other tests build messages meanwhile, which only raises the serial numbers further.
            let floor = next_serial().checked_add(1000).unwrap();
            for next in [floor, NonZeroU32::MIN] {
                let (ours, _theirs) = Channel::pair();
                let mut handover = p2p_handover(HandoverSocket::Socket(ours.into()));
                handover.next_serial = next;

                let _conn = on(Builder::take_over(handover), &runtime)
                    .build()
                    .await
                    .unwrap();

                let serial = signal(1).primary_header().serial_num();
                assert!(serial >= floor, "{serial} is below {floor}");
            }
        });
    }

    /// A connection to a bus that takes over has the unique name of the hand-over, and says no
    /// `Hello`: the first message that the bus gets from it is the first one that it sends.
    #[test]
    #[timeout(15000)]
    fn a_bus_connection_takes_over_with_its_unique_name() {
        under_every_runtime(|runtime| async move {
            // A peer-to-peer connection plays the bus.
            let (mut bus_stream, ours) = over_a_socket_pair(&runtime).await;

            let conn = on(Builder::take_over(bus_handover(ours)), &runtime)
                .build()
                .await
                .unwrap();

            assert!(
                conn.is_bus(),
                "a bus connection was taken over as a peer-to-peer one"
            );
            assert_eq!(conn.unique_name().unwrap().as_str(), UNIQUE_NAME);
            conn.emit_signal(None::<()>, PATH, INTERFACE, "Test", &())
                .await
                .unwrap();
            let msg = bus_stream.next().await.unwrap().unwrap();
            assert_eq!(msg.message_type(), Type::Signal);
            assert_eq!(msg.header().sender().unwrap(), UNIQUE_NAME);
        });
    }

    /// An interface served on a connection to a bus that took over gets the method calls to the
    /// unique name of the hand-over.
    #[cfg(all(unix, feature = "service"))]
    #[test]
    #[timeout(15000)]
    fn a_bus_connection_that_took_over_serves_calls_to_its_unique_name() {
        under_every_runtime(|runtime| async move {
            let (bus, ours) = over_a_socket_pair(&runtime).await;
            let bus = Connection::from(&bus);

            let _conn = on(Builder::take_over(bus_handover(ours)), &runtime)
                .serve_at(PATH, Echo)
                .build()
                .await
                .unwrap();

            let reply = bus
                .call_method(Some(UNIQUE_NAME), PATH, Some(INTERFACE), "Echo", &"hello")
                .await
                .unwrap();
            assert_eq!(reply.body().deserialize::<String>().unwrap(), "hello");
        });
    }

    /// A method call that a connection left unread goes to the interface served on the connection
    /// that takes over, which replies to it.
    #[cfg(all(unix, feature = "service"))]
    #[test]
    #[timeout(15000)]
    fn a_method_call_left_unread_is_served_once_taken_over() {
        under_every_runtime(|runtime| async move {
            let (mut peer_stream, ours) = over_a_socket_pair(&runtime).await;
            let call = Message::method_call(PATH, "Echo")
                .interface(INTERFACE)
                .build(&"left unread")
                .unwrap();
            let mut handover = p2p_handover(HandoverSocket::UnixStream(ours));
            handover.unread_bytes = call.data().to_vec();

            let _conn = on(Builder::take_over(handover), &runtime)
                .serve_at(PATH, Echo)
                .build()
                .await
                .unwrap();

            let reply = peer_stream.next().await.unwrap().unwrap();
            assert_eq!(reply.message_type(), Type::MethodReturn);
            assert_eq!(
                reply.header().reply_serial(),
                Some(call.primary_header().serial_num()),
            );
            assert_eq!(reply.body().deserialize::<&str>().unwrap(), "left unread");
        });
    }

    /// A connection that checks the bodies of the messages that it receives checks those that it
    /// takes over too.
    #[cfg(all(unix, feature = "bus-impl"))]
    #[test]
    #[timeout(15000)]
    fn the_bodies_of_messages_left_unread_are_checked() {
        under_every_runtime(|runtime| async move {
            let (_peer, ours) = UnixStream::pair().unwrap();
            let builder = Message::signal(PATH, INTERFACE, "Test");
            // SAFETY: The body carries no file descriptors, and the connection is to check it.
            let invalid = unsafe { builder.build_raw_body(&[2, 0, 0, 0], "b", vec![]) }.unwrap();
            let valid = signal(1);
            let mut handover = p2p_handover(HandoverSocket::UnixStream(ours));
            handover.unread_bytes = [&valid.data()[..], &invalid.data()[..]].concat();

            let mut stream = on(Builder::take_over(handover).validate_bodies(), &runtime)
                .build_message_stream()
                .await
                .unwrap();

            let received = stream.next().await.unwrap().unwrap();
            assert_eq!(&received.data()[..], &valid.data()[..]);
            let error = stream.next().await.unwrap().unwrap_err();
            assert!(matches!(error, Error::Failure(_)), "{error:?}");
            assert!(
                stream.next().await.is_none(),
                "the stream outlived its error"
            );
        });
    }

    /// A hand-over turned into bytes and file descriptors, and back, has every field that it had,
    /// and a connection that takes over from it goes on where the one that was handed over
    /// stopped, with the messages and the descriptors that it left unread.
    #[test]
    #[timeout(15000)]
    fn a_hand_over_goes_through_bytes_and_descriptors() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer, probe) = served(&runtime).await;
            let pipes = [pipe(), pipe()];
            let msgs = [
                Message::signal(PATH, INTERFACE, "WithFds")
                    .build(&crate::Fd::from(&pipes[0]))
                    .unwrap(),
                Message::signal(PATH, INTERFACE, "WithFds")
                    .build(&crate::Fd::from(&pipes[1]))
                    .unwrap(),
            ];
            send_with_fds(&peer, &msgs[0].data()[..], &[pipes[0].as_fd()]);
            send_with_fds(&peer, &msgs[1].data()[..20], &[pipes[1].as_fd()]);
            wait_until(|| rustix::io::ioctl_fionread(&probe).unwrap() == 0);
            let mut handover = stream.hand_over().await.unwrap();
            handover.unique_name = Some(UNIQUE_NAME.try_into().unwrap());
            let unread_bytes = handover.unread_bytes.clone();
            let next_serial = handover.next_serial;
            let guid = handover.server_guid.clone();

            let (bytes, fds) = handover.encode().unwrap();

            assert_eq!(fds.len(), 3);
            let handover = Handover::decode(&bytes, fds).unwrap();
            assert!(
                matches!(handover.socket, HandoverSocket::UnixStream(_)),
                "a unix socket was decoded as {:?}",
                handover.socket,
            );
            assert_eq!(handover.server_guid, guid);
            assert_eq!(handover.unique_name.as_ref().unwrap().as_str(), UNIQUE_NAME);
            assert_eq!(handover.unread_bytes, unread_bytes);
            let inodes: Vec<_> = handover.unread_fds.iter().map(inode).collect();
            assert_eq!(inodes, pipes.each_ref().map(inode));
            assert!(handover.unix_fd_passing);
            assert!(handover.p2p);
            assert_eq!(handover.next_serial, next_serial);
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            assert_eq!(
                Connection::from(&stream).unique_name().unwrap().as_str(),
                UNIQUE_NAME
            );
            (&peer).write_all(&msgs[1].data()[20..]).unwrap();
            for (sent, pipe) in msgs.iter().zip(&pipes) {
                let received = stream.next().await.unwrap().unwrap();
                assert_eq!(&received.data()[..], &sent.data()[..]);
                let inodes: Vec<_> = received.data().fds().iter().map(inode).collect();
                assert_eq!(inodes, [inode(pipe)]);
            }
        });
    }

    /// The hand-over of a connection to a bus over a TCP socket, with nothing left unread and no
    /// passing of file descriptors, goes through bytes and file descriptors too.
    #[test]
    fn a_tcp_hand_over_goes_through_bytes_and_descriptors() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let address = socket.local_addr().unwrap();
        let mut handover =
            Handover::new(HandoverSocket::TcpStream(socket), Guid::generate().into());
        handover.next_serial = NonZeroU32::new(77).unwrap();
        let guid = handover.server_guid.clone();

        let (bytes, fds) = handover.encode().unwrap();

        assert_eq!(fds.len(), 1);
        let handover = Handover::decode(&bytes, fds).unwrap();
        match &handover.socket {
            HandoverSocket::TcpStream(socket) => assert_eq!(socket.local_addr().unwrap(), address),
            socket => panic!("a TCP socket was decoded as {socket:?}"),
        }
        assert_eq!(handover.server_guid, guid);
        assert!(handover.unique_name.is_none());
        assert!(handover.unread_bytes.is_empty() && handover.unread_fds.is_empty());
        assert!(!handover.unix_fd_passing);
        assert!(!handover.p2p);
        assert_eq!(handover.next_serial.get(), 77);
    }

    /// Bytes of a format version that this version of zbus doesn't know are refused.
    #[test]
    fn an_unknown_format_version_is_refused() {
        let (mut bytes, fds) = encoded();
        bytes[0] += 1;

        let decoded = Handover::decode(&bytes, fds);

        assert!(matches!(decoded, Err(Error::Unsupported)), "{decoded:?}");
    }

    /// Bytes with more or fewer file descriptors than they count, bytes that are not those of a
    /// hand-over, and bytes with a field that no hand-over has, such as an unknown kind of socket,
    /// an invalid GUID or unique name, or a serial number of 0, are refused.
    #[test]
    fn what_is_not_an_encoded_hand_over_is_refused() {
        let (bytes, mut fds) = encoded();
        fds.push(pipe());
        assert_invalid_data(Handover::decode(&bytes, fds));

        let (bytes, mut fds) = encoded();
        fds.pop();
        assert_invalid_data(Handover::decode(&bytes, fds));

        let (bytes, fds) = encoded();
        assert_invalid_data(Handover::decode(&bytes[..bytes.len() - 1], fds));

        let (_, fds) = encoded();
        assert_invalid_data(Handover::decode(&[], fds));

        let guid = Guid::generate();
        let decoded = Handover::decode(&encoded_fields(guid.as_str(), UNIQUE_NAME, 1), socket());
        assert!(decoded.is_ok(), "{decoded:?}");

        let mut bytes = encoded_fields(guid.as_str(), UNIQUE_NAME, 1);
        bytes.push(0);
        assert_invalid_data(Handover::decode(&bytes, socket()));

        let mut bytes = encoded_fields(guid.as_str(), UNIQUE_NAME, 1);
        // The kind of socket comes right after the format version.
        bytes[1] = 2;
        assert_invalid_data(Handover::decode(&bytes, socket()));

        let bytes = encoded_fields("not a GUID", UNIQUE_NAME, 1);
        assert_invalid_data(Handover::decode(&bytes, socket()));

        let bytes = encoded_fields(guid.as_str(), "org.zbus.NotUnique", 1);
        assert_invalid_data(Handover::decode(&bytes, socket()));

        let bytes = encoded_fields(guid.as_str(), UNIQUE_NAME, 0);
        assert_invalid_data(Handover::decode(&bytes, socket()));
    }

    /// The hand-over of a connection over a socket of your own has no descriptor to pass on, so it
    /// isn't turned into bytes and file descriptors.
    #[test]
    fn a_socket_of_your_own_is_not_encoded() {
        let (ours, _theirs) = Channel::pair();
        let handover = p2p_handover(HandoverSocket::Socket(ours.into()));

        let encoded = handover.encode();

        assert!(matches!(encoded, Err(Error::Unsupported)), "{encoded:?}");
    }

    /// Hands over a connection that yielded a whole message, and read `cut` up to `at`, checks what
    /// it handed over, and takes it over. Gives how much room the bytes that it handed over had.
    async fn hand_over_in_the_middle_of(runtime: &Runtime, cut: &Message, at: usize) -> usize {
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
        assert!(
            matches!(handover.socket, HandoverSocket::UnixStream(_)),
            "a unix socket was handed over as {:?}",
            handover.socket,
        );
        let room = handover.unread_bytes.capacity();
        let guid = handover.server_guid.clone();
        // The connection that takes over has nothing to do with the one that was handed over.
        drop(conn);
        let mut stream = on(Builder::take_over(handover), runtime)
            .build_message_stream()
            .await
            .unwrap();
        let conn = Connection::from(&stream);
        assert_eq!(conn.server_guid(), &guid);
        assert!(
            conn.inner.cap_unix_fd,
            "the connection that took over can't pass file descriptors",
        );

        // The peer goes on where it was.
        let mut rest = cut.data()[at..].to_vec();
        rest.extend_from_slice(&last.data()[..]);
        (&peer).write_all(&rest).unwrap();

        for sent in [cut, &last] {
            let received = stream.next().await.unwrap().unwrap();
            assert_eq!(&received.data()[..], &sent.data()[..]);
        }

        // The connection that took over is handed over in turn, in the middle of a message too.
        (&peer).write_all(&cut.data()[..at]).unwrap();
        wait_until(|| rustix::io::ioctl_fionread(&probe).unwrap() == 0);
        let handover = stream.hand_over().await.unwrap();
        assert_eq!(handover.unread_bytes, &cut.data()[..at]);
        assert!(
            matches!(handover.socket, HandoverSocket::UnixStream(_)),
            "a unix socket was handed over again as {:?}",
            handover.socket,
        );

        room
    }

    /// The halves of `socket`, registered on `runtime`, of which the read half only receives
    /// bytes, and says that it can take out what it read as `can_take_unread` says.
    fn recvmsg_only(runtime: &Runtime, socket: UnixStream, can_take_unread: bool) -> BoxedSplit {
        let socket = Arc::new(registered(runtime, socket, UnixOps).unwrap());
        let read = RecvmsgOnly {
            socket: socket.clone(),
            can_take_unread,
        };

        Split::new(Box::new(read), Box::new(socket) as Box<dyn WriteHalf>)
    }

    /// A read half that only receives bytes, and leaves everything else to the defaults of
    /// `ReadHalf`, but for whether it can take out what it read.
    #[derive(Debug)]
    struct RecvmsgOnly {
        socket: Arc<RegisteredIo<UnixOps>>,
        can_take_unread: bool,
    }

    #[async_trait::async_trait]
    impl ReadHalf for RecvmsgOnly {
        async fn recvmsg(&mut self, buffer: &mut [u8]) -> RecvmsgResult {
            self.socket.recv(buffer).await
        }

        // Whether it says so is up to the test. It could, as the default `receive_message` keeps
        // all that it reads in what it is given, and a receipt that is dropped loses nothing.
        fn can_take_unread(&self) -> bool {
            self.can_take_unread
        }
    }

    /// The stream of the server end of a peer-to-peer connection over a unix socket pair, on
    /// `runtime`, and the other end of the pair, through which the test plays the client. Also a
    /// descriptor of the server's end of the pair, which tells how much of what the client sent
    /// the connection is yet to read.
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

    /// As `over_a_socket_pair`, but through a write half of your own that only sends bytes, and
    /// can't resume writes, so that a send that is cut off midway can leave part of a message in
    /// the socket.
    async fn over_a_socket_pair_of_your_own(runtime: &Runtime) -> (MessageStream, UnixStream) {
        let (peer, theirs) = UnixStream::pair().unwrap();
        let socket = Arc::new(registered(runtime, theirs, UnixOps).unwrap());
        let split = Split::new(
            Box::new(Reader::new(socket.clone())) as Box<dyn ReadHalf>,
            Box::new(SendmsgOnly(socket)) as Box<dyn WriteHalf>,
        );
        let stream = on(
            Builder::authenticated_socket(split, Guid::generate()),
            runtime,
        )
        .p2p()
        .build_message_stream()
        .await
        .unwrap();

        (stream, peer)
    }

    /// A write half that only sends bytes, and leaves everything else to the defaults of
    /// `WriteHalf`, which can't resume writes.
    #[derive(Debug)]
    struct SendmsgOnly(Arc<RegisteredIo<UnixOps>>);

    #[async_trait::async_trait]
    impl WriteHalf for SendmsgOnly {
        async fn sendmsg(&mut self, buffer: &[u8], fds: &[BorrowedFd<'_>]) -> io::Result<usize> {
            WriteHalf::sendmsg(&mut self.0, buffer, fds).await
        }

        async fn close(&mut self) -> io::Result<()> {
            WriteHalf::close(&mut self.0).await
        }

        fn can_pass_unix_fd(&self) -> bool {
            true
        }
    }

    /// Has `peer` send two signals to the connection of `conn`, and waits for them to be queued
    /// on its streams. Gives their bytes.
    fn queue_two(conn: &Connection, peer: &UnixStream) -> Vec<u8> {
        let queued: Vec<u8> = [signal(1), signal(2)]
            .iter()
            .flat_map(|msg| msg.data().to_vec())
            .collect();
        (&*peer).write_all(&queued).unwrap();
        wait_until(|| conn.inner.msg_receiver.len() == 2);

        queued
    }

    /// The bytes and the file descriptors of a hand-over over one end of a new unix socket pair,
    /// with a file descriptor left unread.
    fn encoded() -> (Vec<u8>, Vec<OwnedFd>) {
        let (ours, _theirs) = UnixStream::pair().unwrap();
        let mut handover = p2p_handover(HandoverSocket::UnixStream(ours));
        handover.unread_bytes = signal(1).data().to_vec();
        handover.unread_fds = vec![pipe()];

        handover.encode().unwrap()
    }

    /// A hand-over of a peer-to-peer connection over `socket`, with nothing left unread.
    fn p2p_handover(socket: HandoverSocket) -> Handover {
        let mut handover = Handover::new(socket, Guid::generate().into());
        handover.p2p = true;

        handover
    }

    /// A hand-over of a connection to a bus over `socket`, whose unique name is `UNIQUE_NAME`.
    fn bus_handover(socket: UnixStream) -> Handover {
        let mut handover =
            Handover::new(HandoverSocket::UnixStream(socket), Guid::generate().into());
        handover.unique_name = Some(UNIQUE_NAME.try_into().unwrap());

        handover
    }

    /// Polls `sending` until it wrote part of its message to the socket of `peer`, which doesn't
    /// read, and waits for room for the rest.
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
    fn pipe() -> OwnedFd {
        io::pipe().unwrap().0.into()
    }

    /// The inode of the file that `fd` is open on.
    fn inode(fd: &impl AsFd) -> u64 {
        use std::os::unix::fs::MetadataExt;

        std::fs::File::from(fd.as_fd().try_clone_to_owned().unwrap())
            .metadata()
            .unwrap()
            .ino()
    }

    /// Checks that `socket`, which a connection to `peer` handed over, is in non-blocking mode, as
    /// the connection left it, and gets what `peer` sends.
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

    /// The bytes that [`Handover::encode`] gives for a peer-to-peer hand-over over a unix socket,
    /// with nothing left unread, with these fields, which can be ones that it never gives.
    fn encoded_fields(server_guid: &str, unique_name: &str, next_serial: u32) -> Vec<u8> {
        let fields = (
            super::ENCODED_UNIX_STREAM,
            server_guid,
            unique_name,
            super::Bytes(&[]),
            0u32,
            false,
            true,
            next_serial,
        );
        let mut bytes = vec![super::ENCODING_VERSION];
        bytes.extend_from_slice(&wire::to_bytes(Context::new(LE, 0), &fields).unwrap());

        bytes
    }

    /// The file descriptors of a hand-over with no file descriptors left unread: a socket's.
    fn socket() -> Vec<OwnedFd> {
        vec![UnixStream::pair().unwrap().0.into()]
    }

    /// Asserts that `result` is the error of bytes and file descriptors that are not those of a
    /// hand-over.
    fn assert_invalid_data<T: Debug>(result: Result<T>) {
        let Err(Error::InputOutput(e)) = &result else {
            panic!("expected the error of what is not a hand-over: {result:?}");
        };
        assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{e}");
    }

    /// A match rule for every signal.
    fn signals() -> MatchRule<'static> {
        MatchRule::builder().msg_type(Type::Signal).build().unwrap()
    }

    /// A signal that carries `n`.
    fn signal(n: u32) -> Message {
        Message::signal(PATH, INTERFACE, "Test").build(&n).unwrap()
    }

    /// The members of `msgs`.
    fn members(msgs: &[Message]) -> Vec<String> {
        msgs.iter()
            .map(|msg| msg.header().member().unwrap().to_string())
            .collect()
    }

    /// A signal longer than the buffer of a socket holds.
    fn big_signal() -> Message {
        Message::signal(PATH, INTERFACE, "Big")
            .build(&vec![0u8; 1024 * 1024])
            .unwrap()
    }

    /// Waits for `condition` to hold, which a task on another thread is to bring about.
    fn wait_until(mut condition: impl FnMut() -> bool) {
        while !condition() {
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// An interface that answers a call with what it was given.
    #[cfg(all(unix, feature = "service"))]
    struct Echo;

    #[cfg(all(unix, feature = "service"))]
    #[crate::interface(name = "org.zbus.Test")]
    impl Echo {
        fn echo(&self, text: String) -> String {
            text
        }
    }

    const PATH: &str = "/org/zbus/Test";
    const INTERFACE: &str = "org.zbus.Test";
    /// The unique name that a bus gave a connection that it handed over.
    const UNIQUE_NAME: &str = ":1.42";
}

/// The tests of a build without the `p2p` feature, whose hand-overs are those of connections to a
/// bus.
#[cfg(all(test, not(feature = "p2p")))]
mod tests_without_p2p {
    use std::{num::NonZeroU32, os::unix::net::UnixStream};

    use super::{Handover, HandoverSocket};
    use crate::{Error, Guid, OwnedGuid};

    /// The hand-over of a connection to a bus goes through bytes and file descriptors.
    #[test]
    fn a_bus_hand_over_goes_through_bytes_and_descriptors() {
        let (ours, _theirs) = UnixStream::pair().unwrap();
        let mut handover = Handover::new(HandoverSocket::UnixStream(ours), guid());
        handover.unique_name = Some(":1.42".try_into().unwrap());
        handover.unread_bytes = vec![1, 2, 3];
        handover.unix_fd_passing = true;
        handover.next_serial = NonZeroU32::new(77).unwrap();
        let guid = handover.server_guid.clone();

        let (bytes, fds) = handover.encode().unwrap();

        assert_eq!(fds.len(), 1);
        let handover = Handover::decode(&bytes, fds).unwrap();
        assert!(
            matches!(handover.socket, HandoverSocket::UnixStream(_)),
            "a unix socket was decoded as {:?}",
            handover.socket,
        );
        assert_eq!(handover.server_guid, guid);
        assert_eq!(handover.unique_name.unwrap().as_str(), ":1.42");
        assert_eq!(handover.unread_bytes, [1, 2, 3]);
        assert!(handover.unread_fds.is_empty());
        assert!(handover.unix_fd_passing);
        assert_eq!(handover.next_serial.get(), 77);
    }

    /// The hand-over of a peer-to-peer connection isn't decoded in a build that can't build one.
    #[test]
    fn a_peer_to_peer_hand_over_is_not_decoded() {
        let (ours, _theirs) = UnixStream::pair().unwrap();
        let handover = Handover::new(HandoverSocket::UnixStream(ours), guid());
        let (mut bytes, fds) = handover.encode().unwrap();
        // The bytes end with whether the connection is a peer-to-peer one and the next serial
        // number, in 4 bytes each.
        let p2p = bytes.len() - 8;
        assert_eq!(bytes[p2p..p2p + 4], [0; 4]);
        bytes[p2p] = 1;

        let decoded = Handover::decode(&bytes, fds);

        assert!(matches!(decoded, Err(Error::Unsupported)), "{decoded:?}");
    }

    /// The GUID of a server, which a build without the `p2p` feature can't generate.
    fn guid() -> OwnedGuid {
        Guid::from_static_str("0123456789abcdef0123456789abcdef")
            .unwrap()
            .into()
    }
}
