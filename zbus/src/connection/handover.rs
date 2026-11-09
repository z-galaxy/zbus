//! Handing a connection over, for a connection in this process or in another one to go on from
//! where it stopped, as a service or a bus that restarts does.
//!
//! This is behind the `handover` cargo feature, which is off by default, and only on Linux.
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
//! 3. Call [`MessageStream::hand_over`]. It prepares the hand-over, so that no send waits for the
//!    peer from then on, as [`Connection::prepare_hand_over`] does, which you can call yourself as
//!    early as you like for the sends meanwhile not to wait either. Then it stops the connection
//!    reading, wherever the peer is in sending a message, and takes the connection apart, without
//!    waiting for the peer to take what was sent to it. The [`Handover`] that it gives holds the
//!    socket, the messages that were queued on the stream and that it did not yield, what the
//!    connection read off the socket after them and made no message of yet, what the connection
//!    took to send and didn't write, the file descriptors that came with all of those, and what a
//!    connection that goes on needs to know of this one.
//! 4. To pass the hand-over to another process, or to keep it while this one restarts, such as in
//!    the file descriptor store of systemd, copy its record into memory with [`Save::save`], and
//!    pass the record on, with the file descriptors that [`Save::fds`] gives. Then drop the
//!    hand-over. [`Restore::restore`] gives it back from them, in the process that they go to.
//! 5. Build the connection that goes on, in this process or in another one that the hand-over is
//!    passed to, with [`Builder::take_over`], and its stream with `build_message_stream`. It yields
//!    the messages of the hand-over first, then goes on reading the socket where the other one
//!    stopped, and it can be handed over in turn.
//! 6. Call [`Connection::flush`] on it, bounded by a timeout, as the flush waits for the peer to
//!    read: the connection writes what the other one left unwritten ahead of its own sends, and
//!    nothing else does, while the peer may be waiting for the rest of a message
//!    ([`Connection::unwritten_len`] tells whether there is any).
//!
//! Nothing that the peer sent is lost on the way, nor anything that was sent to it, and nothing is
//! written to the socket from the commit of the hand-over until the connection that goes on sends,
//! so the peer sees nothing but a pause. A peer that doesn't read holds no hand-over up, unless a
//! [`Connection::flush`] to it is under way, or the connection runs over a write half of your own
//! that can't resume writes ([`MessageStream::hand_over`] tells more): what it doesn't take goes on
//! with the hand-over. A peer that closed its end of the connection, such as one that sent a last
//! message and went, is no exception, even once a send to it failed: what it sent is handed over,
//! and the connection that goes on comes to its end. For that, the connection keeps what it read of
//! such a peer and made no message of, the file descriptors that came with it included, until it is
//! handed over or dropped.
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
//! use std::os::fd::OwnedFd;
//!
//! use zbus::{
//!     Connection, Error, block_on,
//!     connection::{Builder, Handover},
//! };
//! use zoter::{Restore, Save};
//!
//! # fn main() -> zbus::Result<()> {
//! block_on(async {
//!     let mut stream = Builder::session().build_message_stream().await?;
//!     let name = Connection::from(&stream).unique_name().cloned();
//!     // Take the messages of the connection off `stream` and deal with them, until it is time to
//!     // hand the connection over.
//!
//!     let handover = stream.hand_over().await?;
//!     let mut record = vec![0; handover.saved_len().ok_or(Error::Unsupported)?];
//!     handover.save(&mut record)?;
//!     // Pass `record` and the descriptors on, such as to the file descriptor store of systemd,
//!     // which keeps descriptors of its own, as an item of a `zoter::Handover`, and get them back,
//!     // such as in the next instance of the service. Duplicates stand for that here.
//!     let fds = handover
//!         .fds()
//!         .iter()
//!         .map(|fd| fd.try_clone_to_owned())
//!         .collect::<std::io::Result<Vec<OwnedFd>>>()?;
//!     drop(handover);
//!     let handover = Handover::restore(&record, fds)?;
//!
//!     // The connection goes on, with the name that the bus knows it by. It writes what the one
//!     // that was handed over took to send and didn't write, which the bus may be waiting for.
//!     let stream = Builder::take_over(handover).build_message_stream().await?;
//!     let conn = Connection::from(&stream);
//!     assert_eq!(conn.unique_name().cloned(), name);
//!     conn.flush().await?;
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
//! [`Connection::flush`]: crate::Connection::flush
//! [`Connection::prepare_hand_over`]: crate::Connection::prepare_hand_over
//! [`MessageStream`]: crate::MessageStream
//! [`MessageStream::hand_over`]: crate::MessageStream::hand_over
//! [`Save::save`]: zoter::Save::save
//! [`Save::fds`]: zoter::Save::fds
//! [`Restore::restore`]: zoter::Restore::restore

use std::{
    collections::VecDeque,
    fmt,
    future::{Future, poll_fn},
    io, iter, mem,
    net::TcpStream,
    num::NonZeroU32,
    os::{
        fd::{AsFd, BorrowedFd, OwnedFd},
        unix::net::UnixStream,
    },
    pin::{Pin, pin},
    str,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

use futures_core::Stream;
use zruntime::{
    Event, EventListener,
    broadcast::{Receiver, TryRecvError},
};

use super::socket_write::Unwritten;
use crate::{
    Connection, Error, Guid, Message, OwnedGuid, Result,
    connection::socket::{BoxedSplit, ReadHalf, Split, read_primary_header},
    message::{self, header::MIN_MESSAGE_SIZE},
    names::OwnedUniqueName,
    runtime::IoSource,
    wire::{
        Endian, LE,
        serialized::{Context, Data},
    },
};

/// A connection taken apart by [`MessageStream::hand_over`], or put back together by
/// [`Restore::restore`], for [`Builder::take_over`] to build a connection that goes on from where
/// it stopped, in this process or in another one.
///
/// The other process can be the next instance of a service that restarts, or a helper process.
/// Nothing in a hand-over changes once it exists.
///
/// This type is only available on Linux, with the `handover` feature.
///
/// # Passing it to another process
///
/// [`Save::save`] copies the record of the hand-over into memory that you give, anywhere, such
/// as the place of the connection in a file that is mapped into memory, and [`Save::fds`]
/// gives the file descriptors that go with the record, that of the socket first. Pass the record on
/// as bytes, and the descriptors as descriptors, such as over a unix socket or through the file
/// descriptor store of systemd, and [`Restore::restore`] gives the hand-over back from them,
/// wherever they go.
///
/// The hand-over of a connection over a socket of your own, such as a `Channel`, or over the pipes
/// of a `unixexec:` address, has no record: its halves can only go on in this process, which
/// [`Save::saved_len`] tells.
///
/// What the hand-over holds of what the peer sent is the messages that were queued on the stream,
/// then what was read after them and made no message of yet: whole messages, and the start of one
/// that the peer was in the middle of sending, which can be as long as the longest message that
/// D-Bus allows (128 MiB).
///
/// What the hand-over holds of what was sent to the peer is the rest of a message that a send was
/// cut off in the middle of, and the messages that sends kept once the hand-over was prepared
/// ([`Connection::prepare_hand_over`]), which nothing bounds but how much the peer didn't take.
///
/// [`Builder::take_over`]: super::Builder::take_over
/// [`Connection::prepare_hand_over`]: crate::Connection::prepare_hand_over
/// [`MessageStream::hand_over`]: crate::MessageStream::hand_over
/// [`Save::save`]: zoter::Save::save
/// [`Save::saved_len`]: zoter::Save::saved_len
/// [`Save::fds`]: zoter::Save::fds
/// [`Restore::restore`]: zoter::Restore::restore
#[must_use = "dropping a `Handover` closes the descriptors that it holds, which disconnects the \
              peer unless they were passed on"]
pub struct Handover {
    /// The socket of the connection.
    pub(super) socket: HandoverSocket,
    /// The GUID of the server end of the connection.
    pub(super) server_guid: OwnedGuid,
    /// The unique name of the connection, if it has one: for a connection to a bus, the one that
    /// the bus knows it by, which the connection that goes on keeps, rather than say `Hello`
    /// again.
    pub(super) unique_name: Option<OwnedUniqueName>,
    /// Whether the two ends of the connection agreed to pass file descriptors.
    pub(super) unix_fd_passing: bool,
    /// Whether the connection is a peer-to-peer one rather than one to a bus.
    #[cfg(feature = "p2p")]
    pub(super) p2p: bool,
    /// The serial number that the process was to give the next message that it builds, which the
    /// connection that goes on gives its messages from on, so that a reply to a method call made
    /// before the hand-over isn't taken for the reply to one made after it.
    pub(super) next_serial: NonZeroU32,
    /// The messages that were queued on the stream and that it didn't yield, oldest first, with
    /// their file descriptors.
    pub(super) queued: Vec<Message>,
    /// What was read off the socket after them and made no message of yet, from the start of a
    /// message: the tail of what was read.
    pub(super) tail: Vec<u8>,
    /// The file descriptors that came in with the tail and that no message took, in the order that
    /// they came in.
    pub(super) tail_fds: Vec<OwnedFd>,
    /// What the connection took to send and didn't write.
    pub(super) unwritten: Unwritten,
}

impl Handover {
    /// How many bytes were read off the socket and not yielded as messages: those of the messages
    /// that were queued on the stream and that it didn't yield, then whatever was read after them.
    pub fn unread_len(&self) -> usize {
        len_of(&self.queued) + self.tail.len()
    }

    /// How many bytes the connection took to send and didn't write: the rest of a message that a
    /// write ended in the middle of, if any, then whole messages, which the connection that takes
    /// over writes ahead of its own sends.
    pub fn unwritten_len(&self) -> usize {
        self.unwritten.len()
    }
}

/// The record of the hand-over, and its file descriptors, to pass on to another process, such as
/// through the file descriptor store of systemd, which [`Handover`]'s [`zoter::Restore`] impl
/// gives the hand-over back from.
impl zoter::Save for Handover {
    /// The exact length of the record that [`Save::save`] writes, a multiple of 8, or `None`
    /// for the hand-over of a connection over a socket of your own, which only goes on in this
    /// process: a [`Socket`] of yours, a `Channel`, or the pipes of a `unixexec:` address.
    ///
    /// [`Socket`]: super::Socket
    /// [`Save::save`]: zoter::Save::save
    fn saved_len(&self) -> Option<usize> {
        match self.socket {
            HandoverSocket::Socket(_) => None,
            _ => Some(self.layout().len),
        }
    }

    /// Copy the record of the hand-over into `dest`, which can be any memory, at any address, such
    /// as the place of the connection in a file that is mapped into memory.
    ///
    /// `dest` is to be exactly [`Save::saved_len`] bytes long. Each of its bytes is written
    /// once, the padding of the record included, and the bytes that the hand-over holds are copied
    /// once, from where it holds them. The hand-over is left as it was, so a caller that gives up
    /// on passing it on can still go on with it in this process, once it took back the descriptors
    /// that it passed on.
    ///
    /// A later version of zbus restores the records that this one saves, as a service that
    /// restarts after an update needs. A version of zbus that can't read a record, as an earlier
    /// one may not, refuses it.
    ///
    /// # Errors
    ///
    /// Nothing is written on an error, which is one of:
    ///
    /// * [`io::ErrorKind::Unsupported`] for the hand-over of a connection over a socket of your
    ///   own, whose `saved_len` is `None`.
    /// * [`io::ErrorKind::InvalidInput`] for a `dest` that is not `saved_len` bytes long, or for a
    ///   hand-over with more messages or file descriptors than a record can count (4 billion).
    ///
    /// [`Save::saved_len`]: zoter::Save::saved_len
    fn save(&self, dest: &mut [u8]) -> io::Result<()> {
        let kind = match &self.socket {
            HandoverSocket::UnixStream(_) => UNIX_STREAM,
            HandoverSocket::TcpStream(_) => TCP_STREAM,
            HandoverSocket::Socket(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "the hand-over of a connection over a socket of your own has no record",
                ));
            }
        };
        let layout = self.layout();
        if dest.len() != layout.len {
            return Err(invalid_input("the memory is not as long as the record"));
        }
        let kept = &self.unwritten.kept;
        let counts = [
            fd_count_of(&self.queued) + self.tail_fds.len() + fd_count_of(kept),
            self.queued.len(),
            self.tail_fds.len(),
            kept.len(),
        ];
        // Each count of the record takes 4 bytes, and so does each of those in its table.
        let mut table = self
            .queued
            .iter()
            .chain(kept)
            .flat_map(|msg| [msg.data().len(), msg.data().fds().len()]);
        if counts
            .iter()
            .copied()
            .chain(&mut table)
            .any(|count| u32::try_from(count).is_err())
        {
            return Err(invalid_input(
                "too many messages or file descriptors for a record",
            ));
        }
        let name = self.unique_name.as_ref().map_or("", |name| name.as_str());
        let mut flags = 0;
        if self.unix_fd_passing {
            flags |= FD_PASSING;
        }
        #[cfg(feature = "p2p")]
        if self.p2p {
            flags |= P2P;
        }
        // The GUID in the form of 32 hex digits that D-Bus gives it, whatever form it was given in.
        let mut guid = [0; 32];
        uuid::Uuid::try_parse(self.server_guid.as_str())
            .expect("a GUID is a UUID")
            .as_simple()
            .encode_lower(&mut guid);
        let rest = self.unwritten.rest();

        // Nothing fails from here on.
        let mut record = Record(dest);
        record.put(MAGIC);
        record.put(&MAJOR.to_le_bytes());
        // 104 bytes and those of a unique name, which has 255 at most.
        record.put(&(layout.header_len as u16).to_le_bytes());
        // The record needs no feature of its format that some version of zbus doesn't know.
        record.put(&0u32.to_le_bytes());
        record.put(&(layout.len as u64).to_le_bytes());
        record.put(&[kind, flags, name.len() as u8, 0]);
        record.put(&self.next_serial.get().to_le_bytes());
        for count in counts {
            record.put(&(count as u32).to_le_bytes());
        }
        for len in [layout.inbound, layout.outbound, rest.len()] {
            record.put(&(len as u64).to_le_bytes());
        }
        record.put(&guid);
        record.put(name.as_bytes());
        record.zeros(layout.header_len - NAME - name.len());
        for msg in self.queued.iter().chain(kept) {
            record.put(&(msg.data().len() as u32).to_le_bytes());
            record.put(&(msg.data().fds().len() as u32).to_le_bytes());
        }
        for msg in &self.queued {
            record.put(msg.data());
        }
        record.put(&self.tail);
        record.zeros(layout.inbound.next_multiple_of(8) - layout.inbound);
        record.put(rest);
        for msg in kept {
            record.put(msg.data());
        }
        record.zeros(layout.outbound.next_multiple_of(8) - layout.outbound);
        debug_assert!(
            record.0.is_empty(),
            "the record is longer than what was copied into it"
        );

        Ok(())
    }

    /// The file descriptors to pass on with the record, in the order that [`Restore::restore`]
    /// takes them: that of the socket, then those of the messages that were queued on the stream,
    /// in their order, those that came in with what was read after them, and those of the messages
    /// that the connection took to send and didn't write, in their order. There are none for a
    /// hand-over whose [`Save::saved_len`] is `None`.
    ///
    /// They are borrowed, so none is duplicated, and this can't fail on the limit of descriptors
    /// that a process may have open. That of the socket is close-on-exec in a hand-over that
    /// [`MessageStream::hand_over`](crate::MessageStream::hand_over) gave. The others, and all
    /// those of a restored hand-over, are as they came in, and zbus receives descriptors without
    /// close-on-exec.
    ///
    /// [`Save::saved_len`]: zoter::Save::saved_len
    /// [`Restore::restore`]: zoter::Restore::restore
    fn fds(&self) -> Vec<BorrowedFd<'_>> {
        let socket = match &self.socket {
            HandoverSocket::UnixStream(socket) => socket.as_fd(),
            HandoverSocket::TcpStream(socket) => socket.as_fd(),
            HandoverSocket::Socket(_) => return Vec::new(),
        };
        let kept = &self.unwritten.kept;

        iter::once(socket)
            .chain(fds_of(&self.queued))
            .chain(self.tail_fds.iter().map(AsFd::as_fd))
            .chain(fds_of(kept))
            .collect()
    }
}

/// The hand-over that [`zoter::Save`] saved, out of its record and its file descriptors.
impl zoter::Restore for Handover {
    type Error = Error;

    /// The hand-over that [`Save::save`] copied into `record`, given exactly those bytes, at
    /// any address, with the file descriptors that [`Save::fds`] gave, in their order, in this
    /// process or in another one.
    ///
    /// This copies what it needs out of `record` before it returns.
    ///
    /// # Errors
    ///
    /// * [`Error::Unsupported`] for a record that this version of zbus can't read, as one that a
    ///   later version saved may be, and for the record of a peer-to-peer connection in a build
    ///   without the `p2p` feature.
    /// * An I/O error ([`Error::InputOutput`]) of kind `InvalidData` for anything else that is not
    ///   a record that `save` copied, and for a number of file descriptors other than the one that
    ///   it counts.
    ///
    /// The file descriptors are closed on an error.
    ///
    /// [`Save::save`]: zoter::Save::save
    /// [`Save::fds`]: zoter::Save::fds
    fn restore(record: &[u8], fds: Vec<OwnedFd>) -> Result<Handover> {
        if record.get(..MAGIC.len()) != Some(MAGIC) {
            return Err(malformed());
        }
        match u16::from_le_bytes(field(record, 8)?) {
            MAJOR => (),
            major if major > MAJOR => return Err(Error::Unsupported),
            _ => return Err(malformed()),
        }
        if u32::from_le_bytes(field(record, 12)?) & !KNOWN_FEATURES != 0 {
            return Err(Error::Unsupported);
        }
        let [kind, flags, name_len, _] = field(record, 24)?;
        #[cfg(not(feature = "p2p"))]
        if flags & P2P != 0 {
            return Err(Error::Unsupported);
        }
        let header_len = usize::from(u16::from_le_bytes(field(record, 10)?));
        let name_len = usize::from(name_len);
        // A later minor version of the format adds fields at the end of the header, which this
        // skips, as it reads the fields that it knows only.
        if header_len % 8 != 0
            || header_len < (NAME + name_len).next_multiple_of(8)
            || len_at(record, 16)? != record.len()
        {
            return Err(malformed());
        }
        let next_serial = NonZeroU32::new(u32::from_le_bytes(field(record, 28)?));
        let next_serial = next_serial.ok_or_else(malformed)?;
        let count_at = |at| field(record, at).map(|count| u32::from_le_bytes(count) as usize);
        let (fd_count, queued, tail_fds, kept) =
            (count_at(32)?, count_at(36)?, count_at(40)?, count_at(44)?);
        let (inbound, outbound) = (len_at(record, 48)?, len_at(record, 56)?);
        let rest = len_at(record, 64)?;
        // The socket's descriptor comes first. Counted without adding to `fd_count`, which can
        // overflow on a 32-bit target.
        if fds.len().checked_sub(1) != Some(fd_count) {
            return Err(malformed());
        }
        let server_guid = str::from_utf8(bytes(record, 72, 32)?).map_err(|_| malformed())?;
        let server_guid = Guid::try_from(server_guid).map_err(|_| malformed())?;
        let unique_name = match bytes(record, NAME, name_len)? {
            [] => None,
            name => {
                let name = str::from_utf8(name).map_err(|_| malformed())?;

                Some(OwnedUniqueName::try_from(name).map_err(|_| malformed())?)
            }
        };

        // The table, the inbound bytes and the outbound ones, each of those padded to a multiple of
        // 8, up to the end of the record.
        let table_len = queued
            .checked_add(kept)
            .and_then(|entries| entries.checked_mul(8))
            .ok_or_else(malformed)?;
        let table = bytes(record, header_len, table_len)?;
        let (mut inbound, inbound_end) = section(record, header_len + table_len, inbound)?;
        let (outbound, end) = section(record, inbound_end, outbound)?;
        if end != record.len() {
            return Err(malformed());
        }
        let (rest, mut outbound) = outbound.split_at_checked(rest).ok_or_else(malformed)?;
        let (queued_table, kept_table) = table.split_at(8 * queued);

        let mut fds = fds.into_iter();
        let socket = fds.next().expect("the descriptors were counted");
        let socket = match kind {
            UNIX_STREAM => HandoverSocket::UnixStream(UnixStream::from(socket)),
            TCP_STREAM => HandoverSocket::TcpStream(TcpStream::from(socket)),
            _ => return Err(malformed()),
        };
        let queued = queued_table
            .as_chunks::<8>()
            .0
            .iter()
            .map(|entry| framed(entry, &mut inbound, &mut fds))
            .collect::<Result<Vec<_>>>()?;
        let tail = inbound.to_vec();
        let tail_fds: Vec<_> = fds.by_ref().take(tail_fds).collect();
        let tail_fds_counted = tail_fds.len() == count_at(40)?;
        let kept = kept_table
            .as_chunks::<8>()
            .0
            .iter()
            .map(|entry| framed(entry, &mut outbound, &mut fds))
            .collect::<Result<VecDeque<_>>>()?;
        // Each message took as many bytes and descriptors as the table gives it, so these are
        // those that the record counts if none is left over, and none ran short.
        if !outbound.is_empty() || !tail_fds_counted || fds.next().is_some() {
            return Err(malformed());
        }
        // What is left of a message that a write ended in the middle of goes out as bytes, as
        // they are.
        let cut = (!rest.is_empty()).then(|| (Data::new(rest.to_vec(), Context::new(LE, 0)), 0));

        Ok(Handover {
            socket,
            server_guid: server_guid.to_owned().into(),
            unique_name,
            unix_fd_passing: flags & FD_PASSING != 0,
            #[cfg(feature = "p2p")]
            p2p: flags & P2P != 0,
            next_serial,
            queued,
            tail,
            tail_fds,
            unwritten: Unwritten { cut, kept },
        })
    }
}

impl Handover {
    /// How the record of the hand-over is laid out.
    fn layout(&self) -> Layout {
        let name = self
            .unique_name
            .as_ref()
            .map_or(0, |name| name.as_str().len());
        let header_len = (NAME + name).next_multiple_of(8);
        let kept = &self.unwritten.kept;
        let table_len = 8 * (self.queued.len() + kept.len());
        let inbound = len_of(&self.queued) + self.tail.len();
        let outbound = self.unwritten.rest().len() + len_of(kept);
        let len =
            header_len + table_len + inbound.next_multiple_of(8) + outbound.next_multiple_of(8);

        Layout {
            header_len,
            inbound,
            outbound,
            len,
        }
    }
}

/// Shows the kind of socket, the unique name, and how many bytes and file descriptors were left
/// unread and unwritten, rather than which: the bytes can be as many as the longest message that
/// D-Bus allows.
impl fmt::Debug for Handover {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kept = &self.unwritten.kept;

        f.debug_struct("Handover")
            .field("socket", &self.socket)
            .field("unique_name", &self.unique_name)
            .field("unread_len", &self.unread_len())
            .field(
                "unread_fds",
                &(fd_count_of(&self.queued) + self.tail_fds.len()),
            )
            .field("unwritten_len", &self.unwritten_len())
            .field("unwritten_fds", &fd_count_of(kept))
            .finish_non_exhaustive()
    }
}

/// The socket of a connection that was handed over.
pub(super) enum HandoverSocket {
    /// A unix-domain stream that zbus drove itself: the only descriptor of it that zbus kept,
    /// which the hand-over made, in non-blocking mode, as zbus switched the socket to that.
    UnixStream(UnixStream),
    /// A TCP stream that zbus drove itself, as for `UnixStream`.
    TcpStream(TcpStream),
    /// The halves of any other socket, which can only go on in this process. Those that zbus made
    /// run on the runtime of the connection that they came from, so they need it to keep running.
    Socket(BoxedSplit),
}

/// Shows the kind of socket, and its descriptor if any, rather than the halves of a socket of your
/// own, of which a read half can hold bytes.
impl fmt::Debug for HandoverSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnixStream(socket) => f.debug_tuple("UnixStream").field(socket).finish(),
            Self::TcpStream(socket) => f.debug_tuple("TcpStream").field(socket).finish(),
            Self::Socket(_) => f.write_str("Socket"),
        }
    }
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
        self.claim(stream)?;
        // From here on, for good, no send waits for the peer, a send under way included, which
        // lets go of the socket at once, so that the hand-over doesn't wait for the peer either.
        // There is nothing to prepare over a write half of your own that can't resume writes, as
        // sends over it wait for the half, and so does the hand-over, for a send under way.
        self.prepare_hand_over();
        self.stop_reading();
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

    /// Claim the connection for the hand-overs through the stream that `stream` stands for,
    /// unless its reader can't stop, or a hand-over through another stream claimed it first,
    /// which a hand-over through this one then fails on.
    fn claim(&self, stream: &Arc<()>) -> Result<()> {
        let stop = &self.inner.socket_status.stop;
        if !stop.can_stop.load(Ordering::Acquire) {
            return Err(Error::Unsupported);
        }

        stop.claim(stream)
    }

    /// Ask the reader to stop reading the socket, wherever it is in a message.
    ///
    /// This applies to every clone of the connection, and does nothing the second time, or on a
    /// connection that is closed already, which goes on ending as a closed one does, rather than
    /// be taken for one that stopped.
    fn stop_reading(&self) {
        if self.inner.is_closed() {
            return;
        }
        let stop = &self.inner.socket_status.stop;
        if !stop.asked.swap(true, Ordering::AcqRel) {
            stop.asked_event.notify(usize::MAX);
        }
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
        // Under the lock that every send, flush, shutdown and hand-over takes. A send over a write
        // half of your own that can't resume writes holds it until it is through, and so does a
        // flush, which waits for the peer to take what the connection has left to write. Any other
        // send lets go of it at once, as the hand-over is prepared.
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
        if write.is_shut_down() {
            return Err(not_connected("the connection was closed").into());
        }
        if !matches!(*end, ReaderEnd::Stopped(_) | ReaderEnd::Ended(_)) {
            return Err(not_connected("the connection ended before it stopped reading").into());
        }
        if write.is_torn() {
            return Err(torn());
        }
        // The last step that can fail, so that a failure commits nothing. The messages keep their
        // descriptors, which the hand-over borrows from them, as other streams may hold them too.
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
        // What the connection has left to write goes with the hand-over, the rest of a message
        // that a send was cut off in the middle of first, for the connection that takes over to
        // write ahead of its own sends.
        let (write_half, writes, unwritten) = write
            .take_half()
            .expect("the write half is only taken by a hand-over, which there was none of");
        inner.socket_status.closed.store(true, Ordering::Release);
        inner.socket_status.closed_event.notify(usize::MAX);
        drop(guard);

        let ReadParts {
            half: read_half,
            queued: taken_over,
            unread_bytes: tail,
            unread_fds: tail_fds,
        } = parts;
        // The error that the reader ended on, if any, is no message to hand over. The messages that
        // the reader was yet to yield, of those that the connection took over, come after those
        // that it yielded.
        let mut queued: Vec<Message> = taken.drain(..).flatten().collect();
        queued.extend(taken_over);
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
            unix_fd_passing: inner.cap_unix_fd,
            #[cfg(feature = "p2p")]
            p2p: !self.is_bus(),
            next_serial: message::header::next_serial(),
            queued,
            tail,
            tail_fds,
            unwritten,
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
            .field("queued", &parts.queued.len())
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
    /// The messages that a connection that this one took over left unread, which the reader was
    /// yet to yield, ahead of what it read off the socket, oldest first.
    pub queued: VecDeque<Message>,
    /// What was read off the socket and is yet to make a message, from the start of a message.
    pub unread_bytes: Vec<u8>,
    /// The file descriptors that came in with what was read, and that no message took, in the
    /// order that they came in.
    pub unread_fds: Vec<OwnedFd>,
}

impl ReadParts {
    /// What the reader leaves: `queued`, the messages that it was yet to yield of those that the
    /// connection took over, and `half`, with what it read and is yet to return in a message taken
    /// out of it, after `unread_bytes` and `unread_fds`, which the reader kept for its next read.
    ///
    /// The last call to receive a message is over, or was dropped wherever it waited, so the half
    /// has all that it read in hand.
    pub(super) fn new(
        mut half: Box<dyn ReadHalf>,
        queued: VecDeque<Message>,
        mut unread_bytes: Vec<u8>,
        mut unread_fds: Vec<OwnedFd>,
    ) -> Self {
        half.take_unread(&mut unread_bytes, &mut unread_fds);

        Self {
            half,
            queued,
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

/// How many bytes `msgs` come to.
fn len_of<'m>(msgs: impl IntoIterator<Item = &'m Message>) -> usize {
    msgs.into_iter().map(|msg| msg.data().len()).sum()
}

/// How many file descriptors `msgs` carry.
fn fd_count_of<'m>(msgs: impl IntoIterator<Item = &'m Message>) -> usize {
    msgs.into_iter().map(|msg| msg.data().fds().len()).sum()
}

/// The file descriptors that `msgs` carry, in their order.
fn fds_of<'m>(msgs: impl IntoIterator<Item = &'m Message>) -> impl Iterator<Item = BorrowedFd<'m>> {
    msgs.into_iter()
        .flat_map(|msg| msg.data().fds())
        .map(AsFd::as_fd)
}

// The record of a hand-over, as `Handover::save` copies it and `Handover::restore` reads it.
//
// Its integers are little-endian, and its offsets are from its start, so that it can be anywhere
// in memory, at any address. Its header is:
//
//      0  8 bytes  `MAGIC`
//      8  2 bytes  the major version of the format, `MAJOR`
//     10  2 bytes  how long the header is, a multiple of 8: a later minor version of the format
//                  adds fields at its end, which an earlier one skips
//     12  4 bytes  the features of the format that the record needs, as bits, which a version of
//                  zbus that doesn't know one of refuses: none yet (`KNOWN_FEATURES`)
//     16  8 bytes  how long the record is
//     24  1 byte   the kind of socket: `UNIX_STREAM` or `TCP_STREAM`
//     25  1 byte   flags: `FD_PASSING` and `P2P`
//     26  1 byte   how long the unique name is, `N`, 0 for none
//     27  1 byte   0
//     28  4 bytes  the serial number of the next message
//     32  4 bytes  how many file descriptors come after that of the socket
//     36  4 bytes  how many messages were queued, `Q`
//     40  4 bytes  how many file descriptors came in with what was read after them, the tail
//     44  4 bytes  how many whole messages were left to write, `W`
//     48  8 bytes  how long the inbound bytes are: those of the `Q` messages, then the tail
//     56  8 bytes  how long the outbound bytes are: the rest of a message that a write ended in
//                  the middle of, then the `W` messages
//     64  8 bytes  how long that rest is
//     72 32 bytes  the GUID of the server, in hex
//    104  N bytes  the unique name (`NAME`), then zeros up to the end of the header
//
// Then comes a table, of the length and the number of file descriptors, in 4 bytes each, of each
// of the `Q` messages, then of each of the `W` ones. Then the inbound bytes, and zeros up to a
// multiple of 8, then the outbound bytes, and zeros up to a multiple of 8, which ends the record.
//
// The file descriptors that go with the record are that of the socket, then those of the `Q`
// messages, in their order, then those of the tail, then those of the `W` messages, in their order.

/// How the record of a hand-over is laid out: the lengths that its header gives.
struct Layout {
    /// How long the header is, with the unique name and the zeros after it.
    header_len: usize,
    /// How long the bytes that were read off the socket are.
    inbound: usize,
    /// How long the bytes left to write are.
    outbound: usize,
    /// How long the whole record is.
    len: usize,
}

/// Memory that a record is copied into, from its start on.
struct Record<'d>(&'d mut [u8]);

impl Record<'_> {
    /// Copy `bytes` into the memory, after what was copied into it before.
    fn put(&mut self, bytes: &[u8]) {
        let (head, rest) = mem::take(&mut self.0).split_at_mut(bytes.len());
        head.copy_from_slice(bytes);
        self.0 = rest;
    }

    /// Zero the next `len` bytes of the memory.
    fn zeros(&mut self, len: usize) {
        let (head, rest) = mem::take(&mut self.0).split_at_mut(len);
        head.fill(0);
        self.0 = rest;
    }
}

/// The `N` bytes of `record` at `at`.
fn field<const N: usize>(record: &[u8], at: usize) -> Result<[u8; N]> {
    let bytes = bytes(record, at, N)?;

    Ok(bytes
        .try_into()
        .expect("the bytes of a field are as many as it has"))
}

/// The length of 8 bytes of `record` at `at`.
fn len_at(record: &[u8], at: usize) -> Result<usize> {
    usize::try_from(u64::from_le_bytes(field(record, at)?)).map_err(|_| malformed())
}

/// The `len` bytes of `record` at `at`.
fn bytes(record: &[u8], at: usize, len: usize) -> Result<&[u8]> {
    at.checked_add(len)
        .and_then(|end| record.get(at..end))
        .ok_or_else(malformed)
}

/// The `len` bytes of `record` at `at`, which zeros follow up to a multiple of 8, and where those
/// end.
fn section(record: &[u8], at: usize, len: usize) -> Result<(&[u8], usize)> {
    let padded = len.checked_next_multiple_of(8).ok_or_else(malformed)?;
    let (section, padding) = bytes(record, at, padded)?.split_at(len);
    if padding.iter().any(|byte| *byte != 0) {
        return Err(malformed());
    }

    Ok((section, at + padded))
}

/// The message that `entry` of the table of a record tells of, which `bytes` start with, with as
/// many of `fds` as the entry counts, which it takes off them.
fn framed(
    entry: &[u8],
    bytes: &mut &[u8],
    fds: &mut impl Iterator<Item = OwnedFd>,
) -> Result<Message> {
    let (len, count) = entry.split_at(4);
    let len = u32::from_le_bytes(len.try_into().expect("4 bytes")) as usize;
    let count = u32::from_le_bytes(count.try_into().expect("4 bytes")) as usize;
    let (msg, rest) = bytes.split_at_checked(len).ok_or_else(malformed)?;
    *bytes = rest;
    // The table gives each message the length that its primary header says that it has.
    if len < MIN_MESSAGE_SIZE {
        return Err(malformed());
    }
    let (primary_header, whole) = read_primary_header(msg).map_err(|_| malformed())?;
    let msg_fds: Vec<_> = fds.by_ref().take(count).collect();
    if whole != len || msg_fds.len() != count {
        return Err(malformed());
    }
    let ctxt = Context::new(Endian::from(primary_header.endian_sig()), 0);
    let data = Data::new_fds(msg.to_vec(), ctxt, msg_fds);
    let msg = Message::from_raw_parts(data, 0).map_err(|_| malformed())?;
    // A message carries as many descriptors as its header counts.
    if msg.header().unix_fds().unwrap_or(0) as usize != count {
        return Err(malformed());
    }

    Ok(msg)
}

/// The error of a record, or of file descriptors, that [`Restore::restore`] can make no hand-over
/// of.
///
/// [`Restore::restore`]: zoter::Restore::restore
fn malformed() -> Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "not the record of a hand-over and its file descriptors",
    )
    .into()
}

/// The error, for `reason`, of memory that [`Save::save`] can't copy a record into.
///
/// [`Save::save`]: zoter::Save::save
fn invalid_input(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, reason)
}

/// The bytes that a record starts with.
const MAGIC: &[u8; 8] = b"zbus-hov";
/// The major version of the format of the record, which a version of zbus refuses a record of a
/// later one of, as it can't read it.
const MAJOR: u16 = 1;
/// The features of the format of the record that this version of zbus knows, as the bits of those
/// that a record needs: none yet.
const KNOWN_FEATURES: u32 = 0;
/// Where the unique name starts in a record, after the fields that every header has.
const NAME: usize = 104;
/// The kind of socket of a record of a unix-domain stream.
const UNIX_STREAM: u8 = 0;
/// The kind of socket of a record of a TCP stream.
const TCP_STREAM: u8 = 1;
/// The flag of a record of a connection whose two ends agreed to pass file descriptors.
const FD_PASSING: u8 = 1;
/// The flag of a record of a peer-to-peer connection.
const P2P: u8 = 2;

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
        collections::VecDeque,
        fmt::Debug,
        future::Future,
        io::{self, ErrorKind, IoSlice, Read, Write},
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

    use zoter::{Restore, Save};

    use super::{Handover, HandoverSocket, MAJOR, NAME, ReaderEnd, TCP_STREAM, UNIX_STREAM};
    use crate::{
        Address, Connection, Error, Guid, MatchRule, Message, MessageStream, Result,
        address::transport::{Transport, Unix, UnixSocket},
        connection::{
            AuthMechanism, Builder,
            socket::{BoxedSplit, Channel, ReadHalf, RecvmsgResult, Split, WriteHalf},
            socket_write::Unwritten,
        },
        message::{Type, header::next_serial},
        runtime::{
            Runtime,
            io::{Reader, RegisteredIo, UnixOps, registered},
            test_runtime::{AbortingRuntime, TestRuntime, under_every_runtime},
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
            assert_eq!(handover.unread_len(), msg.data().len());
            assert_eq!(unread(&handover), &msg.data()[..]);
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
    /// whatever the peer sent before. A connection that takes over, in this process or through the
    /// record of the hand-over, yields the message that the peer sent whole, then comes to the
    /// same end.
    #[test]
    #[timeout(15000)]
    fn a_connection_whose_peer_closed_its_end_is_handed_over() {
        under_every_runtime(|runtime| async move {
            for (ended, record) in [(true, false), (false, false), (true, true), (false, true)] {
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

                    let case =
                        format!("ended: {ended}, record: {record}, whole: {whole}, cut: {cut}");
                    assert!(stream.next().await.is_none(), "{case}");
                    assert!(
                        matches!(handover.socket, HandoverSocket::UnixStream(_)),
                        "{case}: a unix socket was handed over as {:?}",
                        handover.socket,
                    );
                    // What the connection didn't read is still in the socket, up to its end.
                    assert!(sent.starts_with(&unread(&handover)), "{case}");
                    let handover = if record {
                        through_a_record(handover, 5)
                    } else {
                        handover
                    };
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

            assert_eq!(unread(&handover), &last.data()[..]);
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

            assert_eq!(unread(&handover), &last.data()[..]);
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
    /// taken off it by the hand-over itself, which lets the reader stop. They are handed over as
    /// messages, those that the reader read after them too, and nothing is written to the socket.
    /// A connection that takes over yields them first.
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
                assert_eq!(unread(&handover), left, "taken: {taken}");
                assert!(!handover.queued.is_empty(), "taken: {taken}");
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
    /// the descriptors that came with that, which `fds` gives in that order, after that of the
    /// socket. A connection that takes over, in this process or through the record of the
    /// hand-over, yields each message with its own descriptors.
    #[test]
    #[timeout(15000)]
    fn messages_queued_on_the_stream_are_handed_over_with_their_descriptors() {
        under_every_runtime(|runtime| async move {
            for record in [false, true] {
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
                assert_eq!(unread(&handover), sent, "record: {record}");
                assert_eq!(handover.queued.len(), 2, "record: {record}");
                assert_eq!(handover.tail, &msgs[2].data()[..20], "record: {record}");
                assert_eq!(handover.fds().len(), 1 + pipes.len());
                let inodes: Vec<_> = handover
                    .fds()
                    .into_iter()
                    .skip(1)
                    .map(|fd| inode(&fd))
                    .collect();
                assert_eq!(inodes, pipes.each_ref().map(inode), "record: {record}");
                drop(conn);
                let handover = if record {
                    through_a_record(handover, 0)
                } else {
                    handover
                };
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
                    assert_eq!(inodes, sent, "record: {record}");
                }
            }
        });
    }

    /// The messages that a connection hands over are the messages that its stream had queued, not
    /// copies of their bytes, and so are those that a connection that takes over in this process
    /// yields, at positions among its own.
    #[test]
    #[timeout(15000)]
    fn queued_messages_are_handed_over_as_they_are() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = channel_pair(&runtime).await;
            let mut probe = stream.clone();
            let msgs = [signal(1), signal(2)];
            for msg in &msgs {
                peer.send(msg).await.unwrap();
            }
            let queued = [
                probe.next().await.unwrap().unwrap(),
                probe.next().await.unwrap().unwrap(),
            ];

            let handover = stream.hand_over().await.unwrap();

            let bytes = |msg: &Message| msg.data().as_ptr();
            assert_eq!(
                handover.queued.iter().map(bytes).collect::<Vec<_>>(),
                queued.iter().map(bytes).collect::<Vec<_>>(),
            );
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            let received = [
                stream.next().await.unwrap().unwrap(),
                stream.next().await.unwrap().unwrap(),
            ];
            assert_eq!(
                received.iter().map(bytes).collect::<Vec<_>>(),
                queued.iter().map(bytes).collect::<Vec<_>>(),
            );
            assert!(
                received[0].recv_position() < received[1].recv_position(),
                "{received:?}"
            );
        });
    }

    /// A connection that took over is handed over in turn with the messages that it was handed
    /// and has yet to yield: those that its stream had queued, then those that its reader had yet
    /// to queue, then what was read after them. A connection that takes over from that hand-over
    /// yields all of them, in order.
    #[test]
    #[timeout(15000)]
    fn a_connection_that_took_over_hands_over_what_it_is_yet_to_yield() {
        under_every_runtime(|runtime| async move {
            let msgs = [1, 2, 3, 4].map(signal);
            let tail = &msgs[3].data()[..10];
            let (ours, peer) = UnixStream::pair().unwrap();
            let mut handover = handover(HandoverSocket::UnixStream(ours), true);
            handover.queued = msgs[..3].to_vec();
            handover.tail = tail.to_vec();
            // The stream of the connection that takes over has room for one message, so its
            // reader waits for room for the second, with the third still to queue.
            let mut stream = on(Builder::take_over(handover), &runtime)
                .max_queued(NonZeroUsize::MIN)
                .build_message_stream()
                .await
                .unwrap();
            let conn = Connection::from(&stream);
            wait_until(|| conn.inner.msg_receiver.is_full());

            let handover = stream.hand_over().await.unwrap();

            assert_eq!(handover.queued.len(), 3);
            assert_eq!(
                unread(&handover),
                [concat(&msgs[..3]), tail.to_vec()].concat()
            );
            drop(conn);
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            (&peer).write_all(&msgs[3].data()[10..]).unwrap();
            for sent in &msgs {
                let received = stream.next().await.unwrap().unwrap();
                assert_eq!(&received.data()[..], &sent.data()[..]);
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
    /// as much as came in is handed over in the allocation of the message, which has room for the
    /// rest of it.
    #[test]
    #[timeout(15000)]
    fn a_long_message_cut_short_is_handed_over() {
        under_every_runtime(|runtime| async move {
            let long = Message::signal(PATH, INTERFACE, "Long")
                .build(&vec![7u8; 10 * 1024])
                .unwrap();

            let room = hand_over_in_the_middle_of(&runtime, &long, 6000).await;

            assert_eq!(
                room,
                long.data().len(),
                "the hand-over has no room for the rest of the message",
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

            assert_eq!(unread(&handover), &msg.data()[..20]);
            let inodes: Vec<_> = handover
                .fds()
                .into_iter()
                .skip(1)
                .map(|fd| inode(&fd))
                .collect();
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
    /// hands over a new descriptor of the socket as a TCP stream, whose record says so.
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

            assert_eq!(saved(&handover).0[24], TCP_STREAM);
            match handover.socket {
                HandoverSocket::TcpStream(socket) => carries_what_is_sent(socket, &peer).await,
                socket => panic!("a TCP socket was handed over as {socket:?}"),
            }
        });
    }

    /// A connection to a `unix:` address hands over a new descriptor of its socket as a unix
    /// stream, whose record says so.
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

            assert_eq!(saved(&handover).0[24], UNIX_STREAM);
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

    /// A hand-over given up on while a send through a write half of your own that can't resume
    /// writes holds the socket leaves the connection stopped: calls fail before they are sent, and
    /// so does a stream for a match rule. The send goes on once the peer reads, and messages that
    /// expect no reply still go out. The messages that the hand-over took off its stream stay with
    /// the stream, so a hand-over through another stream, which would miss them, fails, while one
    /// through the same stream hands the connection over with them.
    #[test]
    #[timeout(15000)]
    fn a_hand_over_given_up_on_goes_on_through_its_stream() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = over_a_socket_pair_of_your_own(&runtime).await;
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

            assert_eq!(unread(&handover), queued);
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

    /// A send through a write half of your own that can't resume writes, under way as the
    /// hand-over starts, which the hand-over waits for, and which fails midway as the peer goes,
    /// fails no hand-over, any more than one that goes through: the peer receives nothing more, so
    /// nothing follows the part of the message in the socket.
    #[test]
    #[timeout(15000)]
    fn a_send_cut_off_midway_as_the_peer_goes_fails_no_hand_over() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer) = over_a_socket_pair_of_your_own(&runtime).await;
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
            assert_eq!(unread(&handed_over.unwrap()), queued);
        });
    }

    /// A send over a socket that zbus drives, cut off midway, as the peer doesn't read, holds no
    /// hand-over up: the hand-over holds the rest of its message, and the connection that takes
    /// over, in this process or through the record of the hand-over, counts it from the start,
    /// and writes it ahead of its first send, once the peer reads. The peer gets the message
    /// whole, then the next one.
    #[test]
    #[timeout(15000)]
    fn the_rest_of_a_send_cut_off_midway_is_handed_over() {
        under_every_runtime(|runtime| async move {
            for record in [false, true] {
                let (mut stream, peer, _probe) = served(&runtime).await;
                let conn = Connection::from(&stream);
                let (big, next) = (big_signal(), signal(1));
                stuck(&runtime, pin!(conn.send(&big)), &peer).await;
                let unwritten = conn.unwritten_len();
                assert!(unwritten > 0, "the send left nothing to write");

                let handover = stream.hand_over().await.unwrap();

                assert_eq!(handover.unwritten_len(), unwritten, "record: {record}");
                assert_eq!(conn.unwritten_len(), 0);
                let handover = if record {
                    through_a_record(handover, 2)
                } else {
                    handover
                };
                let conn = on(Builder::take_over(handover), &runtime)
                    .build()
                    .await
                    .unwrap();
                assert_eq!(conn.unwritten_len(), unwritten, "record: {record}");
                let reading = read_in_the_background(&peer, big.data().len() + next.data().len());
                conn.send(&next).await.unwrap();
                let received = reading.join().unwrap();
                assert_eq!(received, [&big.data()[..], &next.data()[..]].concat());
                assert_eq!(conn.unwritten_len(), 0);
            }
        });
    }

    /// A send that waits for a peer that doesn't read as the hand-over starts holds the hand-over
    /// up no more than one that was cut off: the hand-over has it stop waiting, as it prepares
    /// the connection, and holds what the send kept, the rest of the message that it was in the
    /// middle of and the messages after it, which the connection that takes over writes.
    #[test]
    #[timeout(15000)]
    fn a_send_that_waits_for_the_peer_holds_no_hand_over_up() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer, _probe) = served(&runtime).await;
            let conn = Connection::from(&stream);
            let msgs = [big_signal(), signal(1)];
            let mut sending = pin!(conn.send_all(&msgs));
            stuck(&runtime, sending.as_mut(), &peer).await;

            let (handed_over, sent) = futures_util::future::join(stream.hand_over(), sending).await;

            sent.unwrap();
            let handover = handed_over.unwrap();
            let [big, after] = &msgs;
            let unwritten = handover.unwritten_len();
            assert!(
                after.data().len() < unwritten && unwritten < big.data().len() + after.data().len(),
                "{unwritten} bytes left to write",
            );
            let conn = on(Builder::take_over(handover), &runtime)
                .build()
                .await
                .unwrap();
            let reading = read_in_the_background(&peer, concat(&msgs).len());
            conn.flush().await.unwrap();
            assert_eq!(reading.join().unwrap(), concat(&msgs));
        });
    }

    /// A flush that waits for a peer that doesn't read holds a hand-over up, as it waits for the
    /// peer to take what the connection has left to write: a hand-over given up on meanwhile goes
    /// on through its stream once the flush is through.
    #[test]
    #[timeout(15000)]
    fn a_flush_under_way_holds_the_hand_over_up() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer, _probe) = served(&runtime).await;
            let conn = Connection::from(&stream);
            let big = big_signal();
            stuck(&runtime, pin!(conn.send(&big)), &peer).await;
            let mut flushing = pin!(conn.flush());
            let flushed = futures_lite::future::poll_once(flushing.as_mut()).await;
            assert!(flushed.is_none(), "the flush didn't wait for the peer");

            let given_up = runtime
                .timeout(stream.hand_over(), Duration::from_millis(100))
                .await;

            let Err(Error::InputOutput(e)) = &given_up else {
                panic!("the hand-over didn't wait for the flush: {given_up:?}");
            };
            assert_eq!(e.kind(), ErrorKind::TimedOut, "{e}");
            let reading = read_in_the_background(&peer, big.data().len());
            flushing.await.unwrap();
            assert_eq!(reading.join().unwrap(), &big.data()[..]);
            let handover = stream.hand_over().await.unwrap();
            assert_eq!(handover.unwritten_len(), 0);
        });
    }

    /// The messages that sends kept once a hand-over of the connection was prepared are handed over
    /// with their file descriptors, which `fds` gives last, after that of the socket and those of
    /// what was read, and the connection that takes over, through the record of the hand-over,
    /// writes each with its own.
    #[test]
    #[timeout(15000)]
    fn messages_left_to_write_are_handed_over_with_their_descriptors() {
        under_every_runtime(|runtime| async move {
            let (mut stream, peer, _probe) = served(&runtime).await;
            let conn = Connection::from(&stream);
            let pipes = [pipe(), pipe(), pipe()];
            let with = |i: usize| {
                Message::signal(PATH, INTERFACE, "WithFd")
                    .build(&crate::Fd::from(&pipes[i]))
                    .unwrap()
            };
            let received = with(0);
            send_with_fds(&peer, &received.data()[..], &[pipes[0].as_fd()]);
            wait_until(|| conn.inner.msg_receiver.len() == 1);
            assert!(conn.prepare_hand_over());
            let kept = [big_signal(), with(1), with(2)];
            conn.send_all(&kept).await.unwrap();

            let handover = stream.hand_over().await.unwrap();

            assert!(handover.unwritten_len() > concat(&kept[1..]).len());
            let inodes: Vec<_> = handover
                .fds()
                .into_iter()
                .skip(1)
                .map(|fd| inode(&fd))
                .collect();
            assert_eq!(inodes, pipes.each_ref().map(inode));
            drop(conn);
            let handover = through_a_record(handover, 6);
            let mut stream = on(Builder::take_over(handover), &runtime)
                .build_message_stream()
                .await
                .unwrap();
            let conn = Connection::from(&stream);
            let msg = stream.next().await.unwrap().unwrap();
            assert_eq!(&msg.data()[..], &received.data()[..]);
            let mut peer = Reader::new(Arc::new(registered(&runtime, peer, UnixOps).unwrap()));
            let receiving = async {
                let mut msgs = vec![];
                for seq in 0..kept.len() {
                    let msg = peer
                        .receive_message(seq as u64, &mut vec![], &mut vec![])
                        .await;
                    msgs.push(msg.unwrap());
                }

                msgs
            };
            let (flushed, msgs) = futures_util::future::join(conn.flush(), receiving).await;
            flushed.unwrap();
            for (msg, sent) in msgs.iter().zip(&kept) {
                assert_eq!(&msg.data()[..], &sent.data()[..]);
                let inodes: Vec<_> = msg.data().fds().iter().map(inode).collect();
                let sent: Vec<_> = sent.data().fds().iter().map(inode).collect();
                assert_eq!(inodes, sent);
            }
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
            assert_eq!(handover.saved_len(), None, "{handover:?}");
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
                    // A channel has no descriptor to pass on.
                    assert_eq!(handover.saved_len(), None, "{handover:?}");
                    assert_eq!(handover.fds().len(), 0);
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

            assert_eq!(unread(&handover), &msgs[1].data()[..10]);
            assert_eq!(handover.saved_len(), None, "{handover:?}");
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

    /// A hand-over of a connection to a bus isn't taken over as a peer-to-peer connection.
    #[test]
    #[timeout(15000)]
    fn a_bus_connection_isnt_taken_over_as_a_peer_to_peer_one() {
        under_every_runtime(|runtime| async move {
            let (ours, _theirs) = Channel::pair();
            let handover = handover(HandoverSocket::Socket(ours.into()), false);

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
                let mut handover = handover(HandoverSocket::Socket(ours.into()), true);
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

    /// A connection to a bus that takes over, in this process or through the record of the
    /// hand-over, has the unique name of the hand-over, and says no `Hello`: the first message that
    /// the bus gets from it is the first one that it sends.
    #[test]
    #[timeout(15000)]
    fn a_bus_connection_takes_over_with_its_unique_name() {
        under_every_runtime(|runtime| async move {
            for record in [false, true] {
                // A peer-to-peer connection plays the bus.
                let (mut bus_stream, ours) = over_a_socket_pair(&runtime).await;
                let handover = handover(HandoverSocket::UnixStream(ours), false);
                let handover = if record {
                    through_a_record(handover, 1)
                } else {
                    handover
                };

                let conn = on(Builder::take_over(handover), &runtime)
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
            }
        });
    }

    /// An interface served on a connection to a bus that took over gets the method calls to the
    /// unique name of the hand-over.
    #[cfg(feature = "service")]
    #[test]
    #[timeout(15000)]
    fn a_bus_connection_that_took_over_serves_calls_to_its_unique_name() {
        under_every_runtime(|runtime| async move {
            let (bus, ours) = over_a_socket_pair(&runtime).await;
            let bus = Connection::from(&bus);

            let _conn = on(
                Builder::take_over(handover(HandoverSocket::UnixStream(ours), false)),
                &runtime,
            )
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

    /// A method call that a connection left unread, queued on its stream or in what it read after
    /// that, goes to the interface served on the connection that takes over, which replies to it.
    #[cfg(feature = "service")]
    #[test]
    #[timeout(15000)]
    fn a_method_call_left_unread_is_served_once_taken_over() {
        under_every_runtime(|runtime| async move {
            for queued in [true, false] {
                let (mut peer_stream, ours) = over_a_socket_pair(&runtime).await;
                let call = Message::method_call(PATH, "Echo")
                    .interface(INTERFACE)
                    .build(&"left unread")
                    .unwrap();
                let mut handover = handover(HandoverSocket::UnixStream(ours), true);
                if queued {
                    handover.queued = vec![call.clone()];
                } else {
                    handover.tail = call.data().to_vec();
                }

                let _conn = on(Builder::take_over(handover), &runtime)
                    .serve_at(PATH, Echo)
                    .build()
                    .await
                    .unwrap();

                let reply = peer_stream.next().await.unwrap().unwrap();
                assert_eq!(reply.message_type(), Type::MethodReturn, "queued: {queued}");
                assert_eq!(
                    reply.header().reply_serial(),
                    Some(call.primary_header().serial_num()),
                );
                assert_eq!(reply.body().deserialize::<&str>().unwrap(), "left unread");
            }
        });
    }

    /// A connection that checks the bodies of the messages that it receives checks those that it
    /// takes over too, whether they were queued on the stream of the connection that was handed
    /// over or in what it read after them.
    #[cfg(feature = "bus-impl")]
    #[test]
    #[timeout(15000)]
    fn the_bodies_of_messages_left_unread_are_checked() {
        under_every_runtime(|runtime| async move {
            for queued in [true, false] {
                let (_peer, ours) = UnixStream::pair().unwrap();
                let builder = Message::signal(PATH, INTERFACE, "Test");
                // SAFETY: The body carries no file descriptors, and the connection is to check it.
                let invalid =
                    unsafe { builder.build_raw_body(&[2, 0, 0, 0], "b", vec![]) }.unwrap();
                let valid = signal(1);
                let mut handover = handover(HandoverSocket::UnixStream(ours), true);
                if queued {
                    handover.queued = vec![valid.clone(), invalid];
                } else {
                    handover.tail = concat(&[valid.clone(), invalid]);
                }

                let mut stream = on(Builder::take_over(handover).validate_bodies(), &runtime)
                    .build_message_stream()
                    .await
                    .unwrap();

                let received = stream.next().await.unwrap().unwrap();
                assert_eq!(&received.data()[..], &valid.data()[..], "queued: {queued}");
                let error = stream.next().await.unwrap().unwrap_err();
                assert!(matches!(error, Error::Failure(_)), "{error:?}");
                assert!(
                    stream.next().await.is_none(),
                    "the stream outlived its error"
                );
            }
        });
    }

    /// What a connection that was handed over took to send and didn't write, the rest of a
    /// message that a write ended in the middle of and whole messages after it, the connection
    /// that takes over, in this process or through the record of the hand-over, counts from the
    /// start, and writes ahead of its first send or flush, and not before: the peer gets the
    /// message whole, the others with their descriptors, then what the connection sends.
    #[test]
    #[timeout(15000)]
    fn what_was_left_unwritten_is_written_ahead_of_the_first_send() {
        under_every_runtime(|runtime| async move {
            for (record, flush) in [(false, false), (false, true), (true, false), (true, true)] {
                let case = format!("record: {record}, flush: {flush}");
                let (ours, peer) = UnixStream::pair().unwrap();
                let (cut, next) = (signal(1), signal(4));
                let kept = [with_fd(2), signal(3)];
                let mut handover = handover(HandoverSocket::UnixStream(ours), true);
                handover.unix_fd_passing = true;
                handover.unwritten = Unwritten {
                    cut: Some((cut.data().clone(), 10)),
                    kept: kept.iter().cloned().collect(),
                };
                let unwritten = cut.data().len() - 10 + concat(&kept).len();
                assert_eq!(handover.unwritten.len(), unwritten);
                let handover = if record {
                    through_a_record(handover, 7)
                } else {
                    handover
                };

                let conn = on(Builder::take_over(handover), &runtime)
                    .build()
                    .await
                    .unwrap();

                assert_eq!(conn.unwritten_len(), unwritten, "{case}");
                assert_nothing_to_read(&peer);
                if flush {
                    conn.flush().await.unwrap();
                } else {
                    conn.send(&next).await.unwrap();
                }
                assert_eq!(conn.unwritten_len(), 0, "{case}");
                let mut peer = Reader::new(Arc::new(registered(&runtime, peer, UnixOps).unwrap()));
                // The peer got the start of the message that was cut before the hand-over.
                let mut start = cut.data()[..10].to_vec();
                let mut sent = vec![&cut];
                sent.extend(&kept);
                if !flush {
                    sent.push(&next);
                }
                for (seq, sent) in sent.into_iter().enumerate() {
                    let received = peer
                        .receive_message(seq as u64, &mut start, &mut vec![])
                        .await
                        .unwrap();
                    assert_eq!(&received.data()[..], &sent.data()[..], "{case}");
                    assert_eq!(
                        received.data().fds().len(),
                        sent.data().fds().len(),
                        "{case}"
                    );
                }
            }
        });
    }

    /// A hand-over that goes through its record and its file descriptors has all that it had,
    /// and a connection that takes over from it goes on where the one that was handed over
    /// stopped, with the messages and the descriptors that it left unread.
    #[test]
    #[timeout(15000)]
    fn a_hand_over_goes_through_its_record() {
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
            let left = unread(&handover);
            let next_serial = handover.next_serial;
            let guid = handover.server_guid.clone();

            let handover = through_a_record(handover, 3);

            assert!(
                matches!(handover.socket, HandoverSocket::UnixStream(_)),
                "a unix socket was restored as {:?}",
                handover.socket,
            );
            assert_eq!(handover.server_guid, guid);
            assert_eq!(handover.unique_name.as_ref().unwrap().as_str(), UNIQUE_NAME);
            assert_eq!(unread(&handover), left);
            let inodes: Vec<_> = handover
                .fds()
                .into_iter()
                .skip(1)
                .map(|fd| inode(&fd))
                .collect();
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
    /// passing of file descriptors, goes through its record too.
    #[test]
    fn a_tcp_hand_over_goes_through_its_record() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let address = socket.local_addr().unwrap();
        let mut handover = handover(HandoverSocket::TcpStream(socket), false);
        handover.unique_name = None;
        handover.next_serial = NonZeroU32::new(77).unwrap();
        let guid = handover.server_guid.clone();

        let handover = through_a_record(handover, 0);

        match &handover.socket {
            HandoverSocket::TcpStream(socket) => assert_eq!(socket.local_addr().unwrap(), address),
            socket => panic!("a TCP socket was restored as {socket:?}"),
        }
        assert_eq!(handover.fds().len(), 1);
        assert_eq!(handover.server_guid, guid);
        assert!(handover.unique_name.is_none());
        assert_eq!(handover.unread_len(), 0);
        assert_eq!(handover.unwritten_len(), 0);
        assert!(!handover.unix_fd_passing);
        assert!(!handover.p2p);
        assert_eq!(handover.next_serial.get(), 77);
    }

    /// A record restores from anywhere in memory, at any address, as it was saved: saved again,
    /// it is the same, to the byte, with its file descriptors in the same order. It is a multiple
    /// of 8 long.
    #[test]
    fn a_record_restores_from_anywhere_in_memory() {
        let (handover, _peer) = every_part();
        let (record, _fds) = saved(&handover);
        assert_eq!(record.len() % 8, 0, "{} bytes of record", record.len());
        let inodes: Vec<_> = handover.fds().into_iter().map(|fd| inode(&fd)).collect();

        let mut handover = handover;
        for at in [1, 2, 3, 4, 5, 6, 7, 8, 13] {
            handover = through_a_record(handover, at);

            assert_eq!(saved(&handover).0, record, "at {at}");
            let restored: Vec<_> = handover.fds().into_iter().map(|fd| inode(&fd)).collect();
            assert_eq!(restored, inodes, "at {at}");
        }
        assert_eq!(handover.queued.len(), 2);
        assert_eq!(handover.tail.len(), 10);
        assert_eq!(handover.unwritten.rest().len(), 15);
        assert_eq!(handover.unwritten.kept.len(), 2);
    }

    /// A record of a later major version of the format, or one that needs a feature of the format
    /// that this version of zbus doesn't know, as a later version may save, is refused as one that
    /// it can't read, while a record of a major version of 0, which there is none of, is not a
    /// record.
    #[test]
    fn a_record_of_a_later_format_is_refused() {
        // A later major version, and the first feature that a record can need.
        for (at, value) in [(8, &(MAJOR + 1).to_le_bytes()[..]), (12, &[1][..])] {
            let (record, fds) =
                changed(|record| record[at..at + value.len()].copy_from_slice(value));

            let restored = Handover::restore(&record, fds);

            assert!(matches!(restored, Err(Error::Unsupported)), "{restored:?}");
        }
        let (record, fds) = changed(|record| record[8..10].copy_from_slice(&[0, 0]));
        assert_invalid_data(Handover::restore(&record, fds));
    }

    /// A record that a later minor version of the format saved, with more fields at the end of its
    /// header, and with flags and a byte of the header that this version doesn't know set, is
    /// restored as one without them.
    #[test]
    fn a_record_with_more_in_its_header_is_restored() {
        let (handover, _peer) = every_part();
        let (record, fds) = saved(&handover);
        let header_len = usize::from(u16::from_le_bytes([record[10], record[11]]));
        let mut later = record[..header_len].to_vec();
        later.extend_from_slice(b"12345678");
        later.extend_from_slice(&record[header_len..]);
        later[10..12].copy_from_slice(&(header_len as u16 + 8).to_le_bytes());
        later[16..24].copy_from_slice(&(record.len() as u64 + 8).to_le_bytes());
        later[25] |= 0x80;
        later[27] = 0xff;

        let restored = Handover::restore(&later, fds).unwrap();

        assert_eq!(saved(&restored).0, record);
    }

    /// Bytes that are not a record that `save` copied, or that come with more or fewer file
    /// descriptors than they count, are refused.
    #[test]
    fn what_is_not_a_record_is_refused() {
        let (handover, _peer) = every_part();
        let (record, _) = saved(&handover);
        let header_len = usize::from(u16::from_le_bytes([record[10], record[11]]));
        // The table, of two queued messages and two left to write, then the bytes read.
        let inbound = header_len + 4 * 8;
        let refused = |what: &str, (record, fds): (Vec<u8>, Vec<OwnedFd>)| {
            let restored = Handover::restore(&record, fds);

            let Err(Error::InputOutput(e)) = &restored else {
                panic!("a record with a wrong {what} was restored: {restored:?}");
            };
            assert_eq!(e.kind(), ErrorKind::InvalidData, "{what}: {e}");
        };
        // Fields, as where they are and a value that the record of `every_part` can't have there.
        let fields: [(&str, usize, &[u8]); 15] = [
            ("magic", 0, b"zbus-hoW"),
            ("odd header", 10, &(header_len as u16 - 4).to_le_bytes()),
            ("short header", 10, &(NAME as u16).to_le_bytes()),
            ("record length", 16, &1u64.to_le_bytes()),
            ("kind", 24, &[2]),
            ("serial", 28, &0u32.to_le_bytes()),
            ("descriptors", 32, &99u32.to_le_bytes()),
            ("tail descriptors", 40, &0u32.to_le_bytes()),
            ("inbound length", 48, &8u64.to_le_bytes()),
            ("rest length", 64, &u64::MAX.to_le_bytes()),
            ("GUID", 72, b"not hex!"),
            ("unique name", NAME, b"org"),
            ("message length", header_len, &17u32.to_le_bytes()),
            ("message descriptors", header_len + 4, &0u32.to_le_bytes()),
            ("message", inbound, b"X"),
        ];
        for (what, at, value) in fields {
            refused(
                what,
                changed(|record| record[at..at + value.len()].copy_from_slice(value)),
            );
        }
        let lengths: [(&str, Change); 5] = [
            ("padding", |record| *record.last_mut().unwrap() = 1),
            ("length, as longer", |record| {
                record.extend_from_slice(&[0; 8])
            }),
            ("length, as shorter", |record| {
                record.truncate(record.len() - 8)
            }),
            ("length, as a header only", |record| record.truncate(NAME)),
            ("length, as nothing", Vec::clear),
        ];
        for (what, change) in lengths {
            refused(what, changed(change));
        }

        let (record, mut fds) = changed(|_| ());
        fds.push(pipe());
        assert_invalid_data(Handover::restore(&record, fds));
        let (record, mut fds) = changed(|_| ());
        fds.pop();
        assert_invalid_data(Handover::restore(&record, fds));
    }

    /// The record of a hand-over is copied into memory that is exactly as long as it, and none
    /// is written otherwise.
    #[test]
    fn a_record_is_saved_only_into_memory_exactly_as_long_as_it() {
        let (handover, _peer) = every_part();
        let len = handover.saved_len().unwrap();

        for len in [len - 1, len + 1, 0] {
            let mut memory = vec![0xa5; len];

            let saved = handover.save(&mut memory);

            let Err(e) = &saved else {
                panic!("a record was saved into {len} bytes: {saved:?}");
            };
            assert_eq!(e.kind(), ErrorKind::InvalidInput, "{e}");
            assert!(
                memory.iter().all(|byte| *byte == 0xa5),
                "the save wrote some"
            );
        }
    }

    /// The hand-over of a connection over a socket of your own, such as a channel, has no record
    /// and no file descriptors to pass on.
    #[test]
    fn a_socket_of_your_own_has_no_record() {
        let (ours, _theirs) = Channel::pair();
        let mut handover = handover(HandoverSocket::Socket(ours.into()), true);
        handover.queued = vec![with_fd(1)];

        let saved = handover.save(&mut []);

        let Err(e) = saved else {
            panic!("a record of a socket of your own was saved");
        };
        assert_eq!(e.kind(), ErrorKind::Unsupported, "{e}");
        assert_eq!(handover.saved_len(), None);
        assert!(handover.fds().is_empty());
    }

    /// A hand-over shows how much it holds rather than what, and can go to another thread.
    #[test]
    fn a_hand_over_shows_how_much_it_holds() {
        fn sendable<T>(_: &T)
        where
            T: Send + Sync,
        {
        }

        let (ours, _theirs) = UnixStream::pair().unwrap();
        let mut handover = handover(HandoverSocket::UnixStream(ours), true);
        handover.tail = vec![b'#'; 10 * 1024];

        let shown = format!("{handover:?}");

        assert!(shown.contains("unread_len: 10240"), "{shown}");
        assert!(!shown.contains("35, 35"), "{shown}");
        sendable(&handover);
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
        assert_eq!(unread(&handover), &cut.data()[..at]);
        assert!(handover.queued.is_empty());
        assert_eq!(handover.fds().len(), 1);
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
        let room = handover.tail.capacity();
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
        assert_eq!(unread(&handover), &cut.data()[..at]);
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

    /// A hand-over of a connection over `socket`, with nothing left unread or unwritten: a
    /// peer-to-peer one if `p2p` says so, and otherwise one to a bus, whose unique name is
    /// `UNIQUE_NAME`.
    fn handover(socket: HandoverSocket, p2p: bool) -> Handover {
        Handover {
            socket,
            server_guid: Guid::generate().into(),
            unique_name: (!p2p).then(|| UNIQUE_NAME.try_into().unwrap()),
            unix_fd_passing: false,
            p2p,
            next_serial: NonZeroU32::MIN,
            queued: vec![],
            tail: vec![],
            tail_fds: vec![],
            unwritten: Unwritten::default(),
        }
    }

    /// A hand-over of a peer-to-peer connection over one end of a new unix socket pair, and the
    /// other end, with some of everything: a unique name, the passing of file descriptors, two
    /// messages queued, the first of which carries a descriptor, the start of a third read after
    /// them, with a descriptor that came with it, and, left to write, the last 15 bytes of a
    /// message that a write ended in the middle of, then two more, the first of which carries a
    /// descriptor.
    fn every_part() -> (Handover, UnixStream) {
        let (ours, theirs) = UnixStream::pair().unwrap();
        let mut handover = handover(HandoverSocket::UnixStream(ours), true);
        handover.unique_name = Some(UNIQUE_NAME.try_into().unwrap());
        handover.unix_fd_passing = true;
        handover.next_serial = NonZeroU32::new(42).unwrap();
        handover.queued = vec![with_fd(1), signal(2)];
        handover.tail = with_fd(3).data()[..10].to_vec();
        handover.tail_fds = vec![pipe()];
        let cut = signal(4);
        let written = cut.data().len() - 15;
        handover.unwritten = Unwritten {
            cut: Some((cut.data().clone(), written)),
            kept: VecDeque::from([with_fd(5), signal(6)]),
        };

        (handover, theirs)
    }

    /// `handover`, passed on through its record, which `save` copies into memory `at` bytes into
    /// a buffer, and its file descriptors, duplicated, as passing them on does: what `restore`
    /// gives back of them.
    fn through_a_record(handover: Handover, at: usize) -> Handover {
        let len = handover
            .saved_len()
            .expect("the hand-over of a socket that zbus drives");
        assert_eq!(len % 8, 0, "{len} bytes of record");
        let mut memory = vec![0xa5; at + len + 7];
        handover.save(&mut memory[at..][..len]).unwrap();
        let fds = duplicates(&handover);
        drop(handover);

        Handover::restore(&memory[at..][..len], fds).unwrap()
    }

    /// The record of `handover`, and duplicates of its file descriptors.
    fn saved(handover: &Handover) -> (Vec<u8>, Vec<OwnedFd>) {
        let mut record = vec![0; handover.saved_len().unwrap()];
        handover.save(&mut record).unwrap();

        (record, duplicates(handover))
    }

    /// The record of the hand-over of [`every_part`], changed by `change`, and duplicates of its
    /// file descriptors.
    fn changed(change: impl Fn(&mut Vec<u8>)) -> (Vec<u8>, Vec<OwnedFd>) {
        let (handover, _peer) = every_part();
        let (mut record, fds) = saved(&handover);
        change(&mut record);

        (record, fds)
    }

    /// A change to the bytes of a record.
    type Change = fn(&mut Vec<u8>);

    /// Duplicates of the file descriptors of `handover`, as passing them on makes.
    fn duplicates(handover: &Handover) -> Vec<OwnedFd> {
        handover
            .fds()
            .into_iter()
            .map(|fd| fd.try_clone_to_owned().unwrap())
            .collect()
    }

    /// The bytes that `handover` left unread: those of the messages that were queued, then those
    /// that were read after them.
    fn unread(handover: &Handover) -> Vec<u8> {
        [concat(&handover.queued), handover.tail.clone()].concat()
    }

    /// The bytes of `msgs`, one after the other.
    fn concat(msgs: &[Message]) -> Vec<u8> {
        msgs.iter().flat_map(|msg| msg.data().to_vec()).collect()
    }

    /// Reads `len` bytes off `peer`, on a thread of its own, as a peer that reads at last.
    fn read_in_the_background(peer: &UnixStream, len: usize) -> thread::JoinHandle<Vec<u8>> {
        let peer = peer.try_clone().unwrap();

        thread::spawn(move || {
            let mut received = vec![0; len];
            (&peer).read_exact(&mut received).unwrap();

            received
        })
    }

    /// Asserts that `peer` has nothing to read, which leaves it in non-blocking mode.
    fn assert_nothing_to_read(peer: &UnixStream) {
        peer.set_nonblocking(true).unwrap();
        let read = (&*peer).read(&mut [0; 1]);
        assert!(
            matches!(&read, Err(e) if e.kind() == ErrorKind::WouldBlock),
            "the peer got {read:?}",
        );
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

    /// Asserts that `result` is the error of a record and file descriptors that are not those of a
    /// hand-over.
    fn assert_invalid_data<T: Debug>(result: Result<T>) {
        let Err(Error::InputOutput(e)) = &result else {
            panic!("expected the error of what is not a record: {result:?}");
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

    /// A signal that carries `n` and the file descriptor of a pipe of its own.
    fn with_fd(n: u32) -> Message {
        Message::signal(PATH, INTERFACE, "WithFd")
            .build(&(n, crate::Fd::from(&pipe())))
            .unwrap()
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
    use std::{
        num::NonZeroU32,
        os::{fd::OwnedFd, unix::net::UnixStream},
    };

    use zoter::{Restore, Save};

    use super::{Handover, HandoverSocket, P2P};
    use crate::{Error, Guid, OwnedGuid, connection::socket_write::Unwritten};

    /// The hand-over of a connection to a bus goes through its record and its file descriptors.
    #[test]
    fn a_bus_hand_over_goes_through_its_record() {
        let handover = bus_handover();
        let guid = handover.server_guid.clone();
        let (record, fds) = saved(&handover);
        drop(handover);

        let handover = Handover::restore(&record, fds).unwrap();

        assert!(
            matches!(handover.socket, HandoverSocket::UnixStream(_)),
            "a unix socket was restored as {:?}",
            handover.socket,
        );
        assert_eq!(handover.server_guid, guid);
        assert_eq!(handover.unique_name.unwrap().as_str(), ":1.42");
        assert_eq!(handover.tail, [1, 2, 3]);
        assert!(handover.tail_fds.is_empty());
        assert!(handover.unix_fd_passing);
        assert_eq!(handover.next_serial.get(), 77);
    }

    /// The record of a peer-to-peer connection isn't restored in a build that can't build one.
    #[test]
    fn a_peer_to_peer_record_is_not_restored() {
        let (mut record, fds) = saved(&bus_handover());
        // The flags of the record.
        record[25] |= P2P;

        let restored = Handover::restore(&record, fds);

        assert!(matches!(restored, Err(Error::Unsupported)), "{restored:?}");
    }

    /// A hand-over of a connection to a bus over one end of a new unix socket pair, with a few
    /// bytes left unread.
    fn bus_handover() -> Handover {
        let (ours, _theirs) = UnixStream::pair().unwrap();

        Handover {
            socket: HandoverSocket::UnixStream(ours),
            server_guid: guid(),
            unique_name: Some(":1.42".try_into().unwrap()),
            unix_fd_passing: true,
            next_serial: NonZeroU32::new(77).unwrap(),
            queued: vec![],
            tail: vec![1, 2, 3],
            tail_fds: vec![],
            unwritten: Unwritten::default(),
        }
    }

    /// The record of `handover`, and duplicates of its file descriptors.
    fn saved(handover: &Handover) -> (Vec<u8>, Vec<OwnedFd>) {
        let mut record = vec![0; handover.saved_len().unwrap()];
        handover.save(&mut record).unwrap();
        let fds = handover
            .fds()
            .into_iter()
            .map(|fd| fd.try_clone_to_owned().unwrap())
            .collect();

        (record, fds)
    }

    /// The GUID of a server, which a build without the `p2p` feature can't generate.
    fn guid() -> OwnedGuid {
        Guid::from_static_str("0123456789abcdef0123456789abcdef")
            .unwrap()
            .into()
    }
}
