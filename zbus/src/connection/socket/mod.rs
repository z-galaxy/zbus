//! The transport layer of a connection.
//!
//! Implement [`Socket`], and the [`ReadHalf`] and [`WriteHalf`] that it splits into, to run a
//! connection over a transport of your own.

#[cfg(feature = "p2p")]
pub mod channel;
#[cfg(feature = "p2p")]
pub use channel::Channel;

mod split;
pub(crate) use split::Writes;
pub use split::{BoxedSplit, Split};

use std::{io, mem};

#[cfg(unix)]
use crate::utils::FDS_MAX;
use crate::{
    Message,
    conn::AuthMechanism,
    fdo::ConnectionCredentials,
    log::trace,
    message::{
        PrimaryHeader,
        header::{MAX_MESSAGE_SIZE, MIN_MESSAGE_SIZE},
    },
    padding_for_8_bytes,
    wire::{
        Endian,
        serialized::{self, Context},
    },
};
#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

#[cfg(unix)]
pub(crate) type RecvmsgResult = io::Result<(usize, Vec<OwnedFd>)>;

#[cfg(not(unix))]
pub(crate) type RecvmsgResult = io::Result<usize>;

/// Trait representing some transport layer over which the DBus protocol can be used.
///
/// In order to allow simultaneous reading and writing, this trait requires you to split the socket
/// into a read half and a write half. The reader and writer halves can be any types that implement
/// [`ReadHalf`] and [`WriteHalf`] respectively.
///
/// A unix or TCP stream needs none of this: hand the socket itself to [`Builder::unix_stream`] or
/// [`Builder::tcp_stream`] and the connection drives it on its own runtime. Implement this trait
/// for a transport that is neither, such as a VSOCK stream, an in-process channel or a tunnel of
/// your own, and hand it to [`Builder::socket`] or [`Builder::authenticated_socket`].
///
/// # Example
///
/// An implementation for any stream of Tokio's, the `VsockStream` of the [tokio-vsock] crate
/// among them:
///
/// ```
/// # #[cfg(unix)]
/// # mod example {
/// use std::{
///     fmt::Debug,
///     io,
///     os::fd::{BorrowedFd, OwnedFd},
/// };
///
/// use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
/// use zbus::{
///     connection::{
///         AuthMechanism,
///         socket::{ReadHalf, Socket, Split, WriteHalf},
///     },
///     export::async_trait::async_trait,
/// };
///
/// /// A stream of Tokio's, as a socket for a connection to run over.
/// #[derive(Debug)]
/// pub struct Stream<S>(pub S);
///
/// impl<S> Socket for Stream<S>
/// where
///     S: AsyncRead + AsyncWrite + Debug + Send + Sync + 'static,
/// {
///     type ReadHalf = Reader<S>;
///     type WriteHalf = Writer<S>;
///
///     fn split(self) -> Split<Reader<S>, Writer<S>> {
///         let (read, write) = tokio::io::split(self.0);
///
///         Split::new(Reader(read), Writer(write))
///     }
/// }
///
/// /// The half of a `Stream` that a connection receives through.
/// #[derive(Debug)]
/// pub struct Reader<S>(tokio::io::ReadHalf<S>);
///
/// #[async_trait]
/// impl<S> ReadHalf for Reader<S>
/// where
///     S: AsyncRead + Debug + Send + Sync + 'static,
/// {
///     async fn recvmsg(&mut self, buffer: &mut [u8]) -> io::Result<(usize, Vec<OwnedFd>)> {
///         let read = self.0.read(buffer).await?;
///
///         // Only bytes come over the stream, never file descriptors.
///         Ok((read, vec![]))
///     }
///
///     fn auth_mechanism(&self) -> AuthMechanism {
///         // The peer has no credentials to check.
///         AuthMechanism::Anonymous
///     }
/// }
///
/// /// The half of a `Stream` that a connection sends through.
/// #[derive(Debug)]
/// pub struct Writer<S>(tokio::io::WriteHalf<S>);
///
/// #[async_trait]
/// impl<S> WriteHalf for Writer<S>
/// where
///     S: AsyncWrite + Debug + Send + Sync + 'static,
/// {
///     async fn sendmsg(&mut self, buffer: &[u8], _fds: &[BorrowedFd<'_>]) -> io::Result<usize> {
///         // There are never any file descriptors to send: neither half says it can pass them,
///         // so the connection turns away any message that carries some.
///         self.0.write(buffer).await
///     }
///
///     async fn flush(&mut self) -> io::Result<()> {
///         // zbus calls this once it has handed over a whole message, so that a stream that
///         // buffers sends it on.
///         self.0.flush().await
///     }
///
///     async fn close(&mut self) -> io::Result<()> {
///         self.0.shutdown().await
///     }
///
///     fn can_resume_writes(&self) -> bool {
///         // `write` takes nothing in a call that is dropped before it returns, and `flush` sends
///         // on what a buffered stream holds, so a send can stop midway and go on later.
///         true
///     }
/// }
/// # }
/// #
/// # // Any stream of Tokio's carries a connection through the halves above, a buffered one too.
/// # #[cfg(all(unix, feature = "p2p"))]
/// # #[tokio::main]
/// # async fn main() -> zbus::Result<()> {
/// #     use tokio::io::BufStream;
/// #     use zbus::{Guid, connection::Builder};
/// #
/// #     let (p0, p1) = tokio::net::UnixStream::pair()?;
/// #     let (p0, p1) = (BufStream::new(p0), BufStream::new(p1));
/// #     let (_client, _server) = futures_util::try_join!(
/// #         Builder::socket(example::Stream(p0)).p2p().build(),
/// #         Builder::socket(example::Stream(p1)).server(Guid::generate()).p2p().build(),
/// #     )?;
/// #
/// #     Ok(())
/// # }
/// #
/// # #[cfg(not(all(unix, feature = "p2p")))]
/// # fn main() {}
/// ```
///
/// `Builder::socket(Stream(stream))` then builds a connection over `stream`. A stream of Tokio's
/// is driven by the Tokio runtime it was created in, so create the stream and build the
/// connection inside that runtime; with the `tokio` feature, the connection runs on that runtime
/// as well. As the peer on such a stream has no credentials to check, the read half asks for the
/// `ANONYMOUS` authentication mechanism, which a bus at the other end has to allow.
///
/// [`Builder::unix_stream`]: crate::connection::Builder::unix_stream
/// [`Builder::tcp_stream`]: crate::connection::Builder::tcp_stream
/// [`Builder::socket`]: crate::connection::Builder::socket
/// [`Builder::authenticated_socket`]: crate::connection::Builder::authenticated_socket
/// [tokio-vsock]: https://docs.rs/tokio-vsock
pub trait Socket {
    /// The type of the read half that [`Socket::split`] returns.
    type ReadHalf: ReadHalf;
    /// The type of the write half that [`Socket::split`] returns.
    type WriteHalf: WriteHalf;

    /// Split the socket into a read half and a write half.
    fn split(self) -> Split<Self::ReadHalf, Self::WriteHalf>
    where
        Self: Sized;
}

/// The read half of a socket.
///
/// See [`Socket`] for more details.
#[async_trait::async_trait]
pub trait ReadHalf: std::fmt::Debug + Send + Sync + 'static {
    /// Receive a message on the socket.
    ///
    /// This is the higher-level method to receive a full D-Bus message.
    ///
    /// The default implementation uses `recvmsg` to receive the message. Implementers should
    /// override either this or `recvmsg`. Note that if you override this method, zbus will not be
    /// able perform an authentication handshake and hence will skip the handshake. Therefore your
    /// implementation will only be useful for pre-authenticated connections or connections that do
    /// not require authentication.
    ///
    /// # Parameters
    ///
    /// - `seq`: The sequence number of the message. The returned message should have this sequence.
    /// - `already_received_bytes`: Sometimes, zbus already received some bytes from the socket
    ///   belonging to the first message(s) (as part of the connection handshake process). This is
    ///   the buffer containing those bytes (if any). If you're implementing this method, most
    ///   likely you can safely ignore this parameter. With the `handover` feature, on Linux, a half
    ///   that can take what it left unread (`ReadHalf::can_take_unread`) is the exception: it has
    ///   to take these ahead of anything that it reads, or fail on them if it can't make messages
    ///   of them, as they are also how a connection that takes over (`Builder::take_over`) gets
    ///   what the other one left unread.
    /// - `already_received_fds`: Same goes for file descriptors belonging to first messages.
    ///
    /// # Cancellation
    ///
    /// With the `handover` feature, on Linux, a hand-over (`MessageStream::hand_over`) stops the
    /// connection reading by dropping the future of this method wherever it waits, if
    /// `ReadHalf::can_take_unread` says that it can.
    ///
    /// The default implementation keeps what it read of a message in `already_received_bytes`
    /// and `already_received_fds`, and never reads past the end of the message. So a call that is
    /// dropped while it waits loses nothing, as long as a call to `recvmsg` that is dropped while
    /// it waits loses nothing either: whatever that call took from the transport, the next one
    /// brings. The next call to this method, given the same two, then goes on from where the
    /// dropped one stopped. A `recvmsg` that reads with the `read` of Tokio's `AsyncReadExt`, as
    /// the one in the example of [`Socket`] does, loses nothing when dropped, while one that
    /// reads with its `read_exact` can lose what it read.
    async fn receive_message(
        &mut self,
        seq: u64,
        already_received_bytes: &mut Vec<u8>,
        #[cfg(unix)] already_received_fds: &mut Vec<OwnedFd>,
    ) -> crate::Result<Message> {
        // The primary header says how long the whole message is, so it is read on its own first,
        // and the message gets an allocation of its exact length once it is in. Each read is
        // straight into `already_received_bytes`, no further than the end of the message, and
        // into room for no more than `READ_MAX` bytes of it.
        let (primary_header, len) = loop {
            let have = already_received_bytes.len();
            let want = if have < MIN_MESSAGE_SIZE {
                MIN_MESSAGE_SIZE
            } else {
                let (primary_header, len) = read_primary_header(already_received_bytes)?;
                if have >= len {
                    break (primary_header, len);
                }

                len
            };
            already_received_bytes.reserve_exact(want - have);
            let mut room = Room::new(already_received_bytes, want.min(have + READ_MAX));
            let received = self.recvmsg(room.unfilled()).await;
            #[cfg(unix)]
            let (read, fds) = received?;
            #[cfg(not(unix))]
            let read = received?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to receive message",
                )
                .into());
            }
            room.filled(read);
            #[cfg(unix)]
            {
                // The descriptors wait in the order they came in, for the messages to take them.
                already_received_fds.extend(fds);
                if already_received_fds.len() > FDS_MAX {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "too many file descriptors that no header counts",
                    )
                    .into());
                }
            }
        };

        take_message(
            seq,
            &primary_header,
            len,
            already_received_bytes,
            #[cfg(unix)]
            already_received_fds,
        )
    }

    /// Attempt to receive bytes from the socket.
    ///
    /// On success, returns the number of bytes read as well as a `Vec` containing
    /// any associated file descriptors.
    ///
    /// The default implementation simply panics. Implementers must override either `read_message`
    /// or this method.
    async fn recvmsg(&mut self, _buf: &mut [u8]) -> RecvmsgResult {
        unimplemented!("`ReadHalf` implementers must either override `read_message` or `recvmsg`");
    }

    /// Return whether passing file descriptors is supported.
    ///
    /// Default implementation returns `false`.
    fn can_pass_unix_fd(&self) -> bool {
        false
    }

    /// The peer credentials.
    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        Ok(ConnectionCredentials::default())
    }

    /// The authentication mechanism to use for this socket on the target OS.
    ///
    /// Default is `AuthMechanism::External`.
    fn auth_mechanism(&self) -> AuthMechanism {
        AuthMechanism::External
    }

    /// Whether this half can take out what it read off the socket and is yet to return in a
    /// message, through [`ReadHalf::take_unread`], for its connection to be handed over
    /// ([`MessageStream::hand_over`]).
    ///
    /// A half that returns `true` promises four things:
    ///
    /// * [`ReadHalf::receive_message`] is cancel-safe. Its connection drops the future of a call
    ///   wherever it waits, and whatever the call read off the socket by then has to be kept,
    ///   either in `already_received_bytes` and `already_received_fds` or in the half, for
    ///   `take_unread` to take out.
    /// * `receive_message` takes `already_received_bytes` and `already_received_fds` ahead of
    ///   anything that it reads, or fails on them if it can't make messages of them, rather than
    ///   leave them behind. That is how a connection that takes over ([`Builder::take_over`]) gets
    ///   what the other one left unread.
    /// * `receive_message` fails with an I/O error of kind
    ///   [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) at the end of the input, once the peer
    ///   closed its end of the socket, as the default implementation does, and with no error of
    ///   that kind otherwise. A connection that comes to that end can still be handed over, with
    ///   what it read, as the peer took nothing away from that, while one that ends on any other
    ///   error can't.
    /// * Nothing underneath the half holds on to bytes of the socket that `take_unread` can't take
    ///   out, as a buffered stream does.
    ///
    /// A half that implements [`ReadHalf::recvmsg`] so that a call dropped while it waits loses
    /// nothing (see the cancellation section of [`ReadHalf::receive_message`]), and that reads
    /// nothing at the end of the input, and leaves `receive_message` to the default
    /// implementation, keeps the first three promises, and holds nothing for `take_unread` to take
    /// out.
    ///
    /// The read half of a `Channel` returns `true`. A channel carries whole messages, so it takes
    /// the whole messages that it is given as already received ahead of those that come over the
    /// channel, and fails on part of one. The end of its input is the other end of the channel
    /// being closed.
    ///
    /// The default implementation returns `false`. Then [`MessageStream::hand_over`] fails with
    /// [`Error::Unsupported`] for a connection over this half.
    ///
    /// This method is only available on Linux, with the `handover` feature.
    ///
    /// [`Builder::take_over`]: crate::connection::Builder::take_over
    /// [`Error::Unsupported`]: crate::Error::Unsupported
    /// [`MessageStream::hand_over`]: crate::MessageStream::hand_over
    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn can_take_unread(&self) -> bool {
        false
    }

    /// Move what this half read off the socket, and is yet to return in a message, to the end of
    /// `bytes`. Move the file descriptors that came in with it, and that no message took, to the
    /// end of `fds`. Both go in the order that they came in.
    ///
    /// A connection calls this once, as it stops reading for a hand-over
    /// ([`MessageStream::hand_over`]), if [`ReadHalf::can_take_unread`] says that it can. That
    /// comes after its last call to [`ReadHalf::receive_message`], which it may have dropped while
    /// that call waited. `bytes` and `fds` then hold the `already_received_bytes` and
    /// `already_received_fds` that the connection kept for its next call.
    ///
    /// The default implementation moves nothing. That suits a half that keeps nothing of its own
    /// from one call of `receive_message` to the next, such as one that leaves `receive_message`
    /// to the default implementation.
    ///
    /// This method is only available on Linux, with the `handover` feature.
    ///
    /// [`MessageStream::hand_over`]: crate::MessageStream::hand_over
    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn take_unread(&mut self, bytes: &mut Vec<u8>, fds: &mut Vec<OwnedFd>) {
        let _ = (bytes, fds);
    }
}

/// The write half of a socket.
///
/// See [`Socket`] for more details.
#[async_trait::async_trait]
pub trait WriteHalf: std::fmt::Debug + Send + Sync + 'static {
    /// Send a message on the socket.
    ///
    /// This is the higher-level method to send a full D-Bus message.
    ///
    /// The default implementation uses `sendmsg` to send the message, and then `flush`.
    /// Implementers should override either this or `sendmsg`. A connection over a half that can
    /// resume writes ([`WriteHalf::can_resume_writes`]) doesn't call this.
    async fn send_message(&mut self, msg: &Message) -> crate::Result<()> {
        let data = msg.data();
        let serial = msg.primary_header().serial_num();

        trace!("Sending message: {:?}", msg);
        let mut pos = 0;
        while pos < data.len() {
            #[cfg(unix)]
            let fds = if pos == 0 {
                data.fds().iter().map(|f| f.as_fd()).collect()
            } else {
                vec![]
            };
            pos += self
                .sendmsg(
                    &data[pos..],
                    #[cfg(unix)]
                    &fds,
                )
                .await?;
        }
        self.flush().await?;
        trace!("Sent message with serial: {}", serial);

        Ok(())
    }

    /// Send several messages on the socket, in order.
    ///
    /// This is the higher-level method to send several full D-Bus messages, which a socket may hand
    /// over with fewer writes than it takes to send them one by one.
    ///
    /// The default implementation sends them one by one with `send_message`. Implementers that can
    /// write several messages at once should override it. File descriptors travel with the write
    /// they are sent with, so a message with file descriptors has to go in a write of its own,
    /// which sends them along: a bus such as dbus-broker gives the file descriptors that come with
    /// a read to the message that the read ends in.
    ///
    /// If this fails, the messages before the one that it failed on have been sent in full, and
    /// that one may have been sent in part.
    ///
    /// A connection over a half that can resume writes ([`WriteHalf::can_resume_writes`]) doesn't
    /// call this.
    async fn send_messages(&mut self, msgs: &[Message]) -> crate::Result<()> {
        for msg in msgs {
            self.send_message(msg).await?;
        }

        Ok(())
    }

    /// Attempt to send a message on the socket
    ///
    /// On success, return the number of bytes written. There may be a partial write, in
    /// which case the caller is responsible for sending the remaining data by calling this
    /// method again until everything is written, or it fails.
    ///
    /// If at least one byte has been written, then all the provided file descriptors will
    /// have been sent as well, and should not be provided again in subsequent calls.
    ///
    /// If the underlying transport does not support transmitting file descriptors, this
    /// will return `Err(ErrorKind::InvalidInput)`.
    ///
    /// The default implementation simply panics. Implementers must override either `send_message`
    /// or this method. A half that can resume writes ([`WriteHalf::can_resume_writes`]) must
    /// override this one, which zbus then calls directly.
    async fn sendmsg(
        &mut self,
        _buffer: &[u8],
        #[cfg(unix)] _fds: &[BorrowedFd<'_>],
    ) -> io::Result<usize> {
        unimplemented!("`WriteHalf` implementers must either override `send_message` or `sendmsg`");
    }

    /// Flush whatever the socket holds on to of the bytes `sendmsg` was given.
    ///
    /// The default `send_message` calls this once it has handed a whole message to `sendmsg`, and a
    /// connection over a half that can resume writes ([`WriteHalf::can_resume_writes`]) once a send
    /// has handed it all of its messages. The authentication handshake calls it each time before
    /// it waits for the peer to answer, so the commands it pipelines go out together, and
    /// [`Connection::flush`] calls it last. A socket that holds on to what it is given, such as one
    /// over a buffered or encrypted stream, has to send it on here: zbus has no other way to flush
    /// it, and the peer would never see it otherwise.
    ///
    /// The default implementation does nothing, which suits a socket that sends what it is given
    /// right away.
    ///
    /// [`Connection::flush`]: crate::Connection::flush
    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// The dbus daemon on `freebsd` and `dragonfly` currently requires sending the zero byte
    /// as a separate message with SCM_CREDS, as part of the `EXTERNAL` authentication on unix
    /// sockets. This method is used by the authentication machinery in zbus to send this
    /// zero byte. Socket implementations based on unix sockets should implement this method.
    #[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
    async fn send_zero_byte(&mut self) -> io::Result<Option<usize>> {
        Ok(None)
    }

    /// Close the socket.
    ///
    /// After this call, it is valid for all reading and writing operations to fail.
    async fn close(&mut self) -> io::Result<()>;

    /// Whether passing file descriptors is supported.
    ///
    /// Default implementation returns `false`.
    fn can_pass_unix_fd(&self) -> bool {
        false
    }

    /// The peer credentials.
    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        Ok(ConnectionCredentials::default())
    }

    /// Whether writes through this half can be resumed: whether, once a send on the connection is
    /// dropped, or fails, in the middle of a message, the next send or [`Connection::flush`] can
    /// write the rest of that message.
    ///
    /// For that, zbus writes through [`WriteHalf::sendmsg`] itself, a message at a time, counting
    /// what each call takes, and calls neither [`WriteHalf::send_message`] nor
    /// [`WriteHalf::send_messages`]. A half that returns `true` promises that `sendmsg`:
    ///
    /// * takes the bytes that it is given as they are, in order, for the peer;
    /// * returns exactly how many of them it took;
    /// * sends the file descriptors that it is given along with the first byte that it takes;
    /// * takes nothing in a call whose future is dropped before it returns;
    ///
    /// and that [`WriteHalf::flush`] sends on all that `sendmsg` took and the half still holds, and
    /// loses none of it in a call whose future is dropped before it returns, which leaves it for
    /// the next call to `flush`.
    ///
    /// zbus hands `sendmsg` the file descriptors of each message along with its first byte, and
    /// calls `flush` once a send wrote all of its messages. A call to `sendmsg` that takes none of
    /// what it is given fails the send, with an I/O error of kind
    /// [`WriteZero`](io::ErrorKind::WriteZero). A send that is dropped, or that fails, midway
    /// leaves the rest of its message for the connection to write ahead of anything else, as
    /// [`Connection::send`] tells.
    ///
    /// With the `handover` feature, on Linux, once a hand-over of the connection is prepared
    /// (`Connection::prepare_hand_over`), a send drops the future of a call to `sendmsg` or `flush`
    /// that waits, rather than wait for it, and the connection keeps what is left, for a later send
    /// or [`Connection::flush`], or for a hand-over to hand over.
    ///
    /// The default implementation returns `false`, which suits a half that sends whole messages
    /// through `send_message` and `send_messages`. Over such a half, a send that is dropped, or
    /// that fails, in the middle of a message can leave part of it on the socket, after which the
    /// peer can make no sense of anything that the connection sends.
    ///
    /// With the `handover` feature, on Linux, a hand-over (`MessageStream::hand_over`) of a
    /// connection over such a half waits for a send under way, and fails once one was cut off
    /// midway. `Connection::prepare_hand_over` returns `false` for such a connection, as its sends
    /// have to wait for the half.
    ///
    /// zbus asks this once, as the connection is built.
    ///
    /// [`Connection::flush`]: crate::Connection::flush
    /// [`Connection::send`]: crate::Connection::send
    fn can_resume_writes(&self) -> bool {
        false
    }
}

/// The primary header that `bytes` start with, and the length of the whole message it opens.
///
/// A message longer than the D-Bus specification allows is an error.
pub(crate) fn read_primary_header(bytes: &[u8]) -> crate::Result<(PrimaryHeader, usize)> {
    let (primary_header, fields_len) = PrimaryHeader::read(bytes)?;
    let header_len = MIN_MESSAGE_SIZE + fields_len as usize;
    let body_padding = padding_for_8_bytes(header_len);
    let body_len = primary_header.body_len() as usize;
    let total_len = header_len + body_padding + body_len;
    if total_len > MAX_MESSAGE_SIZE {
        return Err(crate::Error::ExcessData);
    }

    Ok((primary_header, total_len))
}

/// Take the message that `bytes` start with, which is whole in them, `len` bytes long as
/// `primary_header` says, out of them, with as many of `fds` as its header counts.
pub(crate) fn take_message(
    seq: u64,
    primary_header: &PrimaryHeader,
    len: usize,
    bytes: &mut Vec<u8>,
    #[cfg(unix)] fds: &mut Vec<OwnedFd>,
) -> crate::Result<Message> {
    // Nothing is allocated when nothing follows the message.
    let rest = bytes.split_off(len);
    let bytes = mem::replace(bytes, rest);

    // The message takes as many of the descriptors that wait as its header counts, whichever of
    // its bytes they came with, as the reference implementation of D-Bus does.
    #[cfg(unix)]
    let fds = if fds.is_empty() {
        vec![]
    } else {
        let (_, _, fields) = crate::message::parser::parse(&bytes)?;
        let count = (fields.unix_fds().unwrap_or(0) as usize).min(fds.len());

        fds.drain(..count).collect()
    };
    let endian = Endian::from(primary_header.endian_sig());
    let ctxt = Context::new(endian, 0);
    #[cfg(unix)]
    let bytes = serialized::Data::new_fds(bytes, ctxt, fds);
    #[cfg(not(unix))]
    let bytes = serialized::Data::new(bytes, ctxt);
    Message::from_raw_parts(bytes, seq)
}

/// The most that a read of the default [`ReadHalf::receive_message`] has room for.
///
/// The room is zero-filled before each read, and what the read leaves of it is given up after,
/// so a read costs zero-filling up to this much, however little it brings in. A message longer
/// than this is read in pieces of no more than this.
const READ_MAX: usize = 64 * 1024;

/// Room at the end of a buffer for a read to fill, which the buffer gives up again when this is
/// dropped, but for what the read is said to have filled.
///
/// A read that fails, or whose future is dropped while it waits, then leaves the buffer as it
/// found it.
struct Room<'b> {
    bytes: &'b mut Vec<u8>,
    /// How long `bytes` stays when this is dropped.
    keep: usize,
}

impl<'b> Room<'b> {
    /// Room in `bytes` after what they hold, up to a length of `len`.
    fn new(bytes: &'b mut Vec<u8>, len: usize) -> Self {
        let keep = bytes.len();
        bytes.resize(len, 0);

        Self { bytes, keep }
    }

    /// The part of the room that the read is yet to fill.
    fn unfilled(&mut self) -> &mut [u8] {
        &mut self.bytes[self.keep..]
    }

    /// Keeps the next `len` bytes of the room, which a read filled.
    fn filled(&mut self, len: usize) {
        self.keep += len;
    }
}

impl Drop for Room<'_> {
    fn drop(&mut self) {
        self.bytes.truncate(self.keep);
    }
}

#[async_trait::async_trait]
impl ReadHalf for Box<dyn ReadHalf> {
    fn can_pass_unix_fd(&self) -> bool {
        (**self).can_pass_unix_fd()
    }

    async fn receive_message(
        &mut self,
        seq: u64,
        already_received_bytes: &mut Vec<u8>,
        #[cfg(unix)] already_received_fds: &mut Vec<OwnedFd>,
    ) -> crate::Result<Message> {
        (**self)
            .receive_message(
                seq,
                already_received_bytes,
                #[cfg(unix)]
                already_received_fds,
            )
            .await
    }

    async fn recvmsg(&mut self, buf: &mut [u8]) -> RecvmsgResult {
        (**self).recvmsg(buf).await
    }

    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        (**self).peer_credentials().await
    }

    fn auth_mechanism(&self) -> AuthMechanism {
        (**self).auth_mechanism()
    }

    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn can_take_unread(&self) -> bool {
        (**self).can_take_unread()
    }

    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn take_unread(&mut self, bytes: &mut Vec<u8>, fds: &mut Vec<OwnedFd>) {
        (**self).take_unread(bytes, fds)
    }
}

#[async_trait::async_trait]
impl WriteHalf for Box<dyn WriteHalf> {
    async fn send_message(&mut self, msg: &Message) -> crate::Result<()> {
        (**self).send_message(msg).await
    }

    async fn send_messages(&mut self, msgs: &[Message]) -> crate::Result<()> {
        (**self).send_messages(msgs).await
    }

    async fn sendmsg(
        &mut self,
        buffer: &[u8],
        #[cfg(unix)] fds: &[BorrowedFd<'_>],
    ) -> io::Result<usize> {
        (**self)
            .sendmsg(
                buffer,
                #[cfg(unix)]
                fds,
            )
            .await
    }

    async fn flush(&mut self) -> io::Result<()> {
        (**self).flush().await
    }

    #[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
    async fn send_zero_byte(&mut self) -> io::Result<Option<usize>> {
        (**self).send_zero_byte().await
    }

    async fn close(&mut self) -> io::Result<()> {
        (**self).close().await
    }

    fn can_pass_unix_fd(&self) -> bool {
        (**self).can_pass_unix_fd()
    }

    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        (**self).peer_credentials().await
    }

    fn can_resume_writes(&self) -> bool {
        (**self).can_resume_writes()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    use futures_lite::future::{block_on, poll_once};
    use ntest::timeout;
    use test_log::test;

    use super::*;
    #[cfg(unix)]
    use crate::{Error, Fd};

    /// Messages that come in a few bytes at a time come out whole and in order, however often the
    /// call that receives one is dropped while it waits for more of it, and another made in its
    /// place. What is read of a message that is yet to come in whole stays in the bytes that the
    /// calls are given.
    #[test]
    #[timeout(15000)]
    fn messages_come_out_whole_from_calls_dropped_while_they_wait() {
        let msgs = [
            Message::method_call("/", "Ping")
                .interface("org.freedesktop.DBus.Peer")
                .build(&())
                .unwrap(),
            Message::signal("/", "org.zbus.Test", "Signal")
                .build(&"a string")
                .unwrap(),
            Message::method_call("/", "Test")
                .build(&vec![7u8; 100])
                .unwrap(),
            Message::method_call("/", "Test")
                .build(&(1u32, "more"))
                .unwrap(),
        ];
        let next = Message::method_call("/", "Test")
            .build(&vec![8u8; 100])
            .unwrap();
        let partial = &next.data()[..20];
        let written = [
            msgs.iter().flat_map(|msg| msg.data().to_vec()).collect(),
            partial.to_vec(),
        ]
        .concat();
        // The handshake leaves what it read past its own end for the first call, here all of the
        // first message and the start of the second.
        let mut bytes = written[..msgs[0].data().len() + 5].to_vec();
        // The rest comes in reads of 1, 2 and 3 bytes in turn.
        let mut reads = pieces(&written[bytes.len()..], &[1, 2, 3]);

        let mut half = Scripted::default();
        #[cfg(unix)]
        let mut fds = vec![];
        let mut received = vec![];
        while received.len() < msgs.len() {
            let polled = half.receive(
                &mut bytes,
                #[cfg(unix)]
                &mut fds,
            );
            match polled {
                Some(msg) => received.push(msg.unwrap()),
                // The call was dropped, and the peer sends a little more.
                None => half.push(reads.pop_front().expect("a call waits with all of it read")),
            }
        }
        // The rest is the start of a message, which a call takes in, and then waits for more of.
        half.reads.extend(reads.into_iter().map(Read::from));
        let polled = half.receive(
            &mut bytes,
            #[cfg(unix)]
            &mut fds,
        );

        assert!(polled.is_none(), "a message came out of part of one");
        assert!(half.reads.is_empty(), "a call left reads alone");
        for (received, sent) in received.iter().zip(&msgs) {
            assert_eq!(&received.data()[..], &sent.data()[..]);
        }
        assert_eq!(bytes, partial);
    }

    /// A message several times longer than `READ_MAX` comes out whole, with the call that
    /// receives it dropped each time it waits for more of it, however its reads fall across the
    /// room of each. No read is given room for more than `READ_MAX` bytes.
    #[test]
    #[timeout(15000)]
    fn a_long_message_comes_out_whole_in_reads_of_no_more_than_read_max() {
        let msg = Message::method_call("/", "Test")
            .build(&vec![7u8; 3 * READ_MAX + READ_MAX / 2])
            .unwrap();
        // The message comes in reads of just under, just over and over twice `READ_MAX` in turn.
        let mut reads = pieces(
            &msg.data()[..],
            &[READ_MAX - 1, READ_MAX + 1, 2 * READ_MAX + 3],
        );

        let mut half = Scripted::default();
        let mut bytes = vec![];
        #[cfg(unix)]
        let mut fds = vec![];
        let received = loop {
            let polled = half.receive(
                &mut bytes,
                #[cfg(unix)]
                &mut fds,
            );
            match polled {
                Some(received) => break received.unwrap(),
                // The call was dropped, and the peer sends more.
                None => half.push(reads.pop_front().expect("a call waits with all of it read")),
            }
        };

        assert_eq!(&received.data()[..], &msg.data()[..]);
        assert!(bytes.is_empty(), "bytes were left over");
        assert_eq!(half.largest_room, READ_MAX);
    }

    /// Each message takes as many of the file descriptors that came in as its header counts, in
    /// the order that they came in: those that came with any of its bytes, after those that came
    /// before it and that no message took.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn messages_take_as_many_fds_as_their_headers_count() {
        // The pipes are told apart by their inodes.
        let pipes = [(); 4].map(|()| pipe());
        let inodes = pipes.each_ref().map(inode);
        let fd = |i: usize| Fd::from(pipes[i].as_fd());
        // Message 0 counts one descriptor, message 1 two, message 2 none and message 3 one.
        let msgs = [
            Message::method_call("/", "Test")
                .build(&(0u32, fd(0)))
                .unwrap(),
            Message::method_call("/", "Test")
                .build(&(1u32, fd(1), fd(2)))
                .unwrap(),
            Message::method_call("/", "Test").build(&2u32).unwrap(),
            Message::method_call("/", "Test")
                .build(&(3u32, fd(3)))
                .unwrap(),
        ];
        // The first byte of message 0 comes with the descriptors of messages 0 and 1, and the body
        // of message 3 with its own.
        let (start, body) = msgs[3].data().split_at(20);
        let mut half = Scripted::default();
        half.push_with_fds(msgs[0].data()[..1].to_vec(), clones(&pipes[..3]));
        half.push(
            [
                &msgs[0].data()[1..],
                &msgs[1].data()[..],
                &msgs[2].data()[..],
                start,
            ]
            .concat(),
        );
        half.push_with_fds(body.to_vec(), clones(&pipes[3..]));

        let (mut bytes, mut fds) = (vec![], vec![]);
        let mut received_with = vec![];
        for sent in &msgs {
            let received = half
                .receive(&mut bytes, &mut fds)
                .expect("a call waits with the message read")
                .unwrap();
            assert_eq!(&received.data()[..], &sent.data()[..]);
            let fds = received.data().fds().iter().map(inode);
            received_with.push(
                fds.map(|inode| inodes.iter().position(|i| *i == inode).unwrap())
                    .collect::<Vec<_>>(),
            );
        }

        assert_eq!(
            received_with,
            [&[0][..], &[1, 2], &[], &[3]],
            "the descriptors went with other messages than their own",
        );
        assert!(fds.is_empty(), "descriptors were left over");
    }

    /// A message whose header counts more file descriptors than came in is an error, rather than
    /// a panic, whichever of them the handshake read.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn a_message_with_fewer_fds_than_its_header_counts_is_an_error() {
        let pipes = [pipe(), pipe()];
        let msg = Message::method_call("/", "Test")
            .build(&(Fd::from(pipes[0].as_fd()), Fd::from(pipes[1].as_fd())))
            .unwrap();
        let mut half = Scripted::default();
        half.push(msg.data().to_vec());
        // The handshake read one of the descriptors, and the other never comes.
        let mut fds = clones(&pipes[..1]);

        let received = half
            .receive(&mut vec![], &mut fds)
            .expect("a call waits with the message read");

        assert!(
            matches!(received, Err(Error::MissingParameter(_))),
            "{received:?}",
        );
    }

    /// File descriptors that no header counts wait for the messages after them to take them, but
    /// no more than `FDS_MAX` of them.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn more_than_fds_max_waiting_fds_are_an_error() {
        allow_open_fds(2 * FDS_MAX as u64);
        let pipe = pipe();
        let msg = Message::method_call("/", "Test").build(&()).unwrap();
        let mut half = Scripted::default();
        let extra = (0..FDS_MAX).map(|_| pipe.try_clone().unwrap()).collect();
        half.push_with_fds(msg.data().to_vec(), extra);
        half.push_with_fds(msg.data().to_vec(), clones(&[pipe]));
        let (mut bytes, mut fds) = (vec![], vec![]);

        let first = half
            .receive(&mut bytes, &mut fds)
            .expect("a call waits with the message read");
        let second = half
            .receive(&mut bytes, &mut fds)
            .expect("a call waits with the message read");

        assert_eq!(&first.unwrap().data()[..], &msg.data()[..]);
        assert!(
            matches!(&second, Err(Error::InputOutput(e)) if e.kind() == io::ErrorKind::InvalidData),
            "{second:?}",
        );
    }

    /// A read half that hands out the reads of a script, and waits for ever once it is out, as a
    /// socket whose peer sends nothing more.
    ///
    /// A read that has less room than the next one of the script takes in what it has room for,
    /// and leaves the rest for the next, which no descriptors come with: the kernel hands them over
    /// with the first read that takes in any of the bytes that they came with.
    #[derive(Debug, Default)]
    struct Scripted {
        reads: VecDeque<Read>,
        /// The most room that a read was given.
        largest_room: usize,
    }

    impl Scripted {
        /// What a call to the default `receive_message` comes to before it waits, if anything. A
        /// call that waits is dropped.
        fn receive(
            &mut self,
            bytes: &mut Vec<u8>,
            #[cfg(unix)] fds: &mut Vec<OwnedFd>,
        ) -> Option<crate::Result<Message>> {
            let call = self.receive_message(
                0,
                bytes,
                #[cfg(unix)]
                fds,
            );

            block_on(poll_once(call))
        }

        /// Adds a read of `bytes` to the end of the script, which no descriptors come with.
        fn push(&mut self, bytes: Vec<u8>) {
            self.reads.push_back(Read::from(bytes));
        }

        /// Adds a read of `bytes` to the end of the script, which `fds` come with.
        #[cfg(unix)]
        fn push_with_fds(&mut self, bytes: Vec<u8>, fds: Vec<OwnedFd>) {
            self.reads.push_back(Read { bytes, fds });
        }
    }

    #[async_trait::async_trait]
    impl ReadHalf for Scripted {
        async fn recvmsg(&mut self, buffer: &mut [u8]) -> RecvmsgResult {
            self.largest_room = self.largest_room.max(buffer.len());
            let Some(mut read) = self.reads.pop_front() else {
                return std::future::pending().await;
            };
            let len = read.bytes.len().min(buffer.len());
            buffer[..len].copy_from_slice(&read.bytes[..len]);
            if len < read.bytes.len() {
                self.reads.push_front(Read::from(read.bytes.split_off(len)));
            }

            #[cfg(unix)]
            let received = (len, read.fds);
            #[cfg(not(unix))]
            let received = len;

            Ok(received)
        }
    }

    /// A read of a script: the bytes that it takes in, and the file descriptors that come with
    /// them.
    #[derive(Debug)]
    struct Read {
        bytes: Vec<u8>,
        #[cfg(unix)]
        fds: Vec<OwnedFd>,
    }

    impl From<Vec<u8>> for Read {
        /// A read of `bytes`, which no descriptors come with.
        fn from(bytes: Vec<u8>) -> Self {
            Self {
                bytes,
                #[cfg(unix)]
                fds: vec![],
            }
        }
    }

    /// `bytes` cut into the reads that they come in: one of each of `sizes` in turn, over and over,
    /// but for the last, which takes what is left.
    fn pieces(mut bytes: &[u8], sizes: &[usize]) -> VecDeque<Vec<u8>> {
        let mut sizes = sizes.iter().cycle();
        let mut pieces = VecDeque::new();
        while !bytes.is_empty() {
            let (piece, rest) = bytes.split_at((*sizes.next().unwrap()).min(bytes.len()));
            pieces.push_back(piece.to_vec());
            bytes = rest;
        }

        pieces
    }

    /// The read end of a new pipe, whose write end is closed.
    #[cfg(unix)]
    fn pipe() -> OwnedFd {
        std::io::pipe().unwrap().0.into()
    }

    /// New descriptors of the files that `fds` are open on.
    #[cfg(unix)]
    fn clones(fds: &[OwnedFd]) -> Vec<OwnedFd> {
        fds.iter().map(|fd| fd.try_clone().unwrap()).collect()
    }

    /// The inode of the file that `fd` is open on.
    #[cfg(unix)]
    fn inode(fd: &impl AsFd) -> u64 {
        std::fs::File::from(fd.as_fd().try_clone_to_owned().unwrap())
            .metadata()
            .unwrap()
            .ino()
    }

    /// Raises the limit on the descriptors that the process may have open to `count`, if it is
    /// lower: the limit of 1024 that many systems set leaves no room for a test that has that many
    /// open, besides those of the others.
    #[cfg(unix)]
    fn allow_open_fds(count: u64) {
        use rustix::process::{Resource, getrlimit, setrlimit};

        let mut limit = getrlimit(Resource::Nofile);
        if limit.current.is_some_and(|current| current < count) {
            limit.current = Some(count);
            setrlimit(Resource::Nofile, limit).expect("too low a hard limit on open descriptors");
        }
    }
}
