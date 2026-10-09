#[cfg(feature = "p2p")]
pub mod channel;
#[cfg(feature = "p2p")]
pub use channel::Channel;

mod split;
pub use split::{BoxedSplit, Split};

use std::{io, mem};

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
    type ReadHalf: ReadHalf;
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
    ///   likely you can safely ignore this parameter.
    /// - `already_received_fds`: Same goes for file descriptors belonging to first messages.
    async fn receive_message(
        &mut self,
        seq: u64,
        already_received_bytes: &mut Vec<u8>,
        #[cfg(unix)] already_received_fds: &mut Vec<OwnedFd>,
    ) -> crate::Result<Message> {
        #[cfg(unix)]
        let mut fds = vec![];
        let mut bytes = if already_received_bytes.len() < MIN_MESSAGE_SIZE {
            let mut bytes = vec![];
            if !already_received_bytes.is_empty() {
                mem::swap(already_received_bytes, &mut bytes);
            }
            let mut pos = bytes.len();
            bytes.resize(MIN_MESSAGE_SIZE, 0);
            // We don't have enough data to make a proper message header yet.
            // Some partial read may be in raw_in_buffer, so we try to complete it
            // until we have MIN_MESSAGE_SIZE bytes
            //
            // Given that MIN_MESSAGE_SIZE is 16, this codepath is actually extremely unlikely
            // to be taken more than once
            while pos < MIN_MESSAGE_SIZE {
                let res = self.recvmsg(&mut bytes[pos..]).await?;
                let len = {
                    #[cfg(unix)]
                    {
                        fds.extend(res.1);
                        res.0
                    }
                    #[cfg(not(unix))]
                    {
                        res
                    }
                };
                pos += len;
                if len == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "failed to receive message",
                    )
                    .into());
                }
            }

            bytes
        } else {
            already_received_bytes.drain(..MIN_MESSAGE_SIZE).collect()
        };

        let (primary_header, fields_len) = PrimaryHeader::read(&bytes)?;
        let header_len = MIN_MESSAGE_SIZE + fields_len as usize;
        let body_padding = padding_for_8_bytes(header_len);
        let body_len = primary_header.body_len() as usize;
        let total_len = header_len + body_padding + body_len;
        if total_len > MAX_MESSAGE_SIZE {
            return Err(crate::Error::ExcessData);
        }

        // By this point we have a full primary header, so we know the exact length of the complete
        // message.
        if !already_received_bytes.is_empty() {
            // still have some bytes buffered.
            let pending = total_len - bytes.len();
            let to_take = std::cmp::min(pending, already_received_bytes.len());
            bytes.extend(already_received_bytes.drain(..to_take));
        }
        let mut pos = bytes.len();
        bytes.resize(total_len, 0);

        // Read the rest, if any
        while pos < total_len {
            let res = self.recvmsg(&mut bytes[pos..]).await?;
            let read = {
                #[cfg(unix)]
                {
                    fds.extend(res.1);
                    res.0
                }
                #[cfg(not(unix))]
                {
                    res
                }
            };
            pos += read;
            if read == 0 {
                return Err(crate::Error::InputOutput(
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "failed to receive message",
                    )
                    .into(),
                ));
            }
        }

        // If we reach here, the message is complete; return it
        let endian = Endian::from(primary_header.endian_sig());

        #[cfg(unix)]
        if !already_received_fds.is_empty() {
            use crate::message::header::PRIMARY_HEADER_SIZE;

            let ctxt = Context::new(endian, PRIMARY_HEADER_SIZE);
            let encoded_fields =
                serialized::Data::new(&bytes[PRIMARY_HEADER_SIZE..header_len], ctxt);
            let fields: crate::message::Fields<'_> = encoded_fields.deserialize()?.0;
            let num_required_fds = match fields.unix_fds {
                Some(num_fds) => num_fds as usize,
                _ => 0,
            };
            let num_pending = num_required_fds
                .checked_sub(fds.len())
                .ok_or_else(|| crate::Error::ExcessData)?;
            // If we had previously received FDs, `num_pending` has to be > 0
            if num_pending == 0 {
                return Err(crate::Error::MissingParameter("Missing file descriptors"));
            }
            // All previously received FDs must go first in the list.
            let mut already_received: Vec<_> = already_received_fds.drain(..num_pending).collect();
            mem::swap(&mut already_received, &mut fds);
            fds.extend(already_received);
        }

        let ctxt = Context::new(endian, 0);
        #[cfg(unix)]
        let bytes = serialized::Data::new_fds(bytes, ctxt, fds);
        #[cfg(not(unix))]
        let bytes = serialized::Data::new(bytes, ctxt);
        Message::from_raw_parts(bytes, seq)
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
    /// Implementers should override either this or `sendmsg`.
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

    /// Attempt to send a message on the socket
    ///
    /// On success, return the number of bytes written. There may be a partial write, in
    /// which case the caller is responsible for sending the remaining data by calling this
    /// method again until everything is written or it returns an error of kind `WouldBlock`.
    ///
    /// If at least one byte has been written, then all the provided file descriptors will
    /// have been sent as well, and should not be provided again in subsequent calls.
    ///
    /// If the underlying transport does not support transmitting file descriptors, this
    /// will return `Err(ErrorKind::InvalidInput)`.
    ///
    /// The default implementation simply panics. Implementers must override either `send_message`
    /// or this method.
    async fn sendmsg(
        &mut self,
        _buffer: &[u8],
        #[cfg(unix)] _fds: &[BorrowedFd<'_>],
    ) -> io::Result<usize> {
        unimplemented!("`WriteHalf` implementers must either override `send_message` or `sendmsg`");
    }

    /// Flush whatever the socket holds on to of the bytes `sendmsg` was given.
    ///
    /// The default `send_message` calls this once it has handed a whole message to `sendmsg`. The
    /// authentication handshake calls it each time before it waits for the peer to answer, so the
    /// commands it pipelines go out together. A socket that holds on to what it is given, such as
    /// one over a buffered or encrypted stream, has to send it on here: zbus has no other way to
    /// flush it, and the peer would never see it otherwise.
    ///
    /// The default implementation does nothing, which suits a socket that sends what it is given
    /// right away.
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
}

#[async_trait::async_trait]
impl WriteHalf for Box<dyn WriteHalf> {
    async fn send_message(&mut self, msg: &Message) -> crate::Result<()> {
        (**self).send_message(msg).await
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
}
