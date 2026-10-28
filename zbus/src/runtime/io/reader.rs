//! The read half of a registered socket, which reads ahead of the message it is asked for.

#[cfg(unix)]
use std::{collections::VecDeque, os::fd::OwnedFd};
use std::{io, mem, sync::Arc};

use super::{RegisteredIo, SocketOps};
use crate::{
    Message,
    conn::AuthMechanism,
    connection::socket::{ReadHalf, RecvmsgResult, read_primary_header},
    fdo::ConnectionCredentials,
    message::header::MIN_MESSAGE_SIZE,
    wire::{
        Endian,
        serialized::{self, Context},
    },
};

/// The half of a [`RegisteredIo`] that a connection receives its messages through.
///
/// A read takes in as much as the socket holds, up to the size of a buffer that this half keeps
/// from one message to the next, and each message that is whole in the buffer is taken out of it
/// without reading again. A message that doesn't fit in the buffer gets an allocation of its exact
/// size, which takes over what the buffer holds of it and has the rest read straight into it.
///
/// # File descriptors
///
/// The header of a message says how many file descriptors it carries. The D-Bus specification has
/// the sender send all of them at once, with a write that carries any of the bytes of the message,
/// from the first to the last, so all of them have come in once the message is whole. Each message
/// takes as many of those that came in as its header says, in the order that they came in, as the
/// reference implementation of D-Bus does, whichever messages the reads that returned them hold. A
/// write may carry other messages before or after that message too, as
/// [`WriteHalf::send_messages`] does.
///
/// Descriptors that no header counts wait for the messages after them to take them, as with the
/// reference implementation, in up to `MAX_FD_SETS` sets, each of those that came with one read.
/// A read returns those of one write at most, and is made only once no whole message is left in
/// the buffer. Before a read, the descriptors that wait can then only be those of the message
/// partly in the buffer, and those that the write that carries its end brings for the next one, so
/// no more than three sets wait after it, and a read that leaves more waiting is an error.
///
/// [`WriteHalf::send_messages`]: crate::connection::socket::WriteHalf::send_messages
#[derive(Debug)]
pub(crate) struct Reader<O> {
    socket: Arc<RegisteredIo<O>>,
    /// The bytes read off the socket that are yet to be taken as messages are
    /// `buffer[start..end]`.
    buffer: Vec<u8>,
    start: usize,
    end: usize,
    /// The file descriptors that came with the reads so far and that no message has taken, in
    /// the order they came in, as the sets that came with each read.
    #[cfg(unix)]
    fds: VecDeque<Vec<OwnedFd>>,
}

impl<O> Reader<O>
where
    O: SocketOps,
{
    /// The half of `socket` that messages are received through.
    pub(crate) fn new(socket: Arc<RegisteredIo<O>>) -> Self {
        Self {
            socket,
            buffer: vec![0; BUFFER_SIZE],
            start: 0,
            end: 0,
            #[cfg(unix)]
            fds: VecDeque::new(),
        }
    }

    /// Puts what the handshake read past its own end ahead of what this half has read itself.
    fn take_over(&mut self, bytes: &mut Vec<u8>, #[cfg(unix)] fds: &mut Vec<OwnedFd>) {
        if !bytes.is_empty() {
            let mut buffer = mem::take(bytes);
            buffer.extend_from_slice(&self.buffer[self.start..self.end]);
            self.start = 0;
            self.end = buffer.len();
            // Whatever the handshake read past its end stays in the buffer until it is taken, so
            // the buffer is as long as that if it has to be.
            buffer.resize(self.end.max(BUFFER_SIZE), 0);
            self.buffer = buffer;
        }
        #[cfg(unix)]
        if !fds.is_empty() {
            self.fds.push_front(mem::take(fds));
        }
    }

    /// Reads as much as the socket holds into the room that the buffer has after the bytes it
    /// holds, which it first moves to its start.
    async fn fill(&mut self) -> crate::Result<()> {
        if self.start > 0 {
            self.buffer.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        let received = self.socket.recv(&mut self.buffer[self.end..]).await;
        self.end += self.received(received)?;

        Ok(())
    }

    /// Reads the rest of a message that is longer than the buffer straight into an allocation of
    /// its own, after what the buffer holds of it.
    async fn receive_long(&mut self, len: usize) -> crate::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(len);
        bytes.extend_from_slice(&self.buffer[self.start..self.end]);
        self.start = 0;
        self.end = 0;
        let mut pos = bytes.len();
        bytes.resize(len, 0);
        while pos < len {
            let received = self.socket.recv(&mut bytes[pos..]).await;
            pos += self.received(received)?;
        }

        Ok(bytes)
    }

    /// The number of bytes of a read, whose file descriptors join those that wait for a message.
    fn received(&mut self, received: RecvmsgResult) -> crate::Result<usize> {
        #[cfg(unix)]
        let (read, fds) = received?;
        #[cfg(not(unix))]
        let read = received?;
        if read == 0 {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "failed to receive message").into(),
            );
        }
        #[cfg(unix)]
        if !fds.is_empty() {
            if self.fds.len() == MAX_FD_SETS {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "too many file descriptors that no message counts",
                )
                .into());
            }
            self.fds.push_back(fds);
        }

        Ok(read)
    }

    /// The file descriptors of the message of `bytes`, which has just been taken out of the
    /// buffer: as many of those that wait as its header says it has.
    #[cfg(unix)]
    fn take_fds(&mut self, bytes: &[u8]) -> crate::Result<Vec<OwnedFd>> {
        if self.fds.is_empty() {
            return Ok(vec![]);
        }

        let (_, _, fields) = crate::message::parser::parse(bytes)?;
        let count = fields.unix_fds().unwrap_or(0) as usize;
        let mut fds = vec![];
        while fds.len() < count
            && let Some(set) = self.fds.front_mut()
        {
            let wanted = count - fds.len();
            if set.len() > wanted {
                fds.extend(set.drain(..wanted));
            } else {
                fds.append(set);
                self.fds.pop_front();
            }
        }

        Ok(fds)
    }
}

#[async_trait::async_trait]
impl<O> ReadHalf for Reader<O>
where
    O: SocketOps,
{
    async fn receive_message(
        &mut self,
        seq: u64,
        already_received_bytes: &mut Vec<u8>,
        #[cfg(unix)] already_received_fds: &mut Vec<OwnedFd>,
    ) -> crate::Result<Message> {
        self.take_over(
            already_received_bytes,
            #[cfg(unix)]
            already_received_fds,
        );

        let (primary_header, bytes) = loop {
            let buffered = &self.buffer[self.start..self.end];
            if buffered.len() >= MIN_MESSAGE_SIZE {
                let (primary_header, len) = read_primary_header(buffered)?;
                if buffered.len() >= len {
                    let bytes = buffered[..len].to_vec();
                    self.start += len;
                    if self.start == self.end {
                        self.start = 0;
                        self.end = 0;
                    }

                    break (primary_header, bytes);
                } else if len > self.buffer.len() {
                    break (primary_header, self.receive_long(len).await?);
                }
            }

            self.fill().await?;
        };

        #[cfg(unix)]
        let fds = self.take_fds(&bytes)?;
        let ctxt = Context::new(Endian::from(primary_header.endian_sig()), 0);
        #[cfg(unix)]
        let bytes = serialized::Data::new_fds(bytes, ctxt, fds);
        #[cfg(not(unix))]
        let bytes = serialized::Data::new(bytes, ctxt);

        Message::from_raw_parts(bytes, seq)
    }

    /// Reads straight from the socket.
    ///
    /// Only the handshake reads through this, which it is done with before the first message is
    /// received, so there is never anything read ahead for it to hand out first.
    async fn recvmsg(&mut self, buf: &mut [u8]) -> RecvmsgResult {
        debug_assert_eq!(self.start, self.end, "bytes read ahead would be skipped");

        self.socket.recv(buf).await
    }

    fn can_pass_unix_fd(&self) -> bool {
        self.socket.ops.can_pass_unix_fd()
    }

    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        self.socket
            .ops
            .peer_credentials(&self.socket.source, &self.socket.runtime)
            .await
    }

    fn auth_mechanism(&self) -> AuthMechanism {
        self.socket.ops.auth_mechanism()
    }
}

/// How many sets of file descriptors, each of those that came with one read, may wait for messages
/// to take them.
///
/// See [`Reader`] for why a sender that counts its descriptors never has more wait.
#[cfg(unix)]
const MAX_FD_SETS: usize = 3;

/// The size of the buffer that a [`Reader`] reads into.
///
/// Most messages are much smaller than this, so that one read takes in several of those that
/// come in quick succession, while a large one costs a copy of no more than this out of the buffer
/// before the rest of it is read straight into its own allocation.
const BUFFER_SIZE: usize = 4 * 1024;
