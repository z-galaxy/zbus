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
/// size, kept in the reader, so that a read dropped while it waits loses none of it.
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
    /// A message longer than the buffer, read straight into an allocation of its exact length, of
    /// which the first `long_filled` bytes have come in. Empty when there is none. While there is
    /// one, the buffer holds nothing: the message took over what the buffer held of it.
    long: Vec<u8>,
    long_filled: usize,
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
            long: Vec::new(),
            long_filled: 0,
            #[cfg(unix)]
            fds: VecDeque::new(),
        }
    }

    /// Puts what the handshake read past its own end, or what a connection that was handed over
    /// left unread, ahead of what this half has read itself.
    ///
    /// A message longer than the buffer, or the start of one, and nothing else, as a reader that
    /// was stopped in the middle of one hands it over, becomes the allocation of the message
    /// itself, as it is, rather than go through the buffer.
    fn take_over(&mut self, bytes: &mut Vec<u8>, #[cfg(unix)] fds: &mut Vec<OwnedFd>) {
        debug_assert!(
            self.long.is_empty() || bytes.is_empty(),
            "bytes read before a message partly read would come after it",
        );
        if let Some(len) = self.long_start(bytes) {
            self.long = mem::take(bytes);
            self.long_filled = self.long.len();
            self.long.resize(len, 0);
        } else if !bytes.is_empty() {
            let mut buffer = mem::take(bytes);
            buffer.extend_from_slice(&self.buffer[self.start..self.end]);
            self.start = 0;
            self.end = buffer.len();
            // Whatever the handshake read past its end stays in the buffer until it is taken, so
            // the buffer is as long as that if it has to be, until `shrink` gives the room back.
            buffer.resize(self.end.max(BUFFER_SIZE), 0);
            self.buffer = buffer;
        }
        #[cfg(unix)]
        if !fds.is_empty() {
            self.fds.push_front(mem::take(fds));
        }
    }

    /// The length of the message that `bytes` start, if they hold no more than that message,
    /// which is longer than the buffer, while this half holds nothing.
    fn long_start(&self, bytes: &[u8]) -> Option<usize> {
        if self.start < self.end || !self.long.is_empty() || bytes.len() < MIN_MESSAGE_SIZE {
            return None;
        }
        let (_, len) = read_primary_header(bytes).ok()?;

        (len > BUFFER_SIZE && bytes.len() <= len).then_some(len)
    }

    /// Gives back the room that the buffer grew by, once what it holds fits in a buffer of the
    /// usual size, which it moves to its start.
    ///
    /// The buffer only grows to take what was read before this half, such as all of the messages
    /// that a connection that was handed over left unread, and the start of a long one, which can
    /// add up to many times the usual size. The reader lives as long as its connection, so the
    /// room is given back as soon as those are out of the buffer, rather than kept for good.
    fn shrink(&mut self) {
        if self.buffer.len() <= BUFFER_SIZE || self.end - self.start > BUFFER_SIZE {
            return;
        }
        self.buffer.copy_within(self.start..self.end, 0);
        self.end -= self.start;
        self.start = 0;
        self.buffer.truncate(BUFFER_SIZE);
        self.buffer.shrink_to_fit();
    }

    /// Reads as much as the socket holds into the room that the buffer has after the bytes it
    /// holds, which it first moves to its start.
    async fn fill(&mut self) -> crate::Result<()> {
        if self.start > 0 {
            self.buffer.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        // A half that handed its buffer over with what it held reads into a new one.
        if self.buffer.len() < BUFFER_SIZE {
            self.buffer.resize(BUFFER_SIZE, 0);
        }
        let received = self.socket.recv(&mut self.buffer[self.end..]).await;
        self.end += self.received(received)?;

        Ok(())
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
            if !self.long.is_empty() {
                while self.long_filled < self.long.len() {
                    // Disjoint fields: the socket is borrowed shared, `long` mutably. What a read
                    // brings is counted as soon as it returns, so a call dropped while it waits
                    // for the next one loses nothing.
                    let received = self.socket.recv(&mut self.long[self.long_filled..]).await;
                    self.long_filled += self.received(received)?;
                }
                let bytes = mem::take(&mut self.long);
                self.long_filled = 0;

                break (read_primary_header(&bytes)?.0, bytes);
            }

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
                    self.shrink();

                    break (primary_header, bytes);
                } else if len > BUFFER_SIZE {
                    // The message gets an allocation of its own, which takes over what the buffer
                    // holds of it and has the rest read straight into it. That goes for a buffer
                    // that grew too, which then has no more than the start of this message.
                    self.long = Vec::with_capacity(len);
                    self.long.extend_from_slice(buffered);
                    self.long_filled = self.long.len();
                    self.long.resize(len, 0);
                    self.start = 0;
                    self.end = 0;
                    self.shrink();

                    continue;
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
        debug_assert!(
            self.long.is_empty(),
            "a message partly read would be skipped"
        );

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

    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn can_take_unread(&self) -> bool {
        true
    }

    /// Moves out the message partly read into an allocation of its own, if any, then what the
    /// buffer holds, and the file descriptors that wait, all of their sets in one list.
    ///
    /// Where nothing comes before them in `bytes`, as for a connection that stops reading once it
    /// received a message, the allocation of the message, room for the rest of it included, or
    /// the buffer, with what it holds moved to its start, goes into `bytes` as it is, rather than a
    /// copy of what it holds, for a reader that takes over to take it as its own, as a message can
    /// be as long as D-Bus allows. This half then reads into a new buffer, if it reads again.
    ///
    /// A reader that takes them over puts that list ahead of what it reads as one set. It stands
    /// for the sets that waited here, of which there was at least one, so no more sets wait in
    /// that reader than would have in this one, and a sender that counts its descriptors still
    /// never has more than `MAX_FD_SETS` wait.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn take_unread(&mut self, bytes: &mut Vec<u8>, fds: &mut Vec<OwnedFd>) {
        if !self.long.is_empty() {
            let mut long = mem::take(&mut self.long);
            long.truncate(self.long_filled);
            self.long_filled = 0;
            if bytes.is_empty() {
                *bytes = long;
            } else {
                bytes.append(&mut long);
            }
        }
        let buffered = self.start..self.end;
        if bytes.is_empty() && !buffered.is_empty() {
            self.buffer.copy_within(buffered, 0);
            self.buffer.truncate(self.end - self.start);
            *bytes = mem::take(&mut self.buffer);
        } else {
            bytes.extend_from_slice(&self.buffer[buffered]);
        }
        self.start = 0;
        self.end = 0;
        fds.extend(self.fds.drain(..).flatten());
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
