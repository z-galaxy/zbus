//! A [`Socket`] that connects two peers within the same process.
//!
//! [`Channel::pair`] creates the two ends, each of which splits into a [`Reader`] and a
//! [`Writer`].
//!
//! [`Socket`]: super::Socket

use std::io;

use std::num::NonZeroUsize;

use zruntime::broadcast::{Receiver, RecvError, Sender, channel};

use super::{read_primary_header, take_message};
use crate::{
    Message, conn::AuthMechanism, fdo::ConnectionCredentials, message::header::MIN_MESSAGE_SIZE,
};

/// An in-process channel-based socket.
///
/// This is a pair of two cross-wired channels. Since all communication happens in-process, there is
/// no need for any authentication.
///
/// This type is only available when the `p2p` feature is enabled.
#[derive(Debug)]
pub struct Channel {
    writer: Writer,
    reader: Reader,
}

impl Channel {
    /// Create a pair of cross-wired channels.
    ///
    /// Use [`crate::connection::Builder::authenticated_socket`] to create `Connection` instances
    /// from each channel.
    pub fn pair() -> (Self, Self) {
        let (tx1, rx1) = channel(CHANNEL_CAPACITY);
        let (tx2, rx2) = channel(CHANNEL_CAPACITY);

        (
            Self {
                writer: Writer(tx1),
                reader: Reader(rx2),
            },
            Self {
                writer: Writer(tx2),
                reader: Reader(rx1),
            },
        )
    }
}

impl super::Socket for Channel {
    type ReadHalf = Reader;
    type WriteHalf = Writer;

    // A connection over the channel knows that its sends are whole or nothing, as
    // `Writer::send_message` tells.
    fn split(self) -> super::Split<Self::ReadHalf, Self::WriteHalf> {
        super::Split::with_writes(self.reader, self.writer, Some(super::Writes::Whole))
    }
}

/// The reader half of a [`Channel`].
///
/// This type is only available when the `p2p` feature is enabled.
#[derive(Debug)]
pub struct Reader(Receiver<Message>);

#[async_trait::async_trait]
impl super::ReadHalf for Reader {
    async fn receive_message(
        &mut self,
        seq: u64,
        already_received_bytes: &mut Vec<u8>,
        #[cfg(unix)] already_received_fds: &mut Vec<std::os::fd::OwnedFd>,
    ) -> crate::Result<Message> {
        // Whole messages, as a connection over a channel that was handed over leaves unread, go
        // first. Part of one can only have come in over some other socket, and can't go on over a
        // channel, so it is left where it is rather than lost.
        if !already_received_bytes.is_empty() {
            let (primary_header, len) = match already_received_bytes.len() {
                have if have < MIN_MESSAGE_SIZE => return Err(not_whole()),
                have => match read_primary_header(already_received_bytes)? {
                    (_, len) if have < len => return Err(not_whole()),
                    whole => whole,
                },
            };

            return take_message(
                seq,
                &primary_header,
                len,
                already_received_bytes,
                #[cfg(unix)]
                already_received_fds,
            );
        }
        #[cfg(unix)]
        if !already_received_fds.is_empty() {
            return Err(not_whole());
        }

        let msg = self.0.recv().await.map_err(|e| {
            let kind = match e {
                // The other end is closed, which is the end of the input.
                RecvError::Closed => io::ErrorKind::UnexpectedEof,
                RecvError::Overflowed(_) => io::ErrorKind::BrokenPipe,
            };

            crate::Error::InputOutput(io::Error::new(kind, e).into())
        })?;

        // The message has the position that it had at the other end, which is 0 for one built
        // there. It gets its position among the messages of this end instead, as those taken out
        // of what was already received do, so that the positions rise from one to the next.
        Ok(msg.at_position(seq))
    }

    /// Supports passing file descriptors.
    fn can_pass_unix_fd(&self) -> bool {
        true
    }

    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        self_credentials().await
    }

    fn auth_mechanism(&self) -> AuthMechanism {
        AuthMechanism::Anonymous
    }

    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn can_take_unread(&self) -> bool {
        // zruntime's `Receiver::recv` loses no message when dropped, and the reader holds nothing
        // of its own. A channel carries whole messages, so the bytes and file descriptors that a
        // connection over one leaves unread are those of whole messages, which `receive_message`
        // takes back. Part of a message came from a connection over some other socket, and it
        // fails on that rather than lose it.
        true
    }
}

/// The writer half of a [`Channel`].
///
/// This type is only available when the `p2p` feature is enabled.
#[derive(Debug)]
pub struct Writer(Sender<Message>);

#[async_trait::async_trait]
impl super::WriteHalf for Writer {
    // zruntime's `Sender::broadcast` queues the message whole, or not at all if its future is
    // dropped before it resolves, so no send leaves part of a message on the channel.
    async fn send_message(&mut self, msg: &Message) -> crate::Result<()> {
        self.0
            .broadcast(msg.clone())
            .await
            .map_err(|e| {
                crate::Error::InputOutput(io::Error::new(io::ErrorKind::BrokenPipe, e).into())
            })
            .map(|removed| {
                // We don't enable `overflow` mode so items should never be removed.
                assert!(removed.is_none());
            })
    }

    async fn close(&mut self) -> io::Result<()> {
        self.0.close();

        Ok(())
    }

    /// Supports passing file descriptors.
    fn can_pass_unix_fd(&self) -> bool {
        true
    }

    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        self_credentials().await
    }
}

/// The credentials of the current process.
async fn self_credentials() -> io::Result<ConnectionCredentials> {
    let creds = ConnectionCredentials::default();
    // WASI has no process IDs, and std panics where asked for one there.
    #[cfg(not(target_os = "wasi"))]
    let creds = creds.set_process_id(std::process::id());

    #[cfg(unix)]
    let creds = {
        use rustix::process::{getegid, geteuid};

        creds
            .set_unix_user_id(geteuid().as_raw())
            .add_unix_group_id(getegid().as_raw())
    };
    #[cfg(windows)]
    let creds = {
        let sid = crate::win32::ProcessToken::open(None)?.sid()?;

        creds.set_windows_sid(sid)
    };

    Ok(creds)
}

/// The error of a read half of a channel given part of a message, or file descriptors with no
/// message, as already received.
fn not_whole() -> crate::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "a channel carries whole messages, not parts of them",
    )
    .into()
}

const CHANNEL_CAPACITY: NonZeroUsize = NonZeroUsize::new(32).unwrap();

#[cfg(test)]
mod tests {
    use std::io;

    use super::Channel;
    use crate::{
        Error, Message,
        connection::socket::{ReadHalf, WriteHalf},
    };

    /// The read half of a channel takes the whole messages that it is given as already received
    /// ahead of those that come over the channel.
    #[test]
    fn a_reader_takes_whole_messages_already_received_first() {
        let (channel, peer) = Channel::pair();
        let (mut reader, mut writer) = (channel.reader, peer.writer);
        let msgs = [1u32, 2, 3].map(|n| Message::method_call("/", "Test").build(&n).unwrap());
        let mut bytes = [&msgs[0].data()[..], &msgs[1].data()[..]].concat();

        futures_lite::future::block_on(async {
            writer.send_message(&msgs[2]).await.unwrap();
            for (seq, sent) in msgs.iter().enumerate() {
                let received = reader
                    .receive_message(
                        seq as u64,
                        &mut bytes,
                        #[cfg(unix)]
                        &mut vec![],
                    )
                    .await
                    .unwrap();
                assert_eq!(&received.data()[..], &sent.data()[..]);
            }
        });

        assert!(bytes.is_empty());
    }

    /// The read half of a channel gives each message the position that it is given, the messages
    /// that come over the channel as much as those that it is given as already received, so the
    /// positions rise from the ones to the others.
    #[test]
    fn a_reader_gives_each_message_its_position() {
        let (channel, peer) = Channel::pair();
        let (mut reader, mut writer) = (channel.reader, peer.writer);
        let msgs = [1u32, 2].map(|n| Message::method_call("/", "Test").build(&n).unwrap());
        let mut bytes = msgs[0].data().to_vec();

        let positions = futures_lite::future::block_on(async {
            writer.send_message(&msgs[1]).await.unwrap();
            let mut positions = vec![];
            for seq in [1, 2] {
                let received = reader
                    .receive_message(
                        seq,
                        &mut bytes,
                        #[cfg(unix)]
                        &mut vec![],
                    )
                    .await
                    .unwrap();
                positions.push(received.recv_position());
            }

            positions
        });

        assert!(positions[0] < positions[1], "{positions:?}");
    }

    /// A whole message that the read half of a channel is given as already received takes as
    /// many of the file descriptors that it is given as its header counts.
    #[cfg(unix)]
    #[test]
    fn a_reader_takes_the_descriptors_of_whole_messages_already_received() {
        let (channel, _peer) = Channel::pair();
        let mut reader = channel.reader;
        let (read_end, _write_end) = io::pipe().unwrap();
        let msg = Message::method_call("/", "Test")
            .build(&crate::Fd::from(&read_end))
            .unwrap();
        let mut bytes = msg.data().to_vec();
        let mut fds = vec![read_end.into()];

        let received =
            futures_lite::future::block_on(reader.receive_message(0, &mut bytes, &mut fds))
                .unwrap();

        assert_eq!(received.data().fds().len(), 1);
        assert!(bytes.is_empty() && fds.is_empty());
    }

    /// A channel carries whole messages, so its read half fails on part of one that it is given
    /// as already received, rather than lose it, and leaves it where it was.
    #[test]
    fn a_reader_refuses_part_of_a_message_already_received() {
        let (channel, peer) = Channel::pair();
        // With the other end gone, a call that went on to receive would fail on the closed
        // channel rather than wait.
        drop(peer);
        let mut reader = channel.reader;
        let msg = Message::method_call("/", "Test").build(&()).unwrap();

        // Short of a primary header, and short of the whole message.
        for len in [10, msg.data().len() - 1] {
            let mut bytes = msg.data()[..len].to_vec();

            let received = futures_lite::future::block_on(reader.receive_message(
                0,
                &mut bytes,
                #[cfg(unix)]
                &mut vec![],
            ));

            assert_refused(received);
            assert_eq!(bytes, &msg.data()[..len]);
        }
    }

    /// The read half of a channel fails on file descriptors that it is given as already received
    /// with no message to take them, and leaves them where they were.
    #[cfg(unix)]
    #[test]
    fn a_reader_refuses_descriptors_already_received() {
        use std::os::fd::AsRawFd;

        let (channel, peer) = Channel::pair();
        drop(peer);
        let mut reader = channel.reader;
        let (read_end, _write_end) = io::pipe().unwrap();
        let raw = read_end.as_raw_fd();
        let mut fds = vec![read_end.into()];

        let received =
            futures_lite::future::block_on(reader.receive_message(0, &mut vec![], &mut fds));

        assert_refused(received);
        assert_eq!(
            fds.iter().map(AsRawFd::as_raw_fd).collect::<Vec<_>>(),
            [raw]
        );
    }

    /// Asserts that `received` is the error of a read half that refuses what it was given.
    fn assert_refused(received: crate::Result<Message>) {
        let Err(Error::InputOutput(e)) = &received else {
            panic!("the reader didn't refuse what it was given: {received:?}");
        };
        assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{e}");
    }
}
