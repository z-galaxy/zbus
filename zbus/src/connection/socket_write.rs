//! What a connection writes its socket through.

#[cfg(feature = "handover")]
use std::mem;
#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};
use std::{
    future::poll_fn,
    io::{self, IoSlice},
    slice,
    sync::{Arc, atomic::Ordering},
};

#[cfg(all(feature = "handover", any(unix, windows)))]
use super::handover::SocketKind;
use super::socket_reader::SocketStatus;
#[cfg(feature = "handover")]
use crate::Error;
#[cfg(all(feature = "handover", any(unix, windows)))]
use crate::runtime::IoSource;
use crate::{
    Message, Result,
    connection::socket::{WriteHalf, Writes},
    fdo::ConnectionCredentials,
    log::trace,
    runtime::io::{PollSend, write_messages},
};

/// What a connection writes its socket through, which those that write it, shut it down or hand it
/// over take in turn.
#[derive(Debug)]
pub(super) struct SocketWrite {
    /// The write half of the socket, until the connection is handed over.
    half: Option<Box<dyn WriteHalf>>,
    /// How the connection writes the socket, if zbus made it, as [`Writes`] tells, until the
    /// connection is closed. A socket of yours is written through `half` alone.
    writes: Option<Writes>,
    /// Whether `half` can resume writes ([`WriteHalf::can_resume_writes`]), as it said as the
    /// connection was built. Where `writes` doesn't say otherwise, the connection then writes
    /// through the `sendmsg` of `half` itself, rather than hand it whole messages.
    half_resumes: bool,
    /// What is left to write of the message that a write ended in the middle of, if any.
    rest: Rest,
    /// The socket itself, if zbus drives it and so can give a descriptor of it for a hand-over.
    #[cfg(all(feature = "handover", any(unix, windows)))]
    source: Option<(IoSource, SocketKind)>,
    /// Whether a send of whole messages through `half` failed or was given up on midway, which can
    /// leave part of a message in the socket. The peer can then make no sense of anything sent
    /// after it, so the connection can't be handed over any more. A send that failed as the peer
    /// receives nothing more leaves this as it was, as nothing sent after it reaches the peer, and
    /// so does one through the write half of a channel, which queues each message whole or not at
    /// all.
    #[cfg(feature = "handover")]
    torn: bool,
    /// Whether the connection was closed, which shuts the socket down and lets go of what is left
    /// to write, so that a flush fails from then on. It is set before the socket is shut down, so
    /// it also tells a reader that came to the end of the input as the connection was closed apart
    /// from one whose peer closed its end.
    shut_down: bool,
}

impl SocketWrite {
    /// What a connection writes through `half`, or as `writes` tells, if zbus made the socket.
    /// `status` is where the connection keeps how many bytes it has left to write.
    pub(super) fn new(
        half: Box<dyn WriteHalf>,
        writes: Option<Writes>,
        status: Arc<SocketStatus>,
        #[cfg(all(feature = "handover", any(unix, windows)))] source: Option<(
            IoSource,
            SocketKind,
        )>,
    ) -> Self {
        Self {
            half_resumes: half.can_resume_writes(),
            half: Some(half),
            writes,
            rest: Rest { cut: None, status },
            #[cfg(all(feature = "handover", any(unix, windows)))]
            source,
            #[cfg(feature = "handover")]
            torn: false,
            shut_down: false,
        }
    }

    /// Send `msg`, unless the connection was handed over, as [`SocketWrite::send_messages`] does,
    /// but through [`WriteHalf::send_message`] where it hands the message whole to the half.
    // Written out apart from `send_messages`, rather than as a call to it with the one message,
    // so that the compiler knows that there is only the one: every `Connection::send` runs this.
    pub(super) async fn send_message(&mut self, msg: &Message) -> Result<()> {
        match &self.writes {
            Some(Writes::Driven(socket)) => {
                if !self.rest.is_empty() {
                    write_rest_to(&**socket, &mut self.rest).await?;
                }
                write_messages(&**socket, slice::from_ref(msg), |cut| self.rest.set(cut)).await?;

                return Ok(());
            }
            None if self.half_resumes => {
                let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
                if !self.rest.is_empty() {
                    write_rest_through(half, &mut self.rest).await?;
                }
                write_through(half, msg, &mut self.rest).await?;
                half.flush().await?;

                return Ok(());
            }
            _ => (),
        }
        let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
        #[cfg(feature = "handover")]
        let torn = match self.writes {
            // The channel queues the message whole or not at all.
            #[cfg(feature = "p2p")]
            Some(Writes::Whole) => self.torn,
            // Until the message is through, the socket may hold a part of it.
            _ => mem::replace(&mut self.torn, true),
        };
        let sent = half.send_message(msg).await;
        #[cfg(feature = "handover")]
        self.through(torn, &sent);

        sent
    }

    /// Send `msgs`, unless the connection was handed over.
    ///
    /// The connection writes them itself, after what is left of a message that a write ended in
    /// the middle of, if anything, keeping track in `rest` of how far each write takes them: to a
    /// socket that zbus drives, as [`write_messages`] does, or through the `sendmsg` of a half that
    /// can resume writes, a message at a time, flushing the half once they are all written.
    /// Otherwise it hands them whole to [`WriteHalf::send_messages`].
    pub(super) async fn send_messages(&mut self, msgs: &[Message]) -> Result<()> {
        match &self.writes {
            Some(Writes::Driven(socket)) => {
                if !self.rest.is_empty() {
                    write_rest_to(&**socket, &mut self.rest).await?;
                }
                write_messages(&**socket, msgs, |cut| self.rest.set(cut)).await?;

                return Ok(());
            }
            None if self.half_resumes => {
                let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
                if !self.rest.is_empty() {
                    write_rest_through(half, &mut self.rest).await?;
                }
                for msg in msgs {
                    write_through(half, msg, &mut self.rest).await?;
                }
                half.flush().await?;

                return Ok(());
            }
            _ => (),
        }
        let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
        #[cfg(feature = "handover")]
        let torn = match self.writes {
            // The channel queues each message whole or not at all.
            #[cfg(feature = "p2p")]
            Some(Writes::Whole) => self.torn,
            // Until the messages are through, the socket may hold a part of one.
            _ => mem::replace(&mut self.torn, true),
        };
        let sent = half.send_messages(msgs).await;
        #[cfg(feature = "handover")]
        self.through(torn, &sent);

        sent
    }

    /// Write what is left of a message that a write ended in the middle of, if anything, waiting
    /// on the socket for as long as it takes, then flush the write half, as [`WriteHalf::flush`]
    /// does, unless the connection was closed or handed over.
    pub(super) async fn flush(&mut self) -> Result<()> {
        if self.shut_down {
            return Err(closed().into());
        }
        self.write_rest().await?;
        let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
        half.flush().await?;

        Ok(())
    }

    /// The credentials of the peer, as [`WriteHalf::peer_credentials`] gives them, unless the
    /// connection was handed over.
    pub(super) async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        let half = self.half.as_deref_mut().ok_or_else(handed_over)?;

        half.peer_credentials().await
    }

    /// Shut the socket down, as [`WriteHalf::close`] does, unless the connection was handed over,
    /// which leaves the socket alone: it is no longer the connection's.
    ///
    /// Nothing more is written then: what is left to write of a message is let go of, and a flush
    /// fails from then on. `writes` is let go of too, as a pipe to a helper process, which the
    /// connection writes directly, closes as the last of its descriptors goes, and sends go
    /// through `half` from then on, which fails them.
    pub(super) async fn close(&mut self) -> io::Result<()> {
        let Some(half) = self.half.as_deref_mut() else {
            return Ok(());
        };
        // Set before the socket is shut down, as the shutdown can be given up on midway, and as
        // the reader can come to the end of the input as soon as it is.
        self.shut_down = true;
        self.rest.set(None);
        self.writes = None;

        half.close().await
    }

    /// Write what is left of a message that a write ended in the middle of, for the connection to
    /// be handed over, waiting on the socket for as long as it takes. If the peer receives nothing
    /// more, nothing that follows the part of the message that it got can be misread, so what is
    /// left is let go of instead.
    #[cfg(feature = "handover")]
    pub(super) async fn write_rest_for_hand_over(&mut self) -> Result<()> {
        match self.write_rest().await {
            Err(e) if receives_nothing_more(&e) => {
                self.rest.set(None);

                Ok(())
            }
            written => Ok(written?),
        }
    }

    /// Whether a send of whole messages through `half` failed or was given up on midway, which can
    /// leave part of a message in the socket.
    #[cfg(feature = "handover")]
    pub(super) fn is_torn(&self) -> bool {
        self.torn
    }

    /// Whether the connection was closed.
    #[cfg(feature = "handover")]
    pub(super) fn is_shut_down(&self) -> bool {
        self.shut_down
    }

    /// The socket itself, if zbus drives it and so can give a descriptor of it for a hand-over.
    #[cfg(all(feature = "handover", any(unix, windows)))]
    pub(super) fn source(&self) -> Option<&(IoSource, SocketKind)> {
        self.source.as_ref()
    }

    /// Take the write half out, for the connection to be handed over, with how the connection
    /// writes the socket, if zbus made it, for a connection that goes on over the half to write it
    /// the same way. Let go of all else that the connection holds of the socket. Nothing is
    /// written through the connection from then on.
    #[cfg(feature = "handover")]
    pub(super) fn take_half(&mut self) -> Option<(Box<dyn WriteHalf>, Option<Writes>)> {
        #[cfg(any(unix, windows))]
        {
            self.source = None;
        }
        let half = self.half.take()?;

        Some((half, self.writes.take()))
    }

    /// Write what is left of a message that a write ended in the middle of, if anything, waiting
    /// on the socket for as long as it takes.
    async fn write_rest(&mut self) -> io::Result<()> {
        match &self.writes {
            Some(Writes::Driven(socket)) => write_rest_to(&**socket, &mut self.rest).await,
            None if self.half_resumes => {
                let half = self.half.as_deref_mut().ok_or_else(handed_over)?;

                write_rest_through(half, &mut self.rest).await
            }
            // Only a send that keeps track of how far each write takes it leaves the rest of a
            // message.
            _ => Ok(()),
        }
    }

    /// Put back `torn`, whether the socket was torn before a send, now that the send is through
    /// with `sent`, unless the send failed while the peer may still receive what follows it.
    #[cfg(feature = "handover")]
    fn through(&mut self, torn: bool, sent: &Result<()>) {
        if sent
            .as_ref()
            .is_err_and(|e| !matches!(e, Error::InputOutput(e) if receives_nothing_more(e)))
        {
            return;
        }
        self.torn = torn;
    }
}

/// Write what is left of a message that a write ended in the middle of, if anything, to `socket`,
/// waiting on it for as long as it takes. Its file descriptors went with its first byte.
async fn write_rest_to(socket: &dyn PollSend, rest: &mut Rest) -> io::Result<()> {
    while let Some(left) = rest.left() {
        let written = poll_fn(|cx| {
            socket.poll_send(
                cx,
                &[IoSlice::new(left)],
                #[cfg(unix)]
                &[],
            )
        })
        .await?;
        rest.advance(written);
    }

    Ok(())
}

/// Write what is left of a message that a write ended in the middle of, if anything, through the
/// `sendmsg` of `half`. Its file descriptors went with its first byte.
async fn write_rest_through(half: &mut dyn WriteHalf, rest: &mut Rest) -> io::Result<()> {
    while let Some(left) = rest.left() {
        let written = sendmsg(
            half,
            left,
            #[cfg(unix)]
            &[],
        )
        .await?;
        rest.advance(written);
    }

    Ok(())
}

/// Write `msg` through the `sendmsg` of `half`, the file descriptors of the message along with its
/// first byte, keeping track in `rest` of how far each call takes it.
async fn write_through(half: &mut dyn WriteHalf, msg: &Message, rest: &mut Rest) -> io::Result<()> {
    trace!("Sending message: {:?}", msg);
    let data = msg.data();
    let mut written = 0;
    while written < data.len() {
        #[cfg(unix)]
        let fds: Vec<_> = if written == 0 {
            data.fds().iter().map(|fd| fd.as_fd()).collect()
        } else {
            vec![]
        };
        written += sendmsg(
            half,
            &data[written..],
            #[cfg(unix)]
            &fds,
        )
        .await?;
        rest.set((written < data.len()).then_some((msg, written)));
    }
    trace!(
        "Sent message with serial: {}",
        msg.primary_header().serial_num()
    );

    Ok(())
}

/// One call to the `sendmsg` of `half`. A call that takes none of `buffer` fails, rather than leave
/// the connection to call again for ever.
async fn sendmsg(
    half: &mut dyn WriteHalf,
    buffer: &[u8],
    #[cfg(unix)] fds: &[BorrowedFd<'_>],
) -> io::Result<usize> {
    match half
        .sendmsg(
            buffer,
            #[cfg(unix)]
            fds,
        )
        .await?
    {
        0 => Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "the write half took none of what it was given",
        )),
        written => Ok(written),
    }
}

/// What is left to write of the message that a write ended in the middle of, if any.
#[derive(Debug)]
struct Rest {
    /// The message, and how many of its bytes are written.
    cut: Option<(Message, usize)>,
    /// Where how many bytes are left is kept, for `Connection::unwritten_len` to read.
    status: Arc<SocketStatus>,
}

impl Rest {
    /// Whether nothing is left.
    fn is_empty(&self) -> bool {
        self.cut.is_none()
    }

    /// What is left of the message, if anything.
    fn left(&self) -> Option<&[u8]> {
        self.cut
            .as_ref()
            .map(|(msg, written)| &msg.data()[*written..])
    }

    /// Note how far a write took the messages that it wrote: `written` bytes into `msg`, or to the
    /// end of a message, if `cut` is `None`.
    ///
    /// Inlined, as every write calls it, most of the time to note what the write before it did
    /// too: that it ended at the end of a message.
    #[inline]
    fn set(&mut self, cut: Option<(&Message, usize)>) {
        let Some((msg, written)) = cut else {
            if self.cut.take().is_some() {
                self.status.unwritten.store(0, Ordering::Relaxed);
            }

            return;
        };
        self.status
            .unwritten
            .store(msg.data().len() - written, Ordering::Relaxed);
        self.cut = Some((msg.clone(), written));
    }

    /// Note that a write took `len` more bytes of what is left of the message.
    fn advance(&mut self, len: usize) {
        let Some((msg, written)) = &mut self.cut else {
            return;
        };
        *written += len;
        let left = msg.data().len() - *written;
        self.status.unwritten.store(left, Ordering::Relaxed);
        if left == 0 {
            trace!(
                "Sent message with serial: {}",
                msg.primary_header().serial_num()
            );
            self.cut = None;
        }
    }
}

/// Whether `error`, which a write failed with, says that the peer receives nothing more, as one
/// that closed its end of the socket does: an I/O error of kind `BrokenPipe`, `ConnectionReset`
/// or `ConnectionAborted`. Nothing sent from then on reaches the peer, so part of a message that
/// the write left in the socket is followed by nothing that the peer could misread.
///
/// A connection that was closed fails its sends with `BrokenPipe` too, as the close shuts the
/// socket down, but `SocketWrite::shut_down` keeps it from being handed over anyway.
#[cfg(feature = "handover")]
fn receives_nothing_more(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
    )
}

/// The error of what a connection that was handed over can no longer do.
fn handed_over() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "the connection was handed over",
    )
}

/// The error of what a connection that was closed can no longer do.
fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "the connection was closed")
}

#[cfg(all(test, unix, feature = "p2p"))]
mod tests {
    #[cfg(feature = "unixexec")]
    use std::path::Path;
    use std::{
        future::Future,
        io::{self, ErrorKind, IoSlice, Read, Write},
        os::{
            fd::{AsFd, BorrowedFd, OwnedFd},
            unix::net::UnixStream,
        },
        pin::{Pin, pin},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread::{self, JoinHandle},
        time::Duration,
    };

    use futures_lite::future::poll_once;
    use ntest::timeout;
    use test_log::test;

    #[cfg(feature = "handover")]
    use crate::MessageStream;
    #[cfg(feature = "unixexec")]
    use crate::{
        Address,
        address::transport::{Transport, Unixexec},
    };
    use crate::{
        Connection, Error, Fd, Guid, Message, Result,
        connection::{
            Builder,
            socket::{Channel, ReadHalf, RecvmsgResult, Socket, Split, WriteHalf},
        },
        runtime::{
            IoSource, Runtime,
            io::{Reader, RegisteredIo, SocketOps, UnixOps, registered, tests::Recording},
            test_runtime::under_every_runtime,
        },
    };

    /// A send dropped midway, as the peer doesn't read, leaves the rest of its message, which the
    /// next send writes first: the peer gets the first message whole, then the second.
    #[test]
    #[timeout(15000)]
    fn a_send_dropped_midway_is_finished_by_the_next_send() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;

            finish_a_send_dropped_midway(&runtime, &conn, &peer).await;
        });
    }

    /// `Connection::flush` writes the rest of a message that a send was dropped in the middle of,
    /// and nothing else.
    #[test]
    #[timeout(15000)]
    fn flush_writes_what_a_dropped_send_left() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            let big = big_signal();
            stuck(&runtime, pin!(conn.send(&big)), &peer).await;

            let reading = read_in_the_background(&peer, big.data().len());
            conn.flush().await.unwrap();

            assert_eq!(reading.join().unwrap(), &big.data()[..]);
            assert_eq!(conn.unwritten_len(), 0);
            assert_nothing_to_read(&peer);
            // With nothing left to write, a flush has nothing to wait for.
            conn.flush().await.unwrap();
        });
    }

    /// A send dropped before it wrote any of its message, as it was writing the rest of one that
    /// another send was dropped in the middle of, doesn't send it.
    #[test]
    #[timeout(15000)]
    fn a_message_none_of_which_was_written_is_not_sent() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            let (big, dropped, next) = (big_signal(), signal(1), signal(2));
            stuck(&runtime, pin!(conn.send(&big)), &peer).await;

            let sent = poll_once(pin!(conn.send(&dropped))).await;
            assert!(
                sent.is_none(),
                "the socket took the rest after all: {sent:?}"
            );
            let reading = read_in_the_background(&peer, big.data().len() + next.data().len());
            conn.send(&next).await.unwrap();

            let received = reading.join().unwrap();
            assert_eq!(received, [&big.data()[..], &next.data()[..]].concat());
            assert_nothing_to_read(&peer);
        });
    }

    /// A send of several messages dropped in the middle of one of them leaves the rest of that
    /// one, after those before it, which went whole, and doesn't send those after it.
    #[test]
    #[timeout(15000)]
    fn a_send_all_dropped_midway_sends_none_of_the_messages_after_its_cut() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            let (before, big, after, next) = (signal(1), big_signal(), signal(2), signal(3));
            let msgs = [before.clone(), big.clone(), after];
            stuck(&runtime, pin!(conn.send_all(&msgs)), &peer).await;

            let unwritten = conn.unwritten_len();
            assert!(
                0 < unwritten && unwritten < big.data().len(),
                "{unwritten} of {} bytes left to write",
                big.data().len(),
            );
            let len = before.data().len() + big.data().len() + next.data().len();
            let reading = read_in_the_background(&peer, len);
            conn.send(&next).await.unwrap();

            let received = reading.join().unwrap();
            let sent = [&before.data()[..], &big.data()[..], &next.data()[..]].concat();
            assert_eq!(received, sent);
            assert_nothing_to_read(&peer);
        });
    }

    /// A send that fails midway, on an I/O error, leaves the rest of its message, which the next
    /// send writes first.
    #[test]
    #[timeout(15000)]
    fn a_send_that_fails_midway_leaves_the_rest_of_its_message() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = over(&runtime, FailingMidway::default()).await;
            let (msg, next) = (signal(1), signal(2));

            let failed = conn.send(&msg).await;

            let Err(Error::InputOutput(e)) = &failed else {
                panic!("the send didn't fail on the write: {failed:?}");
            };
            assert_eq!(e.kind(), ErrorKind::Other, "{e}");
            assert_eq!(conn.unwritten_len(), msg.data().len() - CUT);
            conn.send(&next).await.unwrap();
            let mut received = vec![0; msg.data().len() + next.data().len()];
            (&peer).read_exact(&mut received).unwrap();
            assert_eq!(received, [&msg.data()[..], &next.data()[..]].concat());
            assert_eq!(conn.unwritten_len(), 0);
        });
    }

    /// Closing a connection lets go of what it hasn't written: the peer gets the part of the
    /// message that the socket took, then the end of the connection. A flush fails from then on,
    /// as a send does.
    #[test]
    #[timeout(15000)]
    fn closing_a_connection_lets_go_of_what_it_has_not_written() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            let big = big_signal();
            stuck(&runtime, pin!(conn.send(&big)), &peer).await;

            conn.clone().close().await.unwrap();

            assert_eq!(conn.unwritten_len(), 0);
            let flushed = conn.flush().await;
            let Err(Error::InputOutput(e)) = &flushed else {
                panic!("the flush of a closed connection didn't fail: {flushed:?}");
            };
            assert_eq!(e.kind(), ErrorKind::NotConnected, "{e}");
            let sent = conn.send(&signal(1)).await;
            assert!(sent.is_err(), "a send on a closed connection went through");
            let mut received = vec![];
            (&peer).read_to_end(&mut received).unwrap();
            assert!(
                received.len() < big.data().len(),
                "the peer got the whole message"
            );
        });
    }

    /// The file descriptors of a message go with its first byte, so a message cut off midway
    /// comes with them once, and the rest of it goes without them.
    #[test]
    #[timeout(15000)]
    fn the_rest_of_a_message_goes_without_its_descriptors() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            let (read_end, _write_end) = io::pipe().unwrap();
            let big = Message::signal(PATH, INTERFACE, "Big")
                .build(&(Fd::from(read_end.as_fd()), vec![0u8; 1024 * 1024]))
                .unwrap();
            let next = signal(1);
            stuck(&runtime, pin!(conn.send(&big)), &peer).await;

            let mut peer = Reader::new(Arc::new(registered(&runtime, peer, UnixOps).unwrap()));
            let receiving = async {
                let (mut bytes, mut fds) = (vec![], vec![]);
                let first = peer.receive_message(0, &mut bytes, &mut fds).await.unwrap();
                let second = peer.receive_message(1, &mut bytes, &mut fds).await.unwrap();

                (first, second)
            };
            let (sent, (first, second)) = futures_util::join!(conn.send(&next), receiving);

            sent.unwrap();
            assert_eq!(&first.data()[..], &big.data()[..]);
            assert_eq!(first.data().fds().len(), 1);
            assert_eq!(&second.data()[..], &next.data()[..]);
            assert_eq!(second.data().fds().len(), 0);
        });
    }

    /// A send to a channel that is full waits for room, and one that is dropped meanwhile doesn't
    /// send its message, as a channel takes each message whole or not at all. So nothing is left
    /// to write, and the connection can still be handed over.
    #[test]
    #[timeout(15000)]
    fn a_send_dropped_on_a_full_channel_sends_nothing() {
        under_every_runtime(|runtime| async move {
            let (ours, theirs) = Channel::pair();
            let conn = on(
                Builder::authenticated_socket(ours, Guid::generate()),
                &runtime,
            )
            .p2p()
            .build()
            .await
            .unwrap();
            let (mut peer, _sends_nothing) = Socket::split(theirs).take();
            // Signals until the channel takes no more, and then one more, which waits for room.
            let mut queued = vec![];
            loop {
                let msg = signal(queued.len() as u32);
                match poll_once(pin!(conn.send(&msg))).await {
                    Some(sent) => sent.unwrap(),
                    None => break,
                }
                queued.push(msg);
            }

            assert_eq!(conn.unwritten_len(), 0);
            let (mut bytes, mut fds) = (vec![], vec![]);
            let mut received = vec![peer.receive_message(0, &mut bytes, &mut fds).await.unwrap()];
            let next = signal(u32::MAX);
            conn.send(&next).await.unwrap();
            while received.len() < queued.len() + 1 {
                let seq = received.len() as u64;
                let msg = peer.receive_message(seq, &mut bytes, &mut fds).await;
                received.push(msg.unwrap());
            }
            let received: Vec<_> = received.iter().map(|msg| msg.data().to_vec()).collect();
            let sent: Vec<_> = queued
                .iter()
                .chain([&next])
                .map(|msg| msg.data().to_vec())
                .collect();
            assert_eq!(received, sent);
            #[cfg(feature = "handover")]
            {
                let handed_over = MessageStream::from(&conn).hand_over().await;
                assert!(handed_over.is_ok(), "{handed_over:?}");
            }
        });
    }

    /// A connection over a write half of your own that can resume writes writes through its
    /// `sendmsg`, with the file descriptors of each message along with its first byte, and flushes
    /// it once a send wrote all of its messages. It never hands it a whole message.
    #[test]
    #[timeout(15000)]
    fn a_half_that_can_resume_writes_is_written_through_sendmsg() {
        under_every_runtime(|runtime| async move {
            let (conn, peer, counts) = over_a_half_of_your_own(&runtime, true, None).await;
            let pipes = [pipe(), pipe(), pipe()];
            let fd = |i: usize| Fd::from(pipes[i].as_fd());
            let msgs = [
                signal(1),
                Message::signal(PATH, INTERFACE, "WithFds")
                    .build(&fd(0))
                    .unwrap(),
                signal(2),
                Message::signal(PATH, INTERFACE, "WithFds")
                    .build(&(fd(1), fd(2)))
                    .unwrap(),
            ];

            conn.send_all(&msgs[..3]).await.unwrap();
            assert_eq!(counts.flushes.load(Ordering::Relaxed), 1);
            conn.send(&msgs[3]).await.unwrap();
            assert_eq!(counts.flushes.load(Ordering::Relaxed), 2);
            conn.flush().await.unwrap();
            assert_eq!(counts.flushes.load(Ordering::Relaxed), 3);

            let mut peer = Reader::new(Arc::new(registered(&runtime, peer, UnixOps).unwrap()));
            let (mut bytes, mut fds) = (vec![], vec![]);
            for (seq, sent) in msgs.iter().enumerate() {
                let received = peer.receive_message(seq as u64, &mut bytes, &mut fds).await;
                let received = received.unwrap();
                assert_eq!(&received.data()[..], &sent.data()[..]);
                assert_eq!(received.data().fds().len(), sent.data().fds().len());
            }
        });
    }

    /// A send through a write half of your own that can resume writes, dropped midway, is finished
    /// by the next send, as one over a socket that zbus drives is.
    #[test]
    #[timeout(15000)]
    fn a_send_through_a_half_that_can_resume_writes_dropped_midway_is_finished() {
        under_every_runtime(|runtime| async move {
            let (conn, peer, _) = over_a_half_of_your_own(&runtime, true, None).await;

            finish_a_send_dropped_midway(&runtime, &conn, &peer).await;
        });
    }

    /// A send through a write half of your own that can resume writes, which fails midway, leaves
    /// the rest of its message, which the next send writes first.
    #[test]
    #[timeout(15000)]
    fn a_send_through_a_half_that_can_resume_writes_that_fails_midway_leaves_the_rest() {
        under_every_runtime(|runtime| async move {
            // The second call to `sendmsg` fails, once the first took `CAP` bytes.
            let (conn, peer, _) = over_a_half_of_your_own(&runtime, true, Some(2)).await;
            let (msg, next) = (signal(1), signal(2));

            let failed = conn.send(&msg).await;

            let Err(Error::InputOutput(e)) = &failed else {
                panic!("the send didn't fail on the write: {failed:?}");
            };
            assert_eq!(e.kind(), ErrorKind::Other, "{e}");
            assert_eq!(conn.unwritten_len(), msg.data().len() - CAP);
            conn.send(&next).await.unwrap();
            let mut received = vec![0; msg.data().len() + next.data().len()];
            (&peer).read_exact(&mut received).unwrap();
            assert_eq!(received, [&msg.data()[..], &next.data()[..]].concat());
            assert_eq!(conn.unwritten_len(), 0);
        });
    }

    /// A connection over a write half of your own that can't resume writes hands it each send as a
    /// whole, so a send dropped midway leaves nothing for the connection to write, and a flush
    /// only flushes the half.
    #[test]
    #[timeout(15000)]
    fn a_half_that_cant_resume_writes_is_given_whole_messages() {
        under_every_runtime(|runtime| async move {
            let (conn, peer, counts) = over_a_half_of_your_own(&runtime, false, None).await;
            let msgs = [signal(1), signal(2), signal(3)];

            conn.send(&msgs[0]).await.unwrap();
            conn.send_all(&msgs[1..]).await.unwrap();
            assert_eq!(counts.whole.load(Ordering::Relaxed), 2);
            let mut received = vec![0; msgs.iter().map(|msg| msg.data().len()).sum()];
            (&peer).read_exact(&mut received).unwrap();
            let sent: Vec<u8> = msgs.iter().flat_map(|msg| msg.data().to_vec()).collect();
            assert_eq!(received, sent);

            stuck(&runtime, pin!(conn.send(&big_signal())), &peer).await;

            assert_eq!(conn.unwritten_len(), 0);
            let flushed = poll_once(pin!(conn.flush())).await;
            assert!(matches!(flushed, Some(Ok(()))), "{flushed:?}");
            assert_eq!(counts.flushes.load(Ordering::Relaxed), 1);
        });
    }

    /// The halves of a socket that zbus drives, given to a connection as halves of your own, can
    /// resume writes, so a send dropped midway is finished by the next send, as over the socket.
    #[test]
    #[timeout(15000)]
    fn the_halves_of_a_socket_that_zbus_drives_resume_writes_as_halves_of_your_own() {
        under_every_runtime(|runtime| async move {
            let (ours, peer) = UnixStream::pair().unwrap();
            let socket = registered(&runtime, ours, UnixOps).unwrap();
            let (read, write) = Socket::split(socket).take();
            let split = Split::new(
                Box::new(read) as Box<dyn ReadHalf>,
                Box::new(write) as Box<dyn WriteHalf>,
            );
            let conn = on(
                Builder::authenticated_socket(split, Guid::generate()),
                &runtime,
            )
            .p2p()
            .build()
            .await
            .unwrap();

            finish_a_send_dropped_midway(&runtime, &conn, &peer).await;
        });
    }

    /// A send through a write half of your own that can resume writes, cut off midway, fails no
    /// hand-over: the hand-over writes the rest of its message through the half first, once the
    /// peer reads.
    #[cfg(feature = "handover")]
    #[test]
    #[timeout(15000)]
    fn the_rest_of_a_send_through_a_half_that_can_resume_writes_goes_out_before_a_hand_over() {
        under_every_runtime(|runtime| async move {
            let (conn, peer, _) = over_a_half_of_your_own(&runtime, true, None).await;
            let big = big_signal();
            stuck(&runtime, pin!(conn.send(&big)), &peer).await;

            let reading = read_in_the_background(&peer, big.data().len());
            let handed_over = MessageStream::from(&conn).hand_over().await;

            assert!(handed_over.is_ok(), "{handed_over:?}");
            assert_eq!(reading.join().unwrap(), &big.data()[..]);
            assert_eq!(conn.unwritten_len(), 0);
        });
    }

    /// A connection over a socket that zbus drives writes the messages of a send several at a
    /// time, as the write half of the socket does: each write starts at a message and takes up to
    /// 16 of them, and a message with file descriptors goes in a write of its own, which they go
    /// with. A message sent on its own goes in a write of its own.
    #[test]
    #[timeout(15000)]
    fn messages_go_in_writes_of_several() {
        under_every_runtime(|runtime| async move {
            let ops = Recording::default();
            let writes = ops.writes.clone();
            let (conn, theirs) = over(&runtime, ops).await;
            let mut peer = Reader::new(Arc::new(registered(&runtime, theirs, UnixOps).unwrap()));
            // More messages without descriptors than a write takes, then one with, one without,
            // and another with.
            let (read_end, _write_end) = io::pipe().unwrap();
            let msgs: Vec<_> = (0..22u32)
                .map(|i| {
                    let builder = Message::method_call("/", "Test");
                    match i {
                        18 | 20 => builder.build(&(i, Fd::from(read_end.as_fd()))),
                        _ => builder.build(&i),
                    }
                    .unwrap()
                })
                .collect();

            conn.send_all(&msgs[..21]).await.unwrap();
            conn.send(&msgs[21]).await.unwrap();

            assert_eq!(
                *writes.lock().unwrap(),
                [(16, 0), (2, 0), (1, 1), (1, 0), (1, 1), (1, 0)],
                "the writes were not split as expected, as (messages, descriptors)",
            );
            for (seq, sent) in msgs.iter().enumerate() {
                let received = peer
                    .receive_message(seq as u64, &mut vec![], &mut vec![])
                    .await
                    .unwrap();
                assert_eq!(&received.data()[..], &sent.data()[..]);
                assert_eq!(received.data().fds().len(), sent.data().fds().len());
            }
        });
    }

    /// Closing a connection over the pipes of a `unixexec:` helper process closes its standard
    /// input, which the connection writes directly: the helper comes to the end of its input.
    #[cfg(feature = "unixexec")]
    #[test]
    #[timeout(15000)]
    fn closing_a_unixexec_connection_ends_the_input_of_the_helper() {
        under_every_runtime(|runtime| async move {
            let dir = tempfile::tempdir().unwrap();
            let ended = dir.path().join("ended");
            // The helper answers the handshake, takes in all that it is sent until its input ends,
            // and then says so.
            let script = "read -r line; printf 'OK 0123456789abcdef0123456789abcdef\\r\\n'; \
                          cat >/dev/null; : >\"$0\"";
            let helper = Unixexec::new(
                "sh".into(),
                None,
                vec!["-c".into(), script.into(), ended.clone().into()],
            );
            let address = Address::from(Transport::Unixexec(helper));
            let conn = on(Builder::address(address).p2p(), &runtime)
                .build()
                .await
                .unwrap();
            conn.send(&signal(1)).await.unwrap();

            conn.clone().close().await.unwrap();

            wait_for(&ended);
        });
    }

    /// The server end of a peer-to-peer connection over a unix socket pair, on `runtime`, which
    /// zbus drives itself, and the other end of the pair, through which the test plays the client.
    async fn served(runtime: &Runtime) -> (Connection, UnixStream) {
        let (client, server) = UnixStream::pair().unwrap();
        // The client's side of the handshake, all at once, which the server takes as it comes.
        let id = hex::encode(rustix::process::geteuid().as_raw().to_string());
        let commands = format!("\0AUTH EXTERNAL {id}\r\nNEGOTIATE_UNIX_FD\r\nBEGIN\r\n");
        (&client).write_all(commands.as_bytes()).unwrap();
        let guid = Guid::generate();

        let conn = on(Builder::unix_stream(server).server(&guid).p2p(), runtime)
            .build()
            .await
            .unwrap();

        let answer = format!("OK {guid}\r\nAGREE_UNIX_FD\r\n");
        let mut answered = vec![0; answer.len()];
        (&client).read_exact(&mut answered).unwrap();
        assert_eq!(answered, answer.as_bytes());

        (conn, client)
    }

    /// A peer-to-peer connection on `runtime` over one end of a unix socket pair, which zbus
    /// drives with `ops`, and the other end, through which the test plays the peer.
    async fn over<O>(runtime: &Runtime, ops: O) -> (Connection, UnixStream)
    where
        O: SocketOps,
    {
        let (ours, theirs) = UnixStream::pair().unwrap();
        let socket = registered(runtime, ours, ops).unwrap();
        let conn = on(
            Builder::authenticated_socket(socket, Guid::generate()),
            runtime,
        )
        .p2p()
        .build()
        .await
        .unwrap();

        (conn, theirs)
    }

    /// The operations of a unix socket whose first send takes no more than `CUT` bytes, and whose
    /// second fails.
    #[derive(Debug, Default)]
    struct FailingMidway {
        sends: AtomicUsize,
    }

    impl SocketOps for FailingMidway {
        fn recv(&self, source: &IoSource, buffer: &mut [u8]) -> RecvmsgResult {
            UnixOps.recv(source, buffer)
        }

        fn send(
            &self,
            source: &IoSource,
            buffers: &[IoSlice<'_>],
            fds: &[BorrowedFd<'_>],
        ) -> io::Result<usize> {
            match self.sends.fetch_add(1, Ordering::Relaxed) {
                0 => UnixOps.send(source, &[IoSlice::new(&buffers[0][..CUT])], fds),
                1 => Err(io::Error::other("the test failed this send")),
                _ => UnixOps.send(source, buffers, fds),
            }
        }

        fn can_pass_unix_fd(&self) -> bool {
            true
        }
    }

    /// A peer-to-peer connection over one end of a unix socket pair, on `runtime`, through a write
    /// half of your own, and the other end of the pair. The half can resume writes as `resumes`
    /// says, takes at most `CAP` bytes in each call to `sendmsg` if it can, and fails the call
    /// numbered `fail_at`, if any.
    async fn over_a_half_of_your_own(
        runtime: &Runtime,
        resumes: bool,
        fail_at: Option<usize>,
    ) -> (Connection, UnixStream, Arc<Counts>) {
        let (ours, theirs) = UnixStream::pair().unwrap();
        let socket = Arc::new(registered(runtime, ours, UnixOps).unwrap());
        let counts = Arc::new(Counts::default());
        let half = OfYourOwn {
            socket: socket.clone(),
            resumes,
            calls: 0,
            fail_at,
            counts: counts.clone(),
        };
        let split = Split::new(
            Box::new(Reader::new(socket)) as Box<dyn ReadHalf>,
            Box::new(half) as Box<dyn WriteHalf>,
        );
        let conn = on(
            Builder::authenticated_socket(split, Guid::generate()),
            runtime,
        )
        .p2p()
        .build()
        .await
        .unwrap();

        (conn, theirs, counts)
    }

    /// A write half of your own, over a unix socket that zbus drives.
    #[derive(Debug)]
    struct OfYourOwn {
        socket: Arc<RegisteredIo<UnixOps>>,
        /// Whether it can resume writes. If it can, it takes at most `CAP` bytes in each call to
        /// `sendmsg`, and fails if it is handed a whole message.
        resumes: bool,
        /// How many calls to `sendmsg` were made.
        calls: usize,
        /// The call to `sendmsg` that fails, if any.
        fail_at: Option<usize>,
        counts: Arc<Counts>,
    }

    /// What was asked of an [`OfYourOwn`].
    #[derive(Debug, Default)]
    struct Counts {
        /// How many sends it was handed whole.
        whole: AtomicUsize,
        /// How many times it was flushed.
        flushes: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl WriteHalf for OfYourOwn {
        async fn send_message(&mut self, msg: &Message) -> Result<()> {
            assert!(
                !self.resumes,
                "a half that can resume writes was handed {msg:?}"
            );
            self.counts.whole.fetch_add(1, Ordering::Relaxed);

            WriteHalf::send_message(&mut self.socket, msg).await
        }

        async fn send_messages(&mut self, msgs: &[Message]) -> Result<()> {
            assert!(
                !self.resumes,
                "a half that can resume writes was handed {msgs:?}"
            );
            self.counts.whole.fetch_add(1, Ordering::Relaxed);

            WriteHalf::send_messages(&mut self.socket, msgs).await
        }

        async fn sendmsg(&mut self, buffer: &[u8], fds: &[BorrowedFd<'_>]) -> io::Result<usize> {
            self.calls += 1;
            if self.fail_at == Some(self.calls) {
                return Err(io::Error::other("the test failed this write"));
            }
            let len = if self.resumes {
                buffer.len().min(CAP)
            } else {
                buffer.len()
            };

            WriteHalf::sendmsg(&mut self.socket, &buffer[..len], fds).await
        }

        async fn flush(&mut self) -> io::Result<()> {
            self.counts.flushes.fetch_add(1, Ordering::Relaxed);

            Ok(())
        }

        async fn close(&mut self) -> io::Result<()> {
            WriteHalf::close(&mut self.socket).await
        }

        fn can_pass_unix_fd(&self) -> bool {
            true
        }

        fn can_resume_writes(&self) -> bool {
            self.resumes
        }
    }

    /// Has a send of a message longer than the socket of `peer` holds dropped midway, as `peer`
    /// doesn't read, then makes another send as it reads at last: `peer` gets the first message
    /// whole, then the second.
    async fn finish_a_send_dropped_midway(runtime: &Runtime, conn: &Connection, peer: &UnixStream) {
        let (big, next) = (big_signal(), signal(1));
        stuck(runtime, pin!(conn.send(&big)), peer).await;

        let unwritten = conn.unwritten_len();
        assert!(
            0 < unwritten && unwritten < big.data().len(),
            "{unwritten} of {} bytes left to write",
            big.data().len(),
        );
        let reading = read_in_the_background(peer, big.data().len() + next.data().len());
        conn.send(&next).await.unwrap();

        let received = reading.join().unwrap();
        assert_eq!(received, [&big.data()[..], &next.data()[..]].concat());
        assert_eq!(conn.unwritten_len(), 0);
    }

    /// Polls `sending` until it wrote part of its message to the socket of `peer`, which doesn't
    /// read, and waits for room for the rest, then drops it.
    async fn stuck<F>(runtime: &Runtime, mut sending: Pin<&mut F>, peer: &UnixStream)
    where
        F: Future<Output = Result<()>>,
    {
        while rustix::io::ioctl_fionread(peer).unwrap() == 0 {
            let sent = poll_once(sending.as_mut()).await;
            assert!(
                sent.is_none(),
                "the socket took the whole message: {sent:?}"
            );
            // For the runtime to see the socket writable, if it waits for that first.
            runtime.sleep(Duration::from_millis(1)).await;
        }
    }

    /// Reads `len` bytes off `peer`, on a thread of its own, as a peer that reads at last.
    fn read_in_the_background(peer: &UnixStream, len: usize) -> JoinHandle<Vec<u8>> {
        let peer = peer.try_clone().unwrap();

        thread::spawn(move || {
            let mut received = vec![0; len];
            (&peer).read_exact(&mut received).unwrap();

            received
        })
    }

    /// Asserts that `peer` has nothing to read.
    fn assert_nothing_to_read(peer: &UnixStream) {
        peer.set_nonblocking(true).unwrap();
        let read = (&*peer).read(&mut [0; 1]);
        assert!(
            matches!(&read, Err(e) if e.kind() == ErrorKind::WouldBlock),
            "the peer got {read:?}",
        );
    }

    /// `builder`, to build its connection on `runtime`.
    fn on<'a>(builder: Builder<'a>, runtime: &Runtime) -> Builder<'a> {
        match runtime {
            // A builder that is given no runtime takes the one that the test runs on.
            #[cfg(feature = "default-rt")]
            Runtime::ZRuntime(_) => builder,
            #[cfg(feature = "tokio")]
            Runtime::Tokio(_) => builder,
            Runtime::External(runtime) => builder.runtime(runtime.clone()),
        }
    }

    /// Waits for `path` to exist, which a helper process is to bring about.
    #[cfg(feature = "unixexec")]
    fn wait_for(path: &Path) {
        while !path.exists() {
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// The read end of a pipe of its own.
    fn pipe() -> OwnedFd {
        io::pipe().unwrap().0.into()
    }

    /// A signal that carries `n`.
    fn signal(n: u32) -> Message {
        Message::signal(PATH, INTERFACE, "Test").build(&n).unwrap()
    }

    /// A signal longer than the buffer of a socket holds.
    fn big_signal() -> Message {
        Message::signal(PATH, INTERFACE, "Big")
            .build(&vec![0u8; 1024 * 1024])
            .unwrap()
    }

    /// How many bytes the first send of a `FailingMidway` takes.
    const CUT: usize = 16;
    /// The most that a write half of your own that can resume writes takes in a call to `sendmsg`.
    const CAP: usize = 16;
    const PATH: &str = "/org/zbus/Test";
    const INTERFACE: &str = "org.zbus.Test";
}
