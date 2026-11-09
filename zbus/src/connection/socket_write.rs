//! What a connection writes its socket through.

#[cfg(all(target_os = "linux", feature = "handover"))]
use std::collections::VecDeque;
#[cfg(all(target_os = "linux", feature = "handover"))]
use std::mem;
#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};
use std::{
    fmt,
    future::poll_fn,
    io::{self, IoSlice},
    slice,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
};
#[cfg(all(target_os = "linux", feature = "handover"))]
use std::{future::Future, pin::Pin, sync::atomic::AtomicBool};

#[cfg(all(target_os = "linux", feature = "handover"))]
use zruntime::{Event, EventListener};

#[cfg(all(target_os = "linux", feature = "handover"))]
use super::handover::SocketKind;
use super::socket_reader::SocketStatus;
#[cfg(all(target_os = "linux", feature = "handover"))]
use crate::Error;
#[cfg(all(target_os = "linux", feature = "handover"))]
use crate::runtime::IoSource;
#[cfg(all(target_os = "linux", feature = "handover"))]
use crate::runtime::io::MESSAGES_PER_WRITE;
use crate::{
    Message, Result,
    connection::socket::{WriteHalf, Writes},
    fdo::ConnectionCredentials,
    log::trace,
    runtime::io::{PollSend, Progress, poll_write, write_message, write_messages},
    wire::serialized::Data,
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
    /// What is left to write: the rest of the message that a write ended in the middle of, if any,
    /// and the messages that sends kept after it.
    rest: Rest,
    /// The socket itself, if zbus drives it and so can give a descriptor of it for a hand-over.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    source: Option<(IoSource, SocketKind)>,
    /// Whether a send of whole messages through `half` failed or was given up on midway, which can
    /// leave part of a message in the socket. The peer can then make no sense of anything sent
    /// after it, so the connection can't be handed over any more. A send that failed as the peer
    /// receives nothing more leaves this as it was, as nothing sent after it reaches the peer, and
    /// so does one through the write half of a channel, which queues each message whole or not at
    /// all.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    torn: bool,
    /// Whether the connection was closed, which shuts the socket down and lets go of what is left
    /// to write, so that a flush fails from then on. It is set before the socket is shut down, so
    /// it also tells a reader that came to the end of the input as the connection was closed apart
    /// from one whose peer closed its end.
    shut_down: bool,
}

impl SocketWrite {
    /// What a connection writes through `half`, or as `writes` tells, if zbus made the socket.
    /// `status` is where the connection keeps how many bytes it has left to write. `unwritten` is
    /// what a connection that this one took over left unwritten, which it writes ahead of all else.
    pub(super) fn new(
        half: Box<dyn WriteHalf>,
        writes: Option<Writes>,
        status: Arc<SocketStatus>,
        #[cfg(all(target_os = "linux", feature = "handover"))] source: Option<(
            IoSource,
            SocketKind,
        )>,
        #[cfg(all(target_os = "linux", feature = "handover"))] unwritten: Unwritten,
    ) -> Self {
        Self {
            half_resumes: half.can_resume_writes(),
            half: Some(half),
            writes,
            rest: Rest::new(
                status,
                #[cfg(all(target_os = "linux", feature = "handover"))]
                unwritten,
            ),
            #[cfg(all(target_os = "linux", feature = "handover"))]
            source,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            torn: false,
            shut_down: false,
        }
    }

    /// Whether the connection can be prepared for a hand-over, as
    /// `Connection::prepare_hand_over` does: whether its sends can keep what the socket doesn't
    /// take at once, as they keep track of how far each write takes the messages, over a socket
    /// that zbus drives or through a write half that can resume writes, or as the socket queues
    /// each message whole or not at all, as a channel does.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub(super) fn can_prepare_hand_over(&self) -> bool {
        self.writes.is_some() || self.half_resumes
    }

    /// Send `msg`, unless the connection was handed over, as [`SocketWrite::send_messages`] does,
    /// but through [`WriteHalf::send_message`] where it hands the message whole to the half.
    // Written out apart from `send_messages`, rather than as a call to it with the one message,
    // so that the compiler knows that there is only the one: every `Connection::send` runs this.
    pub(super) async fn send_message(&mut self, msg: &Message) -> Result<()> {
        let msgs = slice::from_ref(msg);
        match &self.writes {
            Some(Writes::Driven(socket)) => {
                let mut writing = Writing::sending(&mut self.rest);
                if !writing.rest.is_empty() && !write_unwritten_to(&**socket, &mut writing).await? {
                    writing.rest.keep(msgs);

                    return Ok(());
                }
                let left = write_message(&**socket, msg, &mut writing).await?;
                writing.rest.keep(left);

                return Ok(());
            }
            None if self.half_resumes => {
                let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
                send_through(half, msgs, &mut self.rest).await?;

                return Ok(());
            }
            #[cfg(all(target_os = "linux", feature = "handover", feature = "p2p"))]
            Some(Writes::Whole) => {
                let half = self.half.as_deref_mut().ok_or_else(handed_over)?;

                return queue(half, msgs, &mut self.rest).await;
            }
            _ => (),
        }
        let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
        #[cfg(all(target_os = "linux", feature = "handover"))]
        let torn = match self.writes {
            // The channel queues the message whole or not at all.
            #[cfg(feature = "p2p")]
            Some(Writes::Whole) => self.torn,
            // Until the message is through, the socket may hold a part of it.
            _ => mem::replace(&mut self.torn, true),
        };
        let sent = half.send_message(msg).await;
        #[cfg(all(target_os = "linux", feature = "handover"))]
        self.through(torn, &sent);

        sent
    }

    /// Send `msgs`, unless the connection was handed over.
    ///
    /// The connection writes them itself, after what it has left to write, if anything, keeping
    /// track in `rest` of how far each write takes them: to a socket that zbus drives, as
    /// [`write_messages`] does, or through the `sendmsg` of a half that can resume writes, a
    /// message at a time, flushing the half once they are all written. Once a hand-over of the
    /// connection is prepared, it writes what the socket takes at once, and keeps the rest in
    /// `rest`, as it then does for a channel, which takes each message whole or not at all.
    /// Otherwise it hands them whole to [`WriteHalf::send_messages`].
    pub(super) async fn send_messages(&mut self, msgs: &[Message]) -> Result<()> {
        match &self.writes {
            Some(Writes::Driven(socket)) => {
                let mut writing = Writing::sending(&mut self.rest);
                if !writing.rest.is_empty() && !write_unwritten_to(&**socket, &mut writing).await? {
                    writing.rest.keep(msgs);

                    return Ok(());
                }
                let left = write_messages(&**socket, msgs, &mut writing).await?;
                writing.rest.keep(left);

                return Ok(());
            }
            None if self.half_resumes => {
                let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
                send_through(half, msgs, &mut self.rest).await?;

                return Ok(());
            }
            #[cfg(all(target_os = "linux", feature = "handover", feature = "p2p"))]
            Some(Writes::Whole) => {
                let half = self.half.as_deref_mut().ok_or_else(handed_over)?;

                return queue(half, msgs, &mut self.rest).await;
            }
            _ => (),
        }
        let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
        #[cfg(all(target_os = "linux", feature = "handover"))]
        let torn = match self.writes {
            // The channel queues each message whole or not at all.
            #[cfg(feature = "p2p")]
            Some(Writes::Whole) => self.torn,
            // Until the messages are through, the socket may hold a part of one.
            _ => mem::replace(&mut self.torn, true),
        };
        let sent = half.send_messages(msgs).await;
        #[cfg(all(target_os = "linux", feature = "handover"))]
        self.through(torn, &sent);

        sent
    }

    /// Write what the connection has left to write, if anything, waiting on the socket for as
    /// long as it takes, then flush the write half, as [`WriteHalf::flush`] does, unless the
    /// connection was closed or handed over.
    pub(super) async fn flush(&mut self) -> Result<()> {
        if self.shut_down {
            return Err(closed().into());
        }
        self.write_unwritten().await?;
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
    /// Nothing more is written then: what is left to write is let go of, and a flush fails from
    /// then on. `writes` is let go of too, as a pipe to a helper process, which the connection
    /// writes directly, closes as the last of its descriptors goes, and sends go through `half`
    /// from then on, which fails them.
    pub(super) async fn close(&mut self) -> io::Result<()> {
        let Some(half) = self.half.as_deref_mut() else {
            return Ok(());
        };
        // Set before the socket is shut down, as the shutdown can be given up on midway, and as
        // the reader can come to the end of the input as soon as it is.
        self.shut_down = true;
        self.rest.clear();
        self.writes = None;

        half.close().await
    }

    /// Whether a send of whole messages through `half` failed or was given up on midway, which can
    /// leave part of a message in the socket.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub(super) fn is_torn(&self) -> bool {
        self.torn
    }

    /// Whether the connection was closed.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub(super) fn is_shut_down(&self) -> bool {
        self.shut_down
    }

    /// The socket itself, if zbus drives it and so can give a descriptor of it for a hand-over.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub(super) fn source(&self) -> Option<&(IoSource, SocketKind)> {
        self.source.as_ref()
    }

    /// Take the write half out, for the connection to be handed over, with how the connection
    /// writes the socket, if zbus made it, for a connection that goes on over the half to write it
    /// the same way, and with what is left to write, for that connection to write first. Let go of
    /// all else that the connection holds of the socket. Nothing is written through the connection
    /// from then on.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub(super) fn take_half(&mut self) -> Option<(Box<dyn WriteHalf>, Option<Writes>, Unwritten)> {
        self.source = None;
        let half = self.half.take()?;

        Some((half, self.writes.take(), self.rest.take()))
    }

    /// Write what the connection has left to write, if anything, waiting on the socket for as
    /// long as it takes.
    async fn write_unwritten(&mut self) -> Result<()> {
        let mut writing = Writing::waiting(&mut self.rest);
        match &self.writes {
            Some(Writes::Driven(socket)) => {
                write_unwritten_to(&**socket, &mut writing).await?;
            }
            None if self.half_resumes => {
                let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
                write_unwritten_through(half, &mut writing).await?;
            }
            #[cfg(all(target_os = "linux", feature = "handover", feature = "p2p"))]
            Some(Writes::Whole) => {
                let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
                queue_kept(half, &mut writing).await?;
            }
            // Only a send that keeps track of how far each write takes it leaves anything.
            _ => (),
        }

        Ok(())
    }

    /// Put back `torn`, whether the socket was torn before a send, now that the send is through
    /// with `sent`, unless the send failed while the peer may still receive what follows it.
    #[cfg(all(target_os = "linux", feature = "handover"))]
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

/// Whether a hand-over of a connection is prepared ([`Connection::prepare_hand_over`]), which has
/// its sends stop, rather than wait for the socket, from then on for good.
///
/// [`Connection::prepare_hand_over`]: super::Connection::prepare_hand_over
#[cfg(all(target_os = "linux", feature = "handover"))]
#[derive(Debug, Default)]
pub(super) struct Prepared {
    is_set: AtomicBool,
    /// Notified as the hand-over is prepared, for a send that waits for the socket to stop.
    event: Event,
}

#[cfg(all(target_os = "linux", feature = "handover"))]
impl Prepared {
    /// Note that the hand-over is prepared, for good, and wake the send that waits for the socket,
    /// if any.
    pub(super) fn set(&self) {
        if !self.is_set.swap(true, Ordering::AcqRel) {
            self.event.notify(usize::MAX);
        }
    }

    /// Whether the hand-over is prepared.
    fn is_set(&self) -> bool {
        self.is_set.load(Ordering::Acquire)
    }

    /// A listener notified once the hand-over is prepared, unless it is already.
    fn listen(&self) -> EventListener {
        self.event.listen()
    }
}

/// What a connection took to send and didn't write, as a connection that is handed over leaves it,
/// for the one that takes over to write ahead of anything else.
#[cfg(all(target_os = "linux", feature = "handover"))]
#[derive(Default)]
pub(crate) struct Unwritten {
    /// The bytes of a message that a write ended in the middle of, or of what is left of one, and
    /// how many of them are written. The file descriptors of the message went with its first byte.
    pub(crate) cut: Option<(Data<'static, 'static>, usize)>,
    /// The messages after it, none of whose bytes are written, oldest first.
    pub(crate) kept: VecDeque<Message>,
}

#[cfg(all(target_os = "linux", feature = "handover"))]
impl Unwritten {
    /// What is left of the message that a write ended in the middle of, if anything.
    pub(crate) fn rest(&self) -> &[u8] {
        self.cut
            .as_ref()
            .map_or(&[], |(bytes, written)| &bytes[*written..])
    }

    /// How many bytes are left to write.
    pub(crate) fn len(&self) -> usize {
        let kept = self.kept.iter().map(|msg| msg.data().len()).sum::<usize>();

        self.rest().len() + kept
    }
}

/// Write what the connection has left to write, if anything, to `socket`: the rest of a message
/// that a write ended in the middle of, then the messages that sends kept, as far as `writing` has
/// the writes go. Whether it wrote it all.
async fn write_unwritten_to(socket: &dyn PollSend, writing: &mut Writing<'_>) -> io::Result<bool> {
    if !write_rest_to(socket, writing).await? {
        return Ok(false);
    }
    #[cfg(all(target_os = "linux", feature = "handover"))]
    while !writing.rest.kept.is_empty() {
        let copies = writing.rest.kept_copies();
        let mut taking = TakingKept::new(writing);
        let left = write_messages(socket, &copies, &mut taking).await?;
        // The writes stopped before a message, or in the middle of one, the last one included,
        // which leaves no message without anything written, but the rest of that one.
        if !left.is_empty() || writing.rest.cut.is_some() {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Write what is left of a message that a write ended in the middle of, if anything, to `socket`,
/// as far as `writing` has the writes go. Its file descriptors went with its first byte. Whether it
/// wrote it all.
async fn write_rest_to(socket: &dyn PollSend, writing: &mut Writing<'_>) -> io::Result<bool> {
    // A copy of the bytes, so that `writing` can be asked whether to stop while they are written.
    while let Some((bytes, written)) = writing.rest.cut.clone() {
        let left = [IoSlice::new(&bytes[written..])];
        let polled = poll_fn(|cx| {
            poll_write(
                socket,
                cx,
                &left,
                #[cfg(unix)]
                &[],
                writing,
            )
        });
        let sent = match polled.await {
            Ok(sent) => sent,
            // The socket takes nothing at once, and `writing` has the writes stop.
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(e) => return Err(e),
        };
        writing.rest.advance(sent);
    }

    Ok(true)
}

/// Send `msgs` through the `sendmsg` of `half`, after what the connection has left to write, if
/// anything, keeping track in `rest` of how far each call takes them, then flush `half`.
///
/// Once a hand-over of the connection is prepared, a call that waits is given up on, and so writes
/// nothing, and what is left is kept in `rest`. A flush that waits is given up on too, which leaves
/// what the half holds on to for the next flush.
async fn send_through(
    half: &mut dyn WriteHalf,
    msgs: &[Message],
    rest: &mut Rest,
) -> io::Result<()> {
    let mut writing = Writing::sending(rest);
    if !writing.rest.is_empty() && !write_unwritten_through(half, &mut writing).await? {
        writing.rest.keep(msgs);
    } else {
        let left = write_all_through(half, msgs, &mut writing).await?;
        writing.rest.keep(left);
    }

    flush_through(half, &mut writing).await
}

/// Write what the connection has left to write, if anything, through the `sendmsg` of `half`, as
/// [`write_unwritten_to`] does to a socket that zbus drives. Whether it wrote it all.
async fn write_unwritten_through(
    half: &mut dyn WriteHalf,
    writing: &mut Writing<'_>,
) -> io::Result<bool> {
    if !write_rest_through(half, writing).await? {
        return Ok(false);
    }
    #[cfg(all(target_os = "linux", feature = "handover"))]
    while !writing.rest.kept.is_empty() {
        let copies = writing.rest.kept_copies();
        let mut taking = TakingKept::new(writing);
        let left = write_all_through(half, &copies, &mut taking).await?;
        // As in `write_unwritten_to`.
        if !left.is_empty() || writing.rest.cut.is_some() {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Write what is left of a message that a write ended in the middle of, if anything, through the
/// `sendmsg` of `half`, as far as `writing` has the writes go. Its file descriptors went with its
/// first byte. Whether it wrote it all.
async fn write_rest_through(
    half: &mut dyn WriteHalf,
    writing: &mut Writing<'_>,
) -> io::Result<bool> {
    // A copy of the bytes, so that `writing` can be asked whether to stop while they are written.
    while let Some((bytes, written)) = writing.rest.cut.clone() {
        let sent = sendmsg(
            half,
            &bytes[written..],
            #[cfg(unix)]
            &[],
            writing,
        )
        .await?;
        let Some(sent) = sent else {
            return Ok(false);
        };
        writing.rest.advance(sent);
    }

    Ok(true)
}

/// Write `msgs` through the `sendmsg` of `half`, a message at a time, the file descriptors of each
/// along with its first byte, telling `progress` how far each call takes them.
///
/// Where `progress` says to stop rather than wait for a call ([`Progress::stops`]), the call is
/// given up on, which writes nothing, and this gives back the messages of which nothing is written,
/// for the caller to send later. Otherwise it gives back none.
async fn write_all_through<'m, P>(
    half: &mut dyn WriteHalf,
    msgs: &'m [Message],
    progress: &mut P,
) -> io::Result<&'m [Message]>
where
    P: Progress,
{
    for (done, msg) in msgs.iter().enumerate() {
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
            let sent = sendmsg(
                half,
                &data[written..],
                #[cfg(unix)]
                &fds,
                progress,
            )
            .await?;
            let Some(sent) = sent else {
                return Ok(&msgs[done + usize::from(written > 0)..]);
            };
            written += sent;
            let cut = (written < data.len()).then_some((msg, written));
            progress.wrote(done + usize::from(cut.is_none()), cut);
        }
        trace!(
            "Sent message with serial: {}",
            msg.primary_header().serial_num()
        );
    }

    Ok(&[])
}

/// One call to the `sendmsg` of `half`: how much it wrote, or `None` where `progress` says to stop
/// rather than wait for it ([`Progress::stops`]), which gives the call up, so that it writes
/// nothing. A call that takes none of `buffer` fails, rather than leave the connection to call
/// again for ever.
async fn sendmsg<P>(
    half: &mut dyn WriteHalf,
    buffer: &[u8],
    #[cfg(unix)] fds: &[BorrowedFd<'_>],
    progress: &mut P,
) -> io::Result<Option<usize>>
where
    P: Progress,
{
    let mut sending = half.sendmsg(
        buffer,
        #[cfg(unix)]
        fds,
    );
    let sent = poll_fn(|cx| match sending.as_mut().poll(cx) {
        Poll::Pending if progress.stops(cx) => Poll::Ready(Ok(None)),
        polled => polled.map_ok(Some),
    })
    .await?;
    match sent {
        Some(0) => Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "the write half took none of what it was given",
        )),
        sent => Ok(sent),
    }
}

/// Flush `half`, as a send does once it wrote its messages, unless `progress` says to stop rather
/// than wait for the flush ([`Progress::stops`]), which gives it up: what the half holds on to is
/// then left for the next flush.
async fn flush_through<P>(half: &mut dyn WriteHalf, progress: &mut P) -> io::Result<()>
where
    P: Progress,
{
    let mut flushing = half.flush();

    poll_fn(|cx| match flushing.as_mut().poll(cx) {
        Poll::Pending if progress.stops(cx) => Poll::Ready(Ok(())),
        polled => polled,
    })
    .await
}

/// Queue `msgs` on the channel that `half` writes, after the messages that sends kept, if any,
/// waiting for room until a hand-over of the connection is prepared, and from then on as far as the
/// channel has room for them at once, keeping the rest in `rest`.
#[cfg(all(target_os = "linux", feature = "handover", feature = "p2p"))]
async fn queue(half: &mut dyn WriteHalf, msgs: &[Message], rest: &mut Rest) -> Result<()> {
    let mut writing = Writing::sending(rest);
    if !writing.rest.is_empty() && !queue_kept(half, &mut writing).await? {
        writing.rest.keep(msgs);

        return Ok(());
    }
    let left = queue_all(half, msgs, &mut writing).await?;
    writing.rest.keep(left);

    Ok(())
}

/// Queue the messages that sends kept, if any, on the channel that `half` writes, as far as
/// `writing` has the sends go. Whether it queued them all.
#[cfg(all(target_os = "linux", feature = "handover", feature = "p2p"))]
async fn queue_kept(half: &mut dyn WriteHalf, writing: &mut Writing<'_>) -> Result<bool> {
    while !writing.rest.kept.is_empty() {
        let copies = writing.rest.kept_copies();
        let mut taking = TakingKept::new(writing);
        if !queue_all(half, &copies, &mut taking).await?.is_empty() {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Queue `msgs` on the channel that `half` writes, in order, telling `progress` as each is queued.
///
/// The channel takes each message whole or not at all, as [`WriteHalf::send_message`] of its write
/// half sends it, and a send whose future is dropped before it resolves queues nothing. So where
/// `progress` says to stop rather than wait for room ([`Progress::stops`]), the send of the next
/// message is given up on, and this gives back the messages that it didn't queue, for the caller to
/// send later. Otherwise it gives back none.
#[cfg(all(target_os = "linux", feature = "handover", feature = "p2p"))]
async fn queue_all<'m, P>(
    half: &mut dyn WriteHalf,
    msgs: &'m [Message],
    progress: &mut P,
) -> Result<&'m [Message]>
where
    P: Progress,
{
    for (done, msg) in msgs.iter().enumerate() {
        let mut queueing = half.send_message(msg);
        let queued = poll_fn(|cx| match queueing.as_mut().poll(cx) {
            Poll::Pending if progress.stops(cx) => Poll::Ready(None),
            polled => polled.map(Some),
        })
        .await;
        let Some(queued) = queued else {
            return Ok(&msgs[done..]);
        };
        queued?;
        progress.wrote(done + 1, None);
    }

    Ok(&[])
}

/// The writes of a send, or of a flush, which keep track in `rest` of how far each takes the
/// messages.
struct Writing<'r> {
    rest: &'r mut Rest,
    /// Whether the writes wait for the socket for as long as it takes, as those of a flush do,
    /// rather than stop once a hand-over of the connection is prepared, as those of a send do.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    waits: bool,
}

impl<'r> Writing<'r> {
    /// The writes of a send, which stop, rather than wait for the socket, once a hand-over of the
    /// connection is prepared.
    fn sending(rest: &'r mut Rest) -> Self {
        Self {
            rest,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            waits: false,
        }
    }

    /// The writes of a flush, or of a hand-over, which wait for the socket for as long as it takes.
    fn waiting(rest: &'r mut Rest) -> Self {
        Self {
            rest,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            waits: true,
        }
    }
}

impl Progress for Writing<'_> {
    // Inlined, as every write calls it.
    #[inline]
    fn wrote(&mut self, _done: usize, cut: Option<(&Message, usize)>) {
        self.rest.set(cut);
    }

    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn stops(&mut self, cx: &mut Context<'_>) -> bool {
        !self.waits && self.rest.prepared(cx)
    }

    // Without a hand-over, nothing has a send stop.
    #[cfg(not(all(target_os = "linux", feature = "handover")))]
    fn stops(&mut self, _cx: &mut Context<'_>) -> bool {
        false
    }
}

/// The writes of the messages that sends kept, out of copies of the first few of them, as
/// [`Rest::kept_copies`] gives: each is taken out of `kept` as it is started on, as the writes
/// can't hold on to `kept` while they do that.
#[cfg(all(target_os = "linux", feature = "handover"))]
struct TakingKept<'w, 'r> {
    writing: &'w mut Writing<'r>,
    /// How many of the copies the writes started on.
    taken: usize,
}

#[cfg(all(target_os = "linux", feature = "handover"))]
impl<'w, 'r> TakingKept<'w, 'r> {
    /// The writes of copies of the first messages that sends kept, as `writing` has them go.
    fn new(writing: &'w mut Writing<'r>) -> Self {
        Self { writing, taken: 0 }
    }
}

#[cfg(all(target_os = "linux", feature = "handover"))]
impl Progress for TakingKept<'_, '_> {
    fn wrote(&mut self, done: usize, cut: Option<(&Message, usize)>) {
        let started = done + usize::from(cut.is_some());
        self.writing.rest.took(started - self.taken, cut);
        self.taken = started;
    }

    fn stops(&mut self, cx: &mut Context<'_>) -> bool {
        self.writing.stops(cx)
    }
}

/// What a connection has left to write: the rest of the message that a write ended in the middle
/// of, if any, and the messages after it that sends kept, once a hand-over of the connection is
/// prepared, or that a connection that this one took over left unwritten.
struct Rest {
    /// The bytes of the message that a write ended in the middle of, and how many of them are
    /// written. Those of what a connection that this one took over left of a message are only
    /// what is left of it, none of which is written yet.
    cut: Option<(Data<'static, 'static>, usize)>,
    /// The messages that sends kept, none of whose bytes are written, oldest first.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    kept: VecDeque<Message>,
    /// How many bytes the messages of `kept` come to.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    kept_len: usize,
    /// Notified once a hand-over of the connection is prepared, from the first time that a send
    /// waits for the socket on.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    listener: Option<EventListener>,
    /// Where how many bytes are left is kept, for `Connection::unwritten_len` to read, and whether
    /// a hand-over of the connection is prepared.
    status: Arc<SocketStatus>,
}

impl Rest {
    /// What is left to write of a connection whose status is `status`: what a connection that it
    /// took over left unwritten, `unwritten`, which is counted from the start, if any, and nothing
    /// otherwise.
    fn new(
        status: Arc<SocketStatus>,
        #[cfg(all(target_os = "linux", feature = "handover"))] unwritten: Unwritten,
    ) -> Self {
        #[cfg(all(target_os = "linux", feature = "handover"))]
        let Unwritten { cut, kept } = unwritten;
        #[cfg(not(all(target_os = "linux", feature = "handover")))]
        let cut = None;
        let rest = Self {
            cut,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            kept_len: kept.iter().map(|msg| msg.data().len()).sum(),
            #[cfg(all(target_os = "linux", feature = "handover"))]
            kept,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            listener: None,
            status,
        };
        rest.store();

        rest
    }

    /// Whether nothing is left.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn is_empty(&self) -> bool {
        self.cut.is_none() && self.kept.is_empty()
    }

    /// Whether nothing is left.
    #[cfg(not(all(target_os = "linux", feature = "handover")))]
    fn is_empty(&self) -> bool {
        self.cut.is_none()
    }

    /// Note how far a write took the messages that it wrote: `written` bytes into `msg`, or to the
    /// end of a message, if `cut` is `None`.
    ///
    /// Inlined, as every write calls it, most of the time to note what the write before it did
    /// too: that it ended at the end of a message.
    #[inline]
    fn set(&mut self, cut: Option<(&Message, usize)>) {
        let Some((msg, written)) = cut else {
            if self.cut.is_some() {
                self.cut = None;
                self.store();
            }

            return;
        };
        self.cut = Some((msg.data().clone(), written));
        self.store();
    }

    /// Note that a write took `len` more bytes of what is left of the message.
    fn advance(&mut self, len: usize) {
        let Some((bytes, written)) = &mut self.cut else {
            return;
        };
        *written += len;
        if *written == bytes.len() {
            trace!("Sent the rest of a message");
            self.cut = None;
        }
        self.store();
    }

    /// Keep `msgs`, of which nothing is written, after what is left, for the next send or flush
    /// to write, as a send does once a hand-over of the connection is prepared.
    ///
    /// Inlined, as every send calls it, most of the time with nothing to keep.
    #[inline]
    fn keep(&mut self, msgs: &[Message]) {
        #[cfg(all(target_os = "linux", feature = "handover"))]
        if !msgs.is_empty() {
            self.keep_all(msgs);
        }
        // The writes of a send only stop short once a hand-over is prepared.
        #[cfg(not(all(target_os = "linux", feature = "handover")))]
        debug_assert!(msgs.is_empty(), "a send left messages without a hand-over");
    }

    /// Keep `msgs`, which are some, as [`Rest::keep`] does.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[cold]
    fn keep_all(&mut self, msgs: &[Message]) {
        self.kept_len += msgs.iter().map(|msg| msg.data().len()).sum::<usize>();
        self.kept.extend(msgs.iter().cloned());
        self.store();
    }

    /// Copies of the first messages that sends kept, as many as a write takes at most, for writes
    /// that take them out of `kept` as they go ([`TakingKept`]).
    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn kept_copies(&self) -> Vec<Message> {
        self.kept.iter().take(MESSAGES_PER_WRITE).cloned().collect()
    }

    /// Note that writes started on the first `taken` messages of `kept` since they were last told
    /// of: those before the last are written whole, and so is the last, unless the write ended in
    /// the middle of it, as `cut` then tells, with how many of its bytes are written.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn took(&mut self, taken: usize, cut: Option<(&Message, usize)>) {
        for msg in self.kept.drain(..taken) {
            self.kept_len -= msg.data().len();
        }
        self.cut = cut.map(|(msg, written)| (msg.data().clone(), written));
        self.store();
    }

    /// Take all that is left out, for the connection that takes over to write it.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn take(&mut self) -> Unwritten {
        let unwritten = Unwritten {
            cut: self.cut.take(),
            kept: mem::take(&mut self.kept),
        };
        self.kept_len = 0;
        self.store();

        unwritten
    }

    /// Let go of all that is left.
    fn clear(&mut self) {
        self.cut = None;
        #[cfg(all(target_os = "linux", feature = "handover"))]
        {
            self.kept.clear();
            self.kept_len = 0;
        }
        self.store();
    }

    /// Whether a hand-over of the connection is prepared, which has a send stop rather than wait
    /// for the socket. Until it is, the task of `cx` is woken once it is.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn prepared(&mut self, cx: &mut Context<'_>) -> bool {
        // Armed before the flag is first looked at, so that a hand-over prepared from then on is
        // seen, by the flag or through the listener.
        let prepared = &self.status.prepared;
        let listener = self.listener.get_or_insert_with(|| prepared.listen());

        prepared.is_set() || Pin::new(listener).poll(cx).is_ready()
    }

    /// Note how many bytes are left, for `Connection::unwritten_len` to read.
    #[inline]
    fn store(&self) {
        let left = self
            .cut
            .as_ref()
            .map_or(0, |(bytes, written)| bytes.len() - written);
        #[cfg(all(target_os = "linux", feature = "handover"))]
        let left = left + self.kept_len;
        self.status.unwritten.store(left, Ordering::Relaxed);
    }
}

/// Shows how much is left rather than what, which can be many messages, as this is part of what a
/// connection shows of itself.
impl fmt::Debug for Rest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut rest = f.debug_struct("Rest");
        let cut = self
            .cut
            .as_ref()
            .map(|(bytes, written)| bytes.len() - written);
        rest.field("cut", &cut);
        #[cfg(all(target_os = "linux", feature = "handover"))]
        rest.field("kept", &self.kept.len())
            .field("kept_len", &self.kept_len);

        rest.finish_non_exhaustive()
    }
}

/// Whether `error`, which a write failed with, says that the peer receives nothing more, as one
/// that closed its end of the socket does: an I/O error of kind `BrokenPipe`, `ConnectionReset`
/// or `ConnectionAborted`. Nothing sent from then on reaches the peer, so part of a message that
/// the write left in the socket is followed by nothing that the peer could misread.
///
/// A connection that was closed fails its sends with `BrokenPipe` too, as the close shuts the
/// socket down, but `SocketWrite::shut_down` keeps it from being handed over anyway.
#[cfg(all(target_os = "linux", feature = "handover"))]
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

    #[cfg(all(target_os = "linux", feature = "handover"))]
    use std::{
        collections::VecDeque,
        sync::{Mutex, atomic::AtomicBool},
        task::{Context, Poll, Wake, Waker},
    };

    use futures_lite::future::poll_once;
    use ntest::timeout;
    use test_log::test;

    #[cfg(all(target_os = "linux", feature = "handover"))]
    use zoter::Save;
    #[cfg(all(target_os = "linux", feature = "handover"))]
    use zruntime::Event;

    #[cfg(all(target_os = "linux", feature = "handover"))]
    use super::SocketWrite;
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
    #[cfg(all(target_os = "linux", feature = "handover"))]
    use crate::{
        MessageStream,
        connection::{handover::StopStatus, socket::Writes, socket_reader::SocketStatus},
        runtime::io::PollSend,
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
            #[cfg(all(target_os = "linux", feature = "handover"))]
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
            let (conn, peer, counts) = over_a_half_of_your_own(&runtime, Some(CAP), None).await;
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
            let (conn, peer, _) = over_a_half_of_your_own(&runtime, Some(BIG_CAP), None).await;

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
            let (conn, peer, _) = over_a_half_of_your_own(&runtime, Some(CAP), Some(2)).await;
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
            let (conn, peer, counts) = over_a_half_of_your_own(&runtime, None, None).await;
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

    /// A send through a write half of your own that can resume writes, cut off midway, leaves the
    /// rest of its message for the hand-over, which hands it over with the halves, and the
    /// connection that takes over over them writes it through the half, once the peer reads.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn the_rest_of_a_send_through_a_half_that_can_resume_writes_is_handed_over() {
        under_every_runtime(|runtime| async move {
            let (conn, peer, _) = over_a_half_of_your_own(&runtime, Some(BIG_CAP), None).await;
            let big = big_signal();
            stuck(&runtime, pin!(conn.send(&big)), &peer).await;
            let unwritten = conn.unwritten_len();

            let handover = MessageStream::from(&conn).hand_over().await.unwrap();

            assert_eq!(handover.unwritten_len(), unwritten);
            assert_eq!(handover.saved_len(), None);
            assert_eq!(conn.unwritten_len(), 0);
            let conn = on(Builder::take_over(handover), &runtime)
                .build()
                .await
                .unwrap();
            assert_eq!(conn.unwritten_len(), unwritten);
            let reading = read_in_the_background(&peer, big.data().len());
            conn.flush().await.unwrap();
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

    /// Once a hand-over of the connection is prepared, sends don't wait for a peer that doesn't
    /// read: each writes what the socket takes at once, and the connection keeps the rest, in
    /// order, which `unwritten_len` counts to the byte. Once the peer reads, a flush writes it all.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn sends_dont_wait_once_a_hand_over_is_prepared() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            let (before, big, after, next) = (signal(1), big_signal(), signal(2), signal(3));

            assert!(conn.prepare_hand_over());
            assert!(conn.prepare_hand_over(), "the second call refused");

            let msgs = [before.clone(), big.clone(), after.clone()];
            without_waiting(&runtime, conn.send_all(&msgs)).await;
            let left = conn.unwritten_len();
            let cut = left - after.data().len();
            assert!(
                0 < cut && cut < big.data().len(),
                "{cut} of {} bytes left to write",
                big.data().len(),
            );
            without_waiting(&runtime, conn.send(&next)).await;
            assert_eq!(conn.unwritten_len(), left + next.data().len());
            let sent = [&before, &big, &after, &next]
                .iter()
                .flat_map(|msg| msg.data().to_vec())
                .collect::<Vec<_>>();
            let reading = read_in_the_background(&peer, sent.len());
            conn.flush().await.unwrap();

            assert_eq!(reading.join().unwrap(), sent);
            assert_eq!(conn.unwritten_len(), 0);
            assert_nothing_to_read(&peer);
        });
    }

    /// A send that waits for the socket as a hand-over of the connection is prepared is woken, and
    /// stops waiting: the connection keeps the rest of the message that it was in the middle of,
    /// and the messages after it.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn a_send_waiting_when_a_hand_over_is_prepared_returns() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            let (before, big, after) = (signal(1), big_signal(), signal(2));
            let msgs = [before.clone(), big.clone(), after.clone()];
            let mut sending = pin!(conn.send_all(&msgs));
            stuck(&runtime, sending.as_mut(), &peer).await;
            let woken = Arc::new(Woken::default());
            let waker = Waker::from(woken.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(sending.as_mut().poll(&mut cx).is_pending());
            assert!(!woken.0.load(Ordering::Relaxed));

            assert!(conn.prepare_hand_over());

            assert!(woken.0.load(Ordering::Relaxed), "the send wasn't woken");
            let sent = sending.as_mut().poll(&mut cx);
            assert!(matches!(sent, Poll::Ready(Ok(()))), "{sent:?}");
            let cut = conn.unwritten_len() - after.data().len();
            assert!(
                0 < cut && cut < big.data().len(),
                "{cut} of {} bytes left to write",
                big.data().len(),
            );
            let sent: Vec<u8> = msgs.iter().flat_map(|msg| msg.data().to_vec()).collect();
            let reading = read_in_the_background(&peer, sent.len());
            conn.flush().await.unwrap();

            assert_eq!(reading.join().unwrap(), sent);
            assert_eq!(conn.unwritten_len(), 0);
        });
    }

    /// Once a hand-over of the connection is prepared, a send that the socket takes whole leaves
    /// nothing to write, whatever the runtime knows of the socket.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn a_prepared_send_writes_what_the_socket_takes_at_once() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            let msgs = [signal(1), signal(2), signal(3)];
            assert!(conn.prepare_hand_over());

            without_waiting(&runtime, conn.send(&msgs[0])).await;
            without_waiting(&runtime, conn.send_all(&msgs[1..])).await;

            assert_eq!(conn.unwritten_len(), 0);
            let mut received = vec![0; msgs.iter().map(|msg| msg.data().len()).sum()];
            (&peer).read_exact(&mut received).unwrap();
            let sent: Vec<u8> = msgs.iter().flat_map(|msg| msg.data().to_vec()).collect();
            assert_eq!(received, sent);
        });
    }

    /// A send that fails once a hand-over of the connection is prepared, as the peer went, keeps
    /// none of its messages.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn a_prepared_send_that_fails_keeps_nothing() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            assert!(conn.prepare_hand_over());
            drop(peer);

            let sent = conn.send_all(&[signal(1), signal(2)]).await;

            let Err(Error::InputOutput(e)) = &sent else {
                panic!("the send to a peer that went didn't fail: {sent:?}");
            };
            assert_eq!(e.kind(), ErrorKind::BrokenPipe, "{e}");
            assert_eq!(conn.unwritten_len(), 0);
        });
    }

    /// The file descriptors of a message that a send kept go with its first byte, once a flush
    /// writes it.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn a_kept_message_goes_with_its_descriptors() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            let (read_end, _write_end) = io::pipe().unwrap();
            let big = big_signal();
            let with_fd = Message::signal(PATH, INTERFACE, "WithFd")
                .build(&Fd::from(read_end.as_fd()))
                .unwrap();
            assert!(conn.prepare_hand_over());
            without_waiting(&runtime, conn.send(&big)).await;
            without_waiting(&runtime, conn.send(&with_fd)).await;
            assert!(conn.unwritten_len() > with_fd.data().len());

            let mut peer = Reader::new(Arc::new(registered(&runtime, peer, UnixOps).unwrap()));
            let receiving = async {
                let (mut bytes, mut fds) = (vec![], vec![]);
                let first = peer.receive_message(0, &mut bytes, &mut fds).await.unwrap();
                let second = peer.receive_message(1, &mut bytes, &mut fds).await.unwrap();

                (first, second)
            };
            let (flushed, (first, second)) = futures_util::join!(conn.flush(), receiving);

            flushed.unwrap();
            assert_eq!(&first.data()[..], &big.data()[..]);
            assert_eq!(first.data().fds().len(), 0);
            assert_eq!(&second.data()[..], &with_fd.data()[..]);
            assert_eq!(second.data().fds().len(), 1);
        });
    }

    /// A flush writes the messages that sends kept several at a time, as a send writes those that
    /// it is given.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn kept_messages_go_in_writes_of_several() {
        under_every_runtime(|runtime| async move {
            let ops = Recording::default();
            let writes = ops.writes.clone();
            let (conn, peer) = over(&runtime, ops).await;
            let big = big_signal();
            let msgs: Vec<_> = (0..20).map(signal).collect();
            assert!(conn.prepare_hand_over());
            without_waiting(&runtime, conn.send(&big)).await;
            without_waiting(&runtime, conn.send_all(&msgs)).await;
            writes.lock().unwrap().clear();

            let sent: Vec<u8> = [&big]
                .into_iter()
                .chain(&msgs)
                .flat_map(|msg| msg.data().to_vec())
                .collect();
            let reading = read_in_the_background(&peer, sent.len());
            conn.flush().await.unwrap();

            assert_eq!(reading.join().unwrap(), sent);
            // The rest of the big message goes in writes of its own, and the first write of the
            // kept messages takes as many as a write takes.
            let writes = writes.lock().unwrap();
            assert!(writes.contains(&(16, 0)), "{writes:?}");
        });
    }

    /// The messages of a send go after what sends kept, even where the writes of that stop in the
    /// middle of the last message that they started on, as the socket takes no more: the send
    /// then keeps its messages, whatever the socket takes of them by then, and a flush writes all
    /// of it, in order.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn a_send_keeps_its_messages_until_what_was_kept_is_written() {
        let (kept, msg) = (signal(1), signal(2));
        let half = kept.data().len() / 2;
        let socket = Arc::new(Scripted::new([None, Some(half), None]));
        let status = prepared_status();
        let mut write = SocketWrite::new(
            Box::new(Unused),
            Some(Writes::Driven(socket.clone())),
            status.clone(),
            None,
            Default::default(),
        );

        keep_until_what_was_kept_is_written(&mut write, &socket, &status, &kept, &msg);
    }

    /// The messages of a send through a write half of your own that can resume writes go after
    /// what sends kept, as they do over a socket that zbus drives.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn a_send_through_a_half_keeps_its_messages_until_what_was_kept_is_written() {
        let (kept, msg) = (signal(1), signal(2));
        let half = kept.data().len() / 2;
        let socket = Arc::new(Scripted::new([None, Some(half), None]));
        let status = prepared_status();
        let mut write = SocketWrite::new(
            Box::new(ScriptedHalf(socket.clone())),
            None,
            status.clone(),
            None,
            Default::default(),
        );

        keep_until_what_was_kept_is_written(&mut write, &socket, &status, &kept, &msg);
    }

    /// A connection over a write half of your own that can't resume writes isn't prepared for a
    /// hand-over: a send waits for the half as before.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn a_connection_over_a_half_that_cant_resume_writes_isnt_prepared() {
        under_every_runtime(|runtime| async move {
            let (conn, peer, _) = over_a_half_of_your_own(&runtime, None, None).await;
            let big = big_signal();
            let mut sending = pin!(conn.send(&big));
            stuck(&runtime, sending.as_mut(), &peer).await;

            assert!(!conn.prepare_hand_over());

            let sent = poll_once(sending.as_mut()).await;
            assert!(sent.is_none(), "the send stopped waiting: {sent:?}");
            assert_eq!(conn.unwritten_len(), 0);
        });
    }

    /// Once a hand-over of a connection over a write half of your own that can resume writes is
    /// prepared, sends don't wait for the half: a call to its `sendmsg` that waits is given up on,
    /// and the connection keeps what is left, which a flush writes, before it flushes the half.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn sends_through_a_half_that_can_resume_writes_dont_wait_once_prepared() {
        under_every_runtime(|runtime| async move {
            let (conn, peer, counts) = over_a_half_of_your_own(&runtime, Some(BIG_CAP), None).await;
            let (big, next) = (big_signal(), signal(1));
            assert!(conn.prepare_hand_over());

            without_waiting(&runtime, conn.send(&big)).await;
            without_waiting(&runtime, conn.send(&next)).await;

            // A runtime that waits to hear that the socket is writable before it writes has
            // `sendmsg` wait at its first call, which leaves the whole message to write.
            let cut = conn.unwritten_len() - next.data().len();
            assert!(
                0 < cut && cut <= big.data().len(),
                "{cut} of {} bytes left to write",
                big.data().len(),
            );
            let flushes = counts.flushes.load(Ordering::Relaxed);
            let sent = [&big.data()[..], &next.data()[..]].concat();
            let reading = read_in_the_background(&peer, sent.len());
            conn.flush().await.unwrap();

            assert_eq!(reading.join().unwrap(), sent);
            assert_eq!(conn.unwritten_len(), 0);
            assert_eq!(counts.flushes.load(Ordering::Relaxed), flushes + 1);
        });
    }

    /// Once a hand-over of a connection over a channel is prepared, sends to the full channel
    /// don't wait for room: a send that waits for room as it is prepared is woken, and stops
    /// waiting, and the connection keeps the messages whole, which a flush queues as the peer
    /// takes the others.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn sends_to_a_full_channel_dont_wait_once_prepared() {
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
            let msgs: Vec<_> = (0..64).map(signal).collect();
            // Signals until the channel takes no more, the last of which waits for room.
            let mut queued = 0;
            let mut sending = loop {
                let mut sending = Box::pin(conn.send(&msgs[queued]));
                match poll_once(sending.as_mut()).await {
                    Some(sent) => sent.unwrap(),
                    None => break sending,
                }
                queued += 1;
            };
            let woken = Arc::new(Woken::default());
            let waker = Waker::from(woken.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(sending.as_mut().poll(&mut cx).is_pending());
            assert!(!woken.0.load(Ordering::Relaxed));

            assert!(conn.prepare_hand_over());

            assert!(woken.0.load(Ordering::Relaxed), "the send wasn't woken");
            let sent = sending.as_mut().poll(&mut cx);
            assert!(matches!(sent, Poll::Ready(Ok(()))), "{sent:?}");
            drop(sending);
            assert_eq!(conn.unwritten_len(), msgs[queued].data().len());
            without_waiting(&runtime, conn.send_all(&msgs[queued + 1..queued + 3])).await;
            let kept: usize = msgs[queued..queued + 3]
                .iter()
                .map(|msg| msg.data().len())
                .sum();
            assert_eq!(conn.unwritten_len(), kept);
            let receiving = async {
                let (mut bytes, mut fds) = (vec![], vec![]);
                let mut received = vec![];
                for seq in 0..queued + 3 {
                    let msg = peer.receive_message(seq as u64, &mut bytes, &mut fds).await;
                    received.push(msg.unwrap().data().to_vec());
                }

                received
            };
            let (flushed, received) = futures_util::join!(conn.flush(), receiving);

            flushed.unwrap();
            let sent: Vec<_> = msgs[..queued + 3]
                .iter()
                .map(|msg| msg.data().to_vec())
                .collect();
            assert_eq!(received, sent);
            assert_eq!(conn.unwritten_len(), 0);
        });
    }

    /// A hand-over of a connection whose sends kept messages, once it was prepared, hands them
    /// over, without waiting for the peer to take them, and the connection that takes over writes
    /// them, once the peer reads.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn what_sends_kept_is_handed_over() {
        under_every_runtime(|runtime| async move {
            let (conn, peer) = served(&runtime).await;
            let (big, next) = (big_signal(), signal(1));
            assert!(conn.prepare_hand_over());
            without_waiting(&runtime, conn.send(&big)).await;
            without_waiting(&runtime, conn.send(&next)).await;
            let unwritten = conn.unwritten_len();

            let handover = MessageStream::from(&conn).hand_over().await.unwrap();

            assert_eq!(handover.unwritten_len(), unwritten);
            assert_eq!(conn.unwritten_len(), 0);
            let conn = on(Builder::take_over(handover), &runtime)
                .build()
                .await
                .unwrap();
            let sent = [&big.data()[..], &next.data()[..]].concat();
            let reading = read_in_the_background(&peer, sent.len());
            conn.flush().await.unwrap();
            assert_eq!(reading.join().unwrap(), sent);
            assert_eq!(conn.unwritten_len(), 0);
        });
    }

    /// A hand-over of a connection over a full channel doesn't wait for room either: it hands over
    /// the messages that sends kept, and a send that waited for room as it started, whole, with
    /// the halves of the channel, and the connection that takes over queues them ahead of its own
    /// sends, as the peer takes the others.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn what_sends_to_a_full_channel_kept_is_handed_over() {
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
            let msgs: Vec<_> = (0..64).map(signal).collect();
            // Signals until the channel takes no more, the last of which waits for room.
            let mut queued = 0;
            let sending = loop {
                let mut sending = Box::pin(conn.send(&msgs[queued]));
                match poll_once(sending.as_mut()).await {
                    Some(sent) => sent.unwrap(),
                    None => break sending,
                }
                queued += 1;
            };
            let mut stream = MessageStream::from(&conn);

            let (handed_over, sent) = futures_util::join!(stream.hand_over(), sending);

            sent.unwrap();
            let handover = handed_over.unwrap();
            assert_eq!(handover.unwritten_len(), msgs[queued].data().len());
            assert_eq!(handover.saved_len(), None);
            let conn = on(Builder::take_over(handover), &runtime)
                .build()
                .await
                .unwrap();
            let next = &msgs[queued + 1];
            let receiving = async {
                let (mut bytes, mut fds) = (vec![], vec![]);
                let mut received = vec![];
                for seq in 0..queued + 2 {
                    let msg = peer.receive_message(seq as u64, &mut bytes, &mut fds).await;
                    received.push(msg.unwrap().data().to_vec());
                }

                received
            };
            let (sent, received) = futures_util::join!(conn.send(next), receiving);

            sent.unwrap();
            let sent: Vec<_> = msgs[..queued + 2]
                .iter()
                .map(|msg| msg.data().to_vec())
                .collect();
            assert_eq!(received, sent);
            assert_eq!(conn.unwritten_len(), 0);
        });
    }

    /// Closing a connection whose sends kept messages, once a hand-over of it was prepared, lets
    /// go of them, and a flush fails from then on.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    #[timeout(15000)]
    fn closing_a_prepared_connection_lets_go_of_what_sends_kept() {
        under_every_runtime(|runtime| async move {
            let (conn, _peer) = served(&runtime).await;
            assert!(conn.prepare_hand_over());
            without_waiting(&runtime, conn.send(&big_signal())).await;
            without_waiting(&runtime, conn.send(&signal(1))).await;
            assert!(conn.unwritten_len() > 0);

            conn.clone().close().await.unwrap();

            assert_eq!(conn.unwritten_len(), 0);
            let flushed = conn.flush().await;
            let Err(Error::InputOutput(e)) = &flushed else {
                panic!("the flush of a closed connection didn't fail: {flushed:?}");
            };
            assert_eq!(e.kind(), ErrorKind::NotConnected, "{e}");
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
    /// half of your own, and the other end of the pair. The half can resume writes if `cap` is
    /// some, and then takes at most that many bytes in each call to `sendmsg`. It fails the call
    /// numbered `fail_at`, if any.
    async fn over_a_half_of_your_own(
        runtime: &Runtime,
        cap: Option<usize>,
        fail_at: Option<usize>,
    ) -> (Connection, UnixStream, Arc<Counts>) {
        let (ours, theirs) = UnixStream::pair().unwrap();
        let socket = Arc::new(registered(runtime, ours, UnixOps).unwrap());
        let counts = Arc::new(Counts::default());
        let half = OfYourOwn {
            socket: socket.clone(),
            cap,
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
        /// The most that it takes in each call to `sendmsg`, if it can resume writes, which has it
        /// fail if it is handed a whole message.
        cap: Option<usize>,
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
                self.cap.is_none(),
                "a half that can resume writes was handed {msg:?}"
            );
            self.counts.whole.fetch_add(1, Ordering::Relaxed);

            WriteHalf::send_message(&mut self.socket, msg).await
        }

        async fn send_messages(&mut self, msgs: &[Message]) -> Result<()> {
            assert!(
                self.cap.is_none(),
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
            let len = buffer.len().min(self.cap.unwrap_or(usize::MAX));

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
            self.cap.is_some()
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

    /// Sends `kept` through `write`, of a connection whose hand-over is prepared and whose status
    /// is `status`, which `socket` takes none of, and then `msg`, of which `socket` takes nothing,
    /// as it takes half of `kept`, then no more: `msg` is kept. Then empties `socket`, and flushes.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn keep_until_what_was_kept_is_written(
        write: &mut SocketWrite,
        socket: &Scripted,
        status: &SocketStatus,
        kept: &Message,
        msg: &Message,
    ) {
        let half = kept.data().len() / 2;
        futures_lite::future::block_on(async {
            write.send_message(kept).await.unwrap();
            assert_eq!(status.unwritten.load(Ordering::Relaxed), kept.data().len());

            write.send_message(msg).await.unwrap();

            assert_eq!(socket.written(), &kept.data()[..half]);
            let unwritten = kept.data().len() - half + msg.data().len();
            assert_eq!(status.unwritten.load(Ordering::Relaxed), unwritten);
            socket.empty();
            write.flush().await.unwrap();
        });

        assert_eq!(
            socket.written(),
            [&kept.data()[..], &msg.data()[..]].concat()
        );
        assert_eq!(status.unwritten.load(Ordering::Relaxed), 0);
    }

    /// The status of a connection whose hand-over is prepared.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    fn prepared_status() -> Arc<SocketStatus> {
        let status = Arc::new(SocketStatus {
            activity_event: Event::new(),
            closed: AtomicBool::new(false),
            closed_event: Event::new(),
            unwritten: AtomicUsize::new(0),
            stop: StopStatus::new(),
            prepared: Default::default(),
        });
        status.prepared.set();

        status
    }

    /// A socket of the test's own, as a connection writes a socket that zbus drives. While it is
    /// full, `poll_send` takes nothing, and each `try_send` takes as many bytes at most as the next
    /// of its attempts says, or none, for an attempt of `None`, until it is out of attempts, and
    /// then all. Once it is emptied, `poll_send` takes all too.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[derive(Debug)]
    struct Scripted {
        full: AtomicBool,
        attempts: Mutex<VecDeque<Option<usize>>>,
        /// What it took, in order.
        written: Mutex<Vec<u8>>,
    }

    #[cfg(all(target_os = "linux", feature = "handover"))]
    impl Scripted {
        /// A full socket, which `try_send` makes `attempts` at.
        fn new(attempts: impl IntoIterator<Item = Option<usize>>) -> Self {
            Self {
                full: AtomicBool::new(true),
                attempts: Mutex::new(attempts.into_iter().collect()),
                written: Mutex::default(),
            }
        }

        /// Have it take all that it is given from now on, as a socket whose peer reads at last.
        fn empty(&self) {
            self.full.store(false, Ordering::Relaxed);
        }

        /// What it took, in order.
        fn written(&self) -> Vec<u8> {
            self.written.lock().unwrap().clone()
        }

        /// Takes the first `len` bytes of `buffers`, or all of them if there are fewer.
        fn take(&self, buffers: &[IoSlice<'_>], len: usize) -> usize {
            let mut written = self.written.lock().unwrap();
            let mut taken = 0;
            for buffer in buffers {
                let part = &buffer[..buffer.len().min(len - taken)];
                written.extend_from_slice(part);
                taken += part.len();
            }

            taken
        }
    }

    #[cfg(all(target_os = "linux", feature = "handover"))]
    impl PollSend for Scripted {
        fn poll_send(
            &self,
            _cx: &mut Context<'_>,
            buffers: &[IoSlice<'_>],
            _fds: &[BorrowedFd<'_>],
        ) -> Poll<io::Result<usize>> {
            if self.full.load(Ordering::Relaxed) {
                return Poll::Pending;
            }

            Poll::Ready(Ok(self.take(buffers, usize::MAX)))
        }

        fn try_send(&self, buffers: &[IoSlice<'_>], _fds: &[BorrowedFd<'_>]) -> io::Result<usize> {
            match self.attempts.lock().unwrap().pop_front() {
                Some(None) => Err(ErrorKind::WouldBlock.into()),
                Some(Some(len)) => Ok(self.take(buffers, len)),
                None => Ok(self.take(buffers, usize::MAX)),
            }
        }
    }

    /// A write half of your own that can resume writes, over a [`Scripted`] socket, whose `sendmsg`
    /// waits for as long as the socket takes nothing.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[derive(Debug)]
    struct ScriptedHalf(Arc<Scripted>);

    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[async_trait::async_trait]
    impl WriteHalf for ScriptedHalf {
        async fn sendmsg(&mut self, buffer: &[u8], _fds: &[BorrowedFd<'_>]) -> io::Result<usize> {
            let buffers = [IoSlice::new(buffer)];
            let sent = match self.0.full.load(Ordering::Relaxed) {
                true => self.0.try_send(&buffers, &[]),
                false => Ok(self.0.take(&buffers, usize::MAX)),
            };
            match sent {
                Err(e) if e.kind() == ErrorKind::WouldBlock => std::future::pending().await,
                sent => sent,
            }
        }

        async fn close(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn can_resume_writes(&self) -> bool {
            true
        }
    }

    /// The write half of a socket that zbus drives, which a connection writes directly instead.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[derive(Debug)]
    struct Unused;

    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[async_trait::async_trait]
    impl WriteHalf for Unused {
        async fn close(&mut self) -> io::Result<()> {
            Ok(())
        }
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

    /// Waits for `sending`, on `runtime`, which is not to wait for a peer that doesn't read: a send
    /// that waited for it would time out. It can still give way to the other tasks, as a runtime
    /// that counts what a task does between its waits has it do now and then.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    async fn without_waiting<F>(runtime: &Runtime, sending: F)
    where
        F: Future<Output = Result<()>>,
    {
        let sent = runtime.timeout(sending, Duration::from_secs(5)).await;
        assert!(sent.is_ok(), "the send didn't go through: {sent:?}");
    }

    /// A waker that notes that it was woken.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[derive(Debug, Default)]
    struct Woken(AtomicBool);

    #[cfg(all(target_os = "linux", feature = "handover"))]
    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Relaxed);
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
    /// The most that a write half of your own that can resume writes takes in a call to `sendmsg`
    /// for a send of a message longer than the socket holds, which would take tens of thousands of
    /// calls of `CAP` bytes. It still takes such a message in several calls.
    const BIG_CAP: usize = 64 * 1024;
    const PATH: &str = "/org/zbus/Test";
    const INTERFACE: &str = "org.zbus.Test";
}
