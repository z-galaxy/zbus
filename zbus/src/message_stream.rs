use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::stream::{self, FusedStream};
use ordered_stream::{OrderedStream, PollResult};
use zruntime::{EventListener, broadcast::Receiver as ActiveReceiver};

#[cfg(all(target_os = "linux", feature = "handover"))]
use crate::connection::{Handover, handover::Backlog};
use crate::{
    AsyncDrop, Connection, MatchRule, OwnedMatchRule, Result,
    connection::ConnectionInner,
    log::warn,
    message::{Message, Sequence},
};

/// A [`stream::Stream`] implementation that yields [`Message`] items.
///
/// You can convert a [`Connection`] to this type and back to [`Connection`].
///
/// **NOTE**: You must ensure a `MessageStream` is continuously polled or you will experience hangs.
/// If you don't need to continuously poll the `MessageStream` but need to keep it around for later
/// use, keep the connection around and convert it into a `MessageStream` when needed. The
/// conversion is not an expensive operation so you don't need to worry about performance, unless
/// you do it very frequently. If you need to convert back and forth frequently, you may want to
/// consider keeping both a connection and stream around.
///
/// The stream ends once the connection is closed and everything already queued for it has been
/// yielded. That holds however the connection closed: a [`Connection::close`] of your own, the
/// remote peer going away, an I/O error on the socket, or the runtime taking away the task that
/// reads it.
///
/// With the `handover` feature, on Linux, the stream also ends once a hand-over of its connection
/// that doesn't go through it (`Connection::hand_over`, or `MessageStream::hand_over` on another
/// stream) stops the connection reading, after everything that the connection read before it
/// stopped and queued for the stream. Its `None` then comes with no error before it.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct MessageStream {
    inner: Inner,
    // Armed on a poll that found the queue empty and dropped once it fires, so that the next
    // such poll arms a fresh one.
    closed_listener: Option<EventListener>,
    // Whether this stream has already yielded its `None`. The channel below cannot be asked:
    // a cancelled reader never gets to close it, so it stays open past the stream's end.
    terminated: bool,
    // What a hand-over through this stream took off its queue and the stream is yet to yield,
    // which it yields first.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    backlog: Backlog,
}

impl Clone for MessageStream {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            // A listener and an end belong to the stream that reached them; a clone arms its own
            // listener when it first needs one and runs to its own end.
            closed_listener: None,
            terminated: false,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            backlog: self.backlog.clone(),
        }
    }
}

impl MessageStream {
    /// Create a message stream for the given match rule.
    ///
    /// If `conn` is a bus connection and match rule is for a signal, the match rule will be
    /// registered with the bus and queued for deregistration when the stream is dropped. If you'd
    /// like immediate deregistration, use [`AsyncDrop::async_drop`]. The reason match rules are
    /// only registered with the bus for signals is that the D-Bus specification only allows signals
    /// to be broadcasted and unicast messages are always sent to their destination (regardless
    /// of any match rules registered by the destination) by the bus. Hence there is no need to
    /// register match rules for non-signal messages with the bus.
    ///
    /// Having said that, streams created by this method can still be very useful as it allows you
    /// to avoid needless task wakeups and simplify your stream consuming code.
    ///
    /// You can optionally also request the capacity of the underlying message queue through
    /// `max_queued`. If specified, the capacity is guaranteed to be at least `max_queued`. If not
    /// specified, the default of 64 is assumed. The capacity can also be changed later through
    /// [`MessageStream::set_max_queued`].
    ///
    /// # Example
    ///
    /// ```
    /// # // The example decodes the signal with the `NameOwnerChanged` type of the `DBusProxy`.
    /// # #[cfg(feature = "proxy")]
    /// use zbus::{AsyncDrop, Connection, MatchRule, MessageStream, fdo::NameOwnerChanged};
    /// # #[cfg(feature = "proxy")]
    /// use std::num::NonZeroUsize;
    /// # #[cfg(feature = "proxy")]
    /// use futures_util::{TryStreamExt, future::select, future::Either::{Left, Right}, pin_mut};
    ///
    /// # #[cfg(not(feature = "proxy"))]
    /// # fn main() {}
    /// # #[cfg(feature = "proxy")]
    /// # #[tokio::main]
    /// # async fn main() -> zbus::Result<()> {
    /// let conn = Connection::session().await?;
    /// let rule = MatchRule::builder()
    ///     .msg_type(zbus::message::Type::Signal)
    ///     .sender("org.freedesktop.DBus")
    ///     .interface("org.freedesktop.DBus")
    ///     .member("NameOwnerChanged")
    ///     .add_arg("org.freedesktop.zbus.MatchRuleStreamTest42")
    ///     .build()?;
    /// let mut stream = MessageStream::for_match_rule(
    ///     rule,
    ///     &conn,
    ///     // For such a specific match rule, we don't need a big queue.
    ///     Some(NonZeroUsize::new(1).unwrap()),
    /// ).await?;
    ///
    /// let rule_str = "type='signal',sender='org.freedesktop.DBus',\
    ///                 interface='org.freedesktop.DBus',member='NameOwnerChanged',\
    ///                 arg0='org.freedesktop.zbus.MatchRuleStreamTest42'";
    /// assert_eq!(
    ///     stream.match_rule().map(|r| r.to_string()).as_deref(),
    ///     Some(rule_str),
    /// );
    ///
    /// // We register 2 names, starting with the uninteresting one. If `stream` wasn't filtering
    /// // messages based on the match rule, we'd receive method return calls for each of these 2
    /// // calls first.
    /// //
    /// // Note that the `NameOwnerChanged` signal will not be sent by the bus for the first name
    /// // we register since we setup an arg filter.
    /// conn.request_name("org.freedesktop.zbus.MatchRuleStreamTest44")
    ///     .await?;
    /// conn.request_name("org.freedesktop.zbus.MatchRuleStreamTest42")
    ///     .await?;
    ///
    /// let msg = stream.try_next().await?.unwrap();
    /// let signal = NameOwnerChanged::from_message(msg).unwrap();
    /// assert_eq!(signal.args()?.name(), "org.freedesktop.zbus.MatchRuleStreamTest42");
    /// stream.async_drop().await;
    ///
    /// // Ensure the match rule is deregistered and this connection doesn't receive
    /// // `NameOwnerChanged` signals.
    /// let stream = MessageStream::from(&conn).try_filter_map(|msg| async move {
    ///     Ok(NameOwnerChanged::from_message(msg))
    /// });
    /// conn.release_name("org.freedesktop.zbus.MatchRuleStreamTest42").await?;
    ///
    /// pin_mut!(stream);
    /// let next = stream.try_next();
    /// pin_mut!(next);
    /// let timeout = tokio::time::sleep(std::time::Duration::from_millis(50));
    /// pin_mut!(timeout);
    /// match select(next, timeout).await {
    ///    Left((msg, _)) => unreachable!("unexpected message: {:?}", msg),
    ///    Right((_, _)) => (),
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Caveats
    ///
    /// Since this method relies on [`MatchRule::matches`], it inherits its caveats.
    pub async fn for_match_rule<R>(
        rule: R,
        conn: &Connection,
        max_queued: Option<NonZeroUsize>,
    ) -> Result<Self>
    where
        R: TryInto<OwnedMatchRule>,
        R::Error: Into<crate::Error>,
    {
        let rule = rule.try_into().map_err(Into::into)?;
        let msg_receiver = conn.add_match(rule.clone(), max_queued).await?;

        Ok(Self::for_subscription_channel(
            msg_receiver,
            Some(rule),
            conn,
        ))
    }

    /// The associated match rule, if any.
    pub fn match_rule(&self) -> Option<MatchRule<'_>> {
        self.inner.match_rule.as_deref().cloned()
    }

    /// The maximum number of messages to queue for this stream.
    pub fn max_queued(&self) -> NonZeroUsize {
        self.inner.msg_receiver.capacity()
    }

    /// Set the maximum number of messages to queue for this stream.
    ///
    /// After this call, the capacity is guaranteed to be at least `max_queued`: if it is lower, it
    /// is raised to `max_queued`, and if it is already `max_queued` or more, it is left as it is.
    /// Unlike [`Connection::set_max_queued`], this never lowers the capacity and so never causes
    /// queued messages to be dropped.
    ///
    /// The queue is shared with the other streams that receive the same messages, so the capacity
    /// changes for all of them. For a stream created from a [`Connection`] without a match rule,
    /// that is the main queue of the connection, which [`Connection::max_queued`] reports. For a
    /// stream created by [`MessageStream::for_match_rule`], it is the queue of its match rule,
    /// which the streams created for an equal match rule on the same connection share.
    pub fn set_max_queued(&mut self, max_queued: NonZeroUsize) {
        if max_queued <= self.max_queued() {
            return;
        }
        self.inner.msg_receiver.set_capacity(max_queued);
    }

    /// Hand the connection of this stream over, as [`Connection::hand_over`] does, with the
    /// messages that this stream is yet to yield, for a connection in this process or in another
    /// one to go on from where it stopped, through
    /// [`Builder::take_over`](crate::connection::Builder::take_over).
    ///
    /// [The module of the hand-over](crate::connection::handover) tells how it goes. The stream is
    /// to be one that has had every message of its connection, from the first, as the one that
    /// [`Builder::build_message_stream`] builds along with the connection has. Stop taking
    /// messages off it, and finish what you do with those that you took, before you call this.
    ///
    /// This method is only available on Linux, with the `handover` feature.
    ///
    /// This goes through the steps of [`Connection::hand_over`], whose documentation goes for it
    /// too: what it does to the connection, the calls that its object server has under way, a
    /// peer that closes its end, the cancellation of the hand-over and its errors. While it waits,
    /// for the object server and then for the connection to have stopped, it takes the messages
    /// queued on this stream off it, as a connection that waits for room in the stream to queue a
    /// message there neither reads on nor stops otherwise.
    ///
    /// The [`Handover`] that it gives holds the messages that were queued on this stream and that
    /// it did not yield, as they are, along with the method calls that the object server didn't
    /// take, in the order that the connection read them, each for those that are yet to get it.
    /// The stream then ends: it yields `None`, as it has nothing left to yield.
    ///
    /// # The messages of the stream
    ///
    /// What the hand-over takes off the queue of this stream stays with the stream, not with the
    /// future of the hand-over, until the hand-over commits, so none of what the stream is yet to
    /// yield is lost, whatever comes of the hand-over:
    ///
    /// * Once the hand-over commits, all of it is in the [`Handover`].
    /// * If the hand-over fails, or its future is dropped, the stream yields what the hand-over
    ///   took off its queue, oldest first, then what is still queued. As the connection stopped
    ///   reading, unless the hand-over failed before it stopped it, the stream then ends.
    /// * A later hand-over through this stream goes on from where the one before it stopped, with
    ///   all of that. It hands the connection over if what failed the one before doesn't fail it,
    ///   as with a flush under way that held the hand-over up until it was given up on, or a lack
    ///   of file descriptors that has gone. Otherwise, take the messages off the stream, deal with
    ///   them, and close the connection.
    ///
    /// Once a hand-over through this stream has stopped the connection reading, it may have taken
    /// messages off this stream that another stream doesn't have, as that stream yielded them or
    /// was made after them. A hand-over through any other stream of the connection, a clone of
    /// this one included, or through none ([`Connection::hand_over`]), then fails, rather than
    /// hand the connection over without them.
    ///
    /// While it waits for the calls that the object server has under way, the connection reads on,
    /// and the hand-over takes what it queues on this stream off it: nothing bounds how much that
    /// is but the time that the hand-over waits.
    ///
    /// # Errors
    ///
    /// Those of [`Connection::hand_over`], after which the stream keeps what the hand-over took
    /// off its queue, as told above, and:
    ///
    /// * [`Error::Unsupported`], before anything is done to the connection, which reads on, for a
    ///   stream for a match rule ([`MessageStream::for_match_rule`]), which never had the messages
    ///   that did not match it.
    /// * [`Error::InputOutput`], with nothing taken out of the connection, if a hand-over through
    ///   another stream stopped the connection reading before, as told above.
    ///
    /// [`Builder::build_message_stream`]: crate::connection::Builder::build_message_stream
    /// [`Error::InputOutput`]: crate::Error::InputOutput
    /// [`Error::Unsupported`]: crate::Error::Unsupported
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub async fn hand_over(&mut self) -> Result<Handover> {
        if self.inner.match_rule.is_some() {
            return Err(crate::Error::Unsupported);
        }
        let conn = Connection::from(&*self);
        let handover = conn
            .hand_over_through(Some((&mut self.inner.msg_receiver, &mut self.backlog)))
            .await?;
        // Every message that the stream was yet to yield went into the hand-over.
        self.terminated = true;

        Ok(handover)
    }

    pub(crate) fn for_subscription_channel(
        msg_receiver: ActiveReceiver<Result<Message>>,
        rule: Option<OwnedMatchRule>,
        conn: &Connection,
    ) -> Self {
        let conn_inner = conn.inner.clone();

        Self {
            inner: Inner {
                conn_inner,
                msg_receiver,
                match_rule: rule,
            },
            closed_listener: None,
            terminated: false,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            backlog: Backlog::default(),
        }
    }
}

impl stream::Stream for MessageStream {
    type Item = Result<Message>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        #[cfg(all(target_os = "linux", feature = "handover"))]
        if let Some(item) = this.backlog.pop() {
            return Poll::Ready(Some(item));
        }
        match Pin::new(&mut this.inner.msg_receiver).poll_next(cx) {
            Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
            Poll::Ready(None) => {
                this.terminated = true;

                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        // Nothing is queued, so whether anything can still arrive is a question for the
        // connection. The socket reader closes this channel on its way out, but a runtime is
        // free to drop or abort the task it runs in, and then nobody ever does; the connection's
        // own closed flag is what says so in that case. Arming the listener before reading the
        // flag is what makes the pair race-free.
        if this.closed_listener.is_none() {
            this.closed_listener = Some(this.inner.conn_inner.closed_listener());
        }
        if !this.inner.conn_inner.is_closed() {
            let listener = this
                .closed_listener
                .as_mut()
                .expect("a listener was armed just above");
            if Pin::new(listener).poll(cx).is_pending() {
                return Poll::Pending;
            }
            // The listener has fired and is spent; the next poll that needs one arms it again.
            this.closed_listener = None;
        }

        match Pin::new(&mut this.inner.msg_receiver).poll_next(cx) {
            // A message can have landed while the connection was closing, so the queue has the
            // last word on when this stream ends.
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) | Poll::Pending => {
                this.terminated = true;

                Poll::Ready(None)
            }
        }
    }
}

impl OrderedStream for MessageStream {
    type Data = Result<Message>;
    type Ordering = Sequence;

    fn poll_next_before(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        before: Option<&Self::Ordering>,
    ) -> Poll<PollResult<Self::Ordering, Self::Data>> {
        let this = self.get_mut();

        match stream::Stream::poll_next(Pin::new(this), cx) {
            Poll::Pending if before.is_some() => {
                // Assume the provided Sequence in before was obtained from a Message
                // associated with our Connection (because that's the only supported use case).
                // Because there is only one socket-reader task, any messages that would have been
                // ordered before that message would have already been sitting in the broadcast
                // queue (and we would have seen Ready in our poll).  Because we didn't, we can
                // guarantee that we won't ever produce a message whose sequence is before that
                // provided value, and so we can return NoneBefore.
                //
                // This ensures that ordered_stream::Join will never return Pending while it
                // has a message buffered.
                Poll::Ready(PollResult::NoneBefore)
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(msg))) => Poll::Ready(PollResult::Item {
                ordering: msg.recv_position(),
                data: Ok(msg),
            }),
            Poll::Ready(Some(Err(e))) => Poll::Ready(PollResult::Item {
                ordering: Sequence::LAST,
                data: Err(e),
            }),
            Poll::Ready(None) => Poll::Ready(PollResult::Terminated),
        }
    }
}

impl FusedStream for MessageStream {
    fn is_terminated(&self) -> bool {
        // The queue can be over while the backlog, which comes before it, still holds messages.
        #[cfg(all(target_os = "linux", feature = "handover"))]
        if !self.backlog.is_empty() {
            return false;
        }

        self.terminated || self.inner.msg_receiver.is_terminated()
    }
}

impl From<Connection> for MessageStream {
    fn from(conn: Connection) -> Self {
        let conn_inner = conn.inner;
        let msg_receiver = conn_inner.msg_receiver.activate_cloned();

        Self {
            inner: Inner {
                conn_inner,
                msg_receiver,
                match_rule: None,
            },
            closed_listener: None,
            terminated: false,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            backlog: Backlog::default(),
        }
    }
}

impl From<&Connection> for MessageStream {
    fn from(conn: &Connection) -> Self {
        Self::from(conn.clone())
    }
}

impl From<MessageStream> for Connection {
    fn from(stream: MessageStream) -> Connection {
        Connection::from(&stream)
    }
}

impl From<&MessageStream> for Connection {
    fn from(stream: &MessageStream) -> Connection {
        Connection {
            inner: stream.inner.conn_inner.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct Inner {
    conn_inner: Arc<ConnectionInner>,
    msg_receiver: ActiveReceiver<Result<Message>>,
    match_rule: Option<OwnedMatchRule>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let conn = Connection {
            inner: self.conn_inner.clone(),
        };

        if let Some(rule) = self.match_rule.take() {
            conn.queue_remove_match(rule);
        }
    }
}

impl AsyncDrop for MessageStream {
    async fn async_drop(mut self) {
        let conn = Connection {
            inner: self.inner.conn_inner.clone(),
        };

        if let Some(rule) = self.inner.match_rule.take()
            && let Err(e) = conn.remove_match(rule).await
        {
            warn!("Failed to remove match rule: {}", e);
        }
    }
}
