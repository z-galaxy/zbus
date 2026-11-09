#[cfg(all(target_os = "linux", feature = "handover"))]
use std::collections::VecDeque;
use std::{
    collections::HashMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
#[cfg(all(target_os = "linux", feature = "handover"))]
use std::{
    future::{Future, poll_fn},
    mem,
    pin::{Pin, pin},
    task::Poll,
};

use zruntime::Event;
#[cfg(all(target_os = "linux", feature = "handover"))]
use zruntime::EventListener;

use crate::{
    Message, OwnedMatchRule,
    connection::{MsgBroadcaster, PendingMethodCalls},
    log::{debug, trace},
    message::Type,
    runtime::{Runtime, Task, locks::Mutex},
};

#[cfg(all(target_os = "linux", feature = "handover"))]
use super::handover::{ReadParts, ReaderEnd, StopStatus, ends_input, stopped_error};
use super::socket::ReadHalf;
#[cfg(all(target_os = "linux", feature = "handover"))]
use super::socket_write::Prepared;

#[derive(Debug)]
pub(crate) struct SocketReader {
    /// The read half of the socket, until the reader stops and leaves it to the connection.
    socket: Option<Box<dyn ReadHalf>>,
    senders: Arc<Mutex<HashMap<Option<OwnedMatchRule>, MsgBroadcaster>>>,
    pending_method_calls: PendingMethodCalls,
    already_received_bytes: Vec<u8>,
    #[cfg(unix)]
    already_received_fds: Vec<std::os::fd::OwnedFd>,
    prev_seq: u64,
    socket_status: Arc<SocketStatus>,
    /// Whether the body of each message is checked against its signature as it's received.
    #[cfg(feature = "bus-impl")]
    validate_bodies: bool,
    /// The messages that a connection that this one took over left unread, oldest first, which
    /// the reader yields ahead of what it reads off the socket.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    taken_over: VecDeque<Message>,
}

impl SocketReader {
    pub fn new(
        socket: Box<dyn ReadHalf>,
        senders: Arc<Mutex<HashMap<Option<OwnedMatchRule>, MsgBroadcaster>>>,
        pending_method_calls: PendingMethodCalls,
        already_received_bytes: Vec<u8>,
        #[cfg(unix)] already_received_fds: Vec<std::os::fd::OwnedFd>,
        socket_status: Arc<SocketStatus>,
        #[cfg(feature = "bus-impl")] validate_bodies: bool,
    ) -> Self {
        Self {
            socket: Some(socket),
            senders,
            pending_method_calls,
            already_received_bytes,
            #[cfg(unix)]
            already_received_fds,
            prev_seq: 0,
            socket_status,
            #[cfg(feature = "bus-impl")]
            validate_bodies,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            taken_over: VecDeque::new(),
        }
    }

    /// The reader, to yield `msgs`, which a connection that this one took over left unread, ahead
    /// of what it reads off the socket.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub fn yielding_first(mut self, msgs: Vec<Message>) -> Self {
        self.taken_over = msgs.into();

        self
    }

    pub fn spawn(self, runtime: &Runtime) -> Task<()> {
        runtime.spawn("socket reader", self.receive_msg())
    }

    // Keep receiving messages and put them on the queue, until reading fails or the connection
    // stops reading for a hand-over.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "socket reader", skip(self), level = "trace")
    )]
    async fn receive_msg(mut self) {
        // Armed once, before the flag is first looked at: a stop asked for from now on is seen, by
        // the flag before a read or by the listener during one.
        #[cfg(all(target_os = "linux", feature = "handover"))]
        let mut stop = self.socket_status.stop.listener();
        loop {
            trace!("Waiting for message on the socket..");
            #[cfg(all(target_os = "linux", feature = "handover"))]
            let Some(msg) = self.read_unless_stopped(&mut stop).await else {
                return self.stop().await;
            };
            #[cfg(not(all(target_os = "linux", feature = "handover")))]
            let msg = self.read_socket().await;
            match &msg {
                Ok(msg) => {
                    trace!("Message received on the socket: {:?}", msg);
                    if matches!(msg.message_type(), Type::MethodReturn | Type::Error) {
                        self.dispatch_pending_reply(msg);
                    }
                }
                Err(e) => {
                    trace!("Error reading from the socket: {:?}", e);
                    self.fail_pending_method_calls(e.clone());
                }
            };

            let mut senders = self.senders.lock().await;
            for (rule, sender) in &*senders {
                if let Ok(msg) = &msg
                    && let Some(rule) = rule.as_ref()
                {
                    match rule.matches(msg) {
                        Ok(true) => (),
                        Ok(false) => continue,
                        Err(e) => {
                            debug!("Error matching message against rule: {:?}", e);

                            continue;
                        }
                    }
                }

                if let Err(e) = sender.broadcast(msg.clone()).await {
                    // An error would be due to either of these:
                    //
                    // 1. the channel is closed.
                    // 2. No active receivers.
                    //
                    // In either case, just log it unless this is the channel for the generic
                    // unfiltered stream, where the channel is not created on-demand.
                    if rule.is_some() {
                        trace!(
                            "Error broadcasting message to stream for `{:?}`: {:?}",
                            rule, e
                        );
                    }
                }
            }
            trace!("Broadcasted to all streams: {:?}", msg);

            if msg.is_err() {
                // At the end of the input, the read half and what was read are left for a
                // hand-over, as they are when the connection stops reading. The peer closed its
                // end, which takes nothing away from what the connection read, and the connection
                // that takes over comes to the same end. Nothing waits from here on, so the reader
                // can't be let go of with them.
                #[cfg(all(target_os = "linux", feature = "handover"))]
                {
                    let end = match &msg {
                        Err(e) if stop.is_some() && ends_input(e) => {
                            ReaderEnd::Ended(ReadParts::new(
                                self.socket.take().expect("the reader ends only once"),
                                mem::take(&mut self.taken_over),
                                mem::take(&mut self.already_received_bytes),
                                mem::take(&mut self.already_received_fds),
                            ))
                        }
                        _ => ReaderEnd::Failed,
                    };
                    // Before the connection is closed, which a hand-over may wait for.
                    self.socket_status.stop.end_reading(end);
                }
                // Closed before the streams end, so that a stream that ended finds the connection
                // closed, which tells that end apart from a stop.
                self.socket_status.closed.store(true, Ordering::Release);
                self.socket_status.closed_event.notify(usize::MAX);
                senders.clear();
                trace!("Socket reading task stopped");

                return;
            }
        }
    }

    /// Read the next message off the socket, unless the connection is asked to stop reading
    /// before one comes in whole, which gives the read up wherever it waits.
    ///
    /// A read that comes to a message in the same poll as the stop still yields it. A connection
    /// that can't stop has no `stop` listener, and reads on.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    async fn read_unless_stopped(
        &mut self,
        stop: &mut Option<EventListener>,
    ) -> Option<crate::Result<Message>> {
        let Some(stop) = stop else {
            return Some(self.read_socket().await);
        };
        // A message that is whole in what was read already is left unread too.
        if self.socket_status.stop.asked() {
            return None;
        }
        let mut read = pin!(self.read_socket());

        poll_fn(|cx| match read.as_mut().poll(cx) {
            Poll::Ready(msg) => Poll::Ready(Some(msg)),
            Poll::Pending => Pin::new(&mut *stop).poll(cx).map(|()| None),
        })
        .await
    }

    /// Stop reading, and leave the read half, with what was read off the socket but is yet to
    /// make a message, for the connection to hand over.
    ///
    /// The reader only gets here between messages, so each message that it read whole has gone to
    /// every stream that it was due to by now. Nothing is done to the socket itself.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    async fn stop(mut self) {
        // Each stream yields what is queued for it, then ends. This is the only wait, so a reader
        // let go of here still holds the read half, and ends the connection as one that failed.
        self.senders.lock().await.clear();
        let parts = ReadParts::new(
            self.socket.take().expect("the reader stops only once"),
            mem::take(&mut self.taken_over),
            mem::take(&mut self.already_received_bytes),
            mem::take(&mut self.already_received_fds),
        );
        // The replies to the calls that wait for them can no longer come in through this reader.
        self.pending_method_calls.fail_all(stopped_error());
        self.socket_status
            .stop
            .end_reading(ReaderEnd::Stopped(parts));
        trace!("Socket reading task stopped, as the connection stopped reading");
    }

    fn dispatch_pending_reply(&self, msg: &Message) {
        debug_assert!(matches!(
            msg.message_type(),
            Type::MethodReturn | Type::Error
        ));

        let reply_serial = match msg.header().reply_serial() {
            Some(serial) => serial,
            None => return,
        };

        let result = match msg.message_type() {
            Type::MethodReturn => Ok(msg.clone()),
            Type::Error => Err(msg.clone().into()),
            Type::MethodCall | Type::Signal => return,
        };
        self.pending_method_calls
            .complete_call(reply_serial, msg.recv_position(), result);
    }

    fn fail_pending_method_calls(&self, error: crate::Error) {
        self.pending_method_calls.fail_all(error);
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "trace"))]
    async fn read_socket(&mut self) -> crate::Result<Message> {
        self.socket_status.activity_event.notify(usize::MAX);
        let seq = self.prev_seq + 1;
        // The messages that a connection that this one took over left unread come first, each at
        // its position among those of this connection.
        #[cfg(all(target_os = "linux", feature = "handover"))]
        let taken_over = self.taken_over.pop_front().map(|msg| msg.at_position(seq));
        #[cfg(not(all(target_os = "linux", feature = "handover")))]
        let taken_over = None;
        let msg = match taken_over {
            Some(msg) => msg,
            None => {
                // Through the socket itself rather than through the `Box`, whose own
                // implementation would box the future of the call a second time.
                let socket = self
                    .socket
                    .as_deref_mut()
                    .expect("the reader reads only until it stops");

                socket
                    .receive_message(
                        seq,
                        &mut self.already_received_bytes,
                        #[cfg(unix)]
                        &mut self.already_received_fds,
                    )
                    .await?
            }
        };
        self.prev_seq = seq;
        #[cfg(feature = "bus-impl")]
        if self.validate_bodies {
            msg.validate_body()?;
        }

        Ok(msg)
    }
}

/// Ends the connection whenever the reader ends, however it ended, unless it left the read half
/// for a hand-over.
///
/// A runtime is free to drop or abort a task it was handed, and one that does would otherwise
/// leave [`Connection::closed`], every pending method call and every [`MessageStream`] waiting
/// for a reader that is never coming back. Running this from `Drop` covers every way out,
/// including a future dropped before it was ever polled. The reader's own error path does the
/// same for the errors it sees, and doing it a second time here changes nothing: the connection
/// is already closed and there are no calls left to fail. A reader that left the read half to the
/// connection, as it stopped for a hand-over or came to the end of the input, is through with all
/// of that: the connection is still open in the first case, and was closed by the reader itself in
/// the second.
///
/// [`Connection::closed`]: super::Connection::closed
/// [`MessageStream`]: crate::MessageStream
impl Drop for SocketReader {
    fn drop(&mut self) {
        #[cfg(all(target_os = "linux", feature = "handover"))]
        {
            if self.socket.is_none() {
                return;
            }
            self.socket_status.stop.end_reading(ReaderEnd::Failed);
        }
        self.socket_status.closed.store(true, Ordering::Release);
        self.socket_status.closed_event.notify(usize::MAX);
        self.fail_pending_method_calls(crate::Error::from(io::Error::other(
            "the socket reader task was cancelled",
        )));
    }
}

/// Socket-related state shared between [`super::ConnectionInner`], the socket reader task and the
/// writes of the connection.
#[derive(Debug)]
pub(super) struct SocketStatus {
    pub activity_event: Event,
    pub closed: AtomicBool,
    pub closed_event: Event,
    /// How many bytes the connection took to send and hasn't written yet, which the writes keep
    /// up to date under the lock that they take, for [`super::Connection::unwritten_len`] to read
    /// without it. Nothing else is read along with it, so it is stored and loaded `Relaxed`.
    pub unwritten: AtomicUsize,
    /// Whether the reader is to stop, or did, for the connection to be handed over.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub stop: StopStatus,
    /// Whether a hand-over of the connection is prepared, which has its sends stop, rather than
    /// wait for the socket.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub prepared: Prepared,
}

#[cfg(all(test, feature = "p2p"))]
mod tests {
    use futures_util::{StreamExt, stream::FusedStream};
    use ntest::timeout;

    use crate::{
        Guid, MessageStream,
        connection::{Builder, socket::Channel},
        message::Type,
        runtime::test_runtime::{AbortingRuntime, TestRuntime},
    };

    /// A connection whose reader task is taken away still lets go of everyone waiting on it.
    ///
    /// A runtime is within its rights to drop or abort the task, and the connection has no say
    /// in it, so the reader has to leave the connection closed on its way out either way.
    #[test]
    #[timeout(15000)]
    fn aborting_the_reader_task_closes_the_connection() {
        let runtime = AbortingRuntime::new();
        let (c1, c2) = Channel::pair();

        futures_lite::future::block_on(async {
            let guid = Guid::generate();
            let (client, peer) = futures_util::try_join!(
                Builder::authenticated_socket(c1, guid.clone())
                    .p2p()
                    .runtime(runtime.clone())
                    .build(),
                Builder::authenticated_socket(c2, guid)
                    .p2p()
                    .runtime(TestRuntime::new())
                    .build(),
            )
            .unwrap();
            let mut stream = MessageStream::from(&client);
            let mut peer_stream = MessageStream::from(&peer);

            // Nothing ever answers this call, so only the reader's departure can end the wait
            // for its reply. The peer seeing the call is what says it is registered and away.
            let calling = client.call_method(
                None::<()>,
                "/org/zbus/Test",
                Some("org.zbus.Test"),
                "NoReply",
                &(),
            );
            let aborting = async {
                peer_stream.next().await.unwrap().unwrap();
                runtime.abort_tasks();
            };
            let (reply, ()) = futures_util::future::join(calling, aborting).await;

            assert!(reply.is_err(), "the pending call was answered after all");
            client.closed().await;
            assert!(stream.next().await.is_none(), "the stream lives on");
            // A cancelled reader never gets to close the channel behind the stream, so only the
            // stream itself can say it is spent — and a `select!` loop spins until it does.
            assert!(stream.is_terminated(), "the finished stream denies it");
        });
    }

    /// A connection that ends is closed by the time its streams end, so a stream that ended finds
    /// the connection closed.
    ///
    /// The reader ends on a thread of its own, and a stream that ended before the connection was
    /// closed would find it open only if this thread looked in between, so the test tries many
    /// times.
    #[test]
    #[timeout(15000)]
    fn a_stream_that_ended_finds_the_connection_closed() {
        let (runtime, peer_runtime) = (TestRuntime::new(), TestRuntime::new());

        futures_lite::future::block_on(async {
            for i in 0..1000 {
                let (c1, c2) = Channel::pair();
                let guid = Guid::generate();
                let (conn, peer) = futures_util::try_join!(
                    Builder::authenticated_socket(c1, guid.clone())
                        .p2p()
                        .runtime(runtime.clone())
                        .build(),
                    Builder::authenticated_socket(c2, guid)
                        .p2p()
                        .runtime(peer_runtime.clone())
                        .build(),
                )
                .unwrap();
                let mut stream = MessageStream::from(&conn);

                // The connection comes to the end of its input.
                peer.close().await.unwrap();

                let end = stream.next().await.unwrap();
                assert!(end.is_err(), "the stream didn't end on an error: {end:?}");
                assert!(stream.next().await.is_none(), "the stream lives on");
                assert!(conn.is_closed(), "the connection is open still, at try {i}");
            }
        });
    }

    /// A method call made once the reader ended fails before it is sent, as no reply can come in
    /// any more, though the connection can still send.
    #[test]
    #[timeout(15000)]
    fn a_method_call_made_once_the_reader_ended_is_not_sent() {
        let runtime = AbortingRuntime::new();
        let (c1, c2) = Channel::pair();

        futures_lite::future::block_on(async {
            let guid = Guid::generate();
            let (client, peer) = futures_util::try_join!(
                Builder::authenticated_socket(c1, guid.clone())
                    .p2p()
                    .runtime(runtime.clone())
                    .build(),
                Builder::authenticated_socket(c2, guid)
                    .p2p()
                    .runtime(TestRuntime::new())
                    .build(),
            )
            .unwrap();
            let mut peer_stream = MessageStream::from(&peer);
            runtime.abort_tasks();
            client.closed().await;

            let reply = client
                .call_method(
                    None::<()>,
                    "/org/zbus/Test",
                    Some("org.zbus.Test"),
                    "NoReply",
                    &(),
                )
                .await;
            assert!(reply.is_err(), "the call was answered: {reply:?}");
            client
                .emit_signal(None::<()>, "/org/zbus/Test", "org.zbus.Test", "Test", &())
                .await
                .unwrap();

            // The signal is the first message that the peer gets, so the call never went out.
            let msg = peer_stream.next().await.unwrap().unwrap();
            assert_eq!(msg.message_type(), Type::Signal, "{msg:?}");
        });
    }

    /// A connection that checks the bodies of the messages that it receives ends on one whose body
    /// doesn't hold what its signature says, after it handed on the messages before it.
    #[cfg(feature = "bus-impl")]
    #[test]
    #[timeout(15000)]
    fn invalid_body() {
        use crate::{Error, Message};

        let (c1, c2) = Channel::pair();
        futures_lite::future::block_on(async {
            let guid = Guid::generate();
            let (receiver, sender) = futures_util::try_join!(
                Builder::authenticated_socket(c1, guid.clone())
                    .p2p()
                    .validate_bodies()
                    .runtime(TestRuntime::new())
                    .build(),
                Builder::authenticated_socket(c2, guid)
                    .p2p()
                    .runtime(TestRuntime::new())
                    .build(),
            )
            .unwrap();
            let mut stream = MessageStream::from(&receiver);
            // A signal whose body is a boolean of `bytes`.
            let signal = |bytes: &[u8]| {
                let builder = Message::signal("/org/zbus/Test", "org.zbus.Test", "Test");
                // SAFETY: The body carries no file descriptors, and the receiver is to check it.
                unsafe {
                    builder.build_raw_body(
                        bytes,
                        "b",
                        #[cfg(unix)]
                        vec![],
                    )
                }
                .unwrap()
            };

            sender.send(&signal(&[1, 0, 0, 0])).await.unwrap();
            sender.send(&signal(&[2, 0, 0, 0])).await.unwrap();
            let msg = stream.next().await.unwrap().unwrap();
            assert!(msg.body().deserialize::<bool>().unwrap());
            let error = stream.next().await.unwrap().unwrap_err();
            assert!(matches!(error, Error::Failure(_)), "{error:?}");
            receiver.closed().await;
            assert!(stream.next().await.is_none(), "the stream lives on");
        });
    }
}
