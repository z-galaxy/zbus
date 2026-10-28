use std::{
    collections::HashMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use zruntime::Event;

use crate::{
    Message, OwnedMatchRule,
    connection::{MsgBroadcaster, PendingMethodCalls},
    log::{debug, trace},
    message::Type,
    runtime::{Runtime, Task, locks::Mutex},
};

use super::socket::ReadHalf;

#[derive(Debug)]
pub(crate) struct SocketReader {
    socket: Box<dyn ReadHalf>,
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
            socket,
            senders,
            pending_method_calls,
            already_received_bytes,
            #[cfg(unix)]
            already_received_fds,
            prev_seq: 0,
            socket_status,
            #[cfg(feature = "bus-impl")]
            validate_bodies,
        }
    }

    pub fn spawn(self, runtime: &Runtime) -> Task<()> {
        runtime.spawn("socket reader", self.receive_msg())
    }

    // Keep receiving messages and put them on the queue.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "socket reader", skip(self), level = "trace")
    )]
    async fn receive_msg(mut self) {
        loop {
            trace!("Waiting for message on the socket..");
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
                senders.clear();
                self.socket_status.closed.store(true, Ordering::Release);
                self.socket_status.closed_event.notify(usize::MAX);
                trace!("Socket reading task stopped");

                return;
            }
        }
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
        // Through the socket itself rather than through the `Box`, whose own implementation would
        // box the future of the call a second time.
        let msg = (*self.socket)
            .receive_message(
                seq,
                &mut self.already_received_bytes,
                #[cfg(unix)]
                &mut self.already_received_fds,
            )
            .await?;
        self.prev_seq = seq;
        #[cfg(feature = "bus-impl")]
        if self.validate_bodies {
            msg.validate_body()?;
        }

        Ok(msg)
    }
}

/// Ends the connection whenever the reader stops, however it stopped.
///
/// A runtime is free to drop or abort a task it was handed, and one that does would otherwise
/// leave [`Connection::closed`], every pending method call and every [`MessageStream`] waiting
/// for a reader that is never coming back. Running this from `Drop` covers every way out,
/// including a future dropped before it was ever polled. The reader's own error path does the
/// same for the errors it sees, and doing it a second time here changes nothing: the connection
/// is already closed and there are no calls left to fail.
///
/// [`Connection::closed`]: super::Connection::closed
/// [`MessageStream`]: crate::MessageStream
impl Drop for SocketReader {
    fn drop(&mut self) {
        self.socket_status.closed.store(true, Ordering::Release);
        self.socket_status.closed_event.notify(usize::MAX);
        self.fail_pending_method_calls(crate::Error::from(io::Error::other(
            "the socket reader task was cancelled",
        )));
    }
}

/// Socket-related state shared between [`super::ConnectionInner`] and the socket reader task.
#[derive(Debug)]
pub(super) struct SocketStatus {
    pub activity_event: Event,
    pub closed: AtomicBool,
    pub closed_event: Event,
}

#[cfg(all(test, feature = "p2p"))]
mod tests {
    use futures_util::{StreamExt, stream::FusedStream};
    use ntest::timeout;

    use crate::{
        Guid, MessageStream,
        connection::{Builder, socket::Channel},
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
