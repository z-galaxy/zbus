//! Connection API.
use std::{
    borrow::Cow,
    collections::HashMap,
    future::Future,
    io,
    num::NonZeroUsize,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use zruntime::{
    Event, EventListener,
    broadcast::{InactiveReceiver, Receiver, Sender as Broadcaster, channel},
};

#[cfg(feature = "service")]
use crate::ObjectServer;
#[cfg(feature = "service")]
use crate::log::debug;
#[cfg(all(target_os = "linux", feature = "handover"))]
use crate::runtime::IoSource;
use crate::{
    DBusError, Error, MatchRule, ObjectPath, OwnedGuid, OwnedMatchRule, Result,
    fdo::{ConnectionCredentials, ReleaseNameReply, RequestNameFlags, RequestNameReply},
    log::{Instrument, info, info_span, trace, trace_span, warn},
    message::{self, Flags, Message, Type},
    names::{BusName, ErrorName, InterfaceName, MemberName, OwnedUniqueName, WellKnownName},
    runtime::{Runtime, Task, locks::Mutex, traits},
    utils::next,
};

mod activity_listener;
pub use activity_listener::ActivityListener;

mod builder;
pub use builder::Builder;

pub mod socket;
pub use socket::Socket;

#[cfg(all(target_os = "linux", feature = "handover"))]
pub mod handover;
#[cfg(all(target_os = "linux", feature = "handover"))]
pub use handover::Handover;
#[cfg(all(target_os = "linux", feature = "handover"))]
use handover::SocketKind;
#[cfg(all(target_os = "linux", feature = "handover"))]
use handover::StopStatus;

mod socket_reader;
use socket_reader::{SocketReader, SocketStatus};

mod socket_write;
use socket_write::SocketWrite;

mod pending_method_calls;
use pending_method_calls::{PendingMethodCall, PendingMethodCalls};

pub(crate) mod handshake;
pub use handshake::AuthMechanism;
use handshake::Authenticated;

const DEFAULT_MAX_QUEUED: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// Inner state shared by Connection and WeakConnection
#[derive(Debug)]
pub(crate) struct ConnectionInner {
    server_guid: OwnedGuid,
    #[cfg(unix)]
    cap_unix_fd: bool,
    #[cfg(feature = "p2p")]
    bus_conn: bool,
    unique_name: OnceLock<OwnedUniqueName>,
    registered_names: Mutex<HashMap<WellKnownName<'static>, NameStatus>>,

    socket_status: Arc<SocketStatus>,
    socket_write: Mutex<SocketWrite>,
    /// Whether the connection can be prepared for a hand-over, as
    /// [`Connection::prepare_hand_over`] tells.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    can_prepare_hand_over: bool,

    // Socket reader task
    #[allow(unused)]
    socket_reader_task: OnceLock<Task<()>>,

    pub(crate) msg_receiver: InactiveReceiver<Result<Message>>,
    msg_senders: Arc<Mutex<HashMap<Option<OwnedMatchRule>, MsgBroadcaster>>>,
    pending_method_calls: PendingMethodCalls,

    subscriptions: Mutex<Subscriptions>,

    #[cfg(feature = "service")]
    object_server: OnceLock<ObjectServer>,
    #[cfg(feature = "service")]
    object_server_dispatch_task: OnceLock<Task<()>>,

    // The runtime that runs our tasks and times our calls out. Declared after the task handles
    // so that it outlives them.
    runtime: Runtime,

    drop_event: Event,

    method_timeout: Option<Duration>,
    // Cache the credentials.
    credentials: OnceLock<Arc<ConnectionCredentials>>,
}

impl ConnectionInner {
    /// Whether the connection is closed, in the sense [`Connection::is_closed`] reports.
    ///
    /// The `Acquire` load pairs with the `Release` store whoever closed the connection made, so
    /// everything they did on their way out is visible to whoever sees `true` here.
    pub(crate) fn is_closed(&self) -> bool {
        self.socket_status.closed.load(Ordering::Acquire)
    }

    /// A listener for the moment the connection closes.
    ///
    /// Arming one before reading the flag above is what makes the pair race-free: a connection
    /// that closes between the two still wakes the listener.
    pub(crate) fn closed_listener(&self) -> EventListener {
        self.socket_status.closed_event.listen()
    }
}

impl Drop for ConnectionInner {
    fn drop(&mut self) {
        // Notify anyone waiting that the connection is going away. Since we're being dropped, it's
        // not possible for any new listeners to be created after this notification, so this is
        // race-free.
        self.drop_event.notify(usize::MAX);
    }
}

type Subscriptions = HashMap<OwnedMatchRule, (u64, InactiveReceiver<Result<Message>>)>;

pub(crate) type MsgBroadcaster = Broadcaster<Result<Message>>;

/// A D-Bus connection.
///
/// A connection to a D-Bus bus, or a direct peer.
///
/// Once created, the connection is authenticated and negotiated and messages can be sent or
/// received, such as [method calls] or [signals].
///
/// For higher-level message handling (typed functions, introspection, documentation reasons etc),
/// it is recommended to wrap the low-level D-Bus messages into Rust functions with the
#[cfg_attr(
    all(feature = "proxy", feature = "service"),
    doc = "[`macro@crate::proxy`] and [`macro@crate::interface`] macros instead of doing it",
    doc = "directly on a `Connection`."
)]
#[cfg_attr(
    not(all(feature = "proxy", feature = "service")),
    doc = "`proxy` and `interface` macros (requiring the `proxy` and `service` features",
    doc = "respectively) instead of doing it directly on a `Connection`."
)]
///
/// Typically, a connection is made to the session bus with [`Connection::session`], or to the
#[cfg_attr(
    all(feature = "proxy", feature = "service"),
    doc = "system bus with [`Connection::system`]. Then the connection is used with",
    doc = "[`crate::Proxy`] instances or the on-demand [`ObjectServer`] instance that can be",
    doc = "accessed through [`Connection::object_server`]."
)]
#[cfg_attr(
    not(all(feature = "proxy", feature = "service")),
    doc = "system bus with [`Connection::system`]. Then the connection is used with `Proxy`",
    doc = "instances (requires the `proxy` feature) or the on-demand `ObjectServer` instance that",
    doc = "can be accessed through `Connection::object_server` (requires the `service` feature)."
)]
///
/// `Connection` implements [`Clone`] and cloning it is a very cheap operation, as the underlying
/// data is not cloned. This makes it very convenient to share the connection between different
/// parts of your code. `Connection` also implements [`std::marker::Sync`] and [`std::marker::Send`]
/// so you can send and share a connection instance across threads as well.
///
/// `Connection` keeps internal queues of incoming message. The default capacity of each of these is
/// 64. The capacity of the main (unfiltered) queue is configurable through the [`set_max_queued`]
/// method. When the queue is full, no more messages can be received until room is created for more.
/// This is why it's important to ensure that all [`crate::MessageStream`] instances are
/// continuously polled.
///
/// For sending messages you can use the [`Connection::send`] method.
///
/// To gracefully close a connection while waiting for any outstanding method calls to complete,
/// use [`Connection::graceful_shutdown`]. To immediately close a connection in a way that will
/// disrupt any outstanding method calls, use [`Connection::close`]. If you do not need the
/// shutdown to be immediate and do not care about waiting for outstanding method calls, you can
/// also simply drop the `Connection` instance, which will act similarly to spawning
/// `graceful_shutdown` in the background.
///
/// [method calls]: struct.Connection.html#method.call_method
/// [signals]: struct.Connection.html#method.emit_signal
/// [`Clone`]: https://doc.rust-lang.org/std/clone/trait.Clone.html
/// [`set_max_queued`]: struct.Connection.html#method.set_max_queued
///
/// ### Examples
///
/// #### Get the session bus ID
///
/// ```
/// # zbus::block_on(async {
/// use zbus::Connection;
///
/// let connection = Connection::session().await?;
///
/// let reply_body = connection
///     .call_method(
///         Some("org.freedesktop.DBus"),
///         "/org/freedesktop/DBus",
///         Some("org.freedesktop.DBus"),
///         "GetId",
///         &(),
///     )
///     .await?
///     .body();
///
/// let id: &str = reply_body.deserialize()?;
/// println!("Unique ID of the bus: {}", id);
/// # Ok::<(), zbus::Error>(())
/// # }).unwrap();
/// ```
///
/// #### Monitoring all messages
///
/// Let's eavesdrop on the session bus 😈 using the [Monitor] interface:
///
/// ```rust,no_run
/// # zbus::block_on(async {
/// use futures_util::stream::TryStreamExt;
/// use zbus::{Connection, MessageStream};
///
/// let connection = Connection::session().await?;
///
/// connection
///     .call_method(
///         Some("org.freedesktop.DBus"),
///         "/org/freedesktop/DBus",
///         Some("org.freedesktop.DBus.Monitoring"),
///         "BecomeMonitor",
///         &(&[] as &[&str], 0u32),
///     )
///     .await?;
///
/// let mut stream = MessageStream::from(connection);
/// while let Some(msg) = stream.try_next().await? {
///     println!("Got message: {}", msg);
/// }
///
/// # Ok::<(), zbus::Error>(())
/// # }).unwrap();
/// ```
///
/// This should print something like:
///
/// ```console
/// Got message: Signal NameAcquired from org.freedesktop.DBus
/// Got message: Signal NameLost from org.freedesktop.DBus
/// Got message: Method call GetConnectionUnixProcessID from :1.1324
/// Got message: Error org.freedesktop.DBus.Error.NameHasNoOwner:
///              Could not get PID of name ':1.1332': no such name from org.freedesktop.DBus
/// Got message: Method call AddMatch from :1.918
/// Got message: Method return from org.freedesktop.DBus
/// ```
///
/// [Monitor]: https://dbus.freedesktop.org/doc/dbus-specification.html#bus-messages-become-monitor
#[derive(Clone, Debug)]
#[must_use = "Dropping a `Connection` will close the underlying socket."]
pub struct Connection {
    pub(crate) inner: Arc<ConnectionInner>,
}

impl Connection {
    /// Send `msg` to the peer.
    ///
    /// If `msg` carries file descriptors and the connection can't pass them, this fails with
    /// [`Error::Unsupported`] before it sends anything.
    ///
    /// With the `handover` feature, on Linux, once a hand-over of the connection is prepared
    /// (`Connection::prepare_hand_over`), this doesn't wait for the socket: it writes what the
    /// connection has left to write, then `msg`, as far as the socket takes them at once, and keeps
    /// the rest, for the next send or [`Connection::flush`] to write, or for the hand-over to hand
    /// over.
    ///
    /// # Cancellation
    ///
    /// The future can be dropped before it resolves, such as to bound the send by a timeout, as a
    /// peer that doesn't read holds a send up for as long as it likes. If the connection wrote
    /// none of `msg` by then, `msg` isn't sent. Otherwise the connection writes the rest of it
    /// ahead of anything else that it sends: the next send does, and so does
    /// [`Connection::flush`], which waits for the socket to take it. The same goes for a send that
    /// fails midway, on an I/O error. So the peer gets each message whole, or not at all, and in
    /// the order that they were sent, unless [`Connection::close`] closes the connection with part
    /// of one left to write. [`Connection::unwritten_len`] tells how much is left.
    ///
    /// That is so over the sockets that zbus creates, and over a [`Socket`] of your own whose write
    /// half can resume writes ([`WriteHalf::can_resume_writes`]). A send over any other hands
    /// `msg` to [`WriteHalf::send_message`] instead. If it is dropped, or fails, midway, it can
    /// leave part of `msg` on the socket, which leaves the peer unable to make sense of anything
    /// sent on the connection after it.
    ///
    /// [`WriteHalf::can_resume_writes`]: socket::WriteHalf::can_resume_writes
    /// [`WriteHalf::send_message`]: socket::WriteHalf::send_message
    pub async fn send(&self, msg: &Message) -> Result<()> {
        #[cfg(unix)]
        if !msg.data().fds().is_empty() && !self.inner.cap_unix_fd {
            return Err(Error::Unsupported);
        }

        self.inner.socket_status.activity_event.notify(usize::MAX);
        let mut write = self.inner.socket_write.lock().await;

        write.send_message(msg).await
    }

    /// Send `msgs` to the peer, in order, with as few writes as the socket allows.
    ///
    /// [`Connection::send`] makes at least one write for each message. This hands all of `msgs` to
    /// the socket at once instead: the sockets that zbus creates write several messages at a time.
    /// No other message is sent on the connection in between. Given no messages, this returns at
    /// once and writes nothing, not even what the connection hasn't written yet, which
    /// [`Connection::flush`] writes.
    ///
    /// If any of `msgs` carries file descriptors and the connection can't pass them, this fails
    /// with [`Error::Unsupported`] before it sends anything.
    ///
    /// With the `handover` feature, on Linux, once a hand-over of the connection is prepared
    /// (`Connection::prepare_hand_over`), this doesn't wait for the socket: it writes what the
    /// connection has left to write, then `msgs`, as far as the socket takes them at once, and
    /// keeps the rest, for the next send or [`Connection::flush`] to write, or for the hand-over to
    /// hand over.
    ///
    /// # Cancellation
    ///
    /// As with [`Connection::send`], message by message. If the future is dropped, or the send
    /// fails, before all of `msgs` are written:
    ///
    /// * the messages before the one that the connection was writing were written whole;
    /// * if the connection wrote part of that one, it writes the rest ahead of anything else that
    ///   it sends, and otherwise that one isn't sent;
    /// * the messages after it aren't sent.
    ///
    /// A send over a [`Socket`] of your own whose write half can't resume writes
    /// ([`WriteHalf::can_resume_writes`]) hands all of `msgs` to [`WriteHalf::send_messages`]
    /// instead. If it is dropped, or fails, the messages before the one that it was sending then
    /// have been sent in full, and that one may have been sent in part, which leaves the peer
    /// unable to make sense of anything sent on the connection after it.
    ///
    /// [`WriteHalf::can_resume_writes`]: socket::WriteHalf::can_resume_writes
    /// [`WriteHalf::send_messages`]: socket::WriteHalf::send_messages
    pub async fn send_all(&self, msgs: &[Message]) -> Result<()> {
        #[cfg(unix)]
        if !self.inner.cap_unix_fd && msgs.iter().any(|msg| !msg.data().fds().is_empty()) {
            return Err(Error::Unsupported);
        }
        if msgs.is_empty() {
            return Ok(());
        }

        self.inner.socket_status.activity_event.notify(usize::MAX);
        let mut write = self.inner.socket_write.lock().await;

        write.send_messages(msgs).await
    }

    /// Write all that the connection hasn't written yet, waiting on the socket for as long as it
    /// takes, then flush the write half of the socket ([`WriteHalf::flush`]).
    ///
    /// What the connection hasn't written yet is the rest of a message that a send was dropped, or
    /// failed, in the middle of ([`Connection::send`] tells more), which
    /// [`Connection::unwritten_len`] counts. The next send writes it too, ahead of its own
    /// messages, so this is for a connection that is to send nothing more for a while, or for one
    /// that is about to be closed, whose peer would otherwise wait for the rest of that message.
    ///
    /// With the `handover` feature, on Linux, once a hand-over of the connection is prepared
    /// (`Connection::prepare_hand_over`), it is also the messages that sends kept, after that rest,
    /// as sends no longer wait for the socket, while this does.
    ///
    /// With the `handover` feature, on Linux, for a connection that took over another one
    /// (`Builder::take_over`), it is also, ahead of all else, what that one took to send and didn't
    /// write, which the peer may be waiting for.
    ///
    /// On a connection closed with [`Connection::close`], which lets go of what is left, this fails
    /// with an I/O error of kind [`NotConnected`](io::ErrorKind::NotConnected). A connection whose
    /// peer went away keeps it, and this tries to write it, as a send would.
    ///
    /// This takes the turn to write that a send takes, so it waits for the sends before it, and
    /// those after it wait for it.
    ///
    /// # Cancellation
    ///
    /// The future can be dropped before it resolves, such as to bound the flush by a timeout. It
    /// leaves what it didn't write for the next send, or flush, to write, ahead of anything else.
    /// The same goes for a flush that fails, on an I/O error.
    ///
    /// [`WriteHalf::flush`]: socket::WriteHalf::flush
    pub async fn flush(&self) -> Result<()> {
        self.inner.socket_status.activity_event.notify(usize::MAX);
        let mut write = self.inner.socket_write.lock().await;

        write.flush().await
    }

    /// How many bytes the connection took to send and hasn't written yet.
    ///
    /// That is the rest of a message that a send was dropped, or failed, in the middle of
    /// ([`Connection::send`] tells more), which the next send writes ahead of its own messages, and
    /// so does [`Connection::flush`]. While a send is under way, this counts what is left of the
    /// message that the send is in the middle of, if any, but not the messages that the send is
    /// yet to write any of.
    ///
    /// With the `handover` feature, on Linux, once a hand-over of the connection is prepared
    /// (`Connection::prepare_hand_over`), this also counts the messages that sends kept, whole,
    /// from the moment that a send kept them.
    ///
    /// With the `handover` feature, on Linux, for a connection that took over another one
    /// (`Builder::take_over`), it also counts what that one took to send and didn't write, from the
    /// moment that it is built.
    ///
    /// [`Connection::close`] lets go of these bytes. The peer going away doesn't, so they stay
    /// counted for as long as no write takes them.
    pub fn unwritten_len(&self) -> usize {
        self.inner.socket_status.unwritten.load(Ordering::Relaxed)
    }

    /// Prepare this connection to be handed over ([`MessageStream::hand_over`]): from now on, for
    /// good, no send waits for the socket to take what it sends.
    ///
    /// A peer that doesn't read holds a send up for as long as it likes, as the socket takes no
    /// more than its buffer holds. A service that is about to hand its connections over, such as to
    /// restart, would then wait for the slowest of its peers to read what it sends them meanwhile.
    /// Once the connection is prepared, each send writes what the socket takes at once, and keeps
    /// the rest, in order, ahead of the sends after it, which do the same: the next send writes
    /// what is kept first, as far as the socket takes it then, and [`Connection::flush`] writes all
    /// of it, waiting on the socket for as long as it takes. A send that waits for the socket as
    /// this is called stops waiting the same way. Nothing bounds what the sends keep, which
    /// [`Connection::unwritten_len`] counts. The connection reads on, as before, and a hand-over,
    /// which prepares the connection first, as this does, hands over what the sends kept, for the
    /// connection that takes over to write.
    ///
    /// This can't be undone. It returns `true` once the connection is prepared, the second time
    /// too, which changes nothing. That is so over the sockets that zbus creates, a `Channel` among
    /// them, and over a write half of your own that can resume writes
    /// ([`WriteHalf::can_resume_writes`]). A channel takes each message whole or not at all, and a
    /// peer that doesn't read fills it up as it does a socket: what a send to a full channel keeps
    /// is whole messages, which wait for room. Over a [`Socket`] of your own whose write half can't
    /// resume writes, this returns `false`, changing nothing, as a send over it has to wait for the
    /// half.
    ///
    /// This method is only available on Linux, with the `handover` feature.
    ///
    /// [`MessageStream::hand_over`]: crate::MessageStream::hand_over
    /// [`WriteHalf::can_resume_writes`]: socket::WriteHalf::can_resume_writes
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub fn prepare_hand_over(&self) -> bool {
        let inner = &*self.inner;
        if !inner.can_prepare_hand_over {
            return false;
        }
        inner.socket_status.prepared.set();

        true
    }

    /// Send a method call.
    ///
    /// Create a method-call message, send it over the connection, then wait for the reply.
    ///
    /// On successful reply, an `Ok(Message)` is returned. On error, an `Err` is returned. D-Bus
    /// error replies whose name is defined by [`crate::fdo::Error`] are returned as
    /// [`Error::FDO`], and all other error replies are returned as [`Error::MethodError`].
    pub async fn call_method<'d, 'p, 'i, 'm, D, P, I, M, B>(
        &self,
        destination: Option<D>,
        path: P,
        interface: Option<I>,
        method_name: M,
        body: &B,
    ) -> Result<Message>
    where
        D: TryInto<BusName<'d>>,
        P: TryInto<ObjectPath<'p>>,
        I: TryInto<InterfaceName<'i>>,
        M: TryInto<MemberName<'m>>,
        D::Error: Into<Error>,
        P::Error: Into<Error>,
        I::Error: Into<Error>,
        M::Error: Into<Error>,
        B: serde::ser::Serialize + crate::DynamicType,
    {
        let method = self
            .call_method_raw(
                destination,
                path,
                interface,
                method_name,
                Flags::empty(),
                body,
            )
            .await?
            .expect("no reply");

        if let Some(tout) = self.method_timeout() {
            self.inner.runtime.timeout(method, tout).await
        } else {
            method.await
        }
    }

    /// Send a method call.
    ///
    /// Send the given message, which must be a method call, over the connection and return an
    /// object that allows the reply to be retrieved.  Typically you'd want to use
    /// [`Connection::call_method`] instead.
    ///
    /// If the `flags` do not contain `Flags::NO_REPLY_EXPECTED`, the return value is
    /// guaranteed to be `Ok(Some(_))`, if there was no error encountered.
    ///
    /// INTERNAL NOTE: If this method is ever made pub, flags should become `MethodFlags`.
    pub(crate) async fn call_method_raw<'d, 'p, 'i, 'm, D, P, I, M, B>(
        &self,
        destination: Option<D>,
        path: P,
        interface: Option<I>,
        method_name: M,
        flags: Flags,
        body: &B,
    ) -> Result<Option<PendingMethodCall>>
    where
        D: TryInto<BusName<'d>>,
        P: TryInto<ObjectPath<'p>>,
        I: TryInto<InterfaceName<'i>>,
        M: TryInto<MemberName<'m>>,
        D::Error: Into<Error>,
        P::Error: Into<Error>,
        I::Error: Into<Error>,
        M::Error: Into<Error>,
        B: serde::ser::Serialize + crate::DynamicType,
    {
        let destination = destination
            .map(TryInto::try_into)
            .transpose()
            .map_err(Into::into)?;
        let path = path.try_into().map_err(Into::into)?;
        let interface = interface
            .map(TryInto::try_into)
            .transpose()
            .map_err(Into::into)?;
        let method_name = method_name.try_into().map_err(Into::into)?;
        let msg = self
            .method_call_builder(destination, path, interface, method_name, flags)
            .build(body)?;

        self.send_method_call(msg, flags).await
    }

    /// The builder for a method call message from this connection.
    ///
    /// The generic entry points convert their arguments and serialize the body; everything else
    /// about sending a method call lives here and in [`Self::send_method_call`], compiled once.
    fn method_call_builder<'b>(
        &'b self,
        destination: Option<BusName<'b>>,
        path: ObjectPath<'b>,
        interface: Option<InterfaceName<'b>>,
        method_name: MemberName<'b>,
        flags: Flags,
    ) -> message::Builder<'b> {
        let mut builder = Message::method_call(path, method_name);
        if let Some(sender) = self.unique_name() {
            builder = builder.sender(sender)
        }
        if let Some(destination) = destination {
            builder = builder.destination(destination)
        }
        if let Some(interface) = interface {
            builder = builder.interface(interface)
        }

        builder.with_flags(flags)
    }

    /// Send a method call message and register for its reply, unless none is expected.
    async fn send_method_call(
        &self,
        msg: Message,
        flags: Flags,
    ) -> Result<Option<PendingMethodCall>> {
        let serial = msg.primary_header().serial_num();
        if flags.contains(Flags::NO_REPLY_EXPECTED) {
            self.send(&msg).await?;

            Ok(None)
        } else {
            let pending_call = self.inner.pending_method_calls.register_call(serial)?;
            // The reader fails the calls that wait for their replies once it stops, which can be
            // a while after it was asked to, but a call made since is not to go out at all.
            #[cfg(all(target_os = "linux", feature = "handover"))]
            self.inner.socket_status.stop.ensure_reading()?;
            self.send(&msg).await?;

            Ok(Some(pending_call))
        }
    }

    /// Emit a signal.
    ///
    /// Create a signal message, and send it over the connection.
    pub async fn emit_signal<'d, 'p, 'i, 'm, D, P, I, M, B>(
        &self,
        destination: Option<D>,
        path: P,
        interface: I,
        signal_name: M,
        body: &B,
    ) -> Result<()>
    where
        D: TryInto<BusName<'d>>,
        P: TryInto<ObjectPath<'p>>,
        I: TryInto<InterfaceName<'i>>,
        M: TryInto<MemberName<'m>>,
        D::Error: Into<Error>,
        P::Error: Into<Error>,
        I::Error: Into<Error>,
        M::Error: Into<Error>,
        B: serde::ser::Serialize + crate::DynamicType,
    {
        let destination = destination
            .map(TryInto::try_into)
            .transpose()
            .map_err(Into::into)?;
        let path = path.try_into().map_err(Into::into)?;
        let interface = interface.try_into().map_err(Into::into)?;
        let signal_name = signal_name.try_into().map_err(Into::into)?;
        let m = self
            .signal_builder(destination, path, interface, signal_name)
            .build(body)?;

        self.send(&m).await
    }

    /// The builder for a signal message from this connection.
    ///
    /// Kept non-generic so that it is compiled once; see [`Self::method_call_builder`].
    fn signal_builder<'b>(
        &'b self,
        destination: Option<BusName<'b>>,
        path: ObjectPath<'b>,
        interface: InterfaceName<'b>,
        signal_name: MemberName<'b>,
    ) -> message::Builder<'b> {
        let mut builder = Message::signal(path, interface, signal_name);
        if let Some(sender) = self.unique_name() {
            builder = builder.sender(sender);
        }
        if let Some(destination) = destination {
            builder = builder.destination(destination);
        }

        builder
    }

    /// Reply to a message.
    ///
    /// Given an existing message (likely a method call), send a reply back to the caller with the
    /// given `body`.
    pub async fn reply<B>(&self, call: &zbus::message::Header<'_>, body: &B) -> Result<()>
    where
        B: serde::ser::Serialize + crate::DynamicType,
    {
        let m = self.reply_message(Message::method_return(call), &mut |b| b.build(body))?;
        self.send(&m).await
    }

    /// Reply an error to a message.
    ///
    /// Given an existing message (likely a method call), send an error reply back to the caller
    /// with the given `error_name` and `body`.
    pub async fn reply_error<'e, E, B>(
        &self,
        call: &zbus::message::Header<'_>,
        error_name: E,
        body: &B,
    ) -> Result<()>
    where
        B: serde::ser::Serialize + crate::DynamicType,
        E: TryInto<ErrorName<'e>>,
        E::Error: Into<Error>,
    {
        let m = self.reply_message(Message::error(call, error_name), &mut |b| b.build(body))?;
        self.send(&m).await
    }

    /// Build a reply from `builder`, with this connection's unique name as the sender.
    ///
    /// The body is written through a trait object so that this is compiled once rather than once
    /// per body type.
    fn reply_message<'b>(
        &'b self,
        mut builder: message::Builder<'b>,
        build: &mut dyn FnMut(message::Builder<'_>) -> Result<Message>,
    ) -> Result<Message> {
        if let Some(sender) = self.unique_name() {
            builder = builder.sender(sender);
        }
        build(builder)
    }

    /// Reply an error to a message.
    ///
    /// Given an existing message (likely a method call), send an error reply back to the caller
    /// using one of the standard interface reply types.
    pub async fn reply_dbus_error(
        &self,
        call: &zbus::message::Header<'_>,
        err: impl DBusError,
    ) -> Result<()> {
        let m = err.create_reply(call)?;
        self.send(&m).await
    }

    /// Register a well-known name for this connection.
    ///
    /// When connecting to a bus, the name is requested from the bus. In case of p2p connection, the
    /// name (if requested) is used for self-identification.
    ///
    /// You can request multiple names for the same connection. Use [`Connection::release_name`] for
    /// deregistering names registered through this method.
    ///
    /// Note that exclusive ownership without queueing is requested (using
    /// [`RequestNameFlags::REPLACE_EXISTING`] and [`RequestNameFlags::DO_NOT_QUEUE`] flags) since
    /// that is the most typical case. If that is not what you want, you should use
    /// [`Connection::request_name_with_flags`] instead (but make sure then that name is requested
    /// **after** you've set up your service implementation with the `ObjectServer`).
    ///
    /// # Caveats
    ///
    /// The associated `ObjectServer` will only handle method calls destined for the unique name of
    /// this connection or any of the registered well-known names. If no well-known name is
    /// registered, the method calls destined to all well-known names will be handled.
    ///
    /// Since names registered through any other means than `Connection` or [`Builder`]
    /// API are not known to the connection, method calls destined to those names will only be
    /// handled by the associated `ObjectServer` if none of the names are registered through
    /// `Connection*` API. Simply put, either register all the names through `Connection*` API or
    /// none of them.
    ///
    /// # Errors
    ///
    /// Fails with `zbus::Error::NameTaken` if the name is already owned by another peer.
    pub async fn request_name<'w, W>(&self, well_known_name: W) -> Result<()>
    where
        W: TryInto<WellKnownName<'w>>,
        W::Error: Into<Error>,
    {
        self.request_name_with_flags(well_known_name, RequestNameFlags::default())
            .await
            .map(|_| ())
    }

    /// Register a well-known name for this connection.
    ///
    /// This is the same as [`Connection::request_name`] but allows to specify the flags to use when
    /// requesting the name.
    #[cfg_attr(
        feature = "proxy",
        doc = "If the [`RequestNameFlags::DO_NOT_QUEUE`] flag is not specified and request ends",
        doc = "up in the queue, you can use [`crate::fdo::NameAcquiredStream`] to be notified",
        doc = "when the name is acquired. A queued name request can be cancelled using",
        doc = "[`Connection::release_name`].",
        doc = "",
        doc = "If the [`RequestNameFlags::ALLOW_REPLACEMENT`] flag is specified, the requested",
        doc = "name can be lost if another peer requests the same name. You can use",
        doc = "[`crate::fdo::NameLostStream`] to be notified when the name is lost"
    )]
    #[cfg_attr(
        not(feature = "proxy"),
        doc = "If the [`RequestNameFlags::DO_NOT_QUEUE`] flag is not specified and request ends",
        doc = "up in the queue, you can use `fdo::NameAcquiredStream` (requires the `proxy`",
        doc = "feature) to be notified when the name is acquired. A queued name request can be",
        doc = "cancelled using [`Connection::release_name`].",
        doc = "",
        doc = "If the [`RequestNameFlags::ALLOW_REPLACEMENT`] flag is specified, the requested",
        doc = "name can be lost if another peer requests the same name. You can use",
        doc = "`fdo::NameLostStream` (requires the `proxy` feature) to be notified when the name",
        doc = "is lost"
    )]
    ///
    /// # Example
    ///
    /// ```
    /// # // The example waits for name acquisition and loss through the `DBusProxy` signals.
    /// # #[cfg(feature = "proxy")]
    /// # zbus::block_on(async {
    /// use zbus::{Connection, fdo::{DBusProxy, RequestNameFlags, RequestNameReply}};
    /// use futures_util::stream::StreamExt;
    ///
    /// let name = "org.freedesktop.zbus.QueuedNameTest";
    /// let conn1 = Connection::session().await?;
    /// // This should just work right away.
    /// conn1.request_name_with_flags(name, RequestNameFlags::DO_NOT_QUEUE).await?;
    ///
    /// let conn2 = Connection::session().await?;
    /// // A second request from the another connection will fail with `DO_NOT_QUEUE` flag, which is
    /// // implicit with `request_name` method.
    /// assert!(conn2.request_name(name).await.is_err());
    ///
    /// // Now let's try w/o `DO_NOT_QUEUE` and we should be queued.
    /// let reply = conn2
    ///     .request_name_with_flags(name, RequestNameFlags::ALLOW_REPLACEMENT)
    ///     .await?;
    /// assert_eq!(reply, RequestNameReply::InQueue);
    /// // Another request should just give us the same response.
    /// let reply = conn2
    ///     // The flags on subsequent requests will however be ignored.
    ///     .request_name_with_flags(name, RequestNameFlags::empty())
    ///     .await?;
    /// assert_eq!(reply, RequestNameReply::InQueue);
    /// let mut acquired_stream = DBusProxy::new(&conn2)
    ///     .await?
    ///     .receive_name_acquired()
    ///     .await?;
    /// assert!(conn1.release_name(name).await?);
    /// // This would have waited forever if `conn1` hadn't just release the name.
    /// let acquired = acquired_stream.next().await.unwrap();
    /// assert_eq!(acquired.args().unwrap().name, name);
    ///
    /// // conn2 made the mistake of being too nice and allowed name replacemnt, so conn1 should be
    /// // able to take it back.
    /// let mut lost_stream = DBusProxy::new(&conn2)
    ///     .await?
    ///     .receive_name_lost()
    ///     .await?;
    /// conn1.request_name(name).await?;
    /// let lost = lost_stream.next().await.unwrap();
    /// assert_eq!(lost.args().unwrap().name, name);
    ///
    /// # Ok::<(), zbus::Error>(())
    /// # }).unwrap();
    /// ```
    ///
    /// # Caveats
    ///
    /// * Same as that of [`Connection::request_name`].
    #[cfg_attr(
        feature = "proxy",
        doc = "* If you wish to track changes to name ownership after this call, make sure that",
        doc = "  the [`crate::fdo::NameAcquiredStream`] and/or [`crate::fdo::NameLostStream`]",
        doc = "  instance(s) are created **before** calling this method. Otherwise, you may loose",
        doc = "  the signal if it's emitted after this call but just before the stream instance",
        doc = "  get created."
    )]
    #[cfg_attr(
        not(feature = "proxy"),
        doc = "* If you wish to track changes to name ownership after this call, make sure that",
        doc = "  the `fdo::NameAcquiredStream` and/or `fdo::NameLostStream` instance(s) (requiring",
        doc = "  the `proxy` feature) are created **before** calling this method. Otherwise, you",
        doc = "  may loose the signal if it's emitted after this call but just before the stream",
        doc = "  instance get created."
    )]
    pub async fn request_name_with_flags<'w, W>(
        &self,
        well_known_name: W,
        flags: RequestNameFlags,
    ) -> Result<RequestNameReply>
    where
        W: TryInto<WellKnownName<'w>>,
        W::Error: Into<Error>,
    {
        let well_known_name = well_known_name.try_into().map_err(Into::into)?;

        // Warn if requesting a name before setting up the object server, as this can cause
        // method calls to be lost.
        #[cfg(feature = "service")]
        if self.is_bus() && self.inner.object_server.get().is_none() {
            warn!(
                "Requesting name `{well_known_name}` before setting up the object server. \
                Method calls arriving before interfaces are registered may be lost. \
                Consider using `connection::Builder::serve_at()` and `::name()` instead.",
            );
        }
        // We keep the lock until the end of this function so that the (possibly) spawned task
        // doesn't end up accessing the name entry before it's inserted.
        let mut names = self.inner.registered_names.lock().await;

        match names.get(&well_known_name) {
            Some(NameStatus::Owner(_)) => return Ok(RequestNameReply::AlreadyOwner),
            Some(NameStatus::Queued(_)) => return Ok(RequestNameReply::InQueue),
            None => (),
        }

        if !self.is_bus() {
            names.insert(well_known_name.to_owned(), NameStatus::Owner(None));

            return Ok(RequestNameReply::PrimaryOwner);
        }

        let acquired_match_rule = MatchRule::fdo_signal_builder("NameAcquired")
            .arg(0, well_known_name.as_ref())
            .build()?;
        let mut acquired_stream = self.add_match(acquired_match_rule.into(), None).await?;
        let lost_match_rule = MatchRule::fdo_signal_builder("NameLost")
            .arg(0, well_known_name.as_ref())
            .build()?;
        let mut lost_stream = self.add_match(lost_match_rule.into(), None).await?;
        let reply = self
            .call_method(
                Some("org.freedesktop.DBus"),
                "/org/freedesktop/DBus",
                Some("org.freedesktop.DBus"),
                "RequestName",
                &(well_known_name.clone(), flags),
            )
            .await?
            .body()
            .deserialize::<RequestNameReply>()?;
        let lost_task_name = format!("monitor_name_lost{{name={well_known_name}}}");
        let lost_task_name_span = info_span!("monitor_name_lost", name = %well_known_name);
        let name_lost_fut = if flags.contains(RequestNameFlags::ALLOW_REPLACEMENT) {
            let weak_conn = WeakConnection::from(self);
            let well_known_name = well_known_name.to_owned();
            Some(
                async move {
                    loop {
                        let signal = next(&mut lost_stream).await;
                        let inner = match weak_conn.upgrade() {
                            Some(conn) => conn.inner,
                            None => break,
                        };

                        match signal {
                            Some(signal) => match signal {
                                Ok(_) => {
                                    info!(
                                        "Connection `{}` lost name `{}`",
                                        // SAFETY: This is bus connection so unique name can't be
                                        // None.
                                        inner.unique_name.get().unwrap(),
                                        well_known_name
                                    );
                                    inner.registered_names.lock().await.remove(&well_known_name);

                                    break;
                                }
                                Err(e) => warn!("Failed to parse `NameLost` signal: {}", e),
                            },
                            None => {
                                trace!("`NameLost` signal stream closed");
                                // This is a very strange state we end up in. Now the name is
                                // question remains in the queue
                                // forever. Maybe we can do better here but I
                                // think it's a very unlikely scenario anyway.
                                //
                                // Can happen if the connection is lost/dropped but then the whole
                                // `Connection` instance will go away soon anyway and hence this
                                // strange state along with it.
                                break;
                            }
                        }
                    }
                }
                .instrument(lost_task_name_span),
            )
        } else {
            None
        };
        let status = match reply {
            RequestNameReply::InQueue => {
                let weak_conn = WeakConnection::from(self);
                let well_known_name = well_known_name.to_owned();
                let task_name = format!("monitor_name_acquired{{name={well_known_name}}}");
                let task_name_span = info_span!("monitor_name_acquired", name = %well_known_name);
                let task = self.runtime().spawn(
                    task_name,
                    async move {
                        loop {
                            let signal = next(&mut acquired_stream).await;
                            let inner = match weak_conn.upgrade() {
                                Some(conn) => conn.inner,
                                None => break,
                            };
                            match signal {
                                Some(signal) => match signal {
                                    Ok(_) => {
                                        let mut names = inner.registered_names.lock().await;
                                        if let Some(status) = names.get_mut(&well_known_name) {
                                            let task = name_lost_fut.map(|fut| {
                                                inner.runtime.spawn(lost_task_name, fut)
                                            });
                                            *status = NameStatus::Owner(task);

                                            break;
                                        }
                                        // else the name was released in the meantime. :shrug:
                                    }
                                    Err(e) => warn!("Failed to parse `NameAcquired` signal: {}", e),
                                },
                                None => {
                                    trace!("`NameAcquired` signal stream closed");
                                    // See comment above for similar state in case of `NameLost`
                                    // stream.
                                    break;
                                }
                            }
                        }
                    }
                    .instrument(task_name_span),
                );

                NameStatus::Queued(task)
            }
            RequestNameReply::PrimaryOwner | RequestNameReply::AlreadyOwner => {
                let task = name_lost_fut.map(|fut| self.runtime().spawn(lost_task_name, fut));

                NameStatus::Owner(task)
            }
            RequestNameReply::Exists => return Err(Error::NameTaken),
        };

        names.insert(well_known_name.to_owned(), status);

        Ok(reply)
    }

    /// Deregister a previously registered well-known name for this service on the bus.
    ///
    /// Use this method to deregister a well-known name, registered through
    /// [`Connection::request_name`].
    ///
    /// Unless an error is encountered, returns `Ok(true)` if name was previously registered with
    /// the bus through `self` and it has now been successfully deregistered, `Ok(false)` if name
    /// was not previously registered or already deregistered.
    pub async fn release_name<'w, W>(&self, well_known_name: W) -> Result<bool>
    where
        W: TryInto<WellKnownName<'w>>,
        W::Error: Into<Error>,
    {
        let well_known_name: WellKnownName<'w> = well_known_name.try_into().map_err(Into::into)?;
        let mut names = self.inner.registered_names.lock().await;
        // FIXME: Should be possible to avoid cloning/allocation here
        if names.remove(&well_known_name.to_owned()).is_none() {
            return Ok(false);
        };

        if !self.is_bus() {
            return Ok(true);
        }

        self.call_method(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            Some("org.freedesktop.DBus"),
            "ReleaseName",
            &well_known_name,
        )
        .await?
        .body()
        .deserialize::<ReleaseNameReply>()
        .map(|r| r == ReleaseNameReply::Released)
    }

    /// Check if `self` is a connection to a message bus.
    ///
    /// This will return `false` for p2p connections. When the `p2p` feature is disabled, this will
    /// always return `true`.
    pub fn is_bus(&self) -> bool {
        #[cfg(feature = "p2p")]
        {
            self.inner.bus_conn
        }
        #[cfg(not(feature = "p2p"))]
        {
            true
        }
    }

    /// The unique name of the connection, if set/applicable.
    ///
    /// The unique name is assigned by the message bus, or set manually using
    #[cfg_attr(feature = "bus-impl", doc = "[`Connection::set_unique_name`].")]
    #[cfg_attr(
        not(feature = "bus-impl"),
        doc = "`Connection::set_unique_name` (requires the `bus-impl` feature)."
    )]
    pub fn unique_name(&self) -> Option<&OwnedUniqueName> {
        self.inner.unique_name.get()
    }

    /// Set the unique name of the connection (if not already set).
    ///
    /// This is mainly provided for bus implementations. All other users should not need to use this
    /// method. Hence why this method is only available when the `bus-impl` feature is enabled.
    ///
    /// # Panics
    ///
    /// This method panics if the unique name is already set. It will always panic if the connection
    /// is to a message bus as it's the bus that assigns peers their unique names.
    #[cfg(feature = "bus-impl")]
    pub fn set_unique_name<U>(&self, unique_name: U) -> Result<()>
    where
        U: TryInto<OwnedUniqueName>,
        U::Error: Into<Error>,
    {
        let name = unique_name.try_into().map_err(Into::into)?;
        self.set_unique_name_(name);

        Ok(())
    }

    /// The capacity of the main (unfiltered) queue.
    pub fn max_queued(&self) -> NonZeroUsize {
        self.inner.msg_receiver.capacity()
    }

    /// Set the capacity of the main (unfiltered) queue.
    ///
    /// The queue is shared by all the clones of this connection and by every [`MessageStream`]
    /// created from it without a match rule, so the new capacity applies to all of them.
    ///
    /// Unlike [`MessageStream::set_max_queued`], this can lower the capacity. A message stays in
    /// the queue until every stream that was there when it arrived has received it, so if `max` is
    /// below the number of messages that are queued, the oldest ones are dropped, leaving the
    /// newest `max`. A stream that had not received a dropped message skips it without any
    /// indication, and carries on with the first message that is left.
    ///
    /// [`MessageStream`]: crate::MessageStream
    /// [`MessageStream::set_max_queued`]: crate::MessageStream::set_max_queued
    pub fn set_max_queued(&mut self, max: NonZeroUsize) {
        self.inner.msg_receiver.clone().set_capacity(max);
    }

    /// The server's GUID.
    pub fn server_guid(&self) -> &OwnedGuid {
        &self.inner.server_guid
    }

    /// Spawns `future` on the runtime this connection runs on, under the diagnostic name `name`.
    ///
    /// Hidden because it exists for zbus's own tests, which have to run a future alongside a
    /// connection on whatever runtime that connection was built with. It is not part of the
    /// public API and may change or go without notice.
    #[doc(hidden)]
    pub fn spawn<T>(
        &self,
        name: impl Into<Cow<'static, str>>,
        future: impl Future<Output = T> + Send + 'static,
    ) -> impl traits::TaskHandle<T>
    where
        T: Send + 'static,
    {
        self.inner.runtime.spawn(name, future)
    }

    /// The runtime this connection runs on.
    pub(crate) fn runtime(&self) -> &Runtime {
        &self.inner.runtime
    }

    /// Get a reference to the associated [`ObjectServer`].
    ///
    /// The `ObjectServer` is created on-demand.
    ///
    /// The `ObjectServer` starts listening for method calls in the background, so calls that
    /// arrive before it does go unanswered. Callers that reach the connection through a
    /// well-known name requested after the interfaces are registered do not run into this, but
    /// the peer of a peer-to-peer connection, or a caller of the unique name of the connection,
    /// can. The interfaces registered through [`Builder::serve_at`] are served from the start, as
    /// the `ObjectServer` then listens before the connection reads any message.
    ///
    /// **Note**: Once the `ObjectServer` is created, it will be replying to all method calls
    /// received on `self`. If you want to manually reply to method calls, do not use this
    /// method (or any of the `ObjectServer` related API).
    #[cfg(feature = "service")]
    pub fn object_server(&self) -> &ObjectServer {
        self.ensure_object_server(true)
    }

    #[cfg(feature = "service")]
    pub(crate) fn ensure_object_server(&self, start: bool) -> &ObjectServer {
        self.inner
            .object_server
            .get_or_init(move || self.setup_object_server(start, None))
    }

    #[cfg(feature = "service")]
    fn setup_object_server(&self, start: bool, started_event: Option<Event>) -> ObjectServer {
        if start {
            self.start_object_server(started_event);
        }

        ObjectServer::new(self)
    }

    #[cfg(feature = "service")]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub(crate) fn start_object_server(&self, started_event: Option<Event>) {
        self.inner.object_server_dispatch_task.get_or_init(|| {
            trace!("starting ObjectServer task");
            let weak_conn = WeakConnection::from(self);

            self.inner.runtime.spawn(
                "obj_server_task",
                async move {
                    let mut stream = match weak_conn.upgrade() {
                        Some(conn) => {
                            let mut builder = MatchRule::builder().msg_type(Type::MethodCall);
                            if let Some(unique_name) = conn.unique_name() {
                                builder = builder.destination(&**unique_name);
                            }
                            let rule = builder
                                .build()
                                .expect("a unique name is a valid destination");
                            match conn.add_match(rule.into(), None).await {
                                Ok(stream) => stream,
                                Err(e) => {
                                    // Very unlikely but can happen I guess if connection is closed.
                                    debug!("Failed to create message stream: {}", e);

                                    return;
                                }
                            }
                        }
                        None => {
                            trace!("Connection is gone, stopping associated object server task");

                            return;
                        }
                    };
                    if let Some(started_event) = started_event {
                        started_event.notify(1);
                    }

                    trace!("waiting for incoming method call messages..");
                    while let Some(msg) = next(&mut stream).await.and_then(|m| {
                        if let Err(e) = &m {
                            debug!("Error while reading from object server stream: {:?}", e);
                        }
                        m.ok()
                    }) {
                        if let Some(conn) = weak_conn.upgrade() {
                            let hdr = msg.header();
                            // If we're connected to a bus, skip the destination check as the
                            // server will only send us method calls destined to us.
                            if !conn.is_bus() {
                                match hdr.destination() {
                                    // Unique name is already checked by the match rule.
                                    Some(BusName::Unique(_)) | None => (),
                                    Some(BusName::WellKnown(dest)) => {
                                        let names = conn.inner.registered_names.lock().await;
                                        // destination doesn't matter if no name has been registered
                                        // (probably means the name is registered through external
                                        // means).
                                        if !names.is_empty() && !names.contains_key(dest) {
                                            trace!(
                                                "Got a method call for a different destination: {}",
                                                dest
                                            );

                                            continue;
                                        }
                                    }
                                }
                            }
                            let server = conn.object_server();
                            if let Err(e) = server.dispatch_call(&msg, &hdr).await {
                                debug!(
                                    "Error dispatching message. Message: {:?}, error: {:?}",
                                    msg, e
                                );
                            }
                        } else {
                            // If connection is completely gone, no reason to keep running the task
                            // anymore.
                            trace!("Connection is gone, stopping associated object server task");
                            break;
                        }
                    }
                }
                .instrument(info_span!("obj_server_task")),
            )
        });
    }

    pub(crate) async fn add_match(
        &self,
        rule: OwnedMatchRule,
        max_queued: Option<NonZeroUsize>,
    ) -> Result<Receiver<Result<Message>>> {
        use std::collections::hash_map::Entry;

        // A connection asked to stop reading makes no more streams, though its reader may be yet
        // to stop and close the channels of the others.
        #[cfg(all(target_os = "linux", feature = "handover"))]
        self.inner.socket_status.stop.ensure_reading()?;
        if self.inner.msg_senders.lock().await.is_empty() {
            return Err(self.reader_ended_error());
        }

        let mut subscriptions = self.inner.subscriptions.lock().await;
        let msg_type = rule.msg_type().unwrap_or(Type::Signal);
        match subscriptions.entry(rule.clone()) {
            Entry::Vacant(e) => {
                let max_queued = max_queued.unwrap_or(DEFAULT_MAX_QUEUED);
                let (sender, mut receiver) = channel(max_queued);
                receiver.set_await_active(false);
                if self.is_bus() && msg_type == Type::Signal {
                    self.call_method(
                        Some("org.freedesktop.DBus"),
                        "/org/freedesktop/DBus",
                        Some("org.freedesktop.DBus"),
                        "AddMatch",
                        &e.key(),
                    )
                    .await?;
                }
                // The reader may have ended or stopped in the meantime, and closed the channels of
                // the others. The subscription is only recorded once its channel is in place.
                {
                    let mut senders = self.inner.msg_senders.lock().await;
                    if senders.is_empty() {
                        return Err(self.reader_ended_error());
                    }
                    senders.insert(Some(rule), sender);
                }
                e.insert((1, receiver.clone().deactivate()));

                Ok(receiver)
            }
            Entry::Occupied(mut e) => {
                let (num_subscriptions, receiver) = e.get_mut();
                *num_subscriptions += 1;
                if let Some(max_queued) = max_queued
                    && max_queued > receiver.capacity()
                {
                    receiver.set_capacity(max_queued);
                }

                Ok(receiver.activate_cloned())
            }
        }
    }

    /// The error of a stream that can't be made, as the reader closed the channels of the others.
    fn reader_ended_error(&self) -> Error {
        #[cfg(all(target_os = "linux", feature = "handover"))]
        if let Err(e) = self.inner.socket_status.stop.ensure_reading() {
            return e;
        }

        // The reader ended on an error.
        Error::InputOutput(Arc::new(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "Socket reader task has errored out",
        )))
    }

    pub(crate) async fn remove_match(&self, rule: OwnedMatchRule) -> Result<bool> {
        use std::collections::hash_map::Entry;
        let mut subscriptions = self.inner.subscriptions.lock().await;
        // TODO when it becomes stable, use HashMap::raw_entry and only require expr: &str
        // (both here and in add_match)
        let msg_type = rule.msg_type().unwrap_or(Type::Signal);
        match subscriptions.entry(rule) {
            Entry::Vacant(_) => Ok(false),
            Entry::Occupied(mut e) => {
                let rule = e.key().inner().clone();
                e.get_mut().0 -= 1;
                if e.get().0 == 0 {
                    if self.is_bus() && msg_type == Type::Signal {
                        self.call_method(
                            Some("org.freedesktop.DBus"),
                            "/org/freedesktop/DBus",
                            Some("org.freedesktop.DBus"),
                            "RemoveMatch",
                            &rule,
                        )
                        .await?;
                    }
                    e.remove();
                    self.inner
                        .msg_senders
                        .lock()
                        .await
                        .remove(&Some(rule.into()));
                }
                Ok(true)
            }
        }
    }

    pub(crate) fn queue_remove_match(&self, rule: OwnedMatchRule) {
        let conn = self.clone();
        let task_name = format!("Remove match `{}`", *rule);
        // Nobody is waiting for the outcome: the stream that held the rule is already gone.
        let remove_match = async move {
            let _ = conn.remove_match(rule).await;
        }
        .instrument(trace_span!("{}", task_name));
        self.inner.runtime.spawn(task_name, remove_match).detach()
    }

    /// The method_timeout (if any). See [Builder::method_timeout] for details.
    pub fn method_timeout(&self) -> Option<Duration> {
        self.inner.method_timeout
    }

    pub(crate) async fn new(
        auth: Authenticated,
        writes: Option<socket::Writes>,
        #[cfg(all(target_os = "linux", feature = "handover"))] source: Option<(
            IoSource,
            SocketKind,
        )>,
        #[cfg(all(target_os = "linux", feature = "handover"))] unwritten: socket_write::Unwritten,
        #[allow(unused)] bus_connection: bool,
        runtime: Runtime,
        method_timeout: Option<Duration>,
    ) -> Result<Self> {
        #[cfg(unix)]
        let cap_unix_fd = auth.cap_unix_fd;

        macro_rules! create_msg_broadcast_channel {
            ($size:expr) => {{
                let (msg_sender, msg_receiver) = channel($size);
                let mut msg_receiver = msg_receiver.deactivate();
                msg_receiver.set_await_active(false);

                (msg_sender, msg_receiver)
            }};
        }
        // The unfiltered message channel.
        let (msg_sender, msg_receiver) = create_msg_broadcast_channel!(DEFAULT_MAX_QUEUED);
        let mut msg_senders = HashMap::new();
        msg_senders.insert(None, msg_sender);

        let msg_senders = Arc::new(Mutex::new(msg_senders));
        let pending_method_calls = PendingMethodCalls::default();
        let subscriptions = Mutex::new(HashMap::new());
        let socket_status = Arc::new(SocketStatus {
            activity_event: Event::new(),
            closed: AtomicBool::new(false),
            closed_event: Event::new(),
            unwritten: AtomicUsize::new(0),
            #[cfg(all(target_os = "linux", feature = "handover"))]
            stop: StopStatus::new(),
            #[cfg(all(target_os = "linux", feature = "handover"))]
            prepared: Default::default(),
        });
        let socket_write = SocketWrite::new(
            auth.socket_write,
            writes,
            socket_status.clone(),
            #[cfg(all(target_os = "linux", feature = "handover"))]
            source,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            unwritten,
        );
        #[cfg(all(target_os = "linux", feature = "handover"))]
        let can_prepare_hand_over = socket_write.can_prepare_hand_over();

        let connection = Self {
            inner: Arc::new(ConnectionInner {
                socket_status,
                socket_write: Mutex::new(socket_write),
                #[cfg(all(target_os = "linux", feature = "handover"))]
                can_prepare_hand_over,
                server_guid: auth.server_guid,
                #[cfg(unix)]
                cap_unix_fd,
                #[cfg(feature = "p2p")]
                bus_conn: bus_connection,
                unique_name: OnceLock::new(),
                subscriptions,
                #[cfg(feature = "service")]
                object_server: OnceLock::new(),
                #[cfg(feature = "service")]
                object_server_dispatch_task: OnceLock::new(),
                socket_reader_task: OnceLock::new(),
                msg_senders,
                pending_method_calls,
                msg_receiver,
                registered_names: Mutex::new(HashMap::new()),
                runtime,
                drop_event: Event::new(),
                method_timeout,
                credentials: auth
                    .peer_credentials
                    .map(|credentials| OnceLock::from(Arc::new(credentials)))
                    .unwrap_or_default(),
            }),
        };

        if let Some(unique_name) = auth.unique_name {
            connection.set_unique_name_(unique_name);
        }

        Ok(connection)
    }

    /// Create a `Connection` to the session/user message bus.
    pub async fn session() -> Result<Self> {
        Builder::session().build().await
    }

    /// Create a `Connection` to the system-wide message bus.
    pub async fn system() -> Result<Self> {
        Builder::system().build().await
    }

    /// A listener notified on the next activity on the connection.
    ///
    /// This function is meant for the caller to implement idle or timeout on inactivity.
    /// [`ActivityListener`] says what counts as activity, and what a notification does and does
    /// not tell.
    pub fn monitor_activity(&self) -> ActivityListener {
        ActivityListener(self.inner.socket_status.activity_event.listen())
    }

    /// Returns `true` if the connection has been closed.
    ///
    /// A connection is considered closed either when the remote peer disconnects, an I/O error
    /// occurs on the socket, or [`Connection::close`] is called.
    ///
    /// With the `handover` feature, on Linux, it is also closed once it was handed over
    /// (`MessageStream::hand_over`).
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Waits until the connection is closed.
    ///
    /// A connection is considered closed either when the remote peer disconnects, an I/O error
    /// occurs on the socket, or [`Connection::close`] is called.
    ///
    /// With the `handover` feature, on Linux, it is also closed once it was handed over
    /// (`MessageStream::hand_over`).
    ///
    /// Returns immediately if the connection is already closed.
    pub async fn closed(&self) {
        loop {
            let listener = self.inner.socket_status.closed_event.listen();
            if self.inner.socket_status.closed.load(Ordering::Acquire) {
                return;
            }
            listener.await;
        }
    }

    /// Return the peer credentials.
    ///
    /// The fields are populated on the best effort basis. Some or all fields may not even make
    /// sense for certain sockets or on certain platforms and hence will be set to `None`.
    ///
    /// This method caches the credentials on the first call for you. The server side of a
    /// peer-to-peer connection has them from its handshake already. Once they are known, this
    /// method doesn't wait for the socket, which a send to a peer that doesn't read can take long
    /// to be done with.
    ///
    /// # Caveats
    ///
    /// Currently `linux_security_label` field is not populated.
    pub async fn peer_creds(&self) -> io::Result<&Arc<ConnectionCredentials>> {
        if let Some(creds) = self.inner.credentials.get() {
            return Ok(creds);
        }
        let mut socket_write = self.inner.socket_write.lock().await;

        // Keeping the `socket_write` lock guard ensures that this isn't racy.
        if let Some(creds) = self.inner.credentials.get() {
            return Ok(creds);
        }

        self.inner
            .credentials
            .set(socket_write.peer_credentials().await.map(Arc::new)?)
            .expect("credentials cache set more than once");

        Ok(self
            .inner
            .credentials
            .get()
            .expect("credentials should have been set"))
    }

    /// Close the connection.
    ///
    /// After this call, all reading and writing operations will fail. What the connection hasn't
    /// written yet ([`Connection::unwritten_len`]) is let go of. Call [`Connection::flush`] first
    /// to have it written.
    ///
    /// With the `handover` feature, on Linux, this does nothing to the socket of a connection that
    /// was handed over (`MessageStream::hand_over`), as the socket is no longer the connection's.
    pub async fn close(self) -> Result<()> {
        self.inner.socket_status.activity_event.notify(usize::MAX);
        let result = self
            .inner
            .socket_write
            .lock()
            .await
            .close()
            .await
            .map_err(Into::into);
        self.inner
            .socket_status
            .closed
            .store(true, Ordering::Release);
        self.inner.socket_status.closed_event.notify(usize::MAX);
        result
    }

    /// Gracefully close the connection, waiting for all other references to be dropped.
    ///
    /// This will not disrupt any incoming or outgoing method calls, and will await their
    /// completion.
    ///
    /// # Caveats
    ///
    /// * This will not prevent new incoming messages from keeping the connection alive (and
    ///   indefinitely delaying this method's completion).
    ///
    /// * The shutdown will not complete until the underlying connection is fully dropped, so beware
    ///   of deadlocks if you are holding any other clones of this `Connection`.
    ///
    /// # Example
    ///
    /// ```
    /// # #[cfg(feature = "service")]
    /// # use std::error::Error;
    /// # #[cfg(feature = "service")]
    /// # use zbus::connection::Builder;
    /// # #[cfg(feature = "service")]
    /// # use zbus::interface;
    /// #
    /// # #[cfg(feature = "service")]
    /// # struct MyInterface;
    /// #
    /// # #[cfg(feature = "service")]
    /// # #[interface(name = "foo.bar.baz")]
    /// # impl MyInterface {
    /// #     async fn do_thing(&self) {}
    /// # }
    /// #
    /// # // The example serves an interface, which needs the `service` feature.
    /// # #[cfg(not(feature = "service"))]
    /// # fn main() {}
    /// # #[cfg(feature = "service")]
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn Error>> {
    /// let conn = Builder::session()
    ///     .name("foo.bar.baz")
    ///     .serve_at("/foo/bar/baz", MyInterface)
    ///     .build()
    ///     .await?;
    ///
    /// # let some_exit_condition = std::future::ready(());
    /// some_exit_condition.await;
    ///
    /// conn.graceful_shutdown().await;
    /// #
    /// # Ok(())
    /// # }
    /// ```
    pub async fn graceful_shutdown(self) {
        let listener = self.inner.drop_event.listen();
        drop(self);
        listener.await;
    }

    pub(crate) fn init_socket_reader(
        &self,
        socket_read: Box<dyn socket::ReadHalf>,
        already_read: Vec<u8>,
        #[cfg(unix)] already_received_fds: Vec<std::os::fd::OwnedFd>,
        #[cfg(feature = "bus-impl")] validate_bodies: bool,
        #[cfg(all(target_os = "linux", feature = "handover"))] taken_over: Vec<Message>,
    ) {
        let inner = &self.inner;
        #[cfg(all(target_os = "linux", feature = "handover"))]
        inner
            .socket_status
            .stop
            .can_stop
            .store(socket_read.can_take_unread(), Ordering::Release);
        let reader = SocketReader::new(
            socket_read,
            inner.msg_senders.clone(),
            inner.pending_method_calls.clone(),
            already_read,
            #[cfg(unix)]
            already_received_fds,
            inner.socket_status.clone(),
            #[cfg(feature = "bus-impl")]
            validate_bodies,
        );
        #[cfg(all(target_os = "linux", feature = "handover"))]
        let reader = reader.yielding_first(taken_over);
        inner
            .socket_reader_task
            .set(reader.spawn(&inner.runtime))
            .expect("Attempted to set `socket_reader_task` twice");
    }

    fn set_unique_name_(&self, name: OwnedUniqueName) {
        self.inner
            .unique_name
            .set(name)
            // programmer (probably our) error if this fails.
            .expect("unique name already set");
    }
}

// Internal API that allows keeping a weak connection ref around.
#[derive(Debug, Clone)]
pub(crate) struct WeakConnection {
    inner: Weak<ConnectionInner>,
}

impl WeakConnection {
    /// Upgrade to a Connection.
    pub fn upgrade(&self) -> Option<Connection> {
        self.inner.upgrade().map(|inner| Connection { inner })
    }
}

impl From<&Connection> for WeakConnection {
    fn from(conn: &Connection) -> Self {
        Self {
            inner: Arc::downgrade(&conn.inner),
        }
    }
}

#[derive(Debug)]
enum NameStatus {
    // The task waits for name lost signal if owner allows replacement.
    Owner(#[allow(unused)] Option<Task<()>>),
    // The task waits for name acquisition signal.
    Queued(#[allow(unused)] Task<()>),
}

#[cfg(test)]
mod tests {
    #[cfg(all(feature = "service", any(feature = "default-rt", feature = "tokio")))]
    use super::*;
    #[cfg(all(
        feature = "proxy",
        feature = "service",
        any(feature = "default-rt", feature = "tokio")
    ))]
    use crate::fdo::DBusProxy;
    use crate::runtime::io::tests::RefusedPort;
    #[cfg(all(feature = "service", any(feature = "default-rt", feature = "tokio")))]
    use ntest::timeout;
    #[cfg(all(feature = "service", any(feature = "default-rt", feature = "tokio")))]
    use std::{pin::pin, time::Duration};
    #[cfg(all(
        feature = "proxy",
        feature = "service",
        any(feature = "default-rt", feature = "tokio")
    ))]
    use test_log::test;

    #[cfg(windows)]
    #[test]
    fn connect_autolaunch_session_bus() {
        let addr =
            crate::win32::autolaunch_bus_address().expect("Unable to get session bus address");

        crate::block_on(async {
            addr.connect(&crate::runtime::Runtime::default_for_build()?)
                .await
        })
        .expect("Unable to connect to session bus");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn connect_launchd_session_bus() {
        use crate::address::{Address, Transport, transport::Launchd};
        crate::block_on(async {
            let addr = Address::from(Transport::Launchd(Launchd::new(
                "DBUS_LAUNCHD_SESSION_BUS_SOCKET",
            )));
            addr.connect(&crate::runtime::Runtime::default_for_build()?)
                .await
        })
        .expect("Unable to connect to session bus");
    }

    #[test]
    #[ntest::timeout(15000)]
    fn a_session_connection_over_an_external_runtime() {
        crate::utils::block_on(async {
            let connection = super::Builder::session()
                .runtime(crate::runtime::test_runtime::TestRuntime::new())
                .build()
                .await
                .unwrap();

            connection
                .call_method(
                    Some("org.freedesktop.DBus"),
                    "/org/freedesktop/DBus",
                    Some("org.freedesktop.DBus.Peer"),
                    "Ping",
                    &(),
                )
                .await
                .unwrap();
        });
    }

    #[test]
    #[ntest::timeout(15000)]
    fn a_tcp_host_name_resolves_through_spawn_blocking() {
        let runtime = crate::runtime::test_runtime::TestRuntime::new();
        // A connection to a name tries every address that name resolves to, so the port has to
        // be refused on all of them.
        let refused = RefusedPort::for_host("localhost");
        let address = format!("tcp:host=localhost,port={}", refused.port());

        let error = connection_error(address.as_str(), runtime.clone());

        assert!(
            matches!(&error, crate::Error::Connection(e, _)
                if e.kind() == std::io::ErrorKind::ConnectionRefused),
            "connecting to a closed port reported `{error}` rather than a refused connection",
        );
        // The test runtime connects on blocking work as well: one call for each address the name
        // resolves to, since a refused connection moves on to the next one, and one for the
        // lookup itself.
        let addresses = std::net::ToSocketAddrs::to_socket_addrs(&("localhost", refused.port()))
            .unwrap()
            .count();
        assert_eq!(
            runtime.blocking_calls(),
            addresses + 1,
            "the host name was resolved without asking the runtime for blocking work",
        );
    }

    #[test]
    #[ntest::timeout(15000)]
    fn a_tcp_literal_skips_resolution() {
        let runtime = crate::runtime::test_runtime::TestRuntime::new();
        let refused = RefusedPort::on(std::net::Ipv4Addr::LOCALHOST.into());
        let address = format!("tcp:host=127.0.0.1,port={}", refused.port());

        let error = connection_error(address.as_str(), runtime.clone());

        assert!(
            matches!(&error, crate::Error::Connection(e, _)
                if e.kind() == std::io::ErrorKind::ConnectionRefused),
            "connecting to a closed port reported `{error}` rather than a refused connection",
        );
        // The one call is the connect, which the test runtime makes on blocking work.
        assert_eq!(
            runtime.blocking_calls(),
            1,
            "an address that is already an IP address was handed to the resolver anyway",
        );
    }

    /// The error a connection to `address` over `runtime` fails with.
    fn connection_error(
        address: &str,
        runtime: crate::runtime::test_runtime::TestRuntime,
    ) -> crate::Error {
        crate::utils::block_on(super::Builder::address(address).runtime(runtime).build())
            .unwrap_err()
    }

    #[cfg(all(
        feature = "proxy",
        feature = "service",
        any(feature = "default-rt", feature = "tokio")
    ))]
    #[test]
    #[timeout(15000)]
    fn disconnect_on_drop() {
        // Reproducer for https://github.com/z-galaxy/zbus/issues/308 where setting up the
        // objectserver would cause the connection to not disconnect on drop.
        crate::utils::block_on(test_disconnect_on_drop());
    }

    #[cfg(all(
        feature = "proxy",
        feature = "service",
        any(feature = "default-rt", feature = "tokio")
    ))]
    async fn test_disconnect_on_drop() {
        use futures_lite::StreamExt;

        #[derive(Default)]
        struct MyInterface {}

        #[crate::interface(name = "dev.peelz.FooBar.Baz")]
        impl MyInterface {
            fn do_thing(&self) {}
        }
        let name = "dev.peelz.foobar";
        let connection = Builder::session()
            .name(name)
            .serve_at("/dev/peelz/FooBar", MyInterface::default())
            .build()
            .await
            .unwrap();

        let connection2 = Connection::session().await.unwrap();
        let dbus = DBusProxy::new(&connection2).await.unwrap();
        let mut stream = dbus
            .receive_name_owner_changed_with_args(&[(0, name), (2, "")])
            .await
            .unwrap();

        drop(connection);

        // If the connection is not dropped, this will hang forever.
        stream.next().await.unwrap();

        // Let's still make sure the name is gone.
        let name_has_owner = dbus.name_has_owner(name.try_into().unwrap()).await.unwrap();
        assert!(!name_has_owner);
    }

    #[cfg(all(feature = "service", any(feature = "default-rt", feature = "tokio")))]
    #[tokio::test(start_paused = true)]
    #[timeout(15000)]
    async fn test_graceful_shutdown() {
        // If we have a second reference, it should wait until we drop it.
        let connection = Connection::session().await.unwrap();
        let clone = connection.clone();
        let mut shutdown = pin!(connection.graceful_shutdown());
        // Due to start_paused above, tokio will auto-advance time once the runtime is idle.
        // See https://docs.rs/tokio/latest/tokio/time/fn.pause.html.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(u64::MAX)) => {},
            _ = &mut shutdown => {
                panic!("Graceful shutdown unexpectedly completed");
            }
        }

        drop(clone);
        shutdown.await;

        // An outstanding method call should also be sufficient to keep the connection alive.
        struct GracefulInterface {
            method_called: Event,
            wait_before_return: Option<EventListener>,
            announce_done: Event,
        }

        #[crate::interface(name = "dev.peelz.TestGracefulShutdown")]
        impl GracefulInterface {
            async fn do_thing(&mut self) {
                self.method_called.notify(1);
                if let Some(listener) = self.wait_before_return.take() {
                    listener.await;
                }
                self.announce_done.notify(1);
            }
        }

        let method_called = Event::new();
        let method_called_listener = method_called.listen();

        let trigger_return = Event::new();
        let wait_before_return = Some(trigger_return.listen());

        let announce_done = Event::new();
        let done_listener = announce_done.listen();

        let interface = GracefulInterface {
            method_called,
            wait_before_return,
            announce_done,
        };

        let name = "dev.peelz.TestGracefulShutdown";
        let obj = "/dev/peelz/TestGracefulShutdown";
        let connection = Builder::session()
            .name(name)
            .serve_at(obj, interface)
            .build()
            .await
            .unwrap();

        // Call the method from another connection - it won't return until we tell it to.
        let client_conn = Connection::session().await.unwrap();
        tokio::spawn(async move {
            client_conn
                .call_method(Some(name), obj, Some(name), "DoThing", &())
                .await
                .unwrap();
        });

        // Avoid races - make sure we've actually received the method call before we drop our
        // Connection handle.
        method_called_listener.await;

        let mut shutdown = pin!(connection.graceful_shutdown());
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(u64::MAX)) => {},
            _ = &mut shutdown => {
                // While that method call is outstanding, graceful shutdown should not complete.
                panic!("Graceful shutdown unexpectedly completed");
            }
        }

        // If we let the call complete, then the shutdown should complete eventually.
        trigger_return.notify(1);
        shutdown.await;

        // The method call should have been allowed to finish properly.
        done_listener.await;
    }
}

// Every pipe here is a real socket, which only a backend can create.
#[cfg(feature = "p2p")]
#[cfg(all(test, any(feature = "default-rt", feature = "tokio")))]
mod p2p_tests {
    use crate::wire::{Endian, NATIVE_ENDIAN};
    use futures_util::TryStreamExt;
    use ntest::timeout;
    use test_log::test;
    use zruntime::Event;

    use super::{Builder, Connection, socket};
    #[cfg(all(unix, feature = "tokio", feature = "default-rt"))]
    use crate::runtime::Runtime;
    use crate::{Guid, Message, MessageStream, Result, conn::AuthMechanism};

    // Same numbered client and server are already paired up.
    async fn test_p2p(
        server1: Connection,
        client1: Connection,
        server2: Connection,
        client2: Connection,
    ) -> Result<()> {
        let forward1 = {
            let stream = MessageStream::from(server1.clone());
            let sink = client2.clone();

            stream.try_for_each(move |msg| {
                let sink = sink.clone();
                async move { sink.send(&msg).await }
            })
        };
        let forward2 = {
            let stream = MessageStream::from(client2.clone());
            let sink = server1.clone();

            stream.try_for_each(move |msg| {
                let sink = sink.clone();
                async move { sink.send(&msg).await }
            })
        };
        let _forward_task = client1.runtime().spawn("forward_task", async move {
            let _ = futures_util::try_join!(forward1, forward2);
        });

        let server_ready = Event::new();
        let server_ready_listener = server_ready.listen();
        let client_done = Event::new();
        let client_done_listener = client_done.listen();

        let server_future = async move {
            let mut stream = MessageStream::from(&server2);
            server_ready.notify(1);
            let method = loop {
                let m = stream.try_next().await?.unwrap();
                if m.to_string() == "Method call Test" {
                    assert_eq!(m.body().deserialize::<u64>().unwrap(), 64);
                    break m;
                }
            };

            // Send another message first to check the queueing function on client side.
            server2
                .emit_signal(None::<()>, "/", "org.zbus.p2p", "ASignalForYou", &())
                .await?;
            server2.reply(&method.header(), &("yay")).await?;
            client_done_listener.await;

            Ok(())
        };

        let client_future = async move {
            let mut stream = MessageStream::from(&client1);
            server_ready_listener.await;
            // We want to set non-native endian to ensure that:
            // 1. the message is actually encoded with the specified endian.
            // 2. the server side is able to decode it and replies in the same encoding.
            let endian = match NATIVE_ENDIAN {
                Endian::Little => Endian::Big,
                Endian::Big => Endian::Little,
            };
            let method = Message::method_call("/", "Test")
                .interface("org.zbus.p2p")
                .endian(endian)
                .build(&64u64)?;
            client1.send(&method).await?;
            // Check we didn't miss the signal that was sent during the call.
            let m = stream.try_next().await?.unwrap();
            client_done.notify(1);
            assert_eq!(m.to_string(), "Signal ASignalForYou");
            let reply = stream.try_next().await?.unwrap();
            assert_eq!(reply.to_string(), "Method return");
            // Check if the reply was in the non-native endian.
            assert_eq!(Endian::from(reply.primary_header().endian_sig()), endian);
            reply.body().deserialize::<String>()
        };

        let (val, _) = futures_util::try_join!(client_future, server_future,)?;
        assert_eq!(val, "yay");

        Ok(())
    }

    #[test]
    #[timeout(15000)]
    fn tcp_p2p() {
        crate::utils::block_on(test_tcp_p2p()).unwrap();
    }

    async fn test_tcp_p2p() -> Result<()> {
        let (server1, client1) = tcp_p2p_pipe().await?;
        let (server2, client2) = tcp_p2p_pipe().await?;

        test_p2p(server1, client1, server2, client2).await
    }

    async fn tcp_p2p_pipe() -> Result<(Connection, Connection)> {
        let guid = Guid::generate();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let p1 = std::net::TcpStream::connect(addr).unwrap();
        let p0 = listener.incoming().next().unwrap().unwrap();

        futures_util::try_join!(
            Builder::tcp_stream(p0)
                .server(guid)
                .p2p()
                .auth_mechanism(AuthMechanism::Anonymous)
                .build(),
            Builder::tcp_stream(p1).p2p().build(),
        )
    }

    // The server side of a connection keeps the credentials of the client that its handshake
    // looked up, and gives them while a send holds the socket up.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn server_keeps_the_credentials_of_its_handshake() {
        crate::utils::block_on(async {
            // The client comes first.
            let (_client, server) = unix_p2p_pipe().await.unwrap();

            let _sending = server.inner.socket_write.lock().await;
            let credentials = server.peer_creds().await.unwrap();
            assert_eq!(
                credentials.unix_user_id(),
                Some(rustix::process::geteuid().as_raw())
            );
        });
    }

    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn unix_p2p() {
        crate::utils::block_on(test_unix_p2p()).unwrap();
    }

    #[cfg(unix)]
    async fn test_unix_p2p() -> Result<()> {
        let (server1, client1) = unix_p2p_pipe().await?;
        let (server2, client2) = unix_p2p_pipe().await?;

        test_p2p(server1, client1, server2, client2).await
    }

    #[cfg(unix)]
    async fn unix_p2p_pipe() -> Result<(Connection, Connection)> {
        use std::os::unix::net::UnixStream;

        let guid = Guid::generate();

        let (p0, p1) = UnixStream::pair().unwrap();

        futures_util::try_join!(
            Builder::unix_stream(p1).p2p().build(),
            Builder::unix_stream(p0).server(guid).p2p().build(),
        )
    }

    // A connection built outside every Tokio context lands on zruntime, even where Tokio is
    // compiled in, and carries a whole peer-to-peer conversation there. The driver is
    // `futures_lite`, which leaves the calling thread free of a Tokio context;
    // `crate::utils::block_on` would establish one on this build and so send the builder to the
    // Tokio arm instead. The guard on the spawned task is that runtime's own timer, so the task
    // and the timer are both under test here.
    #[cfg(all(unix, feature = "tokio", feature = "default-rt"))]
    #[test]
    #[timeout(15000)]
    fn unix_p2p_zruntime_backend() {
        use futures_lite::FutureExt;
        use std::time::Duration;

        futures_lite::future::block_on(async {
            let (server1, client1) = unix_p2p_pipe().await.unwrap();
            assert!(matches!(server1.runtime(), Runtime::ZRuntime(_)));
            assert!(matches!(client1.runtime(), Runtime::ZRuntime(_)));

            server1
                .runtime()
                .spawn("a task outside every Tokio context", async {
                    assert!(
                        tokio::runtime::Handle::try_current().is_err(),
                        "the task unexpectedly entered a Tokio runtime",
                    );
                })
                .or(async {
                    client1.runtime().sleep(Duration::from_secs(5)).await;
                    panic!("the spawned task did not run");
                })
                .await
                .unwrap();

            let (server2, client2) = unix_p2p_pipe().await.unwrap();

            test_p2p(server1, client1, server2, client2).await.unwrap();
        });
    }

    #[test]
    #[timeout(15000)]
    fn channel_pair() {
        crate::utils::block_on(test_channel_pair()).unwrap();
    }

    async fn test_channel_pair() -> Result<()> {
        let (server1, client1) = create_channel_pair().await;
        let (server2, client2) = create_channel_pair().await;

        test_p2p(server1, client1, server2, client2).await
    }

    async fn create_channel_pair() -> (Connection, Connection) {
        let (a, b) = socket::Channel::pair();

        let guid = crate::Guid::generate();
        let conn1 = Builder::authenticated_socket(a, guid.clone())
            .p2p()
            .build()
            .await
            .unwrap();
        let conn2 = Builder::authenticated_socket(b, guid)
            .p2p()
            .build()
            .await
            .unwrap();

        (conn1, conn2)
    }

    /// The messages of a `send_all` reach the peer in order, each with its own file descriptors,
    /// over a unix socket, whose write half sends several at once, and over a channel, whose write
    /// half sends them one by one.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn send_all() {
        crate::utils::block_on(async {
            let (server, client) = unix_p2p_pipe().await?;
            assert_sent_all(&client, &server).await?;
            let (server, client) = create_channel_pair().await;
            assert_sent_all(&client, &server).await
        })
        .unwrap();
    }

    /// Has `sender` send 20 messages at once, every seventh of which, from the fourth on, carries
    /// a file descriptor, and checks that `receiver` gets them as they were sent.
    #[cfg(unix)]
    async fn assert_sent_all(sender: &Connection, receiver: &Connection) -> Result<()> {
        let mut stream = MessageStream::from(receiver);
        let stdout = std::io::stdout();
        let msgs = (0..20u32)
            .map(|i| {
                let builder = Message::signal("/", "org.zbus.p2p", "Batch");
                if i % 7 == 3 {
                    builder.build(&(i, crate::Fd::from(&stdout)))
                } else {
                    builder.build(&i)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        sender.send_all(&msgs).await?;

        for sent in &msgs {
            let received = stream.try_next().await?.unwrap();
            assert_eq!(
                received.primary_header().serial_num(),
                sent.primary_header().serial_num(),
            );
            assert_eq!(received.data().fds().len(), sent.data().fds().len());
        }

        Ok(())
    }

    /// A `send_all` over a connection that can't pass file descriptors fails if any of its
    /// messages carries some, before it sends anything.
    #[cfg(unix)]
    #[test]
    #[timeout(15000)]
    fn send_all_of_fds_without_fd_passing() {
        crate::utils::block_on(async {
            let (server, client) = tcp_p2p_pipe().await?;
            let mut stream = MessageStream::from(&server);
            let stdout = std::io::stdout();
            let msgs = [
                Message::signal("/", "org.zbus.p2p", "First").build(&())?,
                Message::signal("/", "org.zbus.p2p", "WithFd").build(&crate::Fd::from(&stdout))?,
            ];

            let error = client.send_all(&msgs).await.unwrap_err();
            assert!(
                matches!(error, crate::Error::Unsupported),
                "the messages failed with `{error}` rather than as unsupported",
            );

            // Nothing went out: the next message is the first one that the peer gets.
            let next = Message::signal("/", "org.zbus.p2p", "Next").build(&())?;
            client.send_all(std::slice::from_ref(&next)).await?;
            let received = stream.try_next().await?.unwrap();
            assert_eq!(
                received.primary_header().serial_num(),
                next.primary_header().serial_num(),
            );

            Result::Ok(())
        })
        .unwrap();
    }

    /// `send_all` hands its messages to the write half of the socket all at once.
    #[test]
    #[timeout(15000)]
    fn send_all_hands_the_messages_over_at_once() {
        /// A write half that only takes several messages at a time.
        #[derive(Debug)]
        struct AtOnce(socket::channel::Writer);

        #[async_trait::async_trait]
        impl socket::WriteHalf for AtOnce {
            async fn send_message(&mut self, _msg: &Message) -> Result<()> {
                panic!("a message was handed over on its own");
            }

            async fn send_messages(&mut self, msgs: &[Message]) -> Result<()> {
                for msg in msgs {
                    self.0.send_message(msg).await?;
                }

                Ok(())
            }

            async fn close(&mut self) -> std::io::Result<()> {
                self.0.close().await
            }
        }

        crate::utils::block_on(async {
            let (a, b) = socket::Channel::pair();
            let (read, write) = socket::Socket::split(a).take();
            let a = socket::Split::new(
                Box::new(read) as Box<dyn socket::ReadHalf>,
                Box::new(AtOnce(write)) as Box<dyn socket::WriteHalf>,
            );
            let guid = Guid::generate();
            let sender = Builder::authenticated_socket(a, guid.clone())
                .p2p()
                .build()
                .await?;
            let receiver = Builder::authenticated_socket(b, guid).p2p().build().await?;
            let mut stream = MessageStream::from(&receiver);
            let msgs = [
                Message::signal("/", "org.zbus.p2p", "First").build(&())?,
                Message::signal("/", "org.zbus.p2p", "Second").build(&())?,
            ];

            sender.send_all(&msgs).await?;

            for sent in &msgs {
                let received = stream.try_next().await?.unwrap();
                assert_eq!(
                    received.primary_header().serial_num(),
                    sent.primary_header().serial_num(),
                );
            }

            Result::Ok(())
        })
        .unwrap();
    }
}
