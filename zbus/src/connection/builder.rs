#[cfg(feature = "service")]
use std::collections::HashMap;
#[cfg(all(target_os = "linux", feature = "handover"))]
use std::os::fd::OwnedFd;
use std::{collections::HashSet, mem, num::NonZeroUsize, vec};
#[cfg(all(target_os = "linux", feature = "handover"))]
use std::{fmt, num::NonZeroU32};
#[cfg(feature = "service")]
use zruntime::Event;
use zruntime::broadcast::Receiver as ActiveReceiver;

// The stream constructors take an owned socket of the platform's own type, whichever runtime the
// connection ends up on.
#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(windows)]
use uds_windows::UnixStream;

#[cfg(any(feature = "bus-impl", feature = "handover"))]
use crate::MessageStream;
#[cfg(all(target_os = "linux", feature = "handover"))]
use crate::runtime::IoSource;
#[cfg(any(unix, windows))]
use crate::runtime::io::UnixOps;
use crate::{
    Connection, Error, Guid, OwnedGuid, Result,
    address::Address,
    fdo::RequestNameFlags,
    message::Message,
    names::WellKnownName,
    runtime::{
        Runtime,
        io::{TcpOps, registered},
        traits,
    },
};
#[cfg(feature = "service")]
use crate::{
    ObjectPath,
    names::InterfaceName,
    object_server::{ArcInterface, Interface},
};
#[cfg(all(target_os = "linux", feature = "handover"))]
use crate::{message::header::raise_next_serial, names::OwnedUniqueName};

#[cfg(all(target_os = "linux", feature = "handover"))]
use super::handover::SocketKind;
use super::{
    DEFAULT_MAX_QUEUED,
    handshake::{AuthMechanism, Authenticated},
    socket::{BoxedSplit, ReadHalf, Split, WriteHalf, Writes},
};
#[cfg(all(target_os = "linux", feature = "handover"))]
use super::{Handover, handover::HandoverSocket, socket_write::Unwritten};

#[derive(Debug)]
enum Target {
    #[cfg(any(unix, windows))]
    UnixStream(UnixStream),
    TcpStream(std::net::TcpStream),
    Address(Address),
    Socket(Split<Box<dyn ReadHalf>, Box<dyn WriteHalf>>),
    AuthenticatedSocket(Split<Box<dyn ReadHalf>, Box<dyn WriteHalf>>),
}

#[cfg(feature = "service")]
type Interfaces<'a> = HashMap<ObjectPath<'a>, HashMap<InterfaceName<'static>, ArcInterface>>;

/// A builder for [`zbus::Connection`].
///
/// The builder allows setting the flags [`RequestNameFlags::ALLOW_REPLACEMENT`] and
/// [`RequestNameFlags::REPLACE_EXISTING`] when requesting names, but the flag
/// [`RequestNameFlags::DO_NOT_QUEUE`] will always be enabled. The reasons are:
///
/// 1. There is no indication given to the caller of [`Self::build`] that the name(s) request was
///    enqueued and that the requested name might not be available right after building.
#[cfg_attr(
    feature = "proxy",
    doc = "2. The name may be acquired in between the time the name is requested and the",
    doc = "   [`crate::fdo::NameAcquiredStream`] is constructed. As a result the service can miss",
    doc = "   the [`crate::fdo::NameAcquired`] signal."
)]
#[cfg_attr(
    not(feature = "proxy"),
    doc = "2. The name may be acquired in between the time the name is requested and the",
    doc = "   `fdo::NameAcquiredStream` (requires the `proxy` feature) is constructed. As a result",
    doc = "   the service can miss the `NameAcquired` signal."
)]
///
/// The constructors and setters take the same loosely typed values as the rest of the API and
/// convert them right away, but they never fail: the first error one hits is recorded — a later
/// call doesn't clear it — and reported by [`Builder::build`].
#[derive(Debug)]
#[must_use]
pub struct Builder<'a> {
    // `None` only when a constructor recorded an error instead of working out a target.
    target: Option<Target>,
    max_queued: Option<NonZeroUsize>,
    // This is only set for p2p server case or pre-authenticated sockets.
    guid: Option<Guid<'a>>,
    #[cfg(feature = "p2p")]
    p2p: bool,
    #[cfg(feature = "service")]
    interfaces: Interfaces<'a>,
    names: HashSet<WellKnownName<'a>>,
    auth_mechanism: Option<AuthMechanism>,
    #[cfg(feature = "bus-impl")]
    unique_name: Option<crate::names::UniqueName<'a>>,
    #[cfg(feature = "bus-impl")]
    validate_bodies: bool,
    request_name_flags: RequestNameFlags,
    method_timeout: Option<std::time::Duration>,
    user_id: Option<u32>,
    // `None` unless the caller picked a runtime, in which case the connection runs on that one
    // instead of the default for the build.
    runtime: Option<Runtime>,
    // How the connection writes the socket that it runs over, once connected, if zbus made it.
    writes: Option<Writes>,
    // The socket that the connection runs over, once connected, if zbus drives it and so can give
    // a descriptor of it for a hand-over.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    io_source: Option<(IoSource, SocketKind)>,
    // What a connection that was handed over leaves to the one that takes it over, if this is
    // the builder of that one.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    taken_over: Option<TakenOver>,
    error: Option<Error>,
}

impl<'a> Builder<'a> {
    /// Create a builder for the session/user message bus connection.
    ///
    /// A failure to find the session bus address is reported by [`Builder::build`].
    pub fn session() -> Self {
        match Address::session() {
            Ok(address) => Self::new(Target::Address(address)),
            Err(e) => Self::with_error(e),
        }
    }

    /// Create a builder for the system-wide message bus connection.
    ///
    /// A failure to find the system bus address is reported by [`Builder::build`].
    pub fn system() -> Self {
        match Address::system() {
            Ok(address) => Self::new(Target::Address(address)),
            Err(e) => Self::with_error(e),
        }
    }

    /// Create a builder for an IBus connection.
    ///
    /// IBus (Intelligent Input Bus) is an input method framework. This method creates a builder
    /// that will query the IBus daemon for its D-Bus address using the `ibus address` command.
    ///
    /// This method is only available when the `ibus` feature is enabled.
    ///
    /// # Platform Support
    ///
    /// This method is available on Unix-like systems where IBus is installed.
    ///
    /// # Errors
    ///
    /// [`Builder::build`] returns an error if:
    /// - The `ibus` command is not found or fails to execute
    /// - The IBus daemon is not running
    /// - The command output cannot be parsed as a valid D-Bus address
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::error::Error;
    /// # use zbus::connection::Builder;
    /// # use zbus::block_on;
    /// #
    /// # block_on(async {
    /// let conn = Builder::ibus()
    ///     .build()
    ///     .await?;
    ///
    /// // Use the connection to interact with IBus services
    /// # drop(conn);
    /// # Ok::<(), zbus::Error>(())
    /// # }).unwrap();
    /// #
    /// # Ok::<_, Box<dyn Error + Send + Sync>>(())
    /// ```
    #[cfg(all(unix, feature = "ibus"))]
    pub fn ibus() -> Self {
        use crate::address::transport::{Ibus, Transport};

        Self::new(Target::Address(Address::from(Transport::Ibus(Ibus::new()))))
    }

    /// Create a builder for a connection that will use the given [D-Bus bus address].
    ///
    /// # Example
    ///
    /// Here is an example of connecting to an IBus service:
    ///
    /// ```no_run
    /// # use std::error::Error;
    /// # use zbus::connection::Builder;
    /// # use zbus::block_on;
    /// #
    /// # block_on(async {
    /// let addr = "unix:\
    ///     path=/home/zeenix/.cache/ibus/dbus-ET0Xzrk9,\
    ///     guid=fdd08e811a6c7ebe1fef0d9e647230da";
    /// let conn = Builder::address(addr)
    ///     .build()
    ///     .await?;
    ///
    /// // Do something useful with `conn`..
    /// #     drop(conn);
    /// #     Ok::<(), zbus::Error>(())
    /// # }).unwrap();
    /// #
    /// # Ok::<_, Box<dyn Error + Send + Sync>>(())
    /// ```
    ///
    /// **Note:** The IBus address is different for each session. You can find the address for your
    /// current session using `ibus address` command. For a more convenient way to connect to IBus,
    /// see `Builder::ibus`, available with the `ibus` feature.
    ///
    /// An invalid address is reported by [`Builder::build`].
    ///
    /// [D-Bus bus address]: https://dbus.freedesktop.org/doc/dbus-specification.html#addresses
    pub fn address<A>(address: A) -> Self
    where
        A: TryInto<Address>,
        A::Error: Into<Error>,
    {
        match address.try_into() {
            Ok(address) => Self::new(Target::Address(address)),
            Err(e) => Self::with_error(e.into()),
        }
    }

    /// Create a builder for a connection over `stream`.
    ///
    /// The stream is a [`std::os::unix::net::UnixStream`] (or [`uds_windows::UnixStream`] on
    /// Windows), and the connection takes ownership of it: it is switched to non-blocking mode
    /// and driven by the runtime the connection is built on. A stream of another kind is handed
    /// over as the socket it wraps, which is `into_std()` for a Tokio stream.
    ///
    /// [`uds_windows::UnixStream`]: https://docs.rs/uds_windows/latest/uds_windows/struct.UnixStream.html
    #[cfg(any(unix, windows))]
    pub fn unix_stream(stream: UnixStream) -> Self {
        Self::new(Target::UnixStream(stream))
    }

    /// Create a builder for a connection over `stream`.
    ///
    /// The stream is a [`std::net::TcpStream`], and the connection takes ownership of it: it is
    /// switched to non-blocking mode and driven by the runtime the connection is built on. A
    /// stream of another kind is handed over as the socket it wraps, which is `into_std()` for a
    /// Tokio stream.
    pub fn tcp_stream(stream: std::net::TcpStream) -> Self {
        Self::new(Target::TcpStream(stream))
    }

    /// Create a builder for a connection that will use the given socket.
    ///
    /// See [`Socket`] for when to implement a socket of your own, and how.
    ///
    /// [`Socket`]: crate::connection::Socket
    pub fn socket<S: Into<BoxedSplit>>(socket: S) -> Self {
        Self::new(Target::Socket(socket.into()))
    }

    /// Create a builder for a connection that will use the given pre-authenticated socket.
    ///
    /// This is similar to [`Builder::socket`], except that the socket is either already
    /// authenticated or does not require authentication.
    ///
    /// With the `handover` feature, on Linux, to go on with a connection that was handed over, use
    /// `Builder::take_over` instead, which also takes what that connection left unread.
    ///
    /// An invalid GUID is reported by [`Builder::build`].
    pub fn authenticated_socket<S, G>(socket: S, guid: G) -> Self
    where
        S: Into<BoxedSplit>,
        G: TryInto<Guid<'a>>,
        G::Error: Into<Error>,
    {
        let mut builder = Self::new(Target::AuthenticatedSocket(socket.into()));
        match guid.try_into() {
            Ok(guid) => builder.guid = Some(guid),
            Err(e) => builder.record(e.into()),
        }

        builder
    }

    /// A builder for the connection that goes on from `handover`, whichever of
    /// [`MessageStream::hand_over`] or [`Restore::restore`] gave it, in this process or in another
    /// one.
    ///
    /// The connection runs over the socket of `handover`. It has the GUID, the unique name, the
    /// passing of file descriptors and the kind (peer-to-peer or bus) of the connection that was
    /// handed over. It goes through no authentication handshake, and a connection to a bus says no
    /// `Hello`.
    ///
    /// This method is only available on Linux, with the `handover` feature.
    ///
    /// It yields the messages that the other connection left unread first: those that were queued
    /// on its stream, as they were, then those of what it read off the socket after them, and then
    /// those that it reads off the socket itself, from where the other one stopped. With the
    /// `bus-impl` feature, `Builder::validate_bodies` has it check the bodies of all of them. It
    /// yields them as soon as it is built, so a [`MessageStream`] made from it once it is built can
    /// miss them. Build it with [`Builder::build_message_stream`] to get them all, on a stream that
    /// it can be handed over through in turn. The interfaces that `Builder::serve_at` (which
    /// requires the `service` feature) adds get the method calls among them.
    ///
    /// What the other connection took to send and didn't write, if anything, it writes ahead of
    /// its own sends: at its first send, or at [`Connection::flush`], and not before.
    /// [`Connection::unwritten_len`] counts it from the start. Call [`Connection::flush`] once the
    /// connection is built, bounded by a timeout, as the flush waits for the peer to read: until
    /// the connection sends, nothing else writes what the other one left, and the peer may be
    /// waiting for the rest of a message.
    ///
    /// Once the connection is built, the messages that this process builds get serial numbers
    /// from the one that the other connection was to give its next message on, unless they
    /// already would. So the replies to method calls made before the hand-over aren't taken for
    /// those of calls made since.
    ///
    /// `Builder::server`, `Builder::auth_mechanism` and `Builder::user_id` have no effect on the
    /// connection here, as there is no handshake, though an invalid GUID given to
    /// `Builder::server` still fails the build. `Builder::unique_name`, if called, replaces the
    /// name of `handover`. Everything else is up to the builder, as usual. Like a [`Handover`], the
    /// builder closes the socket when it is dropped, and so does a build that fails or whose future
    /// is dropped.
    ///
    /// With the `p2p` feature, [`Builder::build`] fails with [`Error::Unsupported`] for a hand-over
    /// of a connection to a bus that `Builder::p2p` was called for.
    ///
    /// [`Connection::flush`]: crate::Connection::flush
    /// [`Connection::unwritten_len`]: crate::Connection::unwritten_len
    /// [`MessageStream::hand_over`]: crate::MessageStream::hand_over
    /// [`Restore::restore`]: zoter::Restore::restore
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub fn take_over(handover: Handover) -> Self {
        let Handover {
            socket,
            server_guid,
            unique_name,
            unix_fd_passing,
            #[cfg(feature = "p2p")]
            p2p,
            next_serial,
            queued,
            tail,
            tail_fds,
            unwritten,
        } = handover;
        // The handshake is behind the socket, so it is taken as authenticated.
        let target = match socket {
            HandoverSocket::UnixStream(stream) => Target::UnixStream(stream),
            HandoverSocket::TcpStream(stream) => Target::TcpStream(stream),
            HandoverSocket::Socket(split) => Target::AuthenticatedSocket(split),
        };
        let mut builder = Self::new(target);
        #[cfg(feature = "p2p")]
        {
            builder.p2p = p2p;
        }
        builder.taken_over = Some(TakenOver {
            server_guid,
            unique_name,
            unix_fd_passing,
            #[cfg(feature = "p2p")]
            p2p,
            next_serial,
            queued,
            tail,
            tail_fds,
            unwritten,
        });

        builder
    }

    /// Specify the mechanism to use during authentication.
    pub fn auth_mechanism(mut self, auth_mechanism: AuthMechanism) -> Self {
        self.auth_mechanism = Some(auth_mechanism);

        self
    }

    /// Specify the user id during authentication.
    ///
    /// This can be useful when using [`AuthMechanism::External`] with `socat`
    /// to avoid the host decide what uid to use and instead provide one
    /// known to have access rights.
    ///
    /// On WASI, which has no user ID of its own, [`AuthMechanism::External`] needs this: the ID is
    /// that of the user the other end of the socket sees the host as.
    #[cfg(any(unix, target_os = "wasi"))]
    pub fn user_id(mut self, id: u32) -> Self {
        self.user_id = Some(id);

        self
    }

    /// The to-be-created connection will be a peer-to-peer connection.
    ///
    /// This method is only available when the `p2p` feature is enabled.
    #[cfg(feature = "p2p")]
    pub fn p2p(mut self) -> Self {
        self.p2p = true;

        self
    }

    /// The to-be-created connection will be a server using the given GUID.
    ///
    /// The to-be-created connection will wait for incoming client authentication handshake and
    /// negotiation messages, for peer-to-peer communications after successful creation.
    ///
    /// This method is only available when the `p2p` feature is enabled.
    ///
    /// **NOTE:** This method is redundant when using [`Builder::authenticated_socket`] since the
    /// latter already sets the GUID for the connection and zbus doesn't differentiate between a
    /// server and a client connection, except for authentication.
    ///
    /// An invalid GUID is reported by [`Builder::build`].
    #[cfg(feature = "p2p")]
    pub fn server<G>(mut self, guid: G) -> Self
    where
        G: TryInto<Guid<'a>>,
        G::Error: Into<Error>,
    {
        match guid.try_into() {
            Ok(guid) => self.guid = Some(guid),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Set the capacity of the main (unfiltered) queue.
    ///
    /// Since typically you'd want to set this at instantiation time, you can set it through the
    /// builder.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::{error::Error, num::NonZeroUsize};
    /// # use zbus::connection::Builder;
    /// # use zbus::block_on;
    /// #
    /// # block_on(async {
    /// let max_queued = NonZeroUsize::new(30).unwrap();
    /// let conn = Builder::session()
    ///     .max_queued(max_queued)
    ///     .build()
    ///     .await?;
    /// assert_eq!(conn.max_queued(), max_queued);
    ///
    /// #     Ok::<(), zbus::Error>(())
    /// # }).unwrap();
    /// #
    /// // Do something useful with `conn`..
    /// # Ok::<_, Box<dyn Error + Send + Sync>>(())
    /// ```
    pub fn max_queued(mut self, max: NonZeroUsize) -> Self {
        self.max_queued = Some(max);

        self
    }

    /// Register a D-Bus [`Interface`] to be served at a given path.
    ///
    /// This is similar to [`zbus::ObjectServer::at`], except that it allows you to have your
    /// interfaces available immediately after the connection is established. Typically, this is
    /// exactly what you'd want. Also in contrast to [`zbus::ObjectServer::at`], this method will
    /// replace any previously added interface with the same name at the same path.
    ///
    /// Standard interfaces (Peer, Introspectable, Properties) are added on your behalf. If you
    /// attempt to add yours, [`Builder::build()`] will fail.
    ///
    /// An invalid path is reported by [`Builder::build`].
    #[cfg(feature = "service")]
    pub fn serve_at<P, I>(self, path: P, iface: I) -> Self
    where
        I: Interface,
        P: TryInto<ObjectPath<'a>>,
        P::Error: Into<Error>,
    {
        self.serve(path, I::name(), ArcInterface::new(iface))
    }

    /// Register a D-Bus [`Interface`] to be served at a given path, and as a fallback for the
    /// paths below it.
    ///
    /// This is to [`zbus::ObjectServer::at_fallback`] what [`Builder::serve_at`] is to
    /// [`zbus::ObjectServer::at`]: the interface serves `path`, and the paths below it that don't
    /// have an interface of the same name of their own, as soon as the connection is
    /// established. Like [`Builder::serve_at`], this method replaces any previously added
    /// interface with the same name at the same path, whether added as a fallback or not.
    ///
    /// An invalid path is reported by [`Builder::build`].
    #[cfg(feature = "service")]
    pub fn serve_at_fallback<P, I>(self, path: P, iface: I) -> Self
    where
        I: Interface,
        P: TryInto<ObjectPath<'a>>,
        P::Error: Into<Error>,
    {
        self.serve(path, I::name(), ArcInterface::new_fallback(iface))
    }

    /// Register a well-known name for this connection on the bus.
    ///
    /// This is similar to [`zbus::Connection::request_name`], except the name is requested as part
    /// of the connection setup ([`Builder::build`]), immediately after interfaces
    #[cfg_attr(
        feature = "service",
        doc = "registered (through [`Builder::serve_at`]) are advertised. Typically this is"
    )]
    #[cfg_attr(
        not(feature = "service"),
        doc = "registered (through `Builder::serve_at`, which requires the `service` feature) are",
        doc = "advertised. Typically this is"
    )]
    /// exactly what you want.
    ///
    /// The methods [`Builder::allow_name_replacements`] and [`Builder::replace_existing_names`]
    /// allow to set the [`zbus::fdo::RequestNameFlags`] used to request the name.
    ///
    /// An invalid name is reported by [`Builder::build`].
    pub fn name<W>(mut self, well_known_name: W) -> Self
    where
        W: TryInto<WellKnownName<'a>>,
        W::Error: Into<Error>,
    {
        match well_known_name.try_into() {
            Ok(well_known_name) => {
                self.names.insert(well_known_name);
            }
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Whether the [`zbus::fdo::RequestNameFlags::ALLOW_REPLACEMENT`] flag will be set when
    /// requesting names.
    pub fn allow_name_replacements(mut self, allow_replacement: bool) -> Self {
        self.request_name_flags
            .set(RequestNameFlags::ALLOW_REPLACEMENT, allow_replacement);
        self
    }

    /// Whether the [`zbus::fdo::RequestNameFlags::REPLACE_EXISTING`] flag will be set when
    /// requesting names.
    pub fn replace_existing_names(mut self, replace_existing: bool) -> Self {
        self.request_name_flags
            .set(RequestNameFlags::REPLACE_EXISTING, replace_existing);
        self
    }

    /// Set the unique name of the connection.
    ///
    /// This is mainly provided for bus implementations. All other users should not need to use this
    /// method. Hence why this method is only available when the `bus-impl` feature is enabled.
    ///
    /// # Panics
    ///
    /// It will panic if the connection is to a message bus as it's the bus that assigns
    /// peers their unique names.
    ///
    /// An invalid name is reported by [`Builder::build`].
    #[cfg(feature = "bus-impl")]
    pub fn unique_name<U>(mut self, unique_name: U) -> Self
    where
        U: TryInto<crate::names::UniqueName<'a>>,
        U::Error: Into<Error>,
    {
        if !self.p2p {
            panic!("unique name can only be set for peer-to-peer connections");
        }
        match unique_name.try_into() {
            Ok(unique_name) => self.unique_name = Some(unique_name),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Check the body of each message that the connection receives against its signature, as zbus
    /// checks the header of each, and end the connection on a message whose body doesn't hold what
    /// its signature says.
    ///
    /// A bus passes the bodies of the messages of its peers on to their recipients without
    /// deserializing them, so nothing else checks them before the recipients do, some of which end
    /// their connection to the bus on a body that doesn't hold what its signature says. A bus sets
    /// this on the connections of its peers, so as to pass on well-formed messages only, as
    /// dbus-daemon and dbus-broker do. [`Body::validate`] makes the same check of a single body.
    ///
    /// This is mainly provided for bus implementations, and hence only available when the
    /// `bus-impl` feature is enabled.
    ///
    /// [`Body::validate`]: crate::message::Body::validate
    #[cfg(feature = "bus-impl")]
    pub fn validate_bodies(mut self) -> Self {
        self.validate_bodies = true;

        self
    }

    /// Set a timeout for method calls.
    ///
    /// Method calls will return
    /// `zbus::Error::InputOutput(std::io::Error(kind: ErrorKind::TimedOut))` if a client does not
    /// receive an answer from a service in time.
    pub fn method_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.method_timeout = Some(timeout);

        self
    }

    /// Run the connection on `runtime`.
    ///
    /// The connection creates and watches its sockets, takes its timers, runs its internal
    /// tasks and hands off its blocking work on `runtime`. zbus never polls `runtime` itself, so
    /// the tasks the connection spawns only make progress while `runtime` runs them. This is how
    /// an application whose event loop is neither of the two zbus can be compiled with puts a
    /// connection on the one it has.
    ///
    /// Without this, the connection runs on the backend zbus is compiled with: Tokio when the
    /// `tokio` feature is on and a Tokio runtime is current on this thread, otherwise
    /// [zruntime](https://docs.rs/zruntime) (the `default-rt` feature). Where that leaves no
    /// backend, [`Builder::build`] reports [`Error::Unsupported`] unless a runtime is set here: in
    /// a build with neither feature, and in a `tokio` build without `default-rt` on a thread
    /// where no Tokio runtime is current.
    pub fn runtime(mut self, runtime: impl traits::Runtime) -> Self {
        self.runtime = Some(Runtime::from_external(runtime));

        self
    }

    /// Build the connection, consuming the builder.
    ///
    /// # Errors
    ///
    /// Returns the first error recorded by a constructor or setter, then any error from
    /// connecting, authenticating or setting the connection up.
    ///
    /// Until server-side bus connection is supported, attempting to build such a connection will
    /// result in a [`Error::Unsupported`] error.
    pub async fn build(self) -> Result<Connection> {
        let (conn, _) = self.build_inner(false).await?;
        Ok(conn)
    }

    /// Build the connection and return a [`MessageStream`] to receive messages from it.
    ///
    /// This is equivalent to [`Self::build`] followed by `MessageStream::from(&conn)`, except
    /// that the stream is set up **before** the socket-reader task is started. No messages can
    /// therefore be lost in the window between `build()` returning and `MessageStream::from`
    /// being called. Use this when the peer may pipeline traffic right after authentication —
    /// e.g. a bus implementation reading a `Hello` method call from a just-connected client.
    ///
    /// To get the [`Connection`] out of the returned stream, use `Connection::from(&stream)` —
    /// this is cheap (an `Arc` clone).
    ///
    /// With the `handover` feature, on Linux, the stream has had every message of the connection,
    /// from the first, so the connection can be handed over through it
    /// (`MessageStream::hand_over`). A connection that takes over (`Builder::take_over`) yields the
    /// messages that it was handed as soon as it is built. A stream made from it once it is built
    /// can miss them, while one built with this gets every one of them.
    ///
    /// This method is only available when the `bus-impl` or the `handover` feature is enabled.
    ///
    /// # Errors
    ///
    /// The same errors as [`Builder::build`].
    ///
    /// # Example
    ///
    /// ```
    /// # // The example runs over a channel, which needs the `p2p` feature.
    /// # #[cfg(not(feature = "p2p"))]
    /// # fn main() {}
    /// # #[cfg(feature = "p2p")]
    /// # fn main() {
    /// # use futures_util::StreamExt;
    /// # use zbus::{
    /// #     Connection, Guid, block_on,
    /// #     connection::{Builder, socket::Channel},
    /// #     message::Message,
    /// # };
    /// #
    /// # block_on(async {
    /// let guid = Guid::generate();
    /// let (c1, c2) = Channel::pair();
    ///
    /// // Bus client sends a method call right away (simulates pipelining after auth).
    /// let client = Builder::authenticated_socket(c1, guid.clone())
    ///     .build()
    ///     .await
    ///     .unwrap();
    /// let hello = Message::method_call("/org/freedesktop/DBus", "Hello")
    ///     .destination("org.freedesktop.DBus")
    ///     .build(&())
    ///     .unwrap();
    /// client.send(&hello).await.unwrap();
    ///
    /// // Server builds *after* the client has already sent.
    /// let mut stream = Builder::authenticated_socket(c2, guid)
    ///     .p2p()
    ///     .build_message_stream()
    ///     .await
    ///     .unwrap();
    ///
    /// let msg = stream.next().await.unwrap().unwrap();
    /// assert_eq!(msg.header().member().unwrap().as_str(), "Hello");
    ///
    /// let _conn: Connection = (&stream).into();
    /// # });
    /// # }
    /// ```
    #[cfg(any(feature = "bus-impl", feature = "handover"))]
    pub async fn build_message_stream(self) -> Result<MessageStream> {
        let (conn, msg_receiver) = self.build_inner(true).await?;
        let msg_receiver = msg_receiver.expect("build_inner(true) always returns Some");

        Ok(MessageStream::for_subscription_channel(
            msg_receiver,
            None,
            &conn,
        ))
    }

    async fn build_inner(
        mut self,
        activate_msg_stream: bool,
    ) -> Result<(Connection, Option<ActiveReceiver<Result<Message>>>)> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }

        let runtime = self
            .runtime
            .take()
            .map_or_else(Runtime::default_for_build, Ok)?;
        // Box the future as it's large and can cause stack overflow.
        Box::pin(self.build_(runtime, activate_msg_stream)).await
    }

    async fn build_(
        mut self,
        runtime: Runtime,
        activate_msg_stream: bool,
    ) -> Result<(Connection, Option<ActiveReceiver<Result<Message>>>)> {
        #[cfg(feature = "p2p")]
        let is_bus_conn = !self.p2p;
        #[cfg(not(feature = "p2p"))]
        let is_bus_conn = true;

        let mut auth = self.connect(is_bus_conn, &runtime).await?;
        // What is left of a hand-over that the connection takes over, if any: the messages that it
        // yields first, and what it writes first.
        #[cfg(all(target_os = "linux", feature = "handover"))]
        let (taken_over, unwritten) = self
            .taken_over
            .take()
            .map(TakenOver::left)
            .unwrap_or_default();

        // SAFETY: `Authenticated` is always built with these fields set to `Some`.
        let socket_read = auth.socket_read.take().unwrap();
        let already_received_bytes = mem::take(&mut auth.already_received_bytes);
        #[cfg(unix)]
        let already_received_fds = mem::take(&mut auth.already_received_fds);

        let mut conn = Connection::new(
            auth,
            self.writes.take(),
            #[cfg(all(target_os = "linux", feature = "handover"))]
            self.io_source.take(),
            #[cfg(all(target_os = "linux", feature = "handover"))]
            unwritten,
            is_bus_conn,
            runtime,
            self.method_timeout,
        )
        .await?;
        conn.set_max_queued(self.max_queued.unwrap_or(DEFAULT_MAX_QUEUED));

        #[cfg(feature = "service")]
        if !self.interfaces.is_empty() {
            let object_server = conn.ensure_object_server(false);
            for (path, interfaces) in self.interfaces {
                for (name, iface) in interfaces {
                    let added = object_server
                        .add_arc_interface(path.clone(), name.clone(), iface.clone())
                        .await?;
                    if !added {
                        return Err(Error::InterfaceExists(name.clone(), path.to_owned()));
                    }
                }
            }

            let started_event = Event::new();
            let listener = started_event.listen();
            conn.start_object_server(Some(started_event));

            listener.await;
        }

        // Set up a message receiver before the socket-reader task is spawned so that the
        // caller cannot miss early messages due to a race with the reader task.
        let msg_receiver = activate_msg_stream.then(|| conn.inner.msg_receiver.activate_cloned());

        // Start the socket reader task.
        conn.init_socket_reader(
            socket_read,
            already_received_bytes,
            #[cfg(unix)]
            already_received_fds,
            #[cfg(feature = "bus-impl")]
            self.validate_bodies,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            taken_over,
        );

        for name in self.names {
            conn.request_name_with_flags(name, self.request_name_flags)
                .await?;
        }

        Ok((conn, msg_receiver))
    }

    fn new(target: Target) -> Self {
        Self::from_parts(Some(target), None)
    }

    /// Create a builder that has no target to connect to, only the error that kept a constructor
    /// from working one out.
    fn with_error(error: Error) -> Self {
        Self::from_parts(None, Some(error))
    }

    fn from_parts(target: Option<Target>, error: Option<Error>) -> Self {
        Self {
            target,
            #[cfg(feature = "p2p")]
            p2p: false,
            max_queued: None,
            guid: None,
            #[cfg(feature = "service")]
            interfaces: HashMap::new(),
            names: HashSet::new(),
            auth_mechanism: None,
            #[cfg(feature = "bus-impl")]
            unique_name: None,
            #[cfg(feature = "bus-impl")]
            validate_bodies: false,
            request_name_flags: RequestNameFlags::default(),
            method_timeout: None,
            user_id: None,
            runtime: None,
            writes: None,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            io_source: None,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            taken_over: None,
            error,
        }
    }

    /// Add `iface` to the interfaces to serve at `path` under `name`, replacing any interface
    /// already added there under that name.
    #[cfg(feature = "service")]
    fn serve<P>(mut self, path: P, name: InterfaceName<'static>, iface: ArcInterface) -> Self
    where
        P: TryInto<ObjectPath<'a>>,
        P::Error: Into<Error>,
    {
        match path.try_into() {
            Ok(path) => {
                self.interfaces.entry(path).or_default().insert(name, iface);
            }
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Record `error`, unless an earlier constructor or setter already recorded one.
    fn record(&mut self, error: Error) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    async fn connect(&mut self, is_bus_conn: bool, runtime: &Runtime) -> Result<Authenticated> {
        #[cfg(not(feature = "bus-impl"))]
        let unique_name = None;
        #[cfg(feature = "bus-impl")]
        let unique_name = self.unique_name.take().map(Into::into);

        let (mut stream, server_guid, authenticated) = self.target_connect(runtime).await?;
        // Before the halves go their own ways, whichever way the connection is made.
        self.writes = stream.take_writes();
        // Ahead of the authenticated socket that it builds on, as the GUID that comes with it is
        // that of the hand-over, not the one that `target_connect` gives.
        #[cfg(all(target_os = "linux", feature = "handover"))]
        if let Some(taken_over) = &mut self.taken_over {
            return taken_over.authenticated(
                stream,
                unique_name,
                #[cfg(feature = "p2p")]
                self.p2p,
            );
        }
        if authenticated {
            let (socket_read, socket_write) = stream.take();
            Ok(Authenticated {
                #[cfg(unix)]
                cap_unix_fd: socket_read.can_pass_unix_fd(),
                socket_read: Some(socket_read),
                socket_write,
                // SAFETY: `server_guid` is provided as arg of `Builder::authenticated_socket`.
                server_guid: server_guid.unwrap(),
                already_received_bytes: vec![],
                unique_name,
                #[cfg(unix)]
                already_received_fds: vec![],
                peer_credentials: None,
            })
        } else {
            #[cfg(feature = "p2p")]
            match self.guid.take() {
                None => {
                    // SASL Handshake
                    Authenticated::client(
                        stream,
                        server_guid,
                        self.auth_mechanism,
                        is_bus_conn,
                        self.user_id,
                    )
                    .await
                }
                Some(guid) => {
                    if !self.p2p {
                        return Err(Error::Unsupported);
                    }

                    let creds = stream.read_mut().peer_credentials().await?;
                    #[cfg(any(unix, target_os = "wasi"))]
                    let client_uid = self.user_id.or_else(|| creds.unix_user_id());
                    #[cfg(windows)]
                    let client_sid = creds.windows_sid().cloned();

                    let mut auth = Authenticated::server(
                        stream,
                        guid.to_owned().into(),
                        #[cfg(any(unix, target_os = "wasi"))]
                        client_uid,
                        #[cfg(windows)]
                        client_sid,
                        self.auth_mechanism,
                        unique_name,
                    )
                    .await?;
                    // The connection keeps the credentials that authenticated the client, rather
                    // than look them up again.
                    auth.peer_credentials = Some(creds);

                    Ok(auth)
                }
            }

            #[cfg(not(feature = "p2p"))]
            Authenticated::client(
                stream,
                server_guid,
                self.auth_mechanism,
                is_bus_conn,
                self.user_id,
            )
            .await
        }
    }

    async fn target_connect(
        &mut self,
        runtime: &Runtime,
    ) -> Result<(BoxedSplit, Option<OwnedGuid>, bool)> {
        let mut authenticated = false;
        let mut guid = None;
        // SAFETY: `self.target` is `None` only when a constructor recorded an error, which
        // `build` returns before it gets here, and this method is only called once.
        let split = match self.target.take().unwrap() {
            #[cfg(unix)]
            Target::UnixStream(stream) => {
                let socket = registered(runtime, stream, UnixOps)?;
                #[cfg(all(target_os = "linux", feature = "handover"))]
                {
                    self.io_source = Some((socket.source().clone(), SocketKind::Unix));
                }

                socket.into()
            }
            #[cfg(windows)]
            Target::UnixStream(stream) => {
                // `uds_windows` is the one stream a connection is built from that hands its
                // socket out raw only, so the connection takes a duplicate of it and lets the
                // stream close the handle it came with.
                let socket =
                    std::os::windows::io::AsSocket::as_socket(&stream).try_clone_to_owned()?;

                registered(runtime, socket, UnixOps)?.into()
            }
            Target::TcpStream(stream) => {
                let socket = registered(runtime, stream, TcpOps)?;
                #[cfg(all(target_os = "linux", feature = "handover"))]
                {
                    self.io_source = Some((socket.source().clone(), SocketKind::Tcp));
                }

                socket.into()
            }
            Target::Address(address) => {
                guid = address.guid().map(|g| g.to_owned().into());
                let stream = address.connect(runtime).await?;
                #[cfg(all(target_os = "linux", feature = "handover"))]
                {
                    self.io_source = stream.source();
                }

                stream.into_split()
            }
            Target::Socket(stream) => stream,
            Target::AuthenticatedSocket(stream) => {
                authenticated = true;
                guid = self.guid.take().map(Into::into);
                stream
            }
        };

        Ok((split, guid, authenticated))
    }
}

/// What [`Builder::take_over`] keeps of a [`Handover`] for the build: everything but the socket,
/// which is the target of the builder.
#[cfg(all(target_os = "linux", feature = "handover"))]
struct TakenOver {
    server_guid: OwnedGuid,
    unique_name: Option<OwnedUniqueName>,
    unix_fd_passing: bool,
    #[cfg(feature = "p2p")]
    p2p: bool,
    next_serial: NonZeroU32,
    queued: Vec<Message>,
    tail: Vec<u8>,
    tail_fds: Vec<OwnedFd>,
    unwritten: Unwritten,
}

#[cfg(all(target_os = "linux", feature = "handover"))]
impl TakenOver {
    /// The connection over `stream` that goes on from the hand-over, with `unique_name` in place
    /// of that of the hand-over, if any, as the builder says that it is `p2p` or not.
    ///
    /// The read half takes what was read after the messages that were queued ahead of what it
    /// reads. The rest is left for [`TakenOver::left`].
    fn authenticated(
        &mut self,
        stream: BoxedSplit,
        unique_name: Option<OwnedUniqueName>,
        #[cfg(feature = "p2p")] p2p: bool,
    ) -> Result<Authenticated> {
        #[cfg(feature = "p2p")]
        if p2p != self.p2p {
            return Err(Error::Unsupported);
        }
        let (socket_read, socket_write) = stream.take();
        raise_next_serial(self.next_serial);

        Ok(Authenticated {
            cap_unix_fd: self.unix_fd_passing && socket_read.can_pass_unix_fd(),
            socket_read: Some(socket_read),
            socket_write,
            server_guid: self.server_guid.clone(),
            already_received_bytes: mem::take(&mut self.tail),
            already_received_fds: mem::take(&mut self.tail_fds),
            unique_name: unique_name.or_else(|| self.unique_name.take()),
            peer_credentials: None,
        })
    }

    /// What is left of the hand-over for the connection that goes on from it, once it is
    /// authenticated: the messages that were queued, which it yields first, and what was left
    /// unwritten, which it writes first.
    fn left(self) -> (Vec<Message>, Unwritten) {
        (self.queued, self.unwritten)
    }
}

/// Shows how many messages, bytes and file descriptors were left unread and unwritten rather than
/// which, as a `Handover` does.
#[cfg(all(target_os = "linux", feature = "handover"))]
impl fmt::Debug for TakenOver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut taken_over = f.debug_struct("TakenOver");
        taken_over
            .field("server_guid", &self.server_guid)
            .field("unique_name", &self.unique_name)
            .field("unix_fd_passing", &self.unix_fd_passing);
        #[cfg(feature = "p2p")]
        taken_over.field("p2p", &self.p2p);

        taken_over
            .field("next_serial", &self.next_serial)
            .field("queued", &self.queued.len())
            .field("tail", &self.tail.len())
            .field("tail_fds", &self.tail_fds.len())
            .field("unwritten", &self.unwritten.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use test_log::test;

    use super::{Address, Builder};
    #[cfg(any(feature = "default-rt", feature = "tokio"))]
    use crate::Error;
    use crate::{names::WellKnownName, utils::block_on};

    // Syntactically valid, so that the builder records no error for the target itself.
    const ADDRESS: &str = "unix:path=/tmp/zbus-connection-builder-tests";

    #[test]
    fn strings() {
        // An invalid string is only reported by `build`.
        let error = block_on(Builder::address("not an address").build()).unwrap_err();
        assert_eq!(error, Address::try_from("not an address").unwrap_err());

        let error = block_on(Builder::address(ADDRESS).name("not a name").build()).unwrap_err();
        assert_eq!(error, WellKnownName::try_from("not a name").unwrap_err());
    }

    // The build gets as far as connecting, which needs a backend.
    #[cfg(any(feature = "default-rt", feature = "tokio"))]
    #[test]
    fn typed_values() {
        // No `Result` anywhere before `build`.
        let address = Address::try_from(ADDRESS).unwrap();
        let name = WellKnownName::try_from("org.zbus.Test").unwrap();
        let error = block_on(Builder::address(address).name(name).build()).unwrap_err();
        // No setter recorded an error, so the build got as far as connecting, which a Tokio
        // connection on Windows turns down before trying: it has nothing to reach a unix socket
        // with.
        #[cfg(all(windows, feature = "tokio"))]
        assert!(matches!(error, Error::Unsupported));
        #[cfg(not(all(windows, feature = "tokio")))]
        assert!(matches!(error, Error::Connection(..)));
    }

    #[test]
    fn a_later_valid_call_keeps_the_first_error() {
        // Calling the setter again, even with a valid name, doesn't clear the error it recorded.
        let error = block_on(
            Builder::address(ADDRESS)
                .name("not a name")
                .name("org.zbus.Test")
                .build(),
        )
        .unwrap_err();
        assert_eq!(error, WellKnownName::try_from("not a name").unwrap_err());
    }
}
