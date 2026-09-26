# Establishing a connection

<!-- toc -->

The first thing you will have to do is to connect to a D-Bus bus or to a D-Bus peer. This is the
entry point of the zbus API.

## Connection to the bus

To connect to the session bus (the *per-user* bus), simply call `Connection::session()`. It
returns an instance of the connection (if all went well). Similarly, to connect to the system bus
(to communicate with services such as [NetworkManager], [BlueZ] or [PID1]), use
`Connection::system()`.

Moreover, it can be converted to a [`MessageStream`] that implements [`futures::stream::Stream`],
which can be used to conveniently receive messages, for the times when low-level API is
more appropriate for your use case.

**Note:** it is common for a D-Bus library to provide a "shared" connection to a bus for a process:
all `session()` share the same underlying connection for example. At the time of this writing,
zbus doesn't do that.

**Note:** on macOS, there is no standard implicit way to connect to a session bus. zbus provides
opt-in compatibility to the Launchd session bus discovery mechanism via the `launchctl getenv` feature.
The official dbus installation method via `Homebrew` provides a session bus installation,
utilizing macOS `LaunchAgents` feature. By default, zbus consumes an address for a bus connection that
is provided via `launchctl getenv DBUS_LAUNCHD_SESSION_BUS_SOCKET` command output.

## Using a custom bus address

You may also specify a custom bus with [`connection::Builder::address`] which takes a D-Bus address
[as specified in the specification][dspec].

## Peer to peer connection

Peer-to-peer connections are bus-less[^bus-less], and the initial handshake protocol is a bit
different. There is the notion of client & server endpoints, but that distinction doesn't matter
once the connection is established (both ends are equal, and can send any messages).

For example to create a bus-less peer-to-peer connection on Unix, you can do:

```rust,noplayground
# #[tokio::main]
# async fn main() -> zbus::Result<()> {
# #[cfg(unix)]
# {
use std::os::unix::net::UnixStream;
use zbus::{connection::Builder, Guid};

let guid = Guid::generate();
let (p0, p1) = UnixStream::pair().unwrap();
# #[allow(unused)]
let (client_conn, server_conn) = futures_util::try_join!(
    // Client
    Builder::unix_stream(p0).p2p().build(),
    // Server
    Builder::unix_stream(p1).server(guid).p2p().build(),
)?;
# }
#
# Ok(())
# }
```

`unix_stream` takes a [`std::os::unix::net::UnixStream`], whichever runtime the connection ends
up on. A stream of another kind is handed over as the socket it wraps: `into_std()` for a
[`tokio::net::UnixStream`].

[`std::os::unix::net::UnixStream`]: https://doc.rust-lang.org/std/os/unix/net/struct.UnixStream.html
[`tokio::net::UnixStream`]: https://docs.rs/tokio/latest/tokio/net/struct.UnixStream.html

**Note:** the `p2p` and `server` methods of `connection::Builder` are only available when `p2p`
cargo feature of `zbus` is enabled.

## Runtimes

A connection takes its readiness notifications, timers, spawned tasks and blocking work from one
runtime, chosen once when it is built. A program with no async runtime of its own drives it with
`zbus::block_on`; see [the FAQ][cob]. A program built on Tokio turns on the `tokio` feature, and a
connection built while a Tokio runtime is current runs on it. Any other executor can use the
built-in backend below as it is, with no integration needed: a connection built outside a
`zbus::block_on` call is run by that backend's own helper thread. [`Builder::runtime`] is there for
an application that wants to hand a connection a runtime of its own instead, not something every
other executor needs. A connection's async locks are zbus's own, except on a Tokio build, where
Tokio's locks stand in instead.

### Built-in backends

With the `builtin-runtime` cargo feature (a default feature), a connection runs on [zruntime], one
per thread that runs `zbus::block_on`, depending on no general-purpose async executor crate. The
thread that drives it is the one inside `zbus::block_on`: between two polls of the future handed to
it, that thread runs the tasks, sockets and timers of every built-in connection built in it — apart
from a handful of blocking system calls (a DNS lookup, a nonce-file read, a peer-credential lookup)
that use a short-lived worker thread of their own instead. Two threads that each call
`zbus::block_on` drive their own connections, in parallel. A thread's runtime serves every
connection built inside its `zbus::block_on` calls, so a task of one connection that runs long
delays the others' I/O until it yields, and a connection used from another thread is still driven by
the thread that built it. A connection built on a thread that is in no `zbus::block_on` at the time
— one that some other executor polls — has no thread of its own to look to, and goes instead on a
single runtime the whole process shares with every other such connection. Only where a connection
has work and no thread is inside `zbus::block_on` — it is polled from some other executor, or a call
returned with the connection alive — does zbus start a helper thread, which leaves once nothing is
left to run. If the `tokio` feature is also enabled and a Tokio runtime is current on the thread
that builds the connection, it runs on Tokio instead. With only `tokio` enabled, a connection always
runs on the Tokio runtime that is current when it is built; building one from a thread with no such
runtime fails with `Error::Unsupported`.

A build with neither feature depends on no `tokio` crate, and on nothing `builtin-runtime` owns:
zbus's own locks build on `event-listener`, not on anything either feature pulls in, so this
build needs neither an extra feature nor an extra crate for them. Every connection in the build
needs an explicit runtime, given through [`Builder::runtime`].

### Supplying your own runtime

[`Builder::runtime`] takes any implementation of [`traits::Runtime`], for an application whose
event loop is neither backend above:

```rust,no_run
use zbus::{Connection, Result, connection::Builder, runtime::traits::Runtime};

async fn connect(runtime: impl Runtime) -> Result<Connection> {
    Builder::session().runtime(runtime).build().await
}
```

An implementation supplies readiness for a registered socket or pipe (through
[`PollIo::poll_io`]), a timer, and spawning a future to run in the background. Every zbus
operation goes through these, except a handful of calls that have no async form: the host-name
lookup for a `tcp:` address, reading the file a `nonce-tcp:` address names, the
supplementary-group lookup behind a peer-credential check, and waiting for the helper process of
a `unixexec:`, `ibus:` or `launchd:` address to exit. These go through [`spawn_blocking`], whose
default runs each one on a thread of its own that exits once the call returns; a runtime that
keeps a pool of threads for blocking work should override it to use that pool instead. The wait
for a helper process starts once the connection has let go of the pipe it reads that process's
output from, and occupies a worker until the program is gone: no time at all for one that has
already exited, and until its input ends for a `unixexec:` program that is still running — one
that ignores the end of its input keeps that worker.

zbus's integration tests include a reference runtime: a single-threaded one built on the
`polling` crate, whose run loop drives a connection's readiness, timers and tasks without
starting a thread of its own. The build it runs in needs no extra feature for zbus's locks
either; only a Tokio build swaps in Tokio's locks instead.

### Bringing your own socket

Besides an address, a connection can be built directly over a socket you already have.
[`Builder::unix_stream`], [`Builder::tcp_stream`] and [`Builder::vsock_stream`] each take an
owned, platform-native stream and register it on the connection's runtime; a stream of another
kind is handed over as the socket it wraps, such as `into_std()` for a Tokio stream.
[`Builder::socket`] and [`Builder::authenticated_socket`] take any other implementation of
`Socket`, for a transport that is not a file descriptor at all, such as an in-process channel.

[NetworkManager]: https://developer.gnome.org/NetworkManager/stable/spec.html
[BlueZ]: https://git.kernel.org/pub/scm/bluetooth/bluez.git/tree/doc
[PID1]: https://www.freedesktop.org/software/systemd/man/latest/org.freedesktop.systemd1.html
[`futures::stream::Stream`]: https://docs.rs/futures/4/futures/stream/trait.Stream.html
[`MessageStream`]: https://docs.rs/zbus/latest/zbus/struct.MessageStream.html
[`connection::Builder::address`]: https://docs.rs/zbus/latest/zbus/connection/struct.Builder.html#method.address
[dspec]: https://dbus.freedesktop.org/doc/dbus-specification.html#addresses
[cob]: faq.html#how-do-i-use-zbus-from-synchronous-code
[`Builder::runtime`]: https://docs.rs/zbus/latest/zbus/connection/struct.Builder.html#method.runtime
[`traits::Runtime`]: https://docs.rs/zbus/latest/zbus/runtime/traits/trait.Runtime.html
[zruntime]: https://docs.rs/zruntime
[`PollIo::poll_io`]: https://docs.rs/zbus/latest/zbus/runtime/traits/trait.PollIo.html#tymethod.poll_io
[`spawn_blocking`]: https://docs.rs/zbus/latest/zbus/runtime/traits/trait.Runtime.html#method.spawn_blocking
[`Builder::unix_stream`]: https://docs.rs/zbus/latest/zbus/connection/struct.Builder.html#method.unix_stream
[`Builder::tcp_stream`]: https://docs.rs/zbus/latest/zbus/connection/struct.Builder.html#method.tcp_stream
[`Builder::vsock_stream`]: https://docs.rs/zbus/latest/zbus/connection/struct.Builder.html#method.vsock_stream
[`Builder::socket`]: https://docs.rs/zbus/latest/zbus/connection/struct.Builder.html#method.socket
[`Builder::authenticated_socket`]: https://docs.rs/zbus/latest/zbus/connection/struct.Builder.html#method.authenticated_socket

[^bus-less]: Unless you implemented them, none of the bus methods will exist.
