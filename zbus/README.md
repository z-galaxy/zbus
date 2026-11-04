# zbus

[![](https://docs.rs/zbus/badge.svg)](https://docs.rs/zbus/) [![](https://img.shields.io/crates/v/zbus)](https://crates.io/crates/zbus)

This is the main subcrate of the [zbus] project, that provides the API to interact with D-Bus. It
takes care of the establishment of a connection, the creation, sending and receiving of different
kind of D-Bus messages (method calls, signals etc) for you.

**Status:** Stable.

## Getting Started

The best way to get started with zbus is the [book](https://z-galaxy.github.io/zbus/), where we start
with basic D-Bus concepts and explain with code samples, how zbus makes D-Bus easy.

## Wire format only

If all you need is the D-Bus wire format (the [serde]-based encoding that used to be the
`zvariant` crate) and the bus name types (that used to be `zbus_names`), and no connection at
all, disable the default features:

```toml
[dependencies]
zbus = { version = "6", default-features = false }
```

That build compiles `zbus::wire` and `zbus::names` and nothing else — no connection, proxy or
object server. The optional wire-format features keep zvariant's names (`arrayvec`, `camino`,
`chrono`, `heapless`, `option-as-array`, `serde_bytes`, `time`, `url`, `uuid`), and enabling any
D-Bus feature (`comms`, `default-rt`, `tokio`, `tokio-multithread`, `p2p`, `bus-impl`, `handover`,
`proxy`, `service`, `object-manager`, `unixexec`, `ibus`) brings the D-Bus API back.

zbus logs through [`tracing`], behind the default `tracing` feature; a `default-features =
false` build that wants zbus's logs must re-enable it explicitly.

## Example code

We'll create a simple D-Bus service and client to demonstrate the usage of zbus. Note that these
examples assume that a D-Bus broker is setup on your machine and you've a session bus running
(`DBUS_SESSION_BUS_ADDRESS` environment variable must be set). This is guaranteed to be the case on
a typical Linux desktop session.

### Service

A simple service that politely greets whoever calls its `SayHello` method:

```rust,no_run
use std::{error::Error, future::pending};
use zbus::{connection, interface};

struct Greeter {
    count: u64
}

#[interface(name = "org.zbus.MyGreeter1")]
impl Greeter {
    // Can be `async` as well.
    fn say_hello(&mut self, name: &str) -> String {
        self.count += 1;
        format!("Hello {}! I have been called {} times.", name, self.count)
    }
}

// Although we use `tokio` here, you can use any async runtime of choice.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let greeter = Greeter { count: 0 };
    let _conn = connection::Builder::session()
        .name("org.zbus.MyGreeter")
        .serve_at("/org/zbus/MyGreeter", greeter)
        .build()
        .await?;

    // Do other things or go to wait forever
    pending::<()>().await;

    Ok(())
}
```

You can use the following command to test it:

```bash
$ busctl --user call org.zbus.MyGreeter /org/zbus/MyGreeter org.zbus.MyGreeter1 SayHello s "Maria"
s "Hello Maria! I have been called 1 times."
```

### Client

Now let's write the client-side code for `MyGreeter` service:

```rust,no_run
use zbus::{Connection, Result, proxy};

#[proxy(
    interface = "org.zbus.MyGreeter1",
    default_service = "org.zbus.MyGreeter",
    default_path = "/org/zbus/MyGreeter"
)]
trait MyGreeter {
    async fn say_hello(&self, name: &str) -> Result<String>;
}

// Although we use `tokio` here, you can use any async runtime of choice.
#[tokio::main]
async fn main() -> Result<()> {
    let connection = Connection::session().await?;

    // `proxy` macro creates `MyGreeterProxy` based on `MyGreeter` trait.
    let proxy = MyGreeterProxy::new(&connection).await?;
    let reply = proxy.say_hello("Maria").await?;
    println!("{reply}");

    Ok(())
}
```

## Synchronous programs

zbus's API is async. A program with no async runtime of its own drives it with `zbus::block_on`,
which runs a future to completion on the calling thread. See [the FAQ][bw] of the book.

## Proxy and service API

Since zbus 6.0, the client-side proxy API and the service-side object server API live behind the
`proxy` and `service` cargo features, respectively. Both are enabled by default. If you are
writing a pure service or a pure client, you can disable the API you don't need to reduce the
size of your binary.

The service side of the standard `org.freedesktop.DBus.ObjectManager` interface,
[`fdo::ObjectManager`], is behind the `object-manager` feature, which builds on `service` and is
also enabled by default.

## Compatibility with async runtimes

zbus is runtime-agnostic. A program with no async runtime of its own drives it with
`zbus::block_on`; see [the FAQ][bw]. A program built on Tokio turns on the `tokio` feature, and a
connection built while a Tokio runtime is current runs on it, with no extra thread of zbus's own.
Any other executor can use the default `zruntime` backend as it is, with no integration
needed: a connection built outside a `zbus::block_on` call is simply run by that backend's own
helper thread. [`connection::Builder::runtime`] is there for an application that wants to hand a
connection a runtime of its own instead — any implementation of [`runtime::traits::Runtime`], which
documents the full contract, the connects and the helper process of a `unixexec:` address
included, and `spawn_blocking`, the one method with a default, which the handful of calls with no
async form go through — not something every other executor needs.

zbus's async locks come from zruntime; a build on zruntime or an external runtime needs no lock
feature and pulls in no lock crate for them. A Tokio build uses Tokio's locks instead.

`zbus::block_on` over a runtime of your own only works while that runtime's loop runs on another
thread. A call made from the loop's own thread deadlocks: it waits on a connection that only makes
progress while the loop it just stopped runs.

### Special tokio support

Enabling the `tokio` feature puts a connection on your [`tokio`] runtime instead of the default
`zruntime` backend, with no thread of zbus's own:

```toml
# Sample Cargo.toml snippet.
[dependencies]
zbus = { version = "6", features = ["tokio"] }
```

The `tokio` and `default-rt` features are additive: with both enabled, zbus picks Tokio when a
Tokio runtime is current on the thread that builds the connection, and zruntime otherwise.
With only `tokio` (no `default-rt`), a connection must be built from a thread running a Tokio
runtime. To leave the zruntime backend out of the build, disable default features and list what
you use instead, e.g. `default-features = false, features = ["tokio", "proxy", "service"]`. The
`zruntime` crate stays in the dependency tree for the `Event` and broadcast channels zbus's own
types build on, but none of its runtime is built.

A program with no async runtime of its own drives the API from inside `zbus::block_on`, which
with the `tokio` feature polls the future on a Tokio runtime of its own, so a connection built
inside that call always runs on tokio when that feature is on. That runtime is single-threaded:
the connection's work only runs while a thread is inside `zbus::block_on`. Enable the
`tokio-multithread` feature (which enables `tokio`) to make it a multi-threaded runtime instead,
whose worker threads keep the connection going in between calls.

**Note**: On Windows, a connection that ends up on Tokio cannot use a Unix domain socket, even when
the `default-rt` feature is also on; see [the corresponding tokio issue on GitHub][tctiog].

## WASI

zbus builds for WASI, `wasm32-wasip1` and `wasm32-wasip2`, on the default `zruntime` backend, on
Tokio (whose sockets need `RUSTFLAGS="--cfg tokio_unstable"` there) and on a runtime of your own.
WASI has no Unix domain sockets, no threads and no processes, so there:

* A connection goes over a TCP socket: to a `tcp:` address, which needs `wasm32-wasip2` and a host
  that grants the network, or over a socket the host hands the program, as a
  `std::net::TcpStream` made from its descriptor and given to `connection::Builder::tcp_stream`.
  That socket may well be a Unix domain one on the host's side, connected to the bus: then
  authenticate with `AuthMechanism::External`, and give `connection::Builder::user_id` the ID of
  the user the bus knows the host as, as WASI has no user ID of its own.
* `Address::session` and `Address::system` have no default to fall back to, so they need
  `DBUS_SESSION_BUS_ADDRESS` and `DBUS_SYSTEM_BUS_ADDRESS` to be set.
* No helper thread runs a connection while no thread is inside `zbus::block_on`, or inside the
  `block_on` of the Tokio runtime it is on: its work, the delivery of signals included, waits for
  the next call. Blocking work, such as resolving a host name, blocks the program's one thread.

[zbus]: https://github.com/z-galaxy/zbus\#readme
[bw]: https://z-galaxy.github.io/zbus/faq.html#how-do-i-use-zbus-from-synchronous-code
[tctiog]: https://github.com/tokio-rs/tokio/issues/2201
[`connection::Builder::runtime`]: https://docs.rs/zbus/latest/zbus/connection/struct.Builder.html#method.runtime
[`fdo::ObjectManager`]: https://docs.rs/zbus/latest/zbus/fdo/struct.ObjectManager.html
[`runtime::traits::Runtime`]: https://docs.rs/zbus/latest/zbus/runtime/traits/trait.Runtime.html
[`tokio`]: https://crates.io/crates/tokio
[serde]: https://crates.io/crates/serde
[`tracing`]: https://crates.io/crates/tracing
