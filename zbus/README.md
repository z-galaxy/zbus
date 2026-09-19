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
`chrono`, `enumflags2`, `heapless`, `option-as-array`, `serde_bytes`, `time`, `url`, `uuid`),
and enabling any D-Bus feature (`comms`, `builtin-runtime`, `tokio`, `blocking-api`, `p2p`,
`bus-impl`, `vsock`, `proxy`, `service`, `unixexec`, `ibus`) brings the D-Bus API back.

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

    // `proxy` macro creates `MyGreaterProxy` based on `MyGreeter` trait.
    let proxy = MyGreeterProxy::new(&connection).await?;
    let reply = proxy.say_hello("Maria").await?;
    println!("{reply}");

    Ok(())
}
```

## Blocking API

While zbus is primarily asynchronous (since 2.0), [blocking wrappers][bw] are provided for
convenience. Since zbus 5.0, blocking API can be disabled by disabling the `blocking-api` cargo
feature.

## Proxy and service API

Since zbus 6.0, the client-side proxy API and the service-side object server API live behind the
`proxy` and `service` cargo features, respectively. Both are enabled by default. If you are
writing a pure service or a pure client, you can disable the API you don't need to reduce the
size of your binary.

## Compatibility with async runtimes

zbus is runtime-agnostic. By default (the `builtin-runtime` feature), a connection runs its I/O,
timers and tasks on two threads of zbus's own: one running the tasks and one running
[`async-io`]'s reactor. With `tokio` instead, it runs on your Tokio runtime, with no extra thread.
For any other runtime, hand an implementation of [`runtime::traits::Runtime`] to
[`connection::Builder::runtime`], which then supplies everything a connection needs, sockets
included — except a handful of calls with no async form (a couple of transport lookups, a
peer-credential group lookup, waiting on a helper process), which go through the trait's
`spawn_blocking` and default to a thread of their own unless the runtime overrides it. The wait on
a `unixexec:` helper is the long one: it starts when the connection lets go of the pipe it reads
the helper's output from — normally as the connection ends — and lasts until the helper is gone,
which a helper that keeps reading its still-open input delays until the last clone of the
connection lets go of the other pipe too. That is a cost at teardown rather than for the life of
the connection, and a runtime that serves `unixexec:` addresses should still override
`spawn_blocking` rather than park a thread there for it.

zbus's async locks are its own, built on `event-listener` rather than supplied by the runtime, so
a build on a runtime of your own needs no lock feature and pulls in no lock crate for them. On a
Tokio build, Tokio's locks stand in instead, so that build carries no second lock implementation.

`zbus::blocking` over a runtime of your own only works while that runtime's loop runs on another
thread. A blocking call made from the loop's own thread deadlocks: it waits on a connection that
only makes progress while the loop it just stopped runs.

### Special tokio support

Enabling the `tokio` feature puts a connection on your [`tokio`] runtime instead of the default
`builtin-runtime` backend, with no thread of zbus's own:

```toml
# Sample Cargo.toml snippet.
[dependencies]
# Also disable the default `builtin-runtime` feature to avoid unused dependencies.
zbus = { version = "6", default-features = false, features = ["tokio"] }
```

The `tokio` and `builtin-runtime` features are additive: with both enabled, zbus picks Tokio when
a Tokio runtime is current on the thread that builds the connection, and `builtin-runtime`
otherwise. With only `tokio` (no `builtin-runtime`), a connection must be built from a thread
running a Tokio runtime.

The blocking API (`zbus::blocking`) drives its connections through its own `block_on`, which uses
tokio whenever the `tokio` feature is enabled, so those connections always run on tokio when that
feature is on.

**Note**: On Windows, a connection that ends up on Tokio cannot use a Unix domain socket, even when
`builtin-runtime` is also compiled in; see [the corresponding tokio issue on GitHub][tctiog].

[zbus]: https://github.com/z-galaxy/zbus\#readme
[bw]: https://docs.rs/zbus/latest/zbus/blocking/index.html
[tctiog]: https://github.com/tokio-rs/tokio/issues/2201
[`async-io`]: https://crates.io/crates/async-io
[`connection::Builder::runtime`]: https://docs.rs/zbus/latest/zbus/connection/struct.Builder.html#method.runtime
[`runtime::traits::Runtime`]: https://docs.rs/zbus/latest/zbus/runtime/traits/trait.Runtime.html
[`tokio`]: https://crates.io/crates/tokio
[serde]: https://crates.io/crates/serde
[`tracing`]: https://crates.io/crates/tracing
