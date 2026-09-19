# Synchronous programs

<!-- toc -->

zbus's API is async, and a program does not need an async runtime of its own to use it:
[`zbus::block_on`] runs a future to completion on the calling thread, and with the default
`builtin-runtime` feature that same thread runs the connection's work in between, so the program
below is one thread with zbus as its one dependency, plus `futures-util` for the stream extension
trait where a program reads a stream. Everything in the other chapters works the same way inside
the future handed to `zbus::block_on`.

Two rules come with it. The future must not block the thread waiting for something the connection
has to do — a synchronous wait for a reply, a busy loop until a signal has arrived — because that
work runs on this very thread between two polls of the future, so such a wait never ends. And
`zbus::block_on` must not be called from inside a task zbus is running: from a method of an
interface the connection serves, say, or from a future polled inside another `zbus::block_on`.
Such a call panics, because it could only wait for the thread it is on.

## Client

```rust,no_run
use futures_util::stream::StreamExt;
use zbus::{Connection, ObjectPath, Result, proxy};

#[proxy(
    default_service = "org.freedesktop.GeoClue2",
    interface = "org.freedesktop.GeoClue2.Manager",
    default_path = "/org/freedesktop/GeoClue2/Manager"
)]
trait Manager {
    #[zbus(object = "Client")]
    /// The method normally returns an `ObjectPath`.
    /// With the object attribute, we can make it return a `ClientProxy` directly.
    fn get_client(&self);
}

#[proxy(
    default_service = "org.freedesktop.GeoClue2",
    interface = "org.freedesktop.GeoClue2.Client"
)]
trait Client {
    fn start(&self) -> Result<()>;
    fn stop(&self) -> Result<()>;

    #[zbus(property)]
    fn set_desktop_id(&mut self, id: &str) -> Result<()>;

    #[zbus(signal)]
    fn location_updated(&self, old: ObjectPath<'_>, new: ObjectPath<'_>) -> Result<()>;
}

#[proxy(
    default_service = "org.freedesktop.GeoClue2",
    interface = "org.freedesktop.GeoClue2.Location"
)]
trait Location {
    #[zbus(property)]
    fn latitude(&self) -> Result<f64>;
    #[zbus(property)]
    fn longitude(&self) -> Result<f64>;
}

fn main() -> Result<()> {
    zbus::block_on(async {
        let conn = Connection::system().await?;
        let manager = ManagerProxy::new(&conn).await?;
        let mut client = manager.get_client().await?;
        // Gotta do this, sorry!
        client.set_desktop_id("org.freedesktop.zbus").await?;

        let mut location_updated = client.receive_location_updated().await?;

        client.start().await?;

        // Wait for the signal.
        let signal = location_updated.next().await.unwrap();
        let args = signal.args()?;

        let location = LocationProxy::builder(&conn)
            .path(args.new())
            .build()
            .await?;
        println!(
            "Latitude: {}\nLongitude: {}",
            location.latitude().await?,
            location.longitude().await?,
        );

        Ok(())
    })
}
```

### Watching for properties

That's almost the same as receiving signals:

```rust,no_run
use futures_util::stream::StreamExt;
use zbus::{Connection, Result, proxy};

#[proxy(
    interface = "org.freedesktop.systemd1.Manager",
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
trait SystemdManager {
    #[zbus(property)]
    fn log_level(&self) -> Result<String>;
}

fn main() -> Result<()> {
    zbus::block_on(async {
        let connection = Connection::session().await?;

        let proxy = SystemdManagerProxy::new(&connection).await?;
        let v = proxy.receive_log_level_changed().await.next().await.unwrap();
        println!("LogLevel changed: {:?}", v.get().await?);

        Ok(())
    })
}
```

## Server

A service is the same as in the [service chapter](service.md), with the connection built and the
object served inside one `zbus::block_on`: the future that never resolves keeps the program alive,
and the calls arriving on the connection are handled in between its polls.

Note that the methods of the interface are called from the connection's own tasks. They are
therefore subject to the second rule above: an interface method must not call `zbus::block_on`.

```rust,no_run
use std::future::pending;

use zbus::{Result, connection, interface, object_server::SignalEmitter};

struct Greeter {
    name: String,
}

#[interface(name = "org.zbus.MyGreeter1")]
impl Greeter {
    fn say_hello(&self, name: &str) -> String {
        format!("Hello {}!", name)
    }

    /// A "GreeterName" property.
    #[zbus(property)]
    fn greeter_name(&self) -> &str {
        &self.name
    }

    /// A setter for the "GreeterName" property.
    ///
    /// Additionally, a `greeter_name_changed` method has been generated for you if you need to
    /// notify listeners that "GreeterName" was updated. It will be automatically called when
    /// using this setter.
    #[zbus(property)]
    fn set_greeter_name(&mut self, name: String) {
        self.name = name;
    }

    /// A signal; the implementation is provided by the macro.
    #[zbus(signal)]
    async fn greeted_everyone(emitter: &SignalEmitter<'_>) -> Result<()>;
}

fn main() -> Result<()> {
    zbus::block_on(async {
        let greeter = Greeter {
            name: "GreeterName".to_string(),
        };
        let _connection = connection::Builder::session()
            .name("org.zbus.MyGreeter")
            .serve_at("/org/zbus/MyGreeter", greeter)
            .build()
            .await?;

        pending::<()>().await;

        Ok(())
    })
}
```

[`zbus::block_on`]: https://docs.rs/zbus/latest/zbus/fn.block_on.html
