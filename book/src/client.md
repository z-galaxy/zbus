# Writing a client proxy

<!-- toc -->

In this chapter, we are going to see how to make low-level D-Bus method calls. Then we are going to
dive in, and derive from a trait to make a convenient Rust binding. Finally, we will learn about
*xmlgen*, a tool to help us generate a boilerplate trait from the XML of an introspected service.

To make this learning "hands-on", we are going to call and bind the cross-desktop notification
service (please refer to this
[reference](https://specifications.freedesktop.org/notification-spec/latest/)
document for further details on this API).

Let's start by playing with the service from the shell, and notify the desktop with [`busctl`][^busctl]:

```bash
busctl --user call \
  org.freedesktop.Notifications \
  /org/freedesktop/Notifications \
  org.freedesktop.Notifications \
  Notify \
  susssasa\{sv\}i \
  "my-app" 0 "dialog-information" "A summary" "Some body" 0 0 5000
```

**Note**: `busctl` has very good auto-completion support in bash or zsh.

Running this command should pop-up a notification dialog on your desktop. If it does not, your
desktop does not support the notification service, and this example will be less interactive.
Nonetheless you can use a similar approach for other services.

This command shows us several aspects of the D-Bus communication:

- `--user`: Connect to and use the user/session bus.

- `call`: Send a method call message. (D-Bus also supports signals, error messages, and method
  replies)

- **destination**: The name of the service (`org.freedesktop.Notifications`).

- **object path**: Object/interface path (`/org/freedesktop/Notifications`).

- **interface**: The interface name (methods are organized in interfaces, here
  `org.freedesktop.Notifications`, same name as the service).

- **method**: The name of the method to call, `Notify`.

- **signature**: That `susssasa{sv}i` means the method takes 8 arguments of various types. 's', for
  example, is for a string. 'as' is for array of strings.

- The method arguments.

See [`busctl`] man page for more details.

## Low-level call from a `zbus::Connection`

zbus `Connection` has a `call_method()` method, which you can use directly:

```rust,no_run
use std::collections::HashMap;
use std::error::Error;

use zbus::{Value, Connection};

// Although we use `tokio` here, you can use any async runtime of choice.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let connection = Connection::session().await?;

    let m = connection.call_method(
        Some("org.freedesktop.Notifications"),
        "/org/freedesktop/Notifications",
        Some("org.freedesktop.Notifications"),
        "Notify",
        &("my-app", 0u32, "dialog-information", "A summary", "Some body",
          vec![""; 0], HashMap::<&str, &Value>::new(), 5000),
    ).await?;
    let reply: u32 = m.body().deserialize().unwrap();
    dbg!(reply);
    Ok(())
}
```

Although this is already quite flexible, and handles various details for you (such as the message
signature), it is also somewhat inconvenient and error-prone: you can easily miss arguments, or give
arguments with the wrong type or other kind of errors (what would happen if you typed `0`, instead
of `0u32`?).

Instead, we want to wrap this `Notify` D-Bus method in a Rust function. Let's see how next.

## Trait-derived proxy call

A trait declaration `T` with a `proxy` attribute will have a derived `TProxy` implemented thanks to
procedural macros. The trait methods will have respective `impl` methods wrapping the D-Bus calls:

```rust,no_run
use std::collections::HashMap;
use std::error::Error;

use zbus::{Value, proxy, Connection};

#[proxy(
    default_service = "org.freedesktop.Notifications",
    default_path = "/org/freedesktop/Notifications"
)]
trait Notifications {
    /// Call the org.freedesktop.Notifications.Notify D-Bus method
    fn notify(&self,
              app_name: &str,
              replaces_id: u32,
              app_icon: &str,
              summary: &str,
              body: &str,
              actions: &[&str],
              hints: HashMap<&str, &Value<'_>>,
              expire_timeout: i32) -> zbus::Result<u32>;
}

// Although we use `tokio` here, you can use any async runtime of choice.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let connection = Connection::session().await?;

    let proxy = NotificationsProxy::new(&connection).await?;
    let reply = proxy.notify(
        "my-app",
        0,
        "dialog-information",
        "A summary", "Some body",
        &[],
        HashMap::new(),
        5000,
    ).await?;
    dbg!(reply);

    Ok(())
}
```

A `TProxy` has a few associated methods, such as `new(connection)`, using the default associated
service name and object path, and an associated builder if you need to specify something
different.

This should help to avoid the kind of mistakes we saw earlier. It's also a bit easier to use, thanks
to Rust type inference. This makes it also possible to have higher-level types, they fit more
naturally with the rest of the code. You can further document the D-Bus API or provide additional
helpers.

> **Note**
>
> For a program without an async runtime of its own, see the [synchronous programs
> chapter][cob].

### Signals

Signals are like methods, except they don't expect a reply. They are typically emitted by services
to notify interested peers of any changes to the state of the service. zbus provides you a
[`Stream`]-based API for receiving signals.

Let's look at this API in action, with an example where we monitor started systemd units.

```rust,no_run
# // NOTE: When changing this, please also keep `zbus/examples/watch-systemd-jobs.rs` in sync.
use futures_util::stream::StreamExt;
use zbus::{OwnedObjectPath, proxy, Connection};

# fn main() {
#     zbus::block_on(watch_systemd_jobs()).expect("Error listening to signal");
# }

#[proxy(
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1",
    interface = "org.freedesktop.systemd1.Manager"
)]
trait Systemd1Manager {
    // Defines signature for D-Bus signal named `JobNew`
    #[zbus(signal)]
    fn job_new(&self, id: u32, job: OwnedObjectPath, unit: String) -> zbus::Result<()>;
}

async fn watch_systemd_jobs() -> zbus::Result<()> {
    let connection = Connection::system().await?;
    // `Systemd1ManagerProxy` is generated from `Systemd1Manager` trait
    let systemd_proxy = Systemd1ManagerProxy::new(&connection).await?;
    // Method `receive_job_new` is generated from `job_new` signal
    let mut new_jobs_stream = systemd_proxy.receive_job_new().await?;

    while let Some(msg) = new_jobs_stream.next().await {
        // struct `JobNewArgs` is generated from `job_new` signal function arguments
        let args: JobNewArgs = msg.args().expect("Error parsing message");

        println!(
            "JobNew received: unit={} id={} path={}",
            args.unit, args.id, args.job
        );
    }

    panic!("Stream ended unexpectedly");
}
```

#### More advanced example

Here is a more elaborate example, where we get our location from
[Geoclue](https://gitlab.freedesktop.org/geoclue/geoclue/-/blob/master/README.md):

```rust,no_run
use zbus::{ObjectPath, proxy, Connection, Result};
use futures_util::stream::StreamExt;

#[proxy(
    default_service = "org.freedesktop.GeoClue2",
    interface = "org.freedesktop.GeoClue2.Manager",
    default_path = "/org/freedesktop/GeoClue2/Manager"
)]
trait Manager {
    #[zbus(object = "Client")]
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

// Although we use `tokio` here, you can use any async runtime of choice.
#[tokio::main]
async fn main() -> Result<()> {
    let conn = Connection::system().await?;
    let manager = ManagerProxy::new(&conn).await?;
    let mut client = manager.get_client().await?;
    // Gotta do this, sorry!
    client.set_desktop_id("org.freedesktop.zbus").await?;

    let props = zbus::fdo::PropertiesProxy::builder(&conn)
        .destination("org.freedesktop.GeoClue2")
        .path(client.inner().path())
        .build()
        .await?;
    let mut props_changed = props.receive_properties_changed().await?;
    let mut location_updated = client.receive_location_updated().await?;

    client.start().await?;

    futures_util::try_join!(
        async {
            while let Some(signal) = props_changed.next().await {
                let args = signal.args()?;

                for (name, value) in args.changed_properties().iter() {
                    println!("{}.{} changed to `{:?}`", args.interface_name(), name, value);
                }
            }

            Ok::<(), zbus::Error>(())
        },
        async {
            while let Some(signal) = location_updated.next().await {
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
            }

            // No need to specify type of Result each time
            Ok(())
        }
    )?;

   Ok(())
}
```

While the Geoclue's D-Bus API is a bit involved, we still ended-up with a not-so-complicated (~100
LOC) code for getting our location.

### Properties

Interfaces can have associated properties, which can be read or set with the
`org.freedesktop.DBus.Properties` interface. Here again, the `#[proxy]` attribute comes to the
rescue to help you. You can annotate a trait method to be a getter:

```rust,noplayground
use zbus::{proxy, Result};

#[proxy]
trait MyInterface {
    #[zbus(property)]
    fn state(&self) -> Result<String>;
}
```

The `state()` method will translate to a `"State"` property `Get` call.

To set the property, prefix the name of the property with `set_`.

For a more real world example, let's try and read two properties from systemd's main service:

```rust,no_run
use zbus::{Connection, proxy, Result};

#[proxy(
    interface = "org.freedesktop.systemd1.Manager",
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
trait SystemdManager {
    #[zbus(property)]
    fn architecture(&self) -> Result<String>;
    #[zbus(property)]
    fn environment(&self) -> Result<Vec<String>>;
}

#[tokio::main]
async fn main() -> Result<()> {
    let connection = Connection::system().await?;

    let proxy = SystemdManagerProxy::new(&connection).await?;
    println!("Host architecture: {}", proxy.architecture().await?);
    println!("Environment variables:");
    for env in proxy.environment().await? {
        println!("  {}", env);
    }

    Ok(())
}
```

You should get an output similar to this:

```none
Host architecture: x86-64
Environment variables:
  HOME=/home/zeenix
  LANG=en_US.UTF-8
  LC_ADDRESS=de_DE.UTF-8
  LC_IDENTIFICATION=de_DE.UTF-8
  LC_MEASUREMENT=de_DE.UTF-8
  LC_MONETARY=de_DE.UTF-8
  LC_NAME=de_DE.UTF-8
  LC_NUMERIC=de_DE.UTF-8
  LC_PAPER=de_DE.UTF-8
  LC_TELEPHONE=de_DE.UTF-8
  LC_TIME=de_DE.UTF-8
  ...
```

#### Watching for changes

By default, the proxy will cache the properties and watch for changes.

To be notified of a property change, you use a stream API, just like for receiving signals. The
methods are named after the properties' names: `receive_<prop_name>_changed()`.

Here is an example:

```rust,no_run
use zbus::{Connection, proxy, Result};
use futures_util::stream::StreamExt;
#
# #[tokio::main]
# async fn main() -> Result<()> {
    #[proxy(
        interface = "org.freedesktop.systemd1.Manager",
        default_service = "org.freedesktop.systemd1",
        default_path = "/org/freedesktop/systemd1"
    )]
    trait SystemdManager {
        #[zbus(property)]
        fn log_level(&self) -> Result<String>;
    }

    let connection = Connection::system().await?;

    let proxy = SystemdManagerProxy::new(&connection).await?;
    let mut stream = proxy.receive_log_level_changed().await;
    while let Some(v) = stream.next().await {
        println!("LogLevel changed: {:?}", v.get().await);
    }
#
#   Ok(())
# }
```

## Generating the trait from an XML interface

The `zbus_xmlgen` crate provides a [developer-friendly tool], that can generate Rust traits from a
given D-Bus introspection XML for you.

**Note:** This tool should not be considered a drop-in Rust-specific replacement for similar tools
available for low-level languages, such as [`gdbus-codegen`]. Unlike those tools, this is only meant
as a starting point to generate the code, once. In many cases, you will want to tweak the generated
code.

The tool can be used to generate rust code directly from a D-Bus service running on our system:

```bash
zbus-xmlgen session \
  org.freedesktop.Notifications \
  /org/freedesktop/Notifications
```

Alternatively you can also get the XML interface from a different source and use it to generate the
interface code. Some packages may also provide the XML directly as an installed file, allowing it to
be queried using [`pkg-config`], for example.

We can fetch the XML interface of the notification service, using the `--xml-interface` option of
the `busctl`[^busctl] command. This option was introduced to `busctl` in systemd v243.

```bash
busctl --user --xml-interface introspect \
  org.freedesktop.Notifications \
  /org/freedesktop/Notifications
```

You should get a similar output:

```xml
<!DOCTYPE node PUBLIC "-//freedesktop//DTD D-BUS Object Introspection 1.0//EN"
                      "http://www.freedesktop.org/standards/dbus/1.0/introspect.dtd">
<node>
  <!-- other interfaces omitted -->
  <interface name="org.freedesktop.Notifications">
    <method name="Notify">
      <arg type="s" name="arg_0" direction="in">
      </arg>
      <arg type="u" name="arg_1" direction="in">
      </arg>
      <arg type="s" name="arg_2" direction="in">
      </arg>
      <arg type="s" name="arg_3" direction="in">
      </arg>
      <arg type="s" name="arg_4" direction="in">
      </arg>
      <arg type="as" name="arg_5" direction="in">
      </arg>
      <arg type="a{sv}" name="arg_6" direction="in">
      </arg>
      <arg type="i" name="arg_7" direction="in">
      </arg>
      <arg type="u" name="arg_8" direction="out">
      </arg>
    </method>
    <method name="CloseNotification">
      <arg type="u" name="arg_0" direction="in">
      </arg>
    </method>
    <method name="GetCapabilities">
      <arg type="as" name="arg_0" direction="out">
      </arg>
    </method>
    <method name="GetServerInformation">
      <arg type="s" name="arg_0" direction="out">
      </arg>
      <arg type="s" name="arg_1" direction="out">
      </arg>
      <arg type="s" name="arg_2" direction="out">
      </arg>
      <arg type="s" name="arg_3" direction="out">
      </arg>
    </method>
    <signal name="NotificationClosed">
      <arg type="u" name="arg_0">
      </arg>
      <arg type="u" name="arg_1">
      </arg>
    </signal>
    <signal name="ActionInvoked">
      <arg type="u" name="arg_0">
      </arg>
      <arg type="s" name="arg_1">
      </arg>
    </signal>
  </interface>
</node>
```

Save the output to a `notify.xml` file. Then call:

```bash
zbus-xmlgen file notify.xml
```

This will give back effortlessly the corresponding Rust traits boilerplate
code:

```rust,noplayground
use zbus::proxy;

#[proxy(
    interface = "org.freedesktop.Notifications",
    default_service = "org.freedesktop.Notifications",
    default_path= "/org/freedesktop/Notifications",
)]
trait Notifications {
    /// CloseNotification method
    fn close_notification(&self, arg_0: u32) -> zbus::Result<()>;

    /// GetCapabilities method
    fn get_capabilities(&self) -> zbus::Result<Vec<String>>;

    /// GetServerInformation method
    fn get_server_information(&self) -> zbus::Result<(String, String, String, String)>;

    /// Notify method
    fn notify(
        &self,
        arg_0: &str,
        arg_1: u32,
        arg_2: &str,
        arg_3: &str,
        arg_4: &str,
        arg_5: &[&str],
        arg_6: std::collections::HashMap<&str, zbus::Value<'_>>,
        arg_7: i32,
    ) -> zbus::Result<u32>;

    /// ActionInvoked signal
    #[zbus(signal)]
    fn action_invoked(&self, arg_0: u32, arg_1: &str) -> zbus::Result<()>;

    /// NotificationClosed signal
    #[zbus(signal)]
    fn notification_closed(&self, arg_0: u32, arg_1: u32) -> zbus::Result<()>;
}
```

It should be usable as such. But you may as well improve a bit the naming of the arguments, use
better types (using `BitFlags`, structs or other custom types), add extra documentation, and other
functions to make the binding more pleasing to use from Rust.

For example, the generated `GetServerInformation` method can be improved to a nicer version:

```rust,noplayground
use serde::{Serialize, Deserialize};
use zbus::{Type, proxy};

#[derive(Debug, Type, Serialize, Deserialize)]
pub struct ServerInformation {
    /// The product name of the server.
    pub name: String,

    /// The vendor name. For example "KDE," "GNOME," "freedesktop.org" or "Microsoft".
    pub vendor: String,

    /// The server's version number.
    pub version: String,

    /// The specification version the server is compliant with.
    pub spec_version: String,
}

#[proxy(
    interface = "org.freedesktop.Notifications",
    default_service = "org.freedesktop.Notifications",
    default_path= "/org/freedesktop/Notifications",
)]
trait Notifications {
    /// Get server information.
    ///
    /// This message returns the information on the server.
    fn get_server_information(&self) -> zbus::Result<ServerInformation>;
}
```

You can learn more from the zbus-ify [binding of
PolicyKit](https://github.com/z-galaxy/zbus_polkit), for example, which was
implemented starting from the *xmlgen* output.

There you have it, a Rust-friendly binding for your D-Bus service!

[`busctl`]: https://www.freedesktop.org/software/systemd/man/busctl.html
[developer-friendly tool]: https://crates.io/crates/zbus_xmlgen
[`gdbus-codegen`]: https://docs.gtk.org/gio/migrating-gdbus.html#generating-code-and-docs
[`pkg-config`]: https://www.freedesktop.org/wiki/Software/pkg-config/
[cob]: blocking.html
[`Stream`]: https://docs.rs/futures/4/futures/stream/trait.Stream.html

[^busctl]: `busctl` is part of [`systemd`](https://systemd.io/).
