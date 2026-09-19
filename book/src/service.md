# Writing a service interface

<!-- toc -->

In this chapter, we are going to implement a service with a method "SayHello", to greet back the
calling client.

We will first discuss the need to associate a service name with the service. Then we are going to
manually handle incoming messages using the low-level API. Finally, we will present the
`ObjectServer` higher-level API and some of its more advanced concepts.

## Taking a service name

As we know from the chapter on [D-Bus concepts], each connection on the bus is given a unique name
(such as ":1.27"). This could be all you need, depending on your use case, and the design of your
D-Bus API. However, typically services use a service name (aka *well-known name*) so peers (clients,
in this context) can easily discover them.

In this example, that is exactly what we're going to do and request the bus for the service name of
our choice:

```rust,no_run
use zbus::{Connection, Result};

// Although we use `tokio` here, you can use any async runtime of choice.
#[tokio::main]
async fn main() -> Result<()> {
    let connection = Connection::session()
        .await?;
    connection
        .request_name("org.zbus.MyGreeter")
        .await?;

    loop {}
}
```

We can check our service is running and is associated with the service name:

```bash
$ busctl --user list | grep zbus
org.zbus.MyGreeter                             412452 service           elmarco :1.396        user@1000.service -       -
```

### 🖐 Hang on

This example is not handling incoming messages yet. Any attempt to call the service will time out
(including the shell completion!).

## Handling low-level messages

At the low-level, you can handle method calls by checking the incoming messages manually.

Let's write a `SayHello` method, that takes a string as argument, and reply with a "hello" greeting
by replacing the loop above with this code:

```rust,no_run
use futures_util::stream::TryStreamExt;

// Although we use `tokio` here, you can use any async runtime of choice.
# #[tokio::main]
# async fn main() -> zbus::Result<()> {
#    let connection = zbus::Connection::session()
#        .await?;
let mut stream = zbus::MessageStream::from(&connection);
#    connection
#        .request_name("org.zbus.MyGreeter")
#        .await?;
#
while let Some(msg) = stream.try_next().await? {
    let msg_header = msg.header();
    dbg!(&msg);

    match msg_header.message_type() {
        zbus::message::Type::MethodCall => {
            // real code would check msg_header path(), interface() and member()
            // handle invalid calls, introspection, errors etc
            let header = msg.header();
            let body = msg.body();
            let arg: &str = body.deserialize()?;
            connection.reply(&header, &(format!("Hello {}!", arg))).await?;

            break;
        }
        _ => continue,
    }
}

# Ok(())
# }
```

And check if it works as expected:

```bash
$ busctl --user call org.zbus.MyGreeter /org/zbus/MyGreeter org.zbus.MyGreeter1 SayHello s "zbus"
s "Hello zbus!"
```

This is the crust of low-level message handling. It should give you all the flexibility you ever
need, but it is also easy to get it wrong. Fortunately, zbus has a simpler solution to offer.

## Using the `ObjectServer`

One can write an `impl` block with a set of methods and let the `interface` procedural macro
write the D-Bus message handling details. It will dispatch the incoming method calls to their
respective handlers, as well as replying to introspection requests.

### `MyGreeter` simple example

Let see how to use it for `MyGreeter` interface:

```rust,no_run
use zbus::{Connection, interface, Result};

struct Greeter;

#[interface(name = "org.zbus.MyGreeter1")]
impl Greeter {
    async fn say_hello(&self, name: &str) -> String {
        format!("Hello {}!", name)
    }
}

// Although we use `tokio` here, you can use any async runtime of choice.
#[tokio::main]
async fn main() -> Result<()> {
    let connection = Connection::session().await?;
    // setup the object server
    connection
        .object_server()
        .at("/org/zbus/MyGreeter", Greeter)
        .await?;
    // before requesting the name
    connection
        .request_name("org.zbus.MyGreeter")
        .await?;

    loop {
        // do something else, wait forever or timeout here:
        // handling D-Bus messages is done in the background
        std::future::pending::<()>().await;
    }
}
```

### ⚠ Service activation pitfalls

A possible footgun here is that you must request the service name **after** you setup the handlers,
otherwise incoming messages may be lost. Activated services may receive calls (or messages) right
after taking their name. This is why it's typically better to make use of `connection::Builder` for
setting up your interfaces and requesting names, and not have to care about this:

```rust,no_run
# use zbus::{connection, interface, Result};
#
#
# struct Greeter;
#
# #[interface(name = "org.zbus.MyGreeter1")]
# impl Greeter {
#     async fn say_hello(&self, name: &str) -> String {
#         format!("Hello {}!", name)
#     }
# }
#
# #[tokio::main]
# async fn main() -> Result<()> {
    let _connection = connection::Builder::session()
        .name("org.zbus.MyGreeter")
        .serve_at("/org/zbus/MyGreeter", Greeter)
        .build()
        .await?;
#     loop {
#         // do something else, wait forever or timeout here:
#         // handling D-Bus messages is done in the background
#         std::future::pending::<()>().await;
#     }
# }
```

It should work with the same `busctl` command used previously.

This time, we can also introspect the service:

```bash
$ busctl --user introspect org.zbus.MyGreeter /org/zbus/MyGreeter
NAME                                TYPE      SIGNATURE RESULT/VALUE FLAGS
org.freedesktop.DBus.Introspectable interface -         -             -
.Introspect                         method    -         s             -
org.freedesktop.DBus.Peer           interface -         -             -
.GetMachineId                       method    -         s             -
.Ping                               method    -         -             -
org.freedesktop.DBus.Properties     interface -         -             -
.Get                                method    ss        v             -
.GetAll                             method    s         a{sv}         -
.Set                                method    ssv       -             -
.PropertiesChanged                  signal    sa{sv}as  -             -
org.zbus.MyGreeter1                 interface -         -             -
.SayHello                           method    s         s             -
```

### A more complete example

`ObjectServer` supports various method attributes to declare properties or signals.

This is a more complete example, demonstrating some of its usages. It also shows a way to
synchronize with the interface handlers from outside, thanks to the `event_listener` crate
(this is just one of the many ways).

```rust,no_run
use zbus::{object_server::SignalEmitter, connection::Builder, interface, fdo, Result};
use event_listener::{Event, Listener};

struct Greeter {
    name: String,
    done: Event,
}

#[interface(name = "org.zbus.MyGreeter1")]
impl Greeter {
    async fn say_hello(&self, name: &str) -> String {
        format!("Hello {}!", name)
    }

    // Rude!
    async fn go_away(
        &self,
        #[zbus(signal_emitter)]
        emitter: SignalEmitter<'_>,
    ) -> fdo::Result<()> {
        emitter.greeted_everyone().await?;
        self.done.notify(1);

        Ok(())
    }

    /// A "GreeterName" property.
    #[zbus(property)]
    async fn greeter_name(&self) -> &str {
        &self.name
    }

    /// A setter for the "GreeterName" property.
    ///
    /// Additionally, a `greeter_name_changed` method has been generated for you if you need to
    /// notify listeners that "GreeterName" was updated. It will be automatically called when
    /// using this setter.
    #[zbus(property)]
    async fn set_greeter_name(&mut self, name: String) {
        self.name = name;
    }

    /// A signal; the implementation is provided by the macro.
    #[zbus(signal)]
    async fn greeted_everyone(emitter: &SignalEmitter<'_>) -> Result<()>;
}

// Although we use `tokio` here, you can use any async runtime of choice.
#[tokio::main]
async fn main() -> Result<()> {
    let greeter = Greeter {
        name: "GreeterName".to_string(),
        done: event_listener::Event::new(),
    };
    let done_listener = greeter.done.listen();
    let connection = Builder::session()
        .name("org.zbus.MyGreeter")
        .serve_at("/org/zbus/MyGreeter", greeter)
        .build()
        .await?;

    done_listener.wait();

    // Let's emit the signal again, just for the fun of it.
    connection
        .object_server()
        .interface("/org/zbus/MyGreeter")
        .await?
        .greeted_everyone()
        .await?;

    Ok(())
}
```

This is the introspection result:

```bash
$ busctl --user introspect org.zbus.MyGreeter /org/zbus/MyGreeter
NAME                                TYPE      SIGNATURE RESULT/VALUE FLAGS
[...]
org.zbus.MyGreeter1                 interface -         -             -
.GoAway                             method    -         -             -
.SayHello                           method    s         s             -
.GreeterName                        property  s         "GreeterName" emits-change writable
.GreetedEveryone                    signal    -         -             -
```

### Method errors

There are two possibilities for the return value of interface methods. The first is for infallible
method calls, where the return type is a directly serializable value, like the `String` in
`say_hello()` above.

The second is a result return value, where the `Ok` variant is the serializable value, and the
error is any type that implements [`zbus::DBusError`]. The [`zbus::fdo::Error`] type implements this
trait, and should cover most common use cases. However, when a custom error type needs to be emitted
from the method as an error reply, it can be created using `derive(zbus::DBusError)`, and used in
the returned `Result<T, E>`.

Property getters and setters follow the same rule as methods: the error is any type that
implements [`zbus::DBusError`], so a custom `derive(zbus::DBusError)` type works there too.
[`zbus::fdo::Error`] covers the common cases; [`zbus::fdo::Error::UnknownProperty`] is the one
you'll reach for most often.

### Sending signals

As you might have noticed in the previous example, the signal methods don't take a `&self` argument
but a `SignalEmitter` reference. While this allows to emit signals whether from inside or outside of
the `interface` methods' context, it does make it a bit inconvenient to use. To make things easier
`interface` generates a trait, `GreeterSignals` that provides the same signal methods but
without the `SignalEmitter` argument. The macro provides two implementations of this trait for:

* `zbus::object_server::InterfaceRef<Greeter>`
* `zbus::object_server::SignalEmitter`

The former is useful for emitting signals from outside the context of an `interface` method and the
latter is useful for emitting signals from inside of it. To make it possible to emit signals from
inside of an `interface` method, methods can ask to receive a `SignalEmitter` passed to them using
the special `zbus(signal_emitter)` attribute.

Please refer to [`interface` documentation][didoc] for more examples and list of other special
attributes you can make use of.

We saw examples of signal emission in action, from both inside and outside an `interface` method, in
the previous example.

### Notifying property changes

For each property declared through the `interface` macro, a `<property_name>_changed` method is
generated that emits the necessary property change signal. Here is how to use it with the previous
example code:

```rust,no_run
# use zbus::interface;
#
# struct Greeter {
#     name: String
# }
#
# #[interface(name = "org.zbus.MyGreeter1")]
# impl Greeter {
#     #[zbus(property)]
#     async fn greeter_name(&self) -> &str {
#         &self.name
#     }
#
#     #[zbus(property)]
#     async fn set_greeter_name(&mut self, name: String) {
#         self.name = name;
#     }
# }
#
# #[tokio::main]
# async fn main() -> zbus::Result<()> {
# let connection = zbus::Connection::session().await?;
# let object_server = connection.object_server();

let iface_ref = object_server.interface::<_, Greeter>("/org/zbus/MyGreeter").await?;
let mut iface = iface_ref.get_mut().await;
iface.name = String::from("👋");
iface.greeter_name_changed(iface_ref.signal_emitter()).await?;
# Ok(())
# }
```

## Proxy generation

`interface` macro can also generate the client-side proxy code for you. It utilizes the [`proxy`]
macro behind the scenes to achieve this. Here is how to use it:

```rust
use zbus::interface;

struct Greeter {
    name: String
}

#[interface(
    name = "org.zbus.MyGreeter.WithProxy",
    // Specifying the `proxy` attribute instructs `interface` to generate the
    // client-side proxy. You can specify proxy-specific attributes
    // (e.g `async_name`) here. All the attributes that are common between
    // `proxy` and `interface` macros (e.g `name`) are automtically forwarded to
    // the `proxy` macro.
    proxy(
        default_path = "/org/zbus/MyGreeter/WithProxy",
        default_service = "org.zbus.MyGreeter.WithProxy",
    ),
)]
impl Greeter {
    #[zbus(property)]
    async fn greeter_name(&self) -> String {
        self.name.clone()
    }

    #[zbus(proxy(no_reply))]
    async fn whatever(&self) {
        println!("Whatever!");
    }
}

# #[tokio::main]
# async fn main() -> zbus::Result<()> {

let greeter = Greeter { name: "GreeterName".to_string() };
let connection = zbus::connection::Builder::session()
        .name("org.zbus.MyGreeter.WithProxy")
        .serve_at("/org/zbus/MyGreeter/WithProxy", greeter)
        .build()
        .await?;
let proxy = GreeterProxy::new(&connection).await?;
assert_eq!(proxy.greeter_name().await?, "GreeterName");
proxy.whatever().await?;

# Ok(())
# }
```

### Known Limitations

While it's extremely useful to be able to generate the client-side proxy code directly from
`interface` as it allows you to avoid duplicating code, there are some limitations to be aware of:

* The `proxy` macro's trait bounds also apply to ordinary `interface` methods. A service-only
  method result needs `Serialize + Type`, while the corresponding generated proxy method also
  needs `DeserializeOwned`.
* Ordinary method return values cannot borrow. A borrowing property getter is supported: its
  generated proxy getter is generic over an owned `DeserializeOwned + Type` result selected by the
  caller.
* Methods returning [`object_server::ResponseDispatchNotifier`] wrapper type will do the same for
  proxy as well.

[D-Bus concepts]: concepts.html#bus-name--service-name
[didoc]: https://docs.rs/zbus/latest/zbus/attr.interface.html
[`zbus::DBusError`]:https://docs.rs/zbus/latest/zbus/trait.DBusError.html
[`zbus::fdo::Error`]: https://docs.rs/zbus/latest/zbus/fdo/enum.Error.html
[`zbus::fdo::Error::UnknownProperty`]: https://docs.rs/zbus/latest/zbus/fdo/enum.Error.html#variant.UnknownProperty
[`proxy`]: https://docs.rs/zbus/latest/zbus/attr.proxy.html
