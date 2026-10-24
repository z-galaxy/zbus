# Upgrading to zbus 6.0

<!-- toc -->

zbus 6.0 is one dependency where there used to be as many as four. The `zvariant`,
`zvariant_derive` and `zbus_names` crates were merged into `zbus`:

| Was | Is now |
| --- | --- |
| the `zvariant` crate | common types at the `zbus` root; codecs in [`zbus::wire`] |
| the `zvariant_derive` crate | the derives, re-exported from `zbus` |
| the `zbus_names` crate | the [`zbus::names`] module |

GVariant support, deprecated in zvariant 5.15, is gone; it lives on in the [zgvariant] crate.

## Which part applies to you

* **You depend on `zbus`.** Bump the version and, if you disable the default features, see
  [Cargo features](#cargo-features). `zbus::zvariant` remains as a deprecated alias module until
  7.0, so most code keeps compiling with a warning; [what it does not cover][breaks] is listed
  below. Move common types and derives to the `zbus` root and encoding and decoding calls to
  `zbus::wire` at your own pace.
* **You depend on `zvariant` and not on `zbus`.** Depend on `zbus` with `default-features = false`
  instead, with the same features: they kept their names, except for the removed `gvariant`,
  `ostree-tests` and `enumflags2`. Rename `zvariant::` to `zbus::` for common types and derives,
  minding that [`Value` is now `Variant`][value-variant], and to `zbus::wire::` for encoding and
  decoding APIs.
* **You depend on `zbus_names`.** Depend on `zbus` with `default-features = false` instead and
  rename `zbus_names::` to `zbus::names::`.
* **You use the `gvariant` feature.** Move to [zgvariant].

## Cargo features

A lot of the API is now behind new Cargo features. Those marked as default are on with a plain
`zbus = "6"`; the others have to be enabled explicitly:

* `comms`: the D-Bus API, i.e. connections, messages and `fdo`. Every other D-Bus feature, such as
  `tokio`, `proxy` or `p2p`, enables it. Without it, zbus is the wire format and the name types.
* `proxy` (default): the client-side API, i.e. `Proxy`, `#[proxy]` and the `fdo::*Proxy` types.
* `service` (default): the service-side API, i.e. `ObjectServer` and `#[interface]`.
* `object-manager` (default): the service-side `fdo::ObjectManager`.
* `tracing` (default): log events and spans through [`tracing`].
* `default-rt` (default): zbus's own runtime, [zruntime]; replaces 5.x's `async-io`.
* `tokio-multithread`: runs `zbus::block_on` on a multi-threaded Tokio runtime.
* `unixexec` and `ibus`: the `unixexec:` and `ibus:` transports. Without them, an address of that
  transport is rejected, including in `DBUS_SESSION_BUS_ADDRESS`.

These features are gone and have to be removed from your dependency entry:

* `async-io`: use `default-rt`.
* `blocking-api`: [the blocking API is gone][blocking-gone].
* `vsock` and `tokio-vsock`: VSOCK is no longer a transport. To use a VSOCK stream you open
  yourself, see [the FAQ](faq.md#how-do-i-connect-over-vsock).
* `enumflags2`: see [The `enumflags2` feature is gone][enumflags2].
* `gvariant` and `ostree-tests`: see [GVariant](#gvariant).
* `async-fs`, `async-executor`, `async-task`, `async-lock`, `async-process` and `blocking`: these
  were never meant to be features. If you use these crates, depend on them directly.

zbus's `tokio` feature no longer enables Tokio's `fs`, `io-util`, `process`, `rt-multi-thread` and
`tracing` features (the last three come back with zbus's `unixexec`/`ibus`, `tokio-multithread`
and `tracing` features). Enable the ones your code uses in your own `tokio` dependency.

A direct `zbus_macros` dependency with `default-features = false` needs its new `proxy` and
`service` features for `#[proxy]` and `#[interface]`, and `comms` for `DBusError`.

zbus 6 needs Rust 1.89 or newer.

## Paths

| zbus 5 / zvariant 5 / zbus_names 4 | zbus 6 |
| --- | --- |
| Common `zvariant::X`, `zbus::zvariant::X` types and derives | `zbus::X` |
| `zvariant::Value` and `OwnedValue` | [`zbus::Variant` and `OwnedVariant`][value-variant] |
| `zvariant::serialized::Context` | `zbus::wire::serialized::Context` |
| `zvariant::as_value`, `zvariant::dbus` | `zbus::as_variant`, `zbus::wire::dbus` |
| `zvariant::signature!` | `zbus::signature!` |
| `zbus_names::X`, `zbus::names::X` | `zbus::names::X` |
| `zvariant::Error`, `zvariant::Result` | `zbus::Error`, `zbus::Result` |
| `zbus_names::Error`, `zbus::names::Error` | `zbus::Error` |
| `zvariant::MaxDepthExceeded` | `zbus::MaxDepthExceeded` |
| `#[zvariant(...)]` on a derive | `#[zbus(...)]`; both spellings stay accepted |
| `#[zvariant(crate = "zvariant")]` | `#[zbus(crate = "zbus::wire")]` |

Encoding and decoding functions, `serialized`, `Endian` and its constants, `DynamicDeserialize`,
`StructureBuilder`, the `*Seed` types, and `signature::{Child, Fields}` are in `zbus::wire`.

## Errors

There is one error type now. `zvariant::Error`, `zbus_names::Error` and the `zbus::Error::Variant`
and `zbus::Error::Names` variants that wrapped them are gone: their variants, such as
`IncorrectType`, `SignatureMismatch` and `InvalidName`, are variants of `zbus::Error` itself. `?`
keeps working. Besides that:

* `zvariant::Error::Message(s)`, what serde's `Error::custom` produced, is `Error::Failure(s)`.
* `Error::InputOutput` displays as `I/O error: <the io::Error>`, with a prefix zvariant's lacked.
* `Error::MissingFramingOffset` and `Error::IncompatibleFormat` are gone with GVariant.
* `Error::Connection` holds a `Box<Address>`.
* `Error::MethodError`, `Error::FDO`, `Error::Connection` and the new `Error::DBus` only exist
  with `comms`.
* The `Invalid*Name` variants that `zbus_names` 4.1 deprecated are gone; `Error::InvalidName` is
  what you get.
* `zbus::Error` no longer implements `From<hex::FromHexError>`.
* `zbus_xml::Error`'s `Variant` and `Name` variants are now a single `Zbus(zbus::Error)` variant.
* An error type of yours that implements `From` for both `zbus::Error` and `zvariant::Error` now
  has the same impl twice (`E0119`). Delete the second one.

D-Bus error replies changed too:

* A reply whose name is one that `fdo::Error` defines is now an `Error::FDO` rather than an
  `Error::MethodError`, so match on `fdo::Error::ServiceUnknown` and the like instead of comparing
  names.
* The new `Error::DBus` holds a D-Bus error of any type. It is how the `<property>_changed` method
  that `#[interface]` generates reports a failing getter's error, an `fdo::Error` included, which
  5.x reported as `Error::FDO`. `fdo::Error::from(error)`, or `MyError::from(error)` for a
  `#[derive(DBusError)]` type with a `#[zbus(error)]` variant, gets the typed error back.

## What the compatibility module covers

`zbus::zvariant` warns on `use zbus::zvariant;` and on every type alias, function and constant it
contains, such as `zbus::zvariant::Value`. It can't warn on what it re-exports, such as the
traits, the derives and the macros. Those keep resolving silently until 7.0, so search for
`zvariant` rather than relying on the warnings. `zbus::names::Error` and `zbus::names::Result` are
deprecated aliases too.

It does not cover:

* Matching the removed `Error::Variant` and `Error::Names` variants.
* Implementing a trait for both `zbus::Error` and `zbus::zvariant::Error`, now the same type.
* Constructing `DynamicTuple` and `OwnedStructure` through the module, since a type alias is not a
  constructor (`E0423`). Use `zbus::DynamicTuple(..)` and `zbus::OwnedStructure(..)`.
* Matching on `Value::Value`, which is now `Variant::Variant`.
* Naming the old crates, as in `extern crate zvariant;` or `#[zbus(crate = "zvariant")]`.

## Other changes in 6.0

### `Value` is now `Variant`

The type that holds a value of any D-Bus type is now named after the D-Bus `VARIANT` type, which is
what it encodes as. The encoding does not change:

| zbus 5 / zvariant 5 | zbus 6 |
| --- | --- |
| `Value` | `Variant` |
| `OwnedValue` | `OwnedVariant` |
| `Value::Value`, a variant holding a variant | `Variant::Variant` |
| `#[derive(Value)]`, `#[derive(OwnedValue)]` | `#[derive(Variant)]`, `#[derive(OwnedVariant)]` |
| `as_value` | `as_variant` |

`Variant::Value` remains as a deprecated function until 7.0, so `Value::Value(inner)` keeps
compiling as an expression, but a pattern has to use `Variant::Variant`.

### Builder setters defer their errors to `build()`

Builder setters and constructors that convert a `TryInto` argument (a name, a path, a GUID, an
address) return `Self` instead of `Result<Self>`, and `build()` returns the first error any of them
hit. Remove the `?` after:

* the setters of `message::Builder`, `proxy::Builder`, `connection::Builder` and
  `match_rule::Builder`;
* `Message::method_call`, `Message::signal`, `Message::error` and `Message::method_return`;
* `connection::Builder::session`, `system`, `ibus`, `address` and `authenticated_socket`.

`match_rule::Builder::build()`, on the other hand, used to be infallible and needs a `?` now.

### Property methods use Serde traits

Properties are converted through Serde and `Type`, like method arguments, rather than through
`Variant` conversions. Client-side getters need `DeserializeOwned + Type` and setters
`Serialize + Type`; service-side getters need `Serialize + Type` and setters
`Deserialize + Type`. A `#[proxy]` property getter has to return an owned type.

Check the `#[serde(...)]` attributes of your property types, since they now decide the wire format.
In particular, a derived `Serialize` encodes a unit enum as its variant index, not its
discriminant; use `serde_repr` where the discriminants are the D-Bus values. A property of the
wrong type fails with `Error::SignatureMismatch` rather than `Error::IncorrectType`.

### `Builder::runtime` replaces `internal_executor`

`Builder::internal_executor`, `Connection::executor()` and the `Executor` and `Task` types are
gone. A connection runs on Tokio when the `tokio` feature is on and a Tokio runtime is current,
otherwise on [zruntime] (the `default-rt` feature), unless you pass an implementation of
[`runtime::traits::Runtime`][`runtime::traits`] to `Builder::runtime`. A build with neither
`default-rt` nor `tokio` has to pass one, or building a connection returns `Error::Unsupported`.

A connection's `MessageStream`s now end once it is closed.

### The blocking API is gone

`zbus::blocking`, the `*ProxyBlocking` types and the `blocking-api` feature have no equivalent in
6.0. Drive the async API with `zbus::block_on` instead, ideally once around the whole program:

```rust,compile_fail,noplayground
// 5.x
let connection = zbus::blocking::Connection::session()?;
let proxy = FooProxyBlocking::new(&connection)?;
let answer = proxy.bar()?;

// 6.0
let answer = zbus::block_on(async {
    let connection = zbus::Connection::session().await?;
    let proxy = FooProxy::new(&connection).await?;
    proxy.bar().await
})?;
```

A blocking iterator becomes the stream it wrapped, driven with `StreamExt::next`. With the `tokio`
feature, `zbus::block_on` uses a single-threaded runtime, so a connection's work only runs inside
it; the `tokio-multithread` feature gives you the multi-threaded runtime 5.x used. See
[the FAQ](faq.md#how-do-i-use-zbus-from-synchronous-code) for more.

The `gen_blocking`, `blocking_name`, `blocking_object` and `gen_async` attributes of `#[proxy]` are
gone, and `async_object` is now spelled `proxy_type_name`.

### The flag types are `bitflags` types

`message::Flags`, `proxy::MethodFlags` and `fdo::RequestNameFlags` were `enumflags2` enums, used as
`BitFlags<Flags>` and so on. They are [`bitflags`] types now, used without the `BitFlags` wrapper,
and their flags are associated constants: `RequestNameFlags::DoNotQueue` is
`RequestNameFlags::DO_NOT_QUEUE`, and so on.

A single flag is a set already, so drop the `.into()` that turned it into a `BitFlags`. `from_bits`
returns an `Option`, and the types implement neither `Display` nor `TryFrom` of their integer.
Decoding now keeps unknown bits instead of failing.

### The `enumflags2` feature is gone

zbus no longer implements its traits for `enumflags2::BitFlags<F>`, and the orphan rule keeps you
from doing so yourself. Make your set of flags a newtype over the integer instead, with the flags
generated by the [`bitflags`] crate:

```rust,noplayground
use serde::{Deserialize, Serialize};
use zbus::{OwnedVariant, Type, Variant};

// Was:
//
// #[enumflags2::bitflags]
// #[repr(u32)]
// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
// pub enum Permission {
//     Read = 0x1,
//     Write = 0x2,
// }
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash,
    Serialize, Deserialize, Type, Variant, OwnedVariant,
)]
pub struct Permissions(u32);

bitflags::bitflags! {
    impl Permissions: u32 {
        const READ = 0x1;
        const WRITE = 0x2;
    }
}
```

It encodes as the `u32` it wraps, as `BitFlags<Permission>` did. If your enum had a
`#[bitflags(default = ...)]` attribute, implement `Default` by hand. To use the type as a
dictionary key or to have its `Debug` name the flags, see [the FAQ][bit-flags].

### Dictionary keys have to be basic types

The key type of a `HashMap` or `BTreeMap` has to implement `Basic` now, as D-Bus only allows basic
types as keys. A map with any other key, such as a `Vec<u8>`, had an invalid signature and could not
have worked; send a `Vec<(K, V)>` instead. A newtype around a basic type did work, and needs a
`Basic` implementation now:

```rust,noplayground
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use zbus::{Basic, Type};

#[derive(Deserialize, Serialize, Type, PartialEq, Eq, Hash)]
struct Key(u32);

impl Basic for Key {
    const SIGNATURE_CHAR: char = u32::SIGNATURE_CHAR;
    const SIGNATURE_STR: &'static str = u32::SIGNATURE_STR;
}

assert_eq!(HashMap::<Key, u8>::SIGNATURE, "a{uy}");
```

### `zbus_xml` borrows from the document and keeps its order

`Node::try_from(&str)` borrows strings from the document where it can. `Annotation`, `Arg` and the
Telepathy types (`TypeDef`, `SimpleType`, `Enum`, `EnumValue`, `Struct`, `Member`, `Mapping`)
gained a lifetime parameter as a result. Use `into_owned()` to keep a tree past its document.

`Interface::methods()`, `signals()`, `properties()`, `annotations()` and `telepathy_types()` return
iterators rather than slices: `.len()` becomes `.count()`, `[0]` and `.first()` become `.next()`.
The new `Interface::children()` yields the methods, signals, properties and annotations together,
in document order. That is also the order that `Node::to_writer` writes, `PartialEq` compares and
`zbus_xmlgen` generates code in now.

The tree types no longer implement `Serialize` and `Deserialize`, and `zbus_xml::Signature` is
replaced by `zbus::Signature`: drop the `.inner()` and `.into_inner()` calls on what `Arg::ty` and
`Property::ty` return.

### A renamed `zbus` dependency needs the `crate` attribute

The macros no longer find a renamed `zbus` dependency, or one reached through a re-export of
another crate, on their own. Pass its name with the `crate` attribute: `crate = "my_zbus"` on
`#[proxy]`, `#[interface]` and `#[derive(DBusError)]`, and `crate = "my_zbus::wire"` on the wire
derives. Alternatively, put `extern crate my_zbus as zbus;` in your crate root, which also covers
`signature!`.

### The APIs deprecated in 5.x are removed

All of these but `#[zbus(signal_context)]` warned in 5.19:

* `object_server::SignalContext` is `SignalEmitter`, `InterfaceRef::signal_context()` is
  `signal_emitter()` and `#[zbus(signal_context)]` is `#[zbus(signal_emitter)]`.
* `Connection::peer_credentials()` is `peer_creds()`, which returns an
  `&Arc<ConnectionCredentials>` rather than an owned `ConnectionCredentials`.
* `zbus::AuthMechanism` is `zbus::connection::AuthMechanism`.
* `DeserializeValue` and `SerializeValue` are `zbus::as_variant::Deserialize` and `Serialize`.
* `zbus_xmlgen::write_interfaces` and `GenTrait` are `CodeGenerator::file_code` and
  `interface_code`. The generator only formats the code after `with_format(true)`.
* `zbus_xml::Error::QuickXml` and `QuickXmlSer` are gone; match on `Error::Xml` or `Error::Io`.
* `object_server::DispatchResult` is what 5.x called `DispatchResult2`, which is now a deprecated
  alias. It only matters for a hand-written `Interface` implementation: the `Async` future returns
  a `Result<(), BoxDBusError>`, and `Interface::get` and `set_mut` fail with a `BoxDBusError`.

### Smaller changes

* **Stream constructors take an owned socket.** `Builder::unix_stream` takes a
  `std::os::unix::net::UnixStream` (`uds_windows::UnixStream` on Windows) and
  `Builder::tcp_stream` a `std::net::TcpStream`. Convert a Tokio stream with `into_std()` and an
  `async_io::Async<T>` with `into_inner()`; `Builder::socket` doesn't accept those types anymore
  either. On Windows, a Tokio connection can't use unix sockets.
* **The encoding context has no format.** `Context::new(Format::DBus, LE, 0)` is
  `Context::new(LE, 0)`, `Signature::alignment(format)` is `alignment_dbus()` and
  `Basic::alignment(format)` is `alignment()`.
* **`StructureSeed` has no lifetime parameter.**
* **`PropertiesProxy::set` takes a `&Variant`.** `#[interface]` methods and property setters
  accept references to any deserializable type, such as `&Variant<'_>`, and to any slice of one,
  such as `&[u32]`, as arguments now.
* **`DBusProxy::start_service_by_name` returns a `StartServiceReply`** instead of a `u32`.
* **Name types validate when converted from a `Variant`**, failing with `Error::InvalidName`.
* **`Optional<T>` compares with the null value before converting**, so the empty string reads back
  as `None` for the name types. These conversions now need `T::NoneType: Into<Variant>`.
* **`FilePath` converts to std types with `TryFrom`**, which fails off unix for paths that are not
  valid UTF-8. `FilePath::as_c_str()` returns the raw bytes.
* **Queue sizes are `NonZeroUsize`** in `max_queued`, `set_max_queued` and `for_match_rule`.
* **`ObjectServer::at` returns `Result<Option<I>>`**: `Some` hands the interface back if the path
  already had one of that name.
* **`Body::deserialize_unchecked` is `Body::deserialize_ignore_signature`.**
* **`proxy::Builder::build` requires `proxy::Defaults`.**
* **`AsyncDrop` is declared without `async-trait`**, so drop `#[async_trait]` from your
  implementations.
* **`Connection::monitor_activity` and `ResponseDispatchNotifier::new` return zbus types** rather
  than `event_listener::EventListener`. Await them.
* **The xdg-dbus-proxy workarounds are gone.** Flatpak apps need xdg-dbus-proxy 0.1.6 or newer.

## A stale zvariant in the dependency graph

If another crate in your tree still depends on zvariant 5, its `Type`, `Value` and `Signature` are
unrelated to zbus's, which leads to errors such as "expected `zbus::Variant`, found
`zvariant::Value`". Find the crate with `cargo tree -i zvariant`, and upgrade it or keep its types
on its side.

## GVariant

The `gvariant` and `ostree-tests` features and everything behind them moved to the [zgvariant]
crate, where `Context::new_gvariant(LE, 0)` is `zgvariant::serialized::Context::new(LE, 0)`.
zgvariant 2 shares its `Signature` type with zbus 6; zgvariant 1 does not, so pass a signature
between the two as a string.

[breaks]: #what-the-compatibility-module-covers
[value-variant]: #value-is-now-variant
[enumflags2]: #the-enumflags2-feature-is-gone
[blocking-gone]: #the-blocking-api-is-gone
[bit-flags]: faq.md#how-do-i-use-bit-flags
[`bitflags`]: https://docs.rs/bitflags
[`tracing`]: https://docs.rs/tracing
[`runtime::traits`]: https://docs.rs/zbus/latest/zbus/runtime/traits/index.html
[`zbus::wire`]: https://docs.rs/zbus/latest/zbus/wire/index.html
[`zbus::names`]: https://docs.rs/zbus/latest/zbus/names/index.html
[zgvariant]: https://crates.io/crates/zgvariant
[zruntime]: https://docs.rs/zruntime
