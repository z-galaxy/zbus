# Upgrading to zbus 6.0

<!-- toc -->

zbus 6.0 is one dependency where there used to be as many as four. The `zvariant`,
`zvariant_derive` and `zbus_names` crates were merged into `zbus`:

| Was | Is now |
| --- | --- |
| the `zvariant` crate | common types at the `zbus` root; codecs in [`zbus::wire`] |
| the `zvariant_derive` crate | the derives, re-exported from `zbus` |
| the `zbus_names` crate | the [`zbus::names`] module |

The D-Bus API — connections, messages, proxies, the object server, `fdo` — now sits behind a
`comms` Cargo feature that every runtime feature turns on, so `zbus` with its default features
disabled is what `zvariant` and `zbus_names` used to be: the wire format and the name types, no
connection code.

GVariant support, deprecated in zvariant 5.15, is gone; it lives on in the [zgvariant] crate.

## Which part applies to you

* **You depend on `zbus`.** Bump the version. `zbus::zvariant` is still there as a deprecated
  alias module, so almost everything keeps compiling with a warning; the handful of things that
  do break are listed [further down][breaks]. Move common types and derives to the `zbus` root and
  direct encoding and decoding calls to `zbus::wire` at your own pace. The compatibility module
  goes away in 7.0.
* **You depend on `zvariant` and not on `zbus`.** Replace the dependency (below) and rename
  common types and derives from `zvariant::` to `zbus::`; use `zbus::wire::` for direct encoding
  and decoding APIs.
* **You depend on `zbus_names`.** Replace the dependency and rename `zbus_names::` to
  `zbus::names::`.
* **You use the `gvariant` feature.** Move to [zgvariant].

## Cargo.toml

zbus users change the version and nothing else:

```toml
[dependencies]
zbus = "6"
```

The `default-features = false, features = ["tokio"]` idiom needs one more feature or two:
`tokio` enables `comms`, the D-Bus connection layer, but the client-side proxy API and the
service-side object server API are now behind the `proxy` and `service` features (see
[below](#proxy-and-service-api-are-separate-features)):

```toml
# Before
[dependencies]
zbus = { version = "5", default-features = false, features = ["tokio"] }

# After
[dependencies]
zbus = { version = "6", default-features = false, features = ["tokio", "proxy", "service"] }
```

Wire-format-only users replace the crate. Every `zvariant` feature kept its name, except
`gvariant` and `ostree-tests`, which are gone:

```toml
# Before
[dependencies]
zvariant = { version = "5", default-features = false, features = ["serde_bytes"] }

# After
[dependencies]
zbus = { version = "6", default-features = false, features = ["serde_bytes"] }
```

and `zbus_names` users likewise:

```toml
# Before
[dependencies]
zbus_names = "4"

# After
[dependencies]
zbus = { version = "6", default-features = false }
```

Four things to know about the features:

* `enumflags2`: zbus and zbus_names both asked `zvariant` for it, so anything with either in
  its graph got `BitFlags<F>: Type` without asking. `comms` still enables it, so a full zbus
  build is unchanged; a `default-features = false` build has to opt in with
  `features = ["enumflags2"]`. The feature also covers more than it did: `BitFlags<F>` converts
  into a `Value` now, not just out of one.
* `arrayvec` was a zvariant-only feature and is available in zbus now.
* `comms` pulls in the `uuid` crate — it parses D-Bus GUIDs — without turning on zbus's own
  `uuid` feature, so the `Uuid` wire impls stay opt-in, as they were.
* Any D-Bus feature (`builtin-runtime`, `tokio`, `p2p`, `bus-impl`, `vsock`)
  enables `comms`. In a workspace where one crate asks for the wire-only build
  and another for the full one, Cargo's feature unification gives everybody the full build.
  That is a build-size question only; nothing behaves differently.

A crate that depends on `zbus_macros` directly keeps `proxy`, `interface` and `DBusError`
without doing anything: `comms`, `proxy` and `service` are default features there. Only a
direct dependency that had turned the (previously empty) defaults off *and* uses those macros
needs `features = ["proxy"]` and/or `features = ["service"]` now (`DBusError` alone needs
`comms`), or drops the `default-features = false`; the wire-format derives and `signature!` never
needed it. zbus's own dependency on the macro crate is such a defaults-off one, and its `comms`,
`proxy` and `service` features switch the macro crate's back on.

## Paths

| zbus 5 / zvariant 5 / zbus_names 4 | zbus 6 |
| --- | --- |
| Common `zvariant::X`, `zbus::zvariant::X` types and derives | `zbus::X` |
| `zvariant::serialized::Context` | `zbus::wire::serialized::Context` |
| `zvariant::as_value`, `zvariant::dbus` | `zbus::as_value`, `zbus::wire::dbus` |
| `zvariant::signature!` | `zbus::signature!` |
| `zbus_names::X`, `zbus::names::X` | `zbus::names::X` |
| `zvariant::Error`, `zvariant::Result` | `zbus::Error`, `zbus::Result` |
| `zbus_names::Error`, `zbus::names::Error` | `zbus::Error` |
| `zvariant::MaxDepthExceeded` | `zbus::MaxDepthExceeded` |
| `#[zvariant(...)]` on a derive | `#[zbus(...)]`; both spellings stay accepted |
| `#[zvariant(crate = "zvariant")]` | `#[zbus(crate = "zbus::wire")]` |

Encoding and decoding functions, `serialized`, `Endian` and its constants, `DynamicDeserialize`,
`StructureBuilder`, the `*Seed` types, and `signature::{Child, Fields}` remain under `zbus::wire`.
Use `zbus::Structure::builder()` to construct structures without importing `StructureBuilder`.

On these derives the `crate` attribute names the module that holds the wire types, so
point it at `zbus::wire`.
Pointing it at `zbus::zvariant` works but routes the generated code through the deprecated
module.

After the rename the wire API reads like this:

```rust,noplayground
use serde::{Deserialize, Serialize};
use zbus::{Type, wire::{serialized::Context, to_bytes, LE}};

#[derive(Deserialize, Serialize, Type, PartialEq, Debug)]
struct Struct<'s> {
    field1: u16,
    field2: i64,
    field3: &'s str,
}

assert_eq!(Struct::SIGNATURE, "(qxs)");

let ctxt = Context::new(LE, 0);
let s = Struct { field1: 42, field2: i64::MAX, field3: "hello" };
let encoded = to_bytes(ctxt, &s).unwrap();
let decoded: Struct<'_> = encoded.deserialize().unwrap().0;
assert_eq!(decoded, s);
```

and the name types like this:

```rust,noplayground
use zbus::names::{InterfaceName, UniqueName};

let interface = InterfaceName::try_from("org.freedesktop.DBus").unwrap();
assert_eq!(interface, "org.freedesktop.DBus");

// A unique name has to start with a colon.
UniqueName::try_from("not.unique").unwrap_err();
```

## Errors

There is one error type now. `zvariant::Error` and `zbus_names::Error` are gone, and so are the
two `zbus::Error` variants that used to wrap them:

```rust,compile_fail,noplayground
use zbus::Error;

fn describe(error: &Error) -> String {
    match error {
        // Neither variant exists in zbus 6.
        Error::Variant(e) => e.to_string(),
        Error::Names(e) => e.to_string(),
        _ => error.to_string(),
    }
}
```

Their contents are variants of `zbus::Error` itself:

```rust,noplayground
use zbus::Error;

fn describe(error: &Error) -> String {
    match error {
        Error::IncorrectType => "a value had the wrong type".to_string(),
        Error::SignatureMismatch(signature, expected) => {
            format!("got {signature}, expected {expected}")
        }
        Error::InvalidObjectPath => "not a valid object path".to_string(),
        Error::InvalidName(reason) => (*reason).to_string(),
        Error::InvalidNameConversion { from, to } => format!("cannot convert {from} to {to}"),
        _ => error.to_string(),
    }
}
```

`?` keeps working everywhere: a function that returned `zvariant::Result<T>` now returns
`zbus::Result<T>`, and the conversion that used to happen has become an identity.

Five details that a `match` or a log line can notice:

* `zvariant::Error::Message(s)` — what serde's `Error::custom` produced — is now
  `zbus::Error::Failure(s)`, which is where zbus already collected such errors.
* `Error::InputOutput` prints as `I/O error: <the io::Error>`, where `zvariant::Error` printed
  the inner error on its own. zbus has always prefixed it and its rendering is the one that
  survived, so this is the one change a log line or a string comparison can see at runtime.
* `Error::MissingFramingOffset` is gone with the rest of GVariant, and so is
  `Error::IncompatibleFormat`: with a single wire format left, nothing can be incompatible with
  it.
* `Error::Connection` carries a `Box<Address>` where zbus 5 inlined the `Address`. `Display` is
  unchanged, but `Error::Connection(_, addr)` now binds a box; dereference it where you need the
  `Address` itself.
* `Error::MethodError`, `Error::FDO` and `Error::Connection` only exist with `comms` enabled.
  A `match` in a wire-only crate cannot name them. `Error` is `#[non_exhaustive]`, so the
  wildcard arm you already need covers them.

Flattening also shows in two places no `match` arm mentions. `Error::Failure("x")` and what
used to be `Error::Variant(zvariant::Error::Message("x"))` are one and the same value now, so
code that relied on those two comparing unequal finds them equal. And `Error::source()` is one
link shorter: the `Variant` and `Names` wrappers used to report their inner error as the source,
where each flattened variant reports what that inner error reported — the `io::Error` behind
`InputOutput`, nothing at all behind `Failure`.

On the names side, the seven `Invalid*Name` variants that `zbus_names` 4.1 deprecated
(`InvalidBusName`, `InvalidWellKnownName`, `InvalidUniqueName`, `InvalidInterfaceName`,
`InvalidMemberName`, `InvalidPropertyName` and `InvalidErrorName`) were dropped rather than
carried over. Nothing had returned them since 4.1; `Error::InvalidName` is what you get.
Code that still spells `zbus::names::Error` or `zbus::names::Result` keeps working: both are
aliases of the root types now. Being aliases, they collide the way `zvariant::Error` does: a
crate that implements `From<zbus::Error>` and `From<zbus::names::Error>` for its own error type
is writing the same impl twice, which the compiler rejects with E0119 — the duplicate-`From`
breakage shown just below.

`zbus_xml` collapsed its error type the same way. Its 5.x `Error` had a `Variant(zvariant::Error)`
variant and a `Name(zbus_names::Error)` variant; 6.0 has a single `Zbus(zbus::Error)` variant
instead. Code matching on either of the old arms changes to match `Zbus(_)`, and `?` conversions
into `zbus_xml`'s `Error` keep working unchanged.

One thing to watch for. If your own error type implemented `From` for both of them:

```rust,compile_fail,noplayground
# #![allow(deprecated)]
struct MyError;

impl From<zbus::Error> for MyError {
    fn from(_: zbus::Error) -> Self {
        MyError
    }
}

// This is what `impl From<zvariant::Error> for MyError` became: the same impl, twice.
impl From<zbus::zvariant::Error> for MyError {
    fn from(_: zbus::zvariant::Error) -> Self {
        MyError
    }
}
```

the compiler now rejects the duplicate. Delete the `zvariant` one.

## What warns, what is silent, what breaks

`zbus::zvariant` is a deprecated module of type aliases, wrapper functions, constants and
re-exports. It warns on:

* `use zbus::zvariant;` and `use zbus::{zvariant, Connection};` — the idiom most zbus code uses.
* Every type alias, function and constant reached through it: `zbus::zvariant::Value`,
  `zbus::zvariant::to_bytes`, `zbus::zvariant::LE` and friends, and the `serialized` and
  `signature` submodules of aliases.

Separately, `zbus::names::Error` and `zbus::names::Result` are deprecated aliases of the root
types, also removed in 7.0.

It stays silent on — Rust cannot attach a deprecation to a `pub use`:

* The traits `Type`, `Basic`, `DynamicType`, `DynamicDeserialize`, `NoneValue`, `ReadBytes` and
  `WriteBytes`.
* The derives `Type`, `Value`, `OwnedValue`, `SerializeDict` and `DeserializeDict`, and the
  `signature!`, `impl_type_with_repr` and `static_str_type` macros.
* The `as_value`, `dbus` and `export` submodules, `DeserializeValue`, `SerializeValue`,
  `to_writer` and `to_writer_for_signature`.

A single `use` line can straddle both lists: `use zbus::zvariant::{OwnedValue, Type, Value};`
warns for the deprecated `OwnedValue` and `Value` aliases and stays silent for the `Type`
derive.

Silent is not the same as unchanged: those paths keep resolving until 7.0 removes the module, so
grep for `zvariant` rather than trusting the warnings to find every site.

And the module does not cover:

* Matching the removed `zbus::Error::Variant(_)` and `zbus::Error::Names(_)` variants.
* Implementing a trait for `zbus::Error` and for `zbus::zvariant::Error`: they are one type
  now, so the second impl is a duplicate (the `From` example above).
* Calling a tuple-struct constructor through an alias. A type alias names a type, not its
  constructor function, so this is `E0423`:

  ```rust,compile_fail,noplayground
  # #![allow(deprecated)]
  let _ = zbus::zvariant::DynamicTuple((1u32, "a"));
  ```

  Spell it `zbus::DynamicTuple((1u32, "a"))`. `DynamicTuple` and `OwnedStructure` are the
  two aliased types this bites; `zbus::as_value::Serialize` also has a public tuple field
  but is re-exported rather than aliased, so it constructs fine either way.
* Anything that named the crates themselves: `extern crate zvariant;`, a `zvariant = "5"`
  dependency, `#[zbus(crate = "zvariant")]`.

The compatibility module is removed in zbus 7.0.

## Other changes in 6.0

More things break in 6.0 without being a consequence of the crate merge. They reach code
that never mentioned `zvariant` or `zbus_names`.

### Builder setters defer their errors to `build()`

Every builder setter generic over `TryInto` (a D-Bus name, an object path, a GUID, an address)
used to convert its argument on the spot and return `Result<Self>`, forcing a `?` after every
call in the chain even when the argument was an already-typed value that could not fail to
convert. Setters now return `Self` and the builder records the first error one of them hits;
`build()` returns that error, so a whole chain built from string arguments needs exactly one
`?`, at the end. Nothing clears a recorded error, so calling a setter again with a valid value
does not rescue a chain that has already passed an invalid one.

Before, this no longer compiles:

```rust,compile_fail,noplayground
use zbus::Message;

fn make_message() -> zbus::Result<Message> {
    Message::method_call("/org/zbus/Test", "Test")?
        .destination("org.zbus.Test")?
        .build(&())
}
```

After:

```rust,noplayground
use zbus::Message;

fn make_message() -> zbus::Result<Message> {
    Message::method_call("/org/zbus/Test", "Test")
        .destination("org.zbus.Test")
        .build(&())
}
```

This covers `message::Builder` and the `Message::method_call`, `Message::signal`,
`Message::error` and `Message::method_return` constructors that create it; `proxy::Builder`; and
`connection::Builder` and its `session`, `system`, `ibus`, `address` and `authenticated_socket`
constructors. `match_rule::Builder::build()` moves the other way: it used to be infallible and
now returns `Result<MatchRule<'_>>`, since it is the one builder whose fields were already fully
validated by the time `build()` ran.

To migrate:

* Remove the `?` (or `.unwrap()`/`.expect(...)`) after every builder setter call.
* Remove the `?` after `Message::method_call`, `Message::signal`, `Message::error` and
  `Message::method_return`.
* Remove the `?` after the `connection::Builder` constructors (`session`, `system`, `ibus`,
  `address`, `authenticated_socket`) and after `server`, `serve_at`, `name` and `unique_name`.
* Add a `?` after `MatchRule::builder()...build()`, which is now fallible.

| Before | After |
| --- | --- |
| `.path("/org/zbus/Foo")?` | `.path("/org/zbus/Foo")` |
| `Message::method_call("/", "Ping")?` | `Message::method_call("/", "Ping")` |
| `MatchRule::builder().build()` | `MatchRule::builder().build()?` |

A stale `?` left after a setter fails to compile with "the `?` operator can only be applied to
values that implement `Try`", pointing straight at the call site to clean up. A missing `?`
after `MatchRule::builder()...build()` fails the opposite way, with a type mismatch between
`MatchRule` and whatever type the rest of the function expected. Both are one-line fixes.

### Property methods use Serde traits

Property APIs now use the same Serde traits and `Type` bounds as regular methods. Client-side
getters require `DeserializeOwned + Type` and setters require `Serialize + Type`; service-side
getters require `Serialize + Type` and setters require `Deserialize + Type`. Replace custom
`Value` and `OwnedValue` conversions with the appropriate Serde traits and `Type`.

Property getters declared with `#[proxy]` must use owned result types. A proxy generated from an
interface getter with a borrowing result is generic over its owned result type; select a
`DeserializeOwned + Type` representation at the call site.

Serde now also determines the property's wire representation. Audit `#[serde(...)]` attributes
before upgrading because they were not used by the old `Value` conversions. In particular, an
ordinary Serde derive serializes a unit enum as its variant index, not an explicit Rust
discriminant. Use `serde_repr` for an integer enum whose discriminants are its D-Bus values.

A property type mismatch now returns `Error::SignatureMismatch` rather than
`Error::IncorrectType`. The new error includes both the actual and expected signatures.

### Stream constructors take an owned socket

The stream constructors on `connection::Builder` no longer change their parameter type when Cargo
features are unified: each one takes the socket the platform owns, and the connection drives it on
whichever runtime it was built with. Hand a stream of another kind over as the socket it wraps.

- `Builder::unix_stream` takes a `std::os::unix::net::UnixStream` (`uds_windows::UnixStream` on
  Windows). A `tokio::net::UnixStream` becomes one with `into_std()`; an `async_io::Async<T>`
  becomes one with `into_inner()`. Tokio cannot watch a unix socket on Windows, so a Tokio
  connection there can no longer use one — through this constructor or through a `unix:` address
  — even where the `builtin-runtime` feature is also on. Give such a connection a TCP or
  `autolaunch:` address instead.
- `Builder::tcp_stream` takes a `std::net::TcpStream`. A `tokio::net::TcpStream` becomes one with
  `into_std()`; an `async_io::Async<T>` becomes one with `into_inner()`.
- `Builder::vsock_stream` takes a `vsock::VsockStream`. It serves a Tokio connection too, so the
  `tokio-vsock` feature and the constructor that took its stream are both gone; `vsock` no longer
  enables `async-io` either.

`Builder::socket` is likewise no longer a way to bring a runtime's own socket type along: the
`Socket`, `ReadHalf` and `WriteHalf` implementations for `async_io::Async<T>` and for Tokio's
stream types are gone. Implement `Socket` for a transport that is none of the three above, such
as an in-process channel or a tunnel of your own.

### The encoding context has no format

`zbus::wire` speaks one format, so `serialized::Context` no longer says which:

```rust,noplayground
use zbus::wire::{serialized::Context, to_bytes, LE};

// Was `Context::new(Format::DBus, LE, 0)`.
let ctxt = Context::new(LE, 0);
let encoded = to_bytes(ctxt, &"hello").unwrap();
assert_eq!(encoded.len(), 10);
```

`Context::new_dbus` is a deprecated alias of `Context::new`, removed in 7.0. `Context::format()`
is gone, and so is the enum it returned: `zbus::wire::serialized::Format`, along with its
`zbus::zvariant::serialized::Format` alias.

Two signatures lose the argument with it. `Signature::alignment(format)` is
`Signature::alignment_dbus()` — plus `Signature::alignment_gvariant()`, behind `zbus_utils`'s
`gvariant` feature, for whoever needs the other rules. And `Basic::alignment(format)` is
`Basic::alignment()`; its default body covers every type zbus can encode, so that one reaches you
only through a direct `T::alignment(..)` call or an `impl Basic` that overrode it.

### `PropertiesProxy::set` takes a `&Value`

The `org.freedesktop.DBus.Properties` proxy — `zbus::fdo::PropertiesProxy` — takes the new value
by reference:

```rust,noplayground
use zbus::{fdo::PropertiesProxy, names::InterfaceName, Value};

async fn mute(proxy: &PropertiesProxy<'_>, iface: InterfaceName<'_>) -> zbus::fdo::Result<()> {
    // Was: proxy.set(iface, "Muted", Value::from(true)).await
    proxy.set(iface, "Muted", &Value::from(true)).await
}
```

`Proxy::set_property` and the setters that `#[proxy]` generates still take the value by value, so
only code that drives the `Properties` interface by hand needs the `&`.

The macro change behind it widens what an `#[interface]` method may take: an argument that is a
reference to anything other than `str` is deserialized as the owned type and then handed to the
method by reference, so `&Value<'_>`, `&Str<'_>` and the other borrowed wire types work as
arguments now. What stops compiling is `&[u8]` (or any other unsized `&[T]`), which used to
deserialize as a borrowed slice and now asks for an unsized `[u8]`; take `Vec<u8>`, or `&Vec<u8>`
to keep the reference. Property setters are unaffected: their value already arrived as a `Value`
to convert.

### Name types validate when converted from a `Value`

`TryFrom<Value>` and `TryFrom<OwnedValue>` for `ErrorName`, `InterfaceName`, `MemberName`,
`PropertyName`, `UniqueName`, `WellKnownName` and their `Owned*` siblings run the validator that
`TryFrom<&str>` has always run, and return `Error::InvalidName` when it fails. In 5.x they wrapped
the string as it came, so a malformed name off the wire became a typed name and surfaced later —
as a method call with an empty destination, say. `BusName` and `OwnedBusName` already validated.

```rust,noplayground
use zbus::{names::UniqueName, Optional, Value};

// Accepted in 5.x, an `Error::InvalidName` now.
UniqueName::try_from(Value::from("not.unique")).unwrap_err();

// The empty string is D-Bus's "no name" sentinel, so it still reads back as `None`.
let name = Optional::<UniqueName<'_>>::try_from(Value::from("")).unwrap();
assert!(Option::<UniqueName<'_>>::from(name).is_none());
```

A property getter typed as a name can therefore fail where it used to hand back a bogus name.

### `zbus_xml` borrows from the document it parses

`zbus_xml` no longer copies every string out of the introspection document.
`Node::try_from(&str)` now borrows names, annotation values, argument names and Telepathy
docstrings from the input where it can; the lifetime that `Node<'a>`, `Interface<'a>` and
friends always carried is now meaningful.

Consequently `Annotation`, `Arg` and the `telepathy` type-definition types (`TypeDef`,
`SimpleType`, `Enum`, `EnumValue`, `Struct`, `Member`, `Mapping`) gained a lifetime parameter.
Code that names them in a type position needs `<'_>` (or a named lifetime); code that only
calls accessors keeps working, since they still return `&str`. A function that hands out a
borrow from one of them now has two lifetimes to choose from, so it has to name the one it
borrows from:

```rust,noplayground
use zbus_xml::{Arg, Node};

// Was: fn describe(arg: &Arg) -> Option<&str>
fn describe<'a>(arg: &'a Arg<'_>) -> Option<&'a str> {
    arg.name()
}

// Keep the tree past the document it was parsed from.
fn parse(xml: &str) -> zbus_xml::Result<Node<'static>> {
    Ok(Node::try_from(xml)?.into_owned())
}
```

`Node::from_reader` and `Node::from_reader_with_warnings` return `Node<'static>`: they read the
whole input into memory, so the tree owns its strings. Callers that bound the result to
`Node<'a>` keep compiling.

To keep a borrowed tree (or part of it) past the document, use the new
`into_owned()`/`to_owned()` methods on every tree type, mirroring `zbus::names` types. Strings
in the tree are `zbus::Str`, the same type the name types wrap, so cloning an owned tree is
cheap.

Two leftovers of the quick-xml parser that 5.2 replaced are gone too. The tree types no longer
implement `Serialize` and `Deserialize` (the derives described quick-xml's `@name` attribute
convention, not a format anyone writes), and `zbus_xml::Signature` — a newtype that only
existed to be deserialized from an owned string — is replaced by `zbus::Signature` itself.
`Arg::ty`, `Property::ty` and the Telepathy `ty()` accessors return `&zbus::Signature`, so
drop any `.inner()` or `.into_inner()` call on their result.

### `Optional<T>` compares the sentinel before converting

`TryFrom<Value>` and `TryFrom<OwnedValue>` for `Optional<T>` check the incoming value against
`T::null_value()` first and convert only when it does not match. Their bound moved from
`T: PartialEq<<T as NoneValue>::NoneType>` to `<T as NoneValue>::NoneType: Into<Value<'_>>`. The
types you would normally put in an `Optional` — strings, numbers, `BitFlags`, the name types —
satisfy it; a `NoneValue` implementation of your own does not automatically, and loses these two
conversions if its `NoneType` has no `Into<Value>`.

The order matters for any `T` whose conversion validates its input. The name types above reject
the empty string, and the sentinel now maps to `None` without being converted at all.

The public `NoneValue::NoneType` associated type for each owned name type is now `String` rather
than `&'static str`. This allows `Optional<Owned*Name>` to implement `DeserializeOwned` without
changing its wire encoding.

### `FilePath` converts back to std types with `TryFrom`

`zbus::FilePath` wraps a nul-terminated byte array (D-Bus signature `ay`), because a file path
need not be valid UTF-8. Its conversions back to the std path types used to assume otherwise:
`From<FilePath> for OsString` built the `OsString` with `OsString::from_encoded_bytes_unchecked`,
undefined behaviour on Windows for bytes that are not valid WTF-8; `From<&FilePath> for &Path`
panicked on a non-UTF-8 path, the case the type exists for; and `From<FilePath> for PathBuf` went
through `to_string_lossy()`, silently replacing invalid bytes with `U+FFFD`.

The four conversions — to `OsString`, `PathBuf`, `&Path` and, new, `&OsStr` — are `TryFrom`
impls now, with `type Error = zbus::Error` on every platform. On unix, where `OsStr` is a byte
string like `FilePath` itself, they hand back the original bytes and never fail. Elsewhere no
lossless mapping to `OsStr` exists, so they succeed only when the bytes are valid UTF-8 and return
`Error::Utf8` otherwise.

Before, this no longer compiles:

```rust,compile_fail,noplayground
use std::path::PathBuf;
use zbus::FilePath;

fn path_buf(file_path: FilePath<'_>) -> PathBuf {
    PathBuf::from(file_path)
}
```

After:

```rust,noplayground
use std::path::PathBuf;
use zbus::FilePath;

fn path_buf(file_path: FilePath<'_>) -> zbus::Result<PathBuf> {
    PathBuf::try_from(file_path)
}
```

The same applies to `OsString::from(file_path)`, `<&Path>::from(&file_path)` and the matching
`.into()` calls: spell them `try_from` or `try_into` and handle the `Result`. On unix the error
arm is unreachable in practice, but the signature is the same everywhere so that portable code
has one form to write.

Code that needs the raw path off unix has `FilePath::as_c_str()`, which is new: it returns the
nul-terminated bytes as they are on every platform, and `FilePath` implements `AsRef<CStr>` to
match.

Conversions into `FilePath` — `From<&Path>`, `From<PathBuf>`, `From<&OsStr>`, `From<OsString>`,
`From<&CStr>`, `From<CString>`, `From<&str>` and so on — are unchanged, and
`FilePath::to_string_lossy()` remains the explicit lossy option.

### Proxy and service API are separate features

The client-side proxy API (`zbus::proxy`, `zbus::Proxy`, `#[proxy]`, the `fdo::*Proxy` types)
and the service-side object server API (`zbus::object_server`, `zbus::ObjectServer`,
`#[interface]`, `Connection::object_server`, `Builder::serve_at`) are behind the `proxy` and
`service` features respectively. Both are default features, so a plain `zbus = "6"` dependency
is unaffected. A `default-features = false` build has to ask for the half it uses:

```toml
[dependencies]
# A pure client.
zbus = { version = "6", default-features = false, features = ["tokio", "proxy"] }
# A pure service.
zbus = { version = "6", default-features = false, features = ["tokio", "service"] }
```

Leaving out the half you don't use keeps its code out of your binary, which even fat LTO could
not do before. Everything else in the D-Bus API (`Connection`, `Message`, `MessageStream`,
`MatchRule`, the plain `fdo` types and errors, `Connection::request_name`) needs neither.

### The `unixexec` and `ibus` transports are features

The `unixexec:` and `ibus:` address transports run a command to reach the bus, which pulls the
async runtime's process support into every binary. They are behind the `unixexec` and `ibus`
features now, both default features, so a plain `zbus = "6"` dependency keeps them. A
`default-features = false` build that connects through either has to name it:

```toml
[dependencies]
zbus = { version = "6", default-features = false, features = ["tokio", "proxy", "unixexec"] }
```

`Transport::Unixexec`, `Transport::Ibus`, `transport::Unixexec`, `transport::Ibus` and
`connection::Builder::ibus` exist only with their feature, and `Address::from_str` rejects the
address of a transport that was left out.

### The service-side `ObjectManager` is a feature

The object server emits `InterfacesAdded` and `InterfacesRemoved` on behalf of any
[`fdo::ObjectManager`] registered above an object, which links that interface, its signal
emission and the property gathering behind it into every service. That is behind the new
`object-manager` feature, a default feature which implies `service`, so a plain `zbus = "6"`
dependency keeps it. A `default-features = false` build that registers an `ObjectManager` has to
name it:

```toml
[dependencies]
zbus = { version = "6", default-features = false, features = ["tokio", "object-manager"] }
```

`fdo::ObjectManager` exists only with the feature. The `ObjectManagerProxy` and its signal
streams only need `proxy`, as before.

[`fdo::ObjectManager`]: https://docs.rs/zbus/6/zbus/fdo/struct.ObjectManager.html

### `Builder::runtime` replaces `internal_executor`

A connection used to pick its executor by cargo feature, with
`Builder::internal_executor(false)` and `Connection::executor().tick()` as the way to drive
zbus's tasks from another runtime. That pair is gone, together with the `Executor` and `Task`
types. A connection takes its readiness, its timers, its internal tasks and its blocking work
from one runtime: Tokio when the `tokio` feature is on and a runtime is current, otherwise the
runtime zbus brings along (the `builtin-runtime` feature, on by default), or whatever you pass
to `Builder::runtime`:

```rust,no_run
use zbus::{Connection, Result, connection::Builder, runtime::traits::Runtime};

async fn connect(runtime: impl Runtime) -> Result<Connection> {
    Builder::session().runtime(runtime).build().await
}
```

5.x's `async-io` feature has no 6.0 equivalent by that name — it's gone outright, with no
compatibility alias. `builtin-runtime` is the default, so a plain `zbus = "6"` dependency
carries it without asking by name; a `default-features = false` build that named `async-io`
explicitly drops that name or renames it to `builtin-runtime`. The backend behind it changed
too: 5.x's feature pulled in the `async-io`, `async-executor`, `async-task` and `blocking`
crates, whereas `builtin-runtime` is zbus's own reactor and task scheduler — one runtime per
process, driven by the thread inside `zbus::block_on` — and depends on none of them.

An implementation of `zbus::runtime::traits::Runtime` supplies a readiness registration, a timer
and task spawning; every async runtime already has all three. Every socket a connection owns goes
through the same registration, and so does the helper process behind a `unixexec:`, `ibus:` or
`launchd:` address: it is now spawned with `std::process::Command` rather than an async runtime's
own process support, so `async-process` is no longer a dependency. A handful of calls that have no
async form — a `tcp:` host-name lookup, a `nonce-tcp:` file read, the peer-credential group lookup,
waiting for a helper process to exit — go through the trait's `spawn_blocking`, which defaults to a
thread of its own per call. zbus never drives the runtime, so the tasks a connection spawns only
run while the runtime runs them. The two built-in backends are implementations of the same trait,
picked by cargo feature, so this method is for the runtime your application already has. Nothing
changes for connections built without `runtime`.

zbus's own async locks are not part of the trait: 5.x takes them from the `async_lock` crate;
6.0 has no dependency on it at all, building its locks itself, on `event-listener`, except when
the `tokio` feature is enabled, where Tokio's locks stand in instead. Neither choice needs a
cargo feature of its own, so a build with `comms` but neither `builtin-runtime` nor `tokio`
pulls in no lock crate for them.

A runtime may also abort or drop a task it was given, so the connection no longer relies on its
socket-reader task running to its end: however that task stops, `Connection::closed()` resolves,
pending method calls fail and every `MessageStream` on the connection ends. A stream therefore
also ends once `Connection::close()` has been called, after yielding whatever it already held.

A build with `comms` but neither `builtin-runtime` nor `tokio` is valid, something 5.x has no
equivalent of, and needs no lock feature for zbus's locks; every connection in it needs a
`runtime`, over an address or over a socket you supply.

The examples rendered throughout this book and zbus's API documentation connect to a bus without
naming a runtime, so they need one of the built-in backends and report `Error::Unsupported` in a
build that has neither.

### The blocking API is gone

5.x's `zbus::blocking` module — `blocking::Connection`, `blocking::Proxy`, the
`*ProxyBlocking` types `#[proxy]` generated, `blocking::ObjectServer` and the signal, property
and message iterators — and its `blocking-api` cargo feature have no 6.0 equivalent. A program
without an async runtime of its own drives the async API with `zbus::block_on`, which on the
default `builtin-runtime` feature also runs the connection's work on the calling thread:

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

One `zbus::block_on` around the whole program is the shape to prefer, because a call returning
with the connection alive leaves its work to a helper thread until the next call. A blocking
iterator becomes the stream it wrapped, driven with `StreamExt::next` inside the future. The
`#[proxy]` attributes `gen_blocking`, `blocking_name` and `blocking_object` are gone with the
proxies they configured, and so is `gen_async`, since the async proxy is the only one;
`async_name` and `async_object` stay. Two rules come with `zbus::block_on`: the future must not
block the thread waiting for work the connection has to do, and the function must not be called
from inside a task zbus is running, where it panics. See the [synchronous programs
chapter](blocking.md).

### Logging through `tracing` is a feature

zbus's log events and spans go through [`tracing`], which is now behind the `tracing`
feature. It is a default feature, so a plain `zbus = "6"` dependency is unaffected. A
`default-features = false` build that wants zbus's logs has to name it:

```toml
[dependencies]
zbus = { version = "6", default-features = false, features = ["tokio", "proxy", "tracing"] }
```

With the feature off, zbus emits no log events or spans at all, and it no longer enables
`tokio`'s own `tracing` feature either.

[`tracing`]: https://docs.rs/tracing

### A proxy without properties has no properties cache

`proxy::Defaults` has a new constant, `HAS_PROPERTIES`, which `#[proxy]` sets from the trait,
and `proxy::Builder::build` requires the trait. A proxy whose interface has no properties never
sets up the properties cache, whatever the `CacheProperties` setting, so a program whose proxies
have none carries no cache code at all. A hand-written `Defaults` implementation can leave the
constant at its default of `true`.

### The xdg-dbus-proxy workarounds are gone

zbus 5 checked the `FLATPAK_ID` environment variable and, when it was set, worked around two bugs in
xdg-dbus-proxy, the proxy Flatpak puts between a sandboxed app and the bus: it did not pipeline the
Unix FD negotiation during the handshake ([xdg-dbus-proxy#21]) and it serialized message creation
and sending behind a global lock ([xdg-dbus-proxy#46]). Both were fixed in xdg-dbus-proxy 0.1.6,
released in 2024. zbus 6 drops the workarounds and stops special-casing Flatpak altogether, so a
sandboxed app gets the same pipelined handshake and lock-free sending as everything else.

If your app runs in a Flatpak whose runtime still ships an older xdg-dbus-proxy, connections can
fail or messages can come out mis-serialized. Update the runtime, get the fix backported, or stay on
zbus 5 until you can.

[xdg-dbus-proxy#21]: https://github.com/flatpak/xdg-dbus-proxy/issues/21
[xdg-dbus-proxy#46]: https://github.com/flatpak/xdg-dbus-proxy/issues/46

## A stale zvariant in the dependency graph

If another crate in your tree still depends on zvariant 5, your build contains two unrelated
`Type`, `Value` and `Signature` types, and the resulting errors ("expected `zbus::Value`,
found `zvariant::Value`") are confusing. `Signature` is duplicated like the other two: zbus 6
re-exports it from `zbus_utils`, which no zvariant 5 release uses. Find the culprit with:

```bash
cargo tree -i zvariant
```

Nothing breaks — the two coexist — but values cannot cross from one to the other. Upgrade that
crate, or keep using its `zvariant` types on its side of the boundary until it migrates.

## GVariant

The `gvariant` and `ostree-tests` Cargo features and everything behind them — `Value::Maybe`,
`Context::new_gvariant`, the GVariant serializer and deserializer — were removed in 6.0. That
code, and its test suite, live in the [zgvariant] crate now:

```toml
[dependencies]
zgvariant = "1"
```

zgvariant only speaks GVariant, so, like zbus, it has no format to choose: what was
`zvariant::serialized::Context::new_gvariant(LE, 0)` is `zgvariant::serialized::Context::new(LE, 0)`
there. The rest reads the same.

zgvariant 2.0 and later share their `Signature` type with zbus 6 — both re-export it from
`zbus_utils` — so a signature parsed by one is the same value in the other. zgvariant 1.x
re-exports it from `zvariant_utils` instead, which makes it a second, unrelated type in a zbus 6
build; move a signature across that boundary through its string form.

[breaks]: #what-warns-what-is-silent-what-breaks
[`zbus::wire`]: https://docs.rs/zbus/latest/zbus/wire/index.html
[`zbus::names`]: https://docs.rs/zbus/latest/zbus/names/index.html
[zgvariant]: https://crates.io/crates/zgvariant
