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
* Any D-Bus feature (`async-io`, `tokio`, `blocking-api`, `p2p`, `bus-impl`, `vsock`,
  `tokio-vsock`) enables `comms`. In a workspace where one crate asks for the wire-only build
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

Seven more things break in 6.0 without being a consequence of the crate merge. They reach code
that never mentioned `zvariant` or `zbus_names`.

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

### Stream constructors name their I/O backend

The stream constructors on `connection::Builder` no longer change their parameter type when Cargo
features are unified. `unix_stream`, `tcp_stream` and `vsock_stream` are gone; choose the
constructor that matches the stream instead:

- `tokio::net::UnixStream`: `Builder::unix_stream` becomes `Builder::tokio_unix_stream` with
  `tokio`.
- `std::os::unix::net::UnixStream` or `uds_windows::UnixStream`: `Builder::unix_stream` becomes
  `Builder::async_io_unix_stream` with `async-io`.
- `tokio::net::TcpStream`: `Builder::tcp_stream` becomes `Builder::tokio_tcp_stream` with `tokio`.
- `std::net::TcpStream`: `Builder::tcp_stream` becomes `Builder::async_io_tcp_stream` with
  `async-io`.
- `tokio_vsock::VsockStream`: `Builder::vsock_stream` becomes `Builder::tokio_vsock_stream` with
  `tokio-vsock`.
- `vsock::VsockStream`: `Builder::vsock_stream` becomes `Builder::async_io_vsock_stream` with
  `vsock`.

`async_io_unix_stream` and `async_io_tcp_stream` already existed in zbus 5.19 and keep their
names. The Unix and TCP renames also apply to `zbus::blocking::connection::Builder`; the blocking
builder has no VSOCK stream constructors.

When the corresponding runtime features are enabled, both sets of applicable constructors are
available; enabling one feature no longer changes a constructor supplied by the other. A supplied
stream's constructor explicitly chooses its I/O backend. For address-created async connections,
transports supported by both backends instead choose at run time: tokio when the current thread is
inside a tokio runtime, and `async-io` otherwise. The stream constructor does not override the
internal task executor selected while building the connection.

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

The `org.freedesktop.DBus.Properties` proxy — `zbus::fdo::PropertiesProxy` and its blocking
sibling — takes the new value by reference:

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

### A proxy without properties has no properties cache

`proxy::Defaults` has a new constant, `HAS_PROPERTIES`, which `#[proxy]` sets from the trait,
and `proxy::Builder::build` requires the trait. A proxy whose interface has no properties never
sets up the properties cache, whatever the `CacheProperties` setting, so a program whose proxies
have none carries no cache code at all. A hand-written `Defaults` implementation can leave the
constant at its default of `true`.

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
