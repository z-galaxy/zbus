//! The D-Bus wire format.
//!
//! This module encodes and decodes data to and from the [D-Bus wire format][dwf]. The format is
//! simple and very efficient, which makes it useful outside of a D-Bus context as well.
//!
//! Commonly used types and traits are re-exported at the [`crate`] root. This module contains the
//! lower-level encoding and decoding API.
//!
//! The API is [serde]-based, so you will find it intuitive if you are already familiar with
//! serde. If you are not, you may want to read serde's [tutorial] first.
//!
//! A modified form of this format, [GVariant][gv], is commonly used for efficient storage of
//! arbitrary data. zbus does not implement it; the [zgvariant] crate does.
//!
//! # Wire format only
//!
//! If this module and [`names`](crate::names) are all you need, turn zbus's default features off:
//!
//! ```toml
//! [dependencies]
//! zbus = { version = "6", default-features = false }
//! ```
//!
//! That build contains no connection, proxy or object server, and pulls in nothing beyond what
//! the encoding itself needs. Runtime features such as `builtin-runtime` and `tokio` enable
//! `comms` for connections and messages. Enable `proxy` for clients and `service` for the object
//! server; both features are enabled by default.
//!
//! # Example
//!
//! Serialization and deserialization go through the [toplevel functions](#functions):
//!
//! ```
//! use std::collections::HashMap;
//! use zbus::{Type, wire::{LE, serialized::Context, to_bytes}};
//! use serde::{Deserialize, Serialize};
//!
//! // All serialization and deserialization API, needs a context.
//! let ctxt = Context::new(LE, 0);
//!
//! // i16
//! let encoded = to_bytes(ctxt, &42i16).unwrap();
//! let decoded: i16 = encoded.deserialize().unwrap().0;
//! assert_eq!(decoded, 42);
//!
//! // strings
//! let encoded = to_bytes(ctxt, &"hello").unwrap();
//! let decoded: &str = encoded.deserialize().unwrap().0;
//! assert_eq!(decoded, "hello");
//!
//! // tuples
//! let t = ("hello", 42i32, true);
//! let encoded = to_bytes(ctxt, &t).unwrap();
//! let decoded: (&str, i32, bool) = encoded.deserialize().unwrap().0;
//! assert_eq!(decoded, t);
//!
//! // Vec
//! let v = vec!["hello", "world!"];
//! let encoded = to_bytes(ctxt, &v).unwrap();
//! let decoded: Vec<&str> = encoded.deserialize().unwrap().0;
//! assert_eq!(decoded, v);
//!
//! // Dictionary
//! let mut map: HashMap<i64, &str> = HashMap::new();
//! map.insert(1, "123");
//! map.insert(2, "456");
//! let encoded = to_bytes(ctxt, &map).unwrap();
//! let decoded: HashMap<i64, &str> = encoded.deserialize().unwrap().0;
//! assert_eq!(decoded[&1], "123");
//! assert_eq!(decoded[&2], "456");
//!
//! // derive macros to handle custom types.
//! #[derive(Deserialize, Serialize, Type, PartialEq, Debug)]
//! struct Struct<'s> {
//!     field1: u16,
//!     field2: i64,
//!     field3: &'s str,
//! }
//!
//! assert_eq!(Struct::SIGNATURE, "(qxs)");
//! let s = Struct {
//!     field1: 42,
//!     field2: i64::MAX,
//!     field3: "hello",
//! };
//! let ctxt = Context::new(LE, 0);
//! let encoded = to_bytes(ctxt, &s).unwrap();
//! let decoded: Struct<'_> = encoded.deserialize().unwrap().0;
//! assert_eq!(decoded, s);
//!
//! // It can handle enums too, just that all variants must have the same number and types of
//! // fields. Names of fields don't matter though. You can make use of `Value` or `OwnedValue`
//! // if you want to encode different data in different fields.
//! #[derive(Deserialize, Serialize, Type, PartialEq, Debug)]
//! enum Enum<'s> {
//!     Variant1 { field1: u16, field2: i64, field3: &'s str },
//!     Variant2(u16, i64, &'s str),
//!     Variant3 { f1: u16, f2: i64, f3: &'s str },
//! }
//!
//! // Enum encoding uses a `u32` to denote the variant index. For unit-type enums that's all
//! // that's needed so the signature is just `u` but complex enums are encoded as a structure
//! // whose first field is the variant index and the second one is the field(s).
//! assert_eq!(Enum::SIGNATURE, "(u(qxs))");
//! let e = Enum::Variant3 {
//!     f1: 42,
//!     f2: i64::MAX,
//!     f3: "hello",
//! };
//! let encoded = to_bytes(ctxt, &e).unwrap();
//! let decoded: Enum<'_> = encoded.deserialize().unwrap().0;
//! assert_eq!(decoded, e);
//!
//! // Enum encoding can be adjusted by using the `serde_repr` crate
//! // and by annotating the representation of the enum with `repr`.
//! use serde_repr::{Serialize_repr, Deserialize_repr};
//!
//! #[derive(Deserialize_repr, Serialize_repr, Type, PartialEq, Debug)]
//! #[repr(u8)]
//! enum UnitEnum {
//!     Variant1,
//!     Variant2,
//!     Variant3,
//! }
//!
//! assert_eq!(UnitEnum::SIGNATURE, "y");
//! let encoded = to_bytes(ctxt, &UnitEnum::Variant2).unwrap();
//! let e: UnitEnum = encoded.deserialize().unwrap().0;
//! assert_eq!(e, UnitEnum::Variant2);
//!
//! // Unit enums can also be (de)serialized as strings.
//! #[derive(Deserialize, Serialize, Type, PartialEq, Debug)]
//! #[zbus(signature = "s")]
//! enum StrEnum {
//!     Variant1,
//!     Variant2,
//!     Variant3,
//!     // Catch-all that preserves the raw value for any variant we don't know about.
//!     #[serde(untagged)]
//!     Other(String),
//! }
//!
//! assert_eq!(StrEnum::SIGNATURE, "s");
//! ```
//!
//! Apart from the obvious requirement of a [`serialized::Context`] instance by the main
//! serialization and deserialization API, the type being serialized or deserialized must also
//! implement [`Type`] in addition to [`Serialize`] or [`Deserialize`], respectively. Please refer
//! to the [`Type`] documentation for more details.
//!
//! Most of the [basic types] of D-Bus match 1-1 with all the primitive Rust types. The only two
//! exceptions being [`Signature`] and [`ObjectPath`], which are really just strings. These types
//! are covered by the [`Basic`] trait.
//!
//! Similarly, most of the [container types] also map nicely to the usual Rust types and
//! collections (as can be seen in the example code above). The only noteworthy exception being
//! the ARRAY type. As arrays in Rust are fixed-sized, serde treats them as tuples and so does
//! this module. This means they are encoded as the D-Bus STRUCT type. If you need to serialize
//! to, or deserialize from a D-Bus array, you'll need to use a [slice] (an array can easily be
//! converted to a slice), a [`Vec`] or an [`arrayvec::ArrayVec`].
//!
//! D-Bus string types, including [`Signature`] and [`ObjectPath`], require one additional
//! restriction that strings in Rust do not. They must not contain any interior null bytes
//! (`'\0'`). Encoding or decoding strings that contain this character returns an error.
//!
//! The generic D-Bus type `VARIANT` is represented by [`Value`], an enum that holds exactly one
//! value of any of the other types. Please refer to the [`Value`] documentation for examples.
//!
//! # no-std
//!
//! `std` is a hard requirement: this module does not build in a `no-std` environment.
//!
//! # Optional features
//!
//! Each of these adds [`Type`] (and, where it applies, [`Value`]) implementations for a
//! third-party crate's types, except `option-as-array`, which instead changes how `Option<T>`
//! is encoded. None of them enables the D-Bus API, and they are all off in a wire-only build
//! (`default-features = false`); `comms` — and therefore every default build — turns
//! `enumflags2` on.
//!
//! | Feature | Types covered |
//! | --- | --- |
//! | `arrayvec` | `arrayvec::ArrayVec` and `arrayvec::ArrayString` |
//! | `camino` | `camino::Utf8Path` and `camino::Utf8PathBuf` |
//! | `chrono` | `chrono`'s date and time types |
//! | `enumflags2` | `enumflags2::BitFlags<F>`, converted to and from `Value` |
//! | `heapless` | `heapless::Vec` and `heapless::String` |
//! | `option-as-array` | `Option<T>`, encoded as an array of 0 or 1 elements |
//! | `serde_bytes` | `serde_bytes::Bytes` and `serde_bytes::ByteBuf` |
//! | `time` | `time`'s date and time types |
//! | `url` | `url::Url` |
//! | `uuid` | `uuid::Uuid` |
//!
//! [dwf]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-marshaling
//! [gv]: https://developer.gnome.org/documentation/specifications/gvariant-specification-1.0.html
//! [zgvariant]: https://crates.io/crates/zgvariant
//! [serde]: https://crates.io/crates/serde
//! [tutorial]: https://serde.rs/
//! [`Type`]: trait@Type
//! [`Value`]: enum@Value
//! [`Serialize`]: trait@serde::Serialize
//! [`Deserialize`]: trait@serde::Deserialize
//! [basic types]: https://dbus.freedesktop.org/doc/dbus-specification.html#basic-types
//! [container types]: https://dbus.freedesktop.org/doc/dbus-specification.html#container-types
//! [slice]: https://doc.rust-lang.org/std/primitive.slice.html
//! [`arrayvec::ArrayVec`]: https://docs.rs/arrayvec/latest/arrayvec/struct.ArrayVec.html

#[macro_use]
mod utils;
pub use utils::*;

mod array;
pub use array::*;

mod basic;
pub use basic::*;

mod dict;
pub use dict::*;

pub mod serialized;

#[cfg(unix)]
mod fd;
#[cfg(unix)]
pub use fd::*;

mod object_path;
pub use object_path::*;

mod file_path;
pub use file_path::*;

mod ser;
pub use ser::*;

mod de;

pub mod dbus;

pub mod signature;
pub use signature::Signature;

mod str;
pub use str::*;

mod structure;
pub use structure::*;

mod optional;
pub use optional::*;

mod value;
pub use value::*;

// The shared derive codegen in zbus_utils emits `<path>::Error` and `<path>::Result` where
// `<path>` is whatever module holds the wire types, and a glob import of this module must bring
// the error types along with them. The spelling users should write is `zbus::Error`.
#[doc(hidden)]
pub use crate::{Error, MaxDepthExceeded, Result};

#[macro_use]
mod r#type;
pub use r#type::*;

mod tuple;
pub use tuple::*;

mod from_value;

mod into_value;

mod owned_value;
pub use owned_value::*;

mod container_depths;

pub mod as_value;

pub use zbus_macros::{DeserializeDict, OwnedValue, SerializeDict, Type, Value, signature};

// Macro support module, not part of the public API.
#[doc(hidden)]
pub mod export {
    pub use serde;
}

// Re-export all of the `endi` API for ease of use.
pub use endi::*;

// `#[macro_export]` puts these at the crate root whatever module they are written in; this is
// the path they are documented under.
#[doc(inline)]
pub use crate::{impl_type_with_repr, static_str_type};
