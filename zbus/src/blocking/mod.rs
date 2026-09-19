//! The blocking API.
//!
//! This module hosts all our blocking API. All the types under this module are thin wrappers
//! around the corresponding asynchronous types. Most of the method calls are simply calling their
//! asynchronous counterparts on the underlying types and use [`block_on`](crate::block_on) to turn
//! them into blocking calls.
//!
//! This module is only available when the `blocking-api` feature is enabled (default).
//!
//! # Caveats
//!
//! Since methods provided by these types run their own little runtime (`block_on`), you must not
//! use them in async contexts because of the infamous [async sandwich footgun][asf]. This is
#![cfg_attr(
    feature = "service",
    doc = " an especially important fact to keep in mind for [`crate::interface`]. While",
    doc = " `interface`"
)]
#![cfg_attr(
    not(feature = "service"),
    doc = " an especially important fact to keep in mind for the `interface` macro (requires the",
    doc = " `service` feature). While `interface`"
)]
//! allows non-async methods for convenience, these methods are called from an async context. The
//! [`blocking` crate] provides an easy way around this problem though.
//!
//! With the built-in runtime, a blocking call made from inside a task that runtime is running — a
//! method of a served interface, say — panics rather than deadlocks, since it would be waiting
//! for the very thread it is on.
//!
//! [asf]: https://rust-lang.github.io/wg-async/vision/shiny_future/users_manual.html#caveat-beware-the-async-sandwich
//! [`blocking` crate]: https://docs.rs/blocking/

pub mod connection;
pub use connection::Connection;

mod message_iterator;
pub use message_iterator::*;
#[cfg(feature = "service")]
pub mod object_server;
#[cfg(feature = "service")]
pub use object_server::ObjectServer;
#[cfg(feature = "proxy")]
pub mod proxy;
#[cfg(feature = "proxy")]
pub use proxy::Proxy;

#[cfg(feature = "proxy")]
pub mod fdo;
