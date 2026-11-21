//! The commands of the authentication protocol of D-Bus, the handshake that comes first on a
//! connection, before its messages.
//!
//! The protocol is a profile of [SASL], made of commands of a line each, which [`Command`] parses
//! and writes. A client authenticates with `AUTH`, which names a mechanism ([`AuthMechanism`])
//! and holds the client's initial response in it. The server answers with `OK` and its GUID, or
//! with `REJECTED` and the mechanisms that it supports, which the client can try next. Once the
//! client is authenticated, it can ask for file descriptors to be passed with
//! `NEGOTIATE_UNIX_FD`, which the server agrees to with `AGREE_UNIX_FD`, and it ends the handshake
//! with `BEGIN`, after which messages follow on the same socket.
//!
//! The client sends a 0 byte ahead of its first command, which is no part of a command: on some
//! platforms, the server takes the credentials of the client from it.
//!
//! A zbus connection goes through the handshake on its own. A program that does its own I/O, such
//! as a bus, goes through it with these.
//!
//! [SASL]: https://dbus.freedesktop.org/doc/dbus-specification.html#auth-protocol

mod auth_mechanism;
pub use auth_mechanism::AuthMechanism;

mod command;
pub use command::Command;
