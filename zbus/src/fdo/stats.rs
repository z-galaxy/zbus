//! D-Bus standard interfaces.
//!
//! The D-Bus specification defines the message bus messages and some standard interfaces that may
//! be useful across various D-Bus applications. This module provides their proxy.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::Result;
use crate::{
    OwnedVariant, Type,
    as_variant::optional,
    names::{BusName, OwnedUniqueName},
    proxy,
};

/// Proxy for the [`org.freedesktop.DBus.Debug.Stats`][link] interface.
///
/// [link]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-bus-debug-stats-interface
#[proxy(
    interface = "org.freedesktop.DBus.Debug.Stats",
    default_service = "org.freedesktop.DBus",
    default_path = "/org/freedesktop/DBus"
)]
pub trait Stats {
    /// Get statistics about the message bus itself.
    fn get_stats(&self) -> Result<Stats>;

    /// Get statistics about a connection, identified by its unique connection name or by any
    /// well-known bus name for which it is the primary owner. This method is not meaningful for
    /// the message bus `org.freedesktop.DBus` itself.
    fn get_connection_stats(&self, name: BusName<'_>) -> Result<ConnectionStats>;

    /// List all of the match rules that are active on this message bus. The keys in the result
    /// dictionary are unique connection names. The values are lists of match rules registered by
    /// that connection, in an unspecified order. If a connection has registered the same match rule
    /// more than once, it is unspecified whether duplicate entries appear in the list.
    fn get_all_match_rules(&self) -> Result<HashMap<OwnedUniqueName, Vec<crate::OwnedMatchRule>>>;
}

/// The stats returned by the [`StatsProxy::get_stats`] method.
#[derive(Debug, Default, Deserialize, PartialEq, Serialize, Type)]
#[zbus(signature = "a{sv}", rename_all = "PascalCase")]
#[serde(default, rename_all = "PascalCase")]
pub struct Stats {
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) serial: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) active_connections: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) incomplete_connections: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) match_rules: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) peak_match_rules: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) peak_match_rules_per_connection: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) bus_names: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) peak_bus_names: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) peak_bus_names_per_connection: Option<u32>,
    #[serde(flatten)]
    pub(crate) rest: HashMap<String, OwnedVariant>,
}

impl Stats {
    /// A serial number which is incremented with each call to the GetStats method.
    pub fn serial(&self) -> Option<u32> {
        self.serial
    }

    /// The number of active connections currently handled by this message bus. The exact meaning of
    /// an active connection is implementation-defined: in the reference dbus-daemon, a connection
    /// is considered to be active after it has successfully called the Hello method.
    pub fn active_connections(&self) -> Option<u32> {
        self.active_connections
    }

    /// The number of incomplete connections currently handled by this message bus. The exact
    /// meaning of an incomplete connection is implementation-defined: in the reference dbus-daemon,
    /// a connection is considered to be incomplete if it is still carrying out the SASL handshake
    /// or if it has not yet successfully called the `Hello` method.
    pub fn incomplete_connections(&self) -> Option<u32> {
        self.incomplete_connections
    }

    /// The total number of match rules that are currently in use.
    pub fn match_rules(&self) -> Option<u32> {
        self.match_rules
    }

    /// The largest total number of match rules that have been in use at any one time.
    pub fn peak_match_rules(&self) -> Option<u32> {
        self.peak_match_rules
    }

    /// The largest number of match rules that have been in use by a single connection at any one
    /// time.
    pub fn peak_match_rules_per_connection(&self) -> Option<u32> {
        self.peak_match_rules_per_connection
    }

    /// The total number of bus names that are currently in use.
    pub fn bus_names(&self) -> Option<u32> {
        self.bus_names
    }

    /// The largest total number of bus names that have been in use at any one time.
    pub fn peak_bus_names(&self) -> Option<u32> {
        self.peak_bus_names
    }

    /// The largest number of bus names that have been in use by a single connection at any one
    /// time.
    pub fn peak_bus_names_per_connection(&self) -> Option<u32> {
        self.peak_bus_names_per_connection
    }

    /// The rest of the statistics, that are not defined by the D-Bus specificiation and hence
    /// specific to individual D-Bus broker implementations.
    pub fn rest(&self) -> &HashMap<String, OwnedVariant> {
        &self.rest
    }
}

/// The stats returned by the [`StatsProxy::get_connection_stats`] method.
#[derive(Debug, Default, Deserialize, PartialEq, Serialize, Type)]
#[zbus(signature = "a{sv}")]
#[serde(default, rename_all = "PascalCase")]
pub struct ConnectionStats {
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) serial: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) unique_name: Option<OwnedUniqueName>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) match_rules: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) peak_match_rules: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) bus_names: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) peak_bus_names: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) incoming_messages: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) outgoing_messages: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) incoming_bytes: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) outgoing_bytes: Option<u32>,
    // `PascalCase` would give `IncomingFds`, where the specification has `IncomingFDs`.
    #[serde(
        rename = "IncomingFDs",
        with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) incoming_fds: Option<u32>,
    // `PascalCase` would give `OutgoingFds`, where the specification has `OutgoingFDs`.
    #[serde(
        rename = "OutgoingFDs",
        with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) outgoing_fds: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) peak_incoming_messages: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) peak_outgoing_messages: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) peak_incoming_bytes: Option<u32>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none")]
    pub(crate) peak_outgoing_bytes: Option<u32>,
    // `PascalCase` would give `PeakIncomingFds`, where the specification has `PeakIncomingFDs`.
    #[serde(
        rename = "PeakIncomingFDs",
        with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) peak_incoming_fds: Option<u32>,
    // `PascalCase` would give `PeakOutgoingFds`, where the specification has `PeakOutgoingFDs`.
    #[serde(
        rename = "PeakOutgoingFDs",
        with = "optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) peak_outgoing_fds: Option<u32>,
    #[serde(flatten)]
    pub(crate) rest: HashMap<String, OwnedVariant>,
}

impl ConnectionStats {
    /// A serial number which is incremented with each call to the GetConnectionStats method.
    pub fn serial(&self) -> Option<u32> {
        self.serial
    }

    /// The unique name of the connection.
    pub fn unique_name(&self) -> Option<&OwnedUniqueName> {
        self.unique_name.as_ref()
    }

    /// The total number of match rules that are currently in use by this connection.
    pub fn match_rules(&self) -> Option<u32> {
        self.match_rules
    }

    /// The largest total number of match rules that have been in use by this connection at any one
    /// time.
    pub fn peak_match_rules(&self) -> Option<u32> {
        self.peak_match_rules
    }

    /// The total number of bus names that are currently in use by this connection.
    pub fn bus_names(&self) -> Option<u32> {
        self.bus_names
    }

    /// The largest total number of bus names that have been in use by this connection at any one
    /// time.
    pub fn peak_bus_names(&self) -> Option<u32> {
        self.peak_bus_names
    }

    /// The number of messages queued in the message bus that have been received from this
    /// connection and not yet dispatched.
    pub fn incoming_messages(&self) -> Option<u32> {
        self.incoming_messages
    }

    /// The number of messages queued in the message bus to be delivered to this connection.
    pub fn outgoing_messages(&self) -> Option<u32> {
        self.outgoing_messages
    }

    /// The number of bytes in the incoming messages, those of [`Self::incoming_messages`].
    pub fn incoming_bytes(&self) -> Option<u32> {
        self.incoming_bytes
    }

    /// The number of bytes in the outgoing messages, those of [`Self::outgoing_messages`].
    pub fn outgoing_bytes(&self) -> Option<u32> {
        self.outgoing_bytes
    }

    /// The number of Unix file descriptors attached to the incoming messages.
    pub fn incoming_fds(&self) -> Option<u32> {
        self.incoming_fds
    }

    /// The number of Unix file descriptors attached to the outgoing messages.
    pub fn outgoing_fds(&self) -> Option<u32> {
        self.outgoing_fds
    }

    /// The value of a `PeakIncomingMessages` entry. The specification doesn't define this entry: it
    /// defines peaks of bytes and file descriptors only.
    pub fn peak_incoming_messages(&self) -> Option<u32> {
        self.peak_incoming_messages
    }

    /// The value of a `PeakOutgoingMessages` entry. The specification doesn't define this entry: it
    /// defines peaks of bytes and file descriptors only.
    pub fn peak_outgoing_messages(&self) -> Option<u32> {
        self.peak_outgoing_messages
    }

    /// The highest number of bytes in the incoming messages at any one time.
    pub fn peak_incoming_bytes(&self) -> Option<u32> {
        self.peak_incoming_bytes
    }

    /// The highest number of bytes in the outgoing messages at any one time.
    pub fn peak_outgoing_bytes(&self) -> Option<u32> {
        self.peak_outgoing_bytes
    }

    /// The highest number of Unix file descriptors attached to the incoming messages at any one
    /// time.
    pub fn peak_incoming_fds(&self) -> Option<u32> {
        self.peak_incoming_fds
    }

    /// The highest number of Unix file descriptors attached to the outgoing messages at any one
    /// time.
    pub fn peak_outgoing_fds(&self) -> Option<u32> {
        self.peak_outgoing_fds
    }

    /// The rest of the statistics, that are not defined by the D-Bus specificiation and hence
    /// specific to individual D-Bus broker implementations.
    pub fn rest(&self) -> &HashMap<String, OwnedVariant> {
        &self.rest
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::ConnectionStats;
    use crate::{
        Variant,
        wire::{LE, serialized::Context, to_bytes},
    };

    #[test]
    fn connection_stats_read_the_entries_of_the_specification() {
        let ctxt = Context::new(LE, 0);
        let encoded = to_bytes(ctxt, &reply()).unwrap();
        let stats: ConnectionStats = encoded.deserialize().unwrap().0;

        assert_eq!(stats.serial(), Some(7));
        assert_eq!(stats.unique_name().unwrap().as_str(), ":1.42");
        assert_eq!(stats.incoming_messages(), Some(10));
        assert_eq!(stats.incoming_bytes(), Some(11));
        assert_eq!(stats.incoming_fds(), Some(12));
        assert_eq!(stats.peak_incoming_bytes(), Some(13));
        assert_eq!(stats.peak_incoming_fds(), Some(14));
        assert_eq!(stats.outgoing_messages(), Some(20));
        assert_eq!(stats.outgoing_bytes(), Some(21));
        assert_eq!(stats.outgoing_fds(), Some(22));
        assert_eq!(stats.peak_outgoing_bytes(), Some(23));
        assert_eq!(stats.peak_outgoing_fds(), Some(24));
        // Every entry of the specification has its own field.
        assert!(stats.rest().is_empty(), "{:?}", stats.rest());
    }

    #[test]
    fn connection_stats_write_the_entries_of_the_specification() {
        let stats = ConnectionStats {
            incoming_fds: Some(1),
            outgoing_fds: Some(2),
            peak_incoming_fds: Some(3),
            peak_outgoing_fds: Some(4),
            ..Default::default()
        };

        let ctxt = Context::new(LE, 0);
        let encoded = to_bytes(ctxt, &stats).unwrap();
        let entries: HashMap<String, u32> = encoded
            .deserialize::<HashMap<String, Variant<'_>>>()
            .unwrap()
            .0
            .into_iter()
            .map(|(name, value)| (name, u32::try_from(value).unwrap()))
            .collect();
        assert_eq!(
            entries,
            HashMap::from([
                ("IncomingFDs".to_string(), 1),
                ("OutgoingFDs".to_string(), 2),
                ("PeakIncomingFDs".to_string(), 3),
                ("PeakOutgoingFDs".to_string(), 4),
            ])
        );
    }

    /// The entries of a `GetConnectionStats` reply, with the names that the specification and
    /// dbus-daemon give them.
    fn reply() -> HashMap<&'static str, Variant<'static>> {
        HashMap::from([
            ("Serial", Variant::from(7u32)),
            ("UniqueName", Variant::from(":1.42")),
            ("MatchRules", Variant::from(3u32)),
            ("PeakMatchRules", Variant::from(4u32)),
            ("BusNames", Variant::from(1u32)),
            ("PeakBusNames", Variant::from(2u32)),
            ("IncomingMessages", Variant::from(10u32)),
            ("IncomingBytes", Variant::from(11u32)),
            ("IncomingFDs", Variant::from(12u32)),
            ("PeakIncomingBytes", Variant::from(13u32)),
            ("PeakIncomingFDs", Variant::from(14u32)),
            ("OutgoingMessages", Variant::from(20u32)),
            ("OutgoingBytes", Variant::from(21u32)),
            ("OutgoingFDs", Variant::from(22u32)),
            ("PeakOutgoingBytes", Variant::from(23u32)),
            ("PeakOutgoingFDs", Variant::from(24u32)),
        ])
    }
}
