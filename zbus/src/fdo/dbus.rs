//! D-Bus standard interfaces.
//!
//! The D-Bus specification defines the message bus messages and some standard interfaces that may
//! be useful across various D-Bus applications. This module provides their proxy.

#[cfg(unix)]
use crate::OwnedFd;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
#[cfg(feature = "proxy")]
use std::collections::HashMap;
use std::fmt;

use super::Result;
use crate::{
    Basic, DeserializeDict, OwnedVariant, SerializeDict, Type, Variant, utils::fmt_bitflags,
};
#[cfg(feature = "proxy")]
use crate::{
    Optional, OwnedGuid,
    names::{
        BusName, OwnedBusName, OwnedInterfaceName, OwnedUniqueName, UniqueName, WellKnownName,
    },
    proxy,
};

#[cfg_attr(
    feature = "proxy",
    doc = "The flags used by the [`DBusProxy::request_name`] method."
)]
#[cfg_attr(
    not(feature = "proxy"),
    doc = "The flags used by `DBusProxy::request_name` (requires the `proxy` feature)."
)]
///
/// This is a [`bitflags`](https://docs.rs/bitflags) type: a set of zero or more of the flags
/// below, combined with `|`. Bits that none of the flags define are retained, not rejected, when
/// the flags are decoded.
///
/// The default flags (returned by [`RequestNameFlags::default`]) are `ALLOW_REPLACEMENT`,
/// `REPLACE_EXISTING`, and `DO_NOT_QUEUE`.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Type,
    Variant,
    OwnedVariant,
)]
pub struct RequestNameFlags(u32);

bitflags::bitflags! {
    impl RequestNameFlags: u32 {
        /// If an application A specifies this flag and succeeds in becoming the owner of the name,
        #[cfg_attr(
            feature = "proxy",
            doc = "and another application B later calls [`DBusProxy::request_name`] with the"
        )]
        #[cfg_attr(
            not(feature = "proxy"),
            doc = "and another application B later calls `DBusProxy::request_name` with the"
        )]
        /// `REPLACE_EXISTING` flag, then application A will lose ownership and receive a
        /// `org.freedesktop.DBus.NameLost` signal, and application B will become the new owner. If
        /// `ALLOW_REPLACEMENT` is not specified by application A, or `REPLACE_EXISTING` is not
        /// specified by application B, then application B will not replace application A as the
        /// owner.
        const ALLOW_REPLACEMENT = 0x01;
        /// Try to replace the current owner if there is one. If this flag is not set the
        /// application will only become the owner of the name if there is no current owner. If
        /// this flag is set, the application will replace the current owner if the current owner
        /// specified `ALLOW_REPLACEMENT`.
        const REPLACE_EXISTING = 0x02;
        /// Without this flag, if an application requests a name that is already owned, the
        /// application will be placed in a queue to own the name when the current owner gives it
        /// up. If this flag is given, the application will not be placed in the queue; the
        /// request for the name will simply fail. This flag also affects behavior when an
        /// application is replaced as name owner; by default the application moves back into the
        /// waiting queue, unless this flag was provided when the application became the name
        /// owner.
        const DO_NOT_QUEUE = 0x04;
    }
}

impl Default for RequestNameFlags {
    fn default() -> Self {
        Self::ALLOW_REPLACEMENT | Self::REPLACE_EXISTING | Self::DO_NOT_QUEUE
    }
}

impl fmt::Debug for RequestNameFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_bitflags("RequestNameFlags", self, f)
    }
}

impl Basic for RequestNameFlags {
    const SIGNATURE_CHAR: char = u32::SIGNATURE_CHAR;
    const SIGNATURE_STR: &'static str = u32::SIGNATURE_STR;
}

#[cfg_attr(
    feature = "proxy",
    doc = "The return code of the [`DBusProxy::request_name`] method."
)]
#[cfg_attr(
    not(feature = "proxy"),
    doc = "The return code of `DBusProxy::request_name` (requires the `proxy` feature)."
)]
#[repr(u32)]
#[derive(Deserialize_repr, Serialize_repr, Type, Debug, PartialEq, Eq)]
pub enum RequestNameReply {
    /// The caller is now the primary owner of the name, replacing any previous owner. Either the
    /// name had no owner before, or the caller specified [`RequestNameFlags::REPLACE_EXISTING`] and
    /// the current owner specified [`RequestNameFlags::ALLOW_REPLACEMENT`].
    PrimaryOwner = 0x01,
    /// The name already had an owner, [`RequestNameFlags::DO_NOT_QUEUE`] was not specified, and
    /// either the current owner did not specify [`RequestNameFlags::ALLOW_REPLACEMENT`] or the
    /// requesting application did not specify [`RequestNameFlags::REPLACE_EXISTING`].
    InQueue = 0x02,
    /// The name already had an owner, [`RequestNameFlags::DO_NOT_QUEUE`] was specified, and either
    /// [`RequestNameFlags::ALLOW_REPLACEMENT`] was not specified by the current owner, or
    /// [`RequestNameFlags::REPLACE_EXISTING`] was not specified by the requesting application.
    Exists = 0x03,
    /// The application trying to request ownership of a name is already the owner of it.
    AlreadyOwner = 0x04,
}

impl std::fmt::Display for RequestNameReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PrimaryOwner => write!(f, "PrimaryOwner"),
            Self::InQueue => write!(f, "InQueue"),
            Self::Exists => write!(f, "Exists"),
            Self::AlreadyOwner => write!(f, "AlreadyOwner"),
        }
    }
}

#[cfg_attr(
    feature = "proxy",
    doc = "The return code of the [`DBusProxy::release_name`] method."
)]
#[cfg_attr(
    not(feature = "proxy"),
    doc = "The return code of `DBusProxy::release_name` (requires the `proxy` feature)."
)]
#[repr(u32)]
#[derive(Deserialize_repr, Serialize_repr, Type, Debug, PartialEq, Eq)]
pub enum ReleaseNameReply {
    /// The caller has released their claim on the given name. Either the caller was the primary
    /// owner of the name, and the name is now unused or taken by somebody waiting in the queue for
    /// the name, or the caller was waiting in the queue for the name and has now been removed from
    /// the queue.
    Released = 0x01,
    /// The given name does not exist on this bus.
    NonExistent = 0x02,
    /// The caller was not the primary owner of this name, and was also not waiting in the queue to
    /// own this name.
    NotOwner = 0x03,
}

impl std::fmt::Display for ReleaseNameReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Released => write!(f, "Released"),
            Self::NonExistent => write!(f, "NonExistent"),
            Self::NotOwner => write!(f, "NotOwner"),
        }
    }
}

#[cfg_attr(
    feature = "proxy",
    doc = "The return code of the [`DBusProxy::start_service_by_name`] method."
)]
#[cfg_attr(
    not(feature = "proxy"),
    doc = "The return code of `DBusProxy::start_service_by_name` (requires the `proxy` feature)."
)]
#[repr(u32)]
#[derive(Deserialize_repr, Serialize_repr, Type, Debug, PartialEq, Eq)]
pub enum StartServiceReply {
    /// The service was successfully started.
    Success = 0x01,
    /// The service was already running.
    AlreadyRunning = 0x02,
}

impl std::fmt::Display for StartServiceReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "Success"),
            Self::AlreadyRunning => write!(f, "AlreadyRunning"),
        }
    }
}

impl TryFrom<u32> for StartServiceReply {
    type Error = super::Error;

    fn try_from(value: u32) -> Result<Self> {
        match value {
            0x01 => Ok(StartServiceReply::Success),
            0x02 => Ok(StartServiceReply::AlreadyRunning),
            _ => Err(super::Error::ZBus(crate::Error::InvalidReply)),
        }
    }
}

/// Credentials of a process connected to a bus server.
///
/// If unable to determine certain credentials (for instance, because the process is not on the same
/// machine as the bus daemon, or because this version of the bus daemon does not support a
/// particular security framework), or if the values of those credentials cannot be represented as
/// documented here, then those credentials are omitted.
///
/// **Note**: unknown keys, in particular those with "." that are not from the specification, will
/// be ignored. Use your own implementation or contribute your keys here, or in the specification.
#[derive(Debug, Default, DeserializeDict, PartialEq, Eq, SerializeDict, Type)]
#[zbus(signature = "a{sv}")]
pub struct ConnectionCredentials {
    #[zbus(rename = "UnixUserID")]
    pub(crate) unix_user_id: Option<u32>,

    #[zbus(rename = "UnixGroupIDs")]
    pub(crate) unix_group_ids: Option<Vec<u32>>,

    #[cfg(unix)]
    #[zbus(rename = "ProcessFD")]
    pub(crate) process_fd: Option<OwnedFd>,

    #[zbus(rename = "ProcessID")]
    pub(crate) process_id: Option<u32>,

    #[zbus(rename = "WindowsSID")]
    pub(crate) windows_sid: Option<String>,

    #[zbus(rename = "LinuxSecurityLabel")]
    pub(crate) linux_security_label: Option<Vec<u8>>,
}

impl ConnectionCredentials {
    /// The credentials of the peer of the unix-domain socket `socket`, as the socket tells them,
    /// with no connection over it, such as for a server to know who connected before it goes
    /// through the handshake of a connection.
    ///
    /// They are those that [`Connection::peer_creds`] gives for a connection over the socket:
    ///
    /// * the user ID of the peer,
    /// * on Linux and Android, its process ID, unless its process has none in the PID namespace of
    ///   this one,
    /// * on Linux 6.5 and later, a file descriptor that pins its process ([`Self::process_fd`]),
    ///   unless the kernel has none to give, as before Linux 6.16 for a process that has exited,
    /// * on Linux and Android, its groups: on Linux 4.13 and later, those that its process had as
    ///   it connected, which the socket tells, and otherwise those that the name service (NSS) has
    ///   for its user.
    ///
    /// On macOS and the BSDs, the socket tells the user ID alone.
    ///
    /// The name service may go to a file, a daemon or the network, so a lookup in it runs off the
    /// calling thread: on the blocking pool of Tokio, if the `tokio` feature is enabled and a Tokio
    /// runtime is current, as for a connection built there, and on the threads that zbus keeps for
    /// blocking work otherwise, which need no runtime to be running.
    ///
    /// This method is only available on Unix platforms, when the `bus-impl` feature is enabled.
    ///
    /// # Errors
    ///
    /// An I/O error ([`Error::InputOutput`]) if the socket can't tell the credentials, such as when
    /// `socket` is not a socket.
    ///
    /// [`Connection::peer_creds`]: crate::Connection::peer_creds
    /// [`Error::InputOutput`]: crate::Error::InputOutput
    #[cfg(all(unix, feature = "bus-impl"))]
    pub async fn of_peer(socket: impl std::os::fd::AsFd) -> crate::Result<Self> {
        Ok(crate::runtime::io::peer_credentials(socket.as_fd(), None).await?)
    }

    /// The numeric Unix user ID, as defined by POSIX.
    pub fn unix_user_id(&self) -> Option<u32> {
        self.unix_user_id
    }

    /// The numeric Unix group IDs (including both the primary group and the supplementary groups),
    /// as defined by POSIX, in numerically sorted order. This array is either complete or absent:
    /// if the message bus is able to determine some but not all of the caller's groups, or if one
    /// of the groups is not representable in a UINT32, it must not add this credential to the
    /// dictionary.
    pub fn unix_group_ids(&self) -> Option<&Vec<u32>> {
        self.unix_group_ids.as_ref()
    }

    /// Same as [`ConnectionCredentials::unix_group_ids`], but consumes `self` and returns the group
    /// IDs Vec.
    pub fn into_unix_group_ids(self) -> Option<Vec<u32>> {
        self.unix_group_ids
    }

    /// A file descriptor pinning the process, on platforms that have this concept. On Linux, the
    /// SO_PEERPIDFD socket option is a suitable implementation. This is safer to use to identify
    /// a process than the ProcessID, as the latter is subject to re-use attacks, while the FD
    /// cannot be recycled. If the original process no longer exists the FD will no longer be
    /// resolvable.
    #[cfg(unix)]
    pub fn process_fd(&self) -> Option<&OwnedFd> {
        self.process_fd.as_ref()
    }

    /// Same as [`ConnectionCredentials::process_fd`], but consumes `self` and returns the owned
    /// file descriptor.
    #[cfg(unix)]
    pub fn into_process_fd(self) -> Option<OwnedFd> {
        self.process_fd
    }

    /// The numeric process ID, on platforms that have this concept. On Unix, this is the process ID
    /// defined by POSIX.
    pub fn process_id(&self) -> Option<u32> {
        self.process_id
    }

    /// The Windows security identifier in its string form, e.g.
    /// `S-1-5-21-3623811015-3361044348-30300820-1013` for a domain or local computer user or
    /// "S-1-5-18` for the LOCAL_SYSTEM user.
    pub fn windows_sid(&self) -> Option<&String> {
        self.windows_sid.as_ref()
    }

    /// Same as [`ConnectionCredentials::windows_sid`], but consumes `self` and returns the SID
    /// string.
    pub fn into_windows_sid(self) -> Option<String> {
        self.windows_sid
    }

    /// On Linux systems, the security label that would result from the SO_PEERSEC getsockopt call.
    /// The array contains the non-zero bytes of the security label in an unspecified
    /// ASCII-compatible encoding, followed by a single zero byte.
    ///
    /// For example, the SELinux context `system_u:system_r:init_t:s0` (a string of length 27) would
    /// be encoded as 28 bytes ending with `':', 's', '0', '\x00'`
    ///
    /// On SELinux systems this is the SELinux context, as output by `ps -Z` or `ls -Z`. Typical
    /// values might include `system_u:system_r:init_t:s0`,
    /// `unconfined_u:unconfined_r:unconfined_t:s0-s0:c0.c1023`, or
    /// `unconfined_u:unconfined_r:chrome_sandbox_t:s0-s0:c0.c1023`.
    ///
    /// On Smack systems, this is the Smack label. Typical values might include `_`, `*`, `User`,
    /// `System` or `System::Shared`.
    ///
    /// On AppArmor systems, this is the AppArmor context, a composite string encoding the AppArmor
    /// label (one or more profiles) and the enforcement mode. Typical values might include
    /// `unconfined`, `/usr/bin/firefox (enforce)` or `user1 (complain)`.
    pub fn linux_security_label(&self) -> Option<&Vec<u8>> {
        self.linux_security_label.as_ref()
    }

    /// Same as [`ConnectionCredentials::linux_security_label`], but consumes `self` and returns
    /// the security label bytes.
    pub fn into_linux_security_label(self) -> Option<Vec<u8>> {
        self.linux_security_label
    }

    /// Set the numeric Unix user ID, as defined by POSIX.
    pub fn set_unix_user_id(mut self, unix_user_id: u32) -> Self {
        self.unix_user_id = Some(unix_user_id);

        self
    }

    /// Add a numeric Unix group ID.
    ///
    /// See [`ConnectionCredentials::unix_group_ids`] for more information.
    pub fn add_unix_group_id(mut self, unix_group_id: u32) -> Self {
        self.unix_group_ids
            .get_or_insert_with(Vec::new)
            .push(unix_group_id);

        self
    }

    /// Set the process FD, on platforms that have this concept
    #[cfg(unix)]
    pub fn set_process_fd(mut self, process_fd: OwnedFd) -> Self {
        self.process_fd = Some(process_fd);

        self
    }

    /// Set the numeric process ID, on platforms that have this concept.
    ///
    /// See [`ConnectionCredentials::process_id`] for more information.
    pub fn set_process_id(mut self, process_id: u32) -> Self {
        self.process_id = Some(process_id);

        self
    }

    /// Set the Windows security identifier in its string form.
    pub fn set_windows_sid(mut self, windows_sid: String) -> Self {
        self.windows_sid = Some(windows_sid);

        self
    }

    /// Set the Linux security label.
    ///
    /// See [`ConnectionCredentials::linux_security_label`] for more information.
    pub fn set_linux_security_label(mut self, linux_security_label: Vec<u8>) -> Self {
        self.linux_security_label = Some(linux_security_label);

        self
    }
}

/// Proxy for the `org.freedesktop.DBus` interface.
#[cfg(feature = "proxy")]
#[proxy(
    default_service = "org.freedesktop.DBus",
    default_path = "/org/freedesktop/DBus",
    interface = "org.freedesktop.DBus"
)]
pub trait DBus {
    /// Adds a match rule to match messages going through the message bus
    #[zbus(name = "AddMatch")]
    fn add_match_rule(&self, rule: crate::MatchRule<'_>) -> Result<()>;

    /// Returns auditing data used by Solaris ADT, in an unspecified binary format.
    fn get_adt_audit_session_data(&self, bus_name: BusName<'_>) -> Result<Vec<u8>>;

    /// Returns as many credentials as possible for the process connected to the server.
    fn get_connection_credentials(&self, bus_name: BusName<'_>) -> Result<ConnectionCredentials>;

    /// Returns the security context used by SELinux, in an unspecified format.
    #[zbus(name = "GetConnectionSELinuxSecurityContext")]
    fn get_connection_selinux_security_context(&self, bus_name: BusName<'_>) -> Result<Vec<u8>>;

    /// Returns the Unix process ID of the process connected to the server.
    #[zbus(name = "GetConnectionUnixProcessID")]
    fn get_connection_unix_process_id(&self, bus_name: BusName<'_>) -> Result<u32>;

    /// Returns the Unix user ID of the process connected to the server.
    fn get_connection_unix_user(&self, bus_name: BusName<'_>) -> Result<u32>;

    /// Gets the unique ID of the bus.
    fn get_id(&self) -> Result<OwnedGuid>;

    /// Returns the unique connection name of the primary owner of the name given.
    fn get_name_owner(&self, name: BusName<'_>) -> Result<OwnedUniqueName>;

    /// Returns the unique name assigned to the connection.
    fn hello(&self) -> Result<OwnedUniqueName>;

    /// Returns a list of all names that can be activated on the bus.
    fn list_activatable_names(&self) -> Result<Vec<OwnedBusName>>;

    /// Returns a list of all currently-owned names on the bus.
    fn list_names(&self) -> Result<Vec<OwnedBusName>>;

    /// List the connections currently queued for a bus name.
    fn list_queued_owners(&self, name: WellKnownName<'_>) -> Result<Vec<OwnedUniqueName>>;

    /// Checks if the specified name exists (currently has an owner).
    fn name_has_owner(&self, name: BusName<'_>) -> Result<bool>;

    /// Ask the message bus to release the method caller's claim to the given name.
    fn release_name(&self, name: WellKnownName<'_>) -> Result<ReleaseNameReply>;

    /// Reload server configuration.
    fn reload_config(&self) -> Result<()>;

    /// Removes the first rule that matches.
    #[zbus(name = "RemoveMatch")]
    fn remove_match_rule(&self, rule: crate::MatchRule<'_>) -> Result<()>;

    /// Ask the message bus to assign the given name to the method caller.
    fn request_name(
        &self,
        name: WellKnownName<'_>,
        flags: RequestNameFlags,
    ) -> Result<RequestNameReply>;

    /// Tries to launch the executable associated with a name (service
    /// activation), as an explicit request.
    fn start_service_by_name(
        &self,
        name: WellKnownName<'_>,
        flags: u32,
    ) -> Result<StartServiceReply>;

    /// This method adds to or modifies that environment when activating services.
    fn update_activation_environment(&self, environment: HashMap<&str, &str>) -> Result<()>;

    /// This signal indicates that the owner of a name has
    /// changed. It's also the signal to use to detect the appearance
    /// of new names on the bus.
    #[zbus(signal)]
    fn name_owner_changed(
        &self,
        name: BusName<'_>,
        old_owner: Optional<UniqueName<'_>>,
        new_owner: Optional<UniqueName<'_>>,
    );

    /// This signal is sent to a specific application when it loses ownership of a name.
    #[zbus(signal)]
    fn name_lost(&self, name: BusName<'_>);

    /// This signal is sent to a specific application when it gains ownership of a name.
    #[zbus(signal)]
    fn name_acquired(&self, name: BusName<'_>);

    /// This property lists abstract “features” provided by the message bus, and can be used by
    /// clients to detect the capabilities of the message bus with which they are communicating.
    #[zbus(property)]
    fn features(&self) -> Result<Vec<String>>;

    /// This property lists interfaces provided by the `/org/freedesktop/DBus` object, and can be
    /// used by clients to detect the capabilities of the message bus with which they are
    /// communicating. Unlike the standard Introspectable interface, querying this property does not
    /// require parsing XML. This property was added in version 1.11.x of the reference
    /// implementation of the message bus.
    ///
    /// The standard `org.freedesktop.DBus` and `org.freedesktop.DBus.Properties` interfaces are not
    /// included in the value of this property, because their presence can be inferred from the fact
    /// that a method call on `org.freedesktop.DBus.Properties` asking for properties of
    /// `org.freedesktop.DBus` was successful. The standard `org.freedesktop.DBus.Peer` and
    /// `org.freedesktop.DBus.Introspectable` interfaces are not included in the value of this
    /// property either, because they do not indicate features of the message bus implementation.
    #[zbus(property)]
    fn interfaces(&self) -> Result<Vec<OwnedInterfaceName>>;
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn request_name_flags_default() {
        let flags = RequestNameFlags::default();
        assert!(flags.contains(RequestNameFlags::ALLOW_REPLACEMENT));
        assert!(flags.contains(RequestNameFlags::REPLACE_EXISTING));
        assert!(flags.contains(RequestNameFlags::DO_NOT_QUEUE));
    }

    #[test]
    fn request_name_flags_signature_and_debug() {
        assert_eq!(RequestNameFlags::SIGNATURE, "u");
        assert_eq!(
            format!("{:?}", RequestNameFlags::default()),
            "RequestNameFlags(ALLOW_REPLACEMENT | REPLACE_EXISTING | DO_NOT_QUEUE)"
        );
    }

    #[test]
    fn request_name_flags_convert_from_borrowed_value() {
        use crate::wire::{LE, serialized::Context, to_bytes};

        // A `Variant` that borrows from the buffer it was decoded from, as one taken out of a
        // received message does.
        let flags = RequestNameFlags::REPLACE_EXISTING | RequestNameFlags::DO_NOT_QUEUE;
        let encoded = to_bytes(Context::new(LE, 0), &Variant::from(flags)).unwrap();
        let (value, _): (Variant<'_>, _) = encoded.deserialize().unwrap();
        assert_eq!(RequestNameFlags::try_from(value).unwrap(), flags);
    }
}
