//! D-Bus standard interfaces.
//!
//! The D-Bus specification defines the message bus messages and some standard interfaces that may
//! be useful across various D-Bus applications. This module provides their proxy.

use super::Result;

#[cfg(feature = "service")]
pub(crate) struct Peer;

/// Service-side implementation for the `org.freedesktop.DBus.Peer` interface.
/// This interface is implemented automatically for any object registered to the
/// [ObjectServer](crate::ObjectServer).
#[cfg(feature = "service")]
#[crate::interface(name = "org.freedesktop.DBus.Peer", introspection_docs = false)]
impl Peer {
    /// On receipt, an application should do nothing other than reply as usual. It does not matter
    /// which object path a ping is sent to.
    fn ping(&self) {}

    /// An application should reply the containing a hex-encoded UUID representing the identity of
    /// the machine the process is running on. This UUID must be the same for all processes on a
    /// single system at least until that system next reboots. It should be the same across reboots
    /// if possible, but this is not always possible to implement and is not guaranteed. It does not
    /// matter which object path a GetMachineId is sent to.
    ///
    /// This method is implemented for:
    /// - Linux: Reads from `/var/lib/dbus/machine-id` or `/etc/machine-id`
    /// - macOS: Uses `gethostuuid()` system call
    /// - FreeBSD/DragonFlyBSD: Reads from standard D-Bus locations, falls back to `kern.hostuuid`
    /// - OpenBSD/NetBSD: Reads from standard D-Bus locations (`/var/db/dbus/machine-id`, etc.)
    /// - Windows: Uses Windows hardware profile GUID
    fn get_machine_id(&self) -> Result<String> {
        super::machine_id()
    }
}

/// Proxy for the `org.freedesktop.DBus.Peer` interface.
#[cfg(feature = "proxy")]
#[crate::proxy(interface = "org.freedesktop.DBus.Peer")]
pub trait Peer {
    /// On receipt, an application should do nothing other than reply as usual. It does not matter
    /// which object path a ping is sent to.
    fn ping(&self) -> zbus::Result<()>;

    /// An application should reply the containing a hex-encoded UUID representing the identity of
    /// the machine the process is running on. This UUID must be the same for all processes on a
    /// single system at least until that system next reboots. It should be the same across reboots
    /// if possible, but this is not always possible to implement and is not guaranteed. It does not
    /// matter which object path a GetMachineId is sent to.
    ///
    /// This method is implemented for:
    /// - Linux: Reads from `/var/lib/dbus/machine-id` or `/etc/machine-id`
    /// - macOS: Uses `gethostuuid()` system call
    /// - FreeBSD/DragonFlyBSD: Reads from standard D-Bus locations, falls back to `kern.hostuuid`
    /// - OpenBSD/NetBSD: Reads from standard D-Bus locations (`/var/db/dbus/machine-id`, etc.)
    /// - Windows: Uses Windows hardware profile GUID
    fn get_machine_id(&self) -> Result<String>;
}
