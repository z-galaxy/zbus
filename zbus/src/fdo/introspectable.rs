//! D-Bus standard interfaces.
//!
//! The D-Bus specification defines the message bus messages and some standard interfaces that may
//! be useful across various D-Bus applications. This module provides their proxy.

#[cfg(feature = "service")]
use super::Error;
use super::Result;
#[cfg(feature = "service")]
use crate::{ObjectServer, interface, message::Header};

/// Service-side implementation for the `org.freedesktop.DBus.Introspectable` interface.
/// This interface is implemented automatically for any object registered to the
/// [ObjectServer](crate::ObjectServer).
#[cfg(feature = "service")]
pub(crate) struct Introspectable;

#[cfg(feature = "service")]
#[interface(
    name = "org.freedesktop.DBus.Introspectable",
    introspection_docs = false
)]
impl Introspectable {
    /// Returns an XML description of the object, including its interfaces (with signals and
    /// methods), objects below it in the object path tree, and its properties.
    async fn introspect(
        &self,
        #[zbus(object_server)] server: &ObjectServer,
        #[zbus(header)] header: Header<'_>,
    ) -> Result<String> {
        let path = header.path().ok_or(crate::Error::MissingField)?;
        let root = server.root().read().await;
        root.introspect(path)
            .await
            .ok_or_else(|| Error::UnknownObject(format!("Unknown object '{path}'")))
    }
}

/// Proxy for the `org.freedesktop.DBus.Introspectable` interface.
#[cfg(feature = "proxy")]
#[crate::proxy(interface = "org.freedesktop.DBus.Introspectable", default_path = "/")]
pub trait Introspectable {
    /// Returns an XML description of the object, including its interfaces (with signals and
    /// methods), objects below it in the object path tree, and its properties.
    fn introspect(&self) -> Result<String>;
}
