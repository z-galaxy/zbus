//! D-Bus standard interfaces.
//!
//! The D-Bus specification defines the message bus messages and some standard interfaces that may
//! be useful across various D-Bus applications. This module provides their proxy.

use std::{borrow::Cow, collections::HashMap};

#[cfg(feature = "service")]
use super::Error;
use super::Result;
#[cfg(feature = "service")]
use crate::{
    BoxDBusError, Connection, ObjectServer, interface, message::Header,
    object_server::SignalEmitter,
};
use crate::{OwnedVariant, Variant, names::InterfaceName};

/// Service-side implementation for the `org.freedesktop.DBus.Properties` interface.
/// This interface is implemented automatically for any object registered to the
/// [ObjectServer].
#[cfg(feature = "service")]
pub struct Properties;

#[cfg(feature = "service")]
#[interface(name = "org.freedesktop.DBus.Properties", introspection_docs = false)]
impl Properties {
    /// Get a property value.
    async fn get(
        &self,
        interface_name: InterfaceName<'_>,
        property_name: &str,
        #[zbus(connection)] conn: &Connection,
        #[zbus(object_server)] server: &ObjectServer,
        #[zbus(header)] header: Header<'_>,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> std::result::Result<OwnedVariant, BoxDBusError> {
        let path = header.path().ok_or(crate::Error::MissingField)?;
        let root = server.root().read().await;
        let iface = root.interface_at(path, &interface_name).map_err(|_| {
            Error::UnknownInterface(format!("Unknown interface '{interface_name}'"))
        })?;

        let res = iface
            .instance
            .read()
            .await
            .get(property_name, server, conn, Some(&header), &emitter)
            .await;
        res.unwrap_or_else(|| {
            Err(Error::UnknownProperty(format!("Unknown property '{property_name}'")).into())
        })
    }

    /// Set a property value.
    #[allow(clippy::too_many_arguments)]
    async fn set(
        &self,
        interface_name: InterfaceName<'_>,
        property_name: &str,
        value: &Variant<'_>,
        #[zbus(object_server)] server: &ObjectServer,
        #[zbus(connection)] connection: &Connection,
        #[zbus(header)] header: Header<'_>,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> std::result::Result<(), BoxDBusError> {
        let path = header.path().ok_or(crate::Error::MissingField)?;
        let root = server.root().read().await;
        let iface = root.interface_at(path, &interface_name).map_err(|_| {
            Error::UnknownInterface(format!("Unknown interface '{interface_name}'"))
        })?;

        match iface.instance.read().await.set(
            property_name,
            value,
            server,
            connection,
            Some(&header),
            &emitter,
        ) {
            zbus::object_server::DispatchResult::RequiresMut => {}
            zbus::object_server::DispatchResult::NotFound => {
                return Err(
                    Error::UnknownProperty(format!("Unknown property '{property_name}'")).into(),
                );
            }
            zbus::object_server::DispatchResult::Async(f) => {
                return f.await;
            }
        }
        let res = iface
            .instance
            .write()
            .await
            .set_mut(
                property_name,
                value,
                server,
                connection,
                Some(&header),
                &emitter,
            )
            .await;
        res.unwrap_or_else(|| {
            Err(Error::UnknownProperty(format!("Unknown property '{property_name}'")).into())
        })
    }

    /// Get all properties.
    async fn get_all(
        &self,
        interface_name: InterfaceName<'_>,
        #[zbus(object_server)] server: &ObjectServer,
        #[zbus(connection)] connection: &Connection,
        #[zbus(header)] header: Header<'_>,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> Result<HashMap<String, OwnedVariant>> {
        let path = header.path().ok_or(crate::Error::MissingField)?;
        let root = server.root().read().await;
        let iface = root.interface_at(path, &interface_name).map_err(|_| {
            Error::UnknownInterface(format!("Unknown interface '{interface_name}'"))
        })?;

        let res = iface
            .instance
            .read()
            .await
            .get_all(server, connection, Some(&header), &emitter)
            .await?;
        Ok(res)
    }

    /// Emit the `org.freedesktop.DBus.Properties.PropertiesChanged` signal.
    #[zbus(signal)]
    #[rustfmt::skip]
    pub async fn properties_changed(
        emitter: &SignalEmitter<'_>,
        interface_name: InterfaceName<'_>,
        changed_properties: HashMap<&str, Variant<'_>>,
        invalidated_properties: Cow<'_, [&str]>,
    ) -> zbus::Result<()>;
}

/// Proxy for the `org.freedesktop.DBus.Properties` interface.
#[cfg(feature = "proxy")]
#[crate::proxy(interface = "org.freedesktop.DBus.Properties")]
pub trait Properties {
    /// Get a property value.
    fn get(&self, interface_name: InterfaceName<'_>, property_name: &str) -> Result<OwnedVariant>;

    /// Set a property value.
    fn set(
        &self,
        interface_name: InterfaceName<'_>,
        property_name: &str,
        value: &Variant<'_>,
    ) -> Result<()>;

    /// Get all properties.
    fn get_all(&self, interface_name: InterfaceName<'_>) -> Result<HashMap<String, OwnedVariant>>;

    /// Emit the `org.freedesktop.DBus.Properties.PropertiesChanged` signal.
    #[zbus(signal)]
    fn properties_changed(
        &self,
        interface_name: InterfaceName<'_>,
        changed_properties: HashMap<&str, Variant<'_>>,
        invalidated_properties: Cow<'_, [&str]>,
    ) -> zbus::Result<()>;
}
