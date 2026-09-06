//! D-Bus standard interfaces.
//!
//! The D-Bus specification defines the message bus messages and some standard interfaces that may
//! be useful across various D-Bus applications. This module provides their proxy.

#[cfg(any(feature = "proxy", feature = "object-manager"))]
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

#[cfg(feature = "object-manager")]
use super::Error;
#[cfg(any(feature = "proxy", feature = "object-manager"))]
use super::Result;
#[cfg(feature = "object-manager")]
use crate::{Connection, ObjectServer, interface, message::Header, object_server::SignalEmitter};
#[cfg(any(feature = "proxy", feature = "object-manager"))]
use crate::{ObjectPath, Value, names::InterfaceName};
use crate::{OwnedObjectPath, OwnedValue, names::OwnedInterfaceName};

#[cfg_attr(
    feature = "proxy",
    doc = "The type returned by the [`ObjectManagerProxy::get_managed_objects`] method."
)]
#[cfg_attr(
    not(feature = "proxy"),
    doc = "The type returned by `ObjectManagerProxy::get_managed_objects` (requires the `proxy`",
    doc = "feature)."
)]
pub type ManagedObjects =
    HashMap<OwnedObjectPath, BTreeMap<OwnedInterfaceName, HashMap<String, OwnedValue>>>;

/// Service-side [Object Manager][om] interface implementation.
///
/// The recommended path to add this interface at is the path form of the well-known name of a D-Bus
/// service, or below. For example, if a D-Bus service is available at the well-known name
/// `org.freedesktop.zbus.ExampleService1`, this interface should typically be registered at
/// `/org/freedesktop/zbus/ExampleService1`, or below (to allow for multiple object managers in a
/// service).
///
/// It is supported, but not recommended, to add this interface at the root path, `/`.
///
/// When added to an `ObjectServer`, the `InterfacesAdded` signal is emitted for all the objects
/// under the `path` it's added at. You can use this fact to minimize the signal emissions by
/// populating the entire (sub)tree under `path` before registering an object manager.
///
/// Because registering it reads the properties of every object under `path`, adding an
/// `ObjectManager` at an ancestor of an interface from within that interface's own `&mut self`
/// method will deadlock. See [`ObjectServer::at`](crate::ObjectServer::at) for details.
///
/// # Example
///
/// Add the provided `ObjectManager` at a path via the [`ObjectServer`](crate::ObjectServer), and
/// every object registered under that path is tracked for you, with `InterfacesAdded` and
/// `InterfacesRemoved` signals emitted automatically.
///
/// ```
/// #
/// # zbus::block_on(async {
/// use zbus::{Connection, fdo::ObjectManager, interface};
///
/// let connection = Connection::session().await?;
/// let object_server = connection.object_server();
///
/// // Register the object manager first, then add the objects it should manage below its path.
/// object_server
///     .at("/org/freedesktop/zbus/ExampleObject1", ObjectManager)
///     .await?;
/// object_server
///     .at(
///         "/org/freedesktop/zbus/ExampleObject1/item1",
///         Item { name: "First".to_string() },
///     )
///     .await?;
///
/// // The specific interface being managed is not the interesting part here.
/// struct Item {
///     name: String,
/// }
///
/// #[interface(name = "org.freedesktop.zbus.ExampleObject1.Item")]
/// impl Item {
///     #[zbus(property)]
///     async fn name(&self) -> String {
///         self.name.clone()
///     }
/// }
/// # Ok::<(), zbus::Error>(())
/// # }).unwrap();
/// ```
///
/// [om]: https://dbus.freedesktop.org/doc/dbus-specification.html#standard-interfaces-objectmanager
#[cfg(feature = "object-manager")]
#[derive(Debug, Clone)]
pub struct ObjectManager;

#[cfg(feature = "object-manager")]
#[interface(
    name = "org.freedesktop.DBus.ObjectManager",
    introspection_docs = false
)]
impl ObjectManager {
    /// The return value of this method is a dict whose keys are object paths. All returned object
    /// paths are children of the object path implementing this interface, i.e. their object paths
    /// start with the ObjectManager's object path plus '/'.
    ///
    /// Each value is a dict whose keys are interfaces names. Each value in this inner dict is the
    /// same dict that would be returned by the org.freedesktop.DBus.Properties.GetAll() method for
    /// that combination of object path and interface. If an interface has no properties, the empty
    /// dict is returned.
    async fn get_managed_objects(
        &self,
        #[zbus(object_server)] server: &ObjectServer,
        #[zbus(connection)] connection: &Connection,
        #[zbus(header)] header: Header<'_>,
    ) -> Result<ManagedObjects> {
        let path = header.path().ok_or(crate::Error::MissingField)?;
        let root = server.root().read().await;
        let node = root
            .get_child(path)
            .ok_or_else(|| Error::UnknownObject(format!("Unknown object '{path}'")))?;

        node.get_managed_objects(server, connection).await
    }

    /// This signal is emitted when either a new object is added or when an existing object gains
    /// one or more interfaces. The `interfaces_and_properties` argument contains a map with the
    /// interfaces and properties (if any) that have been added to the given object path.
    #[zbus(signal)]
    pub async fn interfaces_added(
        emitter: &SignalEmitter<'_>,
        object_path: ObjectPath<'_>,
        interfaces_and_properties: HashMap<InterfaceName<'_>, HashMap<&str, Value<'_>>>,
    ) -> zbus::Result<()>;

    /// This signal is emitted whenever an object is removed or it loses one or more interfaces.
    /// The `interfaces` parameters contains a list of the interfaces that were removed.
    #[zbus(signal)]
    pub async fn interfaces_removed(
        emitter: &SignalEmitter<'_>,
        object_path: ObjectPath<'_>,
        interfaces: Cow<'_, [InterfaceName<'_>]>,
    ) -> zbus::Result<()>;
}

/// Proxy for the `org.freedesktop.DBus.ObjectManager` interface.
#[cfg(feature = "proxy")]
#[crate::proxy(interface = "org.freedesktop.DBus.ObjectManager")]
pub trait ObjectManager {
    /// The return value of this method is a dict whose keys are object paths. All returned object
    /// paths are children of the object path implementing this interface, i.e. their object paths
    /// start with the ObjectManager's object path plus '/'.
    ///
    /// Each value is a dict whose keys are interfaces names. Each value in this inner dict is the
    /// same dict that would be returned by the org.freedesktop.DBus.Properties.GetAll() method for
    /// that combination of object path and interface. If an interface has no properties, the empty
    /// dict is returned.
    fn get_managed_objects(&self) -> Result<ManagedObjects>;

    /// This signal is emitted when either a new object is added or when an existing object gains
    /// one or more interfaces. The `interfaces_and_properties` argument contains a map with the
    /// interfaces and properties (if any) that have been added to the given object path.
    #[zbus(signal)]
    fn interfaces_added(
        &self,
        object_path: ObjectPath<'_>,
        interfaces_and_properties: HashMap<InterfaceName<'_>, HashMap<&str, Value<'_>>>,
    ) -> zbus::Result<()>;

    /// This signal is emitted whenever an object is removed or it loses one or more interfaces.
    /// The `interfaces` parameters contains a list of the interfaces that were removed.
    #[zbus(signal)]
    fn interfaces_removed(
        &self,
        object_path: ObjectPath<'_>,
        interfaces: Cow<'_, [InterfaceName<'_>]>,
    ) -> zbus::Result<()>;
}
