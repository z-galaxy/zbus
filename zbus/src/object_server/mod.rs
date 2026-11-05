//! The object server API.

use std::{any::Any, marker::PhantomData, sync::Arc};

use crate::{
    BoxDBusError, Connection, Error, ObjectPath, OwnedObjectPath, Result,
    connection::WeakConnection,
    fdo,
    log::{Instrument, debug, trace, trace_span},
    message::{Header, Message},
    names::InterfaceName,
    runtime::locks::RwLock,
};
#[cfg(feature = "object-manager")]
use crate::{Variant, fdo::ObjectManager};

mod interface;
pub(crate) use interface::ArcInterface;
#[doc(hidden)]
pub use interface::introspect_doc_comment;
pub use interface::{
    DispatchResult, Interface, InterfaceDeref, InterfaceDerefMut, InterfaceRef, IntoDBusError,
};

/// Deprecated alias of [`DispatchResult`].
#[deprecated(since = "6.0.0", note = "renamed to `DispatchResult`")]
pub type DispatchResult2<'a> = DispatchResult<'a>;

mod signal_emitter;
pub use signal_emitter::SignalEmitter;

mod dispatch_notifier;
pub use dispatch_notifier::{ResponseDispatchListener, ResponseDispatchNotifier};

mod node;
#[cfg(feature = "object-manager")]
use node::ManagerSignal;
pub(crate) use node::{Node, Unknown};

/// An object server, holding server-side D-Bus objects & interfaces.
///
/// Object servers hold interfaces on various object paths, and expose them over D-Bus.
///
/// All object paths will have the standard interfaces implemented on your behalf, such as
/// `org.freedesktop.DBus.Introspectable` or `org.freedesktop.DBus.Properties`.
///
/// # Example
///
/// This example exposes the `org.myiface.Example.Quit` method on the `/org/zbus/path`
/// path.
///
/// ```no_run
/// # use std::error::Error;
/// use zbus::{Connection, interface};
/// use zruntime::Event;
/// # use zbus::block_on;
///
/// struct Example {
///     // Interfaces are owned by the ObjectServer. They can have
///     // `&mut self` methods.
///     quit_event: Event,
/// }
///
/// impl Example {
///     fn new(quit_event: Event) -> Self {
///         Self { quit_event }
///     }
/// }
///
/// #[interface(name = "org.myiface.Example")]
/// impl Example {
///     // This will be the "Quit" D-Bus method.
///     async fn quit(&mut self) {
///         self.quit_event.notify(1);
///     }
///
///     // See `interface` documentation to learn
///     // how to expose properties & signals as well.
/// }
///
/// # block_on(async {
/// let connection = Connection::session().await?;
///
/// let quit_event = Event::new();
/// let quit_listener = quit_event.listen();
/// let interface = Example::new(quit_event);
/// connection
///     .object_server()
///     .at("/org/zbus/path", interface)
///     .await?;
///
/// quit_listener.await;
/// # Ok::<_, Box<dyn Error + Send + Sync>>(())
/// # })?;
/// # Ok::<_, Box<dyn Error + Send + Sync>>(())
/// ```
#[derive(Debug, Clone)]
pub struct ObjectServer {
    conn: WeakConnection,
    root: Arc<RwLock<Node>>,
}

impl ObjectServer {
    /// Create a new D-Bus `ObjectServer`.
    pub(crate) fn new(conn: &Connection) -> Self {
        Self {
            conn: conn.into(),
            root: Arc::new(RwLock::new(Node::new(
                "/".try_into().expect("zvariant bug"),
            ))),
        }
    }

    pub(crate) fn root(&self) -> &RwLock<Node> {
        &self.root
    }

    /// Register a D-Bus [`Interface`] at a given path (see the example above).
    ///
    /// Typically you'd want your interfaces to be registered immediately after the associated
    /// connection is established and therefore use [`zbus::connection::Builder::serve_at`] instead,
    /// which also gets calls served from the very start (see [`Connection::object_server`]).
    /// However, there are situations where you'd need to register interfaces dynamically and that's
    /// where this method becomes useful.
    ///
    /// Returns `None` on success. If an interface of the same name already exists at this path,
    /// the object server is left untouched and the passed `iface` is handed back as `Some`, so
    /// the caller can retry (e.g. at a different path) without having to reconstruct it.
    ///
    /// # Deadlocks
    ///
    /// It is fine to call this method from within an interface method (e.g. to register a child or
    /// sibling object on demand), including from a `&mut self` method. There are however two
    /// exceptions, which **deadlock** when done from within a `&mut self` method of the
    /// currently-executing interface:
    ///
    /// * Registering an `fdo::ObjectManager` at an ancestor of the interface.
    /// * If the interface is a fallback (see [`ObjectServer::at_fallback`]) and an `ObjectManager`
    ///   manages the objects it serves, registering an object that it serves, or unregistering the
    ///   interface of the same name of such an object, as it then serves that object in its place.
    ///
    /// This is because both read the properties of the interface, to emit `InterfacesAdded`
    /// signals, which requires a shared lock on it, while its exclusive lock is held for the
    /// duration of the `&mut self` method. Registering the `ObjectManager` up front (typically at
    /// connection set-up), or doing either from a `&self` method, avoids this. A `&self` method
    /// keeps whatever state it changes behind interior mutability.
    pub async fn at<'p, P, I>(&self, path: P, iface: I) -> Result<Option<I>>
    where
        I: Interface,
        P: TryInto<ObjectPath<'p>>,
        P::Error: Into<Error>,
    {
        self.register(path, iface, ArcInterface::new).await
    }

    /// Register a D-Bus [`Interface`] at a given path, and as a fallback for the paths below it.
    ///
    /// This serves objects that are not registered one by one, such as those with paths that are
    /// not known in advance, or that are only worth creating on demand. The interface serves
    /// `path` as one that [`ObjectServer::at`] registers would, as well as every path below it,
    /// unless that path has an interface of the same name of its own, or a closer ancestor of it
    /// has one registered as a fallback. A path that a fallback serves is an object even if
    /// nothing is registered at it, and it then has the standard interfaces as well.
    ///
    /// The same instance serves all those paths, so its methods learn which object a call is for
    /// from the path of the call. A method gets it from the header of the call, with a
    /// `#[zbus(header)]` argument. A property getter or setter gets it from a
    /// `#[zbus(signal_emitter)]` argument instead, as a getter has no header when it is not
    /// called for a D-Bus call. An interface can refuse a call for a path that it has no object
    /// for, with an error of its choice, such as [`fdo::Error::UnknownObject`]. Note that a
    /// `&mut self` method holds up the calls for all the paths while it runs, and that, under an
    /// `ObjectManager`, it cannot register the objects that the interface serves (see the
    /// deadlocks that [`ObjectServer::at`] describes). An interface that creates objects on demand
    /// does so from `&self` methods instead.
    ///
    /// The objects registered below `path` have the fallback as well: their introspection data
    /// lists it, and an `ObjectManager` reports it for them, with `InterfacesAdded` and
    /// `InterfacesRemoved` signals as registrations change what serves them. The paths with
    /// nothing registered at them are not enumerated though: they are not among the child nodes
    /// in the introspection data of their parents, nor among the objects that an `ObjectManager`
    /// reports. [`ObjectServer::interface`] gets the interface for any path that it serves, along
    /// with a [`SignalEmitter`] for that path.
    ///
    /// To unregister the interface, pass `path` to [`ObjectServer::remove`].
    ///
    /// Returns `None` on success. If an interface of the same name already exists at this path,
    /// registered as a fallback or not, the object server is left untouched and the passed
    /// `iface` is handed back as `Some`.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::error::Error;
    /// use zbus::{Connection, fdo, interface, message::Header};
    ///
    /// /// The items at `/org/zbus/Items/<id>`.
    /// struct Items;
    ///
    /// #[interface(name = "org.zbus.Item")]
    /// impl Items {
    ///     /// The ID of the item that the call is for.
    ///     fn id(&self, #[zbus(header)] header: Header<'_>) -> fdo::Result<String> {
    ///         let path = header.path().map(|path| path.as_str()).unwrap_or_default();
    ///         match path.strip_prefix("/org/zbus/Items/") {
    ///             Some(id) if !id.contains('/') => Ok(id.to_string()),
    ///             _ => Err(fdo::Error::UnknownObject(format!("No item at '{path}'"))),
    ///         }
    ///     }
    /// }
    ///
    /// # zbus::block_on(async {
    /// let connection = Connection::session().await?;
    /// let object_server = connection.object_server();
    /// object_server.at_fallback("/org/zbus/Items", Items).await?;
    ///
    /// // Nothing is registered at this path, but the fallback serves it.
    /// let item = object_server
    ///     .interface::<_, Items>("/org/zbus/Items/42")
    ///     .await?;
    /// assert_eq!(item.signal_emitter().path().as_str(), "/org/zbus/Items/42");
    /// # Ok::<_, Box<dyn Error + Send + Sync>>(())
    /// # })?;
    /// # Ok::<_, Box<dyn Error + Send + Sync>>(())
    /// ```
    pub async fn at_fallback<'p, P, I>(&self, path: P, iface: I) -> Result<Option<I>>
    where
        I: Interface,
        P: TryInto<ObjectPath<'p>>,
        P::Error: Into<Error>,
    {
        self.register(path, iface, ArcInterface::new_fallback).await
    }

    pub(crate) async fn add_arc_interface<'p, P>(
        &self,
        path: P,
        name: InterfaceName<'static>,
        arc_iface: ArcInterface,
    ) -> Result<bool>
    where
        P: TryInto<ObjectPath<'p>>,
        P::Error: Into<Error>,
    {
        self.add_interface_with(path, name, || arc_iface).await
    }

    /// Register `iface` at `path`, as `wrap` wraps it, handing it back if the path already has
    /// an interface of the same name.
    async fn register<'p, P, I>(
        &self,
        path: P,
        iface: I,
        wrap: fn(I) -> ArcInterface,
    ) -> Result<Option<I>>
    where
        I: Interface,
        P: TryInto<ObjectPath<'p>>,
        P::Error: Into<Error>,
    {
        // `iface` is only moved into the object server once we know the slot is free, so that we
        // can hand it back to the caller otherwise.
        let mut iface = Some(iface);
        let added = self
            .add_interface_with(path, I::name(), || {
                wrap(iface.take().expect("closure is called at most once"))
            })
            .await?;

        Ok(if added { None } else { iface })
    }

    /// Register the interface produced by `make` at `path`, under `name`.
    ///
    /// `make` is only called if no interface with that name exists at the path yet. Returns
    /// whether the interface was added.
    async fn add_interface_with<'p, P, F>(
        &self,
        path: P,
        name: InterfaceName<'static>,
        make: F,
    ) -> Result<bool>
    where
        P: TryInto<ObjectPath<'p>>,
        P::Error: Into<Error>,
        F: FnOnce() -> ArcInterface,
    {
        let path = path.try_into().map_err(Into::into)?;
        let mut root = self.root().write().await;
        let existing_depth = root.existing_depth(&path);
        let (node, manager_path) = root.get_child_mut(&path, true);
        let manager_path = manager_path.map(OwnedObjectPath::from);
        let node = node.unwrap();
        if node.has_interface(&name) {
            // The nodes on the path may have been auto-created just now for this very
            // registration; remove the ones that nothing else needs.
            root.remove_node(&path);
            return Ok(false);
        }
        let added = node.add_arc_interface(name.clone(), make());
        debug_assert!(added);
        self.interfaces_added(&root, path, name, existing_depth, manager_path)
            .await?;

        Ok(true)
    }

    /// Unregister a D-Bus [`Interface`] at a given path.
    ///
    /// If there are no more interfaces or children left at that path, destroys the object as
    /// well, along with any ancestor objects that are thereby left without interfaces or
    /// children of their own. The root object is never destroyed. Returns whether the object
    /// was destroyed.
    pub async fn remove<'p, I, P>(&self, path: P) -> Result<bool>
    where
        I: Interface,
        P: TryInto<ObjectPath<'p>>,
        P::Error: Into<Error>,
    {
        self.remove_named(path, I::name()).await
    }

    /// Unregister a D-Bus [`Interface`] at a given path, using its name.
    ///
    /// If there are no more interfaces or children left at that path, destroys the object as
    /// well, along with any ancestor objects that are thereby left without interfaces or
    /// children of their own. The root object is never destroyed. Returns whether the object
    /// was destroyed.
    pub async fn remove_named<'p, P>(
        &self,
        path: P,
        interface_name: InterfaceName<'static>,
    ) -> Result<bool>
    where
        P: TryInto<ObjectPath<'p>>,
        P::Error: Into<Error>,
    {
        let path = path.try_into().map_err(Into::into)?;
        let mut root = self.root.write().await;
        let (node, manager_path) = root.get_child_mut(&path, false);
        let manager_path = manager_path.map(OwnedObjectPath::from);
        let node = node.ok_or(Error::InterfaceNotFound)?;
        let removed = node
            .remove_interface(&interface_name)
            .ok_or(Error::InterfaceNotFound)?;
        // Prune before emitting the (fallible) signals, so that an emission failure can't leave
        // empty nodes behind.
        let pruned = root.remove_node(&path);
        self.interfaces_removed(&root, path, interface_name, removed, &pruned, manager_path)
            .await?;

        Ok(!pruned.is_empty())
    }

    /// Emit the signals of the `ObjectManager`s for the interface `name` just registered at
    /// `path`, `existing_depth` being the [`Node::existing_depth`] of `path` beforehand.
    ///
    /// The signals about `path` and the nodes created on the way to it go out from
    /// `manager_path`, the closest ancestor serving an `ObjectManager`, if there is one (see
    /// [`Node::added_signals`]). When the interface is itself an `ObjectManager`, every object
    /// under `path` is announced from `path` as well.
    #[cfg(feature = "object-manager")]
    async fn interfaces_added(
        &self,
        root: &Node,
        path: ObjectPath<'_>,
        name: InterfaceName<'static>,
        existing_depth: usize,
        manager_path: Option<OwnedObjectPath>,
    ) -> Result<()> {
        if name == ObjectManager::name() {
            // Just added an object manager. Need to signal all managed objects under it.
            let (node, fallbacks) = root.with_fallbacks(&path);
            let node = node.expect("the object manager was registered at an existing node");
            let emitter = SignalEmitter::new(&self.connection(), path.clone())?;
            let objects = node
                .get_managed_objects(fallbacks, self, &self.connection())
                .await?;
            for (path, owned_interfaces) in objects {
                let interfaces = owned_interfaces
                    .iter()
                    .map(|(i, props)| {
                        let props = props
                            .iter()
                            .map(|(k, v)| Ok((k.as_str(), Variant::try_from(v)?)))
                            .collect::<Result<_>>();
                        Ok((i.into(), props?))
                    })
                    .collect::<Result<_>>()?;
                ObjectManager::interfaces_added(&emitter, path.into(), interfaces).await?;
            }
        }
        let signals = root.added_signals(&path, &name, existing_depth, manager_path.as_ref());

        self.emit_manager_signals(signals).await
    }

    #[cfg(not(feature = "object-manager"))]
    async fn interfaces_added(
        &self,
        _root: &Node,
        _path: ObjectPath<'_>,
        _name: InterfaceName<'static>,
        _existing_depth: usize,
        _manager_path: Option<OwnedObjectPath>,
    ) -> Result<()> {
        Ok(())
    }

    /// Emit the signals of the `ObjectManager`s for `removed`, the interface `name` just
    /// unregistered from `path`, `pruned` being the paths of the nodes removed along with it.
    ///
    /// `manager_path` is the closest ancestor of `path` serving an `ObjectManager`, if there is
    /// one (see [`Node::removed_signals`]).
    #[cfg(feature = "object-manager")]
    async fn interfaces_removed(
        &self,
        root: &Node,
        path: ObjectPath<'_>,
        name: InterfaceName<'static>,
        removed: ArcInterface,
        pruned: &[OwnedObjectPath],
        manager_path: Option<OwnedObjectPath>,
    ) -> Result<()> {
        let signals = root.removed_signals(&path, &name, &removed, pruned, manager_path.as_ref());

        self.emit_manager_signals(signals).await
    }

    #[cfg(not(feature = "object-manager"))]
    async fn interfaces_removed(
        &self,
        _root: &Node,
        _path: ObjectPath<'_>,
        _name: InterfaceName<'static>,
        _removed: ArcInterface,
        _pruned: &[OwnedObjectPath],
        _manager_path: Option<OwnedObjectPath>,
    ) -> Result<()> {
        Ok(())
    }

    /// Emit `signals` from the `ObjectManager`s that they are for.
    #[cfg(feature = "object-manager")]
    async fn emit_manager_signals(&self, signals: Vec<ManagerSignal>) -> Result<()> {
        let connection = self.connection();
        for signal in signals {
            match signal {
                ManagerSignal::Added {
                    manager,
                    path,
                    interfaces,
                } => {
                    let mut owned_interfaces = Vec::with_capacity(interfaces.len());
                    for (name, iface) in interfaces {
                        let props = node::properties(&iface, &path, self, &connection).await?;
                        owned_interfaces.push((name, props));
                    }
                    let interfaces = owned_interfaces
                        .iter()
                        .map(|(name, props)| {
                            let props = props
                                .iter()
                                .map(|(k, v)| Ok((k.as_str(), Variant::try_from(v)?)))
                                .collect::<Result<_>>()?;
                            Ok((name.clone(), props))
                        })
                        .collect::<Result<_>>()?;
                    let emitter = SignalEmitter::new(&connection, manager)?;
                    ObjectManager::interfaces_added(&emitter, path.into(), interfaces).await?;
                }
                ManagerSignal::Removed {
                    manager,
                    path,
                    interfaces,
                } => {
                    let emitter = SignalEmitter::new(&connection, manager)?;
                    ObjectManager::interfaces_removed(&emitter, path.into(), interfaces.into())
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// Get the interface at the given path.
    ///
    /// That is the interface that serves calls to the path: the one registered at the path itself,
    /// or else a fallback registered at an ancestor of it (see [`ObjectServer::at_fallback`]). The
    /// [`SignalEmitter`] of the returned [`InterfaceRef`] is for the given path either way.
    ///
    /// # Errors
    ///
    /// If no such interface serves the given path, an `Error::InterfaceNotFound` error is
    /// returned.
    ///
    /// # Examples
    ///
    /// The typical use of this is property changes outside of a dispatched handler:
    ///
    /// ```no_run
    /// # use std::error::Error;
    /// # use zbus::{Connection, interface};
    /// # use zbus::block_on;
    /// #
    /// struct MyIface(u32);
    ///
    /// #[interface(name = "org.myiface.MyIface")]
    /// impl MyIface {
    ///      #[zbus(property)]
    ///      async fn count(&self) -> u32 {
    ///          self.0
    ///      }
    /// }
    ///
    /// # block_on(async {
    /// # let connection = Connection::session().await?;
    /// #
    /// # let path = "/org/zbus/path";
    /// # connection.object_server().at(path, MyIface(0)).await?;
    /// let iface_ref = connection
    ///     .object_server()
    ///     .interface::<_, MyIface>(path).await?;
    /// let mut iface = iface_ref.get_mut().await;
    /// iface.0 = 42;
    /// iface.count_changed(iface_ref.signal_emitter()).await?;
    /// # Ok::<_, Box<dyn Error + Send + Sync>>(())
    /// # })?;
    /// #
    /// # Ok::<_, Box<dyn Error + Send + Sync>>(())
    /// ```
    pub async fn interface<'p, P, I>(&self, path: P) -> Result<InterfaceRef<I>>
    where
        I: Interface,
        P: TryInto<ObjectPath<'p>>,
        P::Error: Into<Error>,
    {
        let path = path.try_into().map_err(Into::into)?;
        let root = self.root().read().await;
        let lock = root
            .interface_at(&path, &I::name())
            .map_err(|_| Error::InterfaceNotFound)?
            .instance;

        // Ensure what we return can later be downcast safely.
        <dyn Any>::downcast_ref::<I>(&*lock.read().await).ok_or(Error::InterfaceNotFound)?;

        let conn = self.connection();
        // SAFETY: We know that there is a valid path on the node as we already converted w/o error.
        let emitter = SignalEmitter::new(&conn, path).unwrap().into_owned();

        Ok(InterfaceRef {
            emitter,
            lock,
            phantom: PhantomData,
        })
    }

    async fn dispatch_call_to_iface(
        &self,
        iface: Arc<RwLock<dyn Interface>>,
        connection: &Connection,
        msg: &Message,
        hdr: &Header<'_>,
    ) -> std::result::Result<(), BoxDBusError> {
        let member = hdr
            .member()
            .ok_or_else(|| fdo::Error::Failed("Missing member".into()))?;
        let iface_name = hdr
            .interface()
            .ok_or_else(|| fdo::Error::Failed("Missing interface".into()))?;

        trace!("acquiring read lock on interface `{}`", iface_name);
        let read_lock = iface.read().await;
        trace!("acquired read lock on interface `{}`", iface_name);
        match read_lock.call(self, connection, msg, member.as_ref()) {
            DispatchResult::NotFound => {
                return Err(fdo::Error::UnknownMethod(format!("Unknown method '{member}'")).into());
            }
            DispatchResult::Async(f) => {
                return f.await;
            }
            DispatchResult::RequiresMut => {}
        }
        drop(read_lock);
        trace!("acquiring write lock on interface `{}`", iface_name);
        let mut write_lock = iface.write().await;
        trace!("acquired write lock on interface `{}`", iface_name);
        match write_lock.call_mut(self, connection, msg, member.as_ref()) {
            DispatchResult::NotFound => {}
            DispatchResult::RequiresMut => {}
            DispatchResult::Async(f) => {
                return f.await;
            }
        }
        drop(write_lock);
        Err(fdo::Error::UnknownMethod(format!("Unknown method '{member}'")).into())
    }

    async fn dispatch_method_call_try(
        &self,
        connection: &Connection,
        msg: &Message,
        hdr: &Header<'_>,
    ) -> std::result::Result<(), BoxDBusError> {
        let path = hdr
            .path()
            .ok_or_else(|| fdo::Error::Failed("Missing object path".into()))?;
        let iface_name = hdr
            .interface()
            // TODO: In the absence of an INTERFACE field, if two or more interfaces on the same
            // object have a method with the same name, it is undefined which of those
            // methods will be invoked. Implementations may choose to either return an
            // error, or deliver the message as though it had an arbitrary one of those
            // interfaces.
            .ok_or_else(|| fdo::Error::Failed("Missing interface".into()))?;
        // Check that the message has a member before spawning.
        // Note that an unknown member will still spawn a task. We should instead gather
        // all the details for the call before spawning.
        // See also https://github.com/z-galaxy/zbus/issues/674 for future of Interface.
        let _ = hdr
            .member()
            .ok_or_else(|| fdo::Error::Failed("Missing member".into()))?;

        // Ensure the root lock isn't held while dispatching the message. That
        // way, the object server can be mutated during that time.
        let (iface, with_spawn) = {
            let root = self.root.read().await;

            // D-Bus spec: org.freedesktop.DBus.Peer interface works on ANY path, even unregistered
            // ones. See: https://dbus.freedesktop.org/doc/dbus-specification.html#standard-interfaces-peer
            // Switch the path to "/" for Peer interface calls.
            let path = if *iface_name == fdo::Peer::name() {
                ObjectPath::from_static_str_unchecked("/")
            } else {
                path.clone()
            };

            let iface = root
                .interface_at(&path, iface_name)
                .map_err(|unknown| match unknown {
                    Unknown::Object => {
                        fdo::Error::UnknownObject(format!("Unknown object '{path}'"))
                    }
                    Unknown::Interface => {
                        fdo::Error::UnknownInterface(format!("Unknown interface '{iface_name}'"))
                    }
                })?;
            (iface.instance, iface.spawn_tasks_for_methods)
        };

        if with_spawn {
            let task_name = format!("`{msg}` method dispatcher");
            let runtime = connection.runtime().clone();
            let connection = connection.clone();
            let msg = msg.clone();
            let dispatch = async move {
                let server = connection.object_server();
                let hdr = msg.header();
                if let Err(e) = server
                    .dispatch_call_to_iface(iface, &connection, &msg, &hdr)
                    .await
                {
                    // When not spawning a task, this error is handled by the caller.
                    debug!("Returning error: {}", e);
                    if let Err(e) = connection.reply_dbus_error(&hdr, e).await {
                        debug!(
                            "Error dispatching message. Message: {:?}, error: {:?}",
                            msg, e
                        );
                    }
                }
            }
            .instrument(trace_span!("{}", task_name));
            // The span above is built from a borrow of `task_name`; moved in only now that it is
            // no longer needed, so the runtime is handed the `String` instead of a copy of it.
            runtime.spawn(task_name, dispatch).detach();
            Ok(())
        } else {
            self.dispatch_call_to_iface(iface, connection, msg, hdr)
                .await
        }
    }

    /// Dispatch an incoming message to a registered interface.
    ///
    /// The object server will handle the message by:
    ///
    /// - looking up the called object path & interface,
    ///
    /// - calling the associated method if one exists,
    ///
    /// - returning a message (responding to the caller with either a return or error message) to
    ///   the caller through the associated server connection.
    ///
    /// Returns an error if the message is malformed.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    pub(crate) async fn dispatch_call(&self, msg: &Message, hdr: &Header<'_>) -> Result<()> {
        let conn = self.connection();

        if let Err(e) = self.dispatch_method_call_try(&conn, msg, hdr).await {
            debug!("Returning error: {}", e);
            conn.reply_dbus_error(hdr, e).await?;
        }
        trace!("Handled: {}", msg);

        Ok(())
    }

    pub(crate) fn connection(&self) -> Connection {
        self.conn
            .upgrade()
            .expect("ObjectServer can't exist w/o an associated Connection")
    }
}
