mod dispatch_result;
pub use dispatch_result::*;
mod interface_ref;
pub use interface_ref::*;
mod interface_deref;
pub use interface_deref::*;

use std::{
    any::Any,
    collections::HashMap,
    fmt::{self, Write},
    sync::Arc,
};

use async_trait::async_trait;

use crate::{
    BoxDBusError, Connection, DBusError, Error, ObjectServer, OwnedVariant, Variant, fdo,
    message::{self, Header, Message},
    names::{InterfaceName, MemberName},
    object_server::SignalEmitter,
    runtime::locks::RwLock,
};

/// Write `lines` as an XML comment at the given indentation level.
///
/// This is what the `#[interface]` macro uses to write doc comments in the introspection data.
#[doc(hidden)]
pub fn introspect_doc_comment(writer: &mut dyn Write, level: usize, lines: &str) {
    writeln!(writer, "{:indent$}<!--", "", indent = level).unwrap();
    for line in lines.split('\n') {
        if line.is_empty() {
            writeln!(writer).unwrap();
        } else {
            writeln!(writer, "{:indent$}{line}", "", indent = level).unwrap();
        }
    }
    writeln!(writer, "{:indent$} -->", "", indent = level).unwrap();
}

/// This trait is used to dispatch messages to an interface instance.
///
/// This trait should be treated as an unstable API and compatibility may break in minor
/// version bumps. Because of this and other reasons, it is not recommended to manually implement
/// this trait. The [`crate::interface`] macro implements it for you.
///
/// If you have an advanced use case where `interface` is inadequate, consider using
/// [`crate::MessageStream`] instead.
#[async_trait]
pub trait Interface: Any + Send + Sync {
    /// Return the name of the interface. Ex: "org.foo.MyInterface"
    fn name() -> InterfaceName<'static>
    where
        Self: Sized;

    /// Whether each method call will be handled from a different spawned task.
    ///
    /// Note: When methods are called from separate tasks, they may not be run in the order in which
    /// they were called.
    fn spawn_tasks_for_methods(&self) -> bool {
        true
    }

    /// Get a property value. Returns `None` if the property doesn't exist.
    ///
    /// A getter may fail with any D-Bus error, hence the boxed error type.
    ///
    /// Note: The header parameter will be None when the getter is not being called as part
    /// of D-Bus communication (for example, when it is called as part of initial object setup,
    /// before it is registered on the bus, or when we manually send out property changed
    /// notifications).
    async fn get(
        &self,
        property_name: &str,
        server: &ObjectServer,
        connection: &Connection,
        header: Option<&message::Header<'_>>,
        emitter: &SignalEmitter<'_>,
    ) -> Option<Result<OwnedVariant, BoxDBusError>>;

    /// Return all the properties.
    ///
    /// A property whose getter fails is left out, so this only fails when a value cannot be
    /// serialized.
    async fn get_all(
        &self,
        object_server: &ObjectServer,
        connection: &Connection,
        header: Option<&message::Header<'_>>,
        emitter: &SignalEmitter<'_>,
    ) -> fdo::Result<HashMap<String, OwnedVariant>>;

    /// Set a property value.
    ///
    /// Return [`DispatchResult::NotFound`] if the property doesn't exist, or
    /// [`DispatchResult::RequiresMut`] if `set_mut` should be used instead. The default
    /// implementation just returns `RequiresMut`. The [`DispatchResult::Async`] future fails
    /// with whatever D-Bus error the setter fails with.
    fn set<'call>(
        &'call self,
        property_name: &'call str,
        value: &'call Variant<'_>,
        object_server: &'call ObjectServer,
        connection: &'call Connection,
        header: Option<&'call message::Header<'_>>,
        emitter: &'call SignalEmitter<'_>,
    ) -> DispatchResult<'call> {
        let _ = (
            property_name,
            value,
            object_server,
            connection,
            header,
            emitter,
        );
        DispatchResult::RequiresMut
    }

    /// Set a property value.
    ///
    /// Returns `None` if the property doesn't exist. A setter may fail with any D-Bus error,
    /// hence the boxed error type.
    ///
    /// This will only be invoked if `set` returned `RequiresMut`.
    async fn set_mut(
        &mut self,
        property_name: &str,
        value: &Variant<'_>,
        object_server: &ObjectServer,
        connection: &Connection,
        header: Option<&Header<'_>>,
        emitter: &SignalEmitter<'_>,
    ) -> Option<Result<(), BoxDBusError>>;

    /// Call a method.
    ///
    /// Return [`DispatchResult::NotFound`] if the method doesn't exist, or
    /// [`DispatchResult::RequiresMut`] if `call_mut` should be used instead.
    ///
    /// It is valid, though inefficient, for this to always return `RequiresMut`.
    fn call<'call>(
        &'call self,
        server: &'call ObjectServer,
        connection: &'call Connection,
        msg: &'call Message,
        name: MemberName<'call>,
    ) -> DispatchResult<'call>;

    /// Call a `&mut self` method.
    ///
    /// This will only be invoked if `call` returned `RequiresMut`.
    fn call_mut<'call>(
        &'call mut self,
        server: &'call ObjectServer,
        connection: &'call Connection,
        msg: &'call Message,
        name: MemberName<'call>,
    ) -> DispatchResult<'call>;

    /// Write introspection XML to the writer, with the given indentation level.
    fn introspect_to_writer(&self, writer: &mut dyn Write, level: usize);
}

/// Conversion of a property handler's failure into the boxed error [`Interface`] carries.
///
/// The `#[interface]` macro applies it to whatever a getter or setter returns. Any `'static`
/// [`DBusError`] is boxed as is. A plain [`Error`] is not a D-Bus error, so it is reported the
/// way [`fdo::Error`] reports it.
#[doc(hidden)]
pub trait IntoDBusError {
    fn into_dbus_error(self) -> BoxDBusError;

    /// The failure as an [`Error`], for a caller that is not replying to a D-Bus call.
    ///
    /// A getter's failure reaches the caller of the generated `PropertiesChanged` emission
    /// helper this way, as [`Error::DBus`].
    fn into_error(self) -> Error;
}

impl<E> IntoDBusError for E
where
    E: DBusError + Send + Sync + 'static,
{
    fn into_dbus_error(self) -> BoxDBusError {
        Box::new(self)
    }

    fn into_error(self) -> Error {
        Error::DBus(Arc::new(self))
    }
}

impl IntoDBusError for Error {
    fn into_dbus_error(self) -> BoxDBusError {
        self.into()
    }

    fn into_error(self) -> Error {
        self
    }
}

/// A type for a reference-counted Interface trait-object, with associated run-time details and a
/// manual Debug impl.
#[derive(Clone)]
pub(crate) struct ArcInterface {
    pub instance: Arc<RwLock<dyn Interface>>,
    pub spawn_tasks_for_methods: bool,
    /// Whether the interface also serves the paths below the one it is registered at.
    pub fallback: bool,
}

impl ArcInterface {
    pub fn new<I>(iface: I) -> Self
    where
        I: Interface,
    {
        let spawn_tasks_for_methods = iface.spawn_tasks_for_methods();
        Self {
            instance: Arc::new(RwLock::new(iface)),
            spawn_tasks_for_methods,
            fallback: false,
        }
    }

    /// Like [`ArcInterface::new`], for an interface that also serves the paths below the one it
    /// is registered at.
    pub fn new_fallback<I>(iface: I) -> Self
    where
        I: Interface,
    {
        Self {
            fallback: true,
            ..Self::new(iface)
        }
    }
}

impl fmt::Debug for ArcInterface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Arc<RwLock<dyn Interface>>")
            .finish_non_exhaustive()
    }
}
