use std::{fmt, sync::Arc};

use crate::{
    Error, Result, fdo,
    message::{Header, Message},
    names::ErrorName,
};

/// A trait that needs to be implemented by error types to be returned from D-Bus methods.
///
/// Typically, you'd use the [`crate::fdo::Error`] since that covers quite a lot of failures but
/// occasionally you might find yourself needing to use a custom error type. You'll need to
/// implement this trait for your error type. The easiest way to achieve that is to make use of the
/// [`DBusError` macro][dm].
///
/// [dm]: derive.DBusError.html
pub trait DBusError {
    /// Generate an error reply message for the given method call.
    fn create_reply(&self, msg: &Header<'_>) -> Result<Message>;

    /// The name of the error.
    ///
    /// Every D-Bus error must have a name. See [`ErrorName`] for more information.
    fn name(&self) -> ErrorName<'_>;

    /// The optional description for the error.
    fn description(&self) -> Option<&str>;
}

/// A [`DBusError`] of any concrete type.
///
/// This is how the object server carries a property getter's or setter's failure, whatever error
/// type the handler fails with. The box is only allocated on failure.
pub type BoxDBusError = Box<dyn DBusError + Send + Sync>;

impl fmt::Display for dyn DBusError + Send + Sync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let description = self.description().unwrap_or("no description");
        write!(f, "{}: {description}", self.name())
    }
}

impl fmt::Debug for dyn DBusError + Send + Sync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DBusError")
            .field("name", &self.name())
            .field("description", &self.description())
            .finish()
    }
}

impl<E> DBusError for Box<E>
where
    E: DBusError + ?Sized,
{
    fn create_reply(&self, msg: &Header<'_>) -> Result<Message> {
        (**self).create_reply(msg)
    }

    fn name(&self) -> ErrorName<'_> {
        (**self).name()
    }

    fn description(&self) -> Option<&str> {
        (**self).description()
    }
}

impl<E> DBusError for Arc<E>
where
    E: DBusError + ?Sized,
{
    fn create_reply(&self, msg: &Header<'_>) -> Result<Message> {
        (**self).create_reply(msg)
    }

    fn name(&self) -> ErrorName<'_> {
        (**self).name()
    }

    fn description(&self) -> Option<&str> {
        (**self).description()
    }
}

impl From<fdo::Error> for BoxDBusError {
    fn from(e: fdo::Error) -> Self {
        Box::new(e)
    }
}

/// An [`fdo::Error`] or other [`DBusError`] carried by an [`Error`] is boxed as is. Any other
/// [`Error`] is reported the way [`fdo::Error`] reports it: the error reply to a call under its own
/// name, and an error of zbus's own, such as an I/O error, as `org.freedesktop.zbus.Error`.
impl From<Error> for BoxDBusError {
    fn from(e: Error) -> Self {
        match e {
            Error::FDO(e) => e,
            Error::DBus(e) => Box::new(e),
            e => Box::new(fdo::Error::from(e)),
        }
    }
}
