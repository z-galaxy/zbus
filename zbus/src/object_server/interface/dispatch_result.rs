use std::{future::Future, pin::Pin};

use zbus::message::Flags;

use crate::{Connection, DynamicType, Error, fdo, message::Message};
use tracing::trace;

/// A helper type returned by [`Interface`](`crate::object_server::Interface`) callbacks.
///
/// The [`Async`](DispatchResult2::Async) variant uses [`fdo::Result`] so that D-Bus error names are
/// preserved without nesting through intermediate [`crate::Error`] conversions.
///
/// This is an unstable type — compatibility may break in minor version bumps.
pub enum DispatchResult2<'a> {
    /// This interface does not support the given method.
    NotFound,

    /// Retry with [`Interface::call_mut`](`crate::object_server::Interface::call_mut`).
    ///
    /// This is equivalent to NotFound if returned by call_mut.
    RequiresMut,

    /// The method was found and will be completed by running this Future.
    Async(Pin<Box<dyn Future<Output = fdo::Result<()>> + Send + 'a>>),
}

impl<'a> DispatchResult2<'a> {
    /// Helper for creating the Async variant.
    pub fn new_async<F, T, E>(conn: &'a Connection, msg: &'a Message, f: F) -> Self
    where
        F: Future<Output = ::std::result::Result<T, E>> + Send + 'a,
        T: serde::Serialize + DynamicType + Send + Sync,
        E: zbus::DBusError + Send,
    {
        DispatchResult2::Async(Box::pin(async move {
            let hdr = msg.header();
            let ret = f.await;
            if !hdr.primary().flags().contains(Flags::NoReplyExpected) {
                match ret {
                    Ok(r) => conn.reply(&hdr, &r).await,
                    Err(e) => conn.reply_dbus_error(&hdr, e).await,
                }
                .map(|_seq| ())
                .map_err(|e| fdo::Error::Failed(e.to_string()))
            } else {
                trace!("No reply expected for {:?} by the caller.", msg);
                Ok(())
            }
        }))
    }

    /// Wrap the future of a generated method handler as the `Async` variant.
    ///
    /// The `#[interface]` macro emits a call to this for every method so that the error conversion
    /// is compiled once rather than inlined into each handler.
    #[doc(hidden)]
    pub fn from_handler<F>(f: F) -> Self
    where
        F: Future<Output = crate::Result<()>> + Send + 'a,
    {
        DispatchResult2::Async(Box::pin(async move { f.await.map_err(handler_error) }))
    }
}

/// The D-Bus error a method handler's failure is reported as.
///
/// A [`DBusError`](crate::DBusError) is passed through unchanged. Anything else becomes
/// `org.freedesktop.DBus.Error.Failed` with the error's text as its description.
#[cold]
fn handler_error(e: Error) -> fdo::Error {
    match e {
        Error::FDO(e) => *e,
        e => fdo::Error::Failed(e.to_string()),
    }
}
