//! The handle to a task a connection spawned on its runtime.

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use super::erased::ExternalTask;

/// A spawned task. Dropping the handle cancels the task;
/// [`TaskHandle::detach`](super::traits::TaskHandle::detach) lets it run on.
#[derive(Debug)]
pub(crate) enum Task<T> {
    #[cfg(feature = "async-io")]
    Builtin(super::builtin::Task<T>),
    #[cfg(feature = "tokio")]
    Tokio(super::tokio_rt::TokioTask<T>),
    /// A task on a runtime the builder was handed, reached through the object-safe mirror of
    /// the task trait.
    External(ExternalTask<T>),
}

impl<T> Task<T>
where
    T: Send + 'static,
{
    /// Detaches the task to let it keep running in the background.
    pub(crate) fn detach(self) {
        match self {
            #[cfg(feature = "async-io")]
            Self::Builtin(task) => super::traits::TaskHandle::detach(task),
            #[cfg(feature = "tokio")]
            Self::Tokio(task) => super::traits::TaskHandle::detach(task),
            Self::External(handle) => super::traits::TaskHandle::detach(handle),
        }
    }
}

impl<T> Future for Task<T>
where
    T: Send + 'static,
{
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            #[cfg(feature = "async-io")]
            Self::Builtin(task) => Pin::new(task).poll(cx),
            #[cfg(feature = "tokio")]
            Self::Tokio(task) => Pin::new(task).poll(cx),
            Self::External(handle) => Pin::new(handle).poll(cx),
        }
    }
}

impl<T> super::traits::TaskHandle<T> for Task<T>
where
    T: Send + 'static,
{
    fn detach(self) {
        Task::detach(self)
    }
}
