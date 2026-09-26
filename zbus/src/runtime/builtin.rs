//! The built-in backend: [zruntime]'s reactor for readiness, its timer, its tasks and the
//! standard hook for blocking work.
//!
//! [zruntime]: https://docs.rs/zruntime

use std::{
    borrow::Cow,
    fmt,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use super::{Interest, IoSource, traits};

/// The runtime zbus brings along by default: a thin wrapper around [`zruntime::Runtime`].
#[derive(Clone, Debug)]
pub(crate) struct Builtin(zruntime::Runtime);

impl Builtin {
    /// A handle on the runtime for what this thread builds, brought into being here if none is
    /// alive.
    ///
    /// What can fail is the reactor: it opens the channel a wait is broken through.
    pub(crate) fn new() -> io::Result<Self> {
        Ok(Self(zruntime::Runtime::current()?))
    }

    /// Queues `future` under the diagnostic name `name` and hands back the task that joins or
    /// cancels it.
    ///
    /// An inherent method rather than part of [`traits::Runtime`], whose `spawn` is public API
    /// and takes `&str`: this one takes the `Cow` the crate-internal [`super::Runtime::spawn`]
    /// already built, rather than making every caller pay for a fresh copy of a name it already
    /// owns.
    pub(super) fn spawn_named<T>(
        &self,
        name: Cow<'static, str>,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T>
    where
        T: Send + 'static,
    {
        Task(self.0.spawn(name, future))
    }
}

impl traits::Runtime for Builtin {
    type RegisteredIoSource = Registration;
    type Sleep = zruntime::Sleep;
    type Task<T>
        = Task<T>
    where
        T: Send + 'static;

    fn register_io_source(&self, source: IoSource) -> io::Result<Registration> {
        self.0.register(source).map(Registration)
    }

    fn sleep(&self, duration: Duration) -> zruntime::Sleep {
        self.0.sleep(duration)
    }

    fn spawn<T>(&self, name: &str, future: impl Future<Output = T> + Send + 'static) -> Task<T>
    where
        T: Send + 'static,
    {
        // A copy of its own: this signature is fixed by `traits::Runtime`, whose caller only
        // hands out a borrow, while a task's cell keeps its name for as long as it lives.
        self.spawn_named(name.to_owned().into(), future)
    }
}

/// Runs `future` to completion on the calling thread, running that thread's runtime alongside it:
/// see [`zruntime::block_on`].
#[cfg(not(feature = "tokio"))]
pub(crate) fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    zruntime::block_on(future)
}

/// A registration on [zruntime]'s reactor.
///
/// [zruntime]: https://docs.rs/zruntime
#[derive(Debug)]
pub(crate) struct Registration(zruntime::Registration);

impl traits::PollIo for Registration {
    fn poll_io<T>(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        operation: impl FnMut() -> io::Result<T>,
    ) -> Poll<io::Result<T>> {
        self.0.poll_io(cx, interest.into(), operation)
    }
}

impl From<Interest> for zruntime::Interest {
    fn from(interest: Interest) -> Self {
        // `Interest` is `non_exhaustive` for zbus's own callers, not for this crate, so an
        // exhaustive match is fine here.
        match interest {
            Interest::Readable => zruntime::Interest::Readable,
            Interest::Writable => zruntime::Interest::Writable,
        }
    }
}

/// A task spawned on a built-in runtime, which cancels that task when dropped.
pub(crate) struct Task<T>(zruntime::Task<T>);

impl<T> fmt::Debug for Task<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Task").field(&self.0).finish()
    }
}

impl<T> Future for Task<T> {
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

impl<T> traits::TaskHandle<T> for Task<T>
where
    T: Send + 'static,
{
    fn detach(self) {
        self.0.detach();
    }
}

#[cfg(test)]
mod tests {
    use ntest::timeout;

    use super::*;
    use crate::runtime::traits::Runtime as _;

    #[test]
    #[timeout(15000)]
    fn a_spawned_task_hands_its_output_back() {
        let runtime = Builtin::new().expect("a built-in runtime");
        let task = runtime.spawn("an answer", async { 42 });

        assert_eq!(zruntime::block_on(task).unwrap(), 42);
    }
}
