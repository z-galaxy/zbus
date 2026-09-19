//! What a runtime supplies to a connection.
//!
//! By default, a connection runs on the runtime zbus brings along (the `builtin-runtime`
//! feature, on by default): one runtime per process, run by the thread inside
//! [`block_on`](crate::block_on) and by a helper thread only where no thread is inside it, with
//! no runtime dependency of its own. A connection built inside a Tokio runtime runs on it
//! instead (the `tokio` feature), and any other runtime reaches a
//! connection through whatever implements [`Runtime`] and is handed to [`Builder::runtime`].
//! Implementing the trait takes a readiness registration, a timer and a task handle, all of
//! which every async runtime already has, together with a hook for blocking work. Both built-in
//! backends are implementations of these traits, and the polling-based runtime in zbus's
//! integration tests is a worked example of the whole contract on a single thread.
//!
//! The async locks a connection holds are not part of the trait: they are zbus's own, built on
//! `event-listener`, so a build that runs on the built-in runtime needs no extra feature and
//! pulls in no lock crate of its own. On a Tokio build, Tokio's locks stand in instead, so that
//! build does not carry a second lock implementation.
//!
//! [`Builder::runtime`]: crate::connection::Builder::runtime

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use super::{Interest, IoSource, blocking_thread};

/// An async runtime, as seen by a connection.
pub trait Runtime: Send + Sync + 'static {
    /// How this runtime watches a registered [`IoSource`] for readiness.
    type RegisteredIoSource: PollIo;
    /// The timer [`Runtime::sleep`] hands back.
    type Sleep: Future<Output = ()> + Send + 'static;
    /// The handle to a task [`Runtime::spawn`] hands back.
    type Task<T>: TaskHandle<T>
    where
        T: Send + 'static;

    /// Register a socket or pipe for readiness notifications.
    ///
    /// The registration must stop watching the source before the [`IoSource`] it was given is
    /// released.
    ///
    /// Every read and write a connection makes on a socket of its own goes through the
    /// registration this returns. A socket handed over as a [`Socket`] implementation of its own
    /// drives itself instead and never reaches this method.
    ///
    /// [`Socket`]: crate::connection::Socket
    fn register_io_source(&self, source: IoSource) -> io::Result<Self::RegisteredIoSource>;

    /// A future that completes once `duration` has passed. Dropping it cancels the timer.
    ///
    /// A wait is asked for as a length rather than as a deadline because the clock it is timed
    /// on is the runtime's own. Tokio's, for one, can be paused and advanced by hand, and a
    /// deadline worked out from [`Instant::now`] on the standard clock would then be a wait of
    /// some altogether different length, or of none at all.
    ///
    /// [`Instant::now`]: std::time::Instant::now
    fn sleep(&self, duration: Duration) -> Self::Sleep;

    /// Run `future` to completion in the background.
    ///
    /// The task runs concurrently with the caller, on whichever thread the runtime chooses. The
    /// handle resolves to what `future` produced, or to `Err` where the runtime lost the task;
    /// only some runtimes can report that. Dropping the handle cancels the task;
    /// [`TaskHandle::detach`] lets it run on.
    ///
    /// `name` says what the task is there for — `"socket reader"`, say. It is for diagnostics
    /// only: nothing zbus does depends on it, and a runtime with nowhere to put it is free to
    /// drop it. The built-in Tokio backend hands it to `tokio::task::Builder`, which is where
    /// tokio-console and a task dump read a task's name from.
    fn spawn<T>(
        &self,
        name: &str,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Self::Task<T>
    where
        T: Send + 'static;

    /// Run `work` off the event loop.
    ///
    /// zbus asks for this for the operations it has no async form of:
    ///
    /// * the host-name lookup for a `tcp:` address,
    /// * reading the file a `nonce-tcp:` address names,
    /// * the peer-credential lookups that go to the name service or to the operating system's table
    ///   of connections,
    /// * reading the `autolaunch:` address on Windows, which takes a named mutex,
    /// * waiting for the helper process of a `unixexec:`, `ibus:` or `launchd:` address to exit.
    ///
    /// The default runs every call on a thread of its own, which exits with the work and whose
    /// failure to start panics, so a runtime that keeps a pool of threads for blocking work
    /// should hand the work to that pool instead. The wait for a helper process starts once the
    /// connection has let go of the pipe it reads that process's output from, and occupies a
    /// worker until the program is gone: no time at all for one that has already exited, and
    /// until its input ends for a `unixexec:` program that is still running — one that ignores
    /// the end of its input keeps that worker.
    ///
    /// Work that has been handed over has to run to completion whether or not the future this
    /// returns is polled, and whether or not that future is dropped. A thread of its own runs
    /// on regardless and so does Tokio's pool; a runtime that cancels a blocking task when its
    /// handle drops has to detach it here instead. The wait for a helper process is why the
    /// difference matters: that work owns the process, so a wait dropped along with its future
    /// is a process nothing will ever reap, which is what a cancelled connection attempt, or a
    /// connection let go of while one is under way, would leave behind.
    ///
    /// This and [`Runtime::spawn`] are called from whichever thread let go of the last thing
    /// that needed them, and that includes the drop of a task the runtime itself was handed: the
    /// pipe a connection reads a helper process's output from goes with the task that held it,
    /// and the wait for that process starts there. So neither may require a task context of its
    /// own, and a runtime that takes a lock to schedule has to tolerate being asked again while
    /// it drops a task.
    fn spawn_blocking<T>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Pin<Box<dyn Future<Output = T> + Send + 'static>>
    where
        T: Send + 'static,
    {
        blocking_thread::run(work)
    }
}

/// I/O on one registered [`IoSource`], polled as the runtime reports it ready.
pub trait PollIo: Send + Sync + 'static {
    /// Run `operation` on the polling thread while the source is ready for `interest`.
    ///
    /// Every readiness signal must run `operation` at least once. The first success it returns,
    /// a partial write included, and the first error other than `WouldBlock` are each what this
    /// call resolves to. A `WouldBlock` says the source was not ready after all: arrange for
    /// `cx`'s waker to be woken once it is, and return [`Poll::Pending`]. Whether readiness is
    /// looked at before or after the first attempt is the implementation's to choose.
    ///
    /// An implementation must never spin while the source is not ready, never block the thread
    /// it is polled on and never hold on to `operation` past the call, and it must stop watching
    /// the source before the [`IoSource`] it was registered with is released.
    fn poll_io<T>(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        operation: impl FnMut() -> io::Result<T>,
    ) -> Poll<io::Result<T>>;
}

/// A handle to a spawned task, which resolves to what that task produced.
///
/// The output travels back through the handle so that the contract is the one a caller who
/// wants it already has: the Tokio backend on Windows awaits the stream that a `tcp:` connect
/// spawned on its runtime produces, and any other value zbus comes to want from a task reaches it
/// the same way. The `Err` case is the runtime having lost the task, which only some runtimes can
/// report.
///
/// Dropping the handle cancels the task. A Tokio implementation wraps its `JoinHandle` in a
/// newtype that aborts on drop; an async-task style handle already behaves this way. A handle is
/// shared between threads with the connection that owns it, so it must be `Sync`.
pub trait TaskHandle<T>: Future<Output = io::Result<T>> + Send + Sync + Unpin + 'static {
    /// Let the task run to completion on its own.
    ///
    /// A detached task is the runtime's to keep: it runs until it ends, and a runtime that shuts
    /// down drops it, as Tokio and async-executor do. A task that is merely forgotten can never be
    /// reached again, and its future holds a clone of the connection that spawned it, so a runtime
    /// that forgot one would keep that connection's state for as long as the process runs. The
    /// object server detaches the task that runs each method call, which is what makes this matter.
    fn detach(self);
}
