//! What a runtime supplies to a connection.
//!
//! By default, a connection runs on [zruntime] (the `default-rt` feature, on by default):
//! one runtime per thread that runs [`block_on`](crate::block_on), driven by that thread, and by
//! a helper thread only for work left with no thread inside `block_on`. A connection built inside
//! a Tokio runtime runs on it instead (the `tokio` feature), and any other runtime reaches a
//! connection through whatever implements [`Runtime`] and is handed to [`Builder::runtime`].
//! Implementing the trait takes a readiness registration, a timer and a task handle, all of which
//! every async runtime already has, and the connects of TCP and unix-domain sockets and, on unix,
//! the spawn of a child process, which a runtime makes with what it has: a non-blocking connect
//! and a way to wait for a child to exit, or, for whichever of those it lacks, a thread for
//! blocking work. Only the hook for blocking work comes with a default, which a runtime with a
//! pool of threads for blocking work replaces. Both built-in backends are implementations of these
//! traits, and the polling-based runtime in zbus's integration tests is a worked example of the
//! whole contract on a single thread: it connects on its own poller, and waits for a child process
//! by looking at it at an interval on its timer.
//!
//! The async locks a connection holds are not part of the trait: they come from zruntime, so no
//! build needs a feature for them, whichever runtime it runs on, nor pulls in a lock crate of its
//! own. A Tokio build uses Tokio's locks instead.
//!
//! [`Builder::runtime`]: crate::connection::Builder::runtime
//! [zruntime]: https://docs.rs/zruntime

#[cfg(unix)]
use std::process::{Command, ExitStatus, Stdio};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use super::{Interest, IoSource};

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

    /// Connect a stream socket to the TCP endpoint at `address`.
    ///
    /// The future resolves to the connected socket in non-blocking mode, which the connection then
    /// registers through [`Runtime::register_io_source`] as it does every socket it does I/O on.
    /// So a runtime that watched the socket to wait for the connect must have stopped doing so by
    /// then: a socket handed back out of the type the runtime wrapped it in, such as through
    /// `into_std`, has. An [`IoSource`] is made from an owned descriptor with `From`: an
    /// `OwnedFd` on unix, an `OwnedSocket` on Windows.
    ///
    /// `address` is an IP address and a port; a host name is resolved before this is called. A
    /// connection that is refused, or that fails in any other way, is the future's `Err`.
    ///
    /// An implementation must not block the thread it is polled on. A runtime with a non-blocking
    /// connect of its own uses it. One without makes a blocking `connect(2)` on a thread for
    /// blocking work, such as one of those [`Runtime::spawn_blocking`] hands work to, and switches
    /// the socket to non-blocking mode once it is connected. That occupies the thread for as long
    /// as the kernel takes over the connection, which for a peer that does not answer is the
    /// system's own connect timeout, and dropping the future does not stop that connect, only
    /// discards the socket it makes.
    fn connect_tcp(&self, address: SocketAddr)
    -> impl Future<Output = io::Result<IoSource>> + Send;

    /// Connect a stream socket to the unix-domain socket at `path`.
    ///
    /// The future resolves to the connected socket in non-blocking mode, which the connection then
    /// registers through [`Runtime::register_io_source`] as it does every socket it does I/O on.
    /// So a runtime that watched the socket to wait for the connect must have stopped doing so by
    /// then: a socket handed back out of the type the runtime wrapped it in, such as through
    /// `into_std`, has. An [`IoSource`] is made from an owned descriptor with `From`: an
    /// `OwnedFd` on unix, an `OwnedSocket` on Windows.
    ///
    /// On Linux and Android, a `path` whose first byte is zero does not name a file. The rest of
    /// it is a name in the abstract socket namespace instead. A `path` that is too long to make a
    /// socket address of, a connection that is refused and any other failure to connect are the
    /// future's `Err`.
    ///
    /// An implementation must not block the thread it is polled on, and has the same two ways of
    /// connecting as [`Runtime::connect_tcp`]: a non-blocking connect of the runtime's own, or a
    /// blocking one on a thread for blocking work. On Linux and Android, a listener whose backlog
    /// is full turns a non-blocking connect away with a would-block error, where a blocking one
    /// waits in the kernel until the listener accepts a connection. So a non-blocking
    /// implementation waits a moment and tries again for as long as that lasts, rather than fail
    /// the connect, and a blocking one holds its thread for as long as the listener takes.
    fn connect_unix(&self, path: &Path) -> impl Future<Output = io::Result<IoSource>> + Send;

    /// Spawn `command` as a child process, and wait for it to exit.
    ///
    /// `stdin`, `stdout` and `stderr` are the standard streams of the process, and take the place
    /// of whatever `command` was given for them. A failure to spawn the process is the method's
    /// `Err`. Otherwise the process has been started by the time this returns, and the future
    /// resolves to its exit status once it has exited, or to the error of a wait that failed. An
    /// implementation must never kill the process, whatever becomes of the future.
    ///
    /// zbus runs a helper process this way: the program of a `unixexec:` address, which it speaks
    /// D-Bus to over a pipe on the process's standard input and another on its standard output,
    /// and the program that an `ibus:` address, and on macOS a `launchd:` one, runs to find out
    /// where the bus is, whose output it reads to its end and whose exit status it checks.
    ///
    /// An implementation must not keep `command` or the streams once the process is spawned. What
    /// zbus reads from a pipe to the process comes to its end only once every copy of the pipe's
    /// write end is closed, and a [`Command`] and a [`Stdio`] each hold a copy for as long as they
    /// live. So they are dropped before this returns, and not handed on to a task or a thread that
    /// outlives the spawn.
    ///
    /// zbus awaits the future of the program of a `unixexec:` address, which nothing else waits
    /// for, on a task of the runtime, so a runtime that keeps its tasks running collects every
    /// such process as it exits. The call that runs an `ibus:` or `launchd:` program awaits the
    /// future itself, and drops it unresolved if the read of the program's output fails or the
    /// call is given up on. A future that is dropped before it resolves leaves the process to the
    /// runtime, which may collect it once it exits, as zruntime does, collect it when it next gets
    /// round to it, as Tokio does, or leave it uncollected.
    ///
    /// A runtime that can wait for a child to exit without a thread, through a pidfd, a kqueue or
    /// a handler for `SIGCHLD`, does so. One that cannot waits for the child with a blocking
    /// `wait` on a thread for blocking work, which it holds for as long as the process runs: a
    /// `unixexec:` program runs until its input ends, which closing the connection does, so one
    /// that ignores the end of its input keeps the thread. Or it looks at the child at an interval
    /// on its timer, as the example runtime in zbus's integration tests does, which holds no thread
    /// but finds the exit only at the next look. The future outlives the borrow of `self`, so a
    /// wait it starts later goes through something it owns: a handle of the runtime's that it
    /// keeps, or a pool of threads it reaches without one, such as the pool behind zruntime's
    /// `unblock`.
    #[cfg(unix)]
    fn spawn_process(
        &self,
        command: Command,
        stdin: Stdio,
        stdout: Stdio,
        stderr: Stdio,
    ) -> io::Result<Pin<Box<dyn Future<Output = io::Result<ExitStatus>> + Send + 'static>>>;

    /// Run `work` off the event loop.
    ///
    /// zbus asks for this for the operations it has no async form of:
    ///
    /// * the host-name lookup for a `tcp:` address,
    /// * reading the file a `nonce-tcp:` address names,
    /// * the peer-credential lookups that go to the name service or to the operating system's table
    ///   of connections,
    /// * reading the `autolaunch:` address on Windows, which takes a named mutex.
    ///
    /// The default runs every call on a thread of zruntime's pool for blocking work, which starts
    /// a thread where none is free and whose failure to start one panics, so a runtime that keeps
    /// a pool of threads for blocking work should hand the work to that pool instead.
    ///
    /// Work that has been handed over has to run to completion whether or not the future this
    /// returns is polled, and whether or not that future is dropped, as it may own something that
    /// only running it to its end lets go of. A pool of zruntime's runs on regardless and so does
    /// Tokio's; a runtime that cancels a blocking task when its handle drops has to detach it
    /// here instead.
    ///
    /// Like [`Runtime::spawn`], this is called from whichever thread let go of the last thing that
    /// needed it, and that includes the drop of a task the runtime itself was handed. So it may
    /// not require a task context of its own, and a runtime that takes a lock to schedule has to
    /// tolerate being asked again while it drops a task.
    fn spawn_blocking<T>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Pin<Box<dyn Future<Output = T> + Send + 'static>>
    where
        T: Send + 'static,
    {
        Box::pin(zruntime::unblock(work))
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
/// The output travels back through the handle, so a value a task produces reaches whoever awaits
/// it. The `Err` case is the runtime having lost the task, which only some runtimes can report.
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
