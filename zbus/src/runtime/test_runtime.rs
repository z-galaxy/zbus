//! A runtime implemented over the dev-dependencies, for the tests of the external path.
//!
//! It is a [`traits::Runtime`] over the smol crates, reached as dev-dependencies: those never
//! enter a user's graph, so this runtime exists in every test build, including the one with no
//! runtime of zbus's own compiled in. Its tasks run on one thread per instance, which lives as
//! long as the process, and its registrations and timers are async-io's. It connects with a
//! blocking connect on its own hook for blocking work, which counts the calls, and waits for a
//! child process on a thread of zruntime's pool, starting the wait with the first poll of the
//! future of the exit status. A process whose future is dropped before then stays uncollected, as
//! the trait lets a runtime leave it, so a test on this runtime sees whether zbus awaits the
//! future itself. That is enough for a test, not a model for a real host.

#[cfg(unix)]
use std::process::{Command, ExitStatus, Stdio};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
    time::Duration,
};

use async_executor::Executor;
use async_io::{Async, ReadableOwned, Timer, WritableOwned};
use socket2::{Domain, SockAddr};

#[cfg(unix)]
use super::erased::BoxFuture;
use super::{Interest, IoSource, Runtime, io::connect_blocking, traits};

/// Runs `body` once under every runtime this build can make.
///
/// Everything but Tokio is driven by `futures_lite`; a Tokio connection is polled inside the
/// runtime it belongs to, which is where a Tokio user would poll it.
pub(crate) fn under_every_runtime<Body, Fut>(body: Body)
where
    Body: Fn(Runtime) -> Fut,
    Fut: Future<Output = ()>,
{
    under_the_polled_runtimes(&body);

    #[cfg(feature = "tokio")]
    under_tokio(&body);
}

/// Runs `body` under the runtimes a test drives with `futures_lite` from the calling thread.
///
/// The last two are the same runtime in the two orders [`traits::PollIo`] leaves an
/// implementation to choose between, so every test in the sweep is run under both.
fn under_the_polled_runtimes<Body, Fut>(body: &Body)
where
    Body: Fn(Runtime) -> Fut,
    Fut: Future<Output = ()>,
{
    #[cfg(feature = "default-rt")]
    futures_lite::future::block_on(body(Runtime::ZRuntime(
        super::ZRuntime::new().expect("a runtime of zbus's own"),
    )));

    futures_lite::future::block_on(body(Runtime::from_external(TestRuntime::new())));
    futures_lite::future::block_on(body(Runtime::from_external(ReadinessFirst::new())));
}

/// Runs `body` inside a Tokio runtime of its own, which is where a Tokio user would poll it.
#[cfg(feature = "tokio")]
fn under_tokio<Body, Fut>(body: &Body)
where
    Body: Fn(Runtime) -> Fut,
    Fut: Future<Output = ()>,
{
    let tokio = tokio::runtime::Runtime::new().unwrap();

    tokio.block_on(async {
        let runtime = super::Tokio::current().expect("a Tokio runtime is current");

        body(Runtime::Tokio(runtime)).await
    });
}

#[derive(Clone, Debug)]
pub(crate) struct TestRuntime {
    executor: Arc<Executor<'static>>,
    // Shared with every clone, so a test that hands one to a connection still sees what the
    // connection asked of it.
    blocking_calls: Arc<Mutex<usize>>,
}

impl TestRuntime {
    /// A runtime with an executor and a thread of its own.
    pub(crate) fn new() -> Self {
        let executor = Arc::new(Executor::new());

        let runner = executor.clone();
        std::thread::Builder::new()
            .name("zbus test runtime".into())
            .spawn(move || futures_lite::future::block_on(runner.run(std::future::pending::<()>())))
            .expect("failed to spawn the test runtime thread");

        Self {
            executor,
            blocking_calls: Arc::default(),
        }
    }

    /// How many pieces of blocking work have been handed to this runtime.
    pub(crate) fn blocking_calls(&self) -> usize {
        *self
            .blocking_calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether every task spawned on this runtime is finished or cancelled.
    pub(crate) fn is_empty(&self) -> bool {
        self.executor.is_empty()
    }
}

impl traits::Runtime for TestRuntime {
    type RegisteredIoSource = Registration;
    type Sleep = Sleep;
    type Task<T>
        = Task<T>
    where
        T: Send + 'static;

    fn register_io_source(&self, source: IoSource) -> io::Result<Registration> {
        Async::new(source).map(Registration)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        Sleep(Timer::after(duration))
    }

    fn spawn<T>(&self, _name: &str, future: impl Future<Output = T> + Send + 'static) -> Task<T>
    where
        T: Send + 'static,
    {
        Task(self.executor.spawn(future))
    }

    fn connect_tcp(
        &self,
        address: SocketAddr,
    ) -> impl Future<Output = io::Result<IoSource>> + Send {
        traits::Runtime::spawn_blocking(self, move || {
            connect_blocking(Domain::for_address(address), &address.into())
        })
    }

    async fn connect_unix(&self, path: &Path) -> io::Result<IoSource> {
        let address = SockAddr::unix(path)?;

        traits::Runtime::spawn_blocking(self, move || connect_blocking(Domain::UNIX, &address))
            .await
    }

    #[cfg(unix)]
    fn spawn_process(
        &self,
        mut command: Command,
        stdin: Stdio,
        stdout: Stdio,
        stderr: Stdio,
    ) -> io::Result<BoxFuture<'static, io::Result<ExitStatus>>> {
        let child = command.stdin(stdin).stdout(stdout).stderr(stderr).spawn();
        // The command holds the streams it was given for as long as it lives, which is longer
        // than the process needs them: a copy of the write end of a pipe that stays open here is
        // a pipe that never ends for whoever reads it.
        drop(command);
        let mut child = child?;

        // The first poll looks at the process, which collects it right there if it has exited
        // already. A process that is still running is waited for with a blocking `wait` on a
        // thread of the pool that `zruntime::unblock` runs its work on, and the pool thread wakes
        // the task that polled last once the process is gone. It is zruntime's pool and not the
        // runtime's own blocking work, as `TestRuntime::blocking_calls` counts the blocking work
        // that zbus hands the runtime, and the runtime starts this wait of its own accord. A wait
        // that is under way on the pool runs to its end whether or not the future is dropped.
        Ok(Box::pin(async move {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }

            zruntime::unblock(move || child.wait()).await
        }))
    }

    fn spawn_blocking<T>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Pin<Box<dyn Future<Output = T> + Send + 'static>>
    where
        T: Send + 'static,
    {
        *self
            .blocking_calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner) += 1;

        Box::pin(zruntime::unblock(work))
    }
}

/// A [`TestRuntime`] whose registrations wait for readiness before they try an operation.
///
/// [`TestRuntime`] polls the other way round, trying the operation first and waiting only once it
/// reports a would-block, and [`traits::PollIo`] allows either. Running the sweeps under both is
/// what holds an operation to the same answer whichever order it is asked in.
#[derive(Clone, Debug)]
pub(crate) struct ReadinessFirst(TestRuntime);

impl ReadinessFirst {
    pub(crate) fn new() -> Self {
        Self(TestRuntime::new())
    }
}

impl traits::Runtime for ReadinessFirst {
    type RegisteredIoSource = ReadinessFirstRegistration;
    type Sleep = Sleep;
    type Task<T>
        = Task<T>
    where
        T: Send + 'static;

    fn register_io_source(&self, source: IoSource) -> io::Result<ReadinessFirstRegistration> {
        Ok(ReadinessFirstRegistration {
            source: Arc::new(Async::new(source)?),
            readable: Mutex::new(None),
            writable: Mutex::new(None),
        })
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        traits::Runtime::sleep(&self.0, duration)
    }

    fn spawn<T>(&self, name: &str, future: impl Future<Output = T> + Send + 'static) -> Task<T>
    where
        T: Send + 'static,
    {
        traits::Runtime::spawn(&self.0, name, future)
    }

    fn connect_tcp(
        &self,
        address: SocketAddr,
    ) -> impl Future<Output = io::Result<IoSource>> + Send {
        traits::Runtime::connect_tcp(&self.0, address)
    }

    fn connect_unix(&self, path: &Path) -> impl Future<Output = io::Result<IoSource>> + Send {
        traits::Runtime::connect_unix(&self.0, path)
    }

    #[cfg(unix)]
    fn spawn_process(
        &self,
        command: Command,
        stdin: Stdio,
        stdout: Stdio,
        stderr: Stdio,
    ) -> io::Result<BoxFuture<'static, io::Result<ExitStatus>>> {
        traits::Runtime::spawn_process(&self.0, command, stdin, stdout, stderr)
    }

    fn spawn_blocking<T>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Pin<Box<dyn Future<Output = T> + Send + 'static>>
    where
        T: Send + 'static,
    {
        traits::Runtime::spawn_blocking(&self.0, work)
    }
}

/// A [`TestRuntime`] that keeps [`traits::Runtime::spawn_blocking`]'s default.
///
/// Everything else it hands out is the test runtime's, so a test of the default hook has a
/// runtime to reach it through.
#[derive(Clone, Debug)]
pub(crate) struct DefaultBlocking(TestRuntime);

impl DefaultBlocking {
    pub(crate) fn new() -> Self {
        Self(TestRuntime::new())
    }
}

impl traits::Runtime for DefaultBlocking {
    type RegisteredIoSource = Registration;
    type Sleep = Sleep;
    type Task<T>
        = Task<T>
    where
        T: Send + 'static;

    fn register_io_source(&self, source: IoSource) -> io::Result<Registration> {
        traits::Runtime::register_io_source(&self.0, source)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        traits::Runtime::sleep(&self.0, duration)
    }

    fn spawn<T>(&self, name: &str, future: impl Future<Output = T> + Send + 'static) -> Task<T>
    where
        T: Send + 'static,
    {
        traits::Runtime::spawn(&self.0, name, future)
    }

    fn connect_tcp(
        &self,
        address: SocketAddr,
    ) -> impl Future<Output = io::Result<IoSource>> + Send {
        traits::Runtime::connect_tcp(&self.0, address)
    }

    fn connect_unix(&self, path: &Path) -> impl Future<Output = io::Result<IoSource>> + Send {
        traits::Runtime::connect_unix(&self.0, path)
    }

    #[cfg(unix)]
    fn spawn_process(
        &self,
        command: Command,
        stdin: Stdio,
        stdout: Stdio,
        stderr: Stdio,
    ) -> io::Result<BoxFuture<'static, io::Result<ExitStatus>>> {
        traits::Runtime::spawn_process(&self.0, command, stdin, stdout, stderr)
    }
}

/// A [`TestRuntime`] with connects of its own, which never connect to anything.
///
/// Each reports where it was asked to connect as its error, so that a test sees which connect ran
/// and what it was handed: this one, and not a connect made some other way, which would have gone
/// to the address and found nothing there.
#[derive(Clone, Debug)]
pub(crate) struct OwnConnects(TestRuntime);

impl OwnConnects {
    pub(crate) fn new() -> Self {
        Self(TestRuntime::new())
    }
}

impl traits::Runtime for OwnConnects {
    type RegisteredIoSource = Registration;
    type Sleep = Sleep;
    type Task<T>
        = Task<T>
    where
        T: Send + 'static;

    fn register_io_source(&self, source: IoSource) -> io::Result<Registration> {
        traits::Runtime::register_io_source(&self.0, source)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        traits::Runtime::sleep(&self.0, duration)
    }

    fn spawn<T>(&self, name: &str, future: impl Future<Output = T> + Send + 'static) -> Task<T>
    where
        T: Send + 'static,
    {
        traits::Runtime::spawn(&self.0, name, future)
    }

    fn connect_tcp(
        &self,
        address: SocketAddr,
    ) -> impl Future<Output = io::Result<IoSource>> + Send {
        std::future::ready(Err(io::Error::other(format!(
            "a connect of its own to {address}"
        ))))
    }

    fn connect_unix(&self, path: &Path) -> impl Future<Output = io::Result<IoSource>> + Send {
        std::future::ready(Err(io::Error::other(format!(
            "a connect of its own to {}",
            path.display(),
        ))))
    }

    #[cfg(unix)]
    fn spawn_process(
        &self,
        command: Command,
        stdin: Stdio,
        stdout: Stdio,
        stderr: Stdio,
    ) -> io::Result<BoxFuture<'static, io::Result<ExitStatus>>> {
        traits::Runtime::spawn_process(&self.0, command, stdin, stdout, stderr)
    }
}

/// A [`TestRuntime`] with a spawn of processes of its own, which never starts a process.
///
/// It reports the program it was asked to run as its error, so that a test sees which spawn ran
/// and what it was handed: this one, and not a spawn made some other way, which would have run
/// the program.
#[cfg(all(unix, any(feature = "unixexec", feature = "ibus", target_os = "macos")))]
#[derive(Clone, Debug)]
pub(crate) struct OwnSpawn(TestRuntime);

#[cfg(all(unix, any(feature = "unixexec", feature = "ibus", target_os = "macos")))]
impl OwnSpawn {
    pub(crate) fn new() -> Self {
        Self(TestRuntime::new())
    }
}

#[cfg(all(unix, any(feature = "unixexec", feature = "ibus", target_os = "macos")))]
impl traits::Runtime for OwnSpawn {
    type RegisteredIoSource = Registration;
    type Sleep = Sleep;
    type Task<T>
        = Task<T>
    where
        T: Send + 'static;

    fn register_io_source(&self, source: IoSource) -> io::Result<Registration> {
        traits::Runtime::register_io_source(&self.0, source)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        traits::Runtime::sleep(&self.0, duration)
    }

    fn spawn<T>(&self, name: &str, future: impl Future<Output = T> + Send + 'static) -> Task<T>
    where
        T: Send + 'static,
    {
        traits::Runtime::spawn(&self.0, name, future)
    }

    fn connect_tcp(
        &self,
        address: SocketAddr,
    ) -> impl Future<Output = io::Result<IoSource>> + Send {
        traits::Runtime::connect_tcp(&self.0, address)
    }

    fn connect_unix(&self, path: &Path) -> impl Future<Output = io::Result<IoSource>> + Send {
        traits::Runtime::connect_unix(&self.0, path)
    }

    fn spawn_process(
        &self,
        command: Command,
        _stdin: Stdio,
        _stdout: Stdio,
        _stderr: Stdio,
    ) -> io::Result<BoxFuture<'static, io::Result<ExitStatus>>> {
        Err(io::Error::other(format!(
            "a spawn of its own for {}",
            command.get_program().to_string_lossy(),
        )))
    }
}

/// A [`TestRuntime`] whose tasks a test can end from the outside.
///
/// Only the p2p tests take a connection apart this way, so it is only built where they are.
///
/// The task trait lets a runtime drop or abort what it was handed, and this stands in for one
/// that does: [`AbortingRuntime::abort_tasks`] ends every task spawned on it so far while the
/// connection that asked for them carries on, which is the situation a connection has to survive.
///
/// The handles pile up: an `AbortHandle` says nothing about whether its task has finished, so
/// there is no pruning one that has. A test aborts once and then drops the runtime, so the list
/// never outgrows the tasks of a single connection.
#[cfg(feature = "p2p")]
#[derive(Clone, Debug)]
pub(crate) struct AbortingRuntime {
    inner: TestRuntime,
    // Shared with every clone, so a test that hands one to a connection can still reach the
    // tasks that connection spawned.
    handles: Arc<Mutex<Vec<futures_util::future::AbortHandle>>>,
}

#[cfg(feature = "p2p")]
impl AbortingRuntime {
    pub(crate) fn new() -> Self {
        Self {
            inner: TestRuntime::new(),
            handles: Arc::default(),
        }
    }

    /// Ends every task spawned on this runtime so far.
    pub(crate) fn abort_tasks(&self) {
        let handles =
            std::mem::take(&mut *self.handles.lock().unwrap_or_else(PoisonError::into_inner));

        for handle in handles {
            handle.abort();
        }
    }
}

#[cfg(feature = "p2p")]
impl traits::Runtime for AbortingRuntime {
    type RegisteredIoSource = Registration;
    type Sleep = Sleep;
    type Task<T>
        = AbortableTask<T>
    where
        T: Send + 'static;

    fn register_io_source(&self, source: IoSource) -> io::Result<Registration> {
        traits::Runtime::register_io_source(&self.inner, source)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        traits::Runtime::sleep(&self.inner, duration)
    }

    fn spawn<T>(
        &self,
        name: &str,
        future: impl Future<Output = T> + Send + 'static,
    ) -> AbortableTask<T>
    where
        T: Send + 'static,
    {
        let (future, handle) = futures_util::future::abortable(future);
        self.handles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(handle);

        // An aborted task leaves its future unfinished, which is exactly what is under test.
        AbortableTask(traits::Runtime::spawn(&self.inner, name, future))
    }

    fn connect_tcp(
        &self,
        address: SocketAddr,
    ) -> impl Future<Output = io::Result<IoSource>> + Send {
        traits::Runtime::connect_tcp(&self.inner, address)
    }

    fn connect_unix(&self, path: &Path) -> impl Future<Output = io::Result<IoSource>> + Send {
        traits::Runtime::connect_unix(&self.inner, path)
    }

    #[cfg(unix)]
    fn spawn_process(
        &self,
        command: Command,
        stdin: Stdio,
        stdout: Stdio,
        stderr: Stdio,
    ) -> io::Result<BoxFuture<'static, io::Result<ExitStatus>>> {
        traits::Runtime::spawn_process(&self.inner, command, stdin, stdout, stderr)
    }
}

/// A registration on async-io's reactor.
#[derive(Debug)]
pub(crate) struct Registration(Async<IoSource>);

impl traits::PollIo for Registration {
    fn poll_io<T>(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        mut operation: impl FnMut() -> io::Result<T>,
    ) -> Poll<io::Result<T>> {
        loop {
            match operation() {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result),
            }
            let ready = match interest {
                Interest::Readable => self.0.poll_readable(cx),
                Interest::Writable => self.0.poll_writable(cx),
            };
            if let Err(e) = std::task::ready!(ready) {
                return Poll::Ready(Err(e));
            }
        }
    }
}

/// A registration on async-io's reactor that waits for readiness first.
///
/// It waits through a future of async-io's for each direction, which it keeps from one poll to the
/// next until it resolves. async-io takes a readiness event that comes in the tick of the reactor
/// that a wait was registered in for one from before the wait, and ignores it. A future remembers
/// the tick of its first poll, so the next event resolves it. `Async::poll_writable` and
/// `Async::poll_readable` take the tick of each poll that registers again instead. On a source
/// that is ready already, such as a socket with room to write, the event of each new registration
/// can then come in that same tick, poll after poll, holding the operation up for seconds.
#[derive(Debug)]
pub(crate) struct ReadinessFirstRegistration {
    source: Arc<Async<IoSource>>,
    readable: Mutex<Option<ReadableOwned<IoSource>>>,
    writable: Mutex<Option<WritableOwned<IoSource>>>,
}

impl traits::PollIo for ReadinessFirstRegistration {
    fn poll_io<T>(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        mut operation: impl FnMut() -> io::Result<T>,
    ) -> Poll<io::Result<T>> {
        loop {
            // A future polled for the first time returns `Pending`, as it registers its wait, so
            // this loop cannot spin.
            let ready = match interest {
                Interest::Readable => {
                    poll_ready(&self.readable, cx, || self.source.clone().readable_owned())
                }
                Interest::Writable => {
                    poll_ready(&self.writable, cx, || self.source.clone().writable_owned())
                }
            };
            if let Err(e) = std::task::ready!(ready) {
                return Poll::Ready(Err(e));
            }
            match operation() {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                result => return Poll::Ready(result),
            }
        }
    }
}

/// Polls the future that `slot` keeps, or a new one that `wait` makes, and lets it go once it
/// resolves.
fn poll_ready<F>(
    slot: &Mutex<Option<F>>,
    cx: &mut Context<'_>,
    wait: impl FnOnce() -> F,
) -> Poll<F::Output>
where
    F: Future + Unpin,
{
    let mut slot = slot.lock().unwrap_or_else(PoisonError::into_inner);
    let ready = std::task::ready!(Pin::new(slot.get_or_insert_with(wait)).poll(cx));
    *slot = None;

    Poll::Ready(ready)
}

/// A timer with the completion instant dropped.
#[derive(Debug)]
pub(crate) struct Sleep(Timer);

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Pin::new(&mut self.0).poll(cx).map(drop)
    }
}

/// An executor task: dropping it cancels the task.
#[derive(Debug)]
pub(crate) struct Task<T>(async_task::Task<T>);

impl<T> traits::TaskHandle<T> for Task<T>
where
    T: Send + 'static,
{
    fn detach(self) {
        self.0.detach();
    }
}

impl<T> Future for Task<T> {
    type Output = io::Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(Ok)
    }
}

/// A task of an [`AbortingRuntime`], whose abort reaches the caller as a lost task.
#[cfg(feature = "p2p")]
pub(crate) struct AbortableTask<T>(Task<Result<T, futures_util::future::Aborted>>);

#[cfg(feature = "p2p")]
impl<T> Future for AbortableTask<T> {
    type Output = io::Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|outcome| match outcome {
            Ok(Ok(value)) => Ok(value),
            // An aborted task produced nothing, which is what the trait's `Err` is for: a
            // runtime that lost the task.
            Ok(Err(_)) => Err(io::Error::other("the runtime aborted the task")),
            Err(e) => Err(e),
        })
    }
}

#[cfg(feature = "p2p")]
impl<T> traits::TaskHandle<T> for AbortableTask<T>
where
    T: Send + 'static,
{
    fn detach(self) {
        traits::TaskHandle::detach(self.0)
    }
}
