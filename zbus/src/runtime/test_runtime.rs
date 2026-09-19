//! A runtime implemented over the dev-dependencies, for the tests of the external path.
//!
//! It is a [`traits::Runtime`] over the smol crates, reached as dev-dependencies: those never
//! enter a user's graph, so this runtime exists in every test build, including the one with no
//! runtime of zbus's own compiled in. Its tasks run on one thread per instance, which lives as
//! long as the process, and its registrations and timers are async-io's: enough for a test, not
//! a model for a real host.

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
    time::Duration,
};

use async_executor::Executor;
use async_io::{Async, Timer};

use super::{Interest, IoSource, Runtime, blocking_thread, traits};

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

/// [`under_every_runtime`], minus any runtime that cannot watch a socket zbus owns.
///
/// Tokio on Windows reaches a socket through a type that owns it, so it has nowhere to keep a
/// descriptor of zbus's own and [`traits::Runtime::register_io_source`] reports `Unsupported`
/// there. So on Windows this sweep leaves Tokio out: a test that has zbus register a descriptor
/// of its own cannot run under it, while one that does not runs under Tokio there as anywhere.
pub(crate) fn under_every_watching_runtime<Body, Fut>(body: Body)
where
    Body: Fn(Runtime) -> Fut,
    Fut: Future<Output = ()>,
{
    under_the_polled_runtimes(&body);

    #[cfg(all(feature = "tokio", not(windows)))]
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
    #[cfg(feature = "async-io")]
    futures_lite::future::block_on(body(Runtime::Builtin(
        super::Builtin::new().expect("a runtime of zbus's own"),
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

        blocking_thread::run(work)
    }
}

/// A [`TestRuntime`] whose registrations wait for readiness before they try an operation.
///
/// [`TestRuntime`] polls the other way round, trying the operation first and waiting only once it
/// reports a would-block, and [`traits::PollIo`] allows either. Running the sweeps under both is
/// what holds an operation to the same answer whichever order it is asked in; the connect
/// predicate, which has to tell a connection still under way from one that has settled, is the
/// one that could tell the difference.
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
        Async::new(source).map(ReadinessFirstRegistration)
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
#[derive(Debug)]
pub(crate) struct ReadinessFirstRegistration(Async<IoSource>);

impl traits::PollIo for ReadinessFirstRegistration {
    fn poll_io<T>(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        mut operation: impl FnMut() -> io::Result<T>,
    ) -> Poll<io::Result<T>> {
        loop {
            // async-io re-arms the interest and returns `Pending` unless a readiness event newer
            // than the one it last reported is already there, so this loop cannot spin.
            let ready = match interest {
                Interest::Readable => self.0.poll_readable(cx),
                Interest::Writable => self.0.poll_writable(cx),
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
