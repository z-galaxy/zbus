//! The runtime zbus brings along, for a connection that is handed no runtime of its own.
//!
//! It is made of a scheduler, which holds the tasks and hands them out to be polled, a reactor,
//! which watches the sockets and keeps the timers, and a seat, which one thread at a time is
//! in to run the two. Everything [`traits::Runtime`] asks of a runtime reaches one of them: a
//! spawned future is queued on the scheduler, a registered source is the reactor's and so is a
//! timer, from the first poll of it onwards, and blocking work goes to a thread of its own, as
//! the trait's default has it.
//!
//! There is one runtime in a process, shared by every connection built without one of its own,
//! for as long as any of them or a thread running it is alive: one channel for breaking a wait
//! from another thread — a pipe on unix and a socket pair on Windows, two descriptors either
//! way — and no thread until there is work with nobody to run it.
//!
//! The thread in the seat is the one inside [`block_on`](crate::block_on), where a program has
//! one: it runs the tasks and waits on the reactor between two polls of its own future. Where
//! none has — the connection is polled from some other executor, or work is left once
//! `block_on` has returned — a helper thread takes the seat, and leaves in the round it finds
//! nothing left to run, watch or time.
//!
//! A panic in a task is caught and fails that task's handle. A panic outside a task — in a
//! waker, say — unwinds the thread in the seat: out of `block_on`, to whoever called it, with
//! the seat freed on the way; out of the helper's loop, with its flag cleared so that the next
//! spawn, registration or timer poll starts another. The wakers the reactor had taken out of
//! its maps to wake are dropped in the unwind without being woken, and the waiters they
//! belonged to are served by the next thread in the seat.
//!
//! Nothing here takes a runtime down while it has work: a detached task that never finishes
//! keeps a helper, and everything that task's future holds, for the life of the process.

mod driver;
mod poll;
mod reactor;
mod scheduler;

use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError, Weak},
    task::{Context, Poll},
    time::Duration,
};

use reactor::Reactor;
use scheduler::{JoinHandle, Scheduler};

use super::{IoSource, traits};

/// The runtime zbus brings along: a scheduler, a reactor, and a seat for whoever runs them.
#[derive(Clone)]
pub(crate) struct Builtin {
    inner: Arc<Inner>,
}

impl Builtin {
    /// A handle on `inner`, whatever registry it is or is not in.
    #[cfg(test)]
    pub(super) fn from_inner(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    /// What this handle is on.
    #[cfg(test)]
    pub(super) fn inner(&self) -> &Arc<Inner> {
        &self.inner
    }

    /// Whether the helper thread is running.
    #[cfg(test)]
    pub(super) fn helper_running(&self) -> bool {
        lock(&self.inner.seat).helper_running()
    }
}

impl fmt::Debug for Builtin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builtin")
            .field("helper", &lock(&self.inner.seat).helper_running())
            .finish_non_exhaustive()
    }
}

impl traits::Runtime for Builtin {
    type RegisteredIoSource = reactor::RegisteredIoSource;
    type Sleep = Sleep;
    type Task<T>
        = Task<T>
    where
        T: Send + 'static;

    fn register_io_source(&self, source: IoSource) -> io::Result<reactor::RegisteredIoSource> {
        let registered = self.inner.reactor.register(source)?;
        // Asked for once the source is in the reactor's map, so that a helper starting here
        // takes it into its very first wait.
        self.inner.ensure_progress();

        Ok(registered)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        Sleep {
            inner: self.inner.clone(),
            sleep: self.inner.reactor.sleep(duration),
        }
    }

    fn spawn<T>(&self, name: &str, future: impl Future<Output = T> + Send + 'static) -> Task<T>
    where
        T: Send + 'static,
    {
        let task = Task(self.inner.scheduler.spawn(name, future));
        // Asked for once the task is on the scheduler's queue, so that a helper starting here
        // finds it there.
        self.inner.ensure_progress();

        task
    }
}

/// A timer on a built-in runtime, which keeps a thread on it for as long as it has a deadline.
///
/// The reactor takes a timer's deadline on the first poll of it rather than where it is made, and
/// the thread that is to fire it has to be there from that poll onwards, however long ago the
/// timer was asked for. So it is the poll that asks for one, and a timer nobody ever polls costs
/// nothing at all.
pub(crate) struct Sleep {
    inner: Arc<Inner>,
    sleep: reactor::Sleep,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if Pin::new(&mut this.sleep).poll(cx).is_ready() {
            return Poll::Ready(());
        }
        // Asked for once the deadline is in the reactor's map, so that a helper starting here
        // waits on it.
        this.inner.ensure_progress();

        Poll::Pending
    }
}

/// A task spawned on a built-in runtime, which cancels that task when dropped.
pub(crate) struct Task<T>(JoinHandle<T>);

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

/// What a runtime is made of, shared by every handle on it and by the thread that runs it.
pub(super) struct Inner {
    scheduler: Arc<Scheduler>,
    reactor: Arc<Reactor>,
    /// Who runs this runtime; the lock the hand-over decisions are made under.
    seat: Mutex<driver::Seat>,
}

impl Inner {
    /// The runtime `registry` names, made here if none is alive.
    pub(super) fn shared_in(registry: &Mutex<Weak<Self>>) -> io::Result<Arc<Self>> {
        let mut shared = lock(registry);
        if let Some(inner) = shared.upgrade() {
            return Ok(inner);
        }
        let inner = Self::new()?;
        *shared = Arc::downgrade(&inner);

        Ok(inner)
    }

    /// A runtime with a scheduler and a reactor of its own, in no registry.
    pub(super) fn new() -> io::Result<Arc<Self>> {
        let reactor = Arc::new(Reactor::new()?);
        let scheduler = {
            let reactor = reactor.clone();

            // A task that becomes ready breaks the wait the seat's holder is in. Nothing in the
            // reactor points back at the scheduler, so this hook closes no cycle.
            Arc::new(Scheduler::new(move || reactor.notify()))
        };

        Ok(Arc::new(Self {
            scheduler,
            reactor,
            seat: Mutex::new(driver::Seat::new()),
        }))
    }

    /// Whether anything is left to run, watch or time.
    pub(super) fn is_busy(&self) -> bool {
        self.scheduler.has_ready() || self.scheduler.live_tasks() > 0 || !self.reactor.is_idle()
    }

    /// Sees to it that the work just handed over is run: a helper is started unless a thread is
    /// in the seat or about to take it. Called after that work is in place, never before.
    ///
    /// Whoever runs the work holds a runtime of its own while it does, so what has been handed
    /// over runs to completion even once every [`Builtin`] clone is gone.
    fn ensure_progress(self: &Arc<Self>) {
        driver::ensure_helper(self);
    }
}

/// The value behind a lock, taken whether or not a panic poisoned it.
pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests;
