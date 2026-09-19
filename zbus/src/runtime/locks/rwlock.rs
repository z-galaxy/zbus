//! A readers-writer lock that a future can hold across an await point.
//!
//! The object server keeps its tree of nodes and the interfaces hanging off it behind one of
//! these: every incoming method call reads the tree to find its target, and adding or removing an
//! object writes it. The guards outlive an await in both directions — an interface's method is a
//! future of its own, and it runs while the guard on that interface is alive — so
//! [`std::sync::RwLock`] cannot serve there: its guards are `!Send`, so a future holding one
//! cannot be spawned on an executor that moves work between threads, and blocking the thread on a
//! contended lock would stall every other future sharing it. Waiting for this lock suspends the
//! future instead and leaves the thread free.
//!
//! The lock is write-preferring: a writer that is waiting holds new readers back, so that a
//! stream of readers cannot starve it. A task already holding a read guard must therefore not ask
//! for a second one, since a writer arriving in between would deadlock it.

use std::{
    cell::UnsafeCell,
    fmt,
    ops::{Deref, DerefMut},
    sync::{self, PoisonError},
};

use event_listener::Event;

/// A readers-writer lock whose `read` and `write` futures wait without blocking the thread.
pub(crate) struct RwLock<T: ?Sized> {
    state: sync::Mutex<RwState>,
    readers_may_enter: Event,
    writer_may_enter: Event,
    value: UnsafeCell<T>,
}

// SAFETY: the lock owns the value outright, so handing the lock to another thread hands that
// thread the value and leaves nothing behind that can reach it.
unsafe impl<T> Send for RwLock<T> where T: ?Sized + Send {}
// SAFETY: the value is reachable through a guard alone, and the state admits either one writer or
// any number of readers. Sharing the lock therefore lets threads take turns with the value, which
// `Send` allows, and hold `&T` at the same time, which `Sync` allows.
unsafe impl<T> Sync for RwLock<T> where T: ?Sized + Send + Sync {}

impl<T> RwLock<T> {
    /// A new lock holding `value`, held by nobody.
    pub const fn new(value: T) -> Self {
        Self {
            state: sync::Mutex::new(RwState {
                readers: 0,
                writer: false,
                writers_waiting: 0,
            }),
            readers_may_enter: Event::new(),
            writer_may_enter: Event::new(),
            value: UnsafeCell::new(value),
        }
    }
}

impl<T> RwLock<T>
where
    T: ?Sized,
{
    /// Acquires shared access, waiting while a writer holds or waits for the lock.
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        loop {
            if let Some(guard) = self.try_read() {
                return guard;
            }
            // Listen before re-checking so a release between the check and the wait is seen.
            let listener = self.readers_may_enter.listen();
            if let Some(guard) = self.try_read() {
                return guard;
            }
            listener.await;
        }
    }

    /// Acquires exclusive access, waiting for every reader and writer to leave.
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        let waiting = WaitingWriter::register(self);
        loop {
            if let Some(guard) = self.try_write() {
                waiting.granted();
                return guard;
            }
            // Listen before re-checking so a release between the check and the wait is seen.
            let listener = self.writer_may_enter.listen();
            if let Some(guard) = self.try_write() {
                waiting.granted();
                return guard;
            }
            listener.await;
        }
    }

    fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let mut state = lock(&self.state);
        if state.writer || state.writers_waiting > 0 {
            return None;
        }
        // A count that wrapped would read as "no readers" and let a writer in beside the live
        // ones, so the overflow is caught here rather than left to the build's overflow checks.
        state.readers = state
            .readers
            .checked_add(1)
            .expect("more readers than a usize counts");

        Some(RwLockReadGuard(self))
    }

    fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        let mut state = lock(&self.state);
        if state.writer || state.readers > 0 {
            return None;
        }
        state.writer = true;

        Some(RwLockWriteGuard(self))
    }
}

impl<T> fmt::Debug for RwLock<T>
where
    T: ?Sized + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("RwLock");
        match self.try_read() {
            Some(guard) => s.field("value", &&*guard),
            None => s.field("value", &format_args!("<locked>")),
        };

        s.finish()
    }
}

#[must_use]
pub(crate) struct RwLockReadGuard<'a, T: ?Sized>(&'a RwLock<T>);

// SAFETY: a read guard yields `&T` and nothing else, so handing one to another thread hands that
// thread a `&T`, which is what `Sync` allows.
unsafe impl<T> Send for RwLockReadGuard<'_, T> where T: ?Sized + Sync {}
// SAFETY: sharing the guard only shares the `&T` it derefs to, which `Sync` allows.
unsafe impl<T> Sync for RwLockReadGuard<'_, T> where T: ?Sized + Sync {}

impl<T> Deref for RwLockReadGuard<'_, T>
where
    T: ?Sized,
{
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard exists, so the reader count is non-zero, which keeps every writer
        // out; the cell is reached through a guard alone, so only shared references to its
        // contents can be live.
        unsafe { &*self.0.value.get() }
    }
}

impl<T> Drop for RwLockReadGuard<'_, T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        let mut state = lock(&self.0.state);
        state.readers = state
            .readers
            .checked_sub(1)
            .expect("a reader released twice");
        if state.readers > 0 {
            return;
        }
        drop(state);

        self.0.writer_may_enter.notify(1);
    }
}

#[must_use]
pub(crate) struct RwLockWriteGuard<'a, T: ?Sized>(&'a RwLock<T>);

// SAFETY: the guard is the only path to the value while it exists, so handing it to another
// thread hands that thread the value, which is what `Send` allows.
unsafe impl<T> Send for RwLockWriteGuard<'_, T> where T: ?Sized + Send {}
// SAFETY: sharing the guard only shares the `&T` it derefs to, which `Sync` allows.
unsafe impl<T> Sync for RwLockWriteGuard<'_, T> where T: ?Sized + Sync {}

impl<T> Deref for RwLockWriteGuard<'_, T>
where
    T: ?Sized,
{
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard exists, so the writer flag is set and the reader count is zero; the
        // cell is reached through a guard alone, so nothing else can reach its contents.
        unsafe { &*self.0.value.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T>
where
    T: ?Sized,
{
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as in `deref`, this guard is the only path to the cell's contents, and `&mut
        // self` rules out a second reference taken through the guard itself.
        unsafe { &mut *self.0.value.get() }
    }
}

impl<T> Drop for RwLockWriteGuard<'_, T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        lock(&self.0.state).writer = false;
        // Who gets in next is settled by `try_read` and `try_write` on the state the waiters find
        // once they wake, so waking both sides can let nobody in early. A notification whose
        // listener is dropped before polling it is passed on to the next listener, so a `write`
        // future abandoned after being woken strands nobody behind it.
        self.0.readers_may_enter.notify(usize::MAX);
        self.0.writer_may_enter.notify(1);
    }
}

/// What every `read` and `write` decides on.
///
/// The blocking mutex around it is held for the few instructions that check and update it, never
/// across an await.
struct RwState {
    readers: usize,
    writer: bool,
    writers_waiting: usize,
}

/// Counts a `write` call as waiting for as long as its future lives, so that readers are held
/// back only while a writer really is waiting: a `write` dropped before it is granted stops
/// counting and lets the readers it was holding back in again.
struct WaitingWriter<'a, T: ?Sized> {
    rwlock: &'a RwLock<T>,
    counted: bool,
}

impl<'a, T> WaitingWriter<'a, T>
where
    T: ?Sized,
{
    fn register(rwlock: &'a RwLock<T>) -> Self {
        lock(&rwlock.state).writers_waiting += 1;

        Self {
            rwlock,
            counted: true,
        }
    }

    fn granted(mut self) {
        self.counted = false;
        lock(&self.rwlock.state).writers_waiting -= 1;
    }
}

impl<T> Drop for WaitingWriter<'_, T>
where
    T: ?Sized,
{
    fn drop(&mut self) {
        if !self.counted {
            return;
        }
        let mut state = lock(&self.rwlock.state);
        state.writers_waiting -= 1;
        if state.writers_waiting > 0 || state.writer {
            return;
        }
        drop(state);

        self.rwlock.readers_may_enter.notify(usize::MAX);
    }
}

/// The state behind the lock, taken whether or not a panic poisoned it.
///
/// Poisoning says nothing here: every path updates the state in a single assignment, so a panic
/// elsewhere cannot leave it half-updated.
fn lock(state: &sync::Mutex<RwState>) -> sync::MutexGuard<'_, RwState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}
