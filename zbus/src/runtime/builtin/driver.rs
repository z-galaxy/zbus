//! Who runs a built-in runtime: the thread inside a `block_on`, or a helper thread where no such
//! thread is there to do it.
//!
//! The scheduler and the reactor are run by one thread at a time, the one in the driver's seat.
//! A thread that enters `block_on` takes the seat if it is free and keeps it until its future is
//! done: between two polls of that future it runs a batch of ready tasks and then waits on the
//! reactor, so a program that drives its connections through `block_on` runs them on its own
//! thread and starts none. A thread that finds the seat taken parks instead, to be polled again
//! when its future is woken or the seat is freed.
//!
//! Work that outlives every `block_on` — a connection polled from some other executor, or a
//! task left running once `block_on` has returned — is run by a helper thread, started where
//! that work is found with nobody in the seat, and gone once nothing is left to run, watch or
//! time.

use std::{
    cell::Cell,
    collections::HashMap,
    ptr::NonNull,
    sync::Arc,
    thread::{self, Thread, ThreadId},
    time::Duration,
};

// What only a `block_on` needs, which a Tokio build has none of.
#[cfg(any(test, not(feature = "tokio")))]
use std::{
    future::Future,
    pin::pin,
    sync::Mutex,
    task::{Context, Poll, Wake, Waker},
};

use super::{Inner, lock, reactor::Reactor};
use crate::log::error;

/// How many ready tasks run between two looks at the reactor, so a busy task queue cannot
/// starve a socket or a timer.
const BATCH: usize = 32;

/// The name the helper thread carries: twelve bytes, which fits the fifteen Linux keeps for one.
pub(super) const THREAD_NAME: &str = "zbus runtime";

thread_local! {
    /// The reactor of the runtime this thread is in the seat of, and nothing on a thread that
    /// is in no seat at all.
    static DRIVER_REACTOR: Cell<Option<NonNull<Reactor>>> = const { Cell::new(None) };

    /// How many `block_on` calls this thread is inside of, in the seat or waiting for it. A
    /// count rather than a flag, so that one call returning does not unmark the call it was
    /// made from.
    static IN_BLOCK_ON: Cell<u32> = const { Cell::new(0) };
}

/// Whether the calling thread is in the seat of the runtime `reactor` belongs to.
///
/// The reactor is named by its address, which stands for it alone while a thread is in its
/// seat: that thread holds the runtime, so the reactor cannot be dropped and its place taken by
/// another until the thread has left.
pub(super) fn on_driver_thread(reactor: &Reactor) -> bool {
    DRIVER_REACTOR.with(Cell::get) == Some(NonNull::from(reactor))
}

/// The runtime a `block_on` is to drive, looked up afresh at each turn of its loop, because the
/// future it polls may be the very thing that brings the runtime into being.
///
/// This and what follows it are what a `block_on` is made of, which a Tokio build has no use
/// for: there `zbus::block_on` is Tokio's own, and only a helper ever takes the seat.
#[cfg(any(test, not(feature = "tokio")))]
pub(super) type Resolve = Arc<dyn Fn() -> Option<Arc<Inner>> + Send + Sync>;

/// Runs `future` to completion on the calling thread, running the runtime `resolve` names
/// alongside it whenever the seat is free.
///
/// Panics when called from a thread that is in the seat already, which is a call from inside a
/// task the runtime is running: such a call could only wait for the thread it is on.
#[cfg(any(test, not(feature = "tokio")))]
pub(super) fn block_on<F>(resolve: Resolve, future: F) -> F::Output
where
    F: Future,
{
    let _inside = InBlockOn::enter();
    // The seat outlives the future, which [`drive`] drops as it returns: whatever that future
    // held — a task, a registration, a timer — is gone before the seat is given up, so only
    // work that outlives this call is handed on to a helper.
    let mut leaving = Leaving {
        resolve: &resolve,
        driving: None,
    };

    drive(&resolve, &mut leaving.driving, future)
}

/// What a `block_on` leaves as it goes: the seat it took, where it took one, and the work it was
/// running with nobody to run it.
///
/// A guard rather than a step after the poll loop, so that a panic out of the future is seen to
/// exactly as a return is. A future that panicked may have built a connection that outlives it,
/// and what drives that connection is decided here.
#[cfg(any(test, not(feature = "tokio")))]
struct Leaving<'a> {
    resolve: &'a Resolve,
    /// The seat this call took, where it took one, given up as this is dropped.
    driving: Option<Driving>,
}

#[cfg(any(test, not(feature = "tokio")))]
impl Drop for Leaving<'_> {
    /// Gives the seat up and hands on what the call leaves behind.
    ///
    /// A thread inside `block_on` asks for no helper, on the promise that it takes the seat on
    /// the next turn of its loop and runs the work itself. A thread that leaves without ever
    /// having taken the seat has no next turn to keep that promise on, so what its last poll
    /// handed over is handed on here; one that had the seat did as much where it gave it up.
    ///
    /// The thread is taken out of the list of those waiting for the seat either way: where it
    /// had the seat, freeing it empties that list; where it had not, it is taken out here.
    fn drop(&mut self) {
        if self.driving.take().is_some() {
            return;
        }
        let Some(inner) = (self.resolve)() else {
            return;
        };
        let mut seat = lock(&inner.seat);
        seat.stop_waiting();
        hand_over(&inner, &mut seat);
    }
}

/// Starts the helper unless a thread is in the seat or about to take it. Called after the work
/// it is to see is in place (a task queued, a source registered, a deadline stored), never
/// before.
///
/// That order is what makes the hand-off safe either way round: a helper that starts here finds
/// the work, and a thread in the seat either sees it in the round it is in or finds it where it
/// decides whether to leave, which it does under this very lock. A thread inside `block_on`
/// takes the seat on the next turn of its loop and finds the work then.
pub(super) fn ensure_helper(inner: &Arc<Inner>) {
    if IN_BLOCK_ON.with(Cell::get) > 0 {
        return;
    }
    let mut seat = lock(&inner.seat);
    if seat.helper || seat.holder == Holder::BlockOn {
        return;
    }
    spawn_helper(inner, &mut seat);
}

/// Who is running a runtime, and who is waiting to.
pub(super) struct Seat {
    holder: Holder,
    /// The threads parked in `block_on` for want of the seat, unparked whenever it is freed.
    ///
    /// Keyed by thread, so that a thread which enters `block_on` again and again while another
    /// keeps the seat has one place here rather than one per call. An entry goes where its
    /// thread takes the seat or leaves `block_on`, so that what is held here is the threads
    /// there may be something to unpark, and no more.
    waiting: HashMap<ThreadId, Thread>,
    /// Whether the helper thread is up. Set under the lock where the thread is started and
    /// cleared under it where the thread decides to leave.
    helper: bool,
}

impl Seat {
    /// A seat nobody is in.
    pub(super) fn new() -> Self {
        Self {
            holder: Holder::Nobody,
            waiting: HashMap::new(),
            helper: false,
        }
    }

    /// Whether the helper thread is up.
    pub(super) fn helper_running(&self) -> bool {
        self.helper
    }

    /// How many threads are down as waiting for the seat.
    #[cfg(test)]
    pub(super) fn waiting(&self) -> usize {
        self.waiting.len()
    }

    /// Takes the calling thread out of the list of those waiting for the seat.
    fn stop_waiting(&mut self) {
        self.waiting.remove(&thread::current().id());
    }

    /// Frees the seat, and unparks every thread waiting for it: to take it, or to find its
    /// future done.
    fn free(&mut self) {
        self.holder = Holder::Nobody;
        DRIVER_REACTOR.with(|reactor| reactor.set(None));
        for (_, thread) in self.waiting.drain() {
            thread.unpark();
        }
    }
}

/// Who is in the seat.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Holder {
    Nobody,
    /// A thread inside `block_on`.
    BlockOn,
    /// The helper thread.
    Helper,
}

/// Polls `future` to completion, running the runtime `resolve` names alongside it whenever the
/// seat is free, and leaving in `driving` the seat it took, where it took one.
///
/// The future is this call's own, and is dropped where it returns, before the caller gives the
/// seat up.
#[cfg(any(test, not(feature = "tokio")))]
fn drive<F>(resolve: &Resolve, driving: &mut Option<Driving>, future: F) -> F::Output
where
    F: Future,
{
    let mut future = pin!(future);
    let signal = Arc::new(Signal {
        thread: thread::current(),
        woken: Mutex::new(false),
        resolve: resolve.clone(),
    });
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let take_seat = || resolve().and_then(|inner| Driving::take(inner, Holder::BlockOn));
    let mut failed_waits = 0u32;
    loop {
        if driving.is_none() {
            *driving = take_seat();
        }
        // Cleared before the poll, so that a wake during it is seen by the round or the park
        // that follows.
        *lock(&signal.woken) = false;
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        // Looked for again, because the poll may have been what brought the runtime into being,
        // and a thread inside `block_on` asks for no helper: where nobody else is in the seat,
        // the work that poll left behind is this thread's to run.
        if driving.is_none() {
            *driving = take_seat();
        }
        match &*driving {
            Some(driving) => driving.round(&signal.woken, &mut failed_waits),
            None => {
                if !*lock(&signal.woken) {
                    thread::park();
                }
            }
        }
    }
}

/// Starts a helper for what is left to run, watch or time on `inner`, unless a thread is in the
/// seat or a helper is up already. Under the seat lock, which `seat` is the guard of.
///
/// Unlike [`ensure_helper`], this pays no heed to the caller being inside `block_on`: it is for
/// the two places where a thread that was running the work stops being able to.
fn hand_over(inner: &Arc<Inner>, seat: &mut Seat) {
    if seat.helper || seat.holder != Holder::Nobody || !inner.is_busy() {
        return;
    }
    spawn_helper(inner, seat);
}

/// Starts the helper thread. Under the seat lock, which `seat` is the guard of.
fn spawn_helper(inner: &Arc<Inner>, seat: &mut Seat) {
    let inner = inner.clone();
    thread::Builder::new()
        .name(THREAD_NAME.into())
        .spawn(move || helper(inner))
        .expect("the thread a built-in runtime's helper runs on");
    // Set once the thread is there: a spawn that fails panics with the flag clear, so that the
    // next call tries again rather than wait on a thread that was never started.
    seat.helper = true;
}

/// What the helper thread does: the seat, and rounds until nothing is left.
///
/// Each round polls what is ready, a batch at a time, and then asks whether anything is left to
/// run, watch or time. Where nothing is, the thread leaves there and then, rather than sit in a
/// wait until something comes along to tell it what it could have worked out for itself. Where
/// something is, the round ends in one wait on the reactor.
fn helper(inner: Arc<Inner>) {
    let Some(driving) = Driving::take(inner, Holder::Helper) else {
        return;
    };
    let mut failed_waits = 0u32;
    loop {
        driving.run_batch();
        if driving.leave_if_idle() {
            return;
        }
        driving.wait(false, &mut failed_waits);
    }
}

/// A thread's time in the seat: taken here, given up when this is dropped.
struct Driving {
    inner: Arc<Inner>,
    who: Holder,
    /// Whether the seat has been given up already, which [`Driving::leave_if_idle`] is the one
    /// thing that does before the drop.
    left: Cell<bool>,
}

impl Driving {
    /// Takes the seat as `who` if it is free.
    ///
    /// A `block_on` that finds it taken is put down as waiting, to be unparked when the seat is
    /// freed. A helper that finds it taken leaves, and takes its flag down under the very lock
    /// the seat's holder looks at that flag under when it leaves: whoever is in the seat asks
    /// for a helper again then, if anything is left by then.
    fn take(inner: Arc<Inner>, who: Holder) -> Option<Self> {
        let mut seat = lock(&inner.seat);
        if seat.holder != Holder::Nobody {
            if who == Holder::Helper {
                seat.helper = false;
            } else {
                let thread = thread::current();
                seat.waiting.insert(thread.id(), thread);
            }

            return None;
        }
        seat.holder = who;
        // A turn of this call's loop before this one may have found the seat taken and put the
        // thread down as waiting for it, which a thread in the seat is not.
        if who == Holder::BlockOn {
            seat.stop_waiting();
        }
        drop(seat);
        DRIVER_REACTOR.with(|reactor| reactor.set(Some(NonNull::from(&*inner.reactor))));

        Some(Self {
            inner,
            who,
            left: Cell::new(false),
        })
    }

    /// Polls up to `BATCH` ready tasks.
    fn run_batch(&self) {
        for _ in 0..BATCH {
            if !self.inner.scheduler.run_one() {
                break;
            }
        }
    }

    /// One round for `block_on`: a batch, then one wait on the reactor, which waits for nothing
    /// where the future this thread is polling has been woken in the meantime, so that the poll
    /// of it comes next and what a source has for the tasks is reported all the same.
    #[cfg(any(test, not(feature = "tokio")))]
    fn round(&self, woken: &Mutex<bool>, failed_waits: &mut u32) {
        self.run_batch();
        // Read and let go of before the wait, which the waker that sets it has to be free to
        // break.
        let woken = *lock(woken);

        self.wait(woken, failed_waits);
    }

    /// One wait on the reactor: bounded by no time at all where a task is ready or `at_once`
    /// says the caller has something of its own to get back to, so that the round after it polls
    /// what is ready, and by nothing where neither holds, so that the thread sleeps until a
    /// source, a deadline or a notification has something for it.
    ///
    /// `failed_waits` counts the failures in a row, for the pause that keeps a wait which fails
    /// every time from becoming a spin; every waiter retries its own operation and sees its own
    /// error.
    fn wait(&self, at_once: bool, failed_waits: &mut u32) {
        let at_most = (at_once || self.inner.scheduler.has_ready()).then_some(Duration::ZERO);
        match self.inner.reactor.wait(at_most) {
            Ok(()) => *failed_waits = 0,
            Err(e) => {
                *failed_waits += 1;
                if *failed_waits == 1 {
                    error!("The runtime's wait failed: {}", e);
                }
                self.inner.reactor.wake_everything();
                thread::sleep(Duration::from_millis(1 << (*failed_waits).min(10)));
            }
        }
    }

    /// Gives the seat up if nothing is left to run, watch or time; what the helper does before
    /// each wait. Under the seat lock, so that a spawn or a registration racing with it either
    /// is seen here or starts a helper itself once the lock is released.
    fn leave_if_idle(&self) -> bool {
        let mut seat = lock(&self.inner.seat);
        if self.inner.is_busy() {
            return false;
        }
        if self.who == Holder::Helper {
            seat.helper = false;
        }
        seat.free();
        self.left.set(true);

        true
    }
}

impl Drop for Driving {
    /// Gives the seat up, unless [`Driving::leave_if_idle`] did already, which is what the flag
    /// this reads says: the seat may be somebody else's by now, and what is asked here is
    /// whether it is still this thread's to give up, not who is in it.
    ///
    /// A `block_on` that leaves work behind starts a helper for it, because whatever is left
    /// may be awaited by a thread that is not going to drive; a helper that unwinds out of the
    /// loop clears its flag, so that the next spawn, registration or timer poll starts another.
    fn drop(&mut self) {
        if self.left.get() {
            return;
        }
        let mut seat = lock(&self.inner.seat);
        if self.who == Holder::Helper {
            seat.helper = false;
        }
        seat.free();
        if self.who == Holder::BlockOn {
            hand_over(&self.inner, &mut seat);
        }
    }
}

/// The waker of the future a `block_on` polls.
#[cfg(any(test, not(feature = "tokio")))]
struct Signal {
    thread: Thread,
    /// Whether the future has been woken since it was last polled.
    woken: Mutex<bool>,
    resolve: Resolve,
}

#[cfg(any(test, not(feature = "tokio")))]
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    /// Marks the future woken and rouses its thread, wherever that thread is: parked, or in the
    /// seat inside the reactor's wait, which is what the notification ends. A wake from the
    /// thread itself writes no notification, because that thread looks at the flag before its
    /// next wait.
    fn wake_by_ref(self: &Arc<Self>) {
        *lock(&self.woken) = true;
        self.thread.unpark();
        if let Some(inner) = (self.resolve)() {
            inner.reactor.notify();
        }
    }
}

/// What says a thread is inside `block_on`, for as long as it is.
#[cfg(any(test, not(feature = "tokio")))]
struct InBlockOn;

#[cfg(any(test, not(feature = "tokio")))]
impl InBlockOn {
    /// Marks the calling thread as inside `block_on`.
    ///
    /// Panics where the thread is in a seat already: the call comes from inside a task the
    /// runtime is running on this thread, and could only ever wait for itself.
    fn enter() -> Self {
        assert!(
            DRIVER_REACTOR.with(Cell::get).is_none(),
            "zbus::block_on called from a task zbus's runtime is running: the call would wait \
             for the thread it is on"
        );
        IN_BLOCK_ON.with(|inside| inside.set(inside.get() + 1));

        Self
    }
}

#[cfg(any(test, not(feature = "tokio")))]
impl Drop for InBlockOn {
    fn drop(&mut self) {
        IN_BLOCK_ON.with(|inside| inside.set(inside.get() - 1));
    }
}
