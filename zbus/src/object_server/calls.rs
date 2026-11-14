//! The method calls that the object server of a connection takes, as a hand-over of the
//! connection needs to know of them.
//!
//! Only a hand-over needs to, so this is only built on Linux, with the `handover` feature: in any
//! other build, the object server takes the calls of its connection with nothing to count.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
};

use zruntime::{
    Event,
    broadcast::{Receiver, TryRecvError},
};

use crate::{Message, Result};

/// The method calls that the object server of a connection takes: how many it has under way, and,
/// once a hand-over of the connection has it take no more, those that it leaves for the object
/// server of the connection that takes over.
#[derive(Debug, Default)]
pub(crate) struct Calls {
    /// How many calls the object server took and is yet to be done with, in units of [`CALL`],
    /// and, below them, the flags [`AWAITED`] and `HELD`.
    ///
    /// They share a word so that taking a call and being done with it cost one atomic operation
    /// each, and no lock: the one that counts a call in tells whether the object server is held,
    /// and the one that counts it out tells whether anyone waits for it.
    under_way: AtomicUsize,
    /// Notified as the last call under way is done with, once anyone waited for that, and as the
    /// task that takes the calls ends.
    changed: Event,
    /// What a hand-over needs to know of the task that takes the calls.
    task: Mutex<Task>,
}

impl Calls {
    /// Wait until no call is under way: until the object server is done with each call that it
    /// took, which is right away if it has none in hand.
    pub(crate) async fn idle(&self) {
        while self.under_way.load(Ordering::Acquire) >= CALL {
            // The listener comes before the look that counts, so that a call done with right
            // after that look isn't missed.
            let listener = self.changed.listen();
            // That look also asks the last call under way to tell once it is done with, in the
            // one operation: either that call finds that it was asked to, or this finds it done
            // with.
            if self.under_way.fetch_or(AWAITED, Ordering::AcqRel) < CALL {
                return;
            }
            listener.await;
        }
    }

    /// Note that the task that takes the calls starts, before it is spawned. The task takes them
    /// off its stream through what this gives, and has ended once that is dropped, with the task
    /// or before it ever ran.
    pub(crate) fn start(self: &Arc<Self>) -> Taking {
        self.task().state = TaskState::Running;

        Taking {
            calls: self.clone(),
            stream: None,
        }
    }

    /// Take `call`, which the stream of the task that takes the calls yielded, for the object
    /// server to answer it: the call is under way until what this gives is dropped. It gives
    /// nothing once a hand-over has the object server take no more calls, and leaves `call` for
    /// the object server of the connection that takes over instead.
    pub(crate) fn take(self: &Arc<Self>, call: &Message) -> Option<UnderWay> {
        // The call counts unless the object server is held, in the one operation, so that a
        // hand-over that holds it and then finds it idle knows that it takes no call after.
        let taken = self
            .under_way
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |under_way| {
                (under_way & HELD == 0).then_some(under_way + CALL)
            });
        if taken.is_err() {
            self.task().left.push_back(call.clone());

            return None;
        }

        Some(UnderWay(self.clone()))
    }

    /// Have the object server take no more calls, for good, as the connection is to be handed
    /// over: it leaves each one that it gets from now on for the object server of the connection
    /// that takes over, and goes on with those that it has under way.
    pub(crate) fn hold(&self) {
        self.under_way.fetch_or(HELD, Ordering::AcqRel);
    }

    /// Wait for the task that takes the calls to have ended, as it does once the connection
    /// stopped reading and the task got all that was queued for it, so that it left all that it
    /// is to leave. A task that never started has nothing to end.
    pub(crate) async fn ended(&self) {
        while self.running() {
            // The listener comes before the look that counts, so that a task that ends right
            // after that look isn't missed.
            let listener = self.changed.listen();
            if !self.running() {
                return;
            }
            listener.await;
        }
    }

    /// Whether the object server ever took calls: whether its task started, so that it had every
    /// method call that the connection read since, but for those that it left.
    pub(crate) fn started(&self) -> bool {
        !matches!(self.task().state, TaskState::NotStarted)
    }

    /// Take the calls that the object server left, oldest first, as the connection is handed over
    /// with them.
    pub(crate) fn take_left(&self) -> VecDeque<Message> {
        std::mem::take(&mut self.task().left)
    }

    fn task(&self) -> MutexGuard<'_, Task> {
        // Nothing can panic while the lock is held.
        self.task.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether the task that takes the calls runs, or is to once it is polled.
    fn running(&self) -> bool {
        matches!(self.task().state, TaskState::Running)
    }

    /// Whether the object server is to take no more calls.
    fn held(&self) -> bool {
        self.under_way.load(Ordering::Acquire) & HELD != 0
    }

    /// Note that a call under way is done with, and tell those that wait for the last one.
    fn done(&self) {
        let before = self.under_way.fetch_sub(CALL, Ordering::AcqRel);
        // Only the last call under way tells, and only once someone waited for it.
        if before & AWAITED != 0 && before - CALL < CALL {
            self.changed.notify(usize::MAX);
        }
    }
}

/// The task of an object server that takes the method calls of its connection off `stream`, from
/// when it is spawned until it ends, which the drop of this tells, whether the task ends on its own
/// or is let go of.
#[derive(Debug)]
pub(crate) struct Taking {
    calls: Arc<Calls>,
    /// The stream of the method calls, once the task has it.
    pub(crate) stream: Option<Receiver<Result<Message>>>,
}

impl Drop for Taking {
    fn drop(&mut self) {
        let calls = &self.calls;
        {
            let mut task = calls.task();
            // A task that is let go of, rather than come to the end of its stream, leaves the
            // calls that were queued for it too.
            if calls.held()
                && let Some(stream) = &mut self.stream
            {
                loop {
                    match stream.try_recv() {
                        Ok(Ok(call)) => task.left.push_back(call),
                        // The error that the connection ended on is no call, and the task goes on
                        // from the oldest call still queued, as its stream would.
                        Ok(Err(_)) | Err(TryRecvError::Overflowed(_)) => (),
                        Err(TryRecvError::Empty | TryRecvError::Closed) => break,
                    }
                }
            }
            task.state = TaskState::Ended;
        }
        calls.changed.notify(usize::MAX);
    }
}

/// A method call that the object server took and is yet to be done with: to have answered, unless
/// it expects no reply. The call is under way until this is dropped, by the task that takes the
/// calls, or by the task that it hands the call on to.
#[derive(Debug)]
pub(crate) struct UnderWay(Arc<Calls>);

impl Drop for UnderWay {
    fn drop(&mut self) {
        self.0.done();
    }
}

/// The flag of [`Calls::under_way`] that tells that someone waited for no call to be under way,
/// so that the last call under way is to tell once it is done with. It stays set from the first
/// such wait on: an object server that nobody ever waited for notifies nobody.
const AWAITED: usize = 1;

/// The flag of [`Calls::under_way`] that tells that the object server is to take no more calls,
/// as the connection is to be handed over.
const HELD: usize = 1 << 1;

/// One call under way, in [`Calls::under_way`]: the count is in the bits above the flags.
const CALL: usize = 1 << 2;

/// The task that takes the calls of an object server, as a hand-over of its connection needs to
/// know of it.
#[derive(Debug, Default)]
struct Task {
    /// Whether the task runs.
    state: TaskState,
    /// The calls that the task left since a hand-over had the object server take no more, oldest
    /// first.
    left: VecDeque<Message>,
}

/// Whether the task that takes the calls of an object server runs.
#[derive(Debug, Default)]
enum TaskState {
    /// It was never spawned: the object server takes no calls.
    #[default]
    NotStarted,
    /// It takes the calls, or is to once it runs.
    Running,
    /// It ended, as the connection did or stopped reading, or it was let go of.
    Ended,
}

#[cfg(test)]
mod tests {
    use std::thread;

    use ntest::timeout;
    use test_log::test;

    use super::{Arc, CALL, Calls, Ordering};
    use crate::{Message, block_on};

    /// A wait for no call to be under way ends once the last call is done with, on another
    /// thread here, wherever the wait is by then: yet to look, looking, or waiting already.
    #[test]
    #[timeout(15000)]
    fn idle_hears_of_the_last_call_done_with_on_another_thread() {
        let call = Message::method_call("/org/zbus/Test", "Call")
            .build(&())
            .unwrap();
        for _ in 0..2000 {
            let calls = Arc::new(Calls::default());
            let _taking = calls.start();
            let under_way: Vec<_> = (0..3).map(|_| calls.take(&call)).collect();
            assert!(under_way.iter().all(Option::is_some));
            let done = thread::spawn(move || {
                for call in under_way {
                    // The call is done with as it is dropped here.
                    let _done = call;
                }
            });

            block_on(calls.idle());

            assert!(calls.under_way.load(Ordering::Acquire) < CALL);
            done.join().unwrap();
        }
    }
}
