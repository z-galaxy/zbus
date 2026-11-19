use std::{fmt, future::poll_fn, pin::Pin};

use futures_core::Stream;

#[cfg(unix)]
pub(crate) const FDS_MAX: usize = 1024; // this is hardcoded in sdbus - nothing in the spec

pub(crate) fn padding_for_8_bytes(value: usize) -> usize {
    padding_for_n_bytes(value, 8)
}

pub(crate) fn padding_for_n_bytes(value: usize, align: usize) -> usize {
    let len_rounded_up = value.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);

    len_rounded_up.wrapping_sub(value)
}

/// Formats a [`bitflags`] type declared with the `impl` form of `bitflags!` for its `Debug`
/// implementation.
///
/// The `impl` form leaves `Debug` to the type, so this writes what the `struct` form of
/// `bitflags!` generates: `Name(A | B)`, any bits that no flag defines as a trailing hex number
/// (`Name(A | 0x8)`), and `Name(0x0)` for an empty set.
pub(crate) fn fmt_bitflags<F>(name: &str, flags: &F, f: &mut fmt::Formatter<'_>) -> fmt::Result
where
    F: bitflags::Flags,
    F::Bits: bitflags::parser::WriteHex,
{
    write!(f, "{name}(")?;
    if flags.is_empty() {
        f.write_str("0x0")?;
    } else {
        bitflags::parser::to_writer(flags, &mut *f)?;
    }
    f.write_str(")")
}

/// The next item of `stream`, or `None` once it has ended.
pub(crate) async fn next<S>(stream: &mut S) -> Option<S::Item>
where
    S: Stream + Unpin,
{
    poll_fn(|cx| Pin::new(&mut *stream).poll_next(cx)).await
}

/// Helper trait for macro-generated code.
///
/// This trait allows macros to refer to the `Ok` and `Err` types of a [Result] that is behind a
/// type alias.  This is currently required because the macros for properties expect a Result
/// return value, but the macro-generated `receive_` functions need to refer to the actual
/// type without the associated error.
#[doc(hidden)]
pub trait ResultAdapter {
    type Ok;
    type Err;
}

impl<T, E> ResultAdapter for Result<T, E> {
    type Ok = T;
    type Err = E;
}

/// Runs a future to completion on the calling thread, and [zruntime](https://docs.rs/zruntime)
/// with it.
///
/// This is for a program that has no async runtime of its own. Put the async code in one
/// call to this function; the call blocks the thread until the future completes. In
/// between polls of the future, the calling thread also runs zruntime's scheduler and I/O
/// reactor for every connection built in it, and two threads that each call this drive
/// their own connections, in parallel. A handful of blocking system calls — a DNS lookup,
/// a nonce-file read, a peer-credential lookup — run on a short-lived worker thread of
/// their own instead, so they never hold up the calling thread. If the call returns while
/// a connection is still alive, a helper thread takes over that connection's work until
/// the next call, or until the connection is gone.
///
/// Because the connections' work runs on the calling thread in between polls, the future must
/// not block that thread waiting for it. A synchronous wait for a reply, or a busy loop until a
/// signal arrives, never finishes.
///
/// Do not call this from inside a task zbus is running, such as a method of a served interface
/// or a future inside another `block_on` call. It panics there, because it would be waiting for
/// the very thread it is on. Calling it on another thread and waiting for that thread from there
/// works here, as each thread runs a runtime of its own, but with the `tokio` feature, which any
/// crate in the program can turn on, every thread's call runs the same Tokio runtime, which the
/// waiting thread holds up, so the call may never return (see
/// [the FAQ](https://z-galaxy.github.io/zbus/faq.html#how-do-i-use-zbus-from-synchronous-code)).
/// Do not call it from another runtime's task either: it blocks that task's thread until the
/// future completes, which deadlocks the program if the future needs that thread to make progress.
#[cfg(all(feature = "default-rt", not(feature = "tokio")))]
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    crate::runtime::zruntime::block_on(future)
}

/// Runs a future to completion on the calling thread.
///
/// In a build with neither `default-rt` nor `tokio` enabled, every connection runs on the
/// runtime given to [`Builder::runtime`]. This call drives no connection itself: it only polls
/// the future passed in, blocking the calling thread until it is done.
///
/// Do not call this from another runtime's task. It blocks that task's thread until the future
/// completes, which deadlocks the program if the future needs that thread to make progress.
///
/// On WASI, a thread cannot park, so the call spins until the future is woken rather than sleep.
/// A WASI program has only the one thread, which is better spent running its runtime: drive a
/// connection there with that runtime's own `block_on` instead.
///
/// [`Builder::runtime`]: crate::connection::Builder::runtime
#[cfg(not(any(feature = "default-rt", feature = "tokio")))]
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::{
        pin::pin,
        sync::{Arc, atomic::Ordering},
        task::{Context, Poll, Waker},
        thread,
    };

    let unparker = Arc::new(Unparker {
        thread: thread::current(),
        woken: Default::default(),
    });
    let waker = Waker::from(unparker.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);

    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }

        // The flag, not the thread's park token, is what says the future was woken: the future
        // may park the thread itself while it is polled, which uses the token up, and anything
        // else may unpark the thread.
        while !unparker.woken.swap(false, Ordering::Acquire) {
            thread::park();
        }
    }
}

/// Runs a future to completion, blocking the calling thread until it is done.
///
/// This is for a program that has no async runtime of its own. Put the async code in one call to
/// this function; the call blocks the thread until the future completes. With the `tokio`
/// feature, the call runs inside a Tokio runtime that zbus creates on first use and keeps for
/// the rest of the process, so a connection built inside the future runs on Tokio as well.
///
/// That runtime is single-threaded: the tasks of the connections on it, such as reading incoming
/// messages and serving method calls, only run while a thread is inside this function, and on
/// one such thread at a time. With the `tokio-multithread` feature, it is a multi-threaded
/// runtime instead, whose worker threads keep running those tasks in parallel, whether or not
/// any thread is inside this function.
///
/// Do not call this from another runtime's task. From inside a Tokio async task it panics, as
/// Tokio's own `block_on` does. From any other runtime's task it blocks that task's thread until
/// the future completes, which deadlocks the program if the future needs that thread to make
/// progress.
///
/// Calling it from the future, or from a method of a connection built inside this function, panics
/// too, and calling it on another thread and waiting for that thread from there is no way around
/// that: the waiting thread holds up the runtime that every call shares, so the call may never
/// return. A synchronous function that runs its D-Bus calls in this function on a thread of its own
/// does that when it is called from there. Any crate in the program can turn the `tokio` feature
/// on, so a library that calls this function has to keep to that too: see
/// [the FAQ](https://z-galaxy.github.io/zbus/faq.html#how-do-i-use-zbus-from-synchronous-code).
#[cfg(feature = "tokio")]
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::sync::OnceLock;

    static TOKIO_RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

    TOKIO_RT
        .get_or_init(|| {
            #[cfg(feature = "tokio-multithread")]
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            #[cfg(not(feature = "tokio-multithread"))]
            let mut builder = tokio::runtime::Builder::new_current_thread();

            builder
                .enable_io()
                .enable_time()
                .build()
                .expect("launch of tokio runtime")
        })
        .block_on(future)
}

/// The waker of [`block_on`] in a build with no runtime of zbus's own: it unparks the thread the
/// call blocks.
#[cfg(not(any(feature = "default-rt", feature = "tokio")))]
struct Unparker {
    thread: std::thread::Thread,
    // Set by a wake-up, and cleared by the blocked thread as it polls the future again.
    woken: std::sync::atomic::AtomicBool,
}

#[cfg(not(any(feature = "default-rt", feature = "tokio")))]
impl std::task::Wake for Unparker {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        // A wake-up the thread has yet to take needs no second unpark.
        if !self.woken.swap(true, std::sync::atomic::Ordering::Release) {
            self.thread.unpark();
        }
    }
}

#[cfg(all(test, not(any(feature = "default-rt", feature = "tokio"))))]
mod tests {
    use std::{future::poll_fn, task::Poll, thread, time::Duration};

    use ntest::timeout;

    use super::block_on;

    /// A future woken from another thread while the calling thread waits for it is polled again.
    #[test]
    #[timeout(15000)]
    fn block_on_polls_again_once_another_thread_wakes_the_future() {
        let event = zruntime::Event::new();
        let listener = event.listen();
        let notifier = thread::spawn(move || {
            // Long enough for the calling thread to be parked by the time the wake-up comes.
            thread::sleep(Duration::from_millis(50));
            event.notify(1);
        });

        block_on(listener);
        notifier.join().unwrap();
    }

    /// A wake-up whose park token the future itself uses up is not lost.
    #[test]
    #[timeout(15000)]
    fn block_on_keeps_a_wake_up_the_future_parked_away() {
        let mut polled = false;

        block_on(poll_fn(|cx| {
            if polled {
                return Poll::Ready(());
            }
            polled = true;
            let waker = cx.waker().clone();
            thread::spawn(move || waker.wake()).join().unwrap();
            // The wake-up above unparked this thread, so this returns at once, taking the token.
            thread::park();

            Poll::Pending
        }));
    }
}
