#[cfg(unix)]
pub(crate) const FDS_MAX: usize = 1024; // this is hardcoded in sdbus - nothing in the spec

pub(crate) fn padding_for_8_bytes(value: usize) -> usize {
    padding_for_n_bytes(value, 8)
}

pub(crate) fn padding_for_n_bytes(value: usize, align: usize) -> usize {
    let len_rounded_up = value.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);

    len_rounded_up.wrapping_sub(value)
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

/// Runs a future to completion on the calling thread, and zbus's runtime with it.
///
/// This is for a program that has no async runtime of its own. Between two polls of the future
/// the thread runs the tasks, sockets and timers of every connection built on zbus's built-in
/// runtime, so such a program is a single thread: zbus starts none for it. Where a call
/// returns with a connection still alive, a helper thread runs that connection's work until
/// the next call, or until the connection is gone. The future must not block the thread
/// waiting for work the runtime has to do — a synchronous wait for a reply, a busy loop
/// until a task has run — because that work runs on this very thread between its polls, so
/// such a wait never ends.
///
/// Do not call this from inside a task zbus is running — from a future polled inside another
/// call to it, or from a method of an interface served on such a connection: the call panics,
/// because it could only wait for the thread it is on. From a future some other runtime is
/// polling it holds that thread until it returns, and where the two end up waiting on each
/// other, neither of them ever does.
#[cfg(all(feature = "builtin-runtime", not(feature = "tokio")))]
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    crate::runtime::builtin::block_on(future)
}

/// Runs a future to completion on the calling thread.
///
/// It runs the future on the calling thread and drives no connection: in this build every
/// connection runs on the runtime handed to [`Builder::runtime`], and this call only waits for
/// that runtime's work.
///
/// Do not call this from an async context, that is, from inside a future another runtime is
/// polling. It holds the thread that future runs on until the call returns, and where the two
/// end up waiting on each other, neither of them ever does.
///
/// [`Builder::runtime`]: crate::connection::Builder::runtime
#[cfg(not(any(feature = "builtin-runtime", feature = "tokio")))]
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    futures_lite::future::block_on(future)
}

/// Runs a future to completion, holding the calling thread until it is done.
///
/// This is for a program that has no async runtime of its own: it turns one call into zbus's
/// async API into a blocking one. With the `tokio` feature the future is polled by a Tokio
/// runtime that zbus builds on the first such call and keeps for the rest of the process, so a
/// connection built inside that future finds a Tokio runtime current and runs on the Tokio
/// backend. The future handed here is the only one this polls — a connection's own work runs on
/// a thread of the connection's runtime.
///
/// Do not call this from an async context, that is, from inside a future another runtime is
/// polling. Tokio panics where its own runtime is the one polling, and any other blocks the
/// thread that future runs on until the call returns.
#[cfg(feature = "tokio")]
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::sync::OnceLock;

    static TOKIO_RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

    TOKIO_RT
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_io()
                .enable_time()
                .build()
                .expect("launch of single-threaded tokio runtime")
        })
        .block_on(future)
}
