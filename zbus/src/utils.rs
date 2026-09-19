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

/// The caller's future and, between two polls of it, the runtime zbus brings along: the tasks,
/// sockets and timers of every connection built on it run on this very thread.
#[cfg(all(feature = "builtin-runtime", not(feature = "tokio")))]
#[doc(hidden)]
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    crate::runtime::builtin::block_on(future)
}

/// With neither backend a connection's tasks run on its runtime, so the blocking facade only has
/// to poll the caller's future.
#[cfg(not(any(feature = "tokio", feature = "builtin-runtime")))]
#[doc(hidden)]
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    futures_lite::future::block_on(future)
}

#[cfg(feature = "tokio")]
#[doc(hidden)]
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
