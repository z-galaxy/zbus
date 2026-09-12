//! Integration with the async runtime that drives a connection.
//!
//! zbus does not ship a runtime of its own. A connection waits for socket readiness and timers,
//! and runs its internal tasks, on `async-io` (the default) or on Tokio, chosen when the
//! connection is built. This module holds what a connection exposes of that choice:
//! [`Executor`] wraps the executor zbus drives for the `async-io` backend (and stands empty
//! when Tokio runs the tasks itself), `Task` is its task handle, and [`AsyncDrop`] is the async
//! counterpart of [`Drop`] that zbus's own types implement.
//!
//! See [`crate::Connection::executor`] for driving a connection from your own runtime.

/// Evaluates the `tokio` or `async-io` expression for the active backend.
///
/// With a single backend it resolves to that backend's expression at compile time; with both it
/// picks at runtime via [`use_tokio`]. The inactive arm is `cfg`-stripped, so each arm only needs
/// to be valid in the configurations where its backend is compiled in.
///
/// The choice is re-evaluated on every call, so this is only safe where it doesn't need to match a
/// particular connection's backend. A connection latches its backend once at build time (see
/// [`Executor::new`]); use that instead of this macro for anything tied to the socket's reactor.
/// The current call sites (timers, the blocking pool) are independent of the socket, so a per-call
/// decision is fine.
macro_rules! select_runtime {
    (tokio: $tokio:expr, async_io: $async_io:expr $(,)?) => {{
        #[cfg(all(feature = "tokio", feature = "async-io"))]
        {
            if $crate::runtime::use_tokio() {
                $tokio
            } else {
                $async_io
            }
        }
        #[cfg(all(feature = "tokio", not(feature = "async-io")))]
        {
            $tokio
        }
        #[cfg(all(feature = "async-io", not(feature = "tokio")))]
        {
            $async_io
        }
    }};
}
pub(crate) use select_runtime;

/// Whether zbus should use tokio (rather than `async-io`) for its I/O.
///
/// Only consulted when `async-io` is compiled in, since that's the only time there's a choice. With
/// both backends we use tokio when a tokio runtime is active on the current thread; with only
/// `async-io` the answer is always `false`. This keeps the features additive: enabling `tokio`
/// elsewhere in the dependency graph doesn't force every zbus user into a tokio runtime.
#[cfg(feature = "async-io")]
pub(crate) fn use_tokio() -> bool {
    #[cfg(feature = "tokio")]
    {
        tokio::runtime::Handle::try_current().is_ok()
    }
    #[cfg(not(feature = "tokio"))]
    {
        false
    }
}

mod async_drop;
pub use async_drop::AsyncDrop;
mod executor;
pub use executor::{Executor, Task};
pub(crate) mod async_lock;
pub(crate) mod timeout;

// Only the `unixexec` and `ibus` transports and, on macOS, the `launchd` one run commands.
#[cfg(all(unix, any(feature = "unixexec", feature = "ibus", target_os = "macos")))]
pub(crate) mod process;

#[cfg(all(test, feature = "tokio", feature = "async-io"))]
mod tests {
    #[test]
    fn use_tokio_reflects_active_runtime() {
        assert!(!super::use_tokio(), "no runtime is active here");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert!(
            runtime.block_on(async { super::use_tokio() }),
            "a tokio runtime is active",
        );
    }
}
