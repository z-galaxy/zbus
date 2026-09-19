//! Integration with the async runtime that drives a connection.
//!
//! zbus does not ship a runtime of its own. A connection watches its socket, takes its timers,
//! runs its internal tasks and hands off its blocking work on `async-io` (the default), on Tokio,
//! or on any implementation of [`traits::Runtime`] handed to [`Builder::runtime`]. Both built-in
//! backends are implementations of that trait, picked by the `async-io` and `tokio` features. The
//! async locks a connection holds are not part of that trait: they are zbus's own, built on
//! `event-listener`, except on a Tokio build, where Tokio's locks stand in so that build does not
//! carry a second lock implementation.
//! [`AsyncDrop`] is the async counterpart of [`Drop`] that zbus's own types implement.
//!
//! [`Builder::runtime`]: crate::connection::Builder::runtime

pub mod traits;

pub(crate) mod io;
mod io_source;
pub use io_source::{Interest, IoSource};
mod async_drop;
pub use async_drop::AsyncDrop;
mod blocking_thread;
#[cfg(feature = "async-io")]
pub(crate) mod builtin;
#[cfg(feature = "async-io")]
pub(crate) use builtin::Builtin;
mod erased;
pub(crate) mod locks;
mod task;
pub(crate) use task::Task;
#[cfg(test)]
pub(crate) mod test_runtime;
mod timeout;
#[cfg(feature = "tokio")]
mod tokio_rt;
#[cfg(feature = "tokio")]
use tokio_rt::Tokio;

// Only the `unixexec` and `ibus` transports and, on macOS, the `launchd` one run a program.
#[cfg(all(unix, any(feature = "unixexec", feature = "ibus", target_os = "macos")))]
pub(crate) mod process;

use std::{future::Future, pin::Pin, sync::Arc};

use erased::ErasedRuntime;

use crate::Result;

/// The runtime a connection runs on, chosen once when it is built.
#[derive(Clone, Debug)]
pub(crate) enum Runtime {
    #[cfg(feature = "async-io")]
    Builtin(Builtin),
    #[cfg(feature = "tokio")]
    Tokio(Tokio),
    /// A runtime handed to the builder, reached through the object-safe mirrors of the traits.
    External(Arc<dyn ErasedRuntime>),
}

impl Runtime {
    /// The runtime for a connection built without an explicit one.
    ///
    /// Tokio when it is compiled in and a runtime is current on this thread, otherwise the
    /// runtime zbus brings along, when that is compiled in. This keeps the features additive:
    /// enabling `tokio` elsewhere in the dependency graph doesn't force every zbus user into a
    /// tokio runtime. Two builds have no default left and report [`Error::Unsupported`] instead,
    /// so that a connection in them has to be given a runtime of its own: one with neither
    /// compiled in, and one with only `tokio` called from a thread where no Tokio runtime is
    /// current.
    ///
    /// [`Error::Unsupported`]: crate::Error::Unsupported
    pub(crate) fn default_for_build() -> Result<Self> {
        #[cfg(feature = "tokio")]
        if let Some(runtime) = Tokio::current() {
            return Ok(Self::Tokio(runtime));
        }
        #[cfg(feature = "async-io")]
        {
            Ok(Self::Builtin(Builtin::new()?))
        }
        #[cfg(not(feature = "async-io"))]
        {
            Err(crate::Error::Unsupported)
        }
    }

    /// The runtime for a connection built with an explicit one.
    ///
    /// Its every operation goes through the erased mirrors of the traits, which is what lets a
    /// connection hold any implementation without being generic over it.
    pub(crate) fn from_external<R>(runtime: R) -> Self
    where
        R: traits::Runtime,
    {
        Self::External(Arc::new(runtime))
    }

    /// Watches `source` for readiness on this runtime.
    ///
    /// Every I/O a connection performs on a socket of its own goes through the registration this
    /// returns, so a socket is only ever driven by the runtime the connection was built with.
    pub(crate) fn register_io_source(&self, source: IoSource) -> std::io::Result<io::Registration> {
        match self {
            #[cfg(feature = "async-io")]
            Self::Builtin(runtime) => {
                traits::Runtime::register_io_source(runtime, source).map(io::Registration::Builtin)
            }
            #[cfg(feature = "tokio")]
            Self::Tokio(runtime) => {
                traits::Runtime::register_io_source(runtime, source).map(io::Registration::Tokio)
            }
            Self::External(runtime) => {
                traits::Runtime::register_io_source(runtime, source).map(io::Registration::External)
            }
        }
    }

    /// Spawns a task onto the runtime, under the diagnostic name `name`.
    pub(crate) fn spawn<T>(
        &self,
        name: &str,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Task<T>
    where
        T: Send + 'static,
    {
        match self {
            #[cfg(feature = "async-io")]
            Self::Builtin(runtime) => Task::Builtin(traits::Runtime::spawn(runtime, name, future)),
            #[cfg(feature = "tokio")]
            Self::Tokio(runtime) => Task::Tokio(traits::Runtime::spawn(runtime, name, future)),
            Self::External(runtime) => {
                Task::External(traits::Runtime::spawn(runtime, name, future))
            }
        }
    }

    /// Runs `work` off the event loop, on whatever this runtime keeps for blocking work.
    ///
    /// The work is handed to the runtime here and not on the first poll, and the future this
    /// hands back is the runtime's own and borrows nothing from here: whoever wants the outcome
    /// awaits it, and whoever does not may drop it, with the work under way either way.
    pub(crate) fn spawn_blocking<T>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Pin<Box<dyn Future<Output = T> + Send + 'static>>
    where
        T: Send + 'static,
    {
        match self {
            #[cfg(feature = "async-io")]
            Self::Builtin(runtime) => traits::Runtime::spawn_blocking(runtime, work),
            #[cfg(feature = "tokio")]
            Self::Tokio(runtime) => traits::Runtime::spawn_blocking(runtime, work),
            Self::External(runtime) => traits::Runtime::spawn_blocking(runtime, work),
        }
    }
}

#[cfg(test)]
mod tests;
