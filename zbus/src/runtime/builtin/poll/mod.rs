//! The wait a platform is asked for, and what it is told to watch.
//!
//! Each implementation watches a list of sources for the length of one wait and reports which of
//! them were found ready, and each keeps a channel of its own that a `notify` writes to, so that
//! a wait can be broken from another thread. The sources are handed over as `IoSource` clones and
//! held for the whole call, so no descriptor in the set can be closed while the platform is
//! looking at it.

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(super) use unix::Poller;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::{MAX_SOURCES, Poller};

/// What to watch a source for.
pub(super) struct Want {
    pub(super) key: usize,
    pub(super) readable: bool,
    pub(super) writable: bool,
}

/// What a source was found ready for; `Want`'s shape.
pub(super) struct Ready {
    pub(super) key: usize,
    pub(super) readable: bool,
    pub(super) writable: bool,
}
