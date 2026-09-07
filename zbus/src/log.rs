//! Internal logging shims.
//!
//! This module forwards to [`tracing`] when the `tracing` feature is enabled, and compiles to
//! no-op macros and types otherwise, so the rest of the crate can log unconditionally without
//! caring whether the feature is on.

#[cfg(feature = "tracing")]
pub(crate) use tracing::{Instrument, debug, info, info_span, trace, trace_span, warn};

#[cfg(not(feature = "tracing"))]
mod noop {
    // The event shims type-check their arguments in a branch that is never taken, the way the
    // `log` crate does when a level is compiled out. Nothing is evaluated at runtime, but the
    // bindings a call site only uses in its log message do not become unused, so the no-`tracing`
    // build stays free of warnings without any `#[allow]` or `_`-prefixed names.
    macro_rules! event {
        ($fmt:literal $(, $arg:expr)* $(,)?) => {
            if false {
                let _ = ::std::format_args!($fmt $(, $arg)*);
            }
        };
    }
    macro_rules! trace {
        ($($arg:tt)*) => { $crate::log::event!($($arg)*) };
    }
    macro_rules! debug {
        ($($arg:tt)*) => { $crate::log::event!($($arg)*) };
    }
    macro_rules! info {
        ($($arg:tt)*) => { $crate::log::event!($($arg)*) };
    }
    // Named differently from `warn` (and re-exported as such below) because `use`-ing it
    // directly as `warn` is ambiguous with the built-in `#[warn(..)]` attribute.
    macro_rules! warn_event {
        ($($arg:tt)*) => { $crate::log::event!($($arg)*) };
    }
    macro_rules! trace_span {
        ($($arg:tt)*) => {
            $crate::log::Span
        };
    }
    macro_rules! info_span {
        ($($arg:tt)*) => {
            $crate::log::Span
        };
    }

    pub(crate) use debug;
    pub(crate) use event;
    pub(crate) use info;
    pub(crate) use info_span;
    pub(crate) use trace;
    pub(crate) use trace_span;
    pub(crate) use warn_event as warn;
}

#[cfg(not(feature = "tracing"))]
pub(crate) use noop::{debug, event, info, info_span, trace, trace_span, warn};

/// A no-op stand-in for `tracing::Span`, used when the `tracing` feature is disabled.
#[cfg(not(feature = "tracing"))]
pub(crate) struct Span;

/// A no-op stand-in for `tracing::Instrument`, used when the `tracing` feature is disabled.
#[cfg(not(feature = "tracing"))]
pub(crate) trait Instrument: Sized {
    fn instrument(self, _span: Span) -> Self {
        self
    }
}

#[cfg(not(feature = "tracing"))]
impl<T> Instrument for T {}
