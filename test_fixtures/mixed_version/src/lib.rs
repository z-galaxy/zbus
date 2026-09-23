//! Compile-only regression for this crate's macros paired with an already-released zbus.
//!
//! Cargo is free to pair them: every released zbus 5.x requires `zbus_macros = "^5.x"`, so
//! pinning zbus while the macro crate moves on resolves to a newer macro against an older
//! runtime. Emitting `__if_blocking_api_feature!` there stops even zbus's own `fdo` proxies from
//! compiling, so the macro falls back to its pre-gate expansion when zbus does not turn
//! `blocking-api-gate` on, and that fallback is what this fixture pins down.
//!
//! The released zbus decides through its own `blocking-api` feature, as it always did, so the
//! blocking proxy must appear with `target-blocking` and be absent without it.

#[zbus::proxy(
    interface = "org.zbus.MixedVersion",
    default_service = "org.zbus.MixedVersion",
    default_path = "/org/zbus/MixedVersion"
)]
pub trait MixedVersion {
    fn ping(&self) -> zbus::Result<()>;

    #[zbus(signal)]
    fn changed(&self, value: u32) -> zbus::Result<()>;
}

// A duplicate definition fails compilation if a blocking proxy leaks out.
#[cfg(not(feature = "target-blocking"))]
pub struct MixedVersionProxyBlocking;

pub fn async_types(_: MixedVersionProxy<'_>, _: ChangedArgs<'_>) {}

#[cfg(feature = "target-blocking")]
pub fn blocking_types(_: MixedVersionProxyBlocking<'_>, _: ChangedIterator) {}
