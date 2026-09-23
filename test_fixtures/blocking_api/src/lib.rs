//! Compile-only regression for a blocking host zbus and an async-only target.
//!
//! The dependency is deliberately renamed, and this crate has no feature named
//! `blocking-api`: generated code must use the feature of the target zbus.

pub mod default_options {
    #[renamed_zbus::proxy(interface = "org.zbus.DefaultOptions", assume_defaults = true)]
    pub trait DefaultOptions {
        fn ping(&self) -> renamed_zbus::Result<()>;

        #[zbus(signal)]
        fn changed(&self, value: u32) -> renamed_zbus::Result<()>;
    }

    // A duplicate definition fails compilation if a blocking proxy leaks out.
    #[cfg(not(feature = "target-blocking"))]
    pub struct DefaultOptionsProxyBlocking;

    pub fn async_types(_: DefaultOptionsProxy<'_>, _: ChangedArgs<'_>) {}

    #[cfg(feature = "target-blocking")]
    pub fn blocking_types(_: DefaultOptionsProxyBlocking<'_>, _: ChangedIterator) {}
}

pub mod explicit_true {
    #[renamed_zbus::proxy(
        interface = "org.zbus.ExplicitTrue",
        assume_defaults = true,
        gen_blocking = true,
        blocking_name = "ExplicitBlocking",
        crate = "renamed_zbus"
    )]
    pub trait ExplicitTrue {
        fn ping(&self) -> renamed_zbus::Result<()>;
    }

    #[cfg(not(feature = "target-blocking"))]
    pub struct ExplicitBlocking;

    pub fn async_type(_: ExplicitTrueProxy<'_>) {}

    #[cfg(feature = "target-blocking")]
    pub fn blocking_type(_: ExplicitBlocking<'_>) {}
}

pub mod explicit_false {
    #[renamed_zbus::proxy(
        interface = "org.zbus.ExplicitFalse",
        assume_defaults = true,
        gen_blocking = false
    )]
    pub trait ExplicitFalse {
        fn ping(&self) -> renamed_zbus::Result<()>;
    }

    pub struct ExplicitFalseProxyBlocking;

    pub fn async_type(_: ExplicitFalseProxy<'_>) {}
}

pub mod blocking_only {
    #[renamed_zbus::proxy(
        interface = "org.zbus.BlockingOnly",
        assume_defaults = true,
        gen_async = false
    )]
    pub trait BlockingOnly {
        #[zbus(signal)]
        fn changed(&self, value: u32) -> renamed_zbus::Result<()>;
    }

    #[cfg(not(feature = "target-blocking"))]
    pub struct BlockingOnlyProxy;
    #[cfg(not(feature = "target-blocking"))]
    pub struct ChangedArgs;
    #[cfg(not(feature = "target-blocking"))]
    pub struct ChangedIterator;

    #[cfg(feature = "target-blocking")]
    pub fn blocking_types(_: BlockingOnlyProxy<'_>, _: ChangedArgs<'_>, _: ChangedIterator) {}
}
