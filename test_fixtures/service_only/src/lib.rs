//! Compile-only fixture: a downstream crate that only enables the `service` feature of `zbus`,
//! while its build script depends on `zbus` with the `proxy` feature.
//!
//! Cargo unifies the features of host dependencies (build scripts and proc-macros) separately
//! from the target ones, so `zbus_macros` is compiled with `proxy` enabled here even though the
//! `zbus` this crate links against is not. The `proxy` attribute of the `interface` macro must
//! therefore decide on the `zbus` side whether to generate a proxy.

#![allow(dead_code)]

use zbus::interface;

struct ServiceOnly;

#[interface(name = "org.freedesktop.ServiceOnlyFixture", proxy)]
impl ServiceOnly {
    fn some_method(&self) -> String {
        "some".to_string()
    }

    #[zbus(property)]
    fn some_property(&self) -> u32 {
        42
    }
}
