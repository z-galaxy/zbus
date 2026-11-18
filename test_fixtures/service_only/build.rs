//! The build script only exists so that the `build-dependencies` are compiled.

use std::marker::PhantomData;

// The dependency on `zbus` is only there for its `proxy` feature (see `Cargo.toml`): without this
// line, nothing would refer to it, and Cargo would report it as unused.
const _: PhantomData<zbus::Proxy<'static>> = PhantomData;

fn main() {}
