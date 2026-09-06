use crate::{
    ObjectPath,
    names::{BusName, InterfaceName},
};

/// Trait for the default associated values of a proxy.
///
/// The trait is automatically implemented by the [`macro@crate::proxy`] macro on your behalf, and
/// may be later used to retrieve the associated constants.
pub trait Defaults {
    const INTERFACE: &'static Option<InterfaceName<'static>>;
    const DESTINATION: &'static Option<BusName<'static>>;
    const PATH: &'static Option<ObjectPath<'static>>;
    /// Whether the interface has any properties.
    ///
    /// A proxy for an interface without properties never sets up a properties cache, so a build
    /// with [`CacheProperties::Yes`](super::CacheProperties::Yes) has nothing to populate. The
    /// `#[proxy]` macro sets this from the trait; a hand-written implementation can leave the
    /// default.
    const HAS_PROPERTIES: bool = true;
}

impl Defaults for super::Proxy<'_> {
    const INTERFACE: &'static Option<InterfaceName<'static>> = &None;
    const DESTINATION: &'static Option<BusName<'static>> = &None;
    const PATH: &'static Option<ObjectPath<'static>> = &None;
}
