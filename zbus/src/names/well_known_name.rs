use crate::{Str, Type, names::utils::define_name_type_impls};
use serde::Serialize;

/// String that identifies a [well-known bus name][wbn].
///
/// # Examples
///
/// ```
/// use zbus::names::WellKnownName;
///
/// // Valid well-known names.
/// let name = WellKnownName::try_from("org.gnome.Service-for_you").unwrap();
/// assert_eq!(name, "org.gnome.Service-for_you");
/// let name = WellKnownName::try_from("a.very.loooooooooooooooooo-ooooooo_0000o0ng.Name").unwrap();
/// assert_eq!(name, "a.very.loooooooooooooooooo-ooooooo_0000o0ng.Name");
///
/// // Invalid well-known names
/// WellKnownName::try_from("").unwrap_err();
/// WellKnownName::try_from("double..dots").unwrap_err();
/// WellKnownName::try_from(".").unwrap_err();
/// WellKnownName::try_from(".start.with.dot").unwrap_err();
/// WellKnownName::try_from("1st.element.starts.with.digit").unwrap_err();
/// WellKnownName::try_from("the.2nd.element.starts.with.digit").unwrap_err();
/// WellKnownName::try_from("no-dots").unwrap_err();
/// ```
///
/// [wbn]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-names-bus
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Type, PartialOrd, Ord)]
#[serde(transparent)]
pub struct WellKnownName<'name>(pub(crate) Str<'name>);

/// Owned sibling of [`WellKnownName`].
#[derive(Clone, Hash, PartialEq, Eq, Serialize, Type, PartialOrd, Ord)]
#[serde(transparent)]
pub struct OwnedWellKnownName(#[serde(borrow)] WellKnownName<'static>);

define_name_type_impls! {
    name: WellKnownName,
    owned: OwnedWellKnownName,
    validate: validate_well_known_name,
}
