use crate::{Str, Type, names::utils::define_name_type_impls};
use serde::Serialize;

/// String that identifies a [property][pn] name on the bus.
///
/// # Examples
///
/// ```
/// use zbus::names::PropertyName;
///
/// // Valid property names.
/// let name = PropertyName::try_from("Property_for_you").unwrap();
/// assert_eq!(name, "Property_for_you");
/// let name = PropertyName::try_from("CamelCase101").unwrap();
/// assert_eq!(name, "CamelCase101");
/// let name = PropertyName::try_from("a_very_loooooooooooooooooo_ooooooo_0000o0ngName").unwrap();
/// assert_eq!(name, "a_very_loooooooooooooooooo_ooooooo_0000o0ngName");
/// let name = PropertyName::try_from("Property_for_you-1").unwrap();
/// assert_eq!(name, "Property_for_you-1");
/// ```
///
/// [pn]: https://dbus.freedesktop.org/doc/dbus-specification.html#standard-interfaces-properties
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Type, PartialOrd, Ord)]
#[serde(transparent)]
pub struct PropertyName<'name>(Str<'name>);

/// Owned sibling of [`PropertyName`].
#[derive(Clone, Hash, PartialEq, Eq, Serialize, Type, PartialOrd, Ord)]
#[serde(transparent)]
pub struct OwnedPropertyName(#[serde(borrow)] PropertyName<'static>);

define_name_type_impls! {
    name: PropertyName,
    owned: OwnedPropertyName,
    validate: validate_property_name,
}
