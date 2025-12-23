use crate::{Error, InvalidNameReason, NameType, Result, utils::define_name_type_impls};
use serde::Serialize;
use zvariant::{OwnedValue, Str, Type, Value};

/// The maximum length of a D-Bus name in bytes.
const MAX_NAME_LENGTH: usize = 255;

/// String that identifies a [property][pn] name on the bus.
///
/// # Examples
///
/// ```
/// use zbus_names::PropertyName;
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
#[derive(
    Clone, Debug, Hash, PartialEq, Eq, Serialize, Type, Value, PartialOrd, Ord, OwnedValue,
)]
pub struct PropertyName<'name>(Str<'name>);

/// Owned sibling of [`PropertyName`].
#[derive(Clone, Hash, PartialEq, Eq, Serialize, Type, Value, PartialOrd, Ord, OwnedValue)]
pub struct OwnedPropertyName(#[serde(borrow)] PropertyName<'static>);

define_name_type_impls! {
    name: PropertyName,
    owned: OwnedPropertyName,
    validate: ensure_correct_property_name,
}

fn ensure_correct_property_name(name: &str) -> Result<()> {
    let name_type = NameType::Property;

    if name.is_empty() {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: InvalidNameReason::Empty,
        });
    }

    if name.len() > MAX_NAME_LENGTH {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: InvalidNameReason::TooLong {
                actual: name.len(),
                max: MAX_NAME_LENGTH,
            },
        });
    }

    Ok(())
}
