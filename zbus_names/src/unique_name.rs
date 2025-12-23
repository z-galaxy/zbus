use crate::{Error, InvalidNameReason, NameType, Result, utils::define_name_type_impls};
use serde::Serialize;
use zvariant::{OwnedValue, Str, Type, Value};

/// The maximum length of a D-Bus name in bytes.
const MAX_NAME_LENGTH: usize = 255;

/// String that identifies a [unique bus name][ubn].
///
/// # Examples
///
/// ```
/// use zbus_names::UniqueName;
///
/// // Valid unique names.
/// let name = UniqueName::try_from(":org.gnome.Service-for_you").unwrap();
/// assert_eq!(name, ":org.gnome.Service-for_you");
/// let name = UniqueName::try_from(":a.very.loooooooooooooooooo-ooooooo_0000o0ng.Name").unwrap();
/// assert_eq!(name, ":a.very.loooooooooooooooooo-ooooooo_0000o0ng.Name");
///
/// // Invalid unique names
/// UniqueName::try_from("").unwrap_err();
/// UniqueName::try_from("dont.start.with.a.colon").unwrap_err();
/// UniqueName::try_from(":double..dots").unwrap_err();
/// UniqueName::try_from(".").unwrap_err();
/// UniqueName::try_from(".start.with.dot").unwrap_err();
/// UniqueName::try_from(":no-dots").unwrap_err();
/// ```
///
/// [ubn]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-names-bus
#[derive(
    Clone, Debug, Hash, PartialEq, Eq, Serialize, Type, Value, PartialOrd, Ord, OwnedValue,
)]
pub struct UniqueName<'name>(pub(crate) Str<'name>);

/// Owned sibling of [`UniqueName`].
#[derive(Clone, Hash, PartialEq, Eq, Serialize, Type, Value, PartialOrd, Ord, OwnedValue)]
pub struct OwnedUniqueName(#[serde(borrow)] UniqueName<'static>);

define_name_type_impls! {
    name: UniqueName,
    owned: OwnedUniqueName,
    validate: validate,
}

fn validate(name: &str) -> Result<()> {
    validate_detailed(name.as_bytes(), NameType::Unique)
}

/// Validate a unique bus name with detailed error reporting.
///
/// Rules:
/// * Only ASCII alphanumeric, `_` or `-`
/// * Must begin with a `:`
/// * Must contain at least one `.`
/// * Each element must be at least 1 character (so name must be minimum 4 characters long)
/// * <= 255 characters
///
/// Note: "org.freedesktop.DBus" is also accepted as a special case.
pub(crate) fn validate_detailed(bytes: &[u8], name_type: NameType) -> Result<()> {
    // Early exit: Check for empty name first
    if bytes.is_empty() {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: InvalidNameReason::Empty,
        });
    }

    // Early exit: Check length before parsing (cheap check for invalid long names)
    if bytes.len() > MAX_NAME_LENGTH {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: InvalidNameReason::TooLong {
                actual: bytes.len(),
                max: MAX_NAME_LENGTH,
            },
        });
    }

    // Special case: "org.freedesktop.DBus" is always valid as a unique name
    if bytes == b"org.freedesktop.DBus" {
        return Ok(());
    }

    // Early exit: Unique names must start with ':'
    if bytes[0] != b':' {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: InvalidNameReason::MissingColonPrefix,
        });
    }

    // Validate the rest of the name (after the colon)
    let rest = &bytes[1..];

    if rest.is_empty() {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: InvalidNameReason::Empty,
        });
    }

    // Check for leading dot after colon
    if rest[0] == b'.' {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: InvalidNameReason::StartsWithDot,
        });
    }

    let mut has_dot = false;
    let mut at_element_start = true;

    for (pos, &byte) in rest.iter().enumerate() {
        if byte == b'.' {
            // Check for consecutive dots (empty element)
            if at_element_start {
                return Err(Error::InvalidNameDetail {
                    name_type,
                    reason: InvalidNameReason::EmptyElement,
                });
            }
            has_dot = true;
            at_element_start = true;
        } else if at_element_start {
            // First character of an element: alphanumeric, underscore, or hyphen
            // Note: Unlike well-known names, unique names CAN start elements with digits
            if !byte.is_ascii_alphanumeric() && byte != b'_' && byte != b'-' {
                return Err(Error::InvalidNameDetail {
                    name_type,
                    reason: InvalidNameReason::InvalidCharacter {
                        character: byte as char,
                        position: pos + 1, // For the leading colon
                    },
                });
            }
            at_element_start = false;
        } else {
            // Subsequent characters: alphanumeric, underscore, or hyphen
            if !byte.is_ascii_alphanumeric() && byte != b'_' && byte != b'-' {
                return Err(Error::InvalidNameDetail {
                    name_type,
                    reason: InvalidNameReason::InvalidCharacter {
                        character: byte as char,
                        position: pos + 1, // For the leading colon
                    },
                });
            }
        }
    }

    // Check trailing dot (empty final element)
    if at_element_start {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: InvalidNameReason::EmptyElement,
        });
    }

    // Must have at least one dot (two elements)
    if !has_dot {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: InvalidNameReason::MissingDotSeparator,
        });
    }

    Ok(())
}
