use crate::{Error, InvalidNameReason, NameType, Result, utils::define_name_type_impls};
use serde::Serialize;
use zvariant::{OwnedValue, Str, Type, Value};

/// The maximum length of a D-Bus name in bytes.
const MAX_NAME_LENGTH: usize = 255;

/// String that identifies a [well-known bus name][wbn].
///
/// # Examples
///
/// ```
/// use zbus_names::WellKnownName;
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
#[derive(
    Clone, Debug, Hash, PartialEq, Eq, Serialize, Type, Value, PartialOrd, Ord, OwnedValue,
)]
pub struct WellKnownName<'name>(pub(crate) Str<'name>);

/// Owned sibling of [`WellKnownName`].
#[derive(Clone, Hash, PartialEq, Eq, Serialize, Type, Value, PartialOrd, Ord, OwnedValue)]
pub struct OwnedWellKnownName(#[serde(borrow)] WellKnownName<'static>);

define_name_type_impls! {
    name: WellKnownName,
    owned: OwnedWellKnownName,
    validate: validate,
}

fn validate(name: &str) -> Result<()> {
    validate_detailed(name.as_bytes(), NameType::WellKnown)
}

/// Validate a well-known bus name with detailed error reporting.
///
/// Rules:
/// * Only ASCII alphanumeric, `_` or `-`
/// * Must not begin with a `.`
/// * Must contain at least one `.`
/// * Each element must:
///   * not begin with a digit
///   * be at least 1 character (so name must be minimum 3 characters long)
/// * <= 255 characters
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

    // Early exit: Check for leading dot
    if bytes[0] == b'.' {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: InvalidNameReason::StartsWithDot,
        });
    }

    let mut has_dot = false;
    let mut element_index = 0;
    let mut at_element_start = true;

    for (pos, &byte) in bytes.iter().enumerate() {
        if byte == b'.' {
            // Check for consecutive dots (empty element)
            if at_element_start {
                return Err(Error::InvalidNameDetail {
                    name_type,
                    reason: InvalidNameReason::EmptyElement,
                });
            }
            has_dot = true;
            element_index += 1;
            at_element_start = true;
        } else if at_element_start {
            // First character of an element: must be alpha, underscore, or hyphen (not digit)
            if byte.is_ascii_digit() {
                return Err(Error::InvalidNameDetail {
                    name_type,
                    reason: InvalidNameReason::ElementStartsWithDigit { element_index },
                });
            }
            if !byte.is_ascii_alphabetic() && byte != b'_' && byte != b'-' {
                return Err(Error::InvalidNameDetail {
                    name_type,
                    reason: InvalidNameReason::InvalidCharacter {
                        character: byte as char,
                        position: pos,
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
                        position: pos,
                    },
                });
            }
        }
    }

    // Check trailing dot (empty final element)
    if at_element_start && !bytes.is_empty() {
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
