use crate::{Error, InvalidNameReason, NameType, Result, utils::define_name_type_impls};
use serde::Serialize;
use zvariant::{OwnedValue, Str, Type, Value};

/// The maximum length of a D-Bus name in bytes.
const MAX_NAME_LENGTH: usize = 255;

/// String that identifies an [interface name][in] on the bus.
///
/// # Examples
///
/// ```
/// use zbus_names::InterfaceName;
///
/// // Valid interface names.
/// let name = InterfaceName::try_from("org.gnome.Interface_for_you").unwrap();
/// assert_eq!(name, "org.gnome.Interface_for_you");
/// let name = InterfaceName::try_from("a.very.loooooooooooooooooo_ooooooo_0000o0ng.Name").unwrap();
/// assert_eq!(name, "a.very.loooooooooooooooooo_ooooooo_0000o0ng.Name");
///
/// // Invalid interface names
/// InterfaceName::try_from("").unwrap_err();
/// InterfaceName::try_from(":start.with.a.colon").unwrap_err();
/// InterfaceName::try_from("double..dots").unwrap_err();
/// InterfaceName::try_from(".").unwrap_err();
/// InterfaceName::try_from(".start.with.dot").unwrap_err();
/// InterfaceName::try_from("no-dots").unwrap_err();
/// InterfaceName::try_from("1st.element.starts.with.digit").unwrap_err();
/// InterfaceName::try_from("the.2nd.element.starts.with.digit").unwrap_err();
/// InterfaceName::try_from("contains.dashes-in.the.name").unwrap_err();
/// ```
///
/// [in]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-names-interface
#[derive(
    Clone, Debug, Hash, PartialEq, Eq, Serialize, Type, Value, PartialOrd, Ord, OwnedValue,
)]
pub struct InterfaceName<'name>(Str<'name>);

/// Owned sibling of [`InterfaceName`].
#[derive(Clone, Hash, PartialEq, Eq, Serialize, Type, Value, PartialOrd, Ord, OwnedValue)]
pub struct OwnedInterfaceName(#[serde(borrow)] InterfaceName<'static>);

define_name_type_impls! {
    name: InterfaceName,
    owned: OwnedInterfaceName,
    validate: validate,
}

fn validate(name: &str) -> Result<()> {
    validate_detailed(name.as_bytes(), NameType::Interface)
}

/// Validate an interface name with detailed error reporting.
///
/// Rules:
/// * Only ASCII alphanumeric and `_`
/// * Must not begin with a `.`
/// * Must contain at least one `.`
/// * Each element must:
///   * not begin with a digit
///   * be at least 1 character (so name must be minimum 3 characters long)
/// * <= 255 characters
///
/// Note: A `-` is not allowed, which is why we can't use the same parser as for `WellKnownName`.
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
            // First character of an element
            if byte.is_ascii_digit() {
                return Err(Error::InvalidNameDetail {
                    name_type,
                    reason: InvalidNameReason::ElementStartsWithDigit { element_index },
                });
            }
            if !byte.is_ascii_alphabetic() && byte != b'_' {
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
            // Subsequent characters in an element: alphanumeric or underscore only
            if !byte.is_ascii_alphanumeric() && byte != b'_' {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_interface_names() {
        // Valid names should pass
        InterfaceName::try_from("org.freedesktop.DBus").unwrap();
        InterfaceName::try_from("a.b").unwrap();
        InterfaceName::try_from("org.gnome.Interface_for_you").unwrap();
        InterfaceName::try_from("_private.Interface").unwrap();
    }

    #[test]
    fn test_empty_name_error() {
        let err = InterfaceName::try_from("").unwrap_err();
        match err {
            Error::InvalidNameDetail {
                name_type: NameType::Interface,
                reason: InvalidNameReason::Empty,
            } => {}
            _ => panic!("Expected Empty error, got: {:?}", err),
        }
    }

    #[test]
    fn test_too_long_name_error() {
        let long_name = "a.".to_string() + &"b".repeat(254);
        let err = InterfaceName::try_from(long_name.as_str()).unwrap_err();
        match err {
            Error::InvalidNameDetail {
                name_type: NameType::Interface,
                reason: InvalidNameReason::TooLong { actual, max: 255 },
            } => {
                assert_eq!(actual, long_name.len());
            }
            _ => panic!("Expected TooLong error, got: {:?}", err),
        }
    }

    #[test]
    fn test_starts_with_dot_error() {
        let err = InterfaceName::try_from(".org.freedesktop").unwrap_err();
        match err {
            Error::InvalidNameDetail {
                name_type: NameType::Interface,
                reason: InvalidNameReason::StartsWithDot,
            } => {}
            _ => panic!("Expected StartsWithDot error, got: {:?}", err),
        }
    }

    #[test]
    fn test_missing_dot_separator_error() {
        let err = InterfaceName::try_from("nodots").unwrap_err();
        match err {
            Error::InvalidNameDetail {
                name_type: NameType::Interface,
                reason: InvalidNameReason::MissingDotSeparator,
            } => {}
            _ => panic!("Expected MissingDotSeparator error, got: {:?}", err),
        }
    }

    #[test]
    fn test_element_starts_with_digit_error() {
        // First element starts with digit
        let err = InterfaceName::try_from("1org.freedesktop").unwrap_err();
        match err {
            Error::InvalidNameDetail {
                name_type: NameType::Interface,
                reason: InvalidNameReason::ElementStartsWithDigit { element_index: 0 },
            } => {}
            _ => panic!("Expected ElementStartsWithDigit(0) error, got: {:?}", err),
        }

        // Second element starts with digit
        let err = InterfaceName::try_from("org.1freedesktop").unwrap_err();
        match err {
            Error::InvalidNameDetail {
                name_type: NameType::Interface,
                reason: InvalidNameReason::ElementStartsWithDigit { element_index: 1 },
            } => {}
            _ => panic!("Expected ElementStartsWithDigit(1) error, got: {:?}", err),
        }
    }

    #[test]
    fn test_empty_element_error() {
        // Consecutive dots
        let err = InterfaceName::try_from("org..freedesktop").unwrap_err();
        match err {
            Error::InvalidNameDetail {
                name_type: NameType::Interface,
                reason: InvalidNameReason::EmptyElement,
            } => {}
            _ => panic!("Expected EmptyElement error, got: {:?}", err),
        }

        // Trailing dot
        let err = InterfaceName::try_from("org.freedesktop.").unwrap_err();
        match err {
            Error::InvalidNameDetail {
                name_type: NameType::Interface,
                reason: InvalidNameReason::EmptyElement,
            } => {}
            _ => panic!(
                "Expected EmptyElement error for trailing dot, got: {:?}",
                err
            ),
        }
    }

    #[test]
    fn test_invalid_character_error() {
        // Dash is not allowed in interface names (unlike well-known names)
        let err = InterfaceName::try_from("org.free-desktop").unwrap_err();
        match err {
            Error::InvalidNameDetail {
                name_type: NameType::Interface,
                reason:
                    InvalidNameReason::InvalidCharacter {
                        character: '-',
                        position,
                    },
            } => {
                assert_eq!(position, 8); // Position of '-'
            }
            _ => panic!("Expected InvalidCharacter('-') error, got: {:?}", err),
        }

        // Colon not allowed
        let err = InterfaceName::try_from(":org.freedesktop").unwrap_err();
        match err {
            Error::InvalidNameDetail {
                name_type: NameType::Interface,
                reason:
                    InvalidNameReason::InvalidCharacter {
                        character: ':',
                        position: 0,
                    },
            } => {}
            _ => panic!("Expected InvalidCharacter(':') error, got: {:?}", err),
        }
    }

    #[test]
    fn test_error_display_message() {
        let err = InterfaceName::try_from("").unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("interface name"),
            "Message should mention interface name"
        );
        assert!(msg.contains("empty"), "Message should mention 'empty'");
    }
}
