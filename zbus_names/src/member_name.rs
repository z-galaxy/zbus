use crate::{Error, InvalidNameReason, NameType, Result, utils::define_name_type_impls};
use serde::Serialize;
use zvariant::{OwnedValue, Str, Type, Value};

/// The maximum length of a D-Bus name in bytes.
const MAX_NAME_LENGTH: usize = 255;

/// String that identifies an [member (method or signal) name][in] on the bus.
///
/// # Examples
///
/// ```
/// use zbus_names::MemberName;
///
/// // Valid member names.
/// let name = MemberName::try_from("Member_for_you").unwrap();
/// assert_eq!(name, "Member_for_you");
/// let name = MemberName::try_from("CamelCase101").unwrap();
/// assert_eq!(name, "CamelCase101");
/// let name = MemberName::try_from("a_very_loooooooooooooooooo_ooooooo_0000o0ngName").unwrap();
/// assert_eq!(name, "a_very_loooooooooooooooooo_ooooooo_0000o0ngName");
///
/// // Invalid member names
/// MemberName::try_from("").unwrap_err();
/// MemberName::try_from(".").unwrap_err();
/// MemberName::try_from("1startWith_a_Digit").unwrap_err();
/// MemberName::try_from("contains.dots_in_the_name").unwrap_err();
/// MemberName::try_from("contains-dashes-in_the_name").unwrap_err();
/// ```
///
/// [in]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-names-member
#[derive(
    Clone, Debug, Hash, PartialEq, Eq, Serialize, Type, Value, PartialOrd, Ord, OwnedValue,
)]
pub struct MemberName<'name>(Str<'name>);

/// Owned sibling of [`MemberName`].
#[derive(Clone, Hash, PartialEq, Eq, Serialize, Type, Value, PartialOrd, Ord, OwnedValue)]
pub struct OwnedMemberName(#[serde(borrow)] MemberName<'static>);

define_name_type_impls! {
    name: MemberName,
    owned: OwnedMemberName,
    validate: validate,
}

fn validate(name: &str) -> Result<()> {
    validate_detailed(name.as_bytes(), NameType::Member)
}

/// Validate a member name with detailed error reporting.
///
/// Rules:
/// * Only ASCII alphanumeric or `_`
/// * Must not begin with a digit
/// * Must contain at least 1 character
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

    // Check first character (must be alpha or underscore, not digit)
    let first = bytes[0];
    if !first.is_ascii_alphabetic() && first != b'_' {
        return Err(Error::InvalidNameDetail {
            name_type,
            reason: if first.is_ascii_digit() {
                InvalidNameReason::ElementStartsWithDigit { element_index: 0 }
            } else {
                InvalidNameReason::InvalidCharacter {
                    character: first as char,
                    position: 0,
                }
            },
        });
    }

    // Fast path: check all remaining characters without tracking position
    let rest = &bytes[1..];
    if rest.iter().all(|&b| b.is_ascii_alphanumeric() || b == b'_') {
        return Ok(());
    }

    // Slow path: find the invalid character position (only reached on error)
    for (pos, &byte) in rest.iter().enumerate() {
        if !byte.is_ascii_alphanumeric() && byte != b'_' {
            return Err(Error::InvalidNameDetail {
                name_type,
                reason: InvalidNameReason::InvalidCharacter {
                    character: byte as char,
                    position: pos + 1,
                },
            });
        }
    }

    // Should be unreachable, but satisfy the compiler
    Ok(())
}
