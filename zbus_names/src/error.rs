use std::{convert::Infallible, error, fmt};
use zvariant::Error as VariantError;

/// The type of D-Bus name being validated.
///
/// This enum identifies which category of D-Bus name failed validation,
/// allowing for more specific error handling and diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum NameType {
    /// A well-known bus name (e.g., `org.freedesktop.DBus`).
    WellKnown,
    /// A unique bus name (e.g., `:1.42`).
    Unique,
    /// An interface name (e.g., `org.freedesktop.DBus.Peer`).
    Interface,
    /// A member (method or signal) name (e.g., `Ping`).
    Member,
    /// A property name.
    Property,
    /// An error name (follows interface name rules).
    Error,
}

impl fmt::Display for NameType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NameType::WellKnown => write!(f, "well-known bus name"),
            NameType::Unique => write!(f, "unique bus name"),
            NameType::Interface => write!(f, "interface name"),
            NameType::Member => write!(f, "member name"),
            NameType::Property => write!(f, "property name"),
            NameType::Error => write!(f, "error name"),
        }
    }
}

/// The specific reason why a D-Bus name validation failed.
///
/// This enum provides detailed information about validation failures,
/// enabling precise error messages and programmatic error handling.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InvalidNameReason {
    /// The name is empty.
    Empty,
    /// The name exceeds the maximum allowed length (255 bytes).
    TooLong {
        /// The actual length of the invalid name.
        actual: usize,
        /// The maximum allowed length (255).
        max: usize,
    },
    /// A unique name must start with a colon (`:`).
    MissingColonPrefix,
    /// The name must not start with a dot (`.`).
    StartsWithDot,
    /// The name must contain at least one dot (`.`) as a separator.
    MissingDotSeparator,
    /// An element in the name starts with a digit.
    ElementStartsWithDigit {
        /// The 0-indexed position of the element that starts with a digit.
        element_index: usize,
    },
    /// An element in the name is empty (e.g., `foo..bar`).
    EmptyElement,
    /// The name contains an invalid character.
    InvalidCharacter {
        /// The invalid character.
        character: char,
        /// The 0-indexed position of the invalid character.
        position: usize,
    },
    /// A generic parsing error (fallback).
    ParseError,
}

impl fmt::Display for InvalidNameReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InvalidNameReason::Empty => write!(f, "name is empty"),
            InvalidNameReason::TooLong { actual, max } => {
                write!(f, "name is too long ({actual} bytes, max {max})")
            }
            InvalidNameReason::MissingColonPrefix => {
                write!(f, "unique name must start with ':'")
            }
            InvalidNameReason::StartsWithDot => write!(f, "name must not start with '.'"),
            InvalidNameReason::MissingDotSeparator => {
                write!(f, "name must contain at least one '.' separator")
            }
            InvalidNameReason::ElementStartsWithDigit { element_index } => {
                write!(f, "element {element_index} starts with a digit")
            }
            InvalidNameReason::EmptyElement => write!(f, "name contains an empty element"),
            InvalidNameReason::InvalidCharacter {
                character,
                position,
            } => {
                write!(f, "invalid character '{character}' at position {position}")
            }
            InvalidNameReason::ParseError => write!(f, "failed to parse name"),
        }
    }
}

/// The error type for `zbus_names`.
///
/// The various errors that can be reported by this crate.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Error {
    Variant(VariantError),
    /// Invalid bus name. The strings describe why the bus name is neither a valid unique nor
    /// well-known name, respectively.
    #[deprecated(
        since = "4.1.0",
        note = "This variant is no longer returned from any of our API.\
                Use `Error::InvalidName` instead."
    )]
    InvalidBusName(String, String),
    /// Invalid well-known bus name.
    #[deprecated(
        since = "4.1.0",
        note = "This variant is no longer returned from any of our API.\
                Use `Error::InvalidName` instead."
    )]
    InvalidWellKnownName(String),
    /// Invalid unique bus name.
    #[deprecated(
        since = "4.1.0",
        note = "This variant is no longer returned from any of our API.\
                Use `Error::InvalidName` instead."
    )]
    InvalidUniqueName(String),
    /// Invalid interface name.
    #[deprecated(
        since = "4.1.0",
        note = "This variant is no longer returned from any of our API.\
                Use `Error::InvalidName` instead."
    )]
    InvalidInterfaceName(String),
    /// Invalid member (method or signal) name.
    #[deprecated(
        since = "4.1.0",
        note = "This variant is no longer returned from any of our API.\
                Use `Error::InvalidName` instead."
    )]
    InvalidMemberName(String),
    /// Invalid property name.
    #[deprecated(
        since = "4.1.0",
        note = "This variant is no longer returned from any of our API.\
                Use `Error::InvalidName` instead."
    )]
    InvalidPropertyName(String),
    /// Invalid error name.
    #[deprecated(
        since = "4.1.0",
        note = "This variant is no longer returned from any of our API.\
                Use `Error::InvalidName` instead."
    )]
    InvalidErrorName(String),
    /// An invalid name (legacy variant with static message).
    InvalidName(&'static str),
    /// An invalid D-Bus name with detailed error information.
    ///
    /// This variant provides specific information about why the name validation failed,
    /// including the type of name and the exact reason for failure.
    InvalidNameDetail {
        /// The type of name that was being validated.
        name_type: NameType,
        /// The specific reason why validation failed.
        reason: InvalidNameReason,
    },
    /// Invalid conversion from name type `from` to name type `to`.
    InvalidNameConversion {
        from: &'static str,
        to: &'static str,
    },
}

impl PartialEq for Error {
    #[allow(deprecated)]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::InvalidBusName(_, _), Self::InvalidBusName(_, _)) => true,
            (Self::InvalidWellKnownName(_), Self::InvalidWellKnownName(_)) => true,
            (Self::InvalidUniqueName(_), Self::InvalidUniqueName(_)) => true,
            (Self::InvalidInterfaceName(_), Self::InvalidInterfaceName(_)) => true,
            (Self::InvalidMemberName(_), Self::InvalidMemberName(_)) => true,
            (Self::InvalidPropertyName(_), Self::InvalidPropertyName(_)) => true,
            (Self::InvalidErrorName(_), Self::InvalidErrorName(_)) => true,
            (Self::InvalidName(_), Self::InvalidName(_)) => true,
            (
                Self::InvalidNameDetail {
                    name_type: t1,
                    reason: r1,
                },
                Self::InvalidNameDetail {
                    name_type: t2,
                    reason: r2,
                },
            ) => t1 == t2 && r1 == r2,
            (Self::InvalidNameConversion { .. }, Self::InvalidNameConversion { .. }) => true,
            (Self::Variant(s), Self::Variant(o)) => s == o,
            (_, _) => false,
        }
    }
}

impl error::Error for Error {
    #[allow(deprecated)]
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::InvalidBusName(_, _) => None,
            Error::InvalidWellKnownName(_) => None,
            Error::InvalidUniqueName(_) => None,
            Error::InvalidInterfaceName(_) => None,
            Error::InvalidErrorName(_) => None,
            Error::InvalidMemberName(_) => None,
            Error::InvalidPropertyName(_) => None,
            Error::InvalidName(_) => None,
            Error::InvalidNameDetail { .. } => None,
            Error::InvalidNameConversion { .. } => None,
            Error::Variant(e) => Some(e),
        }
    }
}

impl fmt::Display for Error {
    #[allow(deprecated)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Variant(e) => write!(f, "{e}"),
            Error::InvalidBusName(unique_err, well_known_err) => {
                write!(
                    f,
                    "Neither a valid unique ({unique_err}) nor a well-known ({well_known_err}) bus name"
                )
            }
            Error::InvalidWellKnownName(s) => write!(f, "Invalid well-known bus name: {s}"),
            Error::InvalidUniqueName(s) => write!(f, "Invalid unique bus name: {s}"),
            Error::InvalidInterfaceName(s) => write!(f, "Invalid interface or error name: {s}"),
            Error::InvalidErrorName(s) => write!(f, "Invalid interface or error name: {s}"),
            Error::InvalidMemberName(s) => write!(f, "Invalid method or signal name: {s}"),
            Error::InvalidPropertyName(s) => write!(f, "Invalid property name: {s}"),
            Error::InvalidName(s) => write!(f, "{s}"),
            Error::InvalidNameDetail { name_type, reason } => {
                write!(f, "Invalid {name_type}: {reason}")
            }
            Error::InvalidNameConversion { from, to } => {
                write!(f, "Invalid conversion from `{from}` to `{to}`")
            }
        }
    }
}

impl From<VariantError> for Error {
    fn from(val: VariantError) -> Self {
        Error::Variant(val)
    }
}

impl From<Infallible> for Error {
    fn from(i: Infallible) -> Self {
        match i {}
    }
}

/// Alias for a `Result` with the error type `zbus_names::Error`.
pub type Result<T> = std::result::Result<T, Error>;
