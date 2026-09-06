use std::{convert::Infallible, error, fmt, io, sync::Arc};

use serde::{de, ser};

#[cfg(feature = "comms")]
use crate::{
    Address, fdo,
    message::{Message, Type},
    names::OwnedErrorName,
};
use crate::{ObjectPath, Signature, names::InterfaceName, wire};

/// The error type for `zbus`.
///
/// The various errors that can be reported by this crate.
#[derive(Clone, Debug)]
#[non_exhaustive]
#[allow(clippy::upper_case_acronyms)]
pub enum Error {
    /// Generic error. All serde errors get transformed into this variant.
    Failure(String),
    /// An I/O error.
    InputOutput(Arc<io::Error>),
    /// Type conversions errors.
    IncorrectType,
    /// Wrapper for [`std::str::Utf8Error`].
    Utf8(std::str::Utf8Error),
    /// Non-0 padding byte(s) encountered.
    PaddingNot0(u8),
    /// The deserialized file descriptor is not in the given FD index.
    UnknownFd,
    /// The provided signature (first argument) was not valid for reading as the requested type.
    /// Details on the expected signatures are in the second argument.
    SignatureMismatch(Signature, String),
    /// Out of bounds range specified.
    OutOfBounds,
    /// The maximum allowed depth for containers in encoding was exceeded.
    MaxDepthExceeded(MaxDepthExceeded),
    /// Error from parsing a signature.
    SignatureParse(wire::signature::Error),
    /// Attempted to create an empty structure (which is not allowed by the D-Bus specification).
    EmptyStructure,
    /// Invalid object path.
    InvalidObjectPath,
    /// An invalid name.
    InvalidName(&'static str),
    /// Invalid conversion from name type `from` to name type `to`.
    InvalidNameConversion {
        from: &'static str,
        to: &'static str,
    },
    /// Interface not found.
    InterfaceNotFound,
    /// Invalid D-Bus address.
    Address(String),
    /// Invalid message field.
    InvalidField,
    /// Data too large.
    ExcessData,
    /// Endian signature invalid or doesn't match expectation.
    IncorrectEndian,
    /// Initial handshake error.
    Handshake(String),
    /// Unexpected or incorrect reply.
    InvalidReply,
    /// A D-Bus method error reply.
    // According to the spec, there can be all kinds of details in D-Bus errors but nobody adds
    // anything more than a string description.
    #[cfg(feature = "comms")]
    MethodError(OwnedErrorName, Option<String>, Message),
    /// A required field is missing in the message headers.
    MissingField,
    /// Invalid D-Bus GUID.
    InvalidGUID,
    /// Unsupported function, or support currently lacking.
    Unsupported,
    /// A [`fdo::Error`] transformed into [`Error`].
    #[cfg(feature = "comms")]
    FDO(Box<fdo::Error>),
    /// The requested name was already claimed by another peer.
    NameTaken,
    /// Invalid [match rule][MR] string.
    ///
    /// [MR]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-bus-routing-match-rules
    InvalidMatchRule,
    /// A required parameter was missing.
    MissingParameter(&'static str),
    /// Serial number in the message header is 0 (which is invalid).
    InvalidSerial,
    /// The given interface already exists at the given path.
    InterfaceExists(InterfaceName<'static>, ObjectPath<'static>),
    /// Failed to connect to the D-Bus server at the given address.
    #[cfg(feature = "comms")]
    Connection(Arc<io::Error>, Box<Address>),
}

// The wire (de)serializers return this type by value out of deeply recursive calls, so its size
// is on a hot path.
const _: () = assert!(size_of::<Error>() <= 64);

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Failure(s1), Self::Failure(s2)) => s1 == s2,
            (Self::IncorrectType, Self::IncorrectType) => true,
            (Self::Utf8(s), Self::Utf8(o)) => s == o,
            (Self::PaddingNot0(s), Self::PaddingNot0(o)) => s == o,
            (Self::UnknownFd, Self::UnknownFd) => true,
            (Self::SignatureMismatch(p1, e1), Self::SignatureMismatch(p2, e2)) => {
                p1 == p2 && e1 == e2
            }
            (Self::OutOfBounds, Self::OutOfBounds) => true,
            (Self::MaxDepthExceeded(m1), Self::MaxDepthExceeded(m2)) => m1 == m2,
            (Self::SignatureParse(e1), Self::SignatureParse(e2)) => e1 == e2,
            (Self::EmptyStructure, Self::EmptyStructure) => true,
            (Self::InvalidObjectPath, Self::InvalidObjectPath) => true,
            (Self::InvalidName(_), Self::InvalidName(_)) => true,
            (Self::InvalidNameConversion { .. }, Self::InvalidNameConversion { .. }) => true,
            (Self::InterfaceNotFound, Self::InterfaceNotFound) => true,
            (Self::Address(_), Self::Address(_)) => true,
            (Self::InvalidField, Self::InvalidField) => true,
            (Self::ExcessData, Self::ExcessData) => true,
            (Self::IncorrectEndian, Self::IncorrectEndian) => true,
            (Self::Handshake(_), Self::Handshake(_)) => true,
            (Self::InvalidReply, Self::InvalidReply) => true,
            #[cfg(feature = "comms")]
            (Self::MethodError(_, _, _), Self::MethodError(_, _, _)) => true,
            (Self::MissingField, Self::MissingField) => true,
            (Self::InvalidGUID, Self::InvalidGUID) => true,
            (Self::Unsupported, Self::Unsupported) => true,
            #[cfg(feature = "comms")]
            (Self::FDO(s), Self::FDO(o)) => s == o,
            (Self::NameTaken, Self::NameTaken) => true,
            (Self::InvalidMatchRule, Self::InvalidMatchRule) => true,
            (Self::InvalidSerial, Self::InvalidSerial) => true,
            (Self::InterfaceExists(s1, s2), Self::InterfaceExists(o1, o2)) => s1 == o1 && s2 == o2,
            #[cfg(feature = "comms")]
            (Self::Connection(_, a1), Self::Connection(_, a2)) => a1 == a2,
            // `InputOutput` and `MissingParameter` deliberately fall through: two I/O errors are
            // never considered equal and the parameter name is not a discriminator.
            (_, _) => false,
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::InputOutput(e) => Some(e),
            Error::Utf8(e) => Some(e),
            #[cfg(feature = "comms")]
            Error::FDO(e) => Some(e),
            #[cfg(feature = "comms")]
            Error::Connection(e, _) => Some(e),
            Error::Failure(_) => None,
            Error::IncorrectType => None,
            Error::PaddingNot0(_) => None,
            Error::UnknownFd => None,
            Error::SignatureMismatch(_, _) => None,
            Error::OutOfBounds => None,
            Error::MaxDepthExceeded(_) => None,
            Error::SignatureParse(_) => None,
            Error::EmptyStructure => None,
            Error::InvalidObjectPath => None,
            Error::InvalidName(_) => None,
            Error::InvalidNameConversion { .. } => None,
            Error::InterfaceNotFound => None,
            Error::Address(_) => None,
            Error::InvalidField => None,
            Error::ExcessData => None,
            Error::IncorrectEndian => None,
            Error::Handshake(_) => None,
            Error::InvalidReply => None,
            #[cfg(feature = "comms")]
            Error::MethodError(_, _, _) => None,
            Error::MissingField => None,
            Error::InvalidGUID => None,
            Error::Unsupported => None,
            Error::NameTaken => None,
            Error::InvalidMatchRule => None,
            Error::MissingParameter(_) => None,
            Error::InvalidSerial => None,
            Error::InterfaceExists(_, _) => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Failure(e) => write!(f, "{e}"),
            Error::InputOutput(e) => write!(f, "I/O error: {e}"),
            Error::IncorrectType => write!(f, "incorrect type"),
            Error::Utf8(e) => write!(f, "{e}"),
            Error::PaddingNot0(b) => write!(f, "Unexpected non-0 padding byte `{b}`"),
            Error::UnknownFd => write!(f, "File descriptor not in the given FD index"),
            Error::SignatureMismatch(provided, expected) => write!(
                f,
                "Signature mismatch: got `{provided}`, expected {expected}",
            ),
            Error::OutOfBounds => write!(
                f,
                // FIXME: using the `Debug` impl of `Range` because it doesn't impl `Display`.
                "Out of bounds range specified",
            ),
            Error::MaxDepthExceeded(max) => write!(f, "{max}"),
            Error::SignatureParse(e) => write!(f, "{e}"),
            Error::EmptyStructure => write!(f, "Attempted to create an empty structure"),
            Error::InvalidObjectPath => write!(f, "Invalid object path"),
            Error::InvalidName(s) => write!(f, "{s}"),
            Error::InvalidNameConversion { from, to } => {
                write!(f, "Invalid conversion from `{from}` to `{to}`")
            }
            Error::InterfaceNotFound => write!(f, "Interface not found"),
            Error::Address(e) => write!(f, "address error: {e}"),
            Error::InvalidField => write!(f, "invalid message field"),
            Error::ExcessData => write!(f, "excess data"),
            Error::IncorrectEndian => write!(f, "incorrect endian"),
            Error::Handshake(e) => write!(f, "D-Bus handshake failed: {e}"),
            Error::InvalidReply => write!(f, "Invalid D-Bus method reply"),
            #[cfg(feature = "comms")]
            Error::MethodError(name, detail, _reply) => write!(
                f,
                "{}: {}",
                **name,
                detail.as_ref().map(|s| s.as_str()).unwrap_or("no details")
            ),
            Error::MissingField => write!(f, "A required field is missing from message headers"),
            Error::InvalidGUID => write!(f, "Invalid GUID"),
            Error::Unsupported => write!(f, "Connection support is lacking"),
            #[cfg(feature = "comms")]
            Error::FDO(e) => write!(f, "{e}"),
            Error::NameTaken => write!(f, "name already taken on the bus"),
            Error::InvalidMatchRule => write!(f, "Invalid match rule string"),
            Error::MissingParameter(p) => {
                write!(f, "Parameter `{p}` was not specified but it is required")
            }
            Error::InvalidSerial => write!(f, "Serial number in the message header is 0"),
            Error::InterfaceExists(i, p) => write!(f, "Interface `{i}` already exists at `{p}`"),
            #[cfg(feature = "comms")]
            Error::Connection(e, addr) => write!(f, "Failed to connect to address `{addr}`: {e}"),
        }
    }
}

impl Error {
    /// A [`SignatureMismatch`](Error::SignatureMismatch) for `signature` where `expected` was
    /// needed.
    ///
    /// The (de)serializers hit this from generic code. Building the error here keeps that error
    /// path out of every instantiation.
    #[cold]
    pub(crate) fn signature_mismatch(signature: &Signature, expected: &str) -> Self {
        Self::SignatureMismatch(signature.clone(), expected.to_string())
    }

    /// A description of the error.
    ///
    /// This is a generic description of the error (if any). For a more detailed description
    /// make use of the [`std::fmt::Display`] implementation, for example, through
    /// [`std::string::ToString`].
    pub fn description(&self) -> Option<&str> {
        match self {
            Error::Failure(e) => Some(e),
            Error::InputOutput(_) => Some("i/o error"),
            Error::IncorrectType => Some("incorrect type"),
            Error::Utf8(_) => Some("invalid UTF-8"),
            Error::PaddingNot0(_) => Some("unexpected non-0 padding byte"),
            Error::UnknownFd => Some("file descriptor not in the given FD index"),
            Error::SignatureMismatch(_, _) => Some("signature mismatch"),
            Error::OutOfBounds => Some("out of bounds range specified"),
            Error::MaxDepthExceeded(_) => Some("maximum allowed container depth exceeded"),
            Error::SignatureParse(_) => Some("invalid signature"),
            Error::EmptyStructure => Some("attempted to create an empty structure"),
            Error::InvalidObjectPath => Some("invalid object path"),
            Error::InvalidName(s) => Some(s),
            Error::InvalidNameConversion { .. } => Some("invalid name conversion"),
            Error::InterfaceNotFound => Some("interface not found"),
            Error::Address(e) => Some(e),
            Error::InvalidField => Some("invalid field"),
            Error::ExcessData => Some("excess data"),
            Error::IncorrectEndian => Some("incorrect endian"),
            Error::Handshake(e) => Some(e),
            Error::InvalidReply => Some("invalid reply"),
            #[cfg(feature = "comms")]
            Error::MethodError(_, desc, _) => desc.as_deref(),
            Error::MissingField => Some("a required field is missing from message headers"),
            Error::InvalidGUID => Some("invalid GUID"),
            Error::Unsupported => Some("connection support is lacking"),
            #[cfg(feature = "comms")]
            Error::FDO(_) => Some("FDO error"),
            Error::NameTaken => Some("name already taken on the bus"),
            Error::InvalidMatchRule => Some("invalid match rule string"),
            Error::MissingParameter(_) => Some("A required parameter is missing"),
            Error::InvalidSerial => Some("serial number in the message header is 0"),
            Error::InterfaceExists(_, _) => Some("interface already exists"),
            #[cfg(feature = "comms")]
            Error::Connection(_, _) => Some("could not connect to specified address"),
        }
    }
}

impl de::Error for Error {
    // TODO: Add more specific error variants to Error enum above so we can implement other methods
    // here too.
    fn custom<T>(msg: T) -> Error
    where
        T: fmt::Display,
    {
        Error::Failure(msg.to_string())
    }
}

impl ser::Error for Error {
    fn custom<T>(msg: T) -> Error
    where
        T: fmt::Display,
    {
        Error::Failure(msg.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(val: io::Error) -> Self {
        Error::InputOutput(Arc::new(val))
    }
}

impl From<wire::signature::Error> for Error {
    fn from(e: wire::signature::Error) -> Self {
        Error::SignatureParse(e)
    }
}

impl From<Infallible> for Error {
    fn from(i: Infallible) -> Self {
        match i {}
    }
}

#[cfg(feature = "comms")]
impl From<fdo::Error> for Error {
    fn from(val: fdo::Error) -> Self {
        match val {
            fdo::Error::ZBus(e) => e,
            e => Error::FDO(Box::new(e)),
        }
    }
}

// For messages that are D-Bus error returns
#[cfg(feature = "comms")]
impl From<Message> for Error {
    fn from(message: Message) -> Error {
        // FIXME: Instead of checking this, we should have Method as trait and specific types for
        // each message type.
        let header = message.header();
        if header.primary().msg_type() != Type::Error {
            return Error::InvalidReply;
        }

        if let Some(name) = header.error_name() {
            let name = name.to_owned().into();
            match message.body().deserialize_unchecked::<&str>() {
                Ok(detail) => Error::MethodError(name, Some(String::from(detail)), message),
                Err(_) => Error::MethodError(name, None, message),
            }
        } else {
            Error::InvalidReply
        }
    }
}

/// Enum representing the max depth exceeded error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxDepthExceeded {
    /// The maximum allowed depth for structures in encoding was exceeded.
    Structure,
    /// The maximum allowed depth for arrays in encoding was exceeded.
    Array,
    /// The maximum allowed depth for containers in encoding was exceeded.
    Container,
}

impl fmt::Display for MaxDepthExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Structure => write!(
                f,
                "Maximum allowed depth for structures in encoding was exceeded"
            ),
            Self::Array => write!(
                f,
                "Maximum allowed depth for arrays in encoding was exceeded"
            ),
            Self::Container => write!(
                f,
                "Maximum allowed depth for containers in encoding was exceeded"
            ),
        }
    }
}

/// Alias for a `Result` with the error type `zbus::Error`.
pub type Result<T> = std::result::Result<T, Error>;
