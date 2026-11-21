use std::{convert::Infallible, error, fmt, io, sync::Arc};

use serde::{de, ser};

#[cfg(feature = "core")]
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
        /// The name of the name type that was converted from, such as `UniqueName`.
        from: &'static str,
        /// The name of the name type that was converted to, such as `WellKnownName`.
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
    ///
    /// An error reply whose name is defined by [`fdo::Error`] is reported as [`Error::FDO`]
    /// instead. This variant is for the names that are not.
    // According to the spec, there can be all kinds of details in D-Bus errors but nobody adds
    // anything more than a string description.
    #[cfg(feature = "core")]
    MethodError(OwnedErrorName, Option<String>, Message),
    /// A required field is missing in the message headers.
    MissingField,
    /// Invalid D-Bus GUID.
    InvalidGUID,
    /// Unsupported function, or support currently lacking.
    Unsupported,
    /// A [`fdo::Error`] transformed into [`Error`].
    ///
    /// This includes the D-Bus error replies whose name is defined by [`fdo::Error`], such as
    /// `org.freedesktop.DBus.Error.ServiceUnknown`, which becomes [`fdo::Error::ServiceUnknown`].
    #[cfg(feature = "core")]
    FDO(Box<fdo::Error>),
    /// A [`DBusError`](crate::DBusError) of any other type.
    ///
    /// This is how the failure of a property getter reaches the caller of the
    /// `PropertiesChanged` emission helper that `#[interface]` generates for the property.
    #[cfg(feature = "core")]
    DBus(Arc<dyn crate::DBusError + Send + Sync>),
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
    #[cfg(feature = "core")]
    Connection(
        /// The I/O error that caused the failure.
        Arc<io::Error>,
        /// The address that could not be connected to.
        Box<Address>,
    ),
    /// An error from zoter while handing a connection over to another process, such as a
    /// restarted service instance.
    ///
    /// This variant is only available on Linux, with the `handover` feature.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    Handover(Arc<zoter::Error>),
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
            #[cfg(feature = "core")]
            (Self::MethodError(_, _, _), Self::MethodError(_, _, _)) => true,
            (Self::MissingField, Self::MissingField) => true,
            (Self::InvalidGUID, Self::InvalidGUID) => true,
            (Self::Unsupported, Self::Unsupported) => true,
            #[cfg(feature = "core")]
            (Self::FDO(s), Self::FDO(o)) => s == o,
            #[cfg(feature = "core")]
            (Self::DBus(s), Self::DBus(o)) => {
                s.name() == o.name() && s.description() == o.description()
            }
            (Self::NameTaken, Self::NameTaken) => true,
            (Self::InvalidMatchRule, Self::InvalidMatchRule) => true,
            (Self::InvalidSerial, Self::InvalidSerial) => true,
            (Self::InterfaceExists(s1, s2), Self::InterfaceExists(o1, o2)) => s1 == o1 && s2 == o2,
            #[cfg(feature = "core")]
            (Self::Connection(_, a1), Self::Connection(_, a2)) => a1 == a2,
            // `InputOutput`, `Handover` and `MissingParameter` deliberately fall through. Two I/O
            // errors are never considered equal, and a zoter error may wrap one. The parameter
            // name is not a discriminator.
            (_, _) => false,
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::InputOutput(e) => Some(e),
            Error::Utf8(e) => Some(e),
            #[cfg(feature = "core")]
            Error::FDO(e) => Some(e),
            #[cfg(feature = "core")]
            Error::DBus(_) => None,
            #[cfg(feature = "core")]
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
            #[cfg(feature = "core")]
            Error::MethodError(_, _, _) => None,
            Error::MissingField => None,
            Error::InvalidGUID => None,
            Error::Unsupported => None,
            Error::NameTaken => None,
            Error::InvalidMatchRule => None,
            Error::MissingParameter(_) => None,
            Error::InvalidSerial => None,
            Error::InterfaceExists(_, _) => None,
            #[cfg(all(target_os = "linux", feature = "handover"))]
            Error::Handover(e) => Some(&**e),
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
            #[cfg(feature = "core")]
            Error::MethodError(name, detail, _reply) => write!(
                f,
                "{}: {}",
                **name,
                detail.as_ref().map(|s| s.as_str()).unwrap_or("no details")
            ),
            Error::MissingField => write!(f, "A required field is missing from message headers"),
            Error::InvalidGUID => write!(f, "Invalid GUID"),
            Error::Unsupported => write!(f, "Connection support is lacking"),
            #[cfg(feature = "core")]
            Error::FDO(e) => write!(f, "{e}"),
            #[cfg(feature = "core")]
            Error::DBus(e) => write!(f, "{e}"),
            Error::NameTaken => write!(f, "name already taken on the bus"),
            Error::InvalidMatchRule => write!(f, "Invalid match rule string"),
            Error::MissingParameter(p) => {
                write!(f, "Parameter `{p}` was not specified but it is required")
            }
            Error::InvalidSerial => write!(f, "Serial number in the message header is 0"),
            Error::InterfaceExists(i, p) => write!(f, "Interface `{i}` already exists at `{p}`"),
            #[cfg(feature = "core")]
            Error::Connection(e, addr) => write!(f, "failed to connect to `{addr}`: {e}"),
            #[cfg(all(target_os = "linux", feature = "handover"))]
            Error::Handover(e) => write!(f, "{e}"),
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
            #[cfg(feature = "core")]
            Error::MethodError(_, desc, _) => desc.as_deref(),
            #[cfg(feature = "core")]
            Error::DBus(e) => e.description(),
            Error::MissingField => Some("a required field is missing from message headers"),
            Error::InvalidGUID => Some("invalid GUID"),
            Error::Unsupported => Some("connection support is lacking"),
            #[cfg(feature = "core")]
            Error::FDO(_) => Some("FDO error"),
            Error::NameTaken => Some("name already taken on the bus"),
            Error::InvalidMatchRule => Some("invalid match rule string"),
            Error::MissingParameter(_) => Some("A required parameter is missing"),
            Error::InvalidSerial => Some("serial number in the message header is 0"),
            Error::InterfaceExists(_, _) => Some("interface already exists"),
            #[cfg(feature = "core")]
            Error::Connection(_, _) => Some("could not connect to specified address"),
            #[cfg(all(target_os = "linux", feature = "handover"))]
            Error::Handover(_) => Some("hand-over error"),
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

/// Convert any zoter error into [`Error::Handover`], including [`zoter::Error::Io`].
///
/// With this conversion, `?` works on zoter's results, such as the result of
/// [`zoter::Save::save`], in a function that returns a [`Result`]. A caller can then match on the
/// variants of [`zoter::Error`] to tell what failed.
///
/// This conversion is only available on Linux, with the `handover` feature.
#[cfg(all(target_os = "linux", feature = "handover"))]
impl From<zoter::Error> for Error {
    fn from(e: zoter::Error) -> Self {
        Error::Handover(Arc::new(e))
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

#[cfg(feature = "core")]
impl From<fdo::Error> for Error {
    fn from(val: fdo::Error) -> Self {
        match val {
            fdo::Error::ZBus(e) => e,
            e => Error::FDO(Box::new(e)),
        }
    }
}

// For messages that are D-Bus error returns
#[cfg(feature = "core")]
impl From<Message> for Error {
    fn from(message: Message) -> Error {
        // FIXME: Instead of checking this, we should have Method as trait and specific types for
        // each message type.
        let header = message.header();
        if header.primary().msg_type() != Type::Error {
            return Error::InvalidReply;
        }

        if let Some(name) = header.error_name() {
            let name: OwnedErrorName = name.to_owned().into();
            let detail: Option<String> = message
                .body()
                .deserialize_ignore_signature::<&str>()
                .ok()
                .map(String::from);
            let method_error = Error::MethodError(name, detail, message);

            // The conversion through `fdo::Error` is always attempted. A name defined by
            // `fdo::Error` becomes the matching `fdo::Error` variant, wrapped in `Error::FDO`. Any
            // other name falls through `fdo::Error::ZBus`, which `From<fdo::Error>` unwraps back
            // into the original `Error::MethodError`.
            Error::from(fdo::Error::from(method_error))
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

#[cfg(all(test, feature = "core"))]
mod tests {
    use std::sync::Arc;

    use crate::{
        DBusError, Error, fdo,
        message::{Header, Message},
        names::ErrorName,
    };

    #[test]
    fn error_reply_with_fdo_name() {
        let error = error_reply(
            "org.freedesktop.DBus.Error.ServiceUnknown",
            &"no such service",
        );

        assert_eq!(
            error,
            Error::FDO(Box::new(fdo::Error::ServiceUnknown(
                "no such service".to_string()
            ))),
        );
    }

    #[test]
    fn error_reply_with_fdo_name_and_no_description() {
        // An empty body has no description to deserialize.
        let error = error_reply("org.freedesktop.DBus.Error.ServiceUnknown", &());

        assert_eq!(
            error,
            Error::FDO(Box::new(fdo::Error::ServiceUnknown(String::new()))),
        );
    }

    #[test]
    fn error_reply_with_other_name() {
        let error = error_reply("org.example.Error.Oops", &"oops");

        let Error::MethodError(name, detail, _) = error else {
            panic!("expected `Error::MethodError`, got {error:?}");
        };
        assert_eq!(name.as_str(), "org.example.Error.Oops");
        assert_eq!(detail.as_deref(), Some("oops"));
    }

    // A method that passes on the error reply to a call of its own, with `?` into an `fdo::Result`
    // for one, replies with that error's name and description. `fdo::Error` holds an error of a
    // name it doesn't define in its catch-all variant.
    #[test]
    fn passed_on_error_reply_keeps_its_name() {
        let error = fdo::Error::from(error_reply("org.example.Error.Oops", &"oops"));
        assert!(
            matches!(error, fdo::Error::ZBus(Error::MethodError(..))),
            "{error:?}"
        );
        assert_reply(&error, "org.example.Error.Oops", "oops");
        assert_eq!(error.to_string(), "org.example.Error.Oops: oops");

        // So does an `fdo::Error` that an error of another domain holds in its catch-all variant.
        let error = Domain::from(error_reply(
            "org.freedesktop.DBus.Error.ServiceUnknown",
            &"no such service",
        ));
        assert!(matches!(error, Domain::ZBus(Error::FDO(_))), "{error:?}");
        assert_reply(
            &error,
            "org.freedesktop.DBus.Error.ServiceUnknown",
            "no such service",
        );

        // A D-Bus error of a type that isn't derived replies as it would on its own, body and all.
        let error = fdo::Error::from(Error::DBus(Arc::new(Detailed)));
        assert!(
            matches!(error, fdo::Error::ZBus(Error::DBus(_))),
            "{error:?}"
        );
        assert_eq!(error.name(), "org.example.Error.Detailed");
        let reply = error.create_reply(&method_call().header()).unwrap();
        let header = reply.header();
        assert_eq!(
            header.error_name().map(|name| name.as_str()),
            Some("org.example.Error.Detailed")
        );
        let body = reply.body();
        assert_eq!(body.deserialize::<(&str, u32)>().unwrap(), ("oops", 42));

        // An error of a domain's own converts into the variant of its name, not the catch-all one.
        let error = Domain::from(error_reply("org.example.Error.Oops", &"oops"));
        assert!(
            matches!(&error, Domain::Oops(description) if description == "oops"),
            "{error:?}"
        );

        // An error that holds no D-Bus error has no name of its own.
        let error = fdo::Error::ZBus(Error::InvalidReply);
        assert_eq!(error.name(), "org.freedesktop.zbus.Error");
        assert_eq!(error.description(), Some("invalid reply"));
    }

    /// A zoter error converts into `Error::Handover` unchanged, including a zoter I/O error. The
    /// result shows the zoter error's message, and returns the zoter error as its source.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    #[test]
    fn from_zoter_error() {
        use std::{error::Error as _, io};

        let error = Error::from(zoter::Error::NoStore);
        let Error::Handover(handover) = &error else {
            panic!("expected `Error::Handover`, got {error:?}");
        };
        assert!(matches!(**handover, zoter::Error::NoStore));
        assert_eq!(error.to_string(), zoter::Error::NoStore.to_string());
        let source = error
            .source()
            .and_then(|e| e.downcast_ref::<zoter::Error>());
        assert!(matches!(source, Some(zoter::Error::NoStore)), "{source:?}");

        let broken_pipe = io::Error::from(io::ErrorKind::BrokenPipe);
        let error = Error::from(zoter::Error::from(broken_pipe));
        let Error::Handover(handover) = &error else {
            panic!("expected `Error::Handover`, got {error:?}");
        };
        assert!(
            matches!(&**handover, zoter::Error::Io(e) if e.kind() == io::ErrorKind::BrokenPipe),
            "{handover:?}"
        );
    }

    fn error_reply<B>(name: &str, body: &B) -> Error
    where
        B: serde::Serialize + crate::DynamicType,
    {
        Message::error(&method_call().header(), name)
            .build(body)
            .unwrap()
            .into()
    }

    /// Check that `error` replies to a call under `name`, with `description` as its body.
    fn assert_reply<E>(error: &E, name: &str, description: &str)
    where
        E: DBusError,
    {
        assert_eq!(error.name(), name);
        assert_eq!(error.description(), Some(description));

        let reply = error.create_reply(&method_call().header()).unwrap();
        let header = reply.header();
        assert_eq!(header.error_name().map(|name| name.as_str()), Some(name));
        assert_eq!(reply.body().deserialize::<&str>().unwrap(), description);
    }

    fn method_call() -> Message {
        Message::method_call("/", "foo")
            .destination(":1.2")
            .build(&())
            .unwrap()
    }

    /// The errors of a domain other than `fdo::Error`'s.
    #[derive(Debug, crate::DBusError)]
    #[zbus(prefix = "org.example.Error")]
    enum Domain {
        #[zbus(error)]
        ZBus(Error),
        Oops(String),
    }

    /// A D-Bus error whose reply carries more than its description.
    struct Detailed;

    impl DBusError for Detailed {
        fn create_reply(&self, call: &Header<'_>) -> crate::Result<Message> {
            Message::error(call, self.name()).build(&("oops", 42u32))
        }

        fn name(&self) -> ErrorName<'_> {
            ErrorName::from_static_str_unchecked("org.example.Error.Detailed")
        }

        fn description(&self) -> Option<&str> {
            Some("oops")
        }
    }
}
