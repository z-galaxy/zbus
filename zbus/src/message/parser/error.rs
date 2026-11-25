//! Why parsing a header fails.

use winnow::error::{FromExternalError, ParserError};

use super::{Input, body};
use crate::{Error, message::EndianSig, wire};

/// Why parsing a header failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ParseError {
    /// The header ends past the end of the bytes.
    Truncated,
    /// The header is too large for where its fields lie to be kept.
    TooLarge,
    /// The message type is unknown.
    MessageType,
    /// The serial number is 0.
    Serial,
    /// A padding byte isn't 0.
    PaddingNot0(u8),
    /// A field has the invalid code 0, a value of the wrong type, or a string or a signature that
    /// a 0 doesn't end, comes more than once, or is a reply serial of 0.
    Field,
    /// A name is invalid, for the reason given.
    Name(&'static str),
    /// The path isn't a valid object path.
    ObjectPath,
    /// The signature of the body is invalid.
    Signature,
    /// The variant value of a field with an unknown code is invalid.
    Variant(body::Invalid),
}

impl ParserError<Input<'_>> for ParseError {
    type Inner = Self;

    /// The error of winnow's parsers that the parser uses, which only fail when the input ends:
    /// every other way to fail goes through `try_map`, and so through [`FromExternalError`].
    fn from_input(_input: &Input<'_>) -> Self {
        Self::Truncated
    }

    fn into_inner(self) -> std::result::Result<Self::Inner, Self> {
        Ok(self)
    }
}

impl FromExternalError<Input<'_>, ParseError> for ParseError {
    fn from_external_error(_input: &Input<'_>, error: ParseError) -> Self {
        error
    }
}

impl ParseError {
    /// The [`Error`] that this is, of `bytes` in the byte order of `endian_sig`.
    #[cold]
    pub(super) fn into_error(self, bytes: &[u8], endian_sig: EndianSig) -> Error {
        match self {
            Self::Truncated => Error::OutOfBounds,
            Self::TooLarge => Error::ExcessData,
            Self::MessageType | Self::Field => Error::InvalidField,
            Self::Serial => Error::InvalidSerial,
            Self::PaddingNot0(byte) => Error::PaddingNot0(byte),
            Self::Name(reason) => Error::InvalidName(reason),
            Self::ObjectPath => Error::InvalidObjectPath,
            Self::Signature => Error::SignatureParse(wire::signature::Error::InvalidSignature),
            Self::Variant(invalid) => invalid.into_error(bytes, endian_sig),
        }
    }
}
