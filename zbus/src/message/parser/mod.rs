//! The parser of the header of a message.
//!
//! The header of a D-Bus message is a primary header of a fixed size, then the length of an array
//! of header fields, and the fields. Each field is a `(code, variant)` structure, whose code tells
//! which field it is, and so the one type that its variant can hold: an object path, a string, a
//! signature or a `u32`. That leaves the header simple enough to go through with a parser of its
//! own, in a single pass, checking each field once as it goes, and [`QuickFields`] keeps where
//! each string field lies in the message, so that the header can be read again without checking
//! its fields again.
//!
//! It accepts the headers that decoding a [`Header`](super::Header) through serde does, but for
//! those that libdbus rejects as well: one with a string or a signature that a 0 doesn't end, or
//! with a field that comes more than once, which serde takes the last value of. It also rejects one
//! that runs past the end of its bytes, which serde takes if all that's missing is the byte that
//! ends its last string. Like serde, it rejects a field with an unknown code, rather than ignoring
//! it as the specification asks, and it doesn't check the padding after the header. That padding is
//! checked as a message is received, along with the rest of what makes a message whole, such as
//! the fields that its type requires.
//!
//! It's made of winnow's combinators, but for the `u32`s, which are taken as 4 bytes and converted
//! at once: winnow's parsers of integers build them a byte at a time, which takes more
//! instructions.

mod body;
mod byte_order;
mod error;
mod fields;
mod primary_header;
mod quick_fields;
#[cfg(test)]
mod tests;

pub(crate) use body::validate as validate_body;
pub(crate) use quick_fields::QuickFields;

use winnow::{LocatingSlice, Parser, binary::length_and_then};

use crate::{
    Error, Result,
    message::{PrimaryHeader, header::MIN_MESSAGE_SIZE},
};
use byte_order::{BigEndian, ByteOrder, LittleEndian};
use error::ParseError;
use fields::fields;
use primary_header::primary_header;

/// Parse the header of the message in `bytes`: its primary header, its length, and its fields.
///
/// What follows the fields, from the padding before the body on, isn't looked at.
pub(crate) fn parse(bytes: &[u8]) -> Result<(PrimaryHeader, usize, QuickFields)> {
    let input = &mut Input::new(bytes);
    let header = match bytes.first() {
        Some(b'l') => header::<LittleEndian>(input),
        Some(b'B') => header::<BigEndian>(input),
        Some(_) => Err(ParseError::Endian),
        None => Err(ParseError::Truncated),
    };
    // A field that comes more than once makes the header invalid, as it does with libdbus, rather
    // than one of its values be taken for that of the field.
    if let Ok((_, _, fields)) = &header
        && fields.repeated()
    {
        return Err(ParseError::Field.into());
    }

    header.map_err(Error::from)
}

/// Parse the primary header at the start of `bytes`, and the length of the header fields after it.
#[cfg(feature = "comms")]
pub(super) fn parse_primary_header(bytes: &[u8]) -> Result<(PrimaryHeader, u32)> {
    let input = &mut Input::new(bytes);
    match bytes.first() {
        Some(b'l') => (primary_header::<LittleEndian>, LittleEndian::u32).parse_next(input),
        Some(b'B') => (primary_header::<BigEndian>, BigEndian::u32).parse_next(input),
        Some(_) => Err(ParseError::Endian),
        None => Err(ParseError::Truncated),
    }
    .map_err(Error::from)
}

/// The header of a message in byte order `O`: its primary header, then its fields, parsed from the
/// bytes up to where their length says they end, so that none can run past it.
fn header<O: ByteOrder>(input: &mut Input<'_>) -> PResult<(PrimaryHeader, usize, QuickFields)> {
    (
        primary_header::<O>,
        length_and_then(fields_len::<O>, fields::<O>),
    )
        .with_span()
        .map(|((primary_header, fields), span)| (primary_header, span.end, fields))
        .parse_next(input)
}

/// The length of the header fields, which may not have them end past what a `u32` holds, as where
/// each field lies is kept in `u32`s.
fn fields_len<O: ByteOrder>(input: &mut Input<'_>) -> PResult<u32> {
    O::u32
        .try_map(|len| match len.checked_add(MIN_MESSAGE_SIZE as u32) {
            Some(_) => Ok(len),
            None => Err(ParseError::TooLarge),
        })
        .parse_next(input)
}

/// The input of the parser: bytes from the start of a message, which knows its offset in them, for
/// the alignment of the fields and for where each lies.
type Input<'i> = LocatingSlice<&'i [u8]>;

/// A parser result, with [`ParseError`] as its error.
type PResult<T> = std::result::Result<T, ParseError>;
