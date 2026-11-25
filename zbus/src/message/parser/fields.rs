//! The header fields: an array of `(code, variant)` structures, whose code tells the one type that
//! the variant can hold, or that it's a field the parser doesn't know and ignores after checking
//! that its variant is well formed.

use std::num::NonZeroU32;

use winnow::{
    Parser,
    combinator::{eof, preceded, repeat_till},
    stream::{Location, Stream},
    token::{any, take},
};
use zbus_utils::{
    names::{self, BusNameKind},
    object_path,
};

use super::{
    ByteOrder, Input, PResult, ParseError, QuickFields, body,
    quick_fields::{Field, FieldPos},
};
use crate::{Signature, message::FieldCode};

/// The header fields, up to the end of `input`, which is where the fields end.
pub(super) fn fields<O: ByteOrder>(input: &mut Input<'_>) -> PResult<QuickFields> {
    repeat_till(0.., preceded(padding, field::<O>), eof)
        .map(|(fields, _)| fields)
        .parse_next(input)
}

/// The padding before a field, up to the next 8-byte boundary from the start of the message, all of
/// whose bytes must be 0.
#[inline(always)]
fn padding(input: &mut Input<'_>) -> PResult<()> {
    let len = input.current_token_start().wrapping_neg() % 8;
    take(len)
        .try_map(
            |padding: &[u8]| match padding.iter().find(|byte| **byte != 0) {
                Some(byte) => Err(ParseError::PaddingNot0(*byte)),
                None => Ok(()),
            },
        )
        .parse_next(input)
}

/// A header field: its code and the signature of the variant that holds its value, then the value.
///
/// A field starts on an 8-byte boundary, and for every known field its code and the signature of
/// its variant take 4 bytes, so a `u32` value, or the length of a string, needs no padding before
/// it.
#[inline(always)]
fn field<O: ByteOrder>(input: &mut Input<'_>) -> PResult<Field> {
    let code = any(input)?;
    if code > UNIX_FDS {
        return unknown::<O>(input);
    }

    match (code, variant_signature(input)?) {
        (PATH, b'o') => path::<O>(input).map(Field::Path),
        (INTERFACE, b's') => name::<O, _>(names::validate_interface_name)
            .map(Field::Interface)
            .parse_next(input),
        (MEMBER, b's') => name::<O, _>(names::validate_member_name)
            .map(Field::Member)
            .parse_next(input),
        (ERROR_NAME, b's') => name::<O, _>(names::validate_error_name)
            .map(Field::ErrorName)
            .parse_next(input),
        (REPLY_SERIAL, b'u') => O::u32
            .try_map(reply_serial)
            .map(Field::ReplySerial)
            .parse_next(input),
        (DESTINATION, b's') => bus_name::<O>
            .map(|(pos, unique)| Field::Destination { pos, unique })
            .parse_next(input),
        (SENDER, b's') => name::<O, _>(names::validate_unique_name)
            .map(Field::Sender)
            .parse_next(input),
        (SIGNATURE, b'g') => body_signature(input),
        (UNIX_FDS, b'u') => O::u32.map(Field::UnixFds).parse_next(input),
        _ => Err(ParseError::Field),
    }
}

/// The signature of the variant that holds the value of a known field, which is a single type for
/// every known field: the code of that type.
#[inline(always)]
fn variant_signature(input: &mut Input<'_>) -> PResult<u8> {
    take(3usize)
        .try_map(|signature: &[u8]| match signature {
            [1, value_type, 0] => Ok(*value_type),
            _ => Err(ParseError::Field),
        })
        .parse_next(input)
}

/// The value of the path field: an object path.
#[inline(always)]
fn path<O: ByteOrder>(input: &mut Input<'_>) -> PResult<FieldPos> {
    string::<O>
        .try_map(|(pos, path)| match object_path::validate(path) {
            Ok(()) => Ok(pos),
            Err(_) => Err(ParseError::ObjectPath),
        })
        .parse_next(input)
}

/// The value of a field that holds a name, which `validate` checks.
#[inline(always)]
fn name<'i, O, V>(validate: V) -> impl Parser<Input<'i>, FieldPos, ParseError>
where
    O: ByteOrder,
    V: Fn(&[u8]) -> Result<(), &'static str>,
{
    string::<O>.try_map(move |(pos, name)| validate(name).map(|()| pos).map_err(ParseError::Name))
}

/// The value of the destination field: a bus name, and whether it's a unique name.
#[inline(always)]
fn bus_name<O: ByteOrder>(input: &mut Input<'_>) -> PResult<(FieldPos, bool)> {
    string::<O>
        .try_map(|(pos, name)| match names::validate_bus_name(name) {
            Ok(kind) => Ok((pos, kind == BusNameKind::Unique)),
            Err(reason) => Err(ParseError::Name(reason)),
        })
        .parse_next(input)
}

/// A string: its length, its bytes, and the 0 that ends it. Where it lies in the message, and its
/// bytes.
#[inline(always)]
fn string<'i, O: ByteOrder>(input: &mut Input<'i>) -> PResult<(FieldPos, &'i [u8])> {
    let len = O::u32(input)?;

    nul_terminated(input, len as usize)
}

/// The value of the reply serial field: the serial number of the message that this one replies
/// to, which can't be 0.
fn reply_serial(serial: u32) -> Result<NonZeroU32, ParseError> {
    NonZeroU32::new(serial).ok_or(ParseError::Field)
}

/// The value of the signature field: the signature of the body, of 255 bytes at most, as its length
/// takes a single byte, then the 0 that ends it.
#[inline(always)]
fn body_signature(input: &mut Input<'_>) -> PResult<Field> {
    let len = any(input)?;
    let (pos, signature) = nul_terminated(input, len.into())?;

    Signature::from_bytes(signature)
        .map(|signature| Field::signature(signature, pos))
        .map_err(|_| ParseError::Signature)
}

/// The next `len` bytes, then the 0 that ends them, as it does a string or a signature: where the
/// bytes lie in the message, and the bytes.
#[inline(always)]
fn nul_terminated<'i>(input: &mut Input<'i>, len: usize) -> PResult<(FieldPos, &'i [u8])> {
    // As `len` is less than the length of what's left, it's less than `usize::MAX`.
    if input.eof_offset() <= len {
        return Err(ParseError::Truncated);
    }
    let start = input.current_token_start();
    match input.next_slice(len + 1).split_last() {
        Some((0, bytes)) => Ok((FieldPos::new(start, start + len), bytes)),
        _ => Err(ParseError::Field),
    }
}

/// A field whose code is beyond `UNIX_FDS`, which the specification asks to ignore.
///
/// Its variant is checked to be well formed, and skipped so parsing goes on with the next field.
fn unknown<O: ByteOrder>(input: &mut Input<'_>) -> PResult<Field> {
    body::validate_variant::<O>(input, HEADER_FIELD_DEPTH, 0).map_err(ParseError::Variant)?;

    Ok(Field::Unknown)
}

/// The array of header fields `a(yv)` and the field structure `(yv)` are 2 containers around the
/// field's variant value.
const HEADER_FIELD_DEPTH: u8 = 2;

const PATH: u8 = FieldCode::Path as u8;
const INTERFACE: u8 = FieldCode::Interface as u8;
const MEMBER: u8 = FieldCode::Member as u8;
const ERROR_NAME: u8 = FieldCode::ErrorName as u8;
const REPLY_SERIAL: u8 = FieldCode::ReplySerial as u8;
const DESTINATION: u8 = FieldCode::Destination as u8;
const SENDER: u8 = FieldCode::Sender as u8;
const SIGNATURE: u8 = FieldCode::Signature as u8;
const UNIX_FDS: u8 = FieldCode::UnixFDs as u8;
