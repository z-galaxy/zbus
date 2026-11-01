//! The parser of the body of a message, which checks that the body holds values of the types of its
//! signature, as the D-Bus format lays them out, without deserializing them.
//!
//! A bus passes the bodies of messages on as they are, so this is all that checks them before
//! their recipients do. It goes over the values in the order of the signature, with the signature
//! as it's written: a variant carries the signature of its value, which is then only checked,
//! rather than parsed, and nothing is allocated. An array of numbers of a fixed size, but for
//! booleans, is skipped in one step, so that a byte array costs no more than a number does.
//!
//! Like the parser of the header, it's compiled once for each byte order, and its error is small,
//! so that what each step returns fits in registers: a value of [`Error`] is made of the error only
//! once the parser fails. It goes through the same input as the parser of the header, winnow's
//! `LocatingSlice`, which knows where in the body it is, for the alignment of each value, but it
//! takes bytes off that input through the `Stream` trait itself, rather than with winnow's `take`,
//! which the compiler doesn't inline. A value of a basic type, or a variant of one, as most are, is
//! checked wherever it is, without a call of a function of its own, as is each entry of a dict, and
//! the bytes of a string are checked 8 at a time for ASCII, as most strings are.

use std::str;

use serde::de::{Error as _, Unexpected};
use winnow::stream::{Location, Stream};
use zbus_utils::{object_path, signature};

use super::{
    Input,
    byte_order::{BigEndian, ByteOrder, LittleEndian},
};
use crate::{Error, MaxDepthExceeded, Result, message::EndianSig};

/// Check that `body`, from its first byte to its last, holds values of the types of `signature` in
/// turn, in the byte order of `endian_sig`, with `fds` file descriptors for those among them to
/// refer to.
///
/// `signature` must be a valid one, as that of a message is once its header is parsed. `body` must
/// start on an 8-byte boundary of its message, as a body does, as the alignment of each value is
/// reckoned from there.
pub(crate) fn validate(
    body: &[u8],
    signature: &[u8],
    endian_sig: EndianSig,
    fds: u32,
) -> Result<()> {
    // A body of no values, as that of many a call and many a reply is, needs no parser.
    if signature.is_empty() {
        return match body.is_empty() {
            true => Ok(()),
            false => Err(Error::ExcessData),
        };
    }

    match endian_sig {
        EndianSig::Little => values::<LittleEndian>(body, signature, fds),
        EndianSig::Big => values::<BigEndian>(body, signature, fds),
    }
    .map_err(|invalid| invalid.into_error(body, endian_sig))
}

/// Values of the types of `signature` in turn, in byte order `O`, which fill `body`.
fn values<O: ByteOrder>(body: &[u8], mut signature: &[u8], fds: u32) -> Check<()> {
    let input = &mut Input::new(body);
    while !signature.is_empty() {
        value::<O>(input, &mut signature, 0, fds)?;
    }

    if input.eof_offset() == 0 {
        Ok(())
    } else {
        Err(Invalid::ExcessData)
    }
}

/// A value of the first complete type of `signature`, which the value takes off `signature`, in
/// `depth` containers.
///
/// Most values are of basic types, or variants of those, which are checked right here, wherever a
/// value is: only a value of a container, or a variant of one, takes a call of a function of its
/// own.
#[inline(always)]
fn value<O: ByteOrder>(
    input: &mut Input<'_>,
    signature: &mut &[u8],
    depth: u8,
    fds: u32,
) -> Check<()> {
    let Some((&code, rest)) = signature.split_first() else {
        return Err(Invalid::Signature);
    };
    match code {
        b'v' => {
            let depth = deeper(depth)?;
            match signature_value(input)? {
                [code] if *code != b'v' => basic::<O>(input, *code, fds)?,
                signature => variant::<O>(input, signature, depth, fds)?,
            }
        }
        // An array of numbers of a fixed size, as most of the bulk of bodies is, takes no more
        // than its length and its padding. Its elements are a level deeper than it, even if there
        // are none, as GLib has them.
        b'a' => match rest.first().and_then(|code| fixed_size(*code)) {
            Some(size) => {
                deeper(depth)?;
                fixed_array::<O>(input, size)?;
                *signature = &rest[1..];

                return Ok(());
            }
            None => return container::<O>(input, signature, depth, fds),
        },
        b'(' => return container::<O>(input, signature, depth, fds),
        _ => basic::<O>(input, code, fds)?,
    }
    *signature = rest;

    Ok(())
}

/// A value of the basic type whose code is `code`.
#[inline(always)]
fn basic<O: ByteOrder>(input: &mut Input<'_>, code: u8, fds: u32) -> Check<()> {
    match code {
        b'y' => bytes(input, 1).map(|_| ()),
        b'b' => match u32::<O>(input)? {
            0 | 1 => Ok(()),
            value => Err(Invalid::Bool(value)),
        },
        b'n' | b'q' => fixed(input, 2),
        b'i' | b'u' => fixed(input, 4),
        // A file descriptor is the index of one among those that the message brought.
        b'h' => match u32::<O>(input)? < fds {
            true => Ok(()),
            false => Err(Invalid::Fd),
        },
        b'x' | b't' | b'd' => fixed(input, 8),
        b's' => {
            let string = string::<O>(input)?;
            if is_ascii_without_nul(string) || is_str(string) {
                return Ok(());
            }
            // Its bytes are followed by the 0 that ends it. A message is 128 MiB long at most.
            let start = input.current_token_start() - string.len() - 1;

            Err(Invalid::Str(start as u32))
        }
        b'o' => {
            // A path is made of ASCII letters, digits, `_` and `/` only, so not of a 0.
            let path = string::<O>(input)?;
            object_path::validate(path).map_err(|_| Invalid::ObjectPath)
        }
        b'g' => signature_value(input).map(|_| ()),
        _ => Err(Invalid::Signature),
    }
}

/// A value of the first complete type of `signature`, if that's an array or a structure, which the
/// value takes off `signature`, in `depth` containers.
fn container<O: ByteOrder>(
    input: &mut Input<'_>,
    signature: &mut &[u8],
    depth: u8,
    fds: u32,
) -> Check<()> {
    let Some((&code, rest)) = signature.split_first() else {
        return Err(Invalid::Signature);
    };
    *signature = rest;
    match code {
        b'a' => array::<O>(input, signature, depth, fds),
        b'(' => structure::<O>(input, signature, deeper(depth)?, fds),
        // A dict entry is only ever an element of an array, which `array` checks.
        _ => Err(Invalid::Signature),
    }
}

/// An array, in `depth` containers, of the first complete type of `signature`, which follows the
/// `a` of the array, and which the array takes off `signature`.
fn array<O: ByteOrder>(
    input: &mut Input<'_>,
    signature: &mut &[u8],
    depth: u8,
    fds: u32,
) -> Check<()> {
    let len = u32::<O>(input)?;
    if len > MAX_ARRAY_LEN {
        return Err(Invalid::ArrayLen(len));
    }
    let element_len = match signature.first() {
        Some(code) if is_basic(*code) => 1,
        _ => complete_type_len(signature)?,
    };
    let (element, rest) = signature.split_at(element_len);
    *signature = rest;
    // The padding before the first element is there even if the array is empty.
    align(input, alignment(element[0]))?;
    let end = input.current_token_start() + len as usize;
    // The elements are a level deeper than the array, but for those of an empty one, of which there
    // are none, unless they're file descriptors, which GLib takes for numbers of a fixed size, and
    // holds a level deeper even then.
    let depth = match element[0] {
        b'h' => deeper(depth)?,
        _ if len == 0 => depth,
        _ => deeper(depth)?,
    };
    match element.split_first() {
        // Dict entries, as those of the dicts that most bodies hold, are checked right here.
        Some((b'{', entry)) => {
            while input.current_token_start() < end {
                dict_entry::<O>(input, entry, deeper(depth)?, fds)?;
            }
        }
        _ => {
            while input.current_token_start() < end {
                value::<O>(input, &mut { element }, depth, fds)?;
            }
        }
    }

    // The last element may not run past the end of the array.
    match input.current_token_start() == end {
        true => Ok(()),
        false => Err(Invalid::OutOfBounds),
    }
}

/// An array of numbers of `size` bytes, which follows the `a` of the array: its length, the padding
/// before its first element, which is there even if the array is empty, and its elements.
#[inline(always)]
fn fixed_array<O: ByteOrder>(input: &mut Input<'_>, size: u32) -> Check<()> {
    let len = u32::<O>(input)?;
    // The size is a power of 2.
    if len > MAX_ARRAY_LEN || len & (size - 1) != 0 {
        return Err(Invalid::ArrayLen(len));
    }
    align(input, size as usize)?;

    bytes(input, len as usize).map(|_| ())
}

/// A structure of the fields that `signature` starts with, up to the `)` that ends them, which
/// follow the `(` of the structure, and which the structure takes off `signature`. The fields are
/// in `depth` containers, the structure included.
fn structure<O: ByteOrder>(
    input: &mut Input<'_>,
    signature: &mut &[u8],
    depth: u8,
    fds: u32,
) -> Check<()> {
    align(input, 8)?;
    loop {
        match signature.split_first() {
            Some((b')', rest)) => {
                *signature = rest;

                return Ok(());
            }
            Some(_) => value::<O>(input, signature, depth, fds)?,
            None => return Err(Invalid::Signature),
        }
    }
}

/// A dict entry of the types of `entry`: those of its key and its value, then the `}` that ends
/// them, which follow the `{` of the entry. The key and the value are in `depth` containers, the
/// entry included.
#[inline(always)]
fn dict_entry<O: ByteOrder>(input: &mut Input<'_>, entry: &[u8], depth: u8, fds: u32) -> Check<()> {
    // The key is of a basic type, as a valid signature has it.
    let Some((&key, mut value_signature)) = entry.split_first() else {
        return Err(Invalid::Signature);
    };
    align(input, 8)?;
    basic::<O>(input, key, fds)?;
    value::<O>(input, &mut value_signature, depth, fds)?;
    match value_signature {
        b"}" => Ok(()),
        _ => Err(Invalid::Signature),
    }
}

/// The value of a variant whose signature is `signature`, unless that's not a single complete
/// type, in `depth` containers, the variant included.
fn variant<O: ByteOrder>(
    input: &mut Input<'_>,
    mut signature: &[u8],
    depth: u8,
    fds: u32,
) -> Check<()> {
    // A valid signature of a single code is that of a single complete type, as is one of 2 that
    // starts with an `a`, of an array of the type of the other.
    match signature {
        [] => return Err(Invalid::Signature),
        [_] | [b'a', _] => (),
        _ if complete_type_len(signature)? != signature.len() => return Err(Invalid::Signature),
        _ => (),
    }
    // Any value that the type could hold, even in an array that's empty, is in no more containers
    // than a value can be in, as GLib has it. A type is no deeper than its signature is long, which
    // most are too short to come near the limit with.
    let max = usize::from(MAX_DEPTH) + 1;
    if usize::from(depth) + signature.len() > max
        && usize::from(depth) + type_depth(signature)?.0 > max
    {
        return Err(Invalid::Depth);
    }

    value::<O>(input, &mut signature, depth, fds)
}

/// The depth of the values in a container that is in `depth` containers, unless it's more than a
/// value can be in.
///
/// The specification has the signature of the body, and that of each variant, hold no more than 32
/// arrays and 32 structures nested in each other, as their parser checks, and a value be in no
/// more than 64 containers in all, variants and dict entries included. libdbus counts the values
/// that a body holds, but for the elements of arrays of a fixed size, which it doesn't look into.
/// GLib counts those too, and all the values that the type of a variant could hold, even in an
/// empty array, so this counts them as GLib does, which rejects all that libdbus rejects.
#[inline(always)]
fn deeper(depth: u8) -> Check<u8> {
    match depth < MAX_DEPTH {
        true => Ok(depth + 1),
        false => Err(Invalid::Depth),
    }
}

/// A signature, as a `g` and a `v` hold it: its length, in a byte, its codes, and a 0.
#[inline(always)]
fn signature_value<'i>(input: &mut Input<'i>) -> Check<&'i [u8]> {
    let len = bytes(input, 1)?[0];
    let signature = terminated(input, len.into())?;
    // Most signatures in a body are those of variants of a basic type, or of an array of one,
    // which need no parser.
    match signature {
        [code] | [b'a', code] if is_basic(*code) => Ok(signature),
        _ => complex_signature(signature).map(|_| signature),
    }
}

/// Check `signature`, of a `g` or a `v`, which isn't of a basic type, nor of an array of one.
fn complex_signature(signature: &[u8]) -> Check<()> {
    // No type has 0 for its code, so the parser rejects a signature with a 0 in it.
    signature::validate(signature).map_err(|_| Invalid::Signature)
}

/// A string, as an `s` and an `o` hold it: its length, in a `u32`, its bytes, and a 0. Its bytes.
#[inline(always)]
fn string<'i, O: ByteOrder>(input: &mut Input<'i>) -> Check<&'i [u8]> {
    let len = u32::<O>(input)?;

    terminated(input, len as usize)
}

/// The next `len` bytes, then the 0 that ends them, as it does a string or a signature.
#[inline(always)]
fn terminated<'i>(input: &mut Input<'i>, len: usize) -> Check<&'i [u8]> {
    // As `len` is less than the length of what's left, it's less than `usize::MAX`.
    if input.eof_offset() <= len {
        return Err(Invalid::OutOfBounds);
    }
    let (end, bytes) = input
        .next_slice(len + 1)
        .split_last()
        .expect("`len + 1` bytes");
    match end {
        0 => Ok(bytes),
        byte => Err(Invalid::StringEnd(*byte)),
    }
}

/// A `u32`, in byte order `O`, after the padding before it.
#[inline(always)]
fn u32<O: ByteOrder>(input: &mut Input<'_>) -> Check<u32> {
    align(input, 4)?;
    let bytes = bytes(input, 4)?;

    Ok(O::u32_from_bytes(
        bytes.try_into().expect("`bytes` returns 4 bytes"),
    ))
}

/// A value of `size` bytes, after the padding before it, up to a multiple of `size`.
#[inline(always)]
fn fixed(input: &mut Input<'_>, size: usize) -> Check<()> {
    align(input, size)?;

    bytes(input, size).map(|_| ())
}

/// The padding up to the next multiple of `alignment`, a power of 2, from the start of the body,
/// all of whose bytes must be 0.
#[inline(always)]
fn align(input: &mut Input<'_>, alignment: usize) -> Check<()> {
    let len = input.current_token_start().wrapping_neg() & (alignment - 1);
    // Most values are aligned already.
    if len == 0 {
        return Ok(());
    }
    match bytes(input, len)?.iter().find(|byte| **byte != 0) {
        Some(byte) => Err(Invalid::PaddingNot0(*byte)),
        None => Ok(()),
    }
}

/// The next `len` bytes.
#[inline(always)]
fn bytes<'i>(input: &mut Input<'i>, len: usize) -> Check<&'i [u8]> {
    if input.eof_offset() < len {
        return Err(Invalid::OutOfBounds);
    }

    Ok(input.next_slice(len))
}

/// Whether `bytes` are all ASCII but 0, as those of most strings are, which makes them UTF-8
/// without a 0 in it.
#[inline(always)]
fn is_ascii_without_nul(bytes: &[u8]) -> bool {
    // The bytes are looked at 8 at a time, in a `u64`. A word holds a byte that is 0, or above
    // 0x7f, if and only if a top bit of a byte is set in the word, or in the word less 1 in each
    // byte: a byte above 0x7f has its own set, and the lowest byte that is 0 turns into 0xff, as
    // the bytes below it, of 1 to 0x7f, don't borrow from it. With bytes of 1 to 0x7f only, none
    // borrows.
    const ONES: u64 = u64::from_ne_bytes([0x01; 8]);
    const TOPS: u64 = u64::from_ne_bytes([0x80; 8]);
    let tops_of = |word: u64| word.wrapping_sub(ONES) | word;
    let len = bytes.len();
    let u32_at = |i: usize| u64::from(u32::from_ne_bytes(bytes[i..i + 4].try_into().unwrap()));
    let tops = match len {
        0 => return true,
        // The bytes, some of them twice, in a word whose other bytes are 1.
        1..=3 => tops_of(
            u64::from(bytes[0])
                | u64::from(bytes[len / 2]) << 8
                | u64::from(bytes[len - 1]) << 16
                | ONES << 24,
        ),
        // The first 4 bytes and the last 4, which overlap unless there are 8.
        4..=8 => tops_of(u32_at(0) | u32_at(len - 4) << 32),
        // The words, and the last 8 bytes, which overlap the last word unless it ends the bytes.
        _ => {
            let (words, _) = bytes.as_chunks::<8>();
            let last = bytes.last_chunk::<8>().expect("more than 8 bytes");
            words
                .iter()
                .fold(tops_of(u64::from_ne_bytes(*last)), |tops, word| {
                    tops | tops_of(u64::from_ne_bytes(*word))
                })
        }
    };

    tops & TOPS == 0
}

/// Whether `string` is UTF-8, without a 0 in it.
fn is_str(string: &[u8]) -> bool {
    !string.contains(&0) && str::from_utf8(string).is_ok()
}

/// Whether `code` is that of a basic type, or of a variant: a complete type of a single code.
fn is_basic(code: u8) -> bool {
    match code {
        b'y' | b'b' | b'n' | b'q' | b'i' | b'u' | b'x' | b't' | b'd' | b's' | b'o' | b'g'
        | b'v' => true,
        // A file descriptor isn't a type but on Unix.
        b'h' => cfg!(unix),
        _ => false,
    }
}

/// How many levels deep the values of the first complete type of `signature` go, and its length.
///
/// A basic type and a variant go 1 level deep, and a container 1 more than its deepest child.
fn type_depth(signature: &[u8]) -> Check<(usize, usize)> {
    match signature.first() {
        Some(b'a') => {
            let (depth, len) = type_depth(&signature[1..])?;

            Ok((depth + 1, len + 1))
        }
        Some(b'(' | b'{') => {
            let (mut depth, mut len) = (0, 1);
            loop {
                match signature.get(len) {
                    Some(b')' | b'}') => return Ok((depth + 1, len + 1)),
                    Some(_) => {
                        let (child_depth, child_len) = type_depth(&signature[len..])?;
                        depth = depth.max(child_depth);
                        len += child_len;
                    }
                    None => return Err(Invalid::Signature),
                }
            }
        }
        Some(_) => Ok((1, 1)),
        None => Err(Invalid::Signature),
    }
}

/// The length of the first complete type of `signature`.
fn complete_type_len(signature: &[u8]) -> Check<usize> {
    match signature.first() {
        Some(b'a') => Ok(1 + complete_type_len(&signature[1..])?),
        Some(b'(' | b'{') => {
            let mut len = 1;
            loop {
                match signature.get(len) {
                    Some(b')' | b'}') => return Ok(len + 1),
                    Some(_) => len += complete_type_len(&signature[len..])?,
                    None => return Err(Invalid::Signature),
                }
            }
        }
        Some(_) => Ok(1),
        None => Err(Invalid::Signature),
    }
}

/// The alignment of a value of the type whose code is `code`.
fn alignment(code: u8) -> usize {
    match code {
        b'y' | b'g' | b'v' => 1,
        b'n' | b'q' => 2,
        b'x' | b't' | b'd' | b'(' | b'{' => 8,
        _ => 4,
    }
}

/// The size of a value of the type whose code is `code`, if all of its values are of that size and
/// any bytes of that size are a valid one: those of the numbers, but for booleans.
fn fixed_size(code: u8) -> Option<u32> {
    match code {
        b'y' => Some(1),
        b'n' | b'q' => Some(2),
        b'i' | b'u' => Some(4),
        b'x' | b't' | b'd' => Some(8),
        _ => None,
    }
}

/// What a step of the parser returns.
type Check<T> = std::result::Result<T, Invalid>;

/// Why a body is invalid, as the parser finds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Invalid {
    /// The values run past the end of the body, or of the array that they're in.
    OutOfBounds,
    /// Bytes are left after the values.
    ExcessData,
    /// A padding byte isn't 0.
    PaddingNot0(u8),
    /// A boolean is neither 0 nor 1.
    Bool(u32),
    /// A string or a signature doesn't end with a 0, but with this byte.
    StringEnd(u8),
    /// The string whose bytes start here, in the body, isn't UTF-8 without a 0 in it.
    Str(u32),
    /// An object path isn't a valid one.
    ObjectPath,
    /// A signature isn't a valid one, or that of a variant isn't a single complete type.
    Signature,
    /// A file descriptor isn't one of those that the message brought.
    Fd,
    /// An array of this length is longer than 64 MiB, or its elements can't fill it.
    ArrayLen(u32),
    /// A value is in more containers than it can be in.
    Depth,
}

impl Invalid {
    /// The error that this is, of `body`, in the byte order of `endian_sig`.
    #[cold]
    fn into_error(self, body: &[u8], endian_sig: EndianSig) -> Error {
        match self {
            Self::OutOfBounds => Error::OutOfBounds,
            Self::ExcessData => Error::ExcessData,
            Self::PaddingNot0(byte) => Error::PaddingNot0(byte),
            Self::Bool(value) => {
                Error::invalid_value(Unexpected::Unsigned(value.into()), &"0 or 1")
            }
            Self::StringEnd(byte) => Error::invalid_value(
                Unexpected::Unsigned(byte.into()),
                &"the 0 that ends a string",
            ),
            Self::Str(start) => {
                // The length of the string comes right before its bytes.
                let start = start as usize;
                let len: [u8; 4] = body[start - 4..start].try_into().expect("4 bytes");
                let len = match endian_sig {
                    EndianSig::Little => LittleEndian::u32_from_bytes(len),
                    EndianSig::Big => BigEndian::u32_from_bytes(len),
                };
                match str::from_utf8(&body[start..start + len as usize]) {
                    Ok(_) => {
                        Error::invalid_value(Unexpected::Char('\0'), &"a string without a 0 in it")
                    }
                    Err(e) => Error::Utf8(e),
                }
            }
            Self::ObjectPath => Error::InvalidObjectPath,
            Self::Signature => Error::SignatureParse(signature::Error::InvalidSignature),
            Self::Fd => Error::UnknownFd,
            Self::ArrayLen(len) if len > MAX_ARRAY_LEN => Error::ExcessData,
            Self::ArrayLen(len) => {
                Error::invalid_length(len as usize, &"a multiple of the size of the elements")
            }
            Self::Depth => Error::MaxDepthExceeded(MaxDepthExceeded::Container),
        }
    }
}

/// How many containers a value can be in, at most.
const MAX_DEPTH: u8 = 64;

/// The length of an array, at most, in bytes, as the specification has it.
const MAX_ARRAY_LEN: u32 = 64 * 1024 * 1024;

// What a step of the parser returns fits in two registers.
const _: () = assert!(size_of::<Check<&[u8]>>() <= 16);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    #[cfg(unix)]
    use std::{io::stdout, os::fd::AsFd};

    use serde::de::IgnoredAny;
    use test_log::test;

    use super::{is_ascii_without_nul, validate};
    #[cfg(unix)]
    use crate::Fd;
    use crate::{
        Error, Message, ObjectPath, Signature, Structure, Variant,
        message::{Body, EndianSig},
        wire::{
            BE, LE,
            serialized::{Context, Data},
        },
    };

    #[test]
    fn built_bodies() {
        for msg in messages() {
            msg.body().validate().unwrap();
        }
    }

    #[test]
    fn changed_bytes() {
        // Bodies that are still valid with a byte changed can be deserialized, but where
        // zbus_utils is built for GVariant as well, as building the whole workspace with all its
        // features has it, and so takes the maybe type of GVariant in the signature of a `g` or a
        // `v`, which deserializing rejects.
        for msg in messages() {
            let body = msg.body();
            let signature = body.signature().to_string();
            let endian_sig = msg.primary_header().endian_sig();
            let fds = msg.header().unix_fds().unwrap_or(0);
            let mut bytes = body.data().bytes().to_vec();
            for i in 0..bytes.len() {
                let byte = bytes[i];
                for value in [0, 1, 2, 3, 4, 7, 8, b'a', b'v', b'(', b'm', 0x80, 0xff] {
                    bytes[i] = value;
                    if validate(&bytes, signature.as_bytes(), endian_sig, fds).is_err() {
                        continue;
                    }
                    match deserialize(&bytes, &body) {
                        Ok(()) => (),
                        Err(e) if e == crate::wire::dbus::maybe_not_in_dbus() => (),
                        Err(e) => panic!("{bytes:?} of `{signature}`: {e}"),
                    }
                }
                bytes[i] = byte;
            }
        }
    }

    #[test]
    fn basic_values() {
        assert!(matches!(check(b"b", &[2, 0, 0, 0]), Err(Error::Failure(_))));
        check(b"b", &[1, 0, 0, 0]).unwrap();
        check(b"yq", &[1, 0, 2, 0]).unwrap();
        assert_eq!(check(b"yq", &[1, 7, 2, 0]), Err(Error::PaddingNot0(7)));
        assert_eq!(check(b"u", &[1, 0, 0]), Err(Error::OutOfBounds));
        assert_eq!(check(b"y", &[1, 0]), Err(Error::ExcessData));
        assert_eq!(check(b"", &[0]), Err(Error::ExcessData));
        check(b"", &[]).unwrap();
    }

    #[test]
    fn strings() {
        check(b"s", &[2, 0, 0, 0, b'h', b'i', 0]).unwrap();
        assert!(matches!(
            check(b"s", &[2, 0, 0, 0, b'h', 0xff, 0]),
            Err(Error::Utf8(_))
        ));
        // A 0 in the string, and a string that doesn't end with one.
        assert!(matches!(
            check(b"s", &[2, 0, 0, 0, b'h', 0, 0]),
            Err(Error::Failure(_))
        ));
        assert!(matches!(
            check(b"s", &[2, 0, 0, 0, b'h', b'i', 1]),
            Err(Error::Failure(_))
        ));
        assert_eq!(
            check(b"s", &[3, 0, 0, 0, b'h', b'i', 0]),
            Err(Error::OutOfBounds)
        );
        assert_eq!(check(b"s", &[u8::MAX; 4]), Err(Error::OutOfBounds));

        check(b"o", &[2, 0, 0, 0, b'/', b'a', 0]).unwrap();
        assert_eq!(
            check(b"o", &[2, 0, 0, 0, b'a', b'/', 0]),
            Err(Error::InvalidObjectPath)
        );

        check(b"g", &[5, b'a', b'{', b's', b'v', b'}', 0]).unwrap();
        assert!(matches!(
            check(b"g", &[2, b'a', b'{', 0]),
            Err(Error::SignatureParse(_))
        ));
    }

    #[test]
    fn ascii() {
        // Every length up to a few words, with each byte in turn changed to one of the edges.
        for len in 0..=33 {
            let mut bytes = vec![b'a'; len];
            assert!(is_ascii_without_nul(&bytes));
            for i in 0..len {
                for byte in [0, 1, 0x7f, 0x80, 0xff] {
                    bytes[i] = byte;
                    let ascii = byte != 0 && byte.is_ascii();
                    assert_eq!(is_ascii_without_nul(&bytes), ascii, "{bytes:?}");
                    // With a 0 below a byte that is 1, which it borrows from.
                    if i > 0 {
                        bytes[i - 1] = 0;
                        assert!(!is_ascii_without_nul(&bytes), "{bytes:?}");
                        bytes[i - 1] = b'a';
                    }
                }
                bytes[i] = b'a';
            }
        }
    }

    #[test]
    fn variants() {
        check(b"v", &[1, b'y', 0, 7]).unwrap();
        check(b"v", &[4, b'(', b'y', b'y', b')', 0, 0, 0, 1, 2]).unwrap();
        // Two types, and none at all.
        assert!(matches!(
            check(b"v", &[2, b'y', b'y', 0, 1, 2]),
            Err(Error::SignatureParse(_))
        ));
        assert!(matches!(
            check(b"v", &[0, 0]),
            Err(Error::SignatureParse(_))
        ));
        // A variant of a variant of a variant and so on, 64 deep at most, as with deserializing.
        for depth in [64, 65] {
            let mut bytes = [1, b'v', 0].repeat(depth - 1);
            bytes.extend([1, b'y', 0, 7]);
            let checked = check(b"v", &bytes);
            let data = Data::new(&bytes[..], Context::new(LE, 0));
            let deserialized = data.deserialize_for_dynamic_signature::<_, Variant<'_>>("v");
            assert_eq!(checked.is_ok(), deserialized.is_ok(), "{checked:?}");
            assert_eq!(checked.is_ok(), depth == 64, "{checked:?}");
        }
    }

    #[test]
    fn depth() {
        // Deserializing counts nesting as the parser does, so it takes each of these bodies if and
        // only if the parser does.

        // Dicts of a single entry each, of a variant of the next dict, the last of which holds a
        // byte: the entry of the first dict is in 2 containers, its array and itself, and each dict
        // adds 3 more, for the variant. With 21 dicts, the byte is in 63 containers, and with 22,
        // the entry of the last dict is in 65, which libdbus rejects as well.
        for (dicts, valid) in [(21, true), (22, false)] {
            let mut bytes = vec![];
            nested_dicts(&mut bytes, dicts);
            let checked = check_and_deserialize(b"a{sv}", &bytes);
            assert_eq!(checked.is_ok(), valid, "{dicts} dicts: {checked:?}");
        }

        // 16 arrays of a variant of 17 arrays of bytes: each signature holds no more than 32
        // arrays, and the bytes are in 33 containers, as an array of bytes holds no other value.
        let mut bytes = vec![];
        let mut variant_signature = vec![18];
        variant_signature.extend([b'a'; 17]);
        variant_signature.extend([b'y', 0]);
        nested_arrays(&mut bytes, 16, 1, &|bytes| {
            bytes.extend(&variant_signature);
            nested_arrays(bytes, 17, 1, &|bytes| bytes.push(7));
        });
        let mut signature = vec![b'a'; 16];
        signature.push(b'v');
        check_and_deserialize(&signature, &bytes).unwrap();

        // A variant of an empty array, or of a structure, nested in 32 others of its kind in its
        // signature, which a signature holds no more than of either.
        for (nested, valid) in [(32, true), (33, false)] {
            let mut bytes = vec![nested + 1];
            bytes.extend([b'a'; 33].iter().take(nested.into()));
            bytes.extend([b'y', 0]);
            nested_arrays(&mut bytes, 1, 4, &|_| ());
            let checked = check_and_deserialize(b"v", &bytes);
            assert_eq!(checked.is_ok(), valid, "{nested} arrays: {checked:?}");

            let mut bytes = vec![2 * nested + 1];
            bytes.extend([b'('; 33].iter().take(nested.into()));
            bytes.push(b'y');
            bytes.extend([b')'; 33].iter().take(nested.into()));
            bytes.push(0);
            pad(&mut bytes, 8);
            bytes.push(7);
            let checked = check_and_deserialize(b"v", &bytes);
            assert_eq!(checked.is_ok(), valid, "{nested} structures: {checked:?}");
        }

        // An array in variants, in as many containers as there are variants. Its elements are a
        // level deeper than it, even if there are none, and a variant holds any value of its type,
        // as GLib has it: GLib rejects these with one more variant, and libdbus doesn't, but for
        // the array of a string.
        let max = Err(Error::MaxDepthExceeded(crate::MaxDepthExceeded::Container));
        for (signature, alignment, elements, variants) in [
            (&b"ay"[..], 1, &[7][..], 63),
            (b"as", 4, &[], 63),
            (b"as", 4, &[1, 0, 0, 0, b'7', 0], 63),
            (b"ab", 4, &[1, 0, 0, 0], 63),
            (b"ab", 4, &[], 63),
            (b"aas", 4, &[], 62),
            (b"a(yy)", 8, &[], 62),
        ] {
            for (variants, expected) in [(variants, Ok(())), (variants + 1, max.clone())] {
                let mut bytes = [1, b'v', 0].repeat(variants - 1);
                bytes.push(signature.len() as u8);
                bytes.extend(signature);
                bytes.push(0);
                nested_arrays(&mut bytes, 1, alignment, &|bytes| bytes.extend(elements));
                let checked = check_and_deserialize(b"v", &bytes);
                assert_eq!(
                    checked, expected,
                    "{variants} variants of {signature:?}, {elements:?}"
                );
            }
        }

        // An array in 31 dicts and in structures of the signature of the body, which no variant
        // declares: with 2 structures, it's in 64 containers, and its elements a level deeper,
        // unless it's empty and they're neither numbers nor file descriptors, as GLib has them.
        // libdbus takes all of these.
        let mut arrays = vec![
            (&b"y"[..], 1, &[7][..], false),
            (b"y", 1, &[], false),
            (b"b", 4, &[1, 0, 0, 0], false),
            (b"b", 4, &[], true),
            (b"s", 4, &[], true),
        ];
        if cfg!(unix) {
            arrays.push((b"h", 4, &[], false));
        }
        for (element, alignment, elements, valid) in arrays {
            for structures in [1, 2] {
                let mut signature = b"a{s".repeat(31);
                signature.extend(b"(".repeat(structures));
                signature.push(b'a');
                signature.extend(element);
                signature.extend(b")".repeat(structures));
                signature.extend(b"}".repeat(31));
                let mut bytes = vec![];
                dicts_of_dicts(&mut bytes, 31, &|bytes| {
                    pad(bytes, 8);
                    nested_arrays(bytes, 1, alignment, &|bytes| bytes.extend(elements));
                });
                let expected = match valid || structures == 1 {
                    true => Ok(()),
                    false => max.clone(),
                };
                let checked = check_and_deserialize(&signature, &bytes);
                assert_eq!(checked, expected, "{structures}, {element:?}, {elements:?}");
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn fds() {
        assert_eq!(validate(&[1, 0, 0, 0], b"h", EndianSig::Little, 2), Ok(()));
        assert_eq!(
            validate(&[2, 0, 0, 0], b"h", EndianSig::Little, 2),
            Err(Error::UnknownFd)
        );
        assert_eq!(
            validate(&[4, 0, 0, 0, 0, 0, 0, 0], b"ah", EndianSig::Little, 1),
            Ok(())
        );
        assert_eq!(
            validate(&[4, 0, 0, 0, 1, 0, 0, 0], b"ah", EndianSig::Little, 1),
            Err(Error::UnknownFd)
        );
    }

    #[test]
    fn arrays() {
        check(b"ay", &[0, 0, 0, 0]).unwrap();
        check(b"ay", &[3, 0, 0, 0, 1, 2, 3]).unwrap();
        assert_eq!(
            check(b"ay", &[4, 0, 0, 0, 1, 2, 3]),
            Err(Error::OutOfBounds)
        );
        assert!(matches!(
            check(b"au", &[5, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(Error::Failure(_))
        ));
        assert!(matches!(
            check(b"ab", &[4, 0, 0, 0, 2, 0, 0, 0]),
            Err(Error::Failure(_))
        ));
        // The padding before the first element is there even if the array is empty.
        check(b"ax", &[0, 0, 0, 0, 0, 0, 0, 0]).unwrap();
        assert_eq!(check(b"ax", &[0, 0, 0, 0]), Err(Error::OutOfBounds));
        assert_eq!(
            check(b"ax", &[0, 0, 0, 0, 0, 1, 0, 0]),
            Err(Error::PaddingNot0(1))
        );
        // An element that runs past the end of its array.
        assert_eq!(
            check(b"as", &[6, 0, 0, 0, 2, 0, 0, 0, b'h', b'i', 0]),
            Err(Error::OutOfBounds)
        );
        // An array longer than 64 MiB.
        assert_eq!(check(b"ay", &[1, 0, 0, 4]), Err(Error::ExcessData));
        // Dict entries, which start on an 8-byte boundary.
        check(
            b"a{yy}",
            &[10, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 3, 4],
        )
        .unwrap();
        assert_eq!(
            check(
                b"a{yy}",
                &[10, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 5, 0, 0, 0, 0, 3, 4]
            ),
            Err(Error::PaddingNot0(5))
        );
    }

    /// Write `dicts` dicts, in little-endian byte order, each of a single entry of the key `k` and
    /// a variant of the next dict, the last of which holds the byte 7.
    fn nested_dicts(bytes: &mut Vec<u8>, dicts: usize) {
        nested_arrays(bytes, 1, 8, &|bytes| {
            bytes.extend([1, 0, 0, 0, b'k', 0]);
            match dicts {
                1 => bytes.extend([1, b'y', 0, 7]),
                _ => {
                    bytes.extend([5, b'a', b'{', b's', b'v', b'}', 0]);
                    nested_dicts(bytes, dicts - 1);
                }
            }
        });
    }

    /// Write `dicts` dicts, in little-endian byte order, each of a single entry of the key `k` and
    /// the next dict, the last of which holds the value that `last` writes.
    fn dicts_of_dicts(bytes: &mut Vec<u8>, dicts: usize, last: &dyn Fn(&mut Vec<u8>)) {
        nested_arrays(bytes, 1, 8, &|bytes| {
            bytes.extend([1, 0, 0, 0, b'k', 0]);
            match dicts {
                1 => last(bytes),
                _ => dicts_of_dicts(bytes, dicts - 1, last),
            }
        });
    }

    /// Write `n` arrays, in little-endian byte order, each of a single element, of the next array,
    /// the last of which has the element that `last` writes, whose alignment is `alignment`.
    fn nested_arrays(bytes: &mut Vec<u8>, n: usize, alignment: usize, last: &dyn Fn(&mut Vec<u8>)) {
        if n == 0 {
            return last(bytes);
        }
        pad(bytes, 4);
        let len = bytes.len();
        bytes.extend([0; 4]);
        pad(bytes, if n == 1 { alignment } else { 4 });
        let start = bytes.len();
        nested_arrays(bytes, n - 1, alignment, last);
        let elements_len = u32::try_from(bytes.len() - start).unwrap();
        bytes[len..len + 4].copy_from_slice(&elements_len.to_le_bytes());
    }

    /// Pad `bytes` with 0s to a multiple of `alignment`.
    fn pad(bytes: &mut Vec<u8>, alignment: usize) {
        bytes.resize(bytes.len().next_multiple_of(alignment), 0);
    }

    /// Check `bytes` against `signature`, in little-endian byte order, without file descriptors.
    fn check(signature: &[u8], bytes: &[u8]) -> crate::Result<()> {
        validate(bytes, signature, EndianSig::Little, 0)
    }

    /// Check `bytes` against `signature`, of a single complete type, as [`check`] does, making sure
    /// that deserializing a value of `signature` from them takes all of them if and only if they're
    /// valid.
    fn check_and_deserialize(signature: &[u8], bytes: &[u8]) -> crate::Result<()> {
        let checked = check(signature, bytes);
        let data = Data::new(bytes, Context::new(LE, 0));
        let deserialized = data.deserialize_for_signature::<_, IgnoredAny>(signature);
        assert_eq!(
            matches!(deserialized, Ok((_, len)) if len == bytes.len()),
            checked.is_ok(),
            "`{}`: {checked:?}, deserialized: {deserialized:?}",
            String::from_utf8_lossy(signature),
        );

        checked
    }

    /// Deserialize `bytes` of `body` as a structure of the types of its signature, whose bytes are
    /// the same as those of the types one after the other.
    fn deserialize(bytes: &[u8], body: &Body) -> crate::Result<()> {
        let ctxt = body.data().context();
        #[cfg(unix)]
        let data =
            Data::new_borrowed_fds(bytes, ctxt, body.data().fds().iter().map(|fd| fd.as_fd()));
        #[cfg(not(unix))]
        let data = Data::new(bytes, ctxt);
        let signature = Signature::try_from(format!("({})", body.signature()).as_str())?;

        data.deserialize_for_dynamic_signature::<_, Structure<'_>>(&signature)
            .map(|_| ())
    }

    /// Messages with bodies of every type, in both byte orders.
    fn messages() -> Vec<Message> {
        let mut messages = vec![];
        for endian in [LE, BE] {
            let msg = || Message::method_call("/org/zbus/Test", "Test").endian(endian);
            let dict = HashMap::from([
                ("y", Variant::from(7u8)),
                ("s", Variant::from("seven")),
                ("v", Variant::Variant(Box::new(Variant::from(7u64)))),
                ("as", Variant::from(vec!["se", "ven"])),
                ("(yx)", Variant::from((7u8, 7i64))),
                ("ay", Variant::from(vec![7u8; 7])),
            ]);
            messages.extend([
                msg().build(&()).unwrap(),
                msg().build(&7u8).unwrap(),
                msg()
                    .build(&(
                        7u8,
                        true,
                        7i16,
                        7u16,
                        7i32,
                        7u32,
                        7i64,
                        7u64,
                        7.0f64,
                        "seven",
                        ObjectPath::try_from("/org/zbus/Seven").unwrap(),
                        Signature::try_from("a{sv}(yx)").unwrap(),
                    ))
                    .unwrap(),
                msg().build(&(vec![7u8; 7], Vec::<u8>::new())).unwrap(),
                msg().build(&(vec![true, false], vec![7.0f64])).unwrap(),
                msg()
                    .build(&(7u8, Vec::<(u8, u64)>::new(), vec![(7u8, 7u64)]))
                    .unwrap(),
                msg()
                    .build(&vec![vec!["se", "ven"], vec![], vec!["7"]])
                    .unwrap(),
                msg().build(&(7u8, dict)).unwrap(),
                msg()
                    .build(&HashMap::from([(7u8, (7i32, "seven"))]))
                    .unwrap(),
                msg().build(&((7u8,), (7u64, (7u8, "seven")))).unwrap(),
            ]);
            #[cfg(unix)]
            messages.push(msg().build(&(7u8, Fd::from(&stdout()))).unwrap());
        }

        messages
    }
}
