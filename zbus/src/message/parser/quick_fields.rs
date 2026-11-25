//! What the parser finds of the header fields: where each string field lies in the message, and
//! the other fields.

use std::{borrow::Cow, num::NonZeroU32, ops::Range, str};

use winnow::stream::Accumulate;

use crate::{
    Error, ObjectPath, Signature,
    message::{FieldCode, Fields, Type, header::PRIMARY_HEADER_SIZE},
    names::{BusName, ErrorName, InterfaceName, MemberName, UniqueName, WellKnownName},
};

/// Where the string fields of a header lie in the bytes of its message, and the other fields.
///
/// Only the parser creates it, from fields that it checked, so that reading them back doesn't have
/// to check them again.
#[derive(Debug, Default, Clone)]
pub(crate) struct QuickFields {
    path: Option<FieldPos>,
    interface: Option<FieldPos>,
    member: Option<FieldPos>,
    error_name: Option<FieldPos>,
    reply_serial: Option<NonZeroU32>,
    destination: Option<FieldPos>,
    /// Whether the destination is a unique name, rather than a well-known one.
    unique_destination: bool,
    sender: Option<FieldPos>,
    signature: Signature,
    /// Where the signature lies, if the message has one.
    signature_pos: Option<FieldPos>,
    unix_fds: Option<u32>,
    /// A bit for the code of each field that came, and [`REPEATED`] if one came more than once.
    codes: u16,
}

impl QuickFields {
    /// The number of file descriptors that accompany the message.
    #[cfg(unix)]
    pub(crate) fn unix_fds(&self) -> Option<u32> {
        self.unix_fds
    }

    /// Check that the fields that a message of type `msg_type` requires are all there.
    pub(crate) fn check_required(&self, msg_type: Type) -> Result<(), Error> {
        let present = match msg_type {
            Type::MethodCall => self.path.is_some() && self.member.is_some(),
            Type::MethodReturn => self.reply_serial.is_some(),
            Type::Error => self.error_name.is_some() && self.reply_serial.is_some(),
            Type::Signal => {
                self.path.is_some() && self.interface.is_some() && self.member.is_some()
            }
        };

        if present {
            Ok(())
        } else {
            Err(Error::MissingField)
        }
    }

    /// The fields, borrowed from the bytes of the message.
    ///
    /// # Safety
    ///
    /// `bytes` must be the bytes that `self` was parsed from.
    pub(in crate::message) unsafe fn fields<'m>(&'m self, bytes: &'m [u8]) -> Fields<'m> {
        // SAFETY: The caller ensures that `bytes` are those that `self` was parsed from.
        unsafe { self.fields_with(bytes, Cow::Borrowed(&self.signature)) }
    }

    /// The fields, borrowed from the bytes of the message, but for the signature of the body,
    /// which is `signature`.
    ///
    /// # Safety
    ///
    /// `bytes` must be the bytes that `self` was parsed from.
    pub(in crate::message) unsafe fn fields_with<'m>(
        &self,
        bytes: &'m [u8],
        signature: Cow<'m, Signature>,
    ) -> Fields<'m> {
        let read = |pos: Option<FieldPos>| {
            pos.map(|pos| {
                // SAFETY: The caller ensures that `bytes` are those that `pos` was parsed from, as
                // the position of a name or a path, which the parser checked, and those are ASCII.
                unsafe { str::from_utf8_unchecked(&bytes[pos.range()]) }
            })
        };

        Fields {
            path: read(self.path).map(ObjectPath::from_str_unchecked),
            interface: read(self.interface).map(InterfaceName::from_str_unchecked),
            member: read(self.member).map(MemberName::from_str_unchecked),
            error_name: read(self.error_name).map(ErrorName::from_str_unchecked),
            reply_serial: self.reply_serial,
            destination: read(self.destination).map(|name| {
                if self.unique_destination {
                    BusName::Unique(UniqueName::from_str_unchecked(name))
                } else {
                    BusName::WellKnown(WellKnownName::from_str_unchecked(name))
                }
            }),
            sender: read(self.sender).map(UniqueName::from_str_unchecked),
            signature,
            unix_fds: self.unix_fds,
        }
    }

    /// The signature of the body.
    pub(in crate::message) fn signature(&self) -> &Signature {
        &self.signature
    }

    /// The signature of the body as it lies in `bytes`, which must be those that `self` was parsed
    /// from.
    pub(in crate::message) fn signature_bytes<'m>(&self, bytes: &'m [u8]) -> &'m [u8] {
        self.signature_pos.map_or(&[], |pos| &bytes[pos.range()])
    }

    /// Where the name in the sender field lies, if the message has one.
    #[cfg(feature = "bus-impl")]
    pub(in crate::message) fn sender_range(&self) -> Option<Range<usize>> {
        self.sender.map(FieldPos::range)
    }

    /// The fields of the same message with a sender field added after the others, whose name lies
    /// at `name`, which the message didn't have.
    #[cfg(feature = "bus-impl")]
    pub(in crate::message) fn with_sender(&self, name: Range<usize>) -> Self {
        Self {
            sender: Some(FieldPos::new(name.start, name.end)),
            ..self.clone()
        }
    }

    /// Whether a field came more than once.
    pub(super) fn repeated(&self) -> bool {
        self.codes & REPEATED != 0
    }
}

/// The fields are gathered as they're parsed. A field that comes more than once makes the header
/// invalid, as it does with libdbus, rather than one of its values be taken for that of the field.
impl Accumulate<Field> for QuickFields {
    fn initial(_capacity: Option<usize>) -> Self {
        Self::default()
    }

    #[inline(always)]
    fn accumulate(&mut self, field: Field) {
        // Without a branch, as fields seldom come more than once.
        let code = 1 << field.code() as u16;
        self.codes |= code | (u16::from(self.codes & code != 0) * REPEATED);

        match field {
            Field::Path(pos) => self.path = Some(pos),
            Field::Interface(pos) => self.interface = Some(pos),
            Field::Member(pos) => self.member = Some(pos),
            Field::ErrorName(pos) => self.error_name = Some(pos),
            Field::ReplySerial(serial) => self.reply_serial = Some(serial),
            Field::Destination { pos, unique } => {
                self.destination = Some(pos);
                self.unique_destination = unique;
            }
            Field::Sender(pos) => self.sender = Some(pos),
            Field::Signature { signature, pos } => {
                self.signature = signature;
                self.signature_pos = Some(pos);
            }
            Field::UnixFds(unix_fds) => self.unix_fds = Some(unix_fds),
        }
    }
}

/// The bit of [`QuickFields::codes`] that tells a field came more than once, which no field code
/// takes, as they start at 1.
const REPEATED: u16 = 1;

/// A header field, as the parser finds it.
pub(super) enum Field {
    Path(FieldPos),
    Interface(FieldPos),
    Member(FieldPos),
    ErrorName(FieldPos),
    ReplySerial(NonZeroU32),
    Destination {
        pos: FieldPos,
        /// Whether the destination is a unique name, rather than a well-known one.
        unique: bool,
    },
    Sender(FieldPos),
    Signature {
        signature: Signature,
        /// Where the signature lies, to check the body against it.
        pos: FieldPos,
    },
    UnixFds(u32),
}

impl Field {
    /// The code of the field.
    #[inline(always)]
    fn code(&self) -> FieldCode {
        match self {
            Self::Path(_) => FieldCode::Path,
            Self::Interface(_) => FieldCode::Interface,
            Self::Member(_) => FieldCode::Member,
            Self::ErrorName(_) => FieldCode::ErrorName,
            Self::ReplySerial(_) => FieldCode::ReplySerial,
            Self::Destination { .. } => FieldCode::Destination,
            Self::Sender(_) => FieldCode::Sender,
            Self::Signature { .. } => FieldCode::Signature,
            Self::UnixFds(_) => FieldCode::UnixFDs,
        }
    }

    /// The signature field, of `signature`, which lies at `pos` in the message.
    pub(super) fn signature(signature: Signature, pos: FieldPos) -> Self {
        Self::Signature { signature, pos }
    }
}

/// The byte range of a string field in the bytes of its message.
///
/// A field comes after the primary header, so it never starts at 0, which leaves room in a
/// `FieldPos` for `None`: an `Option<FieldPos>` takes no more bytes than a `FieldPos` does.
#[derive(Debug, Clone, Copy)]
pub(super) struct FieldPos {
    start: NonZeroU32,
    end: u32,
}

impl FieldPos {
    /// The field at `start..end` in the bytes of the message, which the length of the fields
    /// ensured fits in a `u32`, and which comes after the primary header.
    pub(super) fn new(start: usize, end: usize) -> Self {
        debug_assert!(start >= PRIMARY_HEADER_SIZE);

        Self {
            // SAFETY: The field comes after the primary header, so it doesn't start at 0.
            start: unsafe { NonZeroU32::new_unchecked(start as u32) },
            end: end as u32,
        }
    }

    /// The byte range of the field.
    fn range(self) -> Range<usize> {
        self.start.get() as usize..self.end as usize
    }
}
