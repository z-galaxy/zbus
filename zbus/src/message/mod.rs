//! D-Bus Message.
#[cfg(unix)]
use std::os::fd::OwnedFd;
use std::{fmt, mem, num::NonZeroU32, ops::Range, sync::Arc};

use crate::{
    Error, ObjectPath, Result,
    names::{BusName, ErrorName, InterfaceName, MemberName},
    utils::padding_for_8_bytes,
    wire::{
        Endian,
        serialized::{self, Context},
    },
};

mod builder;
pub use builder::Builder;

mod field_code;
pub(crate) use field_code::FieldCode;

mod fields;
pub(crate) use fields::Fields;

mod body;
pub use body::Body;

pub(crate) mod header;
#[cfg(feature = "bus-impl")]
use header::PRIMARY_HEADER_SIZE;
use header::PROTOCOL_VERSION;
pub use header::{
    EndianSig, Flags, Header, MAX_MESSAGE_SIZE, NATIVE_ENDIAN_SIG, PrimaryHeader, Type,
};

pub(crate) mod parser;
use parser::QuickFields;

#[cfg_attr(
    feature = "comms",
    doc = "A position in the stream of [`Message`] objects received by a single \
           [`zbus::Connection`].",
    doc = "",
    doc = "Note: the relative ordering of values obtained from distinct [`zbus::Connection`] \
           objects is not specified; only sequence numbers originating from the same connection \
           should be compared."
)]
#[cfg_attr(
    not(feature = "comms"),
    doc = "A position in the stream of [`Message`] objects received by a single connection \
           (`zbus::Connection`, which requires the `comms` feature).",
    doc = "",
    doc = "Note: the relative ordering of values obtained from distinct connections is not \
           specified; only sequence numbers originating from the same connection should be \
           compared."
)]
#[derive(Debug, Default, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Sequence {
    recv_seq: u64,
}

impl Sequence {
    /// A sequence number that is higher than any other; used by errors that terminate a stream.
    #[cfg(feature = "comms")]
    pub(crate) const LAST: Self = Self { recv_seq: u64::MAX };

    /// The position itself, which is 1 for the first message that a connection received.
    #[cfg(all(target_os = "linux", feature = "handover"))]
    pub(crate) fn get(self) -> u64 {
        self.recv_seq
    }
}

/// A D-Bus Message.
///
/// The contents of the message are stored in serialized format. To get the body of the message, use
/// the [`Message::body`] method, and use [`Body`] methods to deserialize it. You may also access
/// the header and other details with the various other getters.
///
/// Also provided are constructors for messages of different types. These will mainly be useful for
/// very advanced use cases as typically you will want to create a message for immediate dispatch
#[cfg_attr(
    feature = "comms",
    doc = "and hence use the API provided by [`crate::connection::Connection::call_method`], even \
           when using the low-level API."
)]
#[cfg_attr(
    not(feature = "comms"),
    doc = "and hence use the API provided by `Connection::call_method` (which requires the `comms` \
           feature), even when using the low-level API."
)]
///
/// **Note**: The message owns the received FDs and will close them when dropped. You can
/// deserialize the body (that you get using [`Message::body`]) to [`crate::OwnedFd`] if
/// you want to keep the FDs around after the containing message is dropped.
#[derive(Clone)]
pub struct Message {
    pub(super) inner: Arc<Inner>,
}

pub(super) struct Inner {
    pub(crate) primary_header: PrimaryHeader,
    /// The fields of the header, as found in `bytes`, once they've been looked for.
    quick_fields: std::sync::OnceLock<QuickFields>,
    pub(crate) bytes: serialized::Data<'static, 'static>,
    pub(crate) body_offset: usize,
    pub(crate) recv_seq: Sequence,
}

impl Message {
    /// Create a builder for a message of type [`Type::MethodCall`].
    ///
    /// An invalid path or method name is reported by [`Builder::build`].
    #[must_use]
    pub fn method_call<'b, 'p: 'b, 'm: 'b, P, M>(path: P, method_name: M) -> Builder<'b>
    where
        P: TryInto<ObjectPath<'p>>,
        M: TryInto<MemberName<'m>>,
        P::Error: Into<Error>,
        M::Error: Into<Error>,
    {
        Builder::new(Type::MethodCall)
            .path(path)
            .member(method_name)
    }

    /// Create a builder for a message of type [`Type::Signal`].
    ///
    /// An invalid path, interface name or signal name is reported by [`Builder::build`].
    #[must_use]
    pub fn signal<'b, 'p: 'b, 'i: 'b, 'm: 'b, P, I, M>(
        path: P,
        iface: I,
        signal_name: M,
    ) -> Builder<'b>
    where
        P: TryInto<ObjectPath<'p>>,
        I: TryInto<InterfaceName<'i>>,
        M: TryInto<MemberName<'m>>,
        P::Error: Into<Error>,
        I::Error: Into<Error>,
        M::Error: Into<Error>,
    {
        Builder::new(Type::Signal)
            .path(path)
            .interface(iface)
            .member(signal_name)
    }

    /// Create a builder for a message of type [`Type::MethodReturn`].
    #[must_use]
    pub fn method_return(reply_to: &Header<'_>) -> Builder<'static> {
        Builder::new(Type::MethodReturn).reply_to(reply_to)
    }

    /// Create a builder for a message of type [`Type::Error`].
    ///
    /// An invalid error name is reported by [`Builder::build`].
    #[must_use]
    pub fn error<'b, 'e: 'b, E>(reply_to: &Header<'_>, name: E) -> Builder<'b>
    where
        E: TryInto<ErrorName<'e>>,
        E::Error: Into<Error>,
    {
        Builder::new(Type::Error)
            .reply_to(reply_to)
            .error_name(name)
    }

    /// Build an error reply from the caller and call serial when the original call header is
    /// unavailable.
    ///
    /// This creates a builder for a message of type [`Type::Error`]. The reply goes to `caller`,
    /// and answers the method call that has the serial number `serial`. A bus that keeps only a
    /// record of each call, and not the call itself, is one sender that needs this.
    ///
    /// [`Message::error`] takes the caller, the serial and the endianness from the header of the
    /// call. This function can't know the endianness of the call, so the message uses the native
    /// one unless [`Builder::endian`] sets another.
    ///
    /// An invalid caller name or error name is reported by [`Builder::build`].
    ///
    /// # Example
    ///
    /// ```
    /// use std::num::NonZeroU32;
    /// use zbus::message::Message;
    ///
    /// let serial = NonZeroU32::new(7).unwrap();
    /// let error = Message::error_for_serial(":1.42", serial, "org.zbus.Error.Failed")
    ///     .build(&("kaboom!",))
    ///     .unwrap();
    /// let header = error.header();
    ///
    /// assert_eq!(header.reply_serial(), Some(serial));
    /// assert_eq!(header.destination().unwrap().as_str(), ":1.42");
    /// assert_eq!(header.error_name().unwrap().as_str(), "org.zbus.Error.Failed");
    /// ```
    #[must_use]
    pub fn error_for_serial<'b, 'c: 'b, 'e: 'b, C, E>(
        caller: C,
        serial: NonZeroU32,
        name: E,
    ) -> Builder<'b>
    where
        C: TryInto<BusName<'c>>,
        E: TryInto<ErrorName<'e>>,
        C::Error: Into<Error>,
        E::Error: Into<Error>,
    {
        Builder::new(Type::Error)
            .destination(caller)
            .reply_serial(Some(serial))
            .error_name(name)
    }

    /// Create a message from bytes, as [`Message::try_from_bytes`] does, but without checking that
    /// they end where the header of the message says that it does, or that they carry no more file
    /// descriptors than it counts.
    ///
    /// # Safety
    ///
    /// The caller has nothing to uphold: nothing in this method is unsafe.
    #[deprecated(
        since = "6.6.0",
        note = "Nothing in it is unsafe: use Message::try_from_bytes. This will be removed in \
                zbus 7."
    )]
    pub unsafe fn from_bytes(bytes: serialized::Data<'static, 'static>) -> Result<Self> {
        Self::from_raw_parts(bytes, 0)
    }

    /// Create a message from its bytes, and the file descriptors that it carries.
    ///
    /// The bytes are checked as those of a message that a connection receives are: its header,
    /// in full, its protocol version, the fields that its type requires, the padding after its
    /// header, that they end where its header says that it does, and that it carries as many file
    /// descriptors as its header counts. The body is not checked against its signature:
    /// [`Body::validate`] does that.
    ///
    /// The message is at receive position 0 ([`Message::recv_position`]), as no connection
    /// received it.
    ///
    /// # Errors
    ///
    /// The error that tells what is wrong with the message, such as:
    ///
    /// * [`Error::IncorrectEndian`] if its endian signature isn't the byte order of the context of
    ///   `bytes`;
    /// * [`Error::OutOfBounds`] if `bytes` are fewer than its header says, and
    ///   [`Error::ExcessData`] if they are more;
    /// * [`Error::MissingParameter`] if it carries fewer file descriptors than its header counts,
    ///   and [`Error::ExcessData`] if it carries more.
    ///
    /// # Example
    ///
    /// ```
    /// use zbus::{message::Message, wire::serialized::Data};
    ///
    /// let msg = Message::method_call("/org/zbus/Example", "Ping").build(&())?;
    /// let bytes = Data::new(msg.data().to_vec(), msg.data().context());
    ///
    /// let copy = Message::try_from_bytes(bytes)?;
    /// assert_eq!(copy.header().member().unwrap().as_str(), "Ping");
    /// # Ok::<(), zbus::Error>(())
    /// ```
    pub fn try_from_bytes(bytes: serialized::Data<'static, 'static>) -> Result<Self> {
        let (_, len) = PrimaryHeader::read_with_len(&bytes)?;
        if bytes.len() < len {
            return Err(Error::OutOfBounds);
        }
        if bytes.len() > len {
            return Err(Error::ExcessData);
        }
        let msg = Self::from_raw_parts(bytes, 0)?;
        // Making the message checked that it carries no fewer file descriptors than its header
        // counts.
        #[cfg(unix)]
        if msg.data().fds().len() > msg.quick_fields().unix_fds().unwrap_or(0) as usize {
            return Err(Error::ExcessData);
        }

        Ok(msg)
    }

    /// Take the message that `bytes` start with out of them, once it is whole, along with the file
    /// descriptors that it carries out of `fds`.
    ///
    /// This is how a program that reads messages off a socket of its own makes messages of what
    /// it reads: `bytes` are the bytes that came in and are yet to be taken, and `fds` the file
    /// descriptors that came in with them, in the order that they came in. Each call takes one
    /// message, once `bytes` hold all of it, and returns `None` until then, taking nothing.
    /// [`PrimaryHeader::read_with_len`] tells how many bytes a message has in all.
    ///
    /// The message takes as many of `fds` as its header counts, from the first on, whichever of
    /// its bytes they came with, as the reference implementation of D-Bus gives them out. The
    /// D-Bus specification has them sent along with the bytes of the message, none before its
    /// first byte or after its last, so they are all in once the message is. Those that the
    /// message doesn't take stay in `fds`, for the messages after it.
    ///
    /// The message is checked as one that a connection receives is: its header, in full, its
    /// protocol version, the fields that its type requires, and the padding after its header. The
    /// body is not checked against its signature: [`Body::validate`] does that.
    ///
    /// The message keeps the allocation of `bytes`, and what follows it in `bytes` is copied to a
    /// new one: nothing is copied for a program that reads no further than the end of the message
    /// that it waits for.
    ///
    /// The message is at receive position 0 ([`Message::recv_position`]), as no connection
    /// received it.
    ///
    /// This method takes `fds` on Unix only, where messages carry file descriptors.
    ///
    /// # Errors
    ///
    /// Fails if the message that `bytes` start with fails these checks, with the error that tells
    /// what is wrong with it, as [`PrimaryHeader::read_with_len`] does once the first bytes are in,
    /// and with [`Error::MissingParameter`] if its header counts more file descriptors than `fds`
    /// hold. On an error, nothing is taken out of `bytes` or `fds`.
    ///
    /// # Example
    ///
    /// Two messages come in, the second one in two parts.
    ///
    /// ```
    /// use zbus::message::Message;
    ///
    /// let ping = Message::method_call("/org/zbus/Example", "Ping").build(&())?;
    /// let pong = Message::method_call("/org/zbus/Example", "Pong").build(&())?;
    /// let mut bytes = ping.data().to_vec();
    /// bytes.extend_from_slice(&pong.data()[..20]);
    /// #[cfg(unix)]
    /// let mut fds = vec![];
    ///
    /// let msg = Message::take_from(&mut bytes, #[cfg(unix)] &mut fds)?.unwrap();
    /// assert_eq!(msg.header().member().unwrap().as_str(), "Ping");
    ///
    /// // What came in of the second message waits for the rest of it.
    /// assert!(Message::take_from(&mut bytes, #[cfg(unix)] &mut fds)?.is_none());
    /// bytes.extend_from_slice(&pong.data()[20..]);
    /// let msg = Message::take_from(&mut bytes, #[cfg(unix)] &mut fds)?.unwrap();
    /// assert_eq!(msg.header().member().unwrap().as_str(), "Pong");
    /// assert!(bytes.is_empty());
    /// # Ok::<(), zbus::Error>(())
    /// ```
    pub fn take_from(
        bytes: &mut Vec<u8>,
        #[cfg(unix)] fds: &mut Vec<OwnedFd>,
    ) -> Result<Option<Self>> {
        Self::take_received(
            bytes,
            #[cfg(unix)]
            fds,
            0,
        )
    }

    /// Create a message from its full contents.
    pub(crate) fn from_raw_parts(
        bytes: serialized::Data<'static, 'static>,
        recv_seq: u64,
    ) -> Result<Self> {
        let (primary_header, header_len, quick_fields) = parser::parse(&bytes)?;
        if Endian::from(primary_header.endian_sig()) != bytes.context().endian() {
            return Err(Error::IncorrectEndian);
        }

        let body_offset = header_len + padding_for_8_bytes(header_len);
        check_whole(
            &primary_header,
            &quick_fields,
            &bytes,
            #[cfg(unix)]
            bytes.fds().len(),
            header_len..body_offset,
        )?;
        let quick_fields = quick_fields.into();

        Ok(Self {
            inner: Arc::new(Inner {
                primary_header,
                quick_fields,
                bytes,
                body_offset,
                recv_seq: Sequence { recv_seq },
            }),
        })
    }

    /// [`Message::take_from`], for a message at position `recv_seq` of those that a connection
    /// received.
    ///
    /// The message is checked as it lies in `bytes`, before anything is taken out of them, so
    /// that nothing is on an error.
    fn take_received(
        bytes: &mut Vec<u8>,
        #[cfg(unix)] fds: &mut Vec<OwnedFd>,
        recv_seq: u64,
    ) -> Result<Option<Self>> {
        if bytes.len() < PrimaryHeader::LEN_WITH_FIELDS_LEN {
            return Ok(None);
        }
        let (_, len) = PrimaryHeader::read_with_len(bytes)?;
        let Some(msg) = bytes.get(..len) else {
            return Ok(None);
        };
        let (primary_header, header_len, quick_fields) = parser::parse(msg)?;
        let body_offset = header_len + padding_for_8_bytes(header_len);
        check_whole(
            &primary_header,
            &quick_fields,
            msg,
            #[cfg(unix)]
            fds.len(),
            header_len..body_offset,
        )?;

        // Nothing is allocated when nothing follows the message.
        let rest = bytes.split_off(len);
        let bytes = mem::replace(bytes, rest);
        let ctxt = Context::new(primary_header.endian_sig().into(), 0);
        #[cfg(unix)]
        let bytes = {
            let count = quick_fields.unix_fds().unwrap_or(0) as usize;

            serialized::Data::new_fds(bytes, ctxt, fds.drain(..count))
        };
        #[cfg(not(unix))]
        let bytes = serialized::Data::new(bytes, ctxt);

        Ok(Some(Self {
            inner: Arc::new(Inner {
                primary_header,
                quick_fields: quick_fields.into(),
                bytes,
                body_offset,
                recv_seq: Sequence { recv_seq },
            }),
        }))
    }

    /// The message, at position `recv_seq` of those that a connection received.
    ///
    /// The position is set on the message itself, unless a clone of it is held elsewhere, such as
    /// by the sender of one that came over a channel, or by a stream of a connection that was
    /// handed over. That clone keeps its own position, so the message is then a new one, which
    /// shares the bytes and the file descriptors of this one, and what was parsed of them.
    #[cfg(any(feature = "p2p", all(target_os = "linux", feature = "handover")))]
    pub(crate) fn at_position(mut self, recv_seq: u64) -> Self {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.recv_seq = Sequence { recv_seq };

            return self;
        }
        let inner = &self.inner;

        Self {
            inner: Arc::new(Inner {
                primary_header: inner.primary_header.clone(),
                quick_fields: inner.quick_fields.clone(),
                bytes: inner.bytes.clone(),
                body_offset: inner.body_offset,
                recv_seq: Sequence { recv_seq },
            }),
        }
    }

    /// The primary header of the message.
    ///
    /// This is the part of the message header that every message has, whatever its type.
    /// [`Message::header`] returns the message header as a whole.
    pub fn primary_header(&self) -> &PrimaryHeader {
        &self.inner.primary_header
    }

    /// The message header.
    pub fn header(&self) -> Header<'_> {
        // SAFETY: The quick fields of a message are only ever created from its own bytes.
        let fields = unsafe { self.quick_fields().fields(&self.inner.bytes) };

        Header::new(self.inner.primary_header.clone(), fields)
    }

    /// The message type.
    pub fn message_type(&self) -> Type {
        self.inner.primary_header.msg_type()
    }

    /// The body that you can deserialize using [`Body::deserialize`].
    ///
    /// # Example
    ///
    /// ```
    /// # use zbus::message::Message;
    /// # (|| -> zbus::Result<()> {
    /// let send_body = (7i32, (2i32, "foo"), vec!["bar"]);
    /// let message = Message::method_call("/", "ping")
    ///     .destination("zbus.test")
    ///     .interface("zbus.test")
    ///     .build(&send_body)?;
    /// let header = message.header();
    /// let body = message.body();
    /// let body: zbus::Structure<'_> = body.deserialize()?;
    /// let fields = body.fields();
    /// assert!(matches!(fields[0], zbus::Variant::I32(7)));
    /// assert!(matches!(fields[1], zbus::Variant::Structure(_)));
    /// assert!(matches!(fields[2], zbus::Variant::Array(_)));
    ///
    /// let reply_body = Message::method_return(&header).build(&body)?.body();
    /// let reply_value : (i32, (i32, &str), Vec<String>) = reply_body.deserialize()?;
    ///
    /// assert_eq!(reply_value.0, 7);
    /// assert_eq!(reply_value.2.len(), 1);
    /// # Ok(()) })().unwrap()
    /// ```
    pub fn body(&self) -> Body {
        Body::new(
            self.inner.bytes.slice(self.inner.body_offset..),
            self.clone(),
        )
    }

    /// Get a reference to the underlying byte encoding of the message.
    pub fn data(&self) -> &serialized::Data<'static, 'static> {
        &self.inner.bytes
    }

    /// Get the receive ordering of a message.
    ///
    /// This may be used to identify how two events were ordered on the bus. It only produces a
    #[cfg_attr(
        feature = "comms",
        doc = "useful ordering for messages that were produced by the same [`zbus::Connection`]."
    )]
    #[cfg_attr(
        not(feature = "comms"),
        doc = "useful ordering for messages that were produced by the same connection."
    )]
    ///
    /// This is completely unrelated to the serial number on the message, which is set by the peer
    /// and might not be ordered at all.
    pub fn recv_position(&self) -> Sequence {
        self.inner.recv_seq
    }

    /// Check the body against its signature, as [`Body::validate`] does, without making a [`Body`]
    /// of it, which takes more than a check of a small body does.
    pub(crate) fn validate_body(&self) -> Result<()> {
        let bytes = self.inner.bytes.bytes();
        let quick_fields = self.quick_fields();
        #[cfg(unix)]
        let fds = quick_fields.unix_fds().unwrap_or(0);
        // Without file descriptors, a signature can't have them.
        #[cfg(not(unix))]
        let fds = 0;

        parser::validate_body(
            &bytes[self.inner.body_offset..],
            quick_fields.signature_bytes(bytes),
            self.inner.primary_header.endian_sig(),
            fds,
        )
    }

    /// The message, with `sender` for its sender field, in place of the one that it has, if any.
    ///
    /// A bus sets the sender field of each message that it passes on to the unique name of the peer
    /// that sent it, as most peers leave it out. The new message is a copy of the bytes of this
    /// one, with the field added, which takes much less than building the message anew with a
    /// [`Builder`] does.
    ///
    /// This is mainly provided for bus implementations, and hence only available when the
    /// `bus-impl` feature is enabled.
    #[cfg(feature = "bus-impl")]
    pub fn with_sender(&self, sender: &crate::names::UniqueName<'_>) -> Result<Self> {
        let data = &self.inner.bytes;
        let bytes = data.bytes();
        let quick_fields = self.quick_fields();
        let endian = Endian::from(self.inner.primary_header.endian_sig());
        // The fields come after the primary header and their length, in an array of structures,
        // each of which starts on an 8-byte boundary.
        let fields_start = PRIMARY_HEADER_SIZE + 4;
        let fields_len = endian.read_u32(&bytes[PRIMARY_HEADER_SIZE..fields_start]) as usize;
        let fields_end = fields_start + fields_len;
        // The fields to keep: all of them, but for the sender field, if any, which takes 8 bytes
        // before the name, for its code, the signature of its value and the length of the name,
        // and the 0 after it.
        let (kept, rest) = match quick_fields.sender_range() {
            Some(name) => {
                let end = name.end + 1;
                let next = (end + padding_for_8_bytes(end)).min(fields_end);

                (fields_start..name.start - 8, next..fields_end)
            }
            None => (fields_start..fields_end, fields_end..fields_end),
        };
        let kept_end = fields_start + kept.len() + rest.len();
        let field_start = kept_end + padding_for_8_bytes(kept_end);
        let name = field_start + 8..field_start + 8 + sender.len();
        let new_fields_end = name.end + 1;
        let body_offset = new_fields_end + padding_for_8_bytes(new_fields_end);
        let body = &bytes[self.inner.body_offset..];
        let len = body_offset + body.len();
        if len > MAX_MESSAGE_SIZE {
            return Err(Error::ExcessData);
        }

        let u32_bytes = |value: usize| {
            let mut bytes = [0; 4];
            endian.write_u32(&mut bytes, value as u32);

            bytes
        };
        let mut new = Vec::with_capacity(len);
        new.extend_from_slice(&bytes[..PRIMARY_HEADER_SIZE]);
        new.extend_from_slice(&u32_bytes(new_fields_end - fields_start));
        new.extend_from_slice(&bytes[kept]);
        new.extend_from_slice(&bytes[rest]);
        new.resize(field_start, 0);
        // The code of the field, and the signature of its value: 1 type, a string.
        new.extend_from_slice(&[FieldCode::Sender as u8, 1, b's', 0]);
        new.extend_from_slice(&u32_bytes(sender.len()));
        new.extend_from_slice(sender.as_bytes());
        new.push(0);
        new.resize(body_offset, 0);
        new.extend_from_slice(body);

        #[cfg(unix)]
        let new = serialized::Data::new_fds(
            new,
            data.context(),
            data.fds()
                .iter()
                .map(|fd| fd.try_to_owned())
                .collect::<Result<Vec<_>>>()?,
        );
        #[cfg(not(unix))]
        let new = serialized::Data::new(new, data.context());

        match quick_fields.sender_range() {
            // The fields that the message keeps lie where they did, so that what parsing them found
            // holds of the new message, but for its sender field.
            None => Ok(Self {
                inner: Arc::new(Inner {
                    primary_header: self.inner.primary_header.clone(),
                    quick_fields: quick_fields.with_sender(name).into(),
                    bytes: new,
                    body_offset,
                    recv_seq: self.inner.recv_seq,
                }),
            }),
            // The fields after the sender field that the message had moved.
            Some(_) => Self::from_raw_parts(new, self.inner.recv_seq.recv_seq),
        }
    }

    fn quick_fields(&self) -> &QuickFields {
        // Only a message that zbus built has its header parsed this late, and its header is valid,
        // unless it was built with a name or path from an unchecked constructor, such as
        // `ObjectPath::from_str_unchecked`.
        self.inner.quick_fields.get_or_init(|| {
            let (_, _, quick_fields) =
                parser::parse(&self.inner.bytes).expect("Invalid field in a built message");

            quick_fields
        })
    }
}

/// Check what makes the message in `bytes` whole, beyond each field of its header, which parsing
/// the header checks: its protocol version, the fields that its type requires, the padding between
/// its header and its body, at `padding`, which must be 0, and, on Unix, that as many file
/// descriptors as its header says came with it, of which there are `fds`.
///
/// Only a received message is checked so: one that zbus builds may lack a field that the
/// specification requires, if it was built so.
fn check_whole(
    primary_header: &PrimaryHeader,
    quick_fields: &QuickFields,
    bytes: &[u8],
    #[cfg(unix)] fds: usize,
    padding: Range<usize>,
) -> Result<()> {
    if primary_header.protocol_version() != PROTOCOL_VERSION {
        return Err(Error::InvalidField);
    }
    quick_fields.check_required(primary_header.msg_type())?;
    let Some(padding) = bytes.get(padding) else {
        return Err(Error::OutOfBounds);
    };
    if let Some(byte) = padding.iter().find(|byte| **byte != 0) {
        return Err(Error::PaddingNot0(*byte));
    }
    #[cfg(unix)]
    if fds < quick_fields.unix_fds().unwrap_or(0) as usize {
        return Err(Error::MissingParameter("Missing file descriptors"));
    }

    Ok(())
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut msg = f.debug_struct("Msg");
        let h = self.header();
        msg.field("type", &h.message_type());
        msg.field("serial", &self.primary_header().serial_num());
        if let Some(sender) = h.sender() {
            msg.field("sender", &sender);
        }
        if let Some(serial) = h.reply_serial() {
            msg.field("reply-serial", &serial);
        }
        if let Some(path) = h.path() {
            msg.field("path", &path);
        }
        if let Some(iface) = h.interface() {
            msg.field("iface", &iface);
        }
        if let Some(member) = h.member() {
            msg.field("member", &member);
        }
        match self.body().signature() {
            crate::Signature::Unit => (),
            s => {
                msg.field("body", &s);
            }
        }
        #[cfg(unix)]
        {
            msg.field("fds", &self.data().fds());
        }
        msg.finish()
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header = self.header();
        let (ty, error_name, sender, member) = (
            header.message_type(),
            header.error_name(),
            header.sender(),
            header.member(),
        );

        match ty {
            Type::MethodCall => {
                write!(f, "Method call")?;
                if let Some(m) = member {
                    write!(f, " {m}")?;
                }
            }
            Type::MethodReturn => {
                write!(f, "Method return")?;
            }
            Type::Error => {
                write!(f, "Error")?;
                if let Some(e) = error_name {
                    write!(f, " {e}")?;
                }

                let body = self.body();
                let msg = body.deserialize_ignore_signature::<&str>();
                if let Ok(msg) = msg {
                    write!(f, ": {msg}")?;
                }
            }
            Type::Signal => {
                write!(f, "Signal")?;
                if let Some(m) = member {
                    write!(f, " {m}")?;
                }
            }
        }

        if let Some(s) = sender {
            write!(f, " from {s}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use crate::Fd;
    use crate::Signature;
    use std::num::NonZeroU32;
    #[cfg(unix)]
    use std::os::fd::{AsFd, AsRawFd, OwnedFd};
    use test_log::test;

    use super::{Message, Sequence, Type, parser};
    use crate::{
        Error,
        wire::{
            BE, Endian, LE,
            serialized::{Context, Data},
        },
    };

    #[test]
    fn test() {
        #[cfg(unix)]
        let stdout = std::io::stdout();
        let m = Message::method_call("/", "do")
            .sender(":1.72")
            .build(&(
                #[cfg(unix)]
                Fd::from(&stdout),
                "foo",
            ))
            .unwrap();
        #[cfg(unix)]
        assert_eq!(
            m.body().signature(),
            &Signature::static_structure(&[&Signature::Fd, &Signature::Str]),
        );
        #[cfg(not(unix))]
        assert_eq!(m.body().signature(), &Signature::Str,);
        #[cfg(unix)]
        {
            let fds = m.data().fds();
            assert_eq!(fds.len(), 1);
            // FDs get dup'ed so it has to be a different FD now.
            assert_ne!(fds[0].as_fd().as_raw_fd(), stdout.as_raw_fd());
        }

        let body: Result<u32, Error> = m.body().deserialize();
        assert!(matches!(body.unwrap_err(), Error::SignatureMismatch(..)));

        assert_eq!(m.to_string(), "Method call do from :1.72");
        let r = Message::method_return(&m.header())
            .build(&("all fine!"))
            .unwrap();
        assert_eq!(r.to_string(), "Method return");
        let e = Message::error(&m.header(), "org.freedesktop.zbus.Error")
            .build(&("kaboom!", 32))
            .unwrap();
        assert_eq!(e.to_string(), "Error org.freedesktop.zbus.Error: kaboom!");
    }

    #[test]
    fn error_for_serial() {
        let call = Message::method_call("/", "do")
            .sender(":1.72")
            .serial(NonZeroU32::new(5).unwrap())
            .build(&())
            .unwrap();
        let name = "org.freedesktop.zbus.Error";
        let from_header = Message::error(&call.header(), name)
            .build(&("kaboom!", 32))
            .unwrap();
        let sender = call.header().sender().unwrap().clone();
        let serial = call.header().primary().serial_num();

        let from_serial = Message::error_for_serial(sender, serial, name)
            .build(&("kaboom!", 32))
            .unwrap();

        let (expected, got) = (from_header.header(), from_serial.header());
        assert_eq!(got.message_type(), Type::Error);
        assert_eq!(got.destination(), expected.destination());
        assert_eq!(got.reply_serial(), Some(serial));
        assert_eq!(got.reply_serial(), expected.reply_serial());
        assert_eq!(got.error_name(), expected.error_name());
        assert_eq!(
            from_serial.body().deserialize::<(String, i32)>().unwrap(),
            from_header.body().deserialize::<(String, i32)>().unwrap(),
        );
        assert_eq!(
            from_serial.body().signature(),
            from_header.body().signature()
        );
    }

    #[cfg(feature = "bus-impl")]
    #[test]
    fn with_sender() {
        use crate::{
            names::UniqueName,
            wire::{BE, LE},
        };

        let sender = UniqueName::try_from(":busd.42").unwrap();
        for endian in [LE, BE] {
            #[cfg(unix)]
            let stdout = std::io::stdout();
            let call = || {
                Message::method_call("/org/zbus/Test", "Test")
                    .destination("org.zbus.Test")
                    .interface("org.zbus.Test")
                    .endian(endian)
            };
            let built = call().build(&(7u32, "seven")).unwrap();
            let messages = [
                // Without a sender field, as most peers send messages to a bus.
                built.clone(),
                Message::signal("/org/zbus/Test", "org.zbus.Test", "Test")
                    .endian(endian)
                    .build(&())
                    .unwrap(),
                Message::method_return(&built.header())
                    .endian(endian)
                    .build(&[7u8; 13])
                    .unwrap(),
                Message::error(&built.header(), "org.zbus.Error")
                    .endian(endian)
                    .build(&"seven")
                    .unwrap(),
                #[cfg(unix)]
                call().build(&(Fd::from(&stdout), 7u8)).unwrap(),
                // With another sender field, before the signature field, and with the same one.
                call().sender(":1.7").build(&"seven").unwrap(),
                call().sender(sender.clone()).build(&"seven").unwrap(),
                // With another sender field, as its last field, as the message has no body.
                Message::method_return(&built.header())
                    .sender(":1.7")
                    .endian(endian)
                    .build(&())
                    .unwrap(),
            ];
            for msg in messages {
                let new = msg.with_sender(&sender).unwrap();
                assert_eq!(new.header().sender(), Some(&sender));
                assert_eq!(new.body().data().bytes(), msg.body().data().bytes());
                assert_eq!(new.recv_position(), msg.recv_position());
                new.body().validate().unwrap();

                // The header is that of the message built with the sender, and a recipient reads
                // the same from the bytes of the new message.
                let header = msg.header();
                let signature = header.signature().clone();
                #[cfg(unix)]
                let fds = msg
                    .data()
                    .fds()
                    .iter()
                    .map(|fd| fd.try_to_owned().unwrap().into())
                    .collect();
                let builder = super::Builder::from(header).sender(sender.clone());
                // SAFETY: The body and its file descriptors are those of a built message.
                let expected = unsafe {
                    builder.build_raw_body(
                        msg.body().data().bytes(),
                        signature,
                        #[cfg(unix)]
                        fds,
                    )
                }
                .unwrap();
                let received = Message::from_raw_parts(new.data().clone(), 0).unwrap();
                for header in [new.header(), received.header()] {
                    assert_eq!(format!("{header:?}"), format!("{:?}", expected.header()));
                }
                #[cfg(unix)]
                assert_eq!(new.data().fds().len(), msg.data().fds().len());
            }
        }
    }

    /// A message that nothing else holds gets its position in place, and one held elsewhere
    /// becomes a new message, which leaves the position of the one held elsewhere as it was.
    #[cfg(feature = "p2p")]
    #[test]
    fn at_position() {
        use std::sync::Arc;

        use super::Sequence;

        let msg = Message::method_call("/", "Test").build(&()).unwrap();
        let held = msg.clone();

        let msg = msg.at_position(1);
        assert_eq!(msg.recv_position(), Sequence { recv_seq: 1 });
        assert_eq!(held.recv_position(), Sequence::default());
        assert!(!Arc::ptr_eq(&msg.inner, &held.inner));
        assert_eq!(msg.data().bytes(), held.data().bytes());

        drop(held);
        let inner = Arc::as_ptr(&msg.inner);
        let msg = msg.at_position(2);
        assert_eq!(msg.recv_position(), Sequence { recv_seq: 2 });
        assert_eq!(Arc::as_ptr(&msg.inner), inner, "the message was copied");
    }

    #[test]
    fn invalid_path_field() {
        // `/org.zbus/Test` is not a valid object path, so a message with it as its path is
        // rejected as it's received, rather than reading its header failing later.
        let m = Message::method_call("/org/zbus/Test", "Ping")
            .build(&())
            .unwrap();
        let mut bytes = m.data().bytes().to_vec();
        let path = bytes
            .windows(14)
            .position(|w| w == b"/org/zbus/Test")
            .unwrap();
        bytes[path + 4] = b'.';
        let data = Data::new(bytes, m.data().context());

        let error = Message::from_raw_parts(data, 0).unwrap_err();
        assert_eq!(error, Error::InvalidObjectPath);
    }

    #[test]
    fn protocol_version() {
        let m = Message::method_call("/org/zbus/Test", "Ping")
            .build(&())
            .unwrap();
        let mut bytes = m.data().bytes().to_vec();
        bytes[3] = 2;
        let data = Data::new(bytes, m.data().context());

        let error = Message::from_raw_parts(data, 0).unwrap_err();
        assert_eq!(error, Error::InvalidField);
    }

    #[test]
    fn required_fields() {
        // A method call with only the fields that it requires, which aren't those that another
        // type of message requires.
        let m = Message::method_call("/org/zbus/Test", "Ping")
            .build(&())
            .unwrap();
        Message::from_raw_parts(m.data().clone(), 0).unwrap();
        for msg_type in [Type::MethodReturn, Type::Error, Type::Signal] {
            let mut bytes = m.data().bytes().to_vec();
            bytes[1] = msg_type as u8;
            let data = Data::new(bytes, m.data().context());

            let error = Message::from_raw_parts(data, 0).unwrap_err();
            assert_eq!(error, Error::MissingField, "{msg_type:?}");
        }

        // The builder makes a reply without a reply serial, if asked to.
        let reply = Message::method_return(&m.header())
            .reply_serial(None)
            .build(&())
            .unwrap();
        let error = Message::from_raw_parts(reply.data().clone(), 0).unwrap_err();
        assert_eq!(error, Error::MissingField);
    }

    #[test]
    fn body_padding() {
        let m = Message::method_call("/org/zbus/Test", "Ping")
            .build(&"Pong")
            .unwrap();
        let (_, header_len, _) = parser::parse(m.data()).unwrap();
        assert_ne!(
            header_len, m.inner.body_offset,
            "the header needs no padding"
        );

        let mut bytes = m.data().bytes().to_vec();
        bytes[header_len] = 7;
        let data = Data::new(bytes, m.data().context());
        let error = Message::from_raw_parts(data, 0).unwrap_err();
        assert_eq!(error, Error::PaddingNot0(7));

        // Bytes that end with the header, without the padding after it.
        let bytes = m.data().bytes()[..header_len].to_vec();
        let data = Data::new(bytes, m.data().context());
        let error = Message::from_raw_parts(data, 0).unwrap_err();
        assert_eq!(error, Error::OutOfBounds);
    }

    #[cfg(unix)]
    #[test]
    fn missing_fds() {
        let stdout = std::io::stdout();
        let m = Message::method_call("/org/zbus/Test", "Ping")
            .build(&(Fd::from(&stdout), Fd::from(&stdout)))
            .unwrap();
        let fds = m
            .data()
            .fds()
            .iter()
            .map(|fd| fd.try_clone().unwrap())
            .collect::<Vec<_>>();
        let bytes = m.data().bytes().to_vec();
        let data = Data::new_fds(bytes.clone(), m.data().context(), fds);
        Message::from_raw_parts(data, 0).unwrap();

        let fds = vec![m.data().fds()[0].try_clone().unwrap()];
        let data = Data::new_fds(bytes, m.data().context(), fds);
        let error = Message::from_raw_parts(data, 0).unwrap_err();
        assert!(matches!(error, Error::MissingParameter(_)), "{error:?}");
    }

    /// A message made of the bytes of another is that message, in the byte order of their
    /// context only.
    #[test]
    fn try_from_bytes() {
        let msg = Message::method_call("/org/zbus/Test", "Test")
            .interface("org.zbus.Test")
            .build(&(7u32, "seven"))
            .unwrap();

        let copy = Message::try_from_bytes(msg.data().clone()).unwrap();
        assert_eq!(
            format!("{:?}", copy.header()),
            format!("{:?}", msg.header())
        );
        let body: (u32, String) = copy.body().deserialize().unwrap();
        assert_eq!(body, (7, "seven".to_string()));
        assert_eq!(copy.recv_position(), Sequence::default());

        let other = match msg.data().context().endian() {
            Endian::Little => BE,
            Endian::Big => LE,
        };
        let data = Data::new(msg.data().to_vec(), Context::new(other, 0));
        let error = Message::try_from_bytes(data).unwrap_err();
        assert_eq!(error, Error::IncorrectEndian);

        // Bytes that end before the end of the message that their header tells, or after it.
        let bytes = msg.data().bytes();
        let context = msg.data().context();
        let short = Data::new(bytes[..bytes.len() - 1].to_vec(), context);
        let error = Message::try_from_bytes(short).unwrap_err();
        assert_eq!(error, Error::OutOfBounds);
        let long = Data::new([bytes, &[0]].concat(), context);
        let error = Message::try_from_bytes(long).unwrap_err();
        assert_eq!(error, Error::ExcessData);
    }

    /// A message made of bytes carries as many file descriptors as its header counts: fewer, or
    /// more, are an error.
    #[cfg(unix)]
    #[test]
    fn try_from_bytes_with_fds() {
        let stdout = std::io::stdout();
        let msg = Message::method_call("/org/zbus/Test", "Test")
            .build(&Fd::from(&stdout))
            .unwrap();
        let data = |count: usize| {
            let fds = (0..count).map(|_| OwnedFd::from(std::io::pipe().unwrap().0));

            Data::new_fds(msg.data().to_vec(), msg.data().context(), fds)
        };

        let copy = Message::try_from_bytes(data(1)).unwrap();
        assert_eq!(copy.data().fds().len(), 1);
        let error = Message::try_from_bytes(data(0)).unwrap_err();
        assert!(matches!(error, Error::MissingParameter(_)), "{error:?}");
        let error = Message::try_from_bytes(data(2)).unwrap_err();
        assert_eq!(error, Error::ExcessData);
    }

    /// Each message is taken out of the bytes that came in once it is whole in them, and nothing
    /// is taken until then.
    #[test]
    fn take_from() {
        let msgs = [1u32, 2].map(|n| Message::method_call("/", "Test").build(&n).unwrap());
        let mut bytes = [&msgs[0].data()[..], &msgs[1].data()[..]].concat();
        #[cfg(unix)]
        let mut fds = vec![];

        // Short of a primary header, and short of the whole message.
        for len in [0, 10, msgs[0].data().len() - 1] {
            let mut part = bytes[..len].to_vec();

            let taken = Message::take_from(
                &mut part,
                #[cfg(unix)]
                &mut fds,
            );

            assert!(taken.unwrap().is_none(), "{len} bytes");
            assert_eq!(part, &bytes[..len]);
        }

        // Two messages back to back, each whole.
        for sent in &msgs {
            let taken = Message::take_from(
                &mut bytes,
                #[cfg(unix)]
                &mut fds,
            )
            .unwrap()
            .unwrap();

            assert_eq!(taken.data().bytes(), sent.data().bytes());
            assert_eq!(
                taken.header().primary().serial_num(),
                sent.primary_header().serial_num()
            );
            assert_eq!(taken.recv_position(), Sequence::default());
        }
        assert!(bytes.is_empty());
    }

    /// Each message takes as many of the file descriptors that came in as its header counts, the
    /// first of them, and leaves the others for the messages after it.
    #[cfg(unix)]
    #[test]
    fn take_from_with_fds() {
        let stdout = std::io::stdout();
        let (first, second, third): (Fd<'_>, Fd<'_>, Fd<'_>) =
            ((&stdout).into(), (&stdout).into(), (&stdout).into());
        let msgs = [
            Message::method_call("/", "Two")
                .build(&(first, second))
                .unwrap(),
            Message::method_call("/", "None").build(&()).unwrap(),
            Message::method_call("/", "One").build(&third).unwrap(),
        ];
        let mut bytes = msgs
            .iter()
            .flat_map(|msg| msg.data().bytes())
            .copied()
            .collect();
        // The descriptors that came in with the bytes, and one more, which comes before the
        // message that takes it.
        let mut fds: Vec<OwnedFd> = (0..4).map(|_| std::io::pipe().unwrap().0.into()).collect();
        let raw: Vec<_> = fds.iter().map(AsRawFd::as_raw_fd).collect();

        for expected in [&raw[..2], &[], &raw[2..3]] {
            let taken = Message::take_from(&mut bytes, &mut fds).unwrap().unwrap();

            let taken: Vec<_> = taken
                .data()
                .fds()
                .iter()
                .map(|fd| fd.as_fd().as_raw_fd())
                .collect();
            assert_eq!(taken, expected);
        }
        assert!(bytes.is_empty());
        assert_eq!(
            fds.iter().map(AsRawFd::as_raw_fd).collect::<Vec<_>>(),
            &raw[3..]
        );
    }

    /// A message whose header counts more file descriptors than came in is an error, which takes
    /// nothing out of the bytes or the file descriptors.
    #[cfg(unix)]
    #[test]
    fn take_from_with_too_few_fds() {
        let stdout = std::io::stdout();
        let (first, second): (Fd<'_>, Fd<'_>) = ((&stdout).into(), (&stdout).into());
        let msg = Message::method_call("/", "Two")
            .build(&(first, second))
            .unwrap();
        let mut bytes = msg.data().to_vec();
        let mut fds: Vec<OwnedFd> = vec![std::io::pipe().unwrap().0.into()];

        let error = Message::take_from(&mut bytes, &mut fds).unwrap_err();

        assert!(matches!(error, Error::MissingParameter(_)), "{error:?}");
        assert_eq!(bytes, msg.data().bytes());
        assert_eq!(fds.len(), 1);
    }

    /// Bytes that start with no valid message are an error, which takes nothing out of them.
    #[test]
    fn take_from_invalid() {
        let msg = Message::method_call("/", "Test").build(&()).unwrap();
        // An unknown endian signature, and a version of the protocol that there is none of.
        for (at, byte, expected) in [
            (0, b'x', Error::IncorrectEndian),
            (3, 2, Error::InvalidField),
        ] {
            let mut bytes = msg.data().to_vec();
            bytes[at] = byte;
            let sent = bytes.clone();

            let error = Message::take_from(
                &mut bytes,
                #[cfg(unix)]
                &mut vec![],
            )
            .unwrap_err();

            assert_eq!(error, expected);
            assert_eq!(bytes, sent);
        }
    }
}
