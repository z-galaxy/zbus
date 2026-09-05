#[cfg(unix)]
use crate::OwnedFd;
use std::{
    borrow::Cow,
    io::{Cursor, Write},
    num::NonZeroU32,
    sync::Arc,
};

use enumflags2::BitFlags;

use crate::{
    DynamicType, Error, ObjectPath, Result, Signature,
    message::{EndianSig, Fields, Flags, Header, Message, PrimaryHeader, Sequence, Type},
    names::{BusName, ErrorName, InterfaceName, MemberName, UniqueName},
    utils::padding_for_8_bytes,
    wire::{Endian, serialized, serialized::Context},
};

use crate::message::header::MAX_MESSAGE_SIZE;

macro_rules! dbus_context {
    ($self:ident, $n_bytes_before: expr) => {
        Context::new($self.header.primary().endian_sig().into(), $n_bytes_before)
    };
}

/// A builder for a [`Message`].
#[derive(Debug, Clone)]
pub struct Builder<'a> {
    header: Header<'a>,
}

impl<'a> Builder<'a> {
    pub(super) fn new(msg_type: Type) -> Self {
        let primary = PrimaryHeader::new(msg_type, 0);
        let fields = Fields::new();
        let header = Header::new(primary, fields);
        Self { header }
    }

    /// Add flags to the message.
    ///
    /// See [`Flags`] documentation for the meaning of the flags.
    ///
    /// The function will return an error if invalid flags are given for the message type.
    pub fn with_flags(mut self, flag: Flags) -> Result<Self> {
        if self.header.message_type() != Type::MethodCall
            && BitFlags::from_flag(flag).contains(Flags::NoReplyExpected)
        {
            return Err(Error::InvalidField);
        }
        let flags = self.header.primary().flags() | flag;
        self.header.primary_mut().set_flags(flags);
        Ok(self)
    }

    /// Set the unique name of the sending connection.
    pub fn sender<'s: 'a, S>(mut self, sender: S) -> Result<Self>
    where
        S: TryInto<UniqueName<'s>>,
        S::Error: Into<Error>,
    {
        self.header.fields_mut().sender = Some(sender.try_into().map_err(Into::into)?);
        Ok(self)
    }

    /// Set the object to send a call to, or the object a signal is emitted from.
    pub fn path<'p: 'a, P>(mut self, path: P) -> Result<Self>
    where
        P: TryInto<ObjectPath<'p>>,
        P::Error: Into<Error>,
    {
        self.header.fields_mut().path = Some(path.try_into().map_err(Into::into)?);
        Ok(self)
    }

    /// Set the interface to invoke a method call on, or that a signal is emitted from.
    pub fn interface<'i: 'a, I>(mut self, interface: I) -> Result<Self>
    where
        I: TryInto<InterfaceName<'i>>,
        I::Error: Into<Error>,
    {
        self.header.fields_mut().interface = Some(interface.try_into().map_err(Into::into)?);
        Ok(self)
    }

    /// Set the member, either the method name or signal name.
    pub fn member<'m: 'a, M>(mut self, member: M) -> Result<Self>
    where
        M: TryInto<MemberName<'m>>,
        M::Error: Into<Error>,
    {
        self.header.fields_mut().member = Some(member.try_into().map_err(Into::into)?);
        Ok(self)
    }

    pub(super) fn error_name<'e: 'a, E>(mut self, error: E) -> Result<Self>
    where
        E: TryInto<ErrorName<'e>>,
        E::Error: Into<Error>,
    {
        self.header.fields_mut().error_name = Some(error.try_into().map_err(Into::into)?);
        Ok(self)
    }

    /// Set the name of the connection this message is intended for.
    pub fn destination<'d: 'a, D>(mut self, destination: D) -> Result<Self>
    where
        D: TryInto<BusName<'d>>,
        D::Error: Into<Error>,
    {
        self.header.fields_mut().destination = Some(destination.try_into().map_err(Into::into)?);
        Ok(self)
    }

    /// Override the generated or inherited serial.  This is a low level modification,
    /// generally you should not need to use this.
    pub fn serial(mut self, serial: NonZeroU32) -> Self {
        self.header.primary_mut().set_serial_num(serial);
        self
    }

    /// Override the reply serial. This is a low level modification, generally you should use
    /// `Message::method_return` instead.
    pub fn reply_serial(mut self, serial: Option<NonZeroU32>) -> Self {
        self.header.fields_mut().reply_serial = serial;
        self
    }

    pub(super) fn reply_to(mut self, reply_to: &Header<'_>) -> Result<Self> {
        let serial = reply_to.primary().serial_num();
        self.header.fields_mut().reply_serial = Some(serial);
        self = self.endian(reply_to.primary().endian_sig().into());

        if let Some(sender) = reply_to.sender() {
            self.destination(sender.to_owned())
        } else {
            Ok(self)
        }
    }

    /// Set the endianness of the message.
    ///
    /// The default endianness is native.
    pub fn endian(mut self, endian: Endian) -> Self {
        let sig = EndianSig::from(endian);
        self.header.primary_mut().set_endian_sig(sig);

        self
    }

    /// Build the [`Message`] with the given body.
    ///
    /// You may pass `()` as the body if the message has no body.
    ///
    /// The caller is currently required to ensure that the resulting message contains the headers
    /// as compliant with the [specification]. Additional checks may be added to this builder over
    /// time as needed.
    ///
    /// [specification]:
    /// https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-header-fields
    pub fn build<B>(self, body: &B) -> Result<Message>
    where
        B: serde::ser::Serialize + DynamicType,
    {
        let ctxt = dbus_context!(self, 0);
        let signature = body.signature();

        // The header carries the body's length and FD count, so the body is serialized first,
        // into its own buffer, and copied into place behind the header. One pass over the body
        // plus a copy of its bytes beats the extra serialization pass that measuring it first
        // would cost, and keeps this generic function down to the serialization itself.
        let mut body_bytes = Vec::new();
        let mut cursor = Cursor::new(&mut body_bytes);
        // SAFETY: The FDs end up in the same Message as the body.
        let written =
            unsafe { crate::wire::to_writer_for_signature(&mut cursor, ctxt, &signature, body) }?;
        #[cfg(unix)]
        let fds = written.into_fds();
        #[cfg(not(unix))]
        let _ = written;

        self.build_from_bytes(
            signature,
            &body_bytes,
            #[cfg(unix)]
            fds,
        )
    }

    /// Create a new message from a raw slice of bytes to populate the body with, rather than by
    /// serializing a value. The message body will be the exact bytes.
    ///
    /// # Safety
    ///
    /// This method is unsafe because it can be used to build an invalid message.
    pub unsafe fn build_raw_body<S>(
        self,
        body_bytes: &[u8],
        signature: S,
        #[cfg(unix)] fds: Vec<OwnedFd>,
    ) -> Result<Message>
    where
        S: TryInto<Signature>,
        S::Error: Into<Error>,
    {
        let signature = signature.try_into().map_err(Into::into)?;

        self.build_from_bytes(
            signature,
            body_bytes,
            #[cfg(unix)]
            fds,
        )
    }

    /// Build the message around an already serialized body.
    fn build_from_bytes(
        self,
        signature: Signature,
        body: &[u8],
        #[cfg(unix)] fds: Vec<OwnedFd>,
    ) -> Result<Message> {
        let ctxt = dbus_context!(self, 0);
        let mut header = self.header;

        header.fields_mut().signature = Cow::Owned(signature);

        let body_len_u32 = body.len().try_into().map_err(|_| Error::ExcessData)?;
        header.primary_mut().set_body_len(body_len_u32);

        #[cfg(unix)]
        {
            let fds_len: u32 = fds.len().try_into().map_err(|_| Error::ExcessData)?;
            if fds_len != 0 {
                header.fields_mut().unix_fds = Some(fds_len);
            }
        }

        let hdr_len = *crate::wire::serialized_size(ctxt, &header)?;
        // We need to align the body to 8-byte boundary.
        let body_padding = padding_for_8_bytes(hdr_len);
        let body_offset = hdr_len + body_padding;
        let total_len = body_offset + body.len();
        if total_len > MAX_MESSAGE_SIZE {
            return Err(Error::ExcessData);
        }
        let mut bytes: Vec<u8> = Vec::with_capacity(total_len);
        let mut cursor = Cursor::new(&mut bytes);

        // SAFETY: There are no FDs involved.
        unsafe { crate::wire::to_writer(&mut cursor, ctxt, &header) }?;
        cursor.write_all(&[0u8; 8][..body_padding])?;
        cursor.write_all(body)?;

        let primary_header = header.into_primary();
        #[cfg(unix)]
        let bytes = serialized::Data::new_fds(bytes, ctxt, fds);
        #[cfg(not(unix))]
        let bytes = serialized::Data::new(bytes, ctxt);

        Ok(Message {
            inner: Arc::new(super::Inner {
                primary_header,
                quick_fields: std::sync::OnceLock::new(),
                bytes,
                body_offset,
                recv_seq: Sequence::default(),
            }),
        })
    }
}

impl<'m> From<Header<'m>> for Builder<'m> {
    fn from(mut header: Header<'m>) -> Self {
        // Signature and Fds are added by body* methods.
        let fields = header.fields_mut();
        fields.signature = Cow::Owned(Signature::Unit);
        fields.unix_fds = None;

        Self { header }
    }
}

#[cfg(test)]
mod tests {
    use super::{Endian, Message};
    use crate::Error;
    use test_log::test;

    #[test]
    fn test_raw() -> Result<(), Error> {
        let raw_body: &[u8] = &[16, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0];
        let message_builder = Message::signal("/", "test.test", "test")?;
        let message_builder = message_builder.endian(Endian::Little);
        let message = unsafe {
            message_builder.build_raw_body(
                raw_body,
                "ai",
                #[cfg(unix)]
                vec![],
            )?
        };

        let output: Vec<i32> = message.body().deserialize()?;
        assert_eq!(output, vec![1, 2, 3, 4]);

        Ok(())
    }
}
