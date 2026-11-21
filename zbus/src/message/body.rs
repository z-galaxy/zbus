use crate::{
    Message, Result, Signature, Type,
    wire::serialized::{self, Data},
};

/// The body of a message.
///
/// This contains the bytes and the signature of the body.
#[derive(Clone, Debug)]
pub struct Body {
    data: Data<'static, 'static>,
    msg: Message,
}

impl Body {
    pub(super) fn new(data: Data<'static, 'static>, msg: Message) -> Self {
        Self { data, msg }
    }

    /// Deserialize the body using the contained signature.
    pub fn deserialize<'s, B>(&'s self) -> Result<B>
    where
        B: crate::wire::DynamicDeserialize<'s>,
    {
        let body_sig = self.signature();

        self.data
            .deserialize_for_dynamic_signature(body_sig)
            .map(|b| b.0)
    }

    /// Deserialize the body, ignoring its signature.
    ///
    /// Unlike [`Body::deserialize`], the signature of the body is not used to guide the
    /// deserialization: `B`'s own signature is assumed instead. This is useful when the body's
    /// signature is already known to match `B`, or when `B` deserializes from a signature that
    /// differs from the body's but is compatible with its encoding.
    ///
    /// Deserialization still fails with an error if the encoded bytes do not match what `B`
    /// expects, so this is not an unsafe or unchecked operation.
    pub fn deserialize_ignore_signature<'d, 'm: 'd, B>(&'m self) -> Result<B>
    where
        B: serde::de::Deserialize<'d> + Type,
    {
        self.data.deserialize().map(|b| b.0)
    }

    /// Check that the body holds values of the types of its signature, as the D-Bus format lays
    /// them out.
    ///
    /// The header of a message is checked as zbus receives the message, but its body only as far as
    /// deserializing it reads it. A bus passes the bodies of messages on without deserializing
    /// them, and checks them with this instead, so as to pass on well-formed messages only.
    ///
    /// The body is checked from its first byte to its last: that each boolean is 0 or 1, each
    /// string is UTF-8, each object path and each signature is a valid one, and each of those ends
    /// with a 0 byte, the only one in it, each variant holds a value of a single complete type,
    /// each file descriptor is one of those that the header of the message says it brought, each
    /// array is 64 MiB long at most and its elements fill it, each padding byte is 0, that the
    /// signature of each variant nests no more than 32 arrays and 32 structures, as that of the
    /// body does, and that no value is in more than 64 containers, variants and dict entries
    /// included. For that, a variant counts as holding any value of its type, and an array as
    /// holding one even if it's empty, unless its elements are neither numbers nor file
    /// descriptors and it's not in a variant, as GLib has it.
    pub fn validate(&self) -> Result<()> {
        self.msg.validate_body()
    }

    /// The signature of the body.
    pub fn signature(&self) -> &Signature {
        self.msg.quick_fields().signature()
    }

    /// The length of the body in bytes.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the body is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get a reference to the underlying byte encoding of the message.
    pub fn data(&self) -> &serialized::Data<'static, 'static> {
        &self.data
    }

    /// Reference to the message this body belongs to.
    pub fn message(&self) -> &Message {
        &self.msg
    }
}
