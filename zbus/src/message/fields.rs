use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error, SeqAccess, Visitor},
    ser::{SerializeSeq, SerializeStruct},
};
use std::{borrow::Cow, num::NonZeroU32};

use crate::{
    ObjectPath, Signature, Type, Variant,
    message::FieldCode,
    names::{BusName, ErrorName, InterfaceName, MemberName, UniqueName},
    wire::as_variant,
};

/// A collection of [`Field`] instances.
///
/// [`Field`]: enum.Field.html
#[derive(Debug, Default, Clone, Type)]
#[zbus(signature = "a(yv)")]
pub(crate) struct Fields<'f> {
    pub path: Option<ObjectPath<'f>>,
    pub interface: Option<InterfaceName<'f>>,
    pub member: Option<MemberName<'f>>,
    pub error_name: Option<ErrorName<'f>>,
    pub reply_serial: Option<NonZeroU32>,
    pub destination: Option<BusName<'f>>,
    pub sender: Option<UniqueName<'f>>,
    pub signature: Cow<'f, Signature>,
    pub unix_fds: Option<u32>,
}

impl Fields<'_> {
    /// Create an empty collection of fields.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Serialize for Fields<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        if let Some(path) = &self.path {
            seq.serialize_element(&(FieldCode::Path, Variant::from(path)))?;
        }
        if let Some(interface) = &self.interface {
            seq.serialize_element(&(FieldCode::Interface, Variant::from(interface.as_str())))?;
        }
        if let Some(member) = &self.member {
            seq.serialize_element(&(FieldCode::Member, Variant::from(member.as_str())))?;
        }
        if let Some(error_name) = &self.error_name {
            seq.serialize_element(&(FieldCode::ErrorName, Variant::from(error_name.as_str())))?;
        }
        if let Some(reply_serial) = self.reply_serial {
            seq.serialize_element(&(FieldCode::ReplySerial, Variant::from(reply_serial.get())))?;
        }
        if let Some(destination) = &self.destination {
            seq.serialize_element(&(FieldCode::Destination, Variant::from(destination.as_str())))?;
        }
        if let Some(sender) = &self.sender {
            seq.serialize_element(&(FieldCode::Sender, Variant::from(sender.as_str())))?;
        }
        if !matches!(&*self.signature, Signature::Unit) {
            seq.serialize_element(&(FieldCode::Signature, SignatureSerializer(&self.signature)))?;
        }
        if let Some(unix_fds) = self.unix_fds {
            seq.serialize_element(&(FieldCode::UnixFDs, Variant::from(unix_fds)))?;
        }
        seq.end()
    }
}

/// Our special serializer for [`Variant::Signature`].
///
/// Normally `Variant` would use the default serializer for `Signature`, which will include the `()`
/// for strucutures but for body signature, that's what what the D-Bus expects so we do the same as
/// `Variant` here, except we serialize signature value as string w/o the `()`.
#[derive(Debug, Type)]
#[zbus(signature = "v")]
struct SignatureSerializer<'a>(&'a Signature);

impl Serialize for SignatureSerializer<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut structure = serializer.serialize_struct("Variant", 2)?;

        structure.serialize_field("signature", &Signature::Signature)?;

        let signature_str = self.0.to_string_no_parens();
        structure.serialize_field("value", &signature_str)?;

        structure.end()
    }
}

impl<'de: 'f, 'f> Deserialize<'de> for Fields<'f> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(FieldsVisitor)
    }
}

struct FieldsVisitor;

impl<'de> Visitor<'de> for FieldsVisitor {
    type Value = Fields<'de>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("D-Bus message header fields")
    }

    fn visit_seq<V>(self, mut visitor: V) -> Result<Fields<'de>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let mut fields = Fields::new();
        while let Some(field) = visitor.next_element::<Field<'de>>()? {
            match field {
                Field::Path(path) => fields.path = Some(path),
                Field::Interface(interface) => fields.interface = Some(interface),
                Field::Member(member) => fields.member = Some(member),
                Field::ErrorName(error_name) => fields.error_name = Some(error_name),
                Field::ReplySerial(reply_serial) => fields.reply_serial = Some(reply_serial),
                Field::Destination(destination) => fields.destination = Some(destination),
                Field::Sender(sender) => fields.sender = Some(sender),
                Field::Signature(signature) => fields.signature = Cow::Owned(signature),
                Field::UnixFds(unix_fds) => fields.unix_fds = Some(unix_fds),
            }
        }

        Ok(fields)
    }
}

/// A header field: a `(code, variant)` structure, whose code tells the one type that its variant
/// can hold, and so the type of the value.
///
/// The value is deserialized as that type, in a variant whose signature has to be that type
/// ([`as_variant::Deserialize`]), rather than as a [`Variant`](enum@Variant), as which it could take
/// far more memory than its bytes, such as 64 bytes for each byte of an `ay` on 64-bit targets. In
/// the D-Bus format, a variant of another type is so refused from its signature, before its value
/// is read.
enum Field<'f> {
    Path(ObjectPath<'f>),
    Interface(InterfaceName<'f>),
    Member(MemberName<'f>),
    ErrorName(ErrorName<'f>),
    ReplySerial(NonZeroU32),
    Destination(BusName<'f>),
    Sender(UniqueName<'f>),
    Signature(Signature),
    UnixFds(u32),
}

impl<'de: 'f, 'f> Deserialize<'de> for Field<'f> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(2, FieldVisitor)
    }
}

struct FieldVisitor;

impl<'de> Visitor<'de> for FieldVisitor {
    type Value = Field<'de>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a D-Bus message header field")
    }

    fn visit_seq<V>(self, mut visitor: V) -> Result<Field<'de>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let code = visitor
            .next_element::<FieldCode>()?
            .ok_or_else(|| V::Error::invalid_length(0, &self))?;
        let field = match code {
            FieldCode::Path => value(&mut visitor)?.map(Field::Path),
            FieldCode::Interface => value(&mut visitor)?.map(Field::Interface),
            FieldCode::Member => value(&mut visitor)?.map(Field::Member),
            FieldCode::ErrorName => value(&mut visitor)?.map(Field::ErrorName),
            FieldCode::ReplySerial => value(&mut visitor)?.map(Field::ReplySerial),
            FieldCode::Destination => value(&mut visitor)?.map(Field::Destination),
            FieldCode::Sender => value(&mut visitor)?.map(Field::Sender),
            FieldCode::Signature => value(&mut visitor)?.map(Field::Signature),
            FieldCode::UnixFDs => value(&mut visitor)?.map(Field::UnixFds),
        };

        field.ok_or_else(|| V::Error::invalid_length(1, &self))
    }
}

/// The value of a field, of type `T`, out of the variant that holds it, which is next in
/// `visitor`.
fn value<'de, V, T>(visitor: &mut V) -> Result<Option<T>, V::Error>
where
    V: SeqAccess<'de>,
    T: Type + Deserialize<'de> + 'de,
{
    let value = visitor.next_element::<as_variant::Deserialize<'de, T>>()?;

    Ok(value.map(|value| value.0))
}
