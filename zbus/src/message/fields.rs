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
};
use zbus_utils::object_path;

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
        while let Some(entry) = visitor.next_element::<DecodedField<'de>>()? {
            let DecodedField::Known(code, value) = entry else {
                continue;
            };
            match code {
                FieldCode::Path => {
                    let path = ObjectPath::try_from(value).map_err(V::Error::custom)?;
                    // A variant doesn't check the object path it holds, unlike the name fields.
                    object_path::validate(path.as_bytes())
                        .map_err(|_| V::Error::custom(crate::Error::InvalidObjectPath))?;
                    fields.path = Some(path)
                }
                FieldCode::Interface => {
                    fields.interface =
                        Some(InterfaceName::try_from(value).map_err(V::Error::custom)?)
                }
                FieldCode::Member => {
                    fields.member = Some(MemberName::try_from(value).map_err(V::Error::custom)?)
                }
                FieldCode::ErrorName => {
                    fields.error_name = Some(ErrorName::try_from(value).map_err(V::Error::custom)?)
                }
                FieldCode::ReplySerial => {
                    let value = u32::try_from(value)
                        .map_err(V::Error::custom)
                        .and_then(|v| v.try_into().map_err(V::Error::custom))?;
                    fields.reply_serial = Some(value)
                }
                FieldCode::Destination => {
                    fields.destination = Some(BusName::try_from(value).map_err(V::Error::custom)?)
                }
                FieldCode::Sender => {
                    fields.sender = Some(UniqueName::try_from(value).map_err(V::Error::custom)?)
                }
                FieldCode::Signature => {
                    fields.signature =
                        Cow::Owned(Signature::try_from(value).map_err(V::Error::custom)?)
                }
                FieldCode::UnixFDs => {
                    fields.unix_fds = Some(u32::try_from(value).map_err(V::Error::custom)?)
                }
            }
        }

        Ok(fields)
    }
}

enum DecodedField<'de> {
    Known(FieldCode, Variant<'de>),
    Unknown,
}

impl<'de> Deserialize<'de> for DecodedField<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FieldEntryVisitor;

        impl<'de> Visitor<'de> for FieldEntryVisitor {
            type Value = DecodedField<'de>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a D-Bus message header field")
            }

            fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let code: u8 = visitor.next_element()?.ok_or_else(|| {
                    serde::de::Error::invalid_length(0, &"a header field code and value")
                })?;
                match FieldCode::try_from(code) {
                    Ok(code) => {
                        let value = visitor.next_element()?.ok_or_else(|| {
                            serde::de::Error::invalid_length(1, &"a header field value")
                        })?;

                        Ok(DecodedField::Known(code, value))
                    }
                    Err(()) if code == 0 => {
                        Err(serde::de::Error::custom(crate::Error::InvalidField))
                    }
                    Err(()) => {
                        let _: serde::de::IgnoredAny =
                            visitor.next_element()?.ok_or_else(|| {
                                serde::de::Error::invalid_length(1, &"a header field value")
                            })?;

                        Ok(DecodedField::Unknown)
                    }
                }
            }
        }

        deserializer.deserialize_tuple(2, FieldEntryVisitor)
    }
}
