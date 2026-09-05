use serde::de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};

use crate::wire::{Dict, Signature, Type, Value};

/// Deserializes a property value directly from its in-memory representation.
#[doc(hidden)]
pub fn deserialize_for_property<'de, T>(value: &'de Value<'_>) -> crate::Result<T>
where
    T: Type + serde::Deserialize<'de>,
{
    T::deserialize(ValueDeserializer::new(value, T::SIGNATURE, false))
}

struct ValueDeserializer<'de, 'sig> {
    value: &'de Value<'de>,
    expected: &'sig Signature,
    unwrap_variant: bool,
    variant_payload: bool,
}

impl<'de, 'sig> ValueDeserializer<'de, 'sig> {
    fn new(value: &'de Value<'de>, expected: &'sig Signature, unwrap_variant: bool) -> Self {
        Self {
            value,
            expected,
            unwrap_variant,
            variant_payload: false,
        }
    }

    fn actual(&self) -> &'de Value<'de> {
        if self.unwrap_variant && self.expected != &Signature::Variant {
            if let Value::Value(value) = self.value {
                return value;
            }
        }

        self.value
    }

    fn check(&self) -> crate::Result<&'de Value<'de>> {
        let value = self.actual();
        if self.expected == &Signature::Variant
            || signatures_compatible(value.value_signature(), self.expected, self.unwrap_variant)
        {
            Ok(value)
        } else {
            Err(crate::Error::signature_mismatch(
                value.value_signature(),
                &self.expected.to_string(),
            ))
        }
    }

    fn variant_access(&self) -> VariantSeqAccess<'de> {
        if self.unwrap_variant || self.variant_payload {
            VariantSeqAccess::new_existing(self.actual())
        } else {
            VariantSeqAccess::new(self.actual())
        }
    }

    #[cfg(feature = "option-as-array")]
    fn child(
        &self,
        value: &'de Value<'de>,
        expected: &'sig Signature,
        unwrap_variant: bool,
    ) -> Self {
        Self::new(value, expected, unwrap_variant)
    }

    fn mismatch(&self, expected: &'static str) -> crate::Error {
        crate::Error::signature_mismatch(self.actual().value_signature(), expected)
    }

    fn bytes(&self) -> crate::Result<Vec<u8>> {
        let expected = self.expected_array_element()?;
        if expected != &Signature::U8 {
            return Err(self.mismatch("an array of bytes"));
        }

        let Value::Array(array) = self.check()? else {
            return Err(self.mismatch("an array of bytes"));
        };
        array
            .inner()
            .iter()
            .map(|value| match Self::new(value, expected, true).check()? {
                Value::U8(value) => Ok(*value),
                _ => Err(self.mismatch("an array of bytes")),
            })
            .collect()
    }

    fn expected_array_element(&self) -> crate::Result<&'sig Signature> {
        match self.expected {
            Signature::Array(element) => Ok(element.signature()),
            _ => Err(self.mismatch("an array")),
        }
    }
}

impl<'de, 'sig> de::Deserializer<'de> for ValueDeserializer<'de, 'sig> {
    type Error = crate::Error;

    fn deserialize_any<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.expected == &Signature::Variant {
            return visitor.visit_seq(self.variant_access());
        }

        match self.check()? {
            Value::U8(value) => visitor.visit_u8(*value),
            Value::Bool(value) => visitor.visit_bool(*value),
            Value::I16(value) => visitor.visit_i16(*value),
            Value::U16(value) => visitor.visit_u16(*value),
            Value::I32(value) => visitor.visit_i32(*value),
            Value::U32(value) => visitor.visit_u32(*value),
            Value::I64(value) => visitor.visit_i64(*value),
            Value::U64(value) => visitor.visit_u64(*value),
            Value::F64(value) => visitor.visit_f64(*value),
            Value::Str(value) => visitor.visit_borrowed_str(value.as_str()),
            Value::Signature(value) => visitor.visit_string(value.to_string()),
            Value::ObjectPath(value) => visitor.visit_borrowed_str(value.as_str()),
            Value::Array(array) => {
                visitor.visit_seq(ArrayAccess::new(array.inner(), self.expected))
            }
            Value::Dict(dict) => visitor.visit_map(DictAccess::new(dict, self.expected)),
            Value::Structure(structure) => {
                visitor.visit_seq(StructureAccess::new(structure.fields(), self.expected))
            }
            #[cfg(unix)]
            Value::Fd(fd) => visitor.visit_i32(std::os::fd::AsRawFd::as_raw_fd(fd)),
            Value::Value(_) => unreachable!("variants are handled above"),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::Bool(value) => visitor.visit_bool(*value),
            _ => Err(self.mismatch("a boolean")),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i16(visitor)
    }

    fn deserialize_i16<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::I16(value) => visitor.visit_i16(*value),
            _ => Err(self.mismatch("an int16")),
        }
    }

    fn deserialize_i32<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::I32(value) => visitor.visit_i32(*value),
            #[cfg(unix)]
            Value::Fd(value) => visitor.visit_i32(std::os::fd::AsRawFd::as_raw_fd(value)),
            _ => Err(self.mismatch("an int32")),
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::I64(value) => visitor.visit_i64(*value),
            _ => Err(self.mismatch("an int64")),
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::U8(value) => visitor.visit_u8(*value),
            _ => Err(self.mismatch("a byte")),
        }
    }

    fn deserialize_u16<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::U16(value) => visitor.visit_u16(*value),
            _ => Err(self.mismatch("a uint16")),
        }
    }

    fn deserialize_u32<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::U32(value) => visitor.visit_u32(*value),
            _ => Err(self.mismatch("a uint32")),
        }
    }

    fn deserialize_u64<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::U64(value) => visitor.visit_u64(*value),
            _ => Err(self.mismatch("a uint64")),
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::F64(value) => {
                if value.is_finite() && *value > f32::MAX as f64 {
                    return Err(de::Error::invalid_value(
                        de::Unexpected::Float(*value),
                        &"Too large for f32",
                    ));
                }
                visitor.visit_f32(*value as f32)
            }
            _ => Err(self.mismatch("a float")),
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::F64(value) => visitor.visit_f64(*value),
            _ => Err(self.mismatch("a double")),
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_str<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::Str(value) => visitor.visit_borrowed_str(value.as_str()),
            Value::Signature(value) => visitor.visit_string(value.to_string()),
            Value::ObjectPath(value) => visitor.visit_borrowed_str(value.as_str()),
            _ => Err(self.mismatch("a string")),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let bytes = self.bytes()?;
        visitor.visit_byte_buf(bytes)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        #[cfg(feature = "option-as-array")]
        {
            let value = self.check()?;
            let array = match value {
                Value::Array(array) => array,
                _ => return Err(self.mismatch("an option array")),
            };
            let element_signature = self.expected_array_element()?;
            match array.inner() {
                [] => visitor.visit_none(),
                [value] => visitor.visit_some(self.child(value, element_signature, true)),
                _ => Err(de::Error::invalid_length(
                    array.len(),
                    &"an option array of 0 or 1 item",
                )),
            }
        }
        #[cfg(not(feature = "option-as-array"))]
        {
            let _ = visitor;
            Err(de::Error::custom(
                "Can only decode Option<T> from a Value if the `option-as-array` feature is \
                 enabled",
            ))
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.check()?;
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.check()?;
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.expected == &Signature::Variant {
            return visitor.visit_seq(self.variant_access());
        }
        match self.check()? {
            Value::Array(array) => {
                visitor.visit_seq(ArrayAccess::new(array.inner(), self.expected))
            }
            Value::Structure(structure) => {
                visitor.visit_seq(StructureAccess::new(structure.fields(), self.expected))
            }
            Value::U8(_) if self.expected == &Signature::U8 => visitor.visit_seq(EmptyAccess),
            _ => Err(self.mismatch("an array or structure")),
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.check()? {
            Value::Dict(dict) => visitor.visit_map(DictAccess::new(dict, self.expected)),
            _ => Err(self.mismatch("a dictionary")),
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if matches!(self.expected, Signature::Dict { .. }) {
            self.deserialize_map(visitor)
        } else {
            self.deserialize_seq(visitor)
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = self.check()?;
        let access = match value {
            Value::Structure(structure) => {
                ValueEnumAccess::structure(structure.fields(), self.expected)?
            }
            _ => ValueEnumAccess::scalar(value, self.expected),
        };
        let _ = name;
        visitor.visit_enum(access)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

struct ArrayAccess<'de> {
    elements: &'de [Value<'de>],
    index: usize,
    expected: Signature,
}

impl<'de> ArrayAccess<'de> {
    fn new(elements: &'de [Value<'de>], expected: &Signature) -> Self {
        let expected = match expected {
            Signature::Array(element) => element.signature().clone(),
            _ => expected.clone(),
        };
        Self {
            elements,
            index: 0,
            expected,
        }
    }
}

impl<'de> SeqAccess<'de> for ArrayAccess<'de> {
    type Error = crate::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> crate::Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        let Some(value) = self.elements.get(self.index) else {
            return Ok(None);
        };
        self.index += 1;
        seed.deserialize(ValueDeserializer::new(value, &self.expected, true))
            .map(Some)
    }
}

struct DictAccess<'de> {
    entries: Box<dyn Iterator<Item = (&'de Value<'de>, &'de Value<'de>)> + 'de>,
    key: Signature,
    value: Signature,
    pending_value: Option<&'de Value<'de>>,
}

impl<'de> DictAccess<'de> {
    fn new(dict: &'de Dict<'de, 'de>, expected: &Signature) -> Self {
        let (key, value) = match expected {
            Signature::Dict { key, value } => (key.signature().clone(), value.signature().clone()),
            _ => (Signature::Variant, Signature::Variant),
        };
        Self {
            entries: Box::new(dict.iter()),
            key,
            value,
            pending_value: None,
        }
    }
}

impl<'de> MapAccess<'de> for DictAccess<'de> {
    type Error = crate::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> crate::Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        let Some((key, value)) = self.entries.next() else {
            return Ok(None);
        };
        self.pending_value = Some(value);
        seed.deserialize(ValueDeserializer::new(key, &self.key, false))
            .map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> crate::Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        let value = self.pending_value.take().ok_or_else(|| {
            crate::Error::Failure("map value requested before map key".to_owned())
        })?;
        seed.deserialize(ValueDeserializer::new(value, &self.value, true))
    }
}

struct StructureAccess<'de> {
    fields: &'de [Value<'de>],
    signatures: Vec<Signature>,
    index: usize,
}

impl<'de> StructureAccess<'de> {
    fn new(fields: &'de [Value<'de>], expected: &Signature) -> Self {
        let signatures = match expected {
            Signature::Structure(signatures) => signatures.iter().cloned().collect(),
            _ => vec![],
        };
        Self {
            fields,
            signatures,
            index: 0,
        }
    }
}

impl<'de> SeqAccess<'de> for StructureAccess<'de> {
    type Error = crate::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> crate::Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        let Some(value) = self.fields.get(self.index) else {
            return Ok(None);
        };
        let Some(signature) = self.signatures.get(self.index) else {
            return Err(de::Error::invalid_length(self.fields.len(), &"a structure"));
        };
        self.index += 1;
        seed.deserialize(ValueDeserializer::new(value, signature, true))
            .map(Some)
    }
}

struct EmptyAccess;

impl<'de> SeqAccess<'de> for EmptyAccess {
    type Error = crate::Error;

    fn next_element_seed<T>(&mut self, _seed: T) -> crate::Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        Ok(None)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(0)
    }
}

struct VariantSeqAccess<'de> {
    signature: Signature,
    value: &'de Value<'de>,
    index: usize,
}

impl<'de> VariantSeqAccess<'de> {
    fn new(value: &'de Value<'de>) -> Self {
        let signature = value.value_signature().clone();
        Self {
            signature,
            value,
            index: 0,
        }
    }

    fn new_existing(value: &'de Value<'de>) -> Self {
        match value {
            Value::Value(value) => Self::new(value),
            value => Self::new(value),
        }
    }
}

impl<'de> SeqAccess<'de> for VariantSeqAccess<'de> {
    type Error = crate::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> crate::Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        match self.index {
            0 => {
                self.index = 1;
                seed.deserialize(serde::de::value::StringDeserializer::<crate::Error>::new(
                    self.signature.to_string(),
                ))
                .map(Some)
            }
            1 => {
                self.index = 2;
                let mut de = ValueDeserializer::new(self.value, &self.signature, false);
                de.variant_payload = self.signature == Signature::Variant;
                seed.deserialize(de).map(Some)
            }
            _ => Ok(None),
        }
    }
}

struct ValueEnumAccess<'de> {
    discriminant: &'de Value<'de>,
    discriminant_signature: Signature,
    payload: Option<&'de Value<'de>>,
    payload_signature: Option<Signature>,
}

impl<'de> ValueEnumAccess<'de> {
    fn scalar(value: &'de Value<'de>, expected: &Signature) -> Self {
        Self {
            discriminant: value,
            discriminant_signature: expected.clone(),
            payload: None,
            payload_signature: None,
        }
    }

    fn structure(value: &'de [Value<'de>], expected: &Signature) -> crate::Result<Self> {
        let Signature::Structure(signatures) = expected else {
            return Err(crate::Error::signature_mismatch(
                expected,
                "an enum structure",
            ));
        };
        let Some(discriminant) = value.first() else {
            return Err(de::Error::invalid_length(0, &"an enum discriminant"));
        };
        let mut signatures = signatures.iter();
        let Some(discriminant_signature) = signatures.next() else {
            return Err(de::Error::invalid_length(0, &"an enum discriminant"));
        };
        let payload = value.get(1);
        let payload_signature = signatures.next();
        if payload.is_some() != payload_signature.is_some() {
            return Err(de::Error::invalid_length(value.len(), &"an enum payload"));
        }
        Ok(Self {
            discriminant,
            discriminant_signature: discriminant_signature.clone(),
            payload,
            payload_signature: payload_signature.cloned(),
        })
    }
}

impl<'de> EnumAccess<'de> for ValueEnumAccess<'de> {
    type Error = crate::Error;
    type Variant = ValueVariantAccess<'de>;

    fn variant_seed<V>(self, seed: V) -> crate::Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        let variant = seed.deserialize(ValueDeserializer::new(
            self.discriminant,
            &self.discriminant_signature,
            false,
        ))?;
        Ok((
            variant,
            ValueVariantAccess {
                payload: self.payload,
                payload_signature: self.payload_signature,
            },
        ))
    }
}

struct ValueVariantAccess<'de> {
    payload: Option<&'de Value<'de>>,
    payload_signature: Option<Signature>,
}

impl<'de> VariantAccess<'de> for ValueVariantAccess<'de> {
    type Error = crate::Error;

    fn unit_variant(self) -> crate::Result<()> {
        if self.payload.is_none() {
            Ok(())
        } else {
            Err(de::Error::invalid_length(2, &"a unit enum"))
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> crate::Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        let Some((value, signature)) = self.payload.zip(self.payload_signature.as_ref()) else {
            return Err(de::Error::invalid_length(1, &"an enum payload"));
        };
        seed.deserialize(ValueDeserializer::new(value, signature, true))
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.fields_access(visitor)
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.fields_access(visitor)
    }
}

impl<'de> ValueVariantAccess<'de> {
    fn fields_access<V>(self, visitor: V) -> crate::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let Some((Value::Structure(structure), Signature::Structure(signatures))) =
            self.payload.zip(self.payload_signature)
        else {
            return Err(de::Error::invalid_type(
                de::Unexpected::Other("non-structure enum payload"),
                &"a structure enum payload",
            ));
        };
        visitor.visit_seq(EnumFieldsAccess::new(
            structure.fields(),
            signatures.iter().cloned().collect(),
        ))
    }
}

struct EnumFieldsAccess<'de> {
    fields: &'de [Value<'de>],
    signatures: Vec<Signature>,
    index: usize,
}

impl<'de> EnumFieldsAccess<'de> {
    fn new(fields: &'de [Value<'de>], signatures: Vec<Signature>) -> Self {
        Self {
            fields,
            signatures,
            index: 0,
        }
    }
}

impl<'de> SeqAccess<'de> for EnumFieldsAccess<'de> {
    type Error = crate::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> crate::Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        let Some(value) = self.fields.get(self.index) else {
            return Ok(None);
        };
        let Some(signature) = self.signatures.get(self.index) else {
            return Err(de::Error::invalid_length(
                self.fields.len(),
                &"an enum payload",
            ));
        };
        self.index += 1;
        seed.deserialize(ValueDeserializer::new(value, signature, true))
            .map(Some)
    }
}

fn signatures_compatible(actual: &Signature, expected: &Signature, unwrap_variant: bool) -> bool {
    if actual == expected {
        return true;
    }
    if unwrap_variant && actual == &Signature::Variant && expected != &Signature::Variant {
        return true;
    }

    match (actual, expected) {
        (Signature::Array(actual), Signature::Array(expected)) => {
            signatures_compatible(actual.signature(), expected.signature(), true)
        }
        (
            Signature::Dict {
                key: actual_key,
                value: actual_value,
            },
            Signature::Dict {
                key: expected_key,
                value: expected_value,
            },
        ) => {
            signatures_compatible(actual_key.signature(), expected_key.signature(), false)
                && signatures_compatible(actual_value.signature(), expected_value.signature(), true)
        }
        (Signature::Structure(actual), Signature::Structure(expected)) => {
            actual.len() == expected.len()
                && actual
                    .iter()
                    .zip(expected.iter())
                    .all(|(actual, expected)| signatures_compatible(actual, expected, false))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde::Deserialize;

    use super::*;
    use crate::wire::{Array, OwnedValue, Structure, Type};

    fn convert_property_value<T>(value: &Value<'_>) -> T
    where
        T: for<'de> Deserialize<'de> + Type,
    {
        deserialize_for_property(value).unwrap()
    }

    #[test]
    fn unwraps_array_variants() {
        let mut array = Array::new(&Signature::Variant);
        array
            .append(Value::Value(Box::new(Value::from("one"))))
            .unwrap();
        array
            .append(Value::Value(Box::new(Value::from("two"))))
            .unwrap();

        assert_eq!(
            convert_property_value::<Vec<String>>(&Value::Array(array)),
            ["one", "two"]
        );
    }

    #[test]
    fn unwraps_variants_in_nested_containers() {
        let mut inner = Array::new(&Signature::Variant);
        inner
            .append(Value::Value(Box::new(Value::from("value"))))
            .unwrap();
        let mut outer = Array::new(inner.signature());
        outer.append(Value::Array(inner)).unwrap();
        let structure = Structure::builder()
            .append_field(Value::Array(outer))
            .build()
            .unwrap();
        let decoded: (Vec<Vec<String>>,) = convert_property_value(&Value::Structure(structure));

        assert_eq!(decoded.0, [vec!["value".to_string()]]);
    }

    #[test]
    fn unwraps_dict_variants() {
        let mut dict = Dict::new(&Signature::Str, &Signature::Variant);
        dict.append(
            Value::from("key"),
            Value::Value(Box::new(Value::from("value"))),
        )
        .unwrap();

        assert_eq!(
            convert_property_value::<HashMap<String, String>>(&Value::Dict(dict))["key"],
            "value"
        );
    }

    #[test]
    fn unwraps_variants_in_nested_dicts() {
        let mut properties = Dict::new(&Signature::Str, &Signature::Variant);
        properties
            .append(
                Value::from("ssid"),
                Value::Value(Box::new(Value::from("home"))),
            )
            .unwrap();
        let mut interfaces = Dict::new(&Signature::Str, properties.signature());
        interfaces
            .append(Value::from("802-11-wireless"), Value::Dict(properties))
            .unwrap();

        let decoded: HashMap<String, HashMap<String, String>> =
            convert_property_value(&Value::Dict(interfaces));
        assert_eq!(decoded["802-11-wireless"]["ssid"], "home");
    }

    #[test]
    fn rejects_unrelated_empty_containers() {
        let array = Value::Array(Array::new(&Signature::I32));
        assert!(deserialize_for_property::<Vec<String>>(&array).is_err());

        let dict = Value::Dict(Dict::new(&Signature::I32, &Signature::Str));
        assert!(deserialize_for_property::<HashMap<String, String>>(&dict).is_err());
    }

    #[test]
    fn preserves_variant_values() {
        let value = Value::U32(42);
        let decoded: Value<'_> = deserialize_for_property(&value).unwrap();
        assert_eq!(decoded, Value::U32(42));

        let decoded = convert_property_value::<OwnedValue>(&value);
        assert_eq!(decoded, OwnedValue::from(42_u32));

        let value = Value::Value(Box::new(Value::U32(42)));
        let decoded = convert_property_value::<OwnedValue>(&value);
        assert_eq!(decoded, OwnedValue::try_from(value).unwrap());
    }

    #[test]
    fn unwraps_variant_structure_and_enum_fields() {
        #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize, crate::Type)]
        struct WithValue(u32, OwnedValue);

        #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize, crate::Type)]
        enum NewType {
            First(OwnedValue),
            Second(OwnedValue),
        }

        #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize, crate::Type)]
        enum Fields {
            First(u8, OwnedValue),
            Second { y: u8, value: OwnedValue },
        }

        let value = WithValue(7, OwnedValue::from(42_u32));
        let serialized = Value::from(super::super::to_owned_value(&value).unwrap());
        assert_eq!(convert_property_value::<WithValue>(&serialized), value);

        let value = NewType::Second(OwnedValue::from(42_u32));
        let serialized = Value::from(super::super::to_owned_value(&value).unwrap());
        assert_eq!(convert_property_value::<NewType>(&serialized), value);

        let value = Fields::Second {
            y: 7,
            value: OwnedValue::from(42_u32),
        };
        let serialized = Value::from(super::super::to_owned_value(&value).unwrap());
        assert_eq!(convert_property_value::<Fields>(&serialized), value);
    }

    #[test]
    fn borrows_strings_from_values() {
        let value = Value::from("borrowed");
        let decoded: &str = deserialize_for_property(&value).unwrap();
        assert_eq!(decoded, "borrowed");
    }

    #[test]
    fn deserializes_signatures() {
        let value = Value::Signature(Signature::try_from("a{sv}").unwrap());
        assert_eq!(
            deserialize_for_property::<Signature>(&value).unwrap(),
            Signature::try_from("a{sv}").unwrap()
        );
    }

    #[test]
    fn deserializes_repr_enums() {
        #[repr(u32)]
        #[derive(Debug, PartialEq, crate::Type, serde_repr::Deserialize_repr)]
        enum Kind {
            First = 1,
            Second = 2,
        }

        let value = Value::U32(2);
        assert_eq!(
            deserialize_for_property::<Kind>(&value).unwrap(),
            Kind::Second
        );
    }

    #[test]
    fn deserializes_wire_enum_representations() {
        #[derive(Debug, PartialEq, crate::Type, serde::Deserialize)]
        enum Unit {
            First,
            Second,
        }

        let value = Value::U32(1);
        assert_eq!(
            deserialize_for_property::<Unit>(&value).unwrap(),
            Unit::Second
        );

        #[derive(Debug, PartialEq, crate::Type, serde::Deserialize)]
        #[zvariant(signature = "s")]
        #[serde(rename_all = "snake_case")]
        enum StringUnit {
            First,
            Second,
        }

        let value = Value::from("second");
        assert_eq!(
            deserialize_for_property::<StringUnit>(&value).unwrap(),
            StringUnit::Second
        );

        #[derive(Debug, PartialEq, crate::Type, serde::Deserialize)]
        enum NewType {
            First(f64),
            Second(f64),
        }

        let value = Structure::builder()
            .append_field(Value::U32(1))
            .append_field(Value::F64(2.5))
            .build()
            .unwrap();
        assert_eq!(
            deserialize_for_property::<NewType>(&Value::Structure(value)).unwrap(),
            NewType::Second(2.5)
        );

        #[derive(Debug, PartialEq, crate::Type, serde::Deserialize)]
        enum Fields {
            First(u8, u32),
            Second { y: u8, t: u32 },
        }

        let payload = Structure::builder()
            .append_field(Value::U8(3))
            .append_field(Value::U32(4))
            .build()
            .unwrap();
        let value = Structure::builder()
            .append_field(Value::U32(1))
            .append_field(Value::Structure(payload))
            .build()
            .unwrap();
        assert_eq!(
            deserialize_for_property::<Fields>(&Value::Structure(value)).unwrap(),
            Fields::Second { y: 3, t: 4 }
        );
    }

    #[test]
    fn deserializes_value_containers_and_dict_structs() {
        #[derive(Debug, PartialEq, crate::DeserializeDict, crate::Type)]
        #[zvariant(signature = "dict")]
        struct DictStruct {
            count: u32,
            name: String,
        }

        let mut dict = Dict::new(&Signature::Str, &Signature::Variant);
        dict.append(Value::from("count"), Value::new(Value::from(7_u32)))
            .unwrap();
        dict.append(Value::from("name"), Value::new(Value::from("dict")))
            .unwrap();
        assert_eq!(
            deserialize_for_property::<DictStruct>(&Value::Dict(dict)).unwrap(),
            DictStruct {
                count: 7,
                name: "dict".to_owned(),
            }
        );

        let mut array = Array::new(&Signature::Variant);
        array.append(Value::new(Value::from("first"))).unwrap();
        array.append(Value::new(Value::from(42_u32))).unwrap();
        assert_eq!(
            deserialize_for_property::<Vec<Value<'_>>>(&Value::Array(array)).unwrap(),
            [Value::from("first"), Value::from(42_u32)],
        );
    }

    #[test]
    fn deserializes_empty_structs() {
        #[derive(serde::Deserialize, crate::Type)]
        struct Empty {}

        deserialize_for_property::<Empty>(&Value::U8(0)).unwrap();
    }

    #[cfg(feature = "serde_bytes")]
    #[test]
    fn deserializes_variant_wrapped_bytes() {
        let mut values = Array::new(&Signature::Variant);
        for byte in [1_u8, 2, 3] {
            values
                .append(Value::Value(Box::new(Value::from(byte))))
                .unwrap();
        }

        assert_eq!(
            deserialize_for_property::<serde_bytes::ByteBuf>(&Value::Array(values)).unwrap(),
            serde_bytes::ByteBuf::from(vec![1, 2, 3]),
        );
    }

    #[cfg(feature = "option-as-array")]
    #[test]
    fn deserializes_option_arrays() {
        let some = Value::Array(Array::from(vec![42_i32]));
        let none = Value::Array(Array::new(&Signature::I32));
        let mut variants = Array::new(&Signature::Variant);
        variants
            .append(Value::Value(Box::new(Value::from(42_i32))))
            .unwrap();
        assert_eq!(
            deserialize_for_property::<Option<i32>>(&some).unwrap(),
            Some(42)
        );
        assert_eq!(
            deserialize_for_property::<Option<i32>>(&none).unwrap(),
            None
        );
        assert_eq!(
            deserialize_for_property::<Option<i32>>(&Value::Array(variants)).unwrap(),
            Some(42)
        );
    }
}
