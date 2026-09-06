//! In-memory serializer for values wrapped by [`super::Serialize`].
//!
//! This is deliberately kept separate from `serialize.rs`: the latter is the public Serde
//! wrapper, while this module is the `Value`-building backend used when no wire representation is
//! needed.

use serde::{
    Serialize,
    ser::{self, SerializeMap, SerializeSeq},
};

use crate::wire::{
    Array, Dict, OwnedValue, Signature, Structure, Type, Value, container_depths::ContainerDepths,
};

#[cfg(unix)]
use std::os::fd::BorrowedFd;

/// Serialize `value` directly into an owned D-Bus variant.
#[doc(hidden)]
pub fn to_owned_value<T>(value: &T) -> crate::Result<OwnedValue>
where
    T: Type + Serialize,
{
    Ok(OwnedValue(to_value(value)?))
}

/// Serialize `value` directly into the payload of an owned D-Bus variant.
fn to_value<T>(value: &T) -> crate::Result<Value<'static>>
where
    T: Type + Serialize,
{
    let mut serializer = ValueSerializer::new(Signature::Variant, ContainerDepths::default());
    let variant = super::Serialize(value).serialize(&mut serializer)?;
    let Value::Value(value) = variant else {
        return Err(crate::Error::Failure(
            "as-value serializer did not produce a variant".to_owned(),
        ));
    };
    Ok(*value)
}

struct ValueSerializer {
    signature: Signature,
    depths: ContainerDepths,
}

impl ValueSerializer {
    fn new(signature: Signature, depths: ContainerDepths) -> Self {
        Self { signature, depths }
    }

    fn mismatch(&self, expected: &str) -> crate::Error {
        crate::Error::signature_mismatch(&self.signature, expected)
    }

    fn expect(&self, signature: Signature) -> crate::Result<()> {
        if self.signature == signature {
            Ok(())
        } else {
            Err(self.mismatch(&format!("`{signature}`")))
        }
    }
}

impl ser::Serializer for &mut ValueSerializer {
    type Ok = Value<'static>;
    type Error = crate::Error;
    type SerializeSeq = SeqSerializer;
    type SerializeTuple = StructSeqSerializer;
    type SerializeTupleStruct = StructSeqSerializer;
    type SerializeTupleVariant = StructSeqSerializer;
    type SerializeMap = MapSerializer;
    type SerializeStruct = StructSeqSerializer;
    type SerializeStructVariant = StructSeqSerializer;

    fn serialize_bool(self, value: bool) -> crate::Result<Self::Ok> {
        self.expect(Signature::Bool).map(|()| Value::Bool(value))
    }

    fn serialize_i8(self, value: i8) -> crate::Result<Self::Ok> {
        self.expect(Signature::I16)
            .map(|()| Value::I16(value as i16))
    }

    fn serialize_i16(self, value: i16) -> crate::Result<Self::Ok> {
        self.expect(Signature::I16).map(|()| Value::I16(value))
    }

    fn serialize_i32(self, value: i32) -> crate::Result<Self::Ok> {
        #[cfg(unix)]
        if self.signature == Signature::Fd {
            // SAFETY: `value` is the raw descriptor supplied by the Serialize implementation.
            // It is borrowed only for the duration of the immediate clone operation.
            let fd = unsafe { BorrowedFd::borrow_raw(value) }.try_clone_to_owned()?;
            return Ok(Value::Fd(crate::Fd::Owned(fd)));
        }

        self.expect(Signature::I32).map(|()| Value::I32(value))
    }

    fn serialize_i64(self, value: i64) -> crate::Result<Self::Ok> {
        self.expect(Signature::I64).map(|()| Value::I64(value))
    }

    fn serialize_u8(self, value: u8) -> crate::Result<Self::Ok> {
        self.expect(Signature::U8).map(|()| Value::U8(value))
    }

    fn serialize_u16(self, value: u16) -> crate::Result<Self::Ok> {
        self.expect(Signature::U16).map(|()| Value::U16(value))
    }

    fn serialize_u32(self, value: u32) -> crate::Result<Self::Ok> {
        self.expect(Signature::U32).map(|()| Value::U32(value))
    }

    fn serialize_u64(self, value: u64) -> crate::Result<Self::Ok> {
        self.expect(Signature::U64).map(|()| Value::U64(value))
    }

    fn serialize_f32(self, value: f32) -> crate::Result<Self::Ok> {
        self.expect(Signature::F64)
            .map(|()| Value::F64(value as f64))
    }

    fn serialize_f64(self, value: f64) -> crate::Result<Self::Ok> {
        self.expect(Signature::F64).map(|()| Value::F64(value))
    }

    fn serialize_char(self, value: char) -> crate::Result<Self::Ok> {
        self.serialize_str(&value.to_string())
    }

    fn serialize_str(self, value: &str) -> crate::Result<Self::Ok> {
        match self.signature {
            Signature::Str => Ok(Value::Str(value.to_owned().into())),
            Signature::ObjectPath => Ok(Value::ObjectPath(crate::ObjectPath::try_from(
                value.to_owned(),
            )?)),
            Signature::Signature => Ok(Value::Signature(value.parse()?)),
            _ => Err(self.mismatch("a string, signature, or object path")),
        }
    }

    fn serialize_bytes(self, value: &[u8]) -> crate::Result<Self::Ok> {
        let Signature::Array(child) = &self.signature else {
            return Err(self.mismatch("an array of bytes"));
        };
        if child.signature() != &Signature::U8 {
            return Err(self.mismatch("an array of bytes"));
        }
        let mut array = Array::new(&Signature::U8);
        for byte in value {
            array.append(Value::U8(*byte))?;
        }
        Ok(Value::Array(array))
    }

    fn serialize_none(self) -> crate::Result<Self::Ok> {
        #[cfg(feature = "option-as-array")]
        {
            self.serialize_seq(Some(0)).and_then(SeqSerializer::end)
        }
        #[cfg(not(feature = "option-as-array"))]
        {
            Err(crate::Error::Unsupported)
        }
    }

    fn serialize_some<T>(self, value: &T) -> crate::Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        #[cfg(feature = "option-as-array")]
        {
            let mut seq = self.serialize_seq(Some(1))?;
            ser::SerializeSeq::serialize_element(&mut seq, value)?;
            ser::SerializeSeq::end(seq)
        }
        #[cfg(not(feature = "option-as-array"))]
        {
            let _ = value;
            Err(crate::Error::Unsupported)
        }
    }

    fn serialize_unit(self) -> crate::Result<Self::Ok> {
        Err(crate::Error::Unsupported)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> crate::Result<Self::Ok> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> crate::Result<Self::Ok> {
        if self.signature == Signature::Str {
            return Ok(Value::Str(variant.to_owned().into()));
        }
        self.serialize_u32(variant_index)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> crate::Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> crate::Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        let mut serializer = StructSerializer::enum_variant(self, variant_index)?;
        serializer.serialize_field(value)?;
        serializer.end()
    }

    fn serialize_seq(self, _len: Option<usize>) -> crate::Result<Self::SerializeSeq> {
        let child = match &self.signature {
            Signature::Array(child) => child.signature().clone(),
            _ => return Err(self.mismatch("an array")),
        };
        Ok(SeqSerializer {
            element_signature: child,
            array_signature: self.signature.clone(),
            elements: Vec::new(),
            depths: self.depths.inc_array()?,
        })
    }

    fn serialize_tuple(self, len: usize) -> crate::Result<Self::SerializeTuple> {
        self.serialize_struct("", len)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> crate::Result<Self::SerializeTupleStruct> {
        self.serialize_struct(name, len)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> crate::Result<Self::SerializeTupleVariant> {
        StructSerializer::enum_variant(self, variant_index).map(StructSeqSerializer::Struct)
    }

    fn serialize_map(self, _len: Option<usize>) -> crate::Result<Self::SerializeMap> {
        let (key_signature, value_signature) = match &self.signature {
            Signature::Dict { key, value } => (key.signature().clone(), value.signature().clone()),
            _ => return Err(self.mismatch("a dict")),
        };
        Ok(MapSerializer {
            key_signature,
            value_signature,
            dict_signature: self.signature.clone(),
            entries: Vec::new(),
            pending_key: None,
            depths: self.depths.inc_array()?,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> crate::Result<Self::SerializeStruct> {
        match &self.signature {
            Signature::Variant => Ok(StructSeqSerializer::Variant(VariantSerializer::new(
                self.depths.inc_variant()?,
            ))),
            Signature::Array(_) => self.serialize_seq(Some(len)).map(StructSeqSerializer::Seq),
            Signature::U8 => Ok(StructSeqSerializer::Struct(StructSerializer::unit(self)?)),
            Signature::Structure(_) => {
                StructSerializer::normal(self).map(StructSeqSerializer::Struct)
            }
            Signature::Dict { .. } => self.serialize_map(Some(len)).map(StructSeqSerializer::Map),
            _ => Err(self.mismatch("a struct, array, u8 or variant")),
        }
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> crate::Result<Self::SerializeStructVariant> {
        StructSerializer::enum_variant(self, variant_index).map(StructSeqSerializer::Struct)
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

struct SeqSerializer {
    element_signature: Signature,
    array_signature: Signature,
    elements: Vec<Value<'static>>,
    depths: ContainerDepths,
}

impl ser::SerializeSeq for SeqSerializer {
    type Ok = Value<'static>;
    type Error = crate::Error;

    fn serialize_element<T>(&mut self, value: &T) -> crate::Result<()>
    where
        T: ?Sized + Serialize,
    {
        let mut serializer = ValueSerializer::new(self.element_signature.clone(), self.depths);
        self.elements.push(value.serialize(&mut serializer)?);
        Ok(())
    }

    fn end(self) -> crate::Result<Self::Ok> {
        let mut array = Array::new(&self.element_signature);
        for element in self.elements {
            array.append(element)?;
        }
        debug_assert_eq!(array.signature(), &self.array_signature);
        Ok(Value::Array(array))
    }
}

struct MapSerializer {
    key_signature: Signature,
    value_signature: Signature,
    dict_signature: Signature,
    entries: Vec<(Value<'static>, Value<'static>)>,
    pending_key: Option<Value<'static>>,
    depths: ContainerDepths,
}

impl ser::SerializeMap for MapSerializer {
    type Ok = Value<'static>;
    type Error = crate::Error;

    fn serialize_key<T>(&mut self, key: &T) -> crate::Result<()>
    where
        T: ?Sized + Serialize,
    {
        if self.pending_key.is_some() {
            return Err(crate::Error::Failure("map value is missing".to_owned()));
        }
        let mut serializer = ValueSerializer::new(self.key_signature.clone(), self.depths);
        self.pending_key = Some(key.serialize(&mut serializer)?);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> crate::Result<()>
    where
        T: ?Sized + Serialize,
    {
        let Some(key) = self.pending_key.take() else {
            return Err(crate::Error::Failure("map key is missing".to_owned()));
        };
        let mut serializer = ValueSerializer::new(self.value_signature.clone(), self.depths);
        self.entries.push((key, value.serialize(&mut serializer)?));
        Ok(())
    }

    fn end(self) -> crate::Result<Self::Ok> {
        if self.pending_key.is_some() {
            return Err(crate::Error::Failure("map value is missing".to_owned()));
        }
        let mut dict = Dict::new(&self.key_signature, &self.value_signature);
        for (key, value) in self.entries {
            dict.append(key, value)?;
        }
        debug_assert_eq!(dict.signature(), &self.dict_signature);
        Ok(Value::Dict(dict))
    }
}

struct StructSerializer {
    expected: Signature,
    fields: Vec<Value<'static>>,
    field_idx: usize,
    depths: ContainerDepths,
    enum_inner: Option<(Signature, Vec<Value<'static>>, usize, ContainerDepths)>,
}

impl StructSerializer {
    fn normal(serializer: &ValueSerializer) -> crate::Result<Self> {
        Ok(Self {
            expected: serializer.signature.clone(),
            fields: Vec::new(),
            field_idx: 0,
            depths: serializer.depths.inc_structure()?,
            enum_inner: None,
        })
    }

    fn unit(serializer: &ValueSerializer) -> crate::Result<Self> {
        serializer.expect(Signature::U8)?;
        Ok(Self {
            expected: Signature::U8,
            fields: vec![Value::U8(0)],
            field_idx: 0,
            depths: serializer.depths,
            enum_inner: None,
        })
    }

    fn enum_variant(serializer: &ValueSerializer, variant_index: u32) -> crate::Result<Self> {
        let Signature::Structure(fields) = &serializer.signature else {
            return Err(serializer.mismatch("a struct"));
        };
        let Some(inner) = fields.get(1) else {
            return Err(serializer.mismatch("an enum structure"));
        };
        let outer_depths = serializer.depths.inc_structure()?;
        let enum_inner = match inner {
            Signature::Structure(_) => {
                Some((inner.clone(), Vec::new(), 0, outer_depths.inc_structure()?))
            }
            _ => None,
        };
        if fields.get(0) != Some(&Signature::U32) {
            return Err(serializer.mismatch("an enum structure beginning with `u`"));
        }
        Ok(Self {
            expected: serializer.signature.clone(),
            fields: vec![Value::U32(variant_index)],
            field_idx: 1,
            depths: outer_depths,
            enum_inner,
        })
    }

    fn serialize_field<T>(&mut self, value: &T) -> crate::Result<()>
    where
        T: ?Sized + Serialize,
    {
        if let Some((inner_signature, inner_fields, inner_idx, depths)) = &mut self.enum_inner {
            let Signature::Structure(fields) = inner_signature else {
                unreachable!()
            };
            let Some(signature) = fields.get(*inner_idx) else {
                return Err(crate::Error::signature_mismatch(
                    inner_signature,
                    "an enum variant with the expected number of fields",
                ));
            };
            *inner_idx += 1;
            let mut serializer = ValueSerializer::new(signature.clone(), *depths);
            inner_fields.push(value.serialize(&mut serializer)?);
            return Ok(());
        }

        let Signature::Structure(fields) = &self.expected else {
            return Err(crate::Error::Unsupported);
        };
        let Some(signature) = fields.get(self.field_idx) else {
            return Err(crate::Error::signature_mismatch(
                &self.expected,
                "a struct with the expected number of fields",
            ));
        };
        self.field_idx += 1;
        let mut serializer = ValueSerializer::new(signature.clone(), self.depths);
        self.fields.push(value.serialize(&mut serializer)?);
        Ok(())
    }

    fn end(mut self) -> crate::Result<Value<'static>> {
        if let Some((inner_signature, inner_fields, inner_idx, _)) = self.enum_inner.take() {
            let Signature::Structure(fields) = &inner_signature else {
                unreachable!()
            };
            if inner_idx != fields.len() {
                return Err(crate::Error::signature_mismatch(
                    &inner_signature,
                    "an enum variant with the expected number of fields",
                ));
            }
            let mut builder = Structure::builder();
            builder.push_value(Value::U32(match self.fields.first() {
                Some(Value::U32(index)) => *index,
                _ => unreachable!(),
            }));
            let mut inner_builder = Structure::builder();
            for field in inner_fields {
                inner_builder.push_value(field);
            }
            builder.push_value(Value::Structure(
                inner_builder.build_with_signature(&inner_signature),
            ));
            return Ok(Value::Structure(
                builder.build_with_signature(&self.expected),
            ));
        }

        if let Signature::Structure(fields) = &self.expected {
            if self.fields.len() != fields.len() {
                return Err(crate::Error::signature_mismatch(
                    &self.expected,
                    "a struct with the expected number of fields",
                ));
            }
        }
        if self.expected == Signature::U8 && self.fields.len() == 1 {
            return Ok(self.fields.pop().expect("unit field is present"));
        }
        let mut builder = Structure::builder();
        for field in self.fields {
            builder.push_value(field);
        }
        Ok(Value::Structure(
            builder.build_with_signature(&self.expected),
        ))
    }
}

struct VariantSerializer {
    signature: Option<Signature>,
    value: Option<Value<'static>>,
    depths: ContainerDepths,
}

impl VariantSerializer {
    fn new(depths: ContainerDepths) -> Self {
        Self {
            signature: None,
            value: None,
            depths,
        }
    }

    fn serialize_field<T>(&mut self, value: &T) -> crate::Result<()>
    where
        T: ?Sized + Serialize,
    {
        if self.signature.is_none() {
            let mut serializer = ValueSerializer::new(Signature::Signature, self.depths);
            let Value::Signature(signature) = value.serialize(&mut serializer)? else {
                unreachable!()
            };
            self.signature = Some(signature);
            return Ok(());
        }
        if self.value.is_some() {
            return Err(crate::Error::Failure(
                "variant has too many fields".to_owned(),
            ));
        }
        let signature = self.signature.clone().expect("checked above");
        let mut serializer = ValueSerializer::new(signature, self.depths);
        self.value = Some(value.serialize(&mut serializer)?);
        Ok(())
    }

    fn end(self) -> crate::Result<Value<'static>> {
        let Some(signature) = self.signature else {
            return Err(crate::Error::Failure(
                "variant is missing its signature".to_owned(),
            ));
        };
        let Some(value) = self.value else {
            return Err(crate::Error::Failure(
                "variant is missing its value".to_owned(),
            ));
        };
        if value.value_signature() != &signature {
            return Err(crate::Error::signature_mismatch(
                value.value_signature(),
                &format!("`{signature}`"),
            ));
        }
        Ok(Value::Value(Box::new(value)))
    }
}

enum StructSeqSerializer {
    Struct(StructSerializer),
    Seq(SeqSerializer),
    Map(MapSerializer),
    Variant(VariantSerializer),
}

impl ser::SerializeTuple for StructSeqSerializer {
    type Ok = Value<'static>;
    type Error = crate::Error;

    fn serialize_element<T>(&mut self, value: &T) -> crate::Result<()>
    where
        T: ?Sized + Serialize,
    {
        match self {
            Self::Struct(serializer) => serializer.serialize_field(value),
            Self::Seq(serializer) => serializer.serialize_element(value),
            Self::Map(serializer) => {
                let _ = serializer;
                Err(crate::Error::Failure("tuple cannot be a map".to_owned()))
            }
            Self::Variant(serializer) => serializer.serialize_field(value),
        }
    }

    fn end(self) -> crate::Result<Self::Ok> {
        match self {
            Self::Struct(serializer) => serializer.end(),
            Self::Seq(serializer) => serializer.end(),
            Self::Map(serializer) => {
                let _ = serializer;
                Err(crate::Error::Failure("tuple cannot be a map".to_owned()))
            }
            Self::Variant(serializer) => serializer.end(),
        }
    }
}

impl ser::SerializeTupleStruct for StructSeqSerializer {
    type Ok = Value<'static>;
    type Error = crate::Error;

    fn serialize_field<T>(&mut self, value: &T) -> crate::Result<()>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeTuple::serialize_element(self, value)
    }

    fn end(self) -> crate::Result<Self::Ok> {
        ser::SerializeTuple::end(self)
    }
}

impl ser::SerializeTupleVariant for StructSeqSerializer {
    type Ok = Value<'static>;
    type Error = crate::Error;

    fn serialize_field<T>(&mut self, value: &T) -> crate::Result<()>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeTuple::serialize_element(self, value)
    }

    fn end(self) -> crate::Result<Self::Ok> {
        ser::SerializeTuple::end(self)
    }
}

impl ser::SerializeStruct for StructSeqSerializer {
    type Ok = Value<'static>;
    type Error = crate::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> crate::Result<()>
    where
        T: ?Sized + Serialize,
    {
        match self {
            Self::Map(serializer) => {
                serializer.serialize_key(&key)?;
                serializer.serialize_value(value)
            }
            _ => ser::SerializeTuple::serialize_element(self, value),
        }
    }

    fn end(self) -> crate::Result<Self::Ok> {
        match self {
            Self::Map(serializer) => serializer.end(),
            serializer => ser::SerializeTuple::end(serializer),
        }
    }
}

impl ser::SerializeStructVariant for StructSeqSerializer {
    type Ok = Value<'static>;
    type Error = crate::Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> crate::Result<()>
    where
        T: ?Sized + Serialize,
    {
        ser::SerializeTuple::serialize_element(self, value)
    }

    fn end(self) -> crate::Result<Self::Ok> {
        ser::SerializeTuple::end(self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::wire::{LE, serialized::Context, to_bytes};

    #[derive(Debug, PartialEq, serde::Serialize, crate::Type)]
    struct Pair(u32, String);

    #[derive(serde::Serialize, crate::Type)]
    struct Empty {}

    #[derive(crate::SerializeDict, crate::Type)]
    #[zvariant(signature = "dict")]
    struct DictStruct {
        count: u32,
        name: String,
    }

    #[derive(Debug, PartialEq, serde::Serialize, crate::Type)]
    enum Enum {
        First(u32, String),
        Second(u32, String),
    }

    #[derive(serde::Serialize, crate::Type)]
    enum UnitEnum {
        First,
        Second,
    }

    #[derive(serde::Serialize, crate::Type)]
    #[zvariant(signature = "s")]
    #[serde(rename_all = "snake_case")]
    enum StringEnum {
        FirstValue,
        SecondValue,
    }

    #[repr(u32)]
    #[derive(serde_repr::Serialize_repr, crate::Type)]
    enum ReprEnum {
        First = 1,
        Second = 2,
    }

    fn assert_matches_wire<T>(value: &T)
    where
        T: Type + Serialize,
    {
        let direct = to_owned_value(value).unwrap();
        let encoded = to_bytes(Context::new(LE, 0), &super::super::Serialize(value)).unwrap();
        let wire: OwnedValue = encoded.deserialize().unwrap().0;
        assert_eq!(Value::from(direct), Value::from(wire));
    }

    #[test]
    fn plain_value_matches_wire_round_trip() {
        let pair = Pair(42, "hello".to_owned());
        assert_matches_wire(&pair);
    }

    #[test]
    fn nested_variant_matches_wire_round_trip() {
        let value = Value::new(Value::new(42_u32));
        assert_matches_wire(&value);
    }

    #[test]
    fn supported_values_match_wire_round_trip() {
        assert_matches_wire(&Empty {});
        assert_matches_wire(&vec![1_u32, 2, 3]);
        let map = HashMap::from([
            ("hi".to_owned(), "hello".to_owned()),
            ("bye".to_owned(), "now".to_owned()),
        ]);
        assert_matches_wire(&map);
        let value = Value::from(to_owned_value(&map).unwrap());
        let decoded: HashMap<String, String> =
            super::super::deserialize_for_property(&value).unwrap();
        assert_eq!(decoded, map);
        assert_matches_wire(&DictStruct {
            count: 7,
            name: "dict".to_owned(),
        });
        assert_matches_wire(&Enum::First(7, "first".to_owned()));
        assert_matches_wire(&Enum::Second(42, "hello".to_owned()));
        assert_matches_wire(&UnitEnum::First);
        assert_matches_wire(&UnitEnum::Second);
        assert_matches_wire(&StringEnum::FirstValue);
        assert_matches_wire(&StringEnum::SecondValue);
        assert_matches_wire(&ReprEnum::First);
        assert_matches_wire(&ReprEnum::Second);

        #[cfg(feature = "option-as-array")]
        {
            assert_matches_wire(&Some(42_u32));
            assert_matches_wire(&None::<u32>);
        }

        #[cfg(feature = "serde_bytes")]
        assert_matches_wire(&serde_bytes::ByteBuf::from(vec![1, 2, 3]));
    }
}
