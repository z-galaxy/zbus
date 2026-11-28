use serde::{Deserialize, Deserializer, Serialize};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    hash::BuildHasher,
};

use crate::wire::{
    Array, Basic, Dict, NoneValue, ObjectPath, Optional, OwnedObjectPath, Signature, Str,
    Structure, Type, Variant,
};

#[cfg(unix)]
use crate::Fd;

// FIXME: Replace with a generic impl<T: TryFrom<Variant>> TryFrom<OwnedVariant> for T?
// https://github.com/z-galaxy/zbus/issues/138

/// Owned [`Variant`](enum.Variant.html)
///
/// Like a [`Variant`](enum@Variant), it holds each value in an array, a dictionary or a structure
/// as a `Variant` of its own: see [`Variant`'s documentation on memory](enum@Variant#memory).
#[derive(Debug, PartialEq, Serialize, Type)]
pub struct OwnedVariant(pub(crate) Variant<'static>);

impl OwnedVariant {
    /// Attempt to clone the value.
    pub fn try_clone(&self) -> Result<Self, crate::Error> {
        self.0.try_clone().map(Self)
    }

    pub(crate) fn into_inner(self) -> Variant<'static> {
        self.0
    }

    pub(crate) fn inner(&self) -> &Variant<'_> {
        &self.0
    }
}

macro_rules! ov_try_from {
    ($to:ty) => {
        impl TryFrom<OwnedVariant> for $to {
            type Error = crate::Error;

            fn try_from(v: OwnedVariant) -> Result<Self, Self::Error> {
                <$to>::try_from(v.0)
            }
        }
    };
}

macro_rules! ov_try_from_ref {
    ($to:ty) => {
        impl<'a> TryFrom<&'a OwnedVariant> for $to {
            type Error = crate::Error;

            fn try_from(v: &'a OwnedVariant) -> Result<Self, Self::Error> {
                <$to>::try_from(&v.0)
            }
        }
    };
}

ov_try_from!(u8);
ov_try_from!(bool);
ov_try_from!(i16);
ov_try_from!(u16);
ov_try_from!(i32);
ov_try_from!(u32);
ov_try_from!(i64);
ov_try_from!(u64);
ov_try_from!(f64);
ov_try_from!(String);
ov_try_from!(Signature);
ov_try_from!(ObjectPath<'static>);
ov_try_from!(OwnedObjectPath);
ov_try_from!(Array<'static>);
ov_try_from!(Dict<'static, 'static>);
ov_try_from!(Str<'static>);
ov_try_from!(Structure<'static>);
#[cfg(unix)]
ov_try_from!(Fd<'static>);

ov_try_from_ref!(u8);
ov_try_from_ref!(bool);
ov_try_from_ref!(i16);
ov_try_from_ref!(u16);
ov_try_from_ref!(i32);
ov_try_from_ref!(u32);
ov_try_from_ref!(i64);
ov_try_from_ref!(u64);
ov_try_from_ref!(f64);
ov_try_from_ref!(&'a str);
ov_try_from_ref!(&'a Signature);
ov_try_from_ref!(&'a ObjectPath<'a>);
ov_try_from_ref!(&'a Array<'a>);
ov_try_from_ref!(&'a Dict<'a, 'a>);
ov_try_from_ref!(&'a Str<'a>);
ov_try_from_ref!(&'a Structure<'a>);
#[cfg(unix)]
ov_try_from_ref!(&'a Fd<'a>);

impl<'a, T> TryFrom<OwnedVariant> for Vec<T>
where
    T: TryFrom<Variant<'a>>,
    T::Error: Into<crate::Error>,
{
    type Error = crate::Error;

    fn try_from(value: OwnedVariant) -> Result<Self, Self::Error> {
        if let Variant::Array(v) = value.0 {
            Self::try_from(v)
        } else {
            Err(crate::Error::IncorrectType)
        }
    }
}

impl<'k, 'v, K, V> TryFrom<OwnedVariant> for BTreeMap<K, V>
where
    K: crate::Basic + TryFrom<Variant<'k>> + std::cmp::Ord,
    V: TryFrom<Variant<'v>>,
    K::Error: Into<crate::wire::Error>,
    V::Error: Into<crate::wire::Error>,
{
    type Error = crate::wire::Error;

    fn try_from(value: OwnedVariant) -> Result<Self, Self::Error> {
        if let Variant::Dict(v) = value.0 {
            Self::try_from(v)
        } else {
            Err(crate::wire::Error::IncorrectType)
        }
    }
}

impl<K, V> From<BTreeMap<K, V>> for OwnedVariant
where
    K: Basic + Into<Variant<'static>> + std::cmp::Ord,
    V: Type + Into<Variant<'static>>,
{
    fn from(value: BTreeMap<K, V>) -> Self {
        Self(value.into())
    }
}

impl<'k, 'v, K, V, H> TryFrom<OwnedVariant> for HashMap<K, V, H>
where
    K: crate::Basic + TryFrom<Variant<'k>> + std::hash::Hash + std::cmp::Eq,
    V: TryFrom<Variant<'v>>,
    H: BuildHasher + Default,
    K::Error: Into<crate::Error>,
    V::Error: Into<crate::Error>,
{
    type Error = crate::Error;

    fn try_from(value: OwnedVariant) -> Result<Self, Self::Error> {
        if let Variant::Dict(v) = value.0 {
            Self::try_from(v)
        } else {
            Err(crate::Error::IncorrectType)
        }
    }
}

impl<K, V, H> From<HashMap<K, V, H>> for OwnedVariant
where
    K: Basic + Into<Variant<'static>> + std::hash::Hash + std::cmp::Eq,
    V: Type + Into<Variant<'static>>,
    H: BuildHasher + Default,
{
    fn from(value: HashMap<K, V, H>) -> Self {
        Self(value.into())
    }
}

impl<'a, T> TryFrom<OwnedVariant> for Optional<T>
where
    T: TryFrom<Variant<'a>> + NoneValue,
    <T as NoneValue>::NoneType: Into<Variant<'a>>,
    T::Error: Into<crate::Error>,
{
    type Error = crate::Error;

    fn try_from(value: OwnedVariant) -> Result<Self, Self::Error> {
        Self::try_from(value.0)
    }
}

impl<V> From<Optional<V>> for OwnedVariant
where
    V: Into<Variant<'static>> + NoneValue<NoneType = V>,
{
    fn from(v: Optional<V>) -> OwnedVariant {
        Self(Variant::from(v))
    }
}

// tuple conversions in `structure` module for avoiding code-duplication.

impl<'a> TryFrom<Variant<'a>> for OwnedVariant {
    type Error = crate::Error;

    fn try_from(v: Variant<'a>) -> crate::Result<Self> {
        v.try_into_owned()
    }
}

impl<'a> TryFrom<&Variant<'a>> for OwnedVariant {
    type Error = crate::Error;

    fn try_from(v: &Variant<'a>) -> crate::Result<Self> {
        v.try_to_owned()
    }
}

macro_rules! to_value {
    ($from:ty, $variant:ident) => {
        impl<'a> From<$from> for OwnedVariant {
            fn from(v: $from) -> Self {
                OwnedVariant(<Variant<'static>>::$variant(v.to_owned()))
            }
        }
    };
}

to_value!(u8, U8);
to_value!(bool, Bool);
to_value!(i16, I16);
to_value!(u16, U16);
to_value!(i32, I32);
to_value!(u32, U32);
to_value!(i64, I64);
to_value!(u64, U64);
to_value!(f64, F64);
to_value!(Str<'a>, Str);
to_value!(ObjectPath<'a>, ObjectPath);

impl From<Signature> for OwnedVariant {
    fn from(v: Signature) -> Self {
        OwnedVariant(<Variant<'static>>::Signature(v))
    }
}

macro_rules! try_to_value {
    ($from:ty) => {
        impl<'a> TryFrom<$from> for OwnedVariant {
            type Error = crate::Error;

            fn try_from(v: $from) -> crate::Result<Self> {
                OwnedVariant::try_from(<Variant<'a>>::from(v))
            }
        }
    };
}

try_to_value!(Array<'a>);
try_to_value!(Dict<'a, 'a>);
try_to_value!(Structure<'a>);
#[cfg(unix)]
try_to_value!(Fd<'a>);

impl From<OwnedVariant> for Variant<'_> {
    fn from(v: OwnedVariant) -> Self {
        v.into_inner()
    }
}

impl<'o> TryFrom<&'o OwnedVariant> for Variant<'o> {
    type Error = crate::Error;

    fn try_from(v: &'o OwnedVariant) -> crate::Result<Variant<'o>> {
        v.inner().try_clone()
    }
}

impl std::ops::Deref for OwnedVariant {
    type Target = Variant<'static>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> Borrow<Variant<'a>> for OwnedVariant {
    fn borrow(&self) -> &Variant<'a> {
        &self.0
    }
}

impl<'de> Deserialize<'de> for OwnedVariant {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Variant::deserialize(deserializer)
            .and_then(|v| v.try_to_owned().map_err(serde::de::Error::custom))
    }
}

impl Clone for OwnedVariant {
    /// Clone the value.
    ///
    /// # Panics
    ///
    /// This method can only fail on Unix platforms for [`Variant::Fd`] variant containing an
    /// [`Fd::Owned`] variant. This happens when the current process exceeds the limit on maximum
    /// number of open file descriptors.
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, error::Error};

    use crate::wire::{LE, OwnedVariant, Variant, serialized::Context, to_bytes};

    #[test]
    fn from_value() -> Result<(), Box<dyn Error>> {
        let v = Variant::from("hi!");
        let ov: OwnedVariant = v.try_into()?;
        assert_eq!(<&str>::try_from(&ov)?, "hi!");
        Ok(())
    }

    #[test]
    fn serde() -> Result<(), Box<dyn Error>> {
        let ec = Context::new(LE, 0);
        let ov: OwnedVariant = Variant::from("hi!").try_into()?;
        let ser = to_bytes(ec, &ov)?;
        let (de, parsed): (Variant<'_>, _) = ser.deserialize()?;
        assert_eq!(<&str>::try_from(&de)?, "hi!");
        assert_eq!(parsed, ser.len());
        Ok(())
    }

    #[test]
    fn map_conversion() -> Result<(), Box<dyn Error>> {
        let mut map = HashMap::<String, String>::new();
        map.insert("one".to_string(), "1".to_string());
        map.insert("two".to_string(), "2".to_string());
        let value = OwnedVariant::from(map.clone());
        // Now convert back
        let map2 = <HashMap<String, String>>::try_from(value)?;
        assert_eq!(map, map2);

        Ok(())
    }
}
