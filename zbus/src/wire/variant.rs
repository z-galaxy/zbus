use core::{
    cmp::Ordering,
    fmt::{Display, Write},
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::discriminant,
    str,
};

use serde::{
    de::{
        Deserialize, DeserializeSeed, Deserializer, Error, MapAccess, SeqAccess, Unexpected,
        Visitor,
    },
    ser::{
        Serialize, SerializeMap, SerializeSeq, SerializeStruct, SerializeTupleStruct, Serializer,
    },
};

use crate::wire::{
    Array, Basic, Dict, DynamicType, ObjectPath, OwnedVariant, Signature, Str, Structure, Type,
    array_display_fmt, dict_display_fmt, structure_display_fmt, utils::*,
};

#[cfg(unix)]
use crate::Fd;

/// A generic container, in the form of an enum that holds exactly one value of any of the other
/// types.
///
/// Note that this type corresponds to the `VARIANT` data type defined by the [D-Bus specification]
/// and as such, its encoding is not the same as that of the enclosed value.
///
/// # Examples
///
/// ```
/// use zbus::{Variant, wire::{LE, serialized::Context, to_bytes}};
///
/// // Create a Variant from an i16
/// let v = Variant::new(i16::MAX);
///
/// // Encode it
/// let ctxt = Context::new(LE, 0);
/// let encoding = to_bytes(ctxt, &v).unwrap();
///
/// // Decode it back
/// let v: Variant = encoding.deserialize().unwrap().0;
///
/// // Check everything is as expected
/// assert_eq!(i16::try_from(&v).unwrap(), i16::MAX);
/// ```
///
/// Now let's try a more complicated example:
///
/// ```
/// use zbus::wire::{to_bytes, serialized::Context, LE};
/// use zbus::{Str, Structure, Variant};
///
/// // Create a Variant from a tuple this time
/// let v = Variant::new((i16::MAX, "hello", true));
///
/// // Same drill as previous example
/// let ctxt = Context::new(LE, 0);
/// let encoding = to_bytes(ctxt, &v).unwrap();
/// let v: Variant = encoding.deserialize().unwrap().0;
///
/// // Check everything is as expected
/// let s = Structure::try_from(v).unwrap();
/// assert_eq!(
///     <(i16, Str, bool)>::try_from(s).unwrap(),
///     (i16::MAX, Str::from("hello"), true),
/// );
/// ```
///
/// [D-Bus specification]: https://dbus.freedesktop.org/doc/dbus-specification.html#container-types
#[derive(Debug, PartialEq, PartialOrd)]
pub enum Variant<'a> {
    // Simple types
    /// A `BYTE` (`y`).
    U8(u8),
    /// A `BOOLEAN` (`b`).
    Bool(bool),
    /// An `INT16` (`n`).
    I16(i16),
    /// A `UINT16` (`q`).
    U16(u16),
    /// An `INT32` (`i`).
    I32(i32),
    /// A `UINT32` (`u`).
    U32(u32),
    /// An `INT64` (`x`).
    I64(i64),
    /// A `UINT64` (`t`).
    U64(u64),
    /// A `DOUBLE` (`d`).
    F64(f64),
    /// A `STRING` (`s`).
    Str(Str<'a>),
    /// A `SIGNATURE` (`g`).
    Signature(Signature),
    /// An `OBJECT_PATH` (`o`).
    ObjectPath(ObjectPath<'a>),
    /// A `VARIANT` (`v`), holding another variant.
    Variant(Box<Variant<'a>>),

    // Container types
    /// An `ARRAY` (`a`) of values of one type.
    Array(Array<'a>),
    /// An `ARRAY` of `DICT_ENTRY` (`a{..}`), that is, a dictionary.
    Dict(Dict<'a, 'a>),
    /// A `STRUCT` (`(..)`).
    Structure(Structure<'a>),

    /// A `UNIX_FD` (`h`).
    #[cfg(unix)]
    Fd(Fd<'a>),
}

impl Hash for Variant<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        discriminant(self).hash(state);
        match self {
            Self::U8(inner) => inner.hash(state),
            Self::Bool(inner) => inner.hash(state),
            Self::I16(inner) => inner.hash(state),
            Self::U16(inner) => inner.hash(state),
            Self::I32(inner) => inner.hash(state),
            Self::U32(inner) => inner.hash(state),
            Self::I64(inner) => inner.hash(state),
            Self::U64(inner) => inner.hash(state),
            // To hold the +0.0 == -0.0 => hash(+0.0) == hash(-0.0) property.
            // See https://doc.rust-lang.org/beta/std/hash/trait.Hash.html#hash-and-eq
            Self::F64(inner) if *inner == 0. => 0f64.to_le_bytes().hash(state),
            Self::F64(inner) => inner.to_le_bytes().hash(state),
            Self::Str(inner) => inner.hash(state),
            Self::Signature(inner) => inner.hash(state),
            Self::ObjectPath(inner) => inner.hash(state),
            Self::Variant(inner) => inner.hash(state),
            Self::Array(inner) => inner.hash(state),
            Self::Dict(inner) => inner.hash(state),
            Self::Structure(inner) => inner.hash(state),
            #[cfg(unix)]
            Self::Fd(inner) => inner.hash(state),
        }
    }
}

impl Eq for Variant<'_> {}

impl Ord for Variant<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other)
            .unwrap_or_else(|| match (self, other) {
                (Self::F64(lhs), Self::F64(rhs)) => lhs.total_cmp(rhs),
                // `partial_cmp` returns `Some(_)` if either the discriminants are different
                // or if both the left hand side and right hand side is `Self::F64(_)`. We can only
                // reach this arm, if only one of the sides is `Self::F64(_)`. So we can just
                // pretend the ordering is equal.
                _ => Ordering::Equal,
            })
    }
}

macro_rules! serialize_value {
    ($self:ident $serializer:ident.$method:ident $($first_arg:expr)*) => {
        match $self {
            Variant::U8(value) => $serializer.$method($($first_arg,)* value),
            Variant::Bool(value) => $serializer.$method($($first_arg,)* value),
            Variant::I16(value) => $serializer.$method($($first_arg,)* value),
            Variant::U16(value) => $serializer.$method($($first_arg,)* value),
            Variant::I32(value) => $serializer.$method($($first_arg,)* value),
            Variant::U32(value) => $serializer.$method($($first_arg,)* value),
            Variant::I64(value) => $serializer.$method($($first_arg,)* value),
            Variant::U64(value) => $serializer.$method($($first_arg,)* value),
            Variant::F64(value) => $serializer.$method($($first_arg,)* value),
            Variant::Str(value) => $serializer.$method($($first_arg,)* value),
            Variant::Signature(value) => $serializer.$method($($first_arg,)* value),
            Variant::ObjectPath(value) => $serializer.$method($($first_arg,)* value),
            Variant::Variant(value) => $serializer.$method($($first_arg,)* value),

            // Container types
            Variant::Array(value) => $serializer.$method($($first_arg,)* value),
            Variant::Dict(value) => $serializer.$method($($first_arg,)* value),
            Variant::Structure(value) => $serializer.$method($($first_arg,)* value),

            #[cfg(unix)]
            Variant::Fd(value) => $serializer.$method($($first_arg,)* value),
        }
    }
}

impl<'a> Variant<'a> {
    /// Make a [`Variant`] for a given value.
    ///
    /// In general, you can use [`Into`] trait on basic types, except
    /// when you explicitly need to wrap [`Variant`] itself, in which
    /// case this constructor comes handy.
    ///
    /// # Examples
    ///
    /// ```
    /// use zbus::Variant;
    ///
    /// let s = Variant::new("hello");
    /// let u: Variant = 51.into();
    /// assert_ne!(s, u);
    /// ```
    ///
    /// [`Variant`]: enum.Variant.html
    /// [`Into`]: https://doc.rust-lang.org/std/convert/trait.Into.html
    pub fn new<T>(value: T) -> Self
    where
        T: Into<Self> + DynamicType,
    {
        // With specialization, we wouldn't have this
        if value.signature() == VARIANT_SIGNATURE_STR {
            Self::Variant(Box::new(value.into()))
        } else {
            value.into()
        }
    }

    /// Make a [`Variant::Variant`], the variant holding a variant, under its pre-6.0 name.
    ///
    /// Before zbus 6.0, this type was named `Value` and so was this variant. This function keeps
    /// expressions like `Value::Value(inner)` compiling through the deprecated
    /// `zbus::zvariant::Value` alias. A pattern can not name a function, so matching on the
    /// variant needs its new name, [`Variant::Variant`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #![allow(deprecated)]
    /// use zbus::Variant;
    ///
    /// let nested = Variant::Value(Box::new(Variant::U32(42)));
    /// assert_eq!(nested, Variant::Variant(Box::new(Variant::U32(42))));
    /// ```
    #[deprecated(since = "6.0.0", note = "use `Variant::Variant` instead")]
    #[allow(non_snake_case)]
    pub fn Value(inner: Box<Variant<'a>>) -> Self {
        Self::Variant(inner)
    }

    /// Try to create an owned version of `self`.
    ///
    /// # Errors
    ///
    /// This method can currently only fail on Unix platforms for [`Variant::Fd`] variant. This
    /// happens when the current process exceeds the maximum number of open file descriptors.
    pub fn try_to_owned(&self) -> crate::Result<OwnedVariant> {
        Ok(OwnedVariant(match self {
            Variant::U8(v) => Variant::U8(*v),
            Variant::Bool(v) => Variant::Bool(*v),
            Variant::I16(v) => Variant::I16(*v),
            Variant::U16(v) => Variant::U16(*v),
            Variant::I32(v) => Variant::I32(*v),
            Variant::U32(v) => Variant::U32(*v),
            Variant::I64(v) => Variant::I64(*v),
            Variant::U64(v) => Variant::U64(*v),
            Variant::F64(v) => Variant::F64(*v),
            Variant::Str(v) => Variant::Str(v.to_owned()),
            Variant::Signature(v) => Variant::Signature(v.to_owned()),
            Variant::ObjectPath(v) => Variant::ObjectPath(v.to_owned()),
            Variant::Variant(v) => {
                let o = OwnedVariant::try_from(&**v)?;
                Variant::Variant(Box::new(o.into_inner()))
            }

            Variant::Array(v) => Variant::Array(v.try_to_owned()?),
            Variant::Dict(v) => Variant::Dict(v.try_to_owned()?),
            Variant::Structure(v) => Variant::Structure(v.try_to_owned()?),
            #[cfg(unix)]
            Variant::Fd(v) => Variant::Fd(v.try_to_owned()?),
        }))
    }

    /// Creates an owned value from `self`.
    ///
    /// This method can currently only fail on Unix platforms for [`Variant::Fd`] variant containing
    /// an [`Fd::Owned`] variant. This happens when the current process exceeds the maximum number
    /// of open file descriptors.
    ///
    /// Results in an extra allocation if the value contains borrowed data.
    pub fn try_into_owned(self) -> crate::Result<OwnedVariant> {
        Ok(OwnedVariant(match self {
            Variant::U8(v) => Variant::U8(v),
            Variant::Bool(v) => Variant::Bool(v),
            Variant::I16(v) => Variant::I16(v),
            Variant::U16(v) => Variant::U16(v),
            Variant::I32(v) => Variant::I32(v),
            Variant::U32(v) => Variant::U32(v),
            Variant::I64(v) => Variant::I64(v),
            Variant::U64(v) => Variant::U64(v),
            Variant::F64(v) => Variant::F64(v),
            Variant::Str(v) => Variant::Str(v.into_owned()),
            Variant::Signature(v) => Variant::Signature(v),
            Variant::ObjectPath(v) => Variant::ObjectPath(v.into_owned()),
            Variant::Variant(v) => Variant::Variant(Box::new(v.try_into_owned()?.into())),
            Variant::Array(v) => Variant::Array(v.try_into_owned()?),
            Variant::Dict(v) => Variant::Dict(v.try_into_owned()?),
            Variant::Structure(v) => Variant::Structure(v.try_into_owned()?),
            #[cfg(unix)]
            Variant::Fd(v) => Variant::Fd(v.try_to_owned()?),
        }))
    }

    /// Get the signature of the enclosed value.
    pub fn value_signature(&self) -> &Signature {
        match self {
            Variant::U8(_) => u8::SIGNATURE,
            Variant::Bool(_) => bool::SIGNATURE,
            Variant::I16(_) => i16::SIGNATURE,
            Variant::U16(_) => u16::SIGNATURE,
            Variant::I32(_) => i32::SIGNATURE,
            Variant::U32(_) => u32::SIGNATURE,
            Variant::I64(_) => i64::SIGNATURE,
            Variant::U64(_) => u64::SIGNATURE,
            Variant::F64(_) => f64::SIGNATURE,
            Variant::Str(_) => <&str>::SIGNATURE,
            Variant::Signature(_) => Signature::SIGNATURE,
            Variant::ObjectPath(_) => ObjectPath::SIGNATURE,
            Variant::Variant(_) => &Signature::Variant,

            // Container types
            Variant::Array(value) => value.signature(),
            Variant::Dict(value) => value.signature(),
            Variant::Structure(value) => value.signature(),

            #[cfg(unix)]
            Variant::Fd(_) => Fd::SIGNATURE,
        }
    }

    /// Try to clone the value.
    ///
    /// # Errors
    ///
    /// This method can currently only fail on Unix platforms for [`Variant::Fd`] variant containing
    /// an [`Fd::Owned`] variant. This happens when the current process exceeds the maximum number
    /// of open file descriptors.
    pub fn try_clone(&self) -> crate::Result<Self> {
        Ok(match self {
            Variant::U8(v) => Variant::U8(*v),
            Variant::Bool(v) => Variant::Bool(*v),
            Variant::I16(v) => Variant::I16(*v),
            Variant::U16(v) => Variant::U16(*v),
            Variant::I32(v) => Variant::I32(*v),
            Variant::U32(v) => Variant::U32(*v),
            Variant::I64(v) => Variant::I64(*v),
            Variant::U64(v) => Variant::U64(*v),
            Variant::F64(v) => Variant::F64(*v),
            Variant::Str(v) => Variant::Str(v.clone()),
            Variant::Signature(v) => Variant::Signature(v.clone()),
            Variant::ObjectPath(v) => Variant::ObjectPath(v.clone()),
            Variant::Variant(v) => Variant::Variant(Box::new(v.try_clone()?)),
            Variant::Array(v) => Variant::Array(v.try_clone()?),
            Variant::Dict(v) => Variant::Dict(v.try_clone()?),
            Variant::Structure(v) => Variant::Structure(v.try_clone()?),
            #[cfg(unix)]
            Variant::Fd(v) => Variant::Fd(v.try_clone()?),
        })
    }

    pub(crate) fn serialize_value_as_struct_field<S>(
        &self,
        name: &'static str,
        serializer: &mut S,
    ) -> Result<(), S::Error>
    where
        S: SerializeStruct,
    {
        serialize_value!(self serializer.serialize_field name)
    }

    pub(crate) fn serialize_value_as_tuple_struct_field<S>(
        &self,
        serializer: &mut S,
    ) -> Result<(), S::Error>
    where
        S: SerializeTupleStruct,
    {
        serialize_value!(self serializer.serialize_field)
    }

    // Really crappy that we need to do this separately for struct and seq cases. :(
    pub(crate) fn serialize_value_as_seq_element<S>(
        &self,
        serializer: &mut S,
    ) -> Result<(), S::Error>
    where
        S: SerializeSeq,
    {
        serialize_value!(self serializer.serialize_element)
    }

    pub(crate) fn serialize_value_as_dict_key<S>(&self, serializer: &mut S) -> Result<(), S::Error>
    where
        S: SerializeMap,
    {
        serialize_value!(self serializer.serialize_key)
    }

    pub(crate) fn serialize_value_as_dict_value<S>(
        &self,
        serializer: &mut S,
    ) -> Result<(), S::Error>
    where
        S: SerializeMap,
    {
        serialize_value!(self serializer.serialize_value)
    }

    /// Try to get the underlying type `T`.
    ///
    /// Note that [`TryFrom<Variant>`] is implemented for various types, and it's usually best to
    /// use that instead. However, in generic code where you also want to unwrap
    /// [`Variant::Variant`], you should use this function (because [`TryFrom<Variant>`] can not
    /// be implemented for `Variant` itself as [`From<Variant>`] is implicitly implemented for
    /// `Variant`).
    ///
    /// # Examples
    ///
    /// ```
    /// use zbus::{Error, Result, Variant};
    ///
    /// fn value_vec_to_type_vec<'a, T>(values: Vec<Variant<'a>>) -> Result<Vec<T>>
    /// where
    ///     T: TryFrom<Variant<'a>>,
    ///     <T as TryFrom<Variant<'a>>>::Error: Into<Error>,
    /// {
    ///     let mut res = vec![];
    ///     for value in values.into_iter() {
    ///         res.push(value.downcast()?);
    ///     }
    ///
    ///     Ok(res)
    /// }
    ///
    /// // Let's try u32 values first
    /// let v = vec![Variant::U32(42), Variant::U32(43)];
    /// let v = value_vec_to_type_vec::<u32>(v).unwrap();
    /// assert_eq!(v[0], 42);
    /// assert_eq!(v[1], 43);
    ///
    /// // Now try Variant values
    /// let v = vec![Variant::new(Variant::U32(42)), Variant::new(Variant::U32(43))];
    /// let v = value_vec_to_type_vec::<Variant>(v).unwrap();
    /// assert_eq!(v[0], Variant::U32(42));
    /// assert_eq!(v[1], Variant::U32(43));
    /// ```
    ///
    /// [`Variant::Variant`]: enum.Variant.html#variant.Variant
    /// [`TryFrom<Variant>`]: https://doc.rust-lang.org/std/convert/trait.TryFrom.html
    /// [`From<Variant>`]: https://doc.rust-lang.org/std/convert/trait.From.html
    pub fn downcast<T>(self) -> Result<T, crate::Error>
    where
        T: TryFrom<Variant<'a>>,
        <T as TryFrom<Variant<'a>>>::Error: Into<crate::Error>,
    {
        if let Variant::Variant(v) = self {
            T::try_from(*v)
        } else {
            T::try_from(self)
        }
        .map_err(Into::into)
    }

    /// Try to get the underlying type `T`.
    ///
    /// Same as [`downcast`] except it doesn't consume `self` and hence requires
    /// `T: TryFrom<&Variant<_>>`.
    ///
    /// # Examples
    ///
    /// ```
    /// use zbus::{Error, Result, Variant};
    ///
    /// fn value_vec_to_type_vec<'a, T>(values: &'a Vec<Variant<'a>>) -> Result<Vec<&'a T>>
    /// where
    ///     &'a T: TryFrom<&'a Variant<'a>>,
    ///     <&'a T as TryFrom<&'a Variant<'a>>>::Error: Into<Error>,
    /// {
    ///     let mut res = vec![];
    ///     for value in values.into_iter() {
    ///         res.push(value.downcast_ref()?);
    ///     }
    ///
    ///     Ok(res)
    /// }
    ///
    /// // Let's try u32 values first
    /// let v = vec![Variant::U32(42), Variant::U32(43)];
    /// let v = value_vec_to_type_vec::<u32>(&v).unwrap();
    /// assert_eq!(*v[0], 42);
    /// assert_eq!(*v[1], 43);
    ///
    /// // Now try Variant values
    /// let v = vec![Variant::new(Variant::U32(42)), Variant::new(Variant::U32(43))];
    /// let v = value_vec_to_type_vec::<Variant>(&v).unwrap();
    /// assert_eq!(*v[0], Variant::U32(42));
    /// assert_eq!(*v[1], Variant::U32(43));
    /// ```
    ///
    /// [`downcast`]: enum.Variant.html#method.downcast
    pub fn downcast_ref<T>(&'a self) -> Result<T, crate::Error>
    where
        T: TryFrom<&'a Variant<'a>>,
        <T as TryFrom<&'a Variant<'a>>>::Error: Into<crate::Error>,
    {
        if let Variant::Variant(v) = self {
            <T>::try_from(v)
        } else {
            <T>::try_from(self)
        }
        .map_err(Into::into)
    }
}

impl Display for Variant<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        value_display_fmt(self, f, true)
    }
}

/// Implemented based on https://gitlab.gnome.org/GNOME/glib/-/blob/e1d47f0b0d0893ac9171e24cc7bf635495376546/glib/gvariant.c#L2213
pub(crate) fn value_display_fmt(
    value: &Variant<'_>,
    f: &mut std::fmt::Formatter<'_>,
    type_annotate: bool,
) -> std::fmt::Result {
    match value {
        Variant::U8(num) => {
            if type_annotate {
                f.write_str("byte ")?;
            }
            write!(f, "0x{num:02x}")
        }
        Variant::Bool(boolean) => {
            write!(f, "{boolean}")
        }
        Variant::I16(num) => {
            if type_annotate {
                f.write_str("int16 ")?;
            }
            write!(f, "{num}")
        }
        Variant::U16(num) => {
            if type_annotate {
                f.write_str("uint16 ")?;
            }
            write!(f, "{num}")
        }
        Variant::I32(num) => {
            // Never annotate this type because it is the default for numbers
            write!(f, "{num}")
        }
        Variant::U32(num) => {
            if type_annotate {
                f.write_str("uint32 ")?;
            }
            write!(f, "{num}")
        }
        Variant::I64(num) => {
            if type_annotate {
                f.write_str("int64 ")?;
            }
            write!(f, "{num}")
        }
        Variant::U64(num) => {
            if type_annotate {
                f.write_str("uint64 ")?;
            }
            write!(f, "{num}")
        }
        Variant::F64(num) => {
            if num.fract() == 0. {
                // Add a dot to make it clear that this is a float
                write!(f, "{num}.")
            } else {
                write!(f, "{num}")
            }
        }
        Variant::Str(string) => {
            write!(f, "{:?}", string.as_str())
        }
        Variant::Signature(val) => {
            if type_annotate {
                f.write_str("signature ")?;
            }
            write!(f, "{:?}", val.to_string())
        }
        Variant::ObjectPath(val) => {
            if type_annotate {
                f.write_str("objectpath ")?;
            }
            write!(f, "{:?}", val.as_str())
        }
        Variant::Variant(child) => {
            f.write_char('<')?;

            // Always annotate types in nested variants, because they are (by nature) of
            // variable type.
            value_display_fmt(child, f, true)?;

            f.write_char('>')?;
            Ok(())
        }
        Variant::Array(array) => array_display_fmt(array, f, type_annotate),
        Variant::Dict(dict) => dict_display_fmt(dict, f, type_annotate),
        Variant::Structure(structure) => structure_display_fmt(structure, f, type_annotate),
        #[cfg(unix)]
        Variant::Fd(handle) => {
            if type_annotate {
                f.write_str("handle ")?;
            }
            write!(f, "{handle}")
        }
    }
}

impl Serialize for Variant<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serializer implementation needs to ensure padding isn't added for Variant.
        let mut structure = serializer.serialize_struct("Variant", 2)?;

        let signature = self.value_signature();
        structure.serialize_field("signature", &signature)?;

        self.serialize_value_as_struct_field("value", &mut structure)?;

        structure.end()
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for Variant<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let visitor = ValueVisitor;

        deserializer.deserialize_any(visitor)
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Variant<'de>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a Variant")
    }

    fn visit_seq<V>(self, mut visitor: V) -> Result<Variant<'de>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let signature = visitor.next_element::<Signature>()?.ok_or_else(|| {
            Error::invalid_value(Unexpected::Other("nothing"), &"a Variant signature")
        })?;
        let seed = ValueSeed::<Variant<'_>> {
            signature: &signature,
            phantom: PhantomData,
        };

        visitor
            .next_element_seed(seed)?
            .ok_or_else(|| Error::invalid_value(Unexpected::Other("nothing"), &"a Variant value"))
    }

    fn visit_map<V>(self, mut visitor: V) -> Result<Variant<'de>, V::Error>
    where
        V: MapAccess<'de>,
    {
        let (_, signature) = visitor.next_entry::<&str, Signature>()?.ok_or_else(|| {
            Error::invalid_value(Unexpected::Other("nothing"), &"a Variant signature")
        })?;
        let _ = visitor.next_key::<&str>()?;

        let seed = ValueSeed::<Variant<'_>> {
            signature: &signature,
            phantom: PhantomData,
        };
        visitor.next_value_seed(seed)
    }
}

pub(crate) struct SignatureSeed<'sig> {
    pub signature: &'sig Signature,
}

impl SignatureSeed<'_> {
    pub(crate) fn visit_array<'de, V>(self, mut visitor: V) -> Result<Array<'de>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let element_signature = match self.signature {
            Signature::Array(child) => child.signature(),
            _ => {
                return Err(Error::invalid_type(
                    Unexpected::Str(&self.signature.to_string()),
                    &"an array signature",
                ));
            }
        };
        let mut array = Array::new_full_signature(self.signature);

        while let Some(elem) = visitor.next_element_seed(ValueSeed::<Variant<'_>> {
            signature: element_signature,
            phantom: PhantomData,
        })? {
            elem.value_signature();
            array.append(elem).map_err(Error::custom)?;
        }

        Ok(array)
    }

    pub(crate) fn visit_struct<'de, V>(self, mut visitor: V) -> Result<Structure<'de>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let fields_signatures = match self.signature {
            Signature::Structure(fields) => fields.iter(),
            _ => {
                return Err(Error::invalid_type(
                    Unexpected::Str(&self.signature.to_string()),
                    &"a structure signature",
                ));
            }
        };

        let mut builder = Structure::builder();
        for field_signature in fields_signatures {
            if let Some(field) = visitor.next_element_seed(ValueSeed::<Variant<'_>> {
                signature: field_signature,
                phantom: PhantomData,
            })? {
                builder = builder.append_field(field);
            }
        }
        Ok(builder.build_with_signature(self.signature))
    }
}

impl<'sig, T> From<ValueSeed<'sig, T>> for SignatureSeed<'sig> {
    fn from(seed: ValueSeed<'sig, T>) -> Self {
        SignatureSeed {
            signature: seed.signature,
        }
    }
}

struct ValueSeed<'sig, T> {
    signature: &'sig Signature,
    phantom: PhantomData<T>,
}

impl<'de, T> ValueSeed<'_, T>
where
    T: Deserialize<'de>,
{
    #[inline]
    fn visit_array<V>(self, visitor: V) -> Result<Variant<'de>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        SignatureSeed::from(self)
            .visit_array(visitor)
            .map(Variant::Array)
    }

    #[inline]
    fn visit_struct<V>(self, visitor: V) -> Result<Variant<'de>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        SignatureSeed::from(self)
            .visit_struct(visitor)
            .map(Variant::Structure)
    }

    #[inline]
    fn visit_variant_as_seq<V>(self, visitor: V) -> Result<Variant<'de>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        ValueVisitor
            .visit_seq(visitor)
            .map(|v| Variant::Variant(Box::new(v)))
    }

    #[inline]
    fn visit_variant_as_map<V>(self, visitor: V) -> Result<Variant<'de>, V::Error>
    where
        V: MapAccess<'de>,
    {
        ValueVisitor
            .visit_map(visitor)
            .map(|v| Variant::Variant(Box::new(v)))
    }
}

macro_rules! value_seed_basic_method {
    ($name:ident, $type:ty) => {
        #[inline]
        fn $name<E>(self, value: $type) -> Result<Variant<'static>, E>
        where
            E: serde::de::Error,
        {
            Ok(value.into())
        }
    };
}

impl<'de, T> Visitor<'de> for ValueSeed<'_, T>
where
    T: Deserialize<'de>,
{
    type Value = Variant<'de>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a Variant value")
    }

    value_seed_basic_method!(visit_bool, bool);
    value_seed_basic_method!(visit_i16, i16);
    value_seed_basic_method!(visit_i64, i64);
    value_seed_basic_method!(visit_u8, u8);
    value_seed_basic_method!(visit_u16, u16);
    value_seed_basic_method!(visit_u32, u32);
    value_seed_basic_method!(visit_u64, u64);
    value_seed_basic_method!(visit_f64, f64);

    fn visit_i32<E>(self, value: i32) -> Result<Variant<'de>, E>
    where
        E: serde::de::Error,
    {
        let v = match &self.signature {
            #[cfg(unix)]
            Signature::Fd => {
                debug_assert!(value >= 0);
                // SAFETY: The `'de` lifetimes will ensure the borrow won't outlive the raw FD.
                let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(value) };
                Fd::Borrowed(fd).into()
            }
            _ => value.into(),
        };

        Ok(v)
    }

    #[inline]
    fn visit_str<E>(self, value: &str) -> Result<Variant<'de>, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(String::from(value))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match &self.signature {
            Signature::Str => Ok(Variant::Str(Str::from(v))),
            Signature::Signature => Signature::try_from(v)
                .map(Variant::Signature)
                .map_err(Error::custom),
            Signature::ObjectPath => Ok(Variant::ObjectPath(ObjectPath::from_str_unchecked(v))),
            _ => {
                let expected = format!(
                    "`{}`, `{}` or `{}`",
                    <&str>::SIGNATURE_STR,
                    Signature::SIGNATURE_STR,
                    ObjectPath::SIGNATURE_STR,
                );
                Err(Error::invalid_type(
                    Unexpected::Str(&self.signature.to_string()),
                    &expected.as_str(),
                ))
            }
        }
    }

    fn visit_seq<V>(self, visitor: V) -> Result<Variant<'de>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        match &self.signature {
            // For some reason rustc doesn't like us using ARRAY_SIGNATURE_CHAR const
            Signature::Array(_) => self.visit_array(visitor),
            Signature::Structure(_) => self.visit_struct(visitor),
            Signature::Variant => self.visit_variant_as_seq(visitor),
            s => Err(Error::invalid_value(
                Unexpected::Str(&s.to_string()),
                &"a Variant signature",
            )),
        }
    }

    fn visit_map<V>(self, mut visitor: V) -> Result<Variant<'de>, V::Error>
    where
        V: MapAccess<'de>,
    {
        let (key_signature, value_signature) = match &self.signature {
            Signature::Dict { key, value } => (key.signature().clone(), value.signature().clone()),
            Signature::Variant => return self.visit_variant_as_map(visitor),
            _ => {
                return Err(Error::invalid_type(
                    Unexpected::Str(&self.signature.to_string()),
                    &"a dict signature",
                ));
            }
        };

        let mut dict = Dict::new_full_signature(self.signature);

        while let Some((key, value)) = visitor.next_entry_seed(
            ValueSeed::<Variant<'_>> {
                signature: &key_signature,
                phantom: PhantomData,
            },
            ValueSeed::<Variant<'_>> {
                signature: &value_signature,
                phantom: PhantomData,
            },
        )? {
            dict.append(key, value).map_err(Error::custom)?;
        }

        Ok(Variant::Dict(dict))
    }

    fn visit_some<D>(self, _deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        panic!("`Maybe` types are not valid in the D-Bus format; use the `zgvariant` crate");
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        panic!("`Maybe` types are not valid in the D-Bus format; use the `zgvariant` crate");
    }
}

impl<'de, T> DeserializeSeed<'de> for ValueSeed<'_, T>
where
    T: Deserialize<'de>,
{
    type Value = Variant<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl Type for Variant<'_> {
    const SIGNATURE: &'static Signature = &Signature::Variant;
}

impl<'a> TryFrom<&Variant<'a>> for Variant<'a> {
    type Error = crate::Error;

    fn try_from(value: &Variant<'a>) -> crate::Result<Variant<'a>> {
        value.try_clone()
    }
}

impl Clone for Variant<'_> {
    /// Clone the value.
    ///
    /// # Panics
    ///
    /// This method can only fail on Unix platforms for [`Variant::Fd`] variant containing an
    /// [`Fd::Owned`] variant. This happens when the current process exceeds the limit on maximum
    /// number of open file descriptors.
    fn clone(&self) -> Self {
        self.try_clone()
            .expect("Process exceeded limit on maximum number of open file descriptors")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn value_display() {
        assert_eq!(
            Variant::new((
                255_u8,
                true,
                -1_i16,
                65535_u16,
                -1,
                1_u32,
                -9223372036854775808_i64,
                18446744073709551615_u64,
                (-1., 1.0, 11000000000., 1.1e-10)
            ))
            .to_string(),
            "(byte 0xff, true, int16 -1, uint16 65535, -1, uint32 1, \
                int64 -9223372036854775808, uint64 18446744073709551615, \
                (-1., 1., 11000000000., 0.00000000011))"
        );

        assert_eq!(
            Variant::new(vec![
                "", " ", "a", r#"""#, "'", "a'b", "a'\"b", "\\", "\n'\"",
            ])
            .to_string(),
            r#"["", " ", "a", "\"", "'", "a'b", "a'\"b", "\\", "\n'\""]"#
        );
        assert_eq!(
            Variant::new(vec![
                "\x07\x08\x09\x0A\x0B\x0C\x0D",
                "\x7F",
                char::from_u32(0xD8000).unwrap().to_string().as_str()
            ])
            .to_string(),
            r#"["\u{7}\u{8}\t\n\u{b}\u{c}\r", "\u{7f}", "\u{d8000}"]"#
        );

        assert_eq!(
            Variant::new((
                vec![crate::signature!(""), crate::signature!("(ysa{sd})")],
                vec![
                    ObjectPath::from_static_str("/").unwrap(),
                    ObjectPath::from_static_str("/a/very/looooooooooooooooooooooooo0000o0ng/path")
                        .unwrap(),
                ],
                vec![
                    Variant::new(0_u8),
                    Variant::new((Variant::new(51), Variant::new(Variant::new(1_u32)))),
                ]
            ))
            .to_string(),
            "([signature \"\", \"(ysa{sd})\"], \
                [objectpath \"/\", \"/a/very/looooooooooooooooooooooooo0000o0ng/path\"], \
                [<byte 0x00>, <(<51>, <<uint32 1>>)>])"
        );

        assert_eq!(Variant::new(vec![] as Vec<Vec<i64>>).to_string(), "@aax []");
        assert_eq!(
            Variant::new(vec![
                vec![0_i16, 1_i16],
                vec![2_i16, 3_i16],
                vec![4_i16, 5_i16]
            ])
            .to_string(),
            "[[int16 0, 1], [2, 3], [4, 5]]"
        );
        assert_eq!(
            Variant::new(vec![
                b"Hello".to_vec(),
                b"Hell\0o".to_vec(),
                b"H\0ello\0".to_vec(),
                b"Hello\0".to_vec(),
                b"\0".to_vec(),
                b" \0".to_vec(),
                b"'\0".to_vec(),
                b"\n'\"\0".to_vec(),
                b"\\\0".to_vec(),
            ])
            .to_string(),
            "[[byte 0x48, 0x65, 0x6c, 0x6c, 0x6f], \
                [0x48, 0x65, 0x6c, 0x6c, 0x00, 0x6f], \
                [0x48, 0x00, 0x65, 0x6c, 0x6c, 0x6f, 0x00], \
                b\"Hello\", b\"\", b\" \", b\"'\", b\"\\n'\\\"\", b\"\\\\\"]"
        );
        // Bytes that aren't printable ASCII are escaped, rather than replaced with U+FFFD as
        // decoding them as UTF-8 would, so that the bytes can be told apart.
        assert_eq!(
            Variant::new(vec![
                b"\x80\xa0\xd0\xff\0".to_vec(),
                "\u{3042}\0".as_bytes().to_vec(),
                b"\x01\t\x7f\0".to_vec(),
            ])
            .to_string(),
            "[b\"\\x80\\xa0\\xd0\\xff\", b\"\\xe3\\x81\\x82\", b\"\\x01\\t\\x7f\"]"
        );

        assert_eq!(
            Variant::new(HashMap::<bool, bool>::new()).to_string(),
            "@a{bb} {}"
        );
        assert_eq!(
            Variant::new(vec![(true, 0_i64)].into_iter().collect::<HashMap<_, _>>()).to_string(),
            "{true: int64 0}",
        );
        // The order of the entries may vary
        let val = Variant::new(
            vec![(32_u16, 64_i64), (100_u16, 200_i64)]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        )
        .to_string();
        assert!(val.starts_with('{'));
        assert!(val.ends_with('}'));
        assert_eq!(val.matches("uint16").count(), 1);
        assert_eq!(val.matches("int64").count(), 1);

        let items_str = val.split(", ").collect::<Vec<_>>();
        assert_eq!(items_str.len(), 2);
        assert!(
            items_str
                .iter()
                .any(|str| str.contains("32") && str.contains(": ") && str.contains("64"))
        );
        assert!(
            items_str
                .iter()
                .any(|str| str.contains("100") && str.contains(": ") && str.contains("200"))
        );

        assert_eq!(
            Variant::new(((true,), (true, false), (true, true, false))).to_string(),
            "((true,), (true, false), (true, true, false))"
        );

        #[cfg(feature = "option-as-array")]
        {
            #[cfg(unix)]
            use std::os::fd::BorrowedFd;

            let s = "(([int16 0], [[int16 0]], [[[int16 0]]]), \
                (@an [], [@an []], [[@an []]]), \
                (@aan [], [@aan []]))";
            assert_eq!(
                Variant::new((
                    (Some(0_i16), Some(Some(0_i16)), Some(Some(Some(0_i16))),),
                    (None::<i16>, Some(None::<i16>), Some(Some(None::<i16>)),),
                    (None::<Option<i16>>, Some(None::<Option<i16>>)),
                ))
                .to_string(),
                s,
            );

            #[cfg(unix)]
            assert_eq!(
                Variant::new(vec![
                    Fd::from(unsafe { BorrowedFd::borrow_raw(0) }),
                    Fd::from(unsafe { BorrowedFd::borrow_raw(-100) })
                ])
                .to_string(),
                "[handle 0, -100]"
            );

            let s = "(@ab [], @ab [], [{\"size\": <(800, 600)>}], \
                [<1>, <{\"dimension\": <([2.4, 1.], [[int16 200]], \
                <(byte 0x03, \"Hello!\")>)>}>], 7777, objectpath \"/\", 8888)";
            assert_eq!(
                Variant::new((
                    None::<bool>,
                    None::<bool>,
                    Some(
                        vec![("size", Variant::new((800, 600)))]
                            .into_iter()
                            .collect::<HashMap<_, _>>()
                    ),
                    vec![
                        Variant::new(1),
                        Variant::new(
                            vec![(
                                "dimension",
                                Variant::new((
                                    vec![2.4, 1.],
                                    Some(Some(200_i16)),
                                    Variant::new((3_u8, "Hello!"))
                                ))
                            )]
                            .into_iter()
                            .collect::<HashMap<_, _>>()
                        )
                    ],
                    7777,
                    ObjectPath::from_static_str("/").unwrap(),
                    8888
                ))
                .to_string(),
                s,
            );
        }
    }
}
