use core::{
    borrow::Borrow,
    fmt::{self, Debug, Display, Formatter},
    ops::Deref,
};
use std::{borrow::Cow, sync::Arc};

use crate::{
    Error, NoneValue, OwnedVariant, Result, Str, Type, Variant,
    names::{
        OwnedUniqueName, OwnedWellKnownName, UniqueName, WellKnownName, utils::impl_str_basic,
    },
};
use serde::{Deserialize, Serialize, de};
use zbus_utils::names::BusNameKind;

/// String that identifies a [bus name].
///
/// # Examples
///
/// ```
/// use zbus::names::BusName;
///
/// // Valid well-known names.
/// let name = BusName::try_from("org.gnome.Service-for_you").unwrap();
/// assert!(matches!(name, BusName::WellKnown(_)));
/// assert_eq!(name, "org.gnome.Service-for_you");
/// let name = BusName::try_from("a.very.loooooooooooooooooo-ooooooo_0000o0ng.Name").unwrap();
/// assert!(matches!(name, BusName::WellKnown(_)));
/// assert_eq!(name, "a.very.loooooooooooooooooo-ooooooo_0000o0ng.Name");
///
/// // Valid unique names.
/// let name = BusName::try_from(":org.gnome.Service-for_you").unwrap();
/// assert!(matches!(name, BusName::Unique(_)));
/// assert_eq!(name, ":org.gnome.Service-for_you");
/// let name = BusName::try_from(":a.very.loooooooooooooooooo-ooooooo_0000o0ng.Name").unwrap();
/// assert!(matches!(name, BusName::Unique(_)));
/// assert_eq!(name, ":a.very.loooooooooooooooooo-ooooooo_0000o0ng.Name");
///
/// // Invalid bus names
/// BusName::try_from("").unwrap_err();
/// BusName::try_from("double..dots").unwrap_err();
/// BusName::try_from(".").unwrap_err();
/// BusName::try_from(".start.with.dot").unwrap_err();
/// BusName::try_from("1start.with.digit").unwrap_err();
/// BusName::try_from("no-dots").unwrap_err();
/// ```
///
/// [bus name]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-names-bus
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(untagged)]
pub enum BusName<'name> {
    /// A [`UniqueName`], which the bus assigns to a connection.
    #[serde(borrow)]
    Unique(UniqueName<'name>),
    /// A [`WellKnownName`], which a connection can request for the service it provides.
    #[serde(borrow)]
    WellKnown(WellKnownName<'name>),
}

impl_str_basic!(BusName<'_>);

impl BusName<'_> {
    /// This is faster than `Clone::clone` when `self` contains owned data.
    pub fn as_ref(&self) -> BusName<'_> {
        match self {
            BusName::Unique(name) => BusName::Unique(name.as_ref()),
            BusName::WellKnown(name) => BusName::WellKnown(name.as_ref()),
        }
    }

    /// The well-known-name as string.
    pub fn as_str(&self) -> &str {
        match self {
            BusName::Unique(name) => name.as_str(),
            BusName::WellKnown(name) => name.as_str(),
        }
    }

    /// Creates an owned clone of `self`.
    pub fn to_owned(&self) -> BusName<'static> {
        match self {
            BusName::Unique(name) => BusName::Unique(name.to_owned()),
            BusName::WellKnown(name) => BusName::WellKnown(name.to_owned()),
        }
    }

    /// Creates an owned clone of `self`.
    pub fn into_owned(self) -> BusName<'static> {
        match self {
            BusName::Unique(name) => BusName::Unique(name.into_owned()),
            BusName::WellKnown(name) => BusName::WellKnown(name.into_owned()),
        }
    }

    /// Same as `try_from`, except it takes a `&'static str`.
    pub fn from_static_str(name: &'static str) -> Result<Self> {
        match Self::try_from(name)? {
            BusName::Unique(_) => Ok(BusName::Unique(UniqueName::from_static_str_unchecked(name))),
            BusName::WellKnown(_) => Ok(BusName::WellKnown(
                WellKnownName::from_static_str_unchecked(name),
            )),
        }
    }
}

impl Deref for BusName<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Borrow<str> for BusName<'_> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Debug for BusName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BusName::Unique(name) => f
                .debug_tuple("BusName::Unique")
                .field(&name.as_str())
                .finish(),
            BusName::WellKnown(name) => f
                .debug_tuple("BusName::WellKnown")
                .field(&name.as_str())
                .finish(),
        }
    }
}

impl Display for BusName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.as_str(), f)
    }
}

impl PartialEq<str> for BusName<'_> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for BusName<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<OwnedBusName> for BusName<'_> {
    fn eq(&self, other: &OwnedBusName) -> bool {
        *self == other.0
    }
}

impl PartialEq<UniqueName<'_>> for BusName<'_> {
    fn eq(&self, other: &UniqueName<'_>) -> bool {
        match self {
            Self::Unique(name) => *name == *other,
            Self::WellKnown(_) => false,
        }
    }
}

impl PartialEq<WellKnownName<'_>> for BusName<'_> {
    fn eq(&self, other: &WellKnownName<'_>) -> bool {
        match self {
            Self::Unique(_) => false,
            Self::WellKnown(name) => *name == *other,
        }
    }
}

impl<'name> NoneValue for BusName<'name> {
    type NoneType = &'name str;

    fn null_value() -> Self::NoneType {
        <&str>::default()
    }
}

// Manual deserialize implementation to get the desired error on invalid bus names.
impl<'de: 'name, 'name> Deserialize<'de> for BusName<'name> {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let name = Str::deserialize(deserializer)?;

        Self::try_from(name).map_err(|e| de::Error::custom(e.to_string()))
    }
}

impl Type for BusName<'_> {
    const SIGNATURE: &'static crate::Signature = &crate::Signature::Str;
}

impl<'name> From<UniqueName<'name>> for BusName<'name> {
    fn from(name: UniqueName<'name>) -> Self {
        BusName::Unique(name)
    }
}

impl<'name> From<WellKnownName<'name>> for BusName<'name> {
    fn from(name: WellKnownName<'name>) -> Self {
        BusName::WellKnown(name)
    }
}

impl<'name> From<&UniqueName<'name>> for BusName<'name> {
    fn from(name: &UniqueName<'name>) -> Self {
        BusName::Unique(name.clone())
    }
}

impl<'name> From<&WellKnownName<'name>> for BusName<'name> {
    fn from(name: &WellKnownName<'name>) -> Self {
        BusName::WellKnown(name.clone())
    }
}

impl<'s> TryFrom<Str<'s>> for BusName<'s> {
    type Error = Error;

    fn try_from(value: Str<'s>) -> Result<Self> {
        match zbus_utils::names::validate_bus_name(value.as_bytes()).map_err(Error::InvalidName)? {
            BusNameKind::Unique => Ok(BusName::Unique(UniqueName(value))),
            BusNameKind::WellKnown => Ok(BusName::WellKnown(WellKnownName(value))),
        }
    }
}

impl<'s> TryFrom<BusName<'s>> for WellKnownName<'s> {
    type Error = Error;

    fn try_from(value: BusName<'s>) -> Result<Self> {
        match value {
            BusName::Unique(_) => Err(Error::InvalidNameConversion {
                from: "UniqueName",
                to: "WellKnownName",
            }),
            BusName::WellKnown(name) => Ok(name),
        }
    }
}

impl<'s> TryFrom<BusName<'s>> for UniqueName<'s> {
    type Error = Error;

    fn try_from(value: BusName<'s>) -> Result<Self> {
        match value {
            BusName::Unique(name) => Ok(name),
            BusName::WellKnown(_) => Err(Error::InvalidNameConversion {
                from: "WellKnownName",
                to: "UniqueName",
            }),
        }
    }
}

impl<'s> TryFrom<&'s str> for BusName<'s> {
    type Error = Error;

    fn try_from(value: &'s str) -> Result<Self> {
        Str::from(value).try_into()
    }
}

impl TryFrom<String> for BusName<'_> {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        Str::from(value).try_into()
    }
}

impl TryFrom<Arc<str>> for BusName<'_> {
    type Error = Error;

    fn try_from(value: Arc<str>) -> Result<Self> {
        Str::from(value).try_into()
    }
}

impl<'s> TryFrom<Variant<'s>> for BusName<'s> {
    type Error = Error;

    fn try_from(value: Variant<'s>) -> Result<Self> {
        Str::try_from(value).and_then(TryInto::try_into)
    }
}

/// This never succeeds but is provided so it's easier to pass `Option::None` values for API
/// requiring `Option<TryInto<impl BusName>>`, since type inference won't work here.
impl TryFrom<()> for BusName<'_> {
    type Error = Error;

    fn try_from(_value: ()) -> Result<Self> {
        unreachable!("Conversion from `()` is not meant to actually work.");
    }
}

impl<'name> TryFrom<Cow<'name, str>> for BusName<'name> {
    type Error = Error;

    fn try_from(value: Cow<'name, str>) -> Result<Self> {
        Str::from(value).try_into()
    }
}

impl<'s> From<BusName<'s>> for Variant<'s> {
    fn from(name: BusName<'s>) -> Self {
        match name {
            BusName::Unique(name) => name.into(),
            BusName::WellKnown(name) => name.into(),
        }
    }
}

impl<'name> From<BusName<'name>> for Str<'name> {
    fn from(value: BusName<'name>) -> Self {
        match value {
            BusName::Unique(name) => name.into(),
            BusName::WellKnown(name) => name.into(),
        }
    }
}

impl<'name> From<&BusName<'name>> for BusName<'name> {
    fn from(name: &BusName<'name>) -> Self {
        name.clone()
    }
}

impl TryFrom<OwnedVariant> for BusName<'_> {
    type Error = Error;

    fn try_from(value: OwnedVariant) -> Result<Self> {
        Str::try_from(value).and_then(TryInto::try_into)
    }
}

impl TryFrom<BusName<'static>> for OwnedVariant {
    type Error = Error;

    fn try_from(name: BusName<'static>) -> Result<Self> {
        match name {
            BusName::Unique(name) => name.try_into(),
            BusName::WellKnown(name) => name.try_into(),
        }
    }
}

impl From<OwnedUniqueName> for BusName<'_> {
    fn from(name: OwnedUniqueName) -> Self {
        BusName::Unique(name.into())
    }
}

impl<'a> From<&'a OwnedUniqueName> for BusName<'a> {
    fn from(name: &'a OwnedUniqueName) -> Self {
        BusName::Unique(name.into())
    }
}

impl From<OwnedWellKnownName> for BusName<'_> {
    fn from(name: OwnedWellKnownName) -> Self {
        BusName::WellKnown(name.into())
    }
}

impl<'a> From<&'a OwnedWellKnownName> for BusName<'a> {
    fn from(name: &'a OwnedWellKnownName) -> Self {
        BusName::WellKnown(name.into())
    }
}

/// Owned sibling of [`BusName`].
#[derive(Clone, Hash, PartialEq, Eq, Serialize, PartialOrd, Ord, Type)]
#[serde(transparent)]
pub struct OwnedBusName(#[serde(borrow)] BusName<'static>);

impl_str_basic!(OwnedBusName);

impl OwnedBusName {
    /// Convert to the inner `BusName`, consuming `self`.
    pub fn into_inner(self) -> BusName<'static> {
        self.0
    }

    /// Get a reference to the inner `BusName`.
    pub fn inner(&self) -> &BusName<'static> {
        &self.0
    }
}

impl Deref for OwnedBusName {
    type Target = BusName<'static>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> Borrow<BusName<'a>> for OwnedBusName {
    fn borrow(&self) -> &BusName<'a> {
        &self.0
    }
}

impl Borrow<str> for OwnedBusName {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl Debug for OwnedBusName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            BusName::Unique(name) => f
                .debug_tuple("OwnedBusName::Unique")
                .field(&name.as_str())
                .finish(),
            BusName::WellKnown(name) => f
                .debug_tuple("OwnedBusName::WellKnown")
                .field(&name.as_str())
                .finish(),
        }
    }
}

impl Display for OwnedBusName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&BusName::from(self), f)
    }
}

impl From<OwnedBusName> for BusName<'_> {
    fn from(name: OwnedBusName) -> Self {
        name.into_inner()
    }
}

impl<'unowned, 'owned: 'unowned> From<&'owned OwnedBusName> for BusName<'unowned> {
    fn from(name: &'owned OwnedBusName) -> Self {
        match &name.0 {
            BusName::Unique(name) => BusName::Unique(UniqueName::from_str_unchecked(name)),
            BusName::WellKnown(name) => BusName::WellKnown(WellKnownName::from_str_unchecked(name)),
        }
    }
}

impl From<BusName<'_>> for OwnedBusName {
    fn from(name: BusName<'_>) -> Self {
        OwnedBusName(name.into_owned())
    }
}

impl TryFrom<&'_ str> for OwnedBusName {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        BusName::try_from(value).map(Self::from)
    }
}

impl TryFrom<String> for OwnedBusName {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        BusName::try_from(value).map(Self::from)
    }
}

impl TryFrom<Cow<'_, str>> for OwnedBusName {
    type Error = Error;

    fn try_from(value: Cow<'_, str>) -> Result<Self> {
        BusName::try_from(value).map(Self::from)
    }
}

impl TryFrom<Variant<'static>> for OwnedBusName {
    type Error = Error;

    fn try_from(value: Variant<'static>) -> Result<Self> {
        BusName::try_from(value).map(Self::from)
    }
}

impl From<OwnedBusName> for Variant<'_> {
    fn from(name: OwnedBusName) -> Self {
        name.0.into()
    }
}

impl TryFrom<OwnedVariant> for OwnedBusName {
    type Error = Error;

    fn try_from(value: OwnedVariant) -> Result<Self> {
        BusName::try_from(value).map(Self::from)
    }
}

impl TryFrom<OwnedBusName> for OwnedVariant {
    type Error = Error;

    fn try_from(name: OwnedBusName) -> Result<Self> {
        name.0.try_into()
    }
}

impl From<OwnedBusName> for Str<'_> {
    fn from(value: OwnedBusName) -> Self {
        match value.0 {
            BusName::Unique(name) => name.into(),
            BusName::WellKnown(name) => name.into(),
        }
    }
}

impl<'de> Deserialize<'de> for OwnedBusName {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        BusName::deserialize(deserializer).map(Self::from)
    }
}

impl PartialEq<&str> for OwnedBusName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<BusName<'_>> for OwnedBusName {
    fn eq(&self, other: &BusName<'_>) -> bool {
        self.0 == *other
    }
}

impl NoneValue for OwnedBusName {
    type NoneType = String;

    fn null_value() -> Self::NoneType {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_borrows_from_message() {
        use crate::wire::{LE, serialized::Context, to_bytes};

        let ctxt = Context::new(LE, 0);
        for s in [":1.23", "org.freedesktop.DBus"] {
            let encoded = to_bytes(ctxt, &s).unwrap();
            let name: BusName<'_> = encoded.deserialize().unwrap().0;
            assert_eq!(name, s);
            assert!(encoded.bytes().as_ptr_range().contains(&name.as_ptr()));
        }

        let encoded = to_bytes(ctxt, &"not a bus name").unwrap();
        encoded.deserialize::<BusName<'_>>().unwrap_err();
        encoded.deserialize::<OwnedBusName>().unwrap_err();
    }

    #[test]
    fn deserialize_takes_owned_string() {
        use serde_json::Value;

        for s in [":1.23", "org.freedesktop.DBus"] {
            assert_eq!(BusName::deserialize(Value::from(s)).unwrap(), s);
            assert_eq!(OwnedBusName::deserialize(Value::from(s)).unwrap(), s);
            // A reader can't lend the string (#287).
            let json = format!("{s:?}");
            let name: OwnedBusName = serde_json::from_reader(json.as_bytes()).unwrap();
            assert_eq!(name, s);
        }

        BusName::deserialize(Value::from("not a bus name")).unwrap_err();
        OwnedBusName::deserialize(Value::from("not a bus name")).unwrap_err();
    }

    #[test]
    fn ron_round_trip() {
        // RON tells a plain string from a newtype struct.
        for s in [":1.23", "org.freedesktop.DBus"] {
            let name = BusName::try_from(s).unwrap();
            let ron = ron::to_string(&name).unwrap();
            assert_eq!(ron, format!("{s:?}"));
            assert_eq!(ron::from_str::<BusName<'_>>(&ron).unwrap(), name);

            let owned = OwnedBusName::from(name);
            assert_eq!(ron::to_string(&owned).unwrap(), ron);
            assert_eq!(ron::from_str::<OwnedBusName>(&ron).unwrap(), owned);
        }
    }
}
