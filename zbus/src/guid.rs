use std::{
    borrow::{Borrow, Cow},
    fmt::{self, Debug, Display, Formatter},
    ops::Deref,
    str::FromStr,
};

use crate::{Str, Type};
use serde::{Deserialize, Serialize, de};

/// A D-Bus server GUID.
///
/// See the D-Bus specification [UUIDs chapter] for details.
///
/// You can create a `Guid` from an existing string with [`Guid::try_from::<&str>`].
///
/// [UUIDs chapter]: https://dbus.freedesktop.org/doc/dbus-specification.html#uuids
#[derive(Clone, Debug, PartialEq, Eq, Hash, Type, Serialize)]
#[serde(transparent)]
pub struct Guid<'g>(Str<'g>);

impl Guid<'_> {
    /// Generate a D-Bus GUID that can be used with e.g. `connection::Builder::server` (which
    /// requires the `p2p` feature), or by a server that does its own I/O.
    ///
    /// This method is only available when the `core-p2p` or the `p2p` feature is enabled (both
    /// disabled by default).
    #[cfg(feature = "core-p2p")]
    pub fn generate() -> Guid<'static> {
        let s = uuid::Uuid::new_v4().as_simple().to_string();
        Guid(s.into())
    }

    /// Return a string slice for the GUID.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Same as `try_from`, except it takes a `&'static str`.
    pub fn from_static_str(guid: &'static str) -> crate::Result<Self> {
        validate_guid(guid)?;

        Ok(Self(Str::from_static(guid)))
    }

    /// Create an owned copy of the GUID.
    pub fn to_owned(&self) -> Guid<'static> {
        Guid(self.0.to_owned())
    }
}

impl fmt::Display for Guid<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl<'g> TryFrom<&'g str> for Guid<'g> {
    type Error = crate::Error;

    /// Create a GUID from a string with 32 hex digits.
    ///
    /// Returns `Err(`[`Error::InvalidGUID`]`)` if the provided string is not a well-formed GUID.
    ///
    /// [`Error::InvalidGUID`]: enum.Error.html#variant.InvalidGUID
    fn try_from(value: &'g str) -> std::result::Result<Self, Self::Error> {
        validate_guid(value)?;

        Ok(Self(Str::from(value)))
    }
}

impl<'g> TryFrom<Str<'g>> for Guid<'g> {
    type Error = crate::Error;

    /// Create a GUID from a string with 32 hex digits.
    ///
    /// Returns `Err(`[`Error::InvalidGUID`]`)` if the provided string is not a well-formed GUID.
    ///
    /// [`Error::InvalidGUID`]: enum.Error.html#variant.InvalidGUID
    fn try_from(value: Str<'g>) -> std::result::Result<Self, Self::Error> {
        validate_guid(&value)?;

        Ok(Guid(value))
    }
}

impl TryFrom<String> for Guid<'_> {
    type Error = crate::Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        validate_guid(&value)?;

        Ok(Guid(value.into()))
    }
}

impl<'g> TryFrom<Cow<'g, str>> for Guid<'g> {
    type Error = crate::Error;

    fn try_from(value: Cow<'g, str>) -> std::result::Result<Self, Self::Error> {
        validate_guid(&value)?;

        Ok(Guid(value.into()))
    }
}

impl FromStr for Guid<'static> {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.try_into().map(|guid: Guid<'_>| guid.to_owned())
    }
}

impl<'de> Deserialize<'de> for Guid<'de> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Str::deserialize(deserializer).and_then(|s| s.try_into().map_err(serde::de::Error::custom))
    }
}

const fn validate_guid(value: &str) -> crate::Result<()> {
    match uuid::Uuid::try_parse(value) {
        Ok(_) => Ok(()),
        Err(_) => Err(crate::Error::InvalidGUID),
    }
}

impl From<Guid<'_>> for String {
    fn from(guid: Guid<'_>) -> Self {
        guid.0.into()
    }
}

impl Deref for Guid<'_> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl<'a> Borrow<Guid<'a>> for OwnedGuid {
    fn borrow(&self) -> &Guid<'a> {
        &self.0
    }
}

impl AsRef<str> for Guid<'_> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for Guid<'_> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

/// Owned version of [`Guid`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, Type, Serialize)]
#[serde(transparent)]
pub struct OwnedGuid(#[serde(borrow)] Guid<'static>);

impl OwnedGuid {
    /// Get a reference to the inner [`Guid`].
    pub fn inner(&self) -> &Guid<'static> {
        &self.0
    }
}

impl Deref for OwnedGuid {
    type Target = Guid<'static>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for OwnedGuid {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl From<OwnedGuid> for Guid<'_> {
    fn from(o: OwnedGuid) -> Self {
        o.0
    }
}

impl<'unowned, 'owned: 'unowned> From<&'owned OwnedGuid> for Guid<'unowned> {
    fn from(guid: &'owned OwnedGuid) -> Self {
        guid.0.clone()
    }
}

impl<'g> From<&Guid<'g>> for Guid<'g> {
    fn from(guid: &Guid<'g>) -> Self {
        guid.clone()
    }
}

impl From<Guid<'_>> for OwnedGuid {
    fn from(guid: Guid<'_>) -> Self {
        OwnedGuid(guid.to_owned())
    }
}

impl From<OwnedGuid> for Str<'_> {
    fn from(value: OwnedGuid) -> Self {
        value.0.0
    }
}

impl<'de> Deserialize<'de> for OwnedGuid {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        Guid::deserialize(deserializer).map(Self::from)
    }
}

impl PartialEq<&str> for OwnedGuid {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<Guid<'_>> for OwnedGuid {
    fn eq(&self, other: &Guid<'_>) -> bool {
        self.0 == *other
    }
}

impl Display for OwnedGuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&Guid::from(self), f)
    }
}

#[cfg(test)]
mod tests {
    use crate::Guid;
    use test_log::test;

    #[test]
    #[cfg(feature = "core-p2p")]
    fn generate() {
        let u1 = Guid::generate();
        let u2 = Guid::generate();
        assert_eq!(u1.as_str().len(), 32);
        assert_eq!(u2.as_str().len(), 32);
        assert_ne!(u1, u2);
        assert_ne!(u1.as_str(), u2.as_str());
    }

    #[test]
    fn parse() {
        let valid = "0123456789ABCDEF0123456789ABCDEF";
        // Not 32 chars.
        let invalid = "0123456789ABCDEF0123456789ABCD";

        assert!(Guid::try_from(valid).is_ok());
        assert!(Guid::try_from(invalid).is_err());
    }

    #[test]
    fn deserialize_borrows_from_message() {
        use crate::{
            OwnedGuid,
            wire::{LE, serialized::Context, to_bytes},
        };

        let ctxt = Context::new(LE, 0);
        let valid = "0123456789ABCDEF0123456789ABCDEF";
        let encoded = to_bytes(ctxt, &valid).unwrap();
        let guid: Guid<'_> = encoded.deserialize().unwrap().0;
        assert_eq!(guid.as_str(), valid);
        assert!(encoded.bytes().as_ptr_range().contains(&guid.as_ptr()));

        let encoded = to_bytes(ctxt, &"not a GUID").unwrap();
        encoded.deserialize::<Guid<'_>>().unwrap_err();
        encoded.deserialize::<OwnedGuid>().unwrap_err();
    }

    #[test]
    fn deserialize_takes_owned_string() {
        use crate::OwnedGuid;
        use serde::Deserialize;
        use serde_json::Value;

        let valid = "0123456789ABCDEF0123456789ABCDEF";
        assert_eq!(
            Guid::deserialize(Value::from(valid)).unwrap().as_str(),
            valid
        );
        assert_eq!(OwnedGuid::deserialize(Value::from(valid)).unwrap(), valid);
        // A reader can't lend the string (#287).
        let json = format!("{valid:?}");
        let guid: OwnedGuid = serde_json::from_reader(json.as_bytes()).unwrap();
        assert_eq!(guid, valid);

        Guid::deserialize(Value::from("not a GUID")).unwrap_err();
        OwnedGuid::deserialize(Value::from("not a GUID")).unwrap_err();
    }

    #[test]
    fn ron_round_trip() {
        use crate::OwnedGuid;

        // RON tells a plain string from a newtype struct.
        let valid = "0123456789ABCDEF0123456789ABCDEF";
        let guid = Guid::try_from(valid).unwrap();
        let ron = ron::to_string(&guid).unwrap();
        assert_eq!(ron, format!("{valid:?}"));
        assert_eq!(ron::from_str::<Guid<'_>>(&ron).unwrap(), guid);

        let owned = OwnedGuid::from(guid);
        assert_eq!(ron::to_string(&owned).unwrap(), ron);
        assert_eq!(ron::from_str::<OwnedGuid>(&ron).unwrap(), owned);
    }
}
