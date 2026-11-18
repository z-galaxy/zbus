use crate::{Str, Type, names::utils::define_name_type_impls};
use serde::Serialize;

/// String that identifies a [unique bus name][ubn].
///
/// # Examples
///
/// ```
/// use zbus::names::UniqueName;
///
/// // Valid unique names.
/// let name = UniqueName::try_from(":org.gnome.Service-for_you").unwrap();
/// assert_eq!(name, ":org.gnome.Service-for_you");
/// let name = UniqueName::try_from(":a.very.loooooooooooooooooo-ooooooo_0000o0ng.Name").unwrap();
/// assert_eq!(name, ":a.very.loooooooooooooooooo-ooooooo_0000o0ng.Name");
///
/// // Invalid unique names
/// UniqueName::try_from("").unwrap_err();
/// UniqueName::try_from("dont.start.with.a.colon").unwrap_err();
/// UniqueName::try_from(":double..dots").unwrap_err();
/// UniqueName::try_from(".").unwrap_err();
/// UniqueName::try_from(".start.with.dot").unwrap_err();
/// UniqueName::try_from(":no-dots").unwrap_err();
/// ```
///
/// [ubn]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-names-bus
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Type, PartialOrd, Ord)]
pub struct UniqueName<'name>(pub(crate) Str<'name>);

/// Owned sibling of [`UniqueName`].
#[derive(Clone, Hash, PartialEq, Eq, Serialize, Type, PartialOrd, Ord)]
pub struct OwnedUniqueName(#[serde(borrow)] UniqueName<'static>);

define_name_type_impls! {
    name: UniqueName,
    owned: OwnedUniqueName,
    validate: validate_unique_name,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OwnedVariant, Variant};

    #[test]
    fn value_conversion_rejects_empty_name() {
        let value = Variant::from("");
        UniqueName::try_from(value).unwrap_err();
        OwnedUniqueName::try_from(Variant::from("")).unwrap_err();
        OwnedUniqueName::try_from(OwnedVariant::from(crate::Str::from(""))).unwrap_err();
    }

    #[test]
    fn optional_value_conversion_maps_empty_name_to_none() {
        use crate::Optional;

        // An empty string is the D-Bus sentinel for "no name" and must map to `None`
        // rather than fail validation.
        let opt = Optional::<UniqueName<'_>>::try_from(Variant::from("")).unwrap();
        assert_eq!(Option::from(opt), None::<UniqueName<'_>>);

        let opt = Optional::<UniqueName<'_>>::try_from(Variant::from(":1.23")).unwrap();
        assert_eq!(opt.as_ref().unwrap().as_str(), ":1.23");

        // Non-empty invalid names must still be rejected.
        Optional::<UniqueName<'_>>::try_from(Variant::from("not a unique name")).unwrap_err();
    }

    #[test]
    fn optional_owned_value_conversion_maps_empty_name_to_none() {
        use crate::Optional;

        // The sentinel must be recognized before the empty name is validated.
        let owned = OwnedVariant::from(crate::Str::from(""));
        let opt = Optional::<OwnedUniqueName>::try_from(owned).unwrap();
        assert!(Option::<OwnedUniqueName>::from(opt).is_none());

        let owned = OwnedVariant::from(crate::Str::from(":1.23"));
        let opt = Optional::<OwnedUniqueName>::try_from(owned).unwrap();
        assert_eq!(
            Option::<OwnedUniqueName>::from(opt).unwrap().as_str(),
            ":1.23"
        );
    }

    #[test]
    fn optional_name_wire_round_trip() {
        use crate::{
            Optional,
            wire::{LE, serialized::Context, to_bytes},
        };

        let ctxt = Context::new(LE, 0);

        // `NameOwnerChanged`-style: empty string on the wire means "no name".
        let encoded = to_bytes(ctxt, &Optional::<UniqueName<'_>>::default()).unwrap();
        let opt: Optional<UniqueName<'_>> = encoded.deserialize().unwrap().0;
        assert!(Option::<UniqueName<'_>>::from(opt).is_none());

        let name = UniqueName::try_from(":1.23").unwrap();
        let encoded = to_bytes(ctxt, &Optional::from(Some(name.clone()))).unwrap();
        let opt: Optional<UniqueName<'_>> = encoded.deserialize().unwrap().0;
        assert_eq!(Option::from(opt), Some(name));

        // Invalid non-empty names on the wire must still be rejected.
        let encoded = to_bytes(ctxt, &"not a unique name").unwrap();
        encoded
            .deserialize::<Optional<UniqueName<'_>>>()
            .unwrap_err();
    }

    #[test]
    fn optional_owned_name_is_deserialize_owned() {
        use crate::{
            Optional,
            names::OwnedBusName,
            wire::{LE, serialized::Context, to_bytes},
        };
        use serde::de::DeserializeOwned;

        fn assert_deserialize_owned<T: DeserializeOwned>() {}

        assert_deserialize_owned::<Optional<OwnedUniqueName>>();
        assert_deserialize_owned::<Optional<OwnedBusName>>();

        let ctxt = Context::new(LE, 0);
        let encoded = to_bytes(ctxt, &Optional::<OwnedUniqueName>::default()).unwrap();
        let opt: Optional<OwnedUniqueName> = encoded.deserialize().unwrap().0;
        assert!(Option::<OwnedUniqueName>::from(opt).is_none());

        let name = OwnedUniqueName::try_from(":1.42").unwrap();
        let encoded = to_bytes(ctxt, &Optional::from(Some(name.clone()))).unwrap();
        let opt: Optional<OwnedUniqueName> = encoded.deserialize().unwrap().0;
        assert_eq!(Option::<OwnedUniqueName>::from(opt), Some(name));
    }

    #[test]
    fn deserialize_borrows_from_message() {
        use crate::wire::{LE, serialized::Context, to_bytes};

        let ctxt = Context::new(LE, 0);
        let encoded = to_bytes(ctxt, &":1.23").unwrap();
        let name: UniqueName<'_> = encoded.deserialize().unwrap().0;
        assert_eq!(name, ":1.23");
        assert!(encoded.bytes().as_ptr_range().contains(&name.as_ptr()));

        let encoded = to_bytes(ctxt, &"not a unique name").unwrap();
        encoded.deserialize::<UniqueName<'_>>().unwrap_err();
        encoded.deserialize::<OwnedUniqueName>().unwrap_err();
    }

    #[test]
    fn deserialize_takes_owned_string() {
        use serde::Deserialize;
        use serde_json::Value;

        let name = UniqueName::deserialize(Value::from(":1.23")).unwrap();
        assert_eq!(name, ":1.23");
        let name = OwnedUniqueName::deserialize(Value::from(":1.23")).unwrap();
        assert_eq!(name, ":1.23");
        // A reader can't lend the string (#287).
        let name: OwnedUniqueName = serde_json::from_reader(br#"":1.23""#.as_slice()).unwrap();
        assert_eq!(name, ":1.23");

        UniqueName::deserialize(Value::from("not a unique name")).unwrap_err();
        OwnedUniqueName::deserialize(Value::from("not a unique name")).unwrap_err();
    }

    #[test]
    fn deserialize_plain_string_in_ron() {
        // RON tells a plain string from a newtype struct.
        let name: UniqueName<'_> = ron::from_str(r#"":1.23""#).unwrap();
        assert_eq!(name, ":1.23");
        ron::from_str::<UniqueName<'_>>(r#""not a unique name""#).unwrap_err();
    }

    #[test]
    fn value_conversion_round_trips_valid_name() {
        let name = UniqueName::try_from(":1.23").unwrap();
        let value = Variant::from(name.clone());
        let parsed = UniqueName::try_from(value).unwrap();
        assert_eq!(parsed, name);

        let owned = OwnedVariant::try_from(name.clone()).unwrap();
        assert_eq!(UniqueName::try_from(owned).unwrap(), name);

        let owned_name = OwnedUniqueName::from(name.clone());
        let value: Variant<'static> = Variant::from(owned_name.clone());
        assert_eq!(OwnedUniqueName::try_from(value).unwrap(), owned_name);

        let owned = OwnedVariant::try_from(owned_name.clone()).unwrap();
        assert_eq!(OwnedUniqueName::try_from(owned).unwrap(), owned_name);
    }
}
