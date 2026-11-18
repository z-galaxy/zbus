use crate::{Str, Type, names::utils::define_name_type_impls};
use serde::Serialize;

/// String that identifies an [member (method or signal) name][in] on the bus.
///
/// # Examples
///
/// ```
/// use zbus::names::MemberName;
///
/// // Valid member names.
/// let name = MemberName::try_from("Member_for_you").unwrap();
/// assert_eq!(name, "Member_for_you");
/// let name = MemberName::try_from("CamelCase101").unwrap();
/// assert_eq!(name, "CamelCase101");
/// let name = MemberName::try_from("a_very_loooooooooooooooooo_ooooooo_0000o0ngName").unwrap();
/// assert_eq!(name, "a_very_loooooooooooooooooo_ooooooo_0000o0ngName");
///
/// // Invalid member names
/// MemberName::try_from("").unwrap_err();
/// MemberName::try_from(".").unwrap_err();
/// MemberName::try_from("1startWith_a_Digit").unwrap_err();
/// MemberName::try_from("contains.dots_in_the_name").unwrap_err();
/// MemberName::try_from("contains-dashes-in_the_name").unwrap_err();
/// ```
///
/// [in]: https://dbus.freedesktop.org/doc/dbus-specification.html#message-protocol-names-member
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Type, PartialOrd, Ord)]
#[serde(transparent)]
pub struct MemberName<'name>(Str<'name>);

/// Owned sibling of [`MemberName`].
#[derive(Clone, Hash, PartialEq, Eq, Serialize, Type, PartialOrd, Ord)]
#[serde(transparent)]
pub struct OwnedMemberName(#[serde(borrow)] MemberName<'static>);

define_name_type_impls! {
    name: MemberName,
    owned: OwnedMemberName,
    validate: validate_member_name,
}
