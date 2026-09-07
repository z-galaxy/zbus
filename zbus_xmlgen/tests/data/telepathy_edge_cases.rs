#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zbus::Type,
    zbus::Value,
    zbus::OwnedValue,
)]
#[zvariant(crate = "zbus::wire")]
pub struct PlayList {
    pub name: String,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde_repr::Deserialize_repr,
    serde_repr::Serialize_repr,
    zbus::Type,
    zbus::Value,
    zbus::OwnedValue,
)]
#[zvariant(crate = "zbus::wire")]
#[repr(i32)]
pub enum OddValues {
    Plus = 7,
    Minus = -1,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zbus::Type,
    zbus::Value,
    zbus::OwnedValue,
)]
#[zvariant(signature = "s", crate = "zbus::wire")]
pub enum Status {
    Ok,
}

pub type StatusName = String;

pub type StatusMap = std::collections::HashMap<String, Status>;

pub type NamedKeyMap = std::collections::HashMap<StatusName, String>;

#[proxy(interface = "com.example.EdgeCases", assume_defaults = true)]
pub trait EdgeCases {
    /// Probe method
    ///
    /// A docstring with a carriage return in the middle.
    fn probe(
        &self,
        play_list: &PlayList,
        dropped_twin: &(u32,),
        dropped_enum: u32,
    ) -> zbus::Result<StatusMap>;

    /// OddValues property
    #[zbus(property)]
    fn odd_values(&self) -> zbus::Result<OddValues>;
    #[zbus(property)]
    fn set_odd_values(&self, value: OddValues) -> zbus::Result<()>;

    /// NamedKeys property
    #[zbus(property)]
    fn named_keys(&self) -> zbus::Result<NamedKeyMap>;

    /// Changed signal
    #[zbus(signal)]
    fn changed(&self, status: Status) -> zbus::Result<()>;
}
