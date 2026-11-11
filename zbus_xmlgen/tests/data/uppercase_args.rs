#[proxy(interface = "org.example.Device", assume_defaults = true)]
pub trait Device {
    /// ConnectProfile method
    fn connect_profile(&self, uuid: &str) -> zbus::Result<()>;

    /// SetAddress method
    fn set_address(&self, ipaddress: &str, prefix_length: u32) -> zbus::Result<String>;

    /// Discovered signal
    #[zbus(signal)]
    fn discovered(&self, uuid: &str, rssi: i16) -> zbus::Result<()>;
}
