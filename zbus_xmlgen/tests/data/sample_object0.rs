#[proxy(interface = "com.example.SampleInterface0", assume_defaults = true)]
pub trait SampleInterface0 {
    /// BarplexSig method
    fn barplex_sig(
        &self,
        rule: &(
            &[i32],
            i32,
            std::collections::HashMap<&str, &str>,
            i32,
            &[i32],
            i32,
            &[&str],
            i32,
            bool,
        ),
    ) -> zbus::Result<Vec<(String, zbus::zvariant::OwnedObjectPath)>>;

    /// Bazic method
    fn bazic(&self, bar: &(i32, i32), foo: &(i32,)) -> zbus::Result<((i32, i32), Vec<(i32,)>)>;

    /// Bazify method
    fn bazify(&self, bar: &(i32, i32, u32)) -> zbus::Result<zbus::zvariant::OwnedValue>;

    /// Frobate method
    fn frobate(
        &self,
        foz: i32,
        foo: i32,
    ) -> zbus::Result<(String, std::collections::HashMap<u32, String>)>;

    /// MogrifyMe method
    fn mogrify_me(&self, bar: &(i32, i32, &[&zbus::zvariant::Value<'_>])) -> zbus::Result<()>;

    /// Odyssey method
    #[allow(clippy::too_many_arguments)]
    fn odyssey(
        &self,
        odysseus: i32,
        penelope: &str,
        telemachus: u32,
        circe: i32,
        athena: bool,
        polyphemus: i32,
        calypso: &zbus::zvariant::Value<'_>,
    ) -> zbus::Result<()>;

    /// Changed signal
    #[zbus(signal)]
    fn changed(&self, new_value: bool) -> zbus::Result<()>;

    /// Changed2 signal
    #[zbus(signal)]
    fn changed2(&self, new_value: bool, new_value2: bool) -> zbus::Result<()>;

    /// SignalArrayOfStrings signal
    #[zbus(signal)]
    fn signal_array_of_strings(&self, array: Vec<&str>) -> zbus::Result<()>;

    /// SignalDictStringToValue signal
    #[zbus(signal)]
    fn signal_dict_string_to_value(
        &self,
        dict: std::collections::HashMap<&str, zbus::zvariant::Value<'_>>,
    ) -> zbus::Result<()>;

    /// SignalValue signal
    #[zbus(signal)]
    fn signal_value(&self, value: zbus::zvariant::Value<'_>) -> zbus::Result<()>;

    /// StateChanged signal
    #[zbus(signal)]
    fn state_changed(&self, state: u32, something_else: u32) -> zbus::Result<()>;

    /// Bar property
    #[zbus(property)]
    fn bar(&self) -> zbus::Result<u8>;
    #[zbus(property)]
    fn set_bar(&self, value: u8) -> zbus::Result<()>;

    /// Foo-Bar property
    #[zbus(property, name = "Foo-Bar")]
    fn foo_bar(&self) -> zbus::Result<u8>;
    #[zbus(property, name = "Foo-Bar")]
    fn set_foo_bar(&self, value: u8) -> zbus::Result<()>;

    /// Matryoshkas property
    #[zbus(property)]
    #[allow(clippy::type_complexity)]
    fn matryoshkas(
        &self,
    ) -> zbus::Result<
        Vec<(
            zbus::zvariant::OwnedObjectPath,
            i32,
            Vec<String>,
            u64,
            std::collections::HashMap<String, zbus::zvariant::OwnedValue>,
        )>,
    >;

    /// State property
    // Note: change signal is shadowed by `StateChanged` signal.
    #[zbus(property(emits_changed_signal = "false"))]
    fn state(&self) -> zbus::Result<u32>;
}
