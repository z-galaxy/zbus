#[proxy(interface = "com.example.SampleInterface0", assume_defaults = true)]
pub trait SampleInterface0 {
    /// Frobate method
    fn frobate(
        &self,
        foz: i32,
        foo: i32,
    ) -> zbus::Result<(String, std::collections::HashMap<u32, String>)>;

    /// Bazic method
    fn bazic(&self, bar: &(i32, i32), foo: &(i32,)) -> zbus::Result<((i32, i32), Vec<(i32,)>)>;

    /// Bazify method
    fn bazify(&self, bar: &(i32, i32, u32)) -> zbus::Result<zbus::OwnedVariant>;

    /// MogrifyMe method
    fn mogrify_me(&self, bar: &(i32, i32, &[&zbus::Variant<'_>])) -> zbus::Result<()>;

    /// GetSignature method
    fn get_signature(&self, challenge: &zbus::Signature) -> zbus::Result<zbus::Signature>;

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
    ) -> zbus::Result<Vec<(String, zbus::OwnedObjectPath)>>;

    /// ComplexType method
    #[allow(clippy::type_complexity)]
    fn complex_type(
        &self,
        not_complex: &[&(
            &(&str, i32, &str, i32, &str),
            &[&(
                &str,
                i32,
                i32,
                f64,
                f64,
                &[f64],
                std::collections::HashMap<&str, &zbus::Variant<'_>>,
            )],
        )],
    ) -> zbus::Result<(
        Vec<(
            (String, String, String, String),
            Vec<(
                String,
                i32,
                i32,
                f64,
                f64,
                Vec<f64>,
                std::collections::HashMap<String, zbus::OwnedVariant>,
            )>,
            std::collections::HashMap<String, zbus::OwnedVariant>,
        )>,
        Vec<(
            i32,
            i32,
            f64,
            u32,
            bool,
            Vec<(String, String, String, String)>,
            std::collections::HashMap<String, zbus::OwnedVariant>,
        )>,
    )>;

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
        calypso: &zbus::Variant<'_>,
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

    /// SignalValue signal
    #[zbus(signal)]
    fn signal_value(&self, value: zbus::Variant<'_>) -> zbus::Result<()>;

    /// SignalDictStringToValue signal
    #[zbus(signal)]
    fn signal_dict_string_to_value(
        &self,
        dict: std::collections::HashMap<&str, zbus::Variant<'_>>,
    ) -> zbus::Result<()>;

    /// StateChanged signal
    #[zbus(signal)]
    fn state_changed(&self, state: u32, something_else: u32) -> zbus::Result<()>;

    /// State property
    // Note: changed signal is shadowed by `state_changed` signal method.
    #[zbus(property(emits_changed_signal = "false"))]
    fn state(&self) -> zbus::Result<u32>;

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
            zbus::OwnedObjectPath,
            i32,
            Vec<String>,
            u64,
            std::collections::HashMap<String, zbus::OwnedVariant>,
        )>,
    >;
}
