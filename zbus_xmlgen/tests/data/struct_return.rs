#[proxy(interface = "test.StructReturn", assume_defaults = true)]
pub trait StructReturn {
    /// ReturnsBareStruct method
    fn returns_bare_struct(&self) -> zbus::Result<((u64, u64),)>;

    /// ReturnsStruct method
    fn returns_struct(&self) -> zbus::Result<((String, String),)>;

    /// ReturnsTwoStrings method
    fn returns_two_strings(&self) -> zbus::Result<(String, String)>;

    /// ReturnsOneString method
    fn returns_one_string(&self) -> zbus::Result<String>;

    /// ReturnsNestedStruct method
    fn returns_nested_struct(&self) -> zbus::Result<(((String, String), i32),)>;
}
