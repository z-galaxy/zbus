#[proxy(interface = "com.example.PropertySetters", assume_defaults = true)]
pub trait PropertySetters {
    /// Variant property
    #[zbus(property)]
    fn variant(&self) -> zbus::Result<zbus::OwnedValue>;
    #[zbus(property)]
    fn set_variant(&self, value: zbus::Value<'_>) -> zbus::Result<()>;

    /// StructPair property
    #[zbus(property)]
    fn struct_pair(&self) -> zbus::Result<(i32, i32)>;
    #[zbus(property)]
    fn set_struct_pair(&self, value: (i32, i32)) -> zbus::Result<()>;

    /// StructSingle property
    #[zbus(property)]
    fn struct_single(&self) -> zbus::Result<(i32,)>;
    #[zbus(property)]
    fn set_struct_single(&self, value: (i32,)) -> zbus::Result<()>;

    /// ArrayOfVariant property
    #[zbus(property)]
    fn array_of_variant(&self) -> zbus::Result<Vec<zbus::OwnedValue>>;
    #[zbus(property)]
    fn set_array_of_variant(&self, value: &[zbus::Value<'_>]) -> zbus::Result<()>;

    /// ArrayOfStruct property
    #[zbus(property)]
    fn array_of_struct(&self) -> zbus::Result<Vec<(i32, i32)>>;
    #[zbus(property)]
    fn set_array_of_struct(&self, value: &[(i32, i32)]) -> zbus::Result<()>;

    /// DictStrToVariant property
    #[zbus(property)]
    fn dict_str_to_variant(
        &self,
    ) -> zbus::Result<std::collections::HashMap<String, zbus::OwnedValue>>;
    #[zbus(property)]
    fn set_dict_str_to_variant(
        &self,
        value: std::collections::HashMap<&str, zbus::Value<'_>>,
    ) -> zbus::Result<()>;

    /// StructWithArrayOfVariant property
    #[zbus(property)]
    fn struct_with_array_of_variant(&self) -> zbus::Result<(Vec<zbus::OwnedValue>,)>;
    #[zbus(property)]
    fn set_struct_with_array_of_variant(&self, value: (&[zbus::Value<'_>],)) -> zbus::Result<()>;
}
