use pretty_assertions::assert_eq;
use std::{env, error::Error, io::Write, path::Path};

use zbus_xml::Node;
use zbus_xmlgen::CodeGenerator;

macro_rules! gen_diff {
    ($infile:literal, $outfile:literal) => {{
        let input = include_str!(concat!("data/", $infile));
        let expected = include_str!(concat!("data/", $outfile));
        #[cfg(windows)]
        let expected = expected.replace("\r\n", "\n");
        let node = Node::from_reader(input.as_bytes())?;
        let r#gen = CodeGenerator::new()
            .with_node_types(node.telepathy_types())
            .with_format(true)
            .interface_code(&node.interfaces()[0])?;

        if env::var("TEST_OVERWRITE").is_ok() {
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join("data")
                .join($outfile);
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(path)?;
            f.write_all(r#gen.as_bytes())?;
            f.flush()?;
            return Ok(());
        }

        assert_eq!(r#gen, expected);
        Ok(())
    }};
}

#[test]
fn sample_object0() -> Result<(), Box<dyn Error>> {
    gen_diff!("sample_object0.xml", "sample_object0.rs")
}

#[test]
fn struct_return() -> Result<(), Box<dyn Error>> {
    gen_diff!("struct_return.xml", "struct_return.rs")
}

#[test]
fn property_setters() -> Result<(), Box<dyn Error>> {
    gen_diff!("property_setters.xml", "property_setters.rs")
}

#[test]
fn telepathy_docstrings() -> Result<(), Box<dyn Error>> {
    gen_diff!("telepathy_docstrings.xml", "telepathy_docstrings.rs")
}

#[test]
fn telepathy_edge_cases() -> Result<(), Box<dyn Error>> {
    gen_diff!("telepathy_edge_cases.xml", "telepathy_edge_cases.rs")
}

#[test]
fn shared_node_types_in_one_file() -> Result<(), Box<dyn Error>> {
    // Two interfaces referencing the same node-level definition in a single file: the Rust
    // definition must be generated only once.
    let input = r#"
        <node xmlns:tp="http://telepathy.freedesktop.org/wiki/DbusSpec#extensions-v0">
            <tp:struct name="Shared_Info">
                <tp:member type="s" name="Info"/>
            </tp:struct>
            <interface name="org.test.iface1">
                <method name="GetInfo">
                    <arg direction="out" name="info" type="(s)" tp:type="Shared_Info"/>
                </method>
            </interface>
            <interface name="org.test.iface2">
                <method name="SetInfo">
                    <arg direction="in" name="info" type="(s)" tp:type="Shared_Info"/>
                </method>
            </interface>
        </node>
    "#;

    let node = Node::from_reader(input.as_bytes())?;
    let code = CodeGenerator::new()
        .with_node_types(node.telepathy_types())
        .file_code(node.interfaces(), &[], "test", "test", "test")?;

    assert_eq!(code.matches("pub struct SharedInfo").count(), 1);
    // Both interfaces still use the shared type.
    assert!(code.contains("zbus::Result<(SharedInfo,)>"));
    assert!(code.contains("info: &SharedInfo"));

    Ok(())
}

#[test]
fn uppercase_args() -> Result<(), Box<dyn Error>> {
    gen_diff!("uppercase_args.xml", "uppercase_args.rs")
}
