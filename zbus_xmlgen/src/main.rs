#![deny(rust_2018_idioms)]

use std::{
    collections::HashSet,
    error::Error,
    fs::{File, OpenOptions},
    io::Write,
};

use clap::Parser;
use snakecase::ascii::to_snakecase;
use zbus::{Connection, ObjectPath, connection, fdo::IntrospectableProxy, names::BusName};
use zbus_xml::{Interface, Node, Warning};

use zbus_xmlgen::CodeGenerator;

mod cli;

enum OutputTarget {
    SingleFile(File),
    Stdout,
    MultipleFiles,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = cli::Args::parse();

    let DBusInfo(node, service, path, input_src) = match args.command {
        cli::Command::System {
            service,
            object_path,
        } => DBusInfo::new(zbus::block_on(Connection::system())?, service, object_path)?,
        cli::Command::Session {
            service,
            object_path,
        } => DBusInfo::new(zbus::block_on(Connection::session())?, service, object_path)?,
        cli::Command::Address {
            address,
            service,
            object_path,
        } => DBusInfo::new(
            zbus::block_on(connection::Builder::address(&*address).build())?,
            service,
            object_path,
        )?,
        cli::Command::File { path } => {
            let input_src = path.file_name().unwrap().to_string_lossy().to_string();
            let f = File::open(path)?;
            let (node, warnings) = Node::from_reader_with_warnings(f)?;
            report_warnings(&warnings);
            DBusInfo(node, None, None, input_src)
        }
    };

    let fdo_iface_prefix = "org.freedesktop.DBus";
    let (fdo_standard_ifaces, needed_ifaces): (Vec<Interface<'_>>, Vec<Interface<'_>>) = node
        .interfaces()
        .iter()
        .cloned()
        .partition(|i| i.name().starts_with(fdo_iface_prefix));

    if !fdo_standard_ifaces.is_empty() {
        eprintln!(
            "Skipping `org.freedesktop.DBus` interfaces, please use https://docs.rs/zbus/latest/zbus/fdo/index.html"
        )
    }

    let output_target = match args.output.as_deref() {
        Some("-") => OutputTarget::Stdout,
        Some(path) => {
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?;
            OutputTarget::SingleFile(file)
        }
        _ => OutputTarget::MultipleFiles,
    };

    let generator = CodeGenerator::new()
        .with_node_types(node.telepathy_types())
        .with_service(service.as_ref())
        .with_path(path.as_ref())
        .with_format(true);
    let file_code = |interfaces: &[Interface<'_>]| {
        generator.file_code(
            interfaces,
            &fdo_standard_ifaces,
            &input_src,
            env!("CARGO_BIN_NAME"),
            env!("CARGO_PKG_VERSION"),
        )
    };

    match output_target {
        OutputTarget::MultipleFiles => {
            for interface in &needed_ifaces {
                let output = file_code(std::slice::from_ref(interface))?;
                let interface_name = interface.name();
                let filename = interface_name
                    .split('.')
                    .next_back()
                    .expect("Failed to split name");
                let filename = to_snakecase(filename);
                std::fs::write(format!("{filename}.rs"), output)?;
                println!("Generated code for `{interface_name}` in {filename}.rs");
            }
        }
        // A single output is one document: all interfaces go into one `file_code` call, so
        // the doc header and any shared type definitions appear only once.
        _ if needed_ifaces.is_empty() => (),
        OutputTarget::Stdout => println!("{}", file_code(&needed_ifaces)?),
        OutputTarget::SingleFile(mut file) => {
            file.write_all(file_code(&needed_ifaces)?.as_bytes())?;
            for interface in &needed_ifaces {
                println!("Generated code for `{}`", interface.name());
            }
        }
    }

    Ok(())
}

struct DBusInfo<'a>(
    Node<'a>,
    Option<BusName<'a>>,
    Option<ObjectPath<'a>>,
    String,
);

impl DBusInfo<'_> {
    fn new(
        connection: Connection,
        service: String,
        object_path: String,
    ) -> Result<Self, Box<dyn Error>> {
        let service: BusName<'_> = service.try_into()?;
        let path: ObjectPath<'_> = object_path.try_into()?;

        let input_src = format!("Interface '{path}' from service '{service}' on system bus",);

        let xml = zbus::block_on(async {
            IntrospectableProxy::builder(&connection)
                .destination(service.clone())
                .path(path.clone())
                .build()
                .await
                .unwrap()
                .introspect()
                .await
        })?;

        let (node, warnings) = Node::from_reader_with_warnings(xml.as_bytes())?;
        report_warnings(&warnings);

        Ok(DBusInfo(node, Some(service), Some(path), input_src))
    }
}

/// Warn on stderr about ignored XML content, once per distinct message.
fn report_warnings(warnings: &[Warning]) {
    let mut seen = HashSet::new();
    for warning in warnings {
        if seen.insert(warning.message()) {
            eprintln!("Warning: {}", warning.message());
        }
    }
}
