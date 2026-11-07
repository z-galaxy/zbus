#![cfg(all(
    feature = "proxy",
    feature = "service",
    any(feature = "default-rt", feature = "tokio")
))]

#[cfg(feature = "object-manager")]
use futures_util::StreamExt;
use ntest::timeout;
#[cfg(feature = "object-manager")]
use std::collections::BTreeMap;
#[cfg(feature = "object-manager")]
use zbus::fdo::{
    InterfacesAddedStream, InterfacesRemovedStream, ObjectManager, ObjectManagerProxy,
};
use zbus::{
    Connection, DBusError, Error, ObjectServer,
    connection::Builder,
    fdo::{IntrospectableProxy, PropertiesProxy},
    message::{Header, Message},
    names::InterfaceName,
    object_server::SignalEmitter,
};

#[test]
#[timeout(15000)]
fn exact_and_fallback() {
    zbus::block_on(exact_and_fallback_());
}

async fn exact_and_fallback_() {
    let prefix = "/org/zbus/FallbackTest/ExactAndFallback";
    let defined = format!("{prefix}/defined");
    let builder = Builder::session().serve_at(defined.as_str(), Demo { label: "exact" });
    let setup = Setup::new(builder).await;
    let server = setup.service.object_server();
    assert!(
        server
            .at_fallback(prefix, Demo { label: "fallback" })
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(setup.get(&defined).await, format!("exact:{defined}"));
    // The fallback serves the prefix itself, as well as the paths below it that nothing is
    // registered at.
    for path in [
        prefix.to_string(),
        format!("{prefix}/undefined"),
        format!("{prefix}/undefined/deeper"),
        // A non-fallback registration does not serve the paths below it.
        format!("{defined}/sub"),
    ] {
        assert_eq!(setup.get(&path).await, format!("fallback:{path}"));
    }
}

#[test]
#[timeout(15000)]
fn auto_created_intermediate_nodes() {
    zbus::block_on(auto_created_intermediate_nodes_());
}

async fn auto_created_intermediate_nodes_() {
    let prefix = "/org/zbus/FallbackTest/Intermediate";
    let leaf = format!("{prefix}/a/b/c");
    let setup = Setup::new(Builder::session().serve_at(leaf.as_str(), Other)).await;
    assert!(
        setup
            .service
            .object_server()
            .at_fallback(prefix, Demo { label: "fallback" })
            .await
            .unwrap()
            .is_none()
    );

    // The nodes at `a` and `a/b` were created implicitly, and they have no `Demo` of their own.
    for path in [format!("{prefix}/a"), format!("{prefix}/a/b"), leaf] {
        assert_eq!(setup.get(&path).await, format!("fallback:{path}"));
    }
}

#[test]
#[timeout(15000)]
fn nested_fallbacks() {
    zbus::block_on(nested_fallbacks_());
}

async fn nested_fallbacks_() {
    let outer = "/org/zbus/FallbackTest/Nested";
    let inner = format!("{outer}/inner");
    let builder = Builder::session().serve_at_fallback(outer, Demo { label: "outer" });
    let setup = Setup::new(builder).await;
    let server = setup.service.object_server();
    assert!(
        server
            .at_fallback(inner.as_str(), Demo { label: "inner" })
            .await
            .unwrap()
            .is_none()
    );

    let path = format!("{outer}/other");
    assert_eq!(setup.get(&path).await, format!("outer:{path}"));
    assert_eq!(setup.get(&inner).await, format!("inner:{inner}"));
    for path in [format!("{inner}/x"), format!("{inner}/x/y")] {
        assert_eq!(setup.get(&path).await, format!("inner:{path}"));
    }

    // Once the closest fallback is gone, the next one up serves its paths.
    assert!(server.remove::<Demo, _>(inner.as_str()).await.unwrap());
    let path = format!("{inner}/x");
    assert_eq!(setup.get(&path).await, format!("outer:{path}"));
}

#[test]
#[timeout(15000)]
fn properties_on_virtual_path() {
    zbus::block_on(properties_on_virtual_path_());
}

async fn properties_on_virtual_path_() {
    let prefix = "/org/zbus/FallbackTest/Properties";
    let virtual_path = format!("{prefix}/virtual/deeper");
    let builder = Builder::session().serve_at_fallback(prefix, Demo { label: "fallback" });
    let setup = Setup::new(builder).await;

    let properties = setup.properties(&virtual_path).await;
    let iface = InterfaceName::try_from(DEMO).unwrap();
    let value = properties.get(iface.clone(), "Path").await.unwrap();
    assert_eq!(String::try_from(value).unwrap(), virtual_path);
    let all = properties.get_all(iface).await.unwrap();
    let value = all.get("Path").unwrap().try_clone().unwrap();
    assert_eq!(String::try_from(value).unwrap(), virtual_path);
}

#[test]
#[timeout(15000)]
fn introspection() {
    zbus::block_on(introspection_());
}

async fn introspection_() {
    let prefix = "/org/zbus/FallbackTest/Introspection";
    let builder = Builder::session().serve_at_fallback(prefix, Demo { label: "fallback" });
    let setup = Setup::new(builder).await;
    let server = setup.service.object_server();

    let xml = setup
        .introspect(&format!("{prefix}/virtual"))
        .await
        .unwrap();
    assert_eq!(demo_count(&xml), 1, "{xml}");
    for name in [
        "org.freedesktop.DBus.Properties",
        "org.freedesktop.DBus.Introspectable",
        "org.freedesktop.DBus.Peer",
    ] {
        assert!(xml.contains(name), "missing {name}: {xml}");
    }
    assert!(!xml.contains("<node name="), "unexpected child: {xml}");

    // Each real object in the subtree lists the inherited interface, unless it has its own.
    server.at(format!("{prefix}/a/b"), Other).await.unwrap();
    server
        .at(format!("{prefix}/own"), Demo { label: "own" })
        .await
        .unwrap();
    let xml = setup.introspect(prefix).await.unwrap();
    // The prefix, `a`, `a/b` and `own`.
    assert_eq!(demo_count(&xml), 4, "{xml}");
    for name in ["a", "b", "own"] {
        assert!(xml.contains(&format!("<node name=\"{name}\">")), "{xml}");
    }
    assert!(!xml.contains("virtual"), "{xml}");
}

#[test]
#[timeout(15000)]
fn errors() {
    zbus::block_on(errors_());
}

async fn errors_() {
    let prefix = "/org/zbus/FallbackTest/Errors";
    let builder = Builder::session().serve_at_fallback(prefix, Demo { label: "fallback" });
    let setup = Setup::new(builder).await;

    // Paths outside the prefix are not objects, not even those that share its start.
    for path in [
        "/org/zbus/FallbackTest/ErrorsOther".to_string(),
        "/elsewhere".to_string(),
    ] {
        assert_eq!(setup.error_name(&path, DEMO, "Get").await, UNKNOWN_OBJECT);
    }

    // An ancestor of the prefix is an (auto-created) object, but one without the interface.
    assert_eq!(
        setup
            .error_name("/org/zbus/FallbackTest", DEMO, "Get")
            .await,
        UNKNOWN_INTERFACE
    );

    let virtual_path = format!("{prefix}/virtual");
    let other = "org.zbus.FallbackTest.Other";
    assert_eq!(
        setup.error_name(&virtual_path, other, "Noop").await,
        UNKNOWN_INTERFACE
    );
    let ping = "org.freedesktop.DBus.Peer";
    setup.call(&virtual_path, ping, "Ping").await.unwrap();
}

#[test]
#[timeout(15000)]
fn at_fallback_conflicts() {
    zbus::block_on(at_fallback_conflicts_());
}

async fn at_fallback_conflicts_() {
    let prefix = "/org/zbus/FallbackTest/Conflicts";
    let exact = format!("{prefix}/exact");
    let builder = Builder::session().serve_at_fallback(prefix, Demo { label: "first" });
    let setup = Setup::new(builder).await;
    let server = setup.service.object_server();
    assert!(
        server
            .at_fallback(prefix, Demo { label: "second" })
            .await
            .unwrap()
            .is_some_and(|demo| demo.label == "second")
    );
    assert!(
        server
            .at(prefix, Demo { label: "third" })
            .await
            .unwrap()
            .is_some()
    );

    assert!(
        server
            .at(exact.as_str(), Demo { label: "exact" })
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        server
            .at_fallback(exact.as_str(), Demo { label: "fourth" })
            .await
            .unwrap()
            .is_some()
    );

    // The rejected registrations changed nothing.
    assert_eq!(setup.get(prefix).await, format!("first:{prefix}"));
    assert_eq!(setup.get(&exact).await, format!("exact:{exact}"));
}

#[test]
#[timeout(15000)]
fn interface_ref_for_virtual_path() {
    zbus::block_on(interface_ref_for_virtual_path_());
}

async fn interface_ref_for_virtual_path_() {
    let prefix = "/org/zbus/FallbackTest/InterfaceRef";
    let virtual_path = format!("{prefix}/virtual/deeper");
    let builder = Builder::session().serve_at_fallback(prefix, Demo { label: "fallback" });
    let setup = Setup::new(builder).await;
    let server = setup.service.object_server();

    let iface_ref = server
        .interface::<_, Demo>(virtual_path.as_str())
        .await
        .unwrap();
    assert_eq!(iface_ref.signal_emitter().path().as_str(), virtual_path);
    assert_eq!(iface_ref.get().await.label, "fallback");

    // Without a fallback for it, the path has no interface.
    let result = server.interface::<_, Demo>("/elsewhere").await;
    assert!(matches!(result, Err(Error::InterfaceNotFound)));
}

#[test]
#[timeout(15000)]
fn removal() {
    zbus::block_on(removal_());
}

async fn removal_() {
    let prefix = "/org/zbus/FallbackTest/Removal";
    let virtual_path = format!("{prefix}/virtual");
    let builder = Builder::session().serve_at_fallback(prefix, Demo { label: "fallback" });
    let setup = Setup::new(builder).await;
    let server = setup.service.object_server();
    assert_eq!(
        setup.get(&virtual_path).await,
        format!("fallback:{virtual_path}")
    );

    assert!(server.remove::<Demo, _>(prefix).await.unwrap());
    for path in [virtual_path.as_str(), prefix] {
        assert_eq!(setup.error_name(path, DEMO, "Get").await, UNKNOWN_OBJECT);
    }
    assert!(
        server
            .interface::<_, Demo>(virtual_path.as_str())
            .await
            .is_err()
    );
}

#[cfg(feature = "object-manager")]
#[test]
#[timeout(15000)]
fn object_manager() {
    zbus::block_on(object_manager_());
}

#[cfg(feature = "object-manager")]
async fn object_manager_() {
    let manager = "/org/zbus/FallbackTest/Manager";
    let prefix = format!("{manager}/items");
    let real = format!("{prefix}/real");
    let setup = Setup::new(Builder::session().serve_at(manager, ObjectManager)).await;
    let server = setup.service.object_server();
    server
        .at_fallback(prefix.as_str(), Demo { label: "fallback" })
        .await
        .unwrap();
    server.at(real.as_str(), Other).await.unwrap();
    // Serving the path below is not reporting it.
    let virtual_path = format!("{prefix}/virtual");
    assert_eq!(
        setup.get(&virtual_path).await,
        format!("fallback:{virtual_path}")
    );

    let objects = ObjectManagerProxy::builder(&setup.client)
        .destination(setup.dest.as_str())
        .path(manager)
        .build()
        .await
        .unwrap()
        .get_managed_objects()
        .await
        .unwrap();
    // The fallback is reported at the path it is registered at and at the objects registered
    // below it, which also have their own interfaces. Paths that nothing is registered at are not
    // reported.
    let interfaces_at = |path: &str| {
        let path = zbus::OwnedObjectPath::try_from(path).unwrap();
        let mut names = objects[&path]
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        names.sort();
        names
    };
    assert_eq!(interfaces_at(&prefix), [DEMO]);
    assert_eq!(interfaces_at(&real), [DEMO, OTHER]);
    // The fallback's properties are evaluated at the path of the object they are reported for.
    let path = zbus::OwnedObjectPath::try_from(real.as_str()).unwrap();
    let demo = &objects[&path][DEMO];
    let reported = demo["Path"].try_clone().unwrap();
    assert_eq!(String::try_from(reported).unwrap(), real);
    assert!(
        objects.keys().all(|path| !path.contains("virtual")),
        "{objects:?}"
    );
}

#[cfg(feature = "object-manager")]
#[test]
#[timeout(15000)]
fn object_manager_signals() {
    zbus::block_on(object_manager_signals_());
}

#[cfg(feature = "object-manager")]
async fn object_manager_signals_() {
    let manager = "/org/zbus/FallbackTest/Signals";
    let items = format!("{manager}/items");
    let child = format!("{items}/child");
    let new = format!("{items}/new");
    let leaf = format!("{new}/leaf");
    let setup = Setup::new(Builder::session().serve_at(manager, ObjectManager)).await;
    let server = setup.service.object_server();

    // Creating the streams waits for the match rules to be in place, so no signal is missed.
    let proxy = ObjectManagerProxy::builder(&setup.client)
        .destination(setup.dest.as_str())
        .path(manager)
        .build()
        .await
        .unwrap();
    let mut added_stream = proxy.receive_interfaces_added().await.unwrap();
    let mut removed_stream = proxy.receive_interfaces_removed().await.unwrap();

    // The implicitly created `items` inherits nothing, so it is not announced.
    server.at(child.as_str(), Other).await.unwrap();
    let signals = added(&mut added_stream, 1).await;
    let expected_signals = BTreeMap::from([(child.clone(), (vec![OTHER.to_string()], None))]);
    assert_eq!(signals, expected_signals);

    // Registering the fallback announces it for the object it is registered at and for the ones
    // below it, with its properties evaluated at each of their paths.
    server
        .at_fallback(items.as_str(), Demo { label: "fallback" })
        .await
        .unwrap();
    let signals = added(&mut added_stream, 2).await;
    let expected_signals = BTreeMap::from([
        (items.clone(), (vec![DEMO.to_string()], Some(items.clone()))),
        (child.clone(), (vec![DEMO.to_string()], Some(child.clone()))),
    ]);
    assert_eq!(signals, expected_signals);

    // The new object and the implicitly created node on the way to it both inherit the fallback.
    server.at(leaf.as_str(), Other).await.unwrap();
    let signals = added(&mut added_stream, 2).await;
    let expected_signals = BTreeMap::from([
        (new.clone(), (vec![DEMO.to_string()], Some(new.clone()))),
        (
            leaf.clone(),
            (
                vec![DEMO.to_string(), OTHER.to_string()],
                Some(leaf.clone()),
            ),
        ),
    ]);
    assert_eq!(signals, expected_signals);

    // Both nodes are pruned, as nothing else is left at them, and they go with all their
    // interfaces, inherited ones included.
    assert!(server.remove::<Other, _>(leaf.as_str()).await.unwrap());
    let signals = removed(&mut removed_stream, 2).await;
    let expected_signals = expected([
        (new.as_str(), &[DEMO][..]),
        (leaf.as_str(), &[DEMO, OTHER][..]),
    ]);
    assert_eq!(signals, expected_signals);

    // The node at `items` stays, as it has a child, but the fallback is gone for both objects.
    assert!(!server.remove::<Demo, _>(items.as_str()).await.unwrap());
    let signals = removed(&mut removed_stream, 2).await;
    let expected_signals = expected([(items.as_str(), &[DEMO][..]), (child.as_str(), &[DEMO][..])]);
    assert_eq!(signals, expected_signals);
}

#[cfg(feature = "object-manager")]
#[test]
#[timeout(15000)]
fn fallback_registering_served_objects() {
    zbus::block_on(fallback_registering_served_objects_());
}

#[cfg(feature = "object-manager")]
async fn fallback_registering_served_objects_() {
    let manager = "/org/zbus/FallbackTest/Factory";
    let items = format!("{manager}/items");
    let one = format!("{items}/one");
    let setup = Setup::new(Builder::session().serve_at(manager, ObjectManager)).await;

    // Creating the stream waits for the match rule to be in place, so no signal is missed. A
    // stream created once the fallback is registered could still get the signal of the fallback,
    // as the bus orders no message of one connection against those of another.
    let proxy = ObjectManagerProxy::builder(&setup.client)
        .destination(setup.dest.as_str())
        .path(manager)
        .build()
        .await
        .unwrap();
    let mut added_stream = proxy.receive_interfaces_added().await.unwrap();
    setup
        .service
        .object_server()
        .at_fallback(items.as_str(), Demo { label: "fallback" })
        .await
        .unwrap();
    let signals = added(&mut added_stream, 1).await;
    let expected_signals =
        BTreeMap::from([(items.clone(), (vec![DEMO.to_string()], Some(items.clone())))]);
    assert_eq!(signals, expected_signals);

    // A `&self` method of the fallback registers an object that the fallback serves, which the
    // `ObjectManager` then reports with the properties of the fallback.
    setup
        .client
        .call_method(
            Some(setup.dest.as_str()),
            items.as_str(),
            Some(DEMO),
            "Create",
            &("one",),
        )
        .await
        .unwrap();
    let signals = added(&mut added_stream, 1).await;
    let expected_signals = BTreeMap::from([(
        one.clone(),
        (vec![DEMO.to_string(), OTHER.to_string()], Some(one.clone())),
    )]);
    assert_eq!(signals, expected_signals);
}

#[test]
#[timeout(15000)]
fn builder_serve_at_fallback() {
    zbus::block_on(builder_serve_at_fallback_());
}

async fn builder_serve_at_fallback_() {
    let prefix = "/org/zbus/FallbackTest/Builder";
    let builder = Builder::session().serve_at_fallback(prefix, Demo { label: "fallback" });
    let setup = Setup::new(builder).await;

    for path in [prefix.to_string(), format!("{prefix}/virtual/deeper")] {
        assert_eq!(setup.get(&path).await, format!("fallback:{path}"));
    }
}

/// A service connection, a client connection and the unique name to address the service by.
///
/// Each test registers something through the service's builder, as building it then waits for
/// the object server to listen for calls, which `Connection::object_server` does not.
struct Setup {
    service: Connection,
    client: Connection,
    dest: String,
}

impl Setup {
    async fn new(builder: Builder<'_>) -> Self {
        let service = builder.build().await.unwrap();
        let client = Connection::session().await.unwrap();
        let dest = service.unique_name().unwrap().to_string();

        Self {
            service,
            client,
            dest,
        }
    }

    async fn call(&self, path: &str, iface: &str, member: &str) -> zbus::Result<Message> {
        self.client
            .call_method(Some(self.dest.as_str()), path, Some(iface), member, &())
            .await
    }

    async fn get(&self, path: &str) -> String {
        let reply = self.call(path, DEMO, "Get").await.unwrap();

        reply.body().deserialize().unwrap()
    }

    async fn error_name(&self, path: &str, iface: &str, member: &str) -> String {
        match self.call(path, iface, member).await {
            // Errors of the standard D-Bus names come back as `fdo::Error`s.
            Err(Error::FDO(error)) => error.name().to_string(),
            Err(Error::MethodError(name, _, _)) => name.to_string(),
            other => panic!("expected a method error for '{path}', got {other:?}"),
        }
    }

    async fn properties(&self, path: &str) -> PropertiesProxy<'_> {
        PropertiesProxy::builder(&self.client)
            .destination(self.dest.as_str())
            .path(path.to_string())
            .build()
            .await
            .unwrap()
    }

    async fn introspect(&self, path: &str) -> zbus::fdo::Result<String> {
        IntrospectableProxy::builder(&self.client)
            .destination(self.dest.as_str())
            .path(path.to_string())
            .build()
            .await
            .unwrap()
            .introspect()
            .await
    }
}

fn demo_count(xml: &str) -> usize {
    xml.matches(&format!("<interface name=\"{DEMO}\">")).count()
}

/// Reads `count` `InterfacesAdded` signals, as a map from object path to its sorted interfaces.
///
/// The `Path` property reported for [`Demo`], if any, is returned along with the interfaces.
#[cfg(feature = "object-manager")]
async fn added(
    stream: &mut InterfacesAddedStream,
    count: usize,
) -> BTreeMap<String, (Vec<String>, Option<String>)> {
    let mut signals = BTreeMap::new();
    for _ in 0..count {
        let signal = stream.next().await.unwrap();
        let args = signal.args().unwrap();
        let mut names = args
            .interfaces_and_properties
            .keys()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        names.sort();
        let path = args
            .interfaces_and_properties
            .iter()
            .find(|(name, _)| name.as_str() == DEMO)
            .map(|(_, properties)| {
                String::try_from(properties["Path"].try_clone().unwrap()).unwrap()
            });
        let previous = signals.insert(args.object_path.to_string(), (names, path));
        assert!(
            previous.is_none(),
            "duplicate signal for {}",
            args.object_path
        );
    }

    signals
}

/// Reads `count` `InterfacesRemoved` signals, as a map from object path to its sorted interfaces.
#[cfg(feature = "object-manager")]
async fn removed(
    stream: &mut InterfacesRemovedStream,
    count: usize,
) -> BTreeMap<String, Vec<String>> {
    let mut signals = BTreeMap::new();
    for _ in 0..count {
        let signal = stream.next().await.unwrap();
        let args = signal.args().unwrap();
        let mut names = args
            .interfaces
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        names.sort();
        let previous = signals.insert(args.object_path.to_string(), names);
        assert!(
            previous.is_none(),
            "duplicate signal for {}",
            args.object_path
        );
    }

    signals
}

/// Builds the expected signals: pairs of an object path and the interfaces it is reported with.
#[cfg(feature = "object-manager")]
fn expected<'a>(
    signals: impl IntoIterator<Item = (&'a str, &'a [&'a str])>,
) -> BTreeMap<String, Vec<String>> {
    signals
        .into_iter()
        .map(|(path, names)| {
            (
                path.to_string(),
                names.iter().map(ToString::to_string).collect(),
            )
        })
        .collect()
}

/// An interface whose replies tell which instance served a call, and for which path.
struct Demo {
    label: &'static str,
}

#[zbus::interface(name = "org.zbus.FallbackTest.Demo")]
impl Demo {
    fn get(&self, #[zbus(header)] header: Header<'_>) -> String {
        format!("{}:{}", self.label, header.path().unwrap())
    }

    #[zbus(property)]
    fn path(&self, #[zbus(signal_emitter)] emitter: SignalEmitter<'_>) -> String {
        emitter.path().to_string()
    }

    /// Registers [`Other`] at `name` below the path of the call, as a factory does.
    ///
    /// It takes `&self`: the `&mut self` method of a fallback cannot register the objects that
    /// the fallback serves, while an `ObjectManager` reports them.
    async fn create(
        &self,
        name: &str,
        #[zbus(object_server)] server: &ObjectServer,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> zbus::fdo::Result<()> {
        let path = format!("{}/{name}", emitter.path());
        server
            .at(path, Other)
            .await
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;

        Ok(())
    }
}

/// An interface unrelated to [`Demo`].
struct Other;

#[zbus::interface(name = "org.zbus.FallbackTest.Other")]
impl Other {
    fn noop(&self) {}
}

const DEMO: &str = "org.zbus.FallbackTest.Demo";
#[cfg(feature = "object-manager")]
const OTHER: &str = "org.zbus.FallbackTest.Other";
const UNKNOWN_OBJECT: &str = "org.freedesktop.DBus.Error.UnknownObject";
const UNKNOWN_INTERFACE: &str = "org.freedesktop.DBus.Error.UnknownInterface";
