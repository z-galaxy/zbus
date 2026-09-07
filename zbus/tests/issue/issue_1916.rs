use ntest::timeout;
use tracing::instrument;
use zbus::{Connection, connection::Builder, fdo::ObjectManager};

struct Iface;

#[zbus::interface(name = "org.zbus.Issue1916")]
impl Iface {
    fn noop(&self) {}
}

struct StandardNamed;

#[zbus::interface(name = "org.freedesktop.DBus.Properties")]
impl StandardNamed {
    fn noop(&self) {}
}

#[instrument]
#[test]
#[timeout(15000)]
fn issue_1916() {
    // Reproducer for issue #1916, where `ObjectServer::remove` only unlinked the leaf node,
    // leaving the auto-created (and now empty & childless) ancestor nodes behind in the tree.
    zbus::block_on(issue_1916_async());
}

async fn issue_1916_async() {
    let leaf = "/org/zbus/issue1916/sub/leaf";
    let server_conn = Builder::session()
        .unwrap()
        .serve_at(leaf, Iface)
        .unwrap()
        .build()
        .await
        .unwrap();
    let client_conn = Connection::session().await.unwrap();
    let introspect = |path: &'static str| {
        let client_conn = client_conn.clone();
        let dest = server_conn.unique_name().unwrap().clone();
        async move {
            zbus::fdo::IntrospectableProxy::builder(&client_conn)
                .destination(dest)
                .unwrap()
                .path(path)
                .unwrap()
                .build()
                .await
                .unwrap()
                .introspect()
                .await
        }
    };
    let object_server = server_conn.object_server();

    // Removing the only interface of the only leaf must prune all the auto-created ancestors.
    assert!(object_server.remove::<Iface, _>(leaf).await.unwrap());
    let xml = introspect("/").await.unwrap();
    assert!(!xml.contains("<node name="), "leftover nodes: {xml}");

    // An ancestor with other children must survive the pruning.
    object_server
        .at("/org/zbus/issue1916/a", Iface)
        .await
        .unwrap();
    object_server
        .at("/org/zbus/issue1916/b", Iface)
        .await
        .unwrap();
    assert!(
        object_server
            .remove::<Iface, _>("/org/zbus/issue1916/a")
            .await
            .unwrap()
    );
    let xml = introspect("/org/zbus/issue1916").await.unwrap();
    assert!(xml.contains("<node name=\"b\">"), "missing node b: {xml}");
    assert!(!xml.contains("<node name=\"a\">"), "leftover node a: {xml}");
    assert!(
        object_server
            .remove::<Iface, _>("/org/zbus/issue1916/b")
            .await
            .unwrap()
    );
    let xml = introspect("/").await.unwrap();
    assert!(!xml.contains("<node name="), "leftover nodes: {xml}");

    // An ancestor with an interface of its own must survive the pruning of its child, even if
    // that interface is an `ObjectManager`.
    object_server.at("/org/zbus", Iface).await.unwrap();
    object_server
        .at("/org/zbus/issue1916", ObjectManager)
        .await
        .unwrap();
    object_server
        .at("/org/zbus/issue1916/agent", Iface)
        .await
        .unwrap();
    assert!(
        object_server
            .remove::<Iface, _>("/org/zbus/issue1916/agent")
            .await
            .unwrap()
    );
    let xml = introspect("/org/zbus").await.unwrap();
    assert!(
        xml.contains("<node name=\"issue1916\">"),
        "ObjectManager node pruned: {xml}"
    );
    let xml = introspect("/org").await.unwrap();
    assert!(
        xml.contains("<node name=\"zbus\">"),
        "node with own interface pruned: {xml}"
    );
    assert!(
        object_server
            .remove::<ObjectManager, _>("/org/zbus/issue1916")
            .await
            .unwrap()
    );
    assert!(object_server.remove::<Iface, _>("/org/zbus").await.unwrap());
    let xml = introspect("/").await.unwrap();
    assert!(!xml.contains("<node name="), "leftover nodes: {xml}");

    // The root node is never destroyed, only its interfaces are removed.
    object_server.at("/", Iface).await.unwrap();
    assert!(!object_server.remove::<Iface, _>("/").await.unwrap());
    let xml = introspect("/").await.unwrap();
    assert!(!xml.contains("org.zbus.Issue1916"), "leftover iface: {xml}");

    // An object whose node still has children must not be destroyed, so that the children stay
    // reachable.
    object_server.at("/org/zbus", Iface).await.unwrap();
    object_server.at("/org/zbus/child", Iface).await.unwrap();
    assert!(!object_server.remove::<Iface, _>("/org/zbus").await.unwrap());
    let xml = introspect("/org/zbus/child").await.unwrap();
    assert!(
        xml.contains("org.zbus.Issue1916"),
        "child iface lost: {xml}"
    );
    // With its own interface already gone, removing the child now prunes the whole chain.
    assert!(
        object_server
            .remove::<Iface, _>("/org/zbus/child")
            .await
            .unwrap()
    );
    let xml = introspect("/").await.unwrap();
    assert!(!xml.contains("<node name="), "leftover nodes: {xml}");

    // An `ObjectManager` registered at the path keeps the object alive as well.
    object_server.at("/org/zbus", ObjectManager).await.unwrap();
    object_server.at("/org/zbus", Iface).await.unwrap();
    assert!(!object_server.remove::<Iface, _>("/org/zbus").await.unwrap());
    assert!(
        object_server
            .remove::<ObjectManager, _>("/org/zbus")
            .await
            .unwrap()
    );
    let xml = introspect("/").await.unwrap();
    assert!(!xml.contains("<node name="), "leftover nodes: {xml}");

    // A failed registration (here due to the name colliding with a default interface) must not
    // leave freshly auto-created nodes behind...
    assert!(
        object_server
            .at("/org/zbus/ghost", StandardNamed)
            .await
            .unwrap()
            .is_some()
    );
    let xml = introspect("/").await.unwrap();
    assert!(!xml.contains("<node name="), "leftover nodes: {xml}");

    // ... while a failed registration at a path still in use must not remove anything.
    object_server.at("/org/zbus/iface", Iface).await.unwrap();
    assert!(
        object_server
            .at("/org/zbus/iface", Iface)
            .await
            .unwrap()
            .is_some()
    );
    let xml = introspect("/org/zbus").await.unwrap();
    assert!(
        xml.contains("<node name=\"iface\">"),
        "node wrongly pruned: {xml}"
    );
    assert!(
        object_server
            .remove::<Iface, _>("/org/zbus/iface")
            .await
            .unwrap()
    );
}
