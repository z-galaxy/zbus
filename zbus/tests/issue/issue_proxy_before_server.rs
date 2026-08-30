use std::time::Duration;

use futures_util::StreamExt;
use ntest::timeout;
use test_log::test;
use zbus::{Result, block_on};

async fn sleep(d: Duration) {
    #[cfg(feature = "tokio")]
    tokio::time::sleep(d).await;
    #[cfg(not(feature = "tokio"))]
    async_io::Timer::after(d).await;
}

/// Interface registered *after* the proxy is created, on a service that does **not** expose an
/// `ObjectManager`. Neither `NameOwnerChanged` nor `InterfacesAdded` will fire — the cache
/// recovers via the `kick_retry` fallback that `Proxy::get_property` triggers after a
/// successful out-of-band `Get`.
#[test]
#[timeout(15000)]
fn proxy_created_before_interface_added() {
    block_on(test_proxy_created_before_interface_added()).unwrap();
}

async fn test_proxy_created_before_interface_added() -> Result<()> {
    #[derive(Debug, Default)]
    struct MyService {
        value: u32,
    }

    #[zbus::interface(name = "org.zbus.TestPropBeforeIface")]
    impl MyService {
        #[zbus(property)]
        fn value(&self) -> u32 {
            self.value
        }
    }

    #[zbus::proxy(
        gen_blocking = false,
        interface = "org.zbus.TestPropBeforeIface",
        default_service = "org.zbus.TestPropBeforeIface",
        default_path = "/org/zbus/TestPropBeforeIface"
    )]
    trait MyService {
        #[zbus(property)]
        fn value(&self) -> zbus::Result<u32>;
    }

    // Service connection claims the well-known name *without* registering the test interface.
    let service_conn = zbus::connection::Builder::session()
        .unwrap()
        .name("org.zbus.TestPropBeforeIface")
        .unwrap()
        .build()
        .await
        .unwrap();
    let _ = service_conn.object_server();
    sleep(Duration::from_millis(50)).await;

    let client_conn = zbus::connection::Builder::session()
        .unwrap()
        .method_timeout(Duration::from_millis(500))
        .build()
        .await
        .unwrap();
    let proxy = MyServiceProxy::builder(&client_conn)
        .destination(service_conn.unique_name().unwrap())
        .unwrap()
        .build()
        .await
        .unwrap();

    // Start a property change stream *before* the interface is registered. After recovery the
    // stream should receive the value from the repopulated cache.
    let mut stream = proxy.receive_value_changed().await;

    // Initial read fails: cache init failed (UnknownObject) and direct Get also fails.
    assert!(proxy.value().await.is_err());

    // Register the interface — no NameOwnerChanged, no InterfacesAdded.
    service_conn
        .object_server()
        .at("/org/zbus/TestPropBeforeIface", MyService { value: 42 })
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    // Direct Get now succeeds and kicks the background populate task.
    assert_eq!(proxy.value().await.unwrap(), 42);

    // The pre-existing property stream sees the repopulation.
    let changed = stream.next().await.expect("property stream closed");
    assert_eq!(changed.get().await.unwrap(), 42);

    // Subsequent reads come from the repopulated cache.
    sleep(Duration::from_millis(50)).await;
    assert_eq!(proxy.value().await.unwrap(), 42);

    Ok(())
}

/// Proxy is created before the *service* connects to the bus at all. The cache recovers via the
/// `NameOwnerChanged` subscription installed by the background populate task — no `get_property`
/// kick is required.
#[test]
#[timeout(15000)]
fn proxy_created_before_service_connects() {
    block_on(test_proxy_created_before_service_connects()).unwrap();
}

async fn test_proxy_created_before_service_connects() -> Result<()> {
    #[derive(Debug, Default)]
    struct MyService {
        value: u32,
    }

    #[zbus::interface(name = "org.zbus.TestPropBeforeService")]
    impl MyService {
        #[zbus(property)]
        fn value(&self) -> u32 {
            self.value
        }
    }

    #[zbus::proxy(
        gen_blocking = false,
        interface = "org.zbus.TestPropBeforeService",
        default_service = "org.zbus.TestPropBeforeService",
        default_path = "/org/zbus/TestPropBeforeService"
    )]
    trait MyService {
        #[zbus(property)]
        fn value(&self) -> zbus::Result<u32>;
    }

    let client_conn = zbus::connection::Builder::session()
        .unwrap()
        .method_timeout(Duration::from_millis(500))
        .build()
        .await
        .unwrap();
    let proxy = MyServiceProxy::new(&client_conn).await.unwrap();

    // Start a property change stream before the service exists.
    let mut stream = proxy.receive_value_changed().await;

    // Cache init fails (name has no owner).
    assert!(proxy.value().await.is_err());

    // Now bring up the service with the interface already registered. NameOwnerChanged will
    // fire and wake the populate task — no further get_property calls are made.
    let service_conn = zbus::connection::Builder::session()
        .unwrap()
        .name("org.zbus.TestPropBeforeService")
        .unwrap()
        .serve_at("/org/zbus/TestPropBeforeService", MyService { value: 7 })
        .unwrap()
        .build()
        .await
        .unwrap();
    let _ = service_conn.object_server();

    let changed = stream.next().await.expect("property stream closed");
    assert_eq!(changed.get().await.unwrap(), 7);

    Ok(())
}

/// Service is already on the bus and exposes an `ObjectManager`, but the test interface is
/// registered *after* the proxy is created. The cache should recover via the `InterfacesAdded`
/// signal emitted by the `ObjectManager` — no `get_property` kick required.
#[test]
#[timeout(15000)]
fn proxy_recovers_via_object_manager() {
    block_on(test_proxy_recovers_via_object_manager()).unwrap();
}

async fn test_proxy_recovers_via_object_manager() -> Result<()> {
    #[derive(Debug, Default)]
    struct MyService {
        value: u32,
    }

    #[zbus::interface(name = "org.zbus.TestPropObjMgr")]
    impl MyService {
        #[zbus(property)]
        fn value(&self) -> u32 {
            self.value
        }
    }

    #[zbus::proxy(
        gen_blocking = false,
        interface = "org.zbus.TestPropObjMgr",
        default_service = "org.zbus.TestPropObjMgr",
        default_path = "/org/zbus/TestPropObjMgr/Obj"
    )]
    trait MyService {
        #[zbus(property)]
        fn value(&self) -> zbus::Result<u32>;
    }

    // Service comes up with the well-known name and an `ObjectManager` at the parent path, but
    // *without* the test interface registered yet.
    let service_conn = zbus::connection::Builder::session()
        .unwrap()
        .name("org.zbus.TestPropObjMgr")
        .unwrap()
        .serve_at("/org/zbus/TestPropObjMgr", zbus::fdo::ObjectManager)
        .unwrap()
        .build()
        .await
        .unwrap();
    let _ = service_conn.object_server();
    sleep(Duration::from_millis(50)).await;

    let client_conn = zbus::connection::Builder::session()
        .unwrap()
        .method_timeout(Duration::from_millis(500))
        .build()
        .await
        .unwrap();
    let proxy = MyServiceProxy::new(&client_conn).await.unwrap();

    // Stream opened before the interface exists — should fire once recovery happens.
    let mut stream = proxy.receive_value_changed().await;

    // Cache init fails: the path/interface is not yet registered.
    assert!(proxy.value().await.is_err());

    // Now register the interface under the ObjectManager. This emits `InterfacesAdded`, which
    // the populate task is subscribed to (filtered by our path), waking it up to retry `init`.
    service_conn
        .object_server()
        .at("/org/zbus/TestPropObjMgr/Obj", MyService { value: 99 })
        .await
        .unwrap();

    // No get_property call between the signal firing and observing the stream update — recovery
    // must happen purely through the `InterfacesAdded` subscription.
    let changed = stream.next().await.expect("property stream closed");
    assert_eq!(changed.get().await.unwrap(), 99);

    Ok(())
}
