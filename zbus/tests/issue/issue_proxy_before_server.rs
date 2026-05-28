use ntest::timeout;
use test_log::test;
use zbus::block_on;

use zbus::Result;

/// Test that demonstrates a bug: if a client proxy is created *before* the server
/// registers the interface, reading a property from that proxy will always fail,
/// even after the server interface has been registered.
///
/// When the proxy is built with caching enabled (default), the background caching
/// task issues a GetAll Properties call. If the interface isn't registered yet, the
/// server responds with UnknownObject/UnknownInterface. This error is stored in the
/// cache's `CachingResult`. Every subsequent call to `get_property` calls
/// `cache.ready().await?` which returns the stored error, so the property can never
/// be read from this proxy instance — even after the interface is properly registered.
#[test]
#[timeout(15000)]
fn proxy_created_before_server_property_fails() {
    block_on(test_proxy_created_before_server_property()).unwrap();
}

async fn test_proxy_created_before_server_property() -> Result<()> {
    use std::time::Duration;

    #[derive(Debug, Default)]
    struct MyService {
        value: u32,
    }

    #[zbus::interface(name = "org.zbus.TestPropertyBeforeServer")]
    impl MyService {
        #[zbus(property)]
        fn value(&self) -> u32 {
            self.value
        }
    }

    #[zbus::proxy(
        gen_blocking = false,
        interface = "org.zbus.TestPropertyBeforeServer",
        default_service = "org.zbus.TestPropertyBeforeServer",
        default_path = "/org/zbus/TestPropertyBeforeServer"
    )]
    trait MyService {
        #[zbus(property)]
        fn value(&self) -> zbus::Result<u32>;
    }

    // Build the service connection and claim the well-known name WITHOUT
    // registering the test interface. Access object_server() to ensure
    // the dispatch task is running so it can respond to incoming calls
    // with proper errors (UnknownObject/UnknownInterface).
    let service_conn = zbus::connection::Builder::session()
        .unwrap()
        .name("org.zbus.TestPropertyBeforeServer")
        .unwrap()
        .build()
        .await
        .unwrap();
    // Start the object server dispatch task so it can reply to method calls.
    let _ = service_conn.object_server();

    // Small delay to ensure the object server dispatch task is ready.
    #[cfg(feature = "tokio")]
    tokio::time::sleep(Duration::from_millis(50)).await;
    #[cfg(not(feature = "tokio"))]
    async_io::Timer::after(Duration::from_millis(50)).await;

    // Create the client proxy BEFORE the server has the interface.
    // The proxy's caching task will issue a GetAll call which will fail with
    // UnknownObject since no interface is registered at the requested path.
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

    // Attempt to read the property. This triggers cache.ready().await which
    // waits for the caching task's GetAll to complete. Since there's no interface
    // registered, the server replies with an error and the cache stores it.
    let first_result = proxy.value().await;
    assert!(
        first_result.is_err(),
        "Expected first property read to fail (interface not registered yet), \
         but it succeeded with value: {:?}",
        first_result.ok()
    );

    // Now register the interface on the server.
    service_conn
        .object_server()
        .at(
            "/org/zbus/TestPropertyBeforeServer",
            MyService { value: 42 },
        )
        .await
        .unwrap();

    // Give the server a moment to be ready.
    #[cfg(feature = "tokio")]
    tokio::time::sleep(Duration::from_millis(100)).await;
    #[cfg(not(feature = "tokio"))]
    async_io::Timer::after(Duration::from_millis(100)).await;

    // Try to read the property again. The interface IS now registered and a fresh
    // Get call would work. But the proxy's cache.ready() still returns the
    // original error, so this always fails — demonstrating the bug.
    let second_result = proxy.value().await;

    // BUG: This assertion documents the broken behavior. Once the bug is fixed this should not panic.
    let _ = second_result.expect("Should return the value after the interface is published");

    Ok(())
}
