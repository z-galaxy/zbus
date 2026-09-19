#![cfg(all(
    feature = "proxy",
    feature = "service",
    any(feature = "builtin-runtime", feature = "tokio")
))]

use ntest::timeout;
use test_log::test;
use zbus::block_on;

use zbus::Result;

#[test]
#[timeout(15000)]
fn uncached_property() {
    block_on(test_uncached_property()).unwrap();
}

async fn test_uncached_property() -> Result<()> {
    // A dummy boolean test service. It starts as `false` and can be
    // flipped to `true`. Two properties can access the inner value, with
    // and without caching.
    #[derive(Default)]
    struct ServiceUncachedPropertyTest(bool);
    #[zbus::interface(name = "org.freedesktop.zbus.UncachedPropertyTest")]
    impl ServiceUncachedPropertyTest {
        #[zbus(property)]
        fn cached_prop(&self) -> bool {
            self.0
        }
        #[zbus(property)]
        fn uncached_prop(&self) -> bool {
            self.0
        }
        async fn set_inner_to_true(&mut self) -> zbus::fdo::Result<()> {
            self.0 = true;
            Ok(())
        }
    }

    #[zbus::proxy(
        interface = "org.freedesktop.zbus.UncachedPropertyTest",
        default_service = "org.freedesktop.zbus.UncachedPropertyTest",
        default_path = "/org/freedesktop/zbus/UncachedPropertyTest"
    )]
    trait UncachedPropertyTest {
        #[zbus(property)]
        fn cached_prop(&self) -> zbus::Result<bool>;

        #[zbus(property(emits_changed_signal = "false"))]
        fn uncached_prop(&self) -> zbus::Result<bool>;

        fn set_inner_to_true(&self) -> zbus::Result<()>;
    }

    let service = zbus::connection::Builder::session()
        .serve_at(
            "/org/freedesktop/zbus/UncachedPropertyTest",
            ServiceUncachedPropertyTest(false),
        )
        .build()
        .await
        .unwrap();

    let dest = service.unique_name().unwrap();

    let client_conn = zbus::Connection::session().await.unwrap();
    let client = UncachedPropertyTestProxy::builder(&client_conn)
        .destination(dest)
        .build()
        .await
        .unwrap();

    // Query properties; this populates the cache too.
    assert!(!client.cached_prop().await.unwrap());
    assert!(!client.uncached_prop().await.unwrap());

    // Flip the inner value so we can observe the different semantics of
    // the two properties.
    client.set_inner_to_true().await.unwrap();

    // Query properties again; the first one should incur a stale read from
    // cache, while the second one should be able to read the live/updated
    // value.
    assert!(!client.cached_prop().await.unwrap());
    assert!(client.uncached_prop().await.unwrap());

    Ok(())
}

#[test]
#[timeout(15000)]
fn serde_property() {
    block_on(test_serde_property()).unwrap();
}

async fn test_serde_property() -> Result<()> {
    use std::collections::HashMap;

    use futures_util::StreamExt;
    use serde::{Deserialize, Serialize};
    use zbus::{Optional, OwnedValue, Str, Type, names::OwnedUniqueName};

    #[derive(Debug, Deserialize, Serialize, Type, PartialEq)]
    #[zvariant(signature = "s")]
    struct CustomString(String);

    struct Service(String);

    #[zbus::interface(name = "org.freedesktop.zbus.SerdePropertyTest")]
    impl Service {
        #[zbus(property)]
        fn cached_prop(&self) -> String {
            self.0.clone()
        }

        #[zbus(property)]
        fn set_cached_prop(&mut self, value: String) {
            self.0 = value;
        }

        #[zbus(property(emits_changed_signal = "false"))]
        fn uncached_prop(&self) -> String {
            self.0.clone()
        }

        #[zbus(property)]
        fn optional_name(&self) -> Optional<OwnedUniqueName> {
            Optional::default()
        }

        #[zbus(property)]
        fn dynamic_array(&self) -> Vec<OwnedValue> {
            vec![OwnedValue::from(Str::from("array value"))]
        }

        #[zbus(property)]
        fn dynamic_dict(&self) -> HashMap<String, OwnedValue> {
            HashMap::from([("key".to_string(), OwnedValue::from(Str::from("dict value")))])
        }
    }

    #[zbus::proxy(
        interface = "org.freedesktop.zbus.SerdePropertyTest",
        default_path = "/org/freedesktop/zbus/SerdePropertyTest"
    )]
    trait SerdePropertyTest {
        #[zbus(property)]
        fn cached_prop(&self) -> zbus::Result<CustomString>;

        #[zbus(property)]
        fn set_cached_prop(&self, value: CustomString) -> zbus::Result<()>;

        #[zbus(property(emits_changed_signal = "false"))]
        fn uncached_prop(&self) -> zbus::Result<CustomString>;

        #[zbus(property)]
        fn optional_name(&self) -> zbus::Result<Optional<OwnedUniqueName>>;

        #[zbus(property)]
        fn dynamic_array(&self) -> zbus::Result<Vec<String>>;

        #[zbus(property)]
        fn dynamic_dict(&self) -> zbus::Result<HashMap<String, String>>;
    }

    let service = zbus::connection::Builder::session()
        .serve_at(
            "/org/freedesktop/zbus/SerdePropertyTest",
            Service("before".to_string()),
        )
        .build()
        .await?;
    let client_conn = zbus::Connection::session().await?;
    let client = SerdePropertyTestProxy::builder(&client_conn)
        .destination(service.unique_name().unwrap())
        .build()
        .await?;

    assert_eq!(
        client.cached_prop().await?,
        CustomString("before".to_string())
    );
    assert_eq!(
        client.uncached_prop().await?,
        CustomString("before".to_string())
    );
    assert!(Option::<OwnedUniqueName>::from(client.optional_name().await?).is_none());
    assert_eq!(client.dynamic_array().await?, ["array value"]);
    assert_eq!(client.dynamic_dict().await?["key"], "dict value");

    let mut changes = client.receive_cached_prop_changed().await;
    assert_eq!(
        changes.next().await.unwrap().get().await?,
        CustomString("before".to_string())
    );

    let (changed, ()) = futures_util::try_join!(
        async { changes.next().await.unwrap().get().await },
        client.set_cached_prop(CustomString("after".to_string())),
    )?;
    assert_eq!(changed, CustomString("after".to_string()));
    assert_eq!(
        client.cached_prop().await?,
        CustomString("after".to_string())
    );
    assert_eq!(
        client.uncached_prop().await?,
        CustomString("after".to_string())
    );

    Ok(())
}
