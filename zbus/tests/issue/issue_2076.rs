use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer};
use test_log::test;
use tracing::instrument;
use zbus::{Result, Signature, Type, block_on, proxy::CacheProperties};

#[instrument]
#[test]
fn unsized_reference_args() {
    // Reproducer for issue #2076, where an `#[interface]` method taking a reference to an unsized
    // type, such as `&[u8]`, failed to compile. Any slice can be taken by reference now.
    block_on(async {
        #[derive(Default)]
        struct Iface {
            data: Vec<u8>,
            path: PathBuf,
            count: u32,
            values: Vec<u32>,
        }

        #[zbus::interface(name = "org.zbus.test.issue2076")]
        impl Iface {
            async fn store(&mut self, data: &[u8]) {
                self.data = data.to_vec();
            }

            fn describe(
                &self,
                data: &[u8],
                path: &Path,
                name: &str,
                count: &u32,
                values: &[u32],
                words: &[&str],
            ) -> String {
                format!(
                    "{data:?} {} {name} {count} {values:?} {words:?}",
                    path.display()
                )
            }

            #[zbus(property)]
            fn data(&self) -> Vec<u8> {
                self.data.clone()
            }

            #[zbus(property)]
            fn set_data(&mut self, data: &[u8]) {
                self.data = data.to_vec();
            }

            #[zbus(property)]
            fn path(&self) -> PathBuf {
                self.path.clone()
            }

            #[zbus(property)]
            fn set_path(&mut self, path: &Path) {
                self.path = path.to_path_buf();
            }

            #[zbus(property)]
            fn count(&self) -> u32 {
                self.count
            }

            #[zbus(property)]
            fn set_count(&mut self, count: &u32) {
                self.count = *count;
            }

            #[zbus(property)]
            fn values(&self) -> Vec<u32> {
                self.values.clone()
            }

            #[zbus(property)]
            fn set_values(&mut self, values: &[u32]) {
                self.values = values.to_vec();
            }
        }

        #[zbus::proxy(
            default_service = "org.zbus.test.issue2076",
            default_path = "/org/zbus/test/issue2076",
            interface = "org.zbus.test.issue2076"
        )]
        trait Iface {
            fn store(&self, data: &[u8]) -> Result<()>;

            fn describe(
                &self,
                data: &[u8],
                path: &str,
                name: &str,
                count: u32,
                values: &[u32],
                words: &[&str],
            ) -> Result<String>;

            #[zbus(property)]
            fn data(&self) -> Result<Vec<u8>>;

            #[zbus(property)]
            fn set_data(&self, data: &[u8]) -> Result<()>;

            #[zbus(property)]
            fn path(&self) -> Result<String>;

            #[zbus(property)]
            fn set_path(&self, path: &str) -> Result<()>;

            #[zbus(property)]
            fn count(&self) -> Result<u32>;

            #[zbus(property)]
            fn set_count(&self, count: u32) -> Result<()>;

            #[zbus(property)]
            fn values(&self) -> Result<Vec<u32>>;

            #[zbus(property)]
            fn set_values(&self, values: &[u32]) -> Result<()>;
        }

        let conn = zbus::connection::Builder::session()
            .name("org.zbus.test.issue2076")
            .serve_at("/org/zbus/test/issue2076", Iface::default())
            .build()
            .await
            .unwrap();
        let proxy = IfaceProxy::builder(&conn)
            .cache_properties(CacheProperties::No)
            .build()
            .await
            .unwrap();

        proxy.store(&[1, 2, 3]).await.unwrap();
        assert_eq!(proxy.data().await.unwrap(), [1, 2, 3]);

        let description = proxy
            .describe(&[4, 5], "/tmp", "name", 6, &[7, 8], &["a", "b"])
            .await
            .unwrap();
        assert_eq!(description, r#"[4, 5] /tmp name 6 [7, 8] ["a", "b"]"#);

        proxy.set_data(&[7, 8]).await.unwrap();
        assert_eq!(proxy.data().await.unwrap(), [7, 8]);
        proxy.set_path("/tmp").await.unwrap();
        assert_eq!(proxy.path().await.unwrap(), "/tmp");
        proxy.set_count(9).await.unwrap();
        assert_eq!(proxy.count().await.unwrap(), 9);
        proxy.set_values(&[10, 11]).await.unwrap();
        assert_eq!(proxy.values().await.unwrap(), [10, 11]);
    })
}

#[instrument]
#[test]
fn reference_arg_priority() {
    // A method argument is borrowed from the message where it can, as in 5.x, including when only
    // the reference has a D-Bus type. A setter argument, whose value can't lend the bytes of an
    // array, takes an owned value first.
    block_on(async {
        struct Iface;

        #[zbus::interface(name = "org.zbus.test.issue2076.Priority")]
        impl Iface {
            fn method(&self, dual: &Dual) -> String {
                dual.0.to_string()
            }

            fn by_ref_only(&self, value: &RefOnly) -> String {
                value.0.to_string()
            }

            #[zbus(property)]
            fn set_setter(&self, dual: &Dual) -> zbus::fdo::Result<()> {
                Err(zbus::fdo::Error::Failed(dual.0.to_string()))
            }
        }

        #[zbus::proxy(
            default_service = "org.zbus.test.issue2076.Priority",
            default_path = "/org/zbus/test/issue2076/Priority",
            interface = "org.zbus.test.issue2076.Priority"
        )]
        trait Iface {
            fn method(&self, dual: &str) -> Result<String>;

            fn by_ref_only(&self, value: &str) -> Result<String>;

            #[zbus(property)]
            fn set_setter(&self, dual: &str) -> Result<()>;
        }

        let conn = zbus::connection::Builder::session()
            .name("org.zbus.test.issue2076.Priority")
            .serve_at("/org/zbus/test/issue2076/Priority", Iface)
            .build()
            .await
            .unwrap();
        let proxy = IfaceProxy::new(&conn).await.unwrap();

        assert_eq!(proxy.method("").await.unwrap(), "borrowed");
        assert_eq!(proxy.by_ref_only("").await.unwrap(), "borrowed");
        match proxy.set_setter("").await {
            Err(zbus::Error::FDO(e)) => assert_eq!(*e, zbus::fdo::Error::Failed("owned".into())),
            other => panic!("unexpected result: {other:?}"),
        }
    })
}

/// A string with a D-Bus type, and deserializing, only as a reference.
struct RefOnly(&'static str);

impl Type for &RefOnly {
    const SIGNATURE: &'static Signature = &Signature::Str;
}

impl<'de> Deserialize<'de> for &RefOnly {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        <&str>::deserialize(deserializer).map(|_| &RefOnly("borrowed"))
    }
}

/// A string that tells whether it was deserialized as a reference or as an owned value.
struct Dual(&'static str);

impl Type for Dual {
    const SIGNATURE: &'static Signature = &Signature::Str;
}

impl<'de> Deserialize<'de> for Dual {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        <&str>::deserialize(deserializer).map(|_| Dual("owned"))
    }
}

impl<'de> Deserialize<'de> for &Dual {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        <&str>::deserialize(deserializer).map(|_| &Dual("borrowed"))
    }
}
