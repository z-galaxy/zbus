use std::{collections::HashSet, marker::PhantomData, sync::Arc};

use crate::{
    ObjectPath, Str,
    names::{BusName, InterfaceName},
};

use crate::{Connection, Error, Proxy, Result, proxy::ProxyInner};

/// The properties caching mode.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CacheProperties {
    /// Cache properties. The properties will be cached upfront as part of the proxy
    /// creation.
    Yes,
    /// Don't cache properties.
    No,
    /// Cache properties but only populate the cache on the first read of a property (default).
    #[default]
    Lazily,
}

/// Builder for proxies.
///
/// Each setter takes the value it is given, whether that's an already parsed name or a string
/// that still needs parsing, and converts it right away, but it never fails: the first error a
/// setter hits is recorded — a later call doesn't clear it — and reported by [`Builder::build`],
/// so a chain of setters never needs a `?` in between.
#[derive(Debug)]
pub struct Builder<'a, T = ()> {
    conn: Connection,
    destination: Option<BusName<'a>>,
    path: Option<ObjectPath<'a>>,
    interface: Option<InterfaceName<'a>>,
    proxy_type: PhantomData<T>,
    cache: CacheProperties,
    uncached_properties: Option<HashSet<Str<'a>>>,
    error: Option<Error>,
}

impl<T> Clone for Builder<'_, T> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            destination: self.destination.clone(),
            path: self.path.clone(),
            interface: self.interface.clone(),
            cache: self.cache,
            uncached_properties: self.uncached_properties.clone(),
            proxy_type: PhantomData,
            error: self.error.clone(),
        }
    }
}

impl<'a, T> Builder<'a, T> {
    /// Set the proxy destination address.
    ///
    /// An invalid destination is reported by [`Builder::build`].
    #[must_use]
    pub fn destination<D>(mut self, destination: D) -> Self
    where
        D: TryInto<BusName<'a>>,
        D::Error: Into<Error>,
    {
        match destination.try_into() {
            Ok(destination) => self.destination = Some(destination),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Set the proxy path.
    ///
    /// An invalid path is reported by [`Builder::build`].
    #[must_use]
    pub fn path<P>(mut self, path: P) -> Self
    where
        P: TryInto<ObjectPath<'a>>,
        P::Error: Into<Error>,
    {
        match path.try_into() {
            Ok(path) => self.path = Some(path),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Set the proxy interface.
    ///
    /// An invalid interface name is reported by [`Builder::build`].
    #[must_use]
    pub fn interface<I>(mut self, interface: I) -> Self
    where
        I: TryInto<InterfaceName<'a>>,
        I::Error: Into<Error>,
    {
        match interface.try_into() {
            Ok(interface) => self.interface = Some(interface),
            Err(e) => self.record(e.into()),
        }

        self
    }

    /// Set the properties caching mode.
    #[must_use]
    pub fn cache_properties(mut self, cache: CacheProperties) -> Self {
        self.cache = cache;
        self
    }

    /// Specify a set of properties (by name) which should be excluded from caching.
    #[must_use]
    pub fn uncached_properties(mut self, properties: &[&'a str]) -> Self {
        self.uncached_properties
            .replace(properties.iter().map(|p| Str::from(*p)).collect());

        self
    }

    pub(crate) fn build_internal(self) -> Result<Proxy<'a>> {
        // The error a setter recorded comes first, then the missing pieces.
        if let Some(error) = self.error {
            return Err(error);
        }

        let conn = self.conn;
        let destination = self
            .destination
            .ok_or(Error::MissingParameter("destination"))?;
        let path = self.path.ok_or(Error::MissingParameter("path"))?;
        let interface = self.interface.ok_or(Error::MissingParameter("interface"))?;
        let cache = self.cache;
        let uncached_properties = self.uncached_properties.unwrap_or_default();

        Ok(Proxy {
            inner: Arc::new(ProxyInner::new(
                conn,
                destination,
                path,
                interface,
                cache,
                uncached_properties,
            )),
        })
    }

    /// Build a proxy from the builder.
    ///
    /// # Errors
    ///
    /// The first error recorded by a setter, then [`Error::MissingParameter`] if the builder is
    /// lacking the necessary parameters to build a proxy.
    pub async fn build(self) -> Result<T>
    where
        T: From<Proxy<'a>> + super::Defaults,
    {
        // The constant lets the whole properties cache be dropped from a program whose proxies
        // have no properties.
        let cache_upfront = T::HAS_PROPERTIES && self.cache == CacheProperties::Yes;
        let proxy = self.build_internal()?;

        if cache_upfront {
            proxy
                .get_property_cache()
                .expect("properties cache not initialized")
                .ready()
                .await?;
        }

        Ok(proxy.into())
    }

    /// Record `error`, unless an earlier setter already recorded one.
    fn record(&mut self, error: Error) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
}

impl<T> Builder<'_, T>
where
    T: super::Defaults,
{
    /// Create a new [`Builder`] for the given connection.
    #[must_use]
    pub fn new(conn: &Connection) -> Self {
        Self {
            conn: conn.clone(),
            destination: T::DESTINATION.clone(),
            path: T::PATH.clone(),
            interface: T::INTERFACE.clone(),
            cache: CacheProperties::default(),
            uncached_properties: None,
            proxy_type: PhantomData,
            error: None,
        }
    }
}

// The test here talks to the session bus, which needs a backend to connect to.
#[cfg(all(test, any(feature = "builtin-runtime", feature = "tokio")))]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    #[ntest::timeout(15000)]
    fn builder() {
        crate::utils::block_on(builder_async());
    }

    async fn builder_async() {
        let conn = Connection::session().await.unwrap();

        // Strings are converted for us.
        let builder = Builder::<Proxy<'_>>::new(&conn)
            .destination("org.freedesktop.DBus")
            .path("/some/path")
            .interface("org.freedesktop.Interface")
            .cache_properties(CacheProperties::No);
        assert!(matches!(
            builder.clone().destination.unwrap(),
            BusName::Unique(_),
        ));
        let proxy = builder.build().await.unwrap();
        assert!(matches!(proxy.inner.destination, BusName::Unique(_)));

        // As are already parsed values, without any error handling on the way.
        let destination = BusName::try_from("org.freedesktop.DBus").unwrap();
        let path = ObjectPath::try_from("/some/path").unwrap();
        let interface = InterfaceName::try_from("org.freedesktop.Interface").unwrap();
        let proxy = Builder::<Proxy<'_>>::new(&conn)
            .destination(&destination)
            .path(path.clone())
            .interface(interface.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(proxy.inner.path, path);
        assert_eq!(proxy.inner.interface, interface);

        // An invalid value is only reported by `build`.
        let err = Builder::<Proxy<'_>>::new(&conn)
            .destination("not a bus name")
            .path("/some/path")
            .interface("org.freedesktop.Interface")
            .build()
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            BusName::try_from("not a bus name").unwrap_err().to_string(),
        );

        // Setting the field again, even to a valid value, doesn't clear the error it recorded.
        let err = Builder::<Proxy<'_>>::new(&conn)
            .destination("not a bus name")
            .destination("org.freedesktop.DBus")
            .path("/some/path")
            .interface("org.freedesktop.Interface")
            .build()
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            BusName::try_from("not a bus name").unwrap_err().to_string(),
        );
    }
}
