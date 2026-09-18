//! A service shaped like GeoClue2's `Manager`, `Client` and `Location` objects, for measuring
//! what a service binary weighs with only the zbus features it uses. `GetClient` hands out one
//! client object; `Start` on it publishes one location and announces it.

use zbus::{
    ObjectPath, OwnedObjectPath, connection::Builder, interface, object_server::SignalEmitter,
};

const NAME: &str = "org.zbus.GeoClue2Fixture";
const CLIENT: &str = "/org/freedesktop/GeoClue2/Client/1";
const LOCATION: &str = "/org/freedesktop/GeoClue2/Location/1";

struct Manager;

#[interface(name = "org.freedesktop.GeoClue2.Manager")]
impl Manager {
    fn get_client(&self) -> OwnedObjectPath {
        ObjectPath::from_static_str_unchecked(CLIENT).into()
    }
}

struct Client {
    desktop_id: String,
}

#[interface(name = "org.freedesktop.GeoClue2.Client")]
impl Client {
    async fn start(
        &self,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> zbus::fdo::Result<()> {
        let none = ObjectPath::from_static_str_unchecked("/");
        let location = ObjectPath::from_static_str_unchecked(LOCATION);
        Self::location_updated(&emitter, none, location).await?;
        Ok(())
    }

    fn stop(&self) {}

    #[zbus(property)]
    fn desktop_id(&self) -> &str {
        &self.desktop_id
    }

    #[zbus(property)]
    fn set_desktop_id(&mut self, id: String) {
        self.desktop_id = id;
    }

    #[zbus(signal)]
    async fn location_updated(
        emitter: &SignalEmitter<'_>,
        old: ObjectPath<'_>,
        new: ObjectPath<'_>,
    ) -> zbus::Result<()>;
}

struct Location;

#[interface(name = "org.freedesktop.GeoClue2.Location")]
impl Location {
    #[zbus(property)]
    fn latitude(&self) -> f64 {
        59.3293
    }

    #[zbus(property)]
    fn longitude(&self) -> f64 {
        18.0686
    }
}

async fn serve() -> zbus::Result<()> {
    let _conn = Builder::session()
        .name(NAME)
        .serve_at("/org/freedesktop/GeoClue2/Manager", Manager)
        .serve_at(
            CLIENT,
            Client {
                desktop_id: String::new(),
            },
        )
        .serve_at(LOCATION, Location)
        .build()
        .await?;
    std::future::pending().await
}

#[cfg(feature = "tokio")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> zbus::Result<()> {
    serve().await
}

#[cfg(not(feature = "tokio"))]
fn main() -> zbus::Result<()> {
    block_on::run(serve())
}

#[cfg(not(feature = "tokio"))]
mod block_on {
    //! Drives one future on the calling thread; the connection's own runtime does the rest.

    use std::{
        future::Future,
        pin::pin,
        sync::Arc,
        task::{Context, Poll, Wake},
        thread::{self, Thread},
    };

    struct Unpark(Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    pub(super) fn run<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let mut future = pin!(future);
        let waker = Arc::new(Unpark(thread::current())).into();
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
            thread::park();
        }
    }
}
