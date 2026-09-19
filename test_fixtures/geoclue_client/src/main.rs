//! The book's GeoClue2 client against the fixture service, for measuring what a client binary
//! weighs with only the zbus features it uses. It asks for a client object, starts it, prints
//! the first location it is told about and exits.

use futures_util::StreamExt;
use zbus::{Connection, ObjectPath, Result, proxy};

const NAME: &str = "org.zbus.GeoClue2Fixture";

#[proxy(
    default_service = "org.zbus.GeoClue2Fixture",
    interface = "org.freedesktop.GeoClue2.Manager",
    default_path = "/org/freedesktop/GeoClue2/Manager"
)]
trait Manager {
    #[zbus(object = "Client")]
    fn get_client(&self);
}

#[proxy(
    default_service = "org.zbus.GeoClue2Fixture",
    interface = "org.freedesktop.GeoClue2.Client"
)]
trait Client {
    fn start(&self) -> Result<()>;
    fn stop(&self) -> Result<()>;

    #[zbus(property)]
    fn set_desktop_id(&mut self, id: &str) -> Result<()>;

    #[zbus(signal)]
    fn location_updated(&self, old: ObjectPath<'_>, new: ObjectPath<'_>) -> Result<()>;
}

#[proxy(
    default_service = "org.zbus.GeoClue2Fixture",
    interface = "org.freedesktop.GeoClue2.Location"
)]
trait Location {
    #[zbus(property)]
    fn latitude(&self) -> Result<f64>;
    #[zbus(property)]
    fn longitude(&self) -> Result<f64>;
}

async fn locate() -> Result<()> {
    let conn = Connection::session().await?;
    let manager = ManagerProxy::new(&conn).await?;
    let mut client = manager.get_client().await?;
    client.set_desktop_id("org.zbus.fixture").await?;
    let mut updates = client.receive_location_updated().await?;
    client.start().await?;
    let update = updates
        .next()
        .await
        .expect("the service announces a location");
    let args = update.args()?;
    let location = LocationProxy::builder(&conn)
        .destination(NAME)
        .path(args.new())
        .build()
        .await?;
    println!(
        "Latitude: {}\nLongitude: {}",
        location.latitude().await?,
        location.longitude().await?,
    );
    client.stop().await
}

#[cfg(feature = "tokio")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    locate().await
}

#[cfg(not(feature = "tokio"))]
fn main() -> Result<()> {
    block_on::run(locate())
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
