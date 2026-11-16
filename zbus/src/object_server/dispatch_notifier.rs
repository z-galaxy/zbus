//! The object server API.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use serde::{Deserialize, Serialize};
use zruntime::{Event, EventListener};

use crate::{Connection, Signature, Type, runtime::traits::TaskHandle};

/// A response wrapper that notifies after the response has been sent.
///
/// Sometimes in [`interface`] method implementations we need to do some other work after the
/// response has been sent off. This wrapper type allows us to do that. Instead of returning your
/// intended response type directly, wrap it in this type and return it from your method. The
/// [`ResponseDispatchListener`] returned by the `new` method is notified when the response has
/// been sent.
///
/// A typical use case is sending off signals after the response has been sent. The easiest way to
/// do that is to give the work to [`ResponseDispatchNotifier::followed_by`], which does it once
/// the response has been sent, as part of the method call. You can also spawn a task of your own
/// from the method, which sends the signal only once the [`ResponseDispatchListener`] of
/// [`ResponseDispatchNotifier::new`] has notified it of the response dispatch: such a task is no
/// part of the call, so a hand-over of the connection doesn't wait for it, and what it sends
/// once the connection is handed over fails.
///
/// # Caveats
///
/// The notification indicates that the response has been sent off, not that destination peer has
/// received it. That can only be guaranteed for a peer-to-peer connection.
///
/// [`interface`]: crate::interface
#[derive(Debug)]
pub struct ResponseDispatchNotifier<R> {
    response: R,
    event: Option<Event>,
}

impl<R> ResponseDispatchNotifier<R> {
    /// A new notifier for `response`, along with the listener it notifies.
    pub fn new(response: R) -> (Self, ResponseDispatchListener) {
        let event = Event::new();
        let listener = ResponseDispatchListener(event.listen());
        (
            Self {
                response,
                event: Some(event),
            },
            listener,
        )
    }

    /// A notifier for `response` that has `work` done once the response has been sent, as part
    /// of the method call that it answers.
    ///
    /// This is for a method that leaves work to be done once it returned, such as a signal that
    /// is to follow its reply. Call it from the method, with the connection that the call came in
    /// on, which the `connection` and `signal_emitter` arguments of an [`interface`] method
    /// give: `work` runs in a task of that connection.
    ///
    /// `work` starts once the response has been sent. It also starts where none is: for a call
    /// that expects no reply, and once the notifier is dropped unsent, as when the method fails
    /// after making it.
    ///
    /// The call is under way until `work` is done. So a hand-over of that connection
    /// (`Connection::hand_over`, with the `handover` feature) waits for `work`, as it waits for
    /// the method, and what `work` sends goes out ahead of the hand-over. Work that never ends
    /// keeps the connection from being handed over, as a method that never returns does, and a
    /// hand-over that `work` itself awaits never resolves.
    ///
    /// # Example
    ///
    /// ```
    /// use zbus::{
    ///     interface,
    ///     object_server::{ResponseDispatchNotifier, SignalEmitter},
    /// };
    ///
    /// struct Greeter;
    ///
    /// #[interface(name = "org.zbus.Greeter")]
    /// impl Greeter {
    ///     /// Greets the caller, and then tells everyone whom it greeted.
    ///     fn greet(
    ///         &self,
    ///         name: String,
    ///         #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ///     ) -> ResponseDispatchNotifier<String> {
    ///         let greeting = format!("Hello {name}!");
    ///         let owned = emitter.to_owned();
    ///
    ///         ResponseDispatchNotifier::followed_by(greeting, emitter.connection(), async move {
    ///             // The reply is on its way to the caller by now.
    ///             if let Err(e) = Self::greeted(&owned, &name).await {
    ///                 eprintln!("Failed to emit `Greeted`: {e}");
    ///             }
    ///         })
    ///     }
    ///
    ///     #[zbus(signal)]
    ///     async fn greeted(emitter: &SignalEmitter<'_>, name: &str) -> zbus::Result<()>;
    /// }
    /// ```
    ///
    /// [`interface`]: crate::interface
    pub fn followed_by<F>(response: R, conn: &Connection, work: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (notifier, listener) = Self::new(response);
        // The work counts as a call under way from here on, while that of the method is, so that
        // the object server never looks idle between the two.
        #[cfg(all(target_os = "linux", feature = "handover"))]
        let under_way = conn.follow_up();
        conn.spawn("response follow-up", async move {
            // Naming it has the task keep it until the work is done.
            #[cfg(all(target_os = "linux", feature = "handover"))]
            let _ = &under_way;
            listener.await;
            work.await;
        })
        .detach();

        notifier
    }

    /// Get the response.
    pub fn response(&self) -> &R {
        &self.response
    }
}

impl<R> Serialize for ResponseDispatchNotifier<R>
where
    R: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.response.serialize(serializer)
    }
}

impl<'de, R> Deserialize<'de> for ResponseDispatchNotifier<R>
where
    R: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self {
            response: R::deserialize(deserializer)?,
            event: None,
        })
    }
}

impl<R> Type for ResponseDispatchNotifier<R>
where
    R: Type,
{
    const SIGNATURE: &'static Signature = R::SIGNATURE;
}

impl<T> Drop for ResponseDispatchNotifier<T> {
    fn drop(&mut self) {
        if let Some(event) = self.event.take() {
            event.notify(usize::MAX);
        }
    }
}

/// A future that completes once a [`ResponseDispatchNotifier`]'s response has been sent off.
///
/// It also completes if the notifier is dropped without its response ever being sent, for
/// example because the connection went away before the object server could send it.
///
/// What a task does once this completed is no part of the method call.
/// [`ResponseDispatchNotifier::followed_by`] is for work that is to be part of it.
///
/// Obtain one from [`ResponseDispatchNotifier::new`].
#[derive(Debug)]
#[must_use = "listeners do nothing unless polled"]
pub struct ResponseDispatchListener(EventListener);

impl Future for ResponseDispatchListener {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}
