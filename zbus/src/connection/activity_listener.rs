use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use zruntime::EventListener;

/// A future that completes on the next activity on a connection.
///
/// The connection notifies activity in three places, each as an operation starts rather than once
/// it has succeeded:
///
/// * [`Connection::send`], [`Connection::send_all`] and [`Connection::flush`] notify on entry,
///   before they wait for their turn to write and before anything is written. A send that fails
///   with [`Error::Unsupported`] before it sends anything, and a `send_all` given no messages,
///   return at once without notifying.
/// * The connection's reader notifies each time it starts to read a message: once as it starts, and
///   again after each message it has received and handed on. It notifies no more once a read fails.
/// * [`Connection::close`] notifies on entry, before it closes anything.
///
/// So a listener that completes does not mean that a message went out or came in: the send may
/// still fail, and the read may wait for a message that never comes. Nor does a listener that
/// stays pending mean that nothing is under way: a send or a read that started before it was
/// created may still be going on. Activity from before the listener was created does not count
/// towards it.
///
/// This is meant for building an idle timeout on top of a connection: obtain a listener from
/// [`Connection::monitor_activity`] and race it against a timer of your own. The listener
/// completing first means the connection was used, so start both over. The timer completing first
/// means that no send, read or close has started for that long, not that none is still under way.
///
/// [`Connection::close`]: crate::Connection::close
/// [`Connection::flush`]: crate::Connection::flush
/// [`Connection::monitor_activity`]: crate::Connection::monitor_activity
/// [`Connection::send`]: crate::Connection::send
/// [`Connection::send_all`]: crate::Connection::send_all
/// [`Error::Unsupported`]: crate::Error::Unsupported
#[derive(Debug)]
#[must_use = "listeners do nothing unless polled"]
pub struct ActivityListener(pub(crate) EventListener);

impl Future for ActivityListener {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}
