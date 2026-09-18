//! The async locks a connection holds.
//!
//! These belong to no particular runtime: any of them can be taken from a future polled
//! anywhere, so they are zbus's own, on `event-listener`, and Tokio's stand in where the
//! `tokio` feature is on so that a Tokio build pulls in no second implementation. Only the
//! object server takes readers-writer locks, so those come along with the `service` feature.

#[cfg(not(feature = "tokio"))]
mod mutex;
#[cfg(not(feature = "tokio"))]
pub(crate) use mutex::Mutex;
#[cfg(all(not(feature = "tokio"), feature = "service"))]
mod rwlock;
#[cfg(all(not(feature = "tokio"), feature = "service"))]
pub(crate) use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "tokio")]
pub(crate) use tokio::sync::Mutex;
#[cfg(all(feature = "tokio", feature = "service"))]
pub(crate) use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(all(test, not(feature = "tokio")))]
mod tests {
    use std::{
        future::Future,
        pin::pin,
        sync::Arc,
        task::{Context, Poll, Waker},
    };

    use super::*;

    fn poll_once<F>(future: &mut F) -> Poll<F::Output>
    where
        F: Future + Unpin,
    {
        pin!(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn a_mutex_admits_one_holder_at_a_time() {
        let mutex = Mutex::new(0);
        let guard = zbus_block_on(mutex.lock());
        let mut second = Box::pin(mutex.lock());
        assert!(poll_once(&mut second).is_pending());
        drop(guard);
        assert!(poll_once(&mut second).is_ready());
    }

    #[test]
    fn a_dropped_lock_future_does_not_strand_the_next_waiter() {
        let mutex = Mutex::new(());
        let guard = zbus_block_on(mutex.lock());
        let mut first = Box::pin(mutex.lock());
        assert!(poll_once(&mut first).is_pending());
        let mut second = Box::pin(mutex.lock());
        assert!(poll_once(&mut second).is_pending());
        // The release notifies `first`, which is then dropped without ever polling that
        // notification: it has to reach `second` instead.
        drop(guard);
        drop(first);
        assert!(poll_once(&mut second).is_ready());
    }

    #[test]
    fn a_mutex_is_shared_across_threads() {
        let mutex = Arc::new(Mutex::new(0u32));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let mutex = mutex.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        *zbus_block_on(mutex.lock()) += 1;
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(*zbus_block_on(mutex.lock()), 8000);
    }

    #[cfg(feature = "service")]
    #[test]
    fn readers_share_and_a_writer_excludes() {
        let lock = RwLock::new(1);
        let first = zbus_block_on(lock.read());
        let second = zbus_block_on(lock.read());
        let mut writer = Box::pin(lock.write());
        assert!(poll_once(&mut writer).is_pending());
        drop(first);
        drop(second);
        let Poll::Ready(mut guard) = poll_once(&mut writer) else {
            panic!("the last reader lets the writer in");
        };
        *guard = 2;
        let mut reader = Box::pin(lock.read());
        assert!(poll_once(&mut reader).is_pending());
        drop(guard);
        let Poll::Ready(guard) = poll_once(&mut reader) else {
            panic!("the writer's release lets readers in");
        };
        assert_eq!(*guard, 2);
    }

    #[cfg(feature = "service")]
    #[test]
    fn a_waiting_writer_holds_new_readers_back() {
        let lock = RwLock::new(());
        let reader = zbus_block_on(lock.read());
        let mut writer = Box::pin(lock.write());
        assert!(poll_once(&mut writer).is_pending());
        let mut late_reader = Box::pin(lock.read());
        assert!(poll_once(&mut late_reader).is_pending());
        drop(reader);
        assert!(poll_once(&mut writer).is_ready());
    }

    #[cfg(feature = "service")]
    #[test]
    fn a_dropped_write_future_does_not_strand_the_next_writer() {
        let lock = RwLock::new(());
        let guard = zbus_block_on(lock.write());
        let mut first = Box::pin(lock.write());
        assert!(poll_once(&mut first).is_pending());
        let mut second = Box::pin(lock.write());
        assert!(poll_once(&mut second).is_pending());
        // The release notifies `first`, which is then dropped without ever polling that
        // notification: it has to reach `second` instead.
        drop(guard);
        drop(first);
        assert!(poll_once(&mut second).is_ready());
    }

    #[cfg(feature = "service")]
    #[test]
    fn a_cancelled_writer_lets_readers_in_again() {
        let lock = RwLock::new(());
        let reader = zbus_block_on(lock.read());
        let mut writer = Box::pin(lock.write());
        assert!(poll_once(&mut writer).is_pending());
        drop(writer);
        assert!(poll_once(&mut Box::pin(lock.read())).is_ready());
        drop(reader);
    }

    #[cfg(feature = "service")]
    #[test]
    fn a_rwlock_coerces_to_an_unsized_value() {
        let lock: Arc<RwLock<dyn std::fmt::Debug + Send + Sync>> = Arc::new(RwLock::new(5u8));
        let guard = zbus_block_on(lock.read());
        assert_eq!(format!("{:?}", &*guard), "5");
    }

    fn zbus_block_on<F>(future: F) -> F::Output
    where
        F: Future,
    {
        futures_lite::future::block_on(future)
    }
}
