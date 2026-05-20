//! Test overflow configuration on `Connection::Builder`.
//!
//! Verifies that `overflow(true)` prevents the socket reader task from blocking when a
//! subscription's broadcast channel is full, which would freeze the whole connection.

#![cfg(all(feature = "comms", feature = "service", feature = "default-rt"))]

use std::{
    sync::mpsc::{self, RecvTimeoutError},
    time::Duration,
};

use futures_util::{StreamExt, future::Either, pin_mut};
use ntest::timeout;
use test_log::test;
use zbus::{Connection, block_on, connection, interface, object_server::InterfaceRef};

struct OverflowTestIface;

#[interface(name = "org.zbus.test.Overflow")]
impl OverflowTestIface {
    #[zbus(signal)]
    async fn state_changed(
        emitter: &zbus::object_server::SignalEmitter<'_>,
        state: &str,
    ) -> zbus::Result<()>;
}

#[zbus::proxy(interface = "org.zbus.test.Overflow", assume_defaults = true)]
trait OverflowTest {
    #[zbus(signal)]
    async fn state_changed(&self, state: &str) -> zbus::Result<()>;
}

async fn setup_server() -> zbus::Result<(Connection, InterfaceRef<OverflowTestIface>)> {
    let conn = connection::Builder::session()
        .serve_at("/org/zbus/test/Overflow", OverflowTestIface)
        .name("org.zbus.test.Overflow")
        .build()
        .await?;

    let iface_ref = conn
        .object_server()
        .interface::<_, OverflowTestIface>("/org/zbus/test/Overflow")
        .await?;

    Ok((conn, iface_ref))
}

/// Emit signals from a separate thread (server-side) so the client side is the only one
/// running on this thread's runtime.
fn emit_signals(iface_ref: InterfaceRef<OverflowTestIface>, count: u32) {
    std::thread::spawn(move || {
        block_on(async move {
            for i in 0..count {
                let emitter = iface_ref.signal_emitter();
                let _ = OverflowTestIface::state_changed(emitter, &format!("State{}", i)).await;
            }
        });
    });
}

fn emit_one_signal(iface_ref: InterfaceRef<OverflowTestIface>, state: &str) {
    let state = state.to_string();
    std::thread::spawn(move || {
        block_on(async move {
            let emitter = iface_ref.signal_emitter();
            let _ = OverflowTestIface::state_changed(emitter, &state).await;
        });
    });
}

/// Basic sanity: a signal can be received without overflow.
#[test]
#[timeout(15000)]
fn basic_signal_reception() {
    let (server_conn, iface_ref) = block_on(async { setup_server().await.unwrap() });
    let server_name = server_conn.unique_name().expect("unique name").clone();

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        block_on(async move {
            let client_conn = connection::Builder::session().build().await.unwrap();
            let proxy = OverflowTestProxy::builder(&client_conn)
                .destination(server_name)
                .path("/org/zbus/test/Overflow")
                .build()
                .await
                .unwrap();

            let mut stream = proxy.receive_state_changed().await.unwrap();

            emit_one_signal(iface_ref, "Hello");

            let received = stream.next().await.is_some();
            tx.send(received).unwrap();
        });
    });

    let received = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("timed out waiting for the signal");
    assert!(received, "Should receive the signal");

    drop(server_conn);
}

/// With `overflow=false` (default), the connection freezes when a subscription's buffer is
/// full: subscribing to a signal again hangs because the socket reader can no longer read
/// the AddMatch response.
#[test]
#[timeout(30000)]
fn overflow_false_freezes_connection() {
    let (server_conn, iface_ref) = block_on(async { setup_server().await.unwrap() });
    let server_name = server_conn.unique_name().expect("unique name").clone();

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        block_on(async move {
            let client_conn = connection::Builder::session().build().await.unwrap();
            let proxy = OverflowTestProxy::builder(&client_conn)
                .destination(server_name)
                .path("/org/zbus/test/Overflow")
                .build()
                .await
                .unwrap();

            // Subscribe to the signal but do NOT consume the stream.
            let _stream_a = proxy.receive_state_changed().await.unwrap();

            // Fill the 64-slot default buffer.
            emit_signals(iface_ref.clone(), 65);

            async_io::Timer::after(Duration::from_millis(500)).await;

            // Try to subscribe again: this hangs when the connection is frozen.
            let mut stream_b = match proxy.receive_state_changed().await {
                Ok(stream_b) => stream_b,
                // Subscription failed, nothing more to report.
                Err(_) => return,
            };

            emit_one_signal(iface_ref, "ShouldNotArrive");

            let timer = async_io::Timer::after(Duration::from_secs(2));
            let signal = stream_b.next();
            pin_mut!(timer);
            pin_mut!(signal);

            let received = match futures_util::future::select(timer, signal).await {
                Either::Left((_, _)) => false,
                Either::Right((msg, _)) => msg.is_some(),
            };
            let _ = tx.send(!received);
        });
    });

    // Receiving a report means the connection kept working: with overflow disabled, it must
    // freeze instead, i-e the report never arrives.
    match rx.recv_timeout(Duration::from_secs(15)) {
        Err(RecvTimeoutError::Timeout) => (),
        Err(e) => panic!("client thread failed unexpectedly: {e}"),
        Ok(_) => panic!("Connection should be frozen with overflow=false"),
    }

    drop(server_conn);
}

/// With `overflow=true`, subscribing to a signal still works after another subscription's
/// buffer is full: the connection is not frozen.
#[test]
#[timeout(30000)]
fn overflow_true_allows_new_subscriptions() {
    let (server_conn, iface_ref) = block_on(async { setup_server().await.unwrap() });
    let server_name = server_conn.unique_name().expect("unique name").clone();

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        block_on(async move {
            let client_conn = connection::Builder::session()
                .overflow(true)
                .build()
                .await
                .unwrap();
            let proxy = OverflowTestProxy::builder(&client_conn)
                .destination(server_name)
                .path("/org/zbus/test/Overflow")
                .build()
                .await
                .unwrap();

            // Subscribe to the signal but do NOT consume the stream.
            let _stream_a = proxy.receive_state_changed().await.unwrap();

            // Exceed the 64-slot default buffer.
            emit_signals(iface_ref.clone(), 70);

            async_io::Timer::after(Duration::from_millis(500)).await;

            // The connection is not frozen: subscribing again works.
            let mut stream_b = proxy.receive_state_changed().await.unwrap();

            emit_one_signal(iface_ref, "FreshState");

            let timer = async_io::Timer::after(Duration::from_secs(2));
            let signal = stream_b.next();
            pin_mut!(timer);
            pin_mut!(signal);

            let received = match futures_util::future::select(timer, signal).await {
                Either::Left((_, _)) => false,
                Either::Right((msg, _)) => msg.is_some(),
            };
            tx.send(received).unwrap();
        });
    });

    let received = rx
        .recv_timeout(Duration::from_secs(15))
        .expect("timed out: connection should not be frozen with overflow=true");
    assert!(
        received,
        "Connection should not be frozen with overflow=true"
    );

    drop(server_conn);
}

/// With `overflow=true`, a full channel drops the oldest messages: after emitting 100
/// signals into a 64-slot buffer, the receiver gets the most recent ~64.
#[test]
#[timeout(30000)]
fn overflow_true_drops_oldest_messages() {
    let (server_conn, iface_ref) = block_on(async { setup_server().await.unwrap() });
    let server_name = server_conn.unique_name().expect("unique name").clone();

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        block_on(async move {
            let client_conn = connection::Builder::session()
                .overflow(true)
                .build()
                .await
                .unwrap();
            let proxy = OverflowTestProxy::builder(&client_conn)
                .destination(server_name)
                .path("/org/zbus/test/Overflow")
                .build()
                .await
                .unwrap();

            let mut stream = proxy.receive_state_changed().await.unwrap();

            // Emit 100 signals with distinct values.
            emit_signals(iface_ref, 100);

            async_io::Timer::after(Duration::from_millis(500)).await;

            // Collect whatever arrives within a short window per message.
            let mut count = 0u32;
            let mut first_state = String::new();
            let mut last_state = String::new();

            loop {
                let timer = async_io::Timer::after(Duration::from_millis(100));
                let signal = stream.next();
                pin_mut!(timer);
                pin_mut!(signal);

                match futures_util::future::select(timer, signal).await {
                    Either::Left((_, _)) => break,
                    Either::Right((Some(signal), _)) => {
                        let args = signal.args().unwrap();
                        if count == 0 {
                            first_state = args.state.to_string();
                        }
                        last_state = args.state.to_string();
                        count += 1;
                    }
                    Either::Right((None, _)) => break,
                }
            }

            tx.send((count, first_state, last_state)).unwrap();
        });
    });

    let (count, first_state, last_state) = rx
        .recv_timeout(Duration::from_secs(15))
        .expect("timed out draining signals");

    assert!(
        count >= 60,
        "Should receive approximately 64 messages (buffer size), got {count}"
    );
    assert!(
        !first_state.contains("State0"),
        "First received should NOT be State0 — oldest messages should be dropped"
    );
    assert!(
        last_state.contains("State9"),
        "Last received should be near State99 — newest messages should be kept, got {last_state}"
    );

    drop(server_conn);
}
