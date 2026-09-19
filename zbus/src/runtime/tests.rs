//! Tests for the runtime a connection runs on, and for the external path in particular.

use std::sync::Arc;

use ntest::timeout;

use super::{
    Runtime,
    test_runtime::{DefaultBlocking, TestRuntime, under_every_runtime},
};

#[cfg(all(feature = "p2p", feature = "service"))]
#[test]
#[timeout(15000)]
fn external_runtime_drives_a_channel_connection() {
    use std::time::Duration;

    use crate::{
        connection::{Builder, socket::Channel},
        interface,
    };

    struct Echo;

    #[interface(name = "org.zbus.ExternalRuntime")]
    impl Echo {
        fn echo(&self, message: String) -> String {
            message
        }
    }

    futures_lite::future::block_on(async {
        let guid = crate::Guid::generate();
        let (c1, c2) = Channel::pair();
        // Serving an interface makes the build wait for the object server to be listening, so
        // the call below cannot race it.
        let server = Builder::authenticated_socket(c1, guid.clone())
            .p2p()
            .runtime(TestRuntime::new())
            .serve_at("/org/zbus/Test", Echo)
            .build()
            .await
            .unwrap();
        let client = Builder::authenticated_socket(c2, guid)
            .p2p()
            .runtime(TestRuntime::new())
            .method_timeout(Duration::from_secs(10))
            .build()
            .await
            .unwrap();
        assert!(matches!(server.runtime(), Runtime::External(_)));

        // A method call round trip proves the reader task and the timeout both run on the
        // external runtime.
        let reply = client
            .call_method(
                None::<()>,
                "/org/zbus/Test",
                Some("org.zbus.ExternalRuntime"),
                "Echo",
                &("hello"),
            )
            .await
            .unwrap();
        assert_eq!(reply.body().deserialize::<String>().unwrap(), "hello");

        // The client is gone before the server; shutting the server down must not hang.
        drop(client);
        server.graceful_shutdown().await;
    });
}

/// An explicit runtime wins over the Tokio runtime a connection would otherwise detect.
#[cfg(all(unix, feature = "builtin-runtime", feature = "tokio", feature = "p2p"))]
#[test]
#[timeout(15000)]
fn an_explicit_runtime_beats_tokio_detection() {
    use std::os::unix::net::UnixStream;

    use crate::connection::Builder;

    let (p0, p1) = UnixStream::pair().unwrap();
    let guid = crate::Guid::generate();

    let tokio = tokio::runtime::Runtime::new().unwrap();
    let (server, _peer) = tokio.block_on(async {
        futures_util::try_join!(
            Builder::unix_stream(p0)
                .server(guid)
                .p2p()
                .runtime(TestRuntime::new())
                .build(),
            Builder::unix_stream(p1).p2p().build(),
        )
        .unwrap()
    });

    // A connection built with no runtime of its own latches the Tokio runtime it is built in,
    // as the peer above did.
    assert!(matches!(server.runtime(), Runtime::External(_)));
}

/// A connection on Tokio keeps the runtime it was built on, wherever it is polled.
#[cfg(all(feature = "tokio", feature = "p2p", feature = "service"))]
#[test]
#[timeout(15000)]
fn a_tokio_connection_keeps_working_outside_the_runtime_context() {
    use std::{thread, time::Duration};

    use crate::{
        connection::{Builder, socket::Channel},
        interface,
    };

    struct Greeter;

    #[interface(name = "org.zbus.TokioRuntime")]
    impl Greeter {
        fn greet(&self) -> String {
            "hello".to_string()
        }
    }

    let tokio = tokio::runtime::Runtime::new().unwrap();
    let guid = crate::Guid::generate();
    let (c1, c2) = Channel::pair();
    let (_server, client) = tokio.block_on(async {
        futures_util::try_join!(
            Builder::authenticated_socket(c1, guid.clone())
                .p2p()
                .serve_at("/org/zbus/Test", Greeter)
                .build(),
            Builder::authenticated_socket(c2, guid)
                .p2p()
                .method_timeout(Duration::from_secs(10))
                .build(),
        )
        .unwrap()
    });
    assert!(matches!(client.runtime(), Runtime::Tokio(_)));

    // A thread of its own has no Tokio runtime to find, so both the tasks this call spawns and
    // the timer it arms have to go to the runtime the connection was built on.
    let reply = thread::spawn(move || {
        futures_lite::future::block_on(client.call_method(
            None::<()>,
            "/org/zbus/Test",
            Some("org.zbus.TokioRuntime"),
            "Greet",
            &(),
        ))
    })
    .join()
    .expect("the calling thread did not panic")
    .unwrap();

    assert_eq!(reply.body().deserialize::<String>().unwrap(), "hello");
}

/// The default blocking hook runs the work on a thread that exits with it.
#[test]
#[timeout(15000)]
fn the_default_blocking_hook_runs_the_work_on_a_short_lived_thread() {
    let runtime = Runtime::External(Arc::new(DefaultBlocking::new()));
    // Other tests are free to run blocking work of their own alongside this one, so the count
    // this one has to come back to is the one it started from.
    #[cfg(target_os = "linux")]
    let before = blocking_threads();

    let name = futures_lite::future::block_on(
        runtime.spawn_blocking(|| std::thread::current().name().map(String::from)),
    );

    assert_eq!(name.as_deref(), Some("zbus blocking work"));
    // The thread is the hook's own, so it is gone once the work is done.
    #[cfg(target_os = "linux")]
    while blocking_threads() > before {
        std::thread::yield_now();
    }
}

/// Blocking work runs even when the future for it is let go of before it is ever polled.
///
/// A runtime is free to queue the work rather than start it at once, and letting go of the
/// future must not take it back out of that queue: work that owns something, such as a process
/// to reap, would leave it behind for good.
#[test]
#[timeout(15000)]
fn blocking_work_runs_even_when_its_future_is_dropped() {
    use std::sync::atomic::{AtomicBool, Ordering};

    under_every_runtime(|runtime| async move {
        let ran = Arc::new(AtomicBool::new(false));
        let setting = ran.clone();

        drop(runtime.spawn_blocking(move || setting.store(true, Ordering::SeqCst)));

        // The work runs wherever the runtime puts it, so the flag is what says it has, and
        // nothing here is left to await.
        while !ran.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
    });
}

/// The threads of this process that are running work of the default blocking hook.
///
/// Linux truncates a thread's name to fifteen bytes, leaving only the start of it to match on.
#[cfg(target_os = "linux")]
pub(super) fn blocking_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("a process on Linux can list its own threads")
        .filter_map(Result::ok)
        .filter(|thread| {
            std::fs::read_to_string(thread.path().join("comm"))
                .is_ok_and(|comm| comm.trim_end().starts_with("zbus blocking"))
        })
        .count()
}

/// The socket an address names is created and connected on the connection's own runtime.
#[cfg(unix)]
#[test]
#[timeout(15000)]
fn an_address_is_connected_on_the_runtime_it_is_given() {
    use crate::{Error, connection::Builder};

    let error = futures_lite::future::block_on(
        Builder::address("unix:path=/nonexistent")
            .runtime(TestRuntime::new())
            .build(),
    )
    .unwrap_err();

    // `NotFound` means the connect reached the socket path, so the runtime really did open a
    // socket.
    assert!(
        matches!(&error, Error::Connection(e, _) if e.kind() == std::io::ErrorKind::NotFound),
        "got {error:?}",
    );
}

/// Without a backend, a connection can only run on a runtime the caller supplies.
#[cfg(not(any(feature = "builtin-runtime", feature = "tokio")))]
#[test]
#[timeout(15000)]
fn a_connection_without_a_runtime_is_unsupported() {
    use crate::{Error, connection::Builder};

    let error = futures_lite::future::block_on(
        Builder::address("unix:path=/tmp/zbus-external-only").build(),
    )
    .unwrap_err();

    assert!(matches!(error, Error::Unsupported), "got {error:?}");
}

#[test]
#[timeout(15000)]
fn an_external_task_runs_to_completion() {
    let runtime = Runtime::External(Arc::new(TestRuntime::new()));
    let (sender, receiver) = std::sync::mpsc::channel();
    let task = runtime.spawn("an external task", async move {
        sender.send(7).expect("the receiver is still alive");
    });

    futures_lite::future::block_on(task).unwrap();
    assert_eq!(receiver.recv().unwrap(), 7);
}

#[test]
#[timeout(15000)]
fn an_external_task_hands_its_output_back() {
    let runtime = Runtime::External(Arc::new(TestRuntime::new()));
    let task = runtime.spawn("an external task with an output", async { 42 });

    assert_eq!(futures_lite::future::block_on(task).unwrap(), 42);
}

/// Dropping the handle to a task stops the task.
#[test]
#[timeout(15000)]
fn dropping_an_external_task_cancels_it() {
    let test_runtime = TestRuntime::new();
    let runtime = Runtime::External(Arc::new(test_runtime.clone()));
    let task = runtime.spawn("an idle external task", std::future::pending::<u8>());
    assert!(!test_runtime.is_empty());

    drop(task);
    // The executor drops a cancelled task on the tick that observes the cancellation, so give it
    // the chance to get there.
    futures_lite::future::block_on(async {
        while !test_runtime.is_empty() {
            async_io::Timer::after(std::time::Duration::from_millis(1)).await;
        }
    });
}

/// The `External` arm of [`Runtime::timeout`] runs on the runtime's own timer.
#[test]
#[timeout(15000)]
fn an_external_timeout_expires_on_the_runtime_timer() {
    use std::{io::ErrorKind, time::Duration};

    let runtime = Runtime::External(Arc::new(TestRuntime::new()));

    let error = futures_lite::future::block_on(runtime.timeout(
        std::future::pending::<crate::Result<()>>(),
        Duration::from_millis(10),
    ))
    .unwrap_err();

    assert!(
        matches!(&error, crate::Error::InputOutput(e) if e.kind() == ErrorKind::TimedOut),
        "got {error:?}",
    );
}

#[test]
#[timeout(15000)]
fn a_detached_external_task_runs_to_completion() {
    let runtime = Runtime::External(Arc::new(TestRuntime::new()));
    let (sender, receiver) = std::sync::mpsc::channel();
    // The task only finishes after its handle is gone, so a cancelling `detach` would show up as
    // a receiver that never hears back.
    let handle_dropped = event_listener::Event::new();
    let listener = handle_dropped.listen();

    runtime
        .spawn("a detached external task", async move {
            listener.await;
            sender.send(()).expect("the receiver is still alive");
        })
        .detach();
    handle_dropped.notify(1);

    receiver.recv().expect("the detached task sent nothing");
}

/// A task spawned through the erased mirror hands its output back to the typed handle.
#[test]
#[timeout(15000)]
fn an_erased_task_hands_its_output_back() {
    use super::{erased::ErasedRuntime, traits};

    let runtime: Arc<dyn ErasedRuntime> = Arc::new(TestRuntime::new());
    let task = traits::Runtime::spawn(&runtime, "an erased task", async { 42 });

    assert_eq!(futures_lite::future::block_on(task).unwrap(), 42);
}
