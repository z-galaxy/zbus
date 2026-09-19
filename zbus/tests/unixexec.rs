// The connection here is built without a runtime of its own, which only a build with a backend
// can do.
#![cfg(all(
    feature = "unixexec",
    not(target_os = "windows"),
    any(feature = "builtin-runtime", feature = "tokio")
))]

use ntest::timeout;
use test_log::test;

use zbus::{Result, block_on, conn::Builder};

#[test]
#[timeout(15000)]
fn unixexec_connection_async() {
    block_on(test_unixexec_connection()).unwrap();
}

/// A bus connection over the standard I/O of `systemd-stdio-bridge`.
///
/// A machine without that program has nothing to say about this, so the test passes there.
async fn test_unixexec_connection() -> Result<()> {
    let connection = match Builder::address("unixexec:path=systemd-stdio-bridge")
        .build()
        .await
    {
        Ok(connection) => connection,
        Err(zbus::Error::Connection(e, _)) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(());
        }
        Err(e) => return Err(e),
    };

    match connection
        .call_method(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            Some("org.freedesktop.DBus"),
            "Hello",
            &(),
        )
        .await
    {
        Err(zbus::Error::MethodError(_, _, _)) => (),
        Err(e) => panic!("{}", e),

        _ => panic!(),
    };

    // `kept` holds the connection open, so that only `close()` can be what closed the helper's
    // input.
    let kept = connection.clone();
    connection.close().await?;
    #[cfg(target_os = "linux")]
    wait_for_the_helper_to_go().await;
    drop(kept);

    Ok(())
}

/// Waits until this process has no children left.
///
/// A helper that was told to exit and then waited for leaves the process table for good, while
/// one that was told nothing stays in it running and one that was never waited for stays in it
/// as a zombie. So an empty list is both halves of what closing a connection promises, and the
/// test's own timeout is what bounds the wait for it.
///
/// The wait gives the runtime its turn between two looks rather than spinning the thread,
/// because zbus's built-in runtime runs on this very thread, between two polls of the future
/// this is inside of, and the wait for the helper is its work to do.
#[cfg(target_os = "linux")]
async fn wait_for_the_helper_to_go() {
    while !children().is_empty() {
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                return std::task::Poll::Ready(());
            }
            yielded = true;
            cx.waker().wake_by_ref();

            std::task::Poll::Pending
        })
        .await;
    }
}

/// The process ids of this process's children, as every one of its threads reports them.
#[cfg(target_os = "linux")]
fn children() -> Vec<String> {
    std::fs::read_dir("/proc/self/task")
        .expect("a process on Linux can list its own threads")
        .filter_map(|thread| thread.ok())
        .filter_map(|thread| std::fs::read_to_string(thread.path().join("children")).ok())
        .flat_map(|children| {
            children
                .split_whitespace()
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .collect()
}
