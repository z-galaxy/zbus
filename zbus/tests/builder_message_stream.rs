#![cfg(feature = "comms")]

//! Tests for [`zbus::connection::Builder::build_message_stream`].
//!
//! Simulates the busd scenario: a bus client pipelines a `Hello` method call as part of its
//! SASL handshake, before the server (bus-impl) has started polling for messages. With plain
//! `build()` + `MessageStream::from`, the Hello can be lost; `build_message_stream` fixes
//! this by setting up the stream before the socket reader task starts.

#![cfg(all(unix, feature = "bus-impl"))]

use futures_util::StreamExt;
use ntest::timeout;
use test_log::test;
use zbus::{Connection, Guid, connection::Builder};

/// Simulates the busd race: a bus client pipelines Hello during SASL auth, before the server
/// has started polling. `build_message_stream` must still deliver it.
#[cfg(feature = "async-io")]
#[test]
#[timeout(15000)]
fn build_message_stream_does_not_drop_pipelined_hello_builtin_runtime() {
    futures_lite::future::block_on(async {
        let (s0, s1) = std::os::unix::net::UnixStream::pair().unwrap();
        let guid = Guid::generate();

        build_message_stream_does_not_drop_pipelined_hello(
            Builder::unix_stream(s0).server(guid).p2p(),
            Builder::unix_stream(s1),
        )
        .await;
    });
}

#[cfg(feature = "tokio")]
#[test]
#[timeout(15000)]
fn build_message_stream_does_not_drop_pipelined_hello_tokio() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (s0, s1) = std::os::unix::net::UnixStream::pair().unwrap();
        let guid = Guid::generate();

        build_message_stream_does_not_drop_pipelined_hello(
            Builder::unix_stream(s0).server(guid).p2p(),
            Builder::unix_stream(s1),
        )
        .await;
    });
}

async fn build_message_stream_does_not_drop_pipelined_hello(
    server_builder: Builder<'_>,
    client_builder: Builder<'_>,
) {
    // Server: SASL auth, set up stream, receive Hello, reply to it.
    let server = async {
        let mut stream = server_builder.build_message_stream().await.unwrap();

        let hello = stream
            .next()
            .await
            .expect("stream terminated unexpectedly")
            .unwrap();
        assert_eq!(hello.header().member().unwrap().as_str(), "Hello");

        // Reply so the client's build() can complete.
        let conn = Connection::from(stream);
        conn.reply(&hello.header(), &(":1.1",)).await.unwrap();
    };

    // Run both concurrently so the SASL handshake completes cooperatively.
    // The client is a bus connection whose build() pipelines Hello during SASL auth.
    let ((), client) = futures_util::join!(server, client_builder.build());
    let _client_conn = client.unwrap();
}
