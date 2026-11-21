//! A zbus connection talks to the server of the fixture, which does its own I/O, as to a bus.
#![cfg(unix)]

use std::{
    io::{Read, Write},
    os::{fd::OwnedFd, unix::net::UnixStream},
    thread,
    time::Duration,
};

use own_io_fixture::{ECHO_INTERFACE, UNIQUE_NAME};
use rustix::process::geteuid;
use zbus::{Connection, connection::Builder, fdo::ConnectionCredentials};

/// The client gets its unique name, pings the server, gets its own credentials, and has a file
/// descriptor echoed, so that one goes both ways. Then it hangs up, which ends the serving.
#[test]
fn a_zbus_client_talks_to_a_server_that_does_its_own_io() {
    let (client, server) = UnixStream::pair().unwrap();
    // If the server waits for good, or the client for a reply, the test fails after this long.
    server.set_read_timeout(Some(TIMEOUT)).unwrap();
    let server = thread::spawn(move || own_io_fixture::serve(server));

    zbus::block_on(async {
        let conn = Builder::unix_stream(client)
            .method_timeout(TIMEOUT)
            .build()
            .await
            .unwrap();
        assert_eq!(conn.unique_name().unwrap().as_str(), UNIQUE_NAME);

        let peer = Some("org.freedesktop.DBus.Peer");
        conn.call_method(Some(BUS), BUS_PATH, peer, "Ping", &())
            .await
            .unwrap();

        let reply = call_bus(&conn, "GetConnectionCredentials", UNIQUE_NAME).await;
        let credentials: ConnectionCredentials = reply.body().deserialize().unwrap();
        assert_eq!(credentials.unix_user_id(), Some(geteuid().as_raw()));
        // `ConnectionCredentials::from_socket` tells the process ID on Linux and Android only.
        #[cfg(any(target_os = "linux", target_os = "android"))]
        assert_eq!(credentials.process_id(), Some(std::process::id()));

        let (mut ours, theirs) = UnixStream::pair().unwrap();
        let echo = Some(ECHO_INTERFACE);
        let fd: zbus::Fd<'_> = (&theirs).into();
        let reply = conn
            .call_method(echo, ECHO_PATH, echo, "Echo", &fd)
            .await
            .unwrap();
        let echoed: zbus::OwnedFd = reply.body().deserialize().unwrap();
        let echoed: OwnedFd = echoed.into();
        let mut echoed: UnixStream = echoed.into();
        echoed.write_all(b"both ways").unwrap();
        let mut read = [0; 9];
        ours.read_exact(&mut read).unwrap();
        assert_eq!(&read, b"both ways");

        conn.close().await.unwrap();
    });

    server.join().unwrap().unwrap();
}

/// The reply of the bus to a call of its `method` with `body`.
async fn call_bus(conn: &Connection, method: &str, body: &str) -> zbus::Message {
    conn.call_method(Some(BUS), BUS_PATH, Some(BUS), method, &body)
        .await
        .unwrap()
}

/// The name of the bus.
const BUS: &str = "org.freedesktop.DBus";

/// The path of the bus.
const BUS_PATH: &str = "/org/freedesktop/DBus";

/// The path that `Echo` is called on.
const ECHO_PATH: &str = "/org/zbus/OwnIo";

/// How long the server waits for the client, and the client for a reply, before the test fails.
const TIMEOUT: Duration = Duration::from_secs(10);
