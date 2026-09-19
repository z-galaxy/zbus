// Everything here talks to a bus, which needs one of the backends to connect to.
#![cfg(all(feature = "comms", any(feature = "builtin-runtime", feature = "tokio")))]

use std::collections::HashMap;

use enumflags2::BitFlags;
use ntest::timeout;
use test_log::test;
use tracing::{debug, instrument};
use zbus::block_on;

use zbus::{OwnedValue, Type, names::UniqueName};

use zbus::{
    Connection, Result,
    fdo::{RequestNameFlags, RequestNameReply},
    message::Message,
};

#[test]
fn msg() {
    let m = Message::method_call("/org/freedesktop/DBus", "GetMachineId")
        .destination("org.freedesktop.DBus")
        .interface("org.freedesktop.DBus.Peer")
        .build(&())
        .unwrap();
    let hdr = m.header();
    assert_eq!(hdr.path().unwrap(), "/org/freedesktop/DBus");
    assert_eq!(hdr.interface().unwrap(), "org.freedesktop.DBus.Peer");
    assert_eq!(hdr.member().unwrap(), "GetMachineId");
}

#[test]
#[timeout(15000)]
#[instrument]
fn basic_connection() {
    block_on(basic_connection_inner()).unwrap();
}

async fn basic_connection_inner() -> Result<()> {
    let connection = Connection::session().await.map_err(|e| {
        debug!("error: {}", e);

        e
    })?;

    // Hello method is already called during connection creation so subsequent calls are
    // expected to fail but only with a D-Bus error.
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

    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
#[test]
#[timeout(15000)]
fn fdpass_systemd() {
    zbus::block_on(fdpass_systemd_async());
}

#[cfg(all(unix, not(target_os = "macos")))]
async fn fdpass_systemd_async() {
    use std::{fs::File, os::unix::io::AsRawFd};
    use zbus::OwnedFd;

    let connection = Connection::system().await.unwrap();

    let reply = connection
        .call_method(
            Some("org.freedesktop.systemd1"),
            "/org/freedesktop/systemd1",
            Some("org.freedesktop.systemd1.Manager"),
            "DumpByFileDescriptor",
            &(),
        )
        .await
        .unwrap();

    let fd: OwnedFd = reply.body().deserialize().unwrap();
    assert!(fd.as_raw_fd() >= 0);
    let f = File::from(std::os::fd::OwnedFd::from(fd));
    f.metadata().unwrap();
}

#[test]
#[instrument]
#[timeout(15000)]
fn freedesktop_api() {
    block_on(freedesktop_api_inner()).unwrap();
}

#[instrument]
async fn freedesktop_api_inner() -> Result<()> {
    let connection = Connection::session().await.map_err(|e| {
        debug!("error: {}", e);

        e
    })?;

    let reply = connection
        .call_method(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            Some("org.freedesktop.DBus"),
            "RequestName",
            &(
                "org.freedesktop.zbus.FreedesktopApi",
                BitFlags::from(RequestNameFlags::ReplaceExisting),
            ),
        )
        .await
        .unwrap();

    let body = reply.body();
    assert_eq!(body.signature(), u32::SIGNATURE);
    let reply: RequestNameReply = body.deserialize().unwrap();
    assert_eq!(reply, RequestNameReply::PrimaryOwner);

    let reply = connection
        .call_method(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            Some("org.freedesktop.DBus"),
            "GetId",
            &(),
        )
        .await
        .unwrap();

    let body = reply.body();
    assert_eq!(body.signature(), <&str>::SIGNATURE);
    let id: &str = body.deserialize().unwrap();
    debug!("Unique ID of the bus: {}", id);

    let reply = connection
        .call_method(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            Some("org.freedesktop.DBus"),
            "NameHasOwner",
            &"org.freedesktop.zbus.FreedesktopApi",
        )
        .await
        .unwrap();

    let body = reply.body();
    assert_eq!(body.signature(), bool::SIGNATURE);
    assert!(body.deserialize::<bool>().unwrap());

    let reply = connection
        .call_method(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            Some("org.freedesktop.DBus"),
            "GetNameOwner",
            &"org.freedesktop.zbus.FreedesktopApi",
        )
        .await
        .unwrap();

    let body = reply.body();
    assert_eq!(body.signature(), <&str>::SIGNATURE);
    assert_eq!(
        body.deserialize::<UniqueName<'_>>().unwrap(),
        *connection.unique_name().unwrap(),
    );

    let reply = connection
        .call_method(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            Some("org.freedesktop.DBus"),
            "GetConnectionCredentials",
            &"org.freedesktop.DBus",
        )
        .await
        .unwrap();

    let body = reply.body();
    assert_eq!(body.signature(), "a{sv}");
    let hashmap: HashMap<&str, OwnedValue> = body.deserialize().unwrap();

    let pid: u32 = (&hashmap["ProcessID"]).try_into().unwrap();
    debug!("DBus bus PID: {}", pid);

    #[cfg(unix)]
    {
        let uid: u32 = (&hashmap["UnixUserID"]).try_into().unwrap();
        debug!("DBus bus UID: {}", uid);
    }

    Ok(())
}

#[cfg(all(unix, feature = "proxy"))]
#[tokio::test]
#[timeout(15000)]
#[instrument]
async fn test_freedesktop_credentials() -> Result<()> {
    use rustix::process::{getegid, geteuid};

    let connection = Connection::session().await?;
    let dbus = zbus::fdo::DBusProxy::new(&connection).await?;
    let credentials = dbus
        .get_connection_credentials(connection.unique_name().unwrap().into())
        .await?;

    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        use tokio::fs::read_to_string;

        if let Some(fd) = credentials.process_fd() {
            let fd = fd.as_raw_fd();
            let fdinfo = read_to_string(&format!("/proc/self/fdinfo/{fd}")).await?;
            let pidline = fdinfo.split('\n').find(|s| s.starts_with("Pid:")).unwrap();
            let pid: u32 = pidline.split('\t').next_back().unwrap().parse().unwrap();
            assert_eq!(std::process::id(), pid);
        }
    }

    assert_eq!(std::process::id(), credentials.process_id().unwrap());
    assert_eq!(geteuid().as_raw(), credentials.unix_user_id().unwrap());

    if let Some(group_ids) = credentials.unix_group_ids() {
        group_ids
            .iter()
            .find(|group| **group == getegid().as_raw())
            .unwrap();
    }

    Ok(())
}

#[cfg(all(unix, feature = "ibus"))]
#[tokio::test]
#[timeout(15000)]
async fn ibus_connection() {
    use std::env;
    use tokio::fs;

    // First try with real IBus if available.
    let result = test_ibus_connection().await;

    match result {
        Ok(_) => return,
        Err(zbus::Error::Address(msg)) if msg.contains("The ibus command failed") => {
            // IBus not available, use mock.
        }
        Err(e) => panic!("Unexpected error: {}", e),
    }

    // If real IBus is not available, set up a mock and try again.
    let temp_dir = std::env::temp_dir().join(format!("zbus-test-{}", std::process::id()));
    fs::create_dir_all(&temp_dir).await.unwrap();

    // Mock ibus script that outputs a valid D-Bus address.
    let mock_ibus = temp_dir.join("ibus");
    let session_address = env::var("DBUS_SESSION_BUS_ADDRESS")
        .unwrap_or_else(|_| "unix:path=/tmp/dbus-test".to_string());

    fs::write(
        &mock_ibus,
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"address\" ]; then\n  echo \"{}\"\nfi\n",
            session_address
        ),
    )
    .await
    .unwrap();

    // Make the script executable.
    use std::os::unix::fs::PermissionsExt as _;
    let perms = std::fs::Permissions::from_mode(0o755);
    fs::set_permissions(&mock_ibus, perms).await.unwrap();

    // Prepend temp directory to PATH so our mock ibus is found first.
    let original_path = env::var("PATH").unwrap_or_default();
    let new_path = format!("{}:{}", temp_dir.display(), original_path);
    unsafe {
        env::set_var("PATH", &new_path);
    }

    let result = test_ibus_connection().await;

    // Restore PATH and remove temp directory.
    unsafe {
        env::set_var("PATH", &original_path);
    }
    fs::remove_dir_all(&temp_dir).await.ok();

    result.unwrap();
}

#[cfg(all(unix, feature = "ibus"))]
async fn test_ibus_connection() -> Result<()> {
    let connection = zbus::connection::Builder::ibus().build().await?;

    // Just verify we can get a unique name.
    assert!(connection.unique_name().is_some());

    Ok(())
}
