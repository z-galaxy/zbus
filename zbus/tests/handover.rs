//! A service on a bus hands its connection over, and the connection that takes over goes on
//! serving the same clients, under the same names, with nothing lost on the way.
//!
//! The tests run on the session bus, as a service that restarts does.

#![cfg(all(
    target_os = "linux",
    feature = "handover",
    feature = "service",
    any(feature = "default-rt", feature = "tokio")
))]

use std::{io, os::fd::OwnedFd};

use futures_util::StreamExt;
use ntest::timeout;
use test_log::test;
use zbus::{
    Connection, MatchRule, MessageStream, block_on,
    connection::{Builder, Handover},
    interface,
    message::Type,
    names::OwnedUniqueName,
};
use zoter::{Restore, Save};

/// A service that owns a well-known name hands its connection over through
/// `Connection::hand_over`, and the record of the hand-over. The connection that takes over, with
/// the same name and interface, has the unique name of the one that was handed over, and the bus
/// has it as the owner of the well-known name still. A call that a client made before the
/// hand-over was answered once, one that it makes while no connection reads is answered by the
/// connection that takes over, and so are those that it makes after that.
#[test]
#[timeout(15000)]
fn a_service_is_handed_over_with_its_name() {
    block_on(async {
        let service = Builder::session()
            .name(NAME)
            .serve_at(PATH, Greeter)
            .build()
            .await
            .unwrap();
        let unique_name = service.unique_name().unwrap().clone();
        let client = Connection::session().await.unwrap();
        assert_eq!(greeting(&client, "before").await, "Hello before!");
        assert_eq!(owner(&client).await, unique_name);

        // The record of the hand-over, and duplicates of its descriptors, as the next instance of
        // the service gets them, once this one let go of the hand-over.
        let (record, fds) = {
            let handover = service.hand_over().await.unwrap();

            assert!(service.is_closed(), "the connection is open still");
            assert_eq!(handover.unread_len(), 0);
            let mut record = vec![0; handover.saved_len().unwrap()];
            handover.save(&mut record).unwrap();
            let fds = handover
                .fds()
                .iter()
                .map(|fd| fd.try_clone_to_owned())
                .collect::<io::Result<Vec<OwnedFd>>>()
                .unwrap();

            (record, fds)
        };
        // The bus has the connection still, so the call waits for whoever takes it over.
        let calling = greeting(&client, "meanwhile");
        let taking_over = async {
            let handover = Handover::restore(&record, fds).unwrap();

            Builder::take_over(handover)
                .name(NAME)
                .serve_at(PATH, Greeter)
                .build()
                .await
                .unwrap()
        };
        let (answer, service) = futures_util::future::join(calling, taking_over).await;

        assert_eq!(answer, "Hello meanwhile!");
        assert_eq!(service.unique_name(), Some(&unique_name));
        assert_eq!(owner(&client).await, unique_name);
        assert_eq!(greeting(&client, "after").await, "Hello after!");
    });
}

/// A stream for a match rule, as the stream of the signals of a proxy is, yields the signals that
/// its connection read, and ends as the connection is handed over. The bus keeps the match rule,
/// and a stream of the connection that takes over adds it again: a signal that both rules ask
/// for still comes once.
#[test]
#[timeout(15000)]
fn a_signal_comes_once_to_a_connection_that_took_over() {
    block_on(async {
        let conn = Connection::session().await.unwrap();
        let emitter = Connection::session().await.unwrap();
        let rule = MatchRule::builder()
            .msg_type(Type::Signal)
            .sender(emitter.unique_name().unwrap())
            .interface(NAME)
            .build()
            .unwrap();
        let mut signals = MessageStream::for_match_rule(rule.clone(), &conn, None)
            .await
            .unwrap();
        tick(&emitter, 1).await;
        assert_eq!(ticked(&mut signals).await, Some(1));

        let handover = conn.hand_over().await.unwrap();

        assert_eq!(ticked(&mut signals).await, None);
        let conn = Builder::take_over(handover).build().await.unwrap();
        let mut signals = MessageStream::for_match_rule(rule, &conn, None)
            .await
            .unwrap();
        for n in [2, 3] {
            tick(&emitter, n).await;
            assert_eq!(ticked(&mut signals).await, Some(n));
        }
    });
}

/// Has `emitter` emit a signal that carries `n`.
async fn tick(emitter: &Connection, n: u32) {
    emitter
        .emit_signal(None::<()>, PATH, NAME, "Tick", &n)
        .await
        .unwrap();
}

/// What the next signal of `signals` carries, if it yields one more.
async fn ticked(signals: &mut MessageStream) -> Option<u32> {
    let signal = signals.next().await?.unwrap();

    Some(signal.body().deserialize().unwrap())
}

/// What the service answers a call of `client` to its `Greet` method with, for `who`.
async fn greeting(client: &Connection, who: &str) -> String {
    client
        .call_method(Some(NAME), PATH, Some(NAME), "Greet", &who)
        .await
        .unwrap()
        .body()
        .deserialize()
        .unwrap()
}

/// The unique name of the connection that the bus has as the owner of the name of the service.
async fn owner(client: &Connection) -> OwnedUniqueName {
    client
        .call_method(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            Some("org.freedesktop.DBus"),
            "GetNameOwner",
            &NAME,
        )
        .await
        .unwrap()
        .body()
        .deserialize()
        .unwrap()
}

/// The interface of the service.
struct Greeter;

#[interface(name = "org.zbus.HandoverTest")]
impl Greeter {
    fn greet(&self, who: &str) -> String {
        format!("Hello {who}!")
    }
}

/// The well-known name of the service, and the name of its interface.
const NAME: &str = "org.zbus.HandoverTest";
const PATH: &str = "/org/zbus/HandoverTest";
