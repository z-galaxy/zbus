//! [`ObjectServer::prepare_hand_over`] has the object server take no more method calls, and waits
//! for those that it has under way.
//!
//! [`ObjectServer::prepare_hand_over`]: zbus::ObjectServer::prepare_hand_over

#![cfg(all(
    target_os = "linux",
    feature = "handover",
    feature = "service",
    any(feature = "default-rt", feature = "tokio")
))]

use std::{
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::StreamExt;
use ntest::timeout;
use test_log::test;
use zbus::{Connection, MessageStream, block_on, connection::Builder, interface, message::Type};
use zruntime::Event;

/// From when the object server took a call until it answered it, the preparation waits, whether
/// the method of the call runs in a task of its own or not, and it resolves at once after, the
/// second time too.
#[test]
#[timeout(15000)]
fn prepare_hand_over_waits_for_the_calls_under_way() {
    block_on(async {
        for inline in [false, true] {
            let gate = Arc::new(Gate::default());
            let builder = Builder::session();
            let builder = match inline {
                false => builder.serve_at(PATH, Gated(gate.clone())),
                true => builder.serve_at(PATH, InlineGated(gate.clone())),
            };
            let service = builder.build().await.unwrap();
            let name = service.unique_name().unwrap().clone();
            let client = Connection::session().await.unwrap();
            let server = service.object_server();

            let calling = client.call_method(Some(&name), PATH, Some(INTERFACE), "Pass", &());
            let waiting = async {
                // The method of the call waits at the gate, so the call is under way.
                gate.reached().await;
                let mut preparing = pin!(server.prepare_hand_over());
                assert!(
                    futures_lite::future::poll_once(preparing.as_mut())
                        .await
                        .is_none(),
                    "prepared with a call under way, inline: {inline}"
                );
                gate.open();
                preparing.await;
            };
            let (reply, ()) = futures_util::future::join(calling, waiting).await;

            reply.unwrap();
            assert!(
                futures_lite::future::poll_once(server.prepare_hand_over())
                    .await
                    .is_some(),
                "prepared again with no call under way, inline: {inline}"
            );
        }
    });
}

/// The preparation of an object server that has no call under way resolves at once, the second
/// time too.
#[test]
#[timeout(15000)]
fn prepare_hand_over_resolves_at_once_with_no_call_under_way() {
    block_on(async {
        let service = Builder::session()
            .serve_at(PATH, Echo)
            .build()
            .await
            .unwrap();
        let server = service.object_server();

        for time in ["first", "second"] {
            assert!(
                futures_lite::future::poll_once(server.prepare_hand_over())
                    .await
                    .is_some(),
                "not prepared at once the {time} time"
            );
        }
    });
}

/// A call that comes in once the object server was prepared is left: the object server of the
/// connection that takes over answers it.
#[test]
#[timeout(15000)]
fn a_call_after_prepare_hand_over_is_left_for_the_connection_that_takes_over() {
    block_on(async {
        let service = Builder::session()
            .serve_at(PATH, Echo)
            .build()
            .await
            .unwrap();
        let name = service.unique_name().unwrap().clone();
        let client = Connection::session().await.unwrap();
        let mut stream = MessageStream::from(&service);
        service.object_server().prepare_hand_over().await;

        let calling = client.call_method(Some(&name), PATH, Some(INTERFACE), "Echo", &"left");
        let taking_over = async {
            // The connection read the call, which the object server leaves.
            loop {
                let msg = stream.next().await.unwrap().unwrap();
                if msg.message_type() == Type::MethodCall {
                    break;
                }
            }
            let handover = service.hand_over().await.unwrap();

            Builder::take_over(handover)
                .serve_at(PATH, Echo)
                .build()
                .await
                .unwrap()
        };
        let (reply, _service) = futures_util::future::join(calling, taking_over).await;

        let reply = reply.unwrap();
        assert_eq!(reply.body().deserialize::<&str>().unwrap(), "left");
    });
}

/// A gate that the methods of the interfaces wait at, until the test opens it.
#[derive(Default)]
struct Gate {
    reached: AtomicBool,
    open: AtomicBool,
    changed: Event,
}

impl Gate {
    /// Wait at the gate until it is open, and tell that it was reached.
    async fn pass(&self) {
        self.reached.store(true, Ordering::SeqCst);
        self.changed.notify(usize::MAX);
        self.wait_for(&self.open).await;
    }

    /// Wait for a method to have reached the gate.
    async fn reached(&self) {
        self.wait_for(&self.reached).await;
    }

    fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
        self.changed.notify(usize::MAX);
    }

    async fn wait_for(&self, flag: &AtomicBool) {
        loop {
            // The listener comes first, so that a change right after the check isn't missed.
            let listener = self.changed.listen();
            if flag.load(Ordering::SeqCst) {
                return;
            }
            listener.await;
        }
    }
}

/// Defines an interface whose method waits at a gate, in a task of its own for each call or not,
/// as `spawn` says.
macro_rules! gated {
    ($name:ident, $spawn:literal) => {
        struct $name(Arc<Gate>);

        #[interface(name = "org.zbus.PrepareHandOverTest", spawn = $spawn)]
        impl $name {
            async fn pass(&self) {
                self.0.pass().await;
            }
        }
    };
}

gated!(Gated, true);
gated!(InlineGated, false);

/// An interface whose method answers at once with what it was given.
struct Echo;

#[interface(name = "org.zbus.PrepareHandOverTest")]
impl Echo {
    fn echo(&self, text: String) -> String {
        text
    }
}

const PATH: &str = "/org/zbus/PrepareHandOverTest";
const INTERFACE: &str = "org.zbus.PrepareHandOverTest";
