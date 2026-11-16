//! [`ResponseDispatchNotifier::followed_by`] has the work that follows a response done once the
//! response was sent, or once it is clear that none is.
//!
//! [`ResponseDispatchNotifier::followed_by`]: zbus::object_server::ResponseDispatchNotifier::followed_by

#![cfg(all(feature = "service", any(feature = "default-rt", feature = "tokio")))]

use futures_util::StreamExt;
use ntest::timeout;
use test_log::test;
use zbus::{
    Connection, Message, MessageStream, block_on,
    connection::Builder,
    fdo, interface,
    message::{Flags, Header, Type},
    object_server::{ResponseDispatchNotifier, SignalEmitter},
};

/// The reply to a call comes first, and what the work that follows the response sends after it.
#[test]
#[timeout(15000)]
fn work_follows_the_response() {
    block_on(async {
        let (_service, client, call) = calling("Announce").await;
        let mut stream = MessageStream::from(&client);
        let call = call.build(&()).unwrap();
        client.send(&call).await.unwrap();

        assert_eq!(
            next(&mut stream, &call, 2).await,
            [Type::MethodReturn, Type::Signal]
        );
    });
}

/// The work is done for a call that expects no reply too, as none is sent.
#[test]
#[timeout(15000)]
fn work_follows_a_call_that_expects_no_reply() {
    block_on(async {
        let (_service, client, call) = calling("Announce").await;
        let mut stream = MessageStream::from(&client);
        let call = call
            .with_flags(Flags::NO_REPLY_EXPECTED)
            .build(&())
            .unwrap();
        client.send(&call).await.unwrap();

        assert_eq!(next(&mut stream, &call, 1).await, [Type::Signal]);
    });
}

/// The work is done for a call whose method fails once it made the response too: the caller
/// gets the error, and what the work sends.
#[test]
#[timeout(15000)]
fn work_follows_a_response_that_is_let_go_of() {
    block_on(async {
        let (_service, client, call) = calling("Fail").await;
        let mut stream = MessageStream::from(&client);
        let call = call.build(&()).unwrap();
        client.send(&call).await.unwrap();

        let mut got = next(&mut stream, &call, 2).await;
        got.sort_by_key(|kind| *kind as u8);
        assert_eq!(got, [Type::Error, Type::Signal]);
    });
}

/// A service on the session bus, a client of it, and a call from the client to `member` of the
/// interface of the service.
async fn calling(
    member: &'static str,
) -> (Connection, Connection, zbus::message::Builder<'static>) {
    let service = Builder::session()
        .serve_at(PATH, Announcer)
        .build()
        .await
        .unwrap();
    let name = service.unique_name().unwrap().to_owned();
    let client = Connection::session().await.unwrap();
    let call = Message::method_call(PATH, member)
        .interface(INTERFACE)
        .destination(name);

    (service, client, call)
}

/// The types of the next `count` messages of `stream` that answer `call`, or are the signal of
/// the interface.
async fn next(stream: &mut MessageStream, call: &Message, count: usize) -> Vec<Type> {
    let serial = call.primary_header().serial_num();
    let mut got = Vec::new();
    while got.len() < count {
        let msg = stream.next().await.unwrap().unwrap();
        let header = msg.header();
        let answers = header.reply_serial() == Some(serial);
        let announced = header.member().is_some_and(|member| member == "Announced");
        if answers || announced {
            got.push(msg.message_type());
        }
    }

    got
}

/// An interface whose methods have a signal to their caller follow their response.
struct Announcer;

#[interface(name = "org.zbus.FollowedByTest")]
impl Announcer {
    /// Answers, and has the signal follow the response.
    fn announce(
        &self,
        #[zbus(header)] header: Header<'_>,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> ResponseDispatchNotifier<()> {
        Self::followed((), &header, emitter)
    }

    /// Makes a response that the signal follows, and fails.
    fn fail(
        &self,
        #[zbus(header)] header: Header<'_>,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> fdo::Result<ResponseDispatchNotifier<()>> {
        let _response = Self::followed((), &header, emitter);

        Err(fdo::Error::Failed("failed".to_string()))
    }

    #[zbus(signal)]
    async fn announced(emitter: &SignalEmitter<'_>) -> zbus::Result<()>;
}

impl Announcer {
    /// `response`, with the signal to the sender of the call of `header` as the work that follows
    /// it.
    fn followed<R>(
        response: R,
        header: &Header<'_>,
        emitter: SignalEmitter<'_>,
    ) -> ResponseDispatchNotifier<R> {
        let caller = header.sender().unwrap().to_owned();
        let conn = emitter.connection().clone();
        let emitter = emitter.into_owned().set_destination(caller.into());

        ResponseDispatchNotifier::followed_by(response, &conn, async move {
            Self::announced(&emitter).await.unwrap();
        })
    }
}

const PATH: &str = "/org/zbus/FollowedByTest";
const INTERFACE: &str = "org.zbus.FollowedByTest";
