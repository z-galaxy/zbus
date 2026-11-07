//! What a connection writes its socket through.

#[cfg(feature = "handover")]
use std::mem;
use std::{io, slice};

#[cfg(all(feature = "handover", any(unix, windows)))]
use super::handover::SocketKind;
#[cfg(feature = "handover")]
use crate::Error;
#[cfg(all(feature = "handover", any(unix, windows)))]
use crate::runtime::IoSource;
use crate::{
    Message, Result,
    connection::socket::{WriteHalf, Writes},
    fdo::ConnectionCredentials,
    runtime::io::write_messages,
};

/// What a connection writes its socket through, which those that write it, shut it down or hand it
/// over take in turn.
#[derive(Debug)]
pub(super) struct SocketWrite {
    /// The write half of the socket, until the connection is handed over.
    half: Option<Box<dyn WriteHalf>>,
    /// How the connection writes the socket, if zbus made it, rather than through `half` alone,
    /// until the connection is closed: directly, if zbus drives it.
    writes: Option<Writes>,
    /// The socket itself, if zbus drives it and so can give a descriptor of it for a hand-over.
    #[cfg(all(feature = "handover", any(unix, windows)))]
    source: Option<(IoSource, SocketKind)>,
    /// Whether a send failed or was given up on midway, which can leave part of a message in the
    /// socket. The peer can then make no sense of anything sent after it, so the connection can't
    /// be handed over any more. A send that failed as the peer receives nothing more leaves this
    /// as it was, as nothing sent after it reaches the peer.
    #[cfg(feature = "handover")]
    torn: bool,
    /// Whether the connection was closed, which shuts the socket down. It is set before the
    /// socket is shut down, so it also tells a reader that came to the end of the input as the
    /// connection was closed apart from one whose peer closed its end.
    #[cfg(feature = "handover")]
    shut_down: bool,
}

impl SocketWrite {
    /// What a connection writes through `half`, or as `writes` tells, if zbus made the socket.
    pub(super) fn new(
        half: Box<dyn WriteHalf>,
        writes: Option<Writes>,
        #[cfg(all(feature = "handover", any(unix, windows)))] source: Option<(
            IoSource,
            SocketKind,
        )>,
    ) -> Self {
        Self {
            half: Some(half),
            writes,
            #[cfg(all(feature = "handover", any(unix, windows)))]
            source,
            #[cfg(feature = "handover")]
            torn: false,
            #[cfg(feature = "handover")]
            shut_down: false,
        }
    }

    /// Send `msg`, as [`WriteHalf::send_message`] does, unless the connection was handed over.
    pub(super) async fn send_message(&mut self, msg: &Message) -> Result<()> {
        let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
        // Until the message is through, the socket may hold a part of it.
        #[cfg(feature = "handover")]
        let torn = mem::replace(&mut self.torn, true);
        let sent = match &self.writes {
            Some(Writes::Driven(socket)) => write_messages(&**socket, slice::from_ref(msg))
                .await
                .map_err(Into::into),
            None => half.send_message(msg).await,
        };
        #[cfg(feature = "handover")]
        self.through(torn, &sent);

        sent
    }

    /// Send `msgs`, as [`WriteHalf::send_messages`] does, unless the connection was handed over.
    pub(super) async fn send_messages(&mut self, msgs: &[Message]) -> Result<()> {
        let half = self.half.as_deref_mut().ok_or_else(handed_over)?;
        // Until the messages are through, the socket may hold a part of one.
        #[cfg(feature = "handover")]
        let torn = mem::replace(&mut self.torn, true);
        let sent = match &self.writes {
            Some(Writes::Driven(socket)) => {
                write_messages(&**socket, msgs).await.map_err(Into::into)
            }
            None => half.send_messages(msgs).await,
        };
        #[cfg(feature = "handover")]
        self.through(torn, &sent);

        sent
    }

    /// The credentials of the peer, as [`WriteHalf::peer_credentials`] gives them, unless the
    /// connection was handed over.
    pub(super) async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        let half = self.half.as_deref_mut().ok_or_else(handed_over)?;

        half.peer_credentials().await
    }

    /// Shut the socket down, as [`WriteHalf::close`] does, unless the connection was handed over,
    /// which leaves the socket alone: it is no longer the connection's.
    ///
    /// The connection lets go of `writes` too, as a pipe to a helper process, which it writes
    /// directly, closes as the last of its descriptors goes, and sends go through `half` from
    /// then on, which fails them.
    pub(super) async fn close(&mut self) -> io::Result<()> {
        let Some(half) = self.half.as_deref_mut() else {
            return Ok(());
        };
        // Set before the socket is shut down, as the shutdown can be given up on midway, and as
        // the reader can come to the end of the input as soon as it is.
        #[cfg(feature = "handover")]
        {
            self.shut_down = true;
        }
        self.writes = None;

        half.close().await
    }

    /// Whether a send failed or was given up on midway, which can leave part of a message in the
    /// socket.
    #[cfg(feature = "handover")]
    pub(super) fn is_torn(&self) -> bool {
        self.torn
    }

    /// Whether the connection was closed.
    #[cfg(feature = "handover")]
    pub(super) fn is_shut_down(&self) -> bool {
        self.shut_down
    }

    /// The socket itself, if zbus drives it and so can give a descriptor of it for a hand-over.
    #[cfg(all(feature = "handover", any(unix, windows)))]
    pub(super) fn source(&self) -> Option<&(IoSource, SocketKind)> {
        self.source.as_ref()
    }

    /// Take the write half out, for the connection to be handed over, with how the connection
    /// writes the socket, if zbus made it, for a connection that goes on over the half to write it
    /// the same way. Let go of all else that the connection holds of the socket. Nothing is
    /// written through the connection from then on.
    #[cfg(feature = "handover")]
    pub(super) fn take_half(&mut self) -> Option<(Box<dyn WriteHalf>, Option<Writes>)> {
        #[cfg(any(unix, windows))]
        {
            self.source = None;
        }
        let half = self.half.take()?;

        Some((half, self.writes.take()))
    }

    /// Put back `torn`, whether the socket was torn before a send, now that the send is through
    /// with `sent`, unless the send failed while the peer may still receive what follows it.
    #[cfg(feature = "handover")]
    fn through(&mut self, torn: bool, sent: &Result<()>) {
        if sent.as_ref().is_err_and(|e| !receives_nothing_more(e)) {
            return;
        }
        self.torn = torn;
    }
}

/// Whether `error`, which a send failed with, says that the peer receives nothing more, as one
/// that closed its end of the socket does: an I/O error of kind `BrokenPipe`, `ConnectionReset`
/// or `ConnectionAborted`. Nothing sent from then on reaches the peer, so part of a message that
/// the send left in the socket is followed by nothing that the peer could misread.
///
/// A connection that was closed fails its sends with `BrokenPipe` too, as the close shuts the
/// socket down, but `SocketWrite::shut_down` keeps it from being handed over anyway.
#[cfg(feature = "handover")]
fn receives_nothing_more(error: &Error) -> bool {
    matches!(
        error,
        Error::InputOutput(e) if matches!(
            e.kind(),
            io::ErrorKind::BrokenPipe
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
        )
    )
}

/// The error of what a connection that was handed over can no longer do.
fn handed_over() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "the connection was handed over",
    )
}

#[cfg(all(test, unix, feature = "p2p"))]
mod tests {
    use std::{
        os::{fd::AsFd, unix::net::UnixStream},
        sync::Arc,
    };
    #[cfg(feature = "unixexec")]
    use std::{path::Path, thread, time::Duration};

    use ntest::timeout;
    use test_log::test;

    #[cfg(feature = "unixexec")]
    use crate::{
        Address,
        address::transport::{Transport, Unixexec},
    };
    use crate::{
        Fd, Guid, Message,
        connection::{Builder, socket::ReadHalf},
        runtime::{
            Runtime,
            io::{Reader, UnixOps, registered, tests::Recording},
            test_runtime::under_every_runtime,
        },
    };

    /// A connection over a socket that zbus drives writes the messages of a send several at a
    /// time, as the write half of the socket does: each write starts at a message and takes up to
    /// 16 of them, and a message with file descriptors goes in a write of its own, which they go
    /// with. A message sent on its own goes in a write of its own.
    #[test]
    #[timeout(15000)]
    fn messages_go_in_writes_of_several() {
        under_every_runtime(|runtime| async move {
            let (ours, theirs) = UnixStream::pair().unwrap();
            let ops = Recording::default();
            let writes = ops.writes.clone();
            let socket = registered(&runtime, ours, ops).unwrap();
            let conn = on(
                Builder::authenticated_socket(socket, Guid::generate()),
                &runtime,
            )
            .p2p()
            .build()
            .await
            .unwrap();
            let mut peer = Reader::new(Arc::new(registered(&runtime, theirs, UnixOps).unwrap()));
            // More messages without descriptors than a write takes, then one with, one without,
            // and another with.
            let (read_end, _write_end) = std::io::pipe().unwrap();
            let msgs: Vec<_> = (0..22u32)
                .map(|i| {
                    let builder = Message::method_call("/", "Test");
                    match i {
                        18 | 20 => builder.build(&(i, Fd::from(read_end.as_fd()))),
                        _ => builder.build(&i),
                    }
                    .unwrap()
                })
                .collect();

            conn.send_all(&msgs[..21]).await.unwrap();
            conn.send(&msgs[21]).await.unwrap();

            assert_eq!(
                *writes.lock().unwrap(),
                [(16, 0), (2, 0), (1, 1), (1, 0), (1, 1), (1, 0)],
                "the writes were not split as expected, as (messages, descriptors)",
            );
            for (seq, sent) in msgs.iter().enumerate() {
                let received = peer
                    .receive_message(seq as u64, &mut vec![], &mut vec![])
                    .await
                    .unwrap();
                assert_eq!(&received.data()[..], &sent.data()[..]);
                assert_eq!(received.data().fds().len(), sent.data().fds().len());
            }
        });
    }

    /// Closing a connection over the pipes of a `unixexec:` helper process closes its standard
    /// input, which the connection writes directly: the helper comes to the end of its input.
    #[cfg(feature = "unixexec")]
    #[test]
    #[timeout(15000)]
    fn closing_a_unixexec_connection_ends_the_input_of_the_helper() {
        under_every_runtime(|runtime| async move {
            let dir = tempfile::tempdir().unwrap();
            let ended = dir.path().join("ended");
            // The helper answers the handshake, takes in all that it is sent until its input ends,
            // and then says so.
            let script = "read -r line; printf 'OK 0123456789abcdef0123456789abcdef\\r\\n'; \
                          cat >/dev/null; : >\"$0\"";
            let helper = Unixexec::new(
                "sh".into(),
                None,
                vec!["-c".into(), script.into(), ended.clone().into()],
            );
            let address = Address::from(Transport::Unixexec(helper));
            let conn = on(Builder::address(address).p2p(), &runtime)
                .build()
                .await
                .unwrap();
            let msg = Message::signal("/org/zbus/Test", "org.zbus.Test", "Test")
                .build(&())
                .unwrap();
            conn.send(&msg).await.unwrap();

            conn.clone().close().await.unwrap();

            wait_for(&ended);
        });
    }

    /// `builder`, to build its connection on `runtime`.
    fn on<'a>(builder: Builder<'a>, runtime: &Runtime) -> Builder<'a> {
        match runtime {
            // A builder that is given no runtime takes the one that the test runs on.
            #[cfg(feature = "default-rt")]
            Runtime::ZRuntime(_) => builder,
            #[cfg(feature = "tokio")]
            Runtime::Tokio(_) => builder,
            Runtime::External(runtime) => builder.runtime(runtime.clone()),
        }
    }

    /// Waits for `path` to exist, which a helper process is to bring about.
    #[cfg(feature = "unixexec")]
    fn wait_for(path: &Path) {
        while !path.exists() {
            thread::sleep(Duration::from_millis(1));
        }
    }
}
