use std::io;

use std::num::NonZeroUsize;

use zruntime::broadcast::{Receiver, Sender, channel};

use crate::{Message, conn::AuthMechanism, fdo::ConnectionCredentials, log::trace};

/// An in-process channel-based socket.
///
/// This is a pair of two cross-wired channels. Since all communication happens in-process, there is
/// no need for any authentication.
///
/// This type is only available when the `p2p` feature is enabled.
#[derive(Debug)]
pub struct Channel {
    writer: Writer,
    reader: Reader,
}

impl Channel {
    /// Create a pair of cross-wired channels.
    ///
    /// Use [`crate::connection::Builder::authenticated_socket`] to create `Connection` instances
    /// from each channel.
    pub fn pair() -> (Self, Self) {
        let (tx1, rx1) = channel(CHANNEL_CAPACITY);
        let (tx2, rx2) = channel(CHANNEL_CAPACITY);

        (
            Self {
                writer: Writer(tx1),
                reader: Reader(rx2),
            },
            Self {
                writer: Writer(tx2),
                reader: Reader(rx1),
            },
        )
    }
}

impl super::Socket for Channel {
    type ReadHalf = Reader;
    type WriteHalf = Writer;

    fn split(self) -> super::Split<Self::ReadHalf, Self::WriteHalf> {
        super::Split {
            read: self.reader,
            write: self.writer,
        }
    }
}

/// The reader half of a [`Channel`].
///
/// This type is only available when the `p2p` feature is enabled.
#[derive(Debug)]
pub struct Reader(Receiver<Message>);

#[async_trait::async_trait]
impl super::ReadHalf for Reader {
    async fn receive_message(
        &mut self,
        _seq: u64,
        _already_received_bytes: &mut Vec<u8>,
        #[cfg(unix)] _already_received_fds: &mut Vec<std::os::fd::OwnedFd>,
    ) -> crate::Result<Message> {
        self.0.recv().await.map_err(|e| {
            crate::Error::InputOutput(io::Error::new(io::ErrorKind::BrokenPipe, e).into())
        })
    }

    /// Supports passing file descriptors.
    fn can_pass_unix_fd(&self) -> bool {
        true
    }

    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        self_credentials().await
    }

    fn auth_mechanism(&self) -> AuthMechanism {
        AuthMechanism::Anonymous
    }
}

/// The writer half of a [`Channel`].
///
/// This type is only available when the `p2p` feature is enabled.
#[derive(Debug)]
pub struct Writer(Sender<Message>);

#[async_trait::async_trait]
impl super::WriteHalf for Writer {
    async fn send_message(&mut self, msg: &Message) -> crate::Result<()> {
        self.0
            .broadcast(msg.clone())
            .await
            .map_err(|e| {
                crate::Error::InputOutput(io::Error::new(io::ErrorKind::BrokenPipe, e).into())
            })
            .map(|removed| {
                // With overflow enabled, the oldest message may be removed.
                // This is expected and safe.
                if let Some(removed) = removed {
                    trace!("Channel overflow: removed oldest message: {:?}", removed);
                }
            })
    }

    async fn close(&mut self) -> io::Result<()> {
        self.0.close();

        Ok(())
    }

    /// Supports passing file descriptors.
    fn can_pass_unix_fd(&self) -> bool {
        true
    }

    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        self_credentials().await
    }
}

/// The credentials of the current process.
async fn self_credentials() -> io::Result<ConnectionCredentials> {
    let mut creds = ConnectionCredentials::default().set_process_id(std::process::id());

    #[cfg(unix)]
    {
        use rustix::process::{getegid, geteuid};

        creds = creds
            .set_unix_user_id(geteuid().as_raw())
            .add_unix_group_id(getegid().as_raw());
    }
    #[cfg(windows)]
    {
        let sid = crate::win32::ProcessToken::open(None)?.sid()?;
        creds = creds.set_windows_sid(sid);
    }

    Ok(creds)
}

const CHANNEL_CAPACITY: NonZeroUsize = NonZeroUsize::new(32).unwrap();
