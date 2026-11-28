//! A server that does its own I/O, as a bus does, with only the part of the D-Bus API of zbus that
//! needs no connection (its `core` and `core-p2p` features).
//!
//! The server reads and writes a unix socket itself, along with the file descriptors that come
//! with the messages. It finds out who its client is with `ConnectionCredentials::from_socket`,
//! does the authentication handshake with `connection::handshake::Command`, tells where each
//! message ends with `PrimaryHeader::parse_with_len`, makes a message of its bytes and of the file
//! descriptors that its header counts with `Message::from_parts`, and builds its replies with the
//! message builders.
//!
//! It is the bus of its one client. As a bus does, it answers `Hello` with [`UNIQUE_NAME`] and then
//! sends `NameAcquired`, and it answers `Peer.Ping` and `GetConnectionCredentials`. It also answers
//! `Echo` of [`ECHO_INTERFACE`], which hands back the file descriptor that it is given, so that
//! file descriptors go both ways. Its test has a zbus connection talk to it.
#![cfg(unix)]

use std::{
    io::{self, IoSlice, IoSliceMut},
    mem::{self, MaybeUninit},
    os::{
        fd::{AsFd, BorrowedFd, OwnedFd},
        unix::net::UnixStream,
    },
};

use rustix::net::{
    RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SendAncillaryBuffer,
    SendAncillaryMessage, SendFlags, recvmsg, sendmsg,
};
use zbus::{
    Fd, Guid,
    connection::handshake::{AuthMechanism, Command},
    fdo::ConnectionCredentials,
    message::{Message, PrimaryHeader, Type},
};

/// Serves the client at the other end of `socket`, until it hangs up.
pub fn serve(socket: UnixStream) -> zbus::Result<()> {
    let credentials = ConnectionCredentials::from_socket(&socket)?;
    let Some(uid) = credentials.unix_user_id() else {
        return Err(handshake_error("the socket tells no user ID"));
    };
    let mut client = Client {
        socket,
        bytes: Vec::new(),
        fds: Vec::new(),
        fd_passing: false,
    };

    client.authenticate(uid)?;
    while let Some(msg) = client.next_message()? {
        for reply in answer(&msg, &credentials)? {
            client.send(&reply)?;
        }
    }

    Ok(())
}

/// The messages that the server sends in answer to `msg`, as the bus of a client with
/// `credentials`.
fn answer(msg: &Message, credentials: &ConnectionCredentials) -> zbus::Result<Vec<Message>> {
    if msg.message_type() != Type::MethodCall {
        return Ok(Vec::new());
    }
    let header = msg.header();
    let reply = || {
        Message::method_return(&header)
            .sender(BUS)
            .destination(UNIQUE_NAME)
    };
    let interface = header.interface().map(|i| i.as_str());
    let member = header.member().map(|m| m.as_str());

    Ok(match (interface, member) {
        (Some(BUS), Some("Hello")) => vec![
            reply().build(&UNIQUE_NAME)?,
            Message::signal(BUS_PATH, BUS, "NameAcquired")
                .sender(BUS)
                .destination(UNIQUE_NAME)
                .build(&UNIQUE_NAME)?,
        ],
        (Some(BUS), Some("GetConnectionCredentials")) => {
            let body = msg.body();
            let name: &str = body.deserialize()?;
            if name != UNIQUE_NAME {
                return error(msg, "org.freedesktop.DBus.Error.NameHasNoOwner");
            }

            vec![reply().build(credentials)?]
        }
        (Some("org.freedesktop.DBus.Peer"), Some("Ping")) => vec![reply().build(&())?],
        (Some(ECHO_INTERFACE), Some("Echo")) => {
            let body = msg.body();
            let fd: Fd<'_> = body.deserialize()?;

            vec![reply().build(&fd)?]
        }
        _ => return error(msg, "org.freedesktop.DBus.Error.UnknownMethod"),
    })
}

/// The error of `name` in answer to `msg`.
fn error(msg: &Message, name: &str) -> zbus::Result<Vec<Message>> {
    let error = Message::error(&msg.header(), name)
        .sender(BUS)
        .destination(UNIQUE_NAME)
        .build(&"The server of the fixture has no answer to this")?;

    Ok(vec![error])
}

/// The client at the other end of a socket, and what it sent that is not taken yet.
struct Client {
    socket: UnixStream,
    /// The bytes that came in, out of which the commands and messages are taken.
    bytes: Vec<u8>,
    /// The file descriptors that came in, oldest first, for the messages to take.
    fds: Vec<OwnedFd>,
    /// Whether the client asked to pass file descriptors, and the server agreed.
    fd_passing: bool,
}

impl Client {
    /// The server side of the authentication handshake, with `EXTERNAL` as the user `uid` that the
    /// socket told, as dbus-broker does.
    fn authenticate(&mut self, uid: u32) -> zbus::Result<()> {
        // The byte that the client sends before its first command, which isn't part of it.
        if self.bytes.is_empty() && self.recv(1)? == 0 {
            return Err(handshake_error("the client went before the handshake"));
        }
        if self.bytes.remove(0) != 0 {
            return Err(handshake_error("no 0 byte ahead of the first command"));
        }
        let mut authenticated = false;
        loop {
            let line = self.line()?;
            let answer = match line.parse::<Command>() {
                Ok(Command::Auth(Some(AuthMechanism::External), Some(id)))
                    if !authenticated && id == uid.to_string().as_bytes() =>
                {
                    authenticated = true;

                    Command::Ok(Guid::generate().into())
                }
                // `AUTH` with a mechanism that zbus doesn't know, which fails to parse, with
                // another mechanism, or with the wrong user.
                Ok(Command::Auth(..)) | Err(_) if !authenticated && line.starts_with("AUTH") => {
                    Command::Rejected(AuthMechanism::External.as_str().into())
                }
                Ok(Command::NegotiateUnixFD) if authenticated => {
                    self.fd_passing = true;

                    Command::AgreeUnixFD
                }
                // What follows `BEGIN` is the first message.
                Ok(Command::Begin) if authenticated => return Ok(()),
                _ => Command::Error("Unsupported or misplaced command".into()),
            };
            self.write(format!("{answer}\r\n").as_bytes(), &[])?;
        }
    }

    /// The next line of the handshake, without the `\r\n` that ends it.
    fn line(&mut self) -> zbus::Result<String> {
        loop {
            if let Some(end) = self.bytes.windows(2).position(|pair| pair == b"\r\n") {
                let line = self.bytes.drain(..end + 2).take(end).collect();

                return String::from_utf8(line).map_err(|_| handshake_error("a line isn't UTF-8"));
            }
            if self.bytes.len() > MAX_LINE {
                return Err(handshake_error("a line is too long"));
            }
            // The client sends the commands of the handshake in one go, and its first message
            // right after them, so this read may take part of that message.
            if self.recv(MAX_LINE)? == 0 {
                return Err(handshake_error("the client went in the handshake"));
            }
        }
    }

    /// The next message from the client, once it is whole, or `None` once the client hung up.
    fn next_message(&mut self) -> zbus::Result<Option<Message>> {
        loop {
            // Read up to the end of the message and no further, so that the message keeps the
            // allocation that it was read into. Read into an allocation of its exact length, and
            // into room for at most `READ_MAX` bytes of it at a time.
            let have = self.bytes.len();
            let want = if have < MIN_MESSAGE_SIZE {
                MIN_MESSAGE_SIZE
            } else {
                let (_, len) = PrimaryHeader::parse_with_len(&self.bytes)?;
                if have >= len {
                    // Only the read of the handshake reads past the end of a message, and nothing
                    // is allocated when nothing follows the message.
                    let rest = self.bytes.split_off(len);
                    let bytes = mem::replace(&mut self.bytes, rest);

                    return Message::from_parts(bytes, &mut self.fds).map(Some);
                }

                len
            };
            self.bytes.reserve_exact(want - have);
            if self.recv((want - have).min(READ_MAX))? == 0 {
                if have > 0 {
                    let eof = io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the client went in the middle of a message",
                    );

                    return Err(eof.into());
                }

                return Ok(None);
            }
        }
    }

    /// Reads up to `max` bytes, with the file descriptors that come with them, and tells how many
    /// bytes came in: 0 once the client hung up.
    fn recv(&mut self, max: usize) -> io::Result<usize> {
        let have = self.bytes.len();
        self.bytes.resize(have + max, 0);
        let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS))];
        let mut control = RecvAncillaryBuffer::new(&mut space);
        let mut buffers = [IoSliceMut::new(&mut self.bytes[have..])];
        let received = recvmsg(&self.socket, &mut buffers, &mut control, RECV_FLAGS);
        let read = received.as_ref().map_or(0, |received| received.bytes);
        self.bytes.truncate(have + read);
        received?;
        for msg in control.drain() {
            if let RecvAncillaryMessage::ScmRights(fds) = msg {
                self.fds.extend(fds);
            }
        }

        Ok(read)
    }

    /// Writes all of `msg`, with its file descriptors sent along with its first byte.
    fn send(&mut self, msg: &Message) -> zbus::Result<()> {
        let data = msg.data();
        let fds: Vec<_> = data.fds().iter().map(AsFd::as_fd).collect();
        if !fds.is_empty() && !self.fd_passing {
            return Err(zbus::Error::Unsupported);
        }

        Ok(self.write(data.bytes(), &fds)?)
    }

    /// Writes all of `bytes`, with `fds` sent along with the first of them.
    fn write(&mut self, bytes: &[u8], fds: &[BorrowedFd<'_>]) -> io::Result<()> {
        let mut written = 0;
        while written < bytes.len() {
            let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(MAX_FDS))];
            let mut control = SendAncillaryBuffer::new(&mut space);
            if written == 0
                && !fds.is_empty()
                && !control.push(SendAncillaryMessage::ScmRights(fds))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "too many file descriptors",
                ));
            }
            let buffers = [IoSlice::new(&bytes[written..])];
            written += sendmsg(&self.socket, &buffers, &mut control, SEND_FLAGS)?;
        }

        Ok(())
    }
}

/// The error of a handshake that failed for `reason`.
fn handshake_error(reason: &str) -> zbus::Error {
    zbus::Error::Handshake(reason.to_string())
}

/// The unique name that the server gives its client.
pub const UNIQUE_NAME: &str = ":own_io.1";

/// The interface of `Echo`, which hands back the file descriptor that it is given.
pub const ECHO_INTERFACE: &str = "org.zbus.OwnIo";

/// The name of the bus.
const BUS: &str = "org.freedesktop.DBus";

/// The path of the bus.
const BUS_PATH: &str = "/org/freedesktop/DBus";

/// The maximum length of a line of the handshake.
const MAX_LINE: usize = 1024;

/// The size of the start of a message that tells its length, which `PrimaryHeader::parse_with_len`
/// reads: the primary header, and the length of the header fields. No message is shorter.
const MIN_MESSAGE_SIZE: usize = 16;

/// The most bytes of a message that one read makes room for, as in zbus's own reader. The room is
/// zero-filled before each read, however much comes, and a header can announce a message of up to
/// 128 MiB.
const READ_MAX: usize = 64 * 1024;

/// The most file descriptors that come with one read: the most that Linux passes in one write
/// (`SCM_MAX_FD`).
const MAX_FDS: usize = 253;

/// The flags of each read: the file descriptors that come with it are closed on `exec`, where the
/// platform has the flag for that.
#[cfg(not(target_vendor = "apple"))]
const RECV_FLAGS: RecvFlags = RecvFlags::CMSG_CLOEXEC;
#[cfg(target_vendor = "apple")]
const RECV_FLAGS: RecvFlags = RecvFlags::empty();

/// The flags of each write: a write to a client that hung up fails, rather than raising a signal,
/// where the platform has the flag for that.
#[cfg(not(target_vendor = "apple"))]
const SEND_FLAGS: SendFlags = SendFlags::NOSIGNAL;
#[cfg(target_vendor = "apple")]
const SEND_FLAGS: SendFlags = SendFlags::empty();
