use std::{
    panic::{RefUnwindSafe, UnwindSafe},
    sync::Arc,
};

use super::{ReadHalf, Socket, WriteHalf};
use crate::runtime::io::PollSend;

/// A pair of socket read and write halves.
#[derive(Debug)]
pub struct Split<R: ReadHalf, W: WriteHalf> {
    pub(super) read: R,
    pub(super) write: W,
    /// How a connection over the halves writes the socket, if zbus made it, as [`Writes`] tells.
    pub(super) writes: Option<Writes>,
}

impl<R: ReadHalf, W: WriteHalf> Split<R, W> {
    /// Create split from read and write halves.
    pub fn new(read: R, write: W) -> Self {
        Self {
            read,
            write,
            writes: None,
        }
    }

    /// Reference to the read half.
    pub fn read(&self) -> &R {
        &self.read
    }

    /// Mutable reference to the read half.
    pub fn read_mut(&mut self) -> &mut R {
        &mut self.read
    }

    /// Reference to the write half.
    pub fn write(&self) -> &W {
        &self.write
    }

    /// Mutable reference to the write half.
    pub fn write_mut(&mut self) -> &mut W {
        &mut self.write
    }

    /// Take the read and write halves.
    pub fn take(self) -> (R, W) {
        (self.read, self.write)
    }

    /// The halves of a socket that zbus made, which a connection over them writes as `writes`
    /// tells.
    pub(crate) fn with_writes(read: R, write: W, writes: Option<Writes>) -> Self {
        Self {
            read,
            write,
            writes,
        }
    }

    /// Take out how a connection over the halves writes the socket, if zbus made it, for the
    /// connection to keep, as the halves themselves go their own ways.
    pub(crate) fn take_writes(&mut self) -> Option<Writes> {
        self.writes.take()
    }
}

/// A boxed `Split`.
pub type BoxedSplit = Split<Box<dyn ReadHalf>, Box<dyn WriteHalf>>;

impl<S: Socket> From<S> for BoxedSplit {
    fn from(socket: S) -> Self {
        let split = socket.split();

        Split {
            read: Box::new(split.read),
            write: Box::new(split.write),
            writes: split.writes,
        }
    }
}

/// How a connection writes a socket that zbus made, which it knows more of than of a socket of
/// yours, whose write half is all that it knows.
///
/// It goes with the halves wherever zbus moves them, boxed or handed over, so that a connection
/// over them knows the socket as the one that they came from did.
#[derive(Debug)]
pub(crate) enum Writes {
    /// zbus drives the socket itself, so the connection writes it directly, several messages at a
    /// time, rather than through the write half.
    Driven(Arc<dyn PollSend>),
    /// The socket is a [`Channel`](super::Channel), which the connection writes through its write
    /// half. That queues each message whole or not at all, even where the send is dropped, so no
    /// send leaves a message cut off.
    #[cfg(feature = "p2p")]
    Whole,
}

// A `Split` is as unwind safe as its halves: the socket that `Writes::Driven` holds is the one
// that the write half writes, and only the connection that runs over the halves uses it.
impl UnwindSafe for Writes {}
impl RefUnwindSafe for Writes {}
