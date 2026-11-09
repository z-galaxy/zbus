#[cfg(target_os = "linux")]
use std::ffi::OsString;
#[cfg(any(unix, windows))]
use std::{borrow::Cow, io, sync::Arc};
use std::{
    ffi::OsStr,
    fmt::{Display, Formatter},
    path::PathBuf,
};

use super::Stream;
#[cfg(unix)]
use super::encode_percents;
#[cfg(any(unix, windows))]
use crate::runtime::io::{RegisteredIo, UnixOps};
use crate::{Address, Error, Result, runtime::Runtime};

/// A Unix domain socket transport in a D-Bus address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unix {
    path: UnixSocket,
}

impl Unix {
    /// Create a new Unix transport with the given path.
    pub fn new(path: UnixSocket) -> Self {
        Self { path }
    }

    /// The path.
    pub fn path(&self) -> &UnixSocket {
        &self.path
    }

    /// Take the path, consuming `self`.
    pub fn take_path(self) -> UnixSocket {
        self.path
    }

    /// Connects to the socket this address names.
    #[cfg(any(unix, windows))]
    pub(super) async fn connect(&self, address: &Address, runtime: &Runtime) -> Result<Stream> {
        let path = match self.path() {
            UnixSocket::File(path) => Cow::Borrowed(path.as_path()),
            #[cfg(target_os = "linux")]
            UnixSocket::Abstract(name) => Cow::Owned(abstract_path(name)),
            // A directory is where a server puts a socket of its own, not something to connect
            // to.
            UnixSocket::Dir(_) | UnixSocket::TmpDir(_) => return Err(Error::Unsupported),
        };

        // A runtime that has no unix-domain sockets, such as Tokio on Windows, answers with
        // `Unsupported`. That says nothing about this address, so it is not a failed connection.
        let source = runtime
            .connect_unix(&path)
            .await
            .map_err(|e| match e.kind() {
                io::ErrorKind::Unsupported => Error::Unsupported,
                _ => Error::Connection(Arc::new(e), Box::new(address.clone())),
            })?;

        Ok(Stream::Unix {
            #[cfg(all(target_os = "linux", feature = "handover"))]
            source: source.clone(),
            split: RegisteredIo::new(runtime, source, UnixOps)?.into(),
        })
    }

    /// Connects to the socket this address names, which WASI has no way to: it has no unix-domain
    /// sockets.
    #[cfg(target_os = "wasi")]
    pub(super) async fn connect(&self, _address: &Address, _runtime: &Runtime) -> Result<Stream> {
        Err(Error::Unsupported)
    }

    pub(super) fn from_options(opts: std::collections::HashMap<&str, &str>) -> crate::Result<Self> {
        let path = opts.get("path");
        let abs = opts.get("abstract");
        let dir = opts.get("dir");
        let tmpdir = opts.get("tmpdir");
        let path = match (path, abs, dir, tmpdir) {
            (Some(p), None, None, None) => UnixSocket::File(PathBuf::from(p)),
            #[cfg(target_os = "linux")]
            (None, Some(p), None, None) => UnixSocket::Abstract(OsString::from(p)),
            #[cfg(not(target_os = "linux"))]
            (None, Some(_), None, None) => {
                return Err(crate::Error::Address(
                    "abstract sockets currently Linux-only".to_owned(),
                ));
            }
            (None, None, Some(p), None) => UnixSocket::Dir(PathBuf::from(p)),
            (None, None, None, Some(p)) => UnixSocket::TmpDir(PathBuf::from(p)),
            _ => {
                return Err(crate::Error::Address("unix: address is invalid".to_owned()));
            }
        };

        Ok(Self::new(path))
    }
}

/// The path that names the abstract socket `name`.
///
/// A unix socket address whose path starts with a zero byte is a name in the Linux abstract
/// namespace rather than one on the filesystem.
#[cfg(target_os = "linux")]
fn abstract_path(name: &OsStr) -> PathBuf {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    let mut bytes = vec![0];
    bytes.extend_from_slice(name.as_bytes());

    PathBuf::from(OsString::from_vec(bytes))
}

impl Display for Unix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unix:{}", self.path)
    }
}

/// A Unix domain socket path in a D-Bus address.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum UnixSocket {
    /// A path to a unix domain socket on the filesystem.
    File(PathBuf),
    /// An abstract unix domain socket name.
    #[cfg(target_os = "linux")]
    Abstract(OsString),
    /// A listenable address using the specified path, in which a socket file with a random file
    /// name starting with 'dbus-' will be created by the server. See [UNIX domain socket address]
    /// reference documentation.
    ///
    /// This address is mostly relevant to server (typically bus broker) implementations.
    ///
    /// [UNIX domain socket address]: https://dbus.freedesktop.org/doc/dbus-specification.html#transports-unix-domain-sockets-addresses
    Dir(PathBuf),
    /// The same as UnixDir, except that on platforms with abstract sockets, the server may attempt
    /// to create an abstract socket whose name starts with this directory instead of a path-based
    /// socket.
    ///
    /// This address is mostly relevant to server (typically bus broker) implementations.
    TmpDir(PathBuf),
}

impl Display for UnixSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_unix_path(f: &mut Formatter<'_>, path: &OsStr) -> std::fmt::Result {
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt;

                encode_percents(f, path.as_bytes())?;
            }

            #[cfg(not(unix))]
            write!(f, "{}", path.to_str().ok_or(std::fmt::Error)?)?;

            Ok(())
        }

        match self {
            UnixSocket::File(path) => {
                f.write_str("path=")?;
                fmt_unix_path(f, path.as_os_str())?;
            }
            #[cfg(target_os = "linux")]
            UnixSocket::Abstract(name) => {
                f.write_str("abstract=")?;
                fmt_unix_path(f, name)?;
            }
            UnixSocket::Dir(path) => {
                f.write_str("dir=")?;
                fmt_unix_path(f, path.as_os_str())?;
            }
            UnixSocket::TmpDir(path) => {
                f.write_str("tmpdir=")?;
                fmt_unix_path(f, path.as_os_str())?;
            }
        }

        Ok(())
    }
}
