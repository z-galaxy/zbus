#[cfg(any(unix, not(feature = "tokio")))]
use crate::{Error, Result};

#[cfg(target_os = "linux")]
use std::ffi::OsString;
use std::{
    ffi::OsStr,
    fmt::{Display, Formatter},
    path::PathBuf,
};

#[cfg(unix)]
use super::encode_percents;

#[cfg(not(feature = "tokio"))]
use async_io::Async;

#[cfg(all(unix, not(feature = "tokio")))]
use std::os::unix::net::UnixStream;
#[cfg(all(windows, not(feature = "tokio")))]
use uds_windows::UnixStream;

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

    #[cfg(not(feature = "tokio"))]
    pub(super) async fn connect(self) -> Result<Async<UnixStream>> {
        let addr = self.take_addr()?;

        #[cfg(unix)]
        let stream = Async::<UnixStream>::connect(addr).await;

        #[cfg(not(unix))]
        let stream = Async::new(self.get_stream(addr).await?);

        stream.map_err(|e| Error::InputOutput(e.into()))
    }

    #[cfg(all(unix, feature = "tokio"))]
    pub(super) async fn connect(self) -> Result<tokio::net::UnixStream> {
        let addr = self.take_addr()?;
        tokio::net::UnixStream::connect(addr)
            .await
            .map_err(|e| Error::InputOutput(e.into()))
    }

    #[cfg(not(unix))]
    async fn get_stream(self, addr: PathBuf) -> Result<UnixStream> {
        crate::Task::spawn_blocking(
            move || -> Result<_> {
                let stream = UnixStream::connect(addr)?;
                stream.set_nonblocking(true)?;

                Ok(stream)
            },
            "unix stream connection",
        )
        .await?
    }

    #[cfg(any(unix, not(feature = "tokio")))]
    fn take_addr(self) -> Result<PathBuf> {
        // This is a `path` because neither uds_windows, tokio, nor async_io provide
        // the SocketAddrExt functions.
        match self.take_path() {
            UnixSocket::File(path) => Ok(path),
            #[cfg(target_os = "linux")]
            UnixSocket::Abstract(name) => {
                use std::os::unix::ffi::OsStringExt;

                let mut v = name.into_vec();
                v.insert(0, 0);

                Ok(PathBuf::from(OsString::from_vec(v)))
            }
            UnixSocket::Dir(_) | UnixSocket::TmpDir(_) => {
                // You can't connect to a unix:dir.
                Err(Error::Unsupported)
            }
        }
    }
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

            #[cfg(windows)]
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
