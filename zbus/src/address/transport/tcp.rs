use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use super::{Stream, encode_percents};
use crate::{
    Address, Error, Result,
    connection::socket::{BoxedSplit, WriteHalf},
    runtime::{
        Runtime,
        io::{RegisteredIo, TcpOps},
    },
};

/// A TCP transport in a D-Bus address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tcp {
    pub(super) host: String,
    pub(super) bind: Option<String>,
    pub(super) port: u16,
    pub(super) family: Option<TcpTransportFamily>,
    pub(super) nonce_file: Option<Vec<u8>>,
}

impl Tcp {
    /// Create a new TCP transport with the given host and port.
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_owned(),
            port,
            bind: None,
            family: None,
            nonce_file: None,
        }
    }

    /// Set the `tcp:` address `bind` value.
    pub fn set_bind(mut self, bind: Option<String>) -> Self {
        self.bind = bind;

        self
    }

    /// Set the `tcp:` address `family` value.
    pub fn set_family(mut self, family: Option<TcpTransportFamily>) -> Self {
        self.family = family;

        self
    }

    /// Set the `tcp:` address `noncefile` value.
    pub fn set_nonce_file(mut self, nonce_file: Option<Vec<u8>>) -> Self {
        self.nonce_file = nonce_file;

        self
    }

    /// The `tcp:` address `host` value.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The `tcp:` address `bind` value.
    pub fn bind(&self) -> Option<&str> {
        self.bind.as_deref()
    }

    /// The `tcp:` address `port` value.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The `tcp:` address `family` value.
    pub fn family(&self) -> Option<TcpTransportFamily> {
        self.family
    }

    /// The nonce file path, if any.
    pub fn nonce_file(&self) -> Option<&[u8]> {
        self.nonce_file.as_deref()
    }

    /// Take ownership of the nonce file path, if any.
    pub fn take_nonce_file(&mut self) -> Option<Vec<u8>> {
        self.nonce_file.take()
    }

    pub(super) fn from_options(
        opts: HashMap<&str, &str>,
        nonce_tcp_required: bool,
    ) -> Result<Self> {
        let bind = None;
        if opts.contains_key("bind") {
            return Err(Error::Address("`bind` isn't yet supported".into()));
        }

        let host = opts
            .get("host")
            .ok_or_else(|| Error::Address("tcp address is missing `host`".into()))?
            .to_string();
        let port = opts
            .get("port")
            .ok_or_else(|| Error::Address("tcp address is missing `port`".into()))?;
        let port = port
            .parse::<u16>()
            .map_err(|_| Error::Address("invalid tcp `port`".into()))?;
        let family = opts
            .get("family")
            .map(|f| TcpTransportFamily::from_str(f))
            .transpose()?;
        let nonce_file = opts
            .get("noncefile")
            .map(|f| super::decode_percents(f))
            .transpose()?;
        if nonce_tcp_required && nonce_file.is_none() {
            return Err(Error::Address(
                "nonce-tcp address is missing `noncefile`".into(),
            ));
        }

        Ok(Self {
            host,
            bind,
            port,
            family,
            nonce_file,
        })
    }

    /// Connects to this address, passing the nonce it names along where there is one.
    pub(super) async fn connect(&self, address: &Address, runtime: &Runtime) -> Result<Stream> {
        let nonce = match self.nonce_file() {
            Some(path) => Some(read_nonce(path, runtime).await?),
            None => None,
        };
        let addresses = self.resolve(address, runtime).await?;
        let mut error = self.nothing_found(address);

        for socket_address in addresses {
            match runtime.connect_tcp(socket_address).await {
                Ok(source) => {
                    #[cfg(all(target_os = "linux", feature = "handover"))]
                    let kept = source.clone();
                    let mut split = BoxedSplit::from(RegisteredIo::new(runtime, source, TcpOps)?);
                    if let Some(nonce) = nonce.as_deref() {
                        write_all(split.write_mut(), nonce).await?;
                    }

                    return Ok(Stream::Tcp {
                        split,
                        #[cfg(all(target_os = "linux", feature = "handover"))]
                        source: kept,
                    });
                }
                Err(e) => error = Error::Connection(Arc::new(e), Box::new(address.clone())),
            }
        }

        Err(error)
    }

    /// The socket addresses this one names, in the order they should be tried.
    ///
    /// A host written as an IP address is one already. Anything else is a name for the resolver,
    /// which goes to `runtime` because looking it up blocks.
    async fn resolve(&self, address: &Address, runtime: &Runtime) -> Result<Vec<SocketAddr>> {
        let port = self.port();
        let addresses = match self.host().parse::<IpAddr>() {
            Ok(ip) => vec![SocketAddr::new(ip, port)],
            Err(_) => {
                let host = self.host().to_owned();

                runtime
                    .spawn_blocking(move || {
                        (host.as_str(), port).to_socket_addrs().map(Vec::from_iter)
                    })
                    .await
                    .map_err(|e| Error::Connection(Arc::new(e), Box::new(address.clone())))?
            }
        };

        Ok(match self.family() {
            Some(TcpTransportFamily::Ipv4) => {
                addresses.into_iter().filter(SocketAddr::is_ipv4).collect()
            }
            Some(TcpTransportFamily::Ipv6) => {
                addresses.into_iter().filter(SocketAddr::is_ipv6).collect()
            }
            None => addresses,
        })
    }

    /// The error a connection attempt that found nowhere to go reports.
    fn nothing_found(&self, address: &Address) -> Error {
        Error::Address(match self.family() {
            Some(family) => format!("no `{family}` addresses found for `{address}`"),
            None => format!("no addresses found for `{address}`"),
        })
    }
}

/// The nonce the server expects before anything else, read off `path`.
async fn read_nonce(path: &[u8], runtime: &Runtime) -> Result<Vec<u8>> {
    let path = nonce_path(path)?;

    runtime
        .spawn_blocking(move || std::fs::read(path))
        .await
        .map_err(Into::into)
}

/// `path`, as this platform names files.
fn nonce_path(path: &[u8]) -> Result<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;

        Ok(PathBuf::from(std::ffi::OsStr::from_bytes(path)))
    }
    // Windows names a file in UTF-16, and preview 2 of WASI in a string, of which std has no
    // stable way to make a path from bytes: so the bytes have to be UTF-8 off unix.
    #[cfg(not(unix))]
    {
        std::str::from_utf8(path)
            .map(PathBuf::from)
            .map_err(|_| Error::Address("nonce file path is invalid UTF-8".to_owned()))
    }
}

/// Writes the whole of `bytes` to `half`, however many writes that takes.
async fn write_all(half: &mut impl WriteHalf, bytes: &[u8]) -> Result<()> {
    let mut rest = bytes;

    while !rest.is_empty() {
        let written = half
            .sendmsg(
                rest,
                #[cfg(unix)]
                &[],
            )
            .await?;
        rest = &rest[written..];
    }

    Ok(())
}

impl Display for Tcp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.nonce_file() {
            Some(nonce_file) => {
                f.write_str("nonce-tcp:noncefile=")?;
                encode_percents(f, nonce_file)?;
                f.write_str(",")?;
            }
            None => f.write_str("tcp:")?,
        }
        f.write_str("host=")?;

        encode_percents(f, self.host().as_bytes())?;

        write!(f, ",port={}", self.port())?;

        if let Some(bind) = self.bind() {
            f.write_str(",bind=")?;
            encode_percents(f, bind.as_bytes())?;
        }

        if let Some(family) = self.family() {
            write!(f, ",family={family}")?;
        }

        Ok(())
    }
}

/// A `tcp:` address family.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TcpTransportFamily {
    /// IPv4, the `ipv4` value of the `family` key of a `tcp:` address.
    Ipv4,
    /// IPv6, the `ipv6` value of the `family` key of a `tcp:` address.
    Ipv6,
}

impl FromStr for TcpTransportFamily {
    type Err = Error;

    fn from_str(family: &str) -> Result<Self> {
        match family {
            "ipv4" => Ok(Self::Ipv4),
            "ipv6" => Ok(Self::Ipv6),
            _ => Err(Error::Address(format!(
                "invalid tcp address `family`: {family}"
            ))),
        }
    }
}

impl Display for TcpTransportFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ipv4 => write!(f, "ipv4"),
            Self::Ipv6 => write!(f, "ipv6"),
        }
    }
}
