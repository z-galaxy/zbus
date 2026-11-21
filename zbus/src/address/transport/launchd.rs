use std::collections::HashMap;
#[cfg(feature = "comms")]
use std::process::Command;

#[cfg(feature = "comms")]
use super::{Transport, Unix, UnixSocket};
use crate::Result;
#[cfg(feature = "comms")]
use crate::runtime::{Runtime, process};

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
/// The transport properties of a launchd D-Bus address.
pub struct Launchd {
    pub(super) env: String,
}

impl Launchd {
    /// Create a new launchd D-Bus address.
    pub fn new(env: &str) -> Self {
        Self {
            env: env.to_string(),
        }
    }

    /// The path of the unix domain socket for the launchd created dbus-daemon.
    pub fn env(&self) -> &str {
        &self.env
    }

    /// Determine the actual transport details behind a launchd address.
    ///
    /// The `launchctl` command runs on `runtime`, which is also what waits for it to exit.
    #[cfg(feature = "comms")]
    pub(super) async fn bus_address(&self, runtime: &Runtime) -> Result<Transport> {
        let mut command = Command::new("launchctl");
        command.args(["getenv", self.env()]);

        let printed = process::stdout(runtime, command)
            .await
            .map_err(|e| crate::Error::Address(format!("The launchctl command failed: {e}")))?;

        let addr = String::from_utf8(printed).map_err(|e| {
            crate::Error::Address(format!("Unable to parse launchctl output as UTF-8: {e}"))
        })?;

        Ok(Transport::Unix(Unix::new(UnixSocket::File(
            addr.trim().into(),
        ))))
    }

    pub(super) fn from_options(opts: HashMap<&str, &str>) -> Result<Self> {
        opts.get("env")
            .ok_or_else(|| crate::Error::Address("missing env key".into()))
            .map(|env| Self {
                env: env.to_string(),
            })
    }
}

impl std::fmt::Display for Launchd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "launchd:env={}", self.env)
    }
}
