use std::{borrow::Cow, fmt, str::FromStr};

use super::AuthMechanism;
use crate::{Error, Guid, OwnedGuid, Result};

/// A command of the authentication protocol, a line of the handshake.
///
/// A command parses from a line ([`FromStr`]) and is written as one ([`Display`]), without the
/// `\r\n` that ends each line of the handshake: a program that does its own I/O takes that off a
/// line as it reads a command, and adds it as it writes one. The data that a command holds, such
/// as the initial response of `AUTH`, is hex-encoded in the line.
///
/// Writing a command and parsing it again gives the same command, but for empty data, which is
/// written as none, for the response of `AUTH` with no mechanism, which the protocol has no line
/// for, and for the whitespace in the text of `ERROR` and `REJECTED`, which is parsed as single
/// spaces between its words.
///
/// [`Display`]: fmt::Display
///
/// # Example
///
/// ```
/// use zbus::{
///     Guid,
///     sasl::{AuthMechanism, Command},
/// };
///
/// // What a client sends first, to be authenticated as the user whose ID is 1000.
/// let auth: Command = "AUTH EXTERNAL 31303030".parse()?;
/// let Command::Auth(Some(AuthMechanism::External), Some(id)) = &auth else {
///     panic!("not the command of the example: {auth}");
/// };
/// assert_eq!(id, b"1000");
///
/// // What a server answers once it knows that its peer is that user.
/// let guid = Guid::try_from("0123456789abcdef0123456789abcdef")?;
/// let ok = Command::Ok(guid.into());
/// assert_eq!(format!("{ok}\r\n"), "OK 0123456789abcdef0123456789abcdef\r\n");
/// # Ok::<(), zbus::Error>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Command {
    /// `AUTH`, from the client: the mechanism to authenticate with, and the initial response of
    /// the client in it, if any.
    ///
    /// Without a mechanism, `AUTH` asks the server for those that it supports, which it answers
    /// with `REJECTED`.
    Auth(Option<AuthMechanism>, Option<Vec<u8>>),
    /// `CANCEL`, from the client: the authentication under way stops.
    Cancel,
    /// `BEGIN`, from the client, once it is authenticated: the handshake ends, and messages
    /// follow.
    Begin,
    /// `DATA`, from either end: the data of the mechanism under way, if any.
    Data(Option<Vec<u8>>),
    /// `ERROR`, from either end: what went wrong, an explanation for people to read, which may be
    /// empty.
    Error(String),
    /// `NEGOTIATE_UNIX_FD`, from the client, once it is authenticated: it asks to pass file
    /// descriptors.
    NegotiateUnixFD,
    /// `REJECTED`, from the server: the mechanisms that it supports, which spaces separate.
    Rejected(Cow<'static, str>),
    /// `OK`, from the server: the client is authenticated, to the server of this GUID.
    Ok(OwnedGuid),
    /// `AGREE_UNIX_FD`, from the server: file descriptors can be passed.
    AgreeUnixFD,
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Auth(Some(mech), Some(resp)) if !resp.is_empty() => {
                write!(f, "AUTH {mech} {}", hex::encode(resp))
            }
            Command::Auth(Some(mech), _) => write!(f, "AUTH {mech}"),
            Command::Auth(None, _) => write!(f, "AUTH"),
            Command::Cancel => write!(f, "CANCEL"),
            Command::Begin => write!(f, "BEGIN"),
            Command::Data(Some(data)) if !data.is_empty() => {
                write!(f, "DATA {}", hex::encode(data))
            }
            Command::Data(_) => write!(f, "DATA"),
            Command::Error(expl) if expl.is_empty() => write!(f, "ERROR"),
            Command::Error(expl) => write!(f, "ERROR {expl}"),
            Command::NegotiateUnixFD => write!(f, "NEGOTIATE_UNIX_FD"),
            Command::Rejected(mechs) if mechs.is_empty() => write!(f, "REJECTED"),
            Command::Rejected(mechs) => write!(f, "REJECTED {mechs}"),
            Command::Ok(guid) => write!(f, "OK {guid}"),
            Command::AgreeUnixFD => write!(f, "AGREE_UNIX_FD"),
        }
    }
}

impl FromStr for Command {
    type Err = Error;

    /// The command of `s`, a line of the handshake without the `\r\n` that ends it, whose words
    /// whitespace separates.
    ///
    /// A line that holds no command of the protocol, or data that isn't valid hex, is an
    /// [`Error::Handshake`], and so is `AUTH` with a mechanism that zbus doesn't support. `OK` with
    /// no GUID, or one that isn't valid, is an error too.
    fn from_str(s: &str) -> Result<Self> {
        let mut words = s.split_ascii_whitespace();
        let cmd = match words.next() {
            Some("AUTH") => {
                let mech = if let Some(m) = words.next() {
                    Some(m.parse()?)
                } else {
                    None
                };
                let resp = match words.next() {
                    Some(resp) => Some(decode_hex(resp)?),
                    None => None,
                };
                Command::Auth(mech, resp)
            }
            Some("CANCEL") => Command::Cancel,
            Some("BEGIN") => Command::Begin,
            Some("DATA") => {
                let data = match words.next() {
                    Some(data) => Some(decode_hex(data)?),
                    None => None,
                };

                Command::Data(data)
            }
            Some("ERROR") => Command::Error(words.collect::<Vec<_>>().join(" ")),
            Some("NEGOTIATE_UNIX_FD") => Command::NegotiateUnixFD,
            Some("REJECTED") => {
                let mechs = words.collect::<Vec<_>>().join(" ").into();
                Command::Rejected(mechs)
            }
            Some("OK") => {
                let guid = words
                    .next()
                    .ok_or_else(|| Error::Handshake("Missing OK server GUID!".into()))?;
                Command::Ok(Guid::from_str(guid)?.into())
            }
            Some("AGREE_UNIX_FD") => Command::AgreeUnixFD,
            _ => return Err(Error::Handshake(format!("Unknown command: {s}"))),
        };
        Ok(cmd)
    }
}

fn decode_hex(s: &str) -> Result<Vec<u8>> {
    hex::decode(s).map_err(|e| Error::Handshake(format!("Invalid hexcode: {e}")))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::Command;
    use crate::{Error, Guid, sasl::AuthMechanism};

    /// Every command is the same once written and parsed again.
    #[test]
    fn round_trips() {
        let guid = Guid::try_from("0123456789abcdef0123456789abcdef").unwrap();
        let commands = [
            (
                Command::Auth(Some(AuthMechanism::External), Some(b"1000".to_vec())),
                "AUTH EXTERNAL 31303030",
            ),
            (
                Command::Auth(Some(AuthMechanism::Anonymous), None),
                "AUTH ANONYMOUS",
            ),
            (Command::Auth(None, None), "AUTH"),
            (Command::Cancel, "CANCEL"),
            (Command::Begin, "BEGIN"),
            (Command::Data(Some(vec![0, 0xfe, 7])), "DATA 00fe07"),
            (Command::Data(None), "DATA"),
            (
                Command::Error("Unsupported or misplaced command".into()),
                "ERROR Unsupported or misplaced command",
            ),
            (Command::Error(String::new()), "ERROR"),
            (Command::NegotiateUnixFD, "NEGOTIATE_UNIX_FD"),
            (
                Command::Rejected("EXTERNAL ANONYMOUS".into()),
                "REJECTED EXTERNAL ANONYMOUS",
            ),
            (Command::Rejected("".into()), "REJECTED"),
            (
                Command::Ok(guid.into()),
                "OK 0123456789abcdef0123456789abcdef",
            ),
            (Command::AgreeUnixFD, "AGREE_UNIX_FD"),
        ];

        for (command, line) in commands {
            assert_eq!(command.to_string(), line);
            assert_eq!(Command::from_str(line).unwrap(), command, "{line}");
        }
    }

    /// What has no line of its own in the protocol is written as the line closest to it: empty
    /// data as none, and the response of `AUTH` with no mechanism not at all. The whitespace in the
    /// text of `ERROR` is parsed as single spaces between its words.
    #[test]
    fn written_as_the_closest_line() {
        let commands = [
            (Command::Data(Some(vec![])), "DATA", Command::Data(None)),
            (
                Command::Auth(Some(AuthMechanism::External), Some(vec![])),
                "AUTH EXTERNAL",
                Command::Auth(Some(AuthMechanism::External), None),
            ),
            (
                Command::Auth(None, Some(b"1000".to_vec())),
                "AUTH",
                Command::Auth(None, None),
            ),
            (
                Command::Error(" two  spaces ".into()),
                "ERROR  two  spaces ",
                Command::Error("two spaces".into()),
            ),
        ];

        for (command, line, parsed) in commands {
            assert_eq!(command.to_string(), line);
            assert_eq!(Command::from_str(line).unwrap(), parsed, "{line}");
        }
    }

    /// A line is parsed whatever whitespace separates its words, and whatever the case of its
    /// hex.
    #[test]
    fn parsed_whatever_the_whitespace() {
        let auth = Command::Auth(Some(AuthMechanism::External), Some(vec![0xab, 0xcd]));
        for line in ["AUTH EXTERNAL abcd", "  AUTH\tEXTERNAL  ABCD \r\n"] {
            assert_eq!(Command::from_str(line).unwrap(), auth, "{line:?}");
        }
    }

    /// A line that holds no command that zbus knows, or that holds one wrong, is an error, `AUTH`
    /// with a mechanism that zbus doesn't support among them.
    #[test]
    fn unknown_or_invalid() {
        for line in [
            "",
            "HELLO",
            "auth EXTERNAL",
            "AUTH DBUS_COOKIE_SHA1 31303030",
            "AUTH EXTERNAL 3",
            "DATA xyz",
            "OK",
        ] {
            let error = Command::from_str(line).unwrap_err();
            assert!(matches!(error, Error::Handshake(_)), "{line:?}: {error:?}");
        }
        let error = Command::from_str("OK 0123").unwrap_err();
        assert_eq!(error, Error::InvalidGUID);
    }
}
