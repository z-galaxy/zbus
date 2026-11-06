use async_trait::async_trait;

use crate::{
    Message,
    conn::socket::ReadHalf,
    log::{trace, warn},
    names::OwnedUniqueName,
};

use super::{
    AuthMechanism, Authenticated, BoxedSplit, Command, Common, Error, Handshake, OwnedGuid, Result,
    sasl_auth_id,
};

/// A representation of an in-progress handshake, client-side
///
/// This struct is an async-compatible representation of the initial handshake that must be
/// performed before a D-Bus connection can be used.
#[derive(Debug)]
pub struct Client {
    common: Common,
    server_guid: Option<OwnedGuid>,
    bus: bool,
    user_id: Result<String>,
}

impl Client {
    /// Start a handshake on this client socket
    pub fn new(
        socket: BoxedSplit,
        mechanism: Option<AuthMechanism>,
        server_guid: Option<OwnedGuid>,
        bus: bool,
        user_id: Option<u32>,
    ) -> Client {
        let mechanism = mechanism.unwrap_or_else(|| socket.read().auth_mechanism());

        Client {
            common: Common::new(socket, mechanism),
            server_guid,
            bus,
            user_id: match user_id {
                Some(value) => Ok(value.to_string()),
                None => sasl_auth_id(),
            },
        }
    }

    fn set_guid(&mut self, guid: OwnedGuid) -> Result<()> {
        match &self.server_guid {
            Some(server_guid) if *server_guid != guid => {
                return Err(Error::Handshake(format!(
                    "Server GUID mismatch: expected {server_guid}, got {guid}",
                )));
            }
            Some(_) => (),
            None => self.server_guid = Some(guid),
        }

        Ok(())
    }

    // The dbus daemon on some platforms requires sending the zero byte as a
    // separate message with SCM_CREDS.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "trace"))]
    #[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
    async fn send_zero_byte(&mut self) -> Result<()> {
        let write = self.common.socket_mut().write_mut();

        let written = match write.send_zero_byte().await.map_err(|e| {
            Error::Handshake(format!("Could not send zero byte with credentials: {e}"))
        })? {
            // This likely means that the socket type is unable to send SCM_CREDS.
            // Let's try to send the 0 byte as a regular message.
            None => write.sendmsg(&[0], &[]).await?,
            Some(n) => n,
        };

        if written != 1 {
            return Err(Error::Handshake(
                "Could not send zero byte with credentials".to_string(),
            ));
        }

        Ok(())
    }

    /// Sends out every command of the handshake at once, followed by the `Hello` call on a bus
    /// connection.
    ///
    /// Nothing the client says depends on how the server answers, so it all goes in one write
    /// and the answers are read afterwards. A server that rejects the mechanism answers `AUTH` with
    /// `REJECTED` and may drop the connection over what follows, which fails the handshake as
    /// waiting for its answer would have.
    ///
    /// Returns how many answers the server gives to the commands after `AUTH`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "trace"))]
    async fn send_commands(&mut self) -> Result<usize> {
        let mechanism = self.common.mechanism();
        trace!("Trying {mechanism} mechanism");
        let user_id = self.user_id.clone();
        let auth_cmd = match mechanism {
            AuthMechanism::Anonymous => Command::Auth(Some(mechanism), Some("zbus".into())),
            AuthMechanism::External => Command::Auth(Some(mechanism), Some(user_id?.into_bytes())),
        };
        let mut commands = Vec::with_capacity(3);
        commands.push(auth_cmd);

        let can_pass_fd = self.common.socket_mut().read_mut().can_pass_unix_fd();
        if can_pass_fd {
            commands.push(Command::NegotiateUnixFD);
        }
        commands.push(Command::Begin);
        let hello_method = if self.bus {
            Some(create_hello_method_call())
        } else {
            None
        };

        self.common
            .write_commands(&commands, hello_method.as_ref().map(|m| &**m.data()))
            .await?;

        // Server replies to all commands except `BEGIN`, and the reply to `AUTH` is read on its
        // own.
        Ok(commands.len() - 2)
    }

    /// Reads the server's answer to `AUTH`.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "trace"))]
    async fn receive_auth_response(&mut self) -> Result<()> {
        let mechanism = self.common.mechanism();

        match self.common.read_command().await? {
            Command::Ok(guid) => {
                trace!("Received OK from server");
                self.set_guid(guid)?;

                Ok(())
            }
            Command::Rejected(accepted) => {
                let list = accepted.replace(" ", ", ");
                Err(Error::Handshake(format!(
                    "{mechanism} rejected by the server. Accepted mechanisms: [{list}]"
                )))
            }
            Command::Error(e) => Err(Error::Handshake(format!("Received error from server: {e}"))),
            cmd => Err(Error::Handshake(format!(
                "Unexpected command from server: {cmd}"
            ))),
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "trace"))]
    async fn receive_secondary_responses(&mut self, expected_n_responses: usize) -> Result<()> {
        for response in self.common.read_commands(expected_n_responses).await? {
            match response {
                Command::Ok(guid) => {
                    trace!("Received OK from server");
                    self.set_guid(guid)?;
                }
                Command::AgreeUnixFD => self.common.set_cap_unix_fd(true),
                Command::Error(e) => warn!("UNIX file descriptor passing rejected: {e}"),
                cmd => {
                    return Err(Error::Handshake(format!(
                        "Unexpected command from server: {cmd}"
                    )));
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Handshake for Client {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "trace"))]
    async fn perform(mut self) -> Result<Authenticated> {
        trace!("Initializing");

        #[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
        self.send_zero_byte().await?;

        let expected_n_responses = self.send_commands().await?;
        self.receive_auth_response().await?;

        if expected_n_responses > 0 {
            self.receive_secondary_responses(expected_n_responses)
                .await?;
        }

        trace!("Handshake done");
        #[cfg(unix)]
        let (socket, mut recv_buffer, received_fds, cap_unix_fd, _) = self.common.into_components();
        #[cfg(not(unix))]
        let (socket, mut recv_buffer, _, _) = self.common.into_components();
        let (mut read, write) = socket.take();

        // If we're a bus connection, we need to read the unique name from `Hello` response.
        let unique_name = if self.bus {
            let unique_name = receive_hello_response(&mut read, &mut recv_buffer).await?;

            Some(unique_name)
        } else {
            None
        };

        Ok(Authenticated {
            socket_write: write,
            socket_read: Some(read),
            server_guid: self.server_guid.unwrap(),
            #[cfg(unix)]
            cap_unix_fd,
            already_received_bytes: recv_buffer,
            #[cfg(unix)]
            already_received_fds: received_fds,
            unique_name,
            peer_credentials: None,
        })
    }
}

fn create_hello_method_call() -> Message {
    Message::method_call("/org/freedesktop/DBus", "Hello")
        .destination("org.freedesktop.DBus")
        .interface("org.freedesktop.DBus")
        .build(&())
        .unwrap()
}

async fn receive_hello_response(
    read: &mut Box<dyn ReadHalf>,
    recv_buffer: &mut Vec<u8>,
) -> Result<OwnedUniqueName> {
    use crate::message::Type;

    let reply = read
        .receive_message(
            0,
            recv_buffer,
            #[cfg(unix)]
            &mut vec![],
        )
        .await?;
    match reply.message_type() {
        Type::MethodReturn => reply.body().deserialize(),
        Type::Error => Err(Error::from(reply)),
        m => Err(Error::Handshake(format!("Unexpected message `{m:?}`"))),
    }
}
