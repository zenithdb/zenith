//! Connection configuration.

use crate::connect::connect;
use crate::connect_raw::connect_raw;
use crate::connect_raw::RawConnection;
use crate::tls::MakeTlsConnect;
use crate::tls::TlsConnect;
use crate::{Client, Connection, Error};
use postgres_protocol2::message::frontend::StartupMessageParams;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

pub use postgres_protocol2::authentication::sasl::ScramKeys;
use tokio::net::TcpStream;

/// TLS configuration.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SslMode {
    /// Do not use TLS.
    Disable,
    /// Attempt to connect with TLS but allow sessions without.
    Prefer,
    /// Require the use of TLS.
    Require,
}

/// Channel binding configuration.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChannelBinding {
    /// Do not use channel binding.
    Disable,
    /// Attempt to use channel binding but allow sessions without.
    Prefer,
    /// Require the use of channel binding.
    Require,
}

/// Replication mode configuration.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicationMode {
    /// Physical replication.
    Physical,
    /// Logical replication.
    Logical,
}

/// A host specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Host {
    /// A TCP hostname.
    Tcp(String),
}

/// Precomputed keys which may override password during auth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthKeys {
    /// A `ClientKey` & `ServerKey` pair for `SCRAM-SHA-256`.
    ScramSha256(ScramKeys<32>),
}

/// Connection configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub(crate) host: Host,
    pub(crate) port: u16,

    pub(crate) password: Option<Vec<u8>>,
    pub(crate) auth_keys: Option<Box<AuthKeys>>,
    pub(crate) ssl_mode: SslMode,
    pub(crate) connect_timeout: Option<Duration>,
    pub(crate) channel_binding: ChannelBinding,
    pub(crate) server_params: StartupMessageParams,

    database: bool,
    username: bool,
}

impl Config {
    /// Creates a new configuration.
    pub fn new(host: String, port: u16) -> Config {
        Config {
            host: Host::Tcp(host),
            port,
            password: None,
            auth_keys: None,
            ssl_mode: SslMode::Prefer,
            connect_timeout: None,
            channel_binding: ChannelBinding::Prefer,
            server_params: StartupMessageParams::default(),

            database: false,
            username: false,
        }
    }

    /// Sets the user to authenticate with.
    ///
    /// Required.
    pub fn user(&mut self, user: &str) -> &mut Config {
        self.set_param("user", user)
    }

    /// Gets the user to authenticate with, if one has been configured with
    /// the `user` method.
    pub fn user_is_set(&self) -> bool {
        self.username
    }

    /// Sets the password to authenticate with.
    pub fn password<T>(&mut self, password: T) -> &mut Config
    where
        T: AsRef<[u8]>,
    {
        self.password = Some(password.as_ref().to_vec());
        self
    }

    /// Gets the password to authenticate with, if one has been configured with
    /// the `password` method.
    pub fn get_password(&self) -> Option<&[u8]> {
        self.password.as_deref()
    }

    /// Sets precomputed protocol-specific keys to authenticate with.
    /// When set, this option will override `password`.
    /// See [`AuthKeys`] for more information.
    pub fn auth_keys(&mut self, keys: AuthKeys) -> &mut Config {
        self.auth_keys = Some(Box::new(keys));
        self
    }

    /// Gets precomputed protocol-specific keys to authenticate with.
    /// if one has been configured with the `auth_keys` method.
    pub fn get_auth_keys(&self) -> Option<AuthKeys> {
        self.auth_keys.as_deref().copied()
    }

    /// Sets the name of the database to connect to.
    ///
    /// Defaults to the user.
    pub fn dbname(&mut self, dbname: &str) -> &mut Config {
        self.set_param("database", dbname)
    }

    /// Gets the name of the database to connect to, if one has been configured
    /// with the `dbname` method.
    pub fn db_is_set(&self) -> bool {
        self.database
    }

    pub fn set_param(&mut self, name: &str, value: &str) -> &mut Config {
        if name == "database" {
            self.database = true;
        } else if name == "user" {
            self.username = true;
        }

        self.server_params.insert(name, value);
        self
    }

    /// Sets the SSL configuration.
    ///
    /// Defaults to `prefer`.
    pub fn ssl_mode(&mut self, ssl_mode: SslMode) -> &mut Config {
        self.ssl_mode = ssl_mode;
        self
    }

    /// Gets the SSL configuration.
    pub fn get_ssl_mode(&self) -> SslMode {
        self.ssl_mode
    }

    /// Gets the hosts that have been added to the configuration with `host`.
    pub fn get_host(&self) -> &Host {
        &self.host
    }

    /// Gets the ports that have been added to the configuration with `port`.
    pub fn get_port(&self) -> u16 {
        self.port
    }

    /// Sets the timeout applied to socket-level connection attempts.
    ///
    /// Note that hostnames can resolve to multiple IP addresses, and this timeout will apply to each address of each
    /// host separately. Defaults to no limit.
    pub fn connect_timeout(&mut self, connect_timeout: Duration) -> &mut Config {
        self.connect_timeout = Some(connect_timeout);
        self
    }

    /// Gets the connection timeout, if one has been set with the
    /// `connect_timeout` method.
    pub fn get_connect_timeout(&self) -> Option<&Duration> {
        self.connect_timeout.as_ref()
    }

    /// Sets the channel binding behavior.
    ///
    /// Defaults to `prefer`.
    pub fn channel_binding(&mut self, channel_binding: ChannelBinding) -> &mut Config {
        self.channel_binding = channel_binding;
        self
    }

    /// Gets the channel binding behavior.
    pub fn get_channel_binding(&self) -> ChannelBinding {
        self.channel_binding
    }

    /// Opens a connection to a PostgreSQL database.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    pub async fn connect<T>(
        &self,
        tls: T,
    ) -> Result<(Client, Connection<TcpStream, T::Stream>), Error>
    where
        T: MakeTlsConnect<TcpStream>,
    {
        connect(tls, self).await
    }

    pub async fn connect_raw<S, T>(
        &self,
        stream: S,
        tls: T,
    ) -> Result<RawConnection<S, T::Stream>, Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: TlsConnect<S>,
    {
        connect_raw(stream, tls, self).await
    }
}

// Omit password from debug output
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Redaction {}
        impl fmt::Debug for Redaction {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "_")
            }
        }

        f.debug_struct("Config")
            .field("password", &self.password.as_ref().map(|_| Redaction {}))
            .field("ssl_mode", &self.ssl_mode)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("connect_timeout", &self.connect_timeout)
            .field("channel_binding", &self.channel_binding)
            .field("server_params", &self.server_params)
            .finish()
    }
}
