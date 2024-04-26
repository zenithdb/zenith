use crate::{
    auth::parse_endpoint_param,
    cancellation::CancelClosure,
    console::{errors::WakeComputeError, messages::MetricsAuxInfo},
    context::RequestMonitoring,
    dns::Dns,
    error::{ReportableError, UserFacingError},
    metrics::{Metrics, NumDbConnectionsGuard},
    proxy::neon_option,
};
use futures::TryFutureExt;
use itertools::Itertools;
use pq_proto::StartupMessageParams;
use std::{io, net::SocketAddr, time::Duration};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_postgres::{
    tls::{MakeTlsConnect, NoTlsError},
    Connection, SocketConfig,
};
use tracing::{error, info, warn};

const COULD_NOT_CONNECT: &str = "Couldn't connect to compute node";

#[derive(Debug, Error)]
pub enum ConnectionError {
    /// This error doesn't seem to reveal any secrets; for instance,
    /// `tokio_postgres::error::Kind` doesn't contain ip addresses and such.
    #[error("{COULD_NOT_CONNECT}: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    CouldNotConnect(#[from] io::Error),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    TlsError(#[from] native_tls::Error),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    WakeComputeError(#[from] WakeComputeError),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    TlsNotSupported(#[from] NoTlsError),
}

impl UserFacingError for ConnectionError {
    fn to_string_client(&self) -> String {
        use ConnectionError::*;
        match self {
            // This helps us drop irrelevant library-specific prefixes.
            // TODO: propagate severity level and other parameters.
            Postgres(err) => match err.as_db_error() {
                Some(err) => {
                    let msg = err.message();

                    if msg.starts_with("unsupported startup parameter: ")
                        || msg.starts_with("unsupported startup parameter in options: ")
                    {
                        format!("{msg}. Please use unpooled connection or remove this parameter from the startup package. More details: https://neon.tech/docs/connect/connection-errors#unsupported-startup-parameter")
                    } else {
                        msg.to_owned()
                    }
                }
                None => err.to_string(),
            },
            WakeComputeError(err) => err.to_string_client(),
            _ => COULD_NOT_CONNECT.to_owned(),
        }
    }
}

impl ReportableError for ConnectionError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ConnectionError::Postgres(e) if e.as_db_error().is_some() => {
                crate::error::ErrorKind::Postgres
            }
            ConnectionError::Postgres(_) => crate::error::ErrorKind::Compute,
            ConnectionError::CouldNotConnect(_) => crate::error::ErrorKind::Compute,
            ConnectionError::TlsError(_) => crate::error::ErrorKind::Compute,
            ConnectionError::TlsNotSupported(_) => crate::error::ErrorKind::Compute,
            ConnectionError::WakeComputeError(e) => e.get_error_kind(),
        }
    }
}

/// A pair of `ClientKey` & `ServerKey` for `SCRAM-SHA-256`.
pub type ScramKeys = tokio_postgres::config::ScramKeys<32>;

/// A config for establishing a connection to compute node.
/// Eventually, `tokio_postgres` will be replaced with something better.
/// Newtype allows us to implement methods on top of it.
#[derive(Clone, Default)]
pub struct ConnCfg(Box<tokio_postgres::Config>);

/// Creation and initialization routines.
impl ConnCfg {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reuse password or auth keys from the other config.
    pub fn reuse_password(&mut self, other: Self) {
        if let Some(password) = other.get_password() {
            self.password(password);
        }

        if let Some(keys) = other.get_auth_keys() {
            self.auth_keys(keys);
        }
    }

    /// Apply startup message params to the connection config.
    pub fn set_startup_params(&mut self, params: &StartupMessageParams) {
        // Only set `user` if it's not present in the config.
        // Link auth flow takes username from the console's response.
        if let (None, Some(user)) = (self.get_user(), params.get("user")) {
            self.user(user);
        }

        // Only set `dbname` if it's not present in the config.
        // Link auth flow takes dbname from the console's response.
        if let (None, Some(dbname)) = (self.get_dbname(), params.get("database")) {
            self.dbname(dbname);
        }

        // Don't add `options` if they were only used for specifying a project.
        // Connection pools don't support `options`, because they affect backend startup.
        if let Some(options) = filtered_options(params) {
            self.options(&options);
        }

        if let Some(app_name) = params.get("application_name") {
            self.application_name(app_name);
        }

        // TODO: This is especially ugly...
        if let Some(replication) = params.get("replication") {
            use tokio_postgres::config::ReplicationMode;
            match replication {
                "true" | "on" | "yes" | "1" => {
                    self.replication_mode(ReplicationMode::Physical);
                }
                "database" => {
                    self.replication_mode(ReplicationMode::Logical);
                }
                _other => {}
            }
        }

        // TODO: extend the list of the forwarded startup parameters.
        // Currently, tokio-postgres doesn't allow us to pass
        // arbitrary parameters, but the ones above are a good start.
        //
        // This and the reverse params problem can be better addressed
        // in a bespoke connection machinery (a new library for that sake).
    }
}

impl std::ops::Deref for ConnCfg {
    type Target = tokio_postgres::Config;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// For now, let's make it easier to setup the config.
impl std::ops::DerefMut for ConnCfg {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ConnCfg {
    /// Establish a raw TCP connection to the compute node.
    async fn connect_raw(
        &self,
        dns: &Dns,
        timeout: Duration,
    ) -> io::Result<(SocketAddr, TcpStream, &str)> {
        use tokio_postgres::config::Host;

        // wrap TcpStream::connect with timeout
        let connect_with_timeout = |host, port| async move {
            let addrs = dns
                .resolve(host)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let timeout = timeout / addrs.len() as u32;

            let mut last_err = None;
            for addr in addrs {
                match tokio::time::timeout(timeout, TcpStream::connect((addr, port))).await {
                    Ok(Ok(stream)) => return Ok(stream),
                    Ok(Err(e)) => last_err = Some(e),
                    Err(_) => {
                        last_err = Some(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!("exceeded connection timeout {timeout:?}"),
                        ))
                    }
                };
            }

            Err(last_err.unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "could not resolve to any address",
                )
            }))
        };

        let connect_once = |host, port| {
            info!("trying to connect to compute node at {host}:{port}");
            connect_with_timeout(host, port).and_then(|socket| async {
                let socket_addr = socket.peer_addr()?;
                // This prevents load balancer from severing the connection.
                socket2::SockRef::from(&socket).set_keepalive(true)?;
                Ok((socket_addr, socket))
            })
        };

        // We can't reuse connection establishing logic from `tokio_postgres` here,
        // because it has no means for extracting the underlying socket which we
        // require for our business.
        let mut connection_error = None;
        let ports = self.0.get_ports();
        let hosts = self.0.get_hosts();
        // the ports array is supposed to have 0 entries, 1 entry, or as many entries as in the hosts array
        if ports.len() > 1 && ports.len() != hosts.len() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "bad compute config, \
                     ports and hosts entries' count does not match: {:?}",
                    self.0
                ),
            ));
        }

        for (i, host) in hosts.iter().enumerate() {
            let port = ports.get(i).or_else(|| ports.first()).unwrap_or(&5432);
            let host = match host {
                Host::Tcp(host) => host.as_str(),
                Host::Unix(_) => continue, // unix sockets are not welcome here
            };

            match connect_once(host, *port).await {
                Ok((sockaddr, stream)) => return Ok((sockaddr, stream, host)),
                Err(err) => {
                    // We can't throw an error here, as there might be more hosts to try.
                    warn!("couldn't connect to compute node at {host}:{port}: {err}");
                    connection_error = Some(err);
                }
            }
        }

        Err(connection_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("bad compute config: {:?}", self.0),
            )
        }))
    }
}

type TlsStream = postgres_native_tls::TlsStream<TcpStream>;

pub struct PostgresConnection {
    /// Socket connected to a compute node.
    pub stream: tokio_postgres::maybe_tls_stream::MaybeTlsStream<TcpStream, TlsStream>,
    /// PostgreSQL connection parameters.
    pub params: std::collections::HashMap<String, String>,
    /// Query cancellation token.
    pub cancel_closure: CancelClosure,
    /// Labels for proxy's metrics.
    pub aux: MetricsAuxInfo,

    _guage: NumDbConnectionsGuard<'static>,
}

impl ConnCfg {
    /// Connect to a corresponding compute node.
    pub async fn connect_managed<Tls: MakeTlsConnect<TcpStream>>(
        &self,
        ctx: &mut RequestMonitoring,
        dns: &Dns,
        timeout: Duration,
        mut tls: Tls,
    ) -> Result<
        (
            SocketAddr,
            tokio_postgres::Client,
            Connection<TcpStream, Tls::Stream>,
        ),
        ConnectionError,
    >
    where
        ConnectionError: From<Tls::Error>,
    {
        let (socket_addr, stream, host) = self.connect_raw(dns, timeout).await?;

        let tls = MakeTlsConnect::<TcpStream>::make_tls_connect(&mut tls, host)?;

        // connect_raw() will not use TLS if sslmode is "disable"
        let (mut client, connection) = self.0.connect_raw(stream, tls).await?;
        tracing::Span::current().record("pid", &tracing::field::display(client.get_process_id()));

        client.set_socket_config(SocketConfig {
            host: tokio_postgres::config::Host::Tcp(host.to_owned()),
            port: socket_addr.port(),
            socket_addr: tokio_postgres::SocketAddr::Tcp(socket_addr),
            connect_timeout: None,
            keepalive: None,
        });

        info!(
            cold_start_info = ctx.cold_start_info.as_str(),
            "connected to compute node at {host} ({socket_addr}) sslmode={:?}",
            self.0.get_ssl_mode()
        );

        Ok((socket_addr, client, connection))
    }

    /// Connect to a corresponding compute node.
    pub async fn connect(
        &self,
        ctx: &mut RequestMonitoring,
        dns: &Dns,
        allow_self_signed_compute: bool,
        aux: MetricsAuxInfo,
        timeout: Duration,
    ) -> Result<PostgresConnection, ConnectionError> {
        let tls_connector = native_tls::TlsConnector::builder()
            .danger_accept_invalid_certs(allow_self_signed_compute)
            .build()
            .unwrap();
        let mk_tls = postgres_native_tls::MakeTlsConnector::new(tls_connector);

        let (socket_addr, client, connection) =
            self.connect_managed(ctx, dns, timeout, mk_tls).await?;
        let stream = connection.stream.into_inner();

        // This is very ugly but as of now there's no better way to
        // extract the connection parameters from tokio-postgres' connection.
        // TODO: solve this problem in a more elegant manner (e.g. the new library).
        let params = connection.parameters;

        // NB: CancelToken is supposed to hold socket_addr, but we use connect_raw.
        // Yet another reason to rework the connection establishing code.
        let cancel_closure = CancelClosure::new(socket_addr, client.cancel_token());

        let connection = PostgresConnection {
            stream,
            params,
            cancel_closure,
            aux,
            _guage: Metrics::get().proxy.db_connections.guard(ctx.protocol),
        };

        Ok(connection)
    }
}

/// Retrieve `options` from a startup message, dropping all proxy-secific flags.
fn filtered_options(params: &StartupMessageParams) -> Option<String> {
    #[allow(unstable_name_collisions)]
    let options: String = params
        .options_raw()?
        .filter(|opt| parse_endpoint_param(opt).is_none() && neon_option(opt).is_none())
        .intersperse(" ") // TODO: use impl from std once it's stabilized
        .collect();

    // Don't even bother with empty options.
    if options.is_empty() {
        return None;
    }

    Some(options)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filtered_options() {
        // Empty options is unlikely to be useful anyway.
        let params = StartupMessageParams::new([("options", "")]);
        assert_eq!(filtered_options(&params), None);

        // It's likely that clients will only use options to specify endpoint/project.
        let params = StartupMessageParams::new([("options", "project=foo")]);
        assert_eq!(filtered_options(&params), None);

        // Same, because unescaped whitespaces are no-op.
        let params = StartupMessageParams::new([("options", " project=foo ")]);
        assert_eq!(filtered_options(&params).as_deref(), None);

        let params = StartupMessageParams::new([("options", r"\  project=foo \ ")]);
        assert_eq!(filtered_options(&params).as_deref(), Some(r"\  \ "));

        let params = StartupMessageParams::new([("options", "project = foo")]);
        assert_eq!(filtered_options(&params).as_deref(), Some("project = foo"));

        let params = StartupMessageParams::new([(
            "options",
            "project = foo neon_endpoint_type:read_write   neon_lsn:0/2",
        )]);
        assert_eq!(filtered_options(&params).as_deref(), Some("project = foo"));
    }
}
