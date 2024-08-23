use crate::{
    auth::parse_endpoint_param,
    cancellation::CancelClosure,
    console::{errors::WakeComputeError, messages::MetricsAuxInfo, provider::ApiLockError},
    context::RequestMonitoring,
    error::{ReportableError, UserFacingError},
    metrics::{Metrics, NumDbConnectionsGuard},
    proxy::neon_option,
    Host,
};
use futures::{FutureExt, TryFutureExt};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use pq_proto::StartupMessageParams;
use rustls::{client::danger::ServerCertVerifier, pki_types::InvalidDnsNameError};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_postgres::{
    tls::{MakeTlsConnect, NoTlsError},
    Client, Connection,
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
    TlsError(#[from] InvalidDnsNameError),

    #[error("{COULD_NOT_CONNECT}: {0}")]
    WakeComputeError(#[from] WakeComputeError),

    #[error("error acquiring resource permit: {0}")]
    TooManyConnectionAttempts(#[from] ApiLockError),
}

impl From<NoTlsError> for ConnectionError {
    fn from(value: NoTlsError) -> Self {
        Self::CouldNotConnect(io::Error::new(io::ErrorKind::Other, value.to_string()))
    }
}

impl UserFacingError for ConnectionError {
    fn to_string_client(&self) -> String {
        match self {
            // This helps us drop irrelevant library-specific prefixes.
            // TODO: propagate severity level and other parameters.
            ConnectionError::Postgres(err) => match err.as_db_error() {
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
            ConnectionError::WakeComputeError(err) => err.to_string_client(),
            ConnectionError::TooManyConnectionAttempts(_) => {
                "Failed to acquire permit to connect to the database. Too many database connection attempts are currently ongoing.".to_owned()
            }
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
            ConnectionError::WakeComputeError(e) => e.get_error_kind(),
            ConnectionError::TooManyConnectionAttempts(e) => e.get_error_kind(),
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

    pub fn get_host(&self) -> Result<Host, WakeComputeError> {
        match self.0.get_hosts() {
            [tokio_postgres::config::Host::Tcp(s)] => Ok(s.into()),
            // we should not have multiple address or unix addresses.
            _ => Err(WakeComputeError::BadComputeAddress(
                "invalid compute address".into(),
            )),
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
    async fn connect_raw(&self, timeout: Duration) -> io::Result<(SocketAddr, TcpStream, &str)> {
        use tokio_postgres::config::Host;

        // wrap TcpStream::connect with timeout
        let connect_with_timeout = |host, port| {
            tokio::time::timeout(timeout, TcpStream::connect((host, port))).map(
                move |res| match res {
                    Ok(tcpstream_connect_res) => tcpstream_connect_res,
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("exceeded connection timeout {timeout:?}"),
                    )),
                },
            )
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

pub struct PostgresConnection {
    /// Socket connected to a compute node.
    pub stream: tokio_postgres::maybe_tls_stream::MaybeTlsStream<
        tokio::net::TcpStream,
        tokio_postgres_rustls::RustlsStream<tokio::net::TcpStream>,
    >,
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
    pub async fn managed_connect<M: MakeTlsConnect<tokio::net::TcpStream>>(
        &self,
        ctx: &RequestMonitoring,
        timeout: Duration,
        mktls: &mut M,
    ) -> Result<(SocketAddr, Client, Connection<TcpStream, M::Stream>), ConnectionError>
    where
        ConnectionError: From<M::Error>,
    {
        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);
        let (socket_addr, stream, host) = self.connect_raw(timeout).await?;
        drop(pause);

        let tls = mktls.make_tls_connect(host)?;

        // connect_raw() will not use TLS if sslmode is "disable"
        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);
        let (client, connection) = self.0.connect_raw(stream, tls).await?;
        drop(pause);

        Ok((socket_addr, client, connection))
    }

    /// Connect to a corresponding compute node.
    pub async fn connect(
        &self,
        ctx: &RequestMonitoring,
        allow_self_signed_compute: bool,
        aux: MetricsAuxInfo,
        timeout: Duration,
    ) -> Result<PostgresConnection, ConnectionError> {
        let client_config = if allow_self_signed_compute {
            // Allow all certificates for creating the connection
            let verifier = Arc::new(AcceptEverythingVerifier) as Arc<dyn ServerCertVerifier>;
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(verifier)
        } else {
            let root_store = TLS_ROOTS.get_or_try_init(load_certs)?.clone();
            rustls::ClientConfig::builder().with_root_certificates(root_store)
        };
        let client_config = client_config.with_no_client_auth();

        let mut mk_tls = tokio_postgres_rustls::MakeRustlsConnect::new(client_config);

        let (socket_addr, client, connection) =
            self.managed_connect(ctx, timeout, &mut mk_tls).await?;
        tracing::Span::current().record("pid", tracing::field::display(client.get_process_id()));
        let stream = connection.stream.into_inner();

        info!(
            cold_start_info = ctx.cold_start_info().as_str(),
            "connected to compute node ({socket_addr}) sslmode={:?}",
            self.0.get_ssl_mode()
        );

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
            _guage: Metrics::get().proxy.db_connections.guard(ctx.protocol()),
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

fn load_certs() -> Result<Arc<rustls::RootCertStore>, io::Error> {
    let der_certs = rustls_native_certs::load_native_certs()?;
    let mut store = rustls::RootCertStore::empty();
    store.add_parsable_certificates(der_certs);
    Ok(Arc::new(store))
}
static TLS_ROOTS: OnceCell<Arc<rustls::RootCertStore>> = OnceCell::new();

#[derive(Debug)]
struct AcceptEverythingVerifier;
impl ServerCertVerifier for AcceptEverythingVerifier {
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme;
        // The schemes for which `SignatureScheme::supported_in_tls13` returns true.
        vec![
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ED25519,
        ]
    }
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
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
