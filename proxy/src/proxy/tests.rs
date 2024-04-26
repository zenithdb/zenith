//! A group of high-level tests for connection establishing logic and auth.

mod mitm;

use std::time::Duration;

use super::connect_compute::ConnectMechanism;
use super::retry::ShouldRetry;
use super::*;
use crate::auth::backend::{
    ComputeCredentialKeys, ComputeCredentials, ComputeUserInfo, MaybeOwned, TestBackend,
};
use crate::config::{CertResolver, RetryConfig};
use crate::console::caches::NodeInfoCache;
use crate::console::messages::MetricsAuxInfo;
use crate::console::provider::{CachedAllowedIps, CachedRoleSecret, ConsoleBackend};
use crate::console::{self, CachedNodeInfo, NodeInfo};
use crate::dns::Dns;
use crate::error::ErrorKind;
use crate::proxy::retry::retry_after;
use crate::{http, sasl, scram, BranchId, EndpointId, ProjectId};
use anyhow::{bail, Context};
use async_trait::async_trait;
use rstest::rstest;
use rustls::pki_types;
use tokio_postgres::config::SslMode;
use tokio_postgres::tls::{MakeTlsConnect, NoTls};
use tokio_postgres_rustls::{MakeRustlsConnect, RustlsStream};

/// Generate a set of TLS certificates: CA + server.
fn generate_certs(
    hostname: &str,
    common_name: &str,
) -> anyhow::Result<(
    pki_types::CertificateDer<'static>,
    pki_types::CertificateDer<'static>,
    pki_types::PrivateKeyDer<'static>,
)> {
    let ca = rcgen::Certificate::from_params({
        let mut params = rcgen::CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
    })?;

    let cert = rcgen::Certificate::from_params({
        let mut params = rcgen::CertificateParams::new(vec![hostname.into()]);
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        params
    })?;

    Ok((
        pki_types::CertificateDer::from(ca.serialize_der()?),
        pki_types::CertificateDer::from(cert.serialize_der_with_signer(&ca)?),
        pki_types::PrivateKeyDer::Pkcs8(cert.serialize_private_key_der().into()),
    ))
}

struct ClientConfig<'a> {
    config: rustls::ClientConfig,
    hostname: &'a str,
}

impl ClientConfig<'_> {
    fn make_tls_connect<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        self,
    ) -> anyhow::Result<
        impl tokio_postgres::tls::TlsConnect<
            S,
            Error = impl std::fmt::Debug,
            Future = impl Send,
            Stream = RustlsStream<S>,
        >,
    > {
        let mut mk = MakeRustlsConnect::new(self.config);
        let tls = MakeTlsConnect::<S>::make_tls_connect(&mut mk, self.hostname)?;
        Ok(tls)
    }
}

/// Generate TLS certificates and build rustls configs for client and server.
fn generate_tls_config<'a>(
    hostname: &'a str,
    common_name: &'a str,
) -> anyhow::Result<(ClientConfig<'a>, TlsConfig)> {
    let (ca, cert, key) = generate_certs(hostname, common_name)?;

    let tls_config = {
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key.clone_key())?
            .into();

        let mut cert_resolver = CertResolver::new();
        cert_resolver.add_cert(key, vec![cert], true)?;

        let common_names = cert_resolver.get_common_names();

        TlsConfig {
            config,
            common_names,
            cert_resolver: Arc::new(cert_resolver),
        }
    };

    let client_config = {
        let config = rustls::ClientConfig::builder()
            .with_root_certificates({
                let mut store = rustls::RootCertStore::empty();
                store.add(ca)?;
                store
            })
            .with_no_client_auth();

        ClientConfig { config, hostname }
    };

    Ok((client_config, tls_config))
}

#[async_trait]
trait TestAuth: Sized {
    async fn authenticate<S: AsyncRead + AsyncWrite + Unpin + Send>(
        self,
        stream: &mut PqStream<Stream<S>>,
    ) -> anyhow::Result<()> {
        stream.write_message_noflush(&Be::AuthenticationOk)?;
        Ok(())
    }
}

struct NoAuth;
impl TestAuth for NoAuth {}

struct Scram(scram::ServerSecret);

impl Scram {
    async fn new(password: &str) -> anyhow::Result<Self> {
        let secret = scram::ServerSecret::build(password)
            .await
            .context("failed to generate scram secret")?;
        Ok(Scram(secret))
    }

    fn mock() -> Self {
        Scram(scram::ServerSecret::mock(rand::random()))
    }
}

#[async_trait]
impl TestAuth for Scram {
    async fn authenticate<S: AsyncRead + AsyncWrite + Unpin + Send>(
        self,
        stream: &mut PqStream<Stream<S>>,
    ) -> anyhow::Result<()> {
        let outcome = auth::AuthFlow::new(stream)
            .begin(auth::Scram(&self.0, &mut RequestMonitoring::test()))
            .await?
            .authenticate()
            .await?;

        use sasl::Outcome::*;
        match outcome {
            Success(_) => Ok(()),
            Failure(reason) => bail!("autentication failed with an error: {reason}"),
        }
    }
}

/// A dummy proxy impl which performs a handshake and reports auth success.
async fn dummy_proxy(
    client: impl AsyncRead + AsyncWrite + Unpin + Send,
    tls: Option<TlsConfig>,
    auth: impl TestAuth + Send,
) -> anyhow::Result<()> {
    let (client, _) = read_proxy_protocol(client).await?;
    let mut stream = match handshake(client, tls.as_ref(), false).await? {
        HandshakeData::Startup(stream, _) => stream,
        HandshakeData::Cancel(_) => bail!("cancellation not supported"),
    };

    auth.authenticate(&mut stream).await?;

    stream
        .write_message_noflush(&Be::CLIENT_ENCODING)?
        .write_message(&Be::ReadyForQuery)
        .await?;

    Ok(())
}

#[tokio::test]
async fn handshake_tls_is_enforced_by_proxy() -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let (_, server_config) = generate_tls_config("generic-project-name.localhost", "localhost")?;
    let proxy = tokio::spawn(dummy_proxy(client, Some(server_config), NoAuth));

    let client_err = tokio_postgres::Config::new()
        .user("john_doe")
        .dbname("earth")
        .ssl_mode(SslMode::Disable)
        .connect_raw(server, NoTls)
        .await
        .err() // -> Option<E>
        .context("client shouldn't be able to connect")?;

    assert!(client_err.to_string().contains(ERR_INSECURE_CONNECTION));

    let server_err = proxy
        .await?
        .err() // -> Option<E>
        .context("server shouldn't accept client")?;

    assert!(client_err.to_string().contains(&server_err.to_string()));

    Ok(())
}

#[tokio::test]
async fn handshake_tls() -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let (client_config, server_config) =
        generate_tls_config("generic-project-name.localhost", "localhost")?;
    let proxy = tokio::spawn(dummy_proxy(client, Some(server_config), NoAuth));

    let (_client, _conn) = tokio_postgres::Config::new()
        .user("john_doe")
        .dbname("earth")
        .ssl_mode(SslMode::Require)
        .connect_raw(server, client_config.make_tls_connect()?)
        .await?;

    proxy.await?
}

#[tokio::test]
async fn handshake_raw() -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let proxy = tokio::spawn(dummy_proxy(client, None, NoAuth));

    let (_client, _conn) = tokio_postgres::Config::new()
        .user("john_doe")
        .dbname("earth")
        .options("project=generic-project-name")
        .ssl_mode(SslMode::Prefer)
        .connect_raw(server, NoTls)
        .await?;

    proxy.await?
}

#[tokio::test]
async fn keepalive_is_inherited() -> anyhow::Result<()> {
    use tokio::net::{TcpListener, TcpStream};

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let t = tokio::spawn(async move {
        let (client, _) = listener.accept().await?;
        let keepalive = socket2::SockRef::from(&client).keepalive()?;
        anyhow::Ok(keepalive)
    });

    let _ = TcpStream::connect(("127.0.0.1", port)).await?;
    assert!(t.await??, "keepalive should be inherited");

    Ok(())
}

#[rstest]
#[case("password_foo")]
#[case("pwd-bar")]
#[case("")]
#[tokio::test]
async fn scram_auth_good(#[case] password: &str) -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let (client_config, server_config) =
        generate_tls_config("generic-project-name.localhost", "localhost")?;
    let proxy = tokio::spawn(dummy_proxy(
        client,
        Some(server_config),
        Scram::new(password).await?,
    ));

    let (_client, _conn) = tokio_postgres::Config::new()
        .channel_binding(tokio_postgres::config::ChannelBinding::Require)
        .user("user")
        .dbname("db")
        .password(password)
        .ssl_mode(SslMode::Require)
        .connect_raw(server, client_config.make_tls_connect()?)
        .await?;

    proxy.await?
}

#[tokio::test]
async fn scram_auth_disable_channel_binding() -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let (client_config, server_config) =
        generate_tls_config("generic-project-name.localhost", "localhost")?;
    let proxy = tokio::spawn(dummy_proxy(
        client,
        Some(server_config),
        Scram::new("password").await?,
    ));

    let (_client, _conn) = tokio_postgres::Config::new()
        .channel_binding(tokio_postgres::config::ChannelBinding::Disable)
        .user("user")
        .dbname("db")
        .password("password")
        .ssl_mode(SslMode::Require)
        .connect_raw(server, client_config.make_tls_connect()?)
        .await?;

    proxy.await?
}

#[tokio::test]
async fn scram_auth_mock() -> anyhow::Result<()> {
    let (client, server) = tokio::io::duplex(1024);

    let (client_config, server_config) =
        generate_tls_config("generic-project-name.localhost", "localhost")?;
    let proxy = tokio::spawn(dummy_proxy(client, Some(server_config), Scram::mock()));

    use rand::{distributions::Alphanumeric, Rng};
    let password: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(rand::random::<u8>() as usize)
        .map(char::from)
        .collect();

    let _client_err = tokio_postgres::Config::new()
        .user("user")
        .dbname("db")
        .password(&password) // no password will match the mocked secret
        .ssl_mode(SslMode::Require)
        .connect_raw(server, client_config.make_tls_connect()?)
        .await
        .err() // -> Option<E>
        .context("client shouldn't be able to connect")?;

    let _server_err = proxy
        .await?
        .err() // -> Option<E>
        .context("server shouldn't accept client")?;

    Ok(())
}

#[test]
fn connect_compute_total_wait() {
    let mut total_wait = tokio::time::Duration::ZERO;
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 5,
        backoff_factor: 2.0,
    };
    for num_retries in 1..config.max_retries {
        total_wait += retry_after(num_retries, config);
    }
    assert!(f64::abs(total_wait.as_secs_f64() - 15.0) < 0.1);
}

#[derive(Clone, Copy, Debug)]
enum ConnectAction {
    Wake,
    WakeFail,
    WakeRetry,
    Connect,
    Retry,
    Fail,
}

#[derive(Clone)]
struct TestConnectMechanism {
    counter: Arc<std::sync::Mutex<usize>>,
    sequence: Vec<ConnectAction>,
    cache: &'static NodeInfoCache,
}

impl TestConnectMechanism {
    fn verify(&self) {
        let counter = self.counter.lock().unwrap();
        assert_eq!(
            *counter,
            self.sequence.len(),
            "sequence does not proceed to the end"
        );
    }
}

impl TestConnectMechanism {
    fn new(sequence: Vec<ConnectAction>) -> Self {
        Self {
            counter: Arc::new(std::sync::Mutex::new(0)),
            sequence,
            cache: Box::leak(Box::new(NodeInfoCache::new(
                "test",
                1,
                Duration::from_secs(100),
                false,
            ))),
        }
    }
}

#[derive(Debug)]
struct TestConnection;

#[derive(Debug)]
struct TestConnectError {
    retryable: bool,
    kind: crate::error::ErrorKind,
}

impl ReportableError for TestConnectError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        self.kind
    }
}

impl std::fmt::Display for TestConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for TestConnectError {}

impl ShouldRetry for TestConnectError {
    fn could_retry(&self) -> bool {
        self.retryable
    }
}

#[async_trait]
impl ConnectMechanism for TestConnectMechanism {
    type Connection = TestConnection;
    type ConnectError = TestConnectError;
    type Error = anyhow::Error;

    async fn connect_once(
        &self,
        _ctx: &mut RequestMonitoring,
        _dns: &Dns,
        _node_info: &console::CachedNodeInfo,
        _timeout: std::time::Duration,
    ) -> Result<Self::Connection, Self::ConnectError> {
        let mut counter = self.counter.lock().unwrap();
        let action = self.sequence[*counter];
        *counter += 1;
        match action {
            ConnectAction::Connect => Ok(TestConnection),
            ConnectAction::Retry => Err(TestConnectError {
                retryable: true,
                kind: ErrorKind::Compute,
            }),
            ConnectAction::Fail => Err(TestConnectError {
                retryable: false,
                kind: ErrorKind::Compute,
            }),
            x => panic!("expecting action {:?}, connect is called instead", x),
        }
    }

    fn update_connect_config(&self, _conf: &mut compute::ConnCfg) {}
}

impl TestBackend for TestConnectMechanism {
    fn wake_compute(&self) -> Result<CachedNodeInfo, console::errors::WakeComputeError> {
        let mut counter = self.counter.lock().unwrap();
        let action = self.sequence[*counter];
        *counter += 1;
        match action {
            ConnectAction::Wake => Ok(helper_create_cached_node_info(self.cache)),
            ConnectAction::WakeFail => {
                let err = console::errors::ApiError::Console {
                    status: http::StatusCode::FORBIDDEN,
                    text: "TEST".into(),
                };
                assert!(!err.could_retry());
                Err(console::errors::WakeComputeError::ApiError(err))
            }
            ConnectAction::WakeRetry => {
                let err = console::errors::ApiError::Console {
                    status: http::StatusCode::BAD_REQUEST,
                    text: "TEST".into(),
                };
                assert!(err.could_retry());
                Err(console::errors::WakeComputeError::ApiError(err))
            }
            x => panic!("expecting action {:?}, wake_compute is called instead", x),
        }
    }

    fn get_allowed_ips_and_secret(
        &self,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), console::errors::GetAuthInfoError>
    {
        unimplemented!("not used in tests")
    }
    fn get_role_secret(&self) -> Result<CachedRoleSecret, console::errors::GetAuthInfoError> {
        unimplemented!("not used in tests")
    }
}

fn helper_create_cached_node_info(cache: &'static NodeInfoCache) -> CachedNodeInfo {
    let node = NodeInfo {
        config: compute::ConnCfg::new(),
        aux: MetricsAuxInfo {
            endpoint_id: (&EndpointId::from("endpoint")).into(),
            project_id: (&ProjectId::from("project")).into(),
            branch_id: (&BranchId::from("branch")).into(),
            cold_start_info: crate::console::messages::ColdStartInfo::Warm,
        },
        allow_self_signed_compute: false,
    };
    let (_, node) = cache.insert("key".into(), node);
    node
}

fn helper_create_connect_info(
    mechanism: &TestConnectMechanism,
) -> auth::BackendType<'static, ComputeCredentials, &()> {
    let user_info = auth::BackendType::Console(
        MaybeOwned::Owned(ConsoleBackend::Test(Box::new(mechanism.clone()))),
        ComputeCredentials {
            info: ComputeUserInfo {
                endpoint: "endpoint".into(),
                user: "user".into(),
                options: NeonOptions::parse_options_raw(""),
            },
            keys: ComputeCredentialKeys::Password("password".into()),
        },
    );
    user_info
}

#[tokio::test]
async fn connect_to_compute_success() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let mut ctx = RequestMonitoring::test();
    let mechanism = TestConnectMechanism::new(vec![Wake, Connect]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 5,
        backoff_factor: 2.0,
    };
    connect_to_compute(
        &mut ctx,
        &mechanism,
        &user_info,
        &Dns::new(),
        false,
        config,
        config,
    )
    .await
    .unwrap();
    mechanism.verify();
}

#[tokio::test]
async fn connect_to_compute_retry() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let mut ctx = RequestMonitoring::test();
    let mechanism = TestConnectMechanism::new(vec![Wake, Retry, Wake, Connect]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 5,
        backoff_factor: 2.0,
    };
    connect_to_compute(
        &mut ctx,
        &mechanism,
        &user_info,
        &Dns::new(),
        false,
        config,
        config,
    )
    .await
    .unwrap();
    mechanism.verify();
}

/// Test that we don't retry if the error is not retryable.
#[tokio::test]
async fn connect_to_compute_non_retry_1() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let mut ctx = RequestMonitoring::test();
    let mechanism = TestConnectMechanism::new(vec![Wake, Retry, Wake, Fail]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 5,
        backoff_factor: 2.0,
    };
    connect_to_compute(
        &mut ctx,
        &mechanism,
        &user_info,
        &Dns::new(),
        false,
        config,
        config,
    )
    .await
    .unwrap_err();
    mechanism.verify();
}

/// Even for non-retryable errors, we should retry at least once.
#[tokio::test]
async fn connect_to_compute_non_retry_2() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let mut ctx = RequestMonitoring::test();
    let mechanism = TestConnectMechanism::new(vec![Wake, Fail, Wake, Connect]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 5,
        backoff_factor: 2.0,
    };
    connect_to_compute(
        &mut ctx,
        &mechanism,
        &user_info,
        &Dns::new(),
        false,
        config,
        config,
    )
    .await
    .unwrap();
    mechanism.verify();
}

/// Retry for at most `NUM_RETRIES_CONNECT` times.
#[tokio::test]
async fn connect_to_compute_non_retry_3() {
    let _ = env_logger::try_init();
    tokio::time::pause();
    use ConnectAction::*;
    let mut ctx = RequestMonitoring::test();
    let mechanism =
        TestConnectMechanism::new(vec![Wake, Retry, Wake, Retry, Retry, Retry, Retry, Retry]);
    let user_info = helper_create_connect_info(&mechanism);
    let wake_compute_retry_config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 1,
        backoff_factor: 2.0,
    };
    let connect_to_compute_retry_config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 5,
        backoff_factor: 2.0,
    };
    connect_to_compute(
        &mut ctx,
        &mechanism,
        &user_info,
        &Dns::new(),
        false,
        wake_compute_retry_config,
        connect_to_compute_retry_config,
    )
    .await
    .unwrap_err();
    mechanism.verify();
}

/// Should retry wake compute.
#[tokio::test]
async fn wake_retry() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let mut ctx = RequestMonitoring::test();
    let mechanism = TestConnectMechanism::new(vec![WakeRetry, Wake, Connect]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 5,
        backoff_factor: 2.0,
    };
    connect_to_compute(
        &mut ctx,
        &mechanism,
        &user_info,
        &Dns::new(),
        false,
        config,
        config,
    )
    .await
    .unwrap();
    mechanism.verify();
}

/// Wake failed with a non-retryable error.
#[tokio::test]
async fn wake_non_retry() {
    let _ = env_logger::try_init();
    use ConnectAction::*;
    let mut ctx = RequestMonitoring::test();
    let mechanism = TestConnectMechanism::new(vec![WakeRetry, WakeFail]);
    let user_info = helper_create_connect_info(&mechanism);
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_retries: 5,
        backoff_factor: 2.0,
    };
    connect_to_compute(
        &mut ctx,
        &mechanism,
        &user_info,
        &Dns::new(),
        false,
        config,
        config,
    )
    .await
    .unwrap_err();
    mechanism.verify();
}
