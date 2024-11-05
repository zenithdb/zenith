mod classic;
mod console_redirect;
mod hacks;
pub mod jwt;
pub mod local;

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

pub use console_redirect::ConsoleRedirectBackend;
pub(crate) use console_redirect::WebAuthError;
use ipnet::{Ipv4Net, Ipv6Net};
use local::LocalBackend;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::config::AuthKeys;
use tracing::{info, warn};

use crate::auth::credentials::check_peer_addr_is_in_list;
use crate::auth::{self, validate_password_and_exchange, AuthError, ComputeUserInfoMaybeEndpoint};
use crate::cache::Cached;
use crate::config::AuthenticationConfig;
use crate::context::RequestMonitoring;
use crate::control_plane::provider::{CachedNodeInfo, ControlPlaneBackend};
use crate::control_plane::{self, Api, AuthSecret};
use crate::intern::EndpointIdInt;
use crate::metrics::Metrics;
use crate::proxy::connect_compute::ComputeConnectBackend;
use crate::proxy::NeonOptions;
use crate::rate_limiter::{BucketRateLimiter, EndpointRateLimiter, RateBucketInfo};
use crate::stream::Stream;
use crate::types::{EndpointCacheKey, EndpointId, RoleName};
use crate::{scram, stream};

/// The [crate::serverless] module can authenticate either using control-plane
/// to get authentication state, or by using JWKs stored in the filesystem.
#[derive(Clone, Copy)]
pub enum ServerlessBackend<'a> {
    /// Cloud API (V2).
    ControlPlane(&'a ControlPlaneBackend),
    /// Local proxy uses configured auth credentials and does not wake compute
    Local(&'a LocalBackend),
}

#[cfg(test)]
use crate::control_plane::provider::{CachedAllowedIps, CachedRoleSecret};

#[cfg(test)]
pub(crate) trait TestBackend: Send + Sync + 'static {
    fn wake_compute(&self) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError>;
    fn get_allowed_ips_and_secret(
        &self,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), control_plane::errors::GetAuthInfoError>;
    fn dyn_clone(&self) -> Box<dyn TestBackend>;
}

#[cfg(test)]
impl Clone for Box<dyn TestBackend> {
    fn clone(&self) -> Self {
        TestBackend::dyn_clone(&**self)
    }
}

impl std::fmt::Display for ControlPlaneBackend {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlPlaneBackend::Management(endpoint) => fmt
                .debug_tuple("ControlPlane::Management")
                .field(&endpoint.url())
                .finish(),
            #[cfg(any(test, feature = "testing"))]
            ControlPlaneBackend::PostgresMock(endpoint) => fmt
                .debug_tuple("ControlPlane::PostgresMock")
                .field(&endpoint.url())
                .finish(),
            #[cfg(test)]
            ControlPlaneBackend::Test(_) => fmt.debug_tuple("ControlPlane::Test").finish(),
        }
    }
}

pub(crate) struct ComputeCredentials {
    pub(crate) info: ComputeUserInfo,
    pub(crate) keys: ComputeCredentialKeys,
}

#[derive(Debug, Clone)]
pub(crate) struct ComputeUserInfoNoEndpoint {
    pub(crate) user: RoleName,
    pub(crate) options: NeonOptions,
}

#[derive(Debug, Clone)]
pub(crate) struct ComputeUserInfo {
    pub(crate) endpoint: EndpointId,
    pub(crate) user: RoleName,
    pub(crate) options: NeonOptions,
}

impl ComputeUserInfo {
    pub(crate) fn endpoint_cache_key(&self) -> EndpointCacheKey {
        self.options.get_cache_key(&self.endpoint)
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) enum ComputeCredentialKeys {
    #[cfg(any(test, feature = "testing"))]
    Password(Vec<u8>),
    AuthKeys(AuthKeys),
    JwtPayload(Vec<u8>),
    None,
}

impl TryFrom<ComputeUserInfoMaybeEndpoint> for ComputeUserInfo {
    // user name
    type Error = ComputeUserInfoNoEndpoint;

    fn try_from(user_info: ComputeUserInfoMaybeEndpoint) -> Result<Self, Self::Error> {
        match user_info.endpoint_id {
            None => Err(ComputeUserInfoNoEndpoint {
                user: user_info.user,
                options: user_info.options,
            }),
            Some(endpoint) => Ok(ComputeUserInfo {
                endpoint,
                user: user_info.user,
                options: user_info.options,
            }),
        }
    }
}

#[derive(PartialEq, PartialOrd, Hash, Eq, Ord, Debug, Copy, Clone)]
pub struct MaskedIp(IpAddr);

impl MaskedIp {
    fn new(value: IpAddr, prefix: u8) -> Self {
        match value {
            IpAddr::V4(v4) => Self(IpAddr::V4(
                Ipv4Net::new(v4, prefix).map_or(v4, |x| x.trunc().addr()),
            )),
            IpAddr::V6(v6) => Self(IpAddr::V6(
                Ipv6Net::new(v6, prefix).map_or(v6, |x| x.trunc().addr()),
            )),
        }
    }
}

// This can't be just per IP because that would limit some PaaS that share IP addresses
pub type AuthRateLimiter = BucketRateLimiter<(EndpointIdInt, MaskedIp)>;

impl RateBucketInfo {
    /// All of these are per endpoint-maskedip pair.
    /// Context: 4096 rounds of pbkdf2 take about 1ms of cpu time to execute (1 milli-cpu-second or 1mcpus).
    ///
    /// First bucket: 1000mcpus total per endpoint-ip pair
    /// * 4096000 requests per second with 1 hash rounds.
    /// * 1000 requests per second with 4096 hash rounds.
    /// * 6.8 requests per second with 600000 hash rounds.
    pub const DEFAULT_AUTH_SET: [Self; 3] = [
        Self::new(1000 * 4096, Duration::from_secs(1)),
        Self::new(600 * 4096, Duration::from_secs(60)),
        Self::new(300 * 4096, Duration::from_secs(600)),
    ];
}

impl AuthenticationConfig {
    pub(crate) fn check_rate_limit(
        &self,
        ctx: &RequestMonitoring,
        secret: AuthSecret,
        endpoint: &EndpointId,
        is_cleartext: bool,
    ) -> auth::Result<AuthSecret> {
        // we have validated the endpoint exists, so let's intern it.
        let endpoint_int = EndpointIdInt::from(endpoint.normalize());

        // only count the full hash count if password hack or websocket flow.
        // in other words, if proxy needs to run the hashing
        let password_weight = if is_cleartext {
            match &secret {
                #[cfg(any(test, feature = "testing"))]
                AuthSecret::Md5(_) => 1,
                AuthSecret::Scram(s) => s.iterations + 1,
            }
        } else {
            // validating scram takes just 1 hmac_sha_256 operation.
            1
        };

        let limit_not_exceeded = self.rate_limiter.check(
            (
                endpoint_int,
                MaskedIp::new(ctx.peer_addr(), self.rate_limit_ip_subnet),
            ),
            password_weight,
        );

        if !limit_not_exceeded {
            warn!(
                enabled = self.rate_limiter_enabled,
                "rate limiting authentication"
            );
            Metrics::get().proxy.requests_auth_rate_limits_total.inc();
            Metrics::get()
                .proxy
                .endpoints_auth_rate_limits
                .get_metric()
                .measure(endpoint);

            if self.rate_limiter_enabled {
                return Err(auth::AuthError::too_many_connections());
            }
        }

        Ok(secret)
    }
}

/// True to its name, this function encapsulates our current auth trade-offs.
/// Here, we choose the appropriate auth flow based on circumstances.
///
/// All authentication flows will emit an AuthenticationOk message if successful.
async fn auth_quirks(
    ctx: &RequestMonitoring,
    api: &impl control_plane::Api,
    user_info: ComputeUserInfoMaybeEndpoint,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    allow_cleartext: bool,
    config: &'static AuthenticationConfig,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> auth::Result<ComputeCredentials> {
    // If there's no project so far, that entails that client doesn't
    // support SNI or other means of passing the endpoint (project) name.
    // We now expect to see a very specific payload in the place of password.
    let (info, unauthenticated_password) = match user_info.try_into() {
        Err(info) => {
            let (info, password) =
                hacks::password_hack_no_authentication(ctx, info, client).await?;
            ctx.set_endpoint_id(info.endpoint.clone());
            (info, Some(password))
        }
        Ok(info) => (info, None),
    };

    info!("fetching user's authentication info");
    let (allowed_ips, maybe_secret) = api.get_allowed_ips_and_secret(ctx, &info).await?;

    // check allowed list
    if config.ip_allowlist_check_enabled
        && !check_peer_addr_is_in_list(&ctx.peer_addr(), &allowed_ips)
    {
        return Err(auth::AuthError::ip_address_not_allowed(ctx.peer_addr()));
    }

    if !endpoint_rate_limiter.check(info.endpoint.clone().into(), 1) {
        return Err(AuthError::too_many_connections());
    }
    let cached_secret = match maybe_secret {
        Some(secret) => secret,
        None => api.get_role_secret(ctx, &info).await?,
    };
    let (cached_entry, secret) = cached_secret.take_value();

    let secret = if let Some(secret) = secret {
        config.check_rate_limit(
            ctx,
            secret,
            &info.endpoint,
            unauthenticated_password.is_some() || allow_cleartext,
        )?
    } else {
        // If we don't have an authentication secret, we mock one to
        // prevent malicious probing (possible due to missing protocol steps).
        // This mocked secret will never lead to successful authentication.
        info!("authentication info not found, mocking it");
        AuthSecret::Scram(scram::ServerSecret::mock(rand::random()))
    };

    match authenticate_with_secret(
        ctx,
        secret,
        info,
        client,
        unauthenticated_password,
        allow_cleartext,
        config,
    )
    .await
    {
        Ok(keys) => Ok(keys),
        Err(e) => {
            if e.is_password_failed() {
                // The password could have been changed, so we invalidate the cache.
                cached_entry.invalidate();
            }
            Err(e)
        }
    }
}

async fn authenticate_with_secret(
    ctx: &RequestMonitoring,
    secret: AuthSecret,
    info: ComputeUserInfo,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    unauthenticated_password: Option<Vec<u8>>,
    allow_cleartext: bool,
    config: &'static AuthenticationConfig,
) -> auth::Result<ComputeCredentials> {
    if let Some(password) = unauthenticated_password {
        let ep = EndpointIdInt::from(&info.endpoint);

        let auth_outcome =
            validate_password_and_exchange(&config.thread_pool, ep, &password, secret).await?;
        let keys = match auth_outcome {
            crate::sasl::Outcome::Success(key) => key,
            crate::sasl::Outcome::Failure(reason) => {
                info!("auth backend failed with an error: {reason}");
                return Err(auth::AuthError::password_failed(&*info.user));
            }
        };

        // we have authenticated the password
        client.write_message_noflush(&pq_proto::BeMessage::AuthenticationOk)?;

        return Ok(ComputeCredentials { info, keys });
    }

    // -- the remaining flows are self-authenticating --

    // Perform cleartext auth if we're allowed to do that.
    // Currently, we use it for websocket connections (latency).
    if allow_cleartext {
        ctx.set_auth_method(crate::context::AuthMethod::Cleartext);
        return hacks::authenticate_cleartext(ctx, info, client, secret, config).await;
    }

    // Finally, proceed with the main auth flow (SCRAM-based).
    classic::authenticate(ctx, info, client, config, secret).await
}

impl ControlPlaneBackend {
    #[tracing::instrument(fields(allow_cleartext = allow_cleartext), skip_all)]
    pub(crate) async fn authenticate(
        &self,
        ctx: &RequestMonitoring,
        user_info: ComputeUserInfoMaybeEndpoint,
        client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
        allow_cleartext: bool,
        config: &'static AuthenticationConfig,
        endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    ) -> auth::Result<ControlPlaneComputeBackend> {
        info!(
            user = &*user_info.user,
            project = user_info.endpoint(),
            "performing authentication using the console"
        );

        let credentials = auth_quirks(
            ctx,
            self,
            user_info,
            client,
            allow_cleartext,
            config,
            endpoint_rate_limiter,
        )
        .await?;

        info!("user successfully authenticated");
        Ok(ControlPlaneComputeBackend {
            api: self,
            creds: credentials,
        })
    }

    pub(crate) fn attach_to_credentials(
        &self,
        creds: ComputeCredentials,
    ) -> ControlPlaneComputeBackend {
        ControlPlaneComputeBackend { api: self, creds }
    }
}

pub struct ControlPlaneComputeBackend<'a> {
    api: &'a ControlPlaneBackend,
    creds: ComputeCredentials,
}

#[async_trait::async_trait]
impl ComputeConnectBackend for ControlPlaneComputeBackend<'static> {
    async fn wake_compute(
        &self,
        ctx: &RequestMonitoring,
    ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError> {
        self.api.wake_compute(ctx, &self.creds.info).await
    }

    fn get_keys(&self) -> &ComputeCredentialKeys {
        &self.creds.keys
    }
}

#[async_trait::async_trait]
impl ComputeConnectBackend for LocalBackend {
    async fn wake_compute(
        &self,
        _ctx: &RequestMonitoring,
    ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError> {
        Ok(Cached::new_uncached(self.node_info.clone()))
    }

    fn get_keys(&self) -> &ComputeCredentialKeys {
        &ComputeCredentialKeys::None
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::BytesMut;
    use fallible_iterator::FallibleIterator;
    use once_cell::sync::Lazy;
    use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
    use postgres_protocol::message::backend::Message as PgMessage;
    use postgres_protocol::message::frontend;
    use provider::AuthSecret;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

    use super::jwt::JwkCache;
    use super::{auth_quirks, AuthRateLimiter};
    use crate::auth::backend::MaskedIp;
    use crate::auth::{ComputeUserInfoMaybeEndpoint, IpPattern};
    use crate::config::AuthenticationConfig;
    use crate::context::RequestMonitoring;
    use crate::control_plane::provider::{self, CachedAllowedIps, CachedRoleSecret};
    use crate::control_plane::{self, CachedNodeInfo};
    use crate::proxy::NeonOptions;
    use crate::rate_limiter::{EndpointRateLimiter, RateBucketInfo};
    use crate::scram::threadpool::ThreadPool;
    use crate::scram::ServerSecret;
    use crate::stream::{PqStream, Stream};

    struct Auth {
        ips: Vec<IpPattern>,
        secret: AuthSecret,
    }

    impl control_plane::Api for Auth {
        async fn get_role_secret(
            &self,
            _ctx: &RequestMonitoring,
            _user_info: &super::ComputeUserInfo,
        ) -> Result<CachedRoleSecret, control_plane::errors::GetAuthInfoError> {
            Ok(CachedRoleSecret::new_uncached(Some(self.secret.clone())))
        }

        async fn get_allowed_ips_and_secret(
            &self,
            _ctx: &RequestMonitoring,
            _user_info: &super::ComputeUserInfo,
        ) -> Result<
            (CachedAllowedIps, Option<CachedRoleSecret>),
            control_plane::errors::GetAuthInfoError,
        > {
            Ok((
                CachedAllowedIps::new_uncached(Arc::new(self.ips.clone())),
                Some(CachedRoleSecret::new_uncached(Some(self.secret.clone()))),
            ))
        }

        async fn get_endpoint_jwks(
            &self,
            _ctx: &RequestMonitoring,
            _endpoint: crate::types::EndpointId,
        ) -> Result<Vec<super::jwt::AuthRule>, control_plane::errors::GetEndpointJwksError>
        {
            unimplemented!()
        }

        async fn wake_compute(
            &self,
            _ctx: &RequestMonitoring,
            _user_info: &super::ComputeUserInfo,
        ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError> {
            unimplemented!()
        }
    }

    static CONFIG: Lazy<AuthenticationConfig> = Lazy::new(|| AuthenticationConfig {
        jwks_cache: JwkCache::default(),
        thread_pool: ThreadPool::new(1),
        scram_protocol_timeout: std::time::Duration::from_secs(5),
        rate_limiter_enabled: true,
        rate_limiter: AuthRateLimiter::new(&RateBucketInfo::DEFAULT_AUTH_SET),
        rate_limit_ip_subnet: 64,
        ip_allowlist_check_enabled: true,
        is_auth_broker: false,
        accept_jwts: false,
        webauth_confirmation_timeout: std::time::Duration::from_secs(5),
    });

    async fn read_message(r: &mut (impl AsyncRead + Unpin), b: &mut BytesMut) -> PgMessage {
        loop {
            r.read_buf(&mut *b).await.unwrap();
            if let Some(m) = PgMessage::parse(&mut *b).unwrap() {
                break m;
            }
        }
    }

    #[test]
    fn masked_ip() {
        let ip_a = IpAddr::V4([127, 0, 0, 1].into());
        let ip_b = IpAddr::V4([127, 0, 0, 2].into());
        let ip_c = IpAddr::V4([192, 168, 1, 101].into());
        let ip_d = IpAddr::V4([192, 168, 1, 102].into());
        let ip_e = IpAddr::V6("abcd:abcd:abcd:abcd:abcd:abcd:abcd:abcd".parse().unwrap());
        let ip_f = IpAddr::V6("abcd:abcd:abcd:abcd:1234:abcd:abcd:abcd".parse().unwrap());

        assert_ne!(MaskedIp::new(ip_a, 64), MaskedIp::new(ip_b, 64));
        assert_ne!(MaskedIp::new(ip_a, 32), MaskedIp::new(ip_b, 32));
        assert_eq!(MaskedIp::new(ip_a, 30), MaskedIp::new(ip_b, 30));
        assert_eq!(MaskedIp::new(ip_c, 30), MaskedIp::new(ip_d, 30));

        assert_ne!(MaskedIp::new(ip_e, 128), MaskedIp::new(ip_f, 128));
        assert_eq!(MaskedIp::new(ip_e, 64), MaskedIp::new(ip_f, 64));
    }

    #[test]
    fn test_default_auth_rate_limit_set() {
        // these values used to exceed u32::MAX
        assert_eq!(
            RateBucketInfo::DEFAULT_AUTH_SET,
            [
                RateBucketInfo {
                    interval: Duration::from_secs(1),
                    max_rpi: 1000 * 4096,
                },
                RateBucketInfo {
                    interval: Duration::from_secs(60),
                    max_rpi: 600 * 4096 * 60,
                },
                RateBucketInfo {
                    interval: Duration::from_secs(600),
                    max_rpi: 300 * 4096 * 600,
                }
            ]
        );

        for x in RateBucketInfo::DEFAULT_AUTH_SET {
            let y = x.to_string().parse().unwrap();
            assert_eq!(x, y);
        }
    }

    #[tokio::test]
    async fn auth_quirks_scram() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut stream = PqStream::new(Stream::from_raw(server));

        let ctx = RequestMonitoring::test();
        let api = Auth {
            ips: vec![],
            secret: AuthSecret::Scram(ServerSecret::build("my-secret-password").await.unwrap()),
        };

        let user_info = ComputeUserInfoMaybeEndpoint {
            user: "conrad".into(),
            endpoint_id: Some("endpoint".into()),
            options: NeonOptions::default(),
        };

        let handle = tokio::spawn(async move {
            let mut scram = ScramSha256::new(b"my-secret-password", ChannelBinding::unsupported());

            let mut read = BytesMut::new();

            // server should offer scram
            match read_message(&mut client, &mut read).await {
                PgMessage::AuthenticationSasl(a) => {
                    let options: Vec<&str> = a.mechanisms().collect().unwrap();
                    assert_eq!(options, ["SCRAM-SHA-256"]);
                }
                _ => panic!("wrong message"),
            }

            // client sends client-first-message
            let mut write = BytesMut::new();
            frontend::sasl_initial_response("SCRAM-SHA-256", scram.message(), &mut write).unwrap();
            client.write_all(&write).await.unwrap();

            // server response with server-first-message
            match read_message(&mut client, &mut read).await {
                PgMessage::AuthenticationSaslContinue(a) => {
                    scram.update(a.data()).await.unwrap();
                }
                _ => panic!("wrong message"),
            }

            // client response with client-final-message
            write.clear();
            frontend::sasl_response(scram.message(), &mut write).unwrap();
            client.write_all(&write).await.unwrap();

            // server response with server-final-message
            match read_message(&mut client, &mut read).await {
                PgMessage::AuthenticationSaslFinal(a) => {
                    scram.finish(a.data()).unwrap();
                }
                _ => panic!("wrong message"),
            }
        });
        let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
            EndpointRateLimiter::DEFAULT,
            64,
        ));

        let _creds = auth_quirks(
            &ctx,
            &api,
            user_info,
            &mut stream,
            false,
            &CONFIG,
            endpoint_rate_limiter,
        )
        .await
        .unwrap();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn auth_quirks_cleartext() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut stream = PqStream::new(Stream::from_raw(server));

        let ctx = RequestMonitoring::test();
        let api = Auth {
            ips: vec![],
            secret: AuthSecret::Scram(ServerSecret::build("my-secret-password").await.unwrap()),
        };

        let user_info = ComputeUserInfoMaybeEndpoint {
            user: "conrad".into(),
            endpoint_id: Some("endpoint".into()),
            options: NeonOptions::default(),
        };

        let handle = tokio::spawn(async move {
            let mut read = BytesMut::new();
            let mut write = BytesMut::new();

            // server should offer cleartext
            match read_message(&mut client, &mut read).await {
                PgMessage::AuthenticationCleartextPassword => {}
                _ => panic!("wrong message"),
            }

            // client responds with password
            write.clear();
            frontend::password_message(b"my-secret-password", &mut write).unwrap();
            client.write_all(&write).await.unwrap();
        });
        let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
            EndpointRateLimiter::DEFAULT,
            64,
        ));

        let _creds = auth_quirks(
            &ctx,
            &api,
            user_info,
            &mut stream,
            true,
            &CONFIG,
            endpoint_rate_limiter,
        )
        .await
        .unwrap();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn auth_quirks_password_hack() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut stream = PqStream::new(Stream::from_raw(server));

        let ctx = RequestMonitoring::test();
        let api = Auth {
            ips: vec![],
            secret: AuthSecret::Scram(ServerSecret::build("my-secret-password").await.unwrap()),
        };

        let user_info = ComputeUserInfoMaybeEndpoint {
            user: "conrad".into(),
            endpoint_id: None,
            options: NeonOptions::default(),
        };

        let handle = tokio::spawn(async move {
            let mut read = BytesMut::new();

            // server should offer cleartext
            match read_message(&mut client, &mut read).await {
                PgMessage::AuthenticationCleartextPassword => {}
                _ => panic!("wrong message"),
            }

            // client responds with password
            let mut write = BytesMut::new();
            frontend::password_message(b"endpoint=my-endpoint;my-secret-password", &mut write)
                .unwrap();
            client.write_all(&write).await.unwrap();
        });

        let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
            EndpointRateLimiter::DEFAULT,
            64,
        ));

        let creds = auth_quirks(
            &ctx,
            &api,
            user_info,
            &mut stream,
            true,
            &CONFIG,
            endpoint_rate_limiter,
        )
        .await
        .unwrap();

        assert_eq!(creds.info.endpoint, "my-endpoint");

        handle.await.unwrap();
    }
}
