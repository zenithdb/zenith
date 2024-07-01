mod classic;
mod hacks;
mod link;

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use ipnet::{Ipv4Net, Ipv6Net};
pub use link::LinkAuthError;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::config::AuthKeys;
use tracing::{info, warn};

use crate::auth::credentials::check_peer_addr_is_in_list;
use crate::auth::{validate_password_and_exchange, AuthError};
use crate::cache::Cached;
use crate::console::errors::GetAuthInfoError;
use crate::console::provider::{CachedRoleSecret, ConsoleBackend};
use crate::console::{AuthSecret, NodeInfo};
use crate::context::RequestMonitoring;
use crate::intern::EndpointIdInt;
use crate::metrics::Metrics;
use crate::proxy::connect_compute::ComputeConnectBackend;
use crate::proxy::NeonOptions;
use crate::rate_limiter::{BucketRateLimiter, EndpointRateLimiter, RateBucketInfo};
use crate::stream::Stream;
use crate::{
    auth::{self, ComputeUserInfoMaybeEndpoint},
    config::AuthenticationConfig,
    console::{
        self,
        provider::{CachedAllowedIps, CachedNodeInfo},
        Api,
    },
    stream, url,
};
use crate::{scram, EndpointCacheKey, EndpointId, RoleName};

/// Alternative to [`std::borrow::Cow`] but doesn't need `T: ToOwned` as we don't need that functionality
pub enum MaybeOwned<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}

impl<T> std::ops::Deref for MaybeOwned<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            MaybeOwned::Owned(t) => t,
            MaybeOwned::Borrowed(t) => t,
        }
    }
}

/// This type serves two purposes:
///
/// * When `T` is `()`, it's just a regular auth backend selector
///   which we use in [`crate::config::ProxyConfig`].
///
/// * However, when we substitute `T` with [`ComputeUserInfoMaybeEndpoint`],
///   this helps us provide the credentials only to those auth
///   backends which require them for the authentication process.
pub enum BackendType<'a, T, D> {
    /// Cloud API (V2).
    Console(MaybeOwned<'a, ConsoleBackend>, T),
    /// Authentication via a web browser.
    Link(MaybeOwned<'a, url::ApiUrl>, D),
}

pub trait TestBackend: Send + Sync + 'static {
    fn wake_compute(&self) -> Result<CachedNodeInfo, console::errors::WakeComputeError>;
    fn get_allowed_ips_and_secret(
        &self,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), console::errors::GetAuthInfoError>;
    fn get_role_secret(&self) -> Result<CachedRoleSecret, console::errors::GetAuthInfoError>;
}

impl std::fmt::Display for BackendType<'_, (), ()> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BackendType::*;
        match self {
            Console(api, _) => match &**api {
                ConsoleBackend::Console(endpoint) => {
                    fmt.debug_tuple("Console").field(&endpoint.url()).finish()
                }
                #[cfg(any(test, feature = "testing"))]
                ConsoleBackend::Postgres(endpoint) => {
                    fmt.debug_tuple("Postgres").field(&endpoint.url()).finish()
                }
                #[cfg(test)]
                ConsoleBackend::Test(_) => fmt.debug_tuple("Test").finish(),
            },
            Link(url, _) => fmt.debug_tuple("Link").field(&url.as_str()).finish(),
        }
    }
}

impl<T, D> BackendType<'_, T, D> {
    /// Very similar to [`std::option::Option::as_ref`].
    /// This helps us pass structured config to async tasks.
    pub fn as_ref(&self) -> BackendType<'_, &T, &D> {
        use BackendType::*;
        match self {
            Console(c, x) => Console(MaybeOwned::Borrowed(c), x),
            Link(c, x) => Link(MaybeOwned::Borrowed(c), x),
        }
    }
}

impl<'a, T, D> BackendType<'a, T, D> {
    /// Very similar to [`std::option::Option::map`].
    /// Maps [`BackendType<T>`] to [`BackendType<R>`] by applying
    /// a function to a contained value.
    pub fn map<R>(self, f: impl FnOnce(T) -> R) -> BackendType<'a, R, D> {
        use BackendType::*;
        match self {
            Console(c, x) => Console(c, f(x)),
            Link(c, x) => Link(c, x),
        }
    }
}
impl<'a, T, D, E> BackendType<'a, Result<T, E>, D> {
    /// Very similar to [`std::option::Option::transpose`].
    /// This is most useful for error handling.
    pub fn transpose(self) -> Result<BackendType<'a, T, D>, E> {
        use BackendType::*;
        match self {
            Console(c, x) => x.map(|x| Console(c, x)),
            Link(c, x) => Ok(Link(c, x)),
        }
    }
}

pub struct ComputeCredentials {
    pub info: ComputeUserInfo,
    pub keys: ComputeCredentialKeys,
}

#[derive(Debug, Clone)]
pub struct ComputeUserInfoNoEndpoint {
    pub user: RoleName,
    pub options: NeonOptions,
}

#[derive(Debug, Clone)]
pub struct ComputeUserInfo {
    pub endpoint: EndpointId,
    pub user: RoleName,
    pub options: NeonOptions,
}

impl ComputeUserInfo {
    pub fn endpoint_cache_key(&self) -> EndpointCacheKey {
        self.options.get_cache_key((&self.endpoint).into())
    }
}

pub enum ComputeCredentialKeys {
    Password(Vec<u8>),
    AuthKeys(AuthKeys),
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
    pub fn check_rate_limit(
        &self,
        ctx: &mut RequestMonitoring,
        config: &AuthenticationConfig,
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
                MaskedIp::new(ctx.peer_addr, config.rate_limit_ip_subnet),
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
    ctx: &mut RequestMonitoring,
    api: &impl console::Api,
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
            let res = hacks::password_hack_no_authentication(ctx, info, client).await?;

            ctx.set_endpoint_id(res.info.endpoint.clone());
            let password = match res.keys {
                ComputeCredentialKeys::Password(p) => p,
                _ => unreachable!("password hack should return a password"),
            };
            (res.info, Some(password))
        }
        Ok(info) => (info, None),
    };

    info!("fetching user's authentication info");
    let (allowed_ips, maybe_secret) = api.get_allowed_ips_and_secret(ctx, &info).await?;

    // check allowed list
    if !check_peer_addr_is_in_list(&ctx.peer_addr, &allowed_ips) {
        return Err(auth::AuthError::ip_address_not_allowed(ctx.peer_addr));
    }

    if !endpoint_rate_limiter.check(info.endpoint.clone().into(), 1) {
        return Err(AuthError::too_many_connections());
    }
    let cached_secret = match maybe_secret {
        Some(secret) => secret,
        None => api.get_role_secret(ctx, &info).await?,
    };
    let (cached_entry, secret) = cached_secret.take_value();

    let secret = match secret {
        Some(secret) => config.check_rate_limit(
            ctx,
            config,
            secret,
            &info.endpoint,
            unauthenticated_password.is_some() || allow_cleartext,
        )?,
        None => {
            // If we don't have an authentication secret, we mock one to
            // prevent malicious probing (possible due to missing protocol steps).
            // This mocked secret will never lead to successful authentication.
            info!("authentication info not found, mocking it");
            AuthSecret::Scram(scram::ServerSecret::mock(rand::random()))
        }
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
            if e.is_auth_failed() {
                // The password could have been changed, so we invalidate the cache.
                cached_entry.invalidate();
            }
            Err(e)
        }
    }
}

async fn authenticate_with_secret(
    ctx: &mut RequestMonitoring,
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
                return Err(auth::AuthError::auth_failed(&*info.user));
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

impl<'a> BackendType<'a, ComputeUserInfoMaybeEndpoint, &()> {
    /// Get compute endpoint name from the credentials.
    pub fn get_endpoint(&self) -> Option<EndpointId> {
        use BackendType::*;

        match self {
            Console(_, user_info) => user_info.endpoint_id.clone(),
            Link(_, _) => Some("link".into()),
        }
    }

    /// Get username from the credentials.
    pub fn get_user(&self) -> &str {
        use BackendType::*;

        match self {
            Console(_, user_info) => &user_info.user,
            Link(_, _) => "link",
        }
    }

    /// Authenticate the client via the requested backend, possibly using credentials.
    #[tracing::instrument(fields(allow_cleartext = allow_cleartext), skip_all)]
    pub async fn authenticate(
        self,
        ctx: &mut RequestMonitoring,
        client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
        allow_cleartext: bool,
        config: &'static AuthenticationConfig,
        endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    ) -> auth::Result<BackendType<'a, ComputeCredentials, NodeInfo>> {
        use BackendType::*;

        let res = match self {
            Console(api, user_info) => {
                info!(
                    user = &*user_info.user,
                    project = user_info.endpoint(),
                    "performing authentication using the console"
                );

                let credentials = auth_quirks(
                    ctx,
                    &*api,
                    user_info,
                    client,
                    allow_cleartext,
                    config,
                    endpoint_rate_limiter,
                )
                .await?;
                BackendType::Console(api, credentials)
            }
            // NOTE: this auth backend doesn't use client credentials.
            Link(url, _) => {
                info!("performing link authentication");

                let info = link::authenticate(ctx, &url, client).await?;

                BackendType::Link(url, info)
            }
        };

        info!("user successfully authenticated");
        Ok(res)
    }
}

impl BackendType<'_, ComputeUserInfo, &()> {
    pub async fn get_role_secret(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<CachedRoleSecret, GetAuthInfoError> {
        use BackendType::*;
        match self {
            Console(api, user_info) => api.get_role_secret(ctx, user_info).await,
            Link(_, _) => Ok(Cached::new_uncached(None)),
        }
    }

    pub async fn get_allowed_ips_and_secret(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), GetAuthInfoError> {
        use BackendType::*;
        match self {
            Console(api, user_info) => api.get_allowed_ips_and_secret(ctx, user_info).await,
            Link(_, _) => Ok((Cached::new_uncached(Arc::new(vec![])), None)),
        }
    }
}

#[async_trait::async_trait]
impl ComputeConnectBackend for BackendType<'_, ComputeCredentials, NodeInfo> {
    async fn wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<CachedNodeInfo, console::errors::WakeComputeError> {
        use BackendType::*;

        match self {
            Console(api, creds) => api.wake_compute(ctx, &creds.info).await,
            Link(_, info) => Ok(Cached::new_uncached(info.clone())),
        }
    }

    fn get_keys(&self) -> Option<&ComputeCredentialKeys> {
        match self {
            BackendType::Console(_, creds) => Some(&creds.keys),
            BackendType::Link(_, _) => None,
        }
    }
}

#[async_trait::async_trait]
impl ComputeConnectBackend for BackendType<'_, ComputeCredentials, &()> {
    async fn wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<CachedNodeInfo, console::errors::WakeComputeError> {
        use BackendType::*;

        match self {
            Console(api, creds) => api.wake_compute(ctx, &creds.info).await,
            Link(_, _) => unreachable!("link auth flow doesn't support waking the compute"),
        }
    }

    fn get_keys(&self) -> Option<&ComputeCredentialKeys> {
        match self {
            BackendType::Console(_, creds) => Some(&creds.keys),
            BackendType::Link(_, _) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, sync::Arc, time::Duration};

    use bytes::BytesMut;
    use fallible_iterator::FallibleIterator;
    use once_cell::sync::Lazy;
    use postgres_protocol::{
        authentication::sasl::{ChannelBinding, ScramSha256},
        message::{backend::Message as PgMessage, frontend},
    };
    use provider::AuthSecret;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

    use crate::{
        auth::{backend::MaskedIp, ComputeUserInfoMaybeEndpoint, IpPattern},
        config::AuthenticationConfig,
        console::{
            self,
            provider::{self, CachedAllowedIps, CachedRoleSecret},
            CachedNodeInfo,
        },
        context::RequestMonitoring,
        proxy::NeonOptions,
        rate_limiter::{EndpointRateLimiter, RateBucketInfo},
        scram::{threadpool::ThreadPool, ServerSecret},
        stream::{PqStream, Stream},
    };

    use super::{auth_quirks, AuthRateLimiter};

    struct Auth {
        ips: Vec<IpPattern>,
        secret: AuthSecret,
    }

    impl console::Api for Auth {
        async fn get_role_secret(
            &self,
            _ctx: &mut RequestMonitoring,
            _user_info: &super::ComputeUserInfo,
        ) -> Result<CachedRoleSecret, console::errors::GetAuthInfoError> {
            Ok(CachedRoleSecret::new_uncached(Some(self.secret.clone())))
        }

        async fn get_allowed_ips_and_secret(
            &self,
            _ctx: &mut RequestMonitoring,
            _user_info: &super::ComputeUserInfo,
        ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), console::errors::GetAuthInfoError>
        {
            Ok((
                CachedAllowedIps::new_uncached(Arc::new(self.ips.clone())),
                Some(CachedRoleSecret::new_uncached(Some(self.secret.clone()))),
            ))
        }

        async fn wake_compute(
            &self,
            _ctx: &mut RequestMonitoring,
            _user_info: &super::ComputeUserInfo,
        ) -> Result<CachedNodeInfo, console::errors::WakeComputeError> {
            unimplemented!()
        }
    }

    static CONFIG: Lazy<AuthenticationConfig> = Lazy::new(|| AuthenticationConfig {
        thread_pool: ThreadPool::new(1),
        scram_protocol_timeout: std::time::Duration::from_secs(5),
        rate_limiter_enabled: true,
        rate_limiter: AuthRateLimiter::new(&RateBucketInfo::DEFAULT_AUTH_SET),
        rate_limit_ip_subnet: 64,
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

        let mut ctx = RequestMonitoring::test();
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
        let endpoint_rate_limiter =
            Arc::new(EndpointRateLimiter::new(&RateBucketInfo::DEFAULT_AUTH_SET));

        let _creds = auth_quirks(
            &mut ctx,
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

        let mut ctx = RequestMonitoring::test();
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
        let endpoint_rate_limiter =
            Arc::new(EndpointRateLimiter::new(&RateBucketInfo::DEFAULT_AUTH_SET));

        let _creds = auth_quirks(
            &mut ctx,
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

        let mut ctx = RequestMonitoring::test();
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

        let endpoint_rate_limiter =
            Arc::new(EndpointRateLimiter::new(&RateBucketInfo::DEFAULT_AUTH_SET));

        let creds = auth_quirks(
            &mut ctx,
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
