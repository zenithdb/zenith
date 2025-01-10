mod classic;
mod console_redirect;
mod hacks;
pub mod jwt;
pub mod local;

use std::net::IpAddr;
use std::sync::Arc;

pub use console_redirect::ConsoleRedirectBackend;
pub(crate) use console_redirect::ConsoleRedirectError;
use ipnet::{Ipv4Net, Ipv6Net};
use local::LocalBackend;
use postgres_client::config::AuthKeys;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info, warn};

use crate::auth::credentials::check_peer_addr_is_in_list;
use crate::auth::{
    self, validate_password_and_exchange, AuthError, ComputeUserInfoMaybeEndpoint, IpPattern,
};
use crate::cache::Cached;
use crate::config::AuthenticationConfig;
use crate::context::RequestContext;
use crate::control_plane::client::ControlPlaneClient;
use crate::control_plane::errors::GetAuthInfoError;
use crate::control_plane::{
    self, AuthSecret, CachedAllowedIps, CachedNodeInfo, CachedRoleSecret, ControlPlaneApi,
};
use crate::intern::EndpointIdInt;
use crate::metrics::Metrics;
use crate::proxy::connect_compute::ComputeConnectBackend;
use crate::proxy::NeonOptions;
use crate::rate_limiter::{BucketRateLimiter, EndpointRateLimiter};
use crate::stream::Stream;
use crate::types::{EndpointCacheKey, EndpointId, RoleName};
use crate::{scram, stream};

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
pub enum Backend<'a, T> {
    /// Cloud API (V2).
    ControlPlane(MaybeOwned<'a, ControlPlaneClient>, T),
    /// Local proxy uses configured auth credentials and does not wake compute
    Local(MaybeOwned<'a, LocalBackend>),
}

impl std::fmt::Display for Backend<'_, ()> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ControlPlane(api, ()) => match &**api {
                ControlPlaneClient::ProxyV1(endpoint) => fmt
                    .debug_tuple("ControlPlane::ProxyV1")
                    .field(&endpoint.url())
                    .finish(),
                #[cfg(any(test, feature = "testing"))]
                ControlPlaneClient::PostgresMock(endpoint) => fmt
                    .debug_tuple("ControlPlane::PostgresMock")
                    .field(&endpoint.url())
                    .finish(),
                #[cfg(test)]
                ControlPlaneClient::Test(_) => fmt.debug_tuple("ControlPlane::Test").finish(),
            },
            Self::Local(_) => fmt.debug_tuple("Local").finish(),
        }
    }
}

impl<T> Backend<'_, T> {
    /// Very similar to [`std::option::Option::as_ref`].
    /// This helps us pass structured config to async tasks.
    pub(crate) fn as_ref(&self) -> Backend<'_, &T> {
        match self {
            Self::ControlPlane(c, x) => Backend::ControlPlane(MaybeOwned::Borrowed(c), x),
            Self::Local(l) => Backend::Local(MaybeOwned::Borrowed(l)),
        }
    }
}

impl<'a, T> Backend<'a, T> {
    /// Very similar to [`std::option::Option::map`].
    /// Maps [`Backend<T>`] to [`Backend<R>`] by applying
    /// a function to a contained value.
    pub(crate) fn map<R>(self, f: impl FnOnce(T) -> R) -> Backend<'a, R> {
        match self {
            Self::ControlPlane(c, x) => Backend::ControlPlane(c, f(x)),
            Self::Local(l) => Backend::Local(l),
        }
    }
}
impl<'a, T, E> Backend<'a, Result<T, E>> {
    /// Very similar to [`std::option::Option::transpose`].
    /// This is most useful for error handling.
    pub(crate) fn transpose(self) -> Result<Backend<'a, T>, E> {
        match self {
            Self::ControlPlane(c, x) => x.map(|x| Backend::ControlPlane(c, x)),
            Self::Local(l) => Ok(Backend::Local(l)),
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

#[derive(Debug, Clone, Default)]
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

impl AuthenticationConfig {
    pub(crate) fn check_rate_limit(
        &self,
        ctx: &RequestContext,
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

#[async_trait::async_trait]
pub(crate) trait BackendIpAllowlist {
    async fn get_allowed_ips(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> auth::Result<Vec<auth::IpPattern>>;
}

/// True to its name, this function encapsulates our current auth trade-offs.
/// Here, we choose the appropriate auth flow based on circumstances.
///
/// All authentication flows will emit an AuthenticationOk message if successful.
async fn auth_quirks(
    ctx: &RequestContext,
    api: &impl control_plane::ControlPlaneApi,
    user_info: ComputeUserInfoMaybeEndpoint,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    allow_cleartext: bool,
    config: &'static AuthenticationConfig,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> auth::Result<(ComputeCredentials, Option<Vec<IpPattern>>)> {
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

    debug!("fetching user's authentication info");
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
        Ok(keys) => Ok((keys, Some(allowed_ips.as_ref().clone()))),
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
    ctx: &RequestContext,
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

impl<'a> Backend<'a, ComputeUserInfoMaybeEndpoint> {
    /// Get username from the credentials.
    pub(crate) fn get_user(&self) -> &str {
        match self {
            Self::ControlPlane(_, user_info) => &user_info.user,
            Self::Local(_) => "local",
        }
    }

    /// Authenticate the client via the requested backend, possibly using credentials.
    #[tracing::instrument(fields(allow_cleartext = allow_cleartext), skip_all)]
    pub(crate) async fn authenticate(
        self,
        ctx: &RequestContext,
        client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
        allow_cleartext: bool,
        config: &'static AuthenticationConfig,
        endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    ) -> auth::Result<(Backend<'a, ComputeCredentials>, Option<Vec<IpPattern>>)> {
        let res = match self {
            Self::ControlPlane(api, user_info) => {
                debug!(
                    user = &*user_info.user,
                    project = user_info.endpoint(),
                    "performing authentication using the console"
                );

                let (credentials, ip_allowlist) = auth_quirks(
                    ctx,
                    &*api,
                    user_info,
                    client,
                    allow_cleartext,
                    config,
                    endpoint_rate_limiter,
                )
                .await?;
                Ok((Backend::ControlPlane(api, credentials), ip_allowlist))
            }
            Self::Local(_) => {
                return Err(auth::AuthError::bad_auth_method("invalid for local proxy"))
            }
        };

        // TODO: replace with some metric
        info!("user successfully authenticated");
        res
    }
}

impl Backend<'_, ComputeUserInfo> {
    pub(crate) async fn get_role_secret(
        &self,
        ctx: &RequestContext,
    ) -> Result<CachedRoleSecret, GetAuthInfoError> {
        match self {
            Self::ControlPlane(api, user_info) => api.get_role_secret(ctx, user_info).await,
            Self::Local(_) => Ok(Cached::new_uncached(None)),
        }
    }

    pub(crate) async fn get_allowed_ips_and_secret(
        &self,
        ctx: &RequestContext,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), GetAuthInfoError> {
        match self {
            Self::ControlPlane(api, user_info) => {
                api.get_allowed_ips_and_secret(ctx, user_info).await
            }
            Self::Local(_) => Ok((Cached::new_uncached(Arc::new(vec![])), None)),
        }
    }
}

#[async_trait::async_trait]
impl BackendIpAllowlist for Backend<'_, ()> {
    async fn get_allowed_ips(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> auth::Result<Vec<auth::IpPattern>> {
        let auth_data = match self {
            Self::ControlPlane(api, ()) => api.get_allowed_ips_and_secret(ctx, user_info).await,
            Self::Local(_) => Ok((Cached::new_uncached(Arc::new(vec![])), None)),
        };

        auth_data
            .map(|(ips, _)| ips.as_ref().clone())
            .map_err(|e| e.into())
    }
}

#[async_trait::async_trait]
impl ComputeConnectBackend for Backend<'_, ComputeCredentials> {
    async fn wake_compute(
        &self,
        ctx: &RequestContext,
    ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError> {
        match self {
            Self::ControlPlane(api, creds) => api.wake_compute(ctx, &creds.info).await,
            Self::Local(local) => Ok(Cached::new_uncached(local.node_info.clone())),
        }
    }

    fn get_keys(&self) -> &ComputeCredentialKeys {
        match self {
            Self::ControlPlane(_, creds) => &creds.keys,
            Self::Local(_) => &ComputeCredentialKeys::None,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unimplemented, clippy::unwrap_used)]

    use std::net::IpAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::BytesMut;
    use control_plane::AuthSecret;
    use fallible_iterator::FallibleIterator;
    use once_cell::sync::Lazy;
    use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
    use postgres_protocol::message::backend::Message as PgMessage;
    use postgres_protocol::message::frontend;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

    use super::jwt::JwkCache;
    use super::{auth_quirks, AuthRateLimiter};
    use crate::auth::backend::MaskedIp;
    use crate::auth::{ComputeUserInfoMaybeEndpoint, IpPattern};
    use crate::config::AuthenticationConfig;
    use crate::context::RequestContext;
    use crate::control_plane::{self, CachedAllowedIps, CachedNodeInfo, CachedRoleSecret};
    use crate::proxy::NeonOptions;
    use crate::rate_limiter::{EndpointRateLimiter, RateBucketInfo};
    use crate::scram::threadpool::ThreadPool;
    use crate::scram::ServerSecret;
    use crate::stream::{PqStream, Stream};

    struct Auth {
        ips: Vec<IpPattern>,
        secret: AuthSecret,
    }

    impl control_plane::ControlPlaneApi for Auth {
        async fn get_role_secret(
            &self,
            _ctx: &RequestContext,
            _user_info: &super::ComputeUserInfo,
        ) -> Result<CachedRoleSecret, control_plane::errors::GetAuthInfoError> {
            Ok(CachedRoleSecret::new_uncached(Some(self.secret.clone())))
        }

        async fn get_allowed_ips_and_secret(
            &self,
            _ctx: &RequestContext,
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
            _ctx: &RequestContext,
            _endpoint: crate::types::EndpointId,
        ) -> Result<Vec<super::jwt::AuthRule>, control_plane::errors::GetEndpointJwksError>
        {
            unimplemented!()
        }

        async fn wake_compute(
            &self,
            _ctx: &RequestContext,
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
        console_redirect_confirmation_timeout: std::time::Duration::from_secs(5),
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

        let ctx = RequestContext::test();
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

        // flush the final server message
        stream.flush().await.unwrap();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn auth_quirks_cleartext() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut stream = PqStream::new(Stream::from_raw(server));

        let ctx = RequestContext::test();
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

        let ctx = RequestContext::test();
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

        assert_eq!(creds.0.info.endpoint, "my-endpoint");

        handle.await.unwrap();
    }
}
