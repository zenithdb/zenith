//! Production console backend.

use super::{
    super::messages::{ConsoleError, GetRoleSecret, WakeCompute},
    errors::{ApiError, GetAuthInfoError, WakeComputeError},
    ApiCaches, ApiLocks, AuthInfo, AuthSecret, CachedAllowedIps, CachedNodeInfo, CachedRoleSecret,
    NodeCachedInfo,
};
use crate::{
    auth::backend::ComputeUserInfo,
    console::messages::ColdStartInfo,
    http,
    metrics::{CacheOutcome, Metrics},
    rate_limiter::EndpointRateLimiter,
    scram, EndpointCacheKey, Host,
};
use crate::{cache::Cached, context::RequestMonitoring};
use futures::TryFutureExt;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::{error, info, info_span, warn, Instrument};

pub struct Api {
    endpoint: http::Endpoint,
    pub caches: &'static ApiCaches,
    pub locks: &'static ApiLocks<EndpointCacheKey>,
    pub wake_compute_endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    jwt: String,
}

impl Api {
    /// Construct an API object containing the auth parameters.
    pub fn new(
        endpoint: http::Endpoint,
        caches: &'static ApiCaches,
        locks: &'static ApiLocks<EndpointCacheKey>,
        wake_compute_endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    ) -> Self {
        let jwt: String = match std::env::var("NEON_PROXY_TO_CONTROLPLANE_TOKEN") {
            Ok(v) => v,
            Err(_) => "".to_string(),
        };
        Self {
            endpoint,
            caches,
            locks,
            wake_compute_endpoint_rate_limiter,
            jwt,
        }
    }

    pub fn url(&self) -> &str {
        self.endpoint.url().as_str()
    }

    async fn do_get_auth_info(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<AuthInfo, GetAuthInfoError> {
        if !self
            .caches
            .endpoints_cache
            .is_valid(ctx, &user_info.endpoint.normalize())
            .await
        {
            info!("endpoint is not valid, skipping the request");
            return Ok(AuthInfo::default());
        }
        let request_id = ctx.session_id.to_string();
        let application_name = ctx.console_application_name();
        async {
            let request = self
                .endpoint
                .get("proxy_get_role_secret")
                .header("X-Request-ID", &request_id)
                .header("Authorization", format!("Bearer {}", &self.jwt))
                .query(&[("session_id", ctx.session_id)])
                .query(&[
                    ("application_name", application_name.as_str()),
                    ("project", user_info.endpoint.as_str()),
                    ("role", user_info.user.as_str()),
                ])
                .build()?;

            info!(url = request.url().as_str(), "sending http request");
            let start = Instant::now();
            let pause = ctx.latency_timer.pause(crate::metrics::Waiting::Cplane);
            let response = self.endpoint.execute(request).await?;
            drop(pause);
            info!(duration = ?start.elapsed(), "received http response");
            let body = match parse_body::<GetRoleSecret>(response).await {
                Ok(body) => body,
                // Error 404 is special: it's ok not to have a secret.
                // TODO(anna): retry
                Err(e) => {
                    if e.get_reason().is_not_found() {
                        return Ok(AuthInfo::default());
                    } else {
                        return Err(e.into());
                    }
                }
            };

            let secret = if body.role_secret.is_empty() {
                None
            } else {
                let secret = scram::ServerSecret::parse(&body.role_secret)
                    .map(AuthSecret::Scram)
                    .ok_or(GetAuthInfoError::BadSecret)?;
                Some(secret)
            };
            let allowed_ips = body.allowed_ips.unwrap_or_default();
            Metrics::get()
                .proxy
                .allowed_ips_number
                .observe(allowed_ips.len() as f64);
            Ok(AuthInfo {
                secret,
                allowed_ips,
                project_id: body.project_id,
            })
        }
        .map_err(crate::error::log_error)
        .instrument(info_span!("http", id = request_id))
        .await
    }

    async fn do_wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<NodeCachedInfo, WakeComputeError> {
        let request_id = ctx.session_id.to_string();
        let application_name = ctx.console_application_name();
        async {
            let mut request_builder = self
                .endpoint
                .get("proxy_wake_compute")
                .header("X-Request-ID", &request_id)
                .header("Authorization", format!("Bearer {}", &self.jwt))
                .query(&[("session_id", ctx.session_id)])
                .query(&[
                    ("application_name", application_name.as_str()),
                    ("project", user_info.endpoint.as_str()),
                ]);

            let options = user_info.options.to_deep_object();
            if !options.is_empty() {
                request_builder = request_builder.query(&options);
            }

            let request = request_builder.build()?;

            info!(url = request.url().as_str(), "sending http request");
            let start = Instant::now();
            let pause = ctx.latency_timer.pause(crate::metrics::Waiting::Cplane);
            let response = self.endpoint.execute(request).await?;
            drop(pause);
            info!(duration = ?start.elapsed(), "received http response");
            let body = parse_body::<WakeCompute>(response).await?;

            // Unfortunately, ownership won't let us use `Option::ok_or` here.
            let (host, port) = match parse_host_port(&body.address) {
                None => return Err(WakeComputeError::BadComputeAddress(body.address)),
                Some(x) => x,
            };
            let host = Host(host.into());

            let node = NodeCachedInfo {
                host,
                port,
                aux: body.aux,
                allow_self_signed_compute: false,
            };

            Ok(node)
        }
        .map_err(crate::error::log_error)
        .instrument(info_span!("http", id = request_id))
        .await
    }
}

impl super::Api for Api {
    #[tracing::instrument(skip_all)]
    async fn get_role_secret(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedRoleSecret, GetAuthInfoError> {
        let normalized_ep = &user_info.endpoint.normalize();
        let user = &user_info.user;
        if let Some(role_secret) = self
            .caches
            .project_info
            .get_role_secret(normalized_ep, user)
        {
            return Ok(role_secret);
        }
        let auth_info = self.do_get_auth_info(ctx, user_info).await?;
        if let Some(project_id) = auth_info.project_id {
            let normalized_ep_int = normalized_ep.into();
            self.caches.project_info.insert_role_secret(
                project_id,
                normalized_ep_int,
                user.into(),
                auth_info.secret.clone(),
            );
            self.caches.project_info.insert_allowed_ips(
                project_id,
                normalized_ep_int,
                Arc::new(auth_info.allowed_ips),
            );
            ctx.set_project_id(project_id);
        }
        // When we just got a secret, we don't need to invalidate it.
        Ok(Cached::new_uncached(auth_info.secret))
    }

    async fn get_allowed_ips_and_secret(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), GetAuthInfoError> {
        let normalized_ep = &user_info.endpoint.normalize();
        if let Some(allowed_ips) = self.caches.project_info.get_allowed_ips(normalized_ep) {
            Metrics::get()
                .proxy
                .allowed_ips_cache_misses
                .inc(CacheOutcome::Hit);
            return Ok((allowed_ips, None));
        }
        Metrics::get()
            .proxy
            .allowed_ips_cache_misses
            .inc(CacheOutcome::Miss);
        let auth_info = self.do_get_auth_info(ctx, user_info).await?;
        let allowed_ips = Arc::new(auth_info.allowed_ips);
        let user = &user_info.user;
        if let Some(project_id) = auth_info.project_id {
            let normalized_ep_int = normalized_ep.into();
            self.caches.project_info.insert_role_secret(
                project_id,
                normalized_ep_int,
                user.into(),
                auth_info.secret.clone(),
            );
            self.caches.project_info.insert_allowed_ips(
                project_id,
                normalized_ep_int,
                allowed_ips.clone(),
            );
            ctx.set_project_id(project_id);
        }
        Ok((
            Cached::new_uncached(allowed_ips),
            Some(Cached::new_uncached(auth_info.secret)),
        ))
    }

    #[tracing::instrument(skip_all)]
    async fn wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, WakeComputeError> {
        let key = user_info.endpoint_cache_key();

        // Every time we do a wakeup http request, the compute node will stay up
        // for some time (highly depends on the console's scale-to-zero policy);
        // The connection info remains the same during that period of time,
        // which means that we might cache it to reduce the load and latency.
        if let Some(cached) = self.caches.node_info.get(&key) {
            info!(key = display(&key), "found cached compute node info");
            ctx.set_project(cached.aux.clone());
            return Ok(cached.map(NodeCachedInfo::into_node_info));
        }

        let permit = self.locks.get_permit(&key).await?;

        // after getting back a permit - it's possible the cache was filled
        // double check
        if permit.should_check_cache() {
            if let Some(cached) = self.caches.node_info.get(&key) {
                info!(key = display(&key), "found cached compute node info");
                ctx.set_project(cached.aux.clone());
                return Ok(cached.map(NodeCachedInfo::into_node_info));
            }
        }

        // check rate limit
        if !self
            .wake_compute_endpoint_rate_limiter
            .check(user_info.endpoint.normalize_intern(), 1)
        {
            info!(key = display(&key), "found cached compute node info");
            return Err(WakeComputeError::TooManyConnections);
        }

        let mut node = permit.release_result(self.do_wake_compute(ctx, user_info).await)?;
        ctx.set_project(node.aux.clone());
        let cold_start_info = node.aux.cold_start_info;
        info!("woken up a compute node");

        // store the cached node as 'warm'
        node.aux.cold_start_info = ColdStartInfo::WarmCached;
        let (_, mut cached) = self.caches.node_info.insert(key.clone(), node);
        cached.aux.cold_start_info = cold_start_info;

        info!(
            key = display(&key),
            "created a cache entry for compute node info"
        );

        Ok(cached.map(NodeCachedInfo::into_node_info))
    }
}

/// Parse http response body, taking status code into account.
async fn parse_body<T: for<'a> serde::Deserialize<'a>>(
    response: http::Response,
) -> Result<T, ApiError> {
    let status = response.status();
    if status.is_success() {
        // We shouldn't log raw body because it may contain secrets.
        info!("request succeeded, processing the body");
        return Ok(response.json().await?);
    }
    let s = response.bytes().await?;
    // Log plaintext to be able to detect, whether there are some cases not covered by the error struct.
    info!("response_error plaintext: {:?}", s);

    // Don't throw an error here because it's not as important
    // as the fact that the request itself has failed.
    let mut body = serde_json::from_slice(&s).unwrap_or_else(|e| {
        warn!("failed to parse error body: {e}");
        ConsoleError {
            error: "reason unclear (malformed error message)".into(),
            http_status_code: status,
            status: None,
        }
    });
    body.http_status_code = status;

    error!("console responded with an error ({status}): {body:?}");
    Err(ApiError::Console(body))
}

fn parse_host_port(input: &str) -> Option<(&str, u16)> {
    let (host, port) = input.rsplit_once(':')?;
    let ipv6_brackets: &[_] = &['[', ']'];
    Some((host.trim_matches(ipv6_brackets), port.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host_port_v4() {
        let (host, port) = parse_host_port("127.0.0.1:5432").expect("failed to parse");
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 5432);
    }

    #[test]
    fn test_parse_host_port_v6() {
        let (host, port) = parse_host_port("[2001:db8::1]:5432").expect("failed to parse");
        assert_eq!(host, "2001:db8::1");
        assert_eq!(port, 5432);
    }

    #[test]
    fn test_parse_host_port_url() {
        let (host, port) = parse_host_port("compute-foo-bar-1234.default.svc.cluster.local:5432")
            .expect("failed to parse");
        assert_eq!(host, "compute-foo-bar-1234.default.svc.cluster.local");
        assert_eq!(port, 5432);
    }
}
