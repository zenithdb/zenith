use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Ok};
use clap::ValueEnum;
use remote_storage::RemoteStorageConfig;

use crate::auth::backend::jwt::JwkCache;
use crate::auth::backend::AuthRateLimiter;
use crate::control_plane::locks::ApiLocks;
use crate::rate_limiter::{RateBucketInfo, RateLimitAlgorithm, RateLimiterConfig};
use crate::scram::threadpool::ThreadPool;
use crate::serverless::cancel_set::CancelSet;
use crate::serverless::GlobalConnPoolOptions;
pub use crate::tls::server_config::{configure_tls, TlsConfig};
use crate::types::Host;

pub struct ProxyConfig {
    pub tls_config: Option<TlsConfig>,
    pub metric_collection: Option<MetricCollectionConfig>,
    pub http_config: HttpConfig,
    pub authentication_config: AuthenticationConfig,
    pub proxy_protocol_v2: ProxyProtocolV2,
    pub region: String,
    pub handshake_timeout: Duration,
    pub wake_compute_retry_config: RetryConfig,
    pub connect_compute_locks: ApiLocks<Host>,
    pub connect_to_compute: ComputeConfig,
}

pub struct ComputeConfig {
    pub retry: RetryConfig,
    pub tls: Arc<rustls::ClientConfig>,
    pub timeout: Duration,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq)]
pub enum ProxyProtocolV2 {
    /// Connection will error if PROXY protocol v2 header is missing
    Required,
    /// Connection will parse PROXY protocol v2 header, but accept the connection if it's missing.
    Supported,
    /// Connection will error if PROXY protocol v2 header is provided
    Rejected,
}

#[derive(Debug)]
pub struct MetricCollectionConfig {
    pub endpoint: reqwest::Url,
    pub interval: Duration,
    pub backup_metric_collection_config: MetricBackupCollectionConfig,
}

pub struct HttpConfig {
    pub accept_websockets: bool,
    pub pool_options: GlobalConnPoolOptions,
    pub cancel_set: CancelSet,
    pub client_conn_threshold: u64,
    pub max_request_size_bytes: usize,
    pub max_response_size_bytes: usize,
}

pub struct AuthenticationConfig {
    pub thread_pool: Arc<ThreadPool>,
    pub scram_protocol_timeout: tokio::time::Duration,
    pub rate_limiter_enabled: bool,
    pub rate_limiter: AuthRateLimiter,
    pub rate_limit_ip_subnet: u8,
    pub ip_allowlist_check_enabled: bool,
    pub jwks_cache: JwkCache,
    pub is_auth_broker: bool,
    pub accept_jwts: bool,
    pub console_redirect_confirmation_timeout: tokio::time::Duration,
}

#[derive(Debug)]
pub struct EndpointCacheConfig {
    /// Batch size to receive all endpoints on the startup.
    pub initial_batch_size: usize,
    /// Batch size to receive endpoints.
    pub default_batch_size: usize,
    /// Timeouts for the stream read operation.
    pub xread_timeout: Duration,
    /// Stream name to read from.
    pub stream_name: String,
    /// Limiter info (to distinguish when to enable cache).
    pub limiter_info: Vec<RateBucketInfo>,
    /// Disable cache.
    /// If true, cache is ignored, but reports all statistics.
    pub disable_cache: bool,
    /// Retry interval for the stream read operation.
    pub retry_interval: Duration,
}

impl EndpointCacheConfig {
    /// Default options for [`crate::control_plane::NodeInfoCache`].
    /// Notice that by default the limiter is empty, which means that cache is disabled.
    pub const CACHE_DEFAULT_OPTIONS: &'static str =
        "initial_batch_size=1000,default_batch_size=10,xread_timeout=5m,stream_name=controlPlane,disable_cache=true,limiter_info=1000@1s,retry_interval=1s";

    /// Parse cache options passed via cmdline.
    /// Example: [`Self::CACHE_DEFAULT_OPTIONS`].
    fn parse(options: &str) -> anyhow::Result<Self> {
        let mut initial_batch_size = None;
        let mut default_batch_size = None;
        let mut xread_timeout = None;
        let mut stream_name = None;
        let mut limiter_info = vec![];
        let mut disable_cache = false;
        let mut retry_interval = None;

        for option in options.split(',') {
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("bad key-value pair: {option}"))?;

            match key {
                "initial_batch_size" => initial_batch_size = Some(value.parse()?),
                "default_batch_size" => default_batch_size = Some(value.parse()?),
                "xread_timeout" => xread_timeout = Some(humantime::parse_duration(value)?),
                "stream_name" => stream_name = Some(value.to_string()),
                "limiter_info" => limiter_info.push(RateBucketInfo::from_str(value)?),
                "disable_cache" => disable_cache = value.parse()?,
                "retry_interval" => retry_interval = Some(humantime::parse_duration(value)?),
                unknown => bail!("unknown key: {unknown}"),
            }
        }
        RateBucketInfo::validate(&mut limiter_info)?;

        Ok(Self {
            initial_batch_size: initial_batch_size.context("missing `initial_batch_size`")?,
            default_batch_size: default_batch_size.context("missing `default_batch_size`")?,
            xread_timeout: xread_timeout.context("missing `xread_timeout`")?,
            stream_name: stream_name.context("missing `stream_name`")?,
            disable_cache,
            limiter_info,
            retry_interval: retry_interval.context("missing `retry_interval`")?,
        })
    }
}

impl FromStr for EndpointCacheConfig {
    type Err = anyhow::Error;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let error = || format!("failed to parse endpoint cache options '{options}'");
        Self::parse(options).with_context(error)
    }
}
#[derive(Debug)]
pub struct MetricBackupCollectionConfig {
    pub interval: Duration,
    pub remote_storage_config: Option<RemoteStorageConfig>,
    pub chunk_size: usize,
}

pub fn remote_storage_from_toml(s: &str) -> anyhow::Result<RemoteStorageConfig> {
    RemoteStorageConfig::from_toml(&s.parse()?)
}

/// Helper for cmdline cache options parsing.
#[derive(Debug)]
pub struct CacheOptions {
    /// Max number of entries.
    pub size: usize,
    /// Entry's time-to-live.
    pub ttl: Duration,
}

impl CacheOptions {
    /// Default options for [`crate::control_plane::NodeInfoCache`].
    pub const CACHE_DEFAULT_OPTIONS: &'static str = "size=4000,ttl=4m";

    /// Parse cache options passed via cmdline.
    /// Example: [`Self::CACHE_DEFAULT_OPTIONS`].
    fn parse(options: &str) -> anyhow::Result<Self> {
        let mut size = None;
        let mut ttl = None;

        for option in options.split(',') {
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("bad key-value pair: {option}"))?;

            match key {
                "size" => size = Some(value.parse()?),
                "ttl" => ttl = Some(humantime::parse_duration(value)?),
                unknown => bail!("unknown key: {unknown}"),
            }
        }

        // TTL doesn't matter if cache is always empty.
        if let Some(0) = size {
            ttl.get_or_insert(Duration::default());
        }

        Ok(Self {
            size: size.context("missing `size`")?,
            ttl: ttl.context("missing `ttl`")?,
        })
    }
}

impl FromStr for CacheOptions {
    type Err = anyhow::Error;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let error = || format!("failed to parse cache options '{options}'");
        Self::parse(options).with_context(error)
    }
}

/// Helper for cmdline cache options parsing.
#[derive(Debug)]
pub struct ProjectInfoCacheOptions {
    /// Max number of entries.
    pub size: usize,
    /// Entry's time-to-live.
    pub ttl: Duration,
    /// Max number of roles per endpoint.
    pub max_roles: usize,
    /// Gc interval.
    pub gc_interval: Duration,
}

impl ProjectInfoCacheOptions {
    /// Default options for [`crate::control_plane::NodeInfoCache`].
    pub const CACHE_DEFAULT_OPTIONS: &'static str =
        "size=10000,ttl=4m,max_roles=10,gc_interval=60m";

    /// Parse cache options passed via cmdline.
    /// Example: [`Self::CACHE_DEFAULT_OPTIONS`].
    fn parse(options: &str) -> anyhow::Result<Self> {
        let mut size = None;
        let mut ttl = None;
        let mut max_roles = None;
        let mut gc_interval = None;

        for option in options.split(',') {
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("bad key-value pair: {option}"))?;

            match key {
                "size" => size = Some(value.parse()?),
                "ttl" => ttl = Some(humantime::parse_duration(value)?),
                "max_roles" => max_roles = Some(value.parse()?),
                "gc_interval" => gc_interval = Some(humantime::parse_duration(value)?),
                unknown => bail!("unknown key: {unknown}"),
            }
        }

        // TTL doesn't matter if cache is always empty.
        if let Some(0) = size {
            ttl.get_or_insert(Duration::default());
        }

        Ok(Self {
            size: size.context("missing `size`")?,
            ttl: ttl.context("missing `ttl`")?,
            max_roles: max_roles.context("missing `max_roles`")?,
            gc_interval: gc_interval.context("missing `gc_interval`")?,
        })
    }
}

impl FromStr for ProjectInfoCacheOptions {
    type Err = anyhow::Error;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let error = || format!("failed to parse cache options '{options}'");
        Self::parse(options).with_context(error)
    }
}

/// This is a config for connect to compute and wake compute.
#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    /// Number of times we should retry.
    pub max_retries: u32,
    /// Retry duration is base_delay * backoff_factor ^ n, where n starts at 0
    pub base_delay: tokio::time::Duration,
    /// Exponential base for retry wait duration
    pub backoff_factor: f64,
}

impl RetryConfig {
    // Default options for RetryConfig.

    /// Total delay for 5 retries with 200ms base delay and 2 backoff factor is about 6s.
    pub const CONNECT_TO_COMPUTE_DEFAULT_VALUES: &'static str =
        "num_retries=5,base_retry_wait_duration=200ms,retry_wait_exponent_base=2";
    /// Total delay for 8 retries with 100ms base delay and 1.6 backoff factor is about 7s.
    /// Cplane has timeout of 60s on each request. 8m7s in total.
    pub const WAKE_COMPUTE_DEFAULT_VALUES: &'static str =
        "num_retries=8,base_retry_wait_duration=100ms,retry_wait_exponent_base=1.6";

    /// Parse retry options passed via cmdline.
    /// Example: [`Self::CONNECT_TO_COMPUTE_DEFAULT_VALUES`].
    pub fn parse(options: &str) -> anyhow::Result<Self> {
        let mut num_retries = None;
        let mut base_retry_wait_duration = None;
        let mut retry_wait_exponent_base = None;

        for option in options.split(',') {
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("bad key-value pair: {option}"))?;

            match key {
                "num_retries" => num_retries = Some(value.parse()?),
                "base_retry_wait_duration" => {
                    base_retry_wait_duration = Some(humantime::parse_duration(value)?);
                }
                "retry_wait_exponent_base" => retry_wait_exponent_base = Some(value.parse()?),
                unknown => bail!("unknown key: {unknown}"),
            }
        }

        Ok(Self {
            max_retries: num_retries.context("missing `num_retries`")?,
            base_delay: base_retry_wait_duration.context("missing `base_retry_wait_duration`")?,
            backoff_factor: retry_wait_exponent_base
                .context("missing `retry_wait_exponent_base`")?,
        })
    }
}

/// Helper for cmdline cache options parsing.
#[derive(serde::Deserialize)]
pub struct ConcurrencyLockOptions {
    /// The number of shards the lock map should have
    pub shards: usize,
    /// The number of allowed concurrent requests for each endpoitn
    #[serde(flatten)]
    pub limiter: RateLimiterConfig,
    /// Garbage collection epoch
    #[serde(deserialize_with = "humantime_serde::deserialize")]
    pub epoch: Duration,
    /// Lock timeout
    #[serde(deserialize_with = "humantime_serde::deserialize")]
    pub timeout: Duration,
}

impl ConcurrencyLockOptions {
    /// Default options for [`crate::control_plane::client::ApiLocks`].
    pub const DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK: &'static str = "permits=0";
    /// Default options for [`crate::control_plane::client::ApiLocks`].
    pub const DEFAULT_OPTIONS_CONNECT_COMPUTE_LOCK: &'static str =
        "shards=64,permits=100,epoch=10m,timeout=10ms";

    // pub const DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK: &'static str = "shards=32,permits=4,epoch=10m,timeout=1s";

    /// Parse lock options passed via cmdline.
    /// Example: [`Self::DEFAULT_OPTIONS_WAKE_COMPUTE_LOCK`].
    fn parse(options: &str) -> anyhow::Result<Self> {
        let options = options.trim();
        if options.starts_with('{') && options.ends_with('}') {
            return Ok(serde_json::from_str(options)?);
        }

        let mut shards = None;
        let mut permits = None;
        let mut epoch = None;
        let mut timeout = None;

        for option in options.split(',') {
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("bad key-value pair: {option}"))?;

            match key {
                "shards" => shards = Some(value.parse()?),
                "permits" => permits = Some(value.parse()?),
                "epoch" => epoch = Some(humantime::parse_duration(value)?),
                "timeout" => timeout = Some(humantime::parse_duration(value)?),
                unknown => bail!("unknown key: {unknown}"),
            }
        }

        // these dont matter if lock is disabled
        if let Some(0) = permits {
            timeout = Some(Duration::default());
            epoch = Some(Duration::default());
            shards = Some(2);
        }

        let permits = permits.context("missing `permits`")?;
        let out = Self {
            shards: shards.context("missing `shards`")?,
            limiter: RateLimiterConfig {
                algorithm: RateLimitAlgorithm::Fixed,
                initial_limit: permits,
            },
            epoch: epoch.context("missing `epoch`")?,
            timeout: timeout.context("missing `timeout`")?,
        };

        ensure!(out.shards > 1, "shard count must be > 1");
        ensure!(
            out.shards.is_power_of_two(),
            "shard count must be a power of two"
        );

        Ok(out)
    }
}

impl FromStr for ConcurrencyLockOptions {
    type Err = anyhow::Error;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let error = || format!("failed to parse cache lock options '{options}'");
        Self::parse(options).with_context(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limiter::Aimd;

    #[test]
    fn test_parse_cache_options() -> anyhow::Result<()> {
        let CacheOptions { size, ttl } = "size=4096,ttl=5min".parse()?;
        assert_eq!(size, 4096);
        assert_eq!(ttl, Duration::from_secs(5 * 60));

        let CacheOptions { size, ttl } = "ttl=4m,size=2".parse()?;
        assert_eq!(size, 2);
        assert_eq!(ttl, Duration::from_secs(4 * 60));

        let CacheOptions { size, ttl } = "size=0,ttl=1s".parse()?;
        assert_eq!(size, 0);
        assert_eq!(ttl, Duration::from_secs(1));

        let CacheOptions { size, ttl } = "size=0".parse()?;
        assert_eq!(size, 0);
        assert_eq!(ttl, Duration::default());

        Ok(())
    }

    #[test]
    fn test_parse_lock_options() -> anyhow::Result<()> {
        let ConcurrencyLockOptions {
            epoch,
            limiter,
            shards,
            timeout,
        } = "shards=32,permits=4,epoch=10m,timeout=1s".parse()?;
        assert_eq!(epoch, Duration::from_secs(10 * 60));
        assert_eq!(timeout, Duration::from_secs(1));
        assert_eq!(shards, 32);
        assert_eq!(limiter.initial_limit, 4);
        assert_eq!(limiter.algorithm, RateLimitAlgorithm::Fixed);

        let ConcurrencyLockOptions {
            epoch,
            limiter,
            shards,
            timeout,
        } = "epoch=60s,shards=16,timeout=100ms,permits=8".parse()?;
        assert_eq!(epoch, Duration::from_secs(60));
        assert_eq!(timeout, Duration::from_millis(100));
        assert_eq!(shards, 16);
        assert_eq!(limiter.initial_limit, 8);
        assert_eq!(limiter.algorithm, RateLimitAlgorithm::Fixed);

        let ConcurrencyLockOptions {
            epoch,
            limiter,
            shards,
            timeout,
        } = "permits=0".parse()?;
        assert_eq!(epoch, Duration::ZERO);
        assert_eq!(timeout, Duration::ZERO);
        assert_eq!(shards, 2);
        assert_eq!(limiter.initial_limit, 0);
        assert_eq!(limiter.algorithm, RateLimitAlgorithm::Fixed);

        Ok(())
    }

    #[test]
    fn test_parse_json_lock_options() -> anyhow::Result<()> {
        let ConcurrencyLockOptions {
            epoch,
            limiter,
            shards,
            timeout,
        } = r#"{"shards":32,"initial_limit":44,"aimd":{"min":5,"max":500,"inc":10,"dec":0.9,"utilisation":0.8},"epoch":"10m","timeout":"1s"}"#
            .parse()?;
        assert_eq!(epoch, Duration::from_secs(10 * 60));
        assert_eq!(timeout, Duration::from_secs(1));
        assert_eq!(shards, 32);
        assert_eq!(limiter.initial_limit, 44);
        assert_eq!(
            limiter.algorithm,
            RateLimitAlgorithm::Aimd {
                conf: Aimd {
                    min: 5,
                    max: 500,
                    dec: 0.9,
                    inc: 10,
                    utilisation: 0.8
                }
            },
        );

        Ok(())
    }
}
