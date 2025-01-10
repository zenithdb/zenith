use std::net::SocketAddr;
use std::pin::pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context};
use camino::{Utf8Path, Utf8PathBuf};
use compute_api::spec::LocalProxySpec;
use dashmap::DashMap;
use futures::future::Either;
use proxy::auth::backend::jwt::JwkCache;
use proxy::auth::backend::local::{LocalBackend, JWKS_ROLE_MAP};
use proxy::auth::{self};
use proxy::cancellation::CancellationHandlerMain;
use proxy::config::{
    self, AuthenticationConfig, ComputeConfig, HttpConfig, ProxyConfig, RetryConfig,
};
use proxy::control_plane::locks::ApiLocks;
use proxy::control_plane::messages::{EndpointJwksResponse, JwksSettings};
use proxy::http::health_server::AppMetrics;
use proxy::intern::RoleNameInt;
use proxy::metrics::{Metrics, ThreadPoolMetrics};
use proxy::rate_limiter::{
    BucketRateLimiter, EndpointRateLimiter, LeakyBucketConfig, RateBucketInfo,
};
use proxy::scram::threadpool::ThreadPool;
use proxy::serverless::cancel_set::CancelSet;
use proxy::serverless::{self, GlobalConnPoolOptions};
use proxy::tls::client_config::compute_client_config_with_root_certs;
use proxy::types::RoleName;
use proxy::url::ApiUrl;

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

use clap::Parser;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use utils::sentry_init::init_sentry;
use utils::{pid_file, project_build_tag, project_git_version};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Neon proxy/router
#[derive(Parser)]
#[command(version = GIT_VERSION, about)]
struct LocalProxyCliArgs {
    /// listen for incoming metrics connections on ip:port
    #[clap(long, default_value = "127.0.0.1:7001")]
    metrics: String,
    /// listen for incoming http connections on ip:port
    #[clap(long)]
    http: String,
    /// timeout for the TLS handshake
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    handshake_timeout: tokio::time::Duration,
    /// lock for `connect_compute` api method. example: "shards=32,permits=4,epoch=10m,timeout=1s". (use `permits=0` to disable).
    #[clap(long, default_value = config::ConcurrencyLockOptions::DEFAULT_OPTIONS_CONNECT_COMPUTE_LOCK)]
    connect_compute_lock: String,
    #[clap(flatten)]
    sql_over_http: SqlOverHttpArgs,
    /// User rate limiter max number of requests per second.
    ///
    /// Provided in the form `<Requests Per Second>@<Bucket Duration Size>`.
    /// Can be given multiple times for different bucket sizes.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_ENDPOINT_SET)]
    user_rps_limit: Vec<RateBucketInfo>,
    /// Whether the auth rate limiter actually takes effect (for testing)
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    auth_rate_limit_enabled: bool,
    /// Authentication rate limiter max number of hashes per second.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_AUTH_SET)]
    auth_rate_limit: Vec<RateBucketInfo>,
    /// The IP subnet to use when considering whether two IP addresses are considered the same.
    #[clap(long, default_value_t = 64)]
    auth_rate_limit_ip_subnet: u8,
    /// Whether to retry the connection to the compute node
    #[clap(long, default_value = config::RetryConfig::CONNECT_TO_COMPUTE_DEFAULT_VALUES)]
    connect_to_compute_retry: String,
    /// Address of the postgres server
    #[clap(long, default_value = "127.0.0.1:5432")]
    postgres: SocketAddr,
    /// Address of the compute-ctl api service
    #[clap(long, default_value = "http://127.0.0.1:3080/")]
    compute_ctl: ApiUrl,
    /// Path of the local proxy config file
    #[clap(long, default_value = "./local_proxy.json")]
    config_path: Utf8PathBuf,
    /// Path of the local proxy PID file
    #[clap(long, default_value = "./local_proxy.pid")]
    pid_path: Utf8PathBuf,
}

#[derive(clap::Args, Clone, Copy, Debug)]
struct SqlOverHttpArgs {
    /// How many connections to pool for each endpoint. Excess connections are discarded
    #[clap(long, default_value_t = 200)]
    sql_over_http_pool_max_total_conns: usize,

    /// How long pooled connections should remain idle for before closing
    #[clap(long, default_value = "5m", value_parser = humantime::parse_duration)]
    sql_over_http_idle_timeout: tokio::time::Duration,

    #[clap(long, default_value_t = 100)]
    sql_over_http_client_conn_threshold: u64,

    #[clap(long, default_value_t = 16)]
    sql_over_http_cancel_set_shards: usize,

    #[clap(long, default_value_t = 10 * 1024 * 1024)] // 10 MiB
    sql_over_http_max_request_size_bytes: usize,

    #[clap(long, default_value_t = 10 * 1024 * 1024)] // 10 MiB
    sql_over_http_max_response_size_bytes: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init_local_proxy()?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    Metrics::install(Arc::new(ThreadPoolMetrics::new(0)));

    // TODO: refactor these to use labels
    debug!("Version: {GIT_VERSION}");
    debug!("Build_tag: {BUILD_TAG}");
    let neon_metrics = ::metrics::NeonMetrics::new(::metrics::BuildInfo {
        revision: GIT_VERSION,
        build_tag: BUILD_TAG,
    });

    let jemalloc = match proxy::jemalloc::MetricRecorder::new() {
        Ok(t) => Some(t),
        Err(e) => {
            tracing::error!(error = ?e, "could not start jemalloc metrics loop");
            None
        }
    };

    let args = LocalProxyCliArgs::parse();
    let config = build_config(&args)?;
    let auth_backend = build_auth_backend(&args)?;

    // before we bind to any ports, write the process ID to a file
    // so that compute-ctl can find our process later
    // in order to trigger the appropriate SIGHUP on config change.
    //
    // This also claims a "lock" that makes sure only one instance
    // of local_proxy runs at a time.
    let _process_guard = loop {
        match pid_file::claim_for_current_process(&args.pid_path) {
            Ok(guard) => break guard,
            Err(e) => {
                // compute-ctl might have tried to read the pid-file to let us
                // know about some config change. We should try again.
                error!(path=?args.pid_path, "could not claim PID file guard: {e:?}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };

    let metrics_listener = TcpListener::bind(args.metrics).await?.into_std()?;
    let http_listener = TcpListener::bind(args.http).await?;
    let shutdown = CancellationToken::new();

    // todo: should scale with CU
    let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
        LeakyBucketConfig {
            rps: 10.0,
            max: 100.0,
        },
        16,
    ));

    let mut maintenance_tasks = JoinSet::new();

    let refresh_config_notify = Arc::new(Notify::new());
    maintenance_tasks.spawn(proxy::signals::handle(shutdown.clone(), {
        let refresh_config_notify = Arc::clone(&refresh_config_notify);
        move || {
            refresh_config_notify.notify_one();
        }
    }));

    // trigger the first config load **after** setting up the signal hook
    // to avoid the race condition where:
    // 1. No config file registered when local_proxy starts up
    // 2. The config file is written but the signal hook is not yet received
    // 3. local_proxy completes startup but has no config loaded, despite there being a registerd config.
    refresh_config_notify.notify_one();
    tokio::spawn(refresh_config_loop(args.config_path, refresh_config_notify));

    maintenance_tasks.spawn(proxy::http::health_server::task_main(
        metrics_listener,
        AppMetrics {
            jemalloc,
            neon_metrics,
            proxy: proxy::metrics::Metrics::get(),
        },
    ));

    let task = serverless::task_main(
        config,
        auth_backend,
        http_listener,
        shutdown.clone(),
        Arc::new(CancellationHandlerMain::new(
            &config.connect_to_compute,
            Arc::new(DashMap::new()),
            None,
            proxy::metrics::CancellationSource::Local,
        )),
        endpoint_rate_limiter,
    );

    match futures::future::select(pin!(maintenance_tasks.join_next()), pin!(task)).await {
        // exit immediately on maintenance task completion
        Either::Left((Some(res), _)) => match proxy::error::flatten_err(res)? {},
        // exit with error immediately if all maintenance tasks have ceased (should be caught by branch above)
        Either::Left((None, _)) => bail!("no maintenance tasks running. invalid state"),
        // exit immediately on client task error
        Either::Right((res, _)) => res?,
    }

    Ok(())
}

/// ProxyConfig is created at proxy startup, and lives forever.
fn build_config(args: &LocalProxyCliArgs) -> anyhow::Result<&'static ProxyConfig> {
    let config::ConcurrencyLockOptions {
        shards,
        limiter,
        epoch,
        timeout,
    } = args.connect_compute_lock.parse()?;
    info!(
        ?limiter,
        shards,
        ?epoch,
        "Using NodeLocks (connect_compute)"
    );
    let connect_compute_locks = ApiLocks::new(
        "connect_compute_lock",
        limiter,
        shards,
        timeout,
        epoch,
        &Metrics::get().proxy.connect_compute_lock,
    )?;

    let http_config = HttpConfig {
        accept_websockets: false,
        pool_options: GlobalConnPoolOptions {
            gc_epoch: Duration::from_secs(60),
            pool_shards: 2,
            idle_timeout: args.sql_over_http.sql_over_http_idle_timeout,
            opt_in: false,

            max_conns_per_endpoint: args.sql_over_http.sql_over_http_pool_max_total_conns,
            max_total_conns: args.sql_over_http.sql_over_http_pool_max_total_conns,
        },
        cancel_set: CancelSet::new(args.sql_over_http.sql_over_http_cancel_set_shards),
        client_conn_threshold: args.sql_over_http.sql_over_http_client_conn_threshold,
        max_request_size_bytes: args.sql_over_http.sql_over_http_max_request_size_bytes,
        max_response_size_bytes: args.sql_over_http.sql_over_http_max_response_size_bytes,
    };

    let compute_config = ComputeConfig {
        retry: RetryConfig::parse(RetryConfig::CONNECT_TO_COMPUTE_DEFAULT_VALUES)?,
        tls: Arc::new(compute_client_config_with_root_certs()?),
        timeout: Duration::from_secs(2),
    };

    Ok(Box::leak(Box::new(ProxyConfig {
        tls_config: None,
        metric_collection: None,
        http_config,
        authentication_config: AuthenticationConfig {
            jwks_cache: JwkCache::default(),
            thread_pool: ThreadPool::new(0),
            scram_protocol_timeout: Duration::from_secs(10),
            rate_limiter_enabled: false,
            rate_limiter: BucketRateLimiter::new(vec![]),
            rate_limit_ip_subnet: 64,
            ip_allowlist_check_enabled: true,
            is_auth_broker: false,
            accept_jwts: true,
            console_redirect_confirmation_timeout: Duration::ZERO,
        },
        proxy_protocol_v2: config::ProxyProtocolV2::Rejected,
        handshake_timeout: Duration::from_secs(10),
        region: "local".into(),
        wake_compute_retry_config: RetryConfig::parse(RetryConfig::WAKE_COMPUTE_DEFAULT_VALUES)?,
        connect_compute_locks,
        connect_to_compute: compute_config,
    })))
}

/// auth::Backend is created at proxy startup, and lives forever.
fn build_auth_backend(
    args: &LocalProxyCliArgs,
) -> anyhow::Result<&'static auth::Backend<'static, ()>> {
    let auth_backend = proxy::auth::Backend::Local(proxy::auth::backend::MaybeOwned::Owned(
        LocalBackend::new(args.postgres, args.compute_ctl.clone()),
    ));

    Ok(Box::leak(Box::new(auth_backend)))
}

#[derive(Error, Debug)]
enum RefreshConfigError {
    #[error(transparent)]
    Read(#[from] std::io::Error),
    #[error(transparent)]
    Parse(#[from] serde_json::Error),
    #[error(transparent)]
    Validate(anyhow::Error),
}

async fn refresh_config_loop(path: Utf8PathBuf, rx: Arc<Notify>) {
    let mut init = true;
    loop {
        rx.notified().await;

        match refresh_config_inner(&path).await {
            Ok(()) => {}
            // don't log for file not found errors if this is the first time we are checking
            // for computes that don't use local_proxy, this is not an error.
            Err(RefreshConfigError::Read(e))
                if init && e.kind() == std::io::ErrorKind::NotFound =>
            {
                debug!(error=?e, ?path, "could not read config file");
            }
            Err(e) => {
                error!(error=?e, ?path, "could not read config file");
            }
        }

        init = false;
    }
}

async fn refresh_config_inner(path: &Utf8Path) -> Result<(), RefreshConfigError> {
    let bytes = tokio::fs::read(&path).await?;
    let data: LocalProxySpec = serde_json::from_slice(&bytes)?;

    let mut jwks_set = vec![];

    fn parse_jwks_settings(jwks: compute_api::spec::JwksSettings) -> anyhow::Result<JwksSettings> {
        let mut jwks_url = url::Url::from_str(&jwks.jwks_url).context("parsing JWKS url")?;

        ensure!(
            jwks_url.has_authority()
                && (jwks_url.scheme() == "http" || jwks_url.scheme() == "https"),
            "Invalid JWKS url. Must be HTTP",
        );

        ensure!(
            jwks_url.host().is_some_and(|h| h != url::Host::Domain("")),
            "Invalid JWKS url. No domain listed",
        );

        // clear username, password and ports
        jwks_url
            .set_username("")
            .expect("url can be a base and has a valid host and is not a file. should not error");
        jwks_url
            .set_password(None)
            .expect("url can be a base and has a valid host and is not a file. should not error");
        // local testing is hard if we need to have a specific restricted port
        if cfg!(not(feature = "testing")) {
            jwks_url.set_port(None).expect(
                "url can be a base and has a valid host and is not a file. should not error",
            );
        }

        // clear query params
        jwks_url.set_fragment(None);
        jwks_url.query_pairs_mut().clear().finish();

        if jwks_url.scheme() != "https" {
            // local testing is hard if we need to set up https support.
            if cfg!(not(feature = "testing")) {
                jwks_url
                    .set_scheme("https")
                    .expect("should not error to set the scheme to https if it was http");
            } else {
                warn!(scheme = jwks_url.scheme(), "JWKS url is not HTTPS");
            }
        }

        Ok(JwksSettings {
            id: jwks.id,
            jwks_url,
            provider_name: jwks.provider_name,
            jwt_audience: jwks.jwt_audience,
            role_names: jwks
                .role_names
                .into_iter()
                .map(RoleName::from)
                .map(|s| RoleNameInt::from(&s))
                .collect(),
        })
    }

    for jwks in data.jwks.into_iter().flatten() {
        jwks_set.push(parse_jwks_settings(jwks).map_err(RefreshConfigError::Validate)?);
    }

    info!("successfully loaded new config");
    JWKS_ROLE_MAP.store(Some(Arc::new(EndpointJwksResponse { jwks: jwks_set })));

    Ok(())
}
