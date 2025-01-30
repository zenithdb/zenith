use anyhow::{anyhow, Context};
use clap::Parser;
use hyper0::Uri;
use metrics::launch_timestamp::LaunchTimestamp;
use metrics::BuildInfo;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use storage_controller::http::make_router;
use storage_controller::metrics::preinitialize_metrics;
use storage_controller::persistence::Persistence;
use storage_controller::service::chaos_injector::ChaosInjector;
use storage_controller::service::safekeeper_reconciler::SafekeeperReconciler;
use storage_controller::service::{
    Config, Service, HEARTBEAT_INTERVAL_DEFAULT, LONG_RECONCILE_THRESHOLD_DEFAULT,
    MAX_OFFLINE_INTERVAL_DEFAULT, MAX_WARMING_UP_INTERVAL_DEFAULT, RECONCILER_CONCURRENCY_DEFAULT,
};
use tokio::signal::unix::SignalKind;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::auth::{JwtAuth, SwappableJwtAuth};
use utils::logging::{self, LogFormat};

use utils::sentry_init::init_sentry;
use utils::{project_build_tag, project_git_version, tcp_listener};

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    /// Host and port to listen on, like `127.0.0.1:1234`
    #[arg(short, long)]
    listen: std::net::SocketAddr,

    /// Public key for JWT authentication of clients
    #[arg(long)]
    public_key: Option<String>,

    /// Token for authenticating this service with the pageservers it controls
    #[arg(long)]
    jwt_token: Option<String>,

    /// Token for authenticating this service with the control plane, when calling
    /// the compute notification endpoint
    #[arg(long)]
    control_plane_jwt_token: Option<String>,

    #[arg(long)]
    peer_jwt_token: Option<String>,

    /// URL to control plane compute notification endpoint
    #[arg(long)]
    compute_hook_url: Option<String>,

    /// URL to connect to postgres, like postgresql://localhost:1234/storage_controller
    #[arg(long)]
    database_url: Option<String>,

    /// Flag to enable dev mode, which permits running without auth
    #[arg(long, default_value = "false")]
    dev: bool,

    /// Grace period before marking unresponsive pageserver offline
    #[arg(long)]
    max_offline_interval: Option<humantime::Duration>,

    /// More tolerant grace period before marking unresponsive pagserver offline used
    /// around pageserver restarts
    #[arg(long)]
    max_warming_up_interval: Option<humantime::Duration>,

    /// Size threshold for automatically splitting shards (disabled by default)
    #[arg(long)]
    split_threshold: Option<u64>,

    /// Maximum number of reconcilers that may run in parallel
    #[arg(long)]
    reconciler_concurrency: Option<usize>,

    /// How long to wait for the initial database connection to be available.
    #[arg(long, default_value = "5s")]
    db_connect_timeout: humantime::Duration,

    #[arg(long, default_value = "false")]
    start_as_candidate: bool,

    // TODO: make this mandatory once the helm chart gets updated
    #[arg(long)]
    address_for_peers: Option<Uri>,

    /// `neon_local` sets this to the path of the neon_local repo dir.
    /// Only relevant for testing.
    // TODO: make `cfg(feature = "testing")`
    #[arg(long)]
    neon_local_repo_dir: Option<PathBuf>,

    /// Chaos testing
    #[arg(long)]
    chaos_interval: Option<humantime::Duration>,

    // Maximum acceptable lag for the secondary location while draining
    // a pageserver
    #[arg(long)]
    max_secondary_lag_bytes: Option<u64>,

    // Period with which to send heartbeats to registered nodes
    #[arg(long)]
    heartbeat_interval: Option<humantime::Duration>,

    #[arg(long)]
    long_reconcile_threshold: Option<humantime::Duration>,
}

enum StrictMode {
    /// In strict mode, we will require that all secrets are loaded, i.e. security features
    /// may not be implicitly turned off by omitting secrets in the environment.
    Strict,
    /// In dev mode, secrets are optional, and omitting a particular secret will implicitly
    /// disable the auth related to it (e.g. no pageserver jwt key -> send unauthenticated
    /// requests, no public key -> don't authenticate incoming requests).
    Dev,
}

impl Default for StrictMode {
    fn default() -> Self {
        Self::Strict
    }
}

/// Secrets may either be provided on the command line (for testing), or loaded from AWS SecretManager: this
/// type encapsulates the logic to decide which and do the loading.
struct Secrets {
    database_url: String,
    public_key: Option<JwtAuth>,
    jwt_token: Option<String>,
    control_plane_jwt_token: Option<String>,
    peer_jwt_token: Option<String>,
}

impl Secrets {
    const DATABASE_URL_ENV: &'static str = "DATABASE_URL";
    const PAGESERVER_JWT_TOKEN_ENV: &'static str = "PAGESERVER_JWT_TOKEN";
    const CONTROL_PLANE_JWT_TOKEN_ENV: &'static str = "CONTROL_PLANE_JWT_TOKEN";
    const PEER_JWT_TOKEN_ENV: &'static str = "PEER_JWT_TOKEN";
    const PUBLIC_KEY_ENV: &'static str = "PUBLIC_KEY";

    /// Load secrets from, in order of preference:
    /// - CLI args if database URL is provided on the CLI
    /// - Environment variables if DATABASE_URL is set.
    async fn load(args: &Cli) -> anyhow::Result<Self> {
        let Some(database_url) = Self::load_secret(&args.database_url, Self::DATABASE_URL_ENV)
        else {
            anyhow::bail!(
                "Database URL is not set (set `--database-url`, or `DATABASE_URL` environment)"
            )
        };

        let public_key = match Self::load_secret(&args.public_key, Self::PUBLIC_KEY_ENV) {
            Some(v) => Some(JwtAuth::from_key(v).context("Loading public key")?),
            None => None,
        };

        let this = Self {
            database_url,
            public_key,
            jwt_token: Self::load_secret(&args.jwt_token, Self::PAGESERVER_JWT_TOKEN_ENV),
            control_plane_jwt_token: Self::load_secret(
                &args.control_plane_jwt_token,
                Self::CONTROL_PLANE_JWT_TOKEN_ENV,
            ),
            peer_jwt_token: Self::load_secret(&args.peer_jwt_token, Self::PEER_JWT_TOKEN_ENV),
        };

        Ok(this)
    }

    fn load_secret(cli: &Option<String>, env_name: &str) -> Option<String> {
        if let Some(v) = cli {
            Some(v.clone())
        } else if let Ok(v) = std::env::var(env_name) {
            Some(v)
        } else {
            None
        }
    }
}

fn main() -> anyhow::Result<()> {
    logging::init(
        LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stdout,
    )?;

    // log using tracing so we don't get confused output by default hook writing to stderr
    utils::logging::replace_panic_hook_with_tracing_panic_hook().forget();

    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        // let sentry send a message (and flush)
        // and trace the error
        hook(info);

        std::process::exit(1);
    }));

    tokio::runtime::Builder::new_current_thread()
        // We use spawn_blocking for database operations, so require approximately
        // as many blocking threads as we will open database connections.
        .max_blocking_threads(Persistence::MAX_CONNECTIONS as usize)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    let launch_ts = Box::leak(Box::new(LaunchTimestamp::generate()));

    preinitialize_metrics();

    let args = Cli::parse();
    tracing::info!(
        "version: {}, launch_timestamp: {}, build_tag {}, listening on {}",
        GIT_VERSION,
        launch_ts.to_string(),
        BUILD_TAG,
        args.listen
    );

    let build_info = BuildInfo {
        revision: GIT_VERSION,
        build_tag: BUILD_TAG,
    };

    let strict_mode = if args.dev {
        StrictMode::Dev
    } else {
        StrictMode::Strict
    };

    let secrets = Secrets::load(&args).await?;

    // Validate required secrets and arguments are provided in strict mode
    match strict_mode {
        StrictMode::Strict
            if (secrets.public_key.is_none()
                || secrets.jwt_token.is_none()
                || secrets.control_plane_jwt_token.is_none()) =>
        {
            // Production systems should always have secrets configured: if public_key was not set
            // then we would implicitly disable auth.
            anyhow::bail!(
                    "Insecure config!  One or more secrets is not set.  This is only permitted in `--dev` mode"
                );
        }
        StrictMode::Strict if args.compute_hook_url.is_none() => {
            // Production systems should always have a compute hook set, to prevent falling
            // back to trying to use neon_local.
            anyhow::bail!(
                "`--compute-hook-url` is not set: this is only permitted in `--dev` mode"
            );
        }
        StrictMode::Strict => {
            tracing::info!("Starting in strict mode: configuration is OK.")
        }
        StrictMode::Dev => {
            tracing::warn!("Starting in dev mode: this may be an insecure configuration.")
        }
    }

    let config = Config {
        jwt_token: secrets.jwt_token,
        control_plane_jwt_token: secrets.control_plane_jwt_token,
        peer_jwt_token: secrets.peer_jwt_token,
        compute_hook_url: args.compute_hook_url,
        max_offline_interval: args
            .max_offline_interval
            .map(humantime::Duration::into)
            .unwrap_or(MAX_OFFLINE_INTERVAL_DEFAULT),
        max_warming_up_interval: args
            .max_warming_up_interval
            .map(humantime::Duration::into)
            .unwrap_or(MAX_WARMING_UP_INTERVAL_DEFAULT),
        reconciler_concurrency: args
            .reconciler_concurrency
            .unwrap_or(RECONCILER_CONCURRENCY_DEFAULT),
        split_threshold: args.split_threshold,
        neon_local_repo_dir: args.neon_local_repo_dir,
        max_secondary_lag_bytes: args.max_secondary_lag_bytes,
        heartbeat_interval: args
            .heartbeat_interval
            .map(humantime::Duration::into)
            .unwrap_or(HEARTBEAT_INTERVAL_DEFAULT),
        long_reconcile_threshold: args
            .long_reconcile_threshold
            .map(humantime::Duration::into)
            .unwrap_or(LONG_RECONCILE_THRESHOLD_DEFAULT),
        address_for_peers: args.address_for_peers,
        start_as_candidate: args.start_as_candidate,
        http_service_port: args.listen.port() as i32,
    };

    // Validate that we can connect to the database
    Persistence::await_connection(&secrets.database_url, args.db_connect_timeout.into()).await?;

    let persistence = Arc::new(Persistence::new(secrets.database_url).await);

    let service = Service::spawn(config, persistence.clone()).await?;

    let http_listener = tcp_listener::bind(args.listen)?;

    let auth = secrets
        .public_key
        .map(|jwt_auth| Arc::new(SwappableJwtAuth::new(jwt_auth)));
    let router = make_router(service.clone(), auth, build_info)
        .build()
        .map_err(|err| anyhow!(err))?;
    let router_service = utils::http::RouterService::new(router).unwrap();

    // Start HTTP server
    let server_shutdown = CancellationToken::new();
    let server = hyper0::Server::from_tcp(http_listener)?
        .serve(router_service)
        .with_graceful_shutdown({
            let server_shutdown = server_shutdown.clone();
            async move {
                server_shutdown.cancelled().await;
            }
        });
    tracing::info!("Serving on {0}", args.listen);
    let server_task = tokio::task::spawn(server);

    let chaos_task = args.chaos_interval.map(|interval| {
        let service = service.clone();
        let cancel = CancellationToken::new();
        let cancel_bg = cancel.clone();
        (
            tokio::task::spawn(
                async move {
                    let mut chaos_injector = ChaosInjector::new(service, interval.into());
                    chaos_injector.run(cancel_bg).await
                }
                .instrument(tracing::info_span!("chaos_injector")),
            ),
            cancel,
        )
    });

    const SAFEKEEPER_RECONCILER_INTERVAL: Duration = Duration::from_secs(120);
    let safekeeper_reconciler_task = {
        let service = service.clone();
        let cancel = CancellationToken::new();
        let cancel_bg = cancel.clone();
        (
            tokio::task::spawn(
                async move {
                    let reconciler =
                        SafekeeperReconciler::new(service, SAFEKEEPER_RECONCILER_INTERVAL);
                    reconciler.run(cancel_bg).await
                }
                .instrument(tracing::info_span!("safekeeper_reconciler")),
            ),
            cancel,
        )
    };

    // Wait until we receive a signal
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt())?;
    let mut sigquit = tokio::signal::unix::signal(SignalKind::quit())?;
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
    tokio::select! {
        _ = sigint.recv() => {},
        _ = sigterm.recv() => {},
        _ = sigquit.recv() => {},
    }
    tracing::info!("Terminating on signal");

    // Stop HTTP server first, so that we don't have to service requests
    // while shutting down Service.
    server_shutdown.cancel();
    match tokio::time::timeout(Duration::from_secs(5), server_task).await {
        Ok(Ok(_)) => {
            tracing::info!("Joined HTTP server task");
        }
        Ok(Err(e)) => {
            tracing::error!("Error joining HTTP server task: {e}")
        }
        Err(_) => {
            tracing::warn!("Timed out joining HTTP server task");
            // We will fall through and shut down the service anyway, any request handlers
            // in flight will experience cancellation & their clients will see a torn connection.
        }
    }

    // If we were injecting chaos, stop that so that we're not calling into Service while it shuts down
    if let Some((chaos_jh, chaos_cancel)) = chaos_task {
        chaos_cancel.cancel();
        chaos_jh.await.ok();
    }
    // Do the same for the safekeeper reconciler
    {
        safekeeper_reconciler_task.1.cancel();
        _ = safekeeper_reconciler_task.0.await;
    }

    service.shutdown().await;
    tracing::info!("Service shutdown complete");

    std::process::exit(0);
}
