//
// Main entry point for the safekeeper executable
//
use anyhow::{bail, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{ArgAction, Parser};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use remote_storage::RemoteStorageConfig;
use sd_notify::NotifyState;
use tokio::runtime::Handle;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinError;
use utils::logging::SecretString;

use std::env::{var, VarError};
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage_broker::Uri;

use tracing::*;
use utils::pid_file;

use metrics::set_build_info_metric;
use safekeeper::defaults::{
    DEFAULT_CONTROL_FILE_SAVE_INTERVAL, DEFAULT_EVICTION_MIN_RESIDENT, DEFAULT_HEARTBEAT_TIMEOUT,
    DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_MAX_OFFLOADER_LAG_BYTES, DEFAULT_PARTIAL_BACKUP_CONCURRENCY,
    DEFAULT_PARTIAL_BACKUP_TIMEOUT, DEFAULT_PG_LISTEN_ADDR,
};
use safekeeper::http;
use safekeeper::wal_service;
use safekeeper::GlobalTimelines;
use safekeeper::SafeKeeperConf;
use safekeeper::{broker, WAL_SERVICE_RUNTIME};
use safekeeper::{control_file, BROKER_RUNTIME};
use safekeeper::{wal_backup, HTTP_RUNTIME};
use storage_broker::DEFAULT_ENDPOINT;
use utils::auth::{JwtAuth, Scope, SwappableJwtAuth};
use utils::{
    id::NodeId,
    logging::{self, LogFormat},
    project_build_tag, project_git_version,
    sentry_init::init_sentry,
    tcp_listener,
};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Configure jemalloc to sample allocations for profiles every 1 MB (1 << 20).
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:20\0";

const PID_FILE_NAME: &str = "safekeeper.pid";
const ID_FILE_NAME: &str = "safekeeper.id";

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

const FEATURES: &[&str] = &[
    #[cfg(feature = "testing")]
    "testing",
];

fn version() -> String {
    format!(
        "{GIT_VERSION} failpoints: {}, features: {:?}",
        fail::has_failpoints(),
        FEATURES,
    )
}

const ABOUT: &str = r#"
A fleet of safekeepers is responsible for reliably storing WAL received from
compute, passing it through consensus (mitigating potential computes brain
split), and serving the hardened part further downstream to pageserver(s).
"#;

#[derive(Parser)]
#[command(name = "Neon safekeeper", version = GIT_VERSION, about = ABOUT, long_about = None)]
struct Args {
    /// Path to the safekeeper data directory.
    #[arg(short = 'D', long, default_value = "./")]
    datadir: Utf8PathBuf,
    /// Safekeeper node id.
    #[arg(long)]
    id: Option<u64>,
    /// Initialize safekeeper with given id and exit.
    #[arg(long)]
    init: bool,
    /// Listen endpoint for receiving/sending WAL in the form host:port.
    #[arg(short, long, default_value = DEFAULT_PG_LISTEN_ADDR)]
    listen_pg: String,
    /// Listen endpoint for receiving/sending WAL in the form host:port allowing
    /// only tenant scoped auth tokens. Pointless if auth is disabled.
    #[arg(long, default_value = None, verbatim_doc_comment)]
    listen_pg_tenant_only: Option<String>,
    /// Listen http endpoint for management and metrics in the form host:port.
    #[arg(long, default_value = DEFAULT_HTTP_LISTEN_ADDR)]
    listen_http: String,
    /// Advertised endpoint for receiving/sending WAL in the form host:port. If not
    /// specified, listen_pg is used to advertise instead.
    #[arg(long, default_value = None)]
    advertise_pg: Option<String>,
    /// Availability zone of the safekeeper.
    #[arg(long)]
    availability_zone: Option<String>,
    /// Do not wait for changes to be written safely to disk. Unsafe.
    #[arg(short, long)]
    no_sync: bool,
    /// Dump control file at path specified by this argument and exit.
    #[arg(long)]
    dump_control_file: Option<Utf8PathBuf>,
    /// Broker endpoint for storage nodes coordination in the form
    /// http[s]://host:port. In case of https schema TLS is connection is
    /// established; plaintext otherwise.
    #[arg(long, default_value = DEFAULT_ENDPOINT, verbatim_doc_comment)]
    broker_endpoint: Uri,
    /// Broker keepalive interval.
    #[arg(long, value_parser= humantime::parse_duration, default_value = storage_broker::DEFAULT_KEEPALIVE_INTERVAL)]
    broker_keepalive_interval: Duration,
    /// Peer safekeeper is considered dead after not receiving heartbeats from
    /// it during this period passed as a human readable duration.
    #[arg(long, value_parser= humantime::parse_duration, default_value = DEFAULT_HEARTBEAT_TIMEOUT, verbatim_doc_comment)]
    heartbeat_timeout: Duration,
    /// Enable/disable peer recovery.
    #[arg(long, default_value = "false", action=ArgAction::Set)]
    peer_recovery: bool,
    /// Remote storage configuration for WAL backup (offloading to s3) as TOML
    /// inline table, e.g.
    ///   {max_concurrent_syncs = 17, max_sync_errors = 13, bucket_name = "<BUCKETNAME>", bucket_region = "<REGION>", concurrency_limit = 119}
    /// Safekeeper offloads WAL to
    ///   [prefix_in_bucket/]<tenant_id>/<timeline_id>/<segment_file>, mirroring
    /// structure on the file system.
    #[arg(long, value_parser = parse_remote_storage, verbatim_doc_comment)]
    remote_storage: Option<RemoteStorageConfig>,
    /// Safekeeper won't be elected for WAL offloading if it is lagging for more than this value in bytes
    #[arg(long, default_value_t = DEFAULT_MAX_OFFLOADER_LAG_BYTES)]
    max_offloader_lag: u64,
    /// Number of max parallel WAL segments to be offloaded to remote storage.
    #[arg(long, default_value = "5")]
    wal_backup_parallel_jobs: usize,
    /// Disable WAL backup to s3. When disabled, safekeeper removes WAL ignoring
    /// WAL backup horizon.
    #[arg(long)]
    disable_wal_backup: bool,
    /// If given, enables auth on incoming connections to WAL service endpoint
    /// (--listen-pg). Value specifies path to a .pem public key used for
    /// validations of JWT tokens. Empty string is allowed and means disabling
    /// auth.
    #[arg(long, verbatim_doc_comment, value_parser = opt_pathbuf_parser)]
    pg_auth_public_key_path: Option<Utf8PathBuf>,
    /// If given, enables auth on incoming connections to tenant only WAL
    /// service endpoint (--listen-pg-tenant-only). Value specifies path to a
    /// .pem public key used for validations of JWT tokens. Empty string is
    /// allowed and means disabling auth.
    #[arg(long, verbatim_doc_comment, value_parser = opt_pathbuf_parser)]
    pg_tenant_only_auth_public_key_path: Option<Utf8PathBuf>,
    /// If given, enables auth on incoming connections to http management
    /// service endpoint (--listen-http). Value specifies path to a .pem public
    /// key used for validations of JWT tokens. Empty string is allowed and
    /// means disabling auth.
    #[arg(long, verbatim_doc_comment, value_parser = opt_pathbuf_parser)]
    http_auth_public_key_path: Option<Utf8PathBuf>,
    /// Format for logging, either 'plain' or 'json'.
    #[arg(long, default_value = "plain")]
    log_format: String,
    /// Run everything in single threaded current thread runtime, might be
    /// useful for debugging.
    #[arg(long)]
    current_thread_runtime: bool,
    /// Keep horizon for walsenders, i.e. don't remove WAL segments that are
    /// still needed for existing replication connection.
    #[arg(long)]
    walsenders_keep_horizon: bool,
    /// Controls how long backup will wait until uploading the partial segment.
    #[arg(long, value_parser = humantime::parse_duration, default_value = DEFAULT_PARTIAL_BACKUP_TIMEOUT, verbatim_doc_comment)]
    partial_backup_timeout: Duration,
    /// Disable task to push messages to broker every second. Supposed to
    /// be used in tests.
    #[arg(long)]
    disable_periodic_broker_push: bool,
    /// Enable automatic switching to offloaded state.
    #[arg(long)]
    enable_offload: bool,
    /// Delete local WAL files after offloading. When disabled, they will be left on disk.
    #[arg(long)]
    delete_offloaded_wal: bool,
    /// Pending updates to control file will be automatically saved after this interval.
    #[arg(long, value_parser = humantime::parse_duration, default_value = DEFAULT_CONTROL_FILE_SAVE_INTERVAL)]
    control_file_save_interval: Duration,
    /// Number of allowed concurrent uploads of partial segments to remote storage.
    #[arg(long, default_value = DEFAULT_PARTIAL_BACKUP_CONCURRENCY)]
    partial_backup_concurrency: usize,
    /// How long a timeline must be resident before it is eligible for eviction.
    /// Usually, timeline eviction has to wait for `partial_backup_timeout` before being eligible for eviction,
    /// but if a timeline is un-evicted and then _not_ written to, it would immediately flap to evicting again,
    /// if it weren't for `eviction_min_resident` preventing that.
    ///
    /// Also defines interval for eviction retries.
    #[arg(long, value_parser = humantime::parse_duration, default_value = DEFAULT_EVICTION_MIN_RESIDENT)]
    eviction_min_resident: Duration,
}

// Like PathBufValueParser, but allows empty string.
fn opt_pathbuf_parser(s: &str) -> Result<Utf8PathBuf, String> {
    Ok(Utf8PathBuf::from_str(s).unwrap())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // We want to allow multiple occurences of the same arg (taking the last) so
    // that neon_local could generate command with defaults + overrides without
    // getting 'argument cannot be used multiple times' error. This seems to be
    // impossible with pure Derive API, so convert struct to Command, modify it,
    // parse arguments, and then fill the struct back.
    let cmd = <Args as clap::CommandFactory>::command()
        .args_override_self(true)
        .version(version());
    let mut matches = cmd.get_matches();
    let mut args = <Args as clap::FromArgMatches>::from_arg_matches_mut(&mut matches)?;

    // I failed to modify opt_pathbuf_parser to return Option<PathBuf> in
    // reasonable time, so turn empty string into option post factum.
    if let Some(pb) = &args.pg_auth_public_key_path {
        if pb.as_os_str().is_empty() {
            args.pg_auth_public_key_path = None;
        }
    }
    if let Some(pb) = &args.pg_tenant_only_auth_public_key_path {
        if pb.as_os_str().is_empty() {
            args.pg_tenant_only_auth_public_key_path = None;
        }
    }
    if let Some(pb) = &args.http_auth_public_key_path {
        if pb.as_os_str().is_empty() {
            args.http_auth_public_key_path = None;
        }
    }

    if let Some(addr) = args.dump_control_file {
        let state = control_file::FileStorage::load_control_file(addr)?;
        let json = serde_json::to_string(&state)?;
        print!("{json}");
        return Ok(());
    }

    // important to keep the order of:
    // 1. init logging
    // 2. tracing panic hook
    // 3. sentry
    logging::init(
        LogFormat::from_config(&args.log_format)?,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stdout,
    )?;
    logging::replace_panic_hook_with_tracing_panic_hook().forget();
    info!("version: {GIT_VERSION}");
    info!("buld_tag: {BUILD_TAG}");

    let args_workdir = &args.datadir;
    let workdir = args_workdir.canonicalize_utf8().with_context(|| {
        format!("Failed to get the absolute path for input workdir {args_workdir:?}")
    })?;

    // Change into the data directory.
    std::env::set_current_dir(&workdir)?;

    // Prevent running multiple safekeepers on the same directory
    let lock_file_path = workdir.join(PID_FILE_NAME);
    let lock_file =
        pid_file::claim_for_current_process(&lock_file_path).context("claim pid file")?;
    info!("claimed pid file at {lock_file_path:?}");
    // ensure that the lock file is held even if the main thread of the process is panics
    // we need to release the lock file only when the current process is gone
    std::mem::forget(lock_file);

    // Set or read our ID.
    let id = set_id(&workdir, args.id.map(NodeId))?;
    if args.init {
        return Ok(());
    }

    let pg_auth = match args.pg_auth_public_key_path.as_ref() {
        None => {
            info!("pg auth is disabled");
            None
        }
        Some(path) => {
            info!("loading pg auth JWT key from {path}");
            Some(Arc::new(
                JwtAuth::from_key_path(path).context("failed to load the auth key")?,
            ))
        }
    };
    let pg_tenant_only_auth = match args.pg_tenant_only_auth_public_key_path.as_ref() {
        None => {
            info!("pg tenant only auth is disabled");
            None
        }
        Some(path) => {
            info!("loading pg tenant only auth JWT key from {path}");
            Some(Arc::new(
                JwtAuth::from_key_path(path).context("failed to load the auth key")?,
            ))
        }
    };
    let http_auth = match args.http_auth_public_key_path.as_ref() {
        None => {
            info!("http auth is disabled");
            None
        }
        Some(path) => {
            info!("loading http auth JWT key(s) from {path}");
            let jwt_auth = JwtAuth::from_key_path(path).context("failed to load the auth key")?;
            Some(Arc::new(SwappableJwtAuth::new(jwt_auth)))
        }
    };

    // Load JWT auth token to connect to other safekeepers for pull_timeline.
    let sk_auth_token = match var("SAFEKEEPER_AUTH_TOKEN") {
        Ok(v) => {
            info!("loaded JWT token for authentication with safekeepers");
            Some(SecretString::from(v))
        }
        Err(VarError::NotPresent) => {
            info!("no JWT token for authentication with safekeepers detected");
            None
        }
        Err(_) => {
            warn!("JWT token for authentication with safekeepers is not unicode");
            None
        }
    };

    let conf = Arc::new(SafeKeeperConf {
        workdir,
        my_id: id,
        listen_pg_addr: args.listen_pg,
        listen_pg_addr_tenant_only: args.listen_pg_tenant_only,
        listen_http_addr: args.listen_http,
        advertise_pg_addr: args.advertise_pg,
        availability_zone: args.availability_zone,
        no_sync: args.no_sync,
        broker_endpoint: args.broker_endpoint,
        broker_keepalive_interval: args.broker_keepalive_interval,
        heartbeat_timeout: args.heartbeat_timeout,
        peer_recovery_enabled: args.peer_recovery,
        remote_storage: args.remote_storage,
        max_offloader_lag_bytes: args.max_offloader_lag,
        wal_backup_enabled: !args.disable_wal_backup,
        backup_parallel_jobs: args.wal_backup_parallel_jobs,
        pg_auth,
        pg_tenant_only_auth,
        http_auth,
        sk_auth_token,
        current_thread_runtime: args.current_thread_runtime,
        walsenders_keep_horizon: args.walsenders_keep_horizon,
        partial_backup_timeout: args.partial_backup_timeout,
        disable_periodic_broker_push: args.disable_periodic_broker_push,
        enable_offload: args.enable_offload,
        delete_offloaded_wal: args.delete_offloaded_wal,
        control_file_save_interval: args.control_file_save_interval,
        partial_backup_concurrency: args.partial_backup_concurrency,
        eviction_min_resident: args.eviction_min_resident,
    });

    // initialize sentry if SENTRY_DSN is provided
    let _sentry_guard = init_sentry(
        Some(GIT_VERSION.into()),
        &[("node_id", &conf.my_id.to_string())],
    );
    start_safekeeper(conf).await
}

/// Result of joining any of main tasks: upper error means task failed to
/// complete, e.g. panicked, inner is error produced by task itself.
type JoinTaskRes = Result<anyhow::Result<()>, JoinError>;

async fn start_safekeeper(conf: Arc<SafeKeeperConf>) -> Result<()> {
    // fsync the datadir to make sure we have a consistent state on disk.
    if !conf.no_sync {
        let dfd = File::open(&conf.workdir).context("open datadir for syncfs")?;
        let started = Instant::now();
        utils::crashsafe::syncfs(dfd)?;
        let elapsed = started.elapsed();
        info!(
            elapsed_ms = elapsed.as_millis(),
            "syncfs data directory done"
        );
    }

    info!("starting safekeeper WAL service on {}", conf.listen_pg_addr);
    let pg_listener = tcp_listener::bind(conf.listen_pg_addr.clone()).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_pg_addr, e);
        e
    })?;

    let pg_listener_tenant_only =
        if let Some(listen_pg_addr_tenant_only) = &conf.listen_pg_addr_tenant_only {
            info!(
                "starting safekeeper tenant scoped WAL service on {}",
                listen_pg_addr_tenant_only
            );
            let listener = tcp_listener::bind(listen_pg_addr_tenant_only.clone()).map_err(|e| {
                error!(
                    "failed to bind to address {}: {}",
                    listen_pg_addr_tenant_only, e
                );
                e
            })?;
            Some(listener)
        } else {
            None
        };

    info!(
        "starting safekeeper HTTP service on {}",
        conf.listen_http_addr
    );
    let http_listener = tcp_listener::bind(conf.listen_http_addr.clone()).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_http_addr, e);
        e
    })?;

    let global_timelines = Arc::new(GlobalTimelines::new(conf.clone()));

    // Register metrics collector for active timelines. It's important to do this
    // after daemonizing, otherwise process collector will be upset.
    let timeline_collector = safekeeper::metrics::TimelineCollector::new(global_timelines.clone());
    metrics::register_internal(Box::new(timeline_collector))?;

    wal_backup::init_remote_storage(&conf).await;

    // Keep handles to main tasks to die if any of them disappears.
    let mut tasks_handles: FuturesUnordered<BoxFuture<(String, JoinTaskRes)>> =
        FuturesUnordered::new();

    // Start wal backup launcher before loading timelines as we'll notify it
    // through the channel about timelines which need offloading, not draining
    // the channel would cause deadlock.
    let current_thread_rt = conf
        .current_thread_runtime
        .then(|| Handle::try_current().expect("no runtime in main"));

    // Load all timelines from disk to memory.
    global_timelines.init().await?;

    // Run everything in current thread rt, if asked.
    if conf.current_thread_runtime {
        info!("running in current thread runtime");
    }

    let wal_service_handle = current_thread_rt
        .as_ref()
        .unwrap_or_else(|| WAL_SERVICE_RUNTIME.handle())
        .spawn(wal_service::task_main(
            conf.clone(),
            pg_listener,
            Scope::SafekeeperData,
            global_timelines.clone(),
        ))
        // wrap with task name for error reporting
        .map(|res| ("WAL service main".to_owned(), res));
    tasks_handles.push(Box::pin(wal_service_handle));

    let global_timelines_ = global_timelines.clone();
    let timeline_housekeeping_handle = current_thread_rt
        .as_ref()
        .unwrap_or_else(|| WAL_SERVICE_RUNTIME.handle())
        .spawn(async move {
            const TOMBSTONE_TTL: Duration = Duration::from_secs(3600 * 24);
            loop {
                tokio::time::sleep(TOMBSTONE_TTL).await;
                global_timelines_.housekeeping(&TOMBSTONE_TTL);
            }
        })
        .map(|res| ("Timeline map housekeeping".to_owned(), res));
    tasks_handles.push(Box::pin(timeline_housekeeping_handle));

    if let Some(pg_listener_tenant_only) = pg_listener_tenant_only {
        let wal_service_handle = current_thread_rt
            .as_ref()
            .unwrap_or_else(|| WAL_SERVICE_RUNTIME.handle())
            .spawn(wal_service::task_main(
                conf.clone(),
                pg_listener_tenant_only,
                Scope::Tenant,
                global_timelines.clone(),
            ))
            // wrap with task name for error reporting
            .map(|res| ("WAL service tenant only main".to_owned(), res));
        tasks_handles.push(Box::pin(wal_service_handle));
    }

    let http_handle = current_thread_rt
        .as_ref()
        .unwrap_or_else(|| HTTP_RUNTIME.handle())
        .spawn(http::task_main(
            conf.clone(),
            http_listener,
            global_timelines.clone(),
        ))
        .map(|res| ("HTTP service main".to_owned(), res));
    tasks_handles.push(Box::pin(http_handle));

    let broker_task_handle = current_thread_rt
        .as_ref()
        .unwrap_or_else(|| BROKER_RUNTIME.handle())
        .spawn(
            broker::task_main(conf.clone(), global_timelines.clone())
                .instrument(info_span!("broker")),
        )
        .map(|res| ("broker main".to_owned(), res));
    tasks_handles.push(Box::pin(broker_task_handle));

    set_build_info_metric(GIT_VERSION, BUILD_TAG);

    // TODO: update tokio-stream, convert to real async Stream with
    // SignalStream, map it to obtain missing signal name, combine streams into
    // single stream we can easily sit on.
    let mut sigquit_stream = signal(SignalKind::quit())?;
    let mut sigint_stream = signal(SignalKind::interrupt())?;
    let mut sigterm_stream = signal(SignalKind::terminate())?;

    // Notify systemd that we are ready. This is important as currently loading
    // timelines takes significant time (~30s in busy regions).
    if let Err(e) = sd_notify::notify(true, &[NotifyState::Ready]) {
        warn!("systemd notify failed: {:?}", e);
    }

    tokio::select! {
        Some((task_name, res)) = tasks_handles.next()=> {
            error!("{} task failed: {:?}, exiting", task_name, res);
            std::process::exit(1);
        }
        // On any shutdown signal, log receival and exit. Additionally, handling
        // SIGQUIT prevents coredump.
        _ = sigquit_stream.recv() => info!("received SIGQUIT, terminating"),
        _ = sigint_stream.recv() => info!("received SIGINT, terminating"),
        _ = sigterm_stream.recv() => info!("received SIGTERM, terminating")

    };
    std::process::exit(0);
}

/// Determine safekeeper id.
fn set_id(workdir: &Utf8Path, given_id: Option<NodeId>) -> Result<NodeId> {
    let id_file_path = workdir.join(ID_FILE_NAME);

    let my_id: NodeId;
    // If file with ID exists, read it in; otherwise set one passed.
    match fs::read(&id_file_path) {
        Ok(id_serialized) => {
            my_id = NodeId(
                std::str::from_utf8(&id_serialized)
                    .context("failed to parse safekeeper id")?
                    .parse()
                    .context("failed to parse safekeeper id")?,
            );
            if let Some(given_id) = given_id {
                if given_id != my_id {
                    bail!(
                        "safekeeper already initialized with id {}, can't set {}",
                        my_id,
                        given_id
                    );
                }
            }
            info!("safekeeper ID {}", my_id);
        }
        Err(error) => match error.kind() {
            ErrorKind::NotFound => {
                my_id = if let Some(given_id) = given_id {
                    given_id
                } else {
                    bail!("safekeeper id is not specified");
                };
                let mut f = File::create(&id_file_path)
                    .with_context(|| format!("Failed to create id file at {id_file_path:?}"))?;
                f.write_all(my_id.to_string().as_bytes())?;
                f.sync_all()?;
                info!("initialized safekeeper id {}", my_id);
            }
            _ => {
                return Err(error.into());
            }
        },
    }
    Ok(my_id)
}

fn parse_remote_storage(storage_conf: &str) -> anyhow::Result<RemoteStorageConfig> {
    RemoteStorageConfig::from_toml(&storage_conf.parse()?)
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Args::command().debug_assert()
}
