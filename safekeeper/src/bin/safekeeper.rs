//
// Main entry point for the safekeeper executable
//
use anyhow::{bail, Context, Result};
use clap::{value_parser, Arg, ArgAction, Command};
use const_format::formatcp;
use remote_storage::RemoteStorageConfig;
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use tokio::sync::mpsc;
use toml_edit::Document;
use tracing::*;
use utils::pid_file;

use metrics::set_build_info_metric;
use safekeeper::broker;
use safekeeper::control_file;
use safekeeper::defaults::{
    DEFAULT_HEARTBEAT_TIMEOUT, DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_MAX_OFFLOADER_LAG_BYTES,
    DEFAULT_PG_LISTEN_ADDR, DEFAULT_WAL_BACKUP_RUNTIME_THREADS,
};
use safekeeper::http;
use safekeeper::remove_wal;
use safekeeper::wal_backup;
use safekeeper::wal_service;
use safekeeper::GlobalTimelines;
use safekeeper::SafeKeeperConf;
use storage_broker::DEFAULT_ENDPOINT;
use utils::auth::JwtAuth;
use utils::{
    http::endpoint,
    id::NodeId,
    logging::{self, LogFormat},
    project_git_version,
    sentry_init::{init_sentry, release_name},
    signals, tcp_listener,
};

const PID_FILE_NAME: &str = "safekeeper.pid";
const ID_FILE_NAME: &str = "safekeeper.id";

project_git_version!(GIT_VERSION);

fn main() -> anyhow::Result<()> {
    let arg_matches = cli().get_matches();

    if let Some(addr) = arg_matches.get_one::<String>("dump-control-file") {
        let state = control_file::FileStorage::load_control_file(Path::new(addr))?;
        let json = serde_json::to_string(&state)?;
        print!("{json}");
        return Ok(());
    }

    let mut conf = SafeKeeperConf::default();

    if let Some(dir) = arg_matches.get_one::<PathBuf>("datadir") {
        // change into the data directory.
        std::env::set_current_dir(dir)?;
    }

    if arg_matches.get_flag("no-sync") {
        conf.no_sync = true;
    }

    if let Some(addr) = arg_matches.get_one::<String>("listen-pg") {
        conf.listen_pg_addr = addr.to_string();
    }

    if let Some(addr) = arg_matches.get_one::<String>("listen-http") {
        conf.listen_http_addr = addr.to_string();
    }

    let mut given_id = None;
    if let Some(given_id_str) = arg_matches.get_one::<String>("id") {
        given_id = Some(NodeId(
            given_id_str
                .parse()
                .context("failed to parse safekeeper id")?,
        ));
    }

    if let Some(addr) = arg_matches.get_one::<String>("broker-endpoint") {
        conf.broker_endpoint = addr.parse().context("failed to parse broker endpoint")?;
    }

    if let Some(heartbeat_timeout_str) = arg_matches.get_one::<String>("heartbeat-timeout") {
        conf.heartbeat_timeout =
            humantime::parse_duration(heartbeat_timeout_str).with_context(|| {
                format!(
                    "failed to parse heartbeat-timeout {}",
                    heartbeat_timeout_str
                )
            })?;
    }

    if let Some(backup_threads) = arg_matches.get_one::<String>("wal-backup-threads") {
        conf.backup_runtime_threads = backup_threads
            .parse()
            .with_context(|| format!("Failed to parse backup threads {}", backup_threads))?;
    }
    if let Some(storage_conf) = arg_matches.get_one::<String>("remote-storage") {
        // funny toml doesn't consider plain inline table as valid document, so wrap in a key to parse
        let storage_conf_toml = format!("remote_storage = {}", storage_conf);
        let parsed_toml = storage_conf_toml.parse::<Document>()?; // parse
        let (_, storage_conf_parsed_toml) = parsed_toml.iter().next().unwrap(); // and strip key off again
        conf.remote_storage = Some(RemoteStorageConfig::from_toml(storage_conf_parsed_toml)?);
    }
    if let Some(max_offloader_lag_str) = arg_matches.get_one::<String>("max-offloader-lag") {
        conf.max_offloader_lag_bytes = max_offloader_lag_str.parse().with_context(|| {
            format!(
                "failed to parse max offloader lag {}",
                max_offloader_lag_str
            )
        })?;
    }
    // Seems like there is no better way to accept bool values explicitly in clap.
    conf.wal_backup_enabled = arg_matches
        .get_one::<String>("enable-wal-backup")
        .unwrap()
        .parse()
        .context("failed to parse bool enable-s3-offload bool")?;

    conf.auth_validation_public_key_path = arg_matches
        .get_one::<String>("auth-validation-public-key-path")
        .map(PathBuf::from);

    if let Some(log_format) = arg_matches.get_one::<String>("log-format") {
        conf.log_format = LogFormat::from_config(log_format)?;
    }

    // initialize sentry if SENTRY_DSN is provided
    let _sentry_guard = init_sentry(release_name!(), &[("node_id", &conf.my_id.to_string())]);
    start_safekeeper(conf, given_id, arg_matches.get_flag("init"))
}

fn start_safekeeper(mut conf: SafeKeeperConf, given_id: Option<NodeId>, init: bool) -> Result<()> {
    logging::init(conf.log_format)?;
    info!("version: {GIT_VERSION}");

    // Prevent running multiple safekeepers on the same directory
    let lock_file_path = conf.workdir.join(PID_FILE_NAME);
    let lock_file =
        pid_file::claim_for_current_process(&lock_file_path).context("claim pid file")?;
    info!("Claimed pid file at {lock_file_path:?}");

    // ensure that the lock file is held even if the main thread of the process is panics
    // we need to release the lock file only when the current process is gone
    std::mem::forget(lock_file);

    info!("Created PID file with PID {}", Pid::this().to_string());

    // Set or read our ID.
    set_id(&mut conf, given_id)?;
    if init {
        return Ok(());
    }

    info!(
        "Starting safekeeper http handler on {}",
        conf.listen_http_addr
    );
    let http_listener = tcp_listener::bind(conf.listen_http_addr.clone()).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_http_addr, e);
        e
    })?;

    info!(
        "Starting safekeeper pg protocol handler on {}",
        conf.listen_pg_addr
    );
    let pg_listener = tcp_listener::bind(conf.listen_pg_addr.clone()).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_pg_addr, e);
        e
    })?;

    let auth = match conf.auth_validation_public_key_path.as_ref() {
        None => {
            info!("Auth is disabled");
            None
        }
        Some(path) => {
            info!("Loading JWT auth key from {}", path.display());
            Some(Arc::new(
                JwtAuth::from_key_path(path).context("failed to load the auth key")?,
            ))
        }
    };

    // Register metrics collector for active timelines. It's important to do this
    // after daemonizing, otherwise process collector will be upset.
    let timeline_collector = safekeeper::metrics::TimelineCollector::new();
    metrics::register_internal(Box::new(timeline_collector))?;

    let signals = signals::install_shutdown_handlers()?;
    let mut threads = vec![];
    let (wal_backup_launcher_tx, wal_backup_launcher_rx) = mpsc::channel(100);

    // Load all timelines from disk to memory.
    GlobalTimelines::init(conf.clone(), wal_backup_launcher_tx)?;

    let conf_ = conf.clone();
    let auth_ = auth.clone();
    threads.push(
        thread::Builder::new()
            .name("http_endpoint_thread".into())
            .spawn(|| {
                let router = http::make_router(conf_, auth_);
                endpoint::serve_thread_main(
                    router,
                    http_listener,
                    std::future::pending(), // never shut down
                )
                .unwrap();
            })?,
    );

    let conf_cloned = conf.clone();
    let safekeeper_thread = thread::Builder::new()
        .name("Safekeeper thread".into())
        .spawn(|| {
            if let Err(e) = wal_service::thread_main(conf_cloned, pg_listener, auth) {
                info!("safekeeper thread terminated: {e}");
            }
        })
        .unwrap();

    threads.push(safekeeper_thread);

    let conf_ = conf.clone();
    threads.push(
        thread::Builder::new()
            .name("broker thread".into())
            .spawn(|| {
                // TODO: add auth?
                broker::thread_main(conf_);
            })?,
    );

    let conf_ = conf.clone();
    threads.push(
        thread::Builder::new()
            .name("WAL removal thread".into())
            .spawn(|| {
                remove_wal::thread_main(conf_);
            })?,
    );

    let conf_ = conf.clone();
    threads.push(
        thread::Builder::new()
            .name("wal backup launcher thread".into())
            .spawn(move || {
                wal_backup::wal_backup_launcher_thread_main(conf_, wal_backup_launcher_rx);
            })?,
    );

    set_build_info_metric(GIT_VERSION);
    // TODO: put more thoughts into handling of failed threads
    // We probably should restart them.

    // NOTE: we still have to handle signals like SIGQUIT to prevent coredumps
    signals.handle(|signal| {
        // TODO: implement graceful shutdown with joining threads etc
        info!(
            "received {}, terminating in immediate shutdown mode",
            signal.name()
        );
        std::process::exit(0);
    })
}

/// Determine safekeeper id and set it in config.
fn set_id(conf: &mut SafeKeeperConf, given_id: Option<NodeId>) -> Result<()> {
    let id_file_path = conf.workdir.join(ID_FILE_NAME);

    let my_id: NodeId;
    // If ID exists, read it in; otherwise set one passed
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
                let mut f = File::create(&id_file_path)?;
                f.write_all(my_id.to_string().as_bytes())?;
                f.sync_all()?;
                info!("initialized safekeeper ID {}", my_id);
            }
            _ => {
                return Err(error.into());
            }
        },
    }
    conf.my_id = my_id;
    Ok(())
}

fn cli() -> Command {
    Command::new("Neon safekeeper")
        .about("Store WAL stream to local file system and push it to WAL receivers")
        .version(GIT_VERSION)
        .arg(
            Arg::new("datadir")
                .short('D')
                .long("dir")
                .value_parser(value_parser!(PathBuf))
                .help("Path to the safekeeper data directory"),
        )
        .arg(
            Arg::new("init")
                .long("init")
                .action(ArgAction::SetTrue)
                .help("Initialize safekeeper with ID"),
        )
        .arg(
            Arg::new("listen-pg")
                .short('l')
                .long("listen-pg")
                .alias("listen") // for compatibility
                .help(formatcp!("listen for incoming WAL data connections on ip:port (default: {DEFAULT_PG_LISTEN_ADDR})")),
        )
        .arg(
            Arg::new("listen-http")
                .long("listen-http")
                .help(formatcp!("http endpoint address for metrics on ip:port (default: {DEFAULT_HTTP_LISTEN_ADDR})")),
        )
        // FIXME this argument is no longer needed since pageserver address is forwarded from compute.
        // However because this argument is in use by console's e2e tests let's keep it for now and remove separately.
        // So currently it is a noop.
        .arg(
            Arg::new("pageserver")
                .short('p')
                .long("pageserver"),
        )
        .arg(
            Arg::new("no-sync")
                .short('n')
                .long("no-sync")
                .action(ArgAction::SetTrue)
                .help("Do not wait for changes to be written safely to disk"),
        )
        .arg(
            Arg::new("dump-control-file")
                .long("dump-control-file")
                .help("Dump control file at path specified by this argument and exit"),
        )
        .arg(
            Arg::new("id").long("id").help("safekeeper node id: integer")
        ).arg(
            Arg::new("broker-endpoint")
            .long("broker-endpoint")
            .help(formatcp!("Broker endpoint for storage nodes coordination in the form http[s]://host:port, default '{DEFAULT_ENDPOINT}'. In case of https schema TLS is connection is established; plaintext otherwise.")),
        )
        .arg(
            Arg::new("heartbeat-timeout")
                .long("heartbeat-timeout")
                .help(formatcp!("Peer is considered dead after not receiving heartbeats from it during this period (default {}s), passed as a human readable duration.", DEFAULT_HEARTBEAT_TIMEOUT.as_secs()))
        )
        .arg(
            Arg::new("wal-backup-threads").long("backup-threads").help(formatcp!("number of threads for wal backup (default {DEFAULT_WAL_BACKUP_RUNTIME_THREADS}")),
        ).arg(
            Arg::new("remote-storage")
                .long("remote-storage")
                .help("Remote storage configuration for WAL backup (offloading to s3) as TOML inline table, e.g. {\"max_concurrent_syncs\" = 17, \"max_sync_errors\": 13, \"bucket_name\": \"<BUCKETNAME>\", \"bucket_region\":\"<REGION>\", \"concurrency_limit\": 119}.\nSafekeeper offloads WAL to [prefix_in_bucket/]<tenant_id>/<timeline_id>/<segment_file>, mirroring structure on the file system.")
        )
        .arg(
            Arg::new("max-offloader-lag")
                .long("max-offloader-lag")
                .help(formatcp!("Safekeeper won't be elected for WAL offloading if it is lagging for more than this value (default {}MB) in bytes", DEFAULT_MAX_OFFLOADER_LAG_BYTES / (1 << 20)))
        )
        .arg(
            Arg::new("enable-wal-backup")
                .long("enable-wal-backup")
                .default_value("true")
                .default_missing_value("true")
                .help("Enable/disable WAL backup to s3. When disabled, safekeeper removes WAL ignoring WAL backup horizon."),
        )
        .arg(
            Arg::new("auth-validation-public-key-path")
                .long("auth-validation-public-key-path")
                .help("Path to an RSA .pem public key which is used to check JWT tokens")
        )
        .arg(
            Arg::new("log-format")
                .long("log-format")
                .help("Format for logging, either 'plain' or 'json'")
        )
}

#[test]
fn verify_cli() {
    cli().debug_assert();
}
