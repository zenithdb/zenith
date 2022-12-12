//! Main entry point for the Page Server executable.

use std::env::{var, VarError};
use std::sync::Arc;
use std::{env, ops::ControlFlow, path::Path, str::FromStr};

use anyhow::{anyhow, Context};
use clap::{Arg, ArgAction, Command};
use fail::FailScenario;
use tracing::*;

use metrics::set_build_info_metric;
use pageserver::{
    config::{defaults::*, PageServerConf},
    http, page_cache, page_service, profiling, task_mgr,
    task_mgr::TaskKind,
    task_mgr::{
        BACKGROUND_RUNTIME, COMPUTE_REQUEST_RUNTIME, MGMT_REQUEST_RUNTIME, WALRECEIVER_RUNTIME,
    },
    tenant_mgr, virtual_file,
};
use remote_storage::GenericRemoteStorage;
use utils::{
    auth::JwtAuth,
    logging,
    postgres_backend::AuthType,
    project_git_version,
    sentry_init::{init_sentry, release_name},
    signals::{self, Signal},
    tcp_listener,
};

project_git_version!(GIT_VERSION);

const PID_FILE_NAME: &str = "pageserver.pid";

const FEATURES: &[&str] = &[
    #[cfg(feature = "testing")]
    "testing",
    #[cfg(feature = "fail/failpoints")]
    "fail/failpoints",
    #[cfg(feature = "profiling")]
    "profiling",
];

fn version() -> String {
    format!(
        "{GIT_VERSION} failpoints: {}, features: {:?}",
        fail::has_failpoints(),
        FEATURES,
    )
}

fn main() -> anyhow::Result<()> {
    let arg_matches = cli().get_matches();

    if arg_matches.get_flag("enabled-features") {
        println!("{{\"features\": {FEATURES:?} }}");
        return Ok(());
    }

    let workdir = arg_matches
        .get_one::<String>("workdir")
        .map(Path::new)
        .unwrap_or_else(|| Path::new(".neon"));
    let workdir = workdir
        .canonicalize()
        .with_context(|| format!("Error opening workdir '{}'", workdir.display()))?;

    let cfg_file_path = workdir.join("pageserver.toml");

    // Set CWD to workdir for non-daemon modes
    env::set_current_dir(&workdir).with_context(|| {
        format!(
            "Failed to set application's current dir to '{}'",
            workdir.display()
        )
    })?;

    let conf = match initialize_config(&cfg_file_path, arg_matches, &workdir)? {
        ControlFlow::Continue(conf) => conf,
        ControlFlow::Break(()) => {
            info!("Pageserver config init successful");
            return Ok(());
        }
    };

    // initialize sentry if SENTRY_DSN is provided
    let _sentry_guard = init_sentry(release_name!(), &[("node_id", &conf.id.to_string())]);

    let tenants_path = conf.tenants_path();
    if !tenants_path.exists() {
        utils::crashsafe::create_dir_all(conf.tenants_path()).with_context(|| {
            format!(
                "Failed to create tenants root dir at '{}'",
                tenants_path.display()
            )
        })?;
    }

    // Initialize up failpoints support
    let scenario = FailScenario::setup();

    // Basic initialization of things that don't change after startup
    virtual_file::init(conf.max_file_descriptors);
    page_cache::init(conf.page_cache_size);

    start_pageserver(conf).context("Failed to start pageserver")?;

    scenario.teardown();
    Ok(())
}

fn initialize_config(
    cfg_file_path: &Path,
    arg_matches: clap::ArgMatches,
    workdir: &Path,
) -> anyhow::Result<ControlFlow<(), &'static PageServerConf>> {
    let init = arg_matches.get_flag("init");
    let update_config = init || arg_matches.get_flag("update-config");

    let (mut toml, config_file_exists) = if cfg_file_path.is_file() {
        if init {
            anyhow::bail!(
                "Config file '{}' already exists, cannot init it, use --update-config to update it",
                cfg_file_path.display()
            );
        }
        // Supplement the CLI arguments with the config file
        let cfg_file_contents = std::fs::read_to_string(&cfg_file_path).with_context(|| {
            format!(
                "Failed to read pageserver config at '{}'",
                cfg_file_path.display()
            )
        })?;
        (
            cfg_file_contents
                .parse::<toml_edit::Document>()
                .with_context(|| {
                    format!(
                        "Failed to parse '{}' as pageserver config",
                        cfg_file_path.display()
                    )
                })?,
            true,
        )
    } else if cfg_file_path.exists() {
        anyhow::bail!(
            "Config file '{}' exists but is not a regular file",
            cfg_file_path.display()
        );
    } else {
        // We're initializing the tenant, so there's no config file yet
        (
            DEFAULT_CONFIG_FILE
                .parse::<toml_edit::Document>()
                .context("could not parse built-in config file")?,
            false,
        )
    };

    if let Some(values) = arg_matches.get_many::<String>("config-override") {
        for option_line in values {
            let doc = toml_edit::Document::from_str(option_line).with_context(|| {
                format!("Option '{option_line}' could not be parsed as a toml document")
            })?;

            for (key, item) in doc.iter() {
                if config_file_exists && update_config && key == "id" && toml.contains_key(key) {
                    anyhow::bail!("Pageserver config file exists at '{}' and has node id already, it cannot be overridden", cfg_file_path.display());
                }
                toml.insert(key, item.clone());
            }
        }
    }

    debug!("Resulting toml: {toml}");
    let conf = PageServerConf::parse_and_validate(&toml, workdir)
        .context("Failed to parse pageserver configuration")?;

    if update_config {
        info!("Writing pageserver config to '{}'", cfg_file_path.display());

        std::fs::write(&cfg_file_path, toml.to_string()).with_context(|| {
            format!(
                "Failed to write pageserver config to '{}'",
                cfg_file_path.display()
            )
        })?;
        info!(
            "Config successfully written to '{}'",
            cfg_file_path.display()
        )
    }

    Ok(if init {
        ControlFlow::Break(())
    } else {
        ControlFlow::Continue(Box::leak(Box::new(conf)))
    })
}

fn start_pageserver(conf: &'static PageServerConf) -> anyhow::Result<()> {
    logging::init(conf.log_format)?;
    info!("version: {}", version());

    // If any failpoints were set from FAILPOINTS environment variable,
    // print them to the log for debugging purposes
    let failpoints = fail::list();
    if !failpoints.is_empty() {
        info!(
            "started with failpoints: {}",
            failpoints
                .iter()
                .map(|(name, actions)| format!("{name}={actions}"))
                .collect::<Vec<String>>()
                .join(";")
        )
    }

    let lock_file_path = conf.workdir.join(PID_FILE_NAME);
    let lock_file =
        utils::pid_file::claim_for_current_process(&lock_file_path).context("claim pid file")?;
    info!("Claimed pid file at {lock_file_path:?}");

    // ensure that the lock file is held even if the main thread of the process is panics
    // we need to release the lock file only when the current process is gone
    std::mem::forget(lock_file);

    info!("Created PID file with PID {}", Pid::this().to_string());

    // TODO: Check that it looks like a valid repository before going further

    // bind sockets before daemonizing so we report errors early and do not return until we are listening
    info!(
        "Starting pageserver http handler on {}",
        conf.listen_http_addr
    );
    let http_listener = tcp_listener::bind(conf.listen_http_addr.clone())?;

    info!(
        "Starting pageserver pg protocol handler on {}",
        conf.listen_pg_addr
    );
    let pageserver_listener = tcp_listener::bind(conf.listen_pg_addr.clone())?;

    let signals = signals::install_shutdown_handlers()?;

    // start profiler (if enabled)
    let profiler_guard = profiling::init_profiler(conf);

    WALRECEIVER_RUNTIME.block_on(pageserver::walreceiver::init_broker_client(conf))?;

    // initialize authentication for incoming connections
    let auth = match &conf.auth_type {
        AuthType::Trust | AuthType::MD5 => None,
        AuthType::NeonJWT => {
            // unwrap is ok because check is performed when creating config, so path is set and file exists
            let key_path = conf.auth_validation_public_key_path.as_ref().unwrap();
            Some(JwtAuth::from_key_path(key_path)?.into())
        }
    };
    info!("Using auth: {:#?}", conf.auth_type);

    match var("ZENITH_AUTH_TOKEN") {
        Ok(v) => {
            info!("Loaded JWT token for authentication with Safekeeper");
            pageserver::config::SAFEKEEPER_AUTH_TOKEN
                .set(Arc::new(v))
                .map_err(|_| anyhow!("Could not initialize SAFEKEEPER_AUTH_TOKEN"))?;
        }
        Err(VarError::NotPresent) => {
            info!("No JWT token for authentication with Safekeeper detected");
        }
        Err(e) => {
            return Err(e).with_context(|| {
                "Failed to either load to detect non-present ZENITH_AUTH_TOKEN environment variable"
            })
        }
    };

    let remote_storage = conf
        .remote_storage_config
        .as_ref()
        .map(GenericRemoteStorage::from_config)
        .transpose()
        .context("Failed to init generic remote storage")?;

    let (init_result_sender, init_result_receiver) =
        std::sync::mpsc::channel::<anyhow::Result<()>>();
    let storage_for_spawn = remote_storage.clone();
    let _handler = BACKGROUND_RUNTIME.spawn(async move {
        let result = tenant_mgr::init_tenant_mgr(conf, storage_for_spawn).await;
        init_result_sender.send(result)
    });
    match init_result_receiver.recv() {
        Ok(init_result) => init_result.context("Failed to init tenant_mgr")?,
        Err(_sender_dropped_err) => {
            anyhow::bail!("Failed to init tenant_mgr: no init status was returned");
        }
    }

    // Spawn all HTTP related tasks in the MGMT_REQUEST_RUNTIME.
    // bind before launching separate thread so the error reported before startup exits

    // Create a Service from the router above to handle incoming requests.
    {
        let _rt_guard = MGMT_REQUEST_RUNTIME.enter();

        let router = http::make_router(conf, auth.clone(), remote_storage)?;
        let service =
            utils::http::RouterService::new(router.build().map_err(|err| anyhow!(err))?).unwrap();
        let server = hyper::Server::from_tcp(http_listener)?
            .serve(service)
            .with_graceful_shutdown(task_mgr::shutdown_watcher());

        task_mgr::spawn(
            MGMT_REQUEST_RUNTIME.handle(),
            TaskKind::HttpEndpointListener,
            None,
            None,
            "http endpoint listener",
            true,
            async {
                server.await?;
                Ok(())
            },
        );
    }

    // Spawn a task to listen for libpq connections. It will spawn further tasks
    // for each connection.
    task_mgr::spawn(
        COMPUTE_REQUEST_RUNTIME.handle(),
        TaskKind::LibpqEndpointListener,
        None,
        None,
        "libpq endpoint listener",
        true,
        async move {
            page_service::libpq_listener_main(conf, auth, pageserver_listener, conf.auth_type).await
        },
    );

    set_build_info_metric(GIT_VERSION);

    // All started up! Now just sit and wait for shutdown signal.
    signals.handle(|signal| match signal {
        Signal::Quit => {
            info!(
                "Got {}. Terminating in immediate shutdown mode",
                signal.name()
            );
            profiling::exit_profiler(conf, &profiler_guard);
            std::process::exit(111);
        }

        Signal::Interrupt | Signal::Terminate => {
            info!(
                "Got {}. Terminating gracefully in fast shutdown mode",
                signal.name()
            );
            profiling::exit_profiler(conf, &profiler_guard);
            BACKGROUND_RUNTIME.block_on(pageserver::shutdown_pageserver(0));
            unreachable!()
        }
    })
}

fn cli() -> Command {
    Command::new("Neon page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .version(version())
        .arg(
            Arg::new("init")
                .long("init")
                .action(ArgAction::SetTrue)
                .help("Initialize pageserver with all given config overrides"),
        )
        .arg(
            Arg::new("workdir")
                .short('D')
                .long("workdir")
                .help("Working directory for the pageserver"),
        )
        // See `settings.md` for more details on the extra configuration patameters pageserver can process
        .arg(
            Arg::new("config-override")
                .short('c')
                .num_args(1)
                .action(ArgAction::Append)
                .help("Additional configuration overrides of the ones from the toml config file (or new ones to add there). \
                Any option has to be a valid toml document, example: `-c=\"foo='hey'\"` `-c=\"foo={value=1}\"`"),
        )
        .arg(
            Arg::new("update-config")
                .long("update-config")
                .action(ArgAction::SetTrue)
                .help("Update the config file when started"),
        )
        .arg(
            Arg::new("enabled-features")
                .long("enabled-features")
                .action(ArgAction::SetTrue)
                .help("Show enabled compile time features"),
        )
}

#[test]
fn verify_cli() {
    cli().debug_assert();
}
