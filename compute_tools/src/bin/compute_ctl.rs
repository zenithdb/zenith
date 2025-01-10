//!
//! Postgres wrapper (`compute_ctl`) is intended to be run as a Docker entrypoint or as a `systemd`
//! `ExecStart` option. It will handle all the `Neon` specifics during compute node
//! initialization:
//! - `compute_ctl` accepts cluster (compute node) specification as a JSON file.
//! - Every start is a fresh start, so the data directory is removed and
//!   initialized again on each run.
//! - If remote_extension_config is provided, it will be used to fetch extensions list
//!   and download `shared_preload_libraries` from the remote storage.
//! - Next it will put configuration files into the `PGDATA` directory.
//! - Sync safekeepers and get commit LSN.
//! - Get `basebackup` from pageserver using the returned on the previous step LSN.
//! - Try to start `postgres` and wait until it is ready to accept connections.
//! - Check and alter/drop/create roles and databases.
//! - Hang waiting on the `postmaster` process to exit.
//!
//! Also `compute_ctl` spawns two separate service threads:
//! - `compute-monitor` checks the last Postgres activity timestamp and saves it
//!   into the shared `ComputeNode`;
//! - `http-endpoint` runs a Hyper HTTP API server, which serves readiness and the
//!   last activity requests.
//!
//! If `AUTOSCALING` environment variable is set, `compute_ctl` will start the
//! `vm-monitor` located in [`neon/libs/vm_monitor`]. For VM compute nodes,
//! `vm-monitor` communicates with the VM autoscaling system. It coordinates
//! downscaling and requests immediate upscaling under resource pressure.
//!
//! Usage example:
//! ```sh
//! compute_ctl -D /var/db/postgres/compute \
//!             -C 'postgresql://cloud_admin@localhost/postgres' \
//!             -S /var/db/postgres/specs/current.json \
//!             -b /usr/local/bin/postgres \
//!             -r http://pg-ext-s3-gateway \
//! ```
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::process::exit;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc, Condvar, Mutex, RwLock};
use std::{thread, time::Duration};

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Arg;
use compute_tools::disk_quota::set_disk_quota;
use compute_tools::lsn_lease::launch_lsn_lease_bg_task_for_static;
use signal_hook::consts::{SIGQUIT, SIGTERM};
use signal_hook::{consts::SIGINT, iterator::Signals};
use tracing::{error, info, warn};
use url::Url;

use compute_api::responses::ComputeStatus;
use compute_api::spec::ComputeSpec;

use compute_tools::compute::{
    forward_termination_signal, ComputeNode, ComputeState, ParsedSpec, PG_PID,
};
use compute_tools::configurator::launch_configurator;
use compute_tools::extension_server::get_pg_version_string;
use compute_tools::http::launch_http_server;
use compute_tools::logger::*;
use compute_tools::monitor::launch_monitor;
use compute_tools::params::*;
use compute_tools::spec::*;
use compute_tools::swap::resize_swap;
use rlimit::{setrlimit, Resource};
use utils::failpoint_support;

// this is an arbitrary build tag. Fine as a default / for testing purposes
// in-case of not-set environment var
const BUILD_TAG_DEFAULT: &str = "latest";

fn main() -> Result<()> {
    let scenario = failpoint_support::init();

    let (build_tag, clap_args) = init()?;

    // enable core dumping for all child processes
    setrlimit(Resource::CORE, rlimit::INFINITY, rlimit::INFINITY)?;

    let (pg_handle, start_pg_result) = {
        // Enter startup tracing context
        let _startup_context_guard = startup_context_from_env();

        let cli_args = process_cli(&clap_args)?;

        let cli_spec = try_spec_from_cli(&clap_args, &cli_args)?;

        let wait_spec_result = wait_spec(build_tag, cli_args, cli_spec)?;

        start_postgres(&clap_args, wait_spec_result)?

        // Startup is finished, exit the startup tracing span
    };

    // PostgreSQL is now running, if startup was successful. Wait until it exits.
    let wait_pg_result = wait_postgres(pg_handle)?;

    let delay_exit = cleanup_after_postgres_exit(start_pg_result)?;

    maybe_delay_exit(delay_exit);

    scenario.teardown();

    deinit_and_exit(wait_pg_result);
}

fn init() -> Result<(String, clap::ArgMatches)> {
    init_tracing_and_logging(DEFAULT_LOG_LEVEL)?;

    let mut signals = Signals::new([SIGINT, SIGTERM, SIGQUIT])?;
    thread::spawn(move || {
        for sig in signals.forever() {
            handle_exit_signal(sig);
        }
    });

    let build_tag = option_env!("BUILD_TAG")
        .unwrap_or(BUILD_TAG_DEFAULT)
        .to_string();
    info!("build_tag: {build_tag}");

    Ok((build_tag, cli().get_matches()))
}

fn process_cli(matches: &clap::ArgMatches) -> Result<ProcessCliResult> {
    let pgbin_default = "postgres";
    let pgbin = matches
        .get_one::<String>("pgbin")
        .map(|s| s.as_str())
        .unwrap_or(pgbin_default);

    let ext_remote_storage = matches
        .get_one::<String>("remote-ext-config")
        // Compatibility hack: if the control plane specified any remote-ext-config
        // use the default value for extension storage proxy gateway.
        // Remove this once the control plane is updated to pass the gateway URL
        .map(|conf| {
            if conf.starts_with("http") {
                conf.trim_end_matches('/')
            } else {
                "http://pg-ext-s3-gateway"
            }
        });

    let http_port = *matches
        .get_one::<u16>("http-port")
        .expect("http-port is required");
    let pgdata = matches
        .get_one::<String>("pgdata")
        .expect("PGDATA path is required");
    let connstr = matches
        .get_one::<String>("connstr")
        .expect("Postgres connection string is required");
    let spec_json = matches.get_one::<String>("spec");
    let spec_path = matches.get_one::<String>("spec-path");
    let resize_swap_on_bind = matches.get_flag("resize-swap-on-bind");
    let set_disk_quota_for_fs = matches.get_one::<String>("set-disk-quota-for-fs");

    Ok(ProcessCliResult {
        connstr,
        pgdata,
        pgbin,
        ext_remote_storage,
        http_port,
        spec_json,
        spec_path,
        resize_swap_on_bind,
        set_disk_quota_for_fs,
    })
}

struct ProcessCliResult<'clap> {
    connstr: &'clap str,
    pgdata: &'clap str,
    pgbin: &'clap str,
    ext_remote_storage: Option<&'clap str>,
    http_port: u16,
    spec_json: Option<&'clap String>,
    spec_path: Option<&'clap String>,
    resize_swap_on_bind: bool,
    set_disk_quota_for_fs: Option<&'clap String>,
}

fn startup_context_from_env() -> Option<opentelemetry::ContextGuard> {
    // Extract OpenTelemetry context for the startup actions from the
    // TRACEPARENT and TRACESTATE env variables, and attach it to the current
    // tracing context.
    //
    // This is used to propagate the context for the 'start_compute' operation
    // from the neon control plane. This allows linking together the wider
    // 'start_compute' operation that creates the compute container, with the
    // startup actions here within the container.
    //
    // There is no standard for passing context in env variables, but a lot of
    // tools use TRACEPARENT/TRACESTATE, so we use that convention too. See
    // https://github.com/open-telemetry/opentelemetry-specification/issues/740
    //
    // Switch to the startup context here, and exit it once the startup has
    // completed and Postgres is up and running.
    //
    // If this pod is pre-created without binding it to any particular endpoint
    // yet, this isn't the right place to enter the startup context. In that
    // case, the control plane should pass the tracing context as part of the
    // /configure API call.
    //
    // NOTE: This is supposed to only cover the *startup* actions. Once
    // postgres is configured and up-and-running, we exit this span. Any other
    // actions that are performed on incoming HTTP requests, for example, are
    // performed in separate spans.
    //
    // XXX: If the pod is restarted, we perform the startup actions in the same
    // context as the original startup actions, which probably doesn't make
    // sense.
    let mut startup_tracing_carrier: HashMap<String, String> = HashMap::new();
    if let Ok(val) = std::env::var("TRACEPARENT") {
        startup_tracing_carrier.insert("traceparent".to_string(), val);
    }
    if let Ok(val) = std::env::var("TRACESTATE") {
        startup_tracing_carrier.insert("tracestate".to_string(), val);
    }
    if !startup_tracing_carrier.is_empty() {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry_sdk::propagation::TraceContextPropagator;
        let guard = TraceContextPropagator::new()
            .extract(&startup_tracing_carrier)
            .attach();
        info!("startup tracing context attached");
        Some(guard)
    } else {
        None
    }
}

fn try_spec_from_cli(
    matches: &clap::ArgMatches,
    ProcessCliResult {
        spec_json,
        spec_path,
        ..
    }: &ProcessCliResult,
) -> Result<CliSpecParams> {
    let compute_id = matches.get_one::<String>("compute-id");
    let control_plane_uri = matches.get_one::<String>("control-plane-uri");

    // First, try to get cluster spec from the cli argument
    if let Some(spec_json) = spec_json {
        info!("got spec from cli argument {}", spec_json);
        return Ok(CliSpecParams {
            spec: Some(serde_json::from_str(spec_json)?),
            live_config_allowed: false,
        });
    }

    // Second, try to read it from the file if path is provided
    if let Some(spec_path) = spec_path {
        let file = File::open(Path::new(spec_path))?;
        return Ok(CliSpecParams {
            spec: Some(serde_json::from_reader(file)?),
            live_config_allowed: true,
        });
    }

    let Some(compute_id) = compute_id else {
        panic!(
            "compute spec should be provided by one of the following ways: \
                --spec OR --spec-path OR --control-plane-uri and --compute-id"
        );
    };
    let Some(control_plane_uri) = control_plane_uri else {
        panic!("must specify both --control-plane-uri and --compute-id or none");
    };

    match get_spec_from_control_plane(control_plane_uri, compute_id) {
        Ok(spec) => Ok(CliSpecParams {
            spec,
            live_config_allowed: true,
        }),
        Err(e) => {
            error!(
                "cannot get response from control plane: {}\n\
                neither spec nor confirmation that compute is in the Empty state was received",
                e
            );
            Err(e)
        }
    }
}

struct CliSpecParams {
    /// If a spec was provided via CLI or file, the [`ComputeSpec`]
    spec: Option<ComputeSpec>,
    live_config_allowed: bool,
}

fn wait_spec(
    build_tag: String,
    ProcessCliResult {
        connstr,
        pgdata,
        pgbin,
        ext_remote_storage,
        resize_swap_on_bind,
        set_disk_quota_for_fs,
        http_port,
        ..
    }: ProcessCliResult,
    CliSpecParams {
        spec,
        live_config_allowed,
    }: CliSpecParams,
) -> Result<WaitSpecResult> {
    let mut new_state = ComputeState::new();
    let spec_set;

    if let Some(spec) = spec {
        let pspec = ParsedSpec::try_from(spec).map_err(|msg| anyhow::anyhow!(msg))?;
        info!("new pspec.spec: {:?}", pspec.spec);
        new_state.pspec = Some(pspec);
        spec_set = true;
    } else {
        spec_set = false;
    }
    let connstr = Url::parse(connstr).context("cannot parse connstr as a URL")?;
    let conn_conf = postgres::config::Config::from_str(connstr.as_str())
        .context("cannot build postgres config from connstr")?;
    let tokio_conn_conf = tokio_postgres::config::Config::from_str(connstr.as_str())
        .context("cannot build tokio postgres config from connstr")?;
    let compute_node = ComputeNode {
        connstr,
        conn_conf,
        tokio_conn_conf,
        pgdata: pgdata.to_string(),
        pgbin: pgbin.to_string(),
        pgversion: get_pg_version_string(pgbin),
        http_port,
        live_config_allowed,
        state: Mutex::new(new_state),
        state_changed: Condvar::new(),
        ext_remote_storage: ext_remote_storage.map(|s| s.to_string()),
        ext_download_progress: RwLock::new(HashMap::new()),
        build_tag,
    };
    let compute = Arc::new(compute_node);

    // If this is a pooled VM, prewarm before starting HTTP server and becoming
    // available for binding. Prewarming helps Postgres start quicker later,
    // because QEMU will already have its memory allocated from the host, and
    // the necessary binaries will already be cached.
    if !spec_set {
        compute.prewarm_postgres()?;
    }

    // Launch http service first, so that we can serve control-plane requests
    // while configuration is still in progress.
    let _http_handle =
        launch_http_server(http_port, &compute).expect("cannot launch http endpoint thread");

    if !spec_set {
        // No spec provided, hang waiting for it.
        info!("no compute spec provided, waiting");

        let mut state = compute.state.lock().unwrap();
        while state.status != ComputeStatus::ConfigurationPending {
            state = compute.state_changed.wait(state).unwrap();

            if state.status == ComputeStatus::ConfigurationPending {
                info!("got spec, continue configuration");
                // Spec is already set by the http server handler.
                break;
            }
        }

        // Record for how long we slept waiting for the spec.
        let now = Utc::now();
        state.metrics.wait_for_spec_ms = now
            .signed_duration_since(state.start_time)
            .to_std()
            .unwrap()
            .as_millis() as u64;

        // Reset start time, so that the total startup time that is calculated later will
        // not include the time that we waited for the spec.
        state.start_time = now;
    }

    launch_lsn_lease_bg_task_for_static(&compute);

    Ok(WaitSpecResult {
        compute,
        resize_swap_on_bind,
        set_disk_quota_for_fs: set_disk_quota_for_fs.cloned(),
    })
}

struct WaitSpecResult {
    compute: Arc<ComputeNode>,
    resize_swap_on_bind: bool,
    set_disk_quota_for_fs: Option<String>,
}

fn start_postgres(
    // need to allow unused because `matches` is only used if target_os = "linux"
    #[allow(unused_variables)] matches: &clap::ArgMatches,
    WaitSpecResult {
        compute,
        resize_swap_on_bind,
        set_disk_quota_for_fs,
    }: WaitSpecResult,
) -> Result<(Option<PostgresHandle>, StartPostgresResult)> {
    // We got all we need, update the state.
    let mut state = compute.state.lock().unwrap();
    state.set_status(ComputeStatus::Init, &compute.state_changed);

    info!(
        "running compute with features: {:?}",
        state.pspec.as_ref().unwrap().spec.features
    );
    // before we release the mutex, fetch some parameters for later.
    let &ComputeSpec {
        swap_size_bytes,
        disk_quota_bytes,
        #[cfg(target_os = "linux")]
        disable_lfc_resizing,
        ..
    } = &state.pspec.as_ref().unwrap().spec;
    drop(state);

    // Launch remaining service threads
    let _monitor_handle = launch_monitor(&compute);
    let _configurator_handle = launch_configurator(&compute);

    let mut prestartup_failed = false;
    let mut delay_exit = false;

    // Resize swap to the desired size if the compute spec says so
    if let (Some(size_bytes), true) = (swap_size_bytes, resize_swap_on_bind) {
        // To avoid 'swapoff' hitting postgres startup, we need to run resize-swap to completion
        // *before* starting postgres.
        //
        // In theory, we could do this asynchronously if SkipSwapon was enabled for VMs, but this
        // carries a risk of introducing hard-to-debug issues - e.g. if postgres sometimes gets
        // OOM-killed during startup because swap wasn't available yet.
        match resize_swap(size_bytes) {
            Ok(()) => {
                let size_mib = size_bytes as f32 / (1 << 20) as f32; // just for more coherent display.
                info!(%size_bytes, %size_mib, "resized swap");
            }
            Err(err) => {
                let err = err.context("failed to resize swap");
                error!("{err:#}");

                // Mark compute startup as failed; don't try to start postgres, and report this
                // error to the control plane when it next asks.
                prestartup_failed = true;
                compute.set_failed_status(err);
                delay_exit = true;
            }
        }
    }

    // Set disk quota if the compute spec says so
    if let (Some(disk_quota_bytes), Some(disk_quota_fs_mountpoint)) =
        (disk_quota_bytes, set_disk_quota_for_fs)
    {
        match set_disk_quota(disk_quota_bytes, &disk_quota_fs_mountpoint) {
            Ok(()) => {
                let size_mib = disk_quota_bytes as f32 / (1 << 20) as f32; // just for more coherent display.
                info!(%disk_quota_bytes, %size_mib, "set disk quota");
            }
            Err(err) => {
                let err = err.context("failed to set disk quota");
                error!("{err:#}");

                // Mark compute startup as failed; don't try to start postgres, and report this
                // error to the control plane when it next asks.
                prestartup_failed = true;
                compute.set_failed_status(err);
                delay_exit = true;
            }
        }
    }

    // Start Postgres
    let mut pg = None;
    if !prestartup_failed {
        pg = match compute.start_compute() {
            Ok(pg) => {
                info!(postmaster_pid = %pg.0.id(), "Postgres was started");
                Some(pg)
            }
            Err(err) => {
                error!("could not start the compute node: {:#}", err);
                compute.set_failed_status(err);
                delay_exit = true;
                None
            }
        };
    } else {
        warn!("skipping postgres startup because pre-startup step failed");
    }

    // Start the vm-monitor if directed to. The vm-monitor only runs on linux
    // because it requires cgroups.
    cfg_if::cfg_if! {
        if #[cfg(target_os = "linux")] {
            use std::env;
            use tokio_util::sync::CancellationToken;
            let vm_monitor_addr = matches
                .get_one::<String>("vm-monitor-addr")
                .expect("--vm-monitor-addr should always be set because it has a default arg");
            let file_cache_connstr = matches.get_one::<String>("filecache-connstr");
            let cgroup = matches.get_one::<String>("cgroup");

            // Only make a runtime if we need to.
            // Note: it seems like you can make a runtime in an inner scope and
            // if you start a task in it it won't be dropped. However, make it
            // in the outermost scope just to be safe.
            let rt = if env::var_os("AUTOSCALING").is_some() {
                Some(
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(4)
                        .enable_all()
                        .build()
                        .expect("failed to create tokio runtime for monitor")
                )
            } else {
                None
            };

            // This token is used internally by the monitor to clean up all threads
            let token = CancellationToken::new();

            // don't pass postgres connection string to vm-monitor if we don't want it to resize LFC
            let pgconnstr = if disable_lfc_resizing.unwrap_or(false) {
                None
            } else {
                file_cache_connstr.cloned()
            };

            let vm_monitor = rt.as_ref().map(|rt| {
                rt.spawn(vm_monitor::start(
                    Box::leak(Box::new(vm_monitor::Args {
                        cgroup: cgroup.cloned(),
                        pgconnstr,
                        addr: vm_monitor_addr.clone(),
                    })),
                    token.clone(),
                ))
            });
        }
    }

    Ok((
        pg,
        StartPostgresResult {
            delay_exit,
            compute,
            #[cfg(target_os = "linux")]
            rt,
            #[cfg(target_os = "linux")]
            token,
            #[cfg(target_os = "linux")]
            vm_monitor,
        },
    ))
}

type PostgresHandle = (std::process::Child, std::thread::JoinHandle<()>);

struct StartPostgresResult {
    delay_exit: bool,
    // passed through from WaitSpecResult
    compute: Arc<ComputeNode>,

    #[cfg(target_os = "linux")]
    rt: Option<tokio::runtime::Runtime>,
    #[cfg(target_os = "linux")]
    token: tokio_util::sync::CancellationToken,
    #[cfg(target_os = "linux")]
    vm_monitor: Option<tokio::task::JoinHandle<Result<()>>>,
}

fn wait_postgres(pg: Option<PostgresHandle>) -> Result<WaitPostgresResult> {
    // Wait for the child Postgres process forever. In this state Ctrl+C will
    // propagate to Postgres and it will be shut down as well.
    let mut exit_code = None;
    if let Some((mut pg, logs_handle)) = pg {
        info!(postmaster_pid = %pg.id(), "Waiting for Postgres to exit");

        let ecode = pg
            .wait()
            .expect("failed to start waiting on Postgres process");
        PG_PID.store(0, Ordering::SeqCst);

        // Process has exited, so we can join the logs thread.
        let _ = logs_handle
            .join()
            .map_err(|e| tracing::error!("log thread panicked: {:?}", e));

        info!("Postgres exited with code {}, shutting down", ecode);
        exit_code = ecode.code()
    }

    Ok(WaitPostgresResult { exit_code })
}

struct WaitPostgresResult {
    exit_code: Option<i32>,
}

fn cleanup_after_postgres_exit(
    StartPostgresResult {
        mut delay_exit,
        compute,
        #[cfg(target_os = "linux")]
        vm_monitor,
        #[cfg(target_os = "linux")]
        token,
        #[cfg(target_os = "linux")]
        rt,
    }: StartPostgresResult,
) -> Result<bool> {
    // Terminate the vm_monitor so it releases the file watcher on
    // /sys/fs/cgroup/neon-postgres.
    // Note: the vm-monitor only runs on linux because it requires cgroups.
    cfg_if::cfg_if! {
        if #[cfg(target_os = "linux")] {
            if let Some(handle) = vm_monitor {
                // Kills all threads spawned by the monitor
                token.cancel();
                // Kills the actual task running the monitor
                handle.abort();

                // If handle is some, rt must have been used to produce it, and
                // hence is also some
                rt.unwrap().shutdown_timeout(Duration::from_secs(2));
            }
        }
    }

    // Maybe sync safekeepers again, to speed up next startup
    let compute_state = compute.state.lock().unwrap().clone();
    let pspec = compute_state.pspec.as_ref().expect("spec must be set");
    if matches!(pspec.spec.mode, compute_api::spec::ComputeMode::Primary) {
        info!("syncing safekeepers on shutdown");
        let storage_auth_token = pspec.storage_auth_token.clone();
        let lsn = compute.sync_safekeepers(storage_auth_token)?;
        info!("synced safekeepers at lsn {lsn}");
    }

    let mut state = compute.state.lock().unwrap();
    if state.status == ComputeStatus::TerminationPending {
        state.status = ComputeStatus::Terminated;
        compute.state_changed.notify_all();
        // we were asked to terminate gracefully, don't exit to avoid restart
        delay_exit = true
    }
    drop(state);

    if let Err(err) = compute.check_for_core_dumps() {
        error!("error while checking for core dumps: {err:?}");
    }

    Ok(delay_exit)
}

fn maybe_delay_exit(delay_exit: bool) {
    // If launch failed, keep serving HTTP requests for a while, so the cloud
    // control plane can get the actual error.
    if delay_exit {
        info!("giving control plane 30s to collect the error before shutdown");
        thread::sleep(Duration::from_secs(30));
    }
}

fn deinit_and_exit(WaitPostgresResult { exit_code }: WaitPostgresResult) -> ! {
    // Shutdown trace pipeline gracefully, so that it has a chance to send any
    // pending traces before we exit. Shutting down OTEL tracing provider may
    // hang for quite some time, see, for example:
    // - https://github.com/open-telemetry/opentelemetry-rust/issues/868
    // - and our problems with staging https://github.com/neondatabase/cloud/issues/3707#issuecomment-1493983636
    //
    // Yet, we want computes to shut down fast enough, as we may need a new one
    // for the same timeline ASAP. So wait no longer than 2s for the shutdown to
    // complete, then just error out and exit the main thread.
    info!("shutting down tracing");
    let (sender, receiver) = mpsc::channel();
    let _ = thread::spawn(move || {
        tracing_utils::shutdown_tracing();
        sender.send(()).ok()
    });
    let shutdown_res = receiver.recv_timeout(Duration::from_millis(2000));
    if shutdown_res.is_err() {
        error!("timed out while shutting down tracing, exiting anyway");
    }

    info!("shutting down");
    exit(exit_code.unwrap_or(1))
}

fn cli() -> clap::Command {
    // Env variable is set by `cargo`
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    clap::Command::new("compute_ctl")
        .version(version)
        .arg(
            Arg::new("http-port")
                .long("http-port")
                .value_name("HTTP_PORT")
                .default_value("3080")
                .value_parser(clap::value_parser!(u16))
                .required(false),
        )
        .arg(
            Arg::new("connstr")
                .short('C')
                .long("connstr")
                .value_name("DATABASE_URL")
                .required(true),
        )
        .arg(
            Arg::new("pgdata")
                .short('D')
                .long("pgdata")
                .value_name("DATADIR")
                .required(true),
        )
        .arg(
            Arg::new("pgbin")
                .short('b')
                .long("pgbin")
                .default_value("postgres")
                .value_name("POSTGRES_PATH"),
        )
        .arg(
            Arg::new("spec")
                .short('s')
                .long("spec")
                .value_name("SPEC_JSON"),
        )
        .arg(
            Arg::new("spec-path")
                .short('S')
                .long("spec-path")
                .value_name("SPEC_PATH"),
        )
        .arg(
            Arg::new("compute-id")
                .short('i')
                .long("compute-id")
                .value_name("COMPUTE_ID"),
        )
        .arg(
            Arg::new("control-plane-uri")
                .short('p')
                .long("control-plane-uri")
                .value_name("CONTROL_PLANE_API_BASE_URI"),
        )
        .arg(
            Arg::new("remote-ext-config")
                .short('r')
                .long("remote-ext-config")
                .value_name("REMOTE_EXT_CONFIG"),
        )
        // TODO(fprasx): we currently have default arguments because the cloud PR
        // to pass them in hasn't been merged yet. We should get rid of them once
        // the PR is merged.
        .arg(
            Arg::new("vm-monitor-addr")
                .long("vm-monitor-addr")
                .default_value("0.0.0.0:10301")
                .value_name("VM_MONITOR_ADDR"),
        )
        .arg(
            Arg::new("cgroup")
                .long("cgroup")
                .default_value("neon-postgres")
                .value_name("CGROUP"),
        )
        .arg(
            Arg::new("filecache-connstr")
                .long("filecache-connstr")
                .default_value(
                    "host=localhost port=5432 dbname=postgres user=cloud_admin sslmode=disable application_name=vm-monitor",
                )
                .value_name("FILECACHE_CONNSTR"),
        )
        .arg(
            Arg::new("resize-swap-on-bind")
                .long("resize-swap-on-bind")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("set-disk-quota-for-fs")
                .long("set-disk-quota-for-fs")
                .value_name("SET_DISK_QUOTA_FOR_FS")
        )
}

/// When compute_ctl is killed, send also termination signal to sync-safekeepers
/// to prevent leakage. TODO: it is better to convert compute_ctl to async and
/// wait for termination which would be easy then.
fn handle_exit_signal(sig: i32) {
    info!("received {sig} termination signal");
    forward_termination_signal();
    exit(1);
}

#[test]
fn verify_cli() {
    cli().debug_assert()
}
