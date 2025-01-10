//! Code to manage compute endpoints
//!
//! In the local test environment, the data for each endpoint is stored in
//!
//! ```text
//!   .neon/endpoints/<endpoint id>
//! ```
//!
//! Some basic information about the endpoint, like the tenant and timeline IDs,
//! are stored in the `endpoint.json` file. The `endpoint.json` file is created
//! when the endpoint is created, and doesn't change afterwards.
//!
//! The endpoint is managed by the `compute_ctl` binary. When an endpoint is
//! started, we launch `compute_ctl` It synchronizes the safekeepers, downloads
//! the basebackup from the pageserver to initialize the data directory, and
//! finally launches the PostgreSQL process. It watches the PostgreSQL process
//! until it exits.
//!
//! When an endpoint is created, a `postgresql.conf` file is also created in
//! the endpoint's directory. The file can be modified before starting PostgreSQL.
//! However, the `postgresql.conf` file in the endpoint directory is not used directly
//! by PostgreSQL. It is passed to `compute_ctl`, and `compute_ctl` writes another
//! copy of it in the data directory.
//!
//! Directory contents:
//!
//! ```text
//! .neon/endpoints/main/
//!     compute.log               - log output of `compute_ctl` and `postgres`
//!     endpoint.json             - serialized `EndpointConf` struct
//!     postgresql.conf           - postgresql settings
//!     spec.json                 - passed to `compute_ctl`
//!     pgdata/
//!         postgresql.conf       - copy of postgresql.conf created by `compute_ctl`
//!         zenith.signal
//!         <other PostgreSQL files>
//! ```
//!
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use compute_api::spec::Database;
use compute_api::spec::PgIdent;
use compute_api::spec::RemoteExtSpec;
use compute_api::spec::Role;
use nix::sys::signal::kill;
use nix::sys::signal::Signal;
use pageserver_api::shard::ShardStripeSize;
use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use url::Host;
use utils::id::{NodeId, TenantId, TimelineId};

use crate::local_env::LocalEnv;
use crate::postgresql_conf::PostgresConf;
use crate::storage_controller::StorageController;

use compute_api::responses::{ComputeStatus, ComputeStatusResponse};
use compute_api::spec::{Cluster, ComputeFeature, ComputeMode, ComputeSpec};

// contents of a endpoint.json file
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct EndpointConf {
    endpoint_id: String,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    mode: ComputeMode,
    pg_port: u16,
    http_port: u16,
    pg_version: u32,
    skip_pg_catalog_updates: bool,
    features: Vec<ComputeFeature>,
}

//
// ComputeControlPlane
//
pub struct ComputeControlPlane {
    base_port: u16,

    // endpoint ID is the key
    pub endpoints: BTreeMap<String, Arc<Endpoint>>,

    env: LocalEnv,
}

impl ComputeControlPlane {
    // Load current endpoints from the endpoints/ subdirectories
    pub fn load(env: LocalEnv) -> Result<ComputeControlPlane> {
        let mut endpoints = BTreeMap::default();
        for endpoint_dir in std::fs::read_dir(env.endpoints_path())
            .with_context(|| format!("failed to list {}", env.endpoints_path().display()))?
        {
            let ep_res = Endpoint::from_dir_entry(endpoint_dir?, &env);
            let ep = match ep_res {
                Ok(ep) => ep,
                Err(e) => match e.downcast::<std::io::Error>() {
                    Ok(e) => {
                        // A parallel task could delete an endpoint while we have just scanned the directory
                        if e.kind() == std::io::ErrorKind::NotFound {
                            continue;
                        } else {
                            Err(e)?
                        }
                    }
                    Err(e) => Err(e)?,
                },
            };
            endpoints.insert(ep.endpoint_id.clone(), Arc::new(ep));
        }

        Ok(ComputeControlPlane {
            base_port: 55431,
            endpoints,
            env,
        })
    }

    fn get_port(&mut self) -> u16 {
        1 + self
            .endpoints
            .values()
            .map(|ep| std::cmp::max(ep.pg_address.port(), ep.http_address.port()))
            .max()
            .unwrap_or(self.base_port)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_endpoint(
        &mut self,
        endpoint_id: &str,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        pg_port: Option<u16>,
        http_port: Option<u16>,
        pg_version: u32,
        mode: ComputeMode,
        skip_pg_catalog_updates: bool,
    ) -> Result<Arc<Endpoint>> {
        let pg_port = pg_port.unwrap_or_else(|| self.get_port());
        let http_port = http_port.unwrap_or_else(|| self.get_port() + 1);
        let ep = Arc::new(Endpoint {
            endpoint_id: endpoint_id.to_owned(),
            pg_address: SocketAddr::new("127.0.0.1".parse().unwrap(), pg_port),
            http_address: SocketAddr::new("127.0.0.1".parse().unwrap(), http_port),
            env: self.env.clone(),
            timeline_id,
            mode,
            tenant_id,
            pg_version,
            // We don't setup roles and databases in the spec locally, so we don't need to
            // do catalog updates. Catalog updates also include check availability
            // data creation. Yet, we have tests that check that size and db dump
            // before and after start are the same. So, skip catalog updates,
            // with this we basically test a case of waking up an idle compute, where
            // we also skip catalog updates in the cloud.
            skip_pg_catalog_updates,
            features: vec![],
        });

        ep.create_endpoint_dir()?;
        std::fs::write(
            ep.endpoint_path().join("endpoint.json"),
            serde_json::to_string_pretty(&EndpointConf {
                endpoint_id: endpoint_id.to_string(),
                tenant_id,
                timeline_id,
                mode,
                http_port,
                pg_port,
                pg_version,
                skip_pg_catalog_updates,
                features: vec![],
            })?,
        )?;
        std::fs::write(
            ep.endpoint_path().join("postgresql.conf"),
            ep.setup_pg_conf()?.to_string(),
        )?;

        self.endpoints
            .insert(ep.endpoint_id.clone(), Arc::clone(&ep));

        Ok(ep)
    }

    pub fn check_conflicting_endpoints(
        &self,
        mode: ComputeMode,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<()> {
        if matches!(mode, ComputeMode::Primary) {
            // this check is not complete, as you could have a concurrent attempt at
            // creating another primary, both reading the state before checking it here,
            // but it's better than nothing.
            let mut duplicates = self.endpoints.iter().filter(|(_k, v)| {
                v.tenant_id == tenant_id
                    && v.timeline_id == timeline_id
                    && v.mode == mode
                    && v.status() != EndpointStatus::Stopped
            });

            if let Some((key, _)) = duplicates.next() {
                bail!("attempting to create a duplicate primary endpoint on tenant {tenant_id}, timeline {timeline_id}: endpoint {key:?} exists already. please don't do this, it is not supported.");
            }
        }
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Endpoint {
    /// used as the directory name
    endpoint_id: String,
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub mode: ComputeMode,

    // port and address of the Postgres server and `compute_ctl`'s HTTP API
    pub pg_address: SocketAddr,
    pub http_address: SocketAddr,

    // postgres major version in the format: 14, 15, etc.
    pg_version: u32,

    // These are not part of the endpoint as such, but the environment
    // the endpoint runs in.
    pub env: LocalEnv,

    // Optimizations
    skip_pg_catalog_updates: bool,

    // Feature flags
    features: Vec<ComputeFeature>,
}

#[derive(PartialEq, Eq)]
pub enum EndpointStatus {
    Running,
    Stopped,
    Crashed,
    RunningNoPidfile,
}

impl std::fmt::Display for EndpointStatus {
    fn fmt(&self, writer: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s = match self {
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Crashed => "crashed",
            Self::RunningNoPidfile => "running, no pidfile",
        };
        write!(writer, "{}", s)
    }
}

impl Endpoint {
    fn from_dir_entry(entry: std::fs::DirEntry, env: &LocalEnv) -> Result<Endpoint> {
        if !entry.file_type()?.is_dir() {
            anyhow::bail!(
                "Endpoint::from_dir_entry failed: '{}' is not a directory",
                entry.path().display()
            );
        }

        // parse data directory name
        let fname = entry.file_name();
        let endpoint_id = fname.to_str().unwrap().to_string();

        // Read the endpoint.json file
        let conf: EndpointConf =
            serde_json::from_slice(&std::fs::read(entry.path().join("endpoint.json"))?)?;

        Ok(Endpoint {
            pg_address: SocketAddr::new("127.0.0.1".parse().unwrap(), conf.pg_port),
            http_address: SocketAddr::new("127.0.0.1".parse().unwrap(), conf.http_port),
            endpoint_id,
            env: env.clone(),
            timeline_id: conf.timeline_id,
            mode: conf.mode,
            tenant_id: conf.tenant_id,
            pg_version: conf.pg_version,
            skip_pg_catalog_updates: conf.skip_pg_catalog_updates,
            features: conf.features,
        })
    }

    fn create_endpoint_dir(&self) -> Result<()> {
        std::fs::create_dir_all(self.endpoint_path()).with_context(|| {
            format!(
                "could not create endpoint directory {}",
                self.endpoint_path().display()
            )
        })
    }

    // Generate postgresql.conf with default configuration
    fn setup_pg_conf(&self) -> Result<PostgresConf> {
        let mut conf = PostgresConf::new();
        conf.append("max_wal_senders", "10");
        conf.append("wal_log_hints", "off");
        conf.append("max_replication_slots", "10");
        conf.append("hot_standby", "on");
        // Set to 1MB to both exercise getPage requests/LFC, and still have enough room for
        // Postgres to operate. Everything smaller might be not enough for Postgres under load,
        // and can cause errors like 'no unpinned buffers available', see
        // <https://github.com/neondatabase/neon/issues/9956>
        conf.append("shared_buffers", "1MB");
        // Postgres defaults to effective_io_concurrency=1, which does not exercise the pageserver's
        // batching logic.  Set this to 2 so that we exercise the code a bit without letting
        // individual tests do a lot of concurrent work on underpowered test machines
        conf.append("effective_io_concurrency", "2");
        conf.append("fsync", "off");
        conf.append("max_connections", "100");
        conf.append("wal_level", "logical");
        // wal_sender_timeout is the maximum time to wait for WAL replication.
        // It also defines how often the walreciever will send a feedback message to the wal sender.
        conf.append("wal_sender_timeout", "5s");
        conf.append("listen_addresses", &self.pg_address.ip().to_string());
        conf.append("port", &self.pg_address.port().to_string());
        conf.append("wal_keep_size", "0");
        // walproposer panics when basebackup is invalid, it is pointless to restart in this case.
        conf.append("restart_after_crash", "off");

        // Load the 'neon' extension
        conf.append("shared_preload_libraries", "neon");

        conf.append_line("");
        // Replication-related configurations, such as WAL sending
        match &self.mode {
            ComputeMode::Primary => {
                // Configure backpressure
                // - Replication write lag depends on how fast the walreceiver can process incoming WAL.
                //   This lag determines latency of get_page_at_lsn. Speed of applying WAL is about 10MB/sec,
                //   so to avoid expiration of 1 minute timeout, this lag should not be larger than 600MB.
                //   Actually latency should be much smaller (better if < 1sec). But we assume that recently
                //   updates pages are not requested from pageserver.
                // - Replication flush lag depends on speed of persisting data by checkpointer (creation of
                //   delta/image layers) and advancing disk_consistent_lsn. Safekeepers are able to
                //   remove/archive WAL only beyond disk_consistent_lsn. Too large a lag can cause long
                //   recovery time (in case of pageserver crash) and disk space overflow at safekeepers.
                // - Replication apply lag depends on speed of uploading changes to S3 by uploader thread.
                //   To be able to restore database in case of pageserver node crash, safekeeper should not
                //   remove WAL beyond this point. Too large lag can cause space exhaustion in safekeepers
                //   (if they are not able to upload WAL to S3).
                conf.append("max_replication_write_lag", "15MB");
                conf.append("max_replication_flush_lag", "10GB");

                if !self.env.safekeepers.is_empty() {
                    // Configure Postgres to connect to the safekeepers
                    conf.append("synchronous_standby_names", "walproposer");

                    let safekeepers = self
                        .env
                        .safekeepers
                        .iter()
                        .map(|sk| format!("localhost:{}", sk.get_compute_port()))
                        .collect::<Vec<String>>()
                        .join(",");
                    conf.append("neon.safekeepers", &safekeepers);
                } else {
                    // We only use setup without safekeepers for tests,
                    // and don't care about data durability on pageserver,
                    // so set more relaxed synchronous_commit.
                    conf.append("synchronous_commit", "remote_write");

                    // Configure the node to stream WAL directly to the pageserver
                    // This isn't really a supported configuration, but can be useful for
                    // testing.
                    conf.append("synchronous_standby_names", "pageserver");
                }
            }
            ComputeMode::Static(lsn) => {
                conf.append("recovery_target_lsn", &lsn.to_string());
            }
            ComputeMode::Replica => {
                assert!(!self.env.safekeepers.is_empty());

                // TODO: use future host field from safekeeper spec
                // Pass the list of safekeepers to the replica so that it can connect to any of them,
                // whichever is available.
                let sk_ports = self
                    .env
                    .safekeepers
                    .iter()
                    .map(|x| x.get_compute_port().to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let sk_hosts = vec!["localhost"; self.env.safekeepers.len()].join(",");

                let connstr = format!(
                    "host={} port={} options='-c timeline_id={} tenant_id={}' application_name=replica replication=true",
                    sk_hosts,
                    sk_ports,
                    &self.timeline_id.to_string(),
                    &self.tenant_id.to_string(),
                );

                let slot_name = format!("repl_{}_", self.timeline_id);
                conf.append("primary_conninfo", connstr.as_str());
                conf.append("primary_slot_name", slot_name.as_str());
                conf.append("hot_standby", "on");
                // prefetching of blocks referenced in WAL doesn't make sense for us
                // Neon hot standby ignores pages that are not in the shared_buffers
                if self.pg_version >= 15 {
                    conf.append("recovery_prefetch", "off");
                }
            }
        }

        Ok(conf)
    }

    pub fn endpoint_path(&self) -> PathBuf {
        self.env.endpoints_path().join(&self.endpoint_id)
    }

    pub fn pgdata(&self) -> PathBuf {
        self.endpoint_path().join("pgdata")
    }

    pub fn status(&self) -> EndpointStatus {
        let timeout = Duration::from_millis(300);
        let has_pidfile = self.pgdata().join("postmaster.pid").exists();
        let can_connect = TcpStream::connect_timeout(&self.pg_address, timeout).is_ok();

        match (has_pidfile, can_connect) {
            (true, true) => EndpointStatus::Running,
            (false, false) => EndpointStatus::Stopped,
            (true, false) => EndpointStatus::Crashed,
            (false, true) => EndpointStatus::RunningNoPidfile,
        }
    }

    fn pg_ctl(&self, args: &[&str], auth_token: &Option<String>) -> Result<()> {
        let pg_ctl_path = self.env.pg_bin_dir(self.pg_version)?.join("pg_ctl");
        let mut cmd = Command::new(&pg_ctl_path);
        cmd.args(
            [
                &[
                    "-D",
                    self.pgdata().to_str().unwrap(),
                    "-w", //wait till pg_ctl actually does what was asked
                ],
                args,
            ]
            .concat(),
        )
        .env_clear()
        .env(
            "LD_LIBRARY_PATH",
            self.env.pg_lib_dir(self.pg_version)?.to_str().unwrap(),
        )
        .env(
            "DYLD_LIBRARY_PATH",
            self.env.pg_lib_dir(self.pg_version)?.to_str().unwrap(),
        );

        // Pass authentication token used for the connections to pageserver and safekeepers
        if let Some(token) = auth_token {
            cmd.env("NEON_AUTH_TOKEN", token);
        }

        let pg_ctl = cmd
            .output()
            .context(format!("{} failed", pg_ctl_path.display()))?;
        if !pg_ctl.status.success() {
            anyhow::bail!(
                "pg_ctl failed, exit code: {}, stdout: {}, stderr: {}",
                pg_ctl.status,
                String::from_utf8_lossy(&pg_ctl.stdout),
                String::from_utf8_lossy(&pg_ctl.stderr),
            );
        }

        Ok(())
    }

    fn wait_for_compute_ctl_to_exit(&self, send_sigterm: bool) -> Result<()> {
        // TODO use background_process::stop_process instead: https://github.com/neondatabase/neon/pull/6482
        let pidfile_path = self.endpoint_path().join("compute_ctl.pid");
        let pid: u32 = std::fs::read_to_string(pidfile_path)?.parse()?;
        let pid = nix::unistd::Pid::from_raw(pid as i32);
        if send_sigterm {
            kill(pid, Signal::SIGTERM).ok();
        }
        crate::background_process::wait_until_stopped("compute_ctl", pid)?;
        Ok(())
    }

    fn read_postgresql_conf(&self) -> Result<String> {
        // Slurp the endpoints/<endpoint id>/postgresql.conf file into
        // memory. We will include it in the spec file that we pass to
        // `compute_ctl`, and `compute_ctl` will write it to the postgresql.conf
        // in the data directory.
        let postgresql_conf_path = self.endpoint_path().join("postgresql.conf");
        match std::fs::read(&postgresql_conf_path) {
            Ok(content) => Ok(String::from_utf8(content)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok("".to_string()),
            Err(e) => Err(anyhow::Error::new(e).context(format!(
                "failed to read config file in {}",
                postgresql_conf_path.to_str().unwrap()
            ))),
        }
    }

    fn build_pageserver_connstr(pageservers: &[(Host, u16)]) -> String {
        pageservers
            .iter()
            .map(|(host, port)| format!("postgresql://no_user@{host}:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Map safekeepers ids to the actual connection strings.
    fn build_safekeepers_connstrs(&self, sk_ids: Vec<NodeId>) -> Result<Vec<String>> {
        let mut safekeeper_connstrings = Vec::new();
        if self.mode == ComputeMode::Primary {
            for sk_id in sk_ids {
                let sk = self
                    .env
                    .safekeepers
                    .iter()
                    .find(|node| node.id == sk_id)
                    .ok_or_else(|| anyhow!("safekeeper {sk_id} does not exist"))?;
                safekeeper_connstrings.push(format!("127.0.0.1:{}", sk.get_compute_port()));
            }
        }
        Ok(safekeeper_connstrings)
    }

    pub async fn start(
        &self,
        auth_token: &Option<String>,
        safekeepers: Vec<NodeId>,
        pageservers: Vec<(Host, u16)>,
        remote_ext_config: Option<&String>,
        shard_stripe_size: usize,
        create_test_user: bool,
    ) -> Result<()> {
        if self.status() == EndpointStatus::Running {
            anyhow::bail!("The endpoint is already running");
        }

        let postgresql_conf = self.read_postgresql_conf()?;

        // We always start the compute node from scratch, so if the Postgres
        // data dir exists from a previous launch, remove it first.
        if self.pgdata().exists() {
            std::fs::remove_dir_all(self.pgdata())?;
        }

        let pageserver_connstring = Self::build_pageserver_connstr(&pageservers);
        assert!(!pageserver_connstring.is_empty());

        let safekeeper_connstrings = self.build_safekeepers_connstrs(safekeepers)?;

        // check for file remote_extensions_spec.json
        // if it is present, read it and pass to compute_ctl
        let remote_extensions_spec_path = self.endpoint_path().join("remote_extensions_spec.json");
        let remote_extensions_spec = std::fs::File::open(remote_extensions_spec_path);
        let remote_extensions: Option<RemoteExtSpec>;

        if let Ok(spec_file) = remote_extensions_spec {
            remote_extensions = serde_json::from_reader(spec_file).ok();
        } else {
            remote_extensions = None;
        };

        // Create spec file
        let spec = ComputeSpec {
            skip_pg_catalog_updates: self.skip_pg_catalog_updates,
            format_version: 1.0,
            operation_uuid: None,
            features: self.features.clone(),
            swap_size_bytes: None,
            disk_quota_bytes: None,
            disable_lfc_resizing: None,
            cluster: Cluster {
                cluster_id: None, // project ID: not used
                name: None,       // project name: not used
                state: None,
                roles: if create_test_user {
                    vec![Role {
                        name: PgIdent::from_str("test").unwrap(),
                        encrypted_password: None,
                        options: None,
                    }]
                } else {
                    Vec::new()
                },
                databases: if create_test_user {
                    vec![Database {
                        name: PgIdent::from_str("neondb").unwrap(),
                        owner: PgIdent::from_str("test").unwrap(),
                        options: None,
                        restrict_conn: false,
                        invalid: false,
                    }]
                } else {
                    Vec::new()
                },
                settings: None,
                postgresql_conf: Some(postgresql_conf),
            },
            delta_operations: None,
            tenant_id: Some(self.tenant_id),
            timeline_id: Some(self.timeline_id),
            mode: self.mode,
            pageserver_connstring: Some(pageserver_connstring),
            safekeeper_connstrings,
            storage_auth_token: auth_token.clone(),
            remote_extensions,
            pgbouncer_settings: None,
            shard_stripe_size: Some(shard_stripe_size),
            local_proxy_config: None,
            reconfigure_concurrency: 1,
        };
        let spec_path = self.endpoint_path().join("spec.json");
        std::fs::write(spec_path, serde_json::to_string_pretty(&spec)?)?;

        // Open log file. We'll redirect the stdout and stderr of `compute_ctl` to it.
        let logfile = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.endpoint_path().join("compute.log"))?;

        // Launch compute_ctl
        let conn_str = self.connstr("cloud_admin", "postgres");
        println!("Starting postgres node at '{}'", conn_str);
        if create_test_user {
            let conn_str = self.connstr("test", "neondb");
            println!("Also at '{}'", conn_str);
        }
        let mut cmd = Command::new(self.env.neon_distrib_dir.join("compute_ctl"));
        cmd.args(["--http-port", &self.http_address.port().to_string()])
            .args(["--pgdata", self.pgdata().to_str().unwrap()])
            .args(["--connstr", &conn_str])
            .args([
                "--spec-path",
                self.endpoint_path().join("spec.json").to_str().unwrap(),
            ])
            .args([
                "--pgbin",
                self.env
                    .pg_bin_dir(self.pg_version)?
                    .join("postgres")
                    .to_str()
                    .unwrap(),
            ])
            .stdin(std::process::Stdio::null())
            .stderr(logfile.try_clone()?)
            .stdout(logfile);

        if let Some(remote_ext_config) = remote_ext_config {
            cmd.args(["--remote-ext-config", remote_ext_config]);
        }

        let child = cmd.spawn()?;
        // set up a scopeguard to kill & wait for the child in case we panic or bail below
        let child = scopeguard::guard(child, |mut child| {
            println!("SIGKILL & wait the started process");
            (|| {
                // TODO: use another signal that can be caught by the child so it can clean up any children it spawned
                child.kill().context("SIGKILL child")?;
                child.wait().context("wait() for child process")?;
                anyhow::Ok(())
            })()
            .with_context(|| format!("scopeguard kill&wait child {child:?}"))
            .unwrap();
        });

        // Write down the pid so we can wait for it when we want to stop
        // TODO use background_process::start_process instead: https://github.com/neondatabase/neon/pull/6482
        let pid = child.id();
        let pidfile_path = self.endpoint_path().join("compute_ctl.pid");
        std::fs::write(pidfile_path, pid.to_string())?;

        // Wait for it to start
        let mut attempt = 0;
        const ATTEMPT_INTERVAL: Duration = Duration::from_millis(100);
        const MAX_ATTEMPTS: u32 = 10 * 90; // Wait up to 1.5 min
        loop {
            attempt += 1;
            match self.get_status().await {
                Ok(state) => {
                    match state.status {
                        ComputeStatus::Init => {
                            if attempt == MAX_ATTEMPTS {
                                bail!("compute startup timed out; still in Init state");
                            }
                            // keep retrying
                        }
                        ComputeStatus::Running => {
                            // All good!
                            break;
                        }
                        ComputeStatus::Failed => {
                            bail!(
                                "compute startup failed: {}",
                                state
                                    .error
                                    .as_deref()
                                    .unwrap_or("<no error from compute_ctl>")
                            );
                        }
                        ComputeStatus::Empty
                        | ComputeStatus::ConfigurationPending
                        | ComputeStatus::Configuration
                        | ComputeStatus::TerminationPending
                        | ComputeStatus::Terminated => {
                            bail!("unexpected compute status: {:?}", state.status)
                        }
                    }
                }
                Err(e) => {
                    if attempt == MAX_ATTEMPTS {
                        return Err(e).context("timed out waiting to connect to compute_ctl HTTP");
                    }
                }
            }
            tokio::time::sleep(ATTEMPT_INTERVAL).await;
        }

        // disarm the scopeguard, let the child outlive this function (and neon_local invoction)
        drop(scopeguard::ScopeGuard::into_inner(child));

        Ok(())
    }

    // Call the /status HTTP API
    pub async fn get_status(&self) -> Result<ComputeStatusResponse> {
        let client = reqwest::Client::new();

        let response = client
            .request(
                reqwest::Method::GET,
                format!(
                    "http://{}:{}/status",
                    self.http_address.ip(),
                    self.http_address.port()
                ),
            )
            .send()
            .await?;

        // Interpret the response
        let status = response.status();
        if !(status.is_client_error() || status.is_server_error()) {
            Ok(response.json().await?)
        } else {
            // reqwest does not export its error construction utility functions, so let's craft the message ourselves
            let url = response.url().to_owned();
            let msg = match response.text().await {
                Ok(err_body) => format!("Error: {}", err_body),
                Err(_) => format!("Http error ({}) at {}.", status.as_u16(), url),
            };
            Err(anyhow::anyhow!(msg))
        }
    }

    pub async fn reconfigure(
        &self,
        mut pageservers: Vec<(Host, u16)>,
        stripe_size: Option<ShardStripeSize>,
        safekeepers: Option<Vec<NodeId>>,
    ) -> Result<()> {
        let mut spec: ComputeSpec = {
            let spec_path = self.endpoint_path().join("spec.json");
            let file = std::fs::File::open(spec_path)?;
            serde_json::from_reader(file)?
        };

        let postgresql_conf = self.read_postgresql_conf()?;
        spec.cluster.postgresql_conf = Some(postgresql_conf);

        // If we weren't given explicit pageservers, query the storage controller
        if pageservers.is_empty() {
            let storage_controller = StorageController::from_env(&self.env);
            let locate_result = storage_controller.tenant_locate(self.tenant_id).await?;
            pageservers = locate_result
                .shards
                .into_iter()
                .map(|shard| {
                    (
                        Host::parse(&shard.listen_pg_addr)
                            .expect("Storage controller reported bad hostname"),
                        shard.listen_pg_port,
                    )
                })
                .collect::<Vec<_>>();
        }

        let pageserver_connstr = Self::build_pageserver_connstr(&pageservers);
        assert!(!pageserver_connstr.is_empty());
        spec.pageserver_connstring = Some(pageserver_connstr);
        if stripe_size.is_some() {
            spec.shard_stripe_size = stripe_size.map(|s| s.0 as usize);
        }

        // If safekeepers are not specified, don't change them.
        if let Some(safekeepers) = safekeepers {
            let safekeeper_connstrings = self.build_safekeepers_connstrs(safekeepers)?;
            spec.safekeeper_connstrings = safekeeper_connstrings;
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .unwrap();
        let response = client
            .post(format!(
                "http://{}:{}/configure",
                self.http_address.ip(),
                self.http_address.port()
            ))
            .header(CONTENT_TYPE.as_str(), "application/json")
            .body(format!(
                "{{\"spec\":{}}}",
                serde_json::to_string_pretty(&spec)?
            ))
            .send()
            .await?;

        let status = response.status();
        if !(status.is_client_error() || status.is_server_error()) {
            Ok(())
        } else {
            let url = response.url().to_owned();
            let msg = match response.text().await {
                Ok(err_body) => format!("Error: {}", err_body),
                Err(_) => format!("Http error ({}) at {}.", status.as_u16(), url),
            };
            Err(anyhow::anyhow!(msg))
        }
    }

    pub fn stop(&self, mode: &str, destroy: bool) -> Result<()> {
        self.pg_ctl(&["-m", mode, "stop"], &None)?;

        // Also wait for the compute_ctl process to die. It might have some
        // cleanup work to do after postgres stops, like syncing safekeepers,
        // etc.
        //
        // If destroying or stop mode is immediate, send it SIGTERM before
        // waiting. Sometimes we do *not* want this cleanup: tests intentionally
        // do stop when majority of safekeepers is down, so sync-safekeepers
        // would hang otherwise. This could be a separate flag though.
        let send_sigterm = destroy || mode == "immediate";
        self.wait_for_compute_ctl_to_exit(send_sigterm)?;
        if destroy {
            println!(
                "Destroying postgres data directory '{}'",
                self.pgdata().to_str().unwrap()
            );
            std::fs::remove_dir_all(self.endpoint_path())?;
        }
        Ok(())
    }

    pub fn connstr(&self, user: &str, db_name: &str) -> String {
        format!(
            "postgresql://{}@{}:{}/{}",
            user,
            self.pg_address.ip(),
            self.pg_address.port(),
            db_name
        )
    }
}
