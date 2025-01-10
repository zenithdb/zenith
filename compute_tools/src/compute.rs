use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::iter::once;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use compute_api::spec::{Database, PgIdent, Role};
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use nix::unistd::Pid;
use postgres;
use postgres::error::SqlState;
use postgres::NoTls;
use tracing::{debug, error, info, instrument, warn};
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

use compute_api::privilege::Privilege;
use compute_api::responses::{ComputeMetrics, ComputeStatus};
use compute_api::spec::{ComputeFeature, ComputeMode, ComputeSpec, ExtVersion};
use utils::measured_stream::MeasuredReader;

use nix::sys::signal::{kill, Signal};
use remote_storage::{DownloadError, RemotePath};
use tokio::spawn;

use crate::installed_extensions::get_installed_extensions;
use crate::local_proxy;
use crate::pg_helpers::*;
use crate::spec::*;
use crate::spec_apply::ApplySpecPhase::{
    CreateAndAlterDatabases, CreateAndAlterRoles, CreateAvailabilityCheck, CreateSuperUser,
    DropInvalidDatabases, DropRoles, HandleNeonExtension, HandleOtherExtensions,
    RenameAndDeleteDatabases, RenameRoles, RunInEachDatabase,
};
use crate::spec_apply::PerDatabasePhase;
use crate::spec_apply::PerDatabasePhase::{
    ChangeSchemaPerms, DeleteDBRoleReferences, DropSubscriptionsForDeletedDatabases,
    HandleAnonExtension,
};
use crate::spec_apply::{apply_operations, MutableApplyContext, DB};
use crate::sync_sk::{check_if_synced, ping_safekeeper};
use crate::{config, extension_server};

pub static SYNC_SAFEKEEPERS_PID: AtomicU32 = AtomicU32::new(0);
pub static PG_PID: AtomicU32 = AtomicU32::new(0);

/// Compute node info shared across several `compute_ctl` threads.
pub struct ComputeNode {
    // Url type maintains proper escaping
    pub connstr: url::Url,
    // We connect to Postgres from many different places, so build configs once
    // and reuse them where needed.
    pub conn_conf: postgres::config::Config,
    pub tokio_conn_conf: tokio_postgres::config::Config,
    pub pgdata: String,
    pub pgbin: String,
    pub pgversion: String,
    /// We should only allow live re- / configuration of the compute node if
    /// it uses 'pull model', i.e. it can go to control-plane and fetch
    /// the latest configuration. Otherwise, there could be a case:
    /// - we start compute with some spec provided as argument
    /// - we push new spec and it does reconfiguration
    /// - but then something happens and compute pod / VM is destroyed,
    ///   so k8s controller starts it again with the **old** spec
    ///
    /// and the same for empty computes:
    /// - we started compute without any spec
    /// - we push spec and it does configuration
    /// - but then it is restarted without any spec again
    pub live_config_allowed: bool,
    /// The port that the compute's HTTP server listens on
    pub http_port: u16,
    /// Volatile part of the `ComputeNode`, which should be used under `Mutex`.
    /// To allow HTTP API server to serving status requests, while configuration
    /// is in progress, lock should be held only for short periods of time to do
    /// read/write, not the whole configuration process.
    pub state: Mutex<ComputeState>,
    /// `Condvar` to allow notifying waiters about state changes.
    pub state_changed: Condvar,
    /// the address of extension storage proxy gateway
    pub ext_remote_storage: Option<String>,
    // key: ext_archive_name, value: started download time, download_completed?
    pub ext_download_progress: RwLock<HashMap<String, (DateTime<Utc>, bool)>>,
    pub build_tag: String,
}

// store some metrics about download size that might impact startup time
#[derive(Clone, Debug)]
pub struct RemoteExtensionMetrics {
    num_ext_downloaded: u64,
    largest_ext_size: u64,
    total_ext_download_size: u64,
}

#[derive(Clone, Debug)]
pub struct ComputeState {
    pub start_time: DateTime<Utc>,
    pub status: ComputeStatus,
    /// Timestamp of the last Postgres activity. It could be `None` if
    /// compute wasn't used since start.
    pub last_active: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub pspec: Option<ParsedSpec>,
    pub metrics: ComputeMetrics,
}

impl ComputeState {
    pub fn new() -> Self {
        Self {
            start_time: Utc::now(),
            status: ComputeStatus::Empty,
            last_active: None,
            error: None,
            pspec: None,
            metrics: ComputeMetrics::default(),
        }
    }

    pub fn set_status(&mut self, status: ComputeStatus, state_changed: &Condvar) {
        let prev = self.status;
        info!("Changing compute status from {} to {}", prev, status);
        self.status = status;
        state_changed.notify_all();
    }

    pub fn set_failed_status(&mut self, err: anyhow::Error, state_changed: &Condvar) {
        self.error = Some(format!("{err:?}"));
        self.set_status(ComputeStatus::Failed, state_changed);
    }
}

impl Default for ComputeState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct ParsedSpec {
    pub spec: ComputeSpec,
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub pageserver_connstr: String,
    pub safekeeper_connstrings: Vec<String>,
    pub storage_auth_token: Option<String>,
}

impl TryFrom<ComputeSpec> for ParsedSpec {
    type Error = String;
    fn try_from(spec: ComputeSpec) -> Result<Self, String> {
        // Extract the options from the spec file that are needed to connect to
        // the storage system.
        //
        // For backwards-compatibility, the top-level fields in the spec file
        // may be empty. In that case, we need to dig them from the GUCs in the
        // cluster.settings field.
        let pageserver_connstr = spec
            .pageserver_connstring
            .clone()
            .or_else(|| spec.cluster.settings.find("neon.pageserver_connstring"))
            .ok_or("pageserver connstr should be provided")?;
        let safekeeper_connstrings = if spec.safekeeper_connstrings.is_empty() {
            if matches!(spec.mode, ComputeMode::Primary) {
                spec.cluster
                    .settings
                    .find("neon.safekeepers")
                    .ok_or("safekeeper connstrings should be provided")?
                    .split(',')
                    .map(|str| str.to_string())
                    .collect()
            } else {
                vec![]
            }
        } else {
            spec.safekeeper_connstrings.clone()
        };
        let storage_auth_token = spec.storage_auth_token.clone();
        let tenant_id: TenantId = if let Some(tenant_id) = spec.tenant_id {
            tenant_id
        } else {
            spec.cluster
                .settings
                .find("neon.tenant_id")
                .ok_or("tenant id should be provided")
                .map(|s| TenantId::from_str(&s))?
                .or(Err("invalid tenant id"))?
        };
        let timeline_id: TimelineId = if let Some(timeline_id) = spec.timeline_id {
            timeline_id
        } else {
            spec.cluster
                .settings
                .find("neon.timeline_id")
                .ok_or("timeline id should be provided")
                .map(|s| TimelineId::from_str(&s))?
                .or(Err("invalid timeline id"))?
        };

        Ok(ParsedSpec {
            spec,
            pageserver_connstr,
            safekeeper_connstrings,
            storage_auth_token,
            tenant_id,
            timeline_id,
        })
    }
}

/// If we are a VM, returns a [`Command`] that will run in the `neon-postgres`
/// cgroup. Otherwise returns the default `Command::new(cmd)`
///
/// This function should be used to start postgres, as it will start it in the
/// neon-postgres cgroup if we are a VM. This allows autoscaling to control
/// postgres' resource usage. The cgroup will exist in VMs because vm-builder
/// creates it during the sysinit phase of its inittab.
fn maybe_cgexec(cmd: &str) -> Command {
    // The cplane sets this env var for autoscaling computes.
    // use `var_os` so we don't have to worry about the variable being valid
    // unicode. Should never be an concern . . . but just in case
    if env::var_os("AUTOSCALING").is_some() {
        let mut command = Command::new("cgexec");
        command.args(["-g", "memory:neon-postgres"]);
        command.arg(cmd);
        command
    } else {
        Command::new(cmd)
    }
}

pub(crate) fn construct_superuser_query(spec: &ComputeSpec) -> String {
    let roles = spec
        .cluster
        .roles
        .iter()
        .map(|r| escape_literal(&r.name))
        .collect::<Vec<_>>();

    let dbs = spec
        .cluster
        .databases
        .iter()
        .map(|db| escape_literal(&db.name))
        .collect::<Vec<_>>();

    let roles_decl = if roles.is_empty() {
        String::from("roles text[] := NULL;")
    } else {
        format!(
            r#"
               roles text[] := ARRAY(SELECT rolname
                                     FROM pg_catalog.pg_roles
                                     WHERE rolname IN ({}));"#,
            roles.join(", ")
        )
    };

    let database_decl = if dbs.is_empty() {
        String::from("dbs text[] := NULL;")
    } else {
        format!(
            r#"
               dbs text[] := ARRAY(SELECT datname
                                   FROM pg_catalog.pg_database
                                   WHERE datname IN ({}));"#,
            dbs.join(", ")
        )
    };

    // ALL PRIVILEGES grants CREATE, CONNECT, and TEMPORARY on all databases
    // (see https://www.postgresql.org/docs/current/ddl-priv.html)
    let query = format!(
        r#"
            DO $$
                DECLARE
                    r text;
                    {}
                    {}
                BEGIN
                    IF NOT EXISTS (
                        SELECT FROM pg_catalog.pg_roles WHERE rolname = 'neon_superuser')
                    THEN
                        CREATE ROLE neon_superuser CREATEDB CREATEROLE NOLOGIN REPLICATION BYPASSRLS IN ROLE pg_read_all_data, pg_write_all_data;
                        IF array_length(roles, 1) IS NOT NULL THEN
                            EXECUTE format('GRANT neon_superuser TO %s',
                                           array_to_string(ARRAY(SELECT quote_ident(x) FROM unnest(roles) as x), ', '));
                            FOREACH r IN ARRAY roles LOOP
                                EXECUTE format('ALTER ROLE %s CREATEROLE CREATEDB', quote_ident(r));
                            END LOOP;
                        END IF;
                        IF array_length(dbs, 1) IS NOT NULL THEN
                            EXECUTE format('GRANT ALL PRIVILEGES ON DATABASE %s TO neon_superuser',
                                           array_to_string(ARRAY(SELECT quote_ident(x) FROM unnest(dbs) as x), ', '));
                        END IF;
                    END IF;
                END
            $$;"#,
        roles_decl, database_decl,
    );

    query
}

impl ComputeNode {
    /// Check that compute node has corresponding feature enabled.
    pub fn has_feature(&self, feature: ComputeFeature) -> bool {
        let state = self.state.lock().unwrap();

        if let Some(s) = state.pspec.as_ref() {
            s.spec.features.contains(&feature)
        } else {
            false
        }
    }

    pub fn set_status(&self, status: ComputeStatus) {
        let mut state = self.state.lock().unwrap();
        state.set_status(status, &self.state_changed);
    }

    pub fn set_failed_status(&self, err: anyhow::Error) {
        let mut state = self.state.lock().unwrap();
        state.set_failed_status(err, &self.state_changed);
    }

    pub fn get_status(&self) -> ComputeStatus {
        self.state.lock().unwrap().status
    }

    // Remove `pgdata` directory and create it again with right permissions.
    fn create_pgdata(&self) -> Result<()> {
        // Ignore removal error, likely it is a 'No such file or directory (os error 2)'.
        // If it is something different then create_dir() will error out anyway.
        let _ok = fs::remove_dir_all(&self.pgdata);
        fs::create_dir(&self.pgdata)?;
        fs::set_permissions(&self.pgdata, fs::Permissions::from_mode(0o700))?;

        Ok(())
    }

    // Get basebackup from the libpq connection to pageserver using `connstr` and
    // unarchive it to `pgdata` directory overriding all its previous content.
    #[instrument(skip_all, fields(%lsn))]
    fn try_get_basebackup(&self, compute_state: &ComputeState, lsn: Lsn) -> Result<()> {
        let spec = compute_state.pspec.as_ref().expect("spec must be set");
        let start_time = Instant::now();

        let shard0_connstr = spec.pageserver_connstr.split(',').next().unwrap();
        let mut config = postgres::Config::from_str(shard0_connstr)?;

        // Use the storage auth token from the config file, if given.
        // Note: this overrides any password set in the connection string.
        if let Some(storage_auth_token) = &spec.storage_auth_token {
            info!("Got storage auth token from spec file");
            config.password(storage_auth_token);
        } else {
            info!("Storage auth token not set");
        }

        // Connect to pageserver
        let mut client = config.connect(NoTls)?;
        let pageserver_connect_micros = start_time.elapsed().as_micros() as u64;

        let basebackup_cmd = match lsn {
            Lsn(0) => {
                if spec.spec.mode != ComputeMode::Primary {
                    format!(
                        "basebackup {} {} --gzip --replica",
                        spec.tenant_id, spec.timeline_id
                    )
                } else {
                    format!("basebackup {} {} --gzip", spec.tenant_id, spec.timeline_id)
                }
            }
            _ => {
                if spec.spec.mode != ComputeMode::Primary {
                    format!(
                        "basebackup {} {} {} --gzip --replica",
                        spec.tenant_id, spec.timeline_id, lsn
                    )
                } else {
                    format!(
                        "basebackup {} {} {} --gzip",
                        spec.tenant_id, spec.timeline_id, lsn
                    )
                }
            }
        };

        let copyreader = client.copy_out(basebackup_cmd.as_str())?;
        let mut measured_reader = MeasuredReader::new(copyreader);
        let mut bufreader = std::io::BufReader::new(&mut measured_reader);

        // Read the archive directly from the `CopyOutReader`
        //
        // Set `ignore_zeros` so that unpack() reads all the Copy data and
        // doesn't stop at the end-of-archive marker. Otherwise, if the server
        // sends an Error after finishing the tarball, we will not notice it.
        let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(&mut bufreader));
        ar.set_ignore_zeros(true);
        ar.unpack(&self.pgdata)?;

        // Report metrics
        let mut state = self.state.lock().unwrap();
        state.metrics.pageserver_connect_micros = pageserver_connect_micros;
        state.metrics.basebackup_bytes = measured_reader.get_byte_count() as u64;
        state.metrics.basebackup_ms = start_time.elapsed().as_millis() as u64;
        Ok(())
    }

    // Gets the basebackup in a retry loop
    #[instrument(skip_all, fields(%lsn))]
    pub fn get_basebackup(&self, compute_state: &ComputeState, lsn: Lsn) -> Result<()> {
        let mut retry_period_ms = 500.0;
        let mut attempts = 0;
        const DEFAULT_ATTEMPTS: u16 = 10;
        #[cfg(feature = "testing")]
        let max_attempts = if let Ok(v) = env::var("NEON_COMPUTE_TESTING_BASEBACKUP_RETRIES") {
            u16::from_str(&v).unwrap()
        } else {
            DEFAULT_ATTEMPTS
        };
        #[cfg(not(feature = "testing"))]
        let max_attempts = DEFAULT_ATTEMPTS;
        loop {
            let result = self.try_get_basebackup(compute_state, lsn);
            match result {
                Ok(_) => {
                    return result;
                }
                Err(ref e) if attempts < max_attempts => {
                    warn!(
                        "Failed to get basebackup: {} (attempt {}/{})",
                        e, attempts, max_attempts
                    );
                    std::thread::sleep(std::time::Duration::from_millis(retry_period_ms as u64));
                    retry_period_ms *= 1.5;
                }
                Err(_) => {
                    return result;
                }
            }
            attempts += 1;
        }
    }

    pub async fn check_safekeepers_synced_async(
        &self,
        compute_state: &ComputeState,
    ) -> Result<Option<Lsn>> {
        // Construct a connection config for each safekeeper
        let pspec: ParsedSpec = compute_state
            .pspec
            .as_ref()
            .expect("spec must be set")
            .clone();
        let sk_connstrs: Vec<String> = pspec.safekeeper_connstrings.clone();
        let sk_configs = sk_connstrs.into_iter().map(|connstr| {
            // Format connstr
            let id = connstr.clone();
            let connstr = format!("postgresql://no_user@{}", connstr);
            let options = format!(
                "-c timeline_id={} tenant_id={}",
                pspec.timeline_id, pspec.tenant_id
            );

            // Construct client
            let mut config = tokio_postgres::Config::from_str(&connstr).unwrap();
            config.options(&options);
            if let Some(storage_auth_token) = pspec.storage_auth_token.clone() {
                config.password(storage_auth_token);
            }

            (id, config)
        });

        // Create task set to query all safekeepers
        let mut tasks = FuturesUnordered::new();
        let quorum = sk_configs.len() / 2 + 1;
        for (id, config) in sk_configs {
            let timeout = tokio::time::Duration::from_millis(100);
            let task = tokio::time::timeout(timeout, ping_safekeeper(id, config));
            tasks.push(tokio::spawn(task));
        }

        // Get a quorum of responses or errors
        let mut responses = Vec::new();
        let mut join_errors = Vec::new();
        let mut task_errors = Vec::new();
        let mut timeout_errors = Vec::new();
        while let Some(response) = tasks.next().await {
            match response {
                Ok(Ok(Ok(r))) => responses.push(r),
                Ok(Ok(Err(e))) => task_errors.push(e),
                Ok(Err(e)) => timeout_errors.push(e),
                Err(e) => join_errors.push(e),
            };
            if responses.len() >= quorum {
                break;
            }
            if join_errors.len() + task_errors.len() + timeout_errors.len() >= quorum {
                break;
            }
        }

        // In case of error, log and fail the check, but don't crash.
        // We're playing it safe because these errors could be transient
        // and we don't yet retry. Also being careful here allows us to
        // be backwards compatible with safekeepers that don't have the
        // TIMELINE_STATUS API yet.
        if responses.len() < quorum {
            error!(
                "failed sync safekeepers check {:?} {:?} {:?}",
                join_errors, task_errors, timeout_errors
            );
            return Ok(None);
        }

        Ok(check_if_synced(responses))
    }

    // Fast path for sync_safekeepers. If they're already synced we get the lsn
    // in one roundtrip. If not, we should do a full sync_safekeepers.
    pub fn check_safekeepers_synced(&self, compute_state: &ComputeState) -> Result<Option<Lsn>> {
        let start_time = Utc::now();

        // Run actual work with new tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create rt");
        let result = rt.block_on(self.check_safekeepers_synced_async(compute_state));

        // Record runtime
        self.state.lock().unwrap().metrics.sync_sk_check_ms = Utc::now()
            .signed_duration_since(start_time)
            .to_std()
            .unwrap()
            .as_millis() as u64;
        result
    }

    // Run `postgres` in a special mode with `--sync-safekeepers` argument
    // and return the reported LSN back to the caller.
    #[instrument(skip_all)]
    pub fn sync_safekeepers(&self, storage_auth_token: Option<String>) -> Result<Lsn> {
        let start_time = Utc::now();

        let mut sync_handle = maybe_cgexec(&self.pgbin)
            .args(["--sync-safekeepers"])
            .env("PGDATA", &self.pgdata) // we cannot use -D in this mode
            .envs(if let Some(storage_auth_token) = &storage_auth_token {
                vec![("NEON_AUTH_TOKEN", storage_auth_token)]
            } else {
                vec![]
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("postgres --sync-safekeepers failed to start");
        SYNC_SAFEKEEPERS_PID.store(sync_handle.id(), Ordering::SeqCst);

        // `postgres --sync-safekeepers` will print all log output to stderr and
        // final LSN to stdout. So we leave stdout to collect LSN, while stderr logs
        // will be collected in a child thread.
        let stderr = sync_handle
            .stderr
            .take()
            .expect("stderr should be captured");
        let logs_handle = handle_postgres_logs(stderr);

        let sync_output = sync_handle
            .wait_with_output()
            .expect("postgres --sync-safekeepers failed");
        SYNC_SAFEKEEPERS_PID.store(0, Ordering::SeqCst);

        // Process has exited, so we can join the logs thread.
        let _ = logs_handle
            .join()
            .map_err(|e| tracing::error!("log thread panicked: {:?}", e));

        if !sync_output.status.success() {
            anyhow::bail!(
                "postgres --sync-safekeepers exited with non-zero status: {}. stdout: {}",
                sync_output.status,
                String::from_utf8(sync_output.stdout)
                    .expect("postgres --sync-safekeepers exited, and stdout is not utf-8"),
            );
        }

        self.state.lock().unwrap().metrics.sync_safekeepers_ms = Utc::now()
            .signed_duration_since(start_time)
            .to_std()
            .unwrap()
            .as_millis() as u64;

        let lsn = Lsn::from_str(String::from_utf8(sync_output.stdout)?.trim())?;

        Ok(lsn)
    }

    /// Do all the preparations like PGDATA directory creation, configuration,
    /// safekeepers sync, basebackup, etc.
    #[instrument(skip_all)]
    pub fn prepare_pgdata(&self, compute_state: &ComputeState) -> Result<()> {
        let pspec = compute_state.pspec.as_ref().expect("spec must be set");
        let spec = &pspec.spec;
        let pgdata_path = Path::new(&self.pgdata);

        // Remove/create an empty pgdata directory and put configuration there.
        self.create_pgdata()?;
        config::write_postgres_conf(
            &pgdata_path.join("postgresql.conf"),
            &pspec.spec,
            self.http_port,
        )?;

        // Syncing safekeepers is only safe with primary nodes: if a primary
        // is already connected it will be kicked out, so a secondary (standby)
        // cannot sync safekeepers.
        let lsn = match spec.mode {
            ComputeMode::Primary => {
                info!("checking if safekeepers are synced");
                let lsn = if let Ok(Some(lsn)) = self.check_safekeepers_synced(compute_state) {
                    lsn
                } else {
                    info!("starting safekeepers syncing");
                    self.sync_safekeepers(pspec.storage_auth_token.clone())
                        .with_context(|| "failed to sync safekeepers")?
                };
                info!("safekeepers synced at LSN {}", lsn);
                lsn
            }
            ComputeMode::Static(lsn) => {
                info!("Starting read-only node at static LSN {}", lsn);
                lsn
            }
            ComputeMode::Replica => {
                info!("Initializing standby from latest Pageserver LSN");
                Lsn(0)
            }
        };

        info!(
            "getting basebackup@{} from pageserver {}",
            lsn, &pspec.pageserver_connstr
        );
        self.get_basebackup(compute_state, lsn).with_context(|| {
            format!(
                "failed to get basebackup@{} from pageserver {}",
                lsn, &pspec.pageserver_connstr
            )
        })?;

        // Update pg_hba.conf received with basebackup.
        update_pg_hba(pgdata_path)?;

        // Place pg_dynshmem under /dev/shm. This allows us to use
        // 'dynamic_shared_memory_type = mmap' so that the files are placed in
        // /dev/shm, similar to how 'dynamic_shared_memory_type = posix' works.
        //
        // Why on earth don't we just stick to the 'posix' default, you might
        // ask.  It turns out that making large allocations with 'posix' doesn't
        // work very well with autoscaling. The behavior we want is that:
        //
        // 1. You can make large DSM allocations, larger than the current RAM
        //    size of the VM, without errors
        //
        // 2. If the allocated memory is really used, the VM is scaled up
        //    automatically to accommodate that
        //
        // We try to make that possible by having swap in the VM. But with the
        // default 'posix' DSM implementation, we fail step 1, even when there's
        // plenty of swap available. PostgreSQL uses posix_fallocate() to create
        // the shmem segment, which is really just a file in /dev/shm in Linux,
        // but posix_fallocate() on tmpfs returns ENOMEM if the size is larger
        // than available RAM.
        //
        // Using 'dynamic_shared_memory_type = mmap' works around that, because
        // the Postgres 'mmap' DSM implementation doesn't use
        // posix_fallocate(). Instead, it uses repeated calls to write(2) to
        // fill the file with zeros. It's weird that that differs between
        // 'posix' and 'mmap', but we take advantage of it. When the file is
        // filled slowly with write(2), the kernel allows it to grow larger, as
        // long as there's swap available.
        //
        // In short, using 'dynamic_shared_memory_type = mmap' allows us one DSM
        // segment to be larger than currently available RAM. But because we
        // don't want to store it on a real file, which the kernel would try to
        // flush to disk, so symlink pg_dynshm to /dev/shm.
        //
        // We don't set 'dynamic_shared_memory_type = mmap' here, we let the
        // control plane control that option. If 'mmap' is not used, this
        // symlink doesn't affect anything.
        //
        // See https://github.com/neondatabase/autoscaling/issues/800
        std::fs::remove_dir(pgdata_path.join("pg_dynshmem"))?;
        symlink("/dev/shm/", pgdata_path.join("pg_dynshmem"))?;

        match spec.mode {
            ComputeMode::Primary => {}
            ComputeMode::Replica | ComputeMode::Static(..) => {
                add_standby_signal(pgdata_path)?;
            }
        }

        Ok(())
    }

    /// Start and stop a postgres process to warm up the VM for startup.
    pub fn prewarm_postgres(&self) -> Result<()> {
        info!("prewarming");

        // Create pgdata
        let pgdata = &format!("{}.warmup", self.pgdata);
        create_pgdata(pgdata)?;

        // Run initdb to completion
        info!("running initdb");
        let initdb_bin = Path::new(&self.pgbin).parent().unwrap().join("initdb");
        Command::new(initdb_bin)
            .args(["--pgdata", pgdata])
            .output()
            .expect("cannot start initdb process");

        // Write conf
        use std::io::Write;
        let conf_path = Path::new(pgdata).join("postgresql.conf");
        let mut file = std::fs::File::create(conf_path)?;
        writeln!(file, "shared_buffers=65536")?;
        writeln!(file, "port=51055")?; // Nobody should be connecting
        writeln!(file, "shared_preload_libraries = 'neon'")?;

        // Start postgres
        info!("starting postgres");
        let mut pg = maybe_cgexec(&self.pgbin)
            .args(["-D", pgdata])
            .spawn()
            .expect("cannot start postgres process");

        // Stop it when it's ready
        info!("waiting for postgres");
        wait_for_postgres(&mut pg, Path::new(pgdata))?;
        // SIGQUIT orders postgres to exit immediately. We don't want to SIGKILL
        // it to avoid orphaned processes prowling around while datadir is
        // wiped.
        let pm_pid = Pid::from_raw(pg.id() as i32);
        kill(pm_pid, Signal::SIGQUIT)?;
        info!("sent SIGQUIT signal");
        pg.wait()?;
        info!("done prewarming");

        // clean up
        let _ok = fs::remove_dir_all(pgdata);
        Ok(())
    }

    /// Start Postgres as a child process and manage DBs/roles.
    /// After that this will hang waiting on the postmaster process to exit.
    /// Returns a handle to the child process and a handle to the logs thread.
    #[instrument(skip_all)]
    pub fn start_postgres(
        &self,
        storage_auth_token: Option<String>,
    ) -> Result<(std::process::Child, std::thread::JoinHandle<()>)> {
        let pgdata_path = Path::new(&self.pgdata);

        // Run postgres as a child process.
        let mut pg = maybe_cgexec(&self.pgbin)
            .args(["-D", &self.pgdata])
            .envs(if let Some(storage_auth_token) = &storage_auth_token {
                vec![("NEON_AUTH_TOKEN", storage_auth_token)]
            } else {
                vec![]
            })
            .stderr(Stdio::piped())
            .spawn()
            .expect("cannot start postgres process");
        PG_PID.store(pg.id(), Ordering::SeqCst);

        // Start a thread to collect logs from stderr.
        let stderr = pg.stderr.take().expect("stderr should be captured");
        let logs_handle = handle_postgres_logs(stderr);

        wait_for_postgres(&mut pg, pgdata_path)?;

        Ok((pg, logs_handle))
    }

    /// Do post configuration of the already started Postgres. This function spawns a background thread to
    /// configure the database after applying the compute spec. Currently, it upgrades the neon extension
    /// version. In the future, it may upgrade all 3rd-party extensions.
    #[instrument(skip_all)]
    pub fn post_apply_config(&self) -> Result<()> {
        let conf = self.get_conn_conf(Some("compute_ctl:post_apply_config"));
        thread::spawn(move || {
            let func = || {
                let mut client = conf.connect(NoTls)?;
                handle_neon_extension_upgrade(&mut client)
                    .context("handle_neon_extension_upgrade")?;
                Ok::<_, anyhow::Error>(())
            };
            if let Err(err) = func() {
                error!("error while post_apply_config: {err:#}");
            }
        });
        Ok(())
    }

    pub fn get_conn_conf(&self, application_name: Option<&str>) -> postgres::Config {
        let mut conf = self.conn_conf.clone();
        if let Some(application_name) = application_name {
            conf.application_name(application_name);
        }
        conf
    }

    pub fn get_tokio_conn_conf(&self, application_name: Option<&str>) -> tokio_postgres::Config {
        let mut conf = self.tokio_conn_conf.clone();
        if let Some(application_name) = application_name {
            conf.application_name(application_name);
        }
        conf
    }

    pub async fn get_maintenance_client(
        conf: &tokio_postgres::Config,
    ) -> Result<tokio_postgres::Client> {
        let mut conf = conf.clone();
        conf.application_name("compute_ctl:apply_config");

        let (client, conn) = match conf.connect(NoTls).await {
            // If connection fails, it may be the old node with `zenith_admin` superuser.
            //
            // In this case we need to connect with old `zenith_admin` name
            // and create new user. We cannot simply rename connected user,
            // but we can create a new one and grant it all privileges.
            Err(e) => match e.code() {
                Some(&SqlState::INVALID_PASSWORD)
                | Some(&SqlState::INVALID_AUTHORIZATION_SPECIFICATION) => {
                    // Connect with zenith_admin if cloud_admin could not authenticate
                    info!(
                        "cannot connect to postgres: {}, retrying with `zenith_admin` username",
                        e
                    );
                    let mut zenith_admin_conf = postgres::config::Config::from(conf.clone());
                    zenith_admin_conf.application_name("compute_ctl:apply_config");
                    zenith_admin_conf.user("zenith_admin");

                    let mut client =
                        zenith_admin_conf.connect(NoTls)
                            .context("broken cloud_admin credential: tried connecting with cloud_admin but could not authenticate, and zenith_admin does not work either")?;

                    // Disable forwarding so that users don't get a cloud_admin role
                    let mut func = || {
                        client.simple_query("SET neon.forward_ddl = false")?;
                        client.simple_query("CREATE USER cloud_admin WITH SUPERUSER")?;
                        client.simple_query("GRANT zenith_admin TO cloud_admin")?;
                        Ok::<_, anyhow::Error>(())
                    };
                    func().context("apply_config setup cloud_admin")?;

                    drop(client);

                    // Reconnect with connstring with expected name
                    conf.connect(NoTls).await?
                }
                _ => return Err(e.into()),
            },
            Ok((client, conn)) => (client, conn),
        };

        spawn(async move {
            if let Err(e) = conn.await {
                error!("maintenance client connection error: {}", e);
            }
        });

        // Disable DDL forwarding because control plane already knows about the roles/databases
        // we're about to modify.
        client
            .simple_query("SET neon.forward_ddl = false")
            .await
            .context("apply_config SET neon.forward_ddl = false")?;

        Ok(client)
    }

    /// Apply the spec to the running PostgreSQL instance.
    /// The caller can decide to run with multiple clients in parallel, or
    /// single mode.  Either way, the commands executed will be the same, and
    /// only commands run in different databases are parallelized.
    #[instrument(skip_all)]
    pub fn apply_spec_sql(
        &self,
        spec: Arc<ComputeSpec>,
        conf: Arc<tokio_postgres::Config>,
        concurrency: usize,
    ) -> Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        info!("Applying config with max {} concurrency", concurrency);
        debug!("Config: {:?}", spec);

        rt.block_on(async {
            // Proceed with post-startup configuration. Note, that order of operations is important.
            let client = Self::get_maintenance_client(&conf).await?;
            let spec = spec.clone();

            let databases = get_existing_dbs_async(&client).await?;
            let roles = get_existing_roles_async(&client)
                .await?
                .into_iter()
                .map(|role| (role.name.clone(), role))
                .collect::<HashMap<String, Role>>();

            let jwks_roles = Arc::new(
                spec.as_ref()
                    .local_proxy_config
                    .iter()
                    .flat_map(|it| &it.jwks)
                    .flatten()
                    .flat_map(|setting| &setting.role_names)
                    .cloned()
                    .collect::<HashSet<_>>(),
            );

            let ctx = Arc::new(tokio::sync::RwLock::new(MutableApplyContext {
                roles,
                dbs: databases,
            }));

            // Apply special pre drop database phase.
            // NOTE: we use the code of RunInEachDatabase phase for parallelism
            // and connection management, but we don't really run it in *each* database,
            // only in databases, we're about to drop.
            info!("Applying PerDatabase (pre-dropdb) phase");
            let concurrency_token = Arc::new(tokio::sync::Semaphore::new(concurrency));

            // Run the phase for each database that we're about to drop.
            let db_processes = spec
                .delta_operations
                .iter()
                .flatten()
                .filter_map(move |op| {
                    if op.action.as_str() == "delete_db" {
                        Some(op.name.clone())
                    } else {
                        None
                    }
                })
                .map(|dbname| {
                    let spec = spec.clone();
                    let ctx = ctx.clone();
                    let jwks_roles = jwks_roles.clone();
                    let mut conf = conf.as_ref().clone();
                    let concurrency_token = concurrency_token.clone();
                    // We only need dbname field for this phase, so set other fields to dummy values
                    let db = DB::UserDB(Database {
                        name: dbname.clone(),
                        owner: "cloud_admin".to_string(),
                        options: None,
                        restrict_conn: false,
                        invalid: false,
                    });

                    debug!("Applying per-database phases for Database {:?}", &db);

                    match &db {
                        DB::SystemDB => {}
                        DB::UserDB(db) => {
                            conf.dbname(db.name.as_str());
                        }
                    }

                    let conf = Arc::new(conf);
                    let fut = Self::apply_spec_sql_db(
                        spec.clone(),
                        conf,
                        ctx.clone(),
                        jwks_roles.clone(),
                        concurrency_token.clone(),
                        db,
                        [DropSubscriptionsForDeletedDatabases].to_vec(),
                    );

                    Ok(spawn(fut))
                })
                .collect::<Vec<Result<_, anyhow::Error>>>();

            for process in db_processes.into_iter() {
                let handle = process?;
                if let Err(e) = handle.await? {
                    // Handle the error case where the database does not exist
                    // We do not check whether the DB exists or not in the deletion phase,
                    // so we shouldn't be strict about it in pre-deletion cleanup as well.
                    if e.to_string().contains("does not exist") {
                        warn!("Error dropping subscription: {}", e);
                    } else {
                        return Err(e);
                    }
                };
            }

            for phase in [
                CreateSuperUser,
                DropInvalidDatabases,
                RenameRoles,
                CreateAndAlterRoles,
                RenameAndDeleteDatabases,
                CreateAndAlterDatabases,
            ] {
                info!("Applying phase {:?}", &phase);
                apply_operations(
                    spec.clone(),
                    ctx.clone(),
                    jwks_roles.clone(),
                    phase,
                    || async { Ok(&client) },
                )
                .await?;
            }

            info!("Applying RunInEachDatabase2 phase");
            let concurrency_token = Arc::new(tokio::sync::Semaphore::new(concurrency));

            let db_processes = spec
                .cluster
                .databases
                .iter()
                .map(|db| DB::new(db.clone()))
                // include
                .chain(once(DB::SystemDB))
                .map(|db| {
                    let spec = spec.clone();
                    let ctx = ctx.clone();
                    let jwks_roles = jwks_roles.clone();
                    let mut conf = conf.as_ref().clone();
                    let concurrency_token = concurrency_token.clone();
                    let db = db.clone();

                    debug!("Applying per-database phases for Database {:?}", &db);

                    match &db {
                        DB::SystemDB => {}
                        DB::UserDB(db) => {
                            conf.dbname(db.name.as_str());
                        }
                    }

                    let conf = Arc::new(conf);
                    let fut = Self::apply_spec_sql_db(
                        spec.clone(),
                        conf,
                        ctx.clone(),
                        jwks_roles.clone(),
                        concurrency_token.clone(),
                        db,
                        [
                            DeleteDBRoleReferences,
                            ChangeSchemaPerms,
                            HandleAnonExtension,
                        ]
                        .to_vec(),
                    );

                    Ok(spawn(fut))
                })
                .collect::<Vec<Result<_, anyhow::Error>>>();

            for process in db_processes.into_iter() {
                let handle = process?;
                handle.await??;
            }

            for phase in vec![
                HandleOtherExtensions,
                HandleNeonExtension,
                CreateAvailabilityCheck,
                DropRoles,
            ] {
                debug!("Applying phase {:?}", &phase);
                apply_operations(
                    spec.clone(),
                    ctx.clone(),
                    jwks_roles.clone(),
                    phase,
                    || async { Ok(&client) },
                )
                .await?;
            }

            Ok::<(), anyhow::Error>(())
        })?;

        Ok(())
    }

    /// Apply SQL migrations of the RunInEachDatabase phase.
    ///
    /// May opt to not connect to databases that don't have any scheduled
    /// operations.  The function is concurrency-controlled with the provided
    /// semaphore.  The caller has to make sure the semaphore isn't exhausted.
    async fn apply_spec_sql_db(
        spec: Arc<ComputeSpec>,
        conf: Arc<tokio_postgres::Config>,
        ctx: Arc<tokio::sync::RwLock<MutableApplyContext>>,
        jwks_roles: Arc<HashSet<String>>,
        concurrency_token: Arc<tokio::sync::Semaphore>,
        db: DB,
        subphases: Vec<PerDatabasePhase>,
    ) -> Result<()> {
        let _permit = concurrency_token.acquire().await?;

        let mut client_conn = None;

        for subphase in subphases {
            apply_operations(
                spec.clone(),
                ctx.clone(),
                jwks_roles.clone(),
                RunInEachDatabase {
                    db: db.clone(),
                    subphase,
                },
                // Only connect if apply_operation actually wants a connection.
                // It's quite possible this database doesn't need any queries,
                // so by not connecting we save time and effort connecting to
                // that database.
                || async {
                    if client_conn.is_none() {
                        let db_client = Self::get_maintenance_client(&conf).await?;
                        client_conn.replace(db_client);
                    }
                    let client = client_conn.as_ref().unwrap();
                    Ok(client)
                },
            )
            .await?;
        }

        drop(client_conn);

        Ok::<(), anyhow::Error>(())
    }

    /// Choose how many concurrent connections to use for applying the spec changes.
    pub fn max_service_connections(
        &self,
        compute_state: &ComputeState,
        spec: &ComputeSpec,
    ) -> usize {
        // If the cluster is in Init state we don't have to deal with user connections,
        // and can thus use all `max_connections` connection slots. However, that's generally not
        // very efficient, so we generally still limit it to a smaller number.
        if compute_state.status == ComputeStatus::Init {
            // If the settings contain 'max_connections', use that as template
            if let Some(config) = spec.cluster.settings.find("max_connections") {
                config.parse::<usize>().ok()
            } else {
                // Otherwise, try to find the setting in the postgresql_conf string
                spec.cluster
                    .postgresql_conf
                    .iter()
                    .flat_map(|conf| conf.split("\n"))
                    .filter_map(|line| {
                        if !line.contains("max_connections") {
                            return None;
                        }

                        let (key, value) = line.split_once("=")?;
                        let key = key
                            .trim_start_matches(char::is_whitespace)
                            .trim_end_matches(char::is_whitespace);

                        let value = value
                            .trim_start_matches(char::is_whitespace)
                            .trim_end_matches(char::is_whitespace);

                        if key != "max_connections" {
                            return None;
                        }

                        value.parse::<usize>().ok()
                    })
                    .next()
            }
            // If max_connections is present, use at most 1/3rd of that.
            // When max_connections is lower than 30, try to use at least 10 connections, but
            // never more than max_connections.
            .map(|limit| match limit {
                0..10 => limit,
                10..30 => 10,
                30.. => limit / 3,
            })
            // If we didn't find max_connections, default to 10 concurrent connections.
            .unwrap_or(10)
        } else {
            // state == Running
            // Because the cluster is already in the Running state, we should assume users are
            // already connected to the cluster, and high concurrency could negatively
            // impact user connectivity. Therefore, we can limit concurrency to the number of
            // reserved superuser connections, which users wouldn't be able to use anyway.
            spec.cluster
                .settings
                .find("superuser_reserved_connections")
                .iter()
                .filter_map(|val| val.parse::<usize>().ok())
                .map(|val| if val > 1 { val - 1 } else { 1 })
                .last()
                .unwrap_or(3)
        }
    }

    /// Do initial configuration of the already started Postgres.
    #[instrument(skip_all)]
    pub fn apply_config(&self, compute_state: &ComputeState) -> Result<()> {
        let conf = self.get_tokio_conn_conf(Some("compute_ctl:apply_config"));

        let conf = Arc::new(conf);
        let spec = Arc::new(
            compute_state
                .pspec
                .as_ref()
                .expect("spec must be set")
                .spec
                .clone(),
        );

        let max_concurrent_connections = self.max_service_connections(compute_state, &spec);

        // Merge-apply spec & changes to PostgreSQL state.
        self.apply_spec_sql(spec.clone(), conf.clone(), max_concurrent_connections)?;

        if let Some(ref local_proxy) = &spec.clone().local_proxy_config {
            info!("configuring local_proxy");
            local_proxy::configure(local_proxy).context("apply_config local_proxy")?;
        }

        // Run migrations separately to not hold up cold starts
        thread::spawn(move || {
            let conf = conf.as_ref().clone();
            let mut conf = postgres::config::Config::from(conf);
            conf.application_name("compute_ctl:migrations");

            match conf.connect(NoTls) {
                Ok(mut client) => {
                    if let Err(e) = handle_migrations(&mut client) {
                        error!("Failed to run migrations: {}", e);
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to connect to the compute for running migrations: {}",
                        e
                    );
                }
            };
        });

        Ok::<(), anyhow::Error>(())
    }

    // Wrapped this around `pg_ctl reload`, but right now we don't use
    // `pg_ctl` for start / stop.
    #[instrument(skip_all)]
    fn pg_reload_conf(&self) -> Result<()> {
        let pgctl_bin = Path::new(&self.pgbin).parent().unwrap().join("pg_ctl");
        Command::new(pgctl_bin)
            .args(["reload", "-D", &self.pgdata])
            .output()
            .expect("cannot run pg_ctl process");
        Ok(())
    }

    /// Similar to `apply_config()`, but does a bit different sequence of operations,
    /// as it's used to reconfigure a previously started and configured Postgres node.
    #[instrument(skip_all)]
    pub fn reconfigure(&self) -> Result<()> {
        let spec = self.state.lock().unwrap().pspec.clone().unwrap().spec;

        if let Some(ref pgbouncer_settings) = spec.pgbouncer_settings {
            info!("tuning pgbouncer");

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create rt");

            // Spawn a thread to do the tuning,
            // so that we don't block the main thread that starts Postgres.
            let pgbouncer_settings = pgbouncer_settings.clone();
            let _handle = thread::spawn(move || {
                let res = rt.block_on(tune_pgbouncer(pgbouncer_settings));
                if let Err(err) = res {
                    error!("error while tuning pgbouncer: {err:?}");
                }
            });
        }

        if let Some(ref local_proxy) = spec.local_proxy_config {
            info!("configuring local_proxy");

            // Spawn a thread to do the configuration,
            // so that we don't block the main thread that starts Postgres.
            let local_proxy = local_proxy.clone();
            let _handle = Some(thread::spawn(move || {
                if let Err(err) = local_proxy::configure(&local_proxy) {
                    error!("error while configuring local_proxy: {err:?}");
                }
            }));
        }

        // Write new config
        let pgdata_path = Path::new(&self.pgdata);
        let postgresql_conf_path = pgdata_path.join("postgresql.conf");
        config::write_postgres_conf(&postgresql_conf_path, &spec, self.http_port)?;

        let max_concurrent_connections = spec.reconfigure_concurrency;

        // Temporarily reset max_cluster_size in config
        // to avoid the possibility of hitting the limit, while we are reconfiguring:
        // creating new extensions, roles, etc.
        config::with_compute_ctl_tmp_override(pgdata_path, "neon.max_cluster_size=-1", || {
            self.pg_reload_conf()?;

            if spec.mode == ComputeMode::Primary {
                let mut conf = tokio_postgres::Config::from_str(self.connstr.as_str()).unwrap();
                conf.application_name("apply_config");
                let conf = Arc::new(conf);

                let spec = Arc::new(spec.clone());

                self.apply_spec_sql(spec, conf, max_concurrent_connections)?;
            }

            Ok(())
        })?;

        self.pg_reload_conf()?;

        let unknown_op = "unknown".to_string();
        let op_id = spec.operation_uuid.as_ref().unwrap_or(&unknown_op);
        info!(
            "finished reconfiguration of compute node for operation {}",
            op_id
        );

        Ok(())
    }

    #[instrument(skip_all)]
    pub fn start_compute(&self) -> Result<(std::process::Child, std::thread::JoinHandle<()>)> {
        let compute_state = self.state.lock().unwrap().clone();
        let pspec = compute_state.pspec.as_ref().expect("spec must be set");
        info!(
            "starting compute for project {}, operation {}, tenant {}, timeline {}",
            pspec.spec.cluster.cluster_id.as_deref().unwrap_or("None"),
            pspec.spec.operation_uuid.as_deref().unwrap_or("None"),
            pspec.tenant_id,
            pspec.timeline_id,
        );

        // tune pgbouncer
        if let Some(pgbouncer_settings) = &pspec.spec.pgbouncer_settings {
            info!("tuning pgbouncer");

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create rt");

            // Spawn a thread to do the tuning,
            // so that we don't block the main thread that starts Postgres.
            let pgbouncer_settings = pgbouncer_settings.clone();
            let _handle = thread::spawn(move || {
                let res = rt.block_on(tune_pgbouncer(pgbouncer_settings));
                if let Err(err) = res {
                    error!("error while tuning pgbouncer: {err:?}");
                }
            });
        }

        if let Some(local_proxy) = &pspec.spec.local_proxy_config {
            info!("configuring local_proxy");

            // Spawn a thread to do the configuration,
            // so that we don't block the main thread that starts Postgres.
            let local_proxy = local_proxy.clone();
            let _handle = thread::spawn(move || {
                if let Err(err) = local_proxy::configure(&local_proxy) {
                    error!("error while configuring local_proxy: {err:?}");
                }
            });
        }

        info!(
            "start_compute spec.remote_extensions {:?}",
            pspec.spec.remote_extensions
        );

        // This part is sync, because we need to download
        // remote shared_preload_libraries before postgres start (if any)
        if let Some(remote_extensions) = &pspec.spec.remote_extensions {
            // First, create control files for all availale extensions
            extension_server::create_control_files(remote_extensions, &self.pgbin);

            let library_load_start_time = Utc::now();
            let remote_ext_metrics = self.prepare_preload_libraries(&pspec.spec)?;

            let library_load_time = Utc::now()
                .signed_duration_since(library_load_start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64;
            let mut state = self.state.lock().unwrap();
            state.metrics.load_ext_ms = library_load_time;
            state.metrics.num_ext_downloaded = remote_ext_metrics.num_ext_downloaded;
            state.metrics.largest_ext_size = remote_ext_metrics.largest_ext_size;
            state.metrics.total_ext_download_size = remote_ext_metrics.total_ext_download_size;
            info!(
                "Loading shared_preload_libraries took {:?}ms",
                library_load_time
            );
            info!("{:?}", remote_ext_metrics);
        }

        self.prepare_pgdata(&compute_state)?;

        let start_time = Utc::now();
        let pg_process = self.start_postgres(pspec.storage_auth_token.clone())?;

        let config_time = Utc::now();
        if pspec.spec.mode == ComputeMode::Primary {
            if !pspec.spec.skip_pg_catalog_updates {
                let pgdata_path = Path::new(&self.pgdata);
                // temporarily reset max_cluster_size in config
                // to avoid the possibility of hitting the limit, while we are applying config:
                // creating new extensions, roles, etc...
                config::with_compute_ctl_tmp_override(
                    pgdata_path,
                    "neon.max_cluster_size=-1",
                    || {
                        self.pg_reload_conf()?;

                        self.apply_config(&compute_state)?;

                        Ok(())
                    },
                )?;
                self.pg_reload_conf()?;
            }
            self.post_apply_config()?;

            let conf = self.get_conn_conf(None);
            thread::spawn(move || {
                let res = get_installed_extensions(conf);
                match res {
                    Ok(extensions) => {
                        info!(
                            "[NEON_EXT_STAT] {}",
                            serde_json::to_string(&extensions)
                                .expect("failed to serialize extensions list")
                        );
                    }
                    Err(err) => error!("could not get installed extensions: {err:?}"),
                }
            });
        }

        let startup_end_time = Utc::now();
        {
            let mut state = self.state.lock().unwrap();
            state.metrics.start_postgres_ms = config_time
                .signed_duration_since(start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64;
            state.metrics.config_ms = startup_end_time
                .signed_duration_since(config_time)
                .to_std()
                .unwrap()
                .as_millis() as u64;
            state.metrics.total_startup_ms = startup_end_time
                .signed_duration_since(compute_state.start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64;
        }
        self.set_status(ComputeStatus::Running);

        info!(
            "finished configuration of compute for project {}",
            pspec.spec.cluster.cluster_id.as_deref().unwrap_or("None")
        );

        // Log metrics so that we can search for slow operations in logs
        let metrics = {
            let state = self.state.lock().unwrap();
            state.metrics.clone()
        };
        info!(?metrics, "compute start finished");

        Ok(pg_process)
    }

    /// Update the `last_active` in the shared state, but ensure that it's a more recent one.
    pub fn update_last_active(&self, last_active: Option<DateTime<Utc>>) {
        let mut state = self.state.lock().unwrap();
        // NB: `Some(<DateTime>)` is always greater than `None`.
        if last_active > state.last_active {
            state.last_active = last_active;
            debug!("set the last compute activity time to: {:?}", last_active);
        }
    }

    // Look for core dumps and collect backtraces.
    //
    // EKS worker nodes have following core dump settings:
    //   /proc/sys/kernel/core_pattern -> core
    //   /proc/sys/kernel/core_uses_pid -> 1
    //   ulimit -c -> unlimited
    // which results in core dumps being written to postgres data directory as core.<pid>.
    //
    // Use that as a default location and pattern, except macos where core dumps are written
    // to /cores/ directory by default.
    //
    // With default Linux settings, the core dump file is called just "core", so check for
    // that too.
    pub fn check_for_core_dumps(&self) -> Result<()> {
        let core_dump_dir = match std::env::consts::OS {
            "macos" => Path::new("/cores/"),
            _ => Path::new(&self.pgdata),
        };

        // Collect core dump paths if any
        info!("checking for core dumps in {}", core_dump_dir.display());
        let files = fs::read_dir(core_dump_dir)?;
        let cores = files.filter_map(|entry| {
            let entry = entry.ok()?;

            let is_core_dump = match entry.file_name().to_str()? {
                n if n.starts_with("core.") => true,
                "core" => true,
                _ => false,
            };
            if is_core_dump {
                Some(entry.path())
            } else {
                None
            }
        });

        // Print backtrace for each core dump
        for core_path in cores {
            warn!(
                "core dump found: {}, collecting backtrace",
                core_path.display()
            );

            // Try first with gdb
            let backtrace = Command::new("gdb")
                .args(["--batch", "-q", "-ex", "bt", &self.pgbin])
                .arg(&core_path)
                .output();

            // Try lldb if no gdb is found -- that is handy for local testing on macOS
            let backtrace = match backtrace {
                Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                    warn!("cannot find gdb, trying lldb");
                    Command::new("lldb")
                        .arg("-c")
                        .arg(&core_path)
                        .args(["--batch", "-o", "bt all", "-o", "quit"])
                        .output()
                }
                _ => backtrace,
            }?;

            warn!(
                "core dump backtrace: {}",
                String::from_utf8_lossy(&backtrace.stdout)
            );
            warn!(
                "debugger stderr: {}",
                String::from_utf8_lossy(&backtrace.stderr)
            );
        }

        Ok(())
    }

    /// Select `pg_stat_statements` data and return it as a stringified JSON
    pub async fn collect_insights(&self) -> String {
        let mut result_rows: Vec<String> = Vec::new();
        let conf = self.get_tokio_conn_conf(Some("compute_ctl:collect_insights"));
        let connect_result = conf.connect(NoTls).await;
        let (client, connection) = connect_result.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let result = client
            .simple_query(
                "SELECT
    row_to_json(pg_stat_statements)
FROM
    pg_stat_statements
WHERE
    userid != 'cloud_admin'::regrole::oid
ORDER BY
    (mean_exec_time + mean_plan_time) DESC
LIMIT 100",
            )
            .await;

        if let Ok(raw_rows) = result {
            for message in raw_rows.iter() {
                if let postgres::SimpleQueryMessage::Row(row) = message {
                    if let Some(json) = row.get(0) {
                        result_rows.push(json.to_string());
                    }
                }
            }

            format!("{{\"pg_stat_statements\": [{}]}}", result_rows.join(","))
        } else {
            "{{\"pg_stat_statements\": []}}".to_string()
        }
    }

    // download an archive, unzip and place files in correct locations
    pub async fn download_extension(
        &self,
        real_ext_name: String,
        ext_path: RemotePath,
    ) -> Result<u64, DownloadError> {
        let ext_remote_storage =
            self.ext_remote_storage
                .as_ref()
                .ok_or(DownloadError::BadInput(anyhow::anyhow!(
                    "Remote extensions storage is not configured",
                )))?;

        let ext_archive_name = ext_path.object_name().expect("bad path");

        let mut first_try = false;
        if !self
            .ext_download_progress
            .read()
            .expect("lock err")
            .contains_key(ext_archive_name)
        {
            self.ext_download_progress
                .write()
                .expect("lock err")
                .insert(ext_archive_name.to_string(), (Utc::now(), false));
            first_try = true;
        }
        let (download_start, download_completed) =
            self.ext_download_progress.read().expect("lock err")[ext_archive_name];
        let start_time_delta = Utc::now()
            .signed_duration_since(download_start)
            .to_std()
            .unwrap()
            .as_millis() as u64;

        // how long to wait for extension download if it was started by another process
        const HANG_TIMEOUT: u64 = 3000; // milliseconds

        if download_completed {
            info!("extension already downloaded, skipping re-download");
            return Ok(0);
        } else if start_time_delta < HANG_TIMEOUT && !first_try {
            info!("download {ext_archive_name} already started by another process, hanging untill completion or timeout");
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
            loop {
                info!("waiting for download");
                interval.tick().await;
                let (_, download_completed_now) =
                    self.ext_download_progress.read().expect("lock")[ext_archive_name];
                if download_completed_now {
                    info!("download finished by whoever else downloaded it");
                    return Ok(0);
                }
            }
            // NOTE: the above loop will get terminated
            // based on the timeout of the download function
        }

        // if extension hasn't been downloaded before or the previous
        // attempt to download was at least HANG_TIMEOUT ms ago
        // then we try to download it here
        info!("downloading new extension {ext_archive_name}");

        let download_size = extension_server::download_extension(
            &real_ext_name,
            &ext_path,
            ext_remote_storage,
            &self.pgbin,
        )
        .await
        .map_err(DownloadError::Other);

        if download_size.is_ok() {
            self.ext_download_progress
                .write()
                .expect("bad lock")
                .insert(ext_archive_name.to_string(), (download_start, true));
        }

        download_size
    }

    pub async fn set_role_grants(
        &self,
        db_name: &PgIdent,
        schema_name: &PgIdent,
        privileges: &[Privilege],
        role_name: &PgIdent,
    ) -> Result<()> {
        use tokio_postgres::NoTls;

        let mut conf = self.get_tokio_conn_conf(Some("compute_ctl:set_role_grants"));
        conf.dbname(db_name);

        let (db_client, conn) = conf
            .connect(NoTls)
            .await
            .context("Failed to connect to the database")?;
        tokio::spawn(conn);

        // TODO: support other types of grants apart from schemas?
        let query = format!(
            "GRANT {} ON SCHEMA {} TO {}",
            privileges
                .iter()
                // should not be quoted as it's part of the command.
                // is already sanitized so it's ok
                .map(|p| p.as_str())
                .collect::<Vec<&'static str>>()
                .join(", "),
            // quote the schema and role name as identifiers to sanitize them.
            schema_name.pg_quote(),
            role_name.pg_quote(),
        );
        db_client
            .simple_query(&query)
            .await
            .with_context(|| format!("Failed to execute query: {}", query))?;

        Ok(())
    }

    pub async fn install_extension(
        &self,
        ext_name: &PgIdent,
        db_name: &PgIdent,
        ext_version: ExtVersion,
    ) -> Result<ExtVersion> {
        use tokio_postgres::NoTls;

        let mut conf = self.get_tokio_conn_conf(Some("compute_ctl:install_extension"));
        conf.dbname(db_name);

        let (db_client, conn) = conf
            .connect(NoTls)
            .await
            .context("Failed to connect to the database")?;
        tokio::spawn(conn);

        let version_query = "SELECT extversion FROM pg_extension WHERE extname = $1";
        let version: Option<ExtVersion> = db_client
            .query_opt(version_query, &[&ext_name])
            .await
            .with_context(|| format!("Failed to execute query: {}", version_query))?
            .map(|row| row.get(0));

        // sanitize the inputs as postgres idents.
        let ext_name: String = ext_name.pg_quote();
        let quoted_version: String = ext_version.pg_quote();

        if let Some(installed_version) = version {
            if installed_version == ext_version {
                return Ok(installed_version);
            }
            let query = format!("ALTER EXTENSION {ext_name} UPDATE TO {quoted_version}");
            db_client
                .simple_query(&query)
                .await
                .with_context(|| format!("Failed to execute query: {}", query))?;
        } else {
            let query =
                format!("CREATE EXTENSION IF NOT EXISTS {ext_name} WITH VERSION {quoted_version}");
            db_client
                .simple_query(&query)
                .await
                .with_context(|| format!("Failed to execute query: {}", query))?;
        }

        Ok(ext_version)
    }

    #[tokio::main]
    pub async fn prepare_preload_libraries(
        &self,
        spec: &ComputeSpec,
    ) -> Result<RemoteExtensionMetrics> {
        if self.ext_remote_storage.is_none() {
            return Ok(RemoteExtensionMetrics {
                num_ext_downloaded: 0,
                largest_ext_size: 0,
                total_ext_download_size: 0,
            });
        }
        let remote_extensions = spec
            .remote_extensions
            .as_ref()
            .ok_or(anyhow::anyhow!("Remote extensions are not configured"))?;

        info!("parse shared_preload_libraries from spec.cluster.settings");
        let mut libs_vec = Vec::new();
        if let Some(libs) = spec.cluster.settings.find("shared_preload_libraries") {
            libs_vec = libs
                .split(&[',', '\'', ' '])
                .filter(|s| *s != "neon" && !s.is_empty())
                .map(str::to_string)
                .collect();
        }
        info!("parse shared_preload_libraries from provided postgresql.conf");

        // that is used in neon_local and python tests
        if let Some(conf) = &spec.cluster.postgresql_conf {
            let conf_lines = conf.split('\n').collect::<Vec<&str>>();
            let mut shared_preload_libraries_line = "";
            for line in conf_lines {
                if line.starts_with("shared_preload_libraries") {
                    shared_preload_libraries_line = line;
                }
            }
            let mut preload_libs_vec = Vec::new();
            if let Some(libs) = shared_preload_libraries_line.split("='").nth(1) {
                preload_libs_vec = libs
                    .split(&[',', '\'', ' '])
                    .filter(|s| *s != "neon" && !s.is_empty())
                    .map(str::to_string)
                    .collect();
            }
            libs_vec.extend(preload_libs_vec);
        }

        // Don't try to download libraries that are not in the index.
        // Assume that they are already present locally.
        libs_vec.retain(|lib| remote_extensions.library_index.contains_key(lib));

        info!("Downloading to shared preload libraries: {:?}", &libs_vec);

        let mut download_tasks = Vec::new();
        for library in &libs_vec {
            let (ext_name, ext_path) =
                remote_extensions.get_ext(library, true, &self.build_tag, &self.pgversion)?;
            download_tasks.push(self.download_extension(ext_name, ext_path));
        }
        let results = join_all(download_tasks).await;

        let mut remote_ext_metrics = RemoteExtensionMetrics {
            num_ext_downloaded: 0,
            largest_ext_size: 0,
            total_ext_download_size: 0,
        };
        for result in results {
            let download_size = match result {
                Ok(res) => {
                    remote_ext_metrics.num_ext_downloaded += 1;
                    res
                }
                Err(err) => {
                    // if we failed to download an extension, we don't want to fail the whole
                    // process, but we do want to log the error
                    error!("Failed to download extension: {}", err);
                    0
                }
            };

            remote_ext_metrics.largest_ext_size =
                std::cmp::max(remote_ext_metrics.largest_ext_size, download_size);
            remote_ext_metrics.total_ext_download_size += download_size;
        }
        Ok(remote_ext_metrics)
    }

    /// Waits until current thread receives a state changed notification and
    /// the pageserver connection strings has changed.
    ///
    /// The operation will time out after a specified duration.
    pub fn wait_timeout_while_pageserver_connstr_unchanged(&self, duration: Duration) {
        let state = self.state.lock().unwrap();
        let old_pageserver_connstr = state
            .pspec
            .as_ref()
            .expect("spec must be set")
            .pageserver_connstr
            .clone();
        let mut unchanged = true;
        let _ = self
            .state_changed
            .wait_timeout_while(state, duration, |s| {
                let pageserver_connstr = &s
                    .pspec
                    .as_ref()
                    .expect("spec must be set")
                    .pageserver_connstr;
                unchanged = pageserver_connstr == &old_pageserver_connstr;
                unchanged
            })
            .unwrap();
        if !unchanged {
            info!("Pageserver config changed");
        }
    }
}

pub fn forward_termination_signal() {
    let ss_pid = SYNC_SAFEKEEPERS_PID.load(Ordering::SeqCst);
    if ss_pid != 0 {
        let ss_pid = nix::unistd::Pid::from_raw(ss_pid as i32);
        kill(ss_pid, Signal::SIGTERM).ok();
    }
    let pg_pid = PG_PID.load(Ordering::SeqCst);
    if pg_pid != 0 {
        let pg_pid = nix::unistd::Pid::from_raw(pg_pid as i32);
        // Use 'fast' shutdown (SIGINT) because it also creates a shutdown checkpoint, which is important for
        // ROs to get a list of running xacts faster instead of going through the CLOG.
        // See https://www.postgresql.org/docs/current/server-shutdown.html for the list of modes and signals.
        kill(pg_pid, Signal::SIGINT).ok();
    }
}
