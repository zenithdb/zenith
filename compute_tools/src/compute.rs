use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::BufRead;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::{Condvar, Mutex, RwLock};
use std::thread;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use nix::unistd::Pid;
use postgres::error::SqlState;
use postgres::{Client, NoTls};
use tracing::{debug, error, info, instrument, warn};
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

use compute_api::responses::{ComputeMetrics, ComputeStatus};
use compute_api::spec::{ComputeFeature, ComputeMode, ComputeSpec};
use utils::measured_stream::MeasuredReader;

use nix::sys::signal::{kill, Signal};

use remote_storage::{DownloadError, RemotePath};

use crate::checker::create_availability_check_data;
use crate::logger::inlinify;
use crate::pg_helpers::*;
use crate::spec::*;
use crate::sync_sk::{check_if_synced, ping_safekeeper};
use crate::{config, extension_server};

pub static SYNC_SAFEKEEPERS_PID: AtomicU32 = AtomicU32::new(0);
pub static PG_PID: AtomicU32 = AtomicU32::new(0);

/// Compute node info shared across several `compute_ctl` threads.
pub struct ComputeNode {
    // Url type maintains proper escaping
    pub connstr: url::Url,
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

/// Create special neon_superuser role, that's a slightly nerfed version of a real superuser
/// that we give to customers
#[instrument(skip_all)]
fn create_neon_superuser(spec: &ComputeSpec, client: &mut Client) -> Result<()> {
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
    info!("Neon superuser created: {}", inlinify(&query));
    client
        .simple_query(&query)
        .map_err(|e| anyhow::anyhow!(e).context(query))?;
    Ok(())
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
        state.status = status;
        self.state_changed.notify_all();
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
            // HACK We don't use compression on first start (Lsn(0)) because there's no API for it
            Lsn(0) => format!("basebackup {} {}", spec.tenant_id, spec.timeline_id),
            _ => format!(
                "basebackup {} {} {} --gzip",
                spec.tenant_id, spec.timeline_id, lsn
            ),
        };

        let copyreader = client.copy_out(basebackup_cmd.as_str())?;
        let mut measured_reader = MeasuredReader::new(copyreader);

        // Check the magic number to see if it's a gzip or not. Even though
        // we might explicitly ask for gzip, an old pageserver with no implementation
        // of gzip compression might send us uncompressed data. After some time
        // passes we can assume all pageservers know how to compress and we can
        // delete this check.
        //
        // If the data is not gzip, it will be tar. It will not be mistakenly
        // recognized as gzip because tar starts with an ascii encoding of a filename,
        // and 0x1f and 0x8b are unlikely first characters for any filename. Moreover,
        // we send the "global" directory first from the pageserver, so it definitely
        // won't be recognized as gzip.
        let mut bufreader = std::io::BufReader::new(&mut measured_reader);
        let gzip = {
            let peek = bufreader.fill_buf().unwrap();
            peek[0] == 0x1f && peek[1] == 0x8b
        };

        // Read the archive directly from the `CopyOutReader`
        //
        // Set `ignore_zeros` so that unpack() reads all the Copy data and
        // doesn't stop at the end-of-archive marker. Otherwise, if the server
        // sends an Error after finishing the tarball, we will not notice it.
        if gzip {
            let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(&mut bufreader));
            ar.set_ignore_zeros(true);
            ar.unpack(&self.pgdata)?;
        } else {
            let mut ar = tar::Archive::new(&mut bufreader);
            ar.set_ignore_zeros(true);
            ar.unpack(&self.pgdata)?;
        };

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
    pub fn prepare_pgdata(
        &self,
        compute_state: &ComputeState,
        extension_server_port: u16,
    ) -> Result<()> {
        let pspec = compute_state.pspec.as_ref().expect("spec must be set");
        let spec = &pspec.spec;
        let pgdata_path = Path::new(&self.pgdata);

        // Remove/create an empty pgdata directory and put configuration there.
        self.create_pgdata()?;
        config::write_postgres_conf(
            &pgdata_path.join("postgresql.conf"),
            &pspec.spec,
            Some(extension_server_port),
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
            .args(["-D", pgdata])
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
        let connstr = self.connstr.clone();
        thread::spawn(move || {
            let func = || {
                let mut client = Client::connect(connstr.as_str(), NoTls)?;
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

    /// Do initial configuration of the already started Postgres.
    #[instrument(skip_all)]
    pub fn apply_config(&self, compute_state: &ComputeState) -> Result<()> {
        // If connection fails,
        // it may be the old node with `zenith_admin` superuser.
        //
        // In this case we need to connect with old `zenith_admin` name
        // and create new user. We cannot simply rename connected user,
        // but we can create a new one and grant it all privileges.
        let mut connstr = self.connstr.clone();
        connstr
            .query_pairs_mut()
            .append_pair("application_name", "apply_config");

        let mut client = match Client::connect(connstr.as_str(), NoTls) {
            Err(e) => match e.code() {
                Some(&SqlState::INVALID_PASSWORD)
                | Some(&SqlState::INVALID_AUTHORIZATION_SPECIFICATION) => {
                    // connect with zenith_admin if cloud_admin could not authenticate
                    info!(
                        "cannot connect to postgres: {}, retrying with `zenith_admin` username",
                        e
                    );
                    let mut zenith_admin_connstr = connstr.clone();

                    zenith_admin_connstr
                        .set_username("zenith_admin")
                        .map_err(|_| anyhow::anyhow!("invalid connstr"))?;

                    let mut client =
                        Client::connect(zenith_admin_connstr.as_str(), NoTls)
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

                    // reconnect with connstring with expected name
                    Client::connect(connstr.as_str(), NoTls)?
                }
                _ => return Err(e.into()),
            },
            Ok(client) => client,
        };

        // Disable DDL forwarding because control plane already knows about these roles/databases.
        client
            .simple_query("SET neon.forward_ddl = false")
            .context("apply_config SET neon.forward_ddl = false")?;

        // Proceed with post-startup configuration. Note, that order of operations is important.
        let spec = &compute_state.pspec.as_ref().expect("spec must be set").spec;
        create_neon_superuser(spec, &mut client).context("apply_config create_neon_superuser")?;
        cleanup_instance(&mut client).context("apply_config cleanup_instance")?;
        handle_roles(spec, &mut client).context("apply_config handle_roles")?;
        handle_databases(spec, &mut client).context("apply_config handle_databases")?;
        handle_role_deletions(spec, connstr.as_str(), &mut client)
            .context("apply_config handle_role_deletions")?;
        handle_grants(
            spec,
            &mut client,
            connstr.as_str(),
            self.has_feature(ComputeFeature::AnonExtension),
        )
        .context("apply_config handle_grants")?;
        handle_extensions(spec, &mut client).context("apply_config handle_extensions")?;
        handle_extension_neon(&mut client).context("apply_config handle_extension_neon")?;
        create_availability_check_data(&mut client)
            .context("apply_config create_availability_check_data")?;

        // 'Close' connection
        drop(client);

        // Run migrations separately to not hold up cold starts
        thread::spawn(move || {
            handle_migrations(connstr.clone()).context("apply_config handle_migrations")
        });
        Ok(())
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

        // Write new config
        let pgdata_path = Path::new(&self.pgdata);
        let postgresql_conf_path = pgdata_path.join("postgresql.conf");
        config::write_postgres_conf(&postgresql_conf_path, &spec, None)?;
        // temporarily reset max_cluster_size in config
        // to avoid the possibility of hitting the limit, while we are reconfiguring:
        // creating new extensions, roles, etc...
        config::with_compute_ctl_tmp_override(pgdata_path, "neon.max_cluster_size=-1", || {
            self.pg_reload_conf()?;

            let mut client = Client::connect(self.connstr.as_str(), NoTls)?;

            // Proceed with post-startup configuration. Note, that order of operations is important.
            // Disable DDL forwarding because control plane already knows about these roles/databases.
            if spec.mode == ComputeMode::Primary {
                client.simple_query("SET neon.forward_ddl = false")?;
                cleanup_instance(&mut client)?;
                handle_roles(&spec, &mut client)?;
                handle_databases(&spec, &mut client)?;
                handle_role_deletions(&spec, self.connstr.as_str(), &mut client)?;
                handle_grants(
                    &spec,
                    &mut client,
                    self.connstr.as_str(),
                    self.has_feature(ComputeFeature::AnonExtension),
                )?;
                handle_extensions(&spec, &mut client)?;
                handle_extension_neon(&mut client)?;
                // We can skip handle_migrations here because a new migration can only appear
                // if we have a new version of the compute_ctl binary, which can only happen
                // if compute got restarted, in which case we'll end up inside of apply_config
                // instead of reconfigure.
            }

            // 'Close' connection
            drop(client);

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
    pub fn start_compute(
        &self,
        extension_server_port: u16,
    ) -> Result<(std::process::Child, std::thread::JoinHandle<()>)> {
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

        self.prepare_pgdata(&compute_state, extension_server_port)?;

        let start_time = Utc::now();
        let pg_process = self.start_postgres(pspec.storage_auth_token.clone())?;

        let config_time = Utc::now();
        if pspec.spec.mode == ComputeMode::Primary && !pspec.spec.skip_pg_catalog_updates {
            let pgdata_path = Path::new(&self.pgdata);
            // temporarily reset max_cluster_size in config
            // to avoid the possibility of hitting the limit, while we are applying config:
            // creating new extensions, roles, etc...
            config::with_compute_ctl_tmp_override(pgdata_path, "neon.max_cluster_size=-1", || {
                self.pg_reload_conf()?;

                self.apply_config(&compute_state)?;

                Ok(())
            })?;
            self.pg_reload_conf()?;
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
            let _ = entry.file_name().to_str()?.strip_prefix("core.")?;
            Some(entry.path())
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
        let connect_result = tokio_postgres::connect(self.connstr.as_str(), NoTls).await;
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
