use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use utils::{
    connstring::connection_host_port,
    id::{TenantId, TimelineId},
    lsn::Lsn,
    postgres_backend::AuthType,
};

use crate::local_env::{LocalEnv, DEFAULT_PG_VERSION};
use crate::postgresql_conf::PostgresConf;
use crate::storage::PageServerNode;

//
// ComputeControlPlane
//
pub struct ComputeControlPlane {
    base_port: u16,
    pageserver: Arc<PageServerNode>,
    pub nodes: BTreeMap<(TenantId, String), Arc<PostgresNode>>,
    env: LocalEnv,
}

impl ComputeControlPlane {
    // Load current nodes with ports from data directories on disk
    // Directory structure has the following layout:
    // pgdatadirs
    // |- tenants
    // |  |- <tenant_id>
    // |  |   |- <node name>
    pub fn load(env: LocalEnv) -> Result<ComputeControlPlane> {
        let pageserver = Arc::new(PageServerNode::from_env(&env));

        let mut nodes = BTreeMap::default();
        let pgdatadirspath = &env.pg_data_dirs_path();

        for tenant_dir in fs::read_dir(&pgdatadirspath)
            .with_context(|| format!("failed to list {}", pgdatadirspath.display()))?
        {
            let tenant_dir = tenant_dir?;
            for timeline_dir in fs::read_dir(tenant_dir.path())
                .with_context(|| format!("failed to list {}", tenant_dir.path().display()))?
            {
                let node = PostgresNode::from_dir_entry(timeline_dir?, &env, &pageserver)?;
                nodes.insert((node.tenant_id, node.name.clone()), Arc::new(node));
            }
        }

        Ok(ComputeControlPlane {
            base_port: 55431,
            pageserver,
            nodes,
            env,
        })
    }

    fn get_port(&mut self) -> u16 {
        1 + self
            .nodes
            .iter()
            .map(|(_name, node)| node.address.port())
            .max()
            .unwrap_or(self.base_port)
    }

    pub fn new_node(
        &mut self,
        tenant_id: TenantId,
        name: &str,
        timeline_id: TimelineId,
        lsn: Option<Lsn>,
        port: Option<u16>,
        pg_version: u32,
    ) -> Result<Arc<PostgresNode>> {
        let port = port.unwrap_or_else(|| self.get_port());
        let node = Arc::new(PostgresNode {
            name: name.to_owned(),
            address: SocketAddr::new("127.0.0.1".parse().unwrap(), port),
            env: self.env.clone(),
            pageserver: Arc::clone(&self.pageserver),
            is_test: false,
            timeline_id,
            lsn,
            tenant_id,
            uses_wal_proposer: false,
            pg_version,
        });

        node.create_pgdata()?;
        node.setup_pg_conf(self.env.pageserver.auth_type)?;

        self.nodes
            .insert((tenant_id, node.name.clone()), Arc::clone(&node));

        Ok(node)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PostgresNode {
    pub address: SocketAddr,
    name: String,
    pub env: LocalEnv,
    pageserver: Arc<PageServerNode>,
    is_test: bool,
    pub timeline_id: TimelineId,
    pub lsn: Option<Lsn>, // if it's a read-only node. None for primary
    pub tenant_id: TenantId,
    uses_wal_proposer: bool,
    pg_version: u32,
}

impl PostgresNode {
    fn from_dir_entry(
        entry: std::fs::DirEntry,
        env: &LocalEnv,
        pageserver: &Arc<PageServerNode>,
    ) -> Result<PostgresNode> {
        if !entry.file_type()?.is_dir() {
            anyhow::bail!(
                "PostgresNode::from_dir_entry failed: '{}' is not a directory",
                entry.path().display()
            );
        }

        // parse data directory name
        let fname = entry.file_name();
        let name = fname.to_str().unwrap().to_string();

        // Read config file into memory
        let cfg_path = entry.path().join("postgresql.conf");
        let cfg_path_str = cfg_path.to_string_lossy();
        let mut conf_file = File::open(&cfg_path)
            .with_context(|| format!("failed to open config file in {}", cfg_path_str))?;
        let conf = PostgresConf::read(&mut conf_file)
            .with_context(|| format!("failed to read config file in {}", cfg_path_str))?;

        // Read a few options from the config file
        let context = format!("in config file {}", cfg_path_str);
        let port: u16 = conf.parse_field("port", &context)?;
        let timeline_id: TimelineId = conf.parse_field("neon.timeline_id", &context)?;
        let tenant_id: TenantId = conf.parse_field("neon.tenant_id", &context)?;
        let uses_wal_proposer = conf.get("neon.safekeepers").is_some();

        // Read postgres version from PG_VERSION file to determine which postgres version binary to use.
        // If it doesn't exist, assume broken data directory and use default pg version.
        let pg_version_path = entry.path().join("PG_VERSION");

        let pg_version_str =
            fs::read_to_string(pg_version_path).unwrap_or_else(|_| DEFAULT_PG_VERSION.to_string());
        let pg_version = u32::from_str(&pg_version_str)?;

        // parse recovery_target_lsn, if any
        let recovery_target_lsn: Option<Lsn> =
            conf.parse_field_optional("recovery_target_lsn", &context)?;

        // ok now
        Ok(PostgresNode {
            address: SocketAddr::new("127.0.0.1".parse().unwrap(), port),
            name,
            env: env.clone(),
            pageserver: Arc::clone(pageserver),
            is_test: false,
            timeline_id,
            lsn: recovery_target_lsn,
            tenant_id,
            uses_wal_proposer,
            pg_version,
        })
    }

    fn sync_safekeepers(&self, auth_token: &Option<String>, pg_version: u32) -> Result<Lsn> {
        let pg_path = self.env.pg_bin_dir(pg_version).join("postgres");
        let mut cmd = Command::new(&pg_path);

        cmd.arg("--sync-safekeepers")
            .env_clear()
            .env(
                "LD_LIBRARY_PATH",
                self.env.pg_lib_dir(pg_version).to_str().unwrap(),
            )
            .env(
                "DYLD_LIBRARY_PATH",
                self.env.pg_lib_dir(pg_version).to_str().unwrap(),
            )
            .env("PGDATA", self.pgdata().to_str().unwrap())
            .stdout(Stdio::piped())
            // Comment this to avoid capturing stderr (useful if command hangs)
            .stderr(Stdio::piped());

        if let Some(token) = auth_token {
            cmd.env("ZENITH_AUTH_TOKEN", token);
        }

        let sync_handle = cmd
            .spawn()
            .expect("postgres --sync-safekeepers failed to start");

        let sync_output = sync_handle
            .wait_with_output()
            .expect("postgres --sync-safekeepers failed");
        if !sync_output.status.success() {
            anyhow::bail!(
                "sync-safekeepers failed: '{}'",
                String::from_utf8_lossy(&sync_output.stderr)
            );
        }

        let lsn = Lsn::from_str(std::str::from_utf8(&sync_output.stdout)?.trim())?;
        println!("Safekeepers synced on {}", lsn);
        Ok(lsn)
    }

    /// Get basebackup from the pageserver as a tar archive and extract it
    /// to the `self.pgdata()` directory.
    fn do_basebackup(&self, lsn: Option<Lsn>) -> Result<()> {
        println!(
            "Extracting base backup to create postgres instance: path={} port={}",
            self.pgdata().display(),
            self.address.port()
        );

        let sql = if let Some(lsn) = lsn {
            format!("basebackup {} {} {}", self.tenant_id, self.timeline_id, lsn)
        } else {
            format!("basebackup {} {}", self.tenant_id, self.timeline_id)
        };

        let mut client = self
            .pageserver
            .page_server_psql_client()
            .context("connecting to page server failed")?;

        let copyreader = client
            .copy_out(sql.as_str())
            .context("page server 'basebackup' command failed")?;

        // Read the archive directly from the `CopyOutReader`
        //
        // Set `ignore_zeros` so that unpack() reads all the Copy data and
        // doesn't stop at the end-of-archive marker. Otherwise, if the server
        // sends an Error after finishing the tarball, we will not notice it.
        let mut ar = tar::Archive::new(copyreader);
        ar.set_ignore_zeros(true);
        ar.unpack(&self.pgdata())
            .context("extracting base backup failed")?;

        Ok(())
    }

    fn create_pgdata(&self) -> Result<()> {
        fs::create_dir_all(&self.pgdata()).with_context(|| {
            format!(
                "could not create data directory {}",
                self.pgdata().display()
            )
        })?;
        fs::set_permissions(self.pgdata().as_path(), fs::Permissions::from_mode(0o700))
            .with_context(|| {
                format!(
                    "could not set permissions in data directory {}",
                    self.pgdata().display()
                )
            })
    }

    // Write postgresql.conf with default configuration
    // and PG_VERSION file to the data directory of a new node.
    fn setup_pg_conf(&self, auth_type: AuthType) -> Result<()> {
        let mut conf = PostgresConf::new();
        conf.append("max_wal_senders", "10");
        // wal_log_hints is mandatory when running against pageserver (see gh issue#192)
        // TODO: is it possible to check wal_log_hints at pageserver side via XLOG_PARAMETER_CHANGE?
        conf.append("wal_log_hints", "off");
        conf.append("max_replication_slots", "10");
        conf.append("hot_standby", "on");
        conf.append("shared_buffers", "1MB");
        conf.append("fsync", "off");
        conf.append("max_connections", "100");
        conf.append("wal_level", "replica");
        // wal_sender_timeout is the maximum time to wait for WAL replication.
        // It also defines how often the walreciever will send a feedback message to the wal sender.
        conf.append("wal_sender_timeout", "5s");
        conf.append("listen_addresses", &self.address.ip().to_string());
        conf.append("port", &self.address.port().to_string());
        conf.append("wal_keep_size", "0");
        // walproposer panics when basebackup is invalid, it is pointless to restart in this case.
        conf.append("restart_after_crash", "off");

        // Configure the node to fetch pages from pageserver
        let pageserver_connstr = {
            let (host, port) = connection_host_port(&self.pageserver.pg_connection_config);

            // Set up authentication
            //
            // $ZENITH_AUTH_TOKEN will be replaced with value from environment
            // variable during compute pg startup. It is done this way because
            // otherwise user will be able to retrieve the value using SHOW
            // command or pg_settings
            let password = if let AuthType::NeonJWT = auth_type {
                "$ZENITH_AUTH_TOKEN"
            } else {
                ""
            };
            // NOTE avoiding spaces in connection string, because it is less error prone if we forward it somewhere.
            // Also note that not all parameters are supported here. Because in compute we substitute $ZENITH_AUTH_TOKEN
            // We parse this string and build it back with token from env var, and for simplicity rebuild
            // uses only needed variables namely host, port, user, password.
            format!("postgresql://no_user:{password}@{host}:{port}")
        };
        conf.append("shared_preload_libraries", "neon");
        conf.append_line("");
        conf.append("neon.pageserver_connstring", &pageserver_connstr);
        conf.append("neon.tenant_id", &self.tenant_id.to_string());
        conf.append("neon.timeline_id", &self.timeline_id.to_string());
        if let Some(lsn) = self.lsn {
            conf.append("recovery_target_lsn", &lsn.to_string());
        }

        conf.append_line("");
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
        conf.append("max_replication_write_lag", "500MB");
        conf.append("max_replication_flush_lag", "10GB");

        if !self.env.safekeepers.is_empty() {
            // Configure the node to connect to the safekeepers
            conf.append("synchronous_standby_names", "walproposer");

            let safekeepers = self
                .env
                .safekeepers
                .iter()
                .map(|sk| format!("localhost:{}", sk.pg_port))
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

        let mut file = File::create(self.pgdata().join("postgresql.conf"))?;
        file.write_all(conf.to_string().as_bytes())?;

        let mut file = File::create(self.pgdata().join("PG_VERSION"))?;
        file.write_all(self.pg_version.to_string().as_bytes())?;

        Ok(())
    }

    fn load_basebackup(&self, auth_token: &Option<String>) -> Result<()> {
        let backup_lsn = if let Some(lsn) = self.lsn {
            Some(lsn)
        } else if self.uses_wal_proposer {
            // LSN 0 means that it is bootstrap and we need to download just
            // latest data from the pageserver. That is a bit clumsy but whole bootstrap
            // procedure evolves quite actively right now, so let's think about it again
            // when things would be more stable (TODO).
            let lsn = self.sync_safekeepers(auth_token, self.pg_version)?;
            if lsn == Lsn(0) {
                None
            } else {
                Some(lsn)
            }
        } else {
            None
        };

        self.do_basebackup(backup_lsn)?;

        Ok(())
    }

    pub fn pgdata(&self) -> PathBuf {
        self.env.pg_data_dir(&self.tenant_id, &self.name)
    }

    pub fn status(&self) -> &str {
        let timeout = Duration::from_millis(300);
        let has_pidfile = self.pgdata().join("postmaster.pid").exists();
        let can_connect = TcpStream::connect_timeout(&self.address, timeout).is_ok();

        match (has_pidfile, can_connect) {
            (true, true) => "running",
            (false, false) => "stopped",
            (true, false) => "crashed",
            (false, true) => "running, no pidfile",
        }
    }

    fn pg_ctl(&self, args: &[&str], auth_token: &Option<String>) -> Result<()> {
        let pg_ctl_path = self.env.pg_bin_dir(self.pg_version).join("pg_ctl");
        let mut cmd = Command::new(pg_ctl_path);
        cmd.args(
            [
                &[
                    "-D",
                    self.pgdata().to_str().unwrap(),
                    "-l",
                    self.pgdata().join("pg.log").to_str().unwrap(),
                    "-w", //wait till pg_ctl actually does what was asked
                ],
                args,
            ]
            .concat(),
        )
        .env_clear()
        .env(
            "LD_LIBRARY_PATH",
            self.env.pg_lib_dir(self.pg_version).to_str().unwrap(),
        )
        .env(
            "DYLD_LIBRARY_PATH",
            self.env.pg_lib_dir(self.pg_version).to_str().unwrap(),
        );
        if let Some(token) = auth_token {
            cmd.env("ZENITH_AUTH_TOKEN", token);
        }

        let pg_ctl = cmd.output().context("pg_ctl failed")?;
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

    pub fn start(&self, auth_token: &Option<String>) -> Result<()> {
        // Bail if the node already running.
        if self.status() == "running" {
            anyhow::bail!("The node is already running");
        }

        // 1. We always start compute node from scratch, so
        // if old dir exists, preserve 'postgresql.conf' and drop the directory
        let postgresql_conf_path = self.pgdata().join("postgresql.conf");
        let postgresql_conf = fs::read(&postgresql_conf_path).with_context(|| {
            format!(
                "failed to read config file in {}",
                postgresql_conf_path.to_str().unwrap()
            )
        })?;
        fs::remove_dir_all(&self.pgdata())?;
        self.create_pgdata()?;

        // 2. Bring back config files
        fs::write(&postgresql_conf_path, postgresql_conf)?;

        // 3. Load basebackup
        self.load_basebackup(auth_token)?;

        if self.lsn.is_some() {
            File::create(self.pgdata().join("standby.signal"))?;
        }

        // 4. Finally start the compute node postgres
        println!("Starting postgres node at '{}'", self.connstr());
        self.pg_ctl(&["start"], auth_token)
    }

    pub fn restart(&self, auth_token: &Option<String>) -> Result<()> {
        self.pg_ctl(&["restart"], auth_token)
    }

    pub fn stop(&self, destroy: bool) -> Result<()> {
        // If we are going to destroy data directory,
        // use immediate shutdown mode, otherwise,
        // shutdown gracefully to leave the data directory sane.
        //
        // Compute node always starts from scratch, so stop
        // without destroy only used for testing and debugging.
        //
        if destroy {
            self.pg_ctl(&["-m", "immediate", "stop"], &None)?;
            println!(
                "Destroying postgres data directory '{}'",
                self.pgdata().to_str().unwrap()
            );
            fs::remove_dir_all(&self.pgdata())?;
        } else {
            self.pg_ctl(&["stop"], &None)?;
        }
        Ok(())
    }

    pub fn connstr(&self) -> String {
        format!(
            "host={} port={} user={} dbname={}",
            self.address.ip(),
            self.address.port(),
            "cloud_admin",
            "postgres"
        )
    }

    // XXX: cache that in control plane
    pub fn whoami(&self) -> String {
        let output = Command::new("whoami")
            .output()
            .expect("failed to execute whoami");

        assert!(output.status.success(), "whoami failed");

        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }
}

impl Drop for PostgresNode {
    // destructor to clean up state after test is done
    // XXX: we may detect failed test by setting some flag in catch_unwind()
    // and checking it here. But let just clean datadirs on start.
    fn drop(&mut self) {
        if self.is_test {
            let _ = self.stop(true);
        }
    }
}
