use crate::{
    background_process,
    local_env::{LocalEnv, NeonStorageControllerConf},
};
use camino::{Utf8Path, Utf8PathBuf};
use hyper0::Uri;
use nix::unistd::Pid;
use pageserver_api::{
    controller_api::{
        NodeConfigureRequest, NodeDescribeResponse, NodeRegisterRequest, TenantCreateRequest,
        TenantCreateResponse, TenantLocateResponse, TenantShardMigrateRequest,
        TenantShardMigrateResponse,
    },
    models::{
        TenantShardSplitRequest, TenantShardSplitResponse, TimelineCreateRequest, TimelineInfo,
    },
    shard::{ShardStripeSize, TenantShardId},
};
use pageserver_client::mgmt_api::ResponseErrorMessageExt;
use postgres_backend::AuthType;
use reqwest::Method;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    ffi::OsStr,
    fs,
    net::SocketAddr,
    path::PathBuf,
    process::ExitStatus,
    str::FromStr,
    sync::OnceLock,
    time::{Duration, Instant},
};
use tokio::process::Command;
use tracing::instrument;
use url::Url;
use utils::{
    auth::{encode_from_key_file, Claims, Scope},
    id::{NodeId, TenantId},
};
use whoami::username;

pub struct StorageController {
    env: LocalEnv,
    private_key: Option<Vec<u8>>,
    public_key: Option<String>,
    client: reqwest::Client,
    config: NeonStorageControllerConf,

    // The listen addresses is learned when starting the storage controller,
    // hence the use of OnceLock to init it at the right time.
    listen: OnceLock<SocketAddr>,
}

const COMMAND: &str = "storage_controller";

const STORAGE_CONTROLLER_POSTGRES_VERSION: u32 = 16;

const DB_NAME: &str = "storage_controller";

pub struct NeonStorageControllerStartArgs {
    pub instance_id: u8,
    pub base_port: Option<u16>,
    pub start_timeout: humantime::Duration,
}

impl NeonStorageControllerStartArgs {
    pub fn with_default_instance_id(start_timeout: humantime::Duration) -> Self {
        Self {
            instance_id: 1,
            base_port: None,
            start_timeout,
        }
    }
}

pub struct NeonStorageControllerStopArgs {
    pub instance_id: u8,
    pub immediate: bool,
}

impl NeonStorageControllerStopArgs {
    pub fn with_default_instance_id(immediate: bool) -> Self {
        Self {
            instance_id: 1,
            immediate,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct AttachHookRequest {
    pub tenant_shard_id: TenantShardId,
    pub node_id: Option<NodeId>,
    pub generation_override: Option<i32>,
}

#[derive(Serialize, Deserialize)]
pub struct AttachHookResponse {
    pub gen: Option<u32>,
}

#[derive(Serialize, Deserialize)]
pub struct InspectRequest {
    pub tenant_shard_id: TenantShardId,
}

#[derive(Serialize, Deserialize)]
pub struct InspectResponse {
    pub attachment: Option<(u32, NodeId)>,
}

impl StorageController {
    pub fn from_env(env: &LocalEnv) -> Self {
        // Assume all pageservers have symmetric auth configuration: this service
        // expects to use one JWT token to talk to all of them.
        let ps_conf = env
            .pageservers
            .first()
            .expect("Config is validated to contain at least one pageserver");
        let (private_key, public_key) = match ps_conf.http_auth_type {
            AuthType::Trust => (None, None),
            AuthType::NeonJWT => {
                let private_key_path = env.get_private_key_path();
                let private_key = fs::read(private_key_path).expect("failed to read private key");

                // If pageserver auth is enabled, this implicitly enables auth for this service,
                // using the same credentials.
                let public_key_path =
                    camino::Utf8PathBuf::try_from(env.base_data_dir.join("auth_public_key.pem"))
                        .unwrap();

                // This service takes keys as a string rather than as a path to a file/dir: read the key into memory.
                let public_key = if std::fs::metadata(&public_key_path)
                    .expect("Can't stat public key")
                    .is_dir()
                {
                    // Our config may specify a directory: this is for the pageserver's ability to handle multiple
                    // keys.  We only use one key at a time, so, arbitrarily load the first one in the directory.
                    let mut dir =
                        std::fs::read_dir(&public_key_path).expect("Can't readdir public key path");
                    let dent = dir
                        .next()
                        .expect("Empty key dir")
                        .expect("Error reading key dir");

                    std::fs::read_to_string(dent.path()).expect("Can't read public key")
                } else {
                    std::fs::read_to_string(&public_key_path).expect("Can't read public key")
                };
                (Some(private_key), Some(public_key))
            }
        };

        Self {
            env: env.clone(),
            private_key,
            public_key,
            client: reqwest::ClientBuilder::new()
                .build()
                .expect("Failed to construct http client"),
            config: env.storage_controller.clone(),
            listen: OnceLock::default(),
        }
    }

    fn storage_controller_instance_dir(&self, instance_id: u8) -> PathBuf {
        self.env
            .base_data_dir
            .join(format!("storage_controller_{}", instance_id))
    }

    fn pid_file(&self, instance_id: u8) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            self.storage_controller_instance_dir(instance_id)
                .join("storage_controller.pid"),
        )
        .expect("non-Unicode path")
    }

    /// Find the directory containing postgres subdirectories, such `bin` and `lib`
    ///
    /// This usually uses STORAGE_CONTROLLER_POSTGRES_VERSION of postgres, but will fall back
    /// to other versions if that one isn't found.  Some automated tests create circumstances
    /// where only one version is available in pg_distrib_dir, such as `test_remote_extensions`.
    async fn get_pg_dir(&self, dir_name: &str) -> anyhow::Result<Utf8PathBuf> {
        let prefer_versions = [STORAGE_CONTROLLER_POSTGRES_VERSION, 16, 15, 14];

        for v in prefer_versions {
            let path = Utf8PathBuf::from_path_buf(self.env.pg_dir(v, dir_name)?).unwrap();
            if tokio::fs::try_exists(&path).await? {
                return Ok(path);
            }
        }

        // Fall through
        anyhow::bail!(
            "Postgres directory '{}' not found in {}",
            dir_name,
            self.env.pg_distrib_dir.display(),
        );
    }

    pub async fn get_pg_bin_dir(&self) -> anyhow::Result<Utf8PathBuf> {
        self.get_pg_dir("bin").await
    }

    pub async fn get_pg_lib_dir(&self) -> anyhow::Result<Utf8PathBuf> {
        self.get_pg_dir("lib").await
    }

    /// Readiness check for our postgres process
    async fn pg_isready(&self, pg_bin_dir: &Utf8Path, postgres_port: u16) -> anyhow::Result<bool> {
        let bin_path = pg_bin_dir.join("pg_isready");
        let args = [
            "-h",
            "localhost",
            "-U",
            &username(),
            "-d",
            DB_NAME,
            "-p",
            &format!("{}", postgres_port),
        ];
        let exitcode = Command::new(bin_path).args(args).spawn()?.wait().await?;

        Ok(exitcode.success())
    }

    /// Create our database if it doesn't exist
    ///
    /// This function is equivalent to the `diesel setup` command in the diesel CLI.  We implement
    /// the same steps by hand to avoid imposing a dependency on installing diesel-cli for developers
    /// who just want to run `cargo neon_local` without knowing about diesel.
    ///
    /// Returns the database url
    pub async fn setup_database(&self, postgres_port: u16) -> anyhow::Result<String> {
        let database_url = format!(
            "postgresql://{}@localhost:{}/{DB_NAME}",
            &username(),
            postgres_port
        );

        let pg_bin_dir = self.get_pg_bin_dir().await?;
        let createdb_path = pg_bin_dir.join("createdb");
        let output = Command::new(&createdb_path)
            .args([
                "-h",
                "localhost",
                "-p",
                &format!("{}", postgres_port),
                "-U",
                &username(),
                "-O",
                &username(),
                DB_NAME,
            ])
            .output()
            .await
            .expect("Failed to spawn createdb");

        if !output.status.success() {
            let stderr = String::from_utf8(output.stderr).expect("Non-UTF8 output from createdb");
            if stderr.contains("already exists") {
                tracing::info!("Database {DB_NAME} already exists");
            } else {
                anyhow::bail!("createdb failed with status {}: {stderr}", output.status);
            }
        }

        Ok(database_url)
    }

    pub async fn connect_to_database(
        &self,
        postgres_port: u16,
    ) -> anyhow::Result<(
        tokio_postgres::Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    )> {
        tokio_postgres::Config::new()
            .host("localhost")
            .port(postgres_port)
            // The user is the ambient operating system user name.
            // That is an impurity which we want to fix in => TODO https://github.com/neondatabase/neon/issues/8400
            //
            // Until we get there, use the ambient operating system user name.
            // Recent tokio-postgres versions default to this if the user isn't specified.
            // But tokio-postgres fork doesn't have this upstream commit:
            // https://github.com/sfackler/rust-postgres/commit/cb609be758f3fb5af537f04b584a2ee0cebd5e79
            // => we should rebase our fork => TODO https://github.com/neondatabase/neon/issues/8399
            .user(&username())
            .dbname(DB_NAME)
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(anyhow::Error::new)
    }

    /// Wrapper for the pg_ctl binary, which we spawn as a short-lived subprocess when starting and stopping postgres
    async fn pg_ctl<I, S>(&self, args: I) -> ExitStatus
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let pg_bin_dir = self.get_pg_bin_dir().await.unwrap();
        let bin_path = pg_bin_dir.join("pg_ctl");

        let pg_lib_dir = self.get_pg_lib_dir().await.unwrap();
        let envs = [
            ("LD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
            ("DYLD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
        ];

        Command::new(bin_path)
            .args(args)
            .envs(envs)
            .spawn()
            .expect("Failed to spawn pg_ctl, binary_missing?")
            .wait()
            .await
            .expect("Failed to wait for pg_ctl termination")
    }

    pub async fn start(&self, start_args: NeonStorageControllerStartArgs) -> anyhow::Result<()> {
        let instance_dir = self.storage_controller_instance_dir(start_args.instance_id);
        if let Err(err) = tokio::fs::create_dir(&instance_dir).await {
            if err.kind() != std::io::ErrorKind::AlreadyExists {
                panic!("Failed to create instance dir {instance_dir:?}");
            }
        }

        let (listen, postgres_port) = {
            if let Some(base_port) = start_args.base_port {
                (
                    format!("127.0.0.1:{base_port}"),
                    self.config
                        .database_url
                        .expect("--base-port requires NeonStorageControllerConf::database_url")
                        .port(),
                )
            } else {
                let listen_url = self.env.control_plane_api.clone();

                let listen = format!(
                    "{}:{}",
                    listen_url.host_str().unwrap(),
                    listen_url.port().unwrap()
                );

                (listen, listen_url.port().unwrap() + 1)
            }
        };

        let socket_addr = listen
            .parse()
            .expect("listen address is a valid socket address");
        self.listen
            .set(socket_addr)
            .expect("StorageController::listen is only set here");

        // Do we remove the pid file on stop?
        let pg_started = self.is_postgres_running().await?;
        let pg_lib_dir = self.get_pg_lib_dir().await?;

        if !pg_started {
            // Start a vanilla Postgres process used by the storage controller for persistence.
            let pg_data_path = Utf8PathBuf::from_path_buf(self.env.base_data_dir.clone())
                .unwrap()
                .join("storage_controller_db");
            let pg_bin_dir = self.get_pg_bin_dir().await?;
            let pg_log_path = pg_data_path.join("postgres.log");

            if !tokio::fs::try_exists(&pg_data_path).await? {
                let initdb_args = [
                    "--pgdata",
                    pg_data_path.as_ref(),
                    "--username",
                    &username(),
                    "--no-sync",
                    "--no-instructions",
                ];
                tracing::info!(
                    "Initializing storage controller database with args: {:?}",
                    initdb_args
                );

                // Initialize empty database
                let initdb_path = pg_bin_dir.join("initdb");
                let mut child = Command::new(&initdb_path)
                    .envs(vec![
                        ("LD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
                        ("DYLD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
                    ])
                    .args(initdb_args)
                    .spawn()
                    .expect("Failed to spawn initdb");
                let status = child.wait().await?;
                if !status.success() {
                    anyhow::bail!("initdb failed with status {status}");
                }
            };

            // Write a minimal config file:
            // - Specify the port, since this is chosen dynamically
            // - Switch off fsync, since we're running on lightweight test environments and when e.g. scale testing
            //   the storage controller we don't want a slow local disk to interfere with that.
            //
            // NB: it's important that we rewrite this file on each start command so we propagate changes
            // from `LocalEnv`'s config file (`.neon/config`).
            tokio::fs::write(
                &pg_data_path.join("postgresql.conf"),
                format!("port = {}\nfsync=off\n", postgres_port),
            )
            .await?;

            println!("Starting storage controller database...");
            let db_start_args = [
                "-w",
                "-D",
                pg_data_path.as_ref(),
                "-l",
                pg_log_path.as_ref(),
                "-U",
                &username(),
                "start",
            ];
            tracing::info!(
                "Starting storage controller database with args: {:?}",
                db_start_args
            );

            let db_start_status = self.pg_ctl(db_start_args).await;
            let start_timeout: Duration = start_args.start_timeout.into();
            let db_start_deadline = Instant::now() + start_timeout;
            if !db_start_status.success() {
                return Err(anyhow::anyhow!(
                    "Failed to start postgres {}",
                    db_start_status.code().unwrap()
                ));
            }

            loop {
                if Instant::now() > db_start_deadline {
                    return Err(anyhow::anyhow!("Timed out waiting for postgres to start"));
                }

                match self.pg_isready(&pg_bin_dir, postgres_port).await {
                    Ok(true) => {
                        tracing::info!("storage controller postgres is now ready");
                        break;
                    }
                    Ok(false) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to check postgres status: {e}")
                    }
                }
            }

            self.setup_database(postgres_port).await?;
        }

        let database_url = format!("postgresql://localhost:{}/{DB_NAME}", postgres_port);

        // We support running a startup SQL script to fiddle with the database before we launch storcon.
        // This is used by the test suite.
        let startup_script_path = self
            .env
            .base_data_dir
            .join("storage_controller_db.startup.sql");
        let startup_script = match tokio::fs::read_to_string(&startup_script_path).await {
            Ok(script) => {
                tokio::fs::remove_file(startup_script_path).await?;
                script
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    // always run some startup script so that this code path doesn't bit rot
                    "BEGIN; COMMIT;".to_string()
                } else {
                    anyhow::bail!("Failed to read startup script: {e}")
                }
            }
        };
        let (mut client, conn) = self.connect_to_database(postgres_port).await?;
        let conn = tokio::spawn(conn);
        let tx = client.build_transaction();
        let tx = tx.start().await?;
        tx.batch_execute(&startup_script).await?;
        tx.commit().await?;
        drop(client);
        conn.await??;

        let listen = self
            .listen
            .get()
            .expect("cell is set earlier in this function");
        let address_for_peers = Uri::builder()
            .scheme("http")
            .authority(format!("{}:{}", listen.ip(), listen.port()))
            .path_and_query("")
            .build()
            .unwrap();

        let mut args = vec![
            "-l",
            &listen.to_string(),
            "--dev",
            "--database-url",
            &database_url,
            "--max-offline-interval",
            &humantime::Duration::from(self.config.max_offline).to_string(),
            "--max-warming-up-interval",
            &humantime::Duration::from(self.config.max_warming_up).to_string(),
            "--heartbeat-interval",
            &humantime::Duration::from(self.config.heartbeat_interval).to_string(),
            "--address-for-peers",
            &address_for_peers.to_string(),
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

        if self.config.start_as_candidate {
            args.push("--start-as-candidate".to_string());
        }

        if let Some(private_key) = &self.private_key {
            let claims = Claims::new(None, Scope::PageServerApi);
            let jwt_token =
                encode_from_key_file(&claims, private_key).expect("failed to generate jwt token");
            args.push(format!("--jwt-token={jwt_token}"));

            let peer_claims = Claims::new(None, Scope::Admin);
            let peer_jwt_token = encode_from_key_file(&peer_claims, private_key)
                .expect("failed to generate jwt token");
            args.push(format!("--peer-jwt-token={peer_jwt_token}"));
        }

        if let Some(public_key) = &self.public_key {
            args.push(format!("--public-key=\"{public_key}\""));
        }

        if let Some(control_plane_compute_hook_api) = &self.env.control_plane_compute_hook_api {
            args.push(format!(
                "--compute-hook-url={control_plane_compute_hook_api}"
            ));
        }

        if let Some(split_threshold) = self.config.split_threshold.as_ref() {
            args.push(format!("--split-threshold={split_threshold}"))
        }

        if let Some(lag) = self.config.max_secondary_lag_bytes.as_ref() {
            args.push(format!("--max-secondary-lag-bytes={lag}"))
        }

        if let Some(threshold) = self.config.long_reconcile_threshold {
            args.push(format!(
                "--long-reconcile-threshold={}",
                humantime::Duration::from(threshold)
            ))
        }

        args.push(format!(
            "--neon-local-repo-dir={}",
            self.env.base_data_dir.display()
        ));

        background_process::start_process(
            COMMAND,
            &instance_dir,
            &self.env.storage_controller_bin(),
            args,
            vec![
                ("LD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
                ("DYLD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
            ],
            background_process::InitialPidFile::Create(self.pid_file(start_args.instance_id)),
            &start_args.start_timeout,
            || async {
                match self.ready().await {
                    Ok(_) => Ok(true),
                    Err(_) => Ok(false),
                }
            },
        )
        .await?;

        Ok(())
    }

    pub async fn stop(&self, stop_args: NeonStorageControllerStopArgs) -> anyhow::Result<()> {
        background_process::stop_process(
            stop_args.immediate,
            COMMAND,
            &self.pid_file(stop_args.instance_id),
        )?;

        let storcon_instances = self.env.storage_controller_instances().await?;
        for (instance_id, instanced_dir_path) in storcon_instances {
            if instance_id == stop_args.instance_id {
                continue;
            }

            let pid_file = instanced_dir_path.join("storage_controller.pid");
            let pid = tokio::fs::read_to_string(&pid_file)
                .await
                .map_err(|err| {
                    anyhow::anyhow!("Failed to read storcon pid file at {pid_file:?}: {err}")
                })?
                .parse::<i32>()
                .expect("pid is valid i32");

            let other_proc_alive = !background_process::process_has_stopped(Pid::from_raw(pid))?;
            if other_proc_alive {
                // There is another storage controller instance running, so we return
                // and leave the database running.
                return Ok(());
            }
        }

        let pg_data_path = self.env.base_data_dir.join("storage_controller_db");

        println!("Stopping storage controller database...");
        let pg_stop_args = ["-D", &pg_data_path.to_string_lossy(), "stop"];
        let stop_status = self.pg_ctl(pg_stop_args).await;
        if !stop_status.success() {
            match self.is_postgres_running().await {
                Ok(false) => {
                    println!("Storage controller database is already stopped");
                    return Ok(());
                }
                Ok(true) => {
                    anyhow::bail!("Failed to stop storage controller database");
                }
                Err(err) => {
                    anyhow::bail!("Failed to stop storage controller database: {err}");
                }
            }
        }

        Ok(())
    }

    async fn is_postgres_running(&self) -> anyhow::Result<bool> {
        let pg_data_path = self.env.base_data_dir.join("storage_controller_db");

        let pg_status_args = ["-D", &pg_data_path.to_string_lossy(), "status"];
        let status_exitcode = self.pg_ctl(pg_status_args).await;

        // pg_ctl status returns this exit code if postgres is not running: in this case it is
        // fine that stop failed.  Otherwise it is an error that stop failed.
        const PG_STATUS_NOT_RUNNING: i32 = 3;
        const PG_NO_DATA_DIR: i32 = 4;
        const PG_STATUS_RUNNING: i32 = 0;
        match status_exitcode.code() {
            Some(PG_STATUS_NOT_RUNNING) => Ok(false),
            Some(PG_NO_DATA_DIR) => Ok(false),
            Some(PG_STATUS_RUNNING) => Ok(true),
            Some(code) => Err(anyhow::anyhow!(
                "pg_ctl status returned unexpected status code: {:?}",
                code
            )),
            None => Err(anyhow::anyhow!("pg_ctl status returned no status code")),
        }
    }

    fn get_claims_for_path(path: &str) -> anyhow::Result<Option<Claims>> {
        let category = match path.find('/') {
            Some(idx) => &path[..idx],
            None => path,
        };

        match category {
            "status" | "ready" => Ok(None),
            "control" | "debug" => Ok(Some(Claims::new(None, Scope::Admin))),
            "v1" => Ok(Some(Claims::new(None, Scope::PageServerApi))),
            _ => Err(anyhow::anyhow!("Failed to determine claims for {}", path)),
        }
    }

    /// Simple HTTP request wrapper for calling into storage controller
    async fn dispatch<RQ, RS>(
        &self,
        method: reqwest::Method,
        path: String,
        body: Option<RQ>,
    ) -> anyhow::Result<RS>
    where
        RQ: Serialize + Sized,
        RS: DeserializeOwned + Sized,
    {
        // In the special case of the `storage_controller start` subcommand, we wish
        // to use the API endpoint of the newly started storage controller in order
        // to pass the readiness check. In this scenario [`Self::listen`] will be set
        // (see [`Self::start`]).
        //
        // Otherwise, we infer the storage controller api endpoint from the configured
        // control plane API.
        let url = if let Some(socket_addr) = self.listen.get() {
            Url::from_str(&format!(
                "http://{}:{}/{path}",
                socket_addr.ip().to_canonical(),
                socket_addr.port()
            ))
            .unwrap()
        } else {
            // The configured URL has the /upcall path prefix for pageservers to use: we will strip that out
            // for general purpose API access.
            let listen_url = self.env.control_plane_api.clone();
            Url::from_str(&format!(
                "http://{}:{}/{path}",
                listen_url.host_str().unwrap(),
                listen_url.port().unwrap()
            ))
            .unwrap()
        };

        let mut builder = self.client.request(method, url);
        if let Some(body) = body {
            builder = builder.json(&body)
        }
        if let Some(private_key) = &self.private_key {
            println!("Getting claims for path {}", path);
            if let Some(required_claims) = Self::get_claims_for_path(&path)? {
                println!("Got claims {:?} for path {}", required_claims, path);
                let jwt_token = encode_from_key_file(&required_claims, private_key)?;
                builder = builder.header(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {jwt_token}"),
                );
            }
        }

        let response = builder.send().await?;
        let response = response.error_from_body().await?;

        Ok(response
            .json()
            .await
            .map_err(pageserver_client::mgmt_api::Error::ReceiveBody)?)
    }

    /// Call into the attach_hook API, for use before handing out attachments to pageservers
    #[instrument(skip(self))]
    pub async fn attach_hook(
        &self,
        tenant_shard_id: TenantShardId,
        pageserver_id: NodeId,
    ) -> anyhow::Result<Option<u32>> {
        let request = AttachHookRequest {
            tenant_shard_id,
            node_id: Some(pageserver_id),
            generation_override: None,
        };

        let response = self
            .dispatch::<_, AttachHookResponse>(
                Method::POST,
                "debug/v1/attach-hook".to_string(),
                Some(request),
            )
            .await?;

        Ok(response.gen)
    }

    #[instrument(skip(self))]
    pub async fn inspect(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> anyhow::Result<Option<(u32, NodeId)>> {
        let request = InspectRequest { tenant_shard_id };

        let response = self
            .dispatch::<_, InspectResponse>(
                Method::POST,
                "debug/v1/inspect".to_string(),
                Some(request),
            )
            .await?;

        Ok(response.attachment)
    }

    #[instrument(skip(self))]
    pub async fn tenant_create(
        &self,
        req: TenantCreateRequest,
    ) -> anyhow::Result<TenantCreateResponse> {
        self.dispatch(Method::POST, "v1/tenant".to_string(), Some(req))
            .await
    }

    #[instrument(skip(self))]
    pub async fn tenant_import(&self, tenant_id: TenantId) -> anyhow::Result<TenantCreateResponse> {
        self.dispatch::<(), TenantCreateResponse>(
            Method::POST,
            format!("debug/v1/tenant/{tenant_id}/import"),
            None,
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn tenant_locate(&self, tenant_id: TenantId) -> anyhow::Result<TenantLocateResponse> {
        self.dispatch::<(), _>(
            Method::GET,
            format!("debug/v1/tenant/{tenant_id}/locate"),
            None,
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn tenant_migrate(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
    ) -> anyhow::Result<TenantShardMigrateResponse> {
        self.dispatch(
            Method::PUT,
            format!("control/v1/tenant/{tenant_shard_id}/migrate"),
            Some(TenantShardMigrateRequest {
                tenant_shard_id,
                node_id,
            }),
        )
        .await
    }

    #[instrument(skip(self), fields(%tenant_id, %new_shard_count))]
    pub async fn tenant_split(
        &self,
        tenant_id: TenantId,
        new_shard_count: u8,
        new_stripe_size: Option<ShardStripeSize>,
    ) -> anyhow::Result<TenantShardSplitResponse> {
        self.dispatch(
            Method::PUT,
            format!("control/v1/tenant/{tenant_id}/shard_split"),
            Some(TenantShardSplitRequest {
                new_shard_count,
                new_stripe_size,
            }),
        )
        .await
    }

    #[instrument(skip_all, fields(node_id=%req.node_id))]
    pub async fn node_register(&self, req: NodeRegisterRequest) -> anyhow::Result<()> {
        self.dispatch::<_, ()>(Method::POST, "control/v1/node".to_string(), Some(req))
            .await
    }

    #[instrument(skip_all, fields(node_id=%req.node_id))]
    pub async fn node_configure(&self, req: NodeConfigureRequest) -> anyhow::Result<()> {
        self.dispatch::<_, ()>(
            Method::PUT,
            format!("control/v1/node/{}/config", req.node_id),
            Some(req),
        )
        .await
    }

    pub async fn node_list(&self) -> anyhow::Result<Vec<NodeDescribeResponse>> {
        self.dispatch::<(), Vec<NodeDescribeResponse>>(
            Method::GET,
            "control/v1/node".to_string(),
            None,
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn ready(&self) -> anyhow::Result<()> {
        self.dispatch::<(), ()>(Method::GET, "ready".to_string(), None)
            .await
    }

    #[instrument(skip_all, fields(%tenant_id, timeline_id=%req.new_timeline_id))]
    pub async fn tenant_timeline_create(
        &self,
        tenant_id: TenantId,
        req: TimelineCreateRequest,
    ) -> anyhow::Result<TimelineInfo> {
        self.dispatch(
            Method::POST,
            format!("v1/tenant/{tenant_id}/timeline"),
            Some(req),
        )
        .await
    }
}
