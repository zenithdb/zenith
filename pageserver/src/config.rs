//! Functions for handling page server configuration options
//!
//! Configuration options can be set in the pageserver.toml configuration
//! file, or on the command line.
//! See also `settings.md` for better description on every parameter.

use anyhow::{bail, ensure, Context};
use pageserver_api::models::ImageCompressionAlgorithm;
use pageserver_api::{
    config::{DiskUsageEvictionTaskConfig, MaxVectoredReadBytes},
    shard::TenantShardId,
};
use remote_storage::{RemotePath, RemoteStorageConfig};
use std::env;
use storage_broker::Uri;
use utils::logging::SecretString;
use utils::postgres_client::PostgresClientProtocol;

use once_cell::sync::OnceCell;
use reqwest::Url;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use camino::{Utf8Path, Utf8PathBuf};
use postgres_backend::AuthType;
use utils::{
    id::{NodeId, TimelineId},
    logging::LogFormat,
};

use crate::tenant::storage_layer::inmemory_layer::IndexEntry;
use crate::tenant::{TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME};
use crate::virtual_file;
use crate::virtual_file::io_engine;
use crate::{TENANT_HEATMAP_BASENAME, TENANT_LOCATION_CONFIG_NAME};

/// Global state of pageserver.
///
/// It's mostly immutable configuration, but some semaphores and the
/// like crept in over time and the name stuck.
///
/// Instantiated by deserializing `pageserver.toml` into  [`pageserver_api::config::ConfigToml`]
/// and passing that to [`PageServerConf::parse_and_validate`].
///
/// # Adding a New Field
///
/// 1. Add the field to `pageserver_api::config::ConfigToml`.
/// 2. Fix compiler errors (exhaustive destructuring will guide you).
///
/// For fields that require additional validation or filling in of defaults at runtime,
/// check for examples in the [`PageServerConf::parse_and_validate`] method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageServerConf {
    // Identifier of that particular pageserver so e g safekeepers
    // can safely distinguish different pageservers
    pub id: NodeId,

    /// Example (default): 127.0.0.1:64000
    pub listen_pg_addr: String,
    /// Example (default): 127.0.0.1:9898
    pub listen_http_addr: String,

    /// Current availability zone. Used for traffic metrics.
    pub availability_zone: Option<String>,

    // Timeout when waiting for WAL receiver to catch up to an LSN given in a GetPage@LSN call.
    pub wait_lsn_timeout: Duration,
    // How long to wait for WAL redo to complete.
    pub wal_redo_timeout: Duration,

    pub superuser: String,
    pub locale: String,

    pub page_cache_size: usize,
    pub max_file_descriptors: usize,

    // Repository directory, relative to current working directory.
    // Normally, the page server changes the current working directory
    // to the repository, and 'workdir' is always '.'. But we don't do
    // that during unit testing, because the current directory is global
    // to the process but different unit tests work on different
    // repositories.
    pub workdir: Utf8PathBuf,

    pub pg_distrib_dir: Utf8PathBuf,

    // Authentication
    /// authentication method for the HTTP mgmt API
    pub http_auth_type: AuthType,
    /// authentication method for libpq connections from compute
    pub pg_auth_type: AuthType,
    /// Path to a file or directory containing public key(s) for verifying JWT tokens.
    /// Used for both mgmt and compute auth, if enabled.
    pub auth_validation_public_key_path: Option<Utf8PathBuf>,

    pub remote_storage_config: Option<RemoteStorageConfig>,

    pub default_tenant_conf: crate::tenant::config::TenantConf,

    /// Storage broker endpoints to connect to.
    pub broker_endpoint: Uri,
    pub broker_keepalive_interval: Duration,

    pub log_format: LogFormat,

    /// Number of tenants which will be concurrently loaded from remote storage proactively on startup or attach.
    ///
    /// A lower value implicitly deprioritizes loading such tenants, vs. other work in the system.
    pub concurrent_tenant_warmup: ConfigurableSemaphore,

    /// Number of concurrent [`TenantShard::gather_size_inputs`](crate::tenant::TenantShard::gather_size_inputs) allowed.
    pub concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore,
    /// Limit of concurrent [`TenantShard::gather_size_inputs`] issued by module `eviction_task`.
    /// The number of permits is the same as `concurrent_tenant_size_logical_size_queries`.
    /// See the comment in `eviction_task` for details.
    ///
    /// [`TenantShard::gather_size_inputs`]: crate::tenant::TenantShard::gather_size_inputs
    pub eviction_task_immitated_concurrent_logical_size_queries: ConfigurableSemaphore,

    // How often to collect metrics and send them to the metrics endpoint.
    pub metric_collection_interval: Duration,
    // How often to send unchanged cached metrics to the metrics endpoint.
    pub metric_collection_endpoint: Option<Url>,
    pub metric_collection_bucket: Option<RemoteStorageConfig>,
    pub synthetic_size_calculation_interval: Duration,

    pub disk_usage_based_eviction: Option<DiskUsageEvictionTaskConfig>,

    pub test_remote_failures: u64,

    pub ondemand_download_behavior_treat_error_as_warn: bool,

    /// How long will background tasks be delayed at most after initial load of tenants.
    ///
    /// Our largest initialization completions are in the range of 100-200s, so perhaps 10s works
    /// as we now isolate initial loading, initial logical size calculation and background tasks.
    /// Smaller nodes will have background tasks "not running" for this long unless every timeline
    /// has it's initial logical size calculated. Not running background tasks for some seconds is
    /// not terrible.
    pub background_task_maximum_delay: Duration,

    pub control_plane_api: Option<Url>,

    /// JWT token for use with the control plane API.
    pub control_plane_api_token: Option<SecretString>,

    pub import_pgdata_upcall_api: Option<Url>,
    pub import_pgdata_upcall_api_token: Option<SecretString>,
    pub import_pgdata_aws_endpoint_url: Option<Url>,

    /// If true, pageserver will make best-effort to operate without a control plane: only
    /// for use in major incidents.
    pub control_plane_emergency_mode: bool,

    /// How many heatmap uploads may be done concurrency: lower values implicitly deprioritize
    /// heatmap uploads vs. other remote storage operations.
    pub heatmap_upload_concurrency: usize,

    /// How many remote storage downloads may be done for secondary tenants concurrently.  Implicitly
    /// deprioritises secondary downloads vs. remote storage operations for attached tenants.
    pub secondary_download_concurrency: usize,

    /// Maximum number of WAL records to be ingested and committed at the same time
    pub ingest_batch_size: u64,

    pub virtual_file_io_engine: virtual_file::IoEngineKind,

    pub max_vectored_read_bytes: MaxVectoredReadBytes,

    pub image_compression: ImageCompressionAlgorithm,

    /// Whether to offload archived timelines automatically
    pub timeline_offloading: bool,

    /// How many bytes of ephemeral layer content will we allow per kilobyte of RAM.  When this
    /// is exceeded, we start proactively closing ephemeral layers to limit the total amount
    /// of ephemeral data.
    ///
    /// Setting this to zero disables limits on total ephemeral layer size.
    pub ephemeral_bytes_per_memory_kb: usize,

    pub l0_flush: crate::l0_flush::L0FlushConfig,

    /// Direct IO settings
    pub virtual_file_io_mode: virtual_file::IoMode,

    /// Optionally disable disk syncs (unsafe!)
    pub no_sync: bool,

    pub wal_receiver_protocol: PostgresClientProtocol,

    pub page_service_pipelining: pageserver_api::config::PageServicePipeliningConfig,
}

/// Token for authentication to safekeepers
///
/// We do not want to store this in a PageServerConf because the latter may be logged
/// and/or serialized at a whim, while the token is secret. Currently this token is the
/// same for accessing all tenants/timelines, but may become per-tenant/per-timeline in
/// the future, more tokens and auth may arrive for storage broker, completely changing the logic.
/// Hence, we resort to a global variable for now instead of passing the token from the
/// startup code to the connection code through a dozen layers.
pub static SAFEKEEPER_AUTH_TOKEN: OnceCell<Arc<String>> = OnceCell::new();

impl PageServerConf {
    //
    // Repository paths, relative to workdir.
    //

    pub fn tenants_path(&self) -> Utf8PathBuf {
        self.workdir.join(TENANTS_SEGMENT_NAME)
    }

    pub fn deletion_prefix(&self) -> Utf8PathBuf {
        self.workdir.join("deletion")
    }

    pub fn metadata_path(&self) -> Utf8PathBuf {
        self.workdir.join("metadata.json")
    }

    pub fn deletion_list_path(&self, sequence: u64) -> Utf8PathBuf {
        // Encode a version in the filename, so that if we ever switch away from JSON we can
        // increment this.
        const VERSION: u8 = 1;

        self.deletion_prefix()
            .join(format!("{sequence:016x}-{VERSION:02x}.list"))
    }

    pub fn deletion_header_path(&self) -> Utf8PathBuf {
        // Encode a version in the filename, so that if we ever switch away from JSON we can
        // increment this.
        const VERSION: u8 = 1;

        self.deletion_prefix().join(format!("header-{VERSION:02x}"))
    }

    pub fn tenant_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenants_path().join(tenant_shard_id.to_string())
    }

    /// Points to a place in pageserver's local directory,
    /// where certain tenant's LocationConf be stored.
    pub(crate) fn tenant_location_config_path(
        &self,
        tenant_shard_id: &TenantShardId,
    ) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(TENANT_LOCATION_CONFIG_NAME)
    }

    pub(crate) fn tenant_heatmap_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(TENANT_HEATMAP_BASENAME)
    }

    pub fn timelines_path(&self, tenant_shard_id: &TenantShardId) -> Utf8PathBuf {
        self.tenant_path(tenant_shard_id)
            .join(TIMELINES_SEGMENT_NAME)
    }

    pub fn timeline_path(
        &self,
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
    ) -> Utf8PathBuf {
        self.timelines_path(tenant_shard_id)
            .join(timeline_id.to_string())
    }

    /// Turns storage remote path of a file into its local path.
    pub fn local_path(&self, remote_path: &RemotePath) -> Utf8PathBuf {
        remote_path.with_base(&self.workdir)
    }

    //
    // Postgres distribution paths
    //
    pub fn pg_distrib_dir(&self, pg_version: u32) -> anyhow::Result<Utf8PathBuf> {
        let path = self.pg_distrib_dir.clone();

        #[allow(clippy::manual_range_patterns)]
        match pg_version {
            14 | 15 | 16 | 17 => Ok(path.join(format!("v{pg_version}"))),
            _ => bail!("Unsupported postgres version: {}", pg_version),
        }
    }

    pub fn pg_bin_dir(&self, pg_version: u32) -> anyhow::Result<Utf8PathBuf> {
        Ok(self.pg_distrib_dir(pg_version)?.join("bin"))
    }
    pub fn pg_lib_dir(&self, pg_version: u32) -> anyhow::Result<Utf8PathBuf> {
        Ok(self.pg_distrib_dir(pg_version)?.join("lib"))
    }

    /// Parse a configuration file (pageserver.toml) into a PageServerConf struct,
    /// validating the input and failing on errors.
    ///
    /// This leaves any options not present in the file in the built-in defaults.
    pub fn parse_and_validate(
        id: NodeId,
        config_toml: pageserver_api::config::ConfigToml,
        workdir: &Utf8Path,
    ) -> anyhow::Result<Self> {
        let pageserver_api::config::ConfigToml {
            listen_pg_addr,
            listen_http_addr,
            availability_zone,
            wait_lsn_timeout,
            wal_redo_timeout,
            superuser,
            locale,
            page_cache_size,
            max_file_descriptors,
            pg_distrib_dir,
            http_auth_type,
            pg_auth_type,
            auth_validation_public_key_path,
            remote_storage,
            broker_endpoint,
            broker_keepalive_interval,
            log_format,
            metric_collection_interval,
            metric_collection_endpoint,
            metric_collection_bucket,
            synthetic_size_calculation_interval,
            disk_usage_based_eviction,
            test_remote_failures,
            ondemand_download_behavior_treat_error_as_warn,
            background_task_maximum_delay,
            control_plane_api,
            control_plane_api_token,
            control_plane_emergency_mode,
            import_pgdata_upcall_api,
            import_pgdata_upcall_api_token,
            import_pgdata_aws_endpoint_url,
            heatmap_upload_concurrency,
            secondary_download_concurrency,
            ingest_batch_size,
            max_vectored_read_bytes,
            image_compression,
            timeline_offloading,
            ephemeral_bytes_per_memory_kb,
            l0_flush,
            virtual_file_io_mode,
            concurrent_tenant_warmup,
            concurrent_tenant_size_logical_size_queries,
            virtual_file_io_engine,
            tenant_config,
            no_sync,
            wal_receiver_protocol,
            page_service_pipelining,
        } = config_toml;

        let mut conf = PageServerConf {
            // ------------------------------------------------------------
            // fields that are already fully validated by the ConfigToml Deserialize impl
            // ------------------------------------------------------------
            listen_pg_addr,
            listen_http_addr,
            availability_zone,
            wait_lsn_timeout,
            wal_redo_timeout,
            superuser,
            locale,
            page_cache_size,
            max_file_descriptors,
            http_auth_type,
            pg_auth_type,
            auth_validation_public_key_path,
            remote_storage_config: remote_storage,
            broker_endpoint,
            broker_keepalive_interval,
            log_format,
            metric_collection_interval,
            metric_collection_endpoint,
            metric_collection_bucket,
            synthetic_size_calculation_interval,
            disk_usage_based_eviction,
            test_remote_failures,
            ondemand_download_behavior_treat_error_as_warn,
            background_task_maximum_delay,
            control_plane_api,
            control_plane_emergency_mode,
            heatmap_upload_concurrency,
            secondary_download_concurrency,
            ingest_batch_size,
            max_vectored_read_bytes,
            image_compression,
            timeline_offloading,
            ephemeral_bytes_per_memory_kb,
            import_pgdata_upcall_api,
            import_pgdata_upcall_api_token: import_pgdata_upcall_api_token.map(SecretString::from),
            import_pgdata_aws_endpoint_url,
            wal_receiver_protocol,
            page_service_pipelining,

            // ------------------------------------------------------------
            // fields that require additional validation or custom handling
            // ------------------------------------------------------------
            workdir: workdir.to_owned(),
            pg_distrib_dir: pg_distrib_dir.unwrap_or_else(|| {
                std::env::current_dir()
                    .expect("current_dir() failed")
                    .try_into()
                    .expect("current_dir() is not a valid Utf8Path")
            }),
            control_plane_api_token: control_plane_api_token.map(SecretString::from),
            id,
            default_tenant_conf: tenant_config,
            concurrent_tenant_warmup: ConfigurableSemaphore::new(concurrent_tenant_warmup),
            concurrent_tenant_size_logical_size_queries: ConfigurableSemaphore::new(
                concurrent_tenant_size_logical_size_queries,
            ),
            eviction_task_immitated_concurrent_logical_size_queries: ConfigurableSemaphore::new(
                // re-use `concurrent_tenant_size_logical_size_queries`
                concurrent_tenant_size_logical_size_queries,
            ),
            virtual_file_io_engine: match virtual_file_io_engine {
                Some(v) => v,
                None => match crate::virtual_file::io_engine_feature_test()
                    .context("auto-detect virtual_file_io_engine")?
                {
                    io_engine::FeatureTestResult::PlatformPreferred(v) => v, // make no noise
                    io_engine::FeatureTestResult::Worse { engine, remark } => {
                        // TODO: bubble this up to the caller so we can tracing::warn! it.
                        eprintln!("auto-detected IO engine is not platform-preferred: engine={engine:?} remark={remark:?}");
                        engine
                    }
                },
            },
            l0_flush: l0_flush
                .map(crate::l0_flush::L0FlushConfig::from)
                .unwrap_or_default(),
            virtual_file_io_mode: virtual_file_io_mode.unwrap_or(virtual_file::IoMode::preferred()),
            no_sync: no_sync.unwrap_or(false),
        };

        // ------------------------------------------------------------
        // custom validation code that covers more than one field in isolation
        // ------------------------------------------------------------

        if conf.http_auth_type == AuthType::NeonJWT || conf.pg_auth_type == AuthType::NeonJWT {
            let auth_validation_public_key_path = conf
                .auth_validation_public_key_path
                .get_or_insert_with(|| workdir.join("auth_public_key.pem"));
            ensure!(
                auth_validation_public_key_path.exists(),
                format!(
                    "Can't find auth_validation_public_key at '{auth_validation_public_key_path}'",
                )
            );
        }

        IndexEntry::validate_checkpoint_distance(conf.default_tenant_conf.checkpoint_distance)
            .map_err(anyhow::Error::msg)
            .with_context(|| {
                format!(
                    "effective checkpoint distance is unsupported: {}",
                    conf.default_tenant_conf.checkpoint_distance
                )
            })?;

        Ok(conf)
    }

    #[cfg(test)]
    pub fn test_repo_dir(test_name: &str) -> Utf8PathBuf {
        let test_output_dir = std::env::var("TEST_OUTPUT").unwrap_or("../tmp_check".into());
        Utf8PathBuf::from(format!("{test_output_dir}/test_{test_name}"))
    }

    pub fn dummy_conf(repo_dir: Utf8PathBuf) -> Self {
        let pg_distrib_dir = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../pg_install");

        let config_toml = pageserver_api::config::ConfigToml {
            wait_lsn_timeout: Duration::from_secs(60),
            wal_redo_timeout: Duration::from_secs(60),
            pg_distrib_dir: Some(pg_distrib_dir),
            metric_collection_interval: Duration::from_secs(60),
            synthetic_size_calculation_interval: Duration::from_secs(60),
            background_task_maximum_delay: Duration::ZERO,
            ..Default::default()
        };
        PageServerConf::parse_and_validate(NodeId(0), config_toml, &repo_dir).unwrap()
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PageserverIdentity {
    pub id: NodeId,
}

/// Configurable semaphore permits setting.
///
/// Does not allow semaphore permits to be zero, because at runtime initially zero permits and empty
/// semaphore cannot be distinguished, leading any feature using these to await forever (or until
/// new permits are added).
#[derive(Debug, Clone)]
pub struct ConfigurableSemaphore {
    initial_permits: NonZeroUsize,
    inner: std::sync::Arc<tokio::sync::Semaphore>,
}

impl ConfigurableSemaphore {
    /// Initializse using a non-zero amount of permits.
    ///
    /// Require a non-zero initial permits, because using permits == 0 is a crude way to disable a
    /// feature such as [`TenantShard::gather_size_inputs`]. Otherwise any semaphore using future will
    /// behave like [`futures::future::pending`], just waiting until new permits are added.
    ///
    /// [`TenantShard::gather_size_inputs`]: crate::tenant::TenantShard::gather_size_inputs
    pub fn new(initial_permits: NonZeroUsize) -> Self {
        ConfigurableSemaphore {
            initial_permits,
            inner: std::sync::Arc::new(tokio::sync::Semaphore::new(initial_permits.get())),
        }
    }

    /// Returns the configured amount of permits.
    pub fn initial_permits(&self) -> NonZeroUsize {
        self.initial_permits
    }
}

impl PartialEq for ConfigurableSemaphore {
    fn eq(&self, other: &Self) -> bool {
        // the number of permits can be increased at runtime, so we cannot really fulfill the
        // PartialEq value equality otherwise
        self.initial_permits == other.initial_permits
    }
}

impl Eq for ConfigurableSemaphore {}

impl ConfigurableSemaphore {
    pub fn inner(&self) -> &std::sync::Arc<tokio::sync::Semaphore> {
        &self.inner
    }
}

#[cfg(test)]
mod tests {

    use camino::Utf8PathBuf;
    use utils::id::NodeId;

    use super::PageServerConf;

    #[test]
    fn test_empty_config_toml_is_valid() {
        // we use Default impl of everything in this situation
        let input = r#"
        "#;
        let config_toml = toml_edit::de::from_str::<pageserver_api::config::ConfigToml>(input)
            .expect("empty config is valid");
        let workdir = Utf8PathBuf::from("/nonexistent");
        PageServerConf::parse_and_validate(NodeId(0), config_toml, &workdir)
            .expect("parse_and_validate");
    }

    /// If there's a typo in the pageserver config, we'd rather catch that typo
    /// and fail pageserver startup than silently ignoring the typo, leaving whoever
    /// made it in the believe that their config change is effective.
    ///
    /// The default in serde is to allow unknown fields, so, we rely
    /// on developer+review discipline to add `deny_unknown_fields` when adding
    /// new structs to the config, and these tests here as a regression test.
    ///
    /// The alternative to all of this would be to allow unknown fields in the config.
    /// To catch them, we could have a config check tool or mgmt API endpoint that
    /// compares the effective config with the TOML on disk and makes sure that
    /// the on-disk TOML is a strict subset of the effective config.
    mod unknown_fields_handling {
        macro_rules! test {
            ($short_name:ident, $input:expr) => {
                #[test]
                fn $short_name() {
                    let input = $input;
                    let err = toml_edit::de::from_str::<pageserver_api::config::ConfigToml>(&input)
                        .expect_err("some_invalid_field is an invalid field");
                    dbg!(&err);
                    assert!(err.to_string().contains("some_invalid_field"));
                }
            };
        }
        use indoc::indoc;

        test!(
            toplevel,
            indoc! {r#"
                some_invalid_field = 23
            "#}
        );

        test!(
            toplevel_nested,
            indoc! {r#"
                [some_invalid_field]
                foo = 23
            "#}
        );

        test!(
            disk_usage_based_eviction,
            indoc! {r#"
                [disk_usage_based_eviction]
                some_invalid_field = 23
            "#}
        );

        test!(
            tenant_config,
            indoc! {r#"
                [tenant_config]
                some_invalid_field = 23
            "#}
        );

        test!(
            l0_flush,
            indoc! {r#"
                [l0_flush]
                mode = "direct"
                some_invalid_field = 23
            "#}
        );

        // TODO: fix this => https://github.com/neondatabase/neon/issues/8915
        // test!(
        //     remote_storage_config,
        //     indoc! {r#"
        //         [remote_storage_config]
        //         local_path = "/nonexistent"
        //         some_invalid_field = 23
        //     "#}
        // );
    }
}
