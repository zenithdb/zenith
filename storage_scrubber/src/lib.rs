#![deny(unsafe_code)]
#![deny(clippy::undocumented_unsafe_blocks)]
pub mod checks;
pub mod cloud_admin_api;
pub mod find_large_objects;
pub mod garbage;
pub mod metadata_stream;
pub mod pageserver_physical_gc;
pub mod scan_pageserver_metadata;
pub mod scan_safekeeper_metadata;
pub mod tenant_snapshot;

use std::env;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use aws_config::retry::{RetryConfigBuilder, RetryMode};
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::Client;

use camino::{Utf8Path, Utf8PathBuf};
use clap::ValueEnum;
use futures::{Stream, StreamExt};
use pageserver::tenant::remote_timeline_client::{remote_tenant_path, remote_timeline_path};
use pageserver::tenant::TENANTS_SEGMENT_NAME;
use pageserver_api::shard::TenantShardId;
use remote_storage::{
    DownloadOpts, GenericRemoteStorage, Listing, ListingMode, RemotePath, RemoteStorageConfig,
    RemoteStorageKind, S3Config,
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use storage_controller_client::control_api;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use utils::fs_ext;
use utils::id::{TenantId, TenantTimelineId, TimelineId};

const MAX_RETRIES: usize = 20;
const CLOUD_ADMIN_API_TOKEN_ENV_VAR: &str = "CLOUD_ADMIN_API_TOKEN";

#[derive(Debug, Clone)]
pub struct S3Target {
    pub desc_str: String,
    /// This `prefix_in_bucket` is only equal to the PS/SK config of the same
    /// name for the RootTarget: other instances of S3Target will have prefix_in_bucket
    /// with extra parts.
    pub prefix_in_bucket: String,
    pub delimiter: String,
}

/// Convenience for referring to timelines within a particular shard: more ergonomic
/// than using a 2-tuple.
///
/// This is the shard-aware equivalent of TenantTimelineId.  It's defined here rather
/// than somewhere more broadly exposed, because this kind of thing is rarely needed
/// in the pageserver, as all timeline objects existing in the scope of a particular
/// tenant: the scrubber is different in that it handles collections of data referring to many
/// TenantShardTimelineIds in on place.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TenantShardTimelineId {
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
}

impl TenantShardTimelineId {
    fn new(tenant_shard_id: TenantShardId, timeline_id: TimelineId) -> Self {
        Self {
            tenant_shard_id,
            timeline_id,
        }
    }

    fn as_tenant_timeline_id(&self) -> TenantTimelineId {
        TenantTimelineId::new(self.tenant_shard_id.tenant_id, self.timeline_id)
    }
}

impl Display for TenantShardTimelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.tenant_shard_id, self.timeline_id)
    }
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraversingDepth {
    Tenant,
    Timeline,
}

impl Display for TraversingDepth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Tenant => "tenant",
            Self::Timeline => "timeline",
        })
    }
}

#[derive(ValueEnum, Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum NodeKind {
    Safekeeper,
    Pageserver,
}

impl NodeKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Safekeeper => "safekeeper",
            Self::Pageserver => "pageserver",
        }
    }
}

impl Display for NodeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl S3Target {
    pub fn with_sub_segment(&self, new_segment: &str) -> Self {
        let mut new_self = self.clone();
        if new_self.prefix_in_bucket.is_empty() {
            new_self.prefix_in_bucket = format!("/{}/", new_segment);
        } else {
            if new_self.prefix_in_bucket.ends_with('/') {
                new_self.prefix_in_bucket.pop();
            }
            new_self.prefix_in_bucket =
                [&new_self.prefix_in_bucket, new_segment, ""].join(&new_self.delimiter);
        }
        new_self
    }
}

#[derive(Clone)]
pub enum RootTarget {
    Pageserver(S3Target),
    Safekeeper(S3Target),
}

impl RootTarget {
    pub fn tenants_root(&self) -> S3Target {
        match self {
            Self::Pageserver(root) => root.with_sub_segment(TENANTS_SEGMENT_NAME),
            Self::Safekeeper(root) => root.clone(),
        }
    }

    pub fn tenant_root(&self, tenant_id: &TenantShardId) -> S3Target {
        match self {
            Self::Pageserver(_) => self.tenants_root().with_sub_segment(&tenant_id.to_string()),
            Self::Safekeeper(_) => self
                .tenants_root()
                .with_sub_segment(&tenant_id.tenant_id.to_string()),
        }
    }

    pub(crate) fn tenant_shards_prefix(&self, tenant_id: &TenantId) -> S3Target {
        // Only pageserver remote storage contains tenant-shards
        assert!(matches!(self, Self::Pageserver(_)));
        let Self::Pageserver(root) = self else {
            panic!();
        };

        S3Target {
            desc_str: root.desc_str.clone(),
            prefix_in_bucket: format!(
                "{}/{TENANTS_SEGMENT_NAME}/{tenant_id}",
                root.prefix_in_bucket
            ),
            delimiter: root.delimiter.clone(),
        }
    }

    pub fn timelines_root(&self, tenant_id: &TenantShardId) -> S3Target {
        match self {
            Self::Pageserver(_) => self.tenant_root(tenant_id).with_sub_segment("timelines"),
            Self::Safekeeper(_) => self.tenant_root(tenant_id),
        }
    }

    pub fn timeline_root(&self, id: &TenantShardTimelineId) -> S3Target {
        self.timelines_root(&id.tenant_shard_id)
            .with_sub_segment(&id.timeline_id.to_string())
    }

    /// Given RemotePath "tenants/foo/timelines/bar/layerxyz", prefix it to a literal
    /// key in the S3 bucket.
    pub fn absolute_key(&self, key: &RemotePath) -> String {
        let root = match self {
            Self::Pageserver(root) => root,
            Self::Safekeeper(root) => root,
        };

        let prefix = &root.prefix_in_bucket;
        if prefix.ends_with('/') {
            format!("{prefix}{key}")
        } else {
            format!("{prefix}/{key}")
        }
    }

    pub fn desc_str(&self) -> &str {
        match self {
            Self::Pageserver(root) => &root.desc_str,
            Self::Safekeeper(root) => &root.desc_str,
        }
    }

    pub fn delimiter(&self) -> &str {
        match self {
            Self::Pageserver(root) => &root.delimiter,
            Self::Safekeeper(root) => &root.delimiter,
        }
    }
}

pub fn remote_timeline_path_id(id: &TenantShardTimelineId) -> RemotePath {
    remote_timeline_path(&id.tenant_shard_id, &id.timeline_id)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketConfig(RemoteStorageConfig);

impl BucketConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        if let Ok(legacy) = Self::from_env_legacy() {
            return Ok(legacy);
        }
        let config_toml =
            env::var("REMOTE_STORAGE_CONFIG").context("'REMOTE_STORAGE_CONFIG' retrieval")?;
        let remote_config = RemoteStorageConfig::from_toml_str(&config_toml)?;
        Ok(BucketConfig(remote_config))
    }

    fn from_env_legacy() -> anyhow::Result<Self> {
        let bucket_region = env::var("REGION").context("'REGION' param retrieval")?;
        let bucket_name = env::var("BUCKET").context("'BUCKET' param retrieval")?;
        let prefix_in_bucket = env::var("BUCKET_PREFIX").ok();
        let endpoint = env::var("AWS_ENDPOINT_URL").ok();
        // Create a json object which we then deserialize so that we don't
        // have to repeat all of the S3Config fields.
        let s3_config_json = serde_json::json!({
            "bucket_name": bucket_name,
            "bucket_region": bucket_region,
            "prefix_in_bucket": prefix_in_bucket,
            "endpoint": endpoint,
        });
        let config: RemoteStorageConfig = serde_json::from_value(s3_config_json)?;
        Ok(BucketConfig(config))
    }
    pub fn desc_str(&self) -> String {
        match &self.0.storage {
            RemoteStorageKind::LocalFs { local_path } => {
                format!("local path {local_path}")
            }
            RemoteStorageKind::AwsS3(config) => format!(
                "bucket {}, region {}",
                config.bucket_name, config.bucket_region
            ),
            RemoteStorageKind::AzureContainer(config) => format!(
                "container {}, storage account {:?}, region {}",
                config.container_name, config.storage_account, config.container_region
            ),
        }
    }
    pub fn bucket_name(&self) -> Option<&str> {
        self.0.storage.bucket_name()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketConfigLegacy {
    pub region: String,
    pub bucket: String,
    pub prefix_in_bucket: Option<String>,
}

pub struct ControllerClientConfig {
    /// URL to storage controller.  e.g. http://127.0.0.1:1234 when using `neon_local`
    pub controller_api: Url,

    /// JWT token for authenticating with storage controller.  Requires scope 'scrubber' or 'admin'.
    pub controller_jwt: String,
}

impl ControllerClientConfig {
    pub fn build_client(self) -> control_api::Client {
        control_api::Client::new(self.controller_api, Some(self.controller_jwt))
    }
}

pub struct ConsoleConfig {
    pub token: String,
    pub base_url: Url,
}

impl ConsoleConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let base_url: Url = env::var("CLOUD_ADMIN_API_URL")
            .context("'CLOUD_ADMIN_API_URL' param retrieval")?
            .parse()
            .context("'CLOUD_ADMIN_API_URL' param parsing")?;

        let token = env::var(CLOUD_ADMIN_API_TOKEN_ENV_VAR)
            .context("'CLOUD_ADMIN_API_TOKEN' environment variable fetch")?;

        Ok(Self { base_url, token })
    }
}

pub fn init_logging(file_name: &str) -> Option<WorkerGuard> {
    let stderr_logs = fmt::Layer::new()
        .with_target(false)
        .with_writer(std::io::stderr);

    let disable_file_logging = match std::env::var("PAGESERVER_DISABLE_FILE_LOGGING") {
        Ok(s) => s == "1" || s.to_lowercase() == "true",
        Err(_) => false,
    };

    if disable_file_logging {
        tracing_subscriber::registry()
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
            .with(stderr_logs)
            .init();
        None
    } else {
        let (file_writer, guard) =
            tracing_appender::non_blocking(tracing_appender::rolling::never("./logs/", file_name));
        let file_logs = fmt::Layer::new()
            .with_target(false)
            .with_ansi(false)
            .with_writer(file_writer);
        tracing_subscriber::registry()
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
            .with(stderr_logs)
            .with(file_logs)
            .init();
        Some(guard)
    }
}

async fn init_s3_client(bucket_region: Region) -> Client {
    let mut retry_config_builder = RetryConfigBuilder::new();

    retry_config_builder
        .set_max_attempts(Some(3))
        .set_mode(Some(RetryMode::Adaptive));

    let config = aws_config::defaults(aws_config::BehaviorVersion::v2024_03_28())
        .region(bucket_region)
        .retry_config(retry_config_builder.build())
        .load()
        .await;
    Client::new(&config)
}

fn default_prefix_in_bucket(node_kind: NodeKind) -> &'static str {
    match node_kind {
        NodeKind::Pageserver => "pageserver/v1/",
        NodeKind::Safekeeper => "wal/",
    }
}

fn make_root_target(desc_str: String, prefix_in_bucket: String, node_kind: NodeKind) -> RootTarget {
    let s3_target = S3Target {
        desc_str,
        prefix_in_bucket,
        delimiter: "/".to_string(),
    };
    match node_kind {
        NodeKind::Pageserver => RootTarget::Pageserver(s3_target),
        NodeKind::Safekeeper => RootTarget::Safekeeper(s3_target),
    }
}

async fn init_remote_s3(
    bucket_config: S3Config,
    node_kind: NodeKind,
) -> anyhow::Result<(Arc<Client>, RootTarget)> {
    let bucket_region = Region::new(bucket_config.bucket_region);
    let s3_client = Arc::new(init_s3_client(bucket_region).await);
    let default_prefix = default_prefix_in_bucket(node_kind).to_string();

    let s3_root = make_root_target(
        bucket_config.bucket_name,
        bucket_config.prefix_in_bucket.unwrap_or(default_prefix),
        node_kind,
    );

    Ok((s3_client, s3_root))
}

async fn init_remote(
    mut storage_config: BucketConfig,
    node_kind: NodeKind,
) -> anyhow::Result<(GenericRemoteStorage, RootTarget)> {
    let desc_str = storage_config.desc_str();

    let default_prefix = default_prefix_in_bucket(node_kind).to_string();

    match &mut storage_config.0.storage {
        RemoteStorageKind::AwsS3(ref mut config) => {
            config.prefix_in_bucket.get_or_insert(default_prefix);
        }
        RemoteStorageKind::AzureContainer(ref mut config) => {
            config.prefix_in_container.get_or_insert(default_prefix);
        }
        RemoteStorageKind::LocalFs { .. } => (),
    }

    // We already pass the prefix to the remote client above
    let prefix_in_root_target = String::new();
    let root_target = make_root_target(desc_str, prefix_in_root_target, node_kind);

    let client = GenericRemoteStorage::from_config(&storage_config.0).await?;
    Ok((client, root_target))
}

/// Listing possibly large amounts of keys in a streaming fashion.
fn stream_objects_with_retries<'a>(
    storage_client: &'a GenericRemoteStorage,
    listing_mode: ListingMode,
    s3_target: &'a S3Target,
) -> impl Stream<Item = Result<Listing, anyhow::Error>> + 'a {
    async_stream::stream! {
        let mut trial = 0;
        let cancel = CancellationToken::new();
        let prefix_str = &s3_target
            .prefix_in_bucket
            .strip_prefix("/")
            .unwrap_or(&s3_target.prefix_in_bucket);
        let prefix = RemotePath::from_string(prefix_str)?;
        let mut list_stream =
            storage_client.list_streaming(Some(&prefix), listing_mode, None, &cancel);
        while let Some(res) = list_stream.next().await {
            match res {
                Err(err) => {
                    let yield_err = if err.is_permanent() {
                        true
                    } else {
                        let backoff_time = 1 << trial.min(5);
                        tokio::time::sleep(Duration::from_secs(backoff_time)).await;
                        trial += 1;
                        trial == MAX_RETRIES - 1
                    };
                    if yield_err {
                        yield Err(err)
                            .with_context(|| format!("Failed to list objects {MAX_RETRIES} times"));
                        break;
                    }
                }
                Ok(res) => {
                    trial = 0;
                    yield Ok(res);
                }
            }
        }
    }
}

/// If you want to list a bounded amount of prefixes or keys. For larger numbers of keys/prefixes,
/// use [`stream_objects_with_retries`] instead.
async fn list_objects_with_retries(
    remote_client: &GenericRemoteStorage,
    listing_mode: ListingMode,
    s3_target: &S3Target,
) -> anyhow::Result<Listing> {
    let cancel = CancellationToken::new();
    let prefix_str = &s3_target
        .prefix_in_bucket
        .strip_prefix("/")
        .unwrap_or(&s3_target.prefix_in_bucket);
    let prefix = RemotePath::from_string(prefix_str)?;
    for trial in 0..MAX_RETRIES {
        match remote_client
            .list(Some(&prefix), listing_mode, None, &cancel)
            .await
        {
            Ok(response) => return Ok(response),
            Err(e) => {
                if trial == MAX_RETRIES - 1 {
                    return Err(e)
                        .with_context(|| format!("Failed to list objects {MAX_RETRIES} times"));
                }
                warn!(
                    "list_objects_v2 query failed: bucket_name={}, prefix={}, delimiter={}, error={}",
                    remote_client.bucket_name().unwrap_or_default(),
                    s3_target.prefix_in_bucket,
                    s3_target.delimiter,
                    DisplayErrorContext(e),
                );
                let backoff_time = 1 << trial.min(5);
                tokio::time::sleep(Duration::from_secs(backoff_time)).await;
            }
        }
    }
    panic!("MAX_RETRIES is not allowed to be 0");
}

/// Returns content, last modified time
async fn download_object_with_retries(
    remote_client: &GenericRemoteStorage,
    key: &RemotePath,
) -> anyhow::Result<(Vec<u8>, SystemTime)> {
    let cancel = CancellationToken::new();
    for trial in 0..MAX_RETRIES {
        let mut buf = Vec::new();
        let download = match remote_client
            .download(key, &DownloadOpts::default(), &cancel)
            .await
        {
            Ok(response) => response,
            Err(e) => {
                error!("Failed to download object for key {key}: {e}");
                let backoff_time = 1 << trial.min(5);
                tokio::time::sleep(Duration::from_secs(backoff_time)).await;
                continue;
            }
        };

        match tokio_util::io::StreamReader::new(download.download_stream)
            .read_to_end(&mut buf)
            .await
        {
            Ok(bytes_read) => {
                tracing::debug!("Downloaded {bytes_read} bytes for object {key}");
                return Ok((buf, download.last_modified));
            }
            Err(e) => {
                error!("Failed to stream object body for key {key}: {e}");
                let backoff_time = 1 << trial.min(5);
                tokio::time::sleep(Duration::from_secs(backoff_time)).await;
            }
        }
    }

    anyhow::bail!("Failed to download objects with key {key} {MAX_RETRIES} times")
}

async fn download_object_to_file_s3(
    s3_client: &Client,
    bucket_name: &str,
    key: &str,
    version_id: Option<&str>,
    local_path: &Utf8Path,
) -> anyhow::Result<()> {
    let tmp_path = Utf8PathBuf::from(format!("{local_path}.tmp"));
    for _ in 0..MAX_RETRIES {
        tokio::fs::remove_file(&tmp_path)
            .await
            .or_else(fs_ext::ignore_not_found)?;

        let mut file = tokio::fs::File::create(&tmp_path)
            .await
            .context("Opening output file")?;

        let request = s3_client.get_object().bucket(bucket_name).key(key);

        let request = match version_id {
            Some(version_id) => request.version_id(version_id),
            None => request,
        };

        let response_stream = match request.send().await {
            Ok(response) => response,
            Err(e) => {
                error!(
                    "Failed to download object for key {key} version {}: {e:#}",
                    version_id.unwrap_or("")
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let mut read_stream = response_stream.body.into_async_read();

        tokio::io::copy(&mut read_stream, &mut file).await?;

        tokio::fs::rename(&tmp_path, local_path).await?;
        return Ok(());
    }

    anyhow::bail!("Failed to download objects with key {key} {MAX_RETRIES} times")
}
