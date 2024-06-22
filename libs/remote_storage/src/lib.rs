//! A set of generic storage abstractions for the page server to use when backing up and restoring its state from the external storage.
//! No other modules from this tree are supposed to be used directly by the external code.
//!
//! [`RemoteStorage`] trait a CRUD-like generic abstraction to use for adapting external storages with a few implementations:
//!   * [`local_fs`] allows to use local file system as an external storage
//!   * [`s3_bucket`] uses AWS S3 bucket as an external storage
//!   * [`azure_blob`] allows to use Azure Blob storage as an external storage
//!
#![deny(unsafe_code)]
#![deny(clippy::undocumented_unsafe_blocks)]

mod azure_blob;
mod error;
mod local_fs;
mod metrics;
mod s3_bucket;
mod simulate_failures;
mod support;

use std::{
    collections::HashMap,
    fmt::Debug,
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{bail, Context};
use aws_sdk_s3::types::StorageClass;
use camino::{Utf8Path, Utf8PathBuf};

use bytes::Bytes;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub use self::{
    azure_blob::AzureBlobStorage, local_fs::LocalFs, s3_bucket::S3Bucket,
    simulate_failures::UnreliableWrapper,
};
use s3_bucket::RequestKind;

/// Azure SDK's ETag type is a simple String wrapper: we use this internally instead of repeating it here.
pub use azure_core::Etag;

pub use error::{DownloadError, TimeTravelError, TimeoutOrCancel};

/// Currently, sync happens with AWS S3, that has two limits on requests per second:
/// ~200 RPS for IAM services
/// <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.html>
/// ~3500 PUT/COPY/POST/DELETE or 5500 GET/HEAD S3 requests
/// <https://aws.amazon.com/premiumsupport/knowledge-center/s3-request-limit-avoid-throttling/>
pub const DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT: usize = 100;
/// Set this limit analogously to the S3 limit
///
/// Here, a limit of max 20k concurrent connections was noted.
/// <https://learn.microsoft.com/en-us/answers/questions/1301863/is-there-any-limitation-to-concurrent-connections>
pub const DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT: usize = 100;
/// No limits on the client side, which currenltly means 1000 for AWS S3.
/// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_RequestSyntax>
pub const DEFAULT_MAX_KEYS_PER_LIST_RESPONSE: Option<i32> = None;

/// As defined in S3 docs
pub const MAX_KEYS_PER_DELETE: usize = 1000;

const REMOTE_STORAGE_PREFIX_SEPARATOR: char = '/';

/// Path on the remote storage, relative to some inner prefix.
/// The prefix is an implementation detail, that allows representing local paths
/// as the remote ones, stripping the local storage prefix away.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RemotePath(Utf8PathBuf);

impl Serialize for RemotePath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for RemotePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?;
        Ok(Self(Utf8PathBuf::from(&str)))
    }
}

impl std::fmt::Display for RemotePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl RemotePath {
    pub fn new(relative_path: &Utf8Path) -> anyhow::Result<Self> {
        anyhow::ensure!(
            relative_path.is_relative(),
            "Path {relative_path:?} is not relative"
        );
        Ok(Self(relative_path.to_path_buf()))
    }

    pub fn from_string(relative_path: &str) -> anyhow::Result<Self> {
        Self::new(Utf8Path::new(relative_path))
    }

    pub fn with_base(&self, base_path: &Utf8Path) -> Utf8PathBuf {
        base_path.join(&self.0)
    }

    pub fn object_name(&self) -> Option<&str> {
        self.0.file_name()
    }

    pub fn join(&self, path: impl AsRef<Utf8Path>) -> Self {
        Self(self.0.join(path))
    }

    pub fn get_path(&self) -> &Utf8PathBuf {
        &self.0
    }

    pub fn extension(&self) -> Option<&str> {
        self.0.extension()
    }

    pub fn strip_prefix(&self, p: &RemotePath) -> Result<&Utf8Path, std::path::StripPrefixError> {
        self.0.strip_prefix(&p.0)
    }

    pub fn add_trailing_slash(&self) -> Self {
        // Unwrap safety inputs are guararnteed to be valid UTF-8
        Self(format!("{}/", self.0).try_into().unwrap())
    }
}

/// We don't need callers to be able to pass arbitrary delimiters: just control
/// whether listings will use a '/' separator or not.
///
/// The WithDelimiter mode will populate `prefixes` and `keys` in the result.  The
/// NoDelimiter mode will only populate `keys`.
pub enum ListingMode {
    WithDelimiter,
    NoDelimiter,
}

#[derive(Default)]
pub struct Listing {
    pub prefixes: Vec<RemotePath>,
    pub keys: Vec<RemotePath>,
}

/// Storage (potentially remote) API to manage its state.
/// This storage tries to be unaware of any layered repository context,
/// providing basic CRUD operations for storage files.
#[allow(async_fn_in_trait)]
pub trait RemoteStorage: Send + Sync + 'static {
    /// List objects in remote storage, with semantics matching AWS S3's ListObjectsV2.
    /// (see `<https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>`)
    ///
    /// Note that the prefix is relative to any `prefix_in_bucket` configured for the client, not
    /// from the absolute root of the bucket.
    ///
    /// `mode` configures whether to use a delimiter.  Without a delimiter all keys
    /// within the prefix are listed in the `keys` of the result.  With a delimiter, any "directories" at the top level of
    /// the prefix are returned in the `prefixes` of the result, and keys in the top level of the prefix are
    /// returned in `keys` ().
    ///
    /// `max_keys` controls the maximum number of keys that will be returned.  If this is None, this function
    /// will iteratively call listobjects until it runs out of keys.  Note that this is not safe to use on
    /// unlimted size buckets, as the full list of objects is allocated into a monolithic data structure.
    ///
    async fn list(
        &self,
        prefix: Option<&RemotePath>,
        _mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> Result<Listing, DownloadError>;

    /// Streams the local file contents into remote into the remote storage entry.
    ///
    /// If the operation fails because of timeout or cancellation, the root cause of the error will be
    /// set to `TimeoutOrCancel`.
    async fn upload(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        // S3 PUT request requires the content length to be specified,
        // otherwise it starts to fail with the concurrent connection count increasing.
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()>;

    /// Streams the remote storage entry contents.
    ///
    /// The returned download stream will obey initial timeout and cancellation signal by erroring
    /// on whichever happens first. Only one of the reasons will fail the stream, which is usually
    /// enough for `tokio::io::copy_buf` usage. If needed the error can be filtered out.
    ///
    /// Returns the metadata, if any was stored with the file previously.
    async fn download(
        &self,
        from: &RemotePath,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError>;

    /// Streams a given byte range of the remote storage entry contents.
    ///
    /// The returned download stream will obey initial timeout and cancellation signal by erroring
    /// on whichever happens first. Only one of the reasons will fail the stream, which is usually
    /// enough for `tokio::io::copy_buf` usage. If needed the error can be filtered out.
    ///
    /// Returns the metadata, if any was stored with the file previously.
    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError>;

    /// Delete a single path from remote storage.
    ///
    /// If the operation fails because of timeout or cancellation, the root cause of the error will be
    /// set to `TimeoutOrCancel`. In such situation it is unknown if the deletion went through.
    async fn delete(&self, path: &RemotePath, cancel: &CancellationToken) -> anyhow::Result<()>;

    /// Delete a multiple paths from remote storage.
    ///
    /// If the operation fails because of timeout or cancellation, the root cause of the error will be
    /// set to `TimeoutOrCancel`. In such situation it is unknown which deletions, if any, went
    /// through.
    async fn delete_objects<'a>(
        &self,
        paths: &'a [RemotePath],
        cancel: &CancellationToken,
    ) -> anyhow::Result<()>;

    /// Copy a remote object inside a bucket from one path to another.
    async fn copy(
        &self,
        from: &RemotePath,
        to: &RemotePath,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()>;

    /// Resets the content of everything with the given prefix to the given state
    async fn time_travel_recover(
        &self,
        prefix: Option<&RemotePath>,
        timestamp: SystemTime,
        done_if_after: SystemTime,
        cancel: &CancellationToken,
    ) -> Result<(), TimeTravelError>;
}

/// DownloadStream is sensitive to the timeout and cancellation used with the original
/// [`RemoteStorage::download`] request. The type yields `std::io::Result<Bytes>` to be compatible
/// with `tokio::io::copy_buf`.
// This has 'static because safekeepers do not use cancellation tokens (yet)
pub type DownloadStream =
    Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>>;

pub struct Download {
    pub download_stream: DownloadStream,
    /// The last time the file was modified (`last-modified` HTTP header)
    pub last_modified: SystemTime,
    /// A way to identify this specific version of the resource (`etag` HTTP header)
    pub etag: Etag,
    /// Extra key-value data, associated with the current remote file.
    pub metadata: Option<StorageMetadata>,
}

impl Debug for Download {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Download")
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Every storage, currently supported.
/// Serves as a simple way to pass around the [`RemoteStorage`] without dealing with generics.
#[derive(Clone)]
// Require Clone for `Other` due to https://github.com/rust-lang/rust/issues/26925
pub enum GenericRemoteStorage<Other: Clone = Arc<UnreliableWrapper>> {
    LocalFs(LocalFs),
    AwsS3(Arc<S3Bucket>),
    AzureBlob(Arc<AzureBlobStorage>),
    Unreliable(Other),
}

impl<Other: RemoteStorage> GenericRemoteStorage<Arc<Other>> {
    pub async fn list(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Listing, DownloadError> {
        match self {
            Self::LocalFs(s) => s.list(prefix, mode, max_keys, cancel).await,
            Self::AwsS3(s) => s.list(prefix, mode, max_keys, cancel).await,
            Self::AzureBlob(s) => s.list(prefix, mode, max_keys, cancel).await,
            Self::Unreliable(s) => s.list(prefix, mode, max_keys, cancel).await,
        }
    }

    /// See [`RemoteStorage::upload`]
    pub async fn upload(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        match self {
            Self::LocalFs(s) => s.upload(from, data_size_bytes, to, metadata, cancel).await,
            Self::AwsS3(s) => s.upload(from, data_size_bytes, to, metadata, cancel).await,
            Self::AzureBlob(s) => s.upload(from, data_size_bytes, to, metadata, cancel).await,
            Self::Unreliable(s) => s.upload(from, data_size_bytes, to, metadata, cancel).await,
        }
    }

    pub async fn download(
        &self,
        from: &RemotePath,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        match self {
            Self::LocalFs(s) => s.download(from, cancel).await,
            Self::AwsS3(s) => s.download(from, cancel).await,
            Self::AzureBlob(s) => s.download(from, cancel).await,
            Self::Unreliable(s) => s.download(from, cancel).await,
        }
    }

    pub async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        match self {
            Self::LocalFs(s) => {
                s.download_byte_range(from, start_inclusive, end_exclusive, cancel)
                    .await
            }
            Self::AwsS3(s) => {
                s.download_byte_range(from, start_inclusive, end_exclusive, cancel)
                    .await
            }
            Self::AzureBlob(s) => {
                s.download_byte_range(from, start_inclusive, end_exclusive, cancel)
                    .await
            }
            Self::Unreliable(s) => {
                s.download_byte_range(from, start_inclusive, end_exclusive, cancel)
                    .await
            }
        }
    }

    /// See [`RemoteStorage::delete`]
    pub async fn delete(
        &self,
        path: &RemotePath,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        match self {
            Self::LocalFs(s) => s.delete(path, cancel).await,
            Self::AwsS3(s) => s.delete(path, cancel).await,
            Self::AzureBlob(s) => s.delete(path, cancel).await,
            Self::Unreliable(s) => s.delete(path, cancel).await,
        }
    }

    /// See [`RemoteStorage::delete_objects`]
    pub async fn delete_objects(
        &self,
        paths: &[RemotePath],
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        match self {
            Self::LocalFs(s) => s.delete_objects(paths, cancel).await,
            Self::AwsS3(s) => s.delete_objects(paths, cancel).await,
            Self::AzureBlob(s) => s.delete_objects(paths, cancel).await,
            Self::Unreliable(s) => s.delete_objects(paths, cancel).await,
        }
    }

    /// See [`RemoteStorage::copy`]
    pub async fn copy_object(
        &self,
        from: &RemotePath,
        to: &RemotePath,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        match self {
            Self::LocalFs(s) => s.copy(from, to, cancel).await,
            Self::AwsS3(s) => s.copy(from, to, cancel).await,
            Self::AzureBlob(s) => s.copy(from, to, cancel).await,
            Self::Unreliable(s) => s.copy(from, to, cancel).await,
        }
    }

    /// See [`RemoteStorage::time_travel_recover`].
    pub async fn time_travel_recover(
        &self,
        prefix: Option<&RemotePath>,
        timestamp: SystemTime,
        done_if_after: SystemTime,
        cancel: &CancellationToken,
    ) -> Result<(), TimeTravelError> {
        match self {
            Self::LocalFs(s) => {
                s.time_travel_recover(prefix, timestamp, done_if_after, cancel)
                    .await
            }
            Self::AwsS3(s) => {
                s.time_travel_recover(prefix, timestamp, done_if_after, cancel)
                    .await
            }
            Self::AzureBlob(s) => {
                s.time_travel_recover(prefix, timestamp, done_if_after, cancel)
                    .await
            }
            Self::Unreliable(s) => {
                s.time_travel_recover(prefix, timestamp, done_if_after, cancel)
                    .await
            }
        }
    }
}

impl GenericRemoteStorage {
    pub fn from_config(storage_config: &RemoteStorageConfig) -> anyhow::Result<Self> {
        let timeout = storage_config.timeout;
        Ok(match &storage_config.storage {
            RemoteStorageKind::LocalFs { local_path: path } => {
                info!("Using fs root '{path}' as a remote storage");
                Self::LocalFs(LocalFs::new(path.clone(), timeout)?)
            }
            RemoteStorageKind::AwsS3(s3_config) => {
                // The profile and access key id are only printed here for debugging purposes,
                // their values don't indicate the eventually taken choice for auth.
                let profile = std::env::var("AWS_PROFILE").unwrap_or_else(|_| "<none>".into());
                let access_key_id =
                    std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "<none>".into());
                info!("Using s3 bucket '{}' in region '{}' as a remote storage, prefix in bucket: '{:?}', bucket endpoint: '{:?}', profile: {profile}, access_key_id: {access_key_id}",
                      s3_config.bucket_name, s3_config.bucket_region, s3_config.prefix_in_bucket, s3_config.endpoint);
                Self::AwsS3(Arc::new(S3Bucket::new(s3_config, timeout)?))
            }
            RemoteStorageKind::AzureContainer(azure_config) => {
                let storage_account = azure_config
                    .storage_account
                    .as_deref()
                    .unwrap_or("<AZURE_STORAGE_ACCOUNT>");
                info!("Using azure container '{}' in account '{storage_account}' in region '{}' as a remote storage, prefix in container: '{:?}'",
                      azure_config.container_name, azure_config.container_region, azure_config.prefix_in_container);
                Self::AzureBlob(Arc::new(AzureBlobStorage::new(azure_config, timeout)?))
            }
        })
    }

    pub fn unreliable_wrapper(s: Self, fail_first: u64) -> Self {
        Self::Unreliable(Arc::new(UnreliableWrapper::new(s, fail_first)))
    }

    /// See [`RemoteStorage::upload`], which this method calls with `None` as metadata.
    pub async fn upload_storage_object(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        from_size_bytes: usize,
        to: &RemotePath,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        self.upload(from, from_size_bytes, to, None, cancel)
            .await
            .with_context(|| {
                format!("Failed to upload data of length {from_size_bytes} to storage path {to:?}")
            })
    }

    /// Downloads the storage object into the `to_path` provided.
    /// `byte_range` could be specified to dowload only a part of the file, if needed.
    pub async fn download_storage_object(
        &self,
        byte_range: Option<(u64, Option<u64>)>,
        from: &RemotePath,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        match byte_range {
            Some((start, end)) => self.download_byte_range(from, start, end, cancel).await,
            None => self.download(from, cancel).await,
        }
    }
}

/// Extra set of key-value pairs that contain arbitrary metadata about the storage entry.
/// Immutable, cannot be changed once the file is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMetadata(HashMap<String, String>);

impl<const N: usize> From<[(&str, &str); N]> for StorageMetadata {
    fn from(arr: [(&str, &str); N]) -> Self {
        let map: HashMap<String, String> = arr
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self(map)
    }
}

/// External backup storage configuration, enough for creating a client for that storage.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RemoteStorageConfig {
    /// The storage connection configuration.
    #[serde(flatten)]
    pub storage: RemoteStorageKind,
    /// A common timeout enforced for all requests after concurrency limiter permit has been
    /// acquired.
    #[serde(
        with = "humantime_serde",
        default = "default_timeout",
        skip_serializing_if = "is_default_timeout"
    )]
    pub timeout: Duration,
}

fn default_timeout() -> Duration {
    RemoteStorageConfig::DEFAULT_TIMEOUT
}

fn is_default_timeout(d: &Duration) -> bool {
    *d == RemoteStorageConfig::DEFAULT_TIMEOUT
}

/// A kind of a remote storage to connect to, with its connection configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RemoteStorageKind {
    /// Storage based on local file system.
    /// Specify a root folder to place all stored files into.
    LocalFs { local_path: Utf8PathBuf },
    /// AWS S3 based storage, storing all files in the S3 bucket
    /// specified by the config
    AwsS3(S3Config),
    /// Azure Blob based storage, storing all files in the container
    /// specified by the config
    AzureContainer(AzureConfig),
}

/// AWS S3 bucket coordinates and access credentials to manage the bucket contents (read and write).
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct S3Config {
    /// Name of the bucket to connect to.
    pub bucket_name: String,
    /// The region where the bucket is located at.
    pub bucket_region: String,
    /// A "subfolder" in the bucket, to use the same bucket separately by multiple remote storage users at once.
    pub prefix_in_bucket: Option<String>,
    /// A base URL to send S3 requests to.
    /// By default, the endpoint is derived from a region name, assuming it's
    /// an AWS S3 region name, erroring on wrong region name.
    /// Endpoint provides a way to support other S3 flavors and their regions.
    ///
    /// Example: `http://127.0.0.1:5000`
    pub endpoint: Option<String>,
    /// AWS S3 has various limits on its API calls, we need not to exceed those.
    /// See [`DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT`] for more details.
    #[serde(default = "default_remote_storage_s3_concurrency_limit")]
    pub concurrency_limit: NonZeroUsize,
    #[serde(default = "default_max_keys_per_list_response")]
    pub max_keys_per_list_response: Option<i32>,
    #[serde(
        deserialize_with = "deserialize_storage_class",
        serialize_with = "serialize_storage_class",
        default
    )]
    pub upload_storage_class: Option<StorageClass>,
}

fn default_remote_storage_s3_concurrency_limit() -> NonZeroUsize {
    DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT
        .try_into()
        .unwrap()
}

fn default_max_keys_per_list_response() -> Option<i32> {
    DEFAULT_MAX_KEYS_PER_LIST_RESPONSE
}

impl Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket_name", &self.bucket_name)
            .field("bucket_region", &self.bucket_region)
            .field("prefix_in_bucket", &self.prefix_in_bucket)
            .field("concurrency_limit", &self.concurrency_limit)
            .field(
                "max_keys_per_list_response",
                &self.max_keys_per_list_response,
            )
            .finish()
    }
}

/// Azure  bucket coordinates and access credentials to manage the bucket contents (read and write).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AzureConfig {
    /// Name of the container to connect to.
    pub container_name: String,
    /// Name of the storage account the container is inside of
    pub storage_account: Option<String>,
    /// The region where the bucket is located at.
    pub container_region: String,
    /// A "subfolder" in the container, to use the same container separately by multiple remote storage users at once.
    pub prefix_in_container: Option<String>,
    /// Azure has various limits on its API calls, we need not to exceed those.
    /// See [`DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT`] for more details.
    #[serde(default = "default_remote_storage_azure_concurrency_limit")]
    pub concurrency_limit: NonZeroUsize,
    #[serde(default = "default_max_keys_per_list_response")]
    pub max_keys_per_list_response: Option<i32>,
}

fn default_remote_storage_azure_concurrency_limit() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT).unwrap()
}

impl Debug for AzureConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureConfig")
            .field("bucket_name", &self.container_name)
            .field("storage_account", &self.storage_account)
            .field("bucket_region", &self.container_region)
            .field("prefix_in_container", &self.prefix_in_container)
            .field("concurrency_limit", &self.concurrency_limit)
            .field(
                "max_keys_per_list_response",
                &self.max_keys_per_list_response,
            )
            .finish()
    }
}

fn deserialize_storage_class<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<StorageClass>, D::Error> {
    Option::<String>::deserialize(deserializer).and_then(|s| {
        if let Some(s) = s {
            use serde::de::Error;
            let storage_class = StorageClass::from_str(&s).expect("infallible");
            #[allow(deprecated)]
            if matches!(storage_class, StorageClass::Unknown(_)) {
                return Err(D::Error::custom(format!(
                    "Specified storage class unknown to SDK: '{s}'. Allowed values: {:?}",
                    StorageClass::values()
                )));
            }
            Ok(Some(storage_class))
        } else {
            Ok(None)
        }
    })
}

fn serialize_storage_class<S: serde::Serializer>(
    val: &Option<StorageClass>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let val = val.as_ref().map(StorageClass::as_str);
    Option::<&str>::serialize(&val, serializer)
}

impl RemoteStorageConfig {
    pub const DEFAULT_TIMEOUT: Duration = std::time::Duration::from_secs(120);

    pub fn from_toml(toml: &toml_edit::Item) -> anyhow::Result<Option<RemoteStorageConfig>> {
        let document: toml_edit::Document = match toml {
            toml_edit::Item::Table(toml) => toml.clone().into(),
            toml_edit::Item::Value(toml_edit::Value::InlineTable(toml)) => {
                toml.clone().into_table().into()
            }
            _ => bail!("toml not a table or inline table"),
        };

        if document.is_empty() {
            return Ok(None);
        }

        Ok(Some(toml_edit::de::from_document(document)?))
    }
}

struct ConcurrencyLimiter {
    // Every request to S3 can be throttled or cancelled, if a certain number of requests per second is exceeded.
    // Same goes to IAM, which is queried before every S3 request, if enabled. IAM has even lower RPS threshold.
    // The helps to ensure we don't exceed the thresholds.
    write: Arc<Semaphore>,
    read: Arc<Semaphore>,
}

impl ConcurrencyLimiter {
    fn for_kind(&self, kind: RequestKind) -> &Arc<Semaphore> {
        match kind {
            RequestKind::Get => &self.read,
            RequestKind::Put => &self.write,
            RequestKind::List => &self.read,
            RequestKind::Delete => &self.write,
            RequestKind::Copy => &self.write,
            RequestKind::TimeTravel => &self.write,
        }
    }

    async fn acquire(
        &self,
        kind: RequestKind,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        self.for_kind(kind).acquire().await
    }

    async fn acquire_owned(
        &self,
        kind: RequestKind,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
        Arc::clone(self.for_kind(kind)).acquire_owned().await
    }

    fn new(limit: usize) -> ConcurrencyLimiter {
        Self {
            read: Arc::new(Semaphore::new(limit)),
            write: Arc::new(Semaphore::new(limit)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> anyhow::Result<Option<RemoteStorageConfig>> {
        let toml = input.parse::<toml_edit::Document>().unwrap();
        RemoteStorageConfig::from_toml(toml.as_item())
    }

    #[test]
    fn test_object_name() {
        let k = RemotePath::new(Utf8Path::new("a/b/c")).unwrap();
        assert_eq!(k.object_name(), Some("c"));

        let k = RemotePath::new(Utf8Path::new("a/b/c/")).unwrap();
        assert_eq!(k.object_name(), Some("c"));

        let k = RemotePath::new(Utf8Path::new("a/")).unwrap();
        assert_eq!(k.object_name(), Some("a"));

        // XXX is it impossible to have an empty key?
        let k = RemotePath::new(Utf8Path::new("")).unwrap();
        assert_eq!(k.object_name(), None);
    }

    #[test]
    fn rempte_path_cannot_be_created_from_absolute_ones() {
        let err = RemotePath::new(Utf8Path::new("/")).expect_err("Should fail on absolute paths");
        assert_eq!(err.to_string(), "Path \"/\" is not relative");
    }

    #[test]
    fn parse_localfs_config_with_timeout() {
        let input = "local_path = '.'
timeout = '5s'";

        let config = parse(input).unwrap().expect("it exists");

        assert_eq!(
            config,
            RemoteStorageConfig {
                storage: RemoteStorageKind::LocalFs {
                    local_path: Utf8PathBuf::from(".")
                },
                timeout: Duration::from_secs(5)
            }
        );
    }

    #[test]
    fn test_s3_parsing() {
        let toml = "\
        bucket_name = 'foo-bar'
        bucket_region = 'eu-central-1'
        upload_storage_class = 'INTELLIGENT_TIERING'
        timeout = '7s'
        ";

        let config = parse(toml).unwrap().expect("it exists");

        assert_eq!(
            config,
            RemoteStorageConfig {
                storage: RemoteStorageKind::AwsS3(S3Config {
                    bucket_name: "foo-bar".into(),
                    bucket_region: "eu-central-1".into(),
                    prefix_in_bucket: None,
                    endpoint: None,
                    concurrency_limit: default_remote_storage_s3_concurrency_limit(),
                    max_keys_per_list_response: DEFAULT_MAX_KEYS_PER_LIST_RESPONSE,
                    upload_storage_class: Some(StorageClass::IntelligentTiering),
                }),
                timeout: Duration::from_secs(7)
            }
        );
    }

    #[test]
    fn test_azure_parsing() {
        let toml = "\
        container_name = 'foo-bar'
        container_region = 'westeurope'
        upload_storage_class = 'INTELLIGENT_TIERING'
        timeout = '7s'
        ";

        let config = parse(toml).unwrap().expect("it exists");

        assert_eq!(
            config,
            RemoteStorageConfig {
                storage: RemoteStorageKind::AzureContainer(AzureConfig {
                    container_name: "foo-bar".into(),
                    storage_account: None,
                    container_region: "westeurope".into(),
                    prefix_in_container: None,
                    concurrency_limit: default_remote_storage_azure_concurrency_limit(),
                    max_keys_per_list_response: DEFAULT_MAX_KEYS_PER_LIST_RESPONSE,
                }),
                timeout: Duration::from_secs(7)
            }
        );
    }
}
