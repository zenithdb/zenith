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
mod config;
mod error;
mod local_fs;
mod metrics;
mod s3_bucket;
mod simulate_failures;
mod support;

use std::{
    collections::HashMap,
    fmt::Debug,
    num::NonZeroU32,
    ops::Bound,
    pin::{pin, Pin},
    sync::Arc,
    time::SystemTime,
};

use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};

use bytes::Bytes;
use futures::{stream::Stream, StreamExt};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub use self::{
    azure_blob::AzureBlobStorage, local_fs::LocalFs, s3_bucket::S3Bucket,
    simulate_failures::UnreliableWrapper,
};
use s3_bucket::RequestKind;

pub use crate::config::{AzureConfig, RemoteStorageConfig, RemoteStorageKind, S3Config};

/// Azure SDK's ETag type is a simple String wrapper: we use this internally instead of repeating it here.
pub use azure_core::Etag;

pub use error::{DownloadError, TimeTravelError, TimeoutOrCancel};

/// Default concurrency limit for S3 operations
///
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
///
/// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
pub const MAX_KEYS_PER_DELETE_S3: usize = 1000;

/// As defined in Azure docs
///
/// <https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch>
pub const MAX_KEYS_PER_DELETE_AZURE: usize = 256;

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
#[derive(Copy, Clone)]
pub enum ListingMode {
    WithDelimiter,
    NoDelimiter,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct ListingObject {
    pub key: RemotePath,
    pub last_modified: SystemTime,
    pub size: u64,
}

#[derive(Default)]
pub struct Listing {
    pub prefixes: Vec<RemotePath>,
    pub keys: Vec<ListingObject>,
}

/// Options for downloads. The default value is a plain GET.
pub struct DownloadOpts {
    /// If given, returns [`DownloadError::Unmodified`] if the object still has
    /// the same ETag (using If-None-Match).
    pub etag: Option<Etag>,
    /// The start of the byte range to download, or unbounded.
    pub byte_start: Bound<u64>,
    /// The end of the byte range to download, or unbounded. Must be after the
    /// start bound.
    pub byte_end: Bound<u64>,
    /// Indicate whether we're downloading something small or large: this indirectly controls
    /// timeouts: for something like an index/manifest/heatmap, we should time out faster than
    /// for layer files
    pub kind: DownloadKind,
}

pub enum DownloadKind {
    Large,
    Small,
}

impl Default for DownloadOpts {
    fn default() -> Self {
        Self {
            etag: Default::default(),
            byte_start: Bound::Unbounded,
            byte_end: Bound::Unbounded,
            kind: DownloadKind::Large,
        }
    }
}

impl DownloadOpts {
    /// Returns the byte range with inclusive start and exclusive end, or None
    /// if unbounded.
    pub fn byte_range(&self) -> Option<(u64, Option<u64>)> {
        if self.byte_start == Bound::Unbounded && self.byte_end == Bound::Unbounded {
            return None;
        }
        let start = match self.byte_start {
            Bound::Excluded(i) => i + 1,
            Bound::Included(i) => i,
            Bound::Unbounded => 0,
        };
        let end = match self.byte_end {
            Bound::Excluded(i) => Some(i),
            Bound::Included(i) => Some(i + 1),
            Bound::Unbounded => None,
        };
        if let Some(end) = end {
            assert!(start < end, "range end {end} at or before start {start}");
        }
        Some((start, end))
    }

    /// Returns the byte range as an RFC 2616 Range header value with inclusive
    /// bounds, or None if unbounded.
    pub fn byte_range_header(&self) -> Option<String> {
        self.byte_range()
            .map(|(start, end)| (start, end.map(|end| end - 1))) // make end inclusive
            .map(|(start, end)| match end {
                Some(end) => format!("bytes={start}-{end}"),
                None => format!("bytes={start}-"),
            })
    }
}

/// Storage (potentially remote) API to manage its state.
/// This storage tries to be unaware of any layered repository context,
/// providing basic CRUD operations for storage files.
#[allow(async_fn_in_trait)]
pub trait RemoteStorage: Send + Sync + 'static {
    /// List objects in remote storage, with semantics matching AWS S3's [`ListObjectsV2`].
    ///
    /// The stream is guaranteed to return at least one element, even in the case of errors
    /// (in that case it's an `Err()`), or an empty `Listing`.
    ///
    /// The stream is not ending if it returns an error, as long as [`is_permanent`] returns false on the error.
    /// The `next` function can be retried, and maybe in a future retry, there will be success.
    ///
    /// Note that the prefix is relative to any `prefix_in_bucket` configured for the client, not
    /// from the absolute root of the bucket.
    ///
    /// `mode` configures whether to use a delimiter.  Without a delimiter, all keys
    /// within the prefix are listed in the `keys` of the result.  With a delimiter, any "directories" at the top level of
    /// the prefix are returned in the `prefixes` of the result, and keys in the top level of the prefix are
    /// returned in `keys` ().
    ///
    /// `max_keys` controls the maximum number of keys that will be returned.  If this is None, this function
    /// will iteratively call listobjects until it runs out of keys.  Note that this is not safe to use on
    /// unlimted size buckets, as the full list of objects is allocated into a monolithic data structure.
    ///
    /// [`ListObjectsV2`]: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
    /// [`is_permanent`]: DownloadError::is_permanent
    fn list_streaming(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> impl Stream<Item = Result<Listing, DownloadError>> + Send;

    async fn list(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> Result<Listing, DownloadError> {
        let mut stream = pin!(self.list_streaming(prefix, mode, max_keys, cancel));
        let mut combined = stream.next().await.expect("At least one item required")?;
        while let Some(list) = stream.next().await {
            let list = list?;
            combined.keys.extend(list.keys.into_iter());
            combined.prefixes.extend_from_slice(&list.prefixes);
        }
        Ok(combined)
    }

    /// Obtain metadata information about an object.
    async fn head_object(
        &self,
        key: &RemotePath,
        cancel: &CancellationToken,
    ) -> Result<ListingObject, DownloadError>;

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
        opts: &DownloadOpts,
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
    async fn delete_objects(
        &self,
        paths: &[RemotePath],
        cancel: &CancellationToken,
    ) -> anyhow::Result<()>;

    /// Returns the maximum number of keys that a call to [`Self::delete_objects`] can delete without chunking
    ///
    /// The value returned is only an optimization hint, One can pass larger number of objects to
    /// `delete_objects` as well.
    ///
    /// The value is guaranteed to be >= 1.
    fn max_keys_per_delete(&self) -> usize;

    /// Deletes all objects matching the given prefix.
    ///
    /// NB: this uses NoDelimiter and will match partial prefixes. For example, the prefix /a/b will
    /// delete /a/b, /a/b/*, /a/bc, /a/bc/*, etc.
    ///
    /// If the operation fails because of timeout or cancellation, the root cause of the error will
    /// be set to `TimeoutOrCancel`. In such situation it is unknown which deletions, if any, went
    /// through.
    async fn delete_prefix(
        &self,
        prefix: &RemotePath,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let mut stream =
            pin!(self.list_streaming(Some(prefix), ListingMode::NoDelimiter, None, cancel));
        while let Some(result) = stream.next().await {
            let keys = match result {
                Ok(listing) if listing.keys.is_empty() => continue,
                Ok(listing) => listing.keys.into_iter().map(|o| o.key).collect_vec(),
                Err(DownloadError::Cancelled) => return Err(TimeoutOrCancel::Cancel.into()),
                Err(DownloadError::Timeout) => return Err(TimeoutOrCancel::Timeout.into()),
                Err(err) => return Err(err.into()),
            };
            tracing::info!("Deleting {} keys from remote storage", keys.len());
            self.delete_objects(&keys, cancel).await?;
        }
        Ok(())
    }

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

/// Data part of an ongoing [`Download`].
///
/// `DownloadStream` is sensitive to the timeout and cancellation used with the original
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
// Require Clone for `Other` due to https://github.com/rust-lang/rust/issues/26925
#[derive(Clone)]
pub enum GenericRemoteStorage<Other: Clone = Arc<UnreliableWrapper>> {
    LocalFs(LocalFs),
    AwsS3(Arc<S3Bucket>),
    AzureBlob(Arc<AzureBlobStorage>),
    Unreliable(Other),
}

impl<Other: RemoteStorage> GenericRemoteStorage<Arc<Other>> {
    // See [`RemoteStorage::list`].
    pub async fn list(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> Result<Listing, DownloadError> {
        match self {
            Self::LocalFs(s) => s.list(prefix, mode, max_keys, cancel).await,
            Self::AwsS3(s) => s.list(prefix, mode, max_keys, cancel).await,
            Self::AzureBlob(s) => s.list(prefix, mode, max_keys, cancel).await,
            Self::Unreliable(s) => s.list(prefix, mode, max_keys, cancel).await,
        }
    }

    // See [`RemoteStorage::list_streaming`].
    pub fn list_streaming<'a>(
        &'a self,
        prefix: Option<&'a RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &'a CancellationToken,
    ) -> impl Stream<Item = Result<Listing, DownloadError>> + 'a + Send {
        match self {
            Self::LocalFs(s) => Box::pin(s.list_streaming(prefix, mode, max_keys, cancel))
                as Pin<Box<dyn Stream<Item = Result<Listing, DownloadError>> + Send>>,
            Self::AwsS3(s) => Box::pin(s.list_streaming(prefix, mode, max_keys, cancel)),
            Self::AzureBlob(s) => Box::pin(s.list_streaming(prefix, mode, max_keys, cancel)),
            Self::Unreliable(s) => Box::pin(s.list_streaming(prefix, mode, max_keys, cancel)),
        }
    }

    // See [`RemoteStorage::head_object`].
    pub async fn head_object(
        &self,
        key: &RemotePath,
        cancel: &CancellationToken,
    ) -> Result<ListingObject, DownloadError> {
        match self {
            Self::LocalFs(s) => s.head_object(key, cancel).await,
            Self::AwsS3(s) => s.head_object(key, cancel).await,
            Self::AzureBlob(s) => s.head_object(key, cancel).await,
            Self::Unreliable(s) => s.head_object(key, cancel).await,
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

    /// See [`RemoteStorage::download`]
    pub async fn download(
        &self,
        from: &RemotePath,
        opts: &DownloadOpts,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        match self {
            Self::LocalFs(s) => s.download(from, opts, cancel).await,
            Self::AwsS3(s) => s.download(from, opts, cancel).await,
            Self::AzureBlob(s) => s.download(from, opts, cancel).await,
            Self::Unreliable(s) => s.download(from, opts, cancel).await,
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

    /// [`RemoteStorage::max_keys_per_delete`]
    pub fn max_keys_per_delete(&self) -> usize {
        match self {
            Self::LocalFs(s) => s.max_keys_per_delete(),
            Self::AwsS3(s) => s.max_keys_per_delete(),
            Self::AzureBlob(s) => s.max_keys_per_delete(),
            Self::Unreliable(s) => s.max_keys_per_delete(),
        }
    }

    /// See [`RemoteStorage::delete_prefix`]
    pub async fn delete_prefix(
        &self,
        prefix: &RemotePath,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        match self {
            Self::LocalFs(s) => s.delete_prefix(prefix, cancel).await,
            Self::AwsS3(s) => s.delete_prefix(prefix, cancel).await,
            Self::AzureBlob(s) => s.delete_prefix(prefix, cancel).await,
            Self::Unreliable(s) => s.delete_prefix(prefix, cancel).await,
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
    pub async fn from_config(storage_config: &RemoteStorageConfig) -> anyhow::Result<Self> {
        let timeout = storage_config.timeout;

        // If somkeone overrides timeout to be small without adjusting small_timeout, then adjust it automatically
        let small_timeout = std::cmp::min(storage_config.small_timeout, timeout);

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
                Self::AwsS3(Arc::new(S3Bucket::new(s3_config, timeout).await?))
            }
            RemoteStorageKind::AzureContainer(azure_config) => {
                let storage_account = azure_config
                    .storage_account
                    .as_deref()
                    .unwrap_or("<AZURE_STORAGE_ACCOUNT>");
                info!("Using azure container '{}' in account '{storage_account}' in region '{}' as a remote storage, prefix in container: '{:?}'",
                      azure_config.container_name, azure_config.container_region, azure_config.prefix_in_container);
                Self::AzureBlob(Arc::new(AzureBlobStorage::new(
                    azure_config,
                    timeout,
                    small_timeout,
                )?))
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

    /// The name of the bucket/container/etc.
    pub fn bucket_name(&self) -> Option<&str> {
        match self {
            Self::LocalFs(_s) => None,
            Self::AwsS3(s) => Some(s.bucket_name()),
            Self::AzureBlob(s) => Some(s.container_name()),
            Self::Unreliable(_s) => None,
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
            RequestKind::Head => &self.read,
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

    /// DownloadOpts::byte_range() should generate (inclusive, exclusive) ranges
    /// with optional end bound, or None when unbounded.
    #[test]
    fn download_opts_byte_range() {
        // Consider using test_case or a similar table-driven test framework.
        let cases = [
            // (byte_start, byte_end, expected)
            (Bound::Unbounded, Bound::Unbounded, None),
            (Bound::Unbounded, Bound::Included(7), Some((0, Some(8)))),
            (Bound::Unbounded, Bound::Excluded(7), Some((0, Some(7)))),
            (Bound::Included(3), Bound::Unbounded, Some((3, None))),
            (Bound::Included(3), Bound::Included(7), Some((3, Some(8)))),
            (Bound::Included(3), Bound::Excluded(7), Some((3, Some(7)))),
            (Bound::Excluded(3), Bound::Unbounded, Some((4, None))),
            (Bound::Excluded(3), Bound::Included(7), Some((4, Some(8)))),
            (Bound::Excluded(3), Bound::Excluded(7), Some((4, Some(7)))),
            // 1-sized ranges are fine, 0 aren't and will panic (separate test).
            (Bound::Included(3), Bound::Included(3), Some((3, Some(4)))),
            (Bound::Included(3), Bound::Excluded(4), Some((3, Some(4)))),
        ];

        for (byte_start, byte_end, expect) in cases {
            let opts = DownloadOpts {
                byte_start,
                byte_end,
                ..Default::default()
            };
            let result = opts.byte_range();
            assert_eq!(
                result, expect,
                "byte_start={byte_start:?} byte_end={byte_end:?}"
            );

            // Check generated HTTP header, which uses an inclusive range.
            let expect_header = expect.map(|(start, end)| match end {
                Some(end) => format!("bytes={start}-{}", end - 1), // inclusive end
                None => format!("bytes={start}-"),
            });
            assert_eq!(
                opts.byte_range_header(),
                expect_header,
                "byte_start={byte_start:?} byte_end={byte_end:?}"
            );
        }
    }

    /// DownloadOpts::byte_range() zero-sized byte range should panic.
    #[test]
    #[should_panic]
    fn download_opts_byte_range_zero() {
        DownloadOpts {
            byte_start: Bound::Included(3),
            byte_end: Bound::Excluded(3),
            ..Default::default()
        }
        .byte_range();
    }

    /// DownloadOpts::byte_range() negative byte range should panic.
    #[test]
    #[should_panic]
    fn download_opts_byte_range_negative() {
        DownloadOpts {
            byte_start: Bound::Included(3),
            byte_end: Bound::Included(2),
            ..Default::default()
        }
        .byte_range();
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
}
