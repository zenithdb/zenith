//! AWS S3 storage wrapper around `rusoto` library.
//!
//! Respects `prefix_in_bucket` property from [`S3Config`],
//! allowing multiple api users to independently work with the same S3 bucket, if
//! their bucket prefixes are both specified and different.

use std::sync::Arc;

use anyhow::Context;
use aws_config::{
    environment::credentials::EnvironmentVariableCredentialsProvider,
    imds::credentials::ImdsCredentialsProvider, meta::credentials::CredentialsProviderChain,
    provider_config::ProviderConfig, web_identity_token::WebIdentityTokenCredentialsProvider,
};
use aws_credential_types::cache::CredentialsCache;
use aws_sdk_s3::{
    config::{Config, Region},
    error::SdkError,
    operation::get_object::GetObjectError,
    primitives::ByteStream,
    types::{Delete, ObjectIdentifier},
    Client,
};
use aws_smithy_http::body::SdkBody;
use hyper::Body;
use scopeguard::ScopeGuard;
use tokio::{
    io::{self, AsyncRead},
    sync::Semaphore,
};
use tokio_util::io::ReaderStream;
use tracing::debug;

use super::StorageMetadata;
use crate::{
    Download, DownloadError, RemotePath, RemoteStorage, S3Config, MAX_KEYS_PER_DELETE,
    REMOTE_STORAGE_PREFIX_SEPARATOR,
};

pub(super) mod metrics;

use self::metrics::{AttemptOutcome, RequestKind};

/// AWS S3 storage.
pub struct S3Bucket {
    client: Client,
    bucket_name: String,
    prefix_in_bucket: Option<String>,
    max_keys_per_list_response: Option<i32>,
    concurrency_limiter: ConcurrencyLimiter,
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

#[derive(Default)]
struct GetObjectRequest {
    bucket: String,
    key: String,
    range: Option<String>,
}
impl S3Bucket {
    /// Creates the S3 storage, errors if incorrect AWS S3 configuration provided.
    pub fn new(aws_config: &S3Config) -> anyhow::Result<Self> {
        debug!(
            "Creating s3 remote storage for S3 bucket {}",
            aws_config.bucket_name
        );

        let region = Some(Region::new(aws_config.bucket_region.clone()));

        let credentials_provider = {
            // uses "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
            CredentialsProviderChain::first_try(
                "env",
                EnvironmentVariableCredentialsProvider::new(),
            )
            // uses "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME"
            // needed to access remote extensions bucket
            .or_else("token", {
                let provider_conf = ProviderConfig::without_region().with_region(region.clone());

                WebIdentityTokenCredentialsProvider::builder()
                    .configure(&provider_conf)
                    .build()
            })
            // uses imds v2
            .or_else("imds", ImdsCredentialsProvider::builder().build())
        };

        let mut config_builder = Config::builder()
            .region(region)
            .credentials_cache(CredentialsCache::lazy())
            .credentials_provider(credentials_provider);

        if let Some(custom_endpoint) = aws_config.endpoint.clone() {
            config_builder = config_builder
                .endpoint_url(custom_endpoint)
                .force_path_style(true);
        }
        let client = Client::from_conf(config_builder.build());

        let prefix_in_bucket = aws_config.prefix_in_bucket.as_deref().map(|prefix| {
            let mut prefix = prefix;
            while prefix.starts_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                prefix = &prefix[1..]
            }

            let mut prefix = prefix.to_string();
            while prefix.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                prefix.pop();
            }
            prefix
        });
        Ok(Self {
            client,
            bucket_name: aws_config.bucket_name.clone(),
            max_keys_per_list_response: aws_config.max_keys_per_list_response,
            prefix_in_bucket,
            concurrency_limiter: ConcurrencyLimiter::new(aws_config.concurrency_limit.get()),
        })
    }

    fn s3_object_to_relative_path(&self, key: &str) -> RemotePath {
        let relative_path =
            match key.strip_prefix(self.prefix_in_bucket.as_deref().unwrap_or_default()) {
                Some(stripped) => stripped,
                // we rely on AWS to return properly prefixed paths
                // for requests with a certain prefix
                None => panic!(
                    "Key {} does not start with bucket prefix {:?}",
                    key, self.prefix_in_bucket
                ),
            };
        RemotePath(
            relative_path
                .split(REMOTE_STORAGE_PREFIX_SEPARATOR)
                .collect(),
        )
    }

    pub fn relative_path_to_s3_object(&self, path: &RemotePath) -> String {
        assert_eq!(std::path::MAIN_SEPARATOR, REMOTE_STORAGE_PREFIX_SEPARATOR);
        let path_string = path
            .get_path()
            .as_str()
            .trim_end_matches(REMOTE_STORAGE_PREFIX_SEPARATOR);
        match &self.prefix_in_bucket {
            Some(prefix) => prefix.clone() + "/" + path_string,
            None => path_string.to_string(),
        }
    }

    async fn permit(&self, kind: RequestKind) -> tokio::sync::SemaphorePermit<'_> {
        let started_at = start_counting_cancelled_wait(kind);
        let permit = self
            .concurrency_limiter
            .acquire(kind)
            .await
            .expect("semaphore is never closed");

        let started_at = ScopeGuard::into_inner(started_at);
        metrics::BUCKET_METRICS
            .wait_seconds
            .observe_elapsed(kind, started_at);

        permit
    }

    async fn owned_permit(&self, kind: RequestKind) -> tokio::sync::OwnedSemaphorePermit {
        let started_at = start_counting_cancelled_wait(kind);
        let permit = self
            .concurrency_limiter
            .acquire_owned(kind)
            .await
            .expect("semaphore is never closed");

        let started_at = ScopeGuard::into_inner(started_at);
        metrics::BUCKET_METRICS
            .wait_seconds
            .observe_elapsed(kind, started_at);
        permit
    }

    async fn download_object(&self, request: GetObjectRequest) -> Result<Download, DownloadError> {
        let kind = RequestKind::Get;
        let permit = self.owned_permit(kind).await;

        let started_at = start_measuring_requests(kind);

        let get_object = self
            .client
            .get_object()
            .bucket(request.bucket)
            .key(request.key)
            .set_range(request.range)
            .send()
            .await;

        let started_at = ScopeGuard::into_inner(started_at);

        if get_object.is_err() {
            metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
                kind,
                AttemptOutcome::Err,
                started_at,
            );
        }

        match get_object {
            Ok(object_output) => {
                let metadata = object_output.metadata().cloned().map(StorageMetadata);
                Ok(Download {
                    metadata,
                    download_stream: Box::pin(io::BufReader::new(TimedDownload::new(
                        started_at,
                        RatelimitedAsyncRead::new(permit, object_output.body.into_async_read()),
                    ))),
                })
            }
            Err(SdkError::ServiceError(e)) if matches!(e.err(), GetObjectError::NoSuchKey(_)) => {
                Err(DownloadError::NotFound)
            }
            Err(e) => Err(DownloadError::Other(
                anyhow::Error::new(e).context("download s3 object"),
            )),
        }
    }
}

pin_project_lite::pin_project! {
    /// An `AsyncRead` adapter which carries a permit for the lifetime of the value.
    struct RatelimitedAsyncRead<S> {
        permit: tokio::sync::OwnedSemaphorePermit,
        #[pin]
        inner: S,
    }
}

impl<S: AsyncRead> RatelimitedAsyncRead<S> {
    fn new(permit: tokio::sync::OwnedSemaphorePermit, inner: S) -> Self {
        RatelimitedAsyncRead { permit, inner }
    }
}

impl<S: AsyncRead> AsyncRead for RatelimitedAsyncRead<S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();
        this.inner.poll_read(cx, buf)
    }
}

pin_project_lite::pin_project! {
    /// Times and tracks the outcome of the request.
    struct TimedDownload<S> {
        started_at: std::time::Instant,
        outcome: metrics::AttemptOutcome,
        #[pin]
        inner: S
    }

    impl<S> PinnedDrop for TimedDownload<S> {
        fn drop(mut this: Pin<&mut Self>) {
            metrics::BUCKET_METRICS.req_seconds.observe_elapsed(RequestKind::Get, this.outcome, this.started_at);
        }
    }
}

impl<S: AsyncRead> TimedDownload<S> {
    fn new(started_at: std::time::Instant, inner: S) -> Self {
        TimedDownload {
            started_at,
            outcome: metrics::AttemptOutcome::Cancelled,
            inner,
        }
    }
}

impl<S: AsyncRead> AsyncRead for TimedDownload<S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();
        let before = buf.filled().len();
        let read = std::task::ready!(this.inner.poll_read(cx, buf));

        let read_eof = buf.filled().len() == before;

        match read {
            Ok(()) if read_eof => *this.outcome = AttemptOutcome::Ok,
            Ok(()) => { /* still in progress */ }
            Err(_) => *this.outcome = AttemptOutcome::Err,
        }

        std::task::Poll::Ready(read)
    }
}

#[async_trait::async_trait]
impl RemoteStorage for S3Bucket {
    /// See the doc for `RemoteStorage::list_prefixes`
    /// Note: it won't include empty "directories"
    async fn list_prefixes(
        &self,
        prefix: Option<&RemotePath>,
    ) -> Result<Vec<RemotePath>, DownloadError> {
        let kind = RequestKind::List;

        // get the passed prefix or if it is not set use prefix_in_bucket value
        let list_prefix = prefix
            .map(|p| self.relative_path_to_s3_object(p))
            .or_else(|| self.prefix_in_bucket.clone())
            .map(|mut p| {
                // required to end with a separator
                // otherwise request will return only the entry of a prefix
                if !p.ends_with(REMOTE_STORAGE_PREFIX_SEPARATOR) {
                    p.push(REMOTE_STORAGE_PREFIX_SEPARATOR);
                }
                p
            });

        let mut document_keys = Vec::new();

        let mut continuation_token = None;

        loop {
            let _guard = self.permit(kind).await;
            let started_at = start_measuring_requests(kind);

            let fetch_response = self
                .client
                .list_objects_v2()
                .bucket(self.bucket_name.clone())
                .set_prefix(list_prefix.clone())
                .set_continuation_token(continuation_token)
                .delimiter(REMOTE_STORAGE_PREFIX_SEPARATOR.to_string())
                .set_max_keys(self.max_keys_per_list_response)
                .send()
                .await
                .context("Failed to list S3 prefixes")
                .map_err(DownloadError::Other);

            let started_at = ScopeGuard::into_inner(started_at);

            metrics::BUCKET_METRICS
                .req_seconds
                .observe_elapsed(kind, &fetch_response, started_at);

            let fetch_response = fetch_response?;

            document_keys.extend(
                fetch_response
                    .common_prefixes
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|o| Some(self.s3_object_to_relative_path(o.prefix()?))),
            );

            continuation_token = match fetch_response.next_continuation_token {
                Some(new_token) => Some(new_token),
                None => break,
            };
        }

        Ok(document_keys)
    }

    /// See the doc for `RemoteStorage::list_files`
    async fn list_files(&self, folder: Option<&RemotePath>) -> anyhow::Result<Vec<RemotePath>> {
        let kind = RequestKind::List;

        let folder_name = folder
            .map(|p| self.relative_path_to_s3_object(p))
            .or_else(|| self.prefix_in_bucket.clone());

        // AWS may need to break the response into several parts
        let mut continuation_token = None;
        let mut all_files = vec![];
        loop {
            let _guard = self.permit(kind).await;
            let started_at = start_measuring_requests(kind);

            let response = self
                .client
                .list_objects_v2()
                .bucket(self.bucket_name.clone())
                .set_prefix(folder_name.clone())
                .set_continuation_token(continuation_token)
                .set_max_keys(self.max_keys_per_list_response)
                .send()
                .await
                .context("Failed to list files in S3 bucket");

            let started_at = ScopeGuard::into_inner(started_at);
            metrics::BUCKET_METRICS
                .req_seconds
                .observe_elapsed(kind, &response, started_at);

            let response = response?;

            for object in response.contents().unwrap_or_default() {
                let object_path = object.key().expect("response does not contain a key");
                let remote_path = self.s3_object_to_relative_path(object_path);
                all_files.push(remote_path);
            }
            match response.next_continuation_token {
                Some(new_token) => continuation_token = Some(new_token),
                None => break,
            }
        }
        Ok(all_files)
    }

    async fn upload(
        &self,
        from: impl io::AsyncRead + Unpin + Send + Sync + 'static,
        from_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Put;
        let _guard = self.permit(kind).await;

        let started_at = start_measuring_requests(kind);

        let body = Body::wrap_stream(ReaderStream::new(from));
        let bytes_stream = ByteStream::new(SdkBody::from(body));

        let res = self
            .client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(self.relative_path_to_s3_object(to))
            .set_metadata(metadata.map(|m| m.0))
            .content_length(from_size_bytes.try_into()?)
            .body(bytes_stream)
            .send()
            .await;

        let started_at = ScopeGuard::into_inner(started_at);
        metrics::BUCKET_METRICS
            .req_seconds
            .observe_elapsed(kind, &res, started_at);

        res?;

        Ok(())
    }

    async fn download(&self, from: &RemotePath) -> Result<Download, DownloadError> {
        // if prefix is not none then download file `prefix/from`
        // if prefix is none then download file `from`
        self.download_object(GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: self.relative_path_to_s3_object(from),
            range: None,
        })
        .await
    }

    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError> {
        // S3 accepts ranges as https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        // and needs both ends to be exclusive
        let end_inclusive = end_exclusive.map(|end| end.saturating_sub(1));
        let range = Some(match end_inclusive {
            Some(end_inclusive) => format!("bytes={start_inclusive}-{end_inclusive}"),
            None => format!("bytes={start_inclusive}-"),
        });

        self.download_object(GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: self.relative_path_to_s3_object(from),
            range,
        })
        .await
    }
    async fn delete_objects<'a>(&self, paths: &'a [RemotePath]) -> anyhow::Result<()> {
        let kind = RequestKind::Delete;
        let _guard = self.permit(kind).await;

        let mut delete_objects = Vec::with_capacity(paths.len());
        for path in paths {
            let obj_id = ObjectIdentifier::builder()
                .set_key(Some(self.relative_path_to_s3_object(path)))
                .build();
            delete_objects.push(obj_id);
        }

        for chunk in delete_objects.chunks(MAX_KEYS_PER_DELETE) {
            let started_at = start_measuring_requests(kind);

            let resp = self
                .client
                .delete_objects()
                .bucket(self.bucket_name.clone())
                .delete(Delete::builder().set_objects(Some(chunk.to_vec())).build())
                .send()
                .await;

            let started_at = ScopeGuard::into_inner(started_at);
            metrics::BUCKET_METRICS
                .req_seconds
                .observe_elapsed(kind, &resp, started_at);

            match resp {
                Ok(resp) => {
                    metrics::BUCKET_METRICS
                        .deleted_objects_total
                        .inc_by(chunk.len() as u64);
                    if let Some(errors) = resp.errors {
                        return Err(anyhow::format_err!(
                            "Failed to delete {} objects",
                            errors.len()
                        ));
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    async fn delete(&self, path: &RemotePath) -> anyhow::Result<()> {
        let paths = std::array::from_ref(path);
        self.delete_objects(paths).await
    }
}

/// On drop (cancellation) count towards [`metrics::BucketMetrics::cancelled_waits`].
fn start_counting_cancelled_wait(
    kind: RequestKind,
) -> ScopeGuard<std::time::Instant, impl FnOnce(std::time::Instant), scopeguard::OnSuccess> {
    scopeguard::guard_on_success(std::time::Instant::now(), move |_| {
        metrics::BUCKET_METRICS.cancelled_waits.get(kind).inc()
    })
}

/// On drop (cancellation) add time to [`metrics::BucketMetrics::req_seconds`].
fn start_measuring_requests(
    kind: RequestKind,
) -> ScopeGuard<std::time::Instant, impl FnOnce(std::time::Instant), scopeguard::OnSuccess> {
    scopeguard::guard_on_success(std::time::Instant::now(), move |started_at| {
        metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
            kind,
            AttemptOutcome::Cancelled,
            started_at,
        )
    })
}

#[cfg(test)]
mod tests {
    use camino::Utf8Path;
    use std::num::NonZeroUsize;

    use crate::{RemotePath, S3Bucket, S3Config};

    #[test]
    fn relative_path() {
        let all_paths = ["", "some/path", "some/path/"];
        let all_paths: Vec<RemotePath> = all_paths
            .iter()
            .map(|x| RemotePath::new(Utf8Path::new(x)).expect("bad path"))
            .collect();
        let prefixes = [
            None,
            Some(""),
            Some("test/prefix"),
            Some("test/prefix/"),
            Some("/test/prefix/"),
        ];
        let expected_outputs = vec![
            vec!["", "some/path", "some/path"],
            vec!["/", "/some/path", "/some/path"],
            vec![
                "test/prefix/",
                "test/prefix/some/path",
                "test/prefix/some/path",
            ],
            vec![
                "test/prefix/",
                "test/prefix/some/path",
                "test/prefix/some/path",
            ],
            vec![
                "test/prefix/",
                "test/prefix/some/path",
                "test/prefix/some/path",
            ],
        ];

        for (prefix_idx, prefix) in prefixes.iter().enumerate() {
            let config = S3Config {
                bucket_name: "bucket".to_owned(),
                bucket_region: "region".to_owned(),
                prefix_in_bucket: prefix.map(str::to_string),
                endpoint: None,
                concurrency_limit: NonZeroUsize::new(100).unwrap(),
                max_keys_per_list_response: Some(5),
            };
            let storage = S3Bucket::new(&config).expect("remote storage init");
            for (test_path_idx, test_path) in all_paths.iter().enumerate() {
                let result = storage.relative_path_to_s3_object(test_path);
                let expected = expected_outputs[prefix_idx][test_path_idx];
                assert_eq!(result, expected);
            }
        }
    }
}
