//! AWS S3 storage wrapper around `rusoto` library.
//!
//! Respects `prefix_in_bucket` property from [`S3Config`],
//! allowing multiple api users to independently work with the same S3 bucket, if
//! their bucket prefixes are both specified and different.

use std::{
    borrow::Cow,
    collections::HashMap,
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, Context as _};
use aws_config::{
    default_provider::credentials::DefaultCredentialsChain,
    retry::{RetryConfigBuilder, RetryMode},
    BehaviorVersion,
};
use aws_sdk_s3::{
    config::{AsyncSleep, IdentityCache, Region, SharedAsyncSleep},
    error::SdkError,
    operation::{get_object::GetObjectError, head_object::HeadObjectError},
    types::{Delete, DeleteMarkerEntry, ObjectIdentifier, ObjectVersion, StorageClass},
    Client,
};
use aws_smithy_async::rt::sleep::TokioSleep;
use http_body_util::StreamBody;
use http_types::StatusCode;

use aws_smithy_types::{body::SdkBody, DateTime};
use aws_smithy_types::{byte_stream::ByteStream, date_time::ConversionError};
use bytes::Bytes;
use futures::stream::Stream;
use futures_util::StreamExt;
use hyper::body::Frame;
use scopeguard::ScopeGuard;
use tokio_util::sync::CancellationToken;
use utils::backoff;

use super::StorageMetadata;
use crate::{
    config::S3Config,
    error::Cancelled,
    metrics::{start_counting_cancelled_wait, start_measuring_requests},
    support::PermitCarrying,
    ConcurrencyLimiter, Download, DownloadError, DownloadOpts, Listing, ListingMode, ListingObject,
    RemotePath, RemoteStorage, TimeTravelError, TimeoutOrCancel, MAX_KEYS_PER_DELETE_S3,
    REMOTE_STORAGE_PREFIX_SEPARATOR,
};

use crate::metrics::AttemptOutcome;
pub(super) use crate::metrics::RequestKind;

/// AWS S3 storage.
pub struct S3Bucket {
    client: Client,
    bucket_name: String,
    prefix_in_bucket: Option<String>,
    max_keys_per_list_response: Option<i32>,
    upload_storage_class: Option<StorageClass>,
    concurrency_limiter: ConcurrencyLimiter,
    // Per-request timeout. Accessible for tests.
    pub timeout: Duration,
}

struct GetObjectRequest {
    bucket: String,
    key: String,
    etag: Option<String>,
    range: Option<String>,
}
impl S3Bucket {
    /// Creates the S3 storage, errors if incorrect AWS S3 configuration provided.
    pub async fn new(remote_storage_config: &S3Config, timeout: Duration) -> anyhow::Result<Self> {
        tracing::debug!(
            "Creating s3 remote storage for S3 bucket {}",
            remote_storage_config.bucket_name
        );

        let region = Region::new(remote_storage_config.bucket_region.clone());
        let region_opt = Some(region.clone());

        // https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html
        // https://docs.rs/aws-config/latest/aws_config/default_provider/credentials/struct.DefaultCredentialsChain.html
        // Incomplete list of auth methods used by this:
        // * "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"
        // * "AWS_PROFILE" / `aws sso login --profile <profile>`
        // * "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME"
        // * http (ECS/EKS) container credentials
        // * imds v2
        let credentials_provider = DefaultCredentialsChain::builder()
            .region(region)
            .build()
            .await;

        // AWS SDK requires us to specify how the RetryConfig should sleep when it wants to back off
        let sleep_impl: Arc<dyn AsyncSleep> = Arc::new(TokioSleep::new());

        let sdk_config_loader: aws_config::ConfigLoader = aws_config::defaults(
            #[allow(deprecated)] /* TODO: https://github.com/neondatabase/neon/issues/7665 */
            BehaviorVersion::v2023_11_09(),
        )
        .region(region_opt)
        .identity_cache(IdentityCache::lazy().build())
        .credentials_provider(credentials_provider)
        .sleep_impl(SharedAsyncSleep::from(sleep_impl));

        let sdk_config: aws_config::SdkConfig = std::thread::scope(|s| {
            s.spawn(|| {
                // TODO: make this function async.
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(sdk_config_loader.load())
            })
            .join()
            .unwrap()
        });

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

        // Technically, the `remote_storage_config.endpoint` field only applies to S3 interactions.
        // (In case we ever re-use the `sdk_config` for more than just the S3 client in the future)
        if let Some(custom_endpoint) = remote_storage_config.endpoint.clone() {
            s3_config_builder = s3_config_builder
                .endpoint_url(custom_endpoint)
                .force_path_style(true);
        }

        // We do our own retries (see [`backoff::retry`]).  However, for the AWS SDK to enable rate limiting in response to throttling
        // responses (e.g. 429 on too many ListObjectsv2 requests), we must provide a retry config.  We set it to use at most one
        // attempt, and enable 'Adaptive' mode, which causes rate limiting to be enabled.
        let mut retry_config = RetryConfigBuilder::new();
        retry_config
            .set_max_attempts(Some(1))
            .set_mode(Some(RetryMode::Adaptive));
        s3_config_builder = s3_config_builder.retry_config(retry_config.build());

        let s3_config = s3_config_builder.build();
        let client = aws_sdk_s3::Client::from_conf(s3_config);

        let prefix_in_bucket = remote_storage_config
            .prefix_in_bucket
            .as_deref()
            .map(|prefix| {
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
            bucket_name: remote_storage_config.bucket_name.clone(),
            max_keys_per_list_response: remote_storage_config.max_keys_per_list_response,
            prefix_in_bucket,
            concurrency_limiter: ConcurrencyLimiter::new(
                remote_storage_config.concurrency_limit.get(),
            ),
            upload_storage_class: remote_storage_config.upload_storage_class.clone(),
            timeout,
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
        let path_string = path.get_path().as_str();
        match &self.prefix_in_bucket {
            Some(prefix) => prefix.clone() + "/" + path_string,
            None => path_string.to_string(),
        }
    }

    async fn permit(
        &self,
        kind: RequestKind,
        cancel: &CancellationToken,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, Cancelled> {
        let started_at = start_counting_cancelled_wait(kind);
        let acquire = self.concurrency_limiter.acquire(kind);

        let permit = tokio::select! {
            permit = acquire => permit.expect("semaphore is never closed"),
            _ = cancel.cancelled() => return Err(Cancelled),
        };

        let started_at = ScopeGuard::into_inner(started_at);
        crate::metrics::BUCKET_METRICS
            .wait_seconds
            .observe_elapsed(kind, started_at);

        Ok(permit)
    }

    async fn owned_permit(
        &self,
        kind: RequestKind,
        cancel: &CancellationToken,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, Cancelled> {
        let started_at = start_counting_cancelled_wait(kind);
        let acquire = self.concurrency_limiter.acquire_owned(kind);

        let permit = tokio::select! {
            permit = acquire => permit.expect("semaphore is never closed"),
            _ = cancel.cancelled() => return Err(Cancelled),
        };

        let started_at = ScopeGuard::into_inner(started_at);
        crate::metrics::BUCKET_METRICS
            .wait_seconds
            .observe_elapsed(kind, started_at);
        Ok(permit)
    }

    async fn download_object(
        &self,
        request: GetObjectRequest,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        let kind = RequestKind::Get;

        let permit = self.owned_permit(kind, cancel).await?;

        let started_at = start_measuring_requests(kind);

        let mut builder = self
            .client
            .get_object()
            .bucket(request.bucket)
            .key(request.key)
            .set_range(request.range);

        if let Some(etag) = request.etag {
            builder = builder.if_none_match(etag);
        }

        let get_object = builder.send();

        let get_object = tokio::select! {
            res = get_object => res,
            _ = tokio::time::sleep(self.timeout) => return Err(DownloadError::Timeout),
            _ = cancel.cancelled() => return Err(DownloadError::Cancelled),
        };

        let started_at = ScopeGuard::into_inner(started_at);

        let object_output = match get_object {
            Ok(object_output) => object_output,
            Err(SdkError::ServiceError(e)) if matches!(e.err(), GetObjectError::NoSuchKey(_)) => {
                // Count this in the AttemptOutcome::Ok bucket, because 404 is not
                // an error: we expect to sometimes fetch an object and find it missing,
                // e.g. when probing for timeline indices.
                crate::metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
                    kind,
                    AttemptOutcome::Ok,
                    started_at,
                );
                return Err(DownloadError::NotFound);
            }
            Err(SdkError::ServiceError(e))
                // aws_smithy_runtime_api::http::response::StatusCode isn't
                // re-exported by any aws crates, so just check the numeric
                // status against http_types::StatusCode instead of pulling it.
                if e.raw().status().as_u16() == StatusCode::NotModified =>
            {
                // Count an unmodified file as a success.
                crate::metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
                    kind,
                    AttemptOutcome::Ok,
                    started_at,
                );
                return Err(DownloadError::Unmodified);
            }
            Err(e) => {
                crate::metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
                    kind,
                    AttemptOutcome::Err,
                    started_at,
                );

                return Err(DownloadError::Other(
                    anyhow::Error::new(e).context("download s3 object"),
                ));
            }
        };

        // even if we would have no timeout left, continue anyways. the caller can decide to ignore
        // the errors considering timeouts and cancellation.
        let remaining = self.timeout.saturating_sub(started_at.elapsed());

        let metadata = object_output.metadata().cloned().map(StorageMetadata);
        let etag = object_output
            .e_tag
            .ok_or(DownloadError::Other(anyhow::anyhow!("Missing ETag header")))?
            .into();
        let last_modified = object_output
            .last_modified
            .ok_or(DownloadError::Other(anyhow::anyhow!(
                "Missing LastModified header"
            )))?
            .try_into()
            .map_err(|e: ConversionError| DownloadError::Other(e.into()))?;

        let body = object_output.body;
        let body = ByteStreamAsStream::from(body);
        let body = PermitCarrying::new(permit, body);
        let body = TimedDownload::new(started_at, body);

        let cancel_or_timeout = crate::support::cancel_or_timeout(remaining, cancel.clone());
        let body = crate::support::DownloadStream::new(cancel_or_timeout, body);

        Ok(Download {
            metadata,
            etag,
            last_modified,
            download_stream: Box::pin(body),
        })
    }

    async fn delete_oids(
        &self,
        _permit: &tokio::sync::SemaphorePermit<'_>,
        delete_objects: &[ObjectIdentifier],
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Delete;
        let mut cancel = std::pin::pin!(cancel.cancelled());

        for chunk in delete_objects.chunks(MAX_KEYS_PER_DELETE_S3) {
            let started_at = start_measuring_requests(kind);

            let req = self
                .client
                .delete_objects()
                .bucket(self.bucket_name.clone())
                .delete(
                    Delete::builder()
                        .set_objects(Some(chunk.to_vec()))
                        .build()
                        .context("build request")?,
                )
                .send();

            let resp = tokio::select! {
                resp = req => resp,
                _ = tokio::time::sleep(self.timeout) => return Err(TimeoutOrCancel::Timeout.into()),
                _ = &mut cancel => return Err(TimeoutOrCancel::Cancel.into()),
            };

            let started_at = ScopeGuard::into_inner(started_at);
            crate::metrics::BUCKET_METRICS
                .req_seconds
                .observe_elapsed(kind, &resp, started_at);

            let resp = resp.context("request deletion")?;
            crate::metrics::BUCKET_METRICS
                .deleted_objects_total
                .inc_by(chunk.len() as u64);

            if let Some(errors) = resp.errors {
                // Log a bounded number of the errors within the response:
                // these requests can carry 1000 keys so logging each one
                // would be too verbose, especially as errors may lead us
                // to retry repeatedly.
                const LOG_UP_TO_N_ERRORS: usize = 10;
                for e in errors.iter().take(LOG_UP_TO_N_ERRORS) {
                    tracing::warn!(
                        "DeleteObjects key {} failed: {}: {}",
                        e.key.as_ref().map(Cow::from).unwrap_or("".into()),
                        e.code.as_ref().map(Cow::from).unwrap_or("".into()),
                        e.message.as_ref().map(Cow::from).unwrap_or("".into())
                    );
                }

                return Err(anyhow::anyhow!(
                    "Failed to delete {}/{} objects",
                    errors.len(),
                    chunk.len(),
                ));
            }
        }
        Ok(())
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }
}

pin_project_lite::pin_project! {
    struct ByteStreamAsStream {
        #[pin]
        inner: aws_smithy_types::byte_stream::ByteStream
    }
}

impl From<aws_smithy_types::byte_stream::ByteStream> for ByteStreamAsStream {
    fn from(inner: aws_smithy_types::byte_stream::ByteStream) -> Self {
        ByteStreamAsStream { inner }
    }
}

impl Stream for ByteStreamAsStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // this does the std::io::ErrorKind::Other conversion
        self.project().inner.poll_next(cx).map_err(|x| x.into())
    }

    // cannot implement size_hint because inner.size_hint is remaining size in bytes, which makes
    // sense and Stream::size_hint does not really
}

pin_project_lite::pin_project! {
    /// Times and tracks the outcome of the request.
    struct TimedDownload<S> {
        started_at: std::time::Instant,
        outcome: AttemptOutcome,
        #[pin]
        inner: S
    }

    impl<S> PinnedDrop for TimedDownload<S> {
        fn drop(mut this: Pin<&mut Self>) {
            crate::metrics::BUCKET_METRICS.req_seconds.observe_elapsed(RequestKind::Get, this.outcome, this.started_at);
        }
    }
}

impl<S> TimedDownload<S> {
    fn new(started_at: std::time::Instant, inner: S) -> Self {
        TimedDownload {
            started_at,
            outcome: AttemptOutcome::Cancelled,
            inner,
        }
    }
}

impl<S: Stream<Item = std::io::Result<Bytes>>> Stream for TimedDownload<S> {
    type Item = <S as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::task::ready;

        let this = self.project();

        let res = ready!(this.inner.poll_next(cx));
        match &res {
            Some(Ok(_)) => {}
            Some(Err(_)) => *this.outcome = AttemptOutcome::Err,
            None => *this.outcome = AttemptOutcome::Ok,
        }

        Poll::Ready(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl RemoteStorage for S3Bucket {
    fn list_streaming(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> impl Stream<Item = Result<Listing, DownloadError>> {
        let kind = RequestKind::List;
        // s3 sdk wants i32
        let mut max_keys = max_keys.map(|mk| mk.get() as i32);

        // get the passed prefix or if it is not set use prefix_in_bucket value
        let list_prefix = prefix
            .map(|p| self.relative_path_to_s3_object(p))
            .or_else(|| {
                self.prefix_in_bucket.clone().map(|mut s| {
                    s.push(REMOTE_STORAGE_PREFIX_SEPARATOR);
                    s
                })
            });

        async_stream::stream! {
            let _permit = self.permit(kind, cancel).await?;

            let mut continuation_token = None;
            'outer: loop {
                let started_at = start_measuring_requests(kind);

                // min of two Options, returning Some if one is value and another is
                // None (None is smaller than anything, so plain min doesn't work).
                let request_max_keys = self
                    .max_keys_per_list_response
                    .into_iter()
                    .chain(max_keys.into_iter())
                    .min();
                let mut request = self
                    .client
                    .list_objects_v2()
                    .bucket(self.bucket_name.clone())
                    .set_prefix(list_prefix.clone())
                    .set_continuation_token(continuation_token.clone())
                    .set_max_keys(request_max_keys);

                if let ListingMode::WithDelimiter = mode {
                    request = request.delimiter(REMOTE_STORAGE_PREFIX_SEPARATOR.to_string());
                }

                let request = request.send();

                let response = tokio::select! {
                    res = request => Ok(res),
                    _ = tokio::time::sleep(self.timeout) => Err(DownloadError::Timeout),
                    _ = cancel.cancelled() => Err(DownloadError::Cancelled),
                }?;

                let response = response
                    .context("Failed to list S3 prefixes")
                    .map_err(DownloadError::Other);

                let started_at = ScopeGuard::into_inner(started_at);

                crate::metrics::BUCKET_METRICS
                    .req_seconds
                    .observe_elapsed(kind, &response, started_at);

                let response = match response {
                    Ok(response) => response,
                    Err(e) => {
                        // The error is potentially retryable, so we must rewind the loop after yielding.
                        yield Err(e);
                        continue 'outer;
                    },
                };

                let keys = response.contents();
                let prefixes = response.common_prefixes.as_deref().unwrap_or_default();

                tracing::debug!("list: {} prefixes, {} keys", prefixes.len(), keys.len());
                let mut result = Listing::default();

                for object in keys {
                    let key = object.key().expect("response does not contain a key");
                    let key = self.s3_object_to_relative_path(key);

                    let last_modified = match object.last_modified.map(SystemTime::try_from) {
                        Some(Ok(t)) => t,
                        Some(Err(_)) => {
                            tracing::warn!("Remote storage last_modified {:?} for {} is out of bounds",
                                object.last_modified, key
                        );
                            SystemTime::now()
                        },
                        None => {
                            SystemTime::now()
                        }
                    };

                    let size = object.size.unwrap_or(0) as u64;

                    result.keys.push(ListingObject{
                        key,
                        last_modified,
                        size,
                    });
                    if let Some(mut mk) = max_keys {
                        assert!(mk > 0);
                        mk -= 1;
                        if mk == 0 {
                            // limit reached
                            yield Ok(result);
                            break 'outer;
                        }
                        max_keys = Some(mk);
                    }
                }

                // S3 gives us prefixes like "foo/", we return them like "foo"
                result.prefixes.extend(prefixes.iter().filter_map(|o| {
                    Some(
                        self.s3_object_to_relative_path(
                            o.prefix()?
                                .trim_end_matches(REMOTE_STORAGE_PREFIX_SEPARATOR),
                        ),
                    )
                }));

                yield Ok(result);

                continuation_token = match response.next_continuation_token {
                    Some(new_token) => Some(new_token),
                    None => break,
                };
            }
        }
    }

    async fn head_object(
        &self,
        key: &RemotePath,
        cancel: &CancellationToken,
    ) -> Result<ListingObject, DownloadError> {
        let kind = RequestKind::Head;
        let _permit = self.permit(kind, cancel).await?;

        let started_at = start_measuring_requests(kind);

        let head_future = self
            .client
            .head_object()
            .bucket(self.bucket_name())
            .key(self.relative_path_to_s3_object(key))
            .send();

        let head_future = tokio::time::timeout(self.timeout, head_future);

        let res = tokio::select! {
            res = head_future => res,
            _ = cancel.cancelled() => return Err(TimeoutOrCancel::Cancel.into()),
        };

        let res = res.map_err(|_e| DownloadError::Timeout)?;

        // do not incl. timeouts as errors in metrics but cancellations
        let started_at = ScopeGuard::into_inner(started_at);
        crate::metrics::BUCKET_METRICS
            .req_seconds
            .observe_elapsed(kind, &res, started_at);

        let data = match res {
            Ok(object_output) => object_output,
            Err(SdkError::ServiceError(e)) if matches!(e.err(), HeadObjectError::NotFound(_)) => {
                // Count this in the AttemptOutcome::Ok bucket, because 404 is not
                // an error: we expect to sometimes fetch an object and find it missing,
                // e.g. when probing for timeline indices.
                crate::metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
                    kind,
                    AttemptOutcome::Ok,
                    started_at,
                );
                return Err(DownloadError::NotFound);
            }
            Err(e) => {
                crate::metrics::BUCKET_METRICS.req_seconds.observe_elapsed(
                    kind,
                    AttemptOutcome::Err,
                    started_at,
                );

                return Err(DownloadError::Other(
                    anyhow::Error::new(e).context("s3 head object"),
                ));
            }
        };

        let (Some(last_modified), Some(size)) = (data.last_modified, data.content_length) else {
            return Err(DownloadError::Other(anyhow!(
                "head_object doesn't contain last_modified or content_length"
            )))?;
        };
        Ok(ListingObject {
            key: key.to_owned(),
            last_modified: SystemTime::try_from(last_modified).map_err(|e| {
                DownloadError::Other(anyhow!("can't convert time '{last_modified}': {e}"))
            })?,
            size: size as u64,
        })
    }

    async fn upload(
        &self,
        from: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        from_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Put;
        let _permit = self.permit(kind, cancel).await?;

        let started_at = start_measuring_requests(kind);

        let body = StreamBody::new(from.map(|x| x.map(Frame::data)));
        let bytes_stream = ByteStream::new(SdkBody::from_body_1_x(body));

        let upload = self
            .client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(self.relative_path_to_s3_object(to))
            .set_metadata(metadata.map(|m| m.0))
            .set_storage_class(self.upload_storage_class.clone())
            .content_length(from_size_bytes.try_into()?)
            .body(bytes_stream)
            .send();

        let upload = tokio::time::timeout(self.timeout, upload);

        let res = tokio::select! {
            res = upload => res,
            _ = cancel.cancelled() => return Err(TimeoutOrCancel::Cancel.into()),
        };

        if let Ok(inner) = &res {
            // do not incl. timeouts as errors in metrics but cancellations
            let started_at = ScopeGuard::into_inner(started_at);
            crate::metrics::BUCKET_METRICS
                .req_seconds
                .observe_elapsed(kind, inner, started_at);
        }

        match res {
            Ok(Ok(_put)) => Ok(()),
            Ok(Err(sdk)) => Err(sdk.into()),
            Err(_timeout) => Err(TimeoutOrCancel::Timeout.into()),
        }
    }

    async fn copy(
        &self,
        from: &RemotePath,
        to: &RemotePath,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Copy;
        let _permit = self.permit(kind, cancel).await?;

        let timeout = tokio::time::sleep(self.timeout);

        let started_at = start_measuring_requests(kind);

        // we need to specify bucket_name as a prefix
        let copy_source = format!(
            "{}/{}",
            self.bucket_name,
            self.relative_path_to_s3_object(from)
        );

        let op = self
            .client
            .copy_object()
            .bucket(self.bucket_name.clone())
            .key(self.relative_path_to_s3_object(to))
            .set_storage_class(self.upload_storage_class.clone())
            .copy_source(copy_source)
            .send();

        let res = tokio::select! {
            res = op => res,
            _ = timeout => return Err(TimeoutOrCancel::Timeout.into()),
            _ = cancel.cancelled() => return Err(TimeoutOrCancel::Cancel.into()),
        };

        let started_at = ScopeGuard::into_inner(started_at);
        crate::metrics::BUCKET_METRICS
            .req_seconds
            .observe_elapsed(kind, &res, started_at);

        res?;

        Ok(())
    }

    async fn download(
        &self,
        from: &RemotePath,
        opts: &DownloadOpts,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        // if prefix is not none then download file `prefix/from`
        // if prefix is none then download file `from`
        self.download_object(
            GetObjectRequest {
                bucket: self.bucket_name.clone(),
                key: self.relative_path_to_s3_object(from),
                etag: opts.etag.as_ref().map(|e| e.to_string()),
                range: opts.byte_range_header(),
            },
            cancel,
        )
        .await
    }

    async fn delete_objects(
        &self,
        paths: &[RemotePath],
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let kind = RequestKind::Delete;
        let permit = self.permit(kind, cancel).await?;
        let mut delete_objects = Vec::with_capacity(paths.len());
        for path in paths {
            let obj_id = ObjectIdentifier::builder()
                .set_key(Some(self.relative_path_to_s3_object(path)))
                .build()
                .context("convert path to oid")?;
            delete_objects.push(obj_id);
        }

        self.delete_oids(&permit, &delete_objects, cancel).await
    }

    fn max_keys_per_delete(&self) -> usize {
        MAX_KEYS_PER_DELETE_S3
    }

    async fn delete(&self, path: &RemotePath, cancel: &CancellationToken) -> anyhow::Result<()> {
        let paths = std::array::from_ref(path);
        self.delete_objects(paths, cancel).await
    }

    async fn time_travel_recover(
        &self,
        prefix: Option<&RemotePath>,
        timestamp: SystemTime,
        done_if_after: SystemTime,
        cancel: &CancellationToken,
    ) -> Result<(), TimeTravelError> {
        let kind = RequestKind::TimeTravel;
        let permit = self.permit(kind, cancel).await?;

        let timestamp = DateTime::from(timestamp);
        let done_if_after = DateTime::from(done_if_after);

        tracing::trace!("Target time: {timestamp:?}, done_if_after {done_if_after:?}");

        // get the passed prefix or if it is not set use prefix_in_bucket value
        let prefix = prefix
            .map(|p| self.relative_path_to_s3_object(p))
            .or_else(|| self.prefix_in_bucket.clone());

        let warn_threshold = 3;
        let max_retries = 10;
        let is_permanent = |e: &_| matches!(e, TimeTravelError::Cancelled);

        let mut key_marker = None;
        let mut version_id_marker = None;
        let mut versions_and_deletes = Vec::new();

        loop {
            let response = backoff::retry(
                || async {
                    let op = self
                        .client
                        .list_object_versions()
                        .bucket(self.bucket_name.clone())
                        .set_prefix(prefix.clone())
                        .set_key_marker(key_marker.clone())
                        .set_version_id_marker(version_id_marker.clone())
                        .send();

                    tokio::select! {
                        res = op => res.map_err(|e| TimeTravelError::Other(e.into())),
                        _ = cancel.cancelled() => Err(TimeTravelError::Cancelled),
                    }
                },
                is_permanent,
                warn_threshold,
                max_retries,
                "listing object versions for time_travel_recover",
                cancel,
            )
            .await
            .ok_or_else(|| TimeTravelError::Cancelled)
            .and_then(|x| x)?;

            tracing::trace!(
                "  Got List response version_id_marker={:?}, key_marker={:?}",
                response.version_id_marker,
                response.key_marker
            );
            let versions = response
                .versions
                .unwrap_or_default()
                .into_iter()
                .map(VerOrDelete::from_version);
            let deletes = response
                .delete_markers
                .unwrap_or_default()
                .into_iter()
                .map(VerOrDelete::from_delete_marker);
            itertools::process_results(versions.chain(deletes), |n_vds| {
                versions_and_deletes.extend(n_vds)
            })
            .map_err(TimeTravelError::Other)?;
            fn none_if_empty(v: Option<String>) -> Option<String> {
                v.filter(|v| !v.is_empty())
            }
            version_id_marker = none_if_empty(response.next_version_id_marker);
            key_marker = none_if_empty(response.next_key_marker);
            if version_id_marker.is_none() {
                // The final response is not supposed to be truncated
                if response.is_truncated.unwrap_or_default() {
                    return Err(TimeTravelError::Other(anyhow::anyhow!(
                        "Received truncated ListObjectVersions response for prefix={prefix:?}"
                    )));
                }
                break;
            }
            // Limit the number of versions deletions, mostly so that we don't
            // keep requesting forever if the list is too long, as we'd put the
            // list in RAM.
            // Building a list of 100k entries that reaches the limit roughly takes
            // 40 seconds, and roughly corresponds to tenants of 2 TiB physical size.
            const COMPLEXITY_LIMIT: usize = 100_000;
            if versions_and_deletes.len() >= COMPLEXITY_LIMIT {
                return Err(TimeTravelError::TooManyVersions);
            }
        }

        tracing::info!(
            "Built list for time travel with {} versions and deletions",
            versions_and_deletes.len()
        );

        // Work on the list of references instead of the objects directly,
        // otherwise we get lifetime errors in the sort_by_key call below.
        let mut versions_and_deletes = versions_and_deletes.iter().collect::<Vec<_>>();

        versions_and_deletes.sort_by_key(|vd| (&vd.key, &vd.last_modified));

        let mut vds_for_key = HashMap::<_, Vec<_>>::new();

        for vd in &versions_and_deletes {
            let VerOrDelete {
                version_id, key, ..
            } = &vd;
            if version_id == "null" {
                return Err(TimeTravelError::Other(anyhow!("Received ListVersions response for key={key} with version_id='null', \
                    indicating either disabled versioning, or legacy objects with null version id values")));
            }
            tracing::trace!(
                "Parsing version key={key} version_id={version_id} kind={:?}",
                vd.kind
            );

            vds_for_key.entry(key).or_default().push(vd);
        }
        for (key, versions) in vds_for_key {
            let last_vd = versions.last().unwrap();
            if last_vd.last_modified > done_if_after {
                tracing::trace!("Key {key} has version later than done_if_after, skipping");
                continue;
            }
            // the version we want to restore to.
            let version_to_restore_to =
                match versions.binary_search_by_key(&timestamp, |tpl| tpl.last_modified) {
                    Ok(v) => v,
                    Err(e) => e,
                };
            if version_to_restore_to == versions.len() {
                tracing::trace!("Key {key} has no changes since timestamp, skipping");
                continue;
            }
            let mut do_delete = false;
            if version_to_restore_to == 0 {
                // All versions more recent, so the key didn't exist at the specified time point.
                tracing::trace!(
                    "All {} versions more recent for {key}, deleting",
                    versions.len()
                );
                do_delete = true;
            } else {
                match &versions[version_to_restore_to - 1] {
                    VerOrDelete {
                        kind: VerOrDeleteKind::Version,
                        version_id,
                        ..
                    } => {
                        tracing::trace!("Copying old version {version_id} for {key}...");
                        // Restore the state to the last version by copying
                        let source_id =
                            format!("{}/{key}?versionId={version_id}", self.bucket_name);

                        backoff::retry(
                            || async {
                                let op = self
                                    .client
                                    .copy_object()
                                    .bucket(self.bucket_name.clone())
                                    .key(key)
                                    .set_storage_class(self.upload_storage_class.clone())
                                    .copy_source(&source_id)
                                    .send();

                                tokio::select! {
                                    res = op => res.map_err(|e| TimeTravelError::Other(e.into())),
                                    _ = cancel.cancelled() => Err(TimeTravelError::Cancelled),
                                }
                            },
                            is_permanent,
                            warn_threshold,
                            max_retries,
                            "copying object version for time_travel_recover",
                            cancel,
                        )
                        .await
                        .ok_or_else(|| TimeTravelError::Cancelled)
                        .and_then(|x| x)?;
                        tracing::info!(%version_id, %key, "Copied old version in S3");
                    }
                    VerOrDelete {
                        kind: VerOrDeleteKind::DeleteMarker,
                        ..
                    } => {
                        do_delete = true;
                    }
                }
            };
            if do_delete {
                if matches!(last_vd.kind, VerOrDeleteKind::DeleteMarker) {
                    // Key has since been deleted (but there was some history), no need to do anything
                    tracing::trace!("Key {key} already deleted, skipping.");
                } else {
                    tracing::trace!("Deleting {key}...");

                    let oid = ObjectIdentifier::builder()
                        .key(key.to_owned())
                        .build()
                        .map_err(|e| TimeTravelError::Other(e.into()))?;

                    self.delete_oids(&permit, &[oid], cancel)
                        .await
                        .map_err(|e| {
                            // delete_oid0 will use TimeoutOrCancel
                            if TimeoutOrCancel::caused_by_cancel(&e) {
                                TimeTravelError::Cancelled
                            } else {
                                TimeTravelError::Other(e)
                            }
                        })?;
                }
            }
        }
        Ok(())
    }
}

// Save RAM and only store the needed data instead of the entire ObjectVersion/DeleteMarkerEntry
struct VerOrDelete {
    kind: VerOrDeleteKind,
    last_modified: DateTime,
    version_id: String,
    key: String,
}

#[derive(Debug)]
enum VerOrDeleteKind {
    Version,
    DeleteMarker,
}

impl VerOrDelete {
    fn with_kind(
        kind: VerOrDeleteKind,
        last_modified: Option<DateTime>,
        version_id: Option<String>,
        key: Option<String>,
    ) -> anyhow::Result<Self> {
        let lvk = (last_modified, version_id, key);
        let (Some(last_modified), Some(version_id), Some(key)) = lvk else {
            anyhow::bail!(
                "One (or more) of last_modified, key, and id is None. \
            Is versioning enabled in the bucket? last_modified={:?}, version_id={:?}, key={:?}",
                lvk.0,
                lvk.1,
                lvk.2,
            );
        };
        Ok(Self {
            kind,
            last_modified,
            version_id,
            key,
        })
    }
    fn from_version(v: ObjectVersion) -> anyhow::Result<Self> {
        Self::with_kind(
            VerOrDeleteKind::Version,
            v.last_modified,
            v.version_id,
            v.key,
        )
    }
    fn from_delete_marker(v: DeleteMarkerEntry) -> anyhow::Result<Self> {
        Self::with_kind(
            VerOrDeleteKind::DeleteMarker,
            v.last_modified,
            v.version_id,
            v.key,
        )
    }
}

#[cfg(test)]
mod tests {
    use camino::Utf8Path;
    use std::num::NonZeroUsize;

    use crate::{RemotePath, S3Bucket, S3Config};

    #[tokio::test]
    async fn relative_path() {
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
        let expected_outputs = [
            vec!["", "some/path", "some/path/"],
            vec!["/", "/some/path", "/some/path/"],
            vec![
                "test/prefix/",
                "test/prefix/some/path",
                "test/prefix/some/path/",
            ],
            vec![
                "test/prefix/",
                "test/prefix/some/path",
                "test/prefix/some/path/",
            ],
            vec![
                "test/prefix/",
                "test/prefix/some/path",
                "test/prefix/some/path/",
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
                upload_storage_class: None,
            };
            let storage = S3Bucket::new(&config, std::time::Duration::ZERO)
                .await
                .expect("remote storage init");
            for (test_path_idx, test_path) in all_paths.iter().enumerate() {
                let result = storage.relative_path_to_s3_object(test_path);
                let expected = expected_outputs[prefix_idx][test_path_idx];
                assert_eq!(result, expected);
            }
        }
    }
}
