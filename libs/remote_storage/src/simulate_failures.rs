//! This module provides a wrapper around a real RemoteStorage implementation that
//! causes the first N attempts at each upload or download operatio to fail. For
//! testing purposes.
use bytes::Bytes;
use futures::stream::Stream;
use futures::StreamExt;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Mutex;
use std::time::SystemTime;
use std::{collections::hash_map::Entry, sync::Arc};
use tokio_util::sync::CancellationToken;

use crate::{
    Download, DownloadError, DownloadOpts, GenericRemoteStorage, Listing, ListingMode, RemotePath,
    RemoteStorage, StorageMetadata, TimeTravelError,
};

pub struct UnreliableWrapper {
    inner: GenericRemoteStorage<Arc<VoidStorage>>,

    // This many attempts of each operation will fail, then we let it succeed.
    attempts_to_fail: u64,

    // Tracks how many failed attempts of each operation has been made.
    attempts: Mutex<HashMap<RemoteOp, u64>>,
}

/// Used to identify retries of different unique operation.
#[derive(Debug, Hash, Eq, PartialEq)]
enum RemoteOp {
    ListPrefixes(Option<RemotePath>),
    HeadObject(RemotePath),
    Upload(RemotePath),
    Download(RemotePath),
    Delete(RemotePath),
    DeleteObjects(Vec<RemotePath>),
    TimeTravelRecover(Option<RemotePath>),
}

impl UnreliableWrapper {
    pub fn new(inner: crate::GenericRemoteStorage, attempts_to_fail: u64) -> Self {
        assert!(attempts_to_fail > 0);
        let inner = match inner {
            GenericRemoteStorage::AwsS3(s) => GenericRemoteStorage::AwsS3(s),
            GenericRemoteStorage::AzureBlob(s) => GenericRemoteStorage::AzureBlob(s),
            GenericRemoteStorage::LocalFs(s) => GenericRemoteStorage::LocalFs(s),
            // We could also make this a no-op, as in, extract the inner of the passed generic remote storage
            GenericRemoteStorage::Unreliable(_s) => {
                panic!("Can't wrap unreliable wrapper unreliably")
            }
        };
        UnreliableWrapper {
            inner,
            attempts_to_fail,
            attempts: Mutex::new(HashMap::new()),
        }
    }

    ///
    /// Common functionality for all operations.
    ///
    /// On the first attempts of this operation, return an error. After 'attempts_to_fail'
    /// attempts, let the operation go ahead, and clear the counter.
    ///
    fn attempt(&self, op: RemoteOp) -> anyhow::Result<u64> {
        let mut attempts = self.attempts.lock().unwrap();

        match attempts.entry(op) {
            Entry::Occupied(mut e) => {
                let attempts_before_this = {
                    let p = e.get_mut();
                    *p += 1;
                    *p
                };

                if attempts_before_this >= self.attempts_to_fail {
                    // let it succeed
                    e.remove();
                    Ok(attempts_before_this)
                } else {
                    let error =
                        anyhow::anyhow!("simulated failure of remote operation {:?}", e.key());
                    Err(error)
                }
            }
            Entry::Vacant(e) => {
                let error = anyhow::anyhow!("simulated failure of remote operation {:?}", e.key());
                e.insert(1);
                Err(error)
            }
        }
    }

    async fn delete_inner(
        &self,
        path: &RemotePath,
        attempt: bool,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        if attempt {
            self.attempt(RemoteOp::Delete(path.clone()))?;
        }
        self.inner.delete(path, cancel).await
    }
}

// We never construct this, so the type is not important, just has to not be UnreliableWrapper and impl RemoteStorage.
type VoidStorage = crate::LocalFs;

impl RemoteStorage for UnreliableWrapper {
    fn list_streaming(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> impl Stream<Item = Result<Listing, DownloadError>> + Send {
        async_stream::stream! {
            self.attempt(RemoteOp::ListPrefixes(prefix.cloned()))
                .map_err(DownloadError::Other)?;
            let mut stream = self.inner
                .list_streaming(prefix, mode, max_keys, cancel);
            while let Some(item) = stream.next().await {
                yield item;
            }
        }
    }
    async fn list(
        &self,
        prefix: Option<&RemotePath>,
        mode: ListingMode,
        max_keys: Option<NonZeroU32>,
        cancel: &CancellationToken,
    ) -> Result<Listing, DownloadError> {
        self.attempt(RemoteOp::ListPrefixes(prefix.cloned()))
            .map_err(DownloadError::Other)?;
        self.inner.list(prefix, mode, max_keys, cancel).await
    }

    async fn head_object(
        &self,
        key: &RemotePath,
        cancel: &CancellationToken,
    ) -> Result<crate::ListingObject, DownloadError> {
        self.attempt(RemoteOp::HeadObject(key.clone()))
            .map_err(DownloadError::Other)?;
        self.inner.head_object(key, cancel).await
    }

    async fn upload(
        &self,
        data: impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
        // S3 PUT request requires the content length to be specified,
        // otherwise it starts to fail with the concurrent connection count increasing.
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        self.attempt(RemoteOp::Upload(to.clone()))?;
        self.inner
            .upload(data, data_size_bytes, to, metadata, cancel)
            .await
    }

    async fn download(
        &self,
        from: &RemotePath,
        opts: &DownloadOpts,
        cancel: &CancellationToken,
    ) -> Result<Download, DownloadError> {
        // Note: We treat any byte range as an "attempt" of the same operation.
        // We don't pay attention to the ranges. That's good enough for now.
        self.attempt(RemoteOp::Download(from.clone()))
            .map_err(DownloadError::Other)?;
        self.inner.download(from, opts, cancel).await
    }

    async fn delete(&self, path: &RemotePath, cancel: &CancellationToken) -> anyhow::Result<()> {
        self.delete_inner(path, true, cancel).await
    }

    async fn delete_objects(
        &self,
        paths: &[RemotePath],
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        self.attempt(RemoteOp::DeleteObjects(paths.to_vec()))?;
        let mut error_counter = 0;
        for path in paths {
            // Dont record attempt because it was already recorded above
            if (self.delete_inner(path, false, cancel).await).is_err() {
                error_counter += 1;
            }
        }
        if error_counter > 0 {
            return Err(anyhow::anyhow!(
                "failed to delete {} objects",
                error_counter
            ));
        }
        Ok(())
    }

    fn max_keys_per_delete(&self) -> usize {
        self.inner.max_keys_per_delete()
    }

    async fn copy(
        &self,
        from: &RemotePath,
        to: &RemotePath,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        // copy is equivalent to download + upload
        self.attempt(RemoteOp::Download(from.clone()))?;
        self.attempt(RemoteOp::Upload(to.clone()))?;
        self.inner.copy_object(from, to, cancel).await
    }

    async fn time_travel_recover(
        &self,
        prefix: Option<&RemotePath>,
        timestamp: SystemTime,
        done_if_after: SystemTime,
        cancel: &CancellationToken,
    ) -> Result<(), TimeTravelError> {
        self.attempt(RemoteOp::TimeTravelRecover(prefix.map(|p| p.to_owned())))
            .map_err(TimeTravelError::Other)?;
        self.inner
            .time_travel_recover(prefix, timestamp, done_if_after, cancel)
            .await
    }
}
