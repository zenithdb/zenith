//! The deleter is the final stage in the deletion queue.  It accumulates remote
//! paths to delete, and periodically executes them in batches of up to 1000
//! using the DeleteObjects request.
//!
//! Its purpose is to increase efficiency of remote storage I/O by issuing a smaller
//! number of full-sized DeleteObjects requests, rather than a larger number of
//! smaller requests.

use remote_storage::GenericRemoteStorage;
use remote_storage::RemotePath;
use remote_storage::TimeoutOrCancel;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;
use utils::backoff;
use utils::pausable_failpoint;

use crate::metrics;

use super::DeletionQueueError;
use super::FlushOp;

const AUTOFLUSH_INTERVAL: Duration = Duration::from_secs(10);

pub(super) enum DeleterMessage {
    Delete(Vec<RemotePath>),
    Flush(FlushOp),
}

/// Non-persistent deletion queue, for coalescing multiple object deletes into
/// larger DeleteObjects requests.
pub(super) struct Deleter {
    // Accumulate up to 1000 keys for the next deletion operation
    accumulator: Vec<RemotePath>,

    rx: tokio::sync::mpsc::Receiver<DeleterMessage>,

    cancel: CancellationToken,
    remote_storage: GenericRemoteStorage,
}

impl Deleter {
    pub(super) fn new(
        remote_storage: GenericRemoteStorage,
        rx: tokio::sync::mpsc::Receiver<DeleterMessage>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            remote_storage,
            rx,
            cancel,
            accumulator: Vec::new(),
        }
    }

    /// Wrap the remote `delete_objects` with a failpoint
    async fn remote_delete(&self) -> Result<(), anyhow::Error> {
        // A backoff::retry is used here for two reasons:
        // - To provide a backoff rather than busy-polling the API on errors
        // - To absorb transient 429/503 conditions without hitting our error
        //   logging path for issues deleting objects.
        backoff::retry(
            || async {
                fail::fail_point!("deletion-queue-before-execute", |_| {
                    info!("Skipping execution, failpoint set");

                    metrics::DELETION_QUEUE
                        .remote_errors
                        .with_label_values(&["failpoint"])
                        .inc();
                    Err(anyhow::anyhow!("failpoint: deletion-queue-before-execute"))
                });

                self.remote_storage
                    .delete_objects(&self.accumulator, &self.cancel)
                    .await
            },
            TimeoutOrCancel::caused_by_cancel,
            3,
            10,
            "executing deletion batch",
            &self.cancel,
        )
        .await
        .ok_or_else(|| anyhow::anyhow!("Shutting down"))
        .and_then(|x| x)
    }

    /// Block until everything in accumulator has been executed
    async fn flush(&mut self) -> Result<(), DeletionQueueError> {
        while !self.accumulator.is_empty() && !self.cancel.is_cancelled() {
            pausable_failpoint!("deletion-queue-before-execute-pause");
            match self.remote_delete().await {
                Ok(()) => {
                    // Note: we assume that the remote storage layer returns Ok(()) if some
                    // or all of the deleted objects were already gone.
                    metrics::DELETION_QUEUE
                        .keys_executed
                        .inc_by(self.accumulator.len() as u64);
                    info!(
                        "Executed deletion batch {}..{}",
                        self.accumulator
                            .first()
                            .expect("accumulator should be non-empty"),
                        self.accumulator
                            .last()
                            .expect("accumulator should be non-empty"),
                    );
                    self.accumulator.clear();
                }
                Err(e) => {
                    if self.cancel.is_cancelled() {
                        return Err(DeletionQueueError::ShuttingDown);
                    }
                    warn!("DeleteObjects request failed: {e:#}, will continue trying");
                    metrics::DELETION_QUEUE
                        .remote_errors
                        .with_label_values(&["execute"])
                        .inc();
                }
            };
        }
        if self.cancel.is_cancelled() {
            // Expose an error because we may not have actually flushed everything
            Err(DeletionQueueError::ShuttingDown)
        } else {
            Ok(())
        }
    }

    pub(super) async fn background(&mut self) -> Result<(), DeletionQueueError> {
        let max_keys_per_delete = self.remote_storage.max_keys_per_delete();
        self.accumulator.reserve(max_keys_per_delete);

        loop {
            if self.cancel.is_cancelled() {
                return Err(DeletionQueueError::ShuttingDown);
            }

            let msg = match tokio::time::timeout(AUTOFLUSH_INTERVAL, self.rx.recv()).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    // All queue senders closed
                    info!("Shutting down");
                    return Err(DeletionQueueError::ShuttingDown);
                }
                Err(_) => {
                    // Timeout, we hit deadline to execute whatever we have in hand.  These functions will
                    // return immediately if no work is pending
                    self.flush().await?;

                    continue;
                }
            };

            match msg {
                DeleterMessage::Delete(mut list) => {
                    while !list.is_empty() || self.accumulator.len() == max_keys_per_delete {
                        if self.accumulator.len() == max_keys_per_delete {
                            self.flush().await?;
                            // If we have received this number of keys, proceed with attempting to execute
                            assert_eq!(self.accumulator.len(), 0);
                        }

                        let available_slots = max_keys_per_delete - self.accumulator.len();
                        let take_count = std::cmp::min(available_slots, list.len());
                        for path in list.drain(list.len() - take_count..) {
                            self.accumulator.push(path);
                        }
                    }
                }
                DeleterMessage::Flush(flush_op) => {
                    // If flush() errors, we drop the flush_op and the caller will get
                    // an error recv()'ing their oneshot channel.
                    self.flush().await?;
                    flush_op.notify();
                }
            }
        }
    }
}
