use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::config::PageServerConf;
use crate::control_plane_client::ControlPlaneGenerationsApi;
use crate::metrics::DELETION_QUEUE_DROPPED;
use crate::metrics::DELETION_QUEUE_ERRORS;

use super::executor::ExecutorMessage;
use super::DeletionHeader;
use super::DeletionList;
use super::DeletionQueueError;
use super::FlushOp;
use super::VisibleLsnUpdates;

// After this length of time, do any validation work that is pending,
// even if we haven't accumulated many keys to delete.
//
// This also causes updates to remote_consistent_lsn to be validated, even
// if there were no deletions enqueued.
const AUTOFLUSH_INTERVAL: Duration = Duration::from_secs(10);

// If we have received this number of keys, proceed with attempting to execute
const AUTOFLUSH_KEY_COUNT: usize = 16384;

#[derive(Debug)]
pub(super) enum BackendQueueMessage {
    Delete(DeletionList),
    Flush(FlushOp),
}
pub(super) struct BackendQueueWorker<C>
where
    C: ControlPlaneGenerationsApi,
{
    conf: &'static PageServerConf,
    rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,
    tx: tokio::sync::mpsc::Sender<ExecutorMessage>,

    // Client for calling into control plane API for validation of deletes
    control_plane_client: Option<C>,

    // DeletionLists which are waiting generation validation.  Not safe to
    // execute until [`validate`] has processed them.
    pending_lists: Vec<DeletionList>,

    // DeletionLists which have passed validation and are ready to execute.
    validated_lists: Vec<DeletionList>,

    // Sum of all the lengths of lists in pending_lists
    pending_key_count: usize,

    // Lsn validation state: we read projected LSNs and write back visible LSNs
    // after validation.  This is the LSN equivalent of `pending_validation_lists`:
    // it is drained in [`validate`]
    lsn_table: Arc<std::sync::RwLock<VisibleLsnUpdates>>,

    cancel: CancellationToken,
}

impl<C> BackendQueueWorker<C>
where
    C: ControlPlaneGenerationsApi,
{
    pub(super) fn new(
        conf: &'static PageServerConf,
        rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,
        tx: tokio::sync::mpsc::Sender<ExecutorMessage>,
        control_plane_client: Option<C>,
        lsn_table: Arc<std::sync::RwLock<VisibleLsnUpdates>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            conf,
            rx,
            tx,
            control_plane_client,
            lsn_table,
            pending_lists: Vec::new(),
            validated_lists: Vec::new(),
            pending_key_count: 0,
            cancel,
        }
    }

    async fn cleanup_lists(&mut self, list_paths: Vec<PathBuf>) {
        for list_path in list_paths {
            debug!("Removing deletion list {}", list_path.display());

            if let Err(e) = tokio::fs::remove_file(&list_path).await {
                // Unexpected: we should have permissions and nothing else should
                // be touching these files.  We will leave the file behind.  Subsequent
                // pageservers will try and load it again: hopefully whatever storage
                // issue (probably permissions) has been fixed by then.
                tracing::error!("Failed to delete {}: {e:#}", list_path.display());
                break;
            }
        }
    }

    /// Process any outstanding validations of generations of pending LSN updates or pending
    /// DeletionLists.
    ///
    /// Valid LSN updates propagate back to their result channel immediately, valid DeletionLists
    /// go into the queue of ready-to-execute lists.
    async fn validate(&mut self) -> Result<(), DeletionQueueError> {
        let mut tenant_generations = HashMap::new();
        for list in &self.pending_lists {
            for (tenant_id, tenant_list) in &list.tenants {
                // Note: DeletionLists are in logical time order, so generation always
                // goes up.  By doing a simple insert() we will always end up with
                // the latest generation seen for a tenant.
                tenant_generations.insert(*tenant_id, tenant_list.generation);
            }
        }

        let pending_lsn_updates = {
            let mut lsn_table = self.lsn_table.write().expect("Lock should not be poisoned");
            let mut pending_updates = VisibleLsnUpdates::new();
            std::mem::swap(&mut pending_updates, &mut lsn_table);
            pending_updates
        };
        for (tenant_id, update) in &pending_lsn_updates.tenants {
            let entry = tenant_generations
                .entry(*tenant_id)
                .or_insert(update.generation);
            if update.generation > *entry {
                *entry = update.generation;
            }
        }

        if tenant_generations.is_empty() {
            // No work to do
            return Ok(());
        }

        let tenants_valid = if let Some(control_plane_client) = &self.control_plane_client {
            control_plane_client
                .validate(tenant_generations.iter().map(|(k, v)| (*k, *v)).collect())
                .await
                // The only wait a validation call returns an error is when the cancellation token fires
                .map_err(|_| DeletionQueueError::ShuttingDown)?
        } else {
            // Control plane API disabled.  In legacy mode we consider everything valid.
            tenant_generations.keys().map(|k| (*k, true)).collect()
        };

        let mut validated_sequence: Option<u64> = None;

        // Apply the validation results to the pending LSN updates
        for (tenant_id, tenant_lsn_state) in pending_lsn_updates.tenants {
            let validated_generation = tenant_generations
                .get(&tenant_id)
                .expect("Map was built from the same keys we're reading");

            // If the tenant was missing from the validation response, it has been deleted.  We may treat
            // deletions as valid as the tenant's remote storage is all to be wiped anyway.
            let valid = tenants_valid.get(&tenant_id).copied().unwrap_or(true);

            if valid && *validated_generation == tenant_lsn_state.generation {
                for (_timeline_id, pending_lsn) in tenant_lsn_state.timelines {
                    // Drop result of send: it is legal for the Timeline to have been dropped along
                    // with its queue receiver while we were doing validation.
                    pending_lsn.result_slot.store(pending_lsn.projected);
                }
            } else {
                // If we failed validation, then do not apply any of the projected updates
                warn!("Dropped remote consistent LSN updates for tenant {tenant_id} in stale generation {0:?}", tenant_lsn_state.generation);
            }
        }

        // Apply the validation results to the pending deletion lists
        for list in &mut self.pending_lists {
            // Filter the list based on whether the server responded valid: true.
            // If a tenant is omitted in the response, it has been deleted, and we should
            // proceed with deletion.
            let mut mutated = false;
            list.tenants.retain(|tenant_id, tenant| {
                let validated_generation = tenant_generations
                    .get(tenant_id)
                    .expect("Map was built from the same keys we're reading");

                // If the tenant was missing from the validation response, it has been deleted.  We may treat
                // deletions as valid as the tenant's remote storage is all to be wiped anyway.
                let valid = tenants_valid.get(tenant_id).copied().unwrap_or(true);

                // A list is valid if it comes from the current _or previous_ generation.
                // The previous generation case is due to how we store deletion lists locally:
                // if we see the immediately previous generation in a locally stored deletion list,
                // it proves that this node's disk was used for both current & previous generations,
                // and therefore no other node was involved in between: the two generations may be
                // logically treated as the same.
                let this_list_valid = valid
                    && (tenant.generation == *validated_generation);

                if !this_list_valid {
                    warn!("Dropping stale deletions for tenant {tenant_id} in generation {:?}, objects may be leaked", tenant.generation);
                    DELETION_QUEUE_DROPPED.inc_by(tenant.len() as u64);
                    mutated = true;
                }
                this_list_valid
            });
            list.validated = true;

            if mutated {
                // Save the deletion list if we had to make changes due to stale generations.  The
                // saved list is valid for execution.
                if let Err(e) = list.save(self.conf).await {
                    // Highly unexpected.  Could happen if e.g. disk full.
                    // If we didn't save the trimmed list, it is _not_ valid to execute.
                    warn!("Failed to save modified deletion list {list}: {e:#}");

                    // Rather than have a complex retry process, just drop it and leak the objects,
                    // scrubber will clean up eventually.
                    list.tenants.clear(); // Result is a valid-but-empty list, which is a no-op for execution.
                }
            }

            validated_sequence = Some(list.sequence);
        }

        if let Some(validated_sequence) = validated_sequence {
            // Write the queue header to record how far validation progressed.  This avoids having
            // to rewrite each DeletionList to set validated=true in it.
            let header = DeletionHeader::new(validated_sequence);

            // Drop result because the validated_sequence is an optimization.  If we fail to save it,
            // then restart, we will drop some deletion lists, creating work for scrubber.
            // The save() function logs a warning on error.
            if let Err(e) = header.save(self.conf).await {
                warn!("Failed to write deletion queue header: {e:#}");
                DELETION_QUEUE_ERRORS
                    .with_label_values(&["put_header"])
                    .inc();
            }
        }

        // Transfer the validated lists to the validated queue, for eventual execution
        self.validated_lists.append(&mut self.pending_lists);

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), DeletionQueueError> {
        tracing::debug!("Flushing with {} pending lists", self.pending_lists.len());

        // Issue any required generation validation calls to the control plane
        self.validate().await?;

        // After successful validation, nothing is pending: any lists that
        // made it through validation will be in validated_lists.
        assert!(self.pending_lists.is_empty());
        self.pending_key_count = 0;

        tracing::debug!(
            "Validation complete, have {} validated lists",
            self.validated_lists.len()
        );

        // Return quickly if we have no validated lists to execute.  This avoids flushing the
        // executor when an idle backend hits its autoflush interval
        if self.validated_lists.is_empty() {
            return Ok(());
        }

        // Drain `validated_lists` into the executor
        let mut executing_lists = Vec::new();
        for list in self.validated_lists.drain(..) {
            let list_path = self.conf.deletion_list_path(list.sequence);
            let objects = list.into_remote_paths();
            self.tx
                .send(ExecutorMessage::Delete(objects))
                .await
                .map_err(|_| DeletionQueueError::ShuttingDown)?;
            executing_lists.push(list_path);
        }

        self.flush_executor().await?;

        // Erase the deletion lists whose keys have all be deleted from remote storage
        self.cleanup_lists(executing_lists).await;

        Ok(())
    }

    async fn flush_executor(&mut self) -> Result<(), DeletionQueueError> {
        // Flush the executor, so that all the keys referenced by these deletion lists
        // are actually removed from remote storage.  This is a precondition to deleting
        // the deletion lists themselves.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let flush_op = FlushOp { tx };
        self.tx
            .send(ExecutorMessage::Flush(flush_op))
            .await
            .map_err(|_| DeletionQueueError::ShuttingDown)?;

        rx.await.map_err(|_| DeletionQueueError::ShuttingDown)
    }

    pub(super) async fn background(&mut self) {
        tracing::info!("Started deletion backend worker");

        while !self.cancel.is_cancelled() {
            let msg = match tokio::time::timeout(AUTOFLUSH_INTERVAL, self.rx.recv()).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    // All queue senders closed
                    info!("Shutting down");
                    break;
                }
                Err(_) => {
                    // Timeout, we hit deadline to execute whatever we have in hand.  These functions will
                    // return immediately if no work is pending.
                    // Drop result, because it' a background flush and we don't care whether it really worked.
                    match self.flush().await {
                        Ok(()) => {}
                        Err(DeletionQueueError::ShuttingDown) => {
                            // If we are shutting down, then auto-flush can safely be skipped
                        }
                    }

                    continue;
                }
            };

            match msg {
                BackendQueueMessage::Delete(list) => {
                    if list.validated {
                        // A pre-validated list may only be seen during recovery, if we are recovering
                        // a DeletionList whose on-disk state has validated=true
                        self.validated_lists.push(list)
                    } else {
                        self.pending_key_count += list.len();
                        self.pending_lists.push(list);
                    }

                    if self.pending_key_count > AUTOFLUSH_KEY_COUNT {
                        match self.flush().await {
                            Ok(()) => {}
                            Err(DeletionQueueError::ShuttingDown) => {
                                // If we are shutting down, then auto-flush can safely be skipped
                            }
                        }
                    }
                }
                BackendQueueMessage::Flush(op) => {
                    match self.flush().await {
                        Ok(()) => {
                            op.notify();
                        }
                        Err(DeletionQueueError::ShuttingDown) => {
                            // If we fail due to shutting down, we will just drop `op` to propagate that status.
                        }
                    }
                }
            }
        }
    }
}
