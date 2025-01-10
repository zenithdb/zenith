//! Safekeeper timeline has a background task which is subscribed to `commit_lsn`
//! and `flush_lsn` updates.
//!
//! After the partial segment was updated (`flush_lsn` was changed), the segment
//! will be uploaded to S3 within the configured `partial_backup_timeout`.
//!
//! The filename format for partial segments is
//! `Segment_Term_Flush_Commit_skNN.partial`, where:
//! - `Segment` – the segment name, like `000000010000000000000001`
//! - `Term` – current term
//! - `Flush` – flush_lsn in hex format `{:016X}`, e.g. `00000000346BC568`
//! - `Commit` – commit_lsn in the same hex format
//! - `NN` – safekeeper_id, like `1`
//!
//! The full object name example:
//! `000000010000000000000002_2_0000000002534868_0000000002534410_sk1.partial`
//!
//! Each safekeeper will keep info about remote partial segments in its control
//! file. Code updates state in the control file before doing any S3 operations.
//! This way control file stores information about all potentially existing
//! remote partial segments and can clean them up after uploading a newer version.
use camino::Utf8PathBuf;
use postgres_ffi::{XLogFileName, XLogSegNo, PG_TLI};
use remote_storage::RemotePath;
use safekeeper_api::Term;
use serde::{Deserialize, Serialize};

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use utils::{id::NodeId, lsn::Lsn};

use crate::{
    metrics::{MISC_OPERATION_SECONDS, PARTIAL_BACKUP_UPLOADED_BYTES, PARTIAL_BACKUP_UPLOADS},
    rate_limit::{rand_duration, RateLimiter},
    timeline::WalResidentTimeline,
    timeline_manager::StateSnapshot,
    wal_backup::{self},
    SafeKeeperConf,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UploadStatus {
    /// Upload is in progress. This status should be used only for garbage collection,
    /// don't read data from the remote storage with this status.
    InProgress,
    /// Upload is finished. There is always at most one segment with this status.
    /// It means that the segment is actual and can be used.
    Uploaded,
    /// Deletion is in progress. This status should be used only for garbage collection,
    /// don't read data from the remote storage with this status.
    Deleting,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartialRemoteSegment {
    pub status: UploadStatus,
    pub name: String,
    pub commit_lsn: Lsn,
    pub flush_lsn: Lsn,
    // We should use last_log_term here, otherwise it's possible to have inconsistent data in the
    // remote storage.
    //
    // More info here: https://github.com/neondatabase/neon/pull/8022#discussion_r1654738405
    pub term: Term,
}

impl PartialRemoteSegment {
    fn eq_without_status(&self, other: &Self) -> bool {
        self.name == other.name
            && self.commit_lsn == other.commit_lsn
            && self.flush_lsn == other.flush_lsn
            && self.term == other.term
    }

    pub(crate) fn remote_path(&self, remote_timeline_path: &RemotePath) -> RemotePath {
        remote_timeline_path.join(&self.name)
    }
}

// NB: these structures are a part of a control_file, you can't change them without
// changing the control file format version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct State {
    pub segments: Vec<PartialRemoteSegment>,
}

#[derive(Debug)]
pub(crate) struct ReplaceUploadedSegment {
    pub(crate) previous: PartialRemoteSegment,
    pub(crate) current: PartialRemoteSegment,
}

impl State {
    /// Find an Uploaded segment. There should be only one Uploaded segment at a time.
    pub(crate) fn uploaded_segment(&self) -> Option<PartialRemoteSegment> {
        self.segments
            .iter()
            .find(|seg| seg.status == UploadStatus::Uploaded)
            .cloned()
    }

    /// Replace the name of the Uploaded segment (if one exists) in order to match
    /// it with `destination` safekeeper. Returns a description of the change or None
    /// wrapped in anyhow::Result.
    pub(crate) fn replace_uploaded_segment(
        &mut self,
        source: NodeId,
        destination: NodeId,
    ) -> anyhow::Result<Option<ReplaceUploadedSegment>> {
        let current = self
            .segments
            .iter_mut()
            .find(|seg| seg.status == UploadStatus::Uploaded);

        let current = match current {
            Some(some) => some,
            None => {
                return anyhow::Ok(None);
            }
        };

        // Sanity check that the partial segment we are replacing is belongs
        // to the `source` SK.
        if !current
            .name
            .ends_with(format!("sk{}.partial", source.0).as_str())
        {
            anyhow::bail!(
                "Partial segment name ({}) doesn't match self node id ({})",
                current.name,
                source
            );
        }

        let previous = current.clone();

        let new_name = current.name.replace(
            format!("_sk{}", source.0).as_str(),
            format!("_sk{}", destination.0).as_str(),
        );

        current.name = new_name;

        anyhow::Ok(Some(ReplaceUploadedSegment {
            previous,
            current: current.clone(),
        }))
    }
}

pub struct PartialBackup {
    wal_seg_size: usize,
    tli: WalResidentTimeline,
    conf: SafeKeeperConf,
    local_prefix: Utf8PathBuf,
    remote_timeline_path: RemotePath,

    state: State,
}

impl PartialBackup {
    pub async fn new(tli: WalResidentTimeline, conf: SafeKeeperConf) -> PartialBackup {
        let (_, persistent_state) = tli.get_state().await;
        let wal_seg_size = tli.get_wal_seg_size().await;

        let local_prefix = tli.get_timeline_dir();
        let remote_timeline_path = tli.remote_path.clone();

        PartialBackup {
            wal_seg_size,
            tli,
            state: persistent_state.partial_backup,
            conf,
            local_prefix,
            remote_timeline_path,
        }
    }

    // Read-only methods for getting segment names
    fn segno(&self, lsn: Lsn) -> XLogSegNo {
        lsn.segment_number(self.wal_seg_size)
    }

    fn segment_name(&self, segno: u64) -> String {
        XLogFileName(PG_TLI, segno, self.wal_seg_size)
    }

    fn remote_segment_name(
        &self,
        segno: u64,
        term: u64,
        commit_lsn: Lsn,
        flush_lsn: Lsn,
    ) -> String {
        format!(
            "{}_{}_{:016X}_{:016X}_sk{}.partial",
            self.segment_name(segno),
            term,
            flush_lsn.0,
            commit_lsn.0,
            self.conf.my_id.0,
        )
    }

    fn local_segment_name(&self, segno: u64) -> String {
        format!("{}.partial", self.segment_name(segno))
    }
}

impl PartialBackup {
    /// Takes a lock to read actual safekeeper state and returns a segment that should be uploaded.
    async fn prepare_upload(&self) -> PartialRemoteSegment {
        // this operation takes a lock to get the actual state
        let sk_info = self.tli.get_safekeeper_info(&self.conf).await;
        let flush_lsn = Lsn(sk_info.flush_lsn);
        let commit_lsn = Lsn(sk_info.commit_lsn);
        let last_log_term = sk_info.last_log_term;
        let segno = self.segno(flush_lsn);

        let name = self.remote_segment_name(segno, last_log_term, commit_lsn, flush_lsn);

        PartialRemoteSegment {
            status: UploadStatus::InProgress,
            name,
            commit_lsn,
            flush_lsn,
            term: last_log_term,
        }
    }

    /// Reads segment from disk and uploads it to the remote storage.
    async fn upload_segment(&mut self, prepared: PartialRemoteSegment) -> anyhow::Result<()> {
        let flush_lsn = prepared.flush_lsn;
        let segno = self.segno(flush_lsn);

        // We're going to backup bytes from the start of the segment up to flush_lsn.
        let backup_bytes = flush_lsn.segment_offset(self.wal_seg_size);

        let local_path = self.local_prefix.join(self.local_segment_name(segno));
        let remote_path = prepared.remote_path(&self.remote_timeline_path);

        // Upload first `backup_bytes` bytes of the segment to the remote storage.
        wal_backup::backup_partial_segment(&local_path, &remote_path, backup_bytes).await?;
        PARTIAL_BACKUP_UPLOADED_BYTES.inc_by(backup_bytes as u64);

        // We uploaded the segment, now let's verify that the data is still actual.
        // If the term changed, we cannot guarantee the validity of the uploaded data.
        // If the term is the same, we know the data is not corrupted.
        let sk_info = self.tli.get_safekeeper_info(&self.conf).await;
        if sk_info.last_log_term != prepared.term {
            anyhow::bail!("term changed during upload");
        }
        assert!(prepared.commit_lsn <= Lsn(sk_info.commit_lsn));
        assert!(prepared.flush_lsn <= Lsn(sk_info.flush_lsn));

        Ok(())
    }

    /// Write new state to disk. If in-memory and on-disk states diverged, returns an error.
    async fn commit_state(&mut self, new_state: State) -> anyhow::Result<()> {
        self.tli
            .map_control_file(|cf| {
                if cf.partial_backup != self.state {
                    let memory = self.state.clone();
                    self.state = cf.partial_backup.clone();
                    anyhow::bail!(
                        "partial backup state diverged, memory={:?}, disk={:?}",
                        memory,
                        cf.partial_backup
                    );
                }

                cf.partial_backup = new_state.clone();
                Ok(())
            })
            .await?;
        // update in-memory state
        self.state = new_state;
        Ok(())
    }

    /// Upload the latest version of the partial segment and garbage collect older versions.
    #[instrument(name = "upload", skip_all, fields(name = %prepared.name))]
    async fn do_upload(&mut self, prepared: &PartialRemoteSegment) -> anyhow::Result<()> {
        let _timer = MISC_OPERATION_SECONDS
            .with_label_values(&["partial_do_upload"])
            .start_timer();
        info!("starting upload {:?}", prepared);

        let state_0 = self.state.clone();
        let state_1 = {
            let mut state = state_0.clone();
            state.segments.push(prepared.clone());
            state
        };

        // we're going to upload a new segment, let's write it to disk to make GC later
        self.commit_state(state_1).await?;

        self.upload_segment(prepared.clone()).await?;

        let state_2 = {
            let mut state = state_0.clone();
            for seg in state.segments.iter_mut() {
                seg.status = UploadStatus::Deleting;
            }
            let mut actual_remote_segment = prepared.clone();
            actual_remote_segment.status = UploadStatus::Uploaded;
            state.segments.push(actual_remote_segment);
            state
        };

        // we've uploaded new segment, it's actual, all other segments should be GCed
        self.commit_state(state_2).await?;
        self.gc().await?;

        Ok(())
    }

    // Prepend to the given segments remote prefix and delete them from the
    // remote storage.
    async fn delete_segments(&self, segments_to_delete: &Vec<String>) -> anyhow::Result<()> {
        info!("deleting objects: {:?}", segments_to_delete);
        let mut objects_to_delete = vec![];
        for seg in segments_to_delete.iter() {
            let remote_path = self.remote_timeline_path.join(seg);
            objects_to_delete.push(remote_path);
        }
        wal_backup::delete_objects(&objects_to_delete).await
    }

    /// Delete all non-Uploaded segments from the remote storage. There should be only one
    /// Uploaded segment at a time.
    #[instrument(name = "gc", skip_all)]
    async fn gc(&mut self) -> anyhow::Result<()> {
        let mut segments_to_delete = vec![];

        let new_segments: Vec<PartialRemoteSegment> = self
            .state
            .segments
            .iter()
            .filter_map(|seg| {
                if seg.status == UploadStatus::Uploaded {
                    Some(seg.clone())
                } else {
                    segments_to_delete.push(seg.name.clone());
                    None
                }
            })
            .collect();

        if new_segments.len() == 1 {
            // we have an uploaded segment, it must not be deleted from remote storage
            segments_to_delete.retain(|name| name != &new_segments[0].name);
        } else {
            // there should always be zero or one uploaded segment
            assert!(
                new_segments.is_empty(),
                "too many uploaded segments: {:?}",
                new_segments
            );
        }

        // execute the deletion
        self.delete_segments(&segments_to_delete).await?;

        // now we can update the state on disk
        let new_state = {
            let mut state = self.state.clone();
            state.segments = new_segments;
            state
        };
        self.commit_state(new_state).await?;

        Ok(())
    }

    /// Remove uploaded segment(s) from the state and remote storage. Aimed for
    /// manual intervention, not normally needed.
    /// Returns list of segments which potentially existed in the remote storage.
    pub async fn reset(&mut self) -> anyhow::Result<Vec<String>> {
        let segments_to_delete = self
            .state
            .segments
            .iter()
            .map(|seg| seg.name.clone())
            .collect();

        // First reset cfile state, and only then objects themselves. If the
        // later fails we might leave some garbage behind; that's ok for this
        // single time usage.
        let new_state = State { segments: vec![] };
        self.commit_state(new_state).await?;

        self.delete_segments(&segments_to_delete).await?;
        Ok(segments_to_delete)
    }
}

/// Check if everything is uploaded and partial backup task doesn't need to run.
pub(crate) fn needs_uploading(
    state: &StateSnapshot,
    uploaded: &Option<PartialRemoteSegment>,
) -> bool {
    match uploaded {
        Some(uploaded) => {
            uploaded.status != UploadStatus::Uploaded
                || uploaded.flush_lsn != state.flush_lsn
                || uploaded.commit_lsn != state.commit_lsn
                || uploaded.term != state.last_log_term
        }
        None => true,
    }
}

/// Main task for partial backup. It waits for the flush_lsn to change and then uploads the
/// partial segment to the remote storage. It also does garbage collection of old segments.
///
/// When there is nothing more to do and the last segment was successfully uploaded, the task
/// returns PartialRemoteSegment, to signal readiness for offloading the timeline.
#[instrument(name = "partial_backup", skip_all, fields(ttid = %tli.ttid))]
pub async fn main_task(
    tli: WalResidentTimeline,
    conf: SafeKeeperConf,
    limiter: RateLimiter,
    cancel: CancellationToken,
) -> Option<PartialRemoteSegment> {
    debug!("started");
    let await_duration = conf.partial_backup_timeout;
    let mut first_iteration = true;

    let mut commit_lsn_rx = tli.get_commit_lsn_watch_rx();
    let mut flush_lsn_rx = tli.get_term_flush_lsn_watch_rx();

    let mut backup = PartialBackup::new(tli, conf).await;

    debug!("state: {:?}", backup.state);

    // The general idea is that each safekeeper keeps only one partial segment
    // both in remote storage and in local state. If this is not true, something
    // went wrong.
    const MAX_SIMULTANEOUS_SEGMENTS: usize = 10;

    'outer: loop {
        if backup.state.segments.len() > MAX_SIMULTANEOUS_SEGMENTS {
            warn!(
                "too many segments in control_file state, running gc: {}",
                backup.state.segments.len()
            );

            backup.gc().await.unwrap_or_else(|e| {
                error!("failed to run gc: {:#}", e);
            });
        }

        // wait until we have something to upload
        let uploaded_segment = backup.state.uploaded_segment();
        if let Some(seg) = &uploaded_segment {
            // check if uploaded segment matches the current state
            if flush_lsn_rx.borrow().lsn == seg.flush_lsn
                && *commit_lsn_rx.borrow() == seg.commit_lsn
                && flush_lsn_rx.borrow().term == seg.term
            {
                // we have nothing to do, the last segment is already uploaded
                debug!(
                    "exiting, uploaded up to term={} flush_lsn={} commit_lsn={}",
                    seg.term, seg.flush_lsn, seg.commit_lsn
                );
                return Some(seg.clone());
            }
        }

        // if we don't have any data and zero LSNs, wait for something
        while flush_lsn_rx.borrow().lsn == Lsn(0) {
            tokio::select! {
                _ = backup.tli.cancel.cancelled() => {
                    info!("timeline canceled");
                    return None;
                }
                _ = cancel.cancelled() => {
                    info!("task canceled");
                    return None;
                }
                _ = flush_lsn_rx.changed() => {}
            }
        }

        // smoothing the load after restart, by sleeping for a random time.
        // if this is not the first iteration, we will wait for the full await_duration
        let await_duration = if first_iteration {
            first_iteration = false;
            rand_duration(&await_duration)
        } else {
            await_duration
        };

        // fixing the segno and waiting some time to prevent reuploading the same segment too often
        let pending_segno = backup.segno(flush_lsn_rx.borrow().lsn);
        let timeout = tokio::time::sleep(await_duration);
        tokio::pin!(timeout);
        let mut timeout_expired = false;

        // waiting until timeout expires OR segno changes
        'inner: loop {
            tokio::select! {
                _ = backup.tli.cancel.cancelled() => {
                    info!("timeline canceled");
                    return None;
                }
                _ = cancel.cancelled() => {
                    info!("task canceled");
                    return None;
                }
                _ = commit_lsn_rx.changed() => {}
                _ = flush_lsn_rx.changed() => {
                    let segno = backup.segno(flush_lsn_rx.borrow().lsn);
                    if segno != pending_segno {
                        // previous segment is no longer partial, aborting the wait
                        break 'inner;
                    }
                }
                _ = &mut timeout => {
                    // timeout expired, now we are ready for upload
                    timeout_expired = true;
                    break 'inner;
                }
            }
        }

        if !timeout_expired {
            // likely segno has changed, let's try again in the next iteration
            continue 'outer;
        }

        // limit concurrent uploads
        let _upload_permit = tokio::select! {
            acq = limiter.acquire_partial_backup() => acq,
            _ = cancel.cancelled() => {
                info!("task canceled");
                return None;
            }
        };

        let prepared = backup.prepare_upload().await;
        if let Some(seg) = &uploaded_segment {
            if seg.eq_without_status(&prepared) {
                // we already uploaded this segment, nothing to do
                continue 'outer;
            }
        }

        match backup.do_upload(&prepared).await {
            Ok(()) => {
                debug!(
                    "uploaded {} up to flush_lsn {}",
                    prepared.name, prepared.flush_lsn
                );
                PARTIAL_BACKUP_UPLOADS.with_label_values(&["ok"]).inc();
            }
            Err(e) => {
                info!("failed to upload {}: {:#}", prepared.name, e);
                PARTIAL_BACKUP_UPLOADS.with_label_values(&["error"]).inc();
            }
        }
    }
}
