use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt, TryStreamExt};
use postgres_ffi::{XLogFileName, XLogSegNo, PG_TLI};
use safekeeper_api::{models::TimelineStatus, Term};
use safekeeper_client::mgmt_api;
use safekeeper_client::mgmt_api::Client;
use serde::{Deserialize, Serialize};
use std::{
    cmp::min,
    io::{self, ErrorKind},
    sync::Arc,
};
use tokio::{fs::OpenOptions, io::AsyncWrite, sync::mpsc, task};
use tokio_tar::{Archive, Builder, Header};
use tokio_util::{
    io::{CopyToBytes, SinkWriter},
    sync::PollSender,
};
use tracing::{error, info, instrument};

use crate::{
    control_file::CONTROL_FILE_NAME,
    debug_dump,
    state::{EvictionState, TimelinePersistentState},
    timeline::{Timeline, WalResidentTimeline},
    timelines_global_map::{create_temp_timeline_dir, validate_temp_timeline},
    wal_backup,
    wal_storage::open_wal_file,
    GlobalTimelines,
};
use utils::{
    crashsafe::fsync_async_opt,
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    logging::SecretString,
    lsn::Lsn,
    pausable_failpoint,
};

/// Stream tar archive of timeline to tx.
#[instrument(name = "snapshot", skip_all, fields(ttid = %tli.ttid))]
pub async fn stream_snapshot(
    tli: Arc<Timeline>,
    source: NodeId,
    destination: NodeId,
    tx: mpsc::Sender<Result<Bytes>>,
) {
    match tli.try_wal_residence_guard().await {
        Err(e) => {
            tx.send(Err(anyhow!("Error checking residence: {:#}", e)))
                .await
                .ok();
        }
        Ok(maybe_resident_tli) => {
            if let Err(e) = match maybe_resident_tli {
                Some(resident_tli) => {
                    stream_snapshot_resident_guts(resident_tli, source, destination, tx.clone())
                        .await
                }
                None => stream_snapshot_offloaded_guts(tli, source, destination, tx.clone()).await,
            } {
                // Error type/contents don't matter as they won't can't reach the client
                // (hyper likely doesn't do anything with it), but http stream will be
                // prematurely terminated. It would be nice to try to send the error in
                // trailers though.
                tx.send(Err(anyhow!("snapshot failed"))).await.ok();
                error!("snapshot failed: {:#}", e);
            }
        }
    }
}

/// State needed while streaming the snapshot.
pub struct SnapshotContext {
    pub from_segno: XLogSegNo, // including
    pub upto_segno: XLogSegNo, // including
    pub term: Term,
    pub last_log_term: Term,
    pub flush_lsn: Lsn,
    pub wal_seg_size: usize,
    // used to remove WAL hold off in Drop.
    pub tli: WalResidentTimeline,
}

impl Drop for SnapshotContext {
    fn drop(&mut self) {
        let tli = self.tli.clone();
        task::spawn(async move {
            let mut shared_state = tli.write_shared_state().await;
            shared_state.wal_removal_on_hold = false;
        });
    }
}

/// Build a tokio_tar stream that sends encoded bytes into a Bytes channel.
fn prepare_tar_stream(
    tx: mpsc::Sender<Result<Bytes>>,
) -> tokio_tar::Builder<impl AsyncWrite + Unpin + Send> {
    // tokio-tar wants Write implementor, but we have mpsc tx <Result<Bytes>>;
    // use SinkWriter as a Write impl. That is,
    // - create Sink from the tx. It returns PollSendError if chan is closed.
    let sink = PollSender::new(tx);
    // - SinkWriter needs sink error to be io one, map it.
    let sink_io_err = sink.sink_map_err(|_| io::Error::from(ErrorKind::BrokenPipe));
    // - SinkWriter wants sink type to be just Bytes, not Result<Bytes>, so map
    //   it with with(). Note that with() accepts async function which we don't
    //   need and allows the map to fail, which we don't need either, but hence
    //   two Oks.
    let oksink = sink_io_err.with(|b: Bytes| async { io::Result::Ok(Result::Ok(b)) });
    // - SinkWriter (not surprisingly) wants sink of &[u8], not bytes, so wrap
    // into CopyToBytes. This is a data copy.
    let copy_to_bytes = CopyToBytes::new(oksink);
    let writer = SinkWriter::new(copy_to_bytes);
    let pinned_writer = Box::pin(writer);

    // Note that tokio_tar append_* funcs use tokio::io::copy with 8KB buffer
    // which is also likely suboptimal.
    Builder::new_non_terminated(pinned_writer)
}

/// Implementation of snapshot for an offloaded timeline, only reads control file
pub(crate) async fn stream_snapshot_offloaded_guts(
    tli: Arc<Timeline>,
    source: NodeId,
    destination: NodeId,
    tx: mpsc::Sender<Result<Bytes>>,
) -> Result<()> {
    let mut ar = prepare_tar_stream(tx);

    tli.snapshot_offloaded(&mut ar, source, destination).await?;

    ar.finish().await?;

    Ok(())
}

/// Implementation of snapshot for a timeline which is resident (includes some segment data)
pub async fn stream_snapshot_resident_guts(
    tli: WalResidentTimeline,
    source: NodeId,
    destination: NodeId,
    tx: mpsc::Sender<Result<Bytes>>,
) -> Result<()> {
    let mut ar = prepare_tar_stream(tx);

    let bctx = tli.start_snapshot(&mut ar, source, destination).await?;
    pausable_failpoint!("sk-snapshot-after-list-pausable");

    let tli_dir = tli.get_timeline_dir();
    info!(
        "sending {} segments [{:#X}-{:#X}], term={}, last_log_term={}, flush_lsn={}",
        bctx.upto_segno - bctx.from_segno + 1,
        bctx.from_segno,
        bctx.upto_segno,
        bctx.term,
        bctx.last_log_term,
        bctx.flush_lsn,
    );
    for segno in bctx.from_segno..=bctx.upto_segno {
        let (mut sf, is_partial) = open_wal_file(&tli_dir, segno, bctx.wal_seg_size).await?;
        let mut wal_file_name = XLogFileName(PG_TLI, segno, bctx.wal_seg_size);
        if is_partial {
            wal_file_name.push_str(".partial");
        }
        ar.append_file(&wal_file_name, &mut sf).await?;
    }

    // Do the term check before ar.finish to make archive corrupted in case of
    // term change. Client shouldn't ignore abrupt stream end, but to be sure.
    tli.finish_snapshot(&bctx).await?;

    ar.finish().await?;

    Ok(())
}

impl Timeline {
    /// Simple snapshot for an offloaded timeline: we will only upload a renamed partial segment and
    /// pass a modified control file into the provided tar stream (nothing with data segments on disk, since
    /// we are offloaded and there aren't any)
    async fn snapshot_offloaded<W: AsyncWrite + Unpin + Send>(
        self: &Arc<Timeline>,
        ar: &mut tokio_tar::Builder<W>,
        source: NodeId,
        destination: NodeId,
    ) -> Result<()> {
        // Take initial copy of control file, then release state lock
        let mut control_file = {
            let shared_state = self.write_shared_state().await;

            let control_file = TimelinePersistentState::clone(shared_state.sk.state());

            // Rare race: we got unevicted between entering function and reading control file.
            // We error out and let API caller retry.
            if !matches!(control_file.eviction_state, EvictionState::Offloaded(_)) {
                bail!("Timeline was un-evicted during snapshot, please retry");
            }

            control_file
        };

        // Modify the partial segment of the in-memory copy for the control file to
        // point to the destination safekeeper.
        let replace = control_file
            .partial_backup
            .replace_uploaded_segment(source, destination)?;

        let Some(replace) = replace else {
            // In Manager:: ready_for_eviction, we do not permit eviction unless the timeline
            // has a partial segment.  It is unexpected that
            anyhow::bail!("Timeline has no partial segment, cannot generate snapshot");
        };

        tracing::info!("Replacing uploaded partial segment in in-mem control file: {replace:?}");

        // Optimistically try to copy the partial segment to the destination's path: this
        // can fail if the timeline was un-evicted and modified in the background.
        let remote_timeline_path = &self.remote_path;
        wal_backup::copy_partial_segment(
            &replace.previous.remote_path(remote_timeline_path),
            &replace.current.remote_path(remote_timeline_path),
        )
        .await?;

        // Since the S3 copy succeeded with the path given in our control file snapshot, and
        // we are sending that snapshot in our response, we are giving the caller a consistent
        // snapshot even if our local Timeline was unevicted or otherwise modified in the meantime.
        let buf = control_file
            .write_to_buf()
            .with_context(|| "failed to serialize control store")?;
        let mut header = Header::new_gnu();
        header.set_size(buf.len().try_into().expect("never breaches u64"));
        ar.append_data(&mut header, CONTROL_FILE_NAME, buf.as_slice())
            .await
            .with_context(|| "failed to append to archive")?;

        Ok(())
    }
}

impl WalResidentTimeline {
    /// Start streaming tar archive with timeline:
    /// 1) stream control file under lock;
    /// 2) hold off WAL removal;
    /// 3) collect SnapshotContext to understand which WAL segments should be
    ///    streamed.
    ///
    /// Snapshot streams data up to flush_lsn. To make this safe, we must check
    /// that term doesn't change during the procedure, or we risk sending mix of
    /// WAL from different histories. Term is remembered in the SnapshotContext
    /// and checked in finish_snapshot. Note that in the last segment some WAL
    /// higher than flush_lsn set here might be streamed; that's fine as long as
    /// terms doesn't change.
    ///
    /// Alternatively we could send only up to commit_lsn to get some valid
    /// state which later will be recovered by compute, in this case term check
    /// is not needed, but we likely don't want that as there might be no
    /// compute which could perform the recovery.
    ///
    /// When returned SnapshotContext is dropped WAL hold is removed.
    async fn start_snapshot<W: AsyncWrite + Unpin + Send>(
        &self,
        ar: &mut tokio_tar::Builder<W>,
        source: NodeId,
        destination: NodeId,
    ) -> Result<SnapshotContext> {
        let mut shared_state = self.write_shared_state().await;
        let wal_seg_size = shared_state.get_wal_seg_size();

        let mut control_store = TimelinePersistentState::clone(shared_state.sk.state());
        // Modify the partial segment of the in-memory copy for the control file to
        // point to the destination safekeeper.
        let replace = control_store
            .partial_backup
            .replace_uploaded_segment(source, destination)?;

        if let Some(replace) = replace {
            // The deserialized control file has an uploaded partial. We upload a copy
            // of it to object storage for the destination safekeeper and send an updated
            // control file in the snapshot.
            tracing::info!(
                "Replacing uploaded partial segment in in-mem control file: {replace:?}"
            );

            let remote_timeline_path = &self.tli.remote_path;
            wal_backup::copy_partial_segment(
                &replace.previous.remote_path(remote_timeline_path),
                &replace.current.remote_path(remote_timeline_path),
            )
            .await?;
        }

        let buf = control_store
            .write_to_buf()
            .with_context(|| "failed to serialize control store")?;
        let mut header = Header::new_gnu();
        header.set_size(buf.len().try_into().expect("never breaches u64"));
        ar.append_data(&mut header, CONTROL_FILE_NAME, buf.as_slice())
            .await
            .with_context(|| "failed to append to archive")?;

        // We need to stream since the oldest segment someone (s3 or pageserver)
        // still needs. This duplicates calc_horizon_lsn logic.
        //
        // We know that WAL wasn't removed up to this point because it cannot be
        // removed further than `backup_lsn`. Since we're holding shared_state
        // lock and setting `wal_removal_on_hold` later, it guarantees that WAL
        // won't be removed until we're done.
        let from_lsn = min(
            shared_state.sk.state().remote_consistent_lsn,
            shared_state.sk.state().backup_lsn,
        );
        if from_lsn == Lsn::INVALID {
            // this is possible if snapshot is called before handling first
            // elected message
            bail!("snapshot is called on uninitialized timeline");
        }
        let from_segno = from_lsn.segment_number(wal_seg_size);
        let term = shared_state.sk.state().acceptor_state.term;
        let last_log_term = shared_state.sk.last_log_term();
        let flush_lsn = shared_state.sk.flush_lsn();
        let upto_segno = flush_lsn.segment_number(wal_seg_size);
        // have some limit on max number of segments as a sanity check
        const MAX_ALLOWED_SEGS: u64 = 1000;
        let num_segs = upto_segno - from_segno + 1;
        if num_segs > MAX_ALLOWED_SEGS {
            bail!(
                "snapshot is called on timeline with {} segments, but the limit is {}",
                num_segs,
                MAX_ALLOWED_SEGS
            );
        }

        // Prevent WAL removal while we're streaming data.
        //
        // Since this a flag, not a counter just bail out if already set; we
        // shouldn't need concurrent snapshotting.
        if shared_state.wal_removal_on_hold {
            bail!("wal_removal_on_hold is already true");
        }
        shared_state.wal_removal_on_hold = true;

        // Drop shared_state to release the lock, before calling wal_residence_guard().
        drop(shared_state);

        let tli_copy = self.wal_residence_guard().await?;
        let bctx = SnapshotContext {
            from_segno,
            upto_segno,
            term,
            last_log_term,
            flush_lsn,
            wal_seg_size,
            tli: tli_copy,
        };

        Ok(bctx)
    }

    /// Finish snapshotting: check that term(s) hasn't changed.
    ///
    /// Note that WAL gc hold off is removed in Drop of SnapshotContext to not
    /// forget this if snapshotting fails mid the way.
    pub async fn finish_snapshot(&self, bctx: &SnapshotContext) -> Result<()> {
        let shared_state = self.read_shared_state().await;
        let term = shared_state.sk.state().acceptor_state.term;
        let last_log_term = shared_state.sk.last_log_term();
        // There are some cases to relax this check (e.g. last_log_term might
        // change, but as long as older history is strictly part of new that's
        // fine), but there is no need to do it.
        if bctx.term != term || bctx.last_log_term != last_log_term {
            bail!("term(s) changed during snapshot: were term={}, last_log_term={}, now term={}, last_log_term={}",
              bctx.term, bctx.last_log_term, term, last_log_term);
        }
        Ok(())
    }
}

/// pull_timeline request body.
#[derive(Debug, Deserialize)]
pub struct Request {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub http_hosts: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Response {
    // Donor safekeeper host
    pub safekeeper_host: String,
    // TODO: add more fields?
}

/// Response for debug dump request.
#[derive(Debug, Deserialize)]
pub struct DebugDumpResponse {
    pub start_time: DateTime<Utc>,
    pub finish_time: DateTime<Utc>,
    pub timelines: Vec<debug_dump::Timeline>,
    pub timelines_count: usize,
    pub config: debug_dump::Config,
}

/// Find the most advanced safekeeper and pull timeline from it.
pub async fn handle_request(
    request: Request,
    sk_auth_token: Option<SecretString>,
    global_timelines: Arc<GlobalTimelines>,
) -> Result<Response> {
    let existing_tli = global_timelines.get(TenantTimelineId::new(
        request.tenant_id,
        request.timeline_id,
    ));
    if existing_tli.is_ok() {
        bail!("Timeline {} already exists", request.timeline_id);
    }

    let http_hosts = request.http_hosts.clone();

    // Figure out statuses of potential donors.
    let responses: Vec<Result<TimelineStatus, mgmt_api::Error>> =
        futures::future::join_all(http_hosts.iter().map(|url| async {
            let cclient = Client::new(url.clone(), sk_auth_token.clone());
            let info = cclient
                .timeline_status(request.tenant_id, request.timeline_id)
                .await?;
            Ok(info)
        }))
        .await;

    let mut statuses = Vec::new();
    for (i, response) in responses.into_iter().enumerate() {
        let status = response.context(format!("fetching status from {}", http_hosts[i]))?;
        statuses.push((status, i));
    }

    // Find the most advanced safekeeper
    let (status, i) = statuses
        .into_iter()
        .max_by_key(|(status, _)| {
            (
                status.acceptor_state.epoch,
                status.flush_lsn,
                status.commit_lsn,
            )
        })
        .unwrap();
    let safekeeper_host = http_hosts[i].clone();

    assert!(status.tenant_id == request.tenant_id);
    assert!(status.timeline_id == request.timeline_id);

    pull_timeline(status, safekeeper_host, sk_auth_token, global_timelines).await
}

async fn pull_timeline(
    status: TimelineStatus,
    host: String,
    sk_auth_token: Option<SecretString>,
    global_timelines: Arc<GlobalTimelines>,
) -> Result<Response> {
    let ttid = TenantTimelineId::new(status.tenant_id, status.timeline_id);
    info!(
        "pulling timeline {} from safekeeper {}, commit_lsn={}, flush_lsn={}, term={}, epoch={}",
        ttid,
        host,
        status.commit_lsn,
        status.flush_lsn,
        status.acceptor_state.term,
        status.acceptor_state.epoch
    );

    let conf = &global_timelines.get_global_config();

    let (_tmp_dir, tli_dir_path) = create_temp_timeline_dir(conf, ttid).await?;

    let client = Client::new(host.clone(), sk_auth_token.clone());
    // Request stream with basebackup archive.
    let bb_resp = client
        .snapshot(status.tenant_id, status.timeline_id, conf.my_id)
        .await?;

    // Make Stream of Bytes from it...
    let bb_stream = bb_resp.bytes_stream().map_err(std::io::Error::other);
    // and turn it into StreamReader implementing AsyncRead.
    let bb_reader = tokio_util::io::StreamReader::new(bb_stream);

    // Extract it on the fly to the disk. We don't use simple unpack() to fsync
    // files.
    let mut entries = Archive::new(bb_reader).entries()?;
    while let Some(base_tar_entry) = entries.next().await {
        let mut entry = base_tar_entry?;
        let header = entry.header();
        let file_path = header.path()?.into_owned();
        match header.entry_type() {
            tokio_tar::EntryType::Regular => {
                let utf8_file_path =
                    Utf8PathBuf::from_path_buf(file_path).expect("non-Unicode path");
                let dst_path = tli_dir_path.join(utf8_file_path);
                let mut f = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&dst_path)
                    .await?;
                tokio::io::copy(&mut entry, &mut f).await?;
                // fsync the file
                f.sync_all().await?;
            }
            _ => {
                bail!(
                    "entry {} in backup tar archive is of unexpected type: {:?}",
                    file_path.display(),
                    header.entry_type()
                );
            }
        }
    }

    // fsync temp timeline directory to remember its contents.
    fsync_async_opt(&tli_dir_path, !conf.no_sync).await?;

    // Let's create timeline from temp directory and verify that it's correct
    let (commit_lsn, flush_lsn) = validate_temp_timeline(conf, ttid, &tli_dir_path).await?;
    info!(
        "finished downloading timeline {}, commit_lsn={}, flush_lsn={}",
        ttid, commit_lsn, flush_lsn
    );
    assert!(status.commit_lsn <= status.flush_lsn);

    // Finally, load the timeline.
    let _tli = global_timelines
        .load_temp_timeline(ttid, &tli_dir_path, false)
        .await?;

    Ok(Response {
        safekeeper_host: host,
    })
}
