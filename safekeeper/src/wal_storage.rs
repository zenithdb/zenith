//! This module has everything to deal with WAL -- reading and writing to disk.
//!
//! Safekeeper WAL is stored in the timeline directory, in format similar to pg_wal.
//! PG timeline is always 1, so WAL segments are usually have names like this:
//! - 000000010000000000000001
//! - 000000010000000000000002.partial
//!
//! Note that last file has `.partial` suffix, that's different from postgres.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};
use futures::future::BoxFuture;
use postgres_ffi::v14::xlog_utils::{IsPartialXLogFileName, IsXLogFileName, XLogFromFileName};
use postgres_ffi::{dispatch_pgversion, XLogSegNo, PG_TLI};
use remote_storage::RemotePath;
use std::cmp::{max, min};
use std::future::Future;
use std::io::{self, SeekFrom};
use std::pin::Pin;
use tokio::fs::{self, remove_file, File, OpenOptions};
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::*;
use utils::crashsafe::durable_rename;

use crate::metrics::{
    time_io_closure, WalStorageMetrics, REMOVED_WAL_SEGMENTS, WAL_STORAGE_OPERATION_SECONDS,
};
use crate::state::TimelinePersistentState;
use crate::wal_backup::{read_object, remote_timeline_path};
use postgres_ffi::waldecoder::WalStreamDecoder;
use postgres_ffi::XLogFileName;
use pq_proto::SystemId;
use utils::{id::TenantTimelineId, lsn::Lsn};

pub trait Storage {
    /// Last written LSN.
    fn write_lsn(&self) -> Lsn;
    /// End LSN of last durably stored WAL record.
    fn flush_record_lsn(&self) -> Lsn;

    /// Initialize segment by creating proper long header at the beginning of
    /// the segment and short header at the page of given LSN. This is only used
    /// for timeline initialization because compute will stream data only since
    /// init_lsn. Other segment headers are included in compute stream.
    fn initialize_first_segment(
        &mut self,
        init_lsn: Lsn,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Write piece of WAL from buf to disk, but not necessarily sync it.
    fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> impl Future<Output = Result<()>> + Send;

    /// Truncate WAL at specified LSN, which must be the end of WAL record.
    fn truncate_wal(&mut self, end_pos: Lsn) -> impl Future<Output = Result<()>> + Send;

    /// Durably store WAL on disk, up to the last written WAL record.
    fn flush_wal(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Remove all segments <= given segno. Returns function doing that as we
    /// want to perform it without timeline lock.
    fn remove_up_to(&self, segno_up_to: XLogSegNo) -> BoxFuture<'static, anyhow::Result<()>>;

    /// Release resources associated with the storage -- technically, close FDs.
    /// Currently we don't remove timelines until restart (#3146), so need to
    /// spare descriptors. This would be useful for temporary tli detach as
    /// well.
    fn close(&mut self) {}

    /// Get metrics for this timeline.
    fn get_metrics(&self) -> WalStorageMetrics;
}

/// PhysicalStorage is a storage that stores WAL on disk. Writes are separated from flushes
/// for better performance. Storage is initialized in the constructor.
///
/// WAL is stored in segments, each segment is a file. Last segment has ".partial" suffix in
/// its filename and may be not fully flushed.
///
/// Relationship of LSNs:
/// `write_lsn` >= `write_record_lsn` >= `flush_record_lsn`
///
/// When storage is created first time, all LSNs are zeroes and there are no segments on disk.
pub struct PhysicalStorage {
    metrics: WalStorageMetrics,
    timeline_dir: Utf8PathBuf,

    /// Disables fsync if true.
    no_sync: bool,

    /// Size of WAL segment in bytes.
    wal_seg_size: usize,
    pg_version: u32,
    system_id: u64,

    /// Written to disk, but possibly still in the cache and not fully persisted.
    /// Also can be ahead of record_lsn, if happen to be in the middle of a WAL record.
    write_lsn: Lsn,

    /// The LSN of the last WAL record written to disk. Still can be not fully
    /// flushed.
    ///
    /// Note: Normally it (and flush_record_lsn) is <= write_lsn, but after xlog
    /// switch ingest the reverse is true because we don't bump write_lsn up to
    /// the next segment: WAL stream from the compute doesn't have the gap and
    /// for simplicity / as a sanity check we disallow any non-sequential
    /// writes, so write zeros as is.
    ///
    /// Similar effect is in theory possible due to LSN alignment: if record
    /// ends at *2, decoder will report end lsn as *8 even though we haven't
    /// written these zeros yet. In practice compute likely never sends
    /// non-aligned chunks of data.
    write_record_lsn: Lsn,

    /// The last LSN flushed to disk. May be in the middle of a record.
    ///
    /// NB: when the rest of the system refers to `flush_lsn`, it usually
    /// means `flush_record_lsn`. This `flush_lsn` is only used internally.
    flush_lsn: Lsn,

    /// The LSN of the last WAL record flushed to disk.
    ///
    /// NB: when the rest of the system refers to `flush_lsn`, it usually
    /// means `flush_record_lsn`.
    flush_record_lsn: Lsn,

    /// Decoder is required for detecting boundaries of WAL records.
    decoder: WalStreamDecoder,

    /// Cached open file for the last segment.
    ///
    /// If Some(file) is open, then it always:
    /// - has ".partial" suffix
    /// - points to write_lsn, so no seek is needed for writing
    /// - doesn't point to the end of the segment
    file: Option<File>,

    /// When true, WAL truncation potentially has been interrupted and we need
    /// to finish it before allowing WAL writes; see truncate_wal for details.
    /// In this case [`write_lsn`] can be less than actually written WAL on
    /// disk. In particular, there can be a case with unexpected .partial file.
    ///
    /// Imagine the following:
    /// - 000000010000000000000001
    ///   - it was fully written, but the last record is split between 2
    ///     segments
    ///   - after restart, `find_end_of_wal()` returned 0/1FFFFF0, which is in
    ///     the end of this segment
    ///   - `write_lsn`, `write_record_lsn` and `flush_record_lsn` were
    ///     initialized to 0/1FFFFF0
    /// - 000000010000000000000002.partial
    ///   - it has only 1 byte written, which is not enough to make a full WAL
    ///     record
    ///
    /// Partial segment 002 has no WAL records, and it will be removed by the
    /// next truncate_wal(). This flag will be set to true after the first
    /// truncate_wal() call.
    ///
    /// [`write_lsn`]: Self::write_lsn
    pending_wal_truncation: bool,
}

impl PhysicalStorage {
    /// Create new storage. If commit_lsn is not zero, flush_lsn is tried to be restored from
    /// the disk. Otherwise, all LSNs are set to zero.
    pub fn new(
        ttid: &TenantTimelineId,
        timeline_dir: &Utf8Path,
        state: &TimelinePersistentState,
        no_sync: bool,
    ) -> Result<PhysicalStorage> {
        let wal_seg_size = state.server.wal_seg_size as usize;

        // Find out where stored WAL ends, starting at commit_lsn which is a
        // known recent record boundary (unless we don't have WAL at all).
        //
        // NB: find_end_of_wal MUST be backwards compatible with the previously
        // written WAL. If find_end_of_wal fails to read any WAL written by an
        // older version of the code, we could lose data forever.
        let write_lsn = if state.commit_lsn == Lsn(0) {
            Lsn(0)
        } else {
            let version = state.server.pg_version / 10000;

            dispatch_pgversion!(
                version,
                pgv::xlog_utils::find_end_of_wal(
                    timeline_dir.as_std_path(),
                    wal_seg_size,
                    state.commit_lsn,
                )?,
                bail!("unsupported postgres version: {}", version)
            )
        };

        // note: this assumes we fsync'ed whole datadir on start.
        let flush_lsn = write_lsn;

        debug!(
            "initialized storage for timeline {}, flush_lsn={}, commit_lsn={}, peer_horizon_lsn={}",
            ttid.timeline_id, flush_lsn, state.commit_lsn, state.peer_horizon_lsn,
        );
        if flush_lsn < state.commit_lsn {
            bail!("timeline {} potential data loss: flush_lsn {} by find_end_of_wal is less than commit_lsn  {} from control file", ttid.timeline_id, flush_lsn, state.commit_lsn);
        }
        if flush_lsn < state.peer_horizon_lsn {
            warn!(
                "timeline {}: flush_lsn {} is less than cfile peer_horizon_lsn {}",
                ttid.timeline_id, flush_lsn, state.peer_horizon_lsn
            );
        }

        Ok(PhysicalStorage {
            metrics: WalStorageMetrics::default(),
            timeline_dir: timeline_dir.to_path_buf(),
            no_sync,
            wal_seg_size,
            pg_version: state.server.pg_version,
            system_id: state.server.system_id,
            write_lsn,
            write_record_lsn: write_lsn,
            flush_lsn,
            flush_record_lsn: flush_lsn,
            decoder: WalStreamDecoder::new(write_lsn, state.server.pg_version / 10000),
            file: None,
            pending_wal_truncation: true,
        })
    }

    /// Get all known state of the storage.
    pub fn internal_state(&self) -> (Lsn, Lsn, Lsn, bool) {
        (
            self.write_lsn,
            self.write_record_lsn,
            self.flush_record_lsn,
            self.file.is_some(),
        )
    }

    /// Call fsync if config requires so.
    async fn fsync_file(&mut self, file: &File) -> Result<()> {
        if !self.no_sync {
            self.metrics
                .observe_flush_seconds(time_io_closure(file.sync_all()).await?);
        }
        Ok(())
    }

    /// Call fdatasync if config requires so.
    async fn fdatasync_file(&mut self, file: &File) -> Result<()> {
        if !self.no_sync {
            self.metrics
                .observe_flush_seconds(time_io_closure(file.sync_data()).await?);
        }
        Ok(())
    }

    /// Open or create WAL segment file. Caller must call seek to the wanted position.
    /// Returns `file` and `is_partial`.
    async fn open_or_create(&mut self, segno: XLogSegNo) -> Result<(File, bool)> {
        let (wal_file_path, wal_file_partial_path) =
            wal_file_paths(&self.timeline_dir, segno, self.wal_seg_size);

        // Try to open already completed segment
        if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path).await {
            Ok((file, false))
        } else if let Ok(file) = OpenOptions::new()
            .write(true)
            .open(&wal_file_partial_path)
            .await
        {
            // Try to open existing partial file
            Ok((file, true))
        } else {
            let _timer = WAL_STORAGE_OPERATION_SECONDS
                .with_label_values(&["initialize_segment"])
                .start_timer();
            // Create and fill new partial file
            //
            // We're using fdatasync during WAL writing, so file size must not
            // change; to this end it is filled with zeros here. To avoid using
            // half initialized segment, first bake it under tmp filename and
            // then rename.
            let tmp_path = self.timeline_dir.join("waltmp");
            let file = File::create(&tmp_path)
                .await
                .with_context(|| format!("Failed to open tmp wal file {:?}", &tmp_path))?;

            fail::fail_point!("sk-zero-segment", |_| {
                info!("sk-zero-segment failpoint hit");
                Err(anyhow::anyhow!("failpoint: sk-zero-segment"))
            });
            file.set_len(self.wal_seg_size as u64).await?;

            if let Err(e) = durable_rename(&tmp_path, &wal_file_partial_path, !self.no_sync).await {
                // Probably rename succeeded, but fsync of it failed. Remove
                // the file then to avoid using it.
                remove_file(wal_file_partial_path)
                    .await
                    .or_else(utils::fs_ext::ignore_not_found)?;
                return Err(e.into());
            }
            Ok((file, true))
        }
    }

    /// Write WAL bytes, which are known to be located in a single WAL segment. Returns true if the
    /// segment was completed, closed, and flushed to disk.
    async fn write_in_segment(&mut self, segno: u64, xlogoff: usize, buf: &[u8]) -> Result<bool> {
        let mut file = if let Some(file) = self.file.take() {
            file
        } else {
            let (mut file, is_partial) = self.open_or_create(segno).await?;
            assert!(is_partial, "unexpected write into non-partial segment file");
            file.seek(SeekFrom::Start(xlogoff as u64)).await?;
            file
        };

        file.write_all(buf).await?;
        // Note: flush just ensures write above reaches the OS (this is not
        // needed in case of sync IO as Write::write there calls directly write
        // syscall, but needed in case of async). It does *not* fsyncs the file.
        file.flush().await?;

        if xlogoff + buf.len() == self.wal_seg_size {
            // If we reached the end of a WAL segment, flush and close it.
            self.fdatasync_file(&file).await?;

            // Rename partial file to completed file
            let (wal_file_path, wal_file_partial_path) =
                wal_file_paths(&self.timeline_dir, segno, self.wal_seg_size);
            fs::rename(wal_file_partial_path, wal_file_path).await?;
            Ok(true)
        } else {
            // otherwise, file can be reused later
            self.file = Some(file);
            Ok(false)
        }
    }

    /// Writes WAL to the segment files, until everything is writed. If some segments
    /// are fully written, they are flushed to disk. The last (partial) segment can
    /// be flushed separately later.
    ///
    /// Updates `write_lsn` and `flush_lsn`.
    async fn write_exact(&mut self, pos: Lsn, mut buf: &[u8]) -> Result<()> {
        // TODO: this shouldn't be possible, except possibly with write_lsn == 0.
        // Rename this method to `append_exact`, and make it append-only, removing
        // the `pos` parameter and this check. For this reason, we don't update
        // `flush_lsn` here.
        if self.write_lsn != pos {
            // need to flush the file before discarding it
            if let Some(file) = self.file.take() {
                self.fdatasync_file(&file).await?;
            }

            self.write_lsn = pos;
        }

        while !buf.is_empty() {
            // Extract WAL location for this block
            let xlogoff = self.write_lsn.segment_offset(self.wal_seg_size);
            let segno = self.write_lsn.segment_number(self.wal_seg_size);

            // If crossing a WAL boundary, only write up until we reach wal segment size.
            let bytes_write = if xlogoff + buf.len() > self.wal_seg_size {
                self.wal_seg_size - xlogoff
            } else {
                buf.len()
            };

            let flushed = self
                .write_in_segment(segno, xlogoff, &buf[..bytes_write])
                .await?;
            self.write_lsn += bytes_write as u64;
            if flushed {
                self.flush_lsn = self.write_lsn;
            }
            buf = &buf[bytes_write..];
        }

        Ok(())
    }
}

impl Storage for PhysicalStorage {
    // Last written LSN.
    fn write_lsn(&self) -> Lsn {
        self.write_lsn
    }
    /// End LSN of the last durably stored WAL record.
    fn flush_record_lsn(&self) -> Lsn {
        self.flush_record_lsn
    }

    async fn initialize_first_segment(&mut self, init_lsn: Lsn) -> Result<()> {
        let _timer = WAL_STORAGE_OPERATION_SECONDS
            .with_label_values(&["initialize_first_segment"])
            .start_timer();

        let segno = init_lsn.segment_number(self.wal_seg_size);
        let (mut file, _) = self.open_or_create(segno).await?;
        let major_pg_version = self.pg_version / 10000;
        let wal_seg =
            postgres_ffi::generate_wal_segment(segno, self.system_id, major_pg_version, init_lsn)?;
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&wal_seg).await?;
        file.flush().await?;
        info!("initialized segno {} at lsn {}", segno, init_lsn);
        // note: file is *not* fsynced
        Ok(())
    }

    /// Write WAL to disk.
    async fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
        // Disallow any non-sequential writes, which can result in gaps or overwrites.
        // If we need to move the pointer, use truncate_wal() instead.
        if self.write_lsn > startpos {
            bail!(
                "write_wal rewrites WAL written before, write_lsn={}, startpos={}",
                self.write_lsn,
                startpos
            );
        }
        if self.write_lsn < startpos && self.write_lsn != Lsn(0) {
            bail!(
                "write_wal creates gap in written WAL, write_lsn={}, startpos={}",
                self.write_lsn,
                startpos
            );
        }
        if self.pending_wal_truncation {
            bail!(
                "write_wal called with pending WAL truncation, write_lsn={}, startpos={}",
                self.write_lsn,
                startpos
            );
        }

        let write_seconds = time_io_closure(self.write_exact(startpos, buf)).await?;
        // WAL is written, updating write metrics
        self.metrics.observe_write_seconds(write_seconds);
        self.metrics.observe_write_bytes(buf.len());

        // Figure out the last record's end LSN and update `write_record_lsn`
        // (if we got a whole record). The write may also have closed and
        // flushed a segment, so update `flush_record_lsn` as well.
        if self.decoder.available() != startpos {
            info!(
                "restart decoder from {} to {}",
                self.decoder.available(),
                startpos,
            );
            let pg_version = self.decoder.pg_version;
            self.decoder = WalStreamDecoder::new(startpos, pg_version);
        }
        self.decoder.feed_bytes(buf);

        if self.write_record_lsn <= self.flush_lsn {
            // We may have flushed a previously written record.
            self.flush_record_lsn = self.write_record_lsn;
        }
        while let Some((lsn, _rec)) = self.decoder.poll_decode()? {
            self.write_record_lsn = lsn;
            if lsn <= self.flush_lsn {
                self.flush_record_lsn = lsn;
            }
        }

        Ok(())
    }

    async fn flush_wal(&mut self) -> Result<()> {
        if self.flush_record_lsn == self.write_record_lsn {
            // no need to do extra flush
            return Ok(());
        }

        if let Some(unflushed_file) = self.file.take() {
            self.fdatasync_file(&unflushed_file).await?;
            self.file = Some(unflushed_file);
        } else {
            // We have unflushed data (write_lsn != flush_lsn), but no file. This
            // shouldn't happen, since the segment is flushed on close.
            bail!(
                "unexpected unflushed data with no open file, write_lsn={}, flush_lsn={}",
                self.write_lsn,
                self.flush_record_lsn
            );
        }

        // everything is flushed now, let's update flush_lsn
        self.flush_lsn = self.write_lsn;
        self.flush_record_lsn = self.write_record_lsn;
        Ok(())
    }

    /// Truncate written WAL by removing all WAL segments after the given LSN.
    /// end_pos must point to the end of the WAL record.
    async fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()> {
        let _timer = WAL_STORAGE_OPERATION_SECONDS
            .with_label_values(&["truncate_wal"])
            .start_timer();

        // Streaming must not create a hole, so truncate cannot be called on
        // non-written lsn.
        if self.write_record_lsn != Lsn(0) && end_pos > self.write_record_lsn {
            bail!(
                "truncate_wal called on non-written WAL, write_record_lsn={}, end_pos={}",
                self.write_record_lsn,
                end_pos
            );
        }

        // Quick exit if nothing to do and we know that the state is clean to
        // avoid writing up to 16 MiB of zeros on disk (this happens on each
        // connect).
        if !self.pending_wal_truncation
            && end_pos == self.write_lsn
            && end_pos == self.flush_record_lsn
        {
            return Ok(());
        }

        // Atomicity: we start with LSNs reset because once on disk deletion is
        // started it can't be reversed. However, we might crash/error in the
        // middle, leaving garbage above the truncation point. In theory,
        // concatenated with previous records it might form bogus WAL (though
        // very unlikely in practice because CRC would guard from that). To
        // protect, set pending_wal_truncation flag before beginning: it means
        // truncation must be retried and WAL writes are prohibited until it
        // succeeds. Flag is also set on boot because we don't know if the last
        // state was clean.
        //
        // Protocol (HandleElected before first AppendRequest) ensures we'll
        // always try to ensure clean truncation before any writes.
        self.pending_wal_truncation = true;

        self.write_lsn = end_pos;
        self.flush_lsn = end_pos;
        self.write_record_lsn = end_pos;
        self.flush_record_lsn = end_pos;

        // Close previously opened file, if any
        if let Some(unflushed_file) = self.file.take() {
            self.fdatasync_file(&unflushed_file).await?;
        }

        let xlogoff = end_pos.segment_offset(self.wal_seg_size);
        let segno = end_pos.segment_number(self.wal_seg_size);

        // Remove all segments after the given LSN.
        remove_segments_from_disk(&self.timeline_dir, self.wal_seg_size, |x| x > segno).await?;

        let (file, is_partial) = self.open_or_create(segno).await?;

        // Fill end with zeroes
        file.set_len(xlogoff as u64).await?;
        file.set_len(self.wal_seg_size as u64).await?;
        self.fsync_file(&file).await?;

        if !is_partial {
            // Make segment partial once again
            let (wal_file_path, wal_file_partial_path) =
                wal_file_paths(&self.timeline_dir, segno, self.wal_seg_size);
            fs::rename(wal_file_path, wal_file_partial_path).await?;
        }

        self.pending_wal_truncation = false;
        Ok(())
    }

    fn remove_up_to(&self, segno_up_to: XLogSegNo) -> BoxFuture<'static, anyhow::Result<()>> {
        let timeline_dir = self.timeline_dir.clone();
        let wal_seg_size = self.wal_seg_size;
        Box::pin(async move {
            remove_segments_from_disk(&timeline_dir, wal_seg_size, |x| x <= segno_up_to).await
        })
    }

    fn close(&mut self) {
        // close happens in destructor
        let _open_file = self.file.take();
    }

    fn get_metrics(&self) -> WalStorageMetrics {
        self.metrics.clone()
    }
}

/// Remove all WAL segments in timeline_dir that match the given predicate.
async fn remove_segments_from_disk(
    timeline_dir: &Utf8Path,
    wal_seg_size: usize,
    remove_predicate: impl Fn(XLogSegNo) -> bool,
) -> Result<()> {
    let _timer = WAL_STORAGE_OPERATION_SECONDS
        .with_label_values(&["remove_segments_from_disk"])
        .start_timer();

    let mut n_removed = 0;
    let mut min_removed = u64::MAX;
    let mut max_removed = u64::MIN;

    let mut entries = fs::read_dir(timeline_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let entry_path = entry.path();
        let fname = entry_path.file_name().unwrap();
        /* Ignore files that are not XLOG segments */
        if !IsXLogFileName(fname) && !IsPartialXLogFileName(fname) {
            continue;
        }
        let (segno, _) = XLogFromFileName(fname, wal_seg_size)?;
        if remove_predicate(segno) {
            remove_file(entry_path).await?;
            n_removed += 1;
            min_removed = min(min_removed, segno);
            max_removed = max(max_removed, segno);
            REMOVED_WAL_SEGMENTS.inc();
        }
    }

    if n_removed > 0 {
        info!(
            "removed {} WAL segments [{}; {}]",
            n_removed, min_removed, max_removed
        );
    }
    Ok(())
}

pub struct WalReader {
    remote_path: RemotePath,
    timeline_dir: Utf8PathBuf,
    wal_seg_size: usize,
    pos: Lsn,
    wal_segment: Option<Pin<Box<dyn AsyncRead + Send + Sync>>>,

    // S3 will be used to read WAL if LSN is not available locally
    enable_remote_read: bool,

    // We don't have WAL locally if LSN is less than local_start_lsn
    local_start_lsn: Lsn,
    // We will respond with zero-ed bytes before this Lsn as long as
    // pos is in the same segment as timeline_start_lsn.
    timeline_start_lsn: Lsn,
    // integer version number of PostgreSQL, e.g. 14; 15; 16
    pg_version: u32,
    system_id: SystemId,
    timeline_start_segment: Option<Bytes>,
}

impl WalReader {
    pub fn new(
        ttid: &TenantTimelineId,
        timeline_dir: Utf8PathBuf,
        state: &TimelinePersistentState,
        start_pos: Lsn,
        enable_remote_read: bool,
    ) -> Result<Self> {
        if state.server.wal_seg_size == 0 || state.local_start_lsn == Lsn(0) {
            bail!("state uninitialized, no data to read");
        }

        // TODO: Upgrade to bail!() once we know this couldn't possibly happen
        if state.timeline_start_lsn == Lsn(0) {
            warn!("timeline_start_lsn uninitialized before initializing wal reader");
        }

        if start_pos
            < state
                .timeline_start_lsn
                .segment_lsn(state.server.wal_seg_size as usize)
        {
            bail!(
                "Requested streaming from {}, which is before the start of the timeline {}, and also doesn't start at the first segment of that timeline",
                start_pos,
                state.timeline_start_lsn
            );
        }

        Ok(Self {
            remote_path: remote_timeline_path(ttid)?,
            timeline_dir,
            wal_seg_size: state.server.wal_seg_size as usize,
            pos: start_pos,
            wal_segment: None,
            enable_remote_read,
            local_start_lsn: state.local_start_lsn,
            timeline_start_lsn: state.timeline_start_lsn,
            pg_version: state.server.pg_version / 10000,
            system_id: state.server.system_id,
            timeline_start_segment: None,
        })
    }

    /// Read WAL at current position into provided buf, returns number of bytes
    /// read. It can be smaller than buf size only if segment boundary is
    /// reached.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // If this timeline is new, we may not have a full segment yet, so
        // we pad the first bytes of the timeline's first WAL segment with 0s
        if self.pos < self.timeline_start_lsn {
            debug_assert_eq!(
                self.pos.segment_number(self.wal_seg_size),
                self.timeline_start_lsn.segment_number(self.wal_seg_size)
            );

            // All bytes after timeline_start_lsn are in WAL, but those before
            // are not, so we manually construct an empty segment for the bytes
            // not available in this timeline.
            if self.timeline_start_segment.is_none() {
                let it = postgres_ffi::generate_wal_segment(
                    self.timeline_start_lsn.segment_number(self.wal_seg_size),
                    self.system_id,
                    self.pg_version,
                    self.timeline_start_lsn,
                )?;
                self.timeline_start_segment = Some(it);
            }

            assert!(self.timeline_start_segment.is_some());
            let segment = self.timeline_start_segment.take().unwrap();

            let seg_bytes = &segment[..];

            // How much of the current segment have we already consumed?
            let pos_seg_offset = self.pos.segment_offset(self.wal_seg_size);

            // How many bytes may we consume in total?
            let tl_start_seg_offset = self.timeline_start_lsn.segment_offset(self.wal_seg_size);

            debug_assert!(seg_bytes.len() > pos_seg_offset);
            debug_assert!(seg_bytes.len() > tl_start_seg_offset);

            // Copy as many bytes as possible into the buffer
            let len = (tl_start_seg_offset - pos_seg_offset).min(buf.len());
            buf[0..len].copy_from_slice(&seg_bytes[pos_seg_offset..pos_seg_offset + len]);

            self.pos += len as u64;

            // If we're done with the segment, we can release it's memory.
            // However, if we're not yet done, store it so that we don't have to
            // construct the segment the next time this function is called.
            if self.pos < self.timeline_start_lsn {
                self.timeline_start_segment = Some(segment);
            }

            return Ok(len);
        }

        let mut wal_segment = match self.wal_segment.take() {
            Some(reader) => reader,
            None => self.open_segment().await?,
        };

        // How much to read and send in message? We cannot cross the WAL file
        // boundary, and we don't want send more than provided buffer.
        let xlogoff = self.pos.segment_offset(self.wal_seg_size);
        let send_size = min(buf.len(), self.wal_seg_size - xlogoff);

        // Read some data from the file.
        let buf = &mut buf[0..send_size];
        let send_size = wal_segment.read_exact(buf).await?;
        self.pos += send_size as u64;

        // Decide whether to reuse this file. If we don't set wal_segment here
        // a new reader will be opened next time.
        if self.pos.segment_offset(self.wal_seg_size) != 0 {
            self.wal_segment = Some(wal_segment);
        }

        Ok(send_size)
    }

    /// Open WAL segment at the current position of the reader.
    async fn open_segment(&self) -> Result<Pin<Box<dyn AsyncRead + Send + Sync>>> {
        let xlogoff = self.pos.segment_offset(self.wal_seg_size);
        let segno = self.pos.segment_number(self.wal_seg_size);
        let wal_file_name = XLogFileName(PG_TLI, segno, self.wal_seg_size);

        // Try to open local file, if we may have WAL locally
        if self.pos >= self.local_start_lsn {
            let res = open_wal_file(&self.timeline_dir, segno, self.wal_seg_size).await;
            match res {
                Ok((mut file, _)) => {
                    file.seek(SeekFrom::Start(xlogoff as u64)).await?;
                    return Ok(Box::pin(file));
                }
                Err(e) => {
                    let is_not_found = e.chain().any(|e| {
                        if let Some(e) = e.downcast_ref::<io::Error>() {
                            e.kind() == io::ErrorKind::NotFound
                        } else {
                            false
                        }
                    });
                    if !is_not_found {
                        return Err(e);
                    }
                    // NotFound is expected, fall through to remote read
                }
            };
        }

        // Try to open remote file, if remote reads are enabled
        if self.enable_remote_read {
            let remote_wal_file_path = self.remote_path.join(&wal_file_name);
            return read_object(&remote_wal_file_path, xlogoff as u64).await;
        }

        bail!("WAL segment is not found")
    }
}

/// Helper function for opening WAL segment `segno` in `dir`. Returns file and
/// whether it is .partial.
pub(crate) async fn open_wal_file(
    timeline_dir: &Utf8Path,
    segno: XLogSegNo,
    wal_seg_size: usize,
) -> Result<(tokio::fs::File, bool)> {
    let (wal_file_path, wal_file_partial_path) = wal_file_paths(timeline_dir, segno, wal_seg_size);

    // First try to open the .partial file.
    let mut partial_path = wal_file_path.to_owned();
    partial_path.set_extension("partial");
    if let Ok(opened_file) = tokio::fs::File::open(&wal_file_partial_path).await {
        return Ok((opened_file, true));
    }

    // If that failed, try it without the .partial extension.
    let pf = tokio::fs::File::open(&wal_file_path)
        .await
        .with_context(|| format!("failed to open WAL file {:#}", wal_file_path))
        .map_err(|e| {
            warn!("{}", e);
            e
        })?;

    Ok((pf, false))
}

/// Helper returning full path to WAL segment file and its .partial brother.
pub fn wal_file_paths(
    timeline_dir: &Utf8Path,
    segno: XLogSegNo,
    wal_seg_size: usize,
) -> (Utf8PathBuf, Utf8PathBuf) {
    let wal_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
    let wal_file_path = timeline_dir.join(wal_file_name.clone());
    let wal_file_partial_path = timeline_dir.join(wal_file_name + ".partial");
    (wal_file_path, wal_file_partial_path)
}
