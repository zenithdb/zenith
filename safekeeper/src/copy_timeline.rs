use anyhow::{bail, Result};
use camino::Utf8PathBuf;
use postgres_ffi::{MAX_SEND_SIZE, WAL_SEGMENT_SIZE};
use safekeeper_api::membership::Configuration;
use std::sync::Arc;
use tokio::{
    fs::OpenOptions,
    io::{AsyncSeekExt, AsyncWriteExt},
};
use tracing::{info, warn};
use utils::{id::TenantTimelineId, lsn::Lsn};

use crate::{
    control_file::FileStorage,
    state::TimelinePersistentState,
    timeline::{TimelineError, WalResidentTimeline},
    timelines_global_map::{create_temp_timeline_dir, validate_temp_timeline},
    wal_backup::copy_s3_segments,
    wal_storage::{wal_file_paths, WalReader},
    GlobalTimelines,
};

// we don't want to have more than 10 segments on disk after copy, because they take space
const MAX_BACKUP_LAG: u64 = 10 * WAL_SEGMENT_SIZE as u64;

pub struct Request {
    pub source_ttid: TenantTimelineId,
    pub until_lsn: Lsn,
    pub destination_ttid: TenantTimelineId,
}

pub async fn handle_request(
    request: Request,
    global_timelines: Arc<GlobalTimelines>,
) -> Result<()> {
    // TODO: request.until_lsn MUST be a valid LSN, and we cannot check it :(
    //   if LSN will point to the middle of a WAL record, timeline will be in "broken" state

    match global_timelines.get(request.destination_ttid) {
        // timeline already exists. would be good to check that this timeline is the copy
        // of the source timeline, but it isn't obvious how to do that
        Ok(_) => return Ok(()),
        // timeline not found, we are going to create it
        Err(TimelineError::NotFound(_)) => {}
        // error, probably timeline was deleted
        res => {
            res?;
        }
    }

    let source = global_timelines.get(request.source_ttid)?;
    let source_tli = source.wal_residence_guard().await?;

    let conf = &global_timelines.get_global_config();
    let ttid = request.destination_ttid;

    let (_tmp_dir, tli_dir_path) = create_temp_timeline_dir(conf, ttid).await?;

    let (mem_state, state) = source_tli.get_state().await;
    let start_lsn = state.timeline_start_lsn;
    if start_lsn == Lsn::INVALID {
        bail!("timeline is not initialized");
    }
    let backup_lsn = mem_state.backup_lsn;

    {
        let commit_lsn = mem_state.commit_lsn;
        let flush_lsn = source_tli.get_flush_lsn().await;

        info!(
            "collected info about source timeline: start_lsn={}, backup_lsn={}, commit_lsn={}, flush_lsn={}",
            start_lsn, backup_lsn, commit_lsn, flush_lsn
        );

        assert!(backup_lsn >= start_lsn);
        assert!(commit_lsn >= start_lsn);
        assert!(flush_lsn >= start_lsn);

        if request.until_lsn > flush_lsn {
            bail!(format!(
                "requested LSN {} is beyond the end of the timeline {}",
                request.until_lsn, flush_lsn
            ));
        }
        if request.until_lsn < start_lsn {
            bail!(format!(
                "requested LSN {} is before the start of the timeline {}",
                request.until_lsn, start_lsn
            ));
        }

        if request.until_lsn > commit_lsn {
            warn!("copy_timeline WAL is not fully committed");
        }

        if backup_lsn < request.until_lsn && request.until_lsn.0 - backup_lsn.0 > MAX_BACKUP_LAG {
            // we have a lot of segments that are not backed up. we can try to wait here until
            // segments will be backed up to remote storage, but it's not clear how long to wait
            bail!("too many segments are not backed up");
        }
    }

    let wal_seg_size = state.server.wal_seg_size as usize;
    if wal_seg_size == 0 {
        bail!("wal_seg_size is not set");
    }

    let first_segment = start_lsn.segment_number(wal_seg_size);
    let last_segment = request.until_lsn.segment_number(wal_seg_size);

    let new_backup_lsn = {
        // we can't have new backup_lsn greater than existing backup_lsn or start of the last segment
        let max_backup_lsn = backup_lsn.min(Lsn(last_segment * wal_seg_size as u64));

        if max_backup_lsn <= start_lsn {
            // probably we are starting from the first segment, which was not backed up yet.
            // note that start_lsn can be in the middle of the segment
            start_lsn
        } else {
            // we have some segments backed up, so we will assume all WAL below max_backup_lsn is backed up
            assert!(max_backup_lsn.segment_offset(wal_seg_size) == 0);
            max_backup_lsn
        }
    };

    // all previous segments will be copied inside S3
    let first_ondisk_segment = new_backup_lsn.segment_number(wal_seg_size);
    assert!(first_ondisk_segment <= last_segment);
    assert!(first_ondisk_segment >= first_segment);

    copy_s3_segments(
        wal_seg_size,
        &request.source_ttid,
        &request.destination_ttid,
        first_segment,
        first_ondisk_segment,
    )
    .await?;

    copy_disk_segments(
        &source_tli,
        wal_seg_size,
        new_backup_lsn,
        request.until_lsn,
        &tli_dir_path,
    )
    .await?;

    let mut new_state = TimelinePersistentState::new(
        &request.destination_ttid,
        Configuration::empty(),
        state.server.clone(),
        start_lsn,
        request.until_lsn,
    )?;
    new_state.timeline_start_lsn = start_lsn;
    new_state.peer_horizon_lsn = request.until_lsn;
    new_state.backup_lsn = new_backup_lsn;

    FileStorage::create_new(&tli_dir_path, new_state.clone(), conf.no_sync).await?;

    // now we have a ready timeline in a temp directory
    validate_temp_timeline(conf, request.destination_ttid, &tli_dir_path).await?;
    global_timelines
        .load_temp_timeline(request.destination_ttid, &tli_dir_path, true)
        .await?;

    Ok(())
}

async fn copy_disk_segments(
    tli: &WalResidentTimeline,
    wal_seg_size: usize,
    start_lsn: Lsn,
    end_lsn: Lsn,
    tli_dir_path: &Utf8PathBuf,
) -> Result<()> {
    let mut wal_reader = tli.get_walreader(start_lsn).await?;

    let mut buf = vec![0u8; MAX_SEND_SIZE];

    let first_segment = start_lsn.segment_number(wal_seg_size);
    let last_segment = end_lsn.segment_number(wal_seg_size);

    for segment in first_segment..=last_segment {
        let segment_start = segment * wal_seg_size as u64;
        let segment_end = segment_start + wal_seg_size as u64;

        let copy_start = segment_start.max(start_lsn.0);
        let copy_end = segment_end.min(end_lsn.0);

        let copy_start = copy_start - segment_start;
        let copy_end = copy_end - segment_start;

        let wal_file_path = {
            let (normal, partial) = wal_file_paths(tli_dir_path, segment, wal_seg_size);

            if segment == last_segment {
                partial
            } else {
                normal
            }
        };

        write_segment(
            &mut buf,
            &wal_file_path,
            wal_seg_size as u64,
            copy_start,
            copy_end,
            &mut wal_reader,
        )
        .await?;
    }

    Ok(())
}

async fn write_segment(
    buf: &mut [u8],
    file_path: &Utf8PathBuf,
    wal_seg_size: u64,
    from: u64,
    to: u64,
    reader: &mut WalReader,
) -> Result<()> {
    assert!(from <= to);
    assert!(to <= wal_seg_size);

    #[allow(clippy::suspicious_open_options)]
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&file_path)
        .await?;

    // maybe fill with zeros, as in wal_storage.rs?
    file.set_len(wal_seg_size).await?;
    file.seek(std::io::SeekFrom::Start(from)).await?;

    let mut bytes_left = to - from;
    while bytes_left > 0 {
        let len = bytes_left as usize;
        let len = len.min(buf.len());
        let len = reader.read(&mut buf[..len]).await?;
        file.write_all(&buf[..len]).await?;
        bytes_left -= len as u64;
    }

    file.flush().await?;
    file.sync_all().await?;
    Ok(())
}
