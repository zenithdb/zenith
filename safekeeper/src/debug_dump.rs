//! Utils for dumping full state of the safekeeper.

use std::fs;
use std::fs::DirEntry;
use std::io::BufReader;
use std::io::Read;

use anyhow::Result;
use chrono::{DateTime, Utc};
use postgres_ffi::XLogSegNo;
use serde::Serialize;

use utils::http::json::display_serialize;
use utils::id::TenantTimelineId;
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

use crate::safekeeper::SafeKeeperState;
use crate::safekeeper::SafekeeperMemState;
use crate::safekeeper::TermHistory;

use crate::timeline::ReplicaState;
use crate::GlobalTimelines;

/// Various filters that influence the resulting JSON output.
#[derive(Debug, Serialize)]
pub struct Args {
    /// Dump all available safekeeper state. False by default.
    pub dump_all: bool,

    /// Dump control_file content. Uses value of `dump_all` by default.
    pub dump_control_file: bool,

    /// Dump in-memory state. Uses value of `dump_all` by default.
    pub dump_memory: bool,

    /// Dump all disk files in a timeline directory. Uses value of `dump_all` by default.
    pub dump_disk_content: bool,

    /// Dump full term history. True by default.
    pub dump_term_history: bool,

    /// Filter timelines by tenant_id.
    pub tenant_id: Option<TenantId>,

    /// Filter timelines by timeline_id.
    pub timeline_id: Option<TimelineId>,
}

/// Response for debug dump request.
#[derive(Debug, Serialize)]
pub struct Response {
    pub start_time: DateTime<Utc>,
    pub finish_time: DateTime<Utc>,
    pub timelines: Vec<Timeline>,
    pub timelines_count: usize,
}

#[derive(Debug, Serialize)]
pub struct Timeline {
    #[serde(serialize_with = "display_serialize")]
    pub tenant_id: TenantId,
    #[serde(serialize_with = "display_serialize")]
    pub timeline_id: TimelineId,
    pub control_file: Option<SafeKeeperState>,
    pub memory: Option<Memory>,
    pub disk_content: Option<DiskContent>,
}

#[derive(Debug, Serialize)]
pub struct Memory {
    pub is_cancelled: bool,
    pub peers_info_len: usize,
    pub replicas: Vec<Option<ReplicaState>>,
    pub wal_backup_active: bool,
    pub active: bool,
    pub num_computes: u32,
    pub last_removed_segno: XLogSegNo,
    pub epoch_start_lsn: Lsn,
    pub mem_state: SafekeeperMemState,

    // PhysicalStorage state.
    pub write_lsn: Lsn,
    pub write_record_lsn: Lsn,
    pub flush_lsn: Lsn,
    pub file_open: bool,
}

#[derive(Debug, Serialize)]
pub struct DiskContent {
    pub files: Vec<FileInfo>,
}

#[derive(Debug, Serialize)]
pub struct FileInfo {
    pub name: String,
    pub size: u64,
    pub created: DateTime<Utc>,
    pub modified: DateTime<Utc>,
    pub start_zeroes: u64,
    pub end_zeroes: u64,
    // TODO: add sha256 checksum
}

/// Build debug dump response, using the provided [`Args`] filters.
pub fn build(args: Args) -> Result<Response> {
    let start_time = Utc::now();
    let timelines_count = GlobalTimelines::timelines_count();

    let ptrs_snapshot = if args.tenant_id.is_some() && args.timeline_id.is_some() {
        // If both tenant_id and timeline_id are specified, we can just get the
        // timeline directly, without taking a snapshot of the whole list.
        let ttid = TenantTimelineId::new(args.tenant_id.unwrap(), args.timeline_id.unwrap());
        if let Ok(tli) = GlobalTimelines::get(ttid) {
            vec![tli]
        } else {
            vec![]
        }
    } else {
        // Otherwise, take a snapshot of the whole list.
        GlobalTimelines::get_all()
    };

    // TODO: return Stream instead of Vec
    let mut timelines = Vec::new();
    for tli in ptrs_snapshot {
        let ttid = tli.ttid;
        if let Some(tenant_id) = args.tenant_id {
            if tenant_id != ttid.tenant_id {
                continue;
            }
        }
        if let Some(timeline_id) = args.timeline_id {
            if timeline_id != ttid.timeline_id {
                continue;
            }
        }

        let control_file = if args.dump_control_file {
            let mut state = tli.get_state().1;
            if !args.dump_term_history {
                state.acceptor_state.term_history = TermHistory(vec![]);
            }
            Some(state)
        } else {
            None
        };

        let memory = if args.dump_memory {
            Some(tli.memory_dump())
        } else {
            None
        };

        let disk_content = if args.dump_disk_content {
            // build_disk_content can fail, but we don't want to fail the whole
            // request because of that.
            build_disk_content(&tli.timeline_dir).ok()
        } else {
            None
        };

        let timeline = Timeline {
            tenant_id: ttid.tenant_id,
            timeline_id: ttid.timeline_id,
            control_file,
            memory,
            disk_content,
        };
        timelines.push(timeline);
    }

    Ok(Response {
        start_time,
        finish_time: Utc::now(),
        timelines,
        timelines_count,
    })
}

/// Builds DiskContent from a directory path. It can fail if the directory
/// is deleted between the time we get the path and the time we try to open it.
fn build_disk_content(path: &std::path::Path) -> Result<DiskContent> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path)? {
        if entry.is_err() {
            continue;
        }
        let file = build_file_info(entry?);
        if file.is_err() {
            continue;
        }
        files.push(file?);
    }

    Ok(DiskContent { files })
}

/// Builds FileInfo from DirEntry. Sometimes it can return an error
/// if the file is deleted between the time we get the DirEntry
/// and the time we try to open it.
fn build_file_info(entry: DirEntry) -> Result<FileInfo> {
    let metadata = entry.metadata()?;
    let path = entry.path();
    let name = path
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("")
        .to_owned();
    let mut file = fs::File::open(path)?;
    let mut reader = BufReader::new(&mut file).bytes().filter_map(|x| x.ok());

    let start_zeroes = reader.by_ref().take_while(|&x| x == 0).count() as u64;
    let mut end_zeroes = 0;
    for b in reader {
        if b == 0 {
            end_zeroes += 1;
        } else {
            end_zeroes = 0;
        }
    }

    Ok(FileInfo {
        name,
        size: metadata.len(),
        created: DateTime::from(metadata.created()?),
        modified: DateTime::from(metadata.modified()?),
        start_zeroes,
        end_zeroes,
    })
}
