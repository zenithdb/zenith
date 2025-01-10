//! Utils for dumping full state of the safekeeper.

use std::fs;
use std::fs::DirEntry;
use std::io::BufReader;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::bail;
use anyhow::Result;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use postgres_ffi::XLogSegNo;
use postgres_ffi::MAX_SEND_SIZE;
use safekeeper_api::models::WalSenderState;
use serde::Deserialize;
use serde::Serialize;

use postgres_ffi::v14::xlog_utils::{IsPartialXLogFileName, IsXLogFileName};
use sha2::{Digest, Sha256};
use utils::id::NodeId;
use utils::id::TenantTimelineId;
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

use crate::safekeeper::TermHistory;
use crate::state::TimelineMemState;
use crate::state::TimelinePersistentState;
use crate::timeline::get_timeline_dir;
use crate::timeline::WalResidentTimeline;
use crate::timeline_manager;
use crate::GlobalTimelines;
use crate::SafeKeeperConf;

/// Various filters that influence the resulting JSON output.
#[derive(Debug, Serialize, Deserialize, Clone)]
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

    /// Dump last modified time of WAL segments. Uses value of `dump_all` by default.
    pub dump_wal_last_modified: bool,

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
    pub timelines: Vec<TimelineDumpSer>,
    pub timelines_count: usize,
    pub config: Config,
}

pub struct TimelineDumpSer {
    pub tli: Arc<crate::timeline::Timeline>,
    pub args: Args,
    pub timeline_dir: Utf8PathBuf,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

impl std::fmt::Debug for TimelineDumpSer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimelineDumpSer")
            .field("tli", &self.tli.ttid)
            .field("args", &self.args)
            .finish()
    }
}

impl Serialize for TimelineDumpSer {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let dump = self.runtime.block_on(build_from_tli_dump(
            &self.tli,
            &self.args,
            &self.timeline_dir,
        ));
        dump.serialize(serializer)
    }
}

async fn build_from_tli_dump(
    timeline: &Arc<crate::timeline::Timeline>,
    args: &Args,
    timeline_dir: &Utf8Path,
) -> Timeline {
    let control_file = if args.dump_control_file {
        let mut state = timeline.get_state().await.1;
        if !args.dump_term_history {
            state.acceptor_state.term_history = TermHistory(vec![]);
        }
        Some(state)
    } else {
        None
    };

    let memory = if args.dump_memory {
        Some(timeline.memory_dump().await)
    } else {
        None
    };

    let disk_content = if args.dump_disk_content {
        // build_disk_content can fail, but we don't want to fail the whole
        // request because of that.
        // Note: timeline can be in offloaded state, this is not a problem.
        build_disk_content(timeline_dir).ok()
    } else {
        None
    };

    let wal_last_modified = if args.dump_wal_last_modified {
        get_wal_last_modified(timeline_dir).ok().flatten()
    } else {
        None
    };

    Timeline {
        tenant_id: timeline.ttid.tenant_id,
        timeline_id: timeline.ttid.timeline_id,
        control_file,
        memory,
        disk_content,
        wal_last_modified,
    }
}

/// Safekeeper configuration.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub id: NodeId,
    pub workdir: PathBuf,
    pub listen_pg_addr: String,
    pub listen_http_addr: String,
    pub no_sync: bool,
    pub max_offloader_lag_bytes: u64,
    pub wal_backup_enabled: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Timeline {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub control_file: Option<TimelinePersistentState>,
    pub memory: Option<Memory>,
    pub disk_content: Option<DiskContent>,
    pub wal_last_modified: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Memory {
    pub is_cancelled: bool,
    pub peers_info_len: usize,
    pub walsenders: Vec<WalSenderState>,
    pub wal_backup_active: bool,
    pub active: bool,
    pub num_computes: u32,
    pub last_removed_segno: XLogSegNo,
    pub epoch_start_lsn: Lsn,
    pub mem_state: TimelineMemState,
    pub mgr_status: timeline_manager::Status,

    // PhysicalStorage state.
    pub write_lsn: Lsn,
    pub write_record_lsn: Lsn,
    pub flush_lsn: Lsn,
    pub file_open: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DiskContent {
    pub files: Vec<FileInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
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
pub async fn build(args: Args, global_timelines: Arc<GlobalTimelines>) -> Result<Response> {
    let start_time = Utc::now();
    let timelines_count = global_timelines.timelines_count();
    let config = global_timelines.get_global_config();

    let ptrs_snapshot = if args.tenant_id.is_some() && args.timeline_id.is_some() {
        // If both tenant_id and timeline_id are specified, we can just get the
        // timeline directly, without taking a snapshot of the whole list.
        let ttid = TenantTimelineId::new(args.tenant_id.unwrap(), args.timeline_id.unwrap());
        if let Ok(tli) = global_timelines.get(ttid) {
            vec![tli]
        } else {
            vec![]
        }
    } else {
        // Otherwise, take a snapshot of the whole list.
        global_timelines.get_all()
    };

    let mut timelines = Vec::new();
    let runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    );
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

        timelines.push(TimelineDumpSer {
            tli,
            args: args.clone(),
            timeline_dir: get_timeline_dir(&config, &ttid),
            runtime: runtime.clone(),
        });
    }

    // Tokio forbids to drop runtime in async context, so this is a stupid way
    // to drop it in non async context.
    tokio::task::spawn_blocking(move || {
        let _r = runtime;
    })
    .await?;

    Ok(Response {
        start_time,
        finish_time: Utc::now(),
        timelines,
        timelines_count,
        config: build_config(config),
    })
}

/// Builds DiskContent from a directory path. It can fail if the directory
/// is deleted between the time we get the path and the time we try to open it.
fn build_disk_content(path: &Utf8Path) -> Result<DiskContent> {
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

/// Get highest modified time of WAL segments in the directory.
fn get_wal_last_modified(path: &Utf8Path) -> Result<Option<DateTime<Utc>>> {
    let mut res = None;
    for entry in fs::read_dir(path)? {
        if entry.is_err() {
            continue;
        }
        let entry = entry?;
        /* Ignore files that are not XLOG segments */
        let fname = entry.file_name();
        if !IsXLogFileName(&fname) && !IsPartialXLogFileName(&fname) {
            continue;
        }

        let metadata = entry.metadata()?;
        let modified: DateTime<Utc> = DateTime::from(metadata.modified()?);
        res = std::cmp::max(res, Some(modified));
    }
    Ok(res)
}

/// Converts SafeKeeperConf to Config, filtering out the fields that are not
/// supposed to be exposed.
fn build_config(config: Arc<SafeKeeperConf>) -> Config {
    Config {
        id: config.my_id,
        workdir: config.workdir.clone().into(),
        listen_pg_addr: config.listen_pg_addr.clone(),
        listen_http_addr: config.listen_http_addr.clone(),
        no_sync: config.no_sync,
        max_offloader_lag_bytes: config.max_offloader_lag_bytes,
        wal_backup_enabled: config.wal_backup_enabled,
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimelineDigestRequest {
    pub from_lsn: Lsn,
    pub until_lsn: Lsn,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimelineDigest {
    pub sha256: String,
}

pub async fn calculate_digest(
    tli: &WalResidentTimeline,
    request: TimelineDigestRequest,
) -> Result<TimelineDigest> {
    if request.from_lsn > request.until_lsn {
        bail!("from_lsn is greater than until_lsn");
    }

    let (_, persisted_state) = tli.get_state().await;
    if persisted_state.timeline_start_lsn > request.from_lsn {
        bail!("requested LSN is before the start of the timeline");
    }

    let mut wal_reader = tli.get_walreader(request.from_lsn).await?;

    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; MAX_SEND_SIZE];

    let mut bytes_left = (request.until_lsn.0 - request.from_lsn.0) as usize;
    while bytes_left > 0 {
        let bytes_to_read = std::cmp::min(buf.len(), bytes_left);
        let bytes_read = wal_reader.read(&mut buf[..bytes_to_read]).await?;
        if bytes_read == 0 {
            bail!("wal_reader.read returned 0 bytes");
        }
        hasher.update(&buf[..bytes_read]);
        bytes_left -= bytes_read;
    }

    let digest = hasher.finalize();
    let digest = hex::encode(digest);
    Ok(TimelineDigest { sha256: digest })
}
