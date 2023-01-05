use std::num::NonZeroU64;

use byteorder::{BigEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use utils::{
    id::{NodeId, TenantId, TimelineId},
    lsn::Lsn,
};

use crate::reltag::RelTag;
use anyhow::bail;
use bytes::{BufMut, Bytes, BytesMut};

/// A state of a tenant in pageserver's memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TenantState {
    // This tenant is being loaded from local disk
    Loading,
    // This tenant is being downloaded from cloud storage.
    Attaching,
    /// Tenant is fully operational
    Active,
    /// A tenant is recognized by pageserver, but it is being detached or the
    /// system is being shut down.
    Stopping,
    /// A tenant is recognized by the pageserver, but can no longer be used for
    /// any operations, because it failed to be activated.
    Broken,
}

impl TenantState {
    pub fn has_in_progress_downloads(&self) -> bool {
        match self {
            Self::Loading => true,
            Self::Attaching => true,
            Self::Active => false,
            Self::Stopping => false,
            Self::Broken => false,
        }
    }
}

/// A state of a timeline in pageserver's memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimelineState {
    /// Timeline is fully operational. If the containing Tenant is Active, the timeline's
    /// background jobs are running otherwise they will be launched when the tenant is activated.
    Active,
    /// A timeline is recognized by pageserver, but not yet ready to operate.
    /// The status indicates, that the timeline could eventually go back to Active automatically:
    /// for example, if the owning tenant goes back to Active again.
    Suspended,
    /// A timeline is recognized by pageserver, but not yet ready to operate and not allowed to
    /// automatically become Active after certain events: only a management call can change this status.
    Stopping,
    /// A timeline is recognized by the pageserver, but can no longer be used for
    /// any operations, because it failed to be activated.
    Broken,
}

// It is impossible to specify timeline as an ancestor without also
// specifying the branching LSN.
#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TimelineAncestor {
    pub timeline_id: TimelineId,
    pub start_lsn: Lsn,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub new_timeline_id: Option<TimelineId>,

    // TODO: replace with Option<TimelineAncestor>. It does not make sense
    // to specify LSN without timeline, and specifying timeline without LSN
    // invokes leads to a race condition.
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_timeline_id: Option<TimelineId>,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_start_lsn: Option<Lsn>,

    pub pg_version: Option<u32>,
}

impl TimelineCreateRequest {
    pub fn ancestor(&self) -> anyhow::Result<Option<TimelineAncestor>> {
        match (self.ancestor_timeline_id, self.ancestor_start_lsn) {
            (None, None) => Ok(None),
            (Some(timeline_id), Some(start_lsn)) => Ok(Some(TimelineAncestor {
                timeline_id,
                start_lsn,
            })),
            (Some(_), None) => {
                bail!("ancestor_start_lsn is not specified, this is prone to race conditions")
            }
            (None, Some(_)) => {
                bail!("ancestor_start_lsn is specified without ancestor_timeline_id")
            }
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Default)]
pub struct TenantCreateRequest {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub new_tenant_id: Option<TenantId>,
    pub checkpoint_distance: Option<u64>,
    pub checkpoint_timeout: Option<String>,
    pub compaction_target_size: Option<u64>,
    pub compaction_period: Option<String>,
    pub compaction_threshold: Option<usize>,
    pub gc_horizon: Option<u64>,
    pub gc_period: Option<String>,
    pub image_creation_threshold: Option<usize>,
    pub pitr_interval: Option<String>,
    pub walreceiver_connect_timeout: Option<String>,
    pub lagging_wal_timeout: Option<String>,
    pub max_lsn_wal_lag: Option<NonZeroU64>,
    pub trace_read_requests: Option<bool>,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantCreateResponse(#[serde_as(as = "DisplayFromStr")] pub TenantId);

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: NodeId,
}

impl TenantCreateRequest {
    pub fn new(new_tenant_id: Option<TenantId>) -> TenantCreateRequest {
        TenantCreateRequest {
            new_tenant_id,
            ..Default::default()
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TenantConfigRequest {
    #[serde_as(as = "DisplayFromStr")]
    pub tenant_id: TenantId,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub checkpoint_distance: Option<u64>,
    pub checkpoint_timeout: Option<String>,
    pub compaction_target_size: Option<u64>,
    pub compaction_period: Option<String>,
    pub compaction_threshold: Option<usize>,
    pub gc_horizon: Option<u64>,
    pub gc_period: Option<String>,
    pub image_creation_threshold: Option<usize>,
    pub pitr_interval: Option<String>,
    pub walreceiver_connect_timeout: Option<String>,
    pub lagging_wal_timeout: Option<String>,
    pub max_lsn_wal_lag: Option<NonZeroU64>,
    pub trace_read_requests: Option<bool>,
}

impl TenantConfigRequest {
    pub fn new(tenant_id: TenantId) -> TenantConfigRequest {
        TenantConfigRequest {
            tenant_id,
            checkpoint_distance: None,
            checkpoint_timeout: None,
            compaction_target_size: None,
            compaction_period: None,
            compaction_threshold: None,
            gc_horizon: None,
            gc_period: None,
            image_creation_threshold: None,
            pitr_interval: None,
            walreceiver_connect_timeout: None,
            lagging_wal_timeout: None,
            max_lsn_wal_lag: None,
            trace_read_requests: None,
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
pub struct TenantInfo {
    #[serde_as(as = "DisplayFromStr")]
    pub id: TenantId,
    pub state: TenantState,
    /// Sum of the size of all layer files.
    /// If a layer is present in both local FS and S3, it counts only once.
    pub current_physical_size: Option<u64>, // physical size is only included in `tenant_status` endpoint
    pub has_in_progress_downloads: Option<bool>,
}

/// This represents the output of the "timeline_detail" and "timeline_list" API calls.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimelineInfo {
    #[serde_as(as = "DisplayFromStr")]
    pub tenant_id: TenantId,
    #[serde_as(as = "DisplayFromStr")]
    pub timeline_id: TimelineId,

    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_timeline_id: Option<TimelineId>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_lsn: Option<Lsn>,
    #[serde_as(as = "DisplayFromStr")]
    pub last_record_lsn: Lsn,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub prev_record_lsn: Option<Lsn>,
    #[serde_as(as = "DisplayFromStr")]
    pub latest_gc_cutoff_lsn: Lsn,
    #[serde_as(as = "DisplayFromStr")]
    pub disk_consistent_lsn: Lsn,
    #[serde_as(as = "DisplayFromStr")]
    pub remote_consistent_lsn: Lsn,
    pub current_logical_size: Option<u64>, // is None when timeline is Unloaded
    /// Sum of the size of all layer files.
    /// If a layer is present in both local FS and S3, it counts only once.
    pub current_physical_size: Option<u64>, // is None when timeline is Unloaded
    pub current_logical_size_non_incremental: Option<u64>,

    pub timeline_dir_layer_file_size_sum: Option<u64>,

    pub wal_source_connstr: Option<String>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub last_received_msg_lsn: Option<Lsn>,
    /// the timestamp (in microseconds) of the last received message
    pub last_received_msg_ts: Option<u128>,
    pub pg_version: u32,

    pub state: TimelineState,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DownloadRemoteLayersTaskInfo {
    pub task_id: String,
    pub state: DownloadRemoteLayersTaskState,
    pub total_layer_count: u64,         // stable once `completed`
    pub successful_download_count: u64, // stable once `completed`
    pub failed_download_count: u64,     // stable once `completed`
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DownloadRemoteLayersTaskState {
    Running,
    Completed,
    ShutDown,
}

pub type ConfigureFailpointsRequest = Vec<FailpointConfig>;

/// Information for configuring a single fail point
#[derive(Debug, Serialize, Deserialize)]
pub struct FailpointConfig {
    /// Name of the fail point
    pub name: String,
    /// List of actions to take, using the format described in `fail::cfg`
    ///
    /// We also support `actions = "exit"` to cause the fail point to immediately exit.
    pub actions: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimelineGcRequest {
    pub gc_horizon: Option<u64>,
}

// Wrapped in libpq CopyData
#[derive(PartialEq, Eq)]
pub enum PagestreamFeMessage {
    Exists(PagestreamExistsRequest),
    Nblocks(PagestreamNblocksRequest),
    GetPage(PagestreamGetPageRequest),
    DbSize(PagestreamDbSizeRequest),
}

// Wrapped in libpq CopyData
pub enum PagestreamBeMessage {
    Exists(PagestreamExistsResponse),
    Nblocks(PagestreamNblocksResponse),
    GetPage(PagestreamGetPageResponse),
    Error(PagestreamErrorResponse),
    DbSize(PagestreamDbSizeResponse),
}

#[derive(Debug, PartialEq, Eq)]
pub struct PagestreamExistsRequest {
    pub latest: bool,
    pub lsn: Lsn,
    pub rel: RelTag,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PagestreamNblocksRequest {
    pub latest: bool,
    pub lsn: Lsn,
    pub rel: RelTag,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PagestreamGetPageRequest {
    pub latest: bool,
    pub lsn: Lsn,
    pub rel: RelTag,
    pub blkno: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PagestreamDbSizeRequest {
    pub latest: bool,
    pub lsn: Lsn,
    pub dbnode: u32,
}

#[derive(Debug)]
pub struct PagestreamExistsResponse {
    pub exists: bool,
}

#[derive(Debug)]
pub struct PagestreamNblocksResponse {
    pub n_blocks: u32,
}

#[derive(Debug)]
pub struct PagestreamGetPageResponse {
    pub page: Bytes,
}

#[derive(Debug)]
pub struct PagestreamErrorResponse {
    pub message: String,
}

#[derive(Debug)]
pub struct PagestreamDbSizeResponse {
    pub db_size: i64,
}

impl PagestreamFeMessage {
    pub fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        match self {
            Self::Exists(req) => {
                bytes.put_u8(0);
                bytes.put_u8(u8::from(req.latest));
                bytes.put_u64(req.lsn.0);
                bytes.put_u32(req.rel.spcnode);
                bytes.put_u32(req.rel.dbnode);
                bytes.put_u32(req.rel.relnode);
                bytes.put_u8(req.rel.forknum);
            }

            Self::Nblocks(req) => {
                bytes.put_u8(1);
                bytes.put_u8(u8::from(req.latest));
                bytes.put_u64(req.lsn.0);
                bytes.put_u32(req.rel.spcnode);
                bytes.put_u32(req.rel.dbnode);
                bytes.put_u32(req.rel.relnode);
                bytes.put_u8(req.rel.forknum);
            }

            Self::GetPage(req) => {
                bytes.put_u8(2);
                bytes.put_u8(u8::from(req.latest));
                bytes.put_u64(req.lsn.0);
                bytes.put_u32(req.rel.spcnode);
                bytes.put_u32(req.rel.dbnode);
                bytes.put_u32(req.rel.relnode);
                bytes.put_u8(req.rel.forknum);
                bytes.put_u32(req.blkno);
            }

            Self::DbSize(req) => {
                bytes.put_u8(3);
                bytes.put_u8(u8::from(req.latest));
                bytes.put_u64(req.lsn.0);
                bytes.put_u32(req.dbnode);
            }
        }

        bytes.into()
    }

    pub fn parse<R: std::io::Read>(body: &mut R) -> anyhow::Result<PagestreamFeMessage> {
        // TODO these gets can fail

        // these correspond to the NeonMessageTag enum in pagestore_client.h
        //
        // TODO: consider using protobuf or serde bincode for less error prone
        // serialization.
        let msg_tag = body.read_u8()?;
        match msg_tag {
            0 => Ok(PagestreamFeMessage::Exists(PagestreamExistsRequest {
                latest: body.read_u8()? != 0,
                lsn: Lsn::from(body.read_u64::<BigEndian>()?),
                rel: RelTag {
                    spcnode: body.read_u32::<BigEndian>()?,
                    dbnode: body.read_u32::<BigEndian>()?,
                    relnode: body.read_u32::<BigEndian>()?,
                    forknum: body.read_u8()?,
                },
            })),
            1 => Ok(PagestreamFeMessage::Nblocks(PagestreamNblocksRequest {
                latest: body.read_u8()? != 0,
                lsn: Lsn::from(body.read_u64::<BigEndian>()?),
                rel: RelTag {
                    spcnode: body.read_u32::<BigEndian>()?,
                    dbnode: body.read_u32::<BigEndian>()?,
                    relnode: body.read_u32::<BigEndian>()?,
                    forknum: body.read_u8()?,
                },
            })),
            2 => Ok(PagestreamFeMessage::GetPage(PagestreamGetPageRequest {
                latest: body.read_u8()? != 0,
                lsn: Lsn::from(body.read_u64::<BigEndian>()?),
                rel: RelTag {
                    spcnode: body.read_u32::<BigEndian>()?,
                    dbnode: body.read_u32::<BigEndian>()?,
                    relnode: body.read_u32::<BigEndian>()?,
                    forknum: body.read_u8()?,
                },
                blkno: body.read_u32::<BigEndian>()?,
            })),
            3 => Ok(PagestreamFeMessage::DbSize(PagestreamDbSizeRequest {
                latest: body.read_u8()? != 0,
                lsn: Lsn::from(body.read_u64::<BigEndian>()?),
                dbnode: body.read_u32::<BigEndian>()?,
            })),
            _ => bail!("unknown smgr message tag: {:?}", msg_tag),
        }
    }
}

impl PagestreamBeMessage {
    pub fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        match self {
            Self::Exists(resp) => {
                bytes.put_u8(100); /* tag from pagestore_client.h */
                bytes.put_u8(resp.exists as u8);
            }

            Self::Nblocks(resp) => {
                bytes.put_u8(101); /* tag from pagestore_client.h */
                bytes.put_u32(resp.n_blocks);
            }

            Self::GetPage(resp) => {
                bytes.put_u8(102); /* tag from pagestore_client.h */
                bytes.put(&resp.page[..]);
            }

            Self::Error(resp) => {
                bytes.put_u8(103); /* tag from pagestore_client.h */
                bytes.put(resp.message.as_bytes());
                bytes.put_u8(0); // null terminator
            }
            Self::DbSize(resp) => {
                bytes.put_u8(104); /* tag from pagestore_client.h */
                bytes.put_i64(resp.db_size);
            }
        }

        bytes.into()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use super::*;

    #[test]
    fn test_pagestream() {
        // Test serialization/deserialization of PagestreamFeMessage
        let messages = vec![
            PagestreamFeMessage::Exists(PagestreamExistsRequest {
                latest: true,
                lsn: Lsn(4),
                rel: RelTag {
                    forknum: 1,
                    spcnode: 2,
                    dbnode: 3,
                    relnode: 4,
                },
            }),
            PagestreamFeMessage::Nblocks(PagestreamNblocksRequest {
                latest: false,
                lsn: Lsn(4),
                rel: RelTag {
                    forknum: 1,
                    spcnode: 2,
                    dbnode: 3,
                    relnode: 4,
                },
            }),
            PagestreamFeMessage::GetPage(PagestreamGetPageRequest {
                latest: true,
                lsn: Lsn(4),
                rel: RelTag {
                    forknum: 1,
                    spcnode: 2,
                    dbnode: 3,
                    relnode: 4,
                },
                blkno: 7,
            }),
            PagestreamFeMessage::DbSize(PagestreamDbSizeRequest {
                latest: true,
                lsn: Lsn(4),
                dbnode: 7,
            }),
        ];
        for msg in messages {
            let bytes = msg.serialize();
            let reconstructed = PagestreamFeMessage::parse(&mut bytes.reader()).unwrap();
            assert!(msg == reconstructed);
        }
    }
}
