use std::{
    collections::HashMap,
    num::{NonZeroU64, NonZeroUsize},
    time::SystemTime,
};

use byteorder::{BigEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use strum_macros;
use utils::{
    history_buffer::HistoryBufferWithDropCounter,
    id::{NodeId, TenantId, TimelineId},
    lsn::Lsn,
};

use crate::reltag::RelTag;
use anyhow::bail;
use bytes::{BufMut, Bytes, BytesMut};

/// The state of a tenant in this pageserver.
///
/// ```mermaid
/// stateDiagram-v2
///
///     [*] --> Loading: spawn_load()
///     [*] --> Attaching: spawn_attach()
///
///     Loading --> Activating: activate()
///     Attaching --> Activating: activate()
///     Activating --> Active: infallible
///
///     Loading --> Broken: load() failure
///     Attaching --> Broken: attach() failure
///
///     Active --> Stopping: set_stopping(), part of shutdown & detach
///     Stopping --> Broken: late error in remove_tenant_from_memory
///
///     Broken --> [*]: ignore / detach / shutdown
///     Stopping --> [*]: remove_from_memory complete
///
///     Active --> Broken: cfg(testing)-only tenant break point
/// ```
#[derive(
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    strum_macros::Display,
    strum_macros::EnumVariantNames,
    strum_macros::AsRefStr,
    strum_macros::IntoStaticStr,
)]
#[serde(tag = "slug", content = "data")]
pub enum TenantState {
    /// This tenant is being loaded from local disk.
    ///
    /// `set_stopping()` and `set_broken()` do not work in this state and wait for it to pass.
    Loading,
    /// This tenant is being attached to the pageserver.
    ///
    /// `set_stopping()` and `set_broken()` do not work in this state and wait for it to pass.
    Attaching,
    /// The tenant is transitioning from Loading/Attaching to Active.
    ///
    /// While in this state, the individual timelines are being activated.
    ///
    /// `set_stopping()` and `set_broken()` do not work in this state and wait for it to pass.
    Activating(ActivatingFrom),
    /// The tenant has finished activating and is open for business.
    ///
    /// Transitions out of this state are possible through `set_stopping()` and `set_broken()`.
    Active,
    /// The tenant is recognized by pageserver, but it is being detached or the
    /// system is being shut down.
    ///
    /// Transitions out of this state are possible through `set_broken()`.
    Stopping,
    /// The tenant is recognized by the pageserver, but can no longer be used for
    /// any operations.
    ///
    /// If the tenant fails to load or attach, it will transition to this state
    /// and it is guaranteed that no background tasks are running in its name.
    ///
    /// The other way to transition into this state is from `Stopping` state
    /// through `set_broken()` called from `remove_tenant_from_memory()`. That happens
    /// if the cleanup future executed by `remove_tenant_from_memory()` fails.
    Broken { reason: String, backtrace: String },
}

impl TenantState {
    pub fn attachment_status(&self) -> TenantAttachmentStatus {
        use TenantAttachmentStatus::*;

        // Below TenantState::Activating is used as "transient" or "transparent" state for
        // attachment_status determining.
        match self {
            // The attach procedure writes the marker file before adding the Attaching tenant to the tenants map.
            // So, technically, we can return Attached here.
            // However, as soon as Console observes Attached, it will proceed with the Postgres-level health check.
            // But, our attach task might still be fetching the remote timelines, etc.
            // So, return `Maybe` while Attaching, making Console wait for the attach task to finish.
            Self::Attaching | Self::Activating(ActivatingFrom::Attaching) => Maybe,
            // tenant mgr startup distinguishes attaching from loading via marker file.
            // If it's loading, there is no attach marker file, i.e., attach had finished in the past.
            Self::Loading | Self::Activating(ActivatingFrom::Loading) => Attached,
            // We only reach Active after successful load / attach.
            // So, call atttachment status Attached.
            Self::Active => Attached,
            // If the (initial or resumed) attach procedure fails, the tenant becomes Broken.
            // However, it also becomes Broken if the regular load fails.
            // From Console's perspective there's no practical difference
            // because attachment_status is polled by console only during attach operation execution.
            Self::Broken { reason, .. } => Failed {
                reason: reason.to_owned(),
            },
            // Why is Stopping a Maybe case? Because, during pageserver shutdown,
            // we set the Stopping state irrespective of whether the tenant
            // has finished attaching or not.
            Self::Stopping => Maybe,
        }
    }

    pub fn broken_from_reason(reason: String) -> Self {
        let backtrace_str: String = format!("{}", std::backtrace::Backtrace::force_capture());
        Self::Broken {
            reason,
            backtrace: backtrace_str,
        }
    }
}

impl std::fmt::Debug for TenantState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Broken { reason, backtrace } if !reason.is_empty() => {
                write!(f, "Broken due to: {reason}. Backtrace:\n{backtrace}")
            }
            _ => write!(f, "{self}"),
        }
    }
}

/// The only [`TenantState`] variants we could be `TenantState::Activating` from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ActivatingFrom {
    /// Arrived to [`TenantState::Activating`] from [`TenantState::Loading`]
    Loading,
    /// Arrived to [`TenantState::Activating`] from [`TenantState::Attaching`]
    Attaching,
}

/// A state of a timeline in pageserver's memory.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimelineState {
    /// The timeline is recognized by the pageserver but is not yet operational.
    /// In particular, the walreceiver connection loop is not running for this timeline.
    /// It will eventually transition to state Active or Broken.
    Loading,
    /// The timeline is fully operational.
    /// It can be queried, and the walreceiver connection loop is running.
    Active,
    /// The timeline was previously Loading or Active but is shutting down.
    /// It cannot transition back into any other state.
    Stopping,
    /// The timeline is broken and not operational (previous states: Loading or Active).
    Broken { reason: String, backtrace: String },
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde_as(as = "DisplayFromStr")]
    pub new_timeline_id: TimelineId,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_timeline_id: Option<TimelineId>,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_start_lsn: Option<Lsn>,
    pub pg_version: Option<u32>,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantCreateRequest {
    #[serde_as(as = "DisplayFromStr")]
    pub new_tenant_id: TenantId,
    #[serde(flatten)]
    pub config: TenantConfig, // as we have a flattened field, we should reject all unknown fields in it
}

impl std::ops::Deref for TenantCreateRequest {
    type Target = TenantConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TenantConfig {
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
    // We defer the parsing of the eviction_policy field to the request handler.
    // Otherwise we'd have to move the types for eviction policy into this package.
    // We might do that once the eviction feature has stabilizied.
    // For now, this field is not even documented in the openapi_spec.yml.
    pub eviction_policy: Option<serde_json::Value>,
    pub min_resident_size_override: Option<u64>,
    pub evictions_low_residence_duration_metric_threshold: Option<String>,
    pub gc_feedback: Option<bool>,
    pub master_region: Option<String>,
    pub master_broker_endpoint: Option<String>,
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
    pub fn new(new_tenant_id: TenantId) -> TenantCreateRequest {
        TenantCreateRequest {
            new_tenant_id,
            config: TenantConfig::default(),
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantConfigRequest {
    #[serde_as(as = "DisplayFromStr")]
    pub tenant_id: TenantId,
    #[serde(flatten)]
    pub config: TenantConfig, // as we have a flattened field, we should reject all unknown fields in it
}

impl std::ops::Deref for TenantConfigRequest {
    type Target = TenantConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl TenantConfigRequest {
    pub fn new(tenant_id: TenantId) -> TenantConfigRequest {
        let config = TenantConfig {
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
            eviction_policy: None,
            min_resident_size_override: None,
            evictions_low_residence_duration_metric_threshold: None,
            gc_feedback: None,
            master_region: None,
            master_broker_endpoint: None,
        };
        TenantConfigRequest { tenant_id, config }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TenantAttachRequest {
    pub config: TenantAttachConfig,
}

/// Newtype to enforce deny_unknown_fields on TenantConfig for
/// its usage inside `TenantAttachRequest`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantAttachConfig {
    #[serde(flatten)]
    allowing_unknown_fields: TenantConfig,
}

impl std::ops::Deref for TenantAttachConfig {
    type Target = TenantConfig;

    fn deref(&self) -> &Self::Target {
        &self.allowing_unknown_fields
    }
}

/// See [`TenantState::attachment_status`] and the OpenAPI docs for context.
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "slug", content = "data", rename_all = "snake_case")]
pub enum TenantAttachmentStatus {
    Maybe,
    Attached,
    Failed { reason: String },
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
pub struct TenantInfo {
    #[serde_as(as = "DisplayFromStr")]
    pub id: TenantId,
    // NB: intentionally not part of OpenAPI, we don't want to commit to a specific set of TenantState's
    pub state: TenantState,
    /// Sum of the size of all layer files.
    /// If a layer is present in both local FS and S3, it counts only once.
    pub current_physical_size: Option<u64>, // physical size is only included in `tenant_status` endpoint
    pub attachment_status: TenantAttachmentStatus,
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

#[derive(Debug, Clone, Serialize)]
pub struct LayerMapInfo {
    pub in_memory_layers: Vec<InMemoryLayerInfo>,
    pub historic_layers: Vec<HistoricLayerInfo>,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, enum_map::Enum)]
#[repr(usize)]
pub enum LayerAccessKind {
    GetValueReconstructData,
    Iter,
    KeyIter,
    Dump,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerAccessStatFullDetails {
    pub when_millis_since_epoch: u64,
    pub task_kind: &'static str,
    pub access_kind: LayerAccessKind,
}

/// An event that impacts the layer's residence status.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerResidenceEvent {
    /// The time when the event occurred.
    /// NB: this timestamp is captured while the residence status changes.
    /// So, it might be behind/ahead of the actual residence change by a short amount of time.
    ///
    #[serde(rename = "timestamp_millis_since_epoch")]
    #[serde_as(as = "serde_with::TimestampMilliSeconds")]
    pub timestamp: SystemTime,
    /// The new residence status of the layer.
    pub status: LayerResidenceStatus,
    /// The reason why we had to record this event.
    pub reason: LayerResidenceEventReason,
}

/// The reason for recording a given [`ResidenceEvent`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LayerResidenceEventReason {
    /// The layer map is being populated, e.g. during timeline load or attach.
    /// This includes [`RemoteLayer`] objects created in [`reconcile_with_remote`].
    /// We need to record such events because there is no persistent storage for the events.
    LayerLoad,
    /// We just created the layer (e.g., freeze_and_flush or compaction).
    /// Such layers are always [`LayerResidenceStatus::Resident`].
    LayerCreate,
    /// We on-demand downloaded or evicted the given layer.
    ResidenceChange,
}

/// The residence status of the layer, after the given [`LayerResidenceEvent`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LayerResidenceStatus {
    /// Residence status for a layer file that exists locally.
    /// It may also exist on the remote, we don't care here.
    Resident,
    /// Residence status for a layer file that only exists on the remote.
    Evicted,
}

impl LayerResidenceEvent {
    pub fn new(status: LayerResidenceStatus, reason: LayerResidenceEventReason) -> Self {
        Self {
            status,
            reason,
            timestamp: SystemTime::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LayerAccessStats {
    pub access_count_by_access_kind: HashMap<LayerAccessKind, u64>,
    pub task_kind_access_flag: Vec<&'static str>,
    pub first: Option<LayerAccessStatFullDetails>,
    pub accesses_history: HistoryBufferWithDropCounter<LayerAccessStatFullDetails, 16>,
    pub residence_events_history: HistoryBufferWithDropCounter<LayerResidenceEvent, 16>,
}

#[serde_as]
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind")]
pub enum InMemoryLayerInfo {
    Open {
        #[serde_as(as = "DisplayFromStr")]
        lsn_start: Lsn,
    },
    Frozen {
        #[serde_as(as = "DisplayFromStr")]
        lsn_start: Lsn,
        #[serde_as(as = "DisplayFromStr")]
        lsn_end: Lsn,
    },
}

#[serde_as]
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind")]
pub enum HistoricLayerInfo {
    Delta {
        layer_file_name: String,
        layer_file_size: u64,

        #[serde_as(as = "DisplayFromStr")]
        lsn_start: Lsn,
        #[serde_as(as = "DisplayFromStr")]
        lsn_end: Lsn,
        remote: bool,
        access_stats: LayerAccessStats,
    },
    Image {
        layer_file_name: String,
        layer_file_size: u64,

        #[serde_as(as = "DisplayFromStr")]
        lsn_start: Lsn,
        remote: bool,
        access_stats: LayerAccessStats,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadRemoteLayersTaskSpawnRequest {
    pub max_concurrent_downloads: NonZeroUsize,
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
#[derive(PartialEq, Eq, Debug)]
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
    use serde_json::json;

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

    #[test]
    fn test_tenantinfo_serde() {
        // Test serialization/deserialization of TenantInfo
        let original_active = TenantInfo {
            id: TenantId::generate(),
            state: TenantState::Active,
            current_physical_size: Some(42),
            attachment_status: TenantAttachmentStatus::Attached,
        };
        let expected_active = json!({
            "id": original_active.id.to_string(),
            "state": {
                "slug": "Active",
            },
            "current_physical_size": 42,
            "attachment_status": {
                "slug":"attached",
            }
        });

        let original_broken = TenantInfo {
            id: TenantId::generate(),
            state: TenantState::Broken {
                reason: "reason".into(),
                backtrace: "backtrace info".into(),
            },
            current_physical_size: Some(42),
            attachment_status: TenantAttachmentStatus::Attached,
        };
        let expected_broken = json!({
            "id": original_broken.id.to_string(),
            "state": {
                "slug": "Broken",
                "data": {
                    "backtrace": "backtrace info",
                    "reason": "reason",
                }
            },
            "current_physical_size": 42,
            "attachment_status": {
                "slug":"attached",
            }
        });

        assert_eq!(
            serde_json::to_value(&original_active).unwrap(),
            expected_active
        );

        assert_eq!(
            serde_json::to_value(&original_broken).unwrap(),
            expected_broken
        );
        assert!(format!("{:?}", &original_broken.state).contains("reason"));
        assert!(format!("{:?}", &original_broken.state).contains("backtrace info"));
    }

    #[test]
    fn test_reject_unknown_field() {
        let id = TenantId::generate();
        let create_request = json!({
            "new_tenant_id": id.to_string(),
            "unknown_field": "unknown_value".to_string(),
        });
        let err = serde_json::from_value::<TenantCreateRequest>(create_request).unwrap_err();
        assert!(
            err.to_string().contains("unknown field `unknown_field`"),
            "expect unknown field `unknown_field` error, got: {}",
            err
        );

        let id = TenantId::generate();
        let config_request = json!({
            "tenant_id": id.to_string(),
            "unknown_field": "unknown_value".to_string(),
        });
        let err = serde_json::from_value::<TenantConfigRequest>(config_request).unwrap_err();
        assert!(
            err.to_string().contains("unknown field `unknown_field`"),
            "expect unknown field `unknown_field` error, got: {}",
            err
        );

        let attach_request = json!({
            "config": {
                "unknown_field": "unknown_value".to_string(),
            },
        });
        let err = serde_json::from_value::<TenantAttachRequest>(attach_request).unwrap_err();
        assert!(
            err.to_string().contains("unknown field `unknown_field`"),
            "expect unknown field `unknown_field` error, got: {}",
            err
        );
    }

    #[test]
    fn tenantstatus_activating_serde() {
        let states = [
            TenantState::Activating(ActivatingFrom::Loading),
            TenantState::Activating(ActivatingFrom::Attaching),
        ];
        let expected = "[{\"slug\":\"Activating\",\"data\":\"Loading\"},{\"slug\":\"Activating\",\"data\":\"Attaching\"}]";

        let actual = serde_json::to_string(&states).unwrap();

        assert_eq!(actual, expected);

        let parsed = serde_json::from_str::<Vec<TenantState>>(&actual).unwrap();

        assert_eq!(states.as_slice(), &parsed);
    }

    #[test]
    fn tenantstatus_activating_strum() {
        // tests added, because we use these for metrics
        let examples = [
            (line!(), TenantState::Loading, "Loading"),
            (line!(), TenantState::Attaching, "Attaching"),
            (
                line!(),
                TenantState::Activating(ActivatingFrom::Loading),
                "Activating",
            ),
            (
                line!(),
                TenantState::Activating(ActivatingFrom::Attaching),
                "Activating",
            ),
            (line!(), TenantState::Active, "Active"),
            (line!(), TenantState::Stopping, "Stopping"),
            (
                line!(),
                TenantState::Broken {
                    reason: "Example".into(),
                    backtrace: "Looooong backtrace".into(),
                },
                "Broken",
            ),
        ];

        for (line, rendered, expected) in examples {
            let actual: &'static str = rendered.into();
            assert_eq!(actual, expected, "example on {line}");
        }
    }
}
