pub mod partitioning;
pub mod utilization;

pub use utilization::PageserverUtilization;

use std::{
    borrow::Cow,
    collections::HashMap,
    io::{BufRead, Read},
    num::{NonZeroU64, NonZeroUsize},
    time::{Duration, SystemTime},
};

use byteorder::{BigEndian, ReadBytesExt};
use postgres_ffi::BLCKSZ;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use utils::{
    completion,
    history_buffer::HistoryBufferWithDropCounter,
    id::{NodeId, TenantId, TimelineId},
    lsn::Lsn,
    serde_system_time,
};

use crate::controller_api::PlacementPolicy;
use crate::{
    reltag::RelTag,
    shard::{ShardCount, ShardStripeSize, TenantShardId},
};
use anyhow::bail;
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    Stopping {
        // Because of https://github.com/serde-rs/serde/issues/2105 this has to be a named field,
        // otherwise it will not be skipped during deserialization
        #[serde(skip)]
        progress: completion::Barrier,
    },
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
            Self::Stopping { .. } => Maybe,
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

#[derive(Serialize, Deserialize, Clone)]
pub struct TimelineCreateRequest {
    pub new_timeline_id: TimelineId,
    #[serde(default)]
    pub ancestor_timeline_id: Option<TimelineId>,
    #[serde(default)]
    pub existing_initdb_timeline_id: Option<TimelineId>,
    #[serde(default)]
    pub ancestor_start_lsn: Option<Lsn>,
    pub pg_version: Option<u32>,
}

#[derive(Serialize, Deserialize)]
pub struct TenantShardSplitRequest {
    pub new_shard_count: u8,

    // A tenant's stripe size is only meaningful the first time their shard count goes
    // above 1: therefore during a split from 1->N shards, we may modify the stripe size.
    //
    // If this is set while the stripe count is being increased from an already >1 value,
    // then the request will fail with 400.
    pub new_stripe_size: Option<ShardStripeSize>,
}

#[derive(Serialize, Deserialize)]
pub struct TenantShardSplitResponse {
    pub new_shards: Vec<TenantShardId>,
}

/// Parameters that apply to all shards in a tenant.  Used during tenant creation.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ShardParameters {
    pub count: ShardCount,
    pub stripe_size: ShardStripeSize,
}

impl ShardParameters {
    pub const DEFAULT_STRIPE_SIZE: ShardStripeSize = ShardStripeSize(256 * 1024 / 8);

    pub fn is_unsharded(&self) -> bool {
        self.count.is_unsharded()
    }
}

impl Default for ShardParameters {
    fn default() -> Self {
        Self {
            count: ShardCount::new(0),
            stripe_size: Self::DEFAULT_STRIPE_SIZE,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantCreateRequest {
    pub new_tenant_id: TenantShardId,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u32>,

    // If omitted, create a single shard with TenantShardId::unsharded()
    #[serde(default)]
    #[serde(skip_serializing_if = "ShardParameters::is_unsharded")]
    pub shard_parameters: ShardParameters,

    // This parameter is only meaningful in requests sent to the storage controller
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placement_policy: Option<PlacementPolicy>,

    #[serde(flatten)]
    pub config: TenantConfig, // as we have a flattened field, we should reject all unknown fields in it
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantLoadRequest {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u32>,
}

impl std::ops::Deref for TenantCreateRequest {
    type Target = TenantConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

/// An alternative representation of `pageserver::tenant::TenantConf` with
/// simpler types.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct TenantConfig {
    pub checkpoint_distance: Option<u64>,
    pub checkpoint_timeout: Option<String>,
    pub compaction_target_size: Option<u64>,
    pub compaction_period: Option<String>,
    pub compaction_threshold: Option<usize>,
    // defer parsing compaction_algorithm, like eviction_policy
    pub compaction_algorithm: Option<CompactionAlgorithm>,
    pub gc_horizon: Option<u64>,
    pub gc_period: Option<String>,
    pub image_creation_threshold: Option<usize>,
    pub pitr_interval: Option<String>,
    pub walreceiver_connect_timeout: Option<String>,
    pub lagging_wal_timeout: Option<String>,
    pub max_lsn_wal_lag: Option<NonZeroU64>,
    pub trace_read_requests: Option<bool>,
    pub eviction_policy: Option<EvictionPolicy>,
    pub min_resident_size_override: Option<u64>,
    pub evictions_low_residence_duration_metric_threshold: Option<String>,
    pub heatmap_period: Option<String>,
    pub lazy_slru_download: Option<bool>,
    pub timeline_get_throttle: Option<ThrottleConfig>,
    pub image_layer_creation_check_threshold: Option<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum EvictionPolicy {
    NoEviction,
    LayerAccessThreshold(EvictionPolicyLayerAccessThreshold),
    OnlyImitiate(EvictionPolicyLayerAccessThreshold),
}

impl EvictionPolicy {
    pub fn discriminant_str(&self) -> &'static str {
        match self {
            EvictionPolicy::NoEviction => "NoEviction",
            EvictionPolicy::LayerAccessThreshold(_) => "LayerAccessThreshold",
            EvictionPolicy::OnlyImitiate(_) => "OnlyImitiate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum CompactionAlgorithm {
    Legacy,
    Tiered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictionPolicyLayerAccessThreshold {
    #[serde(with = "humantime_serde")]
    pub period: Duration,
    #[serde(with = "humantime_serde")]
    pub threshold: Duration,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ThrottleConfig {
    pub task_kinds: Vec<String>, // TaskKind
    pub initial: usize,
    #[serde(with = "humantime_serde")]
    pub refill_interval: Duration,
    pub refill_amount: NonZeroUsize,
    pub max: usize,
    pub fair: bool,
}

impl ThrottleConfig {
    pub fn disabled() -> Self {
        Self {
            task_kinds: vec![], // effectively disables the throttle
            // other values don't matter with emtpy `task_kinds`.
            initial: 0,
            refill_interval: Duration::from_millis(1),
            refill_amount: NonZeroUsize::new(1).unwrap(),
            max: 1,
            fair: true,
        }
    }
    /// The requests per second allowed  by the given config.
    pub fn steady_rps(&self) -> f64 {
        (self.refill_amount.get() as f64) / (self.refill_interval.as_secs_f64())
    }
}

/// A flattened analog of a `pagesever::tenant::LocationMode`, which
/// lists out all possible states (and the virtual "Detached" state)
/// in a flat form rather than using rust-style enums.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
pub enum LocationConfigMode {
    AttachedSingle,
    AttachedMulti,
    AttachedStale,
    Secondary,
    Detached,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct LocationConfigSecondary {
    pub warm: bool,
}

/// An alternative representation of `pageserver::tenant::LocationConf`,
/// for use in external-facing APIs.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct LocationConfig {
    pub mode: LocationConfigMode,
    /// If attaching, in what generation?
    #[serde(default)]
    pub generation: Option<u32>,

    // If requesting mode `Secondary`, configuration for that.
    #[serde(default)]
    pub secondary_conf: Option<LocationConfigSecondary>,

    // Shard parameters: if shard_count is nonzero, then other shard_* fields
    // must be set accurately.
    #[serde(default)]
    pub shard_number: u8,
    #[serde(default)]
    pub shard_count: u8,
    #[serde(default)]
    pub shard_stripe_size: u32,

    // This configuration only affects attached mode, but should be provided irrespective
    // of the mode, as a secondary location might transition on startup if the response
    // to the `/re-attach` control plane API requests it.
    pub tenant_conf: TenantConfig,
}

#[derive(Serialize, Deserialize)]
pub struct LocationConfigListResponse {
    pub tenant_shards: Vec<(TenantShardId, Option<LocationConfig>)>,
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantCreateResponse(pub TenantId);

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: NodeId,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantLocationConfigRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantShardId>,
    #[serde(flatten)]
    pub config: LocationConfig, // as we have a flattened field, we should reject all unknown fields in it
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantTimeTravelRequest {
    pub shard_counts: Vec<ShardCount>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantShardLocation {
    pub shard_id: TenantShardId,
    pub node_id: NodeId,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantLocationConfigResponse {
    pub shards: Vec<TenantShardLocation>,
    // If the shards' ShardCount count is >1, stripe_size will be set.
    pub stripe_size: Option<ShardStripeSize>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantConfigRequest {
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
        let config = TenantConfig::default();
        TenantConfigRequest { tenant_id, config }
    }
}

#[derive(Debug, Deserialize)]
pub struct TenantAttachRequest {
    #[serde(default)]
    pub config: TenantAttachConfig,
    #[serde(default)]
    pub generation: Option<u32>,
}

/// Newtype to enforce deny_unknown_fields on TenantConfig for
/// its usage inside `TenantAttachRequest`.
#[derive(Debug, Serialize, Deserialize, Default)]
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

#[derive(Serialize, Deserialize, Clone)]
pub struct TenantInfo {
    pub id: TenantShardId,
    // NB: intentionally not part of OpenAPI, we don't want to commit to a specific set of TenantState's
    pub state: TenantState,
    /// Sum of the size of all layer files.
    /// If a layer is present in both local FS and S3, it counts only once.
    pub current_physical_size: Option<u64>, // physical size is only included in `tenant_status` endpoint
    pub attachment_status: TenantAttachmentStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TenantDetails {
    #[serde(flatten)]
    pub tenant_info: TenantInfo,

    pub walredo: Option<WalRedoManagerStatus>,

    pub timelines: Vec<TimelineId>,
}

/// This represents the output of the "timeline_detail" and "timeline_list" API calls.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimelineInfo {
    pub tenant_id: TenantShardId,
    pub timeline_id: TimelineId,

    pub ancestor_timeline_id: Option<TimelineId>,
    pub ancestor_lsn: Option<Lsn>,
    pub last_record_lsn: Lsn,
    pub prev_record_lsn: Option<Lsn>,
    pub latest_gc_cutoff_lsn: Lsn,
    pub disk_consistent_lsn: Lsn,

    /// The LSN that we have succesfully uploaded to remote storage
    pub remote_consistent_lsn: Lsn,

    /// The LSN that we are advertizing to safekeepers
    pub remote_consistent_lsn_visible: Lsn,

    /// The LSN from the start of the root timeline (never changes)
    pub initdb_lsn: Lsn,

    pub current_logical_size: u64,
    pub current_logical_size_is_accurate: bool,

    pub directory_entries_counts: Vec<u64>,

    /// Sum of the size of all layer files.
    /// If a layer is present in both local FS and S3, it counts only once.
    pub current_physical_size: Option<u64>, // is None when timeline is Unloaded
    pub current_logical_size_non_incremental: Option<u64>,

    pub timeline_dir_layer_file_size_sum: Option<u64>,

    pub wal_source_connstr: Option<String>,
    pub last_received_msg_lsn: Option<Lsn>,
    /// the timestamp (in microseconds) of the last received message
    pub last_received_msg_ts: Option<u128>,
    pub pg_version: u32,

    pub state: TimelineState,

    pub walreceiver_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub task_kind: Cow<'static, str>,
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

/// The reason for recording a given [`LayerResidenceEvent`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LayerResidenceEventReason {
    /// The layer map is being populated, e.g. during timeline load or attach.
    /// This includes [`RemoteLayer`] objects created in [`reconcile_with_remote`].
    /// We need to record such events because there is no persistent storage for the events.
    ///
    // https://github.com/rust-lang/rust/issues/74481
    /// [`RemoteLayer`]: ../../tenant/storage_layer/struct.RemoteLayer.html
    /// [`reconcile_with_remote`]: ../../tenant/struct.Timeline.html#method.reconcile_with_remote
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerAccessStats {
    pub access_count_by_access_kind: HashMap<LayerAccessKind, u64>,
    pub task_kind_access_flag: Vec<Cow<'static, str>>,
    pub first: Option<LayerAccessStatFullDetails>,
    pub accesses_history: HistoryBufferWithDropCounter<LayerAccessStatFullDetails, 16>,
    pub residence_events_history: HistoryBufferWithDropCounter<LayerResidenceEvent, 16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum InMemoryLayerInfo {
    Open { lsn_start: Lsn },
    Frozen { lsn_start: Lsn, lsn_end: Lsn },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum HistoricLayerInfo {
    Delta {
        layer_file_name: String,
        layer_file_size: u64,

        lsn_start: Lsn,
        lsn_end: Lsn,
        remote: bool,
        access_stats: LayerAccessStats,
    },
    Image {
        layer_file_name: String,
        layer_file_size: u64,

        lsn_start: Lsn,
        remote: bool,
        access_stats: LayerAccessStats,
    },
}

impl HistoricLayerInfo {
    pub fn layer_file_name(&self) -> &str {
        match self {
            HistoricLayerInfo::Delta {
                layer_file_name, ..
            } => layer_file_name,
            HistoricLayerInfo::Image {
                layer_file_name, ..
            } => layer_file_name,
        }
    }
    pub fn is_remote(&self) -> bool {
        match self {
            HistoricLayerInfo::Delta { remote, .. } => *remote,
            HistoricLayerInfo::Image { remote, .. } => *remote,
        }
    }
    pub fn set_remote(&mut self, value: bool) {
        let field = match self {
            HistoricLayerInfo::Delta { remote, .. } => remote,
            HistoricLayerInfo::Image { remote, .. } => remote,
        };
        *field = value;
    }
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

#[derive(Debug, Serialize, Deserialize)]
pub struct TimelineGcRequest {
    pub gc_horizon: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRedoManagerProcessStatus {
    pub pid: u32,
    /// The strum-generated `into::<&'static str>()` for `pageserver::walredo::ProcessKind`.
    /// `ProcessKind` are a transitory thing, so, they have no enum representation in `pageserver_api`.
    pub kind: Cow<'static, str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRedoManagerStatus {
    pub last_redo_at: Option<chrono::DateTime<chrono::Utc>>,
    pub process: Option<WalRedoManagerProcessStatus>,
}

/// The progress of a secondary tenant is mostly useful when doing a long running download: e.g. initiating
/// a download job, timing out while waiting for it to run, and then inspecting this status to understand
/// what's happening.
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct SecondaryProgress {
    /// The remote storage LastModified time of the heatmap object we last downloaded.
    pub heatmap_mtime: Option<serde_system_time::SystemTime>,

    /// The number of layers currently on-disk
    pub layers_downloaded: usize,
    /// The number of layers in the most recently seen heatmap
    pub layers_total: usize,

    /// The number of layer bytes currently on-disk
    pub bytes_downloaded: u64,
    /// The number of layer bytes in the most recently seen heatmap
    pub bytes_total: u64,
}

pub mod virtual_file {
    #[derive(
        Copy,
        Clone,
        PartialEq,
        Eq,
        Hash,
        strum_macros::EnumString,
        strum_macros::Display,
        serde_with::DeserializeFromStr,
        serde_with::SerializeDisplay,
        Debug,
    )]
    #[strum(serialize_all = "kebab-case")]
    pub enum IoEngineKind {
        StdFs,
        #[cfg(target_os = "linux")]
        TokioEpollUring,
    }
}

// Wrapped in libpq CopyData
#[derive(PartialEq, Eq, Debug)]
pub enum PagestreamFeMessage {
    Exists(PagestreamExistsRequest),
    Nblocks(PagestreamNblocksRequest),
    GetPage(PagestreamGetPageRequest),
    DbSize(PagestreamDbSizeRequest),
    GetSlruSegment(PagestreamGetSlruSegmentRequest),
}

// Wrapped in libpq CopyData
#[derive(strum_macros::EnumProperty)]
pub enum PagestreamBeMessage {
    Exists(PagestreamExistsResponse),
    Nblocks(PagestreamNblocksResponse),
    GetPage(PagestreamGetPageResponse),
    Error(PagestreamErrorResponse),
    DbSize(PagestreamDbSizeResponse),
    GetSlruSegment(PagestreamGetSlruSegmentResponse),
}

// Keep in sync with `pagestore_client.h`
#[repr(u8)]
enum PagestreamBeMessageTag {
    Exists = 100,
    Nblocks = 101,
    GetPage = 102,
    Error = 103,
    DbSize = 104,
    GetSlruSegment = 105,
}
impl TryFrom<u8> for PagestreamBeMessageTag {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            100 => Ok(PagestreamBeMessageTag::Exists),
            101 => Ok(PagestreamBeMessageTag::Nblocks),
            102 => Ok(PagestreamBeMessageTag::GetPage),
            103 => Ok(PagestreamBeMessageTag::Error),
            104 => Ok(PagestreamBeMessageTag::DbSize),
            105 => Ok(PagestreamBeMessageTag::GetSlruSegment),
            _ => Err(value),
        }
    }
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

#[derive(Debug, PartialEq, Eq)]
pub struct PagestreamGetSlruSegmentRequest {
    pub latest: bool,
    pub lsn: Lsn,
    pub kind: u8,
    pub segno: u32,
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
    pub rel: RelTag,
    pub blkno: u32,
    pub page: Bytes,
}

#[derive(Debug)]
pub struct PagestreamGetSlruSegmentResponse {
    pub segment: Bytes,
}

#[derive(Debug)]
pub struct PagestreamErrorResponse {
    pub message: String,
}

#[derive(Debug)]
pub struct PagestreamDbSizeResponse {
    pub db_size: i64,
}

// This is a cut-down version of TenantHistorySize from the pageserver crate, omitting fields
// that require pageserver-internal types.  It is sufficient to get the total size.
#[derive(Serialize, Deserialize, Debug)]
pub struct TenantHistorySize {
    pub id: TenantId,
    /// Size is a mixture of WAL and logical size, so the unit is bytes.
    ///
    /// Will be none if `?inputs_only=true` was given.
    pub size: Option<u64>,
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

            Self::GetSlruSegment(req) => {
                bytes.put_u8(4);
                bytes.put_u8(u8::from(req.latest));
                bytes.put_u64(req.lsn.0);
                bytes.put_u8(req.kind);
                bytes.put_u32(req.segno);
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
            4 => Ok(PagestreamFeMessage::GetSlruSegment(
                PagestreamGetSlruSegmentRequest {
                    latest: body.read_u8()? != 0,
                    lsn: Lsn::from(body.read_u64::<BigEndian>()?),
                    kind: body.read_u8()?,
                    segno: body.read_u32::<BigEndian>()?,
                },
            )),
            _ => bail!("unknown smgr message tag: {:?}", msg_tag),
        }
    }
}

impl PagestreamBeMessage {
    pub fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        use PagestreamBeMessageTag as Tag;
        match self {
            Self::Exists(resp) => {
                bytes.put_u8(Tag::Exists as u8);
                bytes.put_u8(resp.exists as u8);
            }

            Self::Nblocks(resp) => {
                bytes.put_u8(Tag::Nblocks as u8);
                bytes.put_u32(resp.n_blocks);
            }

            Self::GetPage(resp) => {
                bytes.put_u8(Tag::GetPage as u8);
                bytes.put_u32(resp.rel.spcnode);
                bytes.put_u32(resp.rel.dbnode);
                bytes.put_u32(resp.rel.relnode);
                bytes.put_u8(resp.rel.forknum);
                bytes.put_u32(resp.blkno);
                bytes.put(&resp.page[..]);
            }

            Self::Error(resp) => {
                bytes.put_u8(Tag::Error as u8);
                bytes.put(resp.message.as_bytes());
                bytes.put_u8(0); // null terminator
            }
            Self::DbSize(resp) => {
                bytes.put_u8(Tag::DbSize as u8);
                bytes.put_i64(resp.db_size);
            }

            Self::GetSlruSegment(resp) => {
                bytes.put_u8(Tag::GetSlruSegment as u8);
                bytes.put_u32((resp.segment.len() / BLCKSZ as usize) as u32);
                bytes.put(&resp.segment[..]);
            }
        }

        bytes.into()
    }

    pub fn deserialize(buf: Bytes) -> anyhow::Result<Self> {
        let mut buf = buf.reader();
        let msg_tag = buf.read_u8()?;

        use PagestreamBeMessageTag as Tag;
        let ok =
            match Tag::try_from(msg_tag).map_err(|tag: u8| anyhow::anyhow!("invalid tag {tag}"))? {
                Tag::Exists => {
                    let exists = buf.read_u8()?;
                    Self::Exists(PagestreamExistsResponse {
                        exists: exists != 0,
                    })
                }
                Tag::Nblocks => {
                    let n_blocks = buf.read_u32::<BigEndian>()?;
                    Self::Nblocks(PagestreamNblocksResponse { n_blocks })
                }
                Tag::GetPage => {
                    let rel = RelTag {
                        spcnode: buf.read_u32::<BigEndian>()?,
                        dbnode: buf.read_u32::<BigEndian>()?,
                        relnode: buf.read_u32::<BigEndian>()?,
                        forknum: buf.read_u8()?,
                    };
                    let blkno = buf.read_u32::<BigEndian>()?;
                    let mut page = vec![0; 8192]; // TODO: use MaybeUninit
                    buf.read_exact(&mut page)?;
                    PagestreamBeMessage::GetPage(PagestreamGetPageResponse {
                        rel,
                        blkno,
                        page: page.into(),
                    })
                }
                Tag::Error => {
                    let mut msg = Vec::new();
                    buf.read_until(0, &mut msg)?;
                    let cstring = std::ffi::CString::from_vec_with_nul(msg)?;
                    let rust_str = cstring.to_str()?;
                    PagestreamBeMessage::Error(PagestreamErrorResponse {
                        message: rust_str.to_owned(),
                    })
                }
                Tag::DbSize => {
                    let db_size = buf.read_i64::<BigEndian>()?;
                    Self::DbSize(PagestreamDbSizeResponse { db_size })
                }
                Tag::GetSlruSegment => {
                    let n_blocks = buf.read_u32::<BigEndian>()?;
                    let mut segment = vec![0; n_blocks as usize * BLCKSZ as usize];
                    buf.read_exact(&mut segment)?;
                    Self::GetSlruSegment(PagestreamGetSlruSegmentResponse {
                        segment: segment.into(),
                    })
                }
            };
        let remaining = buf.into_inner();
        if !remaining.is_empty() {
            anyhow::bail!(
                "remaining bytes in msg with tag={msg_tag}: {}",
                remaining.len()
            );
        }
        Ok(ok)
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Exists(_) => "Exists",
            Self::Nblocks(_) => "Nblocks",
            Self::GetPage(_) => "GetPage",
            Self::Error(_) => "Error",
            Self::DbSize(_) => "DbSize",
            Self::GetSlruSegment(_) => "GetSlruSegment",
        }
    }
}

#[cfg(test)]
mod tests {
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
            id: TenantShardId::unsharded(TenantId::generate()),
            state: TenantState::Active,
            current_physical_size: Some(42),
            attachment_status: TenantAttachmentStatus::Attached,
            generation: None,
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
            id: TenantShardId::unsharded(TenantId::generate()),
            state: TenantState::Broken {
                reason: "reason".into(),
                backtrace: "backtrace info".into(),
            },
            current_physical_size: Some(42),
            attachment_status: TenantAttachmentStatus::Attached,
            generation: None,
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
            (
                line!(),
                TenantState::Stopping {
                    progress: utils::completion::Barrier::default(),
                },
                "Stopping",
            ),
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
