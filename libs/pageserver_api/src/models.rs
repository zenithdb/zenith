pub mod detach_ancestor;
pub mod partitioning;
pub mod utilization;

#[cfg(feature = "testing")]
use camino::Utf8PathBuf;
pub use utilization::PageserverUtilization;

use core::ops::Range;
use std::{
    collections::HashMap,
    fmt::Display,
    io::{BufRead, Read},
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    str::FromStr,
    time::{Duration, SystemTime},
};

use byteorder::{BigEndian, ReadBytesExt};
use postgres_ffi::BLCKSZ;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::serde_as;
use utils::{
    completion,
    id::{NodeId, TenantId, TimelineId},
    lsn::Lsn,
    postgres_client::PostgresClientProtocol,
    serde_system_time,
};

use crate::{
    key::{CompactKey, Key},
    reltag::RelTag,
    shard::{ShardCount, ShardStripeSize, TenantShardId},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// The state of a tenant in this pageserver.
///
/// ```mermaid
/// stateDiagram-v2
///
///     [*] --> Attaching: spawn_attach()
///
///     Attaching --> Activating: activate()
///     Activating --> Active: infallible
///
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
    strum_macros::VariantNames,
    strum_macros::AsRefStr,
    strum_macros::IntoStaticStr,
)]
#[serde(tag = "slug", content = "data")]
pub enum TenantState {
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

/// A temporary lease to a specific lsn inside a timeline.
/// Access to the lsn is guaranteed by the pageserver until the expiration indicated by `valid_until`.
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LsnLease {
    #[serde_as(as = "SystemTimeAsRfc3339Millis")]
    pub valid_until: SystemTime,
}

serde_with::serde_conv!(
    SystemTimeAsRfc3339Millis,
    SystemTime,
    |time: &SystemTime| humantime::format_rfc3339_millis(*time).to_string(),
    |value: String| -> Result<_, humantime::TimestampError> { humantime::parse_rfc3339(&value) }
);

impl LsnLease {
    /// The default length for an explicit LSN lease request (10 minutes).
    pub const DEFAULT_LENGTH: Duration = Duration::from_secs(10 * 60);

    /// The default length for an implicit LSN lease granted during
    /// `get_lsn_by_timestamp` request (1 minutes).
    pub const DEFAULT_LENGTH_FOR_TS: Duration = Duration::from_secs(60);

    /// Checks whether the lease is expired.
    pub fn is_expired(&self, now: &SystemTime) -> bool {
        now > &self.valid_until
    }
}

/// The only [`TenantState`] variants we could be `TenantState::Activating` from.
///
/// XXX: We used to have more variants here, but now it's just one, which makes this rather
/// useless. Remove, once we've checked that there's no client code left that looks at this.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ActivatingFrom {
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

#[serde_with::serde_as]
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CompactLsnRange {
    pub start: Lsn,
    pub end: Lsn,
}

#[serde_with::serde_as]
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CompactKeyRange {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub start: Key,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub end: Key,
}

impl From<Range<Lsn>> for CompactLsnRange {
    fn from(range: Range<Lsn>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<Range<Key>> for CompactKeyRange {
    fn from(range: Range<Key>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<CompactLsnRange> for Range<Lsn> {
    fn from(range: CompactLsnRange) -> Self {
        range.start..range.end
    }
}

impl From<CompactKeyRange> for Range<Key> {
    fn from(range: CompactKeyRange) -> Self {
        range.start..range.end
    }
}

impl CompactLsnRange {
    pub fn above(lsn: Lsn) -> Self {
        Self {
            start: lsn,
            end: Lsn::MAX,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CompactInfoResponse {
    pub compact_key_range: Option<CompactKeyRange>,
    pub compact_lsn_range: Option<CompactLsnRange>,
    pub sub_compaction: bool,
    pub running: bool,
    pub job_id: usize,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TimelineCreateRequest {
    pub new_timeline_id: TimelineId,
    #[serde(flatten)]
    pub mode: TimelineCreateRequestMode,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum TimelineCreateRequestMode {
    Branch {
        ancestor_timeline_id: TimelineId,
        #[serde(default)]
        ancestor_start_lsn: Option<Lsn>,
        // TODO: cplane sets this, but, the branching code always
        // inherits the ancestor's pg_version. Earlier code wasn't
        // using a flattened enum, so, it was an accepted field, and
        // we continue to accept it by having it here.
        pg_version: Option<u32>,
    },
    ImportPgdata {
        import_pgdata: TimelineCreateRequestModeImportPgdata,
    },
    // NB: Bootstrap is all-optional, and thus the serde(untagged) will cause serde to stop at Bootstrap.
    // (serde picks the first matching enum variant, in declaration order).
    Bootstrap {
        #[serde(default)]
        existing_initdb_timeline_id: Option<TimelineId>,
        pg_version: Option<u32>,
    },
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TimelineCreateRequestModeImportPgdata {
    pub location: ImportPgdataLocation,
    pub idempotency_key: ImportPgdataIdempotencyKey,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ImportPgdataLocation {
    #[cfg(feature = "testing")]
    LocalFs { path: Utf8PathBuf },
    AwsS3 {
        region: String,
        bucket: String,
        /// A better name for this would be `prefix`; changing requires coordination with cplane.
        /// See <https://github.com/neondatabase/cloud/issues/20646>.
        key: String,
    },
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(transparent)]
pub struct ImportPgdataIdempotencyKey(pub String);

impl ImportPgdataIdempotencyKey {
    pub fn random() -> Self {
        use rand::{distributions::Alphanumeric, Rng};
        Self(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(20)
                .map(char::from)
                .collect(),
        )
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LsnLeaseRequest {
    pub lsn: Lsn,
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

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub enum FieldPatch<T> {
    Upsert(T),
    Remove,
    #[default]
    Noop,
}

impl<T> FieldPatch<T> {
    fn is_noop(&self) -> bool {
        matches!(self, FieldPatch::Noop)
    }

    pub fn apply(self, target: &mut Option<T>) {
        match self {
            Self::Upsert(v) => *target = Some(v),
            Self::Remove => *target = None,
            Self::Noop => {}
        }
    }

    pub fn map<U, E, F: FnOnce(T) -> Result<U, E>>(self, map: F) -> Result<FieldPatch<U>, E> {
        match self {
            Self::Upsert(v) => Ok(FieldPatch::<U>::Upsert(map(v)?)),
            Self::Remove => Ok(FieldPatch::<U>::Remove),
            Self::Noop => Ok(FieldPatch::<U>::Noop),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for FieldPatch<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::deserialize(deserializer).map(|opt| match opt {
            None => FieldPatch::Remove,
            Some(val) => FieldPatch::Upsert(val),
        })
    }
}

impl<T: Serialize> Serialize for FieldPatch<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FieldPatch::Upsert(val) => serializer.serialize_some(val),
            FieldPatch::Remove => serializer.serialize_none(),
            FieldPatch::Noop => unreachable!(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
#[serde(default)]
pub struct TenantConfigPatch {
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub checkpoint_distance: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub checkpoint_timeout: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_target_size: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_period: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_threshold: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_upper_limit: FieldPatch<usize>,
    // defer parsing compaction_algorithm, like eviction_policy
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub compaction_algorithm: FieldPatch<CompactionAlgorithmSettings>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub l0_flush_delay_threshold: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub l0_flush_stall_threshold: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub l0_flush_wait_upload: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_horizon: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_period: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub image_creation_threshold: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub pitr_interval: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub walreceiver_connect_timeout: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub lagging_wal_timeout: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub max_lsn_wal_lag: FieldPatch<NonZeroU64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub eviction_policy: FieldPatch<EvictionPolicy>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub min_resident_size_override: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub evictions_low_residence_duration_metric_threshold: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub heatmap_period: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub lazy_slru_download: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub timeline_get_throttle: FieldPatch<ThrottleConfig>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub image_layer_creation_check_threshold: FieldPatch<u8>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub image_creation_preempt_threshold: FieldPatch<usize>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub lsn_lease_length: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub lsn_lease_length_for_ts: FieldPatch<String>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub timeline_offloading: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub wal_receiver_protocol_override: FieldPatch<PostgresClientProtocol>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub rel_size_v2_enabled: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_compaction_enabled: FieldPatch<bool>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_compaction_initial_threshold_kb: FieldPatch<u64>,
    #[serde(skip_serializing_if = "FieldPatch::is_noop")]
    pub gc_compaction_ratio_percent: FieldPatch<u64>,
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
    pub compaction_upper_limit: Option<usize>,
    // defer parsing compaction_algorithm, like eviction_policy
    pub compaction_algorithm: Option<CompactionAlgorithmSettings>,
    pub l0_flush_delay_threshold: Option<usize>,
    pub l0_flush_stall_threshold: Option<usize>,
    pub l0_flush_wait_upload: Option<bool>,
    pub gc_horizon: Option<u64>,
    pub gc_period: Option<String>,
    pub image_creation_threshold: Option<usize>,
    pub pitr_interval: Option<String>,
    pub walreceiver_connect_timeout: Option<String>,
    pub lagging_wal_timeout: Option<String>,
    pub max_lsn_wal_lag: Option<NonZeroU64>,
    pub eviction_policy: Option<EvictionPolicy>,
    pub min_resident_size_override: Option<u64>,
    pub evictions_low_residence_duration_metric_threshold: Option<String>,
    pub heatmap_period: Option<String>,
    pub lazy_slru_download: Option<bool>,
    pub timeline_get_throttle: Option<ThrottleConfig>,
    pub image_layer_creation_check_threshold: Option<u8>,
    pub image_creation_preempt_threshold: Option<usize>,
    pub lsn_lease_length: Option<String>,
    pub lsn_lease_length_for_ts: Option<String>,
    pub timeline_offloading: Option<bool>,
    pub wal_receiver_protocol_override: Option<PostgresClientProtocol>,
    pub rel_size_v2_enabled: Option<bool>,
    pub gc_compaction_enabled: Option<bool>,
    pub gc_compaction_initial_threshold_kb: Option<u64>,
    pub gc_compaction_ratio_percent: Option<u64>,
}

impl TenantConfig {
    pub fn apply_patch(self, patch: TenantConfigPatch) -> TenantConfig {
        let Self {
            mut checkpoint_distance,
            mut checkpoint_timeout,
            mut compaction_target_size,
            mut compaction_period,
            mut compaction_threshold,
            mut compaction_upper_limit,
            mut compaction_algorithm,
            mut l0_flush_delay_threshold,
            mut l0_flush_stall_threshold,
            mut l0_flush_wait_upload,
            mut gc_horizon,
            mut gc_period,
            mut image_creation_threshold,
            mut pitr_interval,
            mut walreceiver_connect_timeout,
            mut lagging_wal_timeout,
            mut max_lsn_wal_lag,
            mut eviction_policy,
            mut min_resident_size_override,
            mut evictions_low_residence_duration_metric_threshold,
            mut heatmap_period,
            mut lazy_slru_download,
            mut timeline_get_throttle,
            mut image_layer_creation_check_threshold,
            mut image_creation_preempt_threshold,
            mut lsn_lease_length,
            mut lsn_lease_length_for_ts,
            mut timeline_offloading,
            mut wal_receiver_protocol_override,
            mut rel_size_v2_enabled,
            mut gc_compaction_enabled,
            mut gc_compaction_initial_threshold_kb,
            mut gc_compaction_ratio_percent,
        } = self;

        patch.checkpoint_distance.apply(&mut checkpoint_distance);
        patch.checkpoint_timeout.apply(&mut checkpoint_timeout);
        patch
            .compaction_target_size
            .apply(&mut compaction_target_size);
        patch.compaction_period.apply(&mut compaction_period);
        patch.compaction_threshold.apply(&mut compaction_threshold);
        patch
            .compaction_upper_limit
            .apply(&mut compaction_upper_limit);
        patch.compaction_algorithm.apply(&mut compaction_algorithm);
        patch
            .l0_flush_delay_threshold
            .apply(&mut l0_flush_delay_threshold);
        patch
            .l0_flush_stall_threshold
            .apply(&mut l0_flush_stall_threshold);
        patch.l0_flush_wait_upload.apply(&mut l0_flush_wait_upload);
        patch.gc_horizon.apply(&mut gc_horizon);
        patch.gc_period.apply(&mut gc_period);
        patch
            .image_creation_threshold
            .apply(&mut image_creation_threshold);
        patch.pitr_interval.apply(&mut pitr_interval);
        patch
            .walreceiver_connect_timeout
            .apply(&mut walreceiver_connect_timeout);
        patch.lagging_wal_timeout.apply(&mut lagging_wal_timeout);
        patch.max_lsn_wal_lag.apply(&mut max_lsn_wal_lag);
        patch.eviction_policy.apply(&mut eviction_policy);
        patch
            .min_resident_size_override
            .apply(&mut min_resident_size_override);
        patch
            .evictions_low_residence_duration_metric_threshold
            .apply(&mut evictions_low_residence_duration_metric_threshold);
        patch.heatmap_period.apply(&mut heatmap_period);
        patch.lazy_slru_download.apply(&mut lazy_slru_download);
        patch
            .timeline_get_throttle
            .apply(&mut timeline_get_throttle);
        patch
            .image_layer_creation_check_threshold
            .apply(&mut image_layer_creation_check_threshold);
        patch
            .image_creation_preempt_threshold
            .apply(&mut image_creation_preempt_threshold);
        patch.lsn_lease_length.apply(&mut lsn_lease_length);
        patch
            .lsn_lease_length_for_ts
            .apply(&mut lsn_lease_length_for_ts);
        patch.timeline_offloading.apply(&mut timeline_offloading);
        patch
            .wal_receiver_protocol_override
            .apply(&mut wal_receiver_protocol_override);
        patch.rel_size_v2_enabled.apply(&mut rel_size_v2_enabled);
        patch
            .gc_compaction_enabled
            .apply(&mut gc_compaction_enabled);
        patch
            .gc_compaction_initial_threshold_kb
            .apply(&mut gc_compaction_initial_threshold_kb);
        patch
            .gc_compaction_ratio_percent
            .apply(&mut gc_compaction_ratio_percent);

        Self {
            checkpoint_distance,
            checkpoint_timeout,
            compaction_target_size,
            compaction_period,
            compaction_threshold,
            compaction_upper_limit,
            compaction_algorithm,
            l0_flush_delay_threshold,
            l0_flush_stall_threshold,
            l0_flush_wait_upload,
            gc_horizon,
            gc_period,
            image_creation_threshold,
            pitr_interval,
            walreceiver_connect_timeout,
            lagging_wal_timeout,
            max_lsn_wal_lag,
            eviction_policy,
            min_resident_size_override,
            evictions_low_residence_duration_metric_threshold,
            heatmap_period,
            lazy_slru_download,
            timeline_get_throttle,
            image_layer_creation_check_threshold,
            image_creation_preempt_threshold,
            lsn_lease_length,
            lsn_lease_length_for_ts,
            timeline_offloading,
            wal_receiver_protocol_override,
            rel_size_v2_enabled,
            gc_compaction_enabled,
            gc_compaction_initial_threshold_kb,
            gc_compaction_ratio_percent,
        }
    }
}

/// The policy for the aux file storage.
///
/// It can be switched through `switch_aux_file_policy` tenant config.
/// When the first aux file written, the policy will be persisted in the
/// `index_part.json` file and has a limited migration path.
///
/// Currently, we only allow the following migration path:
///
/// Unset -> V1
///       -> V2
///       -> CrossValidation -> V2
#[derive(
    Eq,
    PartialEq,
    Debug,
    Copy,
    Clone,
    strum_macros::EnumString,
    strum_macros::Display,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
)]
#[strum(serialize_all = "kebab-case")]
pub enum AuxFilePolicy {
    /// V1 aux file policy: store everything in AUX_FILE_KEY
    #[strum(ascii_case_insensitive)]
    V1,
    /// V2 aux file policy: store in the AUX_FILE keyspace
    #[strum(ascii_case_insensitive)]
    V2,
    /// Cross validation runs both formats on the write path and does validation
    /// on the read path.
    #[strum(ascii_case_insensitive)]
    CrossValidation,
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

#[derive(
    Eq,
    PartialEq,
    Debug,
    Copy,
    Clone,
    strum_macros::EnumString,
    strum_macros::Display,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
)]
#[strum(serialize_all = "kebab-case")]
pub enum CompactionAlgorithm {
    Legacy,
    Tiered,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
pub enum ImageCompressionAlgorithm {
    // Disabled for writes, support decompressing during read path
    Disabled,
    /// Zstandard compression. Level 0 means and None mean the same (default level). Levels can be negative as well.
    /// For details, see the [manual](http://facebook.github.io/zstd/zstd_manual.html).
    Zstd {
        level: Option<i8>,
    },
}

impl FromStr for ImageCompressionAlgorithm {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut components = s.split(['(', ')']);
        let first = components
            .next()
            .ok_or_else(|| anyhow::anyhow!("empty string"))?;
        match first {
            "disabled" => Ok(ImageCompressionAlgorithm::Disabled),
            "zstd" => {
                let level = if let Some(v) = components.next() {
                    let v: i8 = v.parse()?;
                    Some(v)
                } else {
                    None
                };

                Ok(ImageCompressionAlgorithm::Zstd { level })
            }
            _ => anyhow::bail!("invalid specifier '{first}'"),
        }
    }
}

impl Display for ImageCompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageCompressionAlgorithm::Disabled => write!(f, "disabled"),
            ImageCompressionAlgorithm::Zstd { level } => {
                if let Some(level) = level {
                    write!(f, "zstd({})", level)
                } else {
                    write!(f, "zstd")
                }
            }
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct CompactionAlgorithmSettings {
    pub kind: CompactionAlgorithm,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum L0FlushConfig {
    #[serde(rename_all = "snake_case")]
    Direct { max_concurrency: NonZeroUsize },
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
    /// See [`ThrottleConfigTaskKinds`] for why we do the serde `rename`.
    #[serde(rename = "task_kinds")]
    pub enabled: ThrottleConfigTaskKinds,
    pub initial: u32,
    #[serde(with = "humantime_serde")]
    pub refill_interval: Duration,
    pub refill_amount: NonZeroU32,
    pub max: u32,
}

/// Before <https://github.com/neondatabase/neon/pull/9962>
/// the throttle was a per `Timeline::get`/`Timeline::get_vectored` call.
/// The `task_kinds` field controlled which Pageserver "Task Kind"s
/// were subject to the throttle.
///
/// After that PR, the throttle is applied at pagestream request level
/// and the `task_kinds` field does not apply since the only task kind
/// that us subject to the throttle is that of the page service.
///
/// However, we don't want to make a breaking config change right now
/// because it means we have to migrate all the tenant configs.
/// This will be done in a future PR.
///
/// In the meantime, we use emptiness / non-emptsiness of the `task_kinds`
/// field to determine if the throttle is enabled or not.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct ThrottleConfigTaskKinds(Vec<String>);

impl ThrottleConfigTaskKinds {
    pub fn disabled() -> Self {
        Self(vec![])
    }
    pub fn is_enabled(&self) -> bool {
        !self.0.is_empty()
    }
}

impl ThrottleConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: ThrottleConfigTaskKinds::disabled(),
            // other values don't matter with emtpy `task_kinds`.
            initial: 0,
            refill_interval: Duration::from_millis(1),
            refill_amount: NonZeroU32::new(1).unwrap(),
            max: 1,
        }
    }
    /// The requests per second allowed  by the given config.
    pub fn steady_rps(&self) -> f64 {
        (self.refill_amount.get() as f64) / (self.refill_interval.as_secs_f64())
    }
}

#[cfg(test)]
mod throttle_config_tests {
    use super::*;

    #[test]
    fn test_disabled_is_disabled() {
        let config = ThrottleConfig::disabled();
        assert!(!config.enabled.is_enabled());
    }
    #[test]
    fn test_enabled_backwards_compat() {
        let input = serde_json::json!({
            "task_kinds": ["PageRequestHandler"],
            "initial": 40000,
            "refill_interval": "50ms",
            "refill_amount": 1000,
            "max": 40000,
            "fair": true
        });
        let config: ThrottleConfig = serde_json::from_value(input).unwrap();
        assert!(config.enabled.is_enabled());
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

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: NodeId,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantLocationConfigRequest {
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TenantConfigPatchRequest {
    pub tenant_id: TenantId,
    #[serde(flatten)]
    pub config: TenantConfigPatch, // as we have a flattened field, we should reject all unknown fields in it
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TenantWaitLsnRequest {
    #[serde(flatten)]
    pub timelines: HashMap<TimelineId, Lsn>,
    pub timeout: Duration,
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
    pub generation: u32,

    /// Opaque explanation if gc is being blocked.
    ///
    /// Only looked up for the individual tenant detail, not the listing. This is purely for
    /// debugging, not included in openapi.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc_blocking: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TenantDetails {
    #[serde(flatten)]
    pub tenant_info: TenantInfo,

    pub walredo: Option<WalRedoManagerStatus>,

    pub timelines: Vec<TimelineId>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Debug)]
pub enum TimelineArchivalState {
    Archived,
    Unarchived,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct TimelineArchivalConfigRequest {
    pub state: TimelineArchivalState,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimelinesInfoAndOffloaded {
    pub timelines: Vec<TimelineInfo>,
    pub offloaded: Vec<OffloadedTimelineInfo>,
}

/// Analog of [`TimelineInfo`] for offloaded timelines.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OffloadedTimelineInfo {
    pub tenant_id: TenantShardId,
    pub timeline_id: TimelineId,
    /// Whether the timeline has a parent it has been branched off from or not
    pub ancestor_timeline_id: Option<TimelineId>,
    /// Whether to retain the branch lsn at the ancestor or not
    pub ancestor_retain_lsn: Option<Lsn>,
    /// The time point when the timeline was archived
    pub archived_at: chrono::DateTime<chrono::Utc>,
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

    /// How many bytes of WAL are within this branch's pitr_interval.  If the pitr_interval goes
    /// beyond the branch's branch point, we only count up to the branch point.
    pub pitr_history_size: u64,

    /// Whether this branch's branch point is within its ancestor's PITR interval (i.e. any
    /// ancestor data used by this branch would have been retained anyway).  If this is false, then
    /// this branch may be imposing a cost on the ancestor by causing it to retain layers that it would
    /// otherwise be able to GC.
    pub within_ancestor_pitr: bool,

    pub timeline_dir_layer_file_size_sum: Option<u64>,

    pub wal_source_connstr: Option<String>,
    pub last_received_msg_lsn: Option<Lsn>,
    /// the timestamp (in microseconds) of the last received message
    pub last_received_msg_ts: Option<u128>,
    pub pg_version: u32,

    pub state: TimelineState,

    pub walreceiver_status: String,

    // ALWAYS add new fields at the end of the struct with `Option` to ensure forward/backward compatibility.
    // Backward compatibility: you will get a JSON not containing the newly-added field.
    // Forward compatibility: a previous version of the pageserver will receive a JSON. serde::Deserialize does
    // not deny unknown fields by default so it's safe to set the field to some value, though it won't be
    // read.
    pub is_archived: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerMapInfo {
    pub in_memory_layers: Vec<InMemoryLayerInfo>,
    pub historic_layers: Vec<HistoricLayerInfo>,
}

/// The residence status of a layer
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LayerResidenceStatus {
    /// Residence status for a layer file that exists locally.
    /// It may also exist on the remote, we don't care here.
    Resident,
    /// Residence status for a layer file that only exists on the remote.
    Evicted,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerAccessStats {
    #[serde_as(as = "serde_with::TimestampMilliSeconds")]
    pub access_time: SystemTime,

    #[serde_as(as = "serde_with::TimestampMilliSeconds")]
    pub residence_time: SystemTime,

    pub visible: bool,
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

        l0: bool,
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
    pub fn layer_file_size(&self) -> u64 {
        match self {
            HistoricLayerInfo::Delta {
                layer_file_size, ..
            } => *layer_file_size,
            HistoricLayerInfo::Image {
                layer_file_size, ..
            } => *layer_file_size,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadRemoteLayersTaskSpawnRequest {
    pub max_concurrent_downloads: NonZeroUsize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestAuxFilesRequest {
    pub aux_files: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListAuxFilesRequest {
    pub lsn: Lsn,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRedoManagerStatus {
    pub last_redo_at: Option<chrono::DateTime<chrono::Utc>>,
    pub process: Option<WalRedoManagerProcessStatus>,
}

/// The progress of a secondary tenant.
///
/// It is mostly useful when doing a long running download: e.g. initiating
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

#[derive(Serialize, Deserialize, Debug)]
pub struct TenantScanRemoteStorageShard {
    pub tenant_shard_id: TenantShardId,
    pub generation: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TenantScanRemoteStorageResponse {
    pub shards: Vec<TenantScanRemoteStorageShard>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum TenantSorting {
    ResidentSize,
    MaxLogicalSize,
}

impl Default for TenantSorting {
    fn default() -> Self {
        Self::ResidentSize
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopTenantShardsRequest {
    // How would you like to sort the tenants?
    pub order_by: TenantSorting,

    // How many results?
    pub limit: usize,

    // Omit tenants with more than this many shards (e.g. if this is the max number of shards
    // that the caller would ever split to)
    pub where_shards_lt: Option<ShardCount>,

    // Omit tenants where the ordering metric is less than this (this is an optimization to
    // let us quickly exclude numerous tiny shards)
    pub where_gt: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct TopTenantShardItem {
    pub id: TenantShardId,

    /// Total size of layers on local disk for all timelines in this tenant
    pub resident_size: u64,

    /// Total size of layers in remote storage for all timelines in this tenant
    pub physical_size: u64,

    /// The largest logical size of a timeline within this tenant
    pub max_logical_size: u64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TopTenantShardsResponse {
    pub shards: Vec<TopTenantShardItem>,
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

    /// Direct IO modes for a pageserver.
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
    #[repr(u8)]
    pub enum IoMode {
        /// Uses buffered IO.
        Buffered,
        /// Uses direct IO, error out if the operation fails.
        #[cfg(target_os = "linux")]
        Direct,
    }

    impl IoMode {
        pub const fn preferred() -> Self {
            Self::Buffered
        }
    }

    impl TryFrom<u8> for IoMode {
        type Error = u8;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
            Ok(match value {
                v if v == (IoMode::Buffered as u8) => IoMode::Buffered,
                #[cfg(target_os = "linux")]
                v if v == (IoMode::Direct as u8) => IoMode::Direct,
                x => return Err(x),
            })
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanDisposableKeysResponse {
    pub disposable_count: usize,
    pub not_disposable_count: usize,
}

// Wrapped in libpq CopyData
#[derive(PartialEq, Eq, Debug)]
pub enum PagestreamFeMessage {
    Exists(PagestreamExistsRequest),
    Nblocks(PagestreamNblocksRequest),
    GetPage(PagestreamGetPageRequest),
    DbSize(PagestreamDbSizeRequest),
    GetSlruSegment(PagestreamGetSlruSegmentRequest),
    #[cfg(feature = "testing")]
    Test(PagestreamTestRequest),
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
    #[cfg(feature = "testing")]
    Test(PagestreamTestResponse),
}

// Keep in sync with `pagestore_client.h`
#[repr(u8)]
enum PagestreamFeMessageTag {
    Exists = 0,
    Nblocks = 1,
    GetPage = 2,
    DbSize = 3,
    GetSlruSegment = 4,
    /* future tags above this line */
    /// For testing purposes, not available in production.
    #[cfg(feature = "testing")]
    Test = 99,
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
    /* future tags above this line */
    /// For testing purposes, not available in production.
    #[cfg(feature = "testing")]
    Test = 199,
}

impl TryFrom<u8> for PagestreamFeMessageTag {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(PagestreamFeMessageTag::Exists),
            1 => Ok(PagestreamFeMessageTag::Nblocks),
            2 => Ok(PagestreamFeMessageTag::GetPage),
            3 => Ok(PagestreamFeMessageTag::DbSize),
            4 => Ok(PagestreamFeMessageTag::GetSlruSegment),
            #[cfg(feature = "testing")]
            99 => Ok(PagestreamFeMessageTag::Test),
            _ => Err(value),
        }
    }
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
            #[cfg(feature = "testing")]
            199 => Ok(PagestreamBeMessageTag::Test),
            _ => Err(value),
        }
    }
}

// A GetPage request contains two LSN values:
//
// request_lsn: Get the page version at this point in time.  Lsn::Max is a special value that means
// "get the latest version present". It's used by the primary server, which knows that no one else
// is writing WAL. 'not_modified_since' must be set to a proper value even if request_lsn is
// Lsn::Max. Standby servers use the current replay LSN as the request LSN.
//
// not_modified_since: Hint to the pageserver that the client knows that the page has not been
// modified between 'not_modified_since' and the request LSN. It's always correct to set
// 'not_modified_since equal' to 'request_lsn' (unless Lsn::Max is used as the 'request_lsn'), but
// passing an earlier LSN can speed up the request, by allowing the pageserver to process the
// request without waiting for 'request_lsn' to arrive.
//
// The now-defunct V1 interface contained only one LSN, and a boolean 'latest' flag. The V1 interface was
// sufficient for the primary; the 'lsn' was equivalent to the 'not_modified_since' value, and
// 'latest' was set to true. The V2 interface was added because there was no correct way for a
// standby to request a page at a particular non-latest LSN, and also include the
// 'not_modified_since' hint. That led to an awkward choice of either using an old LSN in the
// request, if the standby knows that the page hasn't been modified since, and risk getting an error
// if that LSN has fallen behind the GC horizon, or requesting the current replay LSN, which could
// require the pageserver unnecessarily to wait for the WAL to arrive up to that point. The new V2
// interface allows sending both LSNs, and let the pageserver do the right thing. There was no
// difference in the responses between V1 and V2.
//
// V3 version of protocol adds request ID to all requests. This request ID is also included in response
// as well as other fields from requests, which allows to verify that we receive response for our request.
// We copy fields from request to response to make checking more reliable: request ID is formed from process ID
// and local counter, so in principle there can be duplicated requests IDs if process PID is reused.
//
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PagestreamProtocolVersion {
    V2,
    V3,
}

pub type RequestId = u64;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamRequest {
    pub reqid: RequestId,
    pub request_lsn: Lsn,
    pub not_modified_since: Lsn,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamExistsRequest {
    pub hdr: PagestreamRequest,
    pub rel: RelTag,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamNblocksRequest {
    pub hdr: PagestreamRequest,
    pub rel: RelTag,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamGetPageRequest {
    pub hdr: PagestreamRequest,
    pub rel: RelTag,
    pub blkno: u32,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamDbSizeRequest {
    pub hdr: PagestreamRequest,
    pub dbnode: u32,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct PagestreamGetSlruSegmentRequest {
    pub hdr: PagestreamRequest,
    pub kind: u8,
    pub segno: u32,
}

#[derive(Debug)]
pub struct PagestreamExistsResponse {
    pub req: PagestreamExistsRequest,
    pub exists: bool,
}

#[derive(Debug)]
pub struct PagestreamNblocksResponse {
    pub req: PagestreamNblocksRequest,
    pub n_blocks: u32,
}

#[derive(Debug)]
pub struct PagestreamGetPageResponse {
    pub req: PagestreamGetPageRequest,
    pub page: Bytes,
}

#[derive(Debug)]
pub struct PagestreamGetSlruSegmentResponse {
    pub req: PagestreamGetSlruSegmentRequest,
    pub segment: Bytes,
}

#[derive(Debug)]
pub struct PagestreamErrorResponse {
    pub req: PagestreamRequest,
    pub message: String,
}

#[derive(Debug)]
pub struct PagestreamDbSizeResponse {
    pub req: PagestreamDbSizeRequest,
    pub db_size: i64,
}

#[cfg(feature = "testing")]
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PagestreamTestRequest {
    pub hdr: PagestreamRequest,
    pub batch_key: u64,
    pub message: String,
}

#[cfg(feature = "testing")]
#[derive(Debug)]
pub struct PagestreamTestResponse {
    pub req: PagestreamTestRequest,
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
    /// Serialize a compute -> pageserver message. This is currently only used in testing
    /// tools. Always uses protocol version 3.
    pub fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        match self {
            Self::Exists(req) => {
                bytes.put_u8(PagestreamFeMessageTag::Exists as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u32(req.rel.spcnode);
                bytes.put_u32(req.rel.dbnode);
                bytes.put_u32(req.rel.relnode);
                bytes.put_u8(req.rel.forknum);
            }

            Self::Nblocks(req) => {
                bytes.put_u8(PagestreamFeMessageTag::Nblocks as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u32(req.rel.spcnode);
                bytes.put_u32(req.rel.dbnode);
                bytes.put_u32(req.rel.relnode);
                bytes.put_u8(req.rel.forknum);
            }

            Self::GetPage(req) => {
                bytes.put_u8(PagestreamFeMessageTag::GetPage as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u32(req.rel.spcnode);
                bytes.put_u32(req.rel.dbnode);
                bytes.put_u32(req.rel.relnode);
                bytes.put_u8(req.rel.forknum);
                bytes.put_u32(req.blkno);
            }

            Self::DbSize(req) => {
                bytes.put_u8(PagestreamFeMessageTag::DbSize as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u32(req.dbnode);
            }

            Self::GetSlruSegment(req) => {
                bytes.put_u8(PagestreamFeMessageTag::GetSlruSegment as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u8(req.kind);
                bytes.put_u32(req.segno);
            }
            #[cfg(feature = "testing")]
            Self::Test(req) => {
                bytes.put_u8(PagestreamFeMessageTag::Test as u8);
                bytes.put_u64(req.hdr.reqid);
                bytes.put_u64(req.hdr.request_lsn.0);
                bytes.put_u64(req.hdr.not_modified_since.0);
                bytes.put_u64(req.batch_key);
                let message = req.message.as_bytes();
                bytes.put_u64(message.len() as u64);
                bytes.put_slice(message);
            }
        }

        bytes.into()
    }

    pub fn parse<R: std::io::Read>(
        body: &mut R,
        protocol_version: PagestreamProtocolVersion,
    ) -> anyhow::Result<PagestreamFeMessage> {
        // these correspond to the NeonMessageTag enum in pagestore_client.h
        //
        // TODO: consider using protobuf or serde bincode for less error prone
        // serialization.
        let msg_tag = body.read_u8()?;
        let (reqid, request_lsn, not_modified_since) = match protocol_version {
            PagestreamProtocolVersion::V2 => (
                0,
                Lsn::from(body.read_u64::<BigEndian>()?),
                Lsn::from(body.read_u64::<BigEndian>()?),
            ),
            PagestreamProtocolVersion::V3 => (
                body.read_u64::<BigEndian>()?,
                Lsn::from(body.read_u64::<BigEndian>()?),
                Lsn::from(body.read_u64::<BigEndian>()?),
            ),
        };

        match PagestreamFeMessageTag::try_from(msg_tag)
            .map_err(|tag: u8| anyhow::anyhow!("invalid tag {tag}"))?
        {
            PagestreamFeMessageTag::Exists => {
                Ok(PagestreamFeMessage::Exists(PagestreamExistsRequest {
                    hdr: PagestreamRequest {
                        reqid,
                        request_lsn,
                        not_modified_since,
                    },
                    rel: RelTag {
                        spcnode: body.read_u32::<BigEndian>()?,
                        dbnode: body.read_u32::<BigEndian>()?,
                        relnode: body.read_u32::<BigEndian>()?,
                        forknum: body.read_u8()?,
                    },
                }))
            }
            PagestreamFeMessageTag::Nblocks => {
                Ok(PagestreamFeMessage::Nblocks(PagestreamNblocksRequest {
                    hdr: PagestreamRequest {
                        reqid,
                        request_lsn,
                        not_modified_since,
                    },
                    rel: RelTag {
                        spcnode: body.read_u32::<BigEndian>()?,
                        dbnode: body.read_u32::<BigEndian>()?,
                        relnode: body.read_u32::<BigEndian>()?,
                        forknum: body.read_u8()?,
                    },
                }))
            }
            PagestreamFeMessageTag::GetPage => {
                Ok(PagestreamFeMessage::GetPage(PagestreamGetPageRequest {
                    hdr: PagestreamRequest {
                        reqid,
                        request_lsn,
                        not_modified_since,
                    },
                    rel: RelTag {
                        spcnode: body.read_u32::<BigEndian>()?,
                        dbnode: body.read_u32::<BigEndian>()?,
                        relnode: body.read_u32::<BigEndian>()?,
                        forknum: body.read_u8()?,
                    },
                    blkno: body.read_u32::<BigEndian>()?,
                }))
            }
            PagestreamFeMessageTag::DbSize => {
                Ok(PagestreamFeMessage::DbSize(PagestreamDbSizeRequest {
                    hdr: PagestreamRequest {
                        reqid,
                        request_lsn,
                        not_modified_since,
                    },
                    dbnode: body.read_u32::<BigEndian>()?,
                }))
            }
            PagestreamFeMessageTag::GetSlruSegment => Ok(PagestreamFeMessage::GetSlruSegment(
                PagestreamGetSlruSegmentRequest {
                    hdr: PagestreamRequest {
                        reqid,
                        request_lsn,
                        not_modified_since,
                    },
                    kind: body.read_u8()?,
                    segno: body.read_u32::<BigEndian>()?,
                },
            )),
            #[cfg(feature = "testing")]
            PagestreamFeMessageTag::Test => Ok(PagestreamFeMessage::Test(PagestreamTestRequest {
                hdr: PagestreamRequest {
                    reqid,
                    request_lsn,
                    not_modified_since,
                },
                batch_key: body.read_u64::<BigEndian>()?,
                message: {
                    let len = body.read_u64::<BigEndian>()?;
                    let mut buf = vec![0; len as usize];
                    body.read_exact(&mut buf)?;
                    String::from_utf8(buf)?
                },
            })),
        }
    }
}

impl PagestreamBeMessage {
    pub fn serialize(&self, protocol_version: PagestreamProtocolVersion) -> Bytes {
        let mut bytes = BytesMut::new();

        use PagestreamBeMessageTag as Tag;
        match protocol_version {
            PagestreamProtocolVersion::V2 => {
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
                        bytes.put(&resp.page[..])
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

                    #[cfg(feature = "testing")]
                    Self::Test(resp) => {
                        bytes.put_u8(Tag::Test as u8);
                        bytes.put_u64(resp.req.batch_key);
                        let message = resp.req.message.as_bytes();
                        bytes.put_u64(message.len() as u64);
                        bytes.put_slice(message);
                    }
                }
            }
            PagestreamProtocolVersion::V3 => {
                match self {
                    Self::Exists(resp) => {
                        bytes.put_u8(Tag::Exists as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u32(resp.req.rel.spcnode);
                        bytes.put_u32(resp.req.rel.dbnode);
                        bytes.put_u32(resp.req.rel.relnode);
                        bytes.put_u8(resp.req.rel.forknum);
                        bytes.put_u8(resp.exists as u8);
                    }

                    Self::Nblocks(resp) => {
                        bytes.put_u8(Tag::Nblocks as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u32(resp.req.rel.spcnode);
                        bytes.put_u32(resp.req.rel.dbnode);
                        bytes.put_u32(resp.req.rel.relnode);
                        bytes.put_u8(resp.req.rel.forknum);
                        bytes.put_u32(resp.n_blocks);
                    }

                    Self::GetPage(resp) => {
                        bytes.put_u8(Tag::GetPage as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u32(resp.req.rel.spcnode);
                        bytes.put_u32(resp.req.rel.dbnode);
                        bytes.put_u32(resp.req.rel.relnode);
                        bytes.put_u8(resp.req.rel.forknum);
                        bytes.put_u32(resp.req.blkno);
                        bytes.put(&resp.page[..])
                    }

                    Self::Error(resp) => {
                        bytes.put_u8(Tag::Error as u8);
                        bytes.put_u64(resp.req.reqid);
                        bytes.put_u64(resp.req.request_lsn.0);
                        bytes.put_u64(resp.req.not_modified_since.0);
                        bytes.put(resp.message.as_bytes());
                        bytes.put_u8(0); // null terminator
                    }
                    Self::DbSize(resp) => {
                        bytes.put_u8(Tag::DbSize as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u32(resp.req.dbnode);
                        bytes.put_i64(resp.db_size);
                    }

                    Self::GetSlruSegment(resp) => {
                        bytes.put_u8(Tag::GetSlruSegment as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u8(resp.req.kind);
                        bytes.put_u32(resp.req.segno);
                        bytes.put_u32((resp.segment.len() / BLCKSZ as usize) as u32);
                        bytes.put(&resp.segment[..]);
                    }

                    #[cfg(feature = "testing")]
                    Self::Test(resp) => {
                        bytes.put_u8(Tag::Test as u8);
                        bytes.put_u64(resp.req.hdr.reqid);
                        bytes.put_u64(resp.req.hdr.request_lsn.0);
                        bytes.put_u64(resp.req.hdr.not_modified_since.0);
                        bytes.put_u64(resp.req.batch_key);
                        let message = resp.req.message.as_bytes();
                        bytes.put_u64(message.len() as u64);
                        bytes.put_slice(message);
                    }
                }
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
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let rel = RelTag {
                        spcnode: buf.read_u32::<BigEndian>()?,
                        dbnode: buf.read_u32::<BigEndian>()?,
                        relnode: buf.read_u32::<BigEndian>()?,
                        forknum: buf.read_u8()?,
                    };
                    let exists = buf.read_u8()? != 0;
                    Self::Exists(PagestreamExistsResponse {
                        req: PagestreamExistsRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            rel,
                        },
                        exists,
                    })
                }
                Tag::Nblocks => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let rel = RelTag {
                        spcnode: buf.read_u32::<BigEndian>()?,
                        dbnode: buf.read_u32::<BigEndian>()?,
                        relnode: buf.read_u32::<BigEndian>()?,
                        forknum: buf.read_u8()?,
                    };
                    let n_blocks = buf.read_u32::<BigEndian>()?;
                    Self::Nblocks(PagestreamNblocksResponse {
                        req: PagestreamNblocksRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            rel,
                        },
                        n_blocks,
                    })
                }
                Tag::GetPage => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let rel = RelTag {
                        spcnode: buf.read_u32::<BigEndian>()?,
                        dbnode: buf.read_u32::<BigEndian>()?,
                        relnode: buf.read_u32::<BigEndian>()?,
                        forknum: buf.read_u8()?,
                    };
                    let blkno = buf.read_u32::<BigEndian>()?;
                    let mut page = vec![0; 8192]; // TODO: use MaybeUninit
                    buf.read_exact(&mut page)?;
                    Self::GetPage(PagestreamGetPageResponse {
                        req: PagestreamGetPageRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            rel,
                            blkno,
                        },
                        page: page.into(),
                    })
                }
                Tag::Error => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let mut msg = Vec::new();
                    buf.read_until(0, &mut msg)?;
                    let cstring = std::ffi::CString::from_vec_with_nul(msg)?;
                    let rust_str = cstring.to_str()?;
                    Self::Error(PagestreamErrorResponse {
                        req: PagestreamRequest {
                            reqid,
                            request_lsn,
                            not_modified_since,
                        },
                        message: rust_str.to_owned(),
                    })
                }
                Tag::DbSize => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let dbnode = buf.read_u32::<BigEndian>()?;
                    let db_size = buf.read_i64::<BigEndian>()?;
                    Self::DbSize(PagestreamDbSizeResponse {
                        req: PagestreamDbSizeRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            dbnode,
                        },
                        db_size,
                    })
                }
                Tag::GetSlruSegment => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let kind = buf.read_u8()?;
                    let segno = buf.read_u32::<BigEndian>()?;
                    let n_blocks = buf.read_u32::<BigEndian>()?;
                    let mut segment = vec![0; n_blocks as usize * BLCKSZ as usize];
                    buf.read_exact(&mut segment)?;
                    Self::GetSlruSegment(PagestreamGetSlruSegmentResponse {
                        req: PagestreamGetSlruSegmentRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            kind,
                            segno,
                        },
                        segment: segment.into(),
                    })
                }
                #[cfg(feature = "testing")]
                Tag::Test => {
                    let reqid = buf.read_u64::<BigEndian>()?;
                    let request_lsn = Lsn(buf.read_u64::<BigEndian>()?);
                    let not_modified_since = Lsn(buf.read_u64::<BigEndian>()?);
                    let batch_key = buf.read_u64::<BigEndian>()?;
                    let len = buf.read_u64::<BigEndian>()?;
                    let mut msg = vec![0; len as usize];
                    buf.read_exact(&mut msg)?;
                    let message = String::from_utf8(msg)?;
                    Self::Test(PagestreamTestResponse {
                        req: PagestreamTestRequest {
                            hdr: PagestreamRequest {
                                reqid,
                                request_lsn,
                                not_modified_since,
                            },
                            batch_key,
                            message,
                        },
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
            #[cfg(feature = "testing")]
            Self::Test(_) => "Test",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PageTraceEvent {
    pub key: CompactKey,
    pub effective_lsn: Lsn,
    pub time: SystemTime,
}

impl Default for PageTraceEvent {
    fn default() -> Self {
        Self {
            key: Default::default(),
            effective_lsn: Default::default(),
            time: std::time::UNIX_EPOCH,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_pagestream() {
        // Test serialization/deserialization of PagestreamFeMessage
        let messages = vec![
            PagestreamFeMessage::Exists(PagestreamExistsRequest {
                hdr: PagestreamRequest {
                    reqid: 0,
                    request_lsn: Lsn(4),
                    not_modified_since: Lsn(3),
                },
                rel: RelTag {
                    forknum: 1,
                    spcnode: 2,
                    dbnode: 3,
                    relnode: 4,
                },
            }),
            PagestreamFeMessage::Nblocks(PagestreamNblocksRequest {
                hdr: PagestreamRequest {
                    reqid: 0,
                    request_lsn: Lsn(4),
                    not_modified_since: Lsn(4),
                },
                rel: RelTag {
                    forknum: 1,
                    spcnode: 2,
                    dbnode: 3,
                    relnode: 4,
                },
            }),
            PagestreamFeMessage::GetPage(PagestreamGetPageRequest {
                hdr: PagestreamRequest {
                    reqid: 0,
                    request_lsn: Lsn(4),
                    not_modified_since: Lsn(3),
                },
                rel: RelTag {
                    forknum: 1,
                    spcnode: 2,
                    dbnode: 3,
                    relnode: 4,
                },
                blkno: 7,
            }),
            PagestreamFeMessage::DbSize(PagestreamDbSizeRequest {
                hdr: PagestreamRequest {
                    reqid: 0,
                    request_lsn: Lsn(4),
                    not_modified_since: Lsn(3),
                },
                dbnode: 7,
            }),
        ];
        for msg in messages {
            let bytes = msg.serialize();
            let reconstructed =
                PagestreamFeMessage::parse(&mut bytes.reader(), PagestreamProtocolVersion::V3)
                    .unwrap();
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
            generation: 1,
            gc_blocking: None,
        };
        let expected_active = json!({
            "id": original_active.id.to_string(),
            "state": {
                "slug": "Active",
            },
            "current_physical_size": 42,
            "attachment_status": {
                "slug":"attached",
            },
            "generation" : 1
        });

        let original_broken = TenantInfo {
            id: TenantShardId::unsharded(TenantId::generate()),
            state: TenantState::Broken {
                reason: "reason".into(),
                backtrace: "backtrace info".into(),
            },
            current_physical_size: Some(42),
            attachment_status: TenantAttachmentStatus::Attached,
            generation: 1,
            gc_blocking: None,
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
            },
            "generation" : 1
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
    }

    #[test]
    fn tenantstatus_activating_serde() {
        let states = [TenantState::Activating(ActivatingFrom::Attaching)];
        let expected = "[{\"slug\":\"Activating\",\"data\":\"Attaching\"}]";

        let actual = serde_json::to_string(&states).unwrap();

        assert_eq!(actual, expected);

        let parsed = serde_json::from_str::<Vec<TenantState>>(&actual).unwrap();

        assert_eq!(states.as_slice(), &parsed);
    }

    #[test]
    fn tenantstatus_activating_strum() {
        // tests added, because we use these for metrics
        let examples = [
            (line!(), TenantState::Attaching, "Attaching"),
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

    #[test]
    fn test_image_compression_algorithm_parsing() {
        use ImageCompressionAlgorithm::*;
        let cases = [
            ("disabled", Disabled),
            ("zstd", Zstd { level: None }),
            ("zstd(18)", Zstd { level: Some(18) }),
            ("zstd(-3)", Zstd { level: Some(-3) }),
        ];

        for (display, expected) in cases {
            assert_eq!(
                ImageCompressionAlgorithm::from_str(display).unwrap(),
                expected,
                "parsing works"
            );
            assert_eq!(format!("{expected}"), display, "Display FromStr roundtrip");

            let ser = serde_json::to_string(&expected).expect("serialization");
            assert_eq!(
                serde_json::from_str::<ImageCompressionAlgorithm>(&ser).unwrap(),
                expected,
                "serde roundtrip"
            );

            assert_eq!(
                serde_json::Value::String(display.to_string()),
                serde_json::to_value(expected).unwrap(),
                "Display is the serde serialization"
            );
        }
    }

    #[test]
    fn test_tenant_config_patch_request_serde() {
        let patch_request = TenantConfigPatchRequest {
            tenant_id: TenantId::from_str("17c6d121946a61e5ab0fe5a2fd4d8215").unwrap(),
            config: TenantConfigPatch {
                checkpoint_distance: FieldPatch::Upsert(42),
                gc_horizon: FieldPatch::Remove,
                compaction_threshold: FieldPatch::Noop,
                ..TenantConfigPatch::default()
            },
        };

        let json = serde_json::to_string(&patch_request).unwrap();

        let expected = r#"{"tenant_id":"17c6d121946a61e5ab0fe5a2fd4d8215","checkpoint_distance":42,"gc_horizon":null}"#;
        assert_eq!(json, expected);

        let decoded: TenantConfigPatchRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.tenant_id, patch_request.tenant_id);
        assert_eq!(decoded.config, patch_request.config);

        // Now apply the patch to a config to demonstrate semantics

        let base = TenantConfig {
            checkpoint_distance: Some(28),
            gc_horizon: Some(100),
            compaction_target_size: Some(1024),
            ..Default::default()
        };

        let expected = TenantConfig {
            checkpoint_distance: Some(42),
            gc_horizon: None,
            ..base.clone()
        };

        let patched = base.apply_patch(decoded.config);

        assert_eq!(patched, expected);
    }
}
