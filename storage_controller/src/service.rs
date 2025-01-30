pub mod chaos_injector;
mod context_iterator;
pub mod safekeeper_reconciler;

use hyper::Uri;
use safekeeper_api::membership::{MemberSet, SafekeeperId};
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    ops::Deref,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    background_node_operations::{
        Drain, Fill, Operation, OperationError, OperationHandler, MAX_RECONCILES_PER_OPERATION,
    },
    compute_hook::{self, NotifyError},
    drain_utils::{self, TenantShardDrain, TenantShardIterator},
    id_lock_map::{trace_exclusive_lock, trace_shared_lock, IdLockMap, TracingExclusiveGuard},
    leadership::Leadership,
    metrics,
    peer_client::GlobalObservedState,
    persistence::{
        AbortShardSplitStatus, ControllerPersistence, DatabaseResult, MetadataHealthPersistence,
        SafekeeperPersistence, ShardGenerationState, TenantFilter, TimelinePersistence,
        TimelineStatusCreating, TimelineStatusKind,
    },
    reconciler::{ReconcileError, ReconcileUnits, ReconcilerConfig, ReconcilerConfigBuilder},
    safekeeper_client::SafekeeperClient,
    scheduler::{MaySchedule, ScheduleContext, ScheduleError, ScheduleMode},
    tenant_shard::{
        MigrateAttachment, ObservedStateDelta, ReconcileNeeded, ReconcilerStatus,
        ScheduleOptimization, ScheduleOptimizationAction,
    },
};
use anyhow::{anyhow, Context};
use control_plane::storage_controller::{
    AttachHookRequest, AttachHookResponse, InspectRequest, InspectResponse,
};
use diesel::result::DatabaseErrorKind;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use itertools::Itertools;
use pageserver_api::{
    controller_api::{
        AvailabilityZone, MetadataHealthRecord, MetadataHealthUpdateRequest, NodeAvailability,
        NodeRegisterRequest, NodeSchedulingPolicy, NodeShard, NodeShardResponse, PlacementPolicy,
        SafekeeperDescribeResponse, ShardSchedulingPolicy, ShardsPreferredAzsRequest,
        ShardsPreferredAzsResponse, SkSchedulingPolicy, TenantCreateRequest, TenantCreateResponse,
        TenantCreateResponseShard, TenantDescribeResponse, TenantDescribeResponseShard,
        TenantLocateResponse, TenantPolicyRequest, TenantShardMigrateRequest,
        TenantShardMigrateResponse,
    },
    models::{
        SecondaryProgress, TenantConfigPatchRequest, TenantConfigRequest,
        TimelineArchivalConfigRequest, TimelineCreateResponseStorcon, TopTenantShardsRequest,
    },
};
use reqwest::StatusCode;
use tracing::{instrument, Instrument};

use crate::pageserver_client::PageserverClient;
use pageserver_api::{
    models::{
        self, LocationConfig, LocationConfigListResponse, LocationConfigMode,
        PageserverUtilization, ShardParameters, TenantConfig, TenantLocationConfigRequest,
        TenantLocationConfigResponse, TenantShardLocation, TenantShardSplitRequest,
        TenantShardSplitResponse, TenantTimeTravelRequest, TimelineCreateRequest, TimelineInfo,
    },
    shard::{ShardCount, ShardIdentity, ShardNumber, ShardStripeSize, TenantShardId},
    upcall_api::{
        ReAttachRequest, ReAttachResponse, ReAttachResponseTenant, ValidateRequest,
        ValidateResponse, ValidateResponseTenant,
    },
};
use pageserver_client::{mgmt_api, BlockUnblock};
use tokio::{sync::mpsc::error::TrySendError, task::JoinSet};
use tokio_util::sync::CancellationToken;
use utils::{
    backoff,
    completion::Barrier,
    failpoint_support,
    generation::{Generation, SafekeeperGeneration},
    http::error::ApiError,
    id::{NodeId, TenantId, TimelineId},
    logging::SecretString,
    pausable_failpoint,
    sync::gate::Gate,
};

use crate::{
    compute_hook::ComputeHook,
    heartbeater::{Heartbeater, PageserverState},
    node::{AvailabilityTransition, Node},
    persistence::{split_state::SplitState, DatabaseError, Persistence, TenantShardPersistence},
    reconciler::attached_location_conf,
    scheduler::Scheduler,
    tenant_shard::{
        IntentState, ObservedState, ObservedStateLocation, ReconcileResult, ReconcileWaitError,
        ReconcilerWaiter, TenantShard,
    },
};

use context_iterator::TenantShardContextIterator;

const WAITER_FILL_DRAIN_POLL_TIMEOUT: Duration = Duration::from_millis(500);

// For operations that should be quick, like attaching a new tenant
const SHORT_RECONCILE_TIMEOUT: Duration = Duration::from_secs(5);

// For operations that might be slow, like migrating a tenant with
// some data in it.
pub const RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

// If we receive a call using Secondary mode initially, it will omit generation.  We will initialize
// tenant shards into this generation, and as long as it remains in this generation, we will accept
// input generation from future requests as authoritative.
const INITIAL_GENERATION: Generation = Generation::new(0);

/// How long [`Service::startup_reconcile`] is allowed to take before it should give
/// up on unresponsive pageservers and proceed.
pub(crate) const STARTUP_RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a node may be unresponsive to heartbeats before we declare it offline.
/// This must be long enough to cover node restarts as well as normal operations: in future
pub const MAX_OFFLINE_INTERVAL_DEFAULT: Duration = Duration::from_secs(30);

/// How long a node may be unresponsive to heartbeats during start up before we declare it
/// offline.
///
/// This is much more lenient than [`MAX_OFFLINE_INTERVAL_DEFAULT`] since the pageserver's
/// handling of the re-attach response may take a long time and blocks heartbeats from
/// being handled on the pageserver side.
pub const MAX_WARMING_UP_INTERVAL_DEFAULT: Duration = Duration::from_secs(300);

/// How often to send heartbeats to registered nodes?
pub const HEARTBEAT_INTERVAL_DEFAULT: Duration = Duration::from_secs(5);

/// How long is too long for a reconciliation?
pub const LONG_RECONCILE_THRESHOLD_DEFAULT: Duration = Duration::from_secs(120);

#[derive(Clone, strum_macros::Display)]
enum TenantOperations {
    Create,
    LocationConfig,
    ConfigSet,
    ConfigPatch,
    TimeTravelRemoteStorage,
    Delete,
    UpdatePolicy,
    ShardSplit,
    SecondaryDownload,
    TimelineCreate,
    TimelineDelete,
    TimelineReconcile,
    AttachHook,
    TimelineArchivalConfig,
    TimelineDetachAncestor,
    TimelineGcBlockUnblock,
    DropDetached,
}

#[derive(Clone, strum_macros::Display)]
enum NodeOperations {
    Register,
    Configure,
    Delete,
}

/// The leadership status for the storage controller process.
/// Allowed transitions are:
/// 1. Leader -> SteppedDown
/// 2. Candidate -> Leader
#[derive(
    Eq,
    PartialEq,
    Copy,
    Clone,
    strum_macros::Display,
    strum_macros::EnumIter,
    measured::FixedCardinalityLabel,
)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum LeadershipStatus {
    /// This is the steady state where the storage controller can produce
    /// side effects in the cluster.
    Leader,
    /// We've been notified to step down by another candidate. No reconciliations
    /// take place in this state.
    SteppedDown,
    /// Initial state for a new storage controller instance. Will attempt to assume leadership.
    #[allow(unused)]
    Candidate,
}

pub const RECONCILER_CONCURRENCY_DEFAULT: usize = 128;

// Depth of the channel used to enqueue shards for reconciliation when they can't do it immediately.
// This channel is finite-size to avoid using excessive memory if we get into a state where reconciles are finishing more slowly
// than they're being pushed onto the queue.
const MAX_DELAYED_RECONCILES: usize = 10000;

// Top level state available to all HTTP handlers
struct ServiceState {
    leadership_status: LeadershipStatus,

    tenants: BTreeMap<TenantShardId, TenantShard>,

    nodes: Arc<HashMap<NodeId, Node>>,

    scheduler: Scheduler,

    /// Ongoing background operation on the cluster if any is running.
    /// Note that only one such operation may run at any given time,
    /// hence the type choice.
    ongoing_operation: Option<OperationHandler>,

    /// Queue of tenants who are waiting for concurrency limits to permit them to reconcile
    delayed_reconcile_rx: tokio::sync::mpsc::Receiver<TenantShardId>,
}

/// Transform an error from a pageserver into an error to return to callers of a storage
/// controller API.
fn passthrough_api_error(node: &Node, e: mgmt_api::Error) -> ApiError {
    match e {
        mgmt_api::Error::SendRequest(e) => {
            // Presume errors sending requests are connectivity/availability issues
            ApiError::ResourceUnavailable(format!("{node} error sending request: {e}").into())
        }
        mgmt_api::Error::ReceiveErrorBody(str) => {
            // Presume errors receiving body are connectivity/availability issues
            ApiError::ResourceUnavailable(
                format!("{node} error receiving error body: {str}").into(),
            )
        }
        mgmt_api::Error::ReceiveBody(err) if err.is_decode() => {
            // Return 500 for decoding errors.
            ApiError::InternalServerError(anyhow::Error::from(err).context("error decoding body"))
        }
        mgmt_api::Error::ReceiveBody(err) => {
            // Presume errors receiving body are connectivity/availability issues except for decoding errors
            let src_str = err.source().map(|e| e.to_string()).unwrap_or_default();
            ApiError::ResourceUnavailable(
                format!("{node} error receiving error body: {err} {}", src_str).into(),
            )
        }
        mgmt_api::Error::ApiError(StatusCode::NOT_FOUND, msg) => {
            ApiError::NotFound(anyhow::anyhow!(format!("{node}: {msg}")).into())
        }
        mgmt_api::Error::ApiError(StatusCode::SERVICE_UNAVAILABLE, msg) => {
            ApiError::ResourceUnavailable(format!("{node}: {msg}").into())
        }
        mgmt_api::Error::ApiError(status @ StatusCode::UNAUTHORIZED, msg)
        | mgmt_api::Error::ApiError(status @ StatusCode::FORBIDDEN, msg) => {
            // Auth errors talking to a pageserver are not auth errors for the caller: they are
            // internal server errors, showing that something is wrong with the pageserver or
            // storage controller's auth configuration.
            ApiError::InternalServerError(anyhow::anyhow!("{node} {status}: {msg}"))
        }
        mgmt_api::Error::ApiError(status @ StatusCode::TOO_MANY_REQUESTS, msg) => {
            // Pass through 429 errors: if pageserver is asking us to wait + retry, we in
            // turn ask our clients to wait + retry
            ApiError::Conflict(format!("{node} {status}: {status} {msg}"))
        }
        mgmt_api::Error::ApiError(status, msg) => {
            // Presume general case of pageserver API errors is that we tried to do something
            // that can't be done right now.
            ApiError::Conflict(format!("{node} {status}: {status} {msg}"))
        }
        mgmt_api::Error::Cancelled => ApiError::ShuttingDown,
    }
}

impl ServiceState {
    fn new(
        nodes: HashMap<NodeId, Node>,
        tenants: BTreeMap<TenantShardId, TenantShard>,
        scheduler: Scheduler,
        delayed_reconcile_rx: tokio::sync::mpsc::Receiver<TenantShardId>,
        initial_leadership_status: LeadershipStatus,
    ) -> Self {
        metrics::update_leadership_status(initial_leadership_status);

        Self {
            leadership_status: initial_leadership_status,
            tenants,
            nodes: Arc::new(nodes),
            scheduler,
            ongoing_operation: None,
            delayed_reconcile_rx,
        }
    }

    fn parts_mut(
        &mut self,
    ) -> (
        &mut Arc<HashMap<NodeId, Node>>,
        &mut BTreeMap<TenantShardId, TenantShard>,
        &mut Scheduler,
    ) {
        (&mut self.nodes, &mut self.tenants, &mut self.scheduler)
    }

    fn get_leadership_status(&self) -> LeadershipStatus {
        self.leadership_status
    }

    fn step_down(&mut self) {
        self.leadership_status = LeadershipStatus::SteppedDown;
        metrics::update_leadership_status(self.leadership_status);
    }

    fn become_leader(&mut self) {
        self.leadership_status = LeadershipStatus::Leader;
        metrics::update_leadership_status(self.leadership_status);
    }
}

#[derive(Clone)]
pub struct Config {
    // All pageservers managed by one instance of this service must have
    // the same public key.  This JWT token will be used to authenticate
    // this service to the pageservers it manages.
    pub jwt_token: Option<String>,

    // This JWT token will be used to authenticate this service to the control plane.
    pub control_plane_jwt_token: Option<String>,

    // This JWT token will be used to authenticate with other storage controller instances
    pub peer_jwt_token: Option<String>,

    /// Where the compute hook should send notifications of pageserver attachment locations
    /// (this URL points to the control plane in prod). If this is None, the compute hook will
    /// assume it is running in a test environment and try to update neon_local.
    pub compute_hook_url: Option<String>,

    /// Grace period within which a pageserver does not respond to heartbeats, but is still
    /// considered active. Once the grace period elapses, the next heartbeat failure will
    /// mark the pagseserver offline.
    pub max_offline_interval: Duration,

    /// Extended grace period within which pageserver may not respond to heartbeats.
    /// This extended grace period kicks in after the node has been drained for restart
    /// and/or upon handling the re-attach request from a node.
    pub max_warming_up_interval: Duration,

    /// How many Reconcilers may be spawned concurrently
    pub reconciler_concurrency: usize,

    /// How large must a shard grow in bytes before we split it?
    /// None disables auto-splitting.
    pub split_threshold: Option<u64>,

    // TODO: make this cfg(feature  = "testing")
    pub neon_local_repo_dir: Option<PathBuf>,

    // Maximum acceptable download lag for the secondary location
    // while draining a node. If the secondary location is lagging
    // by more than the configured amount, then the secondary is not
    // upgraded to primary.
    pub max_secondary_lag_bytes: Option<u64>,

    pub heartbeat_interval: Duration,

    pub address_for_peers: Option<Uri>,

    pub start_as_candidate: bool,

    pub http_service_port: i32,

    pub long_reconcile_threshold: Duration,
}

impl From<DatabaseError> for ApiError {
    fn from(err: DatabaseError) -> ApiError {
        match err {
            DatabaseError::Query(e) => ApiError::InternalServerError(e.into()),
            // FIXME: ApiError doesn't have an Unavailable variant, but ShuttingDown maps to 503.
            DatabaseError::Connection(_) | DatabaseError::ConnectionPool(_) => {
                ApiError::ShuttingDown
            }
            DatabaseError::Logical(reason) | DatabaseError::Migration(reason) => {
                ApiError::InternalServerError(anyhow::anyhow!(reason))
            }
        }
    }
}

enum InitialShardScheduleOutcome {
    Scheduled(TenantCreateResponseShard),
    NotScheduled,
    ShardScheduleError(ScheduleError),
}

pub struct Service {
    inner: Arc<std::sync::RwLock<ServiceState>>,
    config: Config,
    persistence: Arc<Persistence>,
    compute_hook: Arc<ComputeHook>,
    result_tx: tokio::sync::mpsc::UnboundedSender<ReconcileResultRequest>,

    heartbeater: Heartbeater,

    // Channel for background cleanup from failed operations that require cleanup, such as shard split
    abort_tx: tokio::sync::mpsc::UnboundedSender<TenantShardSplitAbort>,

    // Locking on a tenant granularity (covers all shards in the tenant):
    // - Take exclusively for rare operations that mutate the tenant's persistent state (e.g. create/delete/split)
    // - Take in shared mode for operations that need the set of shards to stay the same to complete reliably (e.g. timeline CRUD)
    tenant_op_locks: IdLockMap<TenantId, TenantOperations>,

    // Locking for node-mutating operations: take exclusively for operations that modify the node's persistent state, or
    // that transition it to/from Active.
    node_op_locks: IdLockMap<NodeId, NodeOperations>,

    // Limit how many Reconcilers we will spawn concurrently
    reconciler_concurrency: Arc<tokio::sync::Semaphore>,

    /// Queue of tenants who are waiting for concurrency limits to permit them to reconcile
    /// Send into this queue to promptly attempt to reconcile this shard next time units are available.
    ///
    /// Note that this state logically lives inside ServiceState, but carrying Sender here makes the code simpler
    /// by avoiding needing a &mut ref to something inside the ServiceState.  This could be optimized to
    /// use a VecDeque instead of a channel to reduce synchronization overhead, at the cost of some code complexity.
    delayed_reconcile_tx: tokio::sync::mpsc::Sender<TenantShardId>,

    // Process shutdown will fire this token
    cancel: CancellationToken,

    // Child token of [`Service::cancel`] used by reconcilers
    reconcilers_cancel: CancellationToken,

    // Background tasks will hold this gate
    gate: Gate,

    // Reconcilers background tasks will hold this gate
    reconcilers_gate: Gate,

    /// This waits for initial reconciliation with pageservers to complete.  Until this barrier
    /// passes, it isn't safe to do any actions that mutate tenants.
    pub(crate) startup_complete: Barrier,
}

impl From<ReconcileWaitError> for ApiError {
    fn from(value: ReconcileWaitError) -> Self {
        match value {
            ReconcileWaitError::Shutdown => ApiError::ShuttingDown,
            e @ ReconcileWaitError::Timeout(_) => ApiError::Timeout(format!("{e}").into()),
            e @ ReconcileWaitError::Failed(..) => ApiError::InternalServerError(anyhow::anyhow!(e)),
        }
    }
}

impl From<OperationError> for ApiError {
    fn from(value: OperationError) -> Self {
        match value {
            OperationError::NodeStateChanged(err) | OperationError::FinalizeError(err) => {
                ApiError::InternalServerError(anyhow::anyhow!(err))
            }
            OperationError::Cancelled => ApiError::Conflict("Operation was cancelled".into()),
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum TenantCreateOrUpdate {
    Create(TenantCreateRequest),
    Update(Vec<ShardUpdate>),
}

struct ShardSplitParams {
    old_shard_count: ShardCount,
    new_shard_count: ShardCount,
    new_stripe_size: Option<ShardStripeSize>,
    targets: Vec<ShardSplitTarget>,
    policy: PlacementPolicy,
    config: TenantConfig,
    shard_ident: ShardIdentity,
    preferred_az_id: Option<AvailabilityZone>,
}

// When preparing for a shard split, we may either choose to proceed with the split,
// or find that the work is already done and return NoOp.
enum ShardSplitAction {
    Split(Box<ShardSplitParams>),
    NoOp(TenantShardSplitResponse),
}

// A parent shard which will be split
struct ShardSplitTarget {
    parent_id: TenantShardId,
    node: Node,
    child_ids: Vec<TenantShardId>,
}

/// When we tenant shard split operation fails, we may not be able to clean up immediately, because nodes
/// might not be available.  We therefore use a queue of abort operations processed in the background.
struct TenantShardSplitAbort {
    tenant_id: TenantId,
    /// The target values from the request that failed
    new_shard_count: ShardCount,
    new_stripe_size: Option<ShardStripeSize>,
    /// Until this abort op is complete, no other operations may be done on the tenant
    _tenant_lock: TracingExclusiveGuard<TenantOperations>,
}

#[derive(thiserror::Error, Debug)]
enum TenantShardSplitAbortError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Remote(#[from] mgmt_api::Error),
    #[error("Unavailable")]
    Unavailable,
}

struct ShardUpdate {
    tenant_shard_id: TenantShardId,
    placement_policy: PlacementPolicy,
    tenant_config: TenantConfig,

    /// If this is None, generation is not updated.
    generation: Option<Generation>,

    /// If this is None, scheduling policy is not updated.
    scheduling_policy: Option<ShardSchedulingPolicy>,
}

enum StopReconciliationsReason {
    ShuttingDown,
    SteppingDown,
}

impl std::fmt::Display for StopReconciliationsReason {
    fn fmt(&self, writer: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s = match self {
            Self::ShuttingDown => "Shutting down",
            Self::SteppingDown => "Stepping down",
        };
        write!(writer, "{}", s)
    }
}

pub(crate) enum ReconcileResultRequest {
    ReconcileResult(ReconcileResult),
    Stop,
}

#[derive(Clone)]
struct MutationLocation {
    node: Node,
    generation: Generation,
}

#[derive(Clone)]
struct ShardMutationLocations {
    latest: MutationLocation,
    other: Vec<MutationLocation>,
}

#[derive(Default, Clone)]
struct TenantMutationLocations(BTreeMap<TenantShardId, ShardMutationLocations>);

impl Service {
    pub fn get_config(&self) -> &Config {
        &self.config
    }

    /// Called once on startup, this function attempts to contact all pageservers to build an up-to-date
    /// view of the world, and determine which pageservers are responsive.
    #[instrument(skip_all)]
    async fn startup_reconcile(
        self: &Arc<Service>,
        current_leader: Option<ControllerPersistence>,
        leader_step_down_state: Option<GlobalObservedState>,
        bg_compute_notify_result_tx: tokio::sync::mpsc::Sender<
            Result<(), (TenantShardId, NotifyError)>,
        >,
    ) {
        // Startup reconciliation does I/O to other services: whether they
        // are responsive or not, we should aim to finish within our deadline, because:
        // - If we don't, a k8s readiness hook watching /ready will kill us.
        // - While we're waiting for startup reconciliation, we are not fully
        //   available for end user operations like creating/deleting tenants and timelines.
        //
        // We set multiple deadlines to break up the time available between the phases of work: this is
        // arbitrary, but avoids a situation where the first phase could burn our entire timeout period.
        let start_at = Instant::now();
        let node_scan_deadline = start_at
            .checked_add(STARTUP_RECONCILE_TIMEOUT / 2)
            .expect("Reconcile timeout is a modest constant");

        let observed = if let Some(state) = leader_step_down_state {
            tracing::info!(
                "Using observed state received from leader at {}",
                current_leader.as_ref().unwrap().address
            );

            state
        } else {
            self.build_global_observed_state(node_scan_deadline).await
        };

        // Accumulate a list of any tenant locations that ought to be detached
        let mut cleanup = Vec::new();

        // Send initial heartbeat requests to all nodes loaded from the database
        let all_nodes = {
            let locked = self.inner.read().unwrap();
            locked.nodes.clone()
        };
        let mut nodes_online = self.initial_heartbeat_round(all_nodes.keys()).await;

        // List of tenants for which we will attempt to notify compute of their location at startup
        let mut compute_notifications = Vec::new();

        // Populate intent and observed states for all tenants, based on reported state on pageservers
        tracing::info!("Populating tenant shards' states from initial pageserver scan...");
        let shard_count = {
            let mut locked = self.inner.write().unwrap();
            let (nodes, tenants, scheduler) = locked.parts_mut();

            // Mark nodes online if they responded to us: nodes are offline by default after a restart.
            let mut new_nodes = (**nodes).clone();
            for (node_id, node) in new_nodes.iter_mut() {
                if let Some(utilization) = nodes_online.remove(node_id) {
                    node.set_availability(NodeAvailability::Active(utilization));
                    scheduler.node_upsert(node);
                }
            }
            *nodes = Arc::new(new_nodes);

            for (tenant_shard_id, observed_state) in observed.0 {
                let Some(tenant_shard) = tenants.get_mut(&tenant_shard_id) else {
                    for node_id in observed_state.locations.keys() {
                        cleanup.push((tenant_shard_id, *node_id));
                    }

                    continue;
                };

                tenant_shard.observed = observed_state;
            }

            // Populate each tenant's intent state
            let mut schedule_context = ScheduleContext::default();
            for (tenant_shard_id, tenant_shard) in tenants.iter_mut() {
                if tenant_shard_id.shard_number == ShardNumber(0) {
                    // Reset scheduling context each time we advance to the next Tenant
                    schedule_context = ScheduleContext::default();
                }

                tenant_shard.intent_from_observed(scheduler);
                if let Err(e) = tenant_shard.schedule(scheduler, &mut schedule_context) {
                    // Non-fatal error: we are unable to properly schedule the tenant, perhaps because
                    // not enough pageservers are available.  The tenant may well still be available
                    // to clients.
                    tracing::error!("Failed to schedule tenant {tenant_shard_id} at startup: {e}");
                } else {
                    // If we're both intending and observed to be attached at a particular node, we will
                    // emit a compute notification for this. In the case where our observed state does not
                    // yet match our intent, we will eventually reconcile, and that will emit a compute notification.
                    if let Some(attached_at) = tenant_shard.stably_attached() {
                        compute_notifications.push(compute_hook::ShardUpdate {
                            tenant_shard_id: *tenant_shard_id,
                            node_id: attached_at,
                            stripe_size: tenant_shard.shard.stripe_size,
                            preferred_az: tenant_shard
                                .preferred_az()
                                .map(|az| Cow::Owned(az.clone())),
                        });
                    }
                }
            }

            tenants.len()
        };

        // Before making any obeservable changes to the cluster, persist self
        // as leader in database and memory.
        let leadership = Leadership::new(
            self.persistence.clone(),
            self.config.clone(),
            self.cancel.child_token(),
        );

        if let Err(e) = leadership.become_leader(current_leader).await {
            tracing::error!("Failed to persist self as leader: {e}. Aborting start-up ...");
            std::process::exit(1);
        }

        self.inner.write().unwrap().become_leader();

        // TODO: if any tenant's intent now differs from its loaded generation_pageserver, we should clear that
        // generation_pageserver in the database.

        // Emit compute hook notifications for all tenants which are already stably attached.  Other tenants
        // will emit compute hook notifications when they reconcile.
        //
        // Ordering: our calls to notify_background synchronously establish a relative order for these notifications vs. any later
        // calls into the ComputeHook for the same tenant: we can leave these to run to completion in the background and any later
        // calls will be correctly ordered wrt these.
        //
        // Concurrency: we call notify_background for all tenants, which will create O(N) tokio tasks, but almost all of them
        // will just wait on the ComputeHook::API_CONCURRENCY semaphore immediately, so very cheap until they get that semaphore
        // unit and start doing I/O.
        tracing::info!(
            "Sending {} compute notifications",
            compute_notifications.len()
        );
        self.compute_hook.notify_background(
            compute_notifications,
            bg_compute_notify_result_tx.clone(),
            &self.cancel,
        );

        // Finally, now that the service is up and running, launch reconcile operations for any tenants
        // which require it: under normal circumstances this should only include tenants that were in some
        // transient state before we restarted, or any tenants whose compute hooks failed above.
        tracing::info!("Checking for shards in need of reconciliation...");
        let reconcile_tasks = self.reconcile_all();
        // We will not wait for these reconciliation tasks to run here: we're now done with startup and
        // normal operations may proceed.

        // Clean up any tenants that were found on pageservers but are not known to us.  Do this in the
        // background because it does not need to complete in order to proceed with other work.
        if !cleanup.is_empty() {
            tracing::info!("Cleaning up {} locations in the background", cleanup.len());
            tokio::task::spawn({
                let cleanup_self = self.clone();
                async move { cleanup_self.cleanup_locations(cleanup).await }
            });
        }

        tracing::info!("Startup complete, spawned {reconcile_tasks} reconciliation tasks ({shard_count} shards total)");
    }

    async fn initial_heartbeat_round<'a>(
        &self,
        node_ids: impl Iterator<Item = &'a NodeId>,
    ) -> HashMap<NodeId, PageserverUtilization> {
        assert!(!self.startup_complete.is_ready());

        let all_nodes = {
            let locked = self.inner.read().unwrap();
            locked.nodes.clone()
        };

        let mut nodes_to_heartbeat = HashMap::new();
        for node_id in node_ids {
            match all_nodes.get(node_id) {
                Some(node) => {
                    nodes_to_heartbeat.insert(*node_id, node.clone());
                }
                None => {
                    tracing::warn!("Node {node_id} was removed during start-up");
                }
            }
        }

        tracing::info!("Sending initial heartbeats...");
        let res = self
            .heartbeater
            .heartbeat(Arc::new(nodes_to_heartbeat))
            .await;

        let mut online_nodes = HashMap::new();
        if let Ok(deltas) = res {
            for (node_id, status) in deltas.0 {
                match status {
                    PageserverState::Available { utilization, .. } => {
                        online_nodes.insert(node_id, utilization);
                    }
                    PageserverState::Offline => {}
                    PageserverState::WarmingUp { .. } => {
                        unreachable!("Nodes are never marked warming-up during startup reconcile")
                    }
                }
            }
        }

        online_nodes
    }

    /// Used during [`Self::startup_reconcile`]: issue GETs to all nodes concurrently, with a deadline.
    ///
    /// The result includes only nodes which responded within the deadline
    async fn scan_node_locations(
        &self,
        deadline: Instant,
    ) -> HashMap<NodeId, LocationConfigListResponse> {
        let nodes = {
            let locked = self.inner.read().unwrap();
            locked.nodes.clone()
        };

        let mut node_results = HashMap::new();

        let mut node_list_futs = FuturesUnordered::new();

        tracing::info!("Scanning shards on {} nodes...", nodes.len());
        for node in nodes.values() {
            node_list_futs.push({
                async move {
                    tracing::info!("Scanning shards on node {node}...");
                    let timeout = Duration::from_secs(5);
                    let response = node
                        .with_client_retries(
                            |client| async move { client.list_location_config().await },
                            &self.config.jwt_token,
                            1,
                            5,
                            timeout,
                            &self.cancel,
                        )
                        .await;
                    (node.get_id(), response)
                }
            });
        }

        loop {
            let (node_id, result) = tokio::select! {
                next = node_list_futs.next() => {
                    match next {
                        Some(result) => result,
                        None =>{
                            // We got results for all our nodes
                            break;
                        }

                    }
                },
                _ = tokio::time::sleep(deadline.duration_since(Instant::now())) => {
                    // Give up waiting for anyone who hasn't responded: we will yield the results that we have
                    tracing::info!("Reached deadline while waiting for nodes to respond to location listing requests");
                    break;
                }
            };

            let Some(list_response) = result else {
                tracing::info!("Shutdown during startup_reconcile");
                break;
            };

            match list_response {
                Err(e) => {
                    tracing::warn!("Could not scan node {} ({e})", node_id);
                }
                Ok(listing) => {
                    node_results.insert(node_id, listing);
                }
            }
        }

        node_results
    }

    async fn build_global_observed_state(&self, deadline: Instant) -> GlobalObservedState {
        let node_listings = self.scan_node_locations(deadline).await;
        let mut observed = GlobalObservedState::default();

        for (node_id, location_confs) in node_listings {
            tracing::info!(
                "Received {} shard statuses from pageserver {}",
                location_confs.tenant_shards.len(),
                node_id
            );

            for (tid, location_conf) in location_confs.tenant_shards {
                let entry = observed.0.entry(tid).or_default();
                entry.locations.insert(
                    node_id,
                    ObservedStateLocation {
                        conf: location_conf,
                    },
                );
            }
        }

        observed
    }

    /// Used during [`Self::startup_reconcile`]: detach a list of unknown-to-us tenants from pageservers.
    ///
    /// This is safe to run in the background, because if we don't have this TenantShardId in our map of
    /// tenants, then it is probably something incompletely deleted before: we will not fight with any
    /// other task trying to attach it.
    #[instrument(skip_all)]
    async fn cleanup_locations(&self, cleanup: Vec<(TenantShardId, NodeId)>) {
        let nodes = self.inner.read().unwrap().nodes.clone();

        for (tenant_shard_id, node_id) in cleanup {
            // A node reported a tenant_shard_id which is unknown to us: detach it.
            let Some(node) = nodes.get(&node_id) else {
                // This is legitimate; we run in the background and [`Self::startup_reconcile`] might have identified
                // a location to clean up on a node that has since been removed.
                tracing::info!(
                    "Not cleaning up location {node_id}/{tenant_shard_id}: node not found"
                );
                continue;
            };

            if self.cancel.is_cancelled() {
                break;
            }

            let client = PageserverClient::new(
                node.get_id(),
                node.base_url(),
                self.config.jwt_token.as_deref(),
            );
            match client
                .location_config(
                    tenant_shard_id,
                    LocationConfig {
                        mode: LocationConfigMode::Detached,
                        generation: None,
                        secondary_conf: None,
                        shard_number: tenant_shard_id.shard_number.0,
                        shard_count: tenant_shard_id.shard_count.literal(),
                        shard_stripe_size: 0,
                        tenant_conf: models::TenantConfig::default(),
                    },
                    None,
                    false,
                )
                .await
            {
                Ok(()) => {
                    tracing::info!(
                        "Detached unknown shard {tenant_shard_id} on pageserver {node_id}"
                    );
                }
                Err(e) => {
                    // Non-fatal error: leaving a tenant shard behind that we are not managing shouldn't
                    // break anything.
                    tracing::error!(
                        "Failed to detach unknkown shard {tenant_shard_id} on pageserver {node_id}: {e}"
                    );
                }
            }
        }
    }

    /// Long running background task that periodically wakes up and looks for shards that need
    /// reconciliation.  Reconciliation is fallible, so any reconciliation tasks that fail during
    /// e.g. a tenant create/attach/migrate must eventually be retried: this task is responsible
    /// for those retries.
    #[instrument(skip_all)]
    async fn background_reconcile(self: &Arc<Self>) {
        self.startup_complete.clone().wait().await;

        const BACKGROUND_RECONCILE_PERIOD: Duration = Duration::from_secs(20);
        let mut interval = tokio::time::interval(BACKGROUND_RECONCILE_PERIOD);
        while !self.reconcilers_cancel.is_cancelled() {
            tokio::select! {
              _ = interval.tick() => {
                let reconciles_spawned = self.reconcile_all();
                if reconciles_spawned == 0 {
                    // Run optimizer only when we didn't find any other work to do
                    let optimizations = self.optimize_all().await;
                    if optimizations == 0 {
                        // Run new splits only when no optimizations are pending
                        self.autosplit_tenants().await;
                    }
                }
            }
              _ = self.reconcilers_cancel.cancelled() => return
            }
        }
    }
    #[instrument(skip_all)]
    async fn spawn_heartbeat_driver(&self) {
        self.startup_complete.clone().wait().await;

        let mut interval = tokio::time::interval(self.config.heartbeat_interval);
        while !self.cancel.is_cancelled() {
            tokio::select! {
              _ = interval.tick() => { }
              _ = self.cancel.cancelled() => return
            };

            let nodes = {
                let locked = self.inner.read().unwrap();
                locked.nodes.clone()
            };

            let res = self.heartbeater.heartbeat(nodes).await;
            if let Ok(deltas) = res {
                let mut to_handle = Vec::default();

                for (node_id, state) in deltas.0 {
                    let new_availability = match state {
                        PageserverState::Available { utilization, .. } => {
                            NodeAvailability::Active(utilization)
                        }
                        PageserverState::WarmingUp { started_at } => {
                            NodeAvailability::WarmingUp(started_at)
                        }
                        PageserverState::Offline => {
                            // The node might have been placed in the WarmingUp state
                            // while the heartbeat round was on-going. Hence, filter out
                            // offline transitions for WarmingUp nodes that are still within
                            // their grace period.
                            if let Ok(NodeAvailability::WarmingUp(started_at)) = self
                                .get_node(node_id)
                                .await
                                .as_ref()
                                .map(|n| n.get_availability())
                            {
                                let now = Instant::now();
                                if now - *started_at >= self.config.max_warming_up_interval {
                                    NodeAvailability::Offline
                                } else {
                                    NodeAvailability::WarmingUp(*started_at)
                                }
                            } else {
                                NodeAvailability::Offline
                            }
                        }
                    };

                    let node_lock = trace_exclusive_lock(
                        &self.node_op_locks,
                        node_id,
                        NodeOperations::Configure,
                    )
                    .await;

                    pausable_failpoint!("heartbeat-pre-node-state-configure");

                    // This is the code path for geniune availability transitions (i.e node
                    // goes unavailable and/or comes back online).
                    let res = self
                        .node_state_configure(node_id, Some(new_availability), None, &node_lock)
                        .await;

                    match res {
                        Ok(transition) => {
                            // Keep hold of the lock until the availability transitions
                            // have been handled in
                            // [`Service::handle_node_availability_transitions`] in order avoid
                            // racing with [`Service::external_node_configure`].
                            to_handle.push((node_id, node_lock, transition));
                        }
                        Err(ApiError::NotFound(_)) => {
                            // This should be rare, but legitimate since the heartbeats are done
                            // on a snapshot of the nodes.
                            tracing::info!("Node {} was not found after heartbeat round", node_id);
                        }
                        Err(ApiError::ShuttingDown) => {
                            // No-op: we're shutting down, no need to try and update any nodes' statuses
                        }
                        Err(err) => {
                            // Transition to active involves reconciling: if a node responds to a heartbeat then
                            // becomes unavailable again, we may get an error here.
                            tracing::error!(
                                "Failed to update node state {} after heartbeat round: {}",
                                node_id,
                                err
                            );
                        }
                    }
                }

                // We collected all the transitions above and now we handle them.
                let res = self.handle_node_availability_transitions(to_handle).await;
                if let Err(errs) = res {
                    for (node_id, err) in errs {
                        match err {
                            ApiError::NotFound(_) => {
                                // This should be rare, but legitimate since the heartbeats are done
                                // on a snapshot of the nodes.
                                tracing::info!(
                                    "Node {} was not found after heartbeat round",
                                    node_id
                                );
                            }
                            err => {
                                tracing::error!(
                                    "Failed to handle availability transition for {} after heartbeat round: {}",
                                    node_id,
                                    err
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// Apply the contents of a [`ReconcileResult`] to our in-memory state: if the reconciliation
    /// was successful and intent hasn't changed since the Reconciler was spawned, this will update
    /// the observed state of the tenant such that subsequent calls to [`TenantShard::get_reconcile_needed`]
    /// will indicate that reconciliation is not needed.
    #[instrument(skip_all, fields(
        seq=%result.sequence,
        tenant_id=%result.tenant_shard_id.tenant_id,
        shard_id=%result.tenant_shard_id.shard_slug(),
    ))]
    fn process_result(&self, result: ReconcileResult) {
        let mut locked = self.inner.write().unwrap();
        let (nodes, tenants, _scheduler) = locked.parts_mut();
        let Some(tenant) = tenants.get_mut(&result.tenant_shard_id) else {
            // A reconciliation result might race with removing a tenant: drop results for
            // tenants that aren't in our map.
            return;
        };

        // Usually generation should only be updated via this path, so the max() isn't
        // needed, but it is used to handle out-of-band updates via. e.g. test hook.
        tenant.generation = std::cmp::max(tenant.generation, result.generation);

        // If the reconciler signals that it failed to notify compute, set this state on
        // the shard so that a future [`TenantShard::maybe_reconcile`] will try again.
        tenant.pending_compute_notification = result.pending_compute_notification;

        // Let the TenantShard know it is idle.
        tenant.reconcile_complete(result.sequence);

        // In case a node was deleted while this reconcile is in flight, filter it out of the update we will
        // make to the tenant
        let deltas = result.observed_deltas.into_iter().flat_map(|delta| {
            // In case a node was deleted while this reconcile is in flight, filter it out of the update we will
            // make to the tenant
            let node = nodes.get(delta.node_id())?;

            if node.is_available() {
                return Some(delta);
            }

            // In case a node became unavailable concurrently with the reconcile, observed
            // locations on it are now uncertain. By convention, set them to None in order
            // for them to get refreshed when the node comes back online.
            Some(ObservedStateDelta::Upsert(Box::new((
                node.get_id(),
                ObservedStateLocation { conf: None },
            ))))
        });

        match result.result {
            Ok(()) => {
                tenant.apply_observed_deltas(deltas);
                tenant.waiter.advance(result.sequence);
            }
            Err(e) => {
                match e {
                    ReconcileError::Cancel => {
                        tracing::info!("Reconciler was cancelled");
                    }
                    ReconcileError::Remote(mgmt_api::Error::Cancelled) => {
                        // This might be due to the reconciler getting cancelled, or it might
                        // be due to the `Node` being marked offline.
                        tracing::info!("Reconciler cancelled during pageserver API call");
                    }
                    _ => {
                        tracing::warn!("Reconcile error: {}", e);
                    }
                }

                // Ordering: populate last_error before advancing error_seq,
                // so that waiters will see the correct error after waiting.
                tenant.set_last_error(result.sequence, e);

                // Skip deletions on reconcile failures
                let upsert_deltas =
                    deltas.filter(|delta| matches!(delta, ObservedStateDelta::Upsert(_)));
                tenant.apply_observed_deltas(upsert_deltas);
            }
        }

        // If we just finished detaching all shards for a tenant, it might be time to drop it from memory.
        if tenant.policy == PlacementPolicy::Detached {
            // We may only drop a tenant from memory while holding the exclusive lock on the tenant ID: this protects us
            // from concurrent execution wrt a request handler that might expect the tenant to remain in memory for the
            // duration of the request.
            let guard = self.tenant_op_locks.try_exclusive(
                tenant.tenant_shard_id.tenant_id,
                TenantOperations::DropDetached,
            );
            if let Some(guard) = guard {
                self.maybe_drop_tenant(tenant.tenant_shard_id.tenant_id, &mut locked, &guard);
            }
        }

        // Maybe some other work can proceed now that this job finished.
        if self.reconciler_concurrency.available_permits() > 0 {
            while let Ok(tenant_shard_id) = locked.delayed_reconcile_rx.try_recv() {
                let (nodes, tenants, _scheduler) = locked.parts_mut();
                if let Some(shard) = tenants.get_mut(&tenant_shard_id) {
                    shard.delayed_reconcile = false;
                    self.maybe_reconcile_shard(shard, nodes);
                }

                if self.reconciler_concurrency.available_permits() == 0 {
                    break;
                }
            }
        }
    }

    async fn process_results(
        &self,
        mut result_rx: tokio::sync::mpsc::UnboundedReceiver<ReconcileResultRequest>,
        mut bg_compute_hook_result_rx: tokio::sync::mpsc::Receiver<
            Result<(), (TenantShardId, NotifyError)>,
        >,
    ) {
        loop {
            // Wait for the next result, or for cancellation
            tokio::select! {
                r = result_rx.recv() => {
                    match r {
                        Some(ReconcileResultRequest::ReconcileResult(result)) => {self.process_result(result);},
                        None | Some(ReconcileResultRequest::Stop) => {break;}
                    }
                }
                _ = async{
                    match bg_compute_hook_result_rx.recv().await {
                        Some(result) => {
                            if let Err((tenant_shard_id, notify_error)) = result {
                                tracing::warn!("Marking shard {tenant_shard_id} for notification retry, due to error {notify_error}");
                                let mut locked = self.inner.write().unwrap();
                                if let Some(shard) = locked.tenants.get_mut(&tenant_shard_id) {
                                    shard.pending_compute_notification = true;
                                }

                            }
                        },
                        None => {
                            // This channel is dead, but we don't want to terminate the outer loop{}: just wait for shutdown
                            self.cancel.cancelled().await;
                        }
                    }
                } => {},
                _ = self.cancel.cancelled() => {
                    break;
                }
            };
        }
    }

    async fn process_aborts(
        &self,
        mut abort_rx: tokio::sync::mpsc::UnboundedReceiver<TenantShardSplitAbort>,
    ) {
        loop {
            // Wait for the next result, or for cancellation
            let op = tokio::select! {
                r = abort_rx.recv() => {
                    match r {
                        Some(op) => {op},
                        None => {break;}
                    }
                }
                _ = self.cancel.cancelled() => {
                    break;
                }
            };

            // Retry until shutdown: we must keep this request object alive until it is properly
            // processed, as it holds a lock guard that prevents other operations trying to do things
            // to the tenant while it is in a weird part-split state.
            while !self.cancel.is_cancelled() {
                match self.abort_tenant_shard_split(&op).await {
                    Ok(_) => break,
                    Err(e) => {
                        tracing::warn!(
                            "Failed to abort shard split on {}, will retry: {e}",
                            op.tenant_id
                        );

                        // If a node is unavailable, we hope that it has been properly marked Offline
                        // when we retry, so that the abort op will succeed.  If the abort op is failing
                        // for some other reason, we will keep retrying forever, or until a human notices
                        // and does something about it (either fixing a pageserver or restarting the controller).
                        tokio::time::timeout(Duration::from_secs(5), self.cancel.cancelled())
                            .await
                            .ok();
                    }
                }
            }
        }
    }

    pub async fn spawn(config: Config, persistence: Arc<Persistence>) -> anyhow::Result<Arc<Self>> {
        let (result_tx, result_rx) = tokio::sync::mpsc::unbounded_channel();
        let (abort_tx, abort_rx) = tokio::sync::mpsc::unbounded_channel();

        let leadership_cancel = CancellationToken::new();
        let leadership = Leadership::new(persistence.clone(), config.clone(), leadership_cancel);
        let (leader, leader_step_down_state) = leadership.step_down_current_leader().await?;

        // Apply the migrations **after** the current leader has stepped down
        // (or we've given up waiting for it), but **before** reading from the
        // database. The only exception is reading the current leader before
        // migrating.
        persistence.migration_run().await?;

        tracing::info!("Loading nodes from database...");
        let nodes = persistence
            .list_nodes()
            .await?
            .into_iter()
            .map(Node::from_persistent)
            .collect::<Vec<_>>();
        let nodes: HashMap<NodeId, Node> = nodes.into_iter().map(|n| (n.get_id(), n)).collect();
        tracing::info!("Loaded {} nodes from database.", nodes.len());
        metrics::METRICS_REGISTRY
            .metrics_group
            .storage_controller_pageserver_nodes
            .set(nodes.len() as i64);

        tracing::info!("Loading shards from database...");
        let mut tenant_shard_persistence = persistence.load_active_tenant_shards().await?;
        tracing::info!(
            "Loaded {} shards from database.",
            tenant_shard_persistence.len()
        );

        // If any shard splits were in progress, reset the database state to abort them
        let mut tenant_shard_count_min_max: HashMap<TenantId, (ShardCount, ShardCount)> =
            HashMap::new();
        for tsp in &mut tenant_shard_persistence {
            let shard = tsp.get_shard_identity()?;
            let tenant_shard_id = tsp.get_tenant_shard_id()?;
            let entry = tenant_shard_count_min_max
                .entry(tenant_shard_id.tenant_id)
                .or_insert_with(|| (shard.count, shard.count));
            entry.0 = std::cmp::min(entry.0, shard.count);
            entry.1 = std::cmp::max(entry.1, shard.count);
        }

        for (tenant_id, (count_min, count_max)) in tenant_shard_count_min_max {
            if count_min != count_max {
                // Aborting the split in the database and dropping the child shards is sufficient: the reconciliation in
                // [`Self::startup_reconcile`] will implicitly drop the child shards on remote pageservers, or they'll
                // be dropped later in [`Self::node_activate_reconcile`] if it isn't available right now.
                tracing::info!("Aborting shard split {tenant_id} {count_min:?} -> {count_max:?}");
                let abort_status = persistence.abort_shard_split(tenant_id, count_max).await?;

                // We may never see the Complete status here: if the split was complete, we wouldn't have
                // identified this tenant has having mismatching min/max counts.
                assert!(matches!(abort_status, AbortShardSplitStatus::Aborted));

                // Clear the splitting status in-memory, to reflect that we just aborted in the database
                tenant_shard_persistence.iter_mut().for_each(|tsp| {
                    // Set idle split state on those shards that we will retain.
                    let tsp_tenant_id = TenantId::from_str(tsp.tenant_id.as_str()).unwrap();
                    if tsp_tenant_id == tenant_id
                        && tsp.get_shard_identity().unwrap().count == count_min
                    {
                        tsp.splitting = SplitState::Idle;
                    } else if tsp_tenant_id == tenant_id {
                        // Leave the splitting state on the child shards: this will be used next to
                        // drop them.
                        tracing::info!(
                            "Shard {tsp_tenant_id} will be dropped after shard split abort",
                        );
                    }
                });

                // Drop shards for this tenant which we didn't just mark idle (i.e. child shards of the aborted split)
                tenant_shard_persistence.retain(|tsp| {
                    TenantId::from_str(tsp.tenant_id.as_str()).unwrap() != tenant_id
                        || tsp.splitting == SplitState::Idle
                });
            }
        }

        let mut tenants = BTreeMap::new();

        let mut scheduler = Scheduler::new(nodes.values());

        #[cfg(feature = "testing")]
        {
            use pageserver_api::controller_api::AvailabilityZone;

            // Hack: insert scheduler state for all nodes referenced by shards, as compatibility
            // tests only store the shards, not the nodes.  The nodes will be loaded shortly
            // after when pageservers start up and register.
            let mut node_ids = HashSet::new();
            for tsp in &tenant_shard_persistence {
                if let Some(node_id) = tsp.generation_pageserver {
                    node_ids.insert(node_id);
                }
            }
            for node_id in node_ids {
                tracing::info!("Creating node {} in scheduler for tests", node_id);
                let node = Node::new(
                    NodeId(node_id as u64),
                    "".to_string(),
                    123,
                    "".to_string(),
                    123,
                    AvailabilityZone("test_az".to_string()),
                );

                scheduler.node_upsert(&node);
            }
        }
        for tsp in tenant_shard_persistence {
            let tenant_shard_id = tsp.get_tenant_shard_id()?;

            // We will populate intent properly later in [`Self::startup_reconcile`], initially populate
            // it with what we can infer: the node for which a generation was most recently issued.
            let mut intent = IntentState::new(
                tsp.preferred_az_id
                    .as_ref()
                    .map(|az| AvailabilityZone(az.clone())),
            );
            if let Some(generation_pageserver) = tsp.generation_pageserver.map(|n| NodeId(n as u64))
            {
                if nodes.contains_key(&generation_pageserver) {
                    intent.set_attached(&mut scheduler, Some(generation_pageserver));
                } else {
                    // If a node was removed before being completely drained, it is legal for it to leave behind a `generation_pageserver` referring
                    // to a non-existent node, because node deletion doesn't block on completing the reconciliations that will issue new generations
                    // on different pageservers.
                    tracing::warn!("Tenant shard {tenant_shard_id} references non-existent node {generation_pageserver} in database, will be rescheduled");
                }
            }
            let new_tenant = TenantShard::from_persistent(tsp, intent)?;

            tenants.insert(tenant_shard_id, new_tenant);
        }

        let (startup_completion, startup_complete) = utils::completion::channel();

        // This channel is continuously consumed by process_results, so doesn't need to be very large.
        let (bg_compute_notify_result_tx, bg_compute_notify_result_rx) =
            tokio::sync::mpsc::channel(512);

        let (delayed_reconcile_tx, delayed_reconcile_rx) =
            tokio::sync::mpsc::channel(MAX_DELAYED_RECONCILES);

        let cancel = CancellationToken::new();
        let reconcilers_cancel = cancel.child_token();

        let heartbeater = Heartbeater::new(
            config.jwt_token.clone(),
            config.max_offline_interval,
            config.max_warming_up_interval,
            cancel.clone(),
        );

        let initial_leadership_status = if config.start_as_candidate {
            LeadershipStatus::Candidate
        } else {
            LeadershipStatus::Leader
        };

        let this = Arc::new(Self {
            inner: Arc::new(std::sync::RwLock::new(ServiceState::new(
                nodes,
                tenants,
                scheduler,
                delayed_reconcile_rx,
                initial_leadership_status,
            ))),
            config: config.clone(),
            persistence,
            compute_hook: Arc::new(ComputeHook::new(config.clone())),
            result_tx,
            heartbeater,
            reconciler_concurrency: Arc::new(tokio::sync::Semaphore::new(
                config.reconciler_concurrency,
            )),
            delayed_reconcile_tx,
            abort_tx,
            startup_complete: startup_complete.clone(),
            cancel,
            reconcilers_cancel,
            gate: Gate::default(),
            reconcilers_gate: Gate::default(),
            tenant_op_locks: Default::default(),
            node_op_locks: Default::default(),
        });

        let result_task_this = this.clone();
        tokio::task::spawn(async move {
            // Block shutdown until we're done (we must respect self.cancel)
            if let Ok(_gate) = result_task_this.gate.enter() {
                result_task_this
                    .process_results(result_rx, bg_compute_notify_result_rx)
                    .await
            }
        });

        tokio::task::spawn({
            let this = this.clone();
            async move {
                // Block shutdown until we're done (we must respect self.cancel)
                if let Ok(_gate) = this.gate.enter() {
                    this.process_aborts(abort_rx).await
                }
            }
        });

        tokio::task::spawn({
            let this = this.clone();
            async move {
                if let Ok(_gate) = this.gate.enter() {
                    loop {
                        tokio::select! {
                            _ = this.cancel.cancelled() => {
                                break;
                            },
                            _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                        };
                        this.tenant_op_locks.housekeeping();
                    }
                }
            }
        });

        tokio::task::spawn({
            let this = this.clone();
            // We will block the [`Service::startup_complete`] barrier until [`Self::startup_reconcile`]
            // is done.
            let startup_completion = startup_completion.clone();
            async move {
                // Block shutdown until we're done (we must respect self.cancel)
                let Ok(_gate) = this.gate.enter() else {
                    return;
                };

                this.startup_reconcile(leader, leader_step_down_state, bg_compute_notify_result_tx)
                    .await;

                drop(startup_completion);
            }
        });

        tokio::task::spawn({
            let this = this.clone();
            let startup_complete = startup_complete.clone();
            async move {
                startup_complete.wait().await;
                this.background_reconcile().await;
            }
        });

        tokio::task::spawn({
            let this = this.clone();
            let startup_complete = startup_complete.clone();
            async move {
                startup_complete.wait().await;
                this.spawn_heartbeat_driver().await;
            }
        });

        Ok(this)
    }

    pub(crate) async fn attach_hook(
        &self,
        attach_req: AttachHookRequest,
    ) -> anyhow::Result<AttachHookResponse> {
        let _tenant_lock = trace_exclusive_lock(
            &self.tenant_op_locks,
            attach_req.tenant_shard_id.tenant_id,
            TenantOperations::AttachHook,
        )
        .await;

        // This is a test hook.  To enable using it on tenants that were created directly with
        // the pageserver API (not via this service), we will auto-create any missing tenant
        // shards with default state.
        let insert = {
            match self
                .maybe_load_tenant(attach_req.tenant_shard_id.tenant_id, &_tenant_lock)
                .await
            {
                Ok(_) => false,
                Err(ApiError::NotFound(_)) => true,
                Err(e) => return Err(e.into()),
            }
        };

        if insert {
            let tsp = TenantShardPersistence {
                tenant_id: attach_req.tenant_shard_id.tenant_id.to_string(),
                shard_number: attach_req.tenant_shard_id.shard_number.0 as i32,
                shard_count: attach_req.tenant_shard_id.shard_count.literal() as i32,
                shard_stripe_size: 0,
                generation: attach_req.generation_override.or(Some(0)),
                generation_pageserver: None,
                placement_policy: serde_json::to_string(&PlacementPolicy::Attached(0)).unwrap(),
                config: serde_json::to_string(&TenantConfig::default()).unwrap(),
                splitting: SplitState::default(),
                scheduling_policy: serde_json::to_string(&ShardSchedulingPolicy::default())
                    .unwrap(),
                preferred_az_id: None,
            };

            match self.persistence.insert_tenant_shards(vec![tsp]).await {
                Err(e) => match e {
                    DatabaseError::Query(diesel::result::Error::DatabaseError(
                        DatabaseErrorKind::UniqueViolation,
                        _,
                    )) => {
                        tracing::info!(
                            "Raced with another request to insert tenant {}",
                            attach_req.tenant_shard_id
                        )
                    }
                    _ => return Err(e.into()),
                },
                Ok(()) => {
                    tracing::info!("Inserted shard {} in database", attach_req.tenant_shard_id);

                    let mut locked = self.inner.write().unwrap();
                    locked.tenants.insert(
                        attach_req.tenant_shard_id,
                        TenantShard::new(
                            attach_req.tenant_shard_id,
                            ShardIdentity::unsharded(),
                            PlacementPolicy::Attached(0),
                            None,
                        ),
                    );
                    tracing::info!("Inserted shard {} in memory", attach_req.tenant_shard_id);
                }
            }
        }

        let new_generation = if let Some(req_node_id) = attach_req.node_id {
            let maybe_tenant_conf = {
                let locked = self.inner.write().unwrap();
                locked
                    .tenants
                    .get(&attach_req.tenant_shard_id)
                    .map(|t| t.config.clone())
            };

            match maybe_tenant_conf {
                Some(conf) => {
                    let new_generation = self
                        .persistence
                        .increment_generation(attach_req.tenant_shard_id, req_node_id)
                        .await?;

                    // Persist the placement policy update. This is required
                    // when we reattaching a detached tenant.
                    self.persistence
                        .update_tenant_shard(
                            TenantFilter::Shard(attach_req.tenant_shard_id),
                            Some(PlacementPolicy::Attached(0)),
                            Some(conf),
                            None,
                            None,
                        )
                        .await?;
                    Some(new_generation)
                }
                None => {
                    anyhow::bail!("Attach hook handling raced with tenant removal")
                }
            }
        } else {
            self.persistence.detach(attach_req.tenant_shard_id).await?;
            None
        };

        let mut locked = self.inner.write().unwrap();
        let (_nodes, tenants, scheduler) = locked.parts_mut();

        let tenant_shard = tenants
            .get_mut(&attach_req.tenant_shard_id)
            .expect("Checked for existence above");

        if let Some(new_generation) = new_generation {
            tenant_shard.generation = Some(new_generation);
            tenant_shard.policy = PlacementPolicy::Attached(0);
        } else {
            // This is a detach notification.  We must update placement policy to avoid re-attaching
            // during background scheduling/reconciliation, or during storage controller restart.
            assert!(attach_req.node_id.is_none());
            tenant_shard.policy = PlacementPolicy::Detached;
        }

        if let Some(attaching_pageserver) = attach_req.node_id.as_ref() {
            tracing::info!(
                tenant_id = %attach_req.tenant_shard_id,
                ps_id = %attaching_pageserver,
                generation = ?tenant_shard.generation,
                "issuing",
            );
        } else if let Some(ps_id) = tenant_shard.intent.get_attached() {
            tracing::info!(
                tenant_id = %attach_req.tenant_shard_id,
                %ps_id,
                generation = ?tenant_shard.generation,
                "dropping",
            );
        } else {
            tracing::info!(
            tenant_id = %attach_req.tenant_shard_id,
            "no-op: tenant already has no pageserver");
        }
        tenant_shard
            .intent
            .set_attached(scheduler, attach_req.node_id);

        tracing::info!(
            "attach_hook: tenant {} set generation {:?}, pageserver {}",
            attach_req.tenant_shard_id,
            tenant_shard.generation,
            // TODO: this is an odd number of 0xf's
            attach_req.node_id.unwrap_or(utils::id::NodeId(0xfffffff))
        );

        // Trick the reconciler into not doing anything for this tenant: this helps
        // tests that manually configure a tenant on the pagesrever, and then call this
        // attach hook: they don't want background reconciliation to modify what they
        // did to the pageserver.
        #[cfg(feature = "testing")]
        {
            if let Some(node_id) = attach_req.node_id {
                tenant_shard.observed.locations = HashMap::from([(
                    node_id,
                    ObservedStateLocation {
                        conf: Some(attached_location_conf(
                            tenant_shard.generation.unwrap(),
                            &tenant_shard.shard,
                            &tenant_shard.config,
                            &PlacementPolicy::Attached(0),
                        )),
                    },
                )]);
            } else {
                tenant_shard.observed.locations.clear();
            }
        }

        Ok(AttachHookResponse {
            gen: attach_req
                .node_id
                .map(|_| tenant_shard.generation.expect("Test hook, not used on tenants that are mid-onboarding with a NULL generation").into().unwrap()),
        })
    }

    pub(crate) fn inspect(&self, inspect_req: InspectRequest) -> InspectResponse {
        let locked = self.inner.read().unwrap();

        let tenant_shard = locked.tenants.get(&inspect_req.tenant_shard_id);

        InspectResponse {
            attachment: tenant_shard.and_then(|s| {
                s.intent
                    .get_attached()
                    .map(|ps| (s.generation.expect("Test hook, not used on tenants that are mid-onboarding with a NULL generation").into().unwrap(), ps))
            }),
        }
    }

    // When the availability state of a node transitions to active, we must do a full reconciliation
    // of LocationConfigs on that node.  This is because while a node was offline:
    // - we might have proceeded through startup_reconcile without checking for extraneous LocationConfigs on this node
    // - aborting a tenant shard split might have left rogue child shards behind on this node.
    //
    // This function must complete _before_ setting a `Node` to Active: once it is set to Active, other
    // Reconcilers might communicate with the node, and these must not overlap with the work we do in
    // this function.
    //
    // The reconciliation logic in here is very similar to what [`Self::startup_reconcile`] does, but
    // for written for a single node rather than as a batch job for all nodes.
    #[tracing::instrument(skip_all, fields(node_id=%node.get_id()))]
    async fn node_activate_reconcile(
        &self,
        mut node: Node,
        _lock: &TracingExclusiveGuard<NodeOperations>,
    ) -> Result<(), ApiError> {
        // This Node is a mutable local copy: we will set it active so that we can use its
        // API client to reconcile with the node.  The Node in [`Self::nodes`] will get updated
        // later.
        node.set_availability(NodeAvailability::Active(PageserverUtilization::full()));

        let configs = match node
            .with_client_retries(
                |client| async move { client.list_location_config().await },
                &self.config.jwt_token,
                1,
                5,
                SHORT_RECONCILE_TIMEOUT,
                &self.cancel,
            )
            .await
        {
            None => {
                // We're shutting down (the Node's cancellation token can't have fired, because
                // we're the only scope that has a reference to it, and we didn't fire it).
                return Err(ApiError::ShuttingDown);
            }
            Some(Err(e)) => {
                // This node didn't succeed listing its locations: it may not proceed to active state
                // as it is apparently unavailable.
                return Err(ApiError::PreconditionFailed(
                    format!("Failed to query node location configs, cannot activate ({e})").into(),
                ));
            }
            Some(Ok(configs)) => configs,
        };
        tracing::info!("Loaded {} LocationConfigs", configs.tenant_shards.len());

        let mut cleanup = Vec::new();
        {
            let mut locked = self.inner.write().unwrap();

            for (tenant_shard_id, observed_loc) in configs.tenant_shards {
                let Some(tenant_shard) = locked.tenants.get_mut(&tenant_shard_id) else {
                    cleanup.push(tenant_shard_id);
                    continue;
                };
                tenant_shard
                    .observed
                    .locations
                    .insert(node.get_id(), ObservedStateLocation { conf: observed_loc });
            }
        }

        for tenant_shard_id in cleanup {
            tracing::info!("Detaching {tenant_shard_id}");
            match node
                .with_client_retries(
                    |client| async move {
                        let config = LocationConfig {
                            mode: LocationConfigMode::Detached,
                            generation: None,
                            secondary_conf: None,
                            shard_number: tenant_shard_id.shard_number.0,
                            shard_count: tenant_shard_id.shard_count.literal(),
                            shard_stripe_size: 0,
                            tenant_conf: models::TenantConfig::default(),
                        };
                        client
                            .location_config(tenant_shard_id, config, None, false)
                            .await
                    },
                    &self.config.jwt_token,
                    1,
                    5,
                    SHORT_RECONCILE_TIMEOUT,
                    &self.cancel,
                )
                .await
            {
                None => {
                    // We're shutting down (the Node's cancellation token can't have fired, because
                    // we're the only scope that has a reference to it, and we didn't fire it).
                    return Err(ApiError::ShuttingDown);
                }
                Some(Err(e)) => {
                    // Do not let the node proceed to Active state if it is not responsive to requests
                    // to detach.  This could happen if e.g. a shutdown bug in the pageserver is preventing
                    // detach completing: we should not let this node back into the set of nodes considered
                    // okay for scheduling.
                    return Err(ApiError::Conflict(format!(
                        "Node {node} failed to detach {tenant_shard_id}: {e}"
                    )));
                }
                Some(Ok(_)) => {}
            };
        }

        Ok(())
    }

    pub(crate) async fn re_attach(
        &self,
        reattach_req: ReAttachRequest,
    ) -> Result<ReAttachResponse, ApiError> {
        if let Some(register_req) = reattach_req.register {
            self.node_register(register_req).await?;
        }

        // Ordering: we must persist generation number updates before making them visible in the in-memory state
        let incremented_generations = self.persistence.re_attach(reattach_req.node_id).await?;

        tracing::info!(
            node_id=%reattach_req.node_id,
            "Incremented {} tenant shards' generations",
            incremented_generations.len()
        );

        // Apply the updated generation to our in-memory state, and
        // gather discover secondary locations.
        let mut locked = self.inner.write().unwrap();
        let (nodes, tenants, scheduler) = locked.parts_mut();

        let mut response = ReAttachResponse {
            tenants: Vec::new(),
        };

        // TODO: cancel/restart any running reconciliation for this tenant, it might be trying
        // to call location_conf API with an old generation.  Wait for cancellation to complete
        // before responding to this request.  Requires well implemented CancellationToken logic
        // all the way to where we call location_conf.  Even then, there can still be a location_conf
        // request in flight over the network: TODO handle that by making location_conf API refuse
        // to go backward in generations.

        // Scan through all shards, applying updates for ones where we updated generation
        // and identifying shards that intend to have a secondary location on this node.
        for (tenant_shard_id, shard) in tenants {
            if let Some(new_gen) = incremented_generations.get(tenant_shard_id) {
                let new_gen = *new_gen;
                response.tenants.push(ReAttachResponseTenant {
                    id: *tenant_shard_id,
                    gen: Some(new_gen.into().unwrap()),
                    // A tenant is only put into multi or stale modes in the middle of a [`Reconciler::live_migrate`]
                    // execution.  If a pageserver is restarted during that process, then the reconcile pass will
                    // fail, and start from scratch, so it doesn't make sense for us to try and preserve
                    // the stale/multi states at this point.
                    mode: LocationConfigMode::AttachedSingle,
                });

                shard.generation = std::cmp::max(shard.generation, Some(new_gen));
                if let Some(observed) = shard.observed.locations.get_mut(&reattach_req.node_id) {
                    // Why can we update `observed` even though we're not sure our response will be received
                    // by the pageserver?  Because the pageserver will not proceed with startup until
                    // it has processed response: if it loses it, we'll see another request and increment
                    // generation again, avoiding any uncertainty about dirtiness of tenant's state.
                    if let Some(conf) = observed.conf.as_mut() {
                        conf.generation = new_gen.into();
                    }
                } else {
                    // This node has no observed state for the shard: perhaps it was offline
                    // when the pageserver restarted.  Insert a None, so that the Reconciler
                    // will be prompted to learn the location's state before it makes changes.
                    shard
                        .observed
                        .locations
                        .insert(reattach_req.node_id, ObservedStateLocation { conf: None });
                }
            } else if shard.intent.get_secondary().contains(&reattach_req.node_id) {
                // Ordering: pageserver will not accept /location_config requests until it has
                // finished processing the response from re-attach.  So we can update our in-memory state
                // now, and be confident that we are not stamping on the result of some later location config.
                // TODO: however, we are not strictly ordered wrt ReconcileResults queue,
                // so we might update observed state here, and then get over-written by some racing
                // ReconcileResult.  The impact is low however, since we have set state on pageserver something
                // that matches intent, so worst case if we race then we end up doing a spurious reconcile.

                response.tenants.push(ReAttachResponseTenant {
                    id: *tenant_shard_id,
                    gen: None,
                    mode: LocationConfigMode::Secondary,
                });

                // We must not update observed, because we have no guarantee that our
                // response will be received by the pageserver. This could leave it
                // falsely dirty, but the resulting reconcile should be idempotent.
            }
        }

        // We consider a node Active once we have composed a re-attach response, but we
        // do not call [`Self::node_activate_reconcile`]: the handling of the re-attach response
        // implicitly synchronizes the LocationConfigs on the node.
        //
        // Setting a node active unblocks any Reconcilers that might write to the location config API,
        // but those requests will not be accepted by the node until it has finished processing
        // the re-attach response.
        //
        // Additionally, reset the nodes scheduling policy to match the conditional update done
        // in [`Persistence::re_attach`].
        if let Some(node) = nodes.get(&reattach_req.node_id) {
            let reset_scheduling = matches!(
                node.get_scheduling(),
                NodeSchedulingPolicy::PauseForRestart
                    | NodeSchedulingPolicy::Draining
                    | NodeSchedulingPolicy::Filling
            );

            let mut new_nodes = (**nodes).clone();
            if let Some(node) = new_nodes.get_mut(&reattach_req.node_id) {
                if reset_scheduling {
                    node.set_scheduling(NodeSchedulingPolicy::Active);
                }

                tracing::info!("Marking {} warming-up on reattach", reattach_req.node_id);
                node.set_availability(NodeAvailability::WarmingUp(std::time::Instant::now()));

                scheduler.node_upsert(node);
                let new_nodes = Arc::new(new_nodes);
                *nodes = new_nodes;
            } else {
                tracing::error!(
                    "Reattaching node {} was removed while processing the request",
                    reattach_req.node_id
                );
            }
        }

        Ok(response)
    }

    pub(crate) async fn validate(
        &self,
        validate_req: ValidateRequest,
    ) -> Result<ValidateResponse, DatabaseError> {
        // Fast in-memory check: we may reject validation on anything that doesn't match our
        // in-memory generation for a shard
        let in_memory_result = {
            let mut in_memory_result = Vec::new();
            let locked = self.inner.read().unwrap();
            for req_tenant in validate_req.tenants {
                if let Some(tenant_shard) = locked.tenants.get(&req_tenant.id) {
                    let valid = tenant_shard.generation == Some(Generation::new(req_tenant.gen));
                    tracing::info!(
                        "handle_validate: {}(gen {}): valid={valid} (latest {:?})",
                        req_tenant.id,
                        req_tenant.gen,
                        tenant_shard.generation
                    );

                    in_memory_result.push((req_tenant.id, Generation::new(req_tenant.gen), valid));
                } else {
                    // This is legal: for example during a shard split the pageserver may still
                    // have deletions in its queue from the old pre-split shard, or after deletion
                    // of a tenant that was busy with compaction/gc while being deleted.
                    tracing::info!(
                        "Refusing deletion validation for missing shard {}",
                        req_tenant.id
                    );
                }
            }

            in_memory_result
        };

        // Database calls to confirm validity for anything that passed the in-memory check.  We must do this
        // in case of controller split-brain, where some other controller process might have incremented the generation.
        let db_generations = self
            .persistence
            .shard_generations(in_memory_result.iter().filter_map(|i| {
                if i.2 {
                    Some(&i.0)
                } else {
                    None
                }
            }))
            .await?;
        let db_generations = db_generations.into_iter().collect::<HashMap<_, _>>();

        let mut response = ValidateResponse {
            tenants: Vec::new(),
        };
        for (tenant_shard_id, validate_generation, valid) in in_memory_result.into_iter() {
            let valid = if valid {
                let db_generation = db_generations.get(&tenant_shard_id);
                db_generation == Some(&Some(validate_generation))
            } else {
                // If in-memory state says it's invalid, trust that.  It's always safe to fail a validation, at worst
                // this prevents a pageserver from cleaning up an object in S3.
                false
            };

            response.tenants.push(ValidateResponseTenant {
                id: tenant_shard_id,
                valid,
            })
        }

        Ok(response)
    }

    pub(crate) async fn tenant_create(
        &self,
        create_req: TenantCreateRequest,
    ) -> Result<TenantCreateResponse, ApiError> {
        let tenant_id = create_req.new_tenant_id.tenant_id;

        // Exclude any concurrent attempts to create/access the same tenant ID
        let _tenant_lock = trace_exclusive_lock(
            &self.tenant_op_locks,
            create_req.new_tenant_id.tenant_id,
            TenantOperations::Create,
        )
        .await;
        let (response, waiters) = self.do_tenant_create(create_req).await?;

        if let Err(e) = self.await_waiters(waiters, RECONCILE_TIMEOUT).await {
            // Avoid deadlock: reconcile may fail while notifying compute, if the cloud control plane refuses to
            // accept compute notifications while it is in the process of creating.  Reconciliation will
            // be retried in the background.
            tracing::warn!(%tenant_id, "Reconcile not done yet while creating tenant ({e})");
        }
        Ok(response)
    }

    pub(crate) async fn do_tenant_create(
        &self,
        create_req: TenantCreateRequest,
    ) -> Result<(TenantCreateResponse, Vec<ReconcilerWaiter>), ApiError> {
        let placement_policy = create_req
            .placement_policy
            .clone()
            // As a default, zero secondaries is convenient for tests that don't choose a policy.
            .unwrap_or(PlacementPolicy::Attached(0));

        // This service expects to handle sharding itself: it is an error to try and directly create
        // a particular shard here.
        let tenant_id = if !create_req.new_tenant_id.is_unsharded() {
            return Err(ApiError::BadRequest(anyhow::anyhow!(
                "Attempted to create a specific shard, this API is for creating the whole tenant"
            )));
        } else {
            create_req.new_tenant_id.tenant_id
        };

        tracing::info!(
            "Creating tenant {}, shard_count={:?}",
            create_req.new_tenant_id,
            create_req.shard_parameters.count,
        );

        let create_ids = (0..create_req.shard_parameters.count.count())
            .map(|i| TenantShardId {
                tenant_id,
                shard_number: ShardNumber(i),
                shard_count: create_req.shard_parameters.count,
            })
            .collect::<Vec<_>>();

        // If the caller specifies a None generation, it means "start from default".  This is different
        // to [`Self::tenant_location_config`], where a None generation is used to represent
        // an incompletely-onboarded tenant.
        let initial_generation = if matches!(placement_policy, PlacementPolicy::Secondary) {
            tracing::info!(
                "tenant_create: secondary mode, generation is_some={}",
                create_req.generation.is_some()
            );
            create_req.generation.map(Generation::new)
        } else {
            tracing::info!(
                "tenant_create: not secondary mode, generation is_some={}",
                create_req.generation.is_some()
            );
            Some(
                create_req
                    .generation
                    .map(Generation::new)
                    .unwrap_or(INITIAL_GENERATION),
            )
        };

        let preferred_az_id = {
            let locked = self.inner.read().unwrap();
            // Idempotency: take the existing value if the tenant already exists
            if let Some(shard) = locked.tenants.get(create_ids.first().unwrap()) {
                shard.preferred_az().cloned()
            } else {
                locked.scheduler.get_az_for_new_tenant()
            }
        };

        // Ordering: we persist tenant shards before creating them on the pageserver.  This enables a caller
        // to clean up after themselves by issuing a tenant deletion if something goes wrong and we restart
        // during the creation, rather than risking leaving orphan objects in S3.
        let persist_tenant_shards = create_ids
            .iter()
            .map(|tenant_shard_id| TenantShardPersistence {
                tenant_id: tenant_shard_id.tenant_id.to_string(),
                shard_number: tenant_shard_id.shard_number.0 as i32,
                shard_count: tenant_shard_id.shard_count.literal() as i32,
                shard_stripe_size: create_req.shard_parameters.stripe_size.0 as i32,
                generation: initial_generation.map(|g| g.into().unwrap() as i32),
                // The pageserver is not known until scheduling happens: we will set this column when
                // incrementing the generation the first time we attach to a pageserver.
                generation_pageserver: None,
                placement_policy: serde_json::to_string(&placement_policy).unwrap(),
                config: serde_json::to_string(&create_req.config).unwrap(),
                splitting: SplitState::default(),
                scheduling_policy: serde_json::to_string(&ShardSchedulingPolicy::default())
                    .unwrap(),
                preferred_az_id: preferred_az_id.as_ref().map(|az| az.to_string()),
            })
            .collect();

        match self
            .persistence
            .insert_tenant_shards(persist_tenant_shards)
            .await
        {
            Ok(_) => {}
            Err(DatabaseError::Query(diesel::result::Error::DatabaseError(
                DatabaseErrorKind::UniqueViolation,
                _,
            ))) => {
                // Unique key violation: this is probably a retry.  Because the shard count is part of the unique key,
                // if we see a unique key violation it means that the creation request's shard count matches the previous
                // creation's shard count.
                tracing::info!("Tenant shards already present in database, proceeding with idempotent creation...");
            }
            // Any other database error is unexpected and a bug.
            Err(e) => return Err(ApiError::InternalServerError(anyhow::anyhow!(e))),
        };

        let mut schedule_context = ScheduleContext::default();
        let mut schedule_error = None;
        let mut response_shards = Vec::new();
        for tenant_shard_id in create_ids {
            tracing::info!("Creating shard {tenant_shard_id}...");

            let outcome = self
                .do_initial_shard_scheduling(
                    tenant_shard_id,
                    initial_generation,
                    &create_req.shard_parameters,
                    create_req.config.clone(),
                    placement_policy.clone(),
                    preferred_az_id.as_ref(),
                    &mut schedule_context,
                )
                .await;

            match outcome {
                InitialShardScheduleOutcome::Scheduled(resp) => response_shards.push(resp),
                InitialShardScheduleOutcome::NotScheduled => {}
                InitialShardScheduleOutcome::ShardScheduleError(err) => {
                    schedule_error = Some(err);
                }
            }
        }

        // If we failed to schedule shards, then they are still created in the controller,
        // but we return an error to the requester to avoid a silent failure when someone
        // tries to e.g. create a tenant whose placement policy requires more nodes than
        // are present in the system.  We do this here rather than in the above loop, to
        // avoid situations where we only create a subset of shards in the tenant.
        if let Some(e) = schedule_error {
            return Err(ApiError::Conflict(format!(
                "Failed to schedule shard(s): {e}"
            )));
        }

        let waiters = {
            let mut locked = self.inner.write().unwrap();
            let (nodes, tenants, _scheduler) = locked.parts_mut();
            tenants
                .range_mut(TenantShardId::tenant_range(tenant_id))
                .filter_map(|(_shard_id, shard)| self.maybe_reconcile_shard(shard, nodes))
                .collect::<Vec<_>>()
        };

        Ok((
            TenantCreateResponse {
                shards: response_shards,
            },
            waiters,
        ))
    }

    /// Helper for tenant creation that does the scheduling for an individual shard. Covers both the
    /// case of a new tenant and a pre-existing one.
    #[allow(clippy::too_many_arguments)]
    async fn do_initial_shard_scheduling(
        &self,
        tenant_shard_id: TenantShardId,
        initial_generation: Option<Generation>,
        shard_params: &ShardParameters,
        config: TenantConfig,
        placement_policy: PlacementPolicy,
        preferred_az_id: Option<&AvailabilityZone>,
        schedule_context: &mut ScheduleContext,
    ) -> InitialShardScheduleOutcome {
        let mut locked = self.inner.write().unwrap();
        let (_nodes, tenants, scheduler) = locked.parts_mut();

        use std::collections::btree_map::Entry;
        match tenants.entry(tenant_shard_id) {
            Entry::Occupied(mut entry) => {
                tracing::info!("Tenant shard {tenant_shard_id} already exists while creating");

                if let Err(err) = entry.get_mut().schedule(scheduler, schedule_context) {
                    return InitialShardScheduleOutcome::ShardScheduleError(err);
                }

                if let Some(node_id) = entry.get().intent.get_attached() {
                    let generation = entry
                        .get()
                        .generation
                        .expect("Generation is set when in attached mode");
                    InitialShardScheduleOutcome::Scheduled(TenantCreateResponseShard {
                        shard_id: tenant_shard_id,
                        node_id: *node_id,
                        generation: generation.into().unwrap(),
                    })
                } else {
                    InitialShardScheduleOutcome::NotScheduled
                }
            }
            Entry::Vacant(entry) => {
                let state = entry.insert(TenantShard::new(
                    tenant_shard_id,
                    ShardIdentity::from_params(tenant_shard_id.shard_number, shard_params),
                    placement_policy,
                    preferred_az_id.cloned(),
                ));

                state.generation = initial_generation;
                state.config = config;
                if let Err(e) = state.schedule(scheduler, schedule_context) {
                    return InitialShardScheduleOutcome::ShardScheduleError(e);
                }

                // Only include shards in result if we are attaching: the purpose
                // of the response is to tell the caller where the shards are attached.
                if let Some(node_id) = state.intent.get_attached() {
                    let generation = state
                        .generation
                        .expect("Generation is set when in attached mode");
                    InitialShardScheduleOutcome::Scheduled(TenantCreateResponseShard {
                        shard_id: tenant_shard_id,
                        node_id: *node_id,
                        generation: generation.into().unwrap(),
                    })
                } else {
                    InitialShardScheduleOutcome::NotScheduled
                }
            }
        }
    }

    /// Helper for functions that reconcile a number of shards, and would like to do a timeout-bounded
    /// wait for reconciliation to complete before responding.
    async fn await_waiters(
        &self,
        waiters: Vec<ReconcilerWaiter>,
        timeout: Duration,
    ) -> Result<(), ReconcileWaitError> {
        let deadline = Instant::now().checked_add(timeout).unwrap();
        for waiter in waiters {
            let timeout = deadline.duration_since(Instant::now());
            waiter.wait_timeout(timeout).await?;
        }

        Ok(())
    }

    /// Same as [`Service::await_waiters`], but returns the waiters which are still
    /// in progress
    async fn await_waiters_remainder(
        &self,
        waiters: Vec<ReconcilerWaiter>,
        timeout: Duration,
    ) -> Vec<ReconcilerWaiter> {
        let deadline = Instant::now().checked_add(timeout).unwrap();
        for waiter in waiters.iter() {
            let timeout = deadline.duration_since(Instant::now());
            let _ = waiter.wait_timeout(timeout).await;
        }

        waiters
            .into_iter()
            .filter(|waiter| matches!(waiter.get_status(), ReconcilerStatus::InProgress))
            .collect::<Vec<_>>()
    }

    /// Part of [`Self::tenant_location_config`]: dissect an incoming location config request,
    /// and transform it into either a tenant creation of a series of shard updates.
    ///
    /// If the incoming request makes no changes, a [`TenantCreateOrUpdate::Update`] result will
    /// still be returned.
    fn tenant_location_config_prepare(
        &self,
        tenant_id: TenantId,
        req: TenantLocationConfigRequest,
    ) -> TenantCreateOrUpdate {
        let mut updates = Vec::new();
        let mut locked = self.inner.write().unwrap();
        let (nodes, tenants, _scheduler) = locked.parts_mut();
        let tenant_shard_id = TenantShardId::unsharded(tenant_id);

        // Use location config mode as an indicator of policy.
        let placement_policy = match req.config.mode {
            LocationConfigMode::Detached => PlacementPolicy::Detached,
            LocationConfigMode::Secondary => PlacementPolicy::Secondary,
            LocationConfigMode::AttachedMulti
            | LocationConfigMode::AttachedSingle
            | LocationConfigMode::AttachedStale => {
                if nodes.len() > 1 {
                    PlacementPolicy::Attached(1)
                } else {
                    // Convenience for dev/test: if we just have one pageserver, import
                    // tenants into non-HA mode so that scheduling will succeed.
                    PlacementPolicy::Attached(0)
                }
            }
        };

        // Ordinarily we do not update scheduling policy, but when making major changes
        // like detaching or demoting to secondary-only, we need to force the scheduling
        // mode to Active, or the caller's expected outcome (detach it) will not happen.
        let scheduling_policy = match req.config.mode {
            LocationConfigMode::Detached | LocationConfigMode::Secondary => {
                // Special case: when making major changes like detaching or demoting to secondary-only,
                // we need to force the scheduling mode to Active, or nothing will happen.
                Some(ShardSchedulingPolicy::Active)
            }
            LocationConfigMode::AttachedMulti
            | LocationConfigMode::AttachedSingle
            | LocationConfigMode::AttachedStale => {
                // While attached, continue to respect whatever the existing scheduling mode is.
                None
            }
        };

        let mut create = true;
        for (shard_id, shard) in tenants.range_mut(TenantShardId::tenant_range(tenant_id)) {
            // Saw an existing shard: this is not a creation
            create = false;

            // Shards may have initially been created by a Secondary request, where we
            // would have left generation as None.
            //
            // We only update generation the first time we see an attached-mode request,
            // and if there is no existing generation set. The caller is responsible for
            // ensuring that no non-storage-controller pageserver ever uses a higher
            // generation than they passed in here.
            use LocationConfigMode::*;
            let set_generation = match req.config.mode {
                AttachedMulti | AttachedSingle | AttachedStale if shard.generation.is_none() => {
                    req.config.generation.map(Generation::new)
                }
                _ => None,
            };

            updates.push(ShardUpdate {
                tenant_shard_id: *shard_id,
                placement_policy: placement_policy.clone(),
                tenant_config: req.config.tenant_conf.clone(),
                generation: set_generation,
                scheduling_policy,
            });
        }

        if create {
            use LocationConfigMode::*;
            let generation = match req.config.mode {
                AttachedMulti | AttachedSingle | AttachedStale => req.config.generation,
                // If a caller provided a generation in a non-attached request, ignore it
                // and leave our generation as None: this enables a subsequent update to set
                // the generation when setting an attached mode for the first time.
                _ => None,
            };

            TenantCreateOrUpdate::Create(
                // Synthesize a creation request
                TenantCreateRequest {
                    new_tenant_id: tenant_shard_id,
                    generation,
                    shard_parameters: ShardParameters {
                        count: tenant_shard_id.shard_count,
                        // We only import un-sharded or single-sharded tenants, so stripe
                        // size can be made up arbitrarily here.
                        stripe_size: ShardParameters::DEFAULT_STRIPE_SIZE,
                    },
                    placement_policy: Some(placement_policy),
                    config: req.config.tenant_conf,
                },
            )
        } else {
            assert!(!updates.is_empty());
            TenantCreateOrUpdate::Update(updates)
        }
    }

    /// For APIs that might act on tenants with [`PlacementPolicy::Detached`], first check if
    /// the tenant is present in memory. If not, load it from the database.  If it is found
    /// in neither location, return a NotFound error.
    ///
    /// Caller must demonstrate they hold a lock guard, as otherwise two callers might try and load
    /// it at the same time, or we might race with [`Self::maybe_drop_tenant`]
    async fn maybe_load_tenant(
        &self,
        tenant_id: TenantId,
        _guard: &TracingExclusiveGuard<TenantOperations>,
    ) -> Result<(), ApiError> {
        // Check if the tenant is present in memory, and select an AZ to use when loading
        // if we will load it.
        let load_in_az = {
            let locked = self.inner.read().unwrap();
            let existing = locked
                .tenants
                .range(TenantShardId::tenant_range(tenant_id))
                .next();

            // If the tenant is not present in memory, we expect to load it from database,
            // so let's figure out what AZ to load it into while we have self.inner locked.
            if existing.is_none() {
                locked
                    .scheduler
                    .get_az_for_new_tenant()
                    .ok_or(ApiError::BadRequest(anyhow::anyhow!(
                        "No AZ with nodes found to load tenant"
                    )))?
            } else {
                // We already have this tenant in memory
                return Ok(());
            }
        };

        let tenant_shards = self.persistence.load_tenant(tenant_id).await?;
        if tenant_shards.is_empty() {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("Tenant {} not found", tenant_id).into(),
            ));
        }

        // Update the persistent shards with the AZ that we are about to apply to in-memory state
        self.persistence
            .set_tenant_shard_preferred_azs(
                tenant_shards
                    .iter()
                    .map(|t| {
                        (
                            t.get_tenant_shard_id().expect("Corrupt shard in database"),
                            Some(load_in_az.clone()),
                        )
                    })
                    .collect(),
            )
            .await?;

        let mut locked = self.inner.write().unwrap();
        tracing::info!(
            "Loaded {} shards for tenant {}",
            tenant_shards.len(),
            tenant_id
        );

        locked.tenants.extend(tenant_shards.into_iter().map(|p| {
            let intent = IntentState::new(Some(load_in_az.clone()));
            let shard =
                TenantShard::from_persistent(p, intent).expect("Corrupt shard row in database");

            // Sanity check: when loading on-demand, we should always be loaded something Detached
            debug_assert!(shard.policy == PlacementPolicy::Detached);
            if shard.policy != PlacementPolicy::Detached {
                tracing::error!(
                    "Tenant shard {} loaded on-demand, but has non-Detached policy {:?}",
                    shard.tenant_shard_id,
                    shard.policy
                );
            }

            (shard.tenant_shard_id, shard)
        }));

        Ok(())
    }

    /// If all shards for a tenant are detached, and in a fully quiescent state (no observed locations on pageservers),
    /// and have no reconciler running, then we can drop the tenant from memory.  It will be reloaded on-demand
    /// if we are asked to attach it again (see [`Self::maybe_load_tenant`]).
    ///
    /// Caller must demonstrate they hold a lock guard, as otherwise it is unsafe to drop a tenant from
    /// memory while some other function might assume it continues to exist while not holding the lock on Self::inner.
    fn maybe_drop_tenant(
        &self,
        tenant_id: TenantId,
        locked: &mut std::sync::RwLockWriteGuard<ServiceState>,
        _guard: &TracingExclusiveGuard<TenantOperations>,
    ) {
        let mut tenant_shards = locked.tenants.range(TenantShardId::tenant_range(tenant_id));
        if tenant_shards.all(|(_id, shard)| {
            shard.policy == PlacementPolicy::Detached
                && shard.reconciler.is_none()
                && shard.observed.is_empty()
        }) {
            let keys = locked
                .tenants
                .range(TenantShardId::tenant_range(tenant_id))
                .map(|(id, _)| id)
                .copied()
                .collect::<Vec<_>>();
            for key in keys {
                tracing::info!("Dropping detached tenant shard {} from memory", key);
                locked.tenants.remove(&key);
            }
        }
    }

    /// This API is used by the cloud control plane to migrate unsharded tenants that it created
    /// directly with pageservers into this service.
    ///
    /// Cloud control plane MUST NOT continue issuing GENERATION NUMBERS for this tenant once it
    /// has attempted to call this API. Failure to oblige to this rule may lead to S3 corruption.
    /// Think of the first attempt to call this API as a transfer of absolute authority over the
    /// tenant's source of generation numbers.
    ///
    /// The mode in this request coarse-grained control of tenants:
    /// - Call with mode Attached* to upsert the tenant.
    /// - Call with mode Secondary to either onboard a tenant without attaching it, or
    ///   to set an existing tenant to PolicyMode::Secondary
    /// - Call with mode Detached to switch to PolicyMode::Detached
    pub(crate) async fn tenant_location_config(
        &self,
        tenant_shard_id: TenantShardId,
        req: TenantLocationConfigRequest,
    ) -> Result<TenantLocationConfigResponse, ApiError> {
        // We require an exclusive lock, because we are updating both persistent and in-memory state
        let _tenant_lock = trace_exclusive_lock(
            &self.tenant_op_locks,
            tenant_shard_id.tenant_id,
            TenantOperations::LocationConfig,
        )
        .await;

        let tenant_id = if !tenant_shard_id.is_unsharded() {
            return Err(ApiError::BadRequest(anyhow::anyhow!(
                "This API is for importing single-sharded or unsharded tenants"
            )));
        } else {
            tenant_shard_id.tenant_id
        };

        // In case we are waking up a Detached tenant
        match self.maybe_load_tenant(tenant_id, &_tenant_lock).await {
            Ok(()) | Err(ApiError::NotFound(_)) => {
                // This is a creation or an update
            }
            Err(e) => {
                return Err(e);
            }
        };

        // First check if this is a creation or an update
        let create_or_update = self.tenant_location_config_prepare(tenant_id, req);

        let mut result = TenantLocationConfigResponse {
            shards: Vec::new(),
            stripe_size: None,
        };
        let waiters = match create_or_update {
            TenantCreateOrUpdate::Create(create_req) => {
                let (create_resp, waiters) = self.do_tenant_create(create_req).await?;
                result.shards = create_resp
                    .shards
                    .into_iter()
                    .map(|s| TenantShardLocation {
                        node_id: s.node_id,
                        shard_id: s.shard_id,
                    })
                    .collect();
                waiters
            }
            TenantCreateOrUpdate::Update(updates) => {
                // Persist updates
                // Ordering: write to the database before applying changes in-memory, so that
                // we will not appear time-travel backwards on a restart.

                let mut schedule_context = ScheduleContext::default();
                for ShardUpdate {
                    tenant_shard_id,
                    placement_policy,
                    tenant_config,
                    generation,
                    scheduling_policy,
                } in &updates
                {
                    self.persistence
                        .update_tenant_shard(
                            TenantFilter::Shard(*tenant_shard_id),
                            Some(placement_policy.clone()),
                            Some(tenant_config.clone()),
                            *generation,
                            *scheduling_policy,
                        )
                        .await?;
                }

                // Apply updates in-memory
                let mut waiters = Vec::new();
                {
                    let mut locked = self.inner.write().unwrap();
                    let (nodes, tenants, scheduler) = locked.parts_mut();

                    for ShardUpdate {
                        tenant_shard_id,
                        placement_policy,
                        tenant_config,
                        generation: update_generation,
                        scheduling_policy,
                    } in updates
                    {
                        let Some(shard) = tenants.get_mut(&tenant_shard_id) else {
                            tracing::warn!("Shard {tenant_shard_id} removed while updating");
                            continue;
                        };

                        // Update stripe size
                        if result.stripe_size.is_none() && shard.shard.count.count() > 1 {
                            result.stripe_size = Some(shard.shard.stripe_size);
                        }

                        shard.policy = placement_policy;
                        shard.config = tenant_config;
                        if let Some(generation) = update_generation {
                            shard.generation = Some(generation);
                        }

                        if let Some(scheduling_policy) = scheduling_policy {
                            shard.set_scheduling_policy(scheduling_policy);
                        }

                        shard.schedule(scheduler, &mut schedule_context)?;

                        let maybe_waiter = self.maybe_reconcile_shard(shard, nodes);
                        if let Some(waiter) = maybe_waiter {
                            waiters.push(waiter);
                        }

                        if let Some(node_id) = shard.intent.get_attached() {
                            result.shards.push(TenantShardLocation {
                                shard_id: tenant_shard_id,
                                node_id: *node_id,
                            })
                        }
                    }
                }
                waiters
            }
        };

        if let Err(e) = self.await_waiters(waiters, SHORT_RECONCILE_TIMEOUT).await {
            // Do not treat a reconcile error as fatal: we have already applied any requested
            // Intent changes, and the reconcile can fail for external reasons like unavailable
            // compute notification API.  In these cases, it is important that we do not
            // cause the cloud control plane to retry forever on this API.
            tracing::warn!(
                "Failed to reconcile after /location_config: {e}, returning success anyway"
            );
        }

        // Logging the full result is useful because it lets us cross-check what the cloud control
        // plane's tenant_shards table should contain.
        tracing::info!("Complete, returning {result:?}");

        Ok(result)
    }

    pub(crate) async fn tenant_config_patch(
        &self,
        req: TenantConfigPatchRequest,
    ) -> Result<(), ApiError> {
        let _tenant_lock = trace_exclusive_lock(
            &self.tenant_op_locks,
            req.tenant_id,
            TenantOperations::ConfigPatch,
        )
        .await;

        let tenant_id = req.tenant_id;
        let patch = req.config;

        self.maybe_load_tenant(tenant_id, &_tenant_lock).await?;

        let base = {
            let locked = self.inner.read().unwrap();
            let shards = locked
                .tenants
                .range(TenantShardId::tenant_range(req.tenant_id));

            let mut configs = shards.map(|(_sid, shard)| &shard.config).peekable();

            let first = match configs.peek() {
                Some(first) => (*first).clone(),
                None => {
                    return Err(ApiError::NotFound(
                        anyhow::anyhow!("Tenant {} not found", req.tenant_id).into(),
                    ));
                }
            };

            if !configs.all_equal() {
                tracing::error!("Tenant configs for {} are mismatched. ", req.tenant_id);
                // This can't happen because we atomically update the database records
                // of all shards to the new value in [`Self::set_tenant_config_and_reconcile`].
                return Err(ApiError::InternalServerError(anyhow::anyhow!(
                    "Tenant configs for {} are mismatched",
                    req.tenant_id
                )));
            }

            first
        };

        let updated_config = base.apply_patch(patch);
        self.set_tenant_config_and_reconcile(tenant_id, updated_config)
            .await
    }

    pub(crate) async fn tenant_config_set(&self, req: TenantConfigRequest) -> Result<(), ApiError> {
        // We require an exclusive lock, because we are updating persistent and in-memory state
        let _tenant_lock = trace_exclusive_lock(
            &self.tenant_op_locks,
            req.tenant_id,
            TenantOperations::ConfigSet,
        )
        .await;

        self.maybe_load_tenant(req.tenant_id, &_tenant_lock).await?;

        self.set_tenant_config_and_reconcile(req.tenant_id, req.config)
            .await
    }

    async fn set_tenant_config_and_reconcile(
        &self,
        tenant_id: TenantId,
        config: TenantConfig,
    ) -> Result<(), ApiError> {
        self.persistence
            .update_tenant_shard(
                TenantFilter::Tenant(tenant_id),
                None,
                Some(config.clone()),
                None,
                None,
            )
            .await?;

        let waiters = {
            let mut waiters = Vec::new();
            let mut locked = self.inner.write().unwrap();
            let (nodes, tenants, _scheduler) = locked.parts_mut();
            for (_shard_id, shard) in tenants.range_mut(TenantShardId::tenant_range(tenant_id)) {
                shard.config = config.clone();
                if let Some(waiter) = self.maybe_reconcile_shard(shard, nodes) {
                    waiters.push(waiter);
                }
            }
            waiters
        };

        if let Err(e) = self.await_waiters(waiters, SHORT_RECONCILE_TIMEOUT).await {
            // Treat this as success because we have stored the configuration.  If e.g.
            // a node was unavailable at this time, it should not stop us accepting a
            // configuration change.
            tracing::warn!(%tenant_id, "Accepted configuration update but reconciliation failed: {e}");
        }

        Ok(())
    }

    pub(crate) fn tenant_config_get(
        &self,
        tenant_id: TenantId,
    ) -> Result<HashMap<&str, serde_json::Value>, ApiError> {
        let config = {
            let locked = self.inner.read().unwrap();

            match locked
                .tenants
                .range(TenantShardId::tenant_range(tenant_id))
                .next()
            {
                Some((_tenant_shard_id, shard)) => shard.config.clone(),
                None => {
                    return Err(ApiError::NotFound(
                        anyhow::anyhow!("Tenant not found").into(),
                    ))
                }
            }
        };

        // Unlike the pageserver, we do not have a set of global defaults: the config is
        // entirely per-tenant.  Therefore the distinction between `tenant_specific_overrides`
        // and `effective_config` in the response is meaningless, but we retain that syntax
        // in order to remain compatible with the pageserver API.

        let response = HashMap::from([
            (
                "tenant_specific_overrides",
                serde_json::to_value(&config)
                    .context("serializing tenant specific overrides")
                    .map_err(ApiError::InternalServerError)?,
            ),
            (
                "effective_config",
                serde_json::to_value(&config)
                    .context("serializing effective config")
                    .map_err(ApiError::InternalServerError)?,
            ),
        ]);

        Ok(response)
    }

    pub(crate) async fn tenant_time_travel_remote_storage(
        &self,
        time_travel_req: &TenantTimeTravelRequest,
        tenant_id: TenantId,
        timestamp: Cow<'_, str>,
        done_if_after: Cow<'_, str>,
    ) -> Result<(), ApiError> {
        let _tenant_lock = trace_exclusive_lock(
            &self.tenant_op_locks,
            tenant_id,
            TenantOperations::TimeTravelRemoteStorage,
        )
        .await;

        let node = {
            let mut locked = self.inner.write().unwrap();
            // Just a sanity check to prevent misuse: the API expects that the tenant is fully
            // detached everywhere, and nothing writes to S3 storage. Here, we verify that,
            // but only at the start of the process, so it's really just to prevent operator
            // mistakes.
            for (shard_id, shard) in locked.tenants.range(TenantShardId::tenant_range(tenant_id)) {
                if shard.intent.get_attached().is_some() || !shard.intent.get_secondary().is_empty()
                {
                    return Err(ApiError::InternalServerError(anyhow::anyhow!(
                        "We want tenant to be attached in shard with tenant_shard_id={shard_id}"
                    )));
                }
                let maybe_attached = shard
                    .observed
                    .locations
                    .iter()
                    .filter_map(|(node_id, observed_location)| {
                        observed_location
                            .conf
                            .as_ref()
                            .map(|loc| (node_id, observed_location, loc.mode))
                    })
                    .find(|(_, _, mode)| *mode != LocationConfigMode::Detached);
                if let Some((node_id, _observed_location, mode)) = maybe_attached {
                    return Err(ApiError::InternalServerError(anyhow::anyhow!("We observed attached={mode:?} tenant in node_id={node_id} shard with tenant_shard_id={shard_id}")));
                }
            }
            let scheduler = &mut locked.scheduler;
            // Right now we only perform the operation on a single node without parallelization
            // TODO fan out the operation to multiple nodes for better performance
            let node_id = scheduler.any_available_node()?;
            let node = locked
                .nodes
                .get(&node_id)
                .expect("Pageservers may not be deleted while lock is active");
            node.clone()
        };

        // The shard count is encoded in the remote storage's URL, so we need to handle all historically used shard counts
        let mut counts = time_travel_req
            .shard_counts
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        counts.sort_unstable();

        for count in counts {
            let shard_ids = (0..count.count())
                .map(|i| TenantShardId {
                    tenant_id,
                    shard_number: ShardNumber(i),
                    shard_count: count,
                })
                .collect::<Vec<_>>();
            for tenant_shard_id in shard_ids {
                let client = PageserverClient::new(
                    node.get_id(),
                    node.base_url(),
                    self.config.jwt_token.as_deref(),
                );

                tracing::info!("Doing time travel recovery for shard {tenant_shard_id}",);

                client
                    .tenant_time_travel_remote_storage(
                        tenant_shard_id,
                        &timestamp,
                        &done_if_after,
                    )
                    .await
                    .map_err(|e| {
                        ApiError::InternalServerError(anyhow::anyhow!(
                            "Error doing time travel recovery for shard {tenant_shard_id} on node {}: {e}",
                            node
                        ))
                    })?;
            }
        }
        Ok(())
    }

    pub(crate) async fn tenant_secondary_download(
        &self,
        tenant_id: TenantId,
        wait: Option<Duration>,
    ) -> Result<(StatusCode, SecondaryProgress), ApiError> {
        let _tenant_lock = trace_shared_lock(
            &self.tenant_op_locks,
            tenant_id,
            TenantOperations::SecondaryDownload,
        )
        .await;

        // Acquire lock and yield the collection of shard-node tuples which we will send requests onward to
        let targets = {
            let locked = self.inner.read().unwrap();
            let mut targets = Vec::new();

            for (tenant_shard_id, shard) in
                locked.tenants.range(TenantShardId::tenant_range(tenant_id))
            {
                for node_id in shard.intent.get_secondary() {
                    let node = locked
                        .nodes
                        .get(node_id)
                        .expect("Pageservers may not be deleted while referenced");

                    targets.push((*tenant_shard_id, node.clone()));
                }
            }
            targets
        };

        // Issue concurrent requests to all shards' locations
        let mut futs = FuturesUnordered::new();
        for (tenant_shard_id, node) in targets {
            let client = PageserverClient::new(
                node.get_id(),
                node.base_url(),
                self.config.jwt_token.as_deref(),
            );
            futs.push(async move {
                let result = client
                    .tenant_secondary_download(tenant_shard_id, wait)
                    .await;
                (result, node, tenant_shard_id)
            })
        }

        // Handle any errors returned by pageservers.  This includes cases like this request racing with
        // a scheduling operation, such that the tenant shard we're calling doesn't exist on that pageserver any more, as
        // well as more general cases like 503s, 500s, or timeouts.
        let mut aggregate_progress = SecondaryProgress::default();
        let mut aggregate_status: Option<StatusCode> = None;
        let mut error: Option<mgmt_api::Error> = None;
        while let Some((result, node, tenant_shard_id)) = futs.next().await {
            match result {
                Err(e) => {
                    // Secondary downloads are always advisory: if something fails, we nevertheless report success, so that whoever
                    // is calling us will proceed with whatever migration they're doing, albeit with a slightly less warm cache
                    // than they had hoped for.
                    tracing::warn!("Secondary download error from pageserver {node}: {e}",);
                    error = Some(e)
                }
                Ok((status_code, progress)) => {
                    tracing::info!(%tenant_shard_id, "Shard status={status_code} progress: {progress:?}");
                    aggregate_progress.layers_downloaded += progress.layers_downloaded;
                    aggregate_progress.layers_total += progress.layers_total;
                    aggregate_progress.bytes_downloaded += progress.bytes_downloaded;
                    aggregate_progress.bytes_total += progress.bytes_total;
                    aggregate_progress.heatmap_mtime =
                        std::cmp::max(aggregate_progress.heatmap_mtime, progress.heatmap_mtime);
                    aggregate_status = match aggregate_status {
                        None => Some(status_code),
                        Some(StatusCode::OK) => Some(status_code),
                        Some(cur) => {
                            // Other status codes (e.g. 202) -- do not overwrite.
                            Some(cur)
                        }
                    };
                }
            }
        }

        // If any of the shards return 202, indicate our result as 202.
        match aggregate_status {
            None => {
                match error {
                    Some(e) => {
                        // No successes, and an error: surface it
                        Err(ApiError::Conflict(format!("Error from pageserver: {e}")))
                    }
                    None => {
                        // No shards found
                        Err(ApiError::NotFound(
                            anyhow::anyhow!("Tenant {} not found", tenant_id).into(),
                        ))
                    }
                }
            }
            Some(aggregate_status) => Ok((aggregate_status, aggregate_progress)),
        }
    }

    pub(crate) async fn tenant_delete(&self, tenant_id: TenantId) -> Result<StatusCode, ApiError> {
        let _tenant_lock =
            trace_exclusive_lock(&self.tenant_op_locks, tenant_id, TenantOperations::Delete).await;

        self.maybe_load_tenant(tenant_id, &_tenant_lock).await?;

        self.persistence
            .mark_timelines_for_deletion(tenant_id)
            .await?;

        // Detach all shards. This also deletes local pageserver shard data.
        let (detach_waiters, node) = {
            let mut detach_waiters = Vec::new();
            let mut locked = self.inner.write().unwrap();
            let (nodes, tenants, scheduler) = locked.parts_mut();
            for (_, shard) in tenants.range_mut(TenantShardId::tenant_range(tenant_id)) {
                // Update the tenant's intent to remove all attachments
                shard.policy = PlacementPolicy::Detached;
                shard
                    .schedule(scheduler, &mut ScheduleContext::default())
                    .expect("De-scheduling is infallible");
                debug_assert!(shard.intent.get_attached().is_none());
                debug_assert!(shard.intent.get_secondary().is_empty());

                if let Some(waiter) = self.maybe_reconcile_shard(shard, nodes) {
                    detach_waiters.push(waiter);
                }
            }

            // Pick an arbitrary node to use for remote deletions (does not have to be where the tenant
            // was attached, just has to be able to see the S3 content)
            let node_id = scheduler.any_available_node()?;
            let node = nodes
                .get(&node_id)
                .expect("Pageservers may not be deleted while lock is active");
            (detach_waiters, node.clone())
        };

        // This reconcile wait can fail in a few ways:
        //  A there is a very long queue for the reconciler semaphore
        //  B some pageserver is failing to handle a detach promptly
        //  C some pageserver goes offline right at the moment we send it a request.
        //
        // A and C are transient: the semaphore will eventually become available, and once a node is marked offline
        // the next attempt to reconcile will silently skip detaches for an offline node and succeed.  If B happens,
        // it's a bug, and needs resolving at the pageserver level (we shouldn't just leave attachments behind while
        // deleting the underlying data).
        self.await_waiters(detach_waiters, RECONCILE_TIMEOUT)
            .await?;

        // Delete the entire tenant (all shards) from remote storage via a random pageserver.
        // Passing an unsharded tenant ID will cause the pageserver to remove all remote paths with
        // the tenant ID prefix, including all shards (even possibly stale ones).
        match node
            .with_client_retries(
                |client| async move {
                    client
                        .tenant_delete(TenantShardId::unsharded(tenant_id))
                        .await
                },
                &self.config.jwt_token,
                1,
                3,
                RECONCILE_TIMEOUT,
                &self.cancel,
            )
            .await
            .unwrap_or(Err(mgmt_api::Error::Cancelled))
        {
            Ok(_) => {}
            Err(mgmt_api::Error::Cancelled) => {
                return Err(ApiError::ShuttingDown);
            }
            Err(e) => {
                // This is unexpected: remote deletion should be infallible, unless the object store
                // at large is unavailable.
                tracing::error!("Error deleting via node {node}: {e}");
                return Err(ApiError::InternalServerError(anyhow::anyhow!(e)));
            }
        }

        // Fall through: deletion of the tenant on pageservers is complete, we may proceed to drop
        // our in-memory state and database state.

        // Ordering: we delete persistent state first: if we then
        // crash, we will drop the in-memory state.

        // Drop persistent state.
        self.persistence.delete_tenant(tenant_id).await?;

        // Drop in-memory state
        {
            let mut locked = self.inner.write().unwrap();
            let (_nodes, tenants, scheduler) = locked.parts_mut();

            // Dereference Scheduler from shards before dropping them
            for (_tenant_shard_id, shard) in
                tenants.range_mut(TenantShardId::tenant_range(tenant_id))
            {
                shard.intent.clear(scheduler);
            }

            tenants.retain(|tenant_shard_id, _shard| tenant_shard_id.tenant_id != tenant_id);
            tracing::info!(
                "Deleted tenant {tenant_id}, now have {} tenants",
                locked.tenants.len()
            );
        };

        // Success is represented as 404, to imitate the existing pageserver deletion API
        Ok(StatusCode::NOT_FOUND)
    }

    /// Naming: this configures the storage controller's policies for a tenant, whereas [`Self::tenant_config_set`] is "set the TenantConfig"
    /// for a tenant.  The TenantConfig is passed through to pageservers, whereas this function modifies
    /// the tenant's policies (configuration) within the storage controller
    pub(crate) async fn tenant_update_policy(
        &self,
        tenant_id: TenantId,
        req: TenantPolicyRequest,
    ) -> Result<(), ApiError> {
        // We require an exclusive lock, because we are updating persistent and in-memory state
        let _tenant_lock = trace_exclusive_lock(
            &self.tenant_op_locks,
            tenant_id,
            TenantOperations::UpdatePolicy,
        )
        .await;

        self.maybe_load_tenant(tenant_id, &_tenant_lock).await?;

        failpoint_support::sleep_millis_async!("tenant-update-policy-exclusive-lock");

        let TenantPolicyRequest {
            placement,
            mut scheduling,
        } = req;

        if let Some(PlacementPolicy::Detached | PlacementPolicy::Secondary) = placement {
            // When someone configures a tenant to detach, we force the scheduling policy to enable
            // this to take effect.
            if scheduling.is_none() {
                scheduling = Some(ShardSchedulingPolicy::Active);
            }
        }

        self.persistence
            .update_tenant_shard(
                TenantFilter::Tenant(tenant_id),
                placement.clone(),
                None,
                None,
                scheduling,
            )
            .await?;

        let mut schedule_context = ScheduleContext::default();
        let mut locked = self.inner.write().unwrap();
        let (nodes, tenants, scheduler) = locked.parts_mut();
        for (shard_id, shard) in tenants.range_mut(TenantShardId::tenant_range(tenant_id)) {
            if let Some(placement) = &placement {
                shard.policy = placement.clone();

                tracing::info!(tenant_id=%shard_id.tenant_id, shard_id=%shard_id.shard_slug(),
                               "Updated placement policy to {placement:?}");
            }

            if let Some(scheduling) = &scheduling {
                shard.set_scheduling_policy(*scheduling);

                tracing::info!(tenant_id=%shard_id.tenant_id, shard_id=%shard_id.shard_slug(),
                               "Updated scheduling policy to {scheduling:?}");
            }

            // In case scheduling is being switched back on, try it now.
            shard.schedule(scheduler, &mut schedule_context).ok();
            self.maybe_reconcile_shard(shard, nodes);
        }

        Ok(())
    }

    async fn tenant_timeline_create_pageservers(
        &self,
        tenant_id: TenantId,
        mut create_req: TimelineCreateRequest,
    ) -> Result<TimelineInfo, ApiError> {
        self.tenant_remote_mutation(tenant_id, move |mut targets| async move {
            if targets.0.is_empty() {
                return Err(ApiError::NotFound(
                    anyhow::anyhow!("Tenant not found").into(),
                ));
            };

            let (shard_zero_tid, shard_zero_locations) =
                targets.0.pop_first().expect("Must have at least one shard");
            assert!(shard_zero_tid.is_shard_zero());

            async fn create_one(
                tenant_shard_id: TenantShardId,
                locations: ShardMutationLocations,
                jwt: Option<String>,
                create_req: TimelineCreateRequest,
            ) -> Result<TimelineInfo, ApiError> {
                let latest = locations.latest.node;

                tracing::info!(
                    "Creating timeline on shard {}/{}, attached to node {latest} in generation {:?}",
                    tenant_shard_id,
                    create_req.new_timeline_id,
                    locations.latest.generation
                );

                let client =
                    PageserverClient::new(latest.get_id(), latest.base_url(), jwt.as_deref());

                let timeline_info = client
                    .timeline_create(tenant_shard_id, &create_req)
                    .await
                    .map_err(|e| passthrough_api_error(&latest, e))?;

                // We propagate timeline creations to all attached locations such that a compute
                // for the new timeline is able to start regardless of the current state of the
                // tenant shard reconciliation.
                for location in locations.other {
                    tracing::info!(
                        "Creating timeline on shard {}/{}, stale attached to node {} in generation {:?}",
                        tenant_shard_id,
                        create_req.new_timeline_id,
                        location.node,
                        location.generation
                    );

                    let client = PageserverClient::new(
                        location.node.get_id(),
                        location.node.base_url(),
                        jwt.as_deref(),
                    );

                    let res = client
                        .timeline_create(tenant_shard_id, &create_req)
                        .await;

                    if let Err(e) = res {
                        match e {
                            mgmt_api::Error::ApiError(StatusCode::NOT_FOUND, _) => {
                                // Tenant might have been detached from the stale location,
                                // so ignore 404s.
                            },
                            _ => {
                                return Err(passthrough_api_error(&location.node, e));
                            }
                        }
                    }
                }

                Ok(timeline_info)
            }

            // Because the caller might not provide an explicit LSN, we must do the creation first on a single shard, and then
            // use whatever LSN that shard picked when creating on subsequent shards.  We arbitrarily use shard zero as the shard
            // that will get the first creation request, and propagate the LSN to all the >0 shards.
            let timeline_info = create_one(
                shard_zero_tid,
                shard_zero_locations,
                self.config.jwt_token.clone(),
                create_req.clone(),
            )
            .await?;

            // Propagate the LSN that shard zero picked, if caller didn't provide one
            match &mut create_req.mode {
                models::TimelineCreateRequestMode::Branch { ancestor_start_lsn, .. } if ancestor_start_lsn.is_none() => {
                    *ancestor_start_lsn = timeline_info.ancestor_lsn;
                },
                _ => {}
            }

            // Create timeline on remaining shards with number >0
            if !targets.0.is_empty() {
                // If we had multiple shards, issue requests for the remainder now.
                let jwt = &self.config.jwt_token;
                self.tenant_for_shards(
                    targets
                        .0
                        .iter()
                        .map(|t| (*t.0, t.1.latest.node.clone()))
                        .collect(),
                    |tenant_shard_id: TenantShardId, _node: Node| {
                        let create_req = create_req.clone();
                        let mutation_locations = targets.0.remove(&tenant_shard_id).unwrap();
                        Box::pin(create_one(
                            tenant_shard_id,
                            mutation_locations,
                            jwt.clone(),
                            create_req,
                        ))
                    },
                )
                .await?;
            }

            Ok(timeline_info)
        }).await?
    }

    /// reconcile: create timeline on safekeepers
    ///
    /// Assumes tenant lock is held while calling this function
    async fn tenant_timeline_create_safekeepers_reconcile(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        timeline_persistence: &TimelinePersistence,
        status_creating: &TimelineStatusCreating,
        sk_persistences: &HashMap<i64, SafekeeperPersistence>,
    ) -> Result<(), ApiError> {
        // If quorum is reached, return if we are outside of a specified timeout
        let jwt = self.config.jwt_token.clone().map(SecretString::from);
        let mut joinset = JoinSet::new();

        let mut members = Vec::new();
        for sk in timeline_persistence.sk_set.iter() {
            let Some(sk_p) = sk_persistences.get(sk) else {
                return Err(ApiError::InternalServerError(anyhow!(
                    "couldn't find persisted entry for safekeeper with id {sk}"
                )))?;
            };
            members.push(SafekeeperId {
                id: NodeId(sk_p.id as u64),
                host: sk_p.host.clone(),
                pg_port: sk_p.port as u16,
            });
        }
        let mut mconf = safekeeper_api::membership::Configuration::empty();
        mconf.members = MemberSet::new(members).map_err(ApiError::InternalServerError)?;

        let req = safekeeper_api::models::TimelineCreateRequest {
            commit_lsn: None,
            mconf,
            pg_version: status_creating.pg_version,
            start_lsn: status_creating.start_lsn,
            system_id: None,
            tenant_id,
            timeline_id,
            wal_seg_size: None,
        };
        for sk in timeline_persistence.sk_set.iter() {
            // Unwrap is fine as we already would have returned error above
            let sk_p = sk_persistences.get(sk).unwrap();
            let sk_clone = NodeId(*sk as u64);
            let base_url = sk_p.base_url();
            let jwt = jwt.clone();
            let req = req.clone();
            let cancel = self.cancel.clone();
            joinset.spawn(async move {
                let client = SafekeeperClient::new(sk_clone, base_url, jwt);
                let req = req;
                let retry_result = backoff::retry(
                    || client.create_timeline(&req),
                    |_e| {
                        // TODO find right criteria here for deciding on retries
                        false
                    },
                    3,
                    5,
                    "create timeline on safekeeper",
                    &cancel,
                )
                .await;
                if let Some(res) = retry_result {
                    res.map_err(|e| {
                        ApiError::InternalServerError(
                            anyhow::Error::new(e).context("error creating timeline on safekeeper"),
                        )
                    })
                } else {
                    Err(ApiError::Cancelled)
                }
            });
        }
        // After we have built the joinset, we now wait for the tasks to complete,
        // but with a specified timeout to make sure we return swiftly, either with
        // a failure or success.
        const SK_CREATE_TIMELINE_RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);
        let reconcile_deadline = tokio::time::Instant::now() + SK_CREATE_TIMELINE_RECONCILE_TIMEOUT;

        // Treat the first two tasks to finish differently, mostly when they timeout,
        // because then we won't have a successful quorum.
        // For the third task, we don't rely on it succeeding, and we need this to support
        // continuing operations even if one safekeeper is down.
        let timeout_or_quorum = tokio::time::timeout_at(reconcile_deadline, async {
            (
                joinset.join_next().await.unwrap(),
                joinset.join_next().await.unwrap(),
            )
        })
        .await;
        let mut reconcile_results = Vec::new();
        match timeout_or_quorum {
            Ok((Ok(res_1), Ok(res_2))) => {
                reconcile_results.push(res_1);
                reconcile_results.push(res_2);
            }
            Ok((Err(_), Ok(_)) | (_, Err(_))) => {
                return Err(ApiError::InternalServerError(anyhow!(
                    "task was cancelled while reconciling timeline creation"
                )));
            }
            Err(_) => {
                return Err(ApiError::InternalServerError(anyhow!(
                    "couldn't reconcile timeline creation on safekeepers within timeout"
                )));
            }
        }
        let timeout_or_last =
            tokio::time::timeout_at(reconcile_deadline, joinset.join_next().map(Option::unwrap))
                .await;
        if let Ok(Ok(res)) = timeout_or_last {
            reconcile_results.push(res);
        } else {
            // No error if cancelled or timed out: we already have feedback from a quorum of safekeepers
            tracing::info!("timeout for third reconciliation");
        }
        // check now if quorum was reached in reconcile_results
        let successful = reconcile_results
            .into_iter()
            .filter_map(|res| res.ok())
            .collect::<Vec<_>>();
        tracing::info!(
            "Got {} successful results from reconciliation",
            successful.len()
        );
        let status_kind = if successful.len() < 2 {
            // Failure
            return Err(ApiError::InternalServerError(anyhow!(
                "not enough successful reconciliations to reach quorum, please retry: {}",
                successful.len()
            )));
        } else if successful.len() == 3 {
            // Success, state of timeline is Created
            TimelineStatusKind::Created
        } else if successful.len() == 2 {
            // Success, state of timeline remains Creating
            TimelineStatusKind::Creating
        } else {
            unreachable!(
                "unexpected number of successful reconciliations {}",
                successful.len()
            );
        };

        // notify cplane about creation
        // TODO

        self.persistence
            .update_timeline_status(tenant_id, timeline_id, status_kind, "{}".to_owned())
            .await?;
        Ok(())
    }

    async fn tenant_timeline_create_safekeepers(
        &self,
        tenant_id: TenantId,
        timeline_info: &TimelineInfo,
        create_mode: models::TimelineCreateRequestMode,
    ) -> Result<(SafekeeperGeneration, Vec<NodeId>), ApiError> {
        let timeline_id = timeline_info.timeline_id;
        let pg_version = timeline_info.pg_version;
        let start_lsn = match create_mode {
            models::TimelineCreateRequestMode::Bootstrap { .. } => timeline_info.last_record_lsn,
            models::TimelineCreateRequestMode::Branch { .. } => timeline_info.last_record_lsn,
            models::TimelineCreateRequestMode::ImportPgdata { .. } => {
                return Err(ApiError::InternalServerError(anyhow!(
                    "import pgdata doesn't specify the start lsn, aborting creation on safekeepers"
                )))?;
            }
        };

        // Choose initial set of safekeepers respecting affinity
        let sks = self.safekeepers_for_new_timeline().await?;
        let sks_persistence = sks.iter().map(|sk| sk.0 as i64).collect::<Vec<_>>();
        let status_creating = TimelineStatusCreating {
            pg_version,
            start_lsn,
        };
        let status = serde_json::to_string(&status_creating).unwrap();
        // Add timeline to db
        let timeline_persist = TimelinePersistence {
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            generation: 0,
            sk_set: sks_persistence.clone(),
            cplane_notified_generation: 0,
            status_kind: String::from(TimelineStatusKind::Creating),
            status,
        };
        self.persistence
            .insert_timeline(timeline_persist.clone())
            .await?;
        let sk_persistences = self
            .persistence
            .list_safekeepers()
            .await?
            .into_iter()
            .map(|p| (p.id, p))
            .collect::<HashMap<_, _>>();
        self.tenant_timeline_create_safekeepers_reconcile(
            tenant_id,
            timeline_id,
            &timeline_persist,
            &status_creating,
            &sk_persistences,
        )
        .await?;
        Ok((SafekeeperGeneration::new(0), sks))
    }

    pub(crate) async fn tenant_timeline_create(
        &self,
        tenant_id: TenantId,
        create_req: TimelineCreateRequest,
    ) -> Result<TimelineCreateResponseStorcon, ApiError> {
        let safekeepers = create_req.safekeepers.unwrap_or_default();
        tracing::info!(
            %safekeepers,
            "Creating timeline {}/{}",
            tenant_id,
            create_req.new_timeline_id,
        );

        let _tenant_lock = trace_shared_lock(
            &self.tenant_op_locks,
            tenant_id,
            TenantOperations::TimelineCreate,
        )
        .await;
        failpoint_support::sleep_millis_async!("tenant-create-timeline-shared-lock");
        let create_mode = create_req.mode.clone();

        let timeline_info = self
            .tenant_timeline_create_pageservers(tenant_id, create_req)
            .await?;

        let (safekeepers_generation, safekeepers) = if safekeepers {
            let res = self
                .tenant_timeline_create_safekeepers(tenant_id, &timeline_info, create_mode)
                .await?;
            (Some(res.0.into_inner()), Some(res.1))
        } else {
            (None, None)
        };

        Ok(TimelineCreateResponseStorcon {
            timeline_info,
            safekeepers_generation,
            safekeepers,
        })
    }

    pub(crate) async fn tenant_timeline_archival_config(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: TimelineArchivalConfigRequest,
    ) -> Result<(), ApiError> {
        tracing::info!(
            "Setting archival config of timeline {tenant_id}/{timeline_id} to '{:?}'",
            req.state
        );

        let _tenant_lock = trace_shared_lock(
            &self.tenant_op_locks,
            tenant_id,
            TenantOperations::TimelineArchivalConfig,
        )
        .await;

        self.tenant_remote_mutation(tenant_id, move |targets| async move {
            if targets.0.is_empty() {
                return Err(ApiError::NotFound(
                    anyhow::anyhow!("Tenant not found").into(),
                ));
            }
            async fn config_one(
                tenant_shard_id: TenantShardId,
                timeline_id: TimelineId,
                node: Node,
                jwt: Option<String>,
                req: TimelineArchivalConfigRequest,
            ) -> Result<(), ApiError> {
                tracing::info!(
                    "Setting archival config of timeline on shard {tenant_shard_id}/{timeline_id}, attached to node {node}",
                );

                let client = PageserverClient::new(node.get_id(), node.base_url(), jwt.as_deref());

                client
                    .timeline_archival_config(tenant_shard_id, timeline_id, &req)
                    .await
                    .map_err(|e| match e {
                        mgmt_api::Error::ApiError(StatusCode::PRECONDITION_FAILED, msg) => {
                            ApiError::PreconditionFailed(msg.into_boxed_str())
                        }
                        _ => passthrough_api_error(&node, e),
                    })
            }

            // no shard needs to go first/last; the operation should be idempotent
            // TODO: it would be great to ensure that all shards return the same error
            let locations = targets.0.iter().map(|t| (*t.0, t.1.latest.node.clone())).collect();
            let results = self
                .tenant_for_shards(locations, |tenant_shard_id, node| {
                    futures::FutureExt::boxed(config_one(
                        tenant_shard_id,
                        timeline_id,
                        node,
                        self.config.jwt_token.clone(),
                        req.clone(),
                    ))
                })
                .await?;
            assert!(!results.is_empty(), "must have at least one result");

            Ok(())
        }).await?
    }

    pub(crate) async fn tenant_timeline_detach_ancestor(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<models::detach_ancestor::AncestorDetached, ApiError> {
        tracing::info!("Detaching timeline {tenant_id}/{timeline_id}",);

        let _tenant_lock = trace_shared_lock(
            &self.tenant_op_locks,
            tenant_id,
            TenantOperations::TimelineDetachAncestor,
        )
        .await;

        self.tenant_remote_mutation(tenant_id, move |targets| async move {
            if targets.0.is_empty() {
                return Err(ApiError::NotFound(
                    anyhow::anyhow!("Tenant not found").into(),
                ));
            }

            async fn detach_one(
                tenant_shard_id: TenantShardId,
                timeline_id: TimelineId,
                node: Node,
                jwt: Option<String>,
            ) -> Result<(ShardNumber, models::detach_ancestor::AncestorDetached), ApiError> {
                tracing::info!(
                    "Detaching timeline on shard {tenant_shard_id}/{timeline_id}, attached to node {node}",
                );

                let client = PageserverClient::new(node.get_id(), node.base_url(), jwt.as_deref());

                client
                    .timeline_detach_ancestor(tenant_shard_id, timeline_id)
                    .await
                    .map_err(|e| {
                        use mgmt_api::Error;

                        match e {
                            // no ancestor (ever)
                            Error::ApiError(StatusCode::CONFLICT, msg) => ApiError::Conflict(format!(
                                "{node}: {}",
                                msg.strip_prefix("Conflict: ").unwrap_or(&msg)
                            )),
                            // too many ancestors
                            Error::ApiError(StatusCode::BAD_REQUEST, msg) => {
                                ApiError::BadRequest(anyhow::anyhow!("{node}: {msg}"))
                            }
                            Error::ApiError(StatusCode::INTERNAL_SERVER_ERROR, msg) => {
                                // avoid turning these into conflicts to remain compatible with
                                // pageservers, 500 errors are sadly retryable with timeline ancestor
                                // detach
                                ApiError::InternalServerError(anyhow::anyhow!("{node}: {msg}"))
                            }
                            // rest can be mapped as usual
                            other => passthrough_api_error(&node, other),
                        }
                    })
                    .map(|res| (tenant_shard_id.shard_number, res))
            }

            // no shard needs to go first/last; the operation should be idempotent
            let locations = targets.0.iter().map(|t| (*t.0, t.1.latest.node.clone())).collect();
            let mut results = self
                .tenant_for_shards(locations, |tenant_shard_id, node| {
                    futures::FutureExt::boxed(detach_one(
                        tenant_shard_id,
                        timeline_id,
                        node,
                        self.config.jwt_token.clone(),
                    ))
                })
                .await?;

            let any = results.pop().expect("we must have at least one response");

            let mismatching = results
                .iter()
                .filter(|(_, res)| res != &any.1)
                .collect::<Vec<_>>();
            if !mismatching.is_empty() {
                // this can be hit by races which should not happen because operation lock on cplane
                let matching = results.len() - mismatching.len();
                tracing::error!(
                    matching,
                    compared_against=?any,
                    ?mismatching,
                    "shards returned different results"
                );

                return Err(ApiError::InternalServerError(anyhow::anyhow!("pageservers returned mixed results for ancestor detach; manual intervention is required.")));
            }

            Ok(any.1)
        }).await?
    }

    pub(crate) async fn tenant_timeline_block_unblock_gc(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        dir: BlockUnblock,
    ) -> Result<(), ApiError> {
        let _tenant_lock = trace_shared_lock(
            &self.tenant_op_locks,
            tenant_id,
            TenantOperations::TimelineGcBlockUnblock,
        )
        .await;

        self.tenant_remote_mutation(tenant_id, move |targets| async move {
            if targets.0.is_empty() {
                return Err(ApiError::NotFound(
                    anyhow::anyhow!("Tenant not found").into(),
                ));
            }

            async fn do_one(
                tenant_shard_id: TenantShardId,
                timeline_id: TimelineId,
                node: Node,
                jwt: Option<String>,
                dir: BlockUnblock,
            ) -> Result<(), ApiError> {
                let client = PageserverClient::new(node.get_id(), node.base_url(), jwt.as_deref());

                client
                    .timeline_block_unblock_gc(tenant_shard_id, timeline_id, dir)
                    .await
                    .map_err(|e| passthrough_api_error(&node, e))
            }

            // no shard needs to go first/last; the operation should be idempotent
            let locations = targets
                .0
                .iter()
                .map(|t| (*t.0, t.1.latest.node.clone()))
                .collect();
            self.tenant_for_shards(locations, |tenant_shard_id, node| {
                futures::FutureExt::boxed(do_one(
                    tenant_shard_id,
                    timeline_id,
                    node,
                    self.config.jwt_token.clone(),
                    dir,
                ))
            })
            .await
        })
        .await??;
        Ok(())
    }

    /// Helper for concurrently calling a pageserver API on a number of shards, such as timeline creation.
    ///
    /// On success, the returned vector contains exactly the same number of elements as the input `locations`.
    async fn tenant_for_shards<F, R>(
        &self,
        locations: Vec<(TenantShardId, Node)>,
        mut req_fn: F,
    ) -> Result<Vec<R>, ApiError>
    where
        F: FnMut(
            TenantShardId,
            Node,
        )
            -> std::pin::Pin<Box<dyn futures::Future<Output = Result<R, ApiError>> + Send>>,
    {
        let mut futs = FuturesUnordered::new();
        let mut results = Vec::with_capacity(locations.len());

        for (tenant_shard_id, node) in locations {
            futs.push(req_fn(tenant_shard_id, node));
        }

        while let Some(r) = futs.next().await {
            results.push(r?);
        }

        Ok(results)
    }

    /// Concurrently invoke a pageserver API call on many shards at once
    pub(crate) async fn tenant_for_shards_api<T, O, F>(
        &self,
        locations: Vec<(TenantShardId, Node)>,
        op: O,
        warn_threshold: u32,
        max_retries: u32,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Vec<mgmt_api::Result<T>>
    where
        O: Fn(TenantShardId, PageserverClient) -> F + Copy,
        F: std::future::Future<Output = mgmt_api::Result<T>>,
    {
        let mut futs = FuturesUnordered::new();
        let mut results = Vec::with_capacity(locations.len());

        for (tenant_shard_id, node) in locations {
            futs.push(async move {
                node.with_client_retries(
                    |client| op(tenant_shard_id, client),
                    &self.config.jwt_token,
                    warn_threshold,
                    max_retries,
                    timeout,
                    cancel,
                )
                .await
            });
        }

        while let Some(r) = futs.next().await {
            let r = r.unwrap_or(Err(mgmt_api::Error::Cancelled));
            results.push(r);
        }

        results
    }

    /// Helper for safely working with the shards in a tenant remotely on pageservers, for example
    /// when creating and deleting timelines:
    /// - Makes sure shards are attached somewhere if they weren't already
    /// - Looks up the shards and the nodes where they were most recently attached
    /// - Guarantees that after the inner function returns, the shards' generations haven't moved on: this
    ///   ensures that the remote operation acted on the most recent generation, and is therefore durable.
    async fn tenant_remote_mutation<R, O, F>(
        &self,
        tenant_id: TenantId,
        op: O,
    ) -> Result<R, ApiError>
    where
        O: FnOnce(TenantMutationLocations) -> F,
        F: std::future::Future<Output = R>,
    {
        let mutation_locations = {
            let mut locations = TenantMutationLocations::default();

            // Load the currently attached pageservers for the latest generation of each shard.  This can
            // run concurrently with reconciliations, and it is not guaranteed that the node we find here
            // will still be the latest when we're done: we will check generations again at the end of
            // this function to handle that.
            let generations = self.persistence.tenant_generations(tenant_id).await?;

            if generations
                .iter()
                .any(|i| i.generation.is_none() || i.generation_pageserver.is_none())
            {
                let shard_generations = generations
                    .into_iter()
                    .map(|i| (i.tenant_shard_id, (i.generation, i.generation_pageserver)))
                    .collect::<HashMap<_, _>>();

                // One or more shards has not been attached to a pageserver.  Check if this is because it's configured
                // to be detached (409: caller should give up), or because it's meant to be attached but isn't yet (503: caller should retry)
                let locked = self.inner.read().unwrap();
                for (shard_id, shard) in
                    locked.tenants.range(TenantShardId::tenant_range(tenant_id))
                {
                    match shard.policy {
                        PlacementPolicy::Attached(_) => {
                            // This shard is meant to be attached: the caller is not wrong to try and
                            // use this function, but we can't service the request right now.
                            let Some(generation) = shard_generations.get(shard_id) else {
                                // This can only happen if there is a split brain controller modifying the database.  This should
                                // never happen when testing, and if it happens in production we can only log the issue.
                                debug_assert!(false);
                                tracing::error!("Shard {shard_id} not found in generation state!  Is another rogue controller running?");
                                continue;
                            };
                            let (generation, generation_pageserver) = generation;
                            if let Some(generation) = generation {
                                if generation_pageserver.is_none() {
                                    // This is legitimate only in a very narrow window where the shard was only just configured into
                                    // Attached mode after being created in Secondary or Detached mode, and it has had its generation
                                    // set but not yet had a Reconciler run (reconciler is the only thing that sets generation_pageserver).
                                    tracing::warn!("Shard {shard_id} generation is set ({generation:?}) but generation_pageserver is None, reconciler not run yet?");
                                }
                            } else {
                                // This should never happen: a shard with no generation is only permitted when it was created in some state
                                // other than PlacementPolicy::Attached (and generation is always written to DB before setting Attached in memory)
                                debug_assert!(false);
                                tracing::error!("Shard {shard_id} generation is None, but it is in PlacementPolicy::Attached mode!");
                                continue;
                            }
                        }
                        PlacementPolicy::Secondary | PlacementPolicy::Detached => {
                            return Err(ApiError::Conflict(format!(
                                "Shard {shard_id} tenant has policy {:?}",
                                shard.policy
                            )));
                        }
                    }
                }

                return Err(ApiError::ResourceUnavailable(
                    "One or more shards in tenant is not yet attached".into(),
                ));
            }

            let locked = self.inner.read().unwrap();
            for ShardGenerationState {
                tenant_shard_id,
                generation,
                generation_pageserver,
            } in generations
            {
                let node_id = generation_pageserver.expect("We checked for None above");
                let node = locked
                    .nodes
                    .get(&node_id)
                    .ok_or(ApiError::Conflict(format!(
                        "Raced with removal of node {node_id}"
                    )))?;
                let generation = generation.expect("Checked above");

                let tenant = locked.tenants.get(&tenant_shard_id);

                // TODO(vlad): Abstract the logic that finds stale attached locations
                // from observed state into a [`Service`] method.
                let other_locations = match tenant {
                    Some(tenant) => {
                        let mut other = tenant.attached_locations();
                        let latest_location_index =
                            other.iter().position(|&l| l == (node.get_id(), generation));
                        if let Some(idx) = latest_location_index {
                            other.remove(idx);
                        }

                        other
                    }
                    None => Vec::default(),
                };

                let location = ShardMutationLocations {
                    latest: MutationLocation {
                        node: node.clone(),
                        generation,
                    },
                    other: other_locations
                        .into_iter()
                        .filter_map(|(node_id, generation)| {
                            let node = locked.nodes.get(&node_id)?;

                            Some(MutationLocation {
                                node: node.clone(),
                                generation,
                            })
                        })
                        .collect(),
                };
                locations.0.insert(tenant_shard_id, location);
            }

            locations
        };

        let result = op(mutation_locations.clone()).await;

        // Post-check: are all the generations of all the shards the same as they were initially?  This proves that
        // our remote operation executed on the latest generation and is therefore persistent.
        {
            let latest_generations = self.persistence.tenant_generations(tenant_id).await?;
            if latest_generations
                .into_iter()
                .map(
                    |ShardGenerationState {
                         tenant_shard_id,
                         generation,
                         generation_pageserver: _,
                     }| (tenant_shard_id, generation),
                )
                .collect::<Vec<_>>()
                != mutation_locations
                    .0
                    .into_iter()
                    .map(|i| (i.0, Some(i.1.latest.generation)))
                    .collect::<Vec<_>>()
            {
                // We raced with something that incremented the generation, and therefore cannot be
                // confident that our actions are persistent (they might have hit an old generation).
                //
                // This is safe but requires a retry: ask the client to do that by giving them a 503 response.
                return Err(ApiError::ResourceUnavailable(
                    "Tenant attachment changed, please retry".into(),
                ));
            }
        }

        Ok(result)
    }

    async fn tenant_timeline_delete_safekeepers_reconcile(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        tl_p: &TimelinePersistence,
        sk_persistences: &HashMap<i64, SafekeeperPersistence>,
    ) -> Result<(), ApiError> {
        // If at least one deletion succeeded, return if we are outside of a specified timeout
        let jwt = self.config.jwt_token.clone().map(SecretString::from);
        let mut joinset = JoinSet::new();

        let mut members = Vec::new();
        for sk in tl_p.sk_set.iter() {
            let Some(sk_p) = sk_persistences.get(sk) else {
                return Err(ApiError::InternalServerError(anyhow!(
                    "couldn't find persisted entry for safekeeper with id {sk}"
                )))?;
            };
            members.push(SafekeeperId {
                id: NodeId(sk_p.id as u64),
                host: sk_p.host.clone(),
                pg_port: sk_p.port as u16,
            });
        }

        let sks_to_reconcile = &tl_p.sk_set;
        for sk in sks_to_reconcile.iter() {
            // Unwrap is fine as we already would have returned error above
            let sk_p = sk_persistences.get(sk).unwrap();
            let sk_clone = NodeId(*sk as u64);
            let base_url = sk_p.base_url();
            let jwt = jwt.clone();
            let cancel = self.cancel.clone();
            joinset.spawn(async move {
                let client = SafekeeperClient::new(sk_clone, base_url, jwt);
                let retry_result = backoff::retry(
                    || client.delete_timeline(tenant_id, timeline_id),
                    |_e| {
                        // TODO find right criteria here for deciding on retries
                        false
                    },
                    3,
                    5,
                    "delete timeline on safekeeper",
                    &cancel,
                )
                .await;
                if let Some(res) = retry_result {
                    res.map_err(|e| {
                        ApiError::InternalServerError(
                            anyhow::Error::new(e).context("error deleting timeline on safekeeper"),
                        )
                    })
                } else {
                    Err(ApiError::Cancelled)
                }
            });
        }
        // After we have built the joinset, we now wait for the tasks to complete,
        // but with a specified timeout to make sure we return swiftly, either with
        // a failure or success.
        const SK_DELETE_TIMELINE_RECONCILE_TIMEOUT: Duration = Duration::from_secs(30);
        let reconcile_deadline = tokio::time::Instant::now() + SK_DELETE_TIMELINE_RECONCILE_TIMEOUT;

        // Treat the first task to finish differently, mostly when it times out,
        // because then we won't have any successful deletion.
        // For the second and third task, we don't rely on them succeeding, and we need this to support
        // continuing operations even if a safekeeper is down.
        let timeout_or_first = tokio::time::timeout_at(reconcile_deadline, async {
            joinset.join_next().await.unwrap()
        })
        .await;
        let mut reconcile_results = Vec::new();
        match timeout_or_first {
            Ok(Ok(res_1)) => {
                reconcile_results.push(res_1);
            }
            Ok(Err(_)) => {
                return Err(ApiError::InternalServerError(anyhow!(
                    "task was cancelled while reconciling timeline deletion"
                )));
            }
            Err(_) => {
                return Err(ApiError::InternalServerError(anyhow!(
                    "couldn't reconcile timeline deletion on safekeepers within timeout"
                )));
            }
        }
        let timeout_or_last = tokio::time::timeout_at(reconcile_deadline, async {
            while let Some(next_res) = joinset.join_next().await {
                match next_res {
                    Ok(res) => {
                        reconcile_results.push(res);
                    }
                    Err(e) => {
                        tracing::info!("aborting reconciliation due to join error: {e:?}");
                        break;
                    }
                }
            }
        });
        if let Err(e) = timeout_or_last.await {
            // No error if cancelled or timed out: we already have feedback from a quorum of safekeepers
            tracing::info!(
                "timeout for last {} reconciliations: {e}",
                sks_to_reconcile.len() - 1
            );
        }
        // check now if quorum was reached in reconcile_results
        let successful = reconcile_results
            .into_iter()
            .filter_map(|res| res.ok())
            .collect::<Vec<_>>();
        tracing::info!(
            "Got {} successful results from reconciliation",
            successful.len()
        );
        let new_status_kind = if successful.is_empty() {
            // Failure
            return Err(ApiError::InternalServerError(anyhow!(
                "not enough successful reconciliations to reach quorum, please retry: {}",
                successful.len()
            )));
        } else if successful.len() == sks_to_reconcile.len() {
            // Success, state of timeline is Deleted
            TimelineStatusKind::Deleted
        } else if successful.len() == 2 {
            // Success, state of timeline remains Creating
            TimelineStatusKind::Deleting
        } else {
            unreachable!(
                "unexpected number of successful reconciliations {}",
                successful.len()
            );
        };

        if new_status_kind == TimelineStatusKind::Deleted {
            self.persistence
                .update_timeline_status_deleted(tenant_id, timeline_id)
                .await?;
        }
        Ok(())
    }
    async fn tenant_timeline_delete_safekeepers(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<(), ApiError> {
        let tl = self
            .persistence
            .get_timeline(tenant_id, timeline_id)
            .await?;
        let Some(tl) = tl else {
            tracing::info!("timeline {tenant_id}/{timeline_id} doesn't exist in timelines table, no deletions on safekeepers needed");
            return Ok(());
        };
        let status_kind =
            TimelineStatusKind::from_str(&tl.status_kind).map_err(ApiError::InternalServerError)?;
        if status_kind != TimelineStatusKind::Deleting {
            // Set status to deleting
            let new_status_kind = TimelineStatusKind::Deleting;
            self.persistence
                .update_timeline_status(tenant_id, timeline_id, new_status_kind, "{}".to_owned())
                .await?;
        }
        let sk_persistences = self
            .persistence
            .list_safekeepers()
            .await?
            .into_iter()
            .map(|p| (p.id, p))
            .collect::<HashMap<_, _>>();
        self.tenant_timeline_delete_safekeepers_reconcile(
            tenant_id,
            timeline_id,
            &tl,
            &sk_persistences,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn tenant_timeline_delete(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<StatusCode, ApiError> {
        tracing::info!("Deleting timeline {}/{}", tenant_id, timeline_id,);
        let _tenant_lock = trace_shared_lock(
            &self.tenant_op_locks,
            tenant_id,
            TenantOperations::TimelineDelete,
        )
        .await;

        let ps_fut = self.tenant_remote_mutation(tenant_id, move |mut targets| async move {
            if targets.0.is_empty() {
                return Err(ApiError::NotFound(
                    anyhow::anyhow!("Tenant not found").into(),
                ));
            }

            let (shard_zero_tid, shard_zero_locations) = targets.0.pop_first().expect("Must have at least one shard");
            assert!(shard_zero_tid.is_shard_zero());

            async fn delete_one(
                tenant_shard_id: TenantShardId,
                timeline_id: TimelineId,
                node: Node,
                jwt: Option<String>,
            ) -> Result<StatusCode, ApiError> {
                tracing::info!(
                    "Deleting timeline on shard {tenant_shard_id}/{timeline_id}, attached to node {node}",
                );

                let client = PageserverClient::new(node.get_id(), node.base_url(), jwt.as_deref());
                let res = client
                    .timeline_delete(tenant_shard_id, timeline_id)
                    .await;

                match res {
                    Ok(ok) => Ok(ok),
                    Err(mgmt_api::Error::ApiError(StatusCode::CONFLICT, _)) => Ok(StatusCode::CONFLICT),
                    Err(mgmt_api::Error::ApiError(StatusCode::SERVICE_UNAVAILABLE, msg)) => Err(ApiError::ResourceUnavailable(msg.into())),
                    Err(e) => {
                        Err(
                            ApiError::InternalServerError(anyhow::anyhow!(
                                "Error deleting timeline {timeline_id} on {tenant_shard_id} on node {node}: {e}",
                            ))
                        )
                    }
                }
            }

            let locations = targets.0.iter().map(|t| (*t.0, t.1.latest.node.clone())).collect();
            let statuses = self
                .tenant_for_shards(locations, |tenant_shard_id: TenantShardId, node: Node| {
                    Box::pin(delete_one(
                        tenant_shard_id,
                        timeline_id,
                        node,
                        self.config.jwt_token.clone(),
                    ))
                })
                .await?;

            // If any shards >0 haven't finished deletion yet, don't start deletion on shard zero.
            // We return 409 (Conflict) if deletion was already in progress on any of the shards
            // and 202 (Accepted) if deletion was not already in progress on any of the shards.
            if statuses.iter().any(|s| s == &StatusCode::CONFLICT) {
                return Ok(StatusCode::CONFLICT);
            }

            if statuses.iter().any(|s| s != &StatusCode::NOT_FOUND) {
                return Ok(StatusCode::ACCEPTED);
            }

            // Delete shard zero last: this is not strictly necessary, but since a caller's GET on a timeline will be routed
            // to shard zero, it gives a more obvious behavior that a GET returns 404 once the deletion is done.
            let shard_zero_status = delete_one(
                shard_zero_tid,
                timeline_id,
                shard_zero_locations.latest.node,
                self.config.jwt_token.clone(),
            )
            .await?;
            Ok(shard_zero_status)
        });

        let sk_fut = self.tenant_timeline_delete_safekeepers(tenant_id, timeline_id);

        let (ps_res, sk_res) = tokio::join!(ps_fut, sk_fut);
        sk_res?;
        ps_res?
    }

    /// When you need to send an HTTP request to the pageserver that holds shard0 of a tenant, this
    /// function looks up and returns node. If the tenant isn't found, returns Err(ApiError::NotFound)
    pub(crate) async fn tenant_shard0_node(
        &self,
        tenant_id: TenantId,
    ) -> Result<(Node, TenantShardId), ApiError> {
        // Look up in-memory state and maybe use the node from there.
        {
            let locked = self.inner.read().unwrap();
            let Some((tenant_shard_id, shard)) = locked
                .tenants
                .range(TenantShardId::tenant_range(tenant_id))
                .next()
            else {
                return Err(ApiError::NotFound(
                    anyhow::anyhow!("Tenant {tenant_id} not found").into(),
                ));
            };

            let Some(intent_node_id) = shard.intent.get_attached() else {
                tracing::warn!(
                    tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(),
                    "Shard not scheduled (policy {:?}), cannot generate pass-through URL",
                    shard.policy
                );
                return Err(ApiError::Conflict(
                    "Cannot call timeline API on non-attached tenant".to_string(),
                ));
            };

            if shard.reconciler.is_none() {
                // Optimization: while no reconcile is in flight, we may trust our in-memory state
                // to tell us which pageserver to use. Otherwise we will fall through and hit the database
                let Some(node) = locked.nodes.get(intent_node_id) else {
                    // This should never happen
                    return Err(ApiError::InternalServerError(anyhow::anyhow!(
                        "Shard refers to nonexistent node"
                    )));
                };
                return Ok((node.clone(), *tenant_shard_id));
            }
        };

        // Look up the latest attached pageserver location from the database
        // generation state: this will reflect the progress of any ongoing migration.
        // Note that it is not guaranteed to _stay_ here, our caller must still handle
        // the case where they call through to the pageserver and get a 404.
        let db_result = self.persistence.tenant_generations(tenant_id).await?;
        let Some(ShardGenerationState {
            tenant_shard_id,
            generation: _,
            generation_pageserver: Some(node_id),
        }) = db_result.first()
        else {
            // This can happen if we raced with a tenant deletion or a shard split.  On a retry
            // the caller will either succeed (shard split case), get a proper 404 (deletion case),
            // or a conflict response (case where tenant was detached in background)
            return Err(ApiError::ResourceUnavailable(
                "Shard {} not found in database, or is not attached".into(),
            ));
        };
        let locked = self.inner.read().unwrap();
        let Some(node) = locked.nodes.get(node_id) else {
            // This should never happen
            return Err(ApiError::InternalServerError(anyhow::anyhow!(
                "Shard refers to nonexistent node"
            )));
        };

        Ok((node.clone(), *tenant_shard_id))
    }

    pub(crate) fn tenant_locate(
        &self,
        tenant_id: TenantId,
    ) -> Result<TenantLocateResponse, ApiError> {
        let locked = self.inner.read().unwrap();
        tracing::info!("Locating shards for tenant {tenant_id}");

        let mut result = Vec::new();
        let mut shard_params: Option<ShardParameters> = None;

        for (tenant_shard_id, shard) in locked.tenants.range(TenantShardId::tenant_range(tenant_id))
        {
            let node_id =
                shard
                    .intent
                    .get_attached()
                    .ok_or(ApiError::BadRequest(anyhow::anyhow!(
                        "Cannot locate a tenant that is not attached"
                    )))?;

            let node = locked
                .nodes
                .get(&node_id)
                .expect("Pageservers may not be deleted while referenced");

            result.push(node.shard_location(*tenant_shard_id));

            match &shard_params {
                None => {
                    shard_params = Some(ShardParameters {
                        stripe_size: shard.shard.stripe_size,
                        count: shard.shard.count,
                    });
                }
                Some(params) => {
                    if params.stripe_size != shard.shard.stripe_size {
                        // This should never happen.  We enforce at runtime because it's simpler than
                        // adding an extra per-tenant data structure to store the things that should be the same
                        return Err(ApiError::InternalServerError(anyhow::anyhow!(
                            "Inconsistent shard stripe size parameters!"
                        )));
                    }
                }
            }
        }

        if result.is_empty() {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("No shards for this tenant ID found").into(),
            ));
        }
        let shard_params = shard_params.expect("result is non-empty, therefore this is set");
        tracing::info!(
            "Located tenant {} with params {:?} on shards {}",
            tenant_id,
            shard_params,
            result
                .iter()
                .map(|s| format!("{:?}", s))
                .collect::<Vec<_>>()
                .join(",")
        );

        Ok(TenantLocateResponse {
            shards: result,
            shard_params,
        })
    }

    /// Returns None if the input iterator of shards does not include a shard with number=0
    fn tenant_describe_impl<'a>(
        &self,
        shards: impl Iterator<Item = &'a TenantShard>,
    ) -> Option<TenantDescribeResponse> {
        let mut shard_zero = None;
        let mut describe_shards = Vec::new();

        for shard in shards {
            if shard.tenant_shard_id.is_shard_zero() {
                shard_zero = Some(shard);
            }

            describe_shards.push(TenantDescribeResponseShard {
                tenant_shard_id: shard.tenant_shard_id,
                node_attached: *shard.intent.get_attached(),
                node_secondary: shard.intent.get_secondary().to_vec(),
                last_error: shard
                    .last_error
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|e| format!("{e}"))
                    .unwrap_or("".to_string())
                    .clone(),
                is_reconciling: shard.reconciler.is_some(),
                is_pending_compute_notification: shard.pending_compute_notification,
                is_splitting: matches!(shard.splitting, SplitState::Splitting),
                scheduling_policy: *shard.get_scheduling_policy(),
                preferred_az_id: shard.preferred_az().map(ToString::to_string),
            })
        }

        let shard_zero = shard_zero?;

        Some(TenantDescribeResponse {
            tenant_id: shard_zero.tenant_shard_id.tenant_id,
            shards: describe_shards,
            stripe_size: shard_zero.shard.stripe_size,
            policy: shard_zero.policy.clone(),
            config: shard_zero.config.clone(),
        })
    }

    pub(crate) fn tenant_describe(
        &self,
        tenant_id: TenantId,
    ) -> Result<TenantDescribeResponse, ApiError> {
        let locked = self.inner.read().unwrap();

        self.tenant_describe_impl(
            locked
                .tenants
                .range(TenantShardId::tenant_range(tenant_id))
                .map(|(_k, v)| v),
        )
        .ok_or_else(|| ApiError::NotFound(anyhow::anyhow!("Tenant {tenant_id} not found").into()))
    }

    /// limit & offset are pagination parameters. Since we are walking an in-memory HashMap, `offset` does not
    /// avoid traversing data, it just avoid returning it. This is suitable for our purposes, since our in memory
    /// maps are small enough to traverse fast, our pagination is just to avoid serializing huge JSON responses
    /// in our external API.
    pub(crate) fn tenant_list(
        &self,
        limit: Option<usize>,
        start_after: Option<TenantId>,
    ) -> Vec<TenantDescribeResponse> {
        let locked = self.inner.read().unwrap();

        // Apply start_from parameter
        let shard_range = match start_after {
            None => locked.tenants.range(..),
            Some(tenant_id) => locked.tenants.range(
                TenantShardId {
                    tenant_id,
                    shard_number: ShardNumber(u8::MAX),
                    shard_count: ShardCount(u8::MAX),
                }..,
            ),
        };

        let mut result = Vec::new();
        for (_tenant_id, tenant_shards) in &shard_range.group_by(|(id, _shard)| id.tenant_id) {
            result.push(
                self.tenant_describe_impl(tenant_shards.map(|(_k, v)| v))
                    .expect("Groups are always non-empty"),
            );

            // Enforce `limit` parameter
            if let Some(limit) = limit {
                if result.len() >= limit {
                    break;
                }
            }
        }

        result
    }

    #[instrument(skip_all, fields(tenant_id=%op.tenant_id))]
    async fn abort_tenant_shard_split(
        &self,
        op: &TenantShardSplitAbort,
    ) -> Result<(), TenantShardSplitAbortError> {
        // Cleaning up a split:
        // - Parent shards are not destroyed during a split, just detached.
        // - Failed pageserver split API calls can leave the remote node with just the parent attached,
        //   just the children attached, or both.
        //
        // Therefore our work to do is to:
        // 1. Clean up storage controller's internal state to just refer to parents, no children
        // 2. Call out to pageservers to ensure that children are detached
        // 3. Call out to pageservers to ensure that parents are attached.
        //
        // Crash safety:
        // - If the storage controller stops running during this cleanup *after* clearing the splitting state
        //   from our database, then [`Self::startup_reconcile`] will regard child attachments as garbage
        //   and detach them.
        // - TODO: If the storage controller stops running during this cleanup *before* clearing the splitting state
        //   from our database, then we will re-enter this cleanup routine on startup.

        let TenantShardSplitAbort {
            tenant_id,
            new_shard_count,
            new_stripe_size,
            ..
        } = op;

        // First abort persistent state, if any exists.
        match self
            .persistence
            .abort_shard_split(*tenant_id, *new_shard_count)
            .await?
        {
            AbortShardSplitStatus::Aborted => {
                // Proceed to roll back any child shards created on pageservers
            }
            AbortShardSplitStatus::Complete => {
                // The split completed (we might hit that path if e.g. our database transaction
                // to write the completion landed in the database, but we dropped connection
                // before seeing the result).
                //
                // We must update in-memory state to reflect the successful split.
                self.tenant_shard_split_commit_inmem(
                    *tenant_id,
                    *new_shard_count,
                    *new_stripe_size,
                );
                return Ok(());
            }
        }

        // Clean up in-memory state, and accumulate the list of child locations that need detaching
        let detach_locations: Vec<(Node, TenantShardId)> = {
            let mut detach_locations = Vec::new();
            let mut locked = self.inner.write().unwrap();
            let (nodes, tenants, scheduler) = locked.parts_mut();

            for (tenant_shard_id, shard) in
                tenants.range_mut(TenantShardId::tenant_range(op.tenant_id))
            {
                if shard.shard.count == op.new_shard_count {
                    // Surprising: the phase of [`Self::do_tenant_shard_split`] which inserts child shards in-memory
                    // is infallible, so if we got an error we shouldn't have got that far.
                    tracing::warn!(
                        "During split abort, child shard {tenant_shard_id} found in-memory"
                    );
                    continue;
                }

                // Add the children of this shard to this list of things to detach
                if let Some(node_id) = shard.intent.get_attached() {
                    for child_id in tenant_shard_id.split(*new_shard_count) {
                        detach_locations.push((
                            nodes
                                .get(node_id)
                                .expect("Intent references nonexistent node")
                                .clone(),
                            child_id,
                        ));
                    }
                } else {
                    tracing::warn!(
                        "During split abort, shard {tenant_shard_id} has no attached location"
                    );
                }

                tracing::info!("Restoring parent shard {tenant_shard_id}");

                // Drop any intents that refer to unavailable nodes, to enable this abort to proceed even
                // if the original attachment location is offline.
                if let Some(node_id) = shard.intent.get_attached() {
                    if !nodes.get(node_id).unwrap().is_available() {
                        tracing::info!("Demoting attached intent for {tenant_shard_id} on unavailable node {node_id}");
                        shard.intent.demote_attached(scheduler, *node_id);
                    }
                }
                for node_id in shard.intent.get_secondary().clone() {
                    if !nodes.get(&node_id).unwrap().is_available() {
                        tracing::info!("Dropping secondary intent for {tenant_shard_id} on unavailable node {node_id}");
                        shard.intent.remove_secondary(scheduler, node_id);
                    }
                }

                shard.splitting = SplitState::Idle;
                if let Err(e) = shard.schedule(scheduler, &mut ScheduleContext::default()) {
                    // If this shard can't be scheduled now (perhaps due to offline nodes or
                    // capacity issues), that must not prevent us rolling back a split.  In this
                    // case it should be eventually scheduled in the background.
                    tracing::warn!("Failed to schedule {tenant_shard_id} during shard abort: {e}")
                }

                self.maybe_reconcile_shard(shard, nodes);
            }

            // We don't expect any new_shard_count shards to exist here, but drop them just in case
            tenants.retain(|_id, s| s.shard.count != *new_shard_count);

            detach_locations
        };

        for (node, child_id) in detach_locations {
            if !node.is_available() {
                // An unavailable node cannot be cleaned up now: to avoid blocking forever, we will permit this, and
                // rely on the reconciliation that happens when a node transitions to Active to clean up. Since we have
                // removed child shards from our in-memory state and database, the reconciliation will implicitly remove
                // them from the node.
                tracing::warn!("Node {node} unavailable, can't clean up during split abort. It will be cleaned up when it is reactivated.");
                continue;
            }

            // Detach the remote child.  If the pageserver split API call is still in progress, this call will get
            // a 503 and retry, up to our limit.
            tracing::info!("Detaching {child_id} on {node}...");
            match node
                .with_client_retries(
                    |client| async move {
                        let config = LocationConfig {
                            mode: LocationConfigMode::Detached,
                            generation: None,
                            secondary_conf: None,
                            shard_number: child_id.shard_number.0,
                            shard_count: child_id.shard_count.literal(),
                            // Stripe size and tenant config don't matter when detaching
                            shard_stripe_size: 0,
                            tenant_conf: TenantConfig::default(),
                        };

                        client.location_config(child_id, config, None, false).await
                    },
                    &self.config.jwt_token,
                    1,
                    10,
                    Duration::from_secs(5),
                    &self.cancel,
                )
                .await
            {
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    // We failed to communicate with the remote node.  This is problematic: we may be
                    // leaving it with a rogue child shard.
                    tracing::warn!(
                        "Failed to detach child {child_id} from node {node} during abort"
                    );
                    return Err(e.into());
                }
                None => {
                    // Cancellation: we were shutdown or the node went offline. Shutdown is fine, we'll
                    // clean up on restart. The node going offline requires a retry.
                    return Err(TenantShardSplitAbortError::Unavailable);
                }
            };
        }

        tracing::info!("Successfully aborted split");
        Ok(())
    }

    /// Infallible final stage of [`Self::tenant_shard_split`]: update the contents
    /// of the tenant map to reflect the child shards that exist after the split.
    fn tenant_shard_split_commit_inmem(
        &self,
        tenant_id: TenantId,
        new_shard_count: ShardCount,
        new_stripe_size: Option<ShardStripeSize>,
    ) -> (
        TenantShardSplitResponse,
        Vec<(TenantShardId, NodeId, ShardStripeSize)>,
        Vec<ReconcilerWaiter>,
    ) {
        let mut response = TenantShardSplitResponse {
            new_shards: Vec::new(),
        };
        let mut child_locations = Vec::new();
        let mut waiters = Vec::new();

        {
            let mut locked = self.inner.write().unwrap();

            let parent_ids = locked
                .tenants
                .range(TenantShardId::tenant_range(tenant_id))
                .map(|(shard_id, _)| *shard_id)
                .collect::<Vec<_>>();

            let (nodes, tenants, scheduler) = locked.parts_mut();
            for parent_id in parent_ids {
                let child_ids = parent_id.split(new_shard_count);

                let (pageserver, generation, policy, parent_ident, config, preferred_az) = {
                    let mut old_state = tenants
                        .remove(&parent_id)
                        .expect("It was present, we just split it");

                    // A non-splitting state is impossible, because [`Self::tenant_shard_split`] holds
                    // a TenantId lock and passes it through to [`TenantShardSplitAbort`] in case of cleanup:
                    // nothing else can clear this.
                    assert!(matches!(old_state.splitting, SplitState::Splitting));

                    let old_attached = old_state.intent.get_attached().unwrap();
                    old_state.intent.clear(scheduler);
                    let generation = old_state.generation.expect("Shard must have been attached");
                    (
                        old_attached,
                        generation,
                        old_state.policy.clone(),
                        old_state.shard,
                        old_state.config.clone(),
                        old_state.preferred_az().cloned(),
                    )
                };

                let mut schedule_context = ScheduleContext::default();
                for child in child_ids {
                    let mut child_shard = parent_ident;
                    child_shard.number = child.shard_number;
                    child_shard.count = child.shard_count;
                    if let Some(stripe_size) = new_stripe_size {
                        child_shard.stripe_size = stripe_size;
                    }

                    let mut child_observed: HashMap<NodeId, ObservedStateLocation> = HashMap::new();
                    child_observed.insert(
                        pageserver,
                        ObservedStateLocation {
                            conf: Some(attached_location_conf(
                                generation,
                                &child_shard,
                                &config,
                                &policy,
                            )),
                        },
                    );

                    let mut child_state =
                        TenantShard::new(child, child_shard, policy.clone(), preferred_az.clone());
                    child_state.intent =
                        IntentState::single(scheduler, Some(pageserver), preferred_az.clone());
                    child_state.observed = ObservedState {
                        locations: child_observed,
                    };
                    child_state.generation = Some(generation);
                    child_state.config = config.clone();

                    // The child's TenantShard::splitting is intentionally left at the default value of Idle,
                    // as at this point in the split process we have succeeded and this part is infallible:
                    // we will never need to do any special recovery from this state.

                    child_locations.push((child, pageserver, child_shard.stripe_size));

                    if let Err(e) = child_state.schedule(scheduler, &mut schedule_context) {
                        // This is not fatal, because we've implicitly already got an attached
                        // location for the child shard.  Failure here just means we couldn't
                        // find a secondary (e.g. because cluster is overloaded).
                        tracing::warn!("Failed to schedule child shard {child}: {e}");
                    }
                    // In the background, attach secondary locations for the new shards
                    if let Some(waiter) = self.maybe_reconcile_shard(&mut child_state, nodes) {
                        waiters.push(waiter);
                    }

                    tenants.insert(child, child_state);
                    response.new_shards.push(child);
                }
            }
            (response, child_locations, waiters)
        }
    }

    async fn tenant_shard_split_start_secondaries(
        &self,
        tenant_id: TenantId,
        waiters: Vec<ReconcilerWaiter>,
    ) {
        // Wait for initial reconcile of child shards, this creates the secondary locations
        if let Err(e) = self.await_waiters(waiters, RECONCILE_TIMEOUT).await {
            // This is not a failure to split: it's some issue reconciling the new child shards, perhaps
            // their secondaries couldn't be attached.
            tracing::warn!("Failed to reconcile after split: {e}");
            return;
        }

        // Take the state lock to discover the attached & secondary intents for all shards
        let (attached, secondary) = {
            let locked = self.inner.read().unwrap();
            let mut attached = Vec::new();
            let mut secondary = Vec::new();

            for (tenant_shard_id, shard) in
                locked.tenants.range(TenantShardId::tenant_range(tenant_id))
            {
                let Some(node_id) = shard.intent.get_attached() else {
                    // Unexpected.  Race with a PlacementPolicy change?
                    tracing::warn!(
                        "No attached node on {tenant_shard_id} immediately after shard split!"
                    );
                    continue;
                };

                let Some(secondary_node_id) = shard.intent.get_secondary().first() else {
                    // No secondary location.  Nothing for us to do.
                    continue;
                };

                let attached_node = locked
                    .nodes
                    .get(node_id)
                    .expect("Pageservers may not be deleted while referenced");

                let secondary_node = locked
                    .nodes
                    .get(secondary_node_id)
                    .expect("Pageservers may not be deleted while referenced");

                attached.push((*tenant_shard_id, attached_node.clone()));
                secondary.push((*tenant_shard_id, secondary_node.clone()));
            }
            (attached, secondary)
        };

        if secondary.is_empty() {
            // No secondary locations; nothing for us to do
            return;
        }

        for result in self
            .tenant_for_shards_api(
                attached,
                |tenant_shard_id, client| async move {
                    client.tenant_heatmap_upload(tenant_shard_id).await
                },
                1,
                1,
                SHORT_RECONCILE_TIMEOUT,
                &self.cancel,
            )
            .await
        {
            if let Err(e) = result {
                tracing::warn!("Error calling heatmap upload after shard split: {e}");
                return;
            }
        }

        for result in self
            .tenant_for_shards_api(
                secondary,
                |tenant_shard_id, client| async move {
                    client
                        .tenant_secondary_download(tenant_shard_id, Some(Duration::ZERO))
                        .await
                },
                1,
                1,
                SHORT_RECONCILE_TIMEOUT,
                &self.cancel,
            )
            .await
        {
            if let Err(e) = result {
                tracing::warn!("Error calling secondary download after shard split: {e}");
                return;
            }
        }
    }

    pub(crate) async fn tenant_shard_split(
        &self,
        tenant_id: TenantId,
        split_req: TenantShardSplitRequest,
    ) -> Result<TenantShardSplitResponse, ApiError> {
        // TODO: return 503 if we get stuck waiting for this lock
        // (issue https://github.com/neondatabase/neon/issues/7108)
        let _tenant_lock = trace_exclusive_lock(
            &self.tenant_op_locks,
            tenant_id,
            TenantOperations::ShardSplit,
        )
        .await;

        let new_shard_count = ShardCount::new(split_req.new_shard_count);
        let new_stripe_size = split_req.new_stripe_size;

        // Validate the request and construct parameters.  This phase is fallible, but does not require
        // rollback on errors, as it does no I/O and mutates no state.
        let shard_split_params = match self.prepare_tenant_shard_split(tenant_id, split_req)? {
            ShardSplitAction::NoOp(resp) => return Ok(resp),
            ShardSplitAction::Split(params) => params,
        };

        // Execute this split: this phase mutates state and does remote I/O on pageservers.  If it fails,
        // we must roll back.
        let r = self
            .do_tenant_shard_split(tenant_id, shard_split_params)
            .await;

        let (response, waiters) = match r {
            Ok(r) => r,
            Err(e) => {
                // Split might be part-done, we must do work to abort it.
                tracing::warn!("Enqueuing background abort of split on {tenant_id}");
                self.abort_tx
                    .send(TenantShardSplitAbort {
                        tenant_id,
                        new_shard_count,
                        new_stripe_size,
                        _tenant_lock,
                    })
                    // Ignore error sending: that just means we're shutting down: aborts are ephemeral so it's fine to drop it.
                    .ok();
                return Err(e);
            }
        };

        // The split is now complete.  As an optimization, we will trigger all the child shards to upload
        // a heatmap immediately, and all their secondary locations to start downloading: this avoids waiting
        // for the background heatmap/download interval before secondaries get warm enough to migrate shards
        // in [`Self::optimize_all`]
        self.tenant_shard_split_start_secondaries(tenant_id, waiters)
            .await;
        Ok(response)
    }

    fn prepare_tenant_shard_split(
        &self,
        tenant_id: TenantId,
        split_req: TenantShardSplitRequest,
    ) -> Result<ShardSplitAction, ApiError> {
        fail::fail_point!("shard-split-validation", |_| Err(ApiError::BadRequest(
            anyhow::anyhow!("failpoint")
        )));

        let mut policy = None;
        let mut config = None;
        let mut shard_ident = None;
        let mut preferred_az_id = None;
        // Validate input, and calculate which shards we will create
        let (old_shard_count, targets) =
            {
                let locked = self.inner.read().unwrap();

                let pageservers = locked.nodes.clone();

                let mut targets = Vec::new();

                // In case this is a retry, count how many already-split shards we found
                let mut children_found = Vec::new();
                let mut old_shard_count = None;

                for (tenant_shard_id, shard) in
                    locked.tenants.range(TenantShardId::tenant_range(tenant_id))
                {
                    match shard.shard.count.count().cmp(&split_req.new_shard_count) {
                        Ordering::Equal => {
                            //  Already split this
                            children_found.push(*tenant_shard_id);
                            continue;
                        }
                        Ordering::Greater => {
                            return Err(ApiError::BadRequest(anyhow::anyhow!(
                                "Requested count {} but already have shards at count {}",
                                split_req.new_shard_count,
                                shard.shard.count.count()
                            )));
                        }
                        Ordering::Less => {
                            // Fall through: this shard has lower count than requested,
                            // is a candidate for splitting.
                        }
                    }

                    match old_shard_count {
                        None => old_shard_count = Some(shard.shard.count),
                        Some(old_shard_count) => {
                            if old_shard_count != shard.shard.count {
                                // We may hit this case if a caller asked for two splits to
                                // different sizes, before the first one is complete.
                                // e.g. 1->2, 2->4, where the 4 call comes while we have a mixture
                                // of shard_count=1 and shard_count=2 shards in the map.
                                return Err(ApiError::Conflict(
                                    "Cannot split, currently mid-split".to_string(),
                                ));
                            }
                        }
                    }
                    if policy.is_none() {
                        policy = Some(shard.policy.clone());
                    }
                    if shard_ident.is_none() {
                        shard_ident = Some(shard.shard);
                    }
                    if config.is_none() {
                        config = Some(shard.config.clone());
                    }
                    if preferred_az_id.is_none() {
                        preferred_az_id = shard.preferred_az().cloned();
                    }

                    if tenant_shard_id.shard_count.count() == split_req.new_shard_count {
                        tracing::info!(
                            "Tenant shard {} already has shard count {}",
                            tenant_shard_id,
                            split_req.new_shard_count
                        );
                        continue;
                    }

                    let node_id = shard.intent.get_attached().ok_or(ApiError::BadRequest(
                        anyhow::anyhow!("Cannot split a tenant that is not attached"),
                    ))?;

                    let node = pageservers
                        .get(&node_id)
                        .expect("Pageservers may not be deleted while referenced");

                    targets.push(ShardSplitTarget {
                        parent_id: *tenant_shard_id,
                        node: node.clone(),
                        child_ids: tenant_shard_id
                            .split(ShardCount::new(split_req.new_shard_count)),
                    });
                }

                if targets.is_empty() {
                    if children_found.len() == split_req.new_shard_count as usize {
                        return Ok(ShardSplitAction::NoOp(TenantShardSplitResponse {
                            new_shards: children_found,
                        }));
                    } else {
                        // No shards found to split, and no existing children found: the
                        // tenant doesn't exist at all.
                        return Err(ApiError::NotFound(
                            anyhow::anyhow!("Tenant {} not found", tenant_id).into(),
                        ));
                    }
                }

                (old_shard_count, targets)
            };

        // unwrap safety: we would have returned above if we didn't find at least one shard to split
        let old_shard_count = old_shard_count.unwrap();
        let shard_ident = if let Some(new_stripe_size) = split_req.new_stripe_size {
            // This ShardIdentity will be used as the template for all children, so this implicitly
            // applies the new stripe size to the children.
            let mut shard_ident = shard_ident.unwrap();
            if shard_ident.count.count() > 1 && shard_ident.stripe_size != new_stripe_size {
                return Err(ApiError::BadRequest(anyhow::anyhow!("Attempted to change stripe size ({:?}->{new_stripe_size:?}) on a tenant with multiple shards", shard_ident.stripe_size)));
            }

            shard_ident.stripe_size = new_stripe_size;
            tracing::info!("applied  stripe size {}", shard_ident.stripe_size.0);
            shard_ident
        } else {
            shard_ident.unwrap()
        };
        let policy = policy.unwrap();
        let config = config.unwrap();

        Ok(ShardSplitAction::Split(Box::new(ShardSplitParams {
            old_shard_count,
            new_shard_count: ShardCount::new(split_req.new_shard_count),
            new_stripe_size: split_req.new_stripe_size,
            targets,
            policy,
            config,
            shard_ident,
            preferred_az_id,
        })))
    }

    async fn do_tenant_shard_split(
        &self,
        tenant_id: TenantId,
        params: Box<ShardSplitParams>,
    ) -> Result<(TenantShardSplitResponse, Vec<ReconcilerWaiter>), ApiError> {
        // FIXME: we have dropped self.inner lock, and not yet written anything to the database: another
        // request could occur here, deleting or mutating the tenant.  begin_shard_split checks that the
        // parent shards exist as expected, but it would be neater to do the above pre-checks within the
        // same database transaction rather than pre-check in-memory and then maybe-fail the database write.
        // (https://github.com/neondatabase/neon/issues/6676)

        let ShardSplitParams {
            old_shard_count,
            new_shard_count,
            new_stripe_size,
            mut targets,
            policy,
            config,
            shard_ident,
            preferred_az_id,
        } = *params;

        // Drop any secondary locations: pageservers do not support splitting these, and in any case the
        // end-state for a split tenant will usually be to have secondary locations on different nodes.
        // The reconciliation calls in this block also implicitly cancel+barrier wrt any ongoing reconciliation
        // at the time of split.
        let waiters = {
            let mut locked = self.inner.write().unwrap();
            let mut waiters = Vec::new();
            let (nodes, tenants, scheduler) = locked.parts_mut();
            for target in &mut targets {
                let Some(shard) = tenants.get_mut(&target.parent_id) else {
                    // Paranoia check: this shouldn't happen: we have the oplock for this tenant ID.
                    return Err(ApiError::InternalServerError(anyhow::anyhow!(
                        "Shard {} not found",
                        target.parent_id
                    )));
                };

                if shard.intent.get_attached() != &Some(target.node.get_id()) {
                    // Paranoia check: this shouldn't happen: we have the oplock for this tenant ID.
                    return Err(ApiError::Conflict(format!(
                        "Shard {} unexpectedly rescheduled during split",
                        target.parent_id
                    )));
                }

                // Irrespective of PlacementPolicy, clear secondary locations from intent
                shard.intent.clear_secondary(scheduler);

                // Run Reconciler to execute detach fo secondary locations.
                if let Some(waiter) = self.maybe_reconcile_shard(shard, nodes) {
                    waiters.push(waiter);
                }
            }
            waiters
        };
        self.await_waiters(waiters, RECONCILE_TIMEOUT).await?;

        // Before creating any new child shards in memory or on the pageservers, persist them: this
        // enables us to ensure that we will always be able to clean up if something goes wrong.  This also
        // acts as the protection against two concurrent attempts to split: one of them will get a database
        // error trying to insert the child shards.
        let mut child_tsps = Vec::new();
        for target in &targets {
            let mut this_child_tsps = Vec::new();
            for child in &target.child_ids {
                let mut child_shard = shard_ident;
                child_shard.number = child.shard_number;
                child_shard.count = child.shard_count;

                tracing::info!(
                    "Create child shard persistence with stripe size {}",
                    shard_ident.stripe_size.0
                );

                this_child_tsps.push(TenantShardPersistence {
                    tenant_id: child.tenant_id.to_string(),
                    shard_number: child.shard_number.0 as i32,
                    shard_count: child.shard_count.literal() as i32,
                    shard_stripe_size: shard_ident.stripe_size.0 as i32,
                    // Note: this generation is a placeholder, [`Persistence::begin_shard_split`] will
                    // populate the correct generation as part of its transaction, to protect us
                    // against racing with changes in the state of the parent.
                    generation: None,
                    generation_pageserver: Some(target.node.get_id().0 as i64),
                    placement_policy: serde_json::to_string(&policy).unwrap(),
                    config: serde_json::to_string(&config).unwrap(),
                    splitting: SplitState::Splitting,

                    // Scheduling policies and preferred AZ do not carry through to children
                    scheduling_policy: serde_json::to_string(&ShardSchedulingPolicy::default())
                        .unwrap(),
                    preferred_az_id: preferred_az_id.as_ref().map(|az| az.0.clone()),
                });
            }

            child_tsps.push((target.parent_id, this_child_tsps));
        }

        if let Err(e) = self
            .persistence
            .begin_shard_split(old_shard_count, tenant_id, child_tsps)
            .await
        {
            match e {
                DatabaseError::Query(diesel::result::Error::DatabaseError(
                    DatabaseErrorKind::UniqueViolation,
                    _,
                )) => {
                    // Inserting a child shard violated a unique constraint: we raced with another call to
                    // this function
                    tracing::warn!("Conflicting attempt to split {tenant_id}: {e}");
                    return Err(ApiError::Conflict("Tenant is already splitting".into()));
                }
                _ => return Err(ApiError::InternalServerError(e.into())),
            }
        }
        fail::fail_point!("shard-split-post-begin", |_| Err(
            ApiError::InternalServerError(anyhow::anyhow!("failpoint"))
        ));

        // Now that I have persisted the splitting state, apply it in-memory.  This is infallible, so
        // callers may assume that if splitting is set in memory, then it was persisted, and if splitting
        // is not set in memory, then it was not persisted.
        {
            let mut locked = self.inner.write().unwrap();
            for target in &targets {
                if let Some(parent_shard) = locked.tenants.get_mut(&target.parent_id) {
                    parent_shard.splitting = SplitState::Splitting;
                    // Put the observed state to None, to reflect that it is indeterminate once we start the
                    // split operation.
                    parent_shard
                        .observed
                        .locations
                        .insert(target.node.get_id(), ObservedStateLocation { conf: None });
                }
            }
        }

        // TODO: issue split calls concurrently (this only matters once we're splitting
        // N>1 shards into M shards -- initially we're usually splitting 1 shard into N).

        for target in &targets {
            let ShardSplitTarget {
                parent_id,
                node,
                child_ids,
            } = target;
            let client = PageserverClient::new(
                node.get_id(),
                node.base_url(),
                self.config.jwt_token.as_deref(),
            );
            let response = client
                .tenant_shard_split(
                    *parent_id,
                    TenantShardSplitRequest {
                        new_shard_count: new_shard_count.literal(),
                        new_stripe_size,
                    },
                )
                .await
                .map_err(|e| ApiError::Conflict(format!("Failed to split {}: {}", parent_id, e)))?;

            fail::fail_point!("shard-split-post-remote", |_| Err(ApiError::Conflict(
                "failpoint".to_string()
            )));

            failpoint_support::sleep_millis_async!("shard-split-post-remote-sleep", &self.cancel);

            tracing::info!(
                "Split {} into {}",
                parent_id,
                response
                    .new_shards
                    .iter()
                    .map(|s| format!("{:?}", s))
                    .collect::<Vec<_>>()
                    .join(",")
            );

            if &response.new_shards != child_ids {
                // This should never happen: the pageserver should agree with us on how shard splits work.
                return Err(ApiError::InternalServerError(anyhow::anyhow!(
                    "Splitting shard {} resulted in unexpected IDs: {:?} (expected {:?})",
                    parent_id,
                    response.new_shards,
                    child_ids
                )));
            }
        }

        // TODO: if the pageserver restarted concurrently with our split API call,
        // the actual generation of the child shard might differ from the generation
        // we expect it to have.  In order for our in-database generation to end up
        // correct, we should carry the child generation back in the response and apply it here
        // in complete_shard_split (and apply the correct generation in memory)
        // (or, we can carry generation in the request and reject the request if
        //  it doesn't match, but that requires more retry logic on this side)

        self.persistence
            .complete_shard_split(tenant_id, old_shard_count)
            .await?;

        fail::fail_point!("shard-split-post-complete", |_| Err(
            ApiError::InternalServerError(anyhow::anyhow!("failpoint"))
        ));

        // Replace all the shards we just split with their children: this phase is infallible.
        let (response, child_locations, waiters) =
            self.tenant_shard_split_commit_inmem(tenant_id, new_shard_count, new_stripe_size);

        // Send compute notifications for all the new shards
        let mut failed_notifications = Vec::new();
        for (child_id, child_ps, stripe_size) in child_locations {
            if let Err(e) = self
                .compute_hook
                .notify(
                    compute_hook::ShardUpdate {
                        tenant_shard_id: child_id,
                        node_id: child_ps,
                        stripe_size,
                        preferred_az: preferred_az_id.as_ref().map(Cow::Borrowed),
                    },
                    &self.cancel,
                )
                .await
            {
                tracing::warn!("Failed to update compute of {}->{} during split, proceeding anyway to complete split ({e})",
                        child_id, child_ps);
                failed_notifications.push(child_id);
            }
        }

        // If we failed any compute notifications, make a note to retry later.
        if !failed_notifications.is_empty() {
            let mut locked = self.inner.write().unwrap();
            for failed in failed_notifications {
                if let Some(shard) = locked.tenants.get_mut(&failed) {
                    shard.pending_compute_notification = true;
                }
            }
        }

        Ok((response, waiters))
    }

    pub(crate) async fn tenant_shard_migrate(
        &self,
        tenant_shard_id: TenantShardId,
        migrate_req: TenantShardMigrateRequest,
    ) -> Result<TenantShardMigrateResponse, ApiError> {
        let waiter = {
            let mut locked = self.inner.write().unwrap();
            let (nodes, tenants, scheduler) = locked.parts_mut();

            let Some(node) = nodes.get(&migrate_req.node_id) else {
                return Err(ApiError::BadRequest(anyhow::anyhow!(
                    "Node {} not found",
                    migrate_req.node_id
                )));
            };

            if !node.is_available() {
                // Warn but proceed: the caller may intend to manually adjust the placement of
                // a shard even if the node is down, e.g. if intervening during an incident.
                tracing::warn!("Migrating to unavailable node {node}");
            }

            let Some(shard) = tenants.get_mut(&tenant_shard_id) else {
                return Err(ApiError::NotFound(
                    anyhow::anyhow!("Tenant shard not found").into(),
                ));
            };

            if shard.intent.get_attached() == &Some(migrate_req.node_id) {
                // No-op case: we will still proceed to wait for reconciliation in case it is
                // incomplete from an earlier update to the intent.
                tracing::info!("Migrating: intent is unchanged {:?}", shard.intent);
            } else {
                let old_attached = *shard.intent.get_attached();

                match shard.policy {
                    PlacementPolicy::Attached(n) => {
                        // If our new attached node was a secondary, it no longer should be.
                        shard.intent.remove_secondary(scheduler, migrate_req.node_id);

                        shard.intent.set_attached(scheduler, Some(migrate_req.node_id));

                        // If we were already attached to something, demote that to a secondary
                        if let Some(old_attached) = old_attached {
                            if n > 0 {
                                // Remove other secondaries to make room for the location we'll demote
                                while shard.intent.get_secondary().len() >= n {
                                    shard.intent.pop_secondary(scheduler);
                                }

                                shard.intent.push_secondary(scheduler, old_attached);
                            }
                        }
                    }
                    PlacementPolicy::Secondary => {
                        shard.intent.clear(scheduler);
                        shard.intent.push_secondary(scheduler, migrate_req.node_id);
                    }
                    PlacementPolicy::Detached => {
                        return Err(ApiError::BadRequest(anyhow::anyhow!(
                            "Cannot migrate a tenant that is PlacementPolicy::Detached: configure it to an attached policy first"
                        )))
                    }
                }

                tracing::info!("Migrating: new intent {:?}", shard.intent);
                shard.sequence = shard.sequence.next();
            }

            self.maybe_reconcile_shard(shard, nodes)
        };

        if let Some(waiter) = waiter {
            waiter.wait_timeout(RECONCILE_TIMEOUT).await?;
        } else {
            tracing::info!("Migration is a no-op");
        }

        Ok(TenantShardMigrateResponse {})
    }

    pub(crate) async fn tenant_shard_migrate_secondary(
        &self,
        tenant_shard_id: TenantShardId,
        migrate_req: TenantShardMigrateRequest,
    ) -> Result<TenantShardMigrateResponse, ApiError> {
        let waiter = {
            let mut locked = self.inner.write().unwrap();
            let (nodes, tenants, scheduler) = locked.parts_mut();

            let Some(node) = nodes.get(&migrate_req.node_id) else {
                return Err(ApiError::BadRequest(anyhow::anyhow!(
                    "Node {} not found",
                    migrate_req.node_id
                )));
            };

            if !node.is_available() {
                // Warn but proceed: the caller may intend to manually adjust the placement of
                // a shard even if the node is down, e.g. if intervening during an incident.
                tracing::warn!("Migrating to unavailable node {node}");
            }

            let Some(shard) = tenants.get_mut(&tenant_shard_id) else {
                return Err(ApiError::NotFound(
                    anyhow::anyhow!("Tenant shard not found").into(),
                ));
            };

            if shard.intent.get_secondary().len() == 1
                && shard.intent.get_secondary()[0] == migrate_req.node_id
            {
                tracing::info!(
                    "Migrating secondary to {node}: intent is unchanged {:?}",
                    shard.intent
                );
            } else if shard.intent.get_attached() == &Some(migrate_req.node_id) {
                tracing::info!("Migrating secondary to {node}: already attached where we were asked to create a secondary");
            } else {
                let old_secondaries = shard.intent.get_secondary().clone();
                for secondary in old_secondaries {
                    shard.intent.remove_secondary(scheduler, secondary);
                }

                shard.intent.push_secondary(scheduler, migrate_req.node_id);
                shard.sequence = shard.sequence.next();
                tracing::info!(
                    "Migrating secondary to {node}: new intent {:?}",
                    shard.intent
                );
            }

            self.maybe_reconcile_shard(shard, nodes)
        };

        if let Some(waiter) = waiter {
            waiter.wait_timeout(RECONCILE_TIMEOUT).await?;
        } else {
            tracing::info!("Migration is a no-op");
        }

        Ok(TenantShardMigrateResponse {})
    }

    /// 'cancel' in this context means cancel any ongoing reconcile
    pub(crate) async fn tenant_shard_cancel_reconcile(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<(), ApiError> {
        // Take state lock and fire the cancellation token, after which we drop lock and wait for any ongoing reconcile to complete
        let waiter = {
            let locked = self.inner.write().unwrap();
            let Some(shard) = locked.tenants.get(&tenant_shard_id) else {
                return Err(ApiError::NotFound(
                    anyhow::anyhow!("Tenant shard not found").into(),
                ));
            };

            let waiter = shard.get_waiter();
            match waiter {
                None => {
                    tracing::info!("Shard does not have an ongoing Reconciler");
                    return Ok(());
                }
                Some(waiter) => {
                    tracing::info!("Cancelling Reconciler");
                    shard.cancel_reconciler();
                    waiter
                }
            }
        };

        // Cancellation should be prompt.  If this fails we have still done our job of firing the
        // cancellation token, but by returning an ApiError we will indicate to the caller that
        // the Reconciler is misbehaving and not respecting the cancellation token
        self.await_waiters(vec![waiter], SHORT_RECONCILE_TIMEOUT)
            .await?;

        Ok(())
    }

    /// This is for debug/support only: we simply drop all state for a tenant, without
    /// detaching or deleting it on pageservers.
    pub(crate) async fn tenant_drop(&self, tenant_id: TenantId) -> Result<(), ApiError> {
        self.persistence.delete_tenant(tenant_id).await?;

        let mut locked = self.inner.write().unwrap();
        let (_nodes, tenants, scheduler) = locked.parts_mut();
        let mut shards = Vec::new();
        for (tenant_shard_id, _) in tenants.range(TenantShardId::tenant_range(tenant_id)) {
            shards.push(*tenant_shard_id);
        }

        for shard_id in shards {
            if let Some(mut shard) = tenants.remove(&shard_id) {
                shard.intent.clear(scheduler);
            }
        }

        Ok(())
    }

    /// This is for debug/support only: assuming tenant data is already present in S3, we "create" a
    /// tenant with a very high generation number so that it will see the existing data.
    pub(crate) async fn tenant_import(
        &self,
        tenant_id: TenantId,
    ) -> Result<TenantCreateResponse, ApiError> {
        // Pick an arbitrary available pageserver to use for scanning the tenant in remote storage
        let maybe_node = {
            self.inner
                .read()
                .unwrap()
                .nodes
                .values()
                .find(|n| n.is_available())
                .cloned()
        };
        let Some(node) = maybe_node else {
            return Err(ApiError::BadRequest(anyhow::anyhow!("No nodes available")));
        };

        let client = PageserverClient::new(
            node.get_id(),
            node.base_url(),
            self.config.jwt_token.as_deref(),
        );

        let scan_result = client
            .tenant_scan_remote_storage(tenant_id)
            .await
            .map_err(|e| passthrough_api_error(&node, e))?;

        // A post-split tenant may contain a mixture of shard counts in remote storage: pick the highest count.
        let Some(shard_count) = scan_result
            .shards
            .iter()
            .map(|s| s.tenant_shard_id.shard_count)
            .max()
        else {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("No shards found").into(),
            ));
        };

        // Ideally we would set each newly imported shard's generation independently, but for correctness it is sufficient
        // to
        let generation = scan_result
            .shards
            .iter()
            .map(|s| s.generation)
            .max()
            .expect("We already validated >0 shards");

        // FIXME: we have no way to recover the shard stripe size from contents of remote storage: this will
        // only work if they were using the default stripe size.
        let stripe_size = ShardParameters::DEFAULT_STRIPE_SIZE;

        let (response, waiters) = self
            .do_tenant_create(TenantCreateRequest {
                new_tenant_id: TenantShardId::unsharded(tenant_id),
                generation,

                shard_parameters: ShardParameters {
                    count: shard_count,
                    stripe_size,
                },
                placement_policy: Some(PlacementPolicy::Attached(0)), // No secondaries, for convenient debug/hacking
                config: TenantConfig::default(),
            })
            .await?;

        if let Err(e) = self.await_waiters(waiters, SHORT_RECONCILE_TIMEOUT).await {
            // Since this is a debug/support operation, all kinds of weird issues are possible (e.g. this
            // tenant doesn't exist in the control plane), so don't fail the request if it can't fully
            // reconcile, as reconciliation includes notifying compute.
            tracing::warn!(%tenant_id, "Reconcile not done yet while importing tenant ({e})");
        }

        Ok(response)
    }

    /// For debug/support: a full JSON dump of TenantShards.  Returns a response so that
    /// we don't have to make TenantShard clonable in the return path.
    pub(crate) fn tenants_dump(&self) -> Result<hyper::Response<hyper::Body>, ApiError> {
        let serialized = {
            let locked = self.inner.read().unwrap();
            let result = locked.tenants.values().collect::<Vec<_>>();
            serde_json::to_string(&result).map_err(|e| ApiError::InternalServerError(e.into()))?
        };

        hyper::Response::builder()
            .status(hyper::StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(hyper::Body::from(serialized))
            .map_err(|e| ApiError::InternalServerError(e.into()))
    }

    /// Check the consistency of in-memory state vs. persistent state, and check that the
    /// scheduler's statistics are up to date.
    ///
    /// These consistency checks expect an **idle** system.  If changes are going on while
    /// we run, then we can falsely indicate a consistency issue.  This is sufficient for end-of-test
    /// checks, but not suitable for running continuously in the background in the field.
    pub(crate) async fn consistency_check(&self) -> Result<(), ApiError> {
        let (mut expect_nodes, mut expect_shards) = {
            let locked = self.inner.read().unwrap();

            locked
                .scheduler
                .consistency_check(locked.nodes.values(), locked.tenants.values())
                .context("Scheduler checks")
                .map_err(ApiError::InternalServerError)?;

            let expect_nodes = locked
                .nodes
                .values()
                .map(|n| n.to_persistent())
                .collect::<Vec<_>>();

            let expect_shards = locked
                .tenants
                .values()
                .map(|t| t.to_persistent())
                .collect::<Vec<_>>();

            // This method can only validate the state of an idle system: if a reconcile is in
            // progress, fail out early to avoid giving false errors on state that won't match
            // between database and memory under a ReconcileResult is processed.
            for t in locked.tenants.values() {
                if t.reconciler.is_some() {
                    return Err(ApiError::InternalServerError(anyhow::anyhow!(
                        "Shard {} reconciliation in progress",
                        t.tenant_shard_id
                    )));
                }
            }

            (expect_nodes, expect_shards)
        };

        let mut nodes = self.persistence.list_nodes().await?;
        expect_nodes.sort_by_key(|n| n.node_id);
        nodes.sort_by_key(|n| n.node_id);

        // Errors relating to nodes are deferred so that we don't skip the shard checks below if we have a node error
        let node_result = if nodes != expect_nodes {
            tracing::error!("Consistency check failed on nodes.");
            tracing::error!(
                "Nodes in memory: {}",
                serde_json::to_string(&expect_nodes)
                    .map_err(|e| ApiError::InternalServerError(e.into()))?
            );
            tracing::error!(
                "Nodes in database: {}",
                serde_json::to_string(&nodes)
                    .map_err(|e| ApiError::InternalServerError(e.into()))?
            );
            Err(ApiError::InternalServerError(anyhow::anyhow!(
                "Node consistency failure"
            )))
        } else {
            Ok(())
        };

        let mut persistent_shards = self.persistence.load_active_tenant_shards().await?;
        persistent_shards
            .sort_by_key(|tsp| (tsp.tenant_id.clone(), tsp.shard_number, tsp.shard_count));

        expect_shards.sort_by_key(|tsp| (tsp.tenant_id.clone(), tsp.shard_number, tsp.shard_count));

        // Because JSON contents of persistent tenants might disagree with the fields in current `TenantConfig`
        // definition, we will do an encode/decode cycle to ensure any legacy fields are dropped and any new
        // fields are added, before doing a comparison.
        for tsp in &mut persistent_shards {
            let config: TenantConfig = serde_json::from_str(&tsp.config)
                .map_err(|e| ApiError::InternalServerError(e.into()))?;
            tsp.config = serde_json::to_string(&config).expect("Encoding config is infallible");
        }

        if persistent_shards != expect_shards {
            tracing::error!("Consistency check failed on shards.");

            tracing::error!(
                "Shards in memory: {}",
                serde_json::to_string(&expect_shards)
                    .map_err(|e| ApiError::InternalServerError(e.into()))?
            );
            tracing::error!(
                "Shards in database: {}",
                serde_json::to_string(&persistent_shards)
                    .map_err(|e| ApiError::InternalServerError(e.into()))?
            );

            // The total dump log lines above are useful in testing but in the field grafana will
            // usually just drop them because they're so large. So we also do some explicit logging
            // of just the diffs.
            let persistent_shards = persistent_shards
                .into_iter()
                .map(|tsp| (tsp.get_tenant_shard_id().unwrap(), tsp))
                .collect::<HashMap<_, _>>();
            let expect_shards = expect_shards
                .into_iter()
                .map(|tsp| (tsp.get_tenant_shard_id().unwrap(), tsp))
                .collect::<HashMap<_, _>>();
            for (tenant_shard_id, persistent_tsp) in &persistent_shards {
                match expect_shards.get(tenant_shard_id) {
                    None => {
                        tracing::error!(
                            "Shard {} found in database but not in memory",
                            tenant_shard_id
                        );
                    }
                    Some(expect_tsp) => {
                        if expect_tsp != persistent_tsp {
                            tracing::error!(
                                "Shard {} is inconsistent.  In memory: {}, database has: {}",
                                tenant_shard_id,
                                serde_json::to_string(expect_tsp).unwrap(),
                                serde_json::to_string(&persistent_tsp).unwrap()
                            );
                        }
                    }
                }
            }

            // Having already logged any differences, log any shards that simply aren't present in the database
            for (tenant_shard_id, memory_tsp) in &expect_shards {
                if !persistent_shards.contains_key(tenant_shard_id) {
                    tracing::error!(
                        "Shard {} found in memory but not in database: {}",
                        tenant_shard_id,
                        serde_json::to_string(memory_tsp)
                            .map_err(|e| ApiError::InternalServerError(e.into()))?
                    );
                }
            }

            return Err(ApiError::InternalServerError(anyhow::anyhow!(
                "Shard consistency failure"
            )));
        }

        node_result
    }

    /// For debug/support: a JSON dump of the [`Scheduler`].  Returns a response so that
    /// we don't have to make TenantShard clonable in the return path.
    pub(crate) fn scheduler_dump(&self) -> Result<hyper::Response<hyper::Body>, ApiError> {
        let serialized = {
            let locked = self.inner.read().unwrap();
            serde_json::to_string(&locked.scheduler)
                .map_err(|e| ApiError::InternalServerError(e.into()))?
        };

        hyper::Response::builder()
            .status(hyper::StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(hyper::Body::from(serialized))
            .map_err(|e| ApiError::InternalServerError(e.into()))
    }

    /// This is for debug/support only: we simply drop all state for a tenant, without
    /// detaching or deleting it on pageservers.  We do not try and re-schedule any
    /// tenants that were on this node.
    pub(crate) async fn node_drop(&self, node_id: NodeId) -> Result<(), ApiError> {
        self.persistence.delete_node(node_id).await?;

        let mut locked = self.inner.write().unwrap();

        for shard in locked.tenants.values_mut() {
            shard.deref_node(node_id);
            shard.observed.locations.remove(&node_id);
        }

        let mut nodes = (*locked.nodes).clone();
        nodes.remove(&node_id);
        locked.nodes = Arc::new(nodes);
        metrics::METRICS_REGISTRY
            .metrics_group
            .storage_controller_pageserver_nodes
            .set(locked.nodes.len() as i64);

        locked.scheduler.node_remove(node_id);

        Ok(())
    }

    /// If a node has any work on it, it will be rescheduled: this is "clean" in the sense
    /// that we don't leave any bad state behind in the storage controller, but unclean
    /// in the sense that we are not carefully draining the node.
    pub(crate) async fn node_delete(&self, node_id: NodeId) -> Result<(), ApiError> {
        let _node_lock =
            trace_exclusive_lock(&self.node_op_locks, node_id, NodeOperations::Delete).await;

        // 1. Atomically update in-memory state:
        //    - set the scheduling state to Pause to make subsequent scheduling ops skip it
        //    - update shards' intents to exclude the node, and reschedule any shards whose intents we modified.
        //    - drop the node from the main nodes map, so that when running reconciles complete they do not
        //      re-insert references to this node into the ObservedState of shards
        //    - drop the node from the scheduler
        {
            let mut locked = self.inner.write().unwrap();
            let (nodes, tenants, scheduler) = locked.parts_mut();

            {
                let mut nodes_mut = (*nodes).deref().clone();
                match nodes_mut.get_mut(&node_id) {
                    Some(node) => {
                        // We do not bother setting this in the database, because we're about to delete the row anyway, and
                        // if we crash it would not be desirable to leave the node paused after a restart.
                        node.set_scheduling(NodeSchedulingPolicy::Pause);
                    }
                    None => {
                        tracing::info!(
                            "Node not found: presuming this is a retry and returning success"
                        );
                        return Ok(());
                    }
                }

                *nodes = Arc::new(nodes_mut);
            }

            for (_tenant_id, mut schedule_context, shards) in
                TenantShardContextIterator::new(tenants, ScheduleMode::Normal)
            {
                for shard in shards {
                    if shard.deref_node(node_id) {
                        if let Err(e) = shard.schedule(scheduler, &mut schedule_context) {
                            // TODO: implement force flag to remove a node even if we can't reschedule
                            // a tenant
                            tracing::error!(
                                "Refusing to delete node, shard {} can't be rescheduled: {e}",
                                shard.tenant_shard_id
                            );
                            return Err(e.into());
                        } else {
                            tracing::info!(
                                "Rescheduled shard {} away from node during deletion",
                                shard.tenant_shard_id
                            )
                        }

                        self.maybe_reconcile_shard(shard, nodes);
                    }

                    // Here we remove an existing observed location for the node we're removing, and it will
                    // not be re-added by a reconciler's completion because we filter out removed nodes in
                    // process_result.
                    //
                    // Note that we update the shard's observed state _after_ calling maybe_reconcile_shard: that
                    // means any reconciles we spawned will know about the node we're deleting, enabling them
                    // to do live migrations if it's still online.
                    shard.observed.locations.remove(&node_id);
                }
            }

            scheduler.node_remove(node_id);

            {
                let mut nodes_mut = (**nodes).clone();
                if let Some(mut removed_node) = nodes_mut.remove(&node_id) {
                    // Ensure that any reconciler holding an Arc<> to this node will
                    // drop out when trying to RPC to it (setting Offline state sets the
                    // cancellation token on the Node object).
                    removed_node.set_availability(NodeAvailability::Offline);
                }
                *nodes = Arc::new(nodes_mut);
                metrics::METRICS_REGISTRY
                    .metrics_group
                    .storage_controller_pageserver_nodes
                    .set(nodes.len() as i64);
            }
        }

        // Note: some `generation_pageserver` columns on tenant shards in the database may still refer to
        // the removed node, as this column means "The pageserver to which this generation was issued", and
        // their generations won't get updated until the reconcilers moving them away from this node complete.
        // That is safe because in Service::spawn we only use generation_pageserver if it refers to a node
        // that exists.

        // 2. Actually delete the node from the database and from in-memory state
        tracing::info!("Deleting node from database");
        self.persistence.delete_node(node_id).await?;

        Ok(())
    }

    pub(crate) async fn node_list(&self) -> Result<Vec<Node>, ApiError> {
        let nodes = {
            self.inner
                .read()
                .unwrap()
                .nodes
                .values()
                .cloned()
                .collect::<Vec<_>>()
        };

        Ok(nodes)
    }

    pub(crate) async fn get_node(&self, node_id: NodeId) -> Result<Node, ApiError> {
        self.inner
            .read()
            .unwrap()
            .nodes
            .get(&node_id)
            .cloned()
            .ok_or(ApiError::NotFound(
                format!("Node {node_id} not registered").into(),
            ))
    }

    pub(crate) async fn get_node_shards(
        &self,
        node_id: NodeId,
    ) -> Result<NodeShardResponse, ApiError> {
        let locked = self.inner.read().unwrap();
        let mut shards = Vec::new();
        for (tid, tenant) in locked.tenants.iter() {
            let is_intended_secondary = match (
                tenant.intent.get_attached() == &Some(node_id),
                tenant.intent.get_secondary().contains(&node_id),
            ) {
                (true, true) => {
                    return Err(ApiError::InternalServerError(anyhow::anyhow!(
                        "{} attached as primary+secondary on the same node",
                        tid
                    )))
                }
                (true, false) => Some(false),
                (false, true) => Some(true),
                (false, false) => None,
            };
            let is_observed_secondary = if let Some(ObservedStateLocation { conf: Some(conf) }) =
                tenant.observed.locations.get(&node_id)
            {
                Some(conf.secondary_conf.is_some())
            } else {
                None
            };
            if is_intended_secondary.is_some() || is_observed_secondary.is_some() {
                shards.push(NodeShard {
                    tenant_shard_id: *tid,
                    is_intended_secondary,
                    is_observed_secondary,
                });
            }
        }
        Ok(NodeShardResponse { node_id, shards })
    }

    pub(crate) async fn get_leader(&self) -> DatabaseResult<Option<ControllerPersistence>> {
        self.persistence.get_leader().await
    }

    pub(crate) async fn node_register(
        &self,
        register_req: NodeRegisterRequest,
    ) -> Result<(), ApiError> {
        let _node_lock = trace_exclusive_lock(
            &self.node_op_locks,
            register_req.node_id,
            NodeOperations::Register,
        )
        .await;

        enum RegistrationStatus {
            Matched,
            Mismatched,
            New,
        }

        let registration_status = {
            let locked = self.inner.read().unwrap();
            if let Some(node) = locked.nodes.get(&register_req.node_id) {
                if node.registration_match(&register_req) {
                    RegistrationStatus::Matched
                } else {
                    RegistrationStatus::Mismatched
                }
            } else {
                RegistrationStatus::New
            }
        };

        match registration_status {
            RegistrationStatus::Matched => {
                tracing::info!(
                    "Node {} re-registered with matching address",
                    register_req.node_id
                );

                return Ok(());
            }
            RegistrationStatus::Mismatched => {
                // TODO: decide if we want to allow modifying node addresses without removing and re-adding
                // the node.  Safest/simplest thing is to refuse it, and usually we deploy with
                // a fixed address through the lifetime of a node.
                tracing::warn!(
                    "Node {} tried to register with different address",
                    register_req.node_id
                );
                return Err(ApiError::Conflict(
                    "Node is already registered with different address".to_string(),
                ));
            }
            RegistrationStatus::New => {
                // fallthrough
            }
        }

        // We do not require that a node is actually online when registered (it will start life
        // with it's  availability set to Offline), but we _do_ require that its DNS record exists. We're
        // therefore not immune to asymmetric L3 connectivity issues, but we are protected against nodes
        // that register themselves with a broken DNS config.  We check only the HTTP hostname, because
        // the postgres hostname might only be resolvable to clients (e.g. if we're on a different VPC than clients).
        if tokio::net::lookup_host(format!(
            "{}:{}",
            register_req.listen_http_addr, register_req.listen_http_port
        ))
        .await
        .is_err()
        {
            // If we have a transient DNS issue, it's up to the caller to retry their registration.  Because
            // we can't robustly distinguish between an intermittent issue and a totally bogus DNS situation,
            // we return a soft 503 error, to encourage callers to retry past transient issues.
            return Err(ApiError::ResourceUnavailable(
                format!(
                    "Node {} tried to register with unknown DNS name '{}'",
                    register_req.node_id, register_req.listen_http_addr
                )
                .into(),
            ));
        }

        // Ordering: we must persist the new node _before_ adding it to in-memory state.
        // This ensures that before we use it for anything or expose it via any external
        // API, it is guaranteed to be available after a restart.
        let new_node = Node::new(
            register_req.node_id,
            register_req.listen_http_addr,
            register_req.listen_http_port,
            register_req.listen_pg_addr,
            register_req.listen_pg_port,
            register_req.availability_zone_id.clone(),
        );

        // TODO: idempotency if the node already exists in the database
        self.persistence.insert_node(&new_node).await?;

        let mut locked = self.inner.write().unwrap();
        let mut new_nodes = (*locked.nodes).clone();

        locked.scheduler.node_upsert(&new_node);
        new_nodes.insert(register_req.node_id, new_node);

        locked.nodes = Arc::new(new_nodes);

        metrics::METRICS_REGISTRY
            .metrics_group
            .storage_controller_pageserver_nodes
            .set(locked.nodes.len() as i64);

        tracing::info!(
            "Registered pageserver {} ({}), now have {} pageservers",
            register_req.node_id,
            register_req.availability_zone_id,
            locked.nodes.len()
        );
        Ok(())
    }

    /// Configure in-memory and persistent state of a node as requested
    ///
    /// Note that this function does not trigger any immediate side effects in response
    /// to the changes. That part is handled by [`Self::handle_node_availability_transition`].
    async fn node_state_configure(
        &self,
        node_id: NodeId,
        availability: Option<NodeAvailability>,
        scheduling: Option<NodeSchedulingPolicy>,
        node_lock: &TracingExclusiveGuard<NodeOperations>,
    ) -> Result<AvailabilityTransition, ApiError> {
        if let Some(scheduling) = scheduling {
            // Scheduling is a persistent part of Node: we must write updates to the database before
            // applying them in memory
            self.persistence.update_node(node_id, scheduling).await?;
        }

        // If we're activating a node, then before setting it active we must reconcile any shard locations
        // on that node, in case it is out of sync, e.g. due to being unavailable during controller startup,
        // by calling [`Self::node_activate_reconcile`]
        //
        // The transition we calculate here remains valid later in the function because we hold the op lock on the node:
        // nothing else can mutate its availability while we run.
        let availability_transition = if let Some(input_availability) = availability.as_ref() {
            let (activate_node, availability_transition) = {
                let locked = self.inner.read().unwrap();
                let Some(node) = locked.nodes.get(&node_id) else {
                    return Err(ApiError::NotFound(
                        anyhow::anyhow!("Node {} not registered", node_id).into(),
                    ));
                };

                (
                    node.clone(),
                    node.get_availability_transition(input_availability),
                )
            };

            if matches!(availability_transition, AvailabilityTransition::ToActive) {
                self.node_activate_reconcile(activate_node, node_lock)
                    .await?;
            }
            availability_transition
        } else {
            AvailabilityTransition::Unchanged
        };

        // Apply changes from the request to our in-memory state for the Node
        let mut locked = self.inner.write().unwrap();
        let (nodes, _tenants, scheduler) = locked.parts_mut();

        let mut new_nodes = (**nodes).clone();

        let Some(node) = new_nodes.get_mut(&node_id) else {
            return Err(ApiError::NotFound(
                anyhow::anyhow!("Node not registered").into(),
            ));
        };

        if let Some(availability) = availability {
            node.set_availability(availability);
        }

        if let Some(scheduling) = scheduling {
            node.set_scheduling(scheduling);
        }

        // Update the scheduler, in case the elegibility of the node for new shards has changed
        scheduler.node_upsert(node);

        let new_nodes = Arc::new(new_nodes);
        locked.nodes = new_nodes;

        Ok(availability_transition)
    }

    /// Handle availability transition of one node
    ///
    /// Note that you should first call [`Self::node_state_configure`] to update
    /// the in-memory state referencing that node. If you need to handle more than one transition
    /// consider using [`Self::handle_node_availability_transitions`].
    async fn handle_node_availability_transition(
        &self,
        node_id: NodeId,
        transition: AvailabilityTransition,
        _node_lock: &TracingExclusiveGuard<NodeOperations>,
    ) -> Result<(), ApiError> {
        // Modify scheduling state for any Tenants that are affected by a change in the node's availability state.
        match transition {
            AvailabilityTransition::ToOffline => {
                tracing::info!("Node {} transition to offline", node_id);

                let mut locked = self.inner.write().unwrap();
                let (nodes, tenants, scheduler) = locked.parts_mut();

                let mut tenants_affected: usize = 0;

                for (_tenant_id, mut schedule_context, shards) in
                    TenantShardContextIterator::new(tenants, ScheduleMode::Normal)
                {
                    for tenant_shard in shards {
                        let tenant_shard_id = tenant_shard.tenant_shard_id;
                        if let Some(observed_loc) =
                            tenant_shard.observed.locations.get_mut(&node_id)
                        {
                            // When a node goes offline, we set its observed configuration to None, indicating unknown: we will
                            // not assume our knowledge of the node's configuration is accurate until it comes back online
                            observed_loc.conf = None;
                        }

                        if nodes.len() == 1 {
                            // Special case for single-node cluster: there is no point trying to reschedule
                            // any tenant shards: avoid doing so, in order to avoid spewing warnings about
                            // failures to schedule them.
                            continue;
                        }

                        if !nodes
                            .values()
                            .any(|n| matches!(n.may_schedule(), MaySchedule::Yes(_)))
                        {
                            // Special case for when all nodes are unavailable and/or unschedulable: there is no point
                            // trying to reschedule since there's nowhere else to go. Without this
                            // branch we incorrectly detach tenants in response to node unavailability.
                            continue;
                        }

                        if tenant_shard.intent.demote_attached(scheduler, node_id) {
                            tenant_shard.sequence = tenant_shard.sequence.next();

                            match tenant_shard.schedule(scheduler, &mut schedule_context) {
                                Err(e) => {
                                    // It is possible that some tenants will become unschedulable when too many pageservers
                                    // go offline: in this case there isn't much we can do other than make the issue observable.
                                    // TODO: give TenantShard a scheduling error attribute to be queried later.
                                    tracing::warn!(%tenant_shard_id, "Scheduling error when marking pageserver {} offline: {e}", node_id);
                                }
                                Ok(()) => {
                                    if self.maybe_reconcile_shard(tenant_shard, nodes).is_some() {
                                        tenants_affected += 1;
                                    };
                                }
                            }
                        }
                    }
                }
                tracing::info!(
                    "Launched {} reconciler tasks for tenants affected by node {} going offline",
                    tenants_affected,
                    node_id
                )
            }
            AvailabilityTransition::ToActive => {
                tracing::info!("Node {} transition to active", node_id);

                let mut locked = self.inner.write().unwrap();
                let (nodes, tenants, _scheduler) = locked.parts_mut();

                // When a node comes back online, we must reconcile any tenant that has a None observed
                // location on the node.
                for tenant_shard in tenants.values_mut() {
                    // If a reconciliation is already in progress, rely on the previous scheduling
                    // decision and skip triggering a new reconciliation.
                    if tenant_shard.reconciler.is_some() {
                        continue;
                    }

                    if let Some(observed_loc) = tenant_shard.observed.locations.get_mut(&node_id) {
                        if observed_loc.conf.is_none() {
                            self.maybe_reconcile_shard(tenant_shard, nodes);
                        }
                    }
                }

                // TODO: in the background, we should balance work back onto this pageserver
            }
            // No action required for the intermediate unavailable state.
            // When we transition into active or offline from the unavailable state,
            // the correct handling above will kick in.
            AvailabilityTransition::ToWarmingUpFromActive => {
                tracing::info!("Node {} transition to unavailable from active", node_id);
            }
            AvailabilityTransition::ToWarmingUpFromOffline => {
                tracing::info!("Node {} transition to unavailable from offline", node_id);
            }
            AvailabilityTransition::Unchanged => {
                tracing::debug!("Node {} no availability change during config", node_id);
            }
        }

        Ok(())
    }

    /// Handle availability transition for multiple nodes
    ///
    /// Note that you should first call [`Self::node_state_configure`] for
    /// all nodes being handled here for the handling to use fresh in-memory state.
    async fn handle_node_availability_transitions(
        &self,
        transitions: Vec<(
            NodeId,
            TracingExclusiveGuard<NodeOperations>,
            AvailabilityTransition,
        )>,
    ) -> Result<(), Vec<(NodeId, ApiError)>> {
        let mut errors = Vec::default();
        for (node_id, node_lock, transition) in transitions {
            let res = self
                .handle_node_availability_transition(node_id, transition, &node_lock)
                .await;
            if let Err(err) = res {
                errors.push((node_id, err));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub(crate) async fn node_configure(
        &self,
        node_id: NodeId,
        availability: Option<NodeAvailability>,
        scheduling: Option<NodeSchedulingPolicy>,
    ) -> Result<(), ApiError> {
        let node_lock =
            trace_exclusive_lock(&self.node_op_locks, node_id, NodeOperations::Configure).await;

        let transition = self
            .node_state_configure(node_id, availability, scheduling, &node_lock)
            .await?;
        self.handle_node_availability_transition(node_id, transition, &node_lock)
            .await
    }

    /// Wrapper around [`Self::node_configure`] which only allows changes while there is no ongoing
    /// operation for HTTP api.
    pub(crate) async fn external_node_configure(
        &self,
        node_id: NodeId,
        availability: Option<NodeAvailability>,
        scheduling: Option<NodeSchedulingPolicy>,
    ) -> Result<(), ApiError> {
        {
            let locked = self.inner.read().unwrap();
            if let Some(op) = locked.ongoing_operation.as_ref().map(|op| op.operation) {
                return Err(ApiError::PreconditionFailed(
                    format!("Ongoing background operation forbids configuring: {op}").into(),
                ));
            }
        }

        self.node_configure(node_id, availability, scheduling).await
    }

    pub(crate) async fn start_node_drain(
        self: &Arc<Self>,
        node_id: NodeId,
    ) -> Result<(), ApiError> {
        let (ongoing_op, node_available, node_policy, schedulable_nodes_count) = {
            let locked = self.inner.read().unwrap();
            let nodes = &locked.nodes;
            let node = nodes.get(&node_id).ok_or(ApiError::NotFound(
                anyhow::anyhow!("Node {} not registered", node_id).into(),
            ))?;
            let schedulable_nodes_count = nodes
                .iter()
                .filter(|(_, n)| matches!(n.may_schedule(), MaySchedule::Yes(_)))
                .count();

            (
                locked
                    .ongoing_operation
                    .as_ref()
                    .map(|ongoing| ongoing.operation),
                node.is_available(),
                node.get_scheduling(),
                schedulable_nodes_count,
            )
        };

        if let Some(ongoing) = ongoing_op {
            return Err(ApiError::PreconditionFailed(
                format!("Background operation already ongoing for node: {}", ongoing).into(),
            ));
        }

        if !node_available {
            return Err(ApiError::ResourceUnavailable(
                format!("Node {node_id} is currently unavailable").into(),
            ));
        }

        if schedulable_nodes_count == 0 {
            return Err(ApiError::PreconditionFailed(
                "No other schedulable nodes to drain to".into(),
            ));
        }

        match node_policy {
            NodeSchedulingPolicy::Active => {
                self.node_configure(node_id, None, Some(NodeSchedulingPolicy::Draining))
                    .await?;

                let cancel = self.cancel.child_token();
                let gate_guard = self.gate.enter().map_err(|_| ApiError::ShuttingDown)?;

                self.inner.write().unwrap().ongoing_operation = Some(OperationHandler {
                    operation: Operation::Drain(Drain { node_id }),
                    cancel: cancel.clone(),
                });

                let span = tracing::info_span!(parent: None, "drain_node", %node_id);

                tokio::task::spawn({
                    let service = self.clone();
                    let cancel = cancel.clone();
                    async move {
                        let _gate_guard = gate_guard;

                        scopeguard::defer! {
                            let prev = service.inner.write().unwrap().ongoing_operation.take();

                            if let Some(Operation::Drain(removed_drain)) = prev.map(|h| h.operation) {
                                assert_eq!(removed_drain.node_id, node_id, "We always take the same operation");
                            } else {
                                panic!("We always remove the same operation")
                            }
                        }

                        tracing::info!("Drain background operation starting");
                        let res = service.drain_node(node_id, cancel).await;
                        match res {
                            Ok(()) => {
                                tracing::info!("Drain background operation completed successfully");
                            }
                            Err(OperationError::Cancelled) => {
                                tracing::info!("Drain background operation was cancelled");
                            }
                            Err(err) => {
                                tracing::error!("Drain background operation encountered: {err}")
                            }
                        }
                    }
                }.instrument(span));
            }
            NodeSchedulingPolicy::Draining => {
                return Err(ApiError::Conflict(format!(
                    "Node {node_id} has drain in progress"
                )));
            }
            policy => {
                return Err(ApiError::PreconditionFailed(
                    format!("Node {node_id} cannot be drained due to {policy:?} policy").into(),
                ));
            }
        }

        Ok(())
    }

    pub(crate) async fn cancel_node_drain(&self, node_id: NodeId) -> Result<(), ApiError> {
        let node_available = {
            let locked = self.inner.read().unwrap();
            let nodes = &locked.nodes;
            let node = nodes.get(&node_id).ok_or(ApiError::NotFound(
                anyhow::anyhow!("Node {} not registered", node_id).into(),
            ))?;

            node.is_available()
        };

        if !node_available {
            return Err(ApiError::ResourceUnavailable(
                format!("Node {node_id} is currently unavailable").into(),
            ));
        }

        if let Some(op_handler) = self.inner.read().unwrap().ongoing_operation.as_ref() {
            if let Operation::Drain(drain) = op_handler.operation {
                if drain.node_id == node_id {
                    tracing::info!("Cancelling background drain operation for node {node_id}");
                    op_handler.cancel.cancel();
                    return Ok(());
                }
            }
        }

        Err(ApiError::PreconditionFailed(
            format!("Node {node_id} has no drain in progress").into(),
        ))
    }

    pub(crate) async fn start_node_fill(self: &Arc<Self>, node_id: NodeId) -> Result<(), ApiError> {
        let (ongoing_op, node_available, node_policy, total_nodes_count) = {
            let locked = self.inner.read().unwrap();
            let nodes = &locked.nodes;
            let node = nodes.get(&node_id).ok_or(ApiError::NotFound(
                anyhow::anyhow!("Node {} not registered", node_id).into(),
            ))?;

            (
                locked
                    .ongoing_operation
                    .as_ref()
                    .map(|ongoing| ongoing.operation),
                node.is_available(),
                node.get_scheduling(),
                nodes.len(),
            )
        };

        if let Some(ongoing) = ongoing_op {
            return Err(ApiError::PreconditionFailed(
                format!("Background operation already ongoing for node: {}", ongoing).into(),
            ));
        }

        if !node_available {
            return Err(ApiError::ResourceUnavailable(
                format!("Node {node_id} is currently unavailable").into(),
            ));
        }

        if total_nodes_count <= 1 {
            return Err(ApiError::PreconditionFailed(
                "No other nodes to fill from".into(),
            ));
        }

        match node_policy {
            NodeSchedulingPolicy::Active => {
                self.node_configure(node_id, None, Some(NodeSchedulingPolicy::Filling))
                    .await?;

                let cancel = self.cancel.child_token();
                let gate_guard = self.gate.enter().map_err(|_| ApiError::ShuttingDown)?;

                self.inner.write().unwrap().ongoing_operation = Some(OperationHandler {
                    operation: Operation::Fill(Fill { node_id }),
                    cancel: cancel.clone(),
                });

                let span = tracing::info_span!(parent: None, "fill_node", %node_id);

                tokio::task::spawn({
                    let service = self.clone();
                    let cancel = cancel.clone();
                    async move {
                        let _gate_guard = gate_guard;

                        scopeguard::defer! {
                            let prev = service.inner.write().unwrap().ongoing_operation.take();

                            if let Some(Operation::Fill(removed_fill)) = prev.map(|h| h.operation) {
                                assert_eq!(removed_fill.node_id, node_id, "We always take the same operation");
                            } else {
                                panic!("We always remove the same operation")
                            }
                        }

                        tracing::info!("Fill background operation starting");
                        let res = service.fill_node(node_id, cancel).await;
                        match res {
                            Ok(()) => {
                                tracing::info!("Fill background operation completed successfully");
                            }
                            Err(OperationError::Cancelled) => {
                                tracing::info!("Fill background operation was cancelled");
                            }
                            Err(err) => {
                                tracing::error!("Fill background operation encountered: {err}")
                            }
                        }
                    }
                }.instrument(span));
            }
            NodeSchedulingPolicy::Filling => {
                return Err(ApiError::Conflict(format!(
                    "Node {node_id} has fill in progress"
                )));
            }
            policy => {
                return Err(ApiError::PreconditionFailed(
                    format!("Node {node_id} cannot be filled due to {policy:?} policy").into(),
                ));
            }
        }

        Ok(())
    }

    pub(crate) async fn cancel_node_fill(&self, node_id: NodeId) -> Result<(), ApiError> {
        let node_available = {
            let locked = self.inner.read().unwrap();
            let nodes = &locked.nodes;
            let node = nodes.get(&node_id).ok_or(ApiError::NotFound(
                anyhow::anyhow!("Node {} not registered", node_id).into(),
            ))?;

            node.is_available()
        };

        if !node_available {
            return Err(ApiError::ResourceUnavailable(
                format!("Node {node_id} is currently unavailable").into(),
            ));
        }

        if let Some(op_handler) = self.inner.read().unwrap().ongoing_operation.as_ref() {
            if let Operation::Fill(fill) = op_handler.operation {
                if fill.node_id == node_id {
                    tracing::info!("Cancelling background drain operation for node {node_id}");
                    op_handler.cancel.cancel();
                    return Ok(());
                }
            }
        }

        Err(ApiError::PreconditionFailed(
            format!("Node {node_id} has no fill in progress").into(),
        ))
    }

    /// Like [`Self::maybe_configured_reconcile_shard`], but uses the default reconciler
    /// configuration
    fn maybe_reconcile_shard(
        &self,
        shard: &mut TenantShard,
        nodes: &Arc<HashMap<NodeId, Node>>,
    ) -> Option<ReconcilerWaiter> {
        self.maybe_configured_reconcile_shard(shard, nodes, ReconcilerConfig::default())
    }

    /// Wrap [`TenantShard`] reconciliation methods with acquisition of [`Gate`] and [`ReconcileUnits`],
    fn maybe_configured_reconcile_shard(
        &self,
        shard: &mut TenantShard,
        nodes: &Arc<HashMap<NodeId, Node>>,
        reconciler_config: ReconcilerConfig,
    ) -> Option<ReconcilerWaiter> {
        let reconcile_needed = shard.get_reconcile_needed(nodes);

        match reconcile_needed {
            ReconcileNeeded::No => return None,
            ReconcileNeeded::WaitExisting(waiter) => return Some(waiter),
            ReconcileNeeded::Yes => {
                // Fall through to try and acquire units for spawning reconciler
            }
        };

        let units = match self.reconciler_concurrency.clone().try_acquire_owned() {
            Ok(u) => ReconcileUnits::new(u),
            Err(_) => {
                tracing::info!(tenant_id=%shard.tenant_shard_id.tenant_id, shard_id=%shard.tenant_shard_id.shard_slug(),
                    "Concurrency limited: enqueued for reconcile later");
                if !shard.delayed_reconcile {
                    match self.delayed_reconcile_tx.try_send(shard.tenant_shard_id) {
                        Err(TrySendError::Closed(_)) => {
                            // Weird mid-shutdown case?
                        }
                        Err(TrySendError::Full(_)) => {
                            // It is safe to skip sending our ID in the channel: we will eventually get retried by the background reconcile task.
                            tracing::warn!(
                                "Many shards are waiting to reconcile: delayed_reconcile queue is full"
                            );
                        }
                        Ok(()) => {
                            shard.delayed_reconcile = true;
                        }
                    }
                }

                // We won't spawn a reconciler, but we will construct a waiter that waits for the shard's sequence
                // number to advance.  When this function is eventually called again and succeeds in getting units,
                // it will spawn a reconciler that makes this waiter complete.
                return Some(shard.future_reconcile_waiter());
            }
        };

        let Ok(gate_guard) = self.reconcilers_gate.enter() else {
            // Gate closed: we're shutting down, drop out.
            return None;
        };

        shard.spawn_reconciler(
            &self.result_tx,
            nodes,
            &self.compute_hook,
            reconciler_config,
            &self.config,
            &self.persistence,
            units,
            gate_guard,
            &self.reconcilers_cancel,
        )
    }

    /// Check all tenants for pending reconciliation work, and reconcile those in need.
    /// Additionally, reschedule tenants that require it.
    ///
    /// Returns how many reconciliation tasks were started, or `1` if no reconciles were
    /// spawned but some _would_ have been spawned if `reconciler_concurrency` units where
    /// available.  A return value of 0 indicates that everything is fully reconciled already.
    fn reconcile_all(&self) -> usize {
        let mut locked = self.inner.write().unwrap();
        let (nodes, tenants, scheduler) = locked.parts_mut();
        let pageservers = nodes.clone();

        // This function is an efficient place to update lazy statistics, since we are walking
        // all tenants.
        let mut pending_reconciles = 0;
        let mut az_violations = 0;

        // If we find any tenants to drop from memory, stash them to offload after
        // we're done traversing the map of tenants.
        let mut drop_detached_tenants = Vec::new();

        let mut reconciles_spawned = 0;
        for shard in tenants.values_mut() {
            // Accumulate scheduling statistics
            if let (Some(attached), Some(preferred)) =
                (shard.intent.get_attached(), shard.preferred_az())
            {
                let node_az = nodes
                    .get(attached)
                    .expect("Nodes exist if referenced")
                    .get_availability_zone_id();
                if node_az != preferred {
                    az_violations += 1;
                }
            }

            // Skip checking if this shard is already enqueued for reconciliation
            if shard.delayed_reconcile && self.reconciler_concurrency.available_permits() == 0 {
                // If there is something delayed, then return a nonzero count so that
                // callers like reconcile_all_now do not incorrectly get the impression
                // that the system is in a quiescent state.
                reconciles_spawned = std::cmp::max(1, reconciles_spawned);
                pending_reconciles += 1;
                continue;
            }

            // Eventual consistency: if an earlier reconcile job failed, and the shard is still
            // dirty, spawn another rone
            if self.maybe_reconcile_shard(shard, &pageservers).is_some() {
                reconciles_spawned += 1;
            } else if shard.delayed_reconcile {
                // Shard wanted to reconcile but for some reason couldn't.
                pending_reconciles += 1;
            }

            // If this tenant is detached, try dropping it from memory. This is usually done
            // proactively in [`Self::process_results`], but we do it here to handle the edge
            // case where a reconcile completes while someone else is holding an op lock for the tenant.
            if shard.tenant_shard_id.shard_number == ShardNumber(0)
                && shard.policy == PlacementPolicy::Detached
            {
                if let Some(guard) = self.tenant_op_locks.try_exclusive(
                    shard.tenant_shard_id.tenant_id,
                    TenantOperations::DropDetached,
                ) {
                    drop_detached_tenants.push((shard.tenant_shard_id.tenant_id, guard));
                }
            }
        }

        // Some metrics are calculated from SchedulerNode state, update these periodically
        scheduler.update_metrics();

        // Process any deferred tenant drops
        for (tenant_id, guard) in drop_detached_tenants {
            self.maybe_drop_tenant(tenant_id, &mut locked, &guard);
        }

        metrics::METRICS_REGISTRY
            .metrics_group
            .storage_controller_schedule_az_violation
            .set(az_violations as i64);

        metrics::METRICS_REGISTRY
            .metrics_group
            .storage_controller_pending_reconciles
            .set(pending_reconciles as i64);

        reconciles_spawned
    }

    /// `optimize` in this context means identifying shards which have valid scheduled locations, but
    /// could be scheduled somewhere better:
    /// - Cutting over to a secondary if the node with the secondary is more lightly loaded
    ///    * e.g. after a node fails then recovers, to move some work back to it
    /// - Cutting over to a secondary if it improves the spread of shard attachments within a tenant
    ///    * e.g. after a shard split, the initial attached locations will all be on the node where
    ///      we did the split, but are probably better placed elsewhere.
    /// - Creating new secondary locations if it improves the spreading of a sharded tenant
    ///    * e.g. after a shard split, some locations will be on the same node (where the split
    ///      happened), and will probably be better placed elsewhere.
    ///
    /// To put it more briefly: whereas the scheduler respects soft constraints in a ScheduleContext at
    /// the time of scheduling, this function looks for cases where a better-scoring location is available
    /// according to those same soft constraints.
    async fn optimize_all(&self) -> usize {
        // Limit on how many shards' optmizations each call to this function will execute.  Combined
        // with the frequency of background calls, this acts as an implicit rate limit that runs a small
        // trickle of optimizations in the background, rather than executing a large number in parallel
        // when a change occurs.
        const MAX_OPTIMIZATIONS_EXEC_PER_PASS: usize = 2;

        // Synchronous prepare: scan shards for possible scheduling optimizations
        let candidate_work = self.optimize_all_plan();
        let candidate_work_len = candidate_work.len();

        // Asynchronous validate: I/O to pageservers to make sure shards are in a good state to apply validation
        let validated_work = self.optimize_all_validate(candidate_work).await;

        let was_work_filtered = validated_work.len() != candidate_work_len;

        // Synchronous apply: update the shards' intent states according to validated optimisations
        let mut reconciles_spawned = 0;
        let mut optimizations_applied = 0;
        let mut locked = self.inner.write().unwrap();
        let (nodes, tenants, scheduler) = locked.parts_mut();
        for (tenant_shard_id, optimization) in validated_work {
            let Some(shard) = tenants.get_mut(&tenant_shard_id) else {
                // Shard was dropped between planning and execution;
                continue;
            };
            tracing::info!(tenant_shard_id=%tenant_shard_id, "Applying optimization: {optimization:?}");
            if shard.apply_optimization(scheduler, optimization) {
                optimizations_applied += 1;
                if self.maybe_reconcile_shard(shard, nodes).is_some() {
                    reconciles_spawned += 1;
                }
            }

            if optimizations_applied >= MAX_OPTIMIZATIONS_EXEC_PER_PASS {
                break;
            }
        }

        if was_work_filtered {
            // If we filtered any work out during validation, ensure we return a nonzero value to indicate
            // to callers that the system is not in a truly quiet state, it's going to do some work as soon
            // as these validations start passing.
            reconciles_spawned = std::cmp::max(reconciles_spawned, 1);
        }

        reconciles_spawned
    }

    fn optimize_all_plan(&self) -> Vec<(TenantShardId, ScheduleOptimization)> {
        // How many candidate optimizations we will generate, before evaluating them for readniess: setting
        // this higher than the execution limit gives us a chance to execute some work even if the first
        // few optimizations we find are not ready.
        const MAX_OPTIMIZATIONS_PLAN_PER_PASS: usize = 8;

        let mut work = Vec::new();
        let mut locked = self.inner.write().unwrap();
        let (_nodes, tenants, scheduler) = locked.parts_mut();

        // We are going to plan a bunch of optimisations before applying any of them, so the
        // utilisation stats on nodes will be effectively stale for the >1st optimisation we
        // generate.  To avoid this causing unstable migrations/flapping, it's important that the
        // code in TenantShard for finding optimisations uses [`NodeAttachmentSchedulingScore::disregard_utilization`]
        // to ignore the utilisation component of the score.

        for (_tenant_id, schedule_context, shards) in
            TenantShardContextIterator::new(tenants, ScheduleMode::Speculative)
        {
            for shard in shards {
                if work.len() >= MAX_OPTIMIZATIONS_PLAN_PER_PASS {
                    break;
                }
                match shard.get_scheduling_policy() {
                    ShardSchedulingPolicy::Active => {
                        // Ok to do optimization
                    }
                    ShardSchedulingPolicy::Essential
                    | ShardSchedulingPolicy::Pause
                    | ShardSchedulingPolicy::Stop => {
                        // Policy prevents optimizing this shard.
                        continue;
                    }
                }

                if !matches!(shard.splitting, SplitState::Idle)
                    || matches!(shard.policy, PlacementPolicy::Detached)
                    || shard.reconciler.is_some()
                {
                    // Do not start any optimizations while another change to the tenant is ongoing: this
                    // is not necessary for correctness, but simplifies operations and implicitly throttles
                    // optimization changes to happen in a "trickle" over time.
                    continue;
                }

                // Fast path: we may quickly identify shards that don't have any possible optimisations
                if !shard.maybe_optimizable(scheduler, &schedule_context) {
                    if cfg!(feature = "testing") {
                        // Check that maybe_optimizable doesn't disagree with the actual optimization functions.
                        // Only do this in testing builds because it is not a correctness-critical check, so we shouldn't
                        // panic in prod if we hit this, or spend cycles on it in prod.
                        assert!(shard
                            .optimize_attachment(scheduler, &schedule_context)
                            .is_none());
                        assert!(shard
                            .optimize_secondary(scheduler, &schedule_context)
                            .is_none());
                    }
                    continue;
                }

                if let Some(optimization) =
                    // If idle, maybe optimize attachments: if a shard has a secondary location that is preferable to
                    // its primary location based on soft constraints, cut it over.
                    shard.optimize_attachment(scheduler, &schedule_context)
                {
                    tracing::info!(tenant_shard_id=%shard.tenant_shard_id, "Identified optimization for attachment: {optimization:?}");
                    work.push((shard.tenant_shard_id, optimization));
                    break;
                } else if let Some(optimization) =
                    // If idle, maybe optimize secondary locations: if a shard has a secondary location that would be
                    // better placed on another node, based on ScheduleContext, then adjust it.  This
                    // covers cases like after a shard split, where we might have too many shards
                    // in the same tenant with secondary locations on the node where they originally split.
                    shard.optimize_secondary(scheduler, &schedule_context)
                {
                    tracing::info!(tenant_shard_id=%shard.tenant_shard_id, "Identified optimization for secondary: {optimization:?}");
                    work.push((shard.tenant_shard_id, optimization));
                    break;
                }
            }
        }

        work
    }

    async fn optimize_all_validate(
        &self,
        candidate_work: Vec<(TenantShardId, ScheduleOptimization)>,
    ) -> Vec<(TenantShardId, ScheduleOptimization)> {
        // Take a clone of the node map to use outside the lock in async validation phase
        let validation_nodes = { self.inner.read().unwrap().nodes.clone() };

        let mut want_secondary_status = Vec::new();

        // Validate our plans: this is an async phase where we may do I/O to pageservers to
        // check that the state of locations is acceptable to run the optimization, such as
        // checking that a secondary location is sufficiently warmed-up to cleanly cut over
        // in a live migration.
        let mut validated_work = Vec::new();
        for (tenant_shard_id, optimization) in candidate_work {
            match optimization.action {
                ScheduleOptimizationAction::MigrateAttachment(MigrateAttachment {
                    old_attached_node_id: _,
                    new_attached_node_id,
                }) => {
                    match validation_nodes.get(&new_attached_node_id) {
                        None => {
                            // Node was dropped between planning and validation
                        }
                        Some(node) => {
                            if !node.is_available() {
                                tracing::info!("Skipping optimization migration of {tenant_shard_id} to {new_attached_node_id} because node unavailable");
                            } else {
                                // Accumulate optimizations that require fetching secondary status, so that we can execute these
                                // remote API requests concurrently.
                                want_secondary_status.push((
                                    tenant_shard_id,
                                    node.clone(),
                                    optimization,
                                ));
                            }
                        }
                    }
                }
                ScheduleOptimizationAction::ReplaceSecondary(_)
                | ScheduleOptimizationAction::CreateSecondary(_)
                | ScheduleOptimizationAction::RemoveSecondary(_) => {
                    // No extra checks needed to manage secondaries: this does not interrupt client access
                    validated_work.push((tenant_shard_id, optimization))
                }
            };
        }

        // Call into pageserver API to find out if the destination secondary location is warm enough for a reasonably smooth migration: we
        // do this so that we avoid spawning a Reconciler that would have to wait minutes/hours for a destination to warm up: that reconciler
        // would hold a precious reconcile semaphore unit the whole time it was waiting for the destination to warm up.
        let results = self
            .tenant_for_shards_api(
                want_secondary_status
                    .iter()
                    .map(|i| (i.0, i.1.clone()))
                    .collect(),
                |tenant_shard_id, client| async move {
                    client.tenant_secondary_status(tenant_shard_id).await
                },
                1,
                1,
                SHORT_RECONCILE_TIMEOUT,
                &self.cancel,
            )
            .await;

        for ((tenant_shard_id, node, optimization), secondary_status) in
            want_secondary_status.into_iter().zip(results.into_iter())
        {
            match secondary_status {
                Err(e) => {
                    tracing::info!("Skipping migration of {tenant_shard_id} to {node}, error querying secondary: {e}");
                }
                Ok(progress) => {
                    // We require secondary locations to have less than 10GiB of downloads pending before we will use
                    // them in an optimization
                    const DOWNLOAD_FRESHNESS_THRESHOLD: u64 = 10 * 1024 * 1024 * 1024;

                    if progress.heatmap_mtime.is_none()
                        || progress.bytes_total < DOWNLOAD_FRESHNESS_THRESHOLD
                            && progress.bytes_downloaded != progress.bytes_total
                        || progress.bytes_total - progress.bytes_downloaded
                            > DOWNLOAD_FRESHNESS_THRESHOLD
                    {
                        tracing::info!("Skipping migration of {tenant_shard_id} to {node} because secondary isn't ready: {progress:?}");

                        #[cfg(feature = "testing")]
                        if progress.heatmap_mtime.is_none() {
                            // No heatmap might mean the attached location has never uploaded one, or that
                            // the secondary download hasn't happened yet.  This is relatively unusual in the field,
                            // but fairly common in tests.
                            self.kick_secondary_download(tenant_shard_id).await;
                        }
                    } else {
                        // Location looks ready: proceed
                        tracing::info!(
                            "{tenant_shard_id} secondary on {node} is warm enough for migration: {progress:?}"
                        );
                        validated_work.push((tenant_shard_id, optimization))
                    }
                }
            }
        }

        validated_work
    }

    /// Some aspects of scheduling optimisation wait for secondary locations to be warm.  This
    /// happens on multi-minute timescales in the field, which is fine because optimisation is meant
    /// to be a lazy background thing. However, when testing, it is not practical to wait around, so
    /// we have this helper to move things along faster.
    #[cfg(feature = "testing")]
    async fn kick_secondary_download(&self, tenant_shard_id: TenantShardId) {
        let (attached_node, secondaries) = {
            let locked = self.inner.read().unwrap();
            let Some(shard) = locked.tenants.get(&tenant_shard_id) else {
                tracing::warn!(
                    "Skipping kick of secondary download for {tenant_shard_id}: not found"
                );
                return;
            };

            let Some(attached) = shard.intent.get_attached() else {
                tracing::warn!(
                    "Skipping kick of secondary download for {tenant_shard_id}: no attached"
                );
                return;
            };

            let secondaries = shard
                .intent
                .get_secondary()
                .iter()
                .map(|n| locked.nodes.get(n).unwrap().clone())
                .collect::<Vec<_>>();

            (locked.nodes.get(attached).unwrap().clone(), secondaries)
        };

        // Make remote API calls to upload + download heatmaps: we ignore errors because this is just
        // a 'kick' to let scheduling optimisation run more promptly.
        match attached_node
            .with_client_retries(
                |client| async move { client.tenant_heatmap_upload(tenant_shard_id).await },
                &self.config.jwt_token,
                3,
                10,
                SHORT_RECONCILE_TIMEOUT,
                &self.cancel,
            )
            .await
        {
            Some(Err(e)) => {
                tracing::info!(
                    "Failed to upload heatmap from {attached_node} for {tenant_shard_id}: {e}"
                );
            }
            None => {
                tracing::info!(
                    "Cancelled while uploading heatmap from {attached_node} for {tenant_shard_id}"
                );
            }
            Some(Ok(_)) => {
                tracing::info!(
                    "Successfully uploaded heatmap from {attached_node} for {tenant_shard_id}"
                );
            }
        }

        for secondary_node in secondaries {
            match secondary_node
                .with_client_retries(
                    |client| async move {
                        client
                            .tenant_secondary_download(
                                tenant_shard_id,
                                Some(Duration::from_secs(1)),
                            )
                            .await
                    },
                    &self.config.jwt_token,
                    3,
                    10,
                    SHORT_RECONCILE_TIMEOUT,
                    &self.cancel,
                )
                .await
            {
                Some(Err(e)) => {
                    tracing::info!(
                "Failed to download heatmap from {secondary_node} for {tenant_shard_id}: {e}"
            );
                }
                None => {
                    tracing::info!("Cancelled while downloading heatmap from {secondary_node} for {tenant_shard_id}");
                }
                Some(Ok(progress)) => {
                    tracing::info!("Successfully downloaded heatmap from {secondary_node} for {tenant_shard_id}: {progress:?}");
                }
            }
        }
    }

    /// Look for shards which are oversized and in need of splitting
    async fn autosplit_tenants(self: &Arc<Self>) {
        let Some(split_threshold) = self.config.split_threshold else {
            // Auto-splitting is disabled
            return;
        };

        let nodes = self.inner.read().unwrap().nodes.clone();

        const SPLIT_TO_MAX: ShardCount = ShardCount::new(8);

        let mut top_n = Vec::new();

        // Call into each node to look for big tenants
        let top_n_request = TopTenantShardsRequest {
            // We currently split based on logical size, for simplicity: logical size is a signal of
            // the user's intent to run a large database, whereas physical/resident size can be symptoms
            // of compaction issues.  Eventually we should switch to using resident size to bound the
            // disk space impact of one shard.
            order_by: models::TenantSorting::MaxLogicalSize,
            limit: 10,
            where_shards_lt: Some(SPLIT_TO_MAX),
            where_gt: Some(split_threshold),
        };
        for node in nodes.values() {
            let request_ref = &top_n_request;
            match node
                .with_client_retries(
                    |client| async move {
                        let request = request_ref.clone();
                        client.top_tenant_shards(request.clone()).await
                    },
                    &self.config.jwt_token,
                    3,
                    3,
                    Duration::from_secs(5),
                    &self.cancel,
                )
                .await
            {
                Some(Ok(node_top_n)) => {
                    top_n.extend(node_top_n.shards.into_iter());
                }
                Some(Err(mgmt_api::Error::Cancelled)) => {
                    continue;
                }
                Some(Err(e)) => {
                    tracing::warn!("Failed to fetch top N tenants from {node}: {e}");
                    continue;
                }
                None => {
                    // Node is shutting down
                    continue;
                }
            };
        }

        // Pick the biggest tenant to split first
        top_n.sort_by_key(|i| i.resident_size);

        // Filter out tenants in a prohibiting scheduling mode
        {
            let locked = self.inner.read().unwrap();
            top_n.retain(|i| {
                if let Some(shard) = locked.tenants.get(&i.id) {
                    matches!(shard.get_scheduling_policy(), ShardSchedulingPolicy::Active)
                } else {
                    false
                }
            });
        }

        let Some(split_candidate) = top_n.into_iter().next() else {
            tracing::debug!("No split-elegible shards found");
            return;
        };

        // We spawn a task to run this, so it's exactly like some external API client requesting it.  We don't
        // want to block the background reconcile loop on this.
        tracing::info!("Auto-splitting tenant for size threshold {split_threshold}: current size {split_candidate:?}");

        let this = self.clone();
        tokio::spawn(
            async move {
                match this
                    .tenant_shard_split(
                        split_candidate.id.tenant_id,
                        TenantShardSplitRequest {
                            // Always split to the max number of shards: this avoids stepping through
                            // intervening shard counts and encountering the overrhead of a split+cleanup
                            // each time as a tenant grows, and is not too expensive because our max shard
                            // count is relatively low anyway.
                            // This policy will be adjusted in future once we support higher shard count.
                            new_shard_count: SPLIT_TO_MAX.literal(),
                            new_stripe_size: Some(ShardParameters::DEFAULT_STRIPE_SIZE),
                        },
                    )
                    .await
                {
                    Ok(_) => {
                        tracing::info!("Successful auto-split");
                    }
                    Err(e) => {
                        tracing::error!("Auto-split failed: {e}");
                    }
                }
            }
            .instrument(tracing::info_span!("auto_split", tenant_id=%split_candidate.id.tenant_id)),
        );
    }

    /// Useful for tests: run whatever work a background [`Self::reconcile_all`] would have done, but
    /// also wait for any generated Reconcilers to complete.  Calling this until it returns zero should
    /// put the system into a quiescent state where future background reconciliations won't do anything.
    pub(crate) async fn reconcile_all_now(&self) -> Result<usize, ReconcileWaitError> {
        let reconciles_spawned = self.reconcile_all();
        let reconciles_spawned = if reconciles_spawned == 0 {
            // Only optimize when we are otherwise idle
            self.optimize_all().await
        } else {
            reconciles_spawned
        };

        let waiters = {
            let mut waiters = Vec::new();
            let locked = self.inner.read().unwrap();
            for (_tenant_shard_id, shard) in locked.tenants.iter() {
                if let Some(waiter) = shard.get_waiter() {
                    waiters.push(waiter);
                }
            }
            waiters
        };

        let waiter_count = waiters.len();
        match self.await_waiters(waiters, RECONCILE_TIMEOUT).await {
            Ok(()) => {}
            Err(ReconcileWaitError::Failed(_, reconcile_error))
                if matches!(*reconcile_error, ReconcileError::Cancel) =>
            {
                // Ignore reconciler cancel errors: this reconciler might have shut down
                // because some other change superceded it.  We will return a nonzero number,
                // so the caller knows they might have to call again to quiesce the system.
            }
            Err(e) => {
                return Err(e);
            }
        };

        tracing::info!(
            "{} reconciles in reconcile_all, {} waiters",
            reconciles_spawned,
            waiter_count
        );

        Ok(std::cmp::max(waiter_count, reconciles_spawned))
    }

    async fn stop_reconciliations(&self, reason: StopReconciliationsReason) {
        // Cancel all on-going reconciles and wait for them to exit the gate.
        tracing::info!("{reason}: cancelling and waiting for in-flight reconciles");
        self.reconcilers_cancel.cancel();
        self.reconcilers_gate.close().await;

        // Signal the background loop in [`Service::process_results`] to exit once
        // it has proccessed the results from all the reconciles we cancelled earlier.
        tracing::info!("{reason}: processing results from previously in-flight reconciles");
        self.result_tx.send(ReconcileResultRequest::Stop).ok();
        self.result_tx.closed().await;
    }

    pub async fn shutdown(&self) {
        self.stop_reconciliations(StopReconciliationsReason::ShuttingDown)
            .await;

        // Background tasks hold gate guards: this notifies them of the cancellation and
        // waits for them all to complete.
        tracing::info!("Shutting down: cancelling and waiting for background tasks to exit");
        self.cancel.cancel();
        self.gate.close().await;
    }

    /// Spot check the download lag for a secondary location of a shard.
    /// Should be used as a heuristic, since it's not always precise: the
    /// secondary might have not downloaded the new heat map yet and, hence,
    /// is not aware of the lag.
    ///
    /// Returns:
    /// * Ok(None) if the lag could not be determined from the status,
    /// * Ok(Some(_)) if the lag could be determind
    /// * Err on failures to query the pageserver.
    async fn secondary_lag(
        &self,
        secondary: &NodeId,
        tenant_shard_id: TenantShardId,
    ) -> Result<Option<u64>, mgmt_api::Error> {
        let nodes = self.inner.read().unwrap().nodes.clone();
        let node = nodes.get(secondary).ok_or(mgmt_api::Error::ApiError(
            StatusCode::NOT_FOUND,
            format!("Node with id {} not found", secondary),
        ))?;

        match node
            .with_client_retries(
                |client| async move { client.tenant_secondary_status(tenant_shard_id).await },
                &self.config.jwt_token,
                1,
                3,
                Duration::from_millis(250),
                &self.cancel,
            )
            .await
        {
            Some(Ok(status)) => match status.heatmap_mtime {
                Some(_) => Ok(Some(status.bytes_total - status.bytes_downloaded)),
                None => Ok(None),
            },
            Some(Err(e)) => Err(e),
            None => Err(mgmt_api::Error::Cancelled),
        }
    }

    /// Drain a node by moving the shards attached to it as primaries.
    /// This is a long running operation and it should run as a separate Tokio task.
    pub(crate) async fn drain_node(
        self: &Arc<Self>,
        node_id: NodeId,
        cancel: CancellationToken,
    ) -> Result<(), OperationError> {
        const MAX_SECONDARY_LAG_BYTES_DEFAULT: u64 = 256 * 1024 * 1024;
        let max_secondary_lag_bytes = self
            .config
            .max_secondary_lag_bytes
            .unwrap_or(MAX_SECONDARY_LAG_BYTES_DEFAULT);

        // By default, live migrations are generous about the wait time for getting
        // the secondary location up to speed. When draining, give up earlier in order
        // to not stall the operation when a cold secondary is encountered.
        const SECONDARY_WARMUP_TIMEOUT: Duration = Duration::from_secs(20);
        const SECONDARY_DOWNLOAD_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
        let reconciler_config = ReconcilerConfigBuilder::new()
            .secondary_warmup_timeout(SECONDARY_WARMUP_TIMEOUT)
            .secondary_download_request_timeout(SECONDARY_DOWNLOAD_REQUEST_TIMEOUT)
            .build();

        let mut waiters = Vec::new();

        let mut tid_iter = TenantShardIterator::new({
            let service = self.clone();
            move |last_inspected_shard: Option<TenantShardId>| {
                let locked = &service.inner.read().unwrap();
                let tenants = &locked.tenants;
                let entry = match last_inspected_shard {
                    Some(skip_past) => {
                        // Skip to the last seen tenant shard id
                        let mut cursor = tenants.iter().skip_while(|(tid, _)| **tid != skip_past);

                        // Skip past the last seen
                        cursor.nth(1)
                    }
                    None => tenants.first_key_value(),
                };

                entry.map(|(tid, _)| tid).copied()
            }
        });

        while !tid_iter.finished() {
            if cancel.is_cancelled() {
                match self
                    .node_configure(node_id, None, Some(NodeSchedulingPolicy::Active))
                    .await
                {
                    Ok(()) => return Err(OperationError::Cancelled),
                    Err(err) => {
                        return Err(OperationError::FinalizeError(
                            format!(
                                "Failed to finalise drain cancel of {} by setting scheduling policy to Active: {}",
                                node_id, err
                            )
                            .into(),
                        ));
                    }
                }
            }

            drain_utils::validate_node_state(&node_id, self.inner.read().unwrap().nodes.clone())?;

            while waiters.len() < MAX_RECONCILES_PER_OPERATION {
                let tid = match tid_iter.next() {
                    Some(tid) => tid,
                    None => {
                        break;
                    }
                };

                let tid_drain = TenantShardDrain {
                    drained_node: node_id,
                    tenant_shard_id: tid,
                };

                let dest_node_id = {
                    let locked = self.inner.read().unwrap();

                    match tid_drain
                        .tenant_shard_eligible_for_drain(&locked.tenants, &locked.scheduler)
                    {
                        Some(node_id) => node_id,
                        None => {
                            continue;
                        }
                    }
                };

                match self.secondary_lag(&dest_node_id, tid).await {
                    Ok(Some(lag)) if lag <= max_secondary_lag_bytes => {
                        // The secondary is reasonably up to date.
                        // Migrate to it
                    }
                    Ok(Some(lag)) => {
                        tracing::info!(
                            tenant_id=%tid.tenant_id, shard_id=%tid.shard_slug(),
                            "Secondary on node {dest_node_id} is lagging by {lag}. Skipping reconcile."
                        );
                        continue;
                    }
                    Ok(None) => {
                        tracing::info!(
                            tenant_id=%tid.tenant_id, shard_id=%tid.shard_slug(),
                            "Could not determine lag for secondary on node {dest_node_id}. Skipping reconcile."
                        );
                        continue;
                    }
                    Err(err) => {
                        tracing::warn!(
                            tenant_id=%tid.tenant_id, shard_id=%tid.shard_slug(),
                            "Failed to get secondary lag from node {dest_node_id}. Skipping reconcile: {err}"
                        );
                        continue;
                    }
                }

                {
                    let mut locked = self.inner.write().unwrap();
                    let (nodes, tenants, scheduler) = locked.parts_mut();
                    let rescheduled = tid_drain.reschedule_to_secondary(
                        dest_node_id,
                        tenants,
                        scheduler,
                        nodes,
                    )?;

                    if let Some(tenant_shard) = rescheduled {
                        let waiter = self.maybe_configured_reconcile_shard(
                            tenant_shard,
                            nodes,
                            reconciler_config,
                        );
                        if let Some(some) = waiter {
                            waiters.push(some);
                        }
                    }
                }
            }

            waiters = self
                .await_waiters_remainder(waiters, WAITER_FILL_DRAIN_POLL_TIMEOUT)
                .await;

            failpoint_support::sleep_millis_async!("sleepy-drain-loop", &cancel);
        }

        while !waiters.is_empty() {
            if cancel.is_cancelled() {
                match self
                    .node_configure(node_id, None, Some(NodeSchedulingPolicy::Active))
                    .await
                {
                    Ok(()) => return Err(OperationError::Cancelled),
                    Err(err) => {
                        return Err(OperationError::FinalizeError(
                            format!(
                                "Failed to finalise drain cancel of {} by setting scheduling policy to Active: {}",
                                node_id, err
                            )
                            .into(),
                        ));
                    }
                }
            }

            tracing::info!("Awaiting {} pending drain reconciliations", waiters.len());

            waiters = self
                .await_waiters_remainder(waiters, SHORT_RECONCILE_TIMEOUT)
                .await;
        }

        // At this point we have done the best we could to drain shards from this node.
        // Set the node scheduling policy to `[NodeSchedulingPolicy::PauseForRestart]`
        // to complete the drain.
        if let Err(err) = self
            .node_configure(node_id, None, Some(NodeSchedulingPolicy::PauseForRestart))
            .await
        {
            // This is not fatal. Anything that is polling the node scheduling policy to detect
            // the end of the drain operations will hang, but all such places should enforce an
            // overall timeout. The scheduling policy will be updated upon node re-attach and/or
            // by the counterpart fill operation.
            return Err(OperationError::FinalizeError(
                format!(
                    "Failed to finalise drain of {node_id} by setting scheduling policy to PauseForRestart: {err}"
                )
                .into(),
            ));
        }

        Ok(())
    }

    /// Create a node fill plan (pick secondaries to promote), based on:
    /// 1. Shards which have a secondary on this node, and this node is in their home AZ, and are currently attached to a node
    ///    outside their home AZ, should be migrated back here.
    /// 2. If after step 1 we have not migrated enough shards for this node to have its fair share of
    ///    attached shards, we will promote more shards from the nodes with the most attached shards, unless
    ///    those shards have a home AZ that doesn't match the node we're filling.
    fn fill_node_plan(&self, node_id: NodeId) -> Vec<TenantShardId> {
        let mut locked = self.inner.write().unwrap();
        let (nodes, tenants, _scheduler) = locked.parts_mut();

        let node_az = nodes
            .get(&node_id)
            .expect("Node must exist")
            .get_availability_zone_id()
            .clone();

        // The tenant shard IDs that we plan to promote from secondary to attached on this node
        let mut plan = Vec::new();

        // Collect shards which do not have a preferred AZ & are elegible for moving in stage 2
        let mut free_tids_by_node: HashMap<NodeId, Vec<TenantShardId>> = HashMap::new();

        // Don't respect AZ preferences if there is only one AZ.  This comes up in tests, but it could
        // conceivably come up in real life if deploying a single-AZ region intentionally.
        let respect_azs = nodes
            .values()
            .map(|n| n.get_availability_zone_id())
            .unique()
            .count()
            > 1;

        // Step 1: collect all shards that we are required to migrate back to this node because their AZ preference
        // requires it.
        for (tsid, tenant_shard) in tenants {
            if !tenant_shard.intent.get_secondary().contains(&node_id) {
                // Shard doesn't have a secondary on this node, ignore it.
                continue;
            }

            // AZ check: when filling nodes after a restart, our intent is to move _back_ the
            // shards which belong on this node, not to promote shards whose scheduling preference
            // would be on their currently attached node.  So will avoid promoting shards whose
            // home AZ doesn't match the AZ of the node we're filling.
            match tenant_shard.preferred_az() {
                _ if !respect_azs => {
                    if let Some(primary) = tenant_shard.intent.get_attached() {
                        free_tids_by_node.entry(*primary).or_default().push(*tsid);
                    }
                }
                None => {
                    // Shard doesn't have an AZ preference: it is elegible to be moved, but we
                    // will only do so if our target shard count requires it.
                    if let Some(primary) = tenant_shard.intent.get_attached() {
                        free_tids_by_node.entry(*primary).or_default().push(*tsid);
                    }
                }
                Some(az) if az == &node_az => {
                    // This shard's home AZ is equal to the node we're filling: it should
                    // be moved back to this node as part of filling, unless its currently
                    // attached location is also in its home AZ.
                    if let Some(primary) = tenant_shard.intent.get_attached() {
                        if nodes
                            .get(primary)
                            .expect("referenced node must exist")
                            .get_availability_zone_id()
                            != tenant_shard
                                .preferred_az()
                                .expect("tenant must have an AZ preference")
                        {
                            plan.push(*tsid)
                        }
                    } else {
                        plan.push(*tsid)
                    }
                }
                Some(_) => {
                    // This shard's home AZ is somewhere other than the node we're filling,
                    // it may not be moved back to this node as part of filling.  Ignore it
                }
            }
        }

        // Step 2: also promote any AZ-agnostic shards as required to achieve the target number of attachments
        let fill_requirement = locked.scheduler.compute_fill_requirement(node_id);

        let expected_attached = locked.scheduler.expected_attached_shard_count();
        let nodes_by_load = locked.scheduler.nodes_by_attached_shard_count();

        let mut promoted_per_tenant: HashMap<TenantId, usize> = HashMap::new();

        for (node_id, attached) in nodes_by_load {
            let available = locked.nodes.get(&node_id).is_some_and(|n| n.is_available());
            if !available {
                continue;
            }

            if plan.len() >= fill_requirement
                || free_tids_by_node.is_empty()
                || attached <= expected_attached
            {
                break;
            }

            let can_take = attached - expected_attached;
            let needed = fill_requirement - plan.len();
            let mut take = std::cmp::min(can_take, needed);

            let mut remove_node = false;
            while take > 0 {
                match free_tids_by_node.get_mut(&node_id) {
                    Some(tids) => match tids.pop() {
                        Some(tid) => {
                            let max_promote_for_tenant = std::cmp::max(
                                tid.shard_count.count() as usize / locked.nodes.len(),
                                1,
                            );
                            let promoted = promoted_per_tenant.entry(tid.tenant_id).or_default();
                            if *promoted < max_promote_for_tenant {
                                plan.push(tid);
                                *promoted += 1;
                                take -= 1;
                            }
                        }
                        None => {
                            remove_node = true;
                            break;
                        }
                    },
                    None => {
                        break;
                    }
                }
            }

            if remove_node {
                free_tids_by_node.remove(&node_id);
            }
        }

        plan
    }

    /// Fill a node by promoting its secondaries until the cluster is balanced
    /// with regards to attached shard counts. Note that this operation only
    /// makes sense as a counterpart to the drain implemented in [`Service::drain_node`].
    /// This is a long running operation and it should run as a separate Tokio task.
    pub(crate) async fn fill_node(
        &self,
        node_id: NodeId,
        cancel: CancellationToken,
    ) -> Result<(), OperationError> {
        const SECONDARY_WARMUP_TIMEOUT: Duration = Duration::from_secs(20);
        const SECONDARY_DOWNLOAD_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
        let reconciler_config = ReconcilerConfigBuilder::new()
            .secondary_warmup_timeout(SECONDARY_WARMUP_TIMEOUT)
            .secondary_download_request_timeout(SECONDARY_DOWNLOAD_REQUEST_TIMEOUT)
            .build();

        let mut tids_to_promote = self.fill_node_plan(node_id);
        let mut waiters = Vec::new();

        // Execute the plan we've composed above. Before aplying each move from the plan,
        // we validate to ensure that it has not gone stale in the meantime.
        while !tids_to_promote.is_empty() {
            if cancel.is_cancelled() {
                match self
                    .node_configure(node_id, None, Some(NodeSchedulingPolicy::Active))
                    .await
                {
                    Ok(()) => return Err(OperationError::Cancelled),
                    Err(err) => {
                        return Err(OperationError::FinalizeError(
                            format!(
                                "Failed to finalise drain cancel of {} by setting scheduling policy to Active: {}",
                                node_id, err
                            )
                            .into(),
                        ));
                    }
                }
            }

            {
                let mut locked = self.inner.write().unwrap();
                let (nodes, tenants, scheduler) = locked.parts_mut();

                let node = nodes.get(&node_id).ok_or(OperationError::NodeStateChanged(
                    format!("node {node_id} was removed").into(),
                ))?;

                let current_policy = node.get_scheduling();
                if !matches!(current_policy, NodeSchedulingPolicy::Filling) {
                    // TODO(vlad): maybe cancel pending reconciles before erroring out. need to think
                    // about it
                    return Err(OperationError::NodeStateChanged(
                        format!("node {node_id} changed state to {current_policy:?}").into(),
                    ));
                }

                while waiters.len() < MAX_RECONCILES_PER_OPERATION {
                    if let Some(tid) = tids_to_promote.pop() {
                        if let Some(tenant_shard) = tenants.get_mut(&tid) {
                            // If the node being filled is not a secondary anymore,
                            // skip the promotion.
                            if !tenant_shard.intent.get_secondary().contains(&node_id) {
                                continue;
                            }

                            let previously_attached_to = *tenant_shard.intent.get_attached();
                            match tenant_shard.reschedule_to_secondary(Some(node_id), scheduler) {
                                Err(e) => {
                                    tracing::warn!(
                                        tenant_id=%tid.tenant_id, shard_id=%tid.shard_slug(),
                                        "Scheduling error when filling pageserver {} : {e}", node_id
                                    );
                                }
                                Ok(()) => {
                                    tracing::info!(
                                        tenant_id=%tid.tenant_id, shard_id=%tid.shard_slug(),
                                        "Rescheduled shard while filling node {}: {:?} -> {}",
                                        node_id,
                                        previously_attached_to,
                                        node_id
                                    );

                                    if let Some(waiter) = self.maybe_configured_reconcile_shard(
                                        tenant_shard,
                                        nodes,
                                        reconciler_config,
                                    ) {
                                        waiters.push(waiter);
                                    }
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
            }

            waiters = self
                .await_waiters_remainder(waiters, WAITER_FILL_DRAIN_POLL_TIMEOUT)
                .await;
        }

        while !waiters.is_empty() {
            if cancel.is_cancelled() {
                match self
                    .node_configure(node_id, None, Some(NodeSchedulingPolicy::Active))
                    .await
                {
                    Ok(()) => return Err(OperationError::Cancelled),
                    Err(err) => {
                        return Err(OperationError::FinalizeError(
                            format!(
                                "Failed to finalise drain cancel of {} by setting scheduling policy to Active: {}",
                                node_id, err
                            )
                            .into(),
                        ));
                    }
                }
            }

            tracing::info!("Awaiting {} pending fill reconciliations", waiters.len());

            waiters = self
                .await_waiters_remainder(waiters, SHORT_RECONCILE_TIMEOUT)
                .await;
        }

        if let Err(err) = self
            .node_configure(node_id, None, Some(NodeSchedulingPolicy::Active))
            .await
        {
            // This isn't a huge issue since the filling process starts upon request. However, it
            // will prevent the next drain from starting. The only case in which this can fail
            // is database unavailability. Such a case will require manual intervention.
            return Err(OperationError::FinalizeError(
                format!("Failed to finalise fill of {node_id} by setting scheduling policy to Active: {err}")
                    .into(),
            ));
        }

        Ok(())
    }

    /// Updates scrubber metadata health check results.
    pub(crate) async fn metadata_health_update(
        &self,
        update_req: MetadataHealthUpdateRequest,
    ) -> Result<(), ApiError> {
        let now = chrono::offset::Utc::now();
        let (healthy_records, unhealthy_records) = {
            let locked = self.inner.read().unwrap();
            let healthy_records = update_req
                .healthy_tenant_shards
                .into_iter()
                // Retain only health records associated with tenant shards managed by storage controller.
                .filter(|tenant_shard_id| locked.tenants.contains_key(tenant_shard_id))
                .map(|tenant_shard_id| MetadataHealthPersistence::new(tenant_shard_id, true, now))
                .collect();
            let unhealthy_records = update_req
                .unhealthy_tenant_shards
                .into_iter()
                .filter(|tenant_shard_id| locked.tenants.contains_key(tenant_shard_id))
                .map(|tenant_shard_id| MetadataHealthPersistence::new(tenant_shard_id, false, now))
                .collect();

            (healthy_records, unhealthy_records)
        };

        self.persistence
            .update_metadata_health_records(healthy_records, unhealthy_records, now)
            .await?;
        Ok(())
    }

    /// Lists the tenant shards that has unhealthy metadata status.
    pub(crate) async fn metadata_health_list_unhealthy(
        &self,
    ) -> Result<Vec<TenantShardId>, ApiError> {
        let result = self
            .persistence
            .list_unhealthy_metadata_health_records()
            .await?
            .iter()
            .map(|p| p.get_tenant_shard_id().unwrap())
            .collect();

        Ok(result)
    }

    /// Lists the tenant shards that have not been scrubbed for some duration.
    pub(crate) async fn metadata_health_list_outdated(
        &self,
        not_scrubbed_for: Duration,
    ) -> Result<Vec<MetadataHealthRecord>, ApiError> {
        let earlier = chrono::offset::Utc::now() - not_scrubbed_for;
        let result = self
            .persistence
            .list_outdated_metadata_health_records(earlier)
            .await?
            .into_iter()
            .map(|record| record.into())
            .collect();
        Ok(result)
    }

    pub(crate) fn get_leadership_status(&self) -> LeadershipStatus {
        self.inner.read().unwrap().get_leadership_status()
    }

    pub(crate) async fn step_down(&self) -> GlobalObservedState {
        tracing::info!("Received step down request from peer");
        failpoint_support::sleep_millis_async!("sleep-on-step-down-handling");

        self.inner.write().unwrap().step_down();
        // TODO: would it make sense to have a time-out for this?
        self.stop_reconciliations(StopReconciliationsReason::SteppingDown)
            .await;

        let mut global_observed = GlobalObservedState::default();
        let locked = self.inner.read().unwrap();
        for (tid, tenant_shard) in locked.tenants.iter() {
            global_observed
                .0
                .insert(*tid, tenant_shard.observed.clone());
        }

        global_observed
    }

    pub(crate) async fn safekeepers_for_new_timeline(&self) -> Result<Vec<NodeId>, ApiError> {
        let mut all_safekeepers = self
            .persistence
            .list_safekeepers_with_timeline_count()
            .await?;
        all_safekeepers.sort_by_key(|sk| sk.2);
        let mut sks = Vec::new();
        let mut azs = HashSet::new();
        for (sk_id, az_id, _timeline_count) in all_safekeepers.iter() {
            if !azs.insert(az_id) {
                continue;
            }
            sks.push(*sk_id);
            if sks.len() == 3 {
                break;
            }
        }
        if sks.len() == 3 {
            Ok(sks)
        } else {
            Err(ApiError::InternalServerError(anyhow!(
                "couldn't find three safekeepers in different AZs for new timeline"
            )))
        }
    }

    pub(crate) async fn safekeepers_list(
        &self,
    ) -> Result<Vec<SafekeeperDescribeResponse>, DatabaseError> {
        self.persistence
            .list_safekeepers()
            .await?
            .into_iter()
            .map(|v| v.as_describe_response())
            .collect::<Result<Vec<_>, _>>()
    }

    pub(crate) async fn get_safekeeper(
        &self,
        id: i64,
    ) -> Result<SafekeeperDescribeResponse, DatabaseError> {
        self.persistence
            .safekeeper_get(id)
            .await
            .and_then(|v| v.as_describe_response())
    }

    pub(crate) async fn upsert_safekeeper(
        &self,
        record: crate::persistence::SafekeeperUpsert,
    ) -> Result<(), DatabaseError> {
        self.persistence.safekeeper_upsert(record).await
    }

    pub(crate) async fn set_safekeeper_scheduling_policy(
        &self,
        id: i64,
        scheduling_policy: SkSchedulingPolicy,
    ) -> Result<(), DatabaseError> {
        self.persistence
            .set_safekeeper_scheduling_policy(id, scheduling_policy)
            .await
    }

    pub(crate) async fn update_shards_preferred_azs(
        &self,
        req: ShardsPreferredAzsRequest,
    ) -> Result<ShardsPreferredAzsResponse, ApiError> {
        let preferred_azs = req.preferred_az_ids.into_iter().collect::<Vec<_>>();
        let updated = self
            .persistence
            .set_tenant_shard_preferred_azs(preferred_azs)
            .await
            .map_err(|err| {
                ApiError::InternalServerError(anyhow::anyhow!(
                    "Failed to persist preferred AZs: {err}"
                ))
            })?;

        let mut updated_in_mem_and_db = Vec::default();

        let mut locked = self.inner.write().unwrap();
        for (tid, az_id) in updated {
            let shard = locked.tenants.get_mut(&tid);
            if let Some(shard) = shard {
                shard.set_preferred_az(az_id);
                updated_in_mem_and_db.push(tid);
            }
        }

        Ok(ShardsPreferredAzsResponse {
            updated: updated_in_mem_and_db,
        })
    }
}
