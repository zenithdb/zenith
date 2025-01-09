//!
//! Management HTTP API
//!
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use enumset::EnumSet;
use futures::StreamExt;
use futures::TryFutureExt;
use humantime::format_rfc3339;
use hyper::header;
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use metrics::launch_timestamp::LaunchTimestamp;
use pageserver_api::models::virtual_file::IoMode;
use pageserver_api::models::DownloadRemoteLayersTaskSpawnRequest;
use pageserver_api::models::IngestAuxFilesRequest;
use pageserver_api::models::ListAuxFilesRequest;
use pageserver_api::models::LocationConfig;
use pageserver_api::models::LocationConfigListResponse;
use pageserver_api::models::LocationConfigMode;
use pageserver_api::models::LsnLease;
use pageserver_api::models::LsnLeaseRequest;
use pageserver_api::models::OffloadedTimelineInfo;
use pageserver_api::models::ShardParameters;
use pageserver_api::models::TenantConfigPatchRequest;
use pageserver_api::models::TenantDetails;
use pageserver_api::models::TenantLocationConfigRequest;
use pageserver_api::models::TenantLocationConfigResponse;
use pageserver_api::models::TenantScanRemoteStorageResponse;
use pageserver_api::models::TenantScanRemoteStorageShard;
use pageserver_api::models::TenantShardLocation;
use pageserver_api::models::TenantShardSplitRequest;
use pageserver_api::models::TenantShardSplitResponse;
use pageserver_api::models::TenantSorting;
use pageserver_api::models::TenantState;
use pageserver_api::models::TimelineArchivalConfigRequest;
use pageserver_api::models::TimelineCreateRequestMode;
use pageserver_api::models::TimelineCreateRequestModeImportPgdata;
use pageserver_api::models::TimelinesInfoAndOffloaded;
use pageserver_api::models::TopTenantShardItem;
use pageserver_api::models::TopTenantShardsRequest;
use pageserver_api::models::TopTenantShardsResponse;
use pageserver_api::shard::ShardCount;
use pageserver_api::shard::TenantShardId;
use remote_storage::DownloadError;
use remote_storage::GenericRemoteStorage;
use remote_storage::TimeTravelError;
use tenant_size_model::{svg::SvgBranchKind, SizeResult, StorageModel};
use tokio_util::io::StreamReader;
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::auth::JwtAuth;
use utils::failpoint_support::failpoints_handler;
use utils::http::endpoint::{
    profile_cpu_handler, profile_heap_handler, prometheus_metrics_handler, request_span,
};
use utils::http::request::must_parse_query_param;
use utils::http::request::{get_request_param, must_get_query_param, parse_query_param};

use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::deletion_queue::DeletionQueueClient;
use crate::pgdatadir_mapping::LsnForTimestamp;
use crate::task_mgr::TaskKind;
use crate::tenant::config::{LocationConf, TenantConfOpt};
use crate::tenant::mgr::GetActiveTenantError;
use crate::tenant::mgr::{
    GetTenantError, TenantManager, TenantMapError, TenantMapInsertError, TenantSlotError,
    TenantSlotUpsertError, TenantStateError,
};
use crate::tenant::mgr::{TenantSlot, UpsertLocationError};
use crate::tenant::remote_timeline_client;
use crate::tenant::remote_timeline_client::download_index_part;
use crate::tenant::remote_timeline_client::list_remote_tenant_shards;
use crate::tenant::remote_timeline_client::list_remote_timelines;
use crate::tenant::secondary::SecondaryController;
use crate::tenant::size::ModelInputs;
use crate::tenant::storage_layer::LayerAccessStatsReset;
use crate::tenant::storage_layer::LayerName;
use crate::tenant::timeline::import_pgdata;
use crate::tenant::timeline::offload::offload_timeline;
use crate::tenant::timeline::offload::OffloadError;
use crate::tenant::timeline::CompactFlags;
use crate::tenant::timeline::CompactOptions;
use crate::tenant::timeline::CompactRequest;
use crate::tenant::timeline::CompactionError;
use crate::tenant::timeline::Timeline;
use crate::tenant::GetTimelineError;
use crate::tenant::OffloadedTimeline;
use crate::tenant::{LogicalSizeCalculationCause, PageReconstructError};
use crate::DEFAULT_PG_VERSION;
use crate::{disk_usage_eviction_task, tenant};
use pageserver_api::models::{
    StatusResponse, TenantConfigRequest, TenantInfo, TimelineCreateRequest, TimelineGcRequest,
    TimelineInfo,
};
use utils::{
    auth::SwappableJwtAuth,
    generation::Generation,
    http::{
        endpoint::{self, attach_openapi_ui, auth_middleware, check_permission_with},
        error::{ApiError, HttpErrorBody},
        json::{json_request, json_request_maybe, json_response},
        request::parse_request_param,
        RequestExt, RouterBuilder,
    },
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

// For APIs that require an Active tenant, how long should we block waiting for that state?
// This is not functionally necessary (clients will retry), but avoids generating a lot of
// failed API calls while tenants are activating.
#[cfg(not(feature = "testing"))]
pub(crate) const ACTIVE_TENANT_TIMEOUT: Duration = Duration::from_millis(5000);

// Tests run on slow/oversubscribed nodes, and may need to wait much longer for tenants to
// finish attaching, if calls to remote storage are slow.
#[cfg(feature = "testing")]
pub(crate) const ACTIVE_TENANT_TIMEOUT: Duration = Duration::from_millis(30000);

pub struct State {
    conf: &'static PageServerConf,
    tenant_manager: Arc<TenantManager>,
    auth: Option<Arc<SwappableJwtAuth>>,
    allowlist_routes: &'static [&'static str],
    remote_storage: GenericRemoteStorage,
    broker_client: storage_broker::BrokerClientChannel,
    disk_usage_eviction_state: Arc<disk_usage_eviction_task::State>,
    deletion_queue_client: DeletionQueueClient,
    secondary_controller: SecondaryController,
    latest_utilization: tokio::sync::Mutex<Option<(std::time::Instant, bytes::Bytes)>>,
}

impl State {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conf: &'static PageServerConf,
        tenant_manager: Arc<TenantManager>,
        auth: Option<Arc<SwappableJwtAuth>>,
        remote_storage: GenericRemoteStorage,
        broker_client: storage_broker::BrokerClientChannel,
        disk_usage_eviction_state: Arc<disk_usage_eviction_task::State>,
        deletion_queue_client: DeletionQueueClient,
        secondary_controller: SecondaryController,
    ) -> anyhow::Result<Self> {
        let allowlist_routes = &[
            "/v1/status",
            "/v1/doc",
            "/swagger.yml",
            "/metrics",
            "/profile/cpu",
            "/profile/heap",
        ];
        Ok(Self {
            conf,
            tenant_manager,
            auth,
            allowlist_routes,
            remote_storage,
            broker_client,
            disk_usage_eviction_state,
            deletion_queue_client,
            secondary_controller,
            latest_utilization: Default::default(),
        })
    }
}

#[inline(always)]
fn get_state(request: &Request<Body>) -> &State {
    request
        .data::<Arc<State>>()
        .expect("unknown state type")
        .as_ref()
}

#[inline(always)]
fn get_config(request: &Request<Body>) -> &'static PageServerConf {
    get_state(request).conf
}

/// Check that the requester is authorized to operate on given tenant
fn check_permission(request: &Request<Body>, tenant_id: Option<TenantId>) -> Result<(), ApiError> {
    check_permission_with(request, |claims| {
        crate::auth::check_permission(claims, tenant_id)
    })
}

impl From<PageReconstructError> for ApiError {
    fn from(pre: PageReconstructError) -> ApiError {
        match pre {
            PageReconstructError::Other(other) => ApiError::InternalServerError(other),
            PageReconstructError::MissingKey(e) => ApiError::InternalServerError(e.into()),
            PageReconstructError::Cancelled => ApiError::Cancelled,
            PageReconstructError::AncestorLsnTimeout(e) => ApiError::Timeout(format!("{e}").into()),
            PageReconstructError::WalRedo(pre) => ApiError::InternalServerError(pre),
        }
    }
}

impl From<TenantMapInsertError> for ApiError {
    fn from(tmie: TenantMapInsertError) -> ApiError {
        match tmie {
            TenantMapInsertError::SlotError(e) => e.into(),
            TenantMapInsertError::SlotUpsertError(e) => e.into(),
            TenantMapInsertError::Other(e) => ApiError::InternalServerError(e),
        }
    }
}

impl From<TenantSlotError> for ApiError {
    fn from(e: TenantSlotError) -> ApiError {
        use TenantSlotError::*;
        match e {
            NotFound(tenant_id) => {
                ApiError::NotFound(anyhow::anyhow!("NotFound: tenant {tenant_id}").into())
            }
            InProgress => {
                ApiError::ResourceUnavailable("Tenant is being modified concurrently".into())
            }
            MapState(e) => e.into(),
        }
    }
}

impl From<TenantSlotUpsertError> for ApiError {
    fn from(e: TenantSlotUpsertError) -> ApiError {
        use TenantSlotUpsertError::*;
        match e {
            InternalError(e) => ApiError::InternalServerError(anyhow::anyhow!("{e}")),
            MapState(e) => e.into(),
            ShuttingDown(_) => ApiError::ShuttingDown,
        }
    }
}

impl From<UpsertLocationError> for ApiError {
    fn from(e: UpsertLocationError) -> ApiError {
        use UpsertLocationError::*;
        match e {
            BadRequest(e) => ApiError::BadRequest(e),
            Unavailable(_) => ApiError::ShuttingDown,
            e @ InProgress => ApiError::Conflict(format!("{e}")),
            Flush(e) | InternalError(e) => ApiError::InternalServerError(e),
        }
    }
}

impl From<TenantMapError> for ApiError {
    fn from(e: TenantMapError) -> ApiError {
        use TenantMapError::*;
        match e {
            StillInitializing | ShuttingDown => {
                ApiError::ResourceUnavailable(format!("{e}").into())
            }
        }
    }
}

impl From<TenantStateError> for ApiError {
    fn from(tse: TenantStateError) -> ApiError {
        match tse {
            TenantStateError::IsStopping(_) => {
                ApiError::ResourceUnavailable("Tenant is stopping".into())
            }
            TenantStateError::SlotError(e) => e.into(),
            TenantStateError::SlotUpsertError(e) => e.into(),
            TenantStateError::Other(e) => ApiError::InternalServerError(anyhow!(e)),
        }
    }
}

impl From<GetTenantError> for ApiError {
    fn from(tse: GetTenantError) -> ApiError {
        match tse {
            GetTenantError::NotFound(tid) => ApiError::NotFound(anyhow!("tenant {tid}").into()),
            GetTenantError::ShardNotFound(tid) => {
                ApiError::NotFound(anyhow!("tenant {tid}").into())
            }
            GetTenantError::NotActive(_) => {
                // Why is this not `ApiError::NotFound`?
                // Because we must be careful to never return 404 for a tenant if it does
                // in fact exist locally. If we did, the caller could draw the conclusion
                // that it can attach the tenant to another PS and we'd be in split-brain.
                ApiError::ResourceUnavailable("Tenant not yet active".into())
            }
            GetTenantError::MapState(e) => ApiError::ResourceUnavailable(format!("{e}").into()),
        }
    }
}

impl From<GetTimelineError> for ApiError {
    fn from(gte: GetTimelineError) -> Self {
        // Rationale: tenant is activated only after eligble timelines activate
        ApiError::NotFound(gte.into())
    }
}

impl From<GetActiveTenantError> for ApiError {
    fn from(e: GetActiveTenantError) -> ApiError {
        match e {
            GetActiveTenantError::Broken(reason) => {
                ApiError::InternalServerError(anyhow!("tenant is broken: {}", reason))
            }
            GetActiveTenantError::WillNotBecomeActive(TenantState::Stopping { .. }) => {
                ApiError::ShuttingDown
            }
            GetActiveTenantError::WillNotBecomeActive(_) => ApiError::Conflict(format!("{}", e)),
            GetActiveTenantError::Cancelled => ApiError::ShuttingDown,
            GetActiveTenantError::NotFound(gte) => gte.into(),
            GetActiveTenantError::WaitForActiveTimeout { .. } => {
                ApiError::ResourceUnavailable(format!("{}", e).into())
            }
            GetActiveTenantError::SwitchedTenant => {
                // in our HTTP handlers, this error doesn't happen
                // TODO: separate error types
                ApiError::ResourceUnavailable("switched tenant".into())
            }
        }
    }
}

impl From<crate::tenant::DeleteTimelineError> for ApiError {
    fn from(value: crate::tenant::DeleteTimelineError) -> Self {
        use crate::tenant::DeleteTimelineError::*;
        match value {
            NotFound => ApiError::NotFound(anyhow::anyhow!("timeline not found").into()),
            HasChildren(children) => ApiError::PreconditionFailed(
                format!("Cannot delete timeline which has child timelines: {children:?}")
                    .into_boxed_str(),
            ),
            a @ AlreadyInProgress(_) => ApiError::Conflict(a.to_string()),
            Cancelled => ApiError::ResourceUnavailable("shutting down".into()),
            Other(e) => ApiError::InternalServerError(e),
        }
    }
}

impl From<crate::tenant::TimelineArchivalError> for ApiError {
    fn from(value: crate::tenant::TimelineArchivalError) -> Self {
        use crate::tenant::TimelineArchivalError::*;
        match value {
            NotFound => ApiError::NotFound(anyhow::anyhow!("timeline not found").into()),
            Timeout => ApiError::Timeout("hit pageserver internal timeout".into()),
            Cancelled => ApiError::ShuttingDown,
            e @ HasArchivedParent(_) => {
                ApiError::PreconditionFailed(e.to_string().into_boxed_str())
            }
            HasUnarchivedChildren(children) => ApiError::PreconditionFailed(
                format!(
                    "Cannot archive timeline which has non-archived child timelines: {children:?}"
                )
                .into_boxed_str(),
            ),
            a @ AlreadyInProgress => ApiError::Conflict(a.to_string()),
            Other(e) => ApiError::InternalServerError(e),
        }
    }
}

impl From<crate::tenant::mgr::DeleteTimelineError> for ApiError {
    fn from(value: crate::tenant::mgr::DeleteTimelineError) -> Self {
        use crate::tenant::mgr::DeleteTimelineError::*;
        match value {
            // Report Precondition failed so client can distinguish between
            // "tenant is missing" case from "timeline is missing"
            Tenant(GetTenantError::NotFound(..)) => ApiError::PreconditionFailed(
                "Requested tenant is missing".to_owned().into_boxed_str(),
            ),
            Tenant(t) => ApiError::from(t),
            Timeline(t) => ApiError::from(t),
        }
    }
}

impl From<crate::tenant::mgr::DeleteTenantError> for ApiError {
    fn from(value: crate::tenant::mgr::DeleteTenantError) -> Self {
        use crate::tenant::mgr::DeleteTenantError::*;
        match value {
            SlotError(e) => e.into(),
            Other(o) => ApiError::InternalServerError(o),
            Cancelled => ApiError::ShuttingDown,
        }
    }
}

impl From<crate::tenant::secondary::SecondaryTenantError> for ApiError {
    fn from(ste: crate::tenant::secondary::SecondaryTenantError) -> ApiError {
        use crate::tenant::secondary::SecondaryTenantError;
        match ste {
            SecondaryTenantError::GetTenant(gte) => gte.into(),
            SecondaryTenantError::ShuttingDown => ApiError::ShuttingDown,
        }
    }
}

// Helper function to construct a TimelineInfo struct for a timeline
async fn build_timeline_info(
    timeline: &Arc<Timeline>,
    include_non_incremental_logical_size: bool,
    force_await_initial_logical_size: bool,
    ctx: &RequestContext,
) -> anyhow::Result<TimelineInfo> {
    crate::tenant::debug_assert_current_span_has_tenant_and_timeline_id();

    if force_await_initial_logical_size {
        timeline.clone().await_initial_logical_size().await
    }

    let mut info = build_timeline_info_common(
        timeline,
        ctx,
        tenant::timeline::GetLogicalSizePriority::Background,
    )
    .await?;
    if include_non_incremental_logical_size {
        // XXX we should be using spawn_ondemand_logical_size_calculation here.
        // Otherwise, if someone deletes the timeline / detaches the tenant while
        // we're executing this function, we will outlive the timeline on-disk state.
        info.current_logical_size_non_incremental = Some(
            timeline
                .get_current_logical_size_non_incremental(info.last_record_lsn, ctx)
                .await?,
        );
    }
    Ok(info)
}

async fn build_timeline_info_common(
    timeline: &Arc<Timeline>,
    ctx: &RequestContext,
    logical_size_task_priority: tenant::timeline::GetLogicalSizePriority,
) -> anyhow::Result<TimelineInfo> {
    crate::tenant::debug_assert_current_span_has_tenant_and_timeline_id();
    let initdb_lsn = timeline.initdb_lsn;
    let last_record_lsn = timeline.get_last_record_lsn();
    let (wal_source_connstr, last_received_msg_lsn, last_received_msg_ts) = {
        let guard = timeline.last_received_wal.lock().unwrap();
        if let Some(info) = guard.as_ref() {
            (
                Some(format!("{}", info.wal_source_connconf)), // Password is hidden, but it's for statistics only.
                Some(info.last_received_msg_lsn),
                Some(info.last_received_msg_ts),
            )
        } else {
            (None, None, None)
        }
    };

    let ancestor_timeline_id = timeline.get_ancestor_timeline_id();
    let ancestor_lsn = match timeline.get_ancestor_lsn() {
        Lsn(0) => None,
        lsn @ Lsn(_) => Some(lsn),
    };
    let current_logical_size = timeline.get_current_logical_size(logical_size_task_priority, ctx);
    let current_physical_size = Some(timeline.layer_size_sum().await);
    let state = timeline.current_state();
    // Report is_archived = false if the timeline is still loading
    let is_archived = timeline.is_archived().unwrap_or(false);
    let remote_consistent_lsn_projected = timeline
        .get_remote_consistent_lsn_projected()
        .unwrap_or(Lsn(0));
    let remote_consistent_lsn_visible = timeline
        .get_remote_consistent_lsn_visible()
        .unwrap_or(Lsn(0));

    let walreceiver_status = timeline.walreceiver_status();

    let (pitr_history_size, within_ancestor_pitr) = timeline.get_pitr_history_stats();

    let info = TimelineInfo {
        tenant_id: timeline.tenant_shard_id,
        timeline_id: timeline.timeline_id,
        ancestor_timeline_id,
        ancestor_lsn,
        disk_consistent_lsn: timeline.get_disk_consistent_lsn(),
        remote_consistent_lsn: remote_consistent_lsn_projected,
        remote_consistent_lsn_visible,
        initdb_lsn,
        last_record_lsn,
        prev_record_lsn: Some(timeline.get_prev_record_lsn()),
        latest_gc_cutoff_lsn: *timeline.get_latest_gc_cutoff_lsn(),
        current_logical_size: current_logical_size.size_dont_care_about_accuracy(),
        current_logical_size_is_accurate: match current_logical_size.accuracy() {
            tenant::timeline::logical_size::Accuracy::Approximate => false,
            tenant::timeline::logical_size::Accuracy::Exact => true,
        },
        directory_entries_counts: timeline.get_directory_metrics().to_vec(),
        current_physical_size,
        current_logical_size_non_incremental: None,
        pitr_history_size,
        within_ancestor_pitr,
        timeline_dir_layer_file_size_sum: None,
        wal_source_connstr,
        last_received_msg_lsn,
        last_received_msg_ts,
        pg_version: timeline.pg_version,

        state,
        is_archived: Some(is_archived),

        walreceiver_status,
    };
    Ok(info)
}

fn build_timeline_offloaded_info(offloaded: &Arc<OffloadedTimeline>) -> OffloadedTimelineInfo {
    let &OffloadedTimeline {
        tenant_shard_id,
        timeline_id,
        ancestor_retain_lsn,
        ancestor_timeline_id,
        archived_at,
        ..
    } = offloaded.as_ref();
    OffloadedTimelineInfo {
        tenant_id: tenant_shard_id,
        timeline_id,
        ancestor_retain_lsn,
        ancestor_timeline_id,
        archived_at: archived_at.and_utc(),
    }
}

// healthcheck handler
async fn status_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;
    let config = get_config(&request);
    json_response(StatusCode::OK, StatusResponse { id: config.id })
}

async fn reload_auth_validation_keys_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;
    let config = get_config(&request);
    let state = get_state(&request);
    let Some(shared_auth) = &state.auth else {
        return json_response(StatusCode::BAD_REQUEST, ());
    };
    // unwrap is ok because check is performed when creating config, so path is set and exists
    let key_path = config.auth_validation_public_key_path.as_ref().unwrap();
    info!("Reloading public key(s) for verifying JWT tokens from {key_path:?}");

    match JwtAuth::from_key_path(key_path) {
        Ok(new_auth) => {
            shared_auth.swap(new_auth);
            json_response(StatusCode::OK, ())
        }
        Err(e) => {
            let err_msg = "Error reloading public keys";
            warn!("Error reloading public keys from {key_path:?}: {e:}");
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                HttpErrorBody::from_msg(err_msg.to_string()),
            )
        }
    }
}

async fn timeline_create_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let request_data: TimelineCreateRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let new_timeline_id = request_data.new_timeline_id;
    // fill in the default pg_version if not provided & convert request into domain model
    let params: tenant::CreateTimelineParams = match request_data.mode {
        TimelineCreateRequestMode::Bootstrap {
            existing_initdb_timeline_id,
            pg_version,
        } => tenant::CreateTimelineParams::Bootstrap(tenant::CreateTimelineParamsBootstrap {
            new_timeline_id,
            existing_initdb_timeline_id,
            pg_version: pg_version.unwrap_or(DEFAULT_PG_VERSION),
        }),
        TimelineCreateRequestMode::Branch {
            ancestor_timeline_id,
            ancestor_start_lsn,
            pg_version: _,
        } => tenant::CreateTimelineParams::Branch(tenant::CreateTimelineParamsBranch {
            new_timeline_id,
            ancestor_timeline_id,
            ancestor_start_lsn,
        }),
        TimelineCreateRequestMode::ImportPgdata {
            import_pgdata:
                TimelineCreateRequestModeImportPgdata {
                    location,
                    idempotency_key,
                },
        } => tenant::CreateTimelineParams::ImportPgdata(tenant::CreateTimelineParamsImportPgdata {
            idempotency_key: import_pgdata::index_part_format::IdempotencyKey::new(
                idempotency_key.0,
            ),
            new_timeline_id,
            location: {
                use import_pgdata::index_part_format::Location;
                use pageserver_api::models::ImportPgdataLocation;
                match location {
                    #[cfg(feature = "testing")]
                    ImportPgdataLocation::LocalFs { path } => Location::LocalFs { path },
                    ImportPgdataLocation::AwsS3 {
                        region,
                        bucket,
                        key,
                    } => Location::AwsS3 {
                        region,
                        bucket,
                        key,
                    },
                }
            },
        }),
    };

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Error);

    let state = get_state(&request);

    async {
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;

        tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

        // earlier versions of the code had pg_version and ancestor_lsn in the span
        // => continue to provide that information, but, through a log message that doesn't require us to destructure
        tracing::info!(?params, "creating timeline");

        match tenant
            .create_timeline(params, state.broker_client.clone(), &ctx)
            .await
        {
            Ok(new_timeline) => {
                // Created. Construct a TimelineInfo for it.
                let timeline_info = build_timeline_info_common(
                    &new_timeline,
                    &ctx,
                    tenant::timeline::GetLogicalSizePriority::User,
                )
                .await
                .map_err(ApiError::InternalServerError)?;
                json_response(StatusCode::CREATED, timeline_info)
            }
            Err(_) if tenant.cancel.is_cancelled() => {
                // In case we get some ugly error type during shutdown, cast it into a clean 503.
                json_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    HttpErrorBody::from_msg("Tenant shutting down".to_string()),
                )
            }
            Err(e @ tenant::CreateTimelineError::Conflict) => {
                json_response(StatusCode::CONFLICT, HttpErrorBody::from_msg(e.to_string()))
            }
            Err(e @ tenant::CreateTimelineError::AlreadyCreating) => json_response(
                StatusCode::TOO_MANY_REQUESTS,
                HttpErrorBody::from_msg(e.to_string()),
            ),
            Err(tenant::CreateTimelineError::AncestorLsn(err)) => json_response(
                StatusCode::NOT_ACCEPTABLE,
                HttpErrorBody::from_msg(format!("{err:#}")),
            ),
            Err(e @ tenant::CreateTimelineError::AncestorNotActive) => json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                HttpErrorBody::from_msg(e.to_string()),
            ),
            Err(e @ tenant::CreateTimelineError::AncestorArchived) => json_response(
                StatusCode::NOT_ACCEPTABLE,
                HttpErrorBody::from_msg(e.to_string()),
            ),
            Err(tenant::CreateTimelineError::ShuttingDown) => json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                HttpErrorBody::from_msg("tenant shutting down".to_string()),
            ),
            Err(tenant::CreateTimelineError::Other(err)) => Err(ApiError::InternalServerError(err)),
        }
    }
    .instrument(info_span!("timeline_create",
        tenant_id = %tenant_shard_id.tenant_id,
        shard_id = %tenant_shard_id.shard_slug(),
        timeline_id = %new_timeline_id,
    ))
    .await
}

async fn timeline_list_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let include_non_incremental_logical_size: Option<bool> =
        parse_query_param(&request, "include-non-incremental-logical-size")?;
    let force_await_initial_logical_size: Option<bool> =
        parse_query_param(&request, "force-await-initial-logical-size")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let state = get_state(&request);
    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);

    let response_data = async {
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;

        tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

        let timelines = tenant.list_timelines();

        let mut response_data = Vec::with_capacity(timelines.len());
        for timeline in timelines {
            let timeline_info = build_timeline_info(
                &timeline,
                include_non_incremental_logical_size.unwrap_or(false),
                force_await_initial_logical_size.unwrap_or(false),
                &ctx,
            )
            .instrument(info_span!("build_timeline_info", timeline_id = %timeline.timeline_id))
            .await
            .context("Failed to build timeline info")
            .map_err(ApiError::InternalServerError)?;

            response_data.push(timeline_info);
        }
        Ok::<Vec<TimelineInfo>, ApiError>(response_data)
    }
    .instrument(info_span!("timeline_list",
                tenant_id = %tenant_shard_id.tenant_id,
                shard_id = %tenant_shard_id.shard_slug()))
    .await?;

    json_response(StatusCode::OK, response_data)
}

async fn timeline_and_offloaded_list_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let include_non_incremental_logical_size: Option<bool> =
        parse_query_param(&request, "include-non-incremental-logical-size")?;
    let force_await_initial_logical_size: Option<bool> =
        parse_query_param(&request, "force-await-initial-logical-size")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let state = get_state(&request);
    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);

    let response_data = async {
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;

        tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

        let (timelines, offloadeds) = tenant.list_timelines_and_offloaded();

        let mut timeline_infos = Vec::with_capacity(timelines.len());
        for timeline in timelines {
            let timeline_info = build_timeline_info(
                &timeline,
                include_non_incremental_logical_size.unwrap_or(false),
                force_await_initial_logical_size.unwrap_or(false),
                &ctx,
            )
            .instrument(info_span!("build_timeline_info", timeline_id = %timeline.timeline_id))
            .await
            .context("Failed to build timeline info")
            .map_err(ApiError::InternalServerError)?;

            timeline_infos.push(timeline_info);
        }
        let offloaded_infos = offloadeds
            .into_iter()
            .map(|offloaded| build_timeline_offloaded_info(&offloaded))
            .collect::<Vec<_>>();
        let res = TimelinesInfoAndOffloaded {
            timelines: timeline_infos,
            offloaded: offloaded_infos,
        };
        Ok::<TimelinesInfoAndOffloaded, ApiError>(res)
    }
    .instrument(info_span!("timeline_and_offloaded_list",
                tenant_id = %tenant_shard_id.tenant_id,
                shard_id = %tenant_shard_id.shard_slug()))
    .await?;

    json_response(StatusCode::OK, response_data)
}

async fn timeline_preserve_initdb_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let state = get_state(&request);

    // Part of the process for disaster recovery from safekeeper-stored WAL:
    // If we don't recover into a new timeline but want to keep the timeline ID,
    // then the initdb archive is deleted. This endpoint copies it to a different
    // location where timeline recreation cand find it.

    async {
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;

        let timeline = tenant.get_timeline(timeline_id, false)?;

        timeline
            .preserve_initdb_archive()
            .await
            .context("preserving initdb archive")
            .map_err(ApiError::InternalServerError)?;

        Ok::<_, ApiError>(())
    }
    .instrument(info_span!("timeline_preserve_initdb_archive",
                tenant_id = %tenant_shard_id.tenant_id,
                shard_id = %tenant_shard_id.shard_slug(),
                %timeline_id))
    .await?;

    json_response(StatusCode::OK, ())
}

async fn timeline_archival_config_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    let request_data: TimelineArchivalConfigRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let state = get_state(&request);

    async {
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;

        tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

        tenant
            .apply_timeline_archival_config(
                timeline_id,
                request_data.state,
                state.broker_client.clone(),
                ctx,
            )
            .await?;
        Ok::<_, ApiError>(())
    }
    .instrument(info_span!("timeline_archival_config",
                tenant_id = %tenant_shard_id.tenant_id,
                shard_id = %tenant_shard_id.shard_slug(),
                state = ?request_data.state,
                %timeline_id))
    .await?;

    json_response(StatusCode::OK, ())
}

async fn timeline_detail_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let include_non_incremental_logical_size: Option<bool> =
        parse_query_param(&request, "include-non-incremental-logical-size")?;
    let force_await_initial_logical_size: Option<bool> =
        parse_query_param(&request, "force-await-initial-logical-size")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    // Logical size calculation needs downloading.
    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let state = get_state(&request);

    let timeline_info = async {
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;

        tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

        let timeline = tenant.get_timeline(timeline_id, false)?;

        let timeline_info = build_timeline_info(
            &timeline,
            include_non_incremental_logical_size.unwrap_or(false),
            force_await_initial_logical_size.unwrap_or(false),
            &ctx,
        )
        .await
        .context("get local timeline info")
        .map_err(ApiError::InternalServerError)?;

        Ok::<_, ApiError>(timeline_info)
    }
    .instrument(info_span!("timeline_detail",
                tenant_id = %tenant_shard_id.tenant_id,
                shard_id = %tenant_shard_id.shard_slug(),
                %timeline_id))
    .await?;

    json_response(StatusCode::OK, timeline_info)
}

async fn get_lsn_by_timestamp_handler(
    request: Request<Body>,
    cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let state = get_state(&request);

    if !tenant_shard_id.is_shard_zero() {
        // Requires SLRU contents, which are only stored on shard zero
        return Err(ApiError::BadRequest(anyhow!(
            "Size calculations are only available on shard zero"
        )));
    }

    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let timestamp_raw = must_get_query_param(&request, "timestamp")?;
    let timestamp = humantime::parse_rfc3339(&timestamp_raw)
        .with_context(|| format!("Invalid time: {:?}", timestamp_raw))
        .map_err(ApiError::BadRequest)?;
    let timestamp_pg = postgres_ffi::to_pg_timestamp(timestamp);

    let with_lease = parse_query_param(&request, "with_lease")?.unwrap_or(false);

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);

    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;
    let result = timeline
        .find_lsn_for_timestamp(timestamp_pg, &cancel, &ctx)
        .await?;

    #[derive(serde::Serialize, Debug)]
    struct Result {
        lsn: Lsn,
        kind: &'static str,
        #[serde(default)]
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(flatten)]
        lease: Option<LsnLease>,
    }
    let (lsn, kind) = match result {
        LsnForTimestamp::Present(lsn) => (lsn, "present"),
        LsnForTimestamp::Future(lsn) => (lsn, "future"),
        LsnForTimestamp::Past(lsn) => (lsn, "past"),
        LsnForTimestamp::NoData(lsn) => (lsn, "nodata"),
    };

    let lease = if with_lease {
        timeline
            .init_lsn_lease(lsn, timeline.get_lsn_lease_length_for_ts(), &ctx)
            .inspect_err(|_| {
                warn!("fail to grant a lease to {}", lsn);
            })
            .ok()
    } else {
        None
    };

    let result = Result { lsn, kind, lease };
    let valid_until = result
        .lease
        .as_ref()
        .map(|l| humantime::format_rfc3339_millis(l.valid_until).to_string());
    tracing::info!(
        lsn=?result.lsn,
        kind=%result.kind,
        timestamp=%timestamp_raw,
        valid_until=?valid_until,
        "lsn_by_timestamp finished"
    );
    json_response(StatusCode::OK, result)
}

async fn get_timestamp_of_lsn_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let state = get_state(&request);

    if !tenant_shard_id.is_shard_zero() {
        // Requires SLRU contents, which are only stored on shard zero
        return Err(ApiError::BadRequest(anyhow!(
            "Size calculations are only available on shard zero"
        )));
    }

    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;

    let lsn_str = must_get_query_param(&request, "lsn")?;
    let lsn = Lsn::from_str(&lsn_str)
        .with_context(|| format!("Invalid LSN: {lsn_str:?}"))
        .map_err(ApiError::BadRequest)?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;
    let result = timeline.get_timestamp_for_lsn(lsn, &ctx).await?;

    match result {
        Some(time) => {
            let time = format_rfc3339(
                postgres_ffi::try_from_pg_timestamp(time).map_err(ApiError::InternalServerError)?,
            )
            .to_string();
            json_response(StatusCode::OK, time)
        }
        None => Err(ApiError::NotFound(
            anyhow::anyhow!("Timestamp for lsn {} not found", lsn).into(),
        )),
    }
}

async fn timeline_delete_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let state = get_state(&request);

    let tenant = state
        .tenant_manager
        .get_attached_tenant_shard(tenant_shard_id)
        .map_err(|e| {
            match e {
                // GetTenantError has a built-in conversion to ApiError, but in this context we don't
                // want to treat missing tenants as 404, to avoid ambiguity with successful deletions.
                GetTenantError::NotFound(_) | GetTenantError::ShardNotFound(_) => {
                    ApiError::PreconditionFailed(
                        "Requested tenant is missing".to_string().into_boxed_str(),
                    )
                }
                e => e.into(),
            }
        })?;
    tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;
    tenant.delete_timeline(timeline_id).instrument(info_span!("timeline_delete", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), %timeline_id))
        .await?;

    json_response(StatusCode::ACCEPTED, ())
}

async fn tenant_reset_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let drop_cache: Option<bool> = parse_query_param(&request, "drop_cache")?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);
    let state = get_state(&request);
    state
        .tenant_manager
        .reset_tenant(tenant_shard_id, drop_cache.unwrap_or(false), &ctx)
        .await
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

async fn tenant_list_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;
    let state = get_state(&request);

    let response_data = state
        .tenant_manager
        .list_tenants()
        .map_err(|_| {
            ApiError::ResourceUnavailable("Tenant map is initializing or shutting down".into())
        })?
        .iter()
        .map(|(id, state, gen)| TenantInfo {
            id: *id,
            state: state.clone(),
            current_physical_size: None,
            attachment_status: state.attachment_status(),
            generation: (*gen)
                .into()
                .expect("Tenants are always attached with a generation"),
            gc_blocking: None,
        })
        .collect::<Vec<TenantInfo>>();

    json_response(StatusCode::OK, response_data)
}

async fn tenant_status(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let state = get_state(&request);

    // In tests, sometimes we want to query the state of a tenant without auto-activating it if it's currently waiting.
    let activate = true;
    #[cfg(feature = "testing")]
    let activate = parse_query_param(&request, "activate")?.unwrap_or(activate);

    let tenant_info = async {
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;

        if activate {
            // This is advisory: we prefer to let the tenant activate on-demand when this function is
            // called, but it is still valid to return 200 and describe the current state of the tenant
            // if it doesn't make it into an active state.
            tenant
                .wait_to_become_active(ACTIVE_TENANT_TIMEOUT)
                .await
                .ok();
        }

        // Calculate total physical size of all timelines
        let mut current_physical_size = 0;
        for timeline in tenant.list_timelines().iter() {
            current_physical_size += timeline.layer_size_sum().await;
        }

        let state = tenant.current_state();
        Result::<_, ApiError>::Ok(TenantDetails {
            tenant_info: TenantInfo {
                id: tenant_shard_id,
                state: state.clone(),
                current_physical_size: Some(current_physical_size),
                attachment_status: state.attachment_status(),
                generation: tenant
                    .generation()
                    .into()
                    .expect("Tenants are always attached with a generation"),
                gc_blocking: tenant.gc_block.summary().map(|x| format!("{x:?}")),
            },
            walredo: tenant.wal_redo_manager_status(),
            timelines: tenant.list_timeline_ids(),
        })
    }
    .instrument(info_span!("tenant_status_handler",
                tenant_id = %tenant_shard_id.tenant_id,
                shard_id = %tenant_shard_id.shard_slug()))
    .await?;

    json_response(StatusCode::OK, tenant_info)
}

async fn tenant_delete_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    // TODO openapi spec
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let state = get_state(&request);

    state
        .tenant_manager
        .delete_tenant(tenant_shard_id)
        .instrument(info_span!("tenant_delete_handler",
            tenant_id = %tenant_shard_id.tenant_id,
            shard_id = %tenant_shard_id.shard_slug()
        ))
        .await?;

    json_response(StatusCode::OK, ())
}

/// HTTP endpoint to query the current tenant_size of a tenant.
///
/// This is not used by consumption metrics under [`crate::consumption_metrics`], but can be used
/// to debug any of the calculations. Requires `tenant_id` request parameter, supports
/// `inputs_only=true|false` (default false) which supports debugging failure to calculate model
/// values.
///
/// 'retention_period' query parameter overrides the cutoff that is used to calculate the size
/// (only if it is shorter than the real cutoff).
///
/// Note: we don't update the cached size and prometheus metric here.
/// The retention period might be different, and it's nice to have a method to just calculate it
/// without modifying anything anyway.
async fn tenant_size_handler(
    request: Request<Body>,
    cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let inputs_only: Option<bool> = parse_query_param(&request, "inputs_only")?;
    let retention_period: Option<u64> = parse_query_param(&request, "retention_period")?;
    let headers = request.headers();
    let state = get_state(&request);

    if !tenant_shard_id.is_shard_zero() {
        return Err(ApiError::BadRequest(anyhow!(
            "Size calculations are only available on shard zero"
        )));
    }

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let tenant = state
        .tenant_manager
        .get_attached_tenant_shard(tenant_shard_id)?;
    tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

    // this can be long operation
    let inputs = tenant
        .gather_size_inputs(
            retention_period,
            LogicalSizeCalculationCause::TenantSizeHandler,
            &cancel,
            &ctx,
        )
        .await
        .map_err(|e| match e {
            crate::tenant::size::CalculateSyntheticSizeError::Cancelled => ApiError::ShuttingDown,
            other => ApiError::InternalServerError(anyhow::anyhow!(other)),
        })?;

    let mut sizes = None;
    let accepts_html = headers
        .get(header::ACCEPT)
        .map(|v| v == "text/html")
        .unwrap_or_default();
    if !inputs_only.unwrap_or(false) {
        let storage_model = inputs.calculate_model();
        let size = storage_model.calculate();

        // If request header expects html, return html
        if accepts_html {
            return synthetic_size_html_response(inputs, storage_model, size);
        }
        sizes = Some(size);
    } else if accepts_html {
        return Err(ApiError::BadRequest(anyhow!(
            "inputs_only parameter is incompatible with html output request"
        )));
    }

    /// The type resides in the pageserver not to expose `ModelInputs`.
    #[derive(serde::Serialize)]
    struct TenantHistorySize {
        id: TenantId,
        /// Size is a mixture of WAL and logical size, so the unit is bytes.
        ///
        /// Will be none if `?inputs_only=true` was given.
        size: Option<u64>,
        /// Size of each segment used in the model.
        /// Will be null if `?inputs_only=true` was given.
        segment_sizes: Option<Vec<tenant_size_model::SegmentSizeResult>>,
        inputs: crate::tenant::size::ModelInputs,
    }

    json_response(
        StatusCode::OK,
        TenantHistorySize {
            id: tenant_shard_id.tenant_id,
            size: sizes.as_ref().map(|x| x.total_size),
            segment_sizes: sizes.map(|x| x.segments),
            inputs,
        },
    )
}

async fn tenant_shard_split_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let req: TenantShardSplitRequest = json_request(&mut request).await?;

    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let state = get_state(&request);
    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    let tenant = state
        .tenant_manager
        .get_attached_tenant_shard(tenant_shard_id)?;
    tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

    let new_shards = state
        .tenant_manager
        .shard_split(
            tenant,
            ShardCount::new(req.new_shard_count),
            req.new_stripe_size,
            &ctx,
        )
        .await
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, TenantShardSplitResponse { new_shards })
}

async fn layer_map_info_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let reset: LayerAccessStatsReset =
        parse_query_param(&request, "reset")?.unwrap_or(LayerAccessStatsReset::NoReset);
    let state = get_state(&request);

    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;
    let layer_map_info = timeline
        .layer_map_info(reset)
        .await
        .map_err(|_shutdown| ApiError::ShuttingDown)?;

    json_response(StatusCode::OK, layer_map_info)
}

#[instrument(skip_all, fields(tenant_id, shard_id, timeline_id, layer_name))]
async fn timeline_layer_scan_disposable_keys(
    request: Request<Body>,
    cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let layer_name: LayerName = parse_request_param(&request, "layer_name")?;

    tracing::Span::current().record(
        "tenant_id",
        tracing::field::display(&tenant_shard_id.tenant_id),
    );
    tracing::Span::current().record(
        "shard_id",
        tracing::field::display(tenant_shard_id.shard_slug()),
    );
    tracing::Span::current().record("timeline_id", tracing::field::display(&timeline_id));
    tracing::Span::current().record("layer_name", tracing::field::display(&layer_name));

    let state = get_state(&request);

    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    // technically the timeline need not be active for this scan to complete
    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);

    let guard = timeline.layers.read().await;
    let Some(layer) = guard.try_get_from_key(&layer_name.clone().into()) else {
        return Err(ApiError::NotFound(
            anyhow::anyhow!("Layer {tenant_shard_id}/{timeline_id}/{layer_name} not found").into(),
        ));
    };

    let resident_layer = layer
        .download_and_keep_resident()
        .await
        .map_err(|err| match err {
            tenant::storage_layer::layer::DownloadError::TimelineShutdown
            | tenant::storage_layer::layer::DownloadError::DownloadCancelled => {
                ApiError::ShuttingDown
            }
            tenant::storage_layer::layer::DownloadError::ContextAndConfigReallyDeniesDownloads
            | tenant::storage_layer::layer::DownloadError::DownloadRequired
            | tenant::storage_layer::layer::DownloadError::NotFile(_)
            | tenant::storage_layer::layer::DownloadError::DownloadFailed
            | tenant::storage_layer::layer::DownloadError::PreStatFailed(_) => {
                ApiError::InternalServerError(err.into())
            }
            #[cfg(test)]
            tenant::storage_layer::layer::DownloadError::Failpoint(_) => {
                ApiError::InternalServerError(err.into())
            }
        })?;

    let keys = resident_layer
        .load_keys(&ctx)
        .await
        .map_err(ApiError::InternalServerError)?;

    let shard_identity = timeline.get_shard_identity();

    let mut disposable_count = 0;
    let mut not_disposable_count = 0;
    let cancel = cancel.clone();
    for (i, key) in keys.into_iter().enumerate() {
        if shard_identity.is_key_disposable(&key) {
            disposable_count += 1;
            tracing::debug!(key = %key, key.dbg=?key, "disposable key");
        } else {
            not_disposable_count += 1;
        }
        #[allow(clippy::collapsible_if)]
        if i % 10000 == 0 {
            if cancel.is_cancelled() || timeline.cancel.is_cancelled() || timeline.is_stopping() {
                return Err(ApiError::ShuttingDown);
            }
        }
    }

    json_response(
        StatusCode::OK,
        pageserver_api::models::ScanDisposableKeysResponse {
            disposable_count,
            not_disposable_count,
        },
    )
}

async fn layer_download_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let layer_file_name = get_request_param(&request, "layer_file_name")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let layer_name = LayerName::from_str(layer_file_name)
        .map_err(|s| ApiError::BadRequest(anyhow::anyhow!(s)))?;
    let state = get_state(&request);

    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;
    let downloaded = timeline
        .download_layer(&layer_name)
        .await
        .map_err(ApiError::InternalServerError)?;

    match downloaded {
        Some(true) => json_response(StatusCode::OK, ()),
        Some(false) => json_response(StatusCode::NOT_MODIFIED, ()),
        None => json_response(
            StatusCode::BAD_REQUEST,
            format!("Layer {tenant_shard_id}/{timeline_id}/{layer_file_name} not found"),
        ),
    }
}

async fn evict_timeline_layer_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let layer_file_name = get_request_param(&request, "layer_file_name")?;
    let state = get_state(&request);

    let layer_name = LayerName::from_str(layer_file_name)
        .map_err(|s| ApiError::BadRequest(anyhow::anyhow!(s)))?;

    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;
    let evicted = timeline
        .evict_layer(&layer_name)
        .await
        .map_err(ApiError::InternalServerError)?;

    match evicted {
        Some(true) => json_response(StatusCode::OK, ()),
        Some(false) => json_response(StatusCode::NOT_MODIFIED, ()),
        None => json_response(
            StatusCode::BAD_REQUEST,
            format!("Layer {tenant_shard_id}/{timeline_id}/{layer_file_name} not found"),
        ),
    }
}

async fn timeline_gc_blocking_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    block_or_unblock_gc(request, true).await
}

async fn timeline_gc_unblocking_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    block_or_unblock_gc(request, false).await
}

/// Adding a block is `POST ../block_gc`, removing a block is `POST ../unblock_gc`.
///
/// Both are technically unsafe because they might fire off index uploads, thus they are POST.
async fn block_or_unblock_gc(
    request: Request<Body>,
    block: bool,
) -> Result<Response<Body>, ApiError> {
    use crate::tenant::{
        remote_timeline_client::WaitCompletionError, upload_queue::NotInitialized,
    };
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let state = get_state(&request);

    let tenant = state
        .tenant_manager
        .get_attached_tenant_shard(tenant_shard_id)?;

    tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

    let timeline = tenant.get_timeline(timeline_id, true)?;

    let fut = async {
        if block {
            timeline.block_gc(&tenant).await.map(|_| ())
        } else {
            timeline.unblock_gc(&tenant).await
        }
    };

    let span = tracing::info_span!(
        "block_or_unblock_gc",
        tenant_id = %tenant_shard_id.tenant_id,
        shard_id = %tenant_shard_id.shard_slug(),
        timeline_id = %timeline_id,
        block = block,
    );

    let res = fut.instrument(span).await;

    res.map_err(|e| {
        if e.is::<NotInitialized>() || e.is::<WaitCompletionError>() {
            ApiError::ShuttingDown
        } else {
            ApiError::InternalServerError(e)
        }
    })?;

    json_response(StatusCode::OK, ())
}

/// Get tenant_size SVG graph along with the JSON data.
fn synthetic_size_html_response(
    inputs: ModelInputs,
    storage_model: StorageModel,
    sizes: SizeResult,
) -> Result<Response<Body>, ApiError> {
    let mut timeline_ids: Vec<String> = Vec::new();
    let mut timeline_map: HashMap<TimelineId, usize> = HashMap::new();
    for (index, ti) in inputs.timeline_inputs.iter().enumerate() {
        timeline_map.insert(ti.timeline_id, index);
        timeline_ids.push(ti.timeline_id.to_string());
    }
    let seg_to_branch: Vec<(usize, SvgBranchKind)> = inputs
        .segments
        .iter()
        .map(|seg| {
            (
                *timeline_map.get(&seg.timeline_id).unwrap(),
                seg.kind.into(),
            )
        })
        .collect();

    let svg =
        tenant_size_model::svg::draw_svg(&storage_model, &timeline_ids, &seg_to_branch, &sizes)
            .map_err(ApiError::InternalServerError)?;

    let mut response = String::new();

    use std::fmt::Write;
    write!(response, "<html>\n<body>\n").unwrap();
    write!(response, "<div>\n{svg}\n</div>").unwrap();
    writeln!(response, "Project size: {}", sizes.total_size).unwrap();
    writeln!(response, "<pre>").unwrap();
    writeln!(
        response,
        "{}",
        serde_json::to_string_pretty(&inputs).unwrap()
    )
    .unwrap();
    writeln!(
        response,
        "{}",
        serde_json::to_string_pretty(&sizes.segments).unwrap()
    )
    .unwrap();
    writeln!(response, "</pre>").unwrap();
    write!(response, "</body>\n</html>\n").unwrap();

    html_response(StatusCode::OK, response)
}

pub fn html_response(status: StatusCode, data: String) -> Result<Response<Body>, ApiError> {
    let response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/html")
        .body(Body::from(data.as_bytes().to_vec()))
        .map_err(|e| ApiError::InternalServerError(e.into()))?;
    Ok(response)
}

async fn get_tenant_config_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let state = get_state(&request);

    let tenant = state
        .tenant_manager
        .get_attached_tenant_shard(tenant_shard_id)?;

    let response = HashMap::from([
        (
            "tenant_specific_overrides",
            serde_json::to_value(tenant.tenant_specific_overrides())
                .context("serializing tenant specific overrides")
                .map_err(ApiError::InternalServerError)?,
        ),
        (
            "effective_config",
            serde_json::to_value(tenant.effective_config())
                .context("serializing effective config")
                .map_err(ApiError::InternalServerError)?,
        ),
    ]);

    json_response(StatusCode::OK, response)
}

async fn update_tenant_config_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let request_data: TenantConfigRequest = json_request(&mut request).await?;
    let tenant_id = request_data.tenant_id;
    check_permission(&request, Some(tenant_id))?;

    let new_tenant_conf =
        TenantConfOpt::try_from(&request_data.config).map_err(ApiError::BadRequest)?;

    let state = get_state(&request);

    let tenant_shard_id = TenantShardId::unsharded(tenant_id);

    let tenant = state
        .tenant_manager
        .get_attached_tenant_shard(tenant_shard_id)?;
    tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

    // This is a legacy API that only operates on attached tenants: the preferred
    // API to use is the location_config/ endpoint, which lets the caller provide
    // the full LocationConf.
    let location_conf = LocationConf::attached_single(
        new_tenant_conf.clone(),
        tenant.get_generation(),
        &ShardParameters::default(),
    );

    crate::tenant::Tenant::persist_tenant_config(state.conf, &tenant_shard_id, &location_conf)
        .await
        .map_err(|e| ApiError::InternalServerError(anyhow::anyhow!(e)))?;

    let _ = tenant
        .update_tenant_config(|_crnt| Ok(new_tenant_conf.clone()))
        .expect("Closure returns Ok()");

    json_response(StatusCode::OK, ())
}

async fn patch_tenant_config_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let request_data: TenantConfigPatchRequest = json_request(&mut request).await?;
    let tenant_id = request_data.tenant_id;
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);

    let tenant_shard_id = TenantShardId::unsharded(tenant_id);

    let tenant = state
        .tenant_manager
        .get_attached_tenant_shard(tenant_shard_id)?;
    tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

    let updated = tenant
        .update_tenant_config(|crnt| crnt.apply_patch(request_data.config.clone()))
        .map_err(ApiError::BadRequest)?;

    // This is a legacy API that only operates on attached tenants: the preferred
    // API to use is the location_config/ endpoint, which lets the caller provide
    // the full LocationConf.
    let location_conf = LocationConf::attached_single(
        updated,
        tenant.get_generation(),
        &ShardParameters::default(),
    );

    crate::tenant::Tenant::persist_tenant_config(state.conf, &tenant_shard_id, &location_conf)
        .await
        .map_err(|e| ApiError::InternalServerError(anyhow::anyhow!(e)))?;

    json_response(StatusCode::OK, ())
}

async fn put_tenant_location_config_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;

    let request_data: TenantLocationConfigRequest = json_request(&mut request).await?;
    let flush = parse_query_param(&request, "flush_ms")?.map(Duration::from_millis);
    let lazy = parse_query_param(&request, "lazy")?.unwrap_or(false);
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);
    let state = get_state(&request);
    let conf = state.conf;

    // The `Detached` state is special, it doesn't upsert a tenant, it removes
    // its local disk content and drops it from memory.
    if let LocationConfigMode::Detached = request_data.config.mode {
        if let Err(e) = state
            .tenant_manager
            .detach_tenant(conf, tenant_shard_id, &state.deletion_queue_client)
            .instrument(info_span!("tenant_detach",
                tenant_id = %tenant_shard_id.tenant_id,
                shard_id = %tenant_shard_id.shard_slug()
            ))
            .await
        {
            match e {
                TenantStateError::SlotError(TenantSlotError::NotFound(_)) => {
                    // This API is idempotent: a NotFound on a detach is fine.
                }
                _ => return Err(e.into()),
            }
        }
        return json_response(StatusCode::OK, ());
    }

    let location_conf =
        LocationConf::try_from(&request_data.config).map_err(ApiError::BadRequest)?;

    // lazy==true queues up for activation or jumps the queue like normal when a compute connects,
    // similar to at startup ordering.
    let spawn_mode = if lazy {
        tenant::SpawnMode::Lazy
    } else {
        tenant::SpawnMode::Eager
    };

    let tenant = state
        .tenant_manager
        .upsert_location(tenant_shard_id, location_conf, flush, spawn_mode, &ctx)
        .await?;
    let stripe_size = tenant.as_ref().map(|t| t.get_shard_stripe_size());
    let attached = tenant.is_some();

    if let Some(_flush_ms) = flush {
        match state
            .secondary_controller
            .upload_tenant(tenant_shard_id)
            .await
        {
            Ok(()) => {
                tracing::info!("Uploaded heatmap during flush");
            }
            Err(e) => {
                tracing::warn!("Failed to flush heatmap: {e}");
            }
        }
    } else {
        tracing::info!("No flush requested when configuring");
    }

    // This API returns a vector of pageservers where the tenant is attached: this is
    // primarily for use in the sharding service.  For compatibilty, we also return this
    // when called directly on a pageserver, but the payload is always zero or one shards.
    let mut response = TenantLocationConfigResponse {
        shards: Vec::new(),
        stripe_size: None,
    };
    if attached {
        response.shards.push(TenantShardLocation {
            shard_id: tenant_shard_id,
            node_id: state.conf.id,
        });
        if tenant_shard_id.shard_count.count() > 1 {
            // Stripe size should be set if we are attached
            debug_assert!(stripe_size.is_some());
            response.stripe_size = stripe_size;
        }
    }

    json_response(StatusCode::OK, response)
}

async fn list_location_config_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let state = get_state(&request);
    let slots = state.tenant_manager.list();
    let result = LocationConfigListResponse {
        tenant_shards: slots
            .into_iter()
            .map(|(tenant_shard_id, slot)| {
                let v = match slot {
                    TenantSlot::Attached(t) => Some(t.get_location_conf()),
                    TenantSlot::Secondary(s) => Some(s.get_location_conf()),
                    TenantSlot::InProgress(_) => None,
                };
                (tenant_shard_id, v)
            })
            .collect(),
    };
    json_response(StatusCode::OK, result)
}

async fn get_location_config_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let state = get_state(&request);
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let slot = state.tenant_manager.get(tenant_shard_id);

    let Some(slot) = slot else {
        return Err(ApiError::NotFound(
            anyhow::anyhow!("Tenant shard not found").into(),
        ));
    };

    let result: Option<LocationConfig> = match slot {
        TenantSlot::Attached(t) => Some(t.get_location_conf()),
        TenantSlot::Secondary(s) => Some(s.get_location_conf()),
        TenantSlot::InProgress(_) => None,
    };

    json_response(StatusCode::OK, result)
}

// Do a time travel recovery on the given tenant/tenant shard. Tenant needs to be detached
// (from all pageservers) as it invalidates consistency assumptions.
async fn tenant_time_travel_remote_storage_handler(
    request: Request<Body>,
    cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;

    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let timestamp_raw = must_get_query_param(&request, "travel_to")?;
    let timestamp = humantime::parse_rfc3339(&timestamp_raw)
        .with_context(|| format!("Invalid time for travel_to: {timestamp_raw:?}"))
        .map_err(ApiError::BadRequest)?;

    let done_if_after_raw = must_get_query_param(&request, "done_if_after")?;
    let done_if_after = humantime::parse_rfc3339(&done_if_after_raw)
        .with_context(|| format!("Invalid time for done_if_after: {done_if_after_raw:?}"))
        .map_err(ApiError::BadRequest)?;

    // This is just a sanity check to fend off naive wrong usages of the API:
    // the tenant needs to be detached *everywhere*
    let state = get_state(&request);
    let we_manage_tenant = state.tenant_manager.manages_tenant_shard(tenant_shard_id);
    if we_manage_tenant {
        return Err(ApiError::BadRequest(anyhow!(
            "Tenant {tenant_shard_id} is already attached at this pageserver"
        )));
    }

    if timestamp > done_if_after {
        return Err(ApiError::BadRequest(anyhow!(
            "The done_if_after timestamp comes before the timestamp to recover to"
        )));
    }

    tracing::info!("Issuing time travel request internally. timestamp={timestamp_raw}, done_if_after={done_if_after_raw}");

    remote_timeline_client::upload::time_travel_recover_tenant(
        &state.remote_storage,
        &tenant_shard_id,
        timestamp,
        done_if_after,
        &cancel,
    )
    .await
    .map_err(|e| match e {
        TimeTravelError::BadInput(e) => {
            warn!("bad input error: {e}");
            ApiError::BadRequest(anyhow!("bad input error"))
        }
        TimeTravelError::Unimplemented => {
            ApiError::BadRequest(anyhow!("unimplemented for the configured remote storage"))
        }
        TimeTravelError::Cancelled => ApiError::InternalServerError(anyhow!("cancelled")),
        TimeTravelError::TooManyVersions => {
            ApiError::InternalServerError(anyhow!("too many versions in remote storage"))
        }
        TimeTravelError::Other(e) => {
            warn!("internal error: {e}");
            ApiError::InternalServerError(anyhow!("internal error"))
        }
    })?;

    json_response(StatusCode::OK, ())
}

/// Testing helper to transition a tenant to [`crate::tenant::TenantState::Broken`].
async fn handle_tenant_break(
    r: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&r, "tenant_shard_id")?;

    let state = get_state(&r);
    state
        .tenant_manager
        .get_attached_tenant_shard(tenant_shard_id)?
        .set_broken("broken from test".to_owned())
        .await;

    json_response(StatusCode::OK, ())
}

// Obtains an lsn lease on the given timeline.
async fn lsn_lease_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let lsn = json_request::<LsnLeaseRequest>(&mut request).await?.lsn;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);

    let state = get_state(&request);

    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;

    let result = async {
        timeline
            .init_lsn_lease(lsn, timeline.get_lsn_lease_length(), &ctx)
            .map_err(|e| {
                ApiError::InternalServerError(
                    e.context(format!("invalid lsn lease request at {lsn}")),
                )
            })
    }
    .instrument(info_span!("init_lsn_lease", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug(), %timeline_id))
    .await?;

    json_response(StatusCode::OK, result)
}

// Run GC immediately on given timeline.
async fn timeline_gc_handler(
    mut request: Request<Body>,
    cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let gc_req: TimelineGcRequest = json_request(&mut request).await?;

    let state = get_state(&request);

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let gc_result = state
        .tenant_manager
        .immediate_gc(tenant_shard_id, timeline_id, gc_req, cancel, &ctx)
        .await?;

    json_response(StatusCode::OK, gc_result)
}

// Cancel scheduled compaction tasks
async fn timeline_cancel_compact_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let state = get_state(&request);
    async {
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;
        tenant.cancel_scheduled_compaction(timeline_id);
        json_response(StatusCode::OK, ())
    }
    .instrument(info_span!("timeline_cancel_compact", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug(), %timeline_id))
    .await
}

// Get compact info of a timeline
async fn timeline_compact_info_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let state = get_state(&request);
    async {
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;
        let resp = tenant.get_scheduled_compaction_tasks(timeline_id);
        json_response(StatusCode::OK, resp)
    }
    .instrument(info_span!("timeline_compact_info", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug(), %timeline_id))
    .await
}

// Run compaction immediately on given timeline.
async fn timeline_compact_handler(
    mut request: Request<Body>,
    cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let compact_request = json_request_maybe::<Option<CompactRequest>>(&mut request).await?;

    let state = get_state(&request);

    let mut flags = EnumSet::empty();

    if Some(true) == parse_query_param::<_, bool>(&request, "force_l0_compaction")? {
        flags |= CompactFlags::ForceL0Compaction;
    }
    if Some(true) == parse_query_param::<_, bool>(&request, "force_repartition")? {
        flags |= CompactFlags::ForceRepartition;
    }
    if Some(true) == parse_query_param::<_, bool>(&request, "force_image_layer_creation")? {
        flags |= CompactFlags::ForceImageLayerCreation;
    }
    if Some(true) == parse_query_param::<_, bool>(&request, "enhanced_gc_bottom_most_compaction")? {
        flags |= CompactFlags::EnhancedGcBottomMostCompaction;
    }
    if Some(true) == parse_query_param::<_, bool>(&request, "dry_run")? {
        flags |= CompactFlags::DryRun;
    }

    let wait_until_uploaded =
        parse_query_param::<_, bool>(&request, "wait_until_uploaded")?.unwrap_or(false);

    let wait_until_scheduled_compaction_done =
        parse_query_param::<_, bool>(&request, "wait_until_scheduled_compaction_done")?
            .unwrap_or(false);

    let sub_compaction = compact_request
        .as_ref()
        .map(|r| r.sub_compaction)
        .unwrap_or(false);
    let sub_compaction_max_job_size_mb = compact_request
        .as_ref()
        .and_then(|r| r.sub_compaction_max_job_size_mb);

    let options = CompactOptions {
        compact_key_range: compact_request
            .as_ref()
            .and_then(|r| r.compact_key_range.clone()),
        compact_lsn_range: compact_request
            .as_ref()
            .and_then(|r| r.compact_lsn_range.clone()),
        flags,
        sub_compaction,
        sub_compaction_max_job_size_mb,
    };

    let scheduled = compact_request
        .as_ref()
        .map(|r| r.scheduled)
        .unwrap_or(false);

    async {
        let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
        let timeline = active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id).await?;
        if scheduled {
            let tenant = state
                .tenant_manager
                .get_attached_tenant_shard(tenant_shard_id)?;
            let rx = tenant.schedule_compaction(timeline_id, options).await.map_err(ApiError::InternalServerError)?;
            if wait_until_scheduled_compaction_done {
                // It is possible that this will take a long time, dropping the HTTP request will not cancel the compaction.
                rx.await.ok();
            }
        } else {
            timeline
                .compact_with_options(&cancel, options, &ctx)
                .await
                .map_err(|e| ApiError::InternalServerError(e.into()))?;
            if wait_until_uploaded {
                timeline.remote_client.wait_completion().await
                // XXX map to correct ApiError for the cases where it's due to shutdown
                .context("wait completion").map_err(ApiError::InternalServerError)?;
            }
        }
        json_response(StatusCode::OK, ())
    }
    .instrument(info_span!("manual_compaction", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug(), %timeline_id))
    .await
}

// Run offload immediately on given timeline.
async fn timeline_offload_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let state = get_state(&request);

    async {
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;

        if tenant.get_offloaded_timeline(timeline_id).is_ok() {
            return json_response(StatusCode::OK, ());
        }
        let timeline =
            active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
                .await?;

        if !tenant.timeline_has_no_attached_children(timeline_id) {
            return Err(ApiError::PreconditionFailed(
                "timeline has attached children".into(),
            ));
        }
        if let (false, reason) = timeline.can_offload() {
            return Err(ApiError::PreconditionFailed(
                format!("Timeline::can_offload() check failed: {}", reason) .into(),
            ));
        }
        offload_timeline(&tenant, &timeline)
            .await
            .map_err(|e| {
                match e {
                    OffloadError::Cancelled => ApiError::ResourceUnavailable("Timeline shutting down".into()),
                    _ => ApiError::InternalServerError(anyhow!(e))
                }
            })?;

        json_response(StatusCode::OK, ())
    }
    .instrument(info_span!("manual_timeline_offload", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug(), %timeline_id))
    .await
}

// Run checkpoint immediately on given timeline.
async fn timeline_checkpoint_handler(
    request: Request<Body>,
    cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let state = get_state(&request);

    let mut flags = EnumSet::empty();
    if Some(true) == parse_query_param::<_, bool>(&request, "force_l0_compaction")? {
        flags |= CompactFlags::ForceL0Compaction;
    }
    if Some(true) == parse_query_param::<_, bool>(&request, "force_repartition")? {
        flags |= CompactFlags::ForceRepartition;
    }
    if Some(true) == parse_query_param::<_, bool>(&request, "force_image_layer_creation")? {
        flags |= CompactFlags::ForceImageLayerCreation;
    }

    // By default, checkpoints come with a compaction, but this may be optionally disabled by tests that just want to flush + upload.
    let compact = parse_query_param::<_, bool>(&request, "compact")?.unwrap_or(true);

    let wait_until_flushed: bool =
        parse_query_param(&request, "wait_until_flushed")?.unwrap_or(true);

    let wait_until_uploaded =
        parse_query_param::<_, bool>(&request, "wait_until_uploaded")?.unwrap_or(false);

    async {
        let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
        let timeline = active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id).await?;
        if wait_until_flushed {
            timeline.freeze_and_flush().await
        } else {
            timeline.freeze().await.and(Ok(()))
        }.map_err(|e| {
                match e {
                    tenant::timeline::FlushLayerError::Cancelled => ApiError::ShuttingDown,
                    other => ApiError::InternalServerError(other.into()),

                }
            })?;
        if compact {
            timeline
                .compact(&cancel, flags, &ctx)
                .await
                .map_err(|e|
                    match e {
                        CompactionError::ShuttingDown => ApiError::ShuttingDown,
                        CompactionError::Offload(e) => ApiError::InternalServerError(anyhow::anyhow!(e)),
                        CompactionError::Other(e) => ApiError::InternalServerError(e)
                    }
                )?;
        }

        if wait_until_uploaded {
            tracing::info!("Waiting for uploads to complete...");
            timeline.remote_client.wait_completion().await
            // XXX map to correct ApiError for the cases where it's due to shutdown
            .context("wait completion").map_err(ApiError::InternalServerError)?;
            tracing::info!("Uploads completed up to {}", timeline.get_remote_consistent_lsn_projected().unwrap_or(Lsn(0)));
        }

        json_response(StatusCode::OK, ())
    }
    .instrument(info_span!("manual_checkpoint", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug(), %timeline_id))
    .await
}

async fn timeline_download_remote_layers_handler_post(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let body: DownloadRemoteLayersTaskSpawnRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let state = get_state(&request);

    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;
    match timeline.spawn_download_all_remote_layers(body).await {
        Ok(st) => json_response(StatusCode::ACCEPTED, st),
        Err(st) => json_response(StatusCode::CONFLICT, st),
    }
}

async fn timeline_download_remote_layers_handler_get(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let state = get_state(&request);

    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;
    let info = timeline
        .get_download_all_remote_layers_task_info()
        .context("task never started since last pageserver process start")
        .map_err(|e| ApiError::NotFound(e.into()))?;
    json_response(StatusCode::OK, info)
}

async fn timeline_detach_ancestor_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    use crate::tenant::timeline::detach_ancestor;
    use pageserver_api::models::detach_ancestor::AncestorDetached;

    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;

    let span = tracing::info_span!("detach_ancestor", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), %timeline_id);

    async move {
        let mut options = detach_ancestor::Options::default();

        let rewrite_concurrency =
            parse_query_param::<_, std::num::NonZeroUsize>(&request, "rewrite_concurrency")?;
        let copy_concurrency =
            parse_query_param::<_, std::num::NonZeroUsize>(&request, "copy_concurrency")?;

        [
            (&mut options.rewrite_concurrency, rewrite_concurrency),
            (&mut options.copy_concurrency, copy_concurrency),
        ]
        .into_iter()
        .filter_map(|(target, val)| val.map(|val| (target, val)))
        .for_each(|(target, val)| *target = val);

        let state = get_state(&request);

        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;

        tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

        let ctx = RequestContext::new(TaskKind::DetachAncestor, DownloadBehavior::Download);
        let ctx = &ctx;

        // Flush the upload queues of all timelines before detaching ancestor. We do the same thing again
        // during shutdown. This early upload ensures the pageserver does not need to upload too many
        // things and creates downtime during timeline reloads.
        for timeline in tenant.list_timelines() {
            timeline
                .remote_client
                .wait_completion()
                .await
                .map_err(|e| {
                    ApiError::PreconditionFailed(format!("cannot drain upload queue: {e}").into())
                })?;
        }

        tracing::info!("all timeline upload queues are drained");

        let timeline = tenant.get_timeline(timeline_id, true)?;

        let progress = timeline
            .prepare_to_detach_from_ancestor(&tenant, options, ctx)
            .await?;

        // uncomment to allow early as possible Tenant::drop
        // drop(tenant);

        let resp = match progress {
            detach_ancestor::Progress::Prepared(attempt, prepared) => {
                // it would be great to tag the guard on to the tenant activation future
                let reparented_timelines = state
                    .tenant_manager
                    .complete_detaching_timeline_ancestor(
                        tenant_shard_id,
                        timeline_id,
                        prepared,
                        attempt,
                        ctx,
                    )
                    .await?;

                AncestorDetached {
                    reparented_timelines,
                }
            }
            detach_ancestor::Progress::Done(resp) => resp,
        };

        json_response(StatusCode::OK, resp)
    }
    .instrument(span)
    .await
}

async fn deletion_queue_flush(
    r: Request<Body>,
    cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let state = get_state(&r);

    let execute = parse_query_param(&r, "execute")?.unwrap_or(false);

    let flush = async {
        if execute {
            state.deletion_queue_client.flush_execute().await
        } else {
            state.deletion_queue_client.flush().await
        }
    }
    // DeletionQueueError's only case is shutting down.
    .map_err(|_| ApiError::ShuttingDown);

    tokio::select! {
        res = flush => {
            res.map(|()| json_response(StatusCode::OK, ()))?
        }
        _ = cancel.cancelled() => {
            Err(ApiError::ShuttingDown)
        }
    }
}

/// Try if `GetPage@Lsn` is successful, useful for manual debugging.
async fn getpage_at_lsn_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let state = get_state(&request);

    struct Key(pageserver_api::key::Key);

    impl std::str::FromStr for Key {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
            pageserver_api::key::Key::from_hex(s).map(Key)
        }
    }

    let key: Key = parse_query_param(&request, "key")?
        .ok_or_else(|| ApiError::BadRequest(anyhow!("missing 'key' query parameter")))?;
    let lsn: Lsn = parse_query_param(&request, "lsn")?
        .ok_or_else(|| ApiError::BadRequest(anyhow!("missing 'lsn' query parameter")))?;

    async {
        let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
        let timeline = active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id).await?;

        let page = timeline.get(key.0, lsn, &ctx).await?;

        Result::<_, ApiError>::Ok(
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(hyper::Body::from(page))
                .unwrap(),
        )
    }
    .instrument(info_span!("timeline_get", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug(), %timeline_id))
    .await
}

async fn timeline_collect_keyspace(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;
    let state = get_state(&request);

    let at_lsn: Option<Lsn> = parse_query_param(&request, "at_lsn")?;

    async {
        let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
        let timeline = active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id).await?;
        let at_lsn = at_lsn.unwrap_or_else(|| timeline.get_last_record_lsn());
        let (dense_ks, sparse_ks) = timeline
            .collect_keyspace(at_lsn, &ctx)
            .await
            .map_err(|e| ApiError::InternalServerError(e.into()))?;

        // This API is currently used by pagebench. Pagebench will iterate all keys within the keyspace.
        // Therefore, we split dense/sparse keys in this API.
        let res = pageserver_api::models::partitioning::Partitioning { keys: dense_ks, sparse_keys: sparse_ks, at_lsn };

        json_response(StatusCode::OK, res)
    }
    .instrument(info_span!("timeline_collect_keyspace", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug(), %timeline_id))
    .await
}

async fn active_timeline_of_active_tenant(
    tenant_manager: &TenantManager,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
) -> Result<Arc<Timeline>, ApiError> {
    let tenant = tenant_manager.get_attached_tenant_shard(tenant_shard_id)?;

    tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

    Ok(tenant.get_timeline(timeline_id, true)?)
}

async fn always_panic_handler(
    req: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    // Deliberately cause a panic to exercise the panic hook registered via std::panic::set_hook().
    // For pageserver, the relevant panic hook is `tracing_panic_hook` , and the `sentry` crate's wrapper around it.
    // Use catch_unwind to ensure that tokio nor hyper are distracted by our panic.
    let query = req.uri().query();
    let _ = std::panic::catch_unwind(|| {
        panic!("unconditional panic for testing panic hook integration; request query: {query:?}")
    });
    json_response(StatusCode::NO_CONTENT, ())
}

async fn disk_usage_eviction_run(
    mut r: Request<Body>,
    cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    check_permission(&r, None)?;

    #[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
    struct Config {
        /// How many bytes to evict before reporting that pressure is relieved.
        evict_bytes: u64,

        #[serde(default)]
        eviction_order: pageserver_api::config::EvictionOrder,
    }

    #[derive(Debug, Clone, Copy, serde::Serialize)]
    struct Usage {
        // remains unchanged after instantiation of the struct
        evict_bytes: u64,
        // updated by `add_available_bytes`
        freed_bytes: u64,
    }

    impl crate::disk_usage_eviction_task::Usage for Usage {
        fn has_pressure(&self) -> bool {
            self.evict_bytes > self.freed_bytes
        }

        fn add_available_bytes(&mut self, bytes: u64) {
            self.freed_bytes += bytes;
        }
    }

    let config = json_request::<Config>(&mut r).await?;

    let usage = Usage {
        evict_bytes: config.evict_bytes,
        freed_bytes: 0,
    };

    let state = get_state(&r);
    let eviction_state = state.disk_usage_eviction_state.clone();

    let res = crate::disk_usage_eviction_task::disk_usage_eviction_task_iteration_impl(
        &eviction_state,
        &state.remote_storage,
        usage,
        &state.tenant_manager,
        config.eviction_order.into(),
        &cancel,
    )
    .await;

    info!(?res, "disk_usage_eviction_task_iteration_impl finished");

    let res = res.map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, res)
}

async fn secondary_upload_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let state = get_state(&request);
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    state
        .secondary_controller
        .upload_tenant(tenant_shard_id)
        .await?;

    json_response(StatusCode::OK, ())
}

async fn tenant_scan_remote_handler(
    request: Request<Body>,
    cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let state = get_state(&request);
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;

    let mut response = TenantScanRemoteStorageResponse::default();

    let (shards, _other_keys) =
        list_remote_tenant_shards(&state.remote_storage, tenant_id, cancel.clone())
            .await
            .map_err(|e| ApiError::InternalServerError(anyhow::anyhow!(e)))?;

    for tenant_shard_id in shards {
        let (timeline_ids, _other_keys) =
            list_remote_timelines(&state.remote_storage, tenant_shard_id, cancel.clone())
                .await
                .map_err(|e| ApiError::InternalServerError(anyhow::anyhow!(e)))?;

        let mut generation = Generation::none();
        for timeline_id in timeline_ids {
            match download_index_part(
                &state.remote_storage,
                &tenant_shard_id,
                &timeline_id,
                Generation::MAX,
                &cancel,
            )
            .instrument(info_span!("download_index_part",
                         tenant_id=%tenant_shard_id.tenant_id,
                         shard_id=%tenant_shard_id.shard_slug(),
                         %timeline_id))
            .await
            {
                Ok((index_part, index_generation, _index_mtime)) => {
                    tracing::info!("Found timeline {tenant_shard_id}/{timeline_id} metadata (gen {index_generation:?}, {} layers, {} consistent LSN)",
                        index_part.layer_metadata.len(), index_part.metadata.disk_consistent_lsn());
                    generation = std::cmp::max(generation, index_generation);
                }
                Err(DownloadError::NotFound) => {
                    // This is normal for tenants that were created with multiple shards: they have an unsharded path
                    // containing the timeline's initdb tarball but no index.  Otherwise it is a bit strange.
                    tracing::info!("Timeline path {tenant_shard_id}/{timeline_id} exists in remote storage but has no index, skipping");
                    continue;
                }
                Err(e) => {
                    return Err(ApiError::InternalServerError(anyhow::anyhow!(e)));
                }
            };
        }

        response.shards.push(TenantScanRemoteStorageShard {
            tenant_shard_id,
            generation: generation.into(),
        });
    }

    if response.shards.is_empty() {
        return Err(ApiError::NotFound(
            anyhow::anyhow!("No shards found for tenant ID {tenant_id}").into(),
        ));
    }

    json_response(StatusCode::OK, response)
}

async fn secondary_download_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let state = get_state(&request);
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let wait = parse_query_param(&request, "wait_ms")?.map(Duration::from_millis);

    // We don't need this to issue the download request, but:
    // - it enables us to cleanly return 404 if we get a request for an absent shard
    // - we will use this to provide status feedback in the response
    let Some(secondary_tenant) = state
        .tenant_manager
        .get_secondary_tenant_shard(tenant_shard_id)
    else {
        return Err(ApiError::NotFound(
            anyhow::anyhow!("Shard {} not found", tenant_shard_id).into(),
        ));
    };

    let timeout = wait.unwrap_or(Duration::MAX);

    let result = tokio::time::timeout(
        timeout,
        state.secondary_controller.download_tenant(tenant_shard_id),
    )
    .await;

    let progress = secondary_tenant.progress.lock().unwrap().clone();

    let status = match result {
        Ok(Ok(())) => {
            if progress.layers_downloaded >= progress.layers_total {
                // Download job ran to completion
                StatusCode::OK
            } else {
                // Download dropped out without errors because it ran out of time budget
                StatusCode::ACCEPTED
            }
        }
        // Edge case: downloads aren't usually fallible: things like a missing heatmap are considered
        // okay.  We could get an error here in the unlikely edge case that the tenant
        // was detached between our check above and executing the download job.
        Ok(Err(e)) => return Err(e.into()),
        // A timeout is not an error: we have started the download, we're just not done
        // yet.  The caller will get a response body indicating status.
        Err(_) => StatusCode::ACCEPTED,
    };

    json_response(status, progress)
}

async fn secondary_status_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let state = get_state(&request);
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;

    let Some(secondary_tenant) = state
        .tenant_manager
        .get_secondary_tenant_shard(tenant_shard_id)
    else {
        return Err(ApiError::NotFound(
            anyhow::anyhow!("Shard {} not found", tenant_shard_id).into(),
        ));
    };

    let progress = secondary_tenant.progress.lock().unwrap().clone();

    json_response(StatusCode::OK, progress)
}

async fn handler_404(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(
        StatusCode::NOT_FOUND,
        HttpErrorBody::from_msg("page not found".to_owned()),
    )
}

async fn post_tracing_event_handler(
    mut r: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "lowercase")]
    enum Level {
        Error,
        Warn,
        Info,
        Debug,
        Trace,
    }
    #[derive(Debug, serde::Deserialize)]
    struct Request {
        level: Level,
        message: String,
    }
    let body: Request = json_request(&mut r)
        .await
        .map_err(|_| ApiError::BadRequest(anyhow::anyhow!("invalid JSON body")))?;

    match body.level {
        Level::Error => tracing::error!(?body.message),
        Level::Warn => tracing::warn!(?body.message),
        Level::Info => tracing::info!(?body.message),
        Level::Debug => tracing::debug!(?body.message),
        Level::Trace => tracing::trace!(?body.message),
    }

    json_response(StatusCode::OK, ())
}

async fn put_io_engine_handler(
    mut r: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    check_permission(&r, None)?;
    let kind: crate::virtual_file::IoEngineKind = json_request(&mut r).await?;
    crate::virtual_file::io_engine::set(kind);
    json_response(StatusCode::OK, ())
}

async fn put_io_mode_handler(
    mut r: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    check_permission(&r, None)?;
    let mode: IoMode = json_request(&mut r).await?;
    crate::virtual_file::set_io_mode(mode);
    json_response(StatusCode::OK, ())
}

/// Polled by control plane.
///
/// See [`crate::utilization`].
async fn get_utilization(
    r: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    fail::fail_point!("get-utilization-http-handler", |_| {
        Err(ApiError::ResourceUnavailable("failpoint".into()))
    });

    // this probably could be completely public, but lets make that change later.
    check_permission(&r, None)?;

    let state = get_state(&r);
    let mut g = state.latest_utilization.lock().await;

    let regenerate_every = Duration::from_secs(1);
    let still_valid = g
        .as_ref()
        .is_some_and(|(captured_at, _)| captured_at.elapsed() < regenerate_every);

    // avoid needless statvfs calls even though those should be non-blocking fast.
    // regenerate at most 1Hz to allow polling at any rate.
    if !still_valid {
        let path = state.conf.tenants_path();
        let doc =
            crate::utilization::regenerate(state.conf, path.as_std_path(), &state.tenant_manager)
                .map_err(ApiError::InternalServerError)?;

        let mut buf = Vec::new();
        serde_json::to_writer(&mut buf, &doc)
            .context("serialize")
            .map_err(ApiError::InternalServerError)?;

        let body = bytes::Bytes::from(buf);

        *g = Some((std::time::Instant::now(), body));
    }

    // hyper 0.14 doesn't yet have Response::clone so this is a bit of extra legwork
    let cached = g.as_ref().expect("just set").1.clone();

    Response::builder()
        .header(hyper::http::header::CONTENT_TYPE, "application/json")
        // thought of using http date header, but that is second precision which does not give any
        // debugging aid
        .status(StatusCode::OK)
        .body(hyper::Body::from(cached))
        .context("build response")
        .map_err(ApiError::InternalServerError)
}

async fn list_aux_files(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let body: ListAuxFilesRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let state = get_state(&request);

    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let files = timeline.list_aux_files(body.lsn, &ctx).await?;
    json_response(StatusCode::OK, files)
}

async fn perf_info(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let state = get_state(&request);

    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;

    let result = timeline.perf_info().await;

    json_response(StatusCode::OK, result)
}

async fn ingest_aux_files(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&request, "tenant_shard_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let body: IngestAuxFilesRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_shard_id.tenant_id))?;

    let state = get_state(&request);

    let timeline =
        active_timeline_of_active_tenant(&state.tenant_manager, tenant_shard_id, timeline_id)
            .await?;

    let mut modification = timeline.begin_modification(
        Lsn(timeline.get_last_record_lsn().0 + 8), /* advance LSN by 8 */
    );
    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    for (fname, content) in body.aux_files {
        modification
            .put_file(&fname, content.as_bytes(), &ctx)
            .await
            .map_err(ApiError::InternalServerError)?;
    }
    modification
        .commit(&ctx)
        .await
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

/// Report on the largest tenants on this pageserver, for the storage controller to identify
/// candidates for splitting
async fn post_top_tenants(
    mut r: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    check_permission(&r, None)?;
    let request: TopTenantShardsRequest = json_request(&mut r).await?;
    let state = get_state(&r);

    fn get_size_metric(sizes: &TopTenantShardItem, order_by: &TenantSorting) -> u64 {
        match order_by {
            TenantSorting::ResidentSize => sizes.resident_size,
            TenantSorting::MaxLogicalSize => sizes.max_logical_size,
        }
    }

    #[derive(Eq, PartialEq)]
    struct HeapItem {
        metric: u64,
        sizes: TopTenantShardItem,
    }

    impl PartialOrd for HeapItem {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    /// Heap items have reverse ordering on their metric: this enables using BinaryHeap, which
    /// supports popping the greatest item but not the smallest.
    impl Ord for HeapItem {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            Reverse(self.metric).cmp(&Reverse(other.metric))
        }
    }

    let mut top_n: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(request.limit);

    // FIXME: this is a lot of clones to take this tenant list
    for (tenant_shard_id, tenant_slot) in state.tenant_manager.list() {
        if let Some(shards_lt) = request.where_shards_lt {
            // Ignore tenants which already have >= this many shards
            if tenant_shard_id.shard_count >= shards_lt {
                continue;
            }
        }

        let sizes = match tenant_slot {
            TenantSlot::Attached(tenant) => tenant.get_sizes(),
            TenantSlot::Secondary(_) | TenantSlot::InProgress(_) => {
                continue;
            }
        };
        let metric = get_size_metric(&sizes, &request.order_by);

        if let Some(gt) = request.where_gt {
            // Ignore tenants whose metric is <= the lower size threshold, to do less sorting work
            if metric <= gt {
                continue;
            }
        };

        match top_n.peek() {
            None => {
                // Top N list is empty: candidate becomes first member
                top_n.push(HeapItem { metric, sizes });
            }
            Some(i) if i.metric > metric && top_n.len() < request.limit => {
                // Lowest item in list is greater than our candidate, but we aren't at limit yet: push to end
                top_n.push(HeapItem { metric, sizes });
            }
            Some(i) if i.metric > metric => {
                // List is at limit and lowest value is greater than our candidate, drop it.
            }
            Some(_) => top_n.push(HeapItem { metric, sizes }),
        }

        while top_n.len() > request.limit {
            top_n.pop();
        }
    }

    json_response(
        StatusCode::OK,
        TopTenantShardsResponse {
            shards: top_n.into_iter().map(|i| i.sizes).collect(),
        },
    )
}

async fn put_tenant_timeline_import_basebackup(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let base_lsn: Lsn = must_parse_query_param(&request, "base_lsn")?;
    let end_lsn: Lsn = must_parse_query_param(&request, "end_lsn")?;
    let pg_version: u32 = must_parse_query_param(&request, "pg_version")?;

    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    let span = info_span!("import_basebackup", tenant_id=%tenant_id, timeline_id=%timeline_id, base_lsn=%base_lsn, end_lsn=%end_lsn, pg_version=%pg_version);
    async move {
        let state = get_state(&request);
        let tenant = state
            .tenant_manager
            .get_attached_tenant_shard(TenantShardId::unsharded(tenant_id))?;

        let broker_client = state.broker_client.clone();

        let mut body = StreamReader::new(request.into_body().map(|res| {
            res.map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::Other, anyhow::anyhow!(error))
            })
        }));

        tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

        let timeline = tenant
            .create_empty_timeline(timeline_id, base_lsn, pg_version, &ctx)
            .map_err(ApiError::InternalServerError)
            .await?;

        // TODO mark timeline as not ready until it reaches end_lsn.
        // We might have some wal to import as well, and we should prevent compute
        // from connecting before that and writing conflicting wal.
        //
        // This is not relevant for pageserver->pageserver migrations, since there's
        // no wal to import. But should be fixed if we want to import from postgres.

        // TODO leave clean state on error. For now you can use detach to clean
        // up broken state from a failed import.

        // Import basebackup provided via CopyData
        info!("importing basebackup");

        timeline
            .import_basebackup_from_tar(tenant.clone(), &mut body, base_lsn, broker_client, &ctx)
            .await
            .map_err(ApiError::InternalServerError)?;

        // Read the end of the tar archive.
        read_tar_eof(body)
            .await
            .map_err(ApiError::InternalServerError)?;

        // TODO check checksum
        // Meanwhile you can verify client-side by taking fullbackup
        // and checking that it matches in size with what was imported.
        // It wouldn't work if base came from vanilla postgres though,
        // since we discard some log files.

        info!("done");
        json_response(StatusCode::OK, ())
    }
    .instrument(span)
    .await
}

async fn put_tenant_timeline_import_wal(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let start_lsn: Lsn = must_parse_query_param(&request, "start_lsn")?;
    let end_lsn: Lsn = must_parse_query_param(&request, "end_lsn")?;

    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    let span = info_span!("import_wal", tenant_id=%tenant_id, timeline_id=%timeline_id, start_lsn=%start_lsn, end_lsn=%end_lsn);
    async move {
        let state = get_state(&request);

        let timeline = active_timeline_of_active_tenant(&state.tenant_manager, TenantShardId::unsharded(tenant_id), timeline_id).await?;

        let mut body = StreamReader::new(request.into_body().map(|res| {
            res.map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::Other, anyhow::anyhow!(error))
            })
        }));

        let last_record_lsn = timeline.get_last_record_lsn();
        if last_record_lsn != start_lsn {
            return Err(ApiError::InternalServerError(anyhow::anyhow!("Cannot import WAL from Lsn {start_lsn} because timeline does not start from the same lsn: {last_record_lsn}")));
        }

        // TODO leave clean state on error. For now you can use detach to clean
        // up broken state from a failed import.

        // Import wal provided via CopyData
        info!("importing wal");
        crate::import_datadir::import_wal_from_tar(&timeline, &mut body, start_lsn, end_lsn, &ctx).await.map_err(ApiError::InternalServerError)?;
        info!("wal import complete");

        // Read the end of the tar archive.
        read_tar_eof(body).await.map_err(ApiError::InternalServerError)?;

        // TODO Does it make sense to overshoot?
        if timeline.get_last_record_lsn() < end_lsn {
            return Err(ApiError::InternalServerError(anyhow::anyhow!("Cannot import WAL from Lsn {start_lsn} because timeline does not start from the same lsn: {last_record_lsn}")));
        }

        // Flush data to disk, then upload to s3. No need for a forced checkpoint.
        // We only want to persist the data, and it doesn't matter if it's in the
        // shape of deltas or images.
        info!("flushing layers");
        timeline.freeze_and_flush().await.map_err(|e| match e {
            tenant::timeline::FlushLayerError::Cancelled => ApiError::ShuttingDown,
            other => ApiError::InternalServerError(anyhow::anyhow!(other)),
        })?;

        info!("done");

        json_response(StatusCode::OK, ())
    }.instrument(span).await
}

/// Read the end of a tar archive.
///
/// A tar archive normally ends with two consecutive blocks of zeros, 512 bytes each.
/// `tokio_tar` already read the first such block. Read the second all-zeros block,
/// and check that there is no more data after the EOF marker.
///
/// 'tar' command can also write extra blocks of zeros, up to a record
/// size, controlled by the --record-size argument. Ignore them too.
async fn read_tar_eof(mut reader: (impl tokio::io::AsyncRead + Unpin)) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 512];

    // Read the all-zeros block, and verify it
    let mut total_bytes = 0;
    while total_bytes < 512 {
        let nbytes = reader.read(&mut buf[total_bytes..]).await?;
        total_bytes += nbytes;
        if nbytes == 0 {
            break;
        }
    }
    if total_bytes < 512 {
        anyhow::bail!("incomplete or invalid tar EOF marker");
    }
    if !buf.iter().all(|&x| x == 0) {
        anyhow::bail!("invalid tar EOF marker");
    }

    // Drain any extra zero-blocks after the EOF marker
    let mut trailing_bytes = 0;
    let mut seen_nonzero_bytes = false;
    loop {
        let nbytes = reader.read(&mut buf).await?;
        trailing_bytes += nbytes;
        if !buf.iter().all(|&x| x == 0) {
            seen_nonzero_bytes = true;
        }
        if nbytes == 0 {
            break;
        }
    }
    if seen_nonzero_bytes {
        anyhow::bail!("unexpected non-zero bytes after the tar archive");
    }
    if trailing_bytes % 512 != 0 {
        anyhow::bail!("unexpected number of zeros ({trailing_bytes}), not divisible by tar block size (512 bytes), after the tar archive");
    }
    Ok(())
}

/// Common functionality of all the HTTP API handlers.
///
/// - Adds a tracing span to each request (by `request_span`)
/// - Logs the request depending on the request method (by `request_span`)
/// - Logs the response if it was not successful (by `request_span`
/// - Shields the handler function from async cancellations. Hyper can drop the handler
///   Future if the connection to the client is lost, but most of the pageserver code is
///   not async cancellation safe. This converts the dropped future into a graceful cancellation
///   request with a CancellationToken.
async fn api_handler<R, H>(request: Request<Body>, handler: H) -> Result<Response<Body>, ApiError>
where
    R: std::future::Future<Output = Result<Response<Body>, ApiError>> + Send + 'static,
    H: FnOnce(Request<Body>, CancellationToken) -> R + Send + Sync + 'static,
{
    if request.uri() != &"/v1/failpoints".parse::<Uri>().unwrap() {
        fail::fail_point!("api-503", |_| Err(ApiError::ResourceUnavailable(
            "failpoint".into()
        )));

        fail::fail_point!("api-500", |_| Err(ApiError::InternalServerError(
            anyhow::anyhow!("failpoint")
        )));
    }

    // Spawn a new task to handle the request, to protect the handler from unexpected
    // async cancellations. Most pageserver functions are not async cancellation safe.
    // We arm a drop-guard, so that if Hyper drops the Future, we signal the task
    // with the cancellation token.
    let token = CancellationToken::new();
    let cancel_guard = token.clone().drop_guard();
    let result = request_span(request, move |r| async {
        let handle = tokio::spawn(
            async {
                let token_cloned = token.clone();
                let result = handler(r, token).await;
                if token_cloned.is_cancelled() {
                    // dropguard has executed: we will never turn this result into response.
                    //
                    // at least temporarily do {:?} logging; these failures are rare enough but
                    // could hide difficult errors.
                    match &result {
                        Ok(response) => {
                            let status = response.status();
                            info!(%status, "Cancelled request finished successfully")
                        }
                        Err(e) => error!("Cancelled request finished with an error: {e:?}"),
                    }
                }
                // only logging for cancelled panicked request handlers is the tracing_panic_hook,
                // which should suffice.
                //
                // there is still a chance to lose the result due to race between
                // returning from here and the actual connection closing happening
                // before outer task gets to execute. leaving that up for #5815.
                result
            }
            .in_current_span(),
        );

        match handle.await {
            // TODO: never actually return Err from here, always Ok(...) so that we can log
            // spanned errors. Call api_error_handler instead and return appropriate Body.
            Ok(result) => result,
            Err(e) => {
                // The handler task panicked. We have a global panic handler that logs the
                // panic with its backtrace, so no need to log that here. Only log a brief
                // message to make it clear that we returned the error to the client.
                error!("HTTP request handler task panicked: {e:#}");

                // Don't return an Error here, because then fallback error handler that was
                // installed in make_router() will print the error. Instead, construct the
                // HTTP error response and return that.
                Ok(
                    ApiError::InternalServerError(anyhow!("HTTP request handler task panicked"))
                        .into_response(),
                )
            }
        }
    })
    .await;

    cancel_guard.disarm();

    result
}

/// Like api_handler, but returns an error response if the server is built without
/// the 'testing' feature.
async fn testing_api_handler<R, H>(
    desc: &str,
    request: Request<Body>,
    handler: H,
) -> Result<Response<Body>, ApiError>
where
    R: std::future::Future<Output = Result<Response<Body>, ApiError>> + Send + 'static,
    H: FnOnce(Request<Body>, CancellationToken) -> R + Send + Sync + 'static,
{
    if cfg!(feature = "testing") {
        api_handler(request, handler).await
    } else {
        std::future::ready(Err(ApiError::BadRequest(anyhow!(
            "Cannot {desc} because pageserver was compiled without testing APIs",
        ))))
        .await
    }
}

pub fn make_router(
    state: Arc<State>,
    launch_ts: &'static LaunchTimestamp,
    auth: Option<Arc<SwappableJwtAuth>>,
) -> anyhow::Result<RouterBuilder<hyper::Body, ApiError>> {
    let spec = include_bytes!("openapi_spec.yml");
    let mut router = attach_openapi_ui(endpoint::make_router(), spec, "/swagger.yml", "/v1/doc");
    if auth.is_some() {
        router = router.middleware(auth_middleware(|request| {
            let state = get_state(request);
            if state.allowlist_routes.contains(&request.uri().path()) {
                None
            } else {
                state.auth.as_deref()
            }
        }))
    }

    router = router.middleware(
        endpoint::add_response_header_middleware(
            "PAGESERVER_LAUNCH_TIMESTAMP",
            &launch_ts.to_string(),
        )
        .expect("construct launch timestamp header middleware"),
    );

    Ok(router
        .data(state)
        .get("/metrics", |r| request_span(r, prometheus_metrics_handler))
        .get("/profile/cpu", |r| request_span(r, profile_cpu_handler))
        .get("/profile/heap", |r| request_span(r, profile_heap_handler))
        .get("/v1/status", |r| api_handler(r, status_handler))
        .put("/v1/failpoints", |r| {
            testing_api_handler("manage failpoints", r, failpoints_handler)
        })
        .post("/v1/reload_auth_validation_keys", |r| {
            api_handler(r, reload_auth_validation_keys_handler)
        })
        .get("/v1/tenant", |r| api_handler(r, tenant_list_handler))
        .get("/v1/tenant/:tenant_shard_id", |r| {
            api_handler(r, tenant_status)
        })
        .delete("/v1/tenant/:tenant_shard_id", |r| {
            api_handler(r, tenant_delete_handler)
        })
        .get("/v1/tenant/:tenant_shard_id/synthetic_size", |r| {
            api_handler(r, tenant_size_handler)
        })
        .patch("/v1/tenant/config", |r| {
            api_handler(r, patch_tenant_config_handler)
        })
        .put("/v1/tenant/config", |r| {
            api_handler(r, update_tenant_config_handler)
        })
        .put("/v1/tenant/:tenant_shard_id/shard_split", |r| {
            api_handler(r, tenant_shard_split_handler)
        })
        .get("/v1/tenant/:tenant_shard_id/config", |r| {
            api_handler(r, get_tenant_config_handler)
        })
        .put("/v1/tenant/:tenant_shard_id/location_config", |r| {
            api_handler(r, put_tenant_location_config_handler)
        })
        .get("/v1/location_config", |r| {
            api_handler(r, list_location_config_handler)
        })
        .get("/v1/location_config/:tenant_shard_id", |r| {
            api_handler(r, get_location_config_handler)
        })
        .put(
            "/v1/tenant/:tenant_shard_id/time_travel_remote_storage",
            |r| api_handler(r, tenant_time_travel_remote_storage_handler),
        )
        .get("/v1/tenant/:tenant_shard_id/timeline", |r| {
            api_handler(r, timeline_list_handler)
        })
        .get("/v1/tenant/:tenant_shard_id/timeline_and_offloaded", |r| {
            api_handler(r, timeline_and_offloaded_list_handler)
        })
        .post("/v1/tenant/:tenant_shard_id/timeline", |r| {
            api_handler(r, timeline_create_handler)
        })
        .post("/v1/tenant/:tenant_shard_id/reset", |r| {
            api_handler(r, tenant_reset_handler)
        })
        .post(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/preserve_initdb_archive",
            |r| api_handler(r, timeline_preserve_initdb_handler),
        )
        .put(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/archival_config",
            |r| api_handler(r, timeline_archival_config_handler),
        )
        .get("/v1/tenant/:tenant_shard_id/timeline/:timeline_id", |r| {
            api_handler(r, timeline_detail_handler)
        })
        .get(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/get_lsn_by_timestamp",
            |r| api_handler(r, get_lsn_by_timestamp_handler),
        )
        .get(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/get_timestamp_of_lsn",
            |r| api_handler(r, get_timestamp_of_lsn_handler),
        )
        .post(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/lsn_lease",
            |r| api_handler(r, lsn_lease_handler),
        )
        .put(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/do_gc",
            |r| api_handler(r, timeline_gc_handler),
        )
        .get(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/compact",
            |r| api_handler(r, timeline_compact_info_handler),
        )
        .put(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/compact",
            |r| api_handler(r, timeline_compact_handler),
        )
        .delete(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/compact",
            |r| api_handler(r, timeline_cancel_compact_handler),
        )
        .put(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/offload",
            |r| testing_api_handler("attempt timeline offload", r, timeline_offload_handler),
        )
        .put(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/checkpoint",
            |r| testing_api_handler("run timeline checkpoint", r, timeline_checkpoint_handler),
        )
        .post(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/download_remote_layers",
            |r| api_handler(r, timeline_download_remote_layers_handler_post),
        )
        .get(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/download_remote_layers",
            |r| api_handler(r, timeline_download_remote_layers_handler_get),
        )
        .put(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/detach_ancestor",
            |r| api_handler(r, timeline_detach_ancestor_handler),
        )
        .delete("/v1/tenant/:tenant_shard_id/timeline/:timeline_id", |r| {
            api_handler(r, timeline_delete_handler)
        })
        .get(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/layer",
            |r| api_handler(r, layer_map_info_handler),
        )
        .get(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/layer/:layer_file_name",
            |r| api_handler(r, layer_download_handler),
        )
        .delete(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/layer/:layer_file_name",
            |r| api_handler(r, evict_timeline_layer_handler),
        )
        .post(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/layer/:layer_name/scan_disposable_keys",
            |r| testing_api_handler("timeline_layer_scan_disposable_keys", r, timeline_layer_scan_disposable_keys),
        )
        .post(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/block_gc",
            |r| api_handler(r, timeline_gc_blocking_handler),
        )
        .post(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/unblock_gc",
            |r| api_handler(r, timeline_gc_unblocking_handler),
        )
        .post("/v1/tenant/:tenant_shard_id/heatmap_upload", |r| {
            api_handler(r, secondary_upload_handler)
        })
        .get("/v1/tenant/:tenant_id/scan_remote_storage", |r| {
            api_handler(r, tenant_scan_remote_handler)
        })
        .put("/v1/disk_usage_eviction/run", |r| {
            api_handler(r, disk_usage_eviction_run)
        })
        .put("/v1/deletion_queue/flush", |r| {
            api_handler(r, deletion_queue_flush)
        })
        .get("/v1/tenant/:tenant_shard_id/secondary/status", |r| {
            api_handler(r, secondary_status_handler)
        })
        .post("/v1/tenant/:tenant_shard_id/secondary/download", |r| {
            api_handler(r, secondary_download_handler)
        })
        .put("/v1/tenant/:tenant_shard_id/break", |r| {
            testing_api_handler("set tenant state to broken", r, handle_tenant_break)
        })
        .get("/v1/panic", |r| api_handler(r, always_panic_handler))
        .post("/v1/tracing/event", |r| {
            testing_api_handler("emit a tracing event", r, post_tracing_event_handler)
        })
        .get(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/getpage",
            |r| testing_api_handler("getpage@lsn", r, getpage_at_lsn_handler),
        )
        .get(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/keyspace",
            |r| api_handler(r, timeline_collect_keyspace),
        )
        .put("/v1/io_engine", |r| api_handler(r, put_io_engine_handler))
        .put("/v1/io_mode", |r| api_handler(r, put_io_mode_handler))
        .get("/v1/utilization", |r| api_handler(r, get_utilization))
        .post(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/ingest_aux_files",
            |r| testing_api_handler("ingest_aux_files", r, ingest_aux_files),
        )
        .post(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/list_aux_files",
            |r| testing_api_handler("list_aux_files", r, list_aux_files),
        )
        .post("/v1/top_tenants", |r| api_handler(r, post_top_tenants))
        .post(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/perf_info",
            |r| testing_api_handler("perf_info", r, perf_info),
        )
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/import_basebackup",
            |r| api_handler(r, put_tenant_timeline_import_basebackup),
        )
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/import_wal",
            |r| api_handler(r, put_tenant_timeline_import_wal),
        )
        .any(handler_404))
}
