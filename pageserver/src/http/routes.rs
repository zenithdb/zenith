//!
//! Management HTTP API
//!
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use metrics::launch_timestamp::LaunchTimestamp;
use pageserver_api::models::{DownloadRemoteLayersTaskSpawnRequest, TenantAttachRequest};
use remote_storage::GenericRemoteStorage;
use storage_broker::BrokerClientChannel;
use tenant_size_model::{SizeResult, StorageModel};
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::http::endpoint::request_span;
use utils::http::json::json_request_or_empty_body;
use utils::http::request::{get_request_param, must_get_query_param, parse_query_param};

use super::models::{
    StatusResponse, TenantConfigRequest, TenantCreateRequest, TenantCreateResponse, TenantInfo,
    TimelineCreateRequest, TimelineGcRequest, TimelineInfo,
};
use crate::context::{DownloadBehavior, RequestContext};
use crate::disk_usage_eviction_task;
use crate::metrics::{StorageTimeOperation, STORAGE_TIME_GLOBAL};
use crate::pgdatadir_mapping::LsnForTimestamp;
use crate::task_mgr::TaskKind;
use crate::tenant::config::TenantConfOpt;
use crate::tenant::mgr::{
    GetTenantError, SetNewTenantConfigError, TenantMapInsertError, TenantStateError,
};
use crate::tenant::size::ModelInputs;
use crate::tenant::storage_layer::LayerAccessStatsReset;
use crate::tenant::{LogicalSizeCalculationCause, PageReconstructError, Timeline};
use crate::{config::PageServerConf, tenant::mgr};
use utils::{
    auth::JwtAuth,
    http::{
        endpoint::{self, attach_openapi_ui, auth_middleware, check_permission_with},
        error::{ApiError, HttpErrorBody},
        json::{json_request, json_response},
        request::parse_request_param,
        RequestExt, RouterBuilder,
    },
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

// Imports only used for testing APIs
use super::models::ConfigureFailpointsRequest;

struct State {
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    allowlist_routes: Vec<Uri>,
    remote_storage: Option<GenericRemoteStorage>,
    broker_client: storage_broker::BrokerClientChannel,
    disk_usage_eviction_state: Arc<disk_usage_eviction_task::State>,
}

impl State {
    fn new(
        conf: &'static PageServerConf,
        auth: Option<Arc<JwtAuth>>,
        remote_storage: Option<GenericRemoteStorage>,
        broker_client: storage_broker::BrokerClientChannel,
        disk_usage_eviction_state: Arc<disk_usage_eviction_task::State>,
    ) -> anyhow::Result<Self> {
        let allowlist_routes = ["/v1/status", "/v1/doc", "/swagger.yml"]
            .iter()
            .map(|v| v.parse().unwrap())
            .collect::<Vec<_>>();
        Ok(Self {
            conf,
            auth,
            allowlist_routes,
            remote_storage,
            broker_client,
            disk_usage_eviction_state,
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
            PageReconstructError::Other(pre) => ApiError::InternalServerError(pre),
            PageReconstructError::NeedsDownload(_, _) => {
                // This shouldn't happen, because we use a RequestContext that requests to
                // download any missing layer files on-demand.
                ApiError::InternalServerError(anyhow::anyhow!("need to download remote layer file"))
            }
            PageReconstructError::Cancelled => {
                ApiError::InternalServerError(anyhow::anyhow!("request was cancelled"))
            }
            PageReconstructError::AncestorStopping(_) => {
                ApiError::InternalServerError(anyhow::Error::new(pre))
            }
            PageReconstructError::WalRedo(pre) => {
                ApiError::InternalServerError(anyhow::Error::new(pre))
            }
        }
    }
}

impl From<TenantMapInsertError> for ApiError {
    fn from(tmie: TenantMapInsertError) -> ApiError {
        match tmie {
            TenantMapInsertError::StillInitializing | TenantMapInsertError::ShuttingDown => {
                ApiError::InternalServerError(anyhow::Error::new(tmie))
            }
            TenantMapInsertError::TenantAlreadyExists(id, state) => {
                ApiError::Conflict(format!("tenant {id} already exists, state: {state:?}"))
            }
            TenantMapInsertError::Closure(e) => ApiError::InternalServerError(e),
        }
    }
}

impl From<TenantStateError> for ApiError {
    fn from(tse: TenantStateError) -> ApiError {
        match tse {
            TenantStateError::NotFound(tid) => ApiError::NotFound(anyhow!("tenant {}", tid)),
            _ => ApiError::InternalServerError(anyhow::Error::new(tse)),
        }
    }
}

impl From<GetTenantError> for ApiError {
    fn from(tse: GetTenantError) -> ApiError {
        match tse {
            GetTenantError::NotFound(tid) => ApiError::NotFound(anyhow!("tenant {}", tid)),
            e @ GetTenantError::NotActive(_) => {
                // Why is this not `ApiError::NotFound`?
                // Because we must be careful to never return 404 for a tenant if it does
                // in fact exist locally. If we did, the caller could draw the conclusion
                // that it can attach the tenant to another PS and we'd be in split-brain.
                //
                // (We can produce this variant only in `mgr::get_tenant(..., active=true)` calls).
                ApiError::InternalServerError(anyhow::Error::new(e))
            }
            e @ GetTenantError::NotLoaded(_, _) => {
                ApiError::InternalServerError(anyhow::Error::new(e))
            }
            e @ GetTenantError::NotActivated(_, _) => {
                ApiError::InternalServerError(anyhow::Error::new(e))
            }
        }
    }
}

impl From<SetNewTenantConfigError> for ApiError {
    fn from(e: SetNewTenantConfigError) -> ApiError {
        match e {
            SetNewTenantConfigError::GetTenant(tid) => {
                ApiError::NotFound(anyhow!("tenant {}", tid))
            }
            e @ SetNewTenantConfigError::Persist(_) => {
                ApiError::InternalServerError(anyhow::Error::new(e))
            }
        }
    }
}

impl From<crate::tenant::DeleteTimelineError> for ApiError {
    fn from(value: crate::tenant::DeleteTimelineError) -> Self {
        use crate::tenant::DeleteTimelineError::*;
        match value {
            NotFound => ApiError::NotFound(anyhow::anyhow!("timeline not found")),
            HasChildren => ApiError::BadRequest(anyhow::anyhow!(
                "Cannot delete timeline which has child timelines"
            )),
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
            Tenant(GetTenantError::NotFound(..)) => {
                ApiError::PreconditionFailed("Requested tenant is missing")
            }
            Tenant(t) => ApiError::from(t),
            Timeline(t) => ApiError::from(t),
        }
    }
}

// Helper function to construct a TimelineInfo struct for a timeline
async fn build_timeline_info(
    timeline: &Arc<Timeline>,
    include_non_incremental_logical_size: bool,
    ctx: &RequestContext,
) -> anyhow::Result<TimelineInfo> {
    crate::tenant::debug_assert_current_span_has_tenant_and_timeline_id();

    let mut info = build_timeline_info_common(timeline, ctx)?;
    if include_non_incremental_logical_size {
        // XXX we should be using spawn_ondemand_logical_size_calculation here.
        // Otherwise, if someone deletes the timeline / detaches the tenant while
        // we're executing this function, we will outlive the timeline on-disk state.
        info.current_logical_size_non_incremental = Some(
            timeline
                .get_current_logical_size_non_incremental(
                    info.last_record_lsn,
                    CancellationToken::new(),
                    ctx,
                )
                .await?,
        );
    }
    Ok(info)
}

fn build_timeline_info_common(
    timeline: &Arc<Timeline>,
    ctx: &RequestContext,
) -> anyhow::Result<TimelineInfo> {
    crate::tenant::debug_assert_current_span_has_tenant_and_timeline_id();
    let last_record_lsn = timeline.get_last_record_lsn();
    let (wal_source_connstr, last_received_msg_lsn, last_received_msg_ts) = {
        let guard = timeline.last_received_wal.lock().unwrap();
        if let Some(info) = guard.as_ref() {
            (
                Some(format!("{:?}", info.wal_source_connconf)), // Password is hidden, but it's for statistics only.
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
    let current_logical_size = match timeline.get_current_logical_size(ctx) {
        Ok((size, _)) => Some(size),
        Err(err) => {
            error!("Timeline info creation failed to get current logical size: {err:?}");
            None
        }
    };
    let current_physical_size = Some(timeline.layer_size_sum());
    let state = timeline.current_state();
    let remote_consistent_lsn = timeline.get_remote_consistent_lsn().unwrap_or(Lsn(0));

    let info = TimelineInfo {
        tenant_id: timeline.tenant_id,
        timeline_id: timeline.timeline_id,
        ancestor_timeline_id,
        ancestor_lsn,
        disk_consistent_lsn: timeline.get_disk_consistent_lsn(),
        remote_consistent_lsn,
        last_record_lsn,
        prev_record_lsn: Some(timeline.get_prev_record_lsn()),
        latest_gc_cutoff_lsn: *timeline.get_latest_gc_cutoff_lsn(),
        current_logical_size,
        current_physical_size,
        current_logical_size_non_incremental: None,
        timeline_dir_layer_file_size_sum: None,
        wal_source_connstr,
        last_received_msg_lsn,
        last_received_msg_ts,
        pg_version: timeline.pg_version,

        state,
    };
    Ok(info)
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

async fn timeline_create_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let request_data: TimelineCreateRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_id))?;

    let new_timeline_id = request_data.new_timeline_id;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Error);

    let state = get_state(&request);

    async {
        let tenant = mgr::get_tenant(tenant_id, true).await?;
        match tenant.create_timeline(
            new_timeline_id,
            request_data.ancestor_timeline_id.map(TimelineId::from),
            request_data.ancestor_start_lsn,
            request_data.pg_version.unwrap_or(crate::DEFAULT_PG_VERSION),
            state.broker_client.clone(),
            &ctx,
        )
        .await {
            Ok(Some(new_timeline)) => {
                // Created. Construct a TimelineInfo for it.
                let timeline_info = build_timeline_info_common(&new_timeline, &ctx)
                    .map_err(ApiError::InternalServerError)?;
                json_response(StatusCode::CREATED, timeline_info)
            }
            Ok(None) => json_response(StatusCode::CONFLICT, ()), // timeline already exists
            Err(err) => Err(ApiError::InternalServerError(err)),
        }
    }
    .instrument(info_span!("timeline_create", tenant = %tenant_id, timeline_id = %new_timeline_id, lsn=?request_data.ancestor_start_lsn, pg_version=?request_data.pg_version))
    .await
}

async fn timeline_list_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let include_non_incremental_logical_size: Option<bool> =
        parse_query_param(&request, "include-non-incremental-logical-size")?;
    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);

    let response_data = async {
        let tenant = mgr::get_tenant(tenant_id, true).await?;
        let timelines = tenant.list_timelines();

        let mut response_data = Vec::with_capacity(timelines.len());
        for timeline in timelines {
            let timeline_info = build_timeline_info(
                &timeline,
                include_non_incremental_logical_size.unwrap_or(false),
                &ctx,
            )
            .instrument(info_span!("build_timeline_info", timeline_id = %timeline.timeline_id))
            .await
            .context("Failed to convert tenant timeline {timeline_id} into the local one: {e:?}")
            .map_err(ApiError::InternalServerError)?;

            response_data.push(timeline_info);
        }
        Ok::<Vec<TimelineInfo>, ApiError>(response_data)
    }
    .instrument(info_span!("timeline_list", tenant = %tenant_id))
    .await?;

    json_response(StatusCode::OK, response_data)
}

async fn timeline_detail_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let include_non_incremental_logical_size: Option<bool> =
        parse_query_param(&request, "include-non-incremental-logical-size")?;
    check_permission(&request, Some(tenant_id))?;

    // Logical size calculation needs downloading.
    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);

    let timeline_info = async {
        let tenant = mgr::get_tenant(tenant_id, true).await?;

        let timeline = tenant
            .get_timeline(timeline_id, false)
            .map_err(ApiError::NotFound)?;

        let timeline_info = build_timeline_info(
            &timeline,
            include_non_incremental_logical_size.unwrap_or(false),
            &ctx,
        )
        .await
        .context("get local timeline info")
        .map_err(ApiError::InternalServerError)?;

        Ok::<_, ApiError>(timeline_info)
    }
    .instrument(info_span!("timeline_detail", tenant = %tenant_id, timeline = %timeline_id))
    .await?;

    json_response(StatusCode::OK, timeline_info)
}

async fn get_lsn_by_timestamp_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let timestamp_raw = must_get_query_param(&request, "timestamp")?;
    let timestamp = humantime::parse_rfc3339(&timestamp_raw)
        .with_context(|| format!("Invalid time: {:?}", timestamp_raw))
        .map_err(ApiError::BadRequest)?;
    let timestamp_pg = postgres_ffi::to_pg_timestamp(timestamp);

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    let result = timeline.find_lsn_for_timestamp(timestamp_pg, &ctx).await?;

    let result = match result {
        LsnForTimestamp::Present(lsn) => format!("{lsn}"),
        LsnForTimestamp::Future(_lsn) => "future".into(),
        LsnForTimestamp::Past(_lsn) => "past".into(),
        LsnForTimestamp::NoData(_lsn) => "nodata".into(),
    };
    json_response(StatusCode::OK, result)
}

async fn tenant_attach_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let maybe_body: Option<TenantAttachRequest> = json_request_or_empty_body(&mut request).await?;
    let tenant_conf = match maybe_body {
        Some(request) => TenantConfOpt::try_from(&*request.config).map_err(ApiError::BadRequest)?,
        None => TenantConfOpt::default(),
    };

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    info!("Handling tenant attach {tenant_id}");

    let state = get_state(&request);

    if let Some(remote_storage) = &state.remote_storage {
        mgr::attach_tenant(
            state.conf,
            tenant_id,
            tenant_conf,
            state.broker_client.clone(),
            remote_storage.clone(),
            &ctx,
        )
        .instrument(info_span!("tenant_attach", tenant = %tenant_id))
        .await?;
    } else {
        return Err(ApiError::BadRequest(anyhow!(
            "attach_tenant is not possible because pageserver was configured without remote storage"
        )));
    }

    json_response(StatusCode::ACCEPTED, ())
}

async fn timeline_delete_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    mgr::delete_timeline(tenant_id, timeline_id, &ctx)
        .instrument(info_span!("timeline_delete", tenant = %tenant_id, timeline = %timeline_id))
        .await?;

    json_response(StatusCode::OK, ())
}

async fn tenant_detach_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let detach_ignored: Option<bool> = parse_query_param(&request, "detach_ignored")?;

    let state = get_state(&request);
    let conf = state.conf;
    mgr::detach_tenant(conf, tenant_id, detach_ignored.unwrap_or(false))
        .instrument(info_span!("tenant_detach", tenant = %tenant_id))
        .await?;

    json_response(StatusCode::OK, ())
}

async fn tenant_load_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    let state = get_state(&request);
    mgr::load_tenant(
        state.conf,
        tenant_id,
        state.broker_client.clone(),
        state.remote_storage.clone(),
        &ctx,
    )
    .instrument(info_span!("load", tenant = %tenant_id))
    .await?;

    json_response(StatusCode::ACCEPTED, ())
}

async fn tenant_ignore_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);
    let conf = state.conf;
    mgr::ignore_tenant(conf, tenant_id)
        .instrument(info_span!("ignore_tenant", tenant = %tenant_id))
        .await?;

    json_response(StatusCode::OK, ())
}

async fn tenant_list_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;

    let response_data = mgr::list_tenants()
        .instrument(info_span!("tenant_list"))
        .await
        .map_err(anyhow::Error::new)
        .map_err(ApiError::InternalServerError)?
        .iter()
        .map(|(id, state)| TenantInfo {
            id: *id,
            state: state.clone(),
            current_physical_size: None,
            attachment_status: state.attachment_status(),
        })
        .collect::<Vec<TenantInfo>>();

    json_response(StatusCode::OK, response_data)
}

async fn tenant_status(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let tenant_info = async {
        let tenant = mgr::get_tenant(tenant_id, false).await?;

        // Calculate total physical size of all timelines
        let mut current_physical_size = 0;
        for timeline in tenant.list_timelines().iter() {
            current_physical_size += timeline.layer_size_sum();
        }

        let state = tenant.current_state();
        Result::<_, ApiError>::Ok(TenantInfo {
            id: tenant_id,
            state: state.clone(),
            current_physical_size: Some(current_physical_size),
            attachment_status: state.attachment_status(),
        })
    }
    .instrument(info_span!("tenant_status_handler", tenant = %tenant_id))
    .await?;

    json_response(StatusCode::OK, tenant_info)
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
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let inputs_only: Option<bool> = parse_query_param(&request, "inputs_only")?;
    let retention_period: Option<u64> = parse_query_param(&request, "retention_period")?;
    let headers = request.headers();

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let tenant = mgr::get_tenant(tenant_id, true).await?;

    // this can be long operation
    let inputs = tenant
        .gather_size_inputs(
            retention_period,
            LogicalSizeCalculationCause::TenantSizeHandler,
            &ctx,
        )
        .await
        .map_err(ApiError::InternalServerError)?;

    let mut sizes = None;
    if !inputs_only.unwrap_or(false) {
        let storage_model = inputs
            .calculate_model()
            .map_err(ApiError::InternalServerError)?;
        let size = storage_model.calculate();

        // If request header expects html, return html
        if headers["Accept"] == "text/html" {
            return synthetic_size_html_response(inputs, storage_model, size);
        }
        sizes = Some(size);
    } else if headers["Accept"] == "text/html" {
        return Err(ApiError::BadRequest(anyhow!(
            "inputs_only parameter is incompatible with html output request"
        )));
    }

    /// The type resides in the pageserver not to expose `ModelInputs`.
    #[serde_with::serde_as]
    #[derive(serde::Serialize)]
    struct TenantHistorySize {
        #[serde_as(as = "serde_with::DisplayFromStr")]
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
            id: tenant_id,
            size: sizes.as_ref().map(|x| x.total_size),
            segment_sizes: sizes.map(|x| x.segments),
            inputs,
        },
    )
}

async fn layer_map_info_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let reset: LayerAccessStatsReset =
        parse_query_param(&request, "reset")?.unwrap_or(LayerAccessStatsReset::NoReset);

    check_permission(&request, Some(tenant_id))?;

    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    let layer_map_info = timeline.layer_map_info(reset);

    json_response(StatusCode::OK, layer_map_info)
}

async fn layer_download_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let layer_file_name = get_request_param(&request, "layer_file_name")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    let downloaded = timeline
        .download_layer(layer_file_name)
        .await
        .map_err(ApiError::InternalServerError)?;

    match downloaded {
        Some(true) => json_response(StatusCode::OK, ()),
        Some(false) => json_response(StatusCode::NOT_MODIFIED, ()),
        None => json_response(
            StatusCode::BAD_REQUEST,
            format!("Layer {tenant_id}/{timeline_id}/{layer_file_name} not found"),
        ),
    }
}

async fn evict_timeline_layer_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let layer_file_name = get_request_param(&request, "layer_file_name")?;

    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    let evicted = timeline
        .evict_layer(layer_file_name)
        .await
        .map_err(ApiError::InternalServerError)?;

    match evicted {
        Some(true) => json_response(StatusCode::OK, ()),
        Some(false) => json_response(StatusCode::NOT_MODIFIED, ()),
        None => json_response(
            StatusCode::BAD_REQUEST,
            format!("Layer {tenant_id}/{timeline_id}/{layer_file_name} not found"),
        ),
    }
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
    let seg_to_branch: Vec<usize> = inputs
        .segments
        .iter()
        .map(|seg| *timeline_map.get(&seg.timeline_id).unwrap())
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
        .header(hyper::header::CONTENT_TYPE, "text/html")
        .body(Body::from(data.as_bytes().to_vec()))
        .map_err(|e| ApiError::InternalServerError(e.into()))?;
    Ok(response)
}

async fn tenant_create_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let request_data: TenantCreateRequest = json_request(&mut request).await?;
    let target_tenant_id = request_data.new_tenant_id;
    check_permission(&request, None)?;

    let _timer = STORAGE_TIME_GLOBAL
        .get_metric_with_label_values(&[StorageTimeOperation::CreateTenant.into()])
        .expect("bug")
        .start_timer();

    let tenant_conf =
        TenantConfOpt::try_from(&request_data.config).map_err(ApiError::BadRequest)?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Warn);

    let state = get_state(&request);

    let new_tenant = mgr::create_tenant(
        state.conf,
        tenant_conf,
        target_tenant_id,
        state.broker_client.clone(),
        state.remote_storage.clone(),
        &ctx,
    )
    .instrument(info_span!("tenant_create", tenant = ?target_tenant_id))
    .await?;

    // We created the tenant. Existing API semantics are that the tenant
    // is Active when this function returns.
    if let res @ Err(_) = new_tenant.wait_to_become_active().await {
        // This shouldn't happen because we just created the tenant directory
        // in tenant::mgr::create_tenant, and there aren't any remote timelines
        // to load, so, nothing can really fail during load.
        // Don't do cleanup because we don't know how we got here.
        // The tenant will likely be in `Broken` state and subsequent
        // calls will fail.
        res.context("created tenant failed to become active")
            .map_err(ApiError::InternalServerError)?;
    }

    json_response(
        StatusCode::CREATED,
        TenantCreateResponse(new_tenant.tenant_id()),
    )
}

async fn get_tenant_config_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let tenant = mgr::get_tenant(tenant_id, false).await?;

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

    let tenant_conf =
        TenantConfOpt::try_from(&request_data.config).map_err(ApiError::BadRequest)?;

    let state = get_state(&request);
    mgr::set_new_tenant_config(state.conf, tenant_conf, tenant_id)
        .instrument(info_span!("tenant_config", tenant = ?tenant_id))
        .await?;

    json_response(StatusCode::OK, ())
}

/// Testing helper to transition a tenant to [`crate::tenant::TenantState::Broken`].
async fn handle_tenant_break(
    r: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&r, "tenant_id")?;

    let tenant = crate::tenant::mgr::get_tenant(tenant_id, true)
        .await
        .map_err(|_| ApiError::Conflict(String::from("no active tenant found")))?;

    tenant.set_broken("broken from test".to_owned()).await;

    json_response(StatusCode::OK, ())
}

async fn failpoints_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    if !fail::has_failpoints() {
        return Err(ApiError::BadRequest(anyhow!(
            "Cannot manage failpoints because pageserver was compiled without failpoints support"
        )));
    }

    let failpoints: ConfigureFailpointsRequest = json_request(&mut request).await?;
    for fp in failpoints {
        info!("cfg failpoint: {} {}", fp.name, fp.actions);

        // We recognize one extra "action" that's not natively recognized
        // by the failpoints crate: exit, to immediately kill the process
        let cfg_result = if fp.actions == "exit" {
            fail::cfg_callback(fp.name, || {
                info!("Exit requested by failpoint");
                std::process::exit(1);
            })
        } else {
            fail::cfg(fp.name, &fp.actions)
        };

        if let Err(err_msg) = cfg_result {
            return Err(ApiError::BadRequest(anyhow!(
                "Failed to configure failpoints: {err_msg}"
            )));
        }
    }

    json_response(StatusCode::OK, ())
}

// Run GC immediately on given timeline.
async fn timeline_gc_handler(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let gc_req: TimelineGcRequest = json_request(&mut request).await?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let wait_task_done = mgr::immediate_gc(tenant_id, timeline_id, gc_req, &ctx).await?;
    let gc_result = wait_task_done
        .await
        .context("wait for gc task")
        .map_err(ApiError::InternalServerError)?
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, gc_result)
}

// Run compaction immediately on given timeline.
async fn timeline_compact_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
    let result_receiver = mgr::immediate_compact(tenant_id, timeline_id, &ctx)
        .await
        .context("spawn compaction task")
        .map_err(ApiError::InternalServerError)?;

    let result: anyhow::Result<()> = result_receiver
        .await
        .context("receive compaction result")
        .map_err(ApiError::InternalServerError)?;
    result.map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

// Run checkpoint immediately on given timeline.
async fn timeline_checkpoint_handler(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;
    async {
        let ctx = RequestContext::new(TaskKind::MgmtRequest, DownloadBehavior::Download);
        let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
        timeline
            .freeze_and_flush()
            .await
            .map_err(ApiError::InternalServerError)?;
        timeline
            .compact(&ctx)
            .await
            .map_err(ApiError::InternalServerError)?;

        json_response(StatusCode::OK, ())
    }
    .instrument(info_span!("manual_checkpoint", tenant_id = %tenant_id, timeline_id = %timeline_id))
    .await
}

async fn timeline_download_remote_layers_handler_post(
    mut request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let body: DownloadRemoteLayersTaskSpawnRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_id))?;

    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    match timeline.spawn_download_all_remote_layers(body).await {
        Ok(st) => json_response(StatusCode::ACCEPTED, st),
        Err(st) => json_response(StatusCode::CONFLICT, st),
    }
}

async fn timeline_download_remote_layers_handler_get(
    request: Request<Body>,
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;

    let timeline = active_timeline_of_active_tenant(tenant_id, timeline_id).await?;
    let info = timeline
        .get_download_all_remote_layers_task_info()
        .context("task never started since last pageserver process start")
        .map_err(ApiError::NotFound)?;
    json_response(StatusCode::OK, info)
}

async fn active_timeline_of_active_tenant(
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Result<Arc<Timeline>, ApiError> {
    let tenant = mgr::get_tenant(tenant_id, true).await?;
    tenant
        .get_timeline(timeline_id, true)
        .map_err(ApiError::NotFound)
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
    _cancel: CancellationToken,
) -> Result<Response<Body>, ApiError> {
    check_permission(&r, None)?;

    #[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
    struct Config {
        /// How many bytes to evict before reporting that pressure is relieved.
        evict_bytes: u64,
    }

    #[derive(Debug, Clone, Copy, serde::Serialize)]
    struct Usage {
        // remains unchanged after instantiation of the struct
        config: Config,
        // updated by `add_available_bytes`
        freed_bytes: u64,
    }

    impl crate::disk_usage_eviction_task::Usage for Usage {
        fn has_pressure(&self) -> bool {
            self.config.evict_bytes > self.freed_bytes
        }

        fn add_available_bytes(&mut self, bytes: u64) {
            self.freed_bytes += bytes;
        }
    }

    let config = json_request::<Config>(&mut r)
        .await
        .map_err(|_| ApiError::BadRequest(anyhow::anyhow!("invalid JSON body")))?;

    let usage = Usage {
        config,
        freed_bytes: 0,
    };

    use crate::task_mgr::MGMT_REQUEST_RUNTIME;

    let (tx, rx) = tokio::sync::oneshot::channel();

    let state = get_state(&r);

    let Some(storage) = state.remote_storage.clone() else {
        return Err(ApiError::InternalServerError(anyhow::anyhow!(
            "remote storage not configured, cannot run eviction iteration"
        )))
    };

    let state = state.disk_usage_eviction_state.clone();

    let cancel = CancellationToken::new();
    let child_cancel = cancel.clone();
    let _g = cancel.drop_guard();

    crate::task_mgr::spawn(
        MGMT_REQUEST_RUNTIME.handle(),
        TaskKind::DiskUsageEviction,
        None,
        None,
        "ondemand disk usage eviction",
        false,
        async move {
            let res = crate::disk_usage_eviction_task::disk_usage_eviction_task_iteration_impl(
                &state,
                &storage,
                usage,
                &child_cancel,
            )
            .await;

            info!(?res, "disk_usage_eviction_task_iteration_impl finished");

            let _ = tx.send(res);
            Ok(())
        }
        .in_current_span(),
    );

    let response = rx.await.unwrap().map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, response)
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
                    info!("Cancelled request finished");
                }
                result
            }
            .in_current_span(),
        );

        match handle.await {
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
    conf: &'static PageServerConf,
    launch_ts: &'static LaunchTimestamp,
    auth: Option<Arc<JwtAuth>>,
    broker_client: BrokerClientChannel,
    remote_storage: Option<GenericRemoteStorage>,
    disk_usage_eviction_state: Arc<disk_usage_eviction_task::State>,
) -> anyhow::Result<RouterBuilder<hyper::Body, ApiError>> {
    let spec = include_bytes!("openapi_spec.yml");
    let mut router = attach_openapi_ui(endpoint::make_router(), spec, "/swagger.yml", "/v1/doc");
    if auth.is_some() {
        router = router.middleware(auth_middleware(|request| {
            let state = get_state(request);
            if state.allowlist_routes.contains(request.uri()) {
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
        .data(Arc::new(
            State::new(
                conf,
                auth,
                remote_storage,
                broker_client,
                disk_usage_eviction_state,
            )
            .context("Failed to initialize router state")?,
        ))
        .get("/v1/status", |r| api_handler(r, status_handler))
        .put("/v1/failpoints", |r| {
            testing_api_handler("manage failpoints", r, failpoints_handler)
        })
        .get("/v1/tenant", |r| api_handler(r, tenant_list_handler))
        .post("/v1/tenant", |r| api_handler(r, tenant_create_handler))
        .get("/v1/tenant/:tenant_id", |r| api_handler(r, tenant_status))
        .get("/v1/tenant/:tenant_id/synthetic_size", |r| {
            api_handler(r, tenant_size_handler)
        })
        .put("/v1/tenant/config", |r| {
            api_handler(r, update_tenant_config_handler)
        })
        .get("/v1/tenant/:tenant_id/config", |r| {
            api_handler(r, get_tenant_config_handler)
        })
        .get("/v1/tenant/:tenant_id/timeline", |r| {
            api_handler(r, timeline_list_handler)
        })
        .post("/v1/tenant/:tenant_id/timeline", |r| {
            api_handler(r, timeline_create_handler)
        })
        .post("/v1/tenant/:tenant_id/attach", |r| {
            api_handler(r, tenant_attach_handler)
        })
        .post("/v1/tenant/:tenant_id/detach", |r| {
            api_handler(r, tenant_detach_handler)
        })
        .post("/v1/tenant/:tenant_id/load", |r| {
            api_handler(r, tenant_load_handler)
        })
        .post("/v1/tenant/:tenant_id/ignore", |r| {
            api_handler(r, tenant_ignore_handler)
        })
        .get("/v1/tenant/:tenant_id/timeline/:timeline_id", |r| {
            api_handler(r, timeline_detail_handler)
        })
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/get_lsn_by_timestamp",
            |r| api_handler(r, get_lsn_by_timestamp_handler),
        )
        .put("/v1/tenant/:tenant_id/timeline/:timeline_id/do_gc", |r| {
            api_handler(r, timeline_gc_handler)
        })
        .put("/v1/tenant/:tenant_id/timeline/:timeline_id/compact", |r| {
            testing_api_handler("run timeline compaction", r, timeline_compact_handler)
        })
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/checkpoint",
            |r| testing_api_handler("run timeline checkpoint", r, timeline_checkpoint_handler),
        )
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/download_remote_layers",
            |r| api_handler(r, timeline_download_remote_layers_handler_post),
        )
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/download_remote_layers",
            |r| api_handler(r, timeline_download_remote_layers_handler_get),
        )
        .delete("/v1/tenant/:tenant_id/timeline/:timeline_id", |r| {
            api_handler(r, timeline_delete_handler)
        })
        .get("/v1/tenant/:tenant_id/timeline/:timeline_id/layer", |r| {
            api_handler(r, layer_map_info_handler)
        })
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/layer/:layer_file_name",
            |r| api_handler(r, layer_download_handler),
        )
        .delete(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/layer/:layer_file_name",
            |r| api_handler(r, evict_timeline_layer_handler),
        )
        .put("/v1/disk_usage_eviction/run", |r| {
            api_handler(r, disk_usage_eviction_run)
        })
        .put("/v1/tenant/:tenant_id/break", |r| {
            testing_api_handler("set tenant state to broken", r, handle_tenant_break)
        })
        .get("/v1/panic", |r| api_handler(r, always_panic_handler))
        .post("/v1/tracing/event", |r| {
            testing_api_handler("emit a tracing event", r, post_tracing_event_handler)
        })
        .any(handler_404))
}
