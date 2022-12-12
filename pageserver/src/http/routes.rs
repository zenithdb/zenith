use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use remote_storage::GenericRemoteStorage;
use tracing::*;

use super::models::{
    ConfigureFailpointsRequest, LocalTimelineInfo, RemoteTimelineInfo, StatusResponse,
    TenantConfigRequest, TenantCreateRequest, TenantCreateResponse, TenantInfo,
    TimelineCreateRequest, TimelineGcRequest, TimelineInfo,
};
use crate::pgdatadir_mapping::LsnForTimestamp;
use crate::tenant::Timeline;
use crate::tenant_config::TenantConfOpt;
use crate::CheckpointConfig;
use crate::{config::PageServerConf, tenant_mgr};
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

struct State {
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    allowlist_routes: Vec<Uri>,
    remote_storage: Option<GenericRemoteStorage>,
}

impl State {
    fn new(
        conf: &'static PageServerConf,
        auth: Option<Arc<JwtAuth>>,
        remote_storage: Option<GenericRemoteStorage>,
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

fn check_permission(request: &Request<Body>, tenant_id: Option<TenantId>) -> Result<(), ApiError> {
    check_permission_with(request, |claims| {
        crate::auth::check_permission(claims, tenant_id)
    })
}

// Helper function to construct a TimelineInfo struct for a timeline
fn build_timeline_info(
    timeline: &Arc<Timeline>,
    include_non_incremental_logical_size: bool,
    include_non_incremental_physical_size: bool,
) -> anyhow::Result<TimelineInfo> {
    let mut info = build_timeline_info_common(timeline)?;
    if include_non_incremental_logical_size {
        info.current_logical_size_non_incremental =
            Some(timeline.get_current_logical_size_non_incremental(info.last_record_lsn)?);
    }
    if include_non_incremental_physical_size {
        info.current_physical_size_non_incremental =
            Some(timeline.get_physical_size_non_incremental()?)
    }
    Ok(info)
}

fn build_timeline_info_common(timeline: &Arc<Timeline>) -> anyhow::Result<TimelineInfo> {
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
    let current_logical_size = match timeline.get_current_logical_size() {
        Ok(size) => Some(size),
        Err(err) => {
            error!("Timeline info creation failed to get current logical size: {err:?}");
            None
        }
    };
    let current_physical_size = Some(timeline.get_physical_size());
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
        current_physical_size_non_incremental: None,
        wal_source_connstr,
        last_received_msg_lsn,
        last_received_msg_ts,
        pg_version: timeline.pg_version,

        state,

        // Duplicate some fields in 'local' and 'remote' fields, for backwards-compatility
        // with the control plane.
        local: LocalTimelineInfo {
            ancestor_timeline_id,
            ancestor_lsn,
            current_logical_size,
            current_physical_size,
        },
        remote: RemoteTimelineInfo {
            remote_consistent_lsn: Some(remote_consistent_lsn),
        },
    };
    Ok(info)
}

// healthcheck handler
async fn status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;
    let config = get_config(&request);
    json_response(StatusCode::OK, StatusResponse { id: config.id })
}

async fn timeline_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let request_data: TimelineCreateRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_id))?;

    let new_timeline_id = request_data
        .new_timeline_id
        .unwrap_or_else(TimelineId::generate);

    let tenant = tenant_mgr::get_tenant(tenant_id, true)
        .await
        .map_err(ApiError::NotFound)?;
    match tenant.create_timeline(
        new_timeline_id,
        request_data.ancestor_timeline_id.map(TimelineId::from),
        request_data.ancestor_start_lsn,
        request_data.pg_version.unwrap_or(crate::DEFAULT_PG_VERSION)
    )
    .instrument(info_span!("timeline_create", tenant = %tenant_id, new_timeline = ?request_data.new_timeline_id, timeline_id = %new_timeline_id, lsn=?request_data.ancestor_start_lsn, pg_version=?request_data.pg_version))
    .await {
        Ok(Some(new_timeline)) => {
            // Created. Construct a TimelineInfo for it.
            let timeline_info = build_timeline_info_common(&new_timeline)
                .map_err(ApiError::InternalServerError)?;
            json_response(StatusCode::CREATED, timeline_info)
        }
        Ok(None) => json_response(StatusCode::CONFLICT, ()), // timeline already exists
        Err(err) => Err(ApiError::InternalServerError(err)),
    }
}

async fn timeline_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let include_non_incremental_logical_size =
        query_param_present(&request, "include-non-incremental-logical-size");
    let include_non_incremental_physical_size =
        query_param_present(&request, "include-non-incremental-physical-size");
    check_permission(&request, Some(tenant_id))?;

    let response_data = async {
        let tenant = tenant_mgr::get_tenant(tenant_id, true)
            .await
            .map_err(ApiError::NotFound)?;
        let timelines = tenant.list_timelines();

        let mut response_data = Vec::with_capacity(timelines.len());
        for timeline in timelines {
            let timeline_info = build_timeline_info(
                &timeline,
                include_non_incremental_logical_size,
                include_non_incremental_physical_size,
            )
            .context("Failed to convert tenant timeline {timeline_id} into the local one: {e:?}")
            .map_err(ApiError::InternalServerError)?;

            response_data.push(timeline_info);
        }

        Ok(response_data)
    }
    .instrument(info_span!("timeline_list", tenant = %tenant_id))
    .await?;

    json_response(StatusCode::OK, response_data)
}

/// Checks if a query param is present in the request's URL
fn query_param_present(request: &Request<Body>, param: &str) -> bool {
    request
        .uri()
        .query()
        .map(|v| {
            url::form_urlencoded::parse(v.as_bytes())
                .into_owned()
                .any(|(p, _)| p == param)
        })
        .unwrap_or(false)
}

fn get_query_param(request: &Request<Body>, param_name: &str) -> Result<String, ApiError> {
    request.uri().query().map_or(
        Err(ApiError::BadRequest(anyhow!("empty query in request"))),
        |v| {
            url::form_urlencoded::parse(v.as_bytes())
                .into_owned()
                .find(|(k, _)| k == param_name)
                .map_or(
                    Err(ApiError::BadRequest(anyhow!(
                        "no {param_name} specified in query parameters"
                    ))),
                    |(_, v)| Ok(v),
                )
        },
    )
}

async fn timeline_detail_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let include_non_incremental_logical_size =
        query_param_present(&request, "include-non-incremental-logical-size");
    let include_non_incremental_physical_size =
        query_param_present(&request, "include-non-incremental-physical-size");
    check_permission(&request, Some(tenant_id))?;

    let timeline_info = async {
        let tenant = tenant_mgr::get_tenant(tenant_id, true)
            .await
            .map_err(ApiError::NotFound)?;

        let timeline = tenant
            .get_timeline(timeline_id, false)
            .map_err(ApiError::NotFound)?;

        let timeline_info = build_timeline_info(
            &timeline,
            include_non_incremental_logical_size,
            include_non_incremental_physical_size,
        )
        .context("Failed to get local timeline info: {e:#}")
        .map_err(ApiError::InternalServerError)?;

        Ok::<_, ApiError>(timeline_info)
    }
    .instrument(info_span!("timeline_detail", tenant = %tenant_id, timeline = %timeline_id))
    .await?;

    json_response(StatusCode::OK, timeline_info)
}

async fn get_lsn_by_timestamp_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    let timestamp_raw = get_query_param(&request, "timestamp")?;
    let timestamp = humantime::parse_rfc3339(timestamp_raw.as_str())
        .with_context(|| format!("Invalid time: {:?}", timestamp_raw))
        .map_err(ApiError::BadRequest)?;
    let timestamp_pg = postgres_ffi::to_pg_timestamp(timestamp);

    let timeline = tenant_mgr::get_tenant(tenant_id, true)
        .await
        .and_then(|tenant| tenant.get_timeline(timeline_id, true))
        .map_err(ApiError::NotFound)?;
    let result = match timeline
        .find_lsn_for_timestamp(timestamp_pg)
        .map_err(ApiError::InternalServerError)?
    {
        LsnForTimestamp::Present(lsn) => format!("{lsn}"),
        LsnForTimestamp::Future(_lsn) => "future".into(),
        LsnForTimestamp::Past(_lsn) => "past".into(),
        LsnForTimestamp::NoData(_lsn) => "nodata".into(),
    };
    json_response(StatusCode::OK, result)
}

// TODO makes sense to provide tenant config right away the same way as it handled in tenant_create
async fn tenant_attach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    info!("Handling tenant attach {tenant_id}");

    let state = get_state(&request);

    if let Some(remote_storage) = &state.remote_storage {
        // FIXME: distinguish between "Tenant already exists" and other errors
        tenant_mgr::attach_tenant(state.conf, tenant_id, remote_storage.clone())
            .instrument(info_span!("tenant_attach", tenant = %tenant_id))
            .await
            .map_err(ApiError::InternalServerError)?;
    } else {
        return Err(ApiError::BadRequest(anyhow!(
            "attach_tenant is not possible because pageserver was configured without remote storage"
        )));
    }

    json_response(StatusCode::ACCEPTED, ())
}

async fn timeline_delete_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    tenant_mgr::delete_timeline(tenant_id, timeline_id)
        .instrument(info_span!("timeline_delete", tenant = %tenant_id, timeline = %timeline_id))
        .await
        // FIXME: Errors from `delete_timeline` can occur for a number of reasons, incuding both
        // user and internal errors. Replace this with better handling once the error type permits
        // it.
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

async fn tenant_detach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);
    let conf = state.conf;
    tenant_mgr::detach_tenant(conf, tenant_id)
        .instrument(info_span!("tenant_detach", tenant = %tenant_id))
        .await
        // FIXME: Errors from `detach_tenant` can be caused by both both user and internal errors.
        // Replace this with better handling once the error type permits it.
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

async fn tenant_load_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);
    tenant_mgr::load_tenant(state.conf, tenant_id, state.remote_storage.clone())
        .instrument(info_span!("load", tenant = %tenant_id))
        .await
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::ACCEPTED, ())
}

async fn tenant_ignore_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);
    let conf = state.conf;
    tenant_mgr::ignore_tenant(conf, tenant_id)
        .instrument(info_span!("ignore_tenant", tenant = %tenant_id))
        .await
        // FIXME: Errors from `ignore_tenant` can be caused by both both user and internal errors.
        // Replace this with better handling once the error type permits it.
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

async fn tenant_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;

    let response_data = tenant_mgr::list_tenants()
        .instrument(info_span!("tenant_list"))
        .await
        .iter()
        .map(|(id, state)| TenantInfo {
            id: *id,
            state: *state,
            current_physical_size: None,
            has_in_progress_downloads: Some(state.has_in_progress_downloads()),
        })
        .collect::<Vec<TenantInfo>>();

    json_response(StatusCode::OK, response_data)
}

async fn tenant_status(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let tenant_info = async {
        let tenant = tenant_mgr::get_tenant(tenant_id, false).await?;

        // Calculate total physical size of all timelines
        let mut current_physical_size = 0;
        for timeline in tenant.list_timelines().iter() {
            current_physical_size += timeline.get_physical_size();
        }

        let state = tenant.current_state();
        Ok(TenantInfo {
            id: tenant_id,
            state,
            current_physical_size: Some(current_physical_size),
            has_in_progress_downloads: Some(state.has_in_progress_downloads()),
        })
    }
    .instrument(info_span!("tenant_status_handler", tenant = %tenant_id))
    .await
    .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, tenant_info)
}

async fn tenant_size_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let tenant = tenant_mgr::get_tenant(tenant_id, true)
        .await
        .map_err(ApiError::InternalServerError)?;

    // this can be long operation, it currently is not backed by any request coalescing or similar
    let inputs = tenant
        .gather_size_inputs()
        .await
        .map_err(ApiError::InternalServerError)?;

    let size = inputs.calculate().map_err(ApiError::InternalServerError)?;

    /// Private response type with the additional "unstable" `inputs` field.
    ///
    /// The type is described with `id` and `size` in the openapi_spec file, but the `inputs` is
    /// intentionally left out. The type resides in the pageserver not to expose `ModelInputs`.
    #[serde_with::serde_as]
    #[derive(serde::Serialize)]
    struct TenantHistorySize {
        #[serde_as(as = "serde_with::DisplayFromStr")]
        id: TenantId,
        /// Size is a mixture of WAL and logical size, so the unit is bytes.
        size: u64,
        inputs: crate::tenant::size::ModelInputs,
    }

    json_response(
        StatusCode::OK,
        TenantHistorySize {
            id: tenant_id,
            size,
            inputs,
        },
    )
}

// Helper function to standardize the error messages we produce on bad durations
//
// Intended to be used with anyhow's `with_context`, e.g.:
//
//   let value = result.with_context(bad_duration("name", &value))?;
//
fn bad_duration<'a>(field_name: &'static str, value: &'a str) -> impl 'a + Fn() -> String {
    move || format!("Cannot parse `{field_name}` duration {value:?}")
}

async fn tenant_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;

    let request_data: TenantCreateRequest = json_request(&mut request).await?;

    let mut tenant_conf = TenantConfOpt::default();
    if let Some(gc_period) = request_data.gc_period {
        tenant_conf.gc_period = Some(
            humantime::parse_duration(&gc_period)
                .with_context(bad_duration("gc_period", &gc_period))
                .map_err(ApiError::BadRequest)?,
        );
    }
    tenant_conf.gc_horizon = request_data.gc_horizon;
    tenant_conf.image_creation_threshold = request_data.image_creation_threshold;

    if let Some(pitr_interval) = request_data.pitr_interval {
        tenant_conf.pitr_interval = Some(
            humantime::parse_duration(&pitr_interval)
                .with_context(bad_duration("pitr_interval", &pitr_interval))
                .map_err(ApiError::BadRequest)?,
        );
    }

    if let Some(walreceiver_connect_timeout) = request_data.walreceiver_connect_timeout {
        tenant_conf.walreceiver_connect_timeout = Some(
            humantime::parse_duration(&walreceiver_connect_timeout)
                .with_context(bad_duration(
                    "walreceiver_connect_timeout",
                    &walreceiver_connect_timeout,
                ))
                .map_err(ApiError::BadRequest)?,
        );
    }
    if let Some(lagging_wal_timeout) = request_data.lagging_wal_timeout {
        tenant_conf.lagging_wal_timeout = Some(
            humantime::parse_duration(&lagging_wal_timeout)
                .with_context(bad_duration("lagging_wal_timeout", &lagging_wal_timeout))
                .map_err(ApiError::BadRequest)?,
        );
    }
    if let Some(max_lsn_wal_lag) = request_data.max_lsn_wal_lag {
        tenant_conf.max_lsn_wal_lag = Some(max_lsn_wal_lag);
    }
    if let Some(trace_read_requests) = request_data.trace_read_requests {
        tenant_conf.trace_read_requests = Some(trace_read_requests);
    }

    tenant_conf.checkpoint_distance = request_data.checkpoint_distance;
    if let Some(checkpoint_timeout) = request_data.checkpoint_timeout {
        tenant_conf.checkpoint_timeout = Some(
            humantime::parse_duration(&checkpoint_timeout)
                .with_context(bad_duration("checkpoint_timeout", &checkpoint_timeout))
                .map_err(ApiError::BadRequest)?,
        );
    }

    tenant_conf.compaction_target_size = request_data.compaction_target_size;
    tenant_conf.compaction_threshold = request_data.compaction_threshold;

    if let Some(compaction_period) = request_data.compaction_period {
        tenant_conf.compaction_period = Some(
            humantime::parse_duration(&compaction_period)
                .with_context(bad_duration("compaction_period", &compaction_period))
                .map_err(ApiError::BadRequest)?,
        );
    }

    let target_tenant_id = request_data
        .new_tenant_id
        .map(TenantId::from)
        .unwrap_or_else(TenantId::generate);

    let state = get_state(&request);

    let new_tenant = tenant_mgr::create_tenant(
        state.conf,
        tenant_conf,
        target_tenant_id,
        state.remote_storage.clone(),
    )
    .instrument(info_span!("tenant_create", tenant = ?target_tenant_id))
    .await
    // FIXME: `create_tenant` can fail from both user and internal errors. Replace this
    // with better error handling once the type permits it
    .map_err(ApiError::InternalServerError)?;

    Ok(match new_tenant {
        Some(tenant) => {
            // We created the tenant. Existing API semantics are that the tenant
            // is Active when this function returns.
            if let res @ Err(_) = tenant.wait_to_become_active().await {
                // This shouldn't happen because we just created the tenant directory
                // in tenant_mgr::create_tenant, and there aren't any remote timelines
                // to load, so, nothing can really fail during load.
                // Don't do cleanup because we don't know how we got here.
                // The tenant will likely be in `Broken` state and subsequent
                // calls will fail.
                res.context("created tenant failed to become active")
                    .map_err(ApiError::InternalServerError)?;
            }
            json_response(
                StatusCode::CREATED,
                TenantCreateResponse(tenant.tenant_id()),
            )?
        }
        None => json_response(StatusCode::CONFLICT, ())?,
    })
}

async fn tenant_config_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let request_data: TenantConfigRequest = json_request(&mut request).await?;
    let tenant_id = request_data.tenant_id;
    check_permission(&request, Some(tenant_id))?;

    let mut tenant_conf: TenantConfOpt = Default::default();
    if let Some(gc_period) = request_data.gc_period {
        tenant_conf.gc_period = Some(
            humantime::parse_duration(&gc_period)
                .with_context(bad_duration("gc_period", &gc_period))
                .map_err(ApiError::BadRequest)?,
        );
    }
    tenant_conf.gc_horizon = request_data.gc_horizon;
    tenant_conf.image_creation_threshold = request_data.image_creation_threshold;

    if let Some(pitr_interval) = request_data.pitr_interval {
        tenant_conf.pitr_interval = Some(
            humantime::parse_duration(&pitr_interval)
                .with_context(bad_duration("pitr_interval", &pitr_interval))
                .map_err(ApiError::BadRequest)?,
        );
    }
    if let Some(walreceiver_connect_timeout) = request_data.walreceiver_connect_timeout {
        tenant_conf.walreceiver_connect_timeout = Some(
            humantime::parse_duration(&walreceiver_connect_timeout)
                .with_context(bad_duration(
                    "walreceiver_connect_timeout",
                    &walreceiver_connect_timeout,
                ))
                .map_err(ApiError::BadRequest)?,
        );
    }
    if let Some(lagging_wal_timeout) = request_data.lagging_wal_timeout {
        tenant_conf.lagging_wal_timeout = Some(
            humantime::parse_duration(&lagging_wal_timeout)
                .with_context(bad_duration("lagging_wal_timeout", &lagging_wal_timeout))
                .map_err(ApiError::BadRequest)?,
        );
    }
    if let Some(max_lsn_wal_lag) = request_data.max_lsn_wal_lag {
        tenant_conf.max_lsn_wal_lag = Some(max_lsn_wal_lag);
    }
    if let Some(trace_read_requests) = request_data.trace_read_requests {
        tenant_conf.trace_read_requests = Some(trace_read_requests);
    }

    tenant_conf.checkpoint_distance = request_data.checkpoint_distance;
    if let Some(checkpoint_timeout) = request_data.checkpoint_timeout {
        tenant_conf.checkpoint_timeout = Some(
            humantime::parse_duration(&checkpoint_timeout)
                .with_context(bad_duration("checkpoint_timeout", &checkpoint_timeout))
                .map_err(ApiError::BadRequest)?,
        );
    }
    tenant_conf.compaction_target_size = request_data.compaction_target_size;
    tenant_conf.compaction_threshold = request_data.compaction_threshold;

    if let Some(compaction_period) = request_data.compaction_period {
        tenant_conf.compaction_period = Some(
            humantime::parse_duration(&compaction_period)
                .with_context(bad_duration("compaction_period", &compaction_period))
                .map_err(ApiError::BadRequest)?,
        );
    }

    let state = get_state(&request);
    tenant_mgr::update_tenant_config(state.conf, tenant_conf, tenant_id)
        .instrument(info_span!("tenant_config", tenant = ?tenant_id))
        .await
        // FIXME: `update_tenant_config` can fail because of both user and internal errors.
        // Replace this `map_err` with better error handling once the type permits it
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

async fn failpoints_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
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
async fn timeline_gc_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let gc_req: TimelineGcRequest = json_request(&mut request).await?;

    let wait_task_done = tenant_mgr::immediate_gc(tenant_id, timeline_id, gc_req).await?;
    let gc_result = wait_task_done
        .await
        .context("wait for gc task")
        .map_err(ApiError::InternalServerError)?
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, gc_result)
}

// Run compaction immediately on given timeline.
async fn timeline_compact_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let tenant = tenant_mgr::get_tenant(tenant_id, true)
        .await
        .map_err(ApiError::NotFound)?;
    let timeline = tenant
        .get_timeline(timeline_id, true)
        .map_err(ApiError::NotFound)?;
    timeline
        .compact()
        .await
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

// Run checkpoint immediately on given timeline.
async fn timeline_checkpoint_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let tenant = tenant_mgr::get_tenant(tenant_id, true)
        .await
        .map_err(ApiError::NotFound)?;
    let timeline = tenant
        .get_timeline(timeline_id, true)
        .map_err(ApiError::NotFound)?;
    timeline
        .checkpoint(CheckpointConfig::Forced)
        .await
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

async fn handler_404(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(
        StatusCode::NOT_FOUND,
        HttpErrorBody::from_msg("page not found".to_owned()),
    )
}

pub fn make_router(
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    remote_storage: Option<GenericRemoteStorage>,
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

    // A wrapper around a handler function that returns an error if the server
    // was not configured with testing_mode enabled. This is used to gate API
    // functions that should only be used in tests, never in production.
    macro_rules! testing_api {
        ($handler_desc:literal, $handler:path $(,)?) => {{
            use futures::FutureExt;
            |req: Request<Body>| {
                if conf.testing_mode {
                    $handler(req).left_future()
                } else {
                    async {
                        Err(ApiError::BadRequest(anyhow!(concat!(
                            "Cannot ",
                            $handler_desc,
                            " because pageserver was configured without testing APIs",
                        ))))
                    }
                    .right_future()
                }
            }
        }};
    }

    Ok(router
        .data(Arc::new(
            State::new(conf, auth, remote_storage).context("Failed to initialize router state")?,
        ))
        .get("/v1/status", status_handler)
        .put(
            "/v1/failpoints",
            testing_api!("manage failpoints", failpoints_handler),
        )
        .get("/v1/tenant", tenant_list_handler)
        .post("/v1/tenant", tenant_create_handler)
        .get("/v1/tenant/:tenant_id", tenant_status)
        .get("/v1/tenant/:tenant_id/size", tenant_size_handler)
        .put("/v1/tenant/config", tenant_config_handler)
        .get("/v1/tenant/:tenant_id/timeline", timeline_list_handler)
        .post("/v1/tenant/:tenant_id/timeline", timeline_create_handler)
        .post("/v1/tenant/:tenant_id/attach", tenant_attach_handler)
        .post("/v1/tenant/:tenant_id/detach", tenant_detach_handler)
        .post("/v1/tenant/:tenant_id/load", tenant_load_handler)
        .post("/v1/tenant/:tenant_id/ignore", tenant_ignore_handler)
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id",
            timeline_detail_handler,
        )
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/get_lsn_by_timestamp",
            get_lsn_by_timestamp_handler,
        )
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/do_gc",
            testing_api!("run timeline GC", timeline_gc_handler),
        )
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/compact",
            testing_api!("run timeline compaction", timeline_compact_handler),
        )
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/checkpoint",
            testing_api!("run timeline checkpoint", timeline_checkpoint_handler),
        )
        .delete(
            "/v1/tenant/:tenant_id/timeline/:timeline_id",
            timeline_delete_handler,
        )
        .any(handler_404))
}
