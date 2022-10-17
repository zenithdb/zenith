use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use remote_storage::GenericRemoteStorage;
use tokio::task::JoinError;
use tracing::*;

use super::models::{LocalTimelineInfo, RemoteTimelineInfo, TimelineInfo};
use super::models::{
    StatusResponse, TenantConfigRequest, TenantCreateRequest, TenantCreateResponse, TenantInfo,
    TimelineCreateRequest,
};
use crate::pgdatadir_mapping::LsnForTimestamp;
use crate::storage_sync;
use crate::storage_sync::index::{RemoteIndex, RemoteTimeline};
use crate::tenant::{TenantState, Timeline};
use crate::tenant_config::TenantConfOpt;
use crate::{config::PageServerConf, tenant_mgr};
use utils::{
    auth::JwtAuth,
    http::{
        endpoint::{self, attach_openapi_ui, auth_middleware, check_permission},
        error::{ApiError, HttpErrorBody},
        json::{json_request, json_response},
        request::parse_request_param,
        RequestExt, RouterBuilder,
    },
    id::{TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
};

// Imports only used for testing APIs
#[cfg(feature = "testing")]
use super::models::{ConfigureFailpointsRequest, TimelineGcRequest};
#[cfg(feature = "testing")]
use crate::CheckpointConfig;

struct State {
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    remote_index: RemoteIndex,
    allowlist_routes: Vec<Uri>,
    remote_storage: Option<GenericRemoteStorage>,
}

impl State {
    fn new(
        conf: &'static PageServerConf,
        auth: Option<Arc<JwtAuth>>,
        remote_index: RemoteIndex,
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
            remote_index,
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

// Helper function to construct a TimelineInfo struct for a timeline
async fn build_timeline_info(
    state: &State,
    timeline: &Arc<Timeline>,
    include_non_incremental_logical_size: bool,
    include_non_incremental_physical_size: bool,
) -> anyhow::Result<TimelineInfo> {
    let last_record_lsn = timeline.get_last_record_lsn();
    let (wal_source_connstr, last_received_msg_lsn, last_received_msg_ts) = {
        let guard = timeline.last_received_wal.lock().unwrap();
        if let Some(info) = guard.as_ref() {
            (
                Some(info.wal_source_connstr.clone()),
                Some(info.last_received_msg_lsn),
                Some(info.last_received_msg_ts),
            )
        } else {
            (None, None, None)
        }
    };

    let (remote_consistent_lsn, awaits_download) = if let Some(remote_entry) = state
        .remote_index
        .read()
        .await
        .timeline_entry(&TenantTimelineId {
            tenant_id: timeline.tenant_id,
            timeline_id: timeline.timeline_id,
        }) {
        (
            Some(remote_entry.metadata.disk_consistent_lsn()),
            remote_entry.awaits_download,
        )
    } else {
        (None, false)
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

    let info = TimelineInfo {
        tenant_id: timeline.tenant_id,
        timeline_id: timeline.timeline_id,
        ancestor_timeline_id,
        ancestor_lsn,
        disk_consistent_lsn: timeline.get_disk_consistent_lsn(),
        last_record_lsn,
        prev_record_lsn: Some(timeline.get_prev_record_lsn()),
        latest_gc_cutoff_lsn: *timeline.get_latest_gc_cutoff_lsn(),
        current_logical_size,
        current_physical_size,
        current_logical_size_non_incremental: if include_non_incremental_logical_size {
            Some(timeline.get_current_logical_size_non_incremental(last_record_lsn)?)
        } else {
            None
        },
        current_physical_size_non_incremental: if include_non_incremental_physical_size {
            Some(timeline.get_physical_size_non_incremental()?)
        } else {
            None
        },
        wal_source_connstr,
        last_received_msg_lsn,
        last_received_msg_ts,
        pg_version: timeline.pg_version,

        remote_consistent_lsn,
        awaits_download,

        // Duplicate some fields in 'local' and 'remote' fields, for backwards-compatility
        // with the control plane.
        local: LocalTimelineInfo {
            ancestor_timeline_id,
            ancestor_lsn,
            current_logical_size,
            current_physical_size,
        },
        remote: RemoteTimelineInfo {
            remote_consistent_lsn,
        },
    };
    Ok(info)
}

// healthcheck handler
async fn status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let config = get_config(&request);
    json_response(StatusCode::OK, StatusResponse { id: config.id })
}

async fn timeline_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let request_data: TimelineCreateRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);

    let tenant = tenant_mgr::get_tenant(tenant_id, true).map_err(ApiError::NotFound)?;
    let new_timeline_info = async {
        match tenant.create_timeline(
            request_data.new_timeline_id.map(TimelineId::from),
            request_data.ancestor_timeline_id.map(TimelineId::from),
            request_data.ancestor_start_lsn,
            request_data.pg_version.unwrap_or(crate::DEFAULT_PG_VERSION)
        ).await {
            Ok(Some(new_timeline)) => {
                // Created. Construct a TimelineInfo for it.
                let timeline_info = build_timeline_info(state, &new_timeline, false, false)
                    .await
                    .map_err(ApiError::InternalServerError)?;
                Ok(Some(timeline_info))
            }
            Ok(None) => Ok(None), // timeline already exists
            Err(err) => Err(ApiError::InternalServerError(err)),
        }
    }
    .instrument(info_span!("timeline_create", tenant = %tenant_id, new_timeline = ?request_data.new_timeline_id, lsn=?request_data.ancestor_start_lsn, pg_version=?request_data.pg_version))
        .await?;

    Ok(match new_timeline_info {
        Some(info) => json_response(StatusCode::CREATED, info)?,
        None => json_response(StatusCode::CONFLICT, ())?,
    })
}

async fn timeline_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let include_non_incremental_logical_size =
        query_param_present(&request, "include-non-incremental-logical-size");
    let include_non_incremental_physical_size =
        query_param_present(&request, "include-non-incremental-physical-size");
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);

    let timelines = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("timeline_list", tenant = %tenant_id).entered();
        let tenant = tenant_mgr::get_tenant(tenant_id, true).map_err(ApiError::NotFound)?;
        Ok(tenant.list_timelines())
    })
    .await
    .map_err(|e: JoinError| ApiError::InternalServerError(e.into()))??;

    let mut response_data = Vec::with_capacity(timelines.len());
    for timeline in timelines {
        let timeline_info = build_timeline_info(
            state,
            &timeline,
            include_non_incremental_logical_size,
            include_non_incremental_physical_size,
        )
        .await
        .context("Failed to convert tenant timeline {timeline_id} into the local one: {e:?}")
        .map_err(ApiError::InternalServerError)?;

        response_data.push(timeline_info);
    }

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

    let state = get_state(&request);

    let timeline_info = async {
        let timeline = tokio::task::spawn_blocking(move || {
            tenant_mgr::get_tenant(tenant_id, true)?.get_timeline(timeline_id)
        })
        .await
        .map_err(|e: JoinError| ApiError::InternalServerError(e.into()))?;

        let timeline = timeline.map_err(ApiError::NotFound)?;

        let timeline_info = build_timeline_info(
            state,
            &timeline,
            include_non_incremental_logical_size,
            include_non_incremental_physical_size,
        )
        .await
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
        .and_then(|tenant| tenant.get_timeline(timeline_id))
        .with_context(|| format!("No timeline {timeline_id} in repository for tenant {tenant_id}"))
        .map_err(ApiError::NotFound)?;
    let result = match timeline
        .find_lsn_for_timestamp(timestamp_pg)
        .map_err(ApiError::InternalServerError)?
    {
        LsnForTimestamp::Present(lsn) => format!("{}", lsn),
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

    tokio::task::spawn_blocking(move || match tenant_mgr::get_tenant(tenant_id, false) {
        Ok(tenant) => {
            if tenant.list_timelines().is_empty() {
                info!("Attaching to tenant {tenant_id} with zero timelines");
                Ok(())
            } else {
                Err(ApiError::Conflict(
                    "Tenant is already present locally".to_owned(),
                ))
            }
        }
        Err(_) => Ok(()),
    })
    .await
    .map_err(|e: JoinError| ApiError::InternalServerError(e.into()))??;

    let state = get_state(&request);
    let remote_index = &state.remote_index;

    let mut index_accessor = remote_index.write().await;
    if let Some(tenant_entry) = index_accessor.tenant_entry_mut(&tenant_id) {
        if tenant_entry.has_in_progress_downloads() {
            return Err(ApiError::Conflict(
                "Tenant download is already in progress".to_string(),
            ));
        }

        for (timeline_id, remote_timeline) in tenant_entry.iter_mut() {
            storage_sync::schedule_layer_download(tenant_id, *timeline_id);
            remote_timeline.awaits_download = true;
        }
        return json_response(StatusCode::ACCEPTED, ());
    }
    // no tenant in the index, release the lock to make the potentially lengthy download operation
    drop(index_accessor);

    // download index parts for every tenant timeline
    let remote_timelines = match gather_tenant_timelines_index_parts(state, tenant_id).await {
        Ok(Some(remote_timelines)) => remote_timelines,
        Ok(None) => return Err(ApiError::NotFound(anyhow!("Unknown remote tenant"))),
        Err(e) => {
            error!("Failed to retrieve remote tenant data: {:?}", e);
            return Err(ApiError::NotFound(anyhow!(
                "Failed to retrieve remote tenant"
            )));
        }
    };

    // recheck that download is not in progress because
    // we've released the lock to avoid holding it during the download
    let mut index_accessor = remote_index.write().await;
    let tenant_entry = match index_accessor.tenant_entry_mut(&tenant_id) {
        Some(tenant_entry) => {
            if tenant_entry.has_in_progress_downloads() {
                return Err(ApiError::Conflict(
                    "Tenant download is already in progress".to_string(),
                ));
            }
            tenant_entry
        }
        None => index_accessor.add_tenant_entry(tenant_id),
    };

    // populate remote index with the data from index part and create directories on the local filesystem
    for (timeline_id, mut remote_timeline) in remote_timelines {
        tokio::fs::create_dir_all(state.conf.timeline_path(&timeline_id, &tenant_id))
            .await
            .context("Failed to create new timeline directory")
            .map_err(ApiError::InternalServerError)?;

        remote_timeline.awaits_download = true;
        tenant_entry.insert(timeline_id, remote_timeline);
        // schedule actual download
        storage_sync::schedule_layer_download(tenant_id, timeline_id);
    }

    json_response(StatusCode::ACCEPTED, ())
}

/// Note: is expensive from s3 access perspective,
/// for details see comment to `storage_sync::gather_tenant_timelines_index_parts`
async fn gather_tenant_timelines_index_parts(
    state: &State,
    tenant_id: TenantId,
) -> anyhow::Result<Option<Vec<(TimelineId, RemoteTimeline)>>> {
    let index_parts = match state.remote_storage.as_ref() {
        Some(storage) => {
            storage_sync::gather_tenant_timelines_index_parts(state.conf, storage, tenant_id).await
        }
        None => return Ok(None),
    }
    .with_context(|| format!("Failed to download index parts for tenant {tenant_id}"))?;

    let mut remote_timelines = Vec::with_capacity(index_parts.len());
    for (timeline_id, index_part) in index_parts {
        let timeline_path = state.conf.timeline_path(&timeline_id, &tenant_id);
        let remote_timeline = RemoteTimeline::from_index_part(&timeline_path, index_part)
            .with_context(|| {
                format!("Failed to convert index part into remote timeline for timeline {tenant_id}/{timeline_id}")
            })?;
        remote_timelines.push((timeline_id, remote_timeline));
    }
    Ok(Some(remote_timelines))
}

async fn timeline_delete_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);
    tenant_mgr::delete_timeline(tenant_id, timeline_id)
        .instrument(info_span!("timeline_delete", tenant = %tenant_id, timeline = %timeline_id))
        .await
        // FIXME: Errors from `delete_timeline` can occur for a number of reasons, incuding both
        // user and internal errors. Replace this with better handling once the error type permits
        // it.
        .map_err(ApiError::InternalServerError)?;

    let mut remote_index = state.remote_index.write().await;
    remote_index.remove_timeline_entry(TenantTimelineId {
        tenant_id,
        timeline_id,
    });

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

    let mut remote_index = state.remote_index.write().await;
    remote_index.remove_tenant_entry(&tenant_id);

    json_response(StatusCode::OK, ())
}

async fn tenant_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;

    let state = get_state(&request);
    // clone to avoid holding the lock while awaiting for blocking task
    let remote_index = state.remote_index.read().await.clone();

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_list").entered();
        crate::tenant_mgr::list_tenant_info(&remote_index)
    })
    .await
    .map_err(|e: JoinError| ApiError::InternalServerError(e.into()))?;

    json_response(StatusCode::OK, response_data)
}

async fn tenant_status(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    // if tenant is in progress of downloading it can be absent in global tenant map
    let tenant = tokio::task::spawn_blocking(move || tenant_mgr::get_tenant(tenant_id, false))
        .await
        .map_err(|e: JoinError| ApiError::InternalServerError(e.into()))?;

    let state = get_state(&request);
    let remote_index = &state.remote_index;

    let index_accessor = remote_index.read().await;
    let has_in_progress_downloads = index_accessor
        .tenant_entry(&tenant_id)
        .map(|t| t.has_in_progress_downloads())
        .unwrap_or_else(|| {
            info!("Tenant {tenant_id} not found in remote index");
            false
        });

    let (tenant_state, current_physical_size) = match tenant {
        Ok(tenant) => {
            let timelines = tenant.list_timelines();
            // Calculate total physical size of all timelines
            let mut current_physical_size = 0;
            for timeline in timelines {
                current_physical_size += timeline.get_physical_size();
            }

            (tenant.current_state(), Some(current_physical_size))
        }
        Err(e) => {
            error!("Failed to get local tenant state: {e:#}");
            if has_in_progress_downloads {
                (TenantState::Paused, None)
            } else {
                (TenantState::Broken, None)
            }
        }
    };

    json_response(
        StatusCode::OK,
        TenantInfo {
            id: tenant_id,
            state: tenant_state,
            current_physical_size,
            has_in_progress_downloads: Some(has_in_progress_downloads),
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
    let remote_index = get_state(&request).remote_index.clone();

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

    let new_tenant_id = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_create", tenant = ?target_tenant_id).entered();
        let conf = get_config(&request);

        tenant_mgr::create_tenant(conf, tenant_conf, target_tenant_id, remote_index)
            // FIXME: `create_tenant` can fail from both user and internal errors. Replace this
            // with better error handling once the type permits it
            .map_err(ApiError::InternalServerError)
    })
    .await
    .map_err(|e: JoinError| ApiError::InternalServerError(e.into()))??;

    Ok(match new_tenant_id {
        Some(id) => json_response(StatusCode::CREATED, TenantCreateResponse(id))?,
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

    tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_config", tenant = ?tenant_id).entered();

        let state = get_state(&request);
        tenant_mgr::update_tenant_config(state.conf, tenant_conf, tenant_id)
            // FIXME: `update_tenant_config` can fail because of both user and internal errors.
            // Replace this `map_err` with better error handling once the type permits it
            .map_err(ApiError::InternalServerError)
    })
    .await
    .map_err(|e: JoinError| ApiError::InternalServerError(e.into()))??;

    json_response(StatusCode::OK, ())
}

#[cfg(feature = "testing")]
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
// FIXME: This is just for tests. See test_runner/regress/test_gc.py.
// This probably should require special authentication or a global flag to
// enable, I don't think we want to or need to allow regular clients to invoke
// GC.
//     @hllinnaka in commits ec44f4b29, 3aca717f3
#[cfg(feature = "testing")]
async fn timeline_gc_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    // FIXME: currently this will return a 500 error on bad tenant id; it should be 4XX
    let repo = tenant_mgr::get_tenant(tenant_id, false).map_err(ApiError::NotFound)?;
    let gc_req: TimelineGcRequest = json_request(&mut request).await?;

    let _span_guard =
        info_span!("manual_gc", tenant = %tenant_id, timeline = %timeline_id).entered();
    let gc_horizon = gc_req.gc_horizon.unwrap_or_else(|| repo.get_gc_horizon());

    // Use tenant's pitr setting
    let pitr = repo.get_pitr_interval();
    let result = repo
        .gc_iteration(Some(timeline_id), gc_horizon, pitr, true)
        // FIXME: `gc_iteration` can return an error for multiple reasons; we should handle it
        // better once the types support it.
        .map_err(ApiError::InternalServerError)?;
    json_response(StatusCode::OK, result)
}

// Run compaction immediately on given timeline.
// FIXME This is just for tests. Don't expect this to be exposed to
// the users or the api.
//     @dhammika in commit a0781f229
#[cfg(feature = "testing")]
async fn timeline_compact_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let repo = tenant_mgr::get_tenant(tenant_id, true).map_err(ApiError::NotFound)?;
    let timeline = repo
        .get_timeline(timeline_id)
        .with_context(|| format!("No timeline {timeline_id} in repository for tenant {tenant_id}"))
        .map_err(ApiError::NotFound)?;
    timeline
        .reconstruct()
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

// Run checkpoint immediately on given timeline.
#[cfg(feature = "testing")]
async fn timeline_checkpoint_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let repo = tenant_mgr::get_tenant(tenant_id, true).map_err(ApiError::NotFound)?;
    let timeline = repo
        .get_timeline(timeline_id)
        .with_context(|| format!("No timeline {timeline_id} in repository for tenant {tenant_id}"))
        .map_err(ApiError::NotFound)?;
    timeline
        .checkpoint(CheckpointConfig::Forced)
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
    remote_index: RemoteIndex,
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

    macro_rules! testing_api {
        ($handler_desc:literal, $handler:path $(,)?) => {{
            #[cfg(not(feature = "testing"))]
            async fn cfg_disabled(_req: Request<Body>) -> Result<Response<Body>, ApiError> {
                Err(ApiError::BadRequest(anyhow!(concat!(
                    "Cannot ",
                    $handler_desc,
                    " because pageserver was compiled without testing APIs",
                ))))
            }

            #[cfg(feature = "testing")]
            let handler = $handler;
            #[cfg(not(feature = "testing"))]
            let handler = cfg_disabled;
            handler
        }};
    }

    Ok(router
        .data(Arc::new(
            State::new(conf, auth, remote_index, remote_storage)
                .context("Failed to initialize router state")?,
        ))
        .get("/v1/status", status_handler)
        .put(
            "/v1/failpoints",
            testing_api!("manage failpoints", failpoints_handler),
        )
        .get("/v1/tenant", tenant_list_handler)
        .post("/v1/tenant", tenant_create_handler)
        .get("/v1/tenant/:tenant_id", tenant_status)
        .put("/v1/tenant/config", tenant_config_handler)
        .get("/v1/tenant/:tenant_id/timeline", timeline_list_handler)
        .post("/v1/tenant/:tenant_id/timeline", timeline_create_handler)
        .post("/v1/tenant/:tenant_id/attach", tenant_attach_handler)
        .post("/v1/tenant/:tenant_id/detach", tenant_detach_handler)
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
