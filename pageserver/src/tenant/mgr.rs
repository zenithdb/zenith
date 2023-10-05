//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use camino::{Utf8Path, Utf8PathBuf};
use rand::{distributions::Alphanumeric, Rng};
use std::collections::{hash_map, HashMap};
use std::sync::Arc;
use tokio::fs;

use anyhow::Context;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::*;

use remote_storage::GenericRemoteStorage;
use utils::crashsafe;

use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::control_plane_client::{
    ControlPlaneClient, ControlPlaneGenerationsApi, RetryForeverError,
};
use crate::deletion_queue::DeletionQueueClient;
use crate::task_mgr::{self, TaskKind};
use crate::tenant::config::{LocationConf, LocationMode, TenantConfOpt};
use crate::tenant::delete::DeleteTenantFlow;
use crate::tenant::{
    create_tenant_files, AttachedTenantConf, CreateTenantFilesMode, Tenant, TenantState,
};
use crate::{InitializationOrder, IGNORED_TENANT_FILE_NAME, TEMP_FILE_SUFFIX};

use utils::crashsafe::path_with_suffix_extension;
use utils::fs_ext::PathExt;
use utils::generation::Generation;
use utils::id::{TenantId, TimelineId};

use super::delete::DeleteTenantError;
use super::timeline::delete::DeleteTimelineFlow;
use super::TenantSharedResources;

/// For a tenant that appears in TenantsMap, it may either be
/// - `Attached`: has a full Tenant object, is elegible to service
///    reads and ingest WAL.
/// - `Secondary`: is only keeping a local cache warm.
///
/// Secondary is a totally distinct state rather than being a mode of a `Tenant`, because
/// that way we avoid having to carefully switch a tenant's ingestion etc on and off during
/// its lifetime, and we can preserve some important safety invariants like `Tenant` always
/// having a properly acquired generation (Secondary doesn't need a generation)
#[derive(Clone)]
pub enum TenantSlot {
    Attached(Arc<Tenant>),
    Secondary,
}

impl TenantSlot {
    /// Return the `Tenant` in this slot if attached, else None
    fn get_attached(&self) -> Option<&Arc<Tenant>> {
        match self {
            Self::Attached(t) => Some(t),
            Self::Secondary => None,
        }
    }

    /// Consume self and return the `Tenant` that was in this slot if attached, else None
    fn into_attached(self) -> Option<Arc<Tenant>> {
        match self {
            Self::Attached(t) => Some(t),
            Self::Secondary => None,
        }
    }
}

/// The tenants known to the pageserver.
/// The enum variants are used to distinguish the different states that the pageserver can be in.
pub(crate) enum TenantsMap {
    /// [`init_tenant_mgr`] is not done yet.
    Initializing,
    /// [`init_tenant_mgr`] is done, all on-disk tenants have been loaded.
    /// New tenants can be added using [`tenant_map_insert`].
    Open(HashMap<TenantId, TenantSlot>),
    /// The pageserver has entered shutdown mode via [`shutdown_all_tenants`].
    /// Existing tenants are still accessible, but no new tenants can be created.
    ShuttingDown(HashMap<TenantId, TenantSlot>),
}

impl TenantsMap {
    /// Convenience function for typical usage, where we want to get a `Tenant` object, for
    /// working with attached tenants.  If the TenantId is in the map but in Secondary state,
    /// None is returned.
    pub(crate) fn get(&self, tenant_id: &TenantId) -> Option<&Arc<Tenant>> {
        match self {
            TenantsMap::Initializing => None,
            TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => {
                m.get(tenant_id).and_then(TenantSlot::get_attached)
            }
        }
    }

    /// Get the contents of the map at this tenant ID, even if it is in secondary state.
    pub(crate) fn get_slot(&self, tenant_id: &TenantId) -> Option<&TenantSlot> {
        match self {
            TenantsMap::Initializing => None,
            TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m.get(tenant_id),
        }
    }
    pub(crate) fn remove(&mut self, tenant_id: &TenantId) -> Option<Arc<Tenant>> {
        match self {
            TenantsMap::Initializing => None,
            TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => {
                m.remove(tenant_id).and_then(TenantSlot::into_attached)
            }
        }
    }
}

/// This is "safe" in that that it won't leave behind a partially deleted directory
/// at the original path, because we rename with TEMP_FILE_SUFFIX before starting deleting
/// the contents.
///
/// This is pageserver-specific, as it relies on future processes after a crash to check
/// for TEMP_FILE_SUFFIX when loading things.
async fn safe_remove_tenant_dir_all(path: impl AsRef<Utf8Path>) -> std::io::Result<()> {
    let tmp_path = safe_rename_tenant_dir(path).await?;
    fs::remove_dir_all(tmp_path).await
}

async fn safe_rename_tenant_dir(path: impl AsRef<Utf8Path>) -> std::io::Result<Utf8PathBuf> {
    let parent = path
        .as_ref()
        .parent()
        // It is invalid to call this function with a relative path.  Tenant directories
        // should always have a parent.
        .ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Path must be absolute",
        ))?;
    let rand_suffix = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect::<String>()
        + TEMP_FILE_SUFFIX;
    let tmp_path = path_with_suffix_extension(&path, &rand_suffix);
    fs::rename(path.as_ref(), &tmp_path).await?;
    fs::File::open(parent).await?.sync_all().await?;
    Ok(tmp_path)
}

static TENANTS: Lazy<RwLock<TenantsMap>> = Lazy::new(|| RwLock::new(TenantsMap::Initializing));

/// Initialize repositories with locally available timelines.
/// Timelines that are only partially available locally (remote storage has more data than this pageserver)
/// are scheduled for download and added to the tenant once download is completed.
#[instrument(skip_all)]
pub async fn init_tenant_mgr(
    conf: &'static PageServerConf,
    resources: TenantSharedResources,
    init_order: InitializationOrder,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // Scan local filesystem for attached tenants
    let tenants_dir = conf.tenants_path();

    let mut tenants = HashMap::new();

    // If we are configured to use the control plane API, then it is the source of truth for what tenants to load.
    let tenant_generations = if let Some(client) = ControlPlaneClient::new(conf, &cancel) {
        let result = match client.re_attach().await {
            Ok(tenants) => tenants,
            Err(RetryForeverError::ShuttingDown) => {
                anyhow::bail!("Shut down while waiting for control plane re-attach response")
            }
        };

        // The deletion queue needs to know about the startup attachment state to decide which (if any) stored
        // deletion list entries may still be valid.  We provide that by pushing a recovery operation into
        // the queue. Sequential processing of te queue ensures that recovery is done before any new tenant deletions
        // are processed, even though we don't block on recovery completing here.
        //
        // Must only do this if remote storage is enabled, otherwise deletion queue
        // is not running and channel push will fail.
        if resources.remote_storage.is_some() {
            resources
                .deletion_queue_client
                .recover(result.clone())
                .await?;
        }

        Some(result)
    } else {
        info!("Control plane API not configured, tenant generations are disabled");
        None
    };

    let mut dir_entries = tenants_dir
        .read_dir_utf8()
        .with_context(|| format!("Failed to list tenants dir {tenants_dir:?}"))?;

    let ctx = RequestContext::todo_child(TaskKind::Startup, DownloadBehavior::Warn);

    loop {
        match dir_entries.next() {
            None => break,
            Some(Ok(dir_entry)) => {
                let tenant_dir_path = dir_entry.path().to_path_buf();
                if crate::is_temporary(&tenant_dir_path) {
                    info!("Found temporary tenant directory, removing: {tenant_dir_path}");
                    // No need to use safe_remove_tenant_dir_all because this is already
                    // a temporary path
                    if let Err(e) = fs::remove_dir_all(&tenant_dir_path).await {
                        error!(
                            "Failed to remove temporary directory '{}': {:?}",
                            tenant_dir_path, e
                        );
                    }
                } else {
                    // This case happens if we:
                    // * crash during attach before creating the attach marker file
                    // * crash during tenant delete before removing tenant directory
                    let is_empty = tenant_dir_path.is_empty_dir().with_context(|| {
                        format!("Failed to check whether {tenant_dir_path:?} is an empty dir")
                    })?;
                    if is_empty {
                        info!("removing empty tenant directory {tenant_dir_path:?}");
                        if let Err(e) = fs::remove_dir(&tenant_dir_path).await {
                            error!(
                                "Failed to remove empty tenant directory '{}': {e:#}",
                                tenant_dir_path
                            )
                        }
                        continue;
                    }

                    let tenant_ignore_mark_file = tenant_dir_path.join(IGNORED_TENANT_FILE_NAME);
                    if tenant_ignore_mark_file.exists() {
                        info!("Found an ignore mark file {tenant_ignore_mark_file:?}, skipping the tenant");
                        continue;
                    }

                    let tenant_id = match tenant_dir_path
                        .file_name()
                        .unwrap_or_default()
                        .parse::<TenantId>()
                    {
                        Ok(id) => id,
                        Err(_) => {
                            warn!(
                                "Invalid tenant path (garbage in our repo directory?): {}",
                                tenant_dir_path
                            );
                            continue;
                        }
                    };

                    // Try loading the location configuration
                    let mut location_conf = match Tenant::load_tenant_config(conf, &tenant_id)
                        .context("load tenant config")
                    {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("Marking tenant broken, failed to {e:#}");

                            tenants.insert(
                                tenant_id,
                                TenantSlot::Attached(Tenant::create_broken_tenant(
                                    conf,
                                    tenant_id,
                                    "error loading tenant location configuration".to_string(),
                                )),
                            );

                            continue;
                        }
                    };

                    let generation = if let Some(generations) = &tenant_generations {
                        // We have a generation map: treat it as the authority for whether
                        // this tenant is really attached.
                        if let Some(gen) = generations.get(&tenant_id) {
                            *gen
                        } else {
                            match &location_conf.mode {
                                LocationMode::Secondary(_) => {
                                    // We do not require the control plane's permission for secondary mode
                                    // tenants, because they do no remote writes and hence require no
                                    // generation number
                                    info!("Loaded tenant {tenant_id} in secondary mode");
                                    tenants.insert(tenant_id, TenantSlot::Secondary);
                                }
                                LocationMode::Attached(_) => {
                                    // TODO: augment re-attach API to enable the control plane to
                                    // instruct us about secondary attachments.  That way, instead of throwing
                                    // away local state, we can gracefully fall back to secondary here, if the control
                                    // plane tells us so.
                                    // (https://github.com/neondatabase/neon/issues/5377)
                                    info!("Detaching tenant {tenant_id}, control plane omitted it in re-attach response");
                                    if let Err(e) =
                                        safe_remove_tenant_dir_all(&tenant_dir_path).await
                                    {
                                        error!(
                                            "Failed to remove detached tenant directory '{}': {:?}",
                                            tenant_dir_path, e
                                        );
                                    }
                                }
                            };

                            continue;
                        }
                    } else {
                        // Legacy mode: no generation information, any tenant present
                        // on local disk may activate
                        info!(
                            "Starting tenant {} in legacy mode, no generation",
                            tenant_dir_path
                        );
                        Generation::none()
                    };

                    // Presence of a generation number implies attachment: attach the tenant
                    // if it wasn't already, and apply the generation number.
                    location_conf.attach_in_generation(generation);
                    Tenant::persist_tenant_config(conf, &tenant_id, &location_conf).await?;

                    match schedule_local_tenant_processing(
                        conf,
                        tenant_id,
                        &tenant_dir_path,
                        AttachedTenantConf::try_from(location_conf)?,
                        resources.clone(),
                        Some(init_order.clone()),
                        &TENANTS,
                        &ctx,
                    ) {
                        Ok(tenant) => {
                            tenants.insert(tenant.tenant_id(), TenantSlot::Attached(tenant));
                        }
                        Err(e) => {
                            error!("Failed to collect tenant files from dir {tenants_dir:?} for entry {dir_entry:?}, reason: {e:#}");
                        }
                    }
                }
            }
            Some(Err(e)) => {
                // On error, print it, but continue with the other tenants. If we error out
                // here, the pageserver startup fails altogether, causing outage for *all*
                // tenants. That seems worse.
                error!(
                    "Failed to list tenants dir entry in directory {tenants_dir:?}, reason: {e:?}"
                );
            }
        }
    }

    info!("Processed {} local tenants at startup", tenants.len());

    let mut tenants_map = TENANTS.write().await;
    assert!(matches!(&*tenants_map, &TenantsMap::Initializing));
    *tenants_map = TenantsMap::Open(tenants);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn schedule_local_tenant_processing(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    tenant_path: &Utf8Path,
    location_conf: AttachedTenantConf,
    resources: TenantSharedResources,
    init_order: Option<InitializationOrder>,
    tenants: &'static tokio::sync::RwLock<TenantsMap>,
    ctx: &RequestContext,
) -> anyhow::Result<Arc<Tenant>> {
    anyhow::ensure!(
        tenant_path.is_dir(),
        "Cannot load tenant from path {tenant_path:?}, it either does not exist or not a directory"
    );
    anyhow::ensure!(
        !crate::is_temporary(tenant_path),
        "Cannot load tenant from temporary path {tenant_path:?}"
    );
    anyhow::ensure!(
        !tenant_path.is_empty_dir().with_context(|| {
            format!("Failed to check whether {tenant_path:?} is an empty dir")
        })?,
        "Cannot load tenant from empty directory {tenant_path:?}"
    );

    let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(&tenant_id);
    anyhow::ensure!(
        !conf.tenant_ignore_mark_file_path(&tenant_id).exists(),
        "Cannot load tenant, ignore mark found at {tenant_ignore_mark:?}"
    );

    let tenant = if conf.tenant_attaching_mark_file_path(&tenant_id).exists() {
        info!("tenant {tenant_id} has attaching mark file, resuming its attach operation");
        if resources.remote_storage.is_none() {
            warn!("tenant {tenant_id} has attaching mark file, but pageserver has no remote storage configured");
            Tenant::create_broken_tenant(
                conf,
                tenant_id,
                "attaching mark file present but no remote storage configured".to_string(),
            )
        } else {
            match Tenant::spawn_attach(conf, tenant_id, resources, location_conf, tenants, ctx) {
                Ok(tenant) => tenant,
                Err(e) => {
                    error!("Failed to spawn_attach tenant {tenant_id}, reason: {e:#}");
                    Tenant::create_broken_tenant(conf, tenant_id, format!("{e:#}"))
                }
            }
        }
    } else {
        info!("tenant {tenant_id} is assumed to be loadable, starting load operation");
        // Start loading the tenant into memory. It will initially be in Loading state.
        Tenant::spawn_load(
            conf,
            tenant_id,
            location_conf,
            resources,
            init_order,
            tenants,
            ctx,
        )
    };
    Ok(tenant)
}

///
/// Shut down all tenants. This runs as part of pageserver shutdown.
///
/// NB: We leave the tenants in the map, so that they remain accessible through
/// the management API until we shut it down. If we removed the shut-down tenants
/// from the tenants map, the management API would return 404 for these tenants,
/// because TenantsMap::get() now returns `None`.
/// That could be easily misinterpreted by control plane, the consumer of the
/// management API. For example, it could attach the tenant on a different pageserver.
/// We would then be in split-brain once this pageserver restarts.
#[instrument(skip_all)]
pub async fn shutdown_all_tenants() {
    shutdown_all_tenants0(&TENANTS).await
}

async fn shutdown_all_tenants0(tenants: &tokio::sync::RwLock<TenantsMap>) {
    use utils::completion;

    // Prevent new tenants from being created.
    let tenants_to_shut_down = {
        let mut m = tenants.write().await;
        match &mut *m {
            TenantsMap::Initializing => {
                *m = TenantsMap::ShuttingDown(HashMap::default());
                info!("tenants map is empty");
                return;
            }
            TenantsMap::Open(tenants) => {
                let tenants_clone = tenants.clone();
                *m = TenantsMap::ShuttingDown(std::mem::take(tenants));
                tenants_clone
            }
            TenantsMap::ShuttingDown(_) => {
                // TODO: it is possible that detach and shutdown happen at the same time. as a
                // result, during shutdown we do not wait for detach.
                error!("already shutting down, this function isn't supposed to be called more than once");
                return;
            }
        }
    };

    let started_at = std::time::Instant::now();
    let mut join_set = JoinSet::new();
    for (tenant_id, tenant) in tenants_to_shut_down {
        join_set.spawn(
            async move {
                let freeze_and_flush = true;

                let res = {
                    let (_guard, shutdown_progress) = completion::channel();
                    match tenant {
                        TenantSlot::Attached(t) => {
                            t.shutdown(shutdown_progress, freeze_and_flush).await
                        }
                        TenantSlot::Secondary => {
                            // TODO: once secondary mode downloads are implemented,
                            // ensure they have all stopped before we reach this point.
                            Ok(())
                        }
                    }
                };

                if let Err(other_progress) = res {
                    // join the another shutdown in progress
                    other_progress.wait().await;
                }

                // we cannot afford per tenant logging here, because if s3 is degraded, we are
                // going to log too many lines

                debug!("tenant successfully stopped");
            }
            .instrument(info_span!("shutdown", %tenant_id)),
        );
    }

    let total = join_set.len();
    let mut panicked = 0;
    let mut buffering = true;
    const BUFFER_FOR: std::time::Duration = std::time::Duration::from_millis(500);
    let mut buffered = std::pin::pin!(tokio::time::sleep(BUFFER_FOR));

    while !join_set.is_empty() {
        tokio::select! {
            Some(joined) = join_set.join_next() => {
                match joined {
                    Ok(()) => {}
                    Err(join_error) if join_error.is_cancelled() => {
                        unreachable!("we are not cancelling any of the futures");
                    }
                    Err(join_error) if join_error.is_panic() => {
                        // cannot really do anything, as this panic is likely a bug
                        panicked += 1;
                    }
                    Err(join_error) => {
                        warn!("unknown kind of JoinError: {join_error}");
                    }
                }
                if !buffering {
                    // buffer so that every 500ms since the first update (or starting) we'll log
                    // how far away we are; this is because we will get SIGKILL'd at 10s, and we
                    // are not able to log *then*.
                    buffering = true;
                    buffered.as_mut().reset(tokio::time::Instant::now() + BUFFER_FOR);
                }
            },
            _ = &mut buffered, if buffering => {
                buffering = false;
                info!(remaining = join_set.len(), total, elapsed_ms = started_at.elapsed().as_millis(), "waiting for tenants to shutdown");
            }
        }
    }

    if panicked > 0 {
        warn!(
            panicked,
            total, "observed panicks while shutting down tenants"
        );
    }

    // caller will log how long we took
}

pub async fn create_tenant(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    generation: Generation,
    resources: TenantSharedResources,
    ctx: &RequestContext,
) -> Result<Arc<Tenant>, TenantMapInsertError> {
    tenant_map_insert(tenant_id, || async {

        let location_conf = LocationConf::attached_single(tenant_conf, generation);

        // We're holding the tenants lock in write mode while doing local IO.
        // If this section ever becomes contentious, introduce a new `TenantState::Creating`
        // and do the work in that state.
        let tenant_directory = super::create_tenant_files(conf, &location_conf, &tenant_id, CreateTenantFilesMode::Create).await?;
        // TODO: tenant directory remains on disk if we bail out from here on.
        //       See https://github.com/neondatabase/neon/issues/4233

        let created_tenant =
            schedule_local_tenant_processing(conf, tenant_id, &tenant_directory,
                AttachedTenantConf::try_from(location_conf)?, resources, None, &TENANTS, ctx)?;
        // TODO: tenant object & its background loops remain, untracked in tenant map, if we fail here.
        //      See https://github.com/neondatabase/neon/issues/4233

        let crated_tenant_id = created_tenant.tenant_id();
        anyhow::ensure!(
                tenant_id == crated_tenant_id,
                "loaded created tenant has unexpected tenant id (expect {tenant_id} != actual {crated_tenant_id})",
            );
        Ok(created_tenant)
    }).await
}

#[derive(Debug, thiserror::Error)]
pub enum SetNewTenantConfigError {
    #[error(transparent)]
    GetTenant(#[from] GetTenantError),
    #[error(transparent)]
    Persist(anyhow::Error),
}

pub async fn set_new_tenant_config(
    conf: &'static PageServerConf,
    new_tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
) -> Result<(), SetNewTenantConfigError> {
    info!("configuring tenant {tenant_id}");
    let tenant = get_tenant(tenant_id, true).await?;

    // This is a legacy API that only operates on attached tenants: the preferred
    // API to use is the location_config/ endpoint, which lets the caller provide
    // the full LocationConf.
    let location_conf = LocationConf::attached_single(new_tenant_conf, tenant.generation);

    Tenant::persist_tenant_config(conf, &tenant_id, &location_conf)
        .await
        .map_err(SetNewTenantConfigError::Persist)?;
    tenant.set_new_tenant_config(new_tenant_conf);
    Ok(())
}

#[instrument(skip_all, fields(tenant_id, new_location_config))]
pub(crate) async fn upsert_location(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    new_location_config: LocationConf,
    broker_client: storage_broker::BrokerClientChannel,
    remote_storage: Option<GenericRemoteStorage>,
    deletion_queue_client: DeletionQueueClient,
    ctx: &RequestContext,
) -> Result<(), anyhow::Error> {
    info!("configuring tenant location {tenant_id} to state {new_location_config:?}");

    let mut existing_tenant = match get_tenant(tenant_id, false).await {
        Ok(t) => Some(t),
        Err(GetTenantError::NotFound(_)) => None,
        Err(e) => anyhow::bail!(e),
    };

    // If we need to shut down a Tenant, do that first
    let shutdown_tenant = match (&new_location_config.mode, &existing_tenant) {
        (LocationMode::Secondary(_), Some(t)) => Some(t),
        (LocationMode::Attached(attach_conf), Some(t)) => {
            if attach_conf.generation != t.generation {
                Some(t)
            } else {
                None
            }
        }
        _ => None,
    };

    // TODO: currently we risk concurrent operations interfering with the tenant
    // while we await shutdown, but we also should not hold the TenantsMap lock
    // across the whole operation.  Before we start using this function in production,
    // a follow-on change will revise how concurrency is handled in TenantsMap.
    // (https://github.com/neondatabase/neon/issues/5378)

    if let Some(tenant) = shutdown_tenant {
        let (_guard, progress) = utils::completion::channel();
        info!("Shutting down attached tenant");
        match tenant.shutdown(progress, false).await {
            Ok(()) => {}
            Err(barrier) => {
                info!("Shutdown already in progress, waiting for it to complete");
                barrier.wait().await;
            }
        }
        existing_tenant = None;
    }

    if let Some(tenant) = existing_tenant {
        // Update the existing tenant
        Tenant::persist_tenant_config(conf, &tenant_id, &new_location_config)
            .await
            .map_err(SetNewTenantConfigError::Persist)?;
        tenant.set_new_location_config(AttachedTenantConf::try_from(new_location_config)?);
    } else {
        // Upsert a fresh TenantSlot into TenantsMap.  Do it within the map write lock,
        // and re-check that the state of anything we are replacing is as expected.
        tenant_map_upsert_slot(tenant_id, |old_value| async move {
            if let Some(TenantSlot::Attached(t)) = old_value {
                if !matches!(t.current_state(), TenantState::Stopping { .. }) {
                    anyhow::bail!("Tenant state changed during location configuration update");
                }
            }

            let new_slot = match &new_location_config.mode {
                LocationMode::Secondary(_) => TenantSlot::Secondary,
                LocationMode::Attached(_attach_config) => {
                    // Do a schedule_local_tenant_processing
                    // FIXME: should avoid doing this disk I/O inside the TenantsMap lock,
                    // we have the same problem in load_tenant/attach_tenant.  Probably
                    // need a lock in TenantSlot to fix this.
                    Tenant::persist_tenant_config(conf, &tenant_id, &new_location_config)
                        .await
                        .map_err(SetNewTenantConfigError::Persist)?;
                    let tenant_path = conf.tenant_path(&tenant_id);
                    let resources = TenantSharedResources {
                        broker_client,
                        remote_storage,
                        deletion_queue_client,
                    };
                    let new_tenant = schedule_local_tenant_processing(
                        conf,
                        tenant_id,
                        &tenant_path,
                        AttachedTenantConf::try_from(new_location_config)?,
                        resources,
                        None,
                        &TENANTS,
                        ctx,
                    )
                    .with_context(|| {
                        format!("Failed to schedule tenant processing in path {tenant_path:?}")
                    })?;

                    TenantSlot::Attached(new_tenant)
                }
            };

            Ok(new_slot)
        })
        .await?;
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum GetTenantError {
    #[error("Tenant {0} not found")]
    NotFound(TenantId),
    #[error("Tenant {0} is not active")]
    NotActive(TenantId),
    /// Broken is logically a subset of NotActive, but a distinct error is useful as
    /// NotActive is usually a retryable state for API purposes, whereas Broken
    /// is a stuck error state
    #[error("Tenant is broken: {0}")]
    Broken(String),
}

/// Gets the tenant from the in-memory data, erroring if it's absent or is not fitting to the query.
/// `active_only = true` allows to query only tenants that are ready for operations, erroring on other kinds of tenants.
///
/// This method is cancel-safe.
pub async fn get_tenant(
    tenant_id: TenantId,
    active_only: bool,
) -> Result<Arc<Tenant>, GetTenantError> {
    let m = TENANTS.read().await;
    let tenant = m
        .get(&tenant_id)
        .ok_or(GetTenantError::NotFound(tenant_id))?;

    match tenant.current_state() {
        TenantState::Broken {
            reason,
            backtrace: _,
        } if active_only => Err(GetTenantError::Broken(reason)),
        TenantState::Active => Ok(Arc::clone(tenant)),
        _ => {
            if active_only {
                Err(GetTenantError::NotActive(tenant_id))
            } else {
                Ok(Arc::clone(tenant))
            }
        }
    }
}

pub async fn delete_tenant(
    conf: &'static PageServerConf,
    remote_storage: Option<GenericRemoteStorage>,
    tenant_id: TenantId,
) -> Result<(), DeleteTenantError> {
    DeleteTenantFlow::run(conf, remote_storage, &TENANTS, tenant_id).await
}

#[derive(Debug, thiserror::Error)]
pub enum DeleteTimelineError {
    #[error("Tenant {0}")]
    Tenant(#[from] GetTenantError),

    #[error("Timeline {0}")]
    Timeline(#[from] crate::tenant::DeleteTimelineError),
}

pub async fn delete_timeline(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    _ctx: &RequestContext,
) -> Result<(), DeleteTimelineError> {
    let tenant = get_tenant(tenant_id, true).await?;
    DeleteTimelineFlow::run(&tenant, timeline_id, false).await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum TenantStateError {
    #[error("Tenant {0} not found")]
    NotFound(TenantId),
    #[error("Tenant {0} is stopping")]
    IsStopping(TenantId),
    #[error("Tenant {0} is not active")]
    NotActive(TenantId),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub async fn detach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    detach_ignored: bool,
) -> Result<(), TenantStateError> {
    let tmp_path = detach_tenant0(conf, &TENANTS, tenant_id, detach_ignored).await?;
    // Although we are cleaning up the tenant, this task is not meant to be bound by the lifetime of the tenant in memory.
    // After a tenant is detached, there are no more task_mgr tasks for that tenant_id.
    let task_tenant_id = None;
    task_mgr::spawn(
        task_mgr::BACKGROUND_RUNTIME.handle(),
        TaskKind::MgmtRequest,
        task_tenant_id,
        None,
        "tenant_files_delete",
        false,
        async move {
            fs::remove_dir_all(tmp_path.as_path())
                .await
                .with_context(|| format!("tenant directory {:?} deletion", tmp_path))
        },
    );
    Ok(())
}

async fn detach_tenant0(
    conf: &'static PageServerConf,
    tenants: &tokio::sync::RwLock<TenantsMap>,
    tenant_id: TenantId,
    detach_ignored: bool,
) -> Result<Utf8PathBuf, TenantStateError> {
    let tenant_dir_rename_operation = |tenant_id_to_clean| async move {
        let local_tenant_directory = conf.tenant_path(&tenant_id_to_clean);
        safe_rename_tenant_dir(&local_tenant_directory)
            .await
            .with_context(|| format!("local tenant directory {local_tenant_directory:?} rename"))
    };

    let removal_result =
        remove_tenant_from_memory(tenants, tenant_id, tenant_dir_rename_operation(tenant_id)).await;

    // Ignored tenants are not present in memory and will bail the removal from memory operation.
    // Before returning the error, check for ignored tenant removal case — we only need to clean its local files then.
    if detach_ignored && matches!(removal_result, Err(TenantStateError::NotFound(_))) {
        let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(&tenant_id);
        if tenant_ignore_mark.exists() {
            info!("Detaching an ignored tenant");
            let tmp_path = tenant_dir_rename_operation(tenant_id)
                .await
                .with_context(|| format!("Ignored tenant {tenant_id} local directory rename"))?;
            return Ok(tmp_path);
        }
    }

    removal_result
}

pub async fn load_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    generation: Generation,
    broker_client: storage_broker::BrokerClientChannel,
    remote_storage: Option<GenericRemoteStorage>,
    deletion_queue_client: DeletionQueueClient,
    ctx: &RequestContext,
) -> Result<(), TenantMapInsertError> {
    tenant_map_insert(tenant_id, || async {
        let tenant_path = conf.tenant_path(&tenant_id);
        let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(&tenant_id);
        if tenant_ignore_mark.exists() {
            std::fs::remove_file(&tenant_ignore_mark)
                .with_context(|| format!("Failed to remove tenant ignore mark {tenant_ignore_mark:?} during tenant loading"))?;
        }

        let resources = TenantSharedResources {
            broker_client,
            remote_storage,
            deletion_queue_client
        };

        let mut location_conf = Tenant::load_tenant_config(conf, &tenant_id).map_err( TenantMapInsertError::Other)?;
        location_conf.attach_in_generation(generation);
        Tenant::persist_tenant_config(conf, &tenant_id, &location_conf).await?;

        let new_tenant = schedule_local_tenant_processing(conf, tenant_id, &tenant_path, AttachedTenantConf::try_from(location_conf)?, resources, None,  &TENANTS, ctx)
            .with_context(|| {
                format!("Failed to schedule tenant processing in path {tenant_path:?}")
            })?;

        Ok(new_tenant)
    }).await?;
    Ok(())
}

pub async fn ignore_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
) -> Result<(), TenantStateError> {
    ignore_tenant0(conf, &TENANTS, tenant_id).await
}

async fn ignore_tenant0(
    conf: &'static PageServerConf,
    tenants: &tokio::sync::RwLock<TenantsMap>,
    tenant_id: TenantId,
) -> Result<(), TenantStateError> {
    remove_tenant_from_memory(tenants, tenant_id, async {
        let ignore_mark_file = conf.tenant_ignore_mark_file_path(&tenant_id);
        fs::File::create(&ignore_mark_file)
            .await
            .context("Failed to create ignore mark file")
            .and_then(|_| {
                crashsafe::fsync_file_and_parent(&ignore_mark_file)
                    .context("Failed to fsync ignore mark file")
            })
            .with_context(|| format!("Failed to crate ignore mark for tenant {tenant_id}"))?;
        Ok(())
    })
    .await
}

#[derive(Debug, thiserror::Error)]
pub enum TenantMapListError {
    #[error("tenant map is still initializing")]
    Initializing,
}

///
/// Get list of tenants, for the mgmt API
///
pub async fn list_tenants() -> Result<Vec<(TenantId, TenantState)>, TenantMapListError> {
    let tenants = TENANTS.read().await;
    let m = match &*tenants {
        TenantsMap::Initializing => return Err(TenantMapListError::Initializing),
        TenantsMap::Open(m) | TenantsMap::ShuttingDown(m) => m,
    };
    Ok(m.iter()
        .filter_map(|(id, tenant)| match tenant {
            TenantSlot::Attached(tenant) => Some((*id, tenant.current_state())),
            TenantSlot::Secondary => None,
        })
        .collect())
}

/// Execute Attach mgmt API command.
///
/// Downloading all the tenant data is performed in the background, this merely
/// spawns the background task and returns quickly.
pub async fn attach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    generation: Generation,
    tenant_conf: TenantConfOpt,
    resources: TenantSharedResources,
    ctx: &RequestContext,
) -> Result<(), TenantMapInsertError> {
    tenant_map_insert(tenant_id, || async {
        let location_conf = LocationConf::attached_single(tenant_conf, generation);
        let tenant_dir = create_tenant_files(conf, &location_conf, &tenant_id, CreateTenantFilesMode::Attach).await?;
        // TODO: tenant directory remains on disk if we bail out from here on.
        //       See https://github.com/neondatabase/neon/issues/4233

        // Without the attach marker, schedule_local_tenant_processing will treat the attached tenant as fully attached
        let marker_file_exists = conf
            .tenant_attaching_mark_file_path(&tenant_id)
            .try_exists()
            .context("check for attach marker file existence")?;
        anyhow::ensure!(marker_file_exists, "create_tenant_files should have created the attach marker file");

        let attached_tenant = schedule_local_tenant_processing(conf, tenant_id, &tenant_dir, AttachedTenantConf::try_from(location_conf)?, resources, None, &TENANTS, ctx)?;
        // TODO: tenant object & its background loops remain, untracked in tenant map, if we fail here.
        //      See https://github.com/neondatabase/neon/issues/4233

        let attached_tenant_id = attached_tenant.tenant_id();
        anyhow::ensure!(
            tenant_id == attached_tenant_id,
            "loaded created tenant has unexpected tenant id (expect {tenant_id} != actual {attached_tenant_id})",
        );
        Ok(attached_tenant)
    })
    .await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum TenantMapInsertError {
    #[error("tenant map is still initializing")]
    StillInitializing,
    #[error("tenant map is shutting down")]
    ShuttingDown,
    #[error("tenant {0} already exists, state: {1:?}")]
    TenantAlreadyExists(TenantId, TenantState),
    #[error("tenant {0} already exists in secondary state")]
    TenantExistsSecondary(TenantId),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Give the given closure access to the tenants map entry for the given `tenant_id`, iff that
/// entry is vacant. The closure is responsible for creating the tenant object and inserting
/// it into the tenants map through the vacnt entry that it receives as argument.
///
/// NB: the closure should return quickly because the current implementation of tenants map
/// serializes access through an `RwLock`.
async fn tenant_map_insert<F, R>(
    tenant_id: TenantId,
    insert_fn: F,
) -> Result<Arc<Tenant>, TenantMapInsertError>
where
    F: FnOnce() -> R,
    R: std::future::Future<Output = anyhow::Result<Arc<Tenant>>>,
{
    let mut guard = TENANTS.write().await;
    let m = match &mut *guard {
        TenantsMap::Initializing => return Err(TenantMapInsertError::StillInitializing),
        TenantsMap::ShuttingDown(_) => return Err(TenantMapInsertError::ShuttingDown),
        TenantsMap::Open(m) => m,
    };
    match m.entry(tenant_id) {
        hash_map::Entry::Occupied(e) => match e.get() {
            TenantSlot::Attached(t) => Err(TenantMapInsertError::TenantAlreadyExists(
                tenant_id,
                t.current_state(),
            )),
            TenantSlot::Secondary => Err(TenantMapInsertError::TenantExistsSecondary(tenant_id)),
        },
        hash_map::Entry::Vacant(v) => match insert_fn().await {
            Ok(tenant) => {
                v.insert(TenantSlot::Attached(tenant.clone()));
                Ok(tenant)
            }
            Err(e) => Err(TenantMapInsertError::Other(e)),
        },
    }
}

async fn tenant_map_upsert_slot<'a, F, R>(
    tenant_id: TenantId,
    upsert_fn: F,
) -> Result<(), TenantMapInsertError>
where
    F: FnOnce(Option<TenantSlot>) -> R,
    R: std::future::Future<Output = anyhow::Result<TenantSlot>>,
{
    let mut guard = TENANTS.write().await;
    let m = match &mut *guard {
        TenantsMap::Initializing => return Err(TenantMapInsertError::StillInitializing),
        TenantsMap::ShuttingDown(_) => return Err(TenantMapInsertError::ShuttingDown),
        TenantsMap::Open(m) => m,
    };

    match upsert_fn(m.remove(&tenant_id)).await {
        Ok(upsert_val) => {
            m.insert(tenant_id, upsert_val);
            Ok(())
        }
        Err(e) => Err(TenantMapInsertError::Other(e)),
    }
}

/// Stops and removes the tenant from memory, if it's not [`TenantState::Stopping`] already, bails otherwise.
/// Allows to remove other tenant resources manually, via `tenant_cleanup`.
/// If the cleanup fails, tenant will stay in memory in [`TenantState::Broken`] state, and another removal
/// operation would be needed to remove it.
async fn remove_tenant_from_memory<V, F>(
    tenants: &tokio::sync::RwLock<TenantsMap>,
    tenant_id: TenantId,
    tenant_cleanup: F,
) -> Result<V, TenantStateError>
where
    F: std::future::Future<Output = anyhow::Result<V>>,
{
    use utils::completion;

    // It's important to keep the tenant in memory after the final cleanup, to avoid cleanup races.
    // The exclusive lock here ensures we don't miss the tenant state updates before trying another removal.
    // tenant-wde cleanup operations may take some time (removing the entire tenant directory), we want to
    // avoid holding the lock for the entire process.
    let tenant = {
        match tenants
            .write()
            .await
            .get_slot(&tenant_id)
            .ok_or(TenantStateError::NotFound(tenant_id))?
        {
            TenantSlot::Attached(t) => Some(t.clone()),
            TenantSlot::Secondary => None,
        }
    };

    // allow pageserver shutdown to await for our completion
    let (_guard, progress) = completion::channel();

    // If the tenant was attached, shut it down gracefully.  For secondary
    // locations this part is not necessary
    match tenant {
        Some(attached_tenant) => {
            // whenever we remove a tenant from memory, we don't want to flush and wait for upload
            let freeze_and_flush = false;

            // shutdown is sure to transition tenant to stopping, and wait for all tasks to complete, so
            // that we can continue safely to cleanup.
            match attached_tenant.shutdown(progress, freeze_and_flush).await {
                Ok(()) => {}
                Err(_other) => {
                    // if pageserver shutdown or other detach/ignore is already ongoing, we don't want to
                    // wait for it but return an error right away because these are distinct requests.
                    return Err(TenantStateError::IsStopping(tenant_id));
                }
            }
        }
        None => {
            // Nothing to wait on when not attached, proceed.
        }
    }

    match tenant_cleanup
        .await
        .with_context(|| format!("Failed to run cleanup for tenant {tenant_id}"))
    {
        Ok(hook_value) => {
            let mut tenants_accessor = tenants.write().await;
            if tenants_accessor.remove(&tenant_id).is_none() {
                warn!("Tenant {tenant_id} got removed from memory before operation finished");
            }
            Ok(hook_value)
        }
        Err(e) => {
            let tenants_accessor = tenants.read().await;
            match tenants_accessor.get(&tenant_id) {
                Some(tenant) => {
                    tenant.set_broken(e.to_string()).await;
                }
                None => {
                    warn!("Tenant {tenant_id} got removed from memory");
                    return Err(TenantStateError::NotFound(tenant_id));
                }
            }
            Err(TenantStateError::Other(e))
        }
    }
}

use {
    crate::repository::GcResult, pageserver_api::models::TimelineGcRequest,
    utils::http::error::ApiError,
};

pub async fn immediate_gc(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    gc_req: TimelineGcRequest,
    ctx: &RequestContext,
) -> Result<tokio::sync::oneshot::Receiver<Result<GcResult, anyhow::Error>>, ApiError> {
    let guard = TENANTS.read().await;
    let tenant = guard
        .get(&tenant_id)
        .map(Arc::clone)
        .with_context(|| format!("tenant {tenant_id}"))
        .map_err(|e| ApiError::NotFound(e.into()))?;

    let gc_horizon = gc_req.gc_horizon.unwrap_or_else(|| tenant.get_gc_horizon());
    // Use tenant's pitr setting
    let pitr = tenant.get_pitr_interval();

    // Run in task_mgr to avoid race with tenant_detach operation
    let ctx = ctx.detached_child(TaskKind::GarbageCollector, DownloadBehavior::Download);
    let (task_done, wait_task_done) = tokio::sync::oneshot::channel();
    task_mgr::spawn(
        &tokio::runtime::Handle::current(),
        TaskKind::GarbageCollector,
        Some(tenant_id),
        Some(timeline_id),
        &format!("timeline_gc_handler garbage collection run for tenant {tenant_id} timeline {timeline_id}"),
        false,
        async move {
            fail::fail_point!("immediate_gc_task_pre");
            let result = tenant
                .gc_iteration(Some(timeline_id), gc_horizon, pitr, &ctx)
                .instrument(info_span!("manual_gc", %tenant_id, %timeline_id))
                .await;
                // FIXME: `gc_iteration` can return an error for multiple reasons; we should handle it
                // better once the types support it.
            match task_done.send(result) {
                Ok(_) => (),
                Err(result) => error!("failed to send gc result: {result:?}"),
            }
            Ok(())
        }
    );

    // drop the guard until after we've spawned the task so that timeline shutdown will wait for the task
    drop(guard);

    Ok(wait_task_done)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use tracing::{info_span, Instrument};

    use crate::tenant::mgr::TenantSlot;

    use super::{super::harness::TenantHarness, TenantsMap};

    #[tokio::test(start_paused = true)]
    async fn shutdown_joins_remove_tenant_from_memory() {
        // the test is a bit ugly with the lockstep together with spawned tasks. the aim is to make
        // sure `shutdown_all_tenants0` per-tenant processing joins in any active
        // remove_tenant_from_memory calls, which is enforced by making the operation last until
        // we've ran `shutdown_all_tenants0` for a long time.

        let (t, _ctx) = TenantHarness::create("shutdown_joins_detach")
            .unwrap()
            .load()
            .await;

        // harness loads it to active, which is forced and nothing is running on the tenant

        let id = t.tenant_id();

        // tenant harness configures the logging and we cannot escape it
        let _e = info_span!("testing", tenant_id = %id).entered();

        let tenants = HashMap::from([(id, TenantSlot::Attached(t.clone()))]);
        let tenants = Arc::new(tokio::sync::RwLock::new(TenantsMap::Open(tenants)));

        let (until_cleanup_completed, can_complete_cleanup) = utils::completion::channel();
        let (until_cleanup_started, cleanup_started) = utils::completion::channel();

        // start a "detaching operation", which will take a while, until can_complete_cleanup
        let cleanup_task = {
            let jh = tokio::spawn({
                let tenants = tenants.clone();
                async move {
                    let cleanup = async move {
                        drop(until_cleanup_started);
                        can_complete_cleanup.wait().await;
                        anyhow::Ok(())
                    };
                    super::remove_tenant_from_memory(&tenants, id, cleanup).await
                }
                .instrument(info_span!("foobar", tenant_id = %id))
            });

            // now the long cleanup should be in place, with the stopping state
            cleanup_started.wait().await;
            jh
        };

        let mut cleanup_progress = std::pin::pin!(t
            .shutdown(utils::completion::Barrier::default(), false)
            .await
            .unwrap_err()
            .wait());

        let mut shutdown_task = {
            let (until_shutdown_started, shutdown_started) = utils::completion::channel();

            let shutdown_task = tokio::spawn(async move {
                drop(until_shutdown_started);
                super::shutdown_all_tenants0(&tenants).await;
            });

            shutdown_started.wait().await;
            shutdown_task
        };

        // if the joining in is removed from shutdown_all_tenants0, the shutdown_task should always
        // get to complete within timeout and fail the test. it is expected to continue awaiting
        // until completion or SIGKILL during normal shutdown.
        //
        // the timeout is long to cover anything that shutdown_task could be doing, but it is
        // handled instantly because we use tokio's time pausing in this test. 100s is much more than
        // what we get from systemd on shutdown (10s).
        let long_time = std::time::Duration::from_secs(100);
        tokio::select! {
            _ = &mut shutdown_task => unreachable!("shutdown must continue, until_cleanup_completed is not dropped"),
            _ = &mut cleanup_progress => unreachable!("cleanup progress must continue, until_cleanup_completed is not dropped"),
            _ = tokio::time::sleep(long_time) => {},
        }

        // allow the remove_tenant_from_memory and thus eventually the shutdown to continue
        drop(until_cleanup_completed);

        let (je, ()) = tokio::join!(shutdown_task, cleanup_progress);
        je.expect("Tenant::shutdown shutdown not have panicked");
        cleanup_task
            .await
            .expect("no panicking")
            .expect("remove_tenant_from_memory failed");

        futures::future::poll_immediate(
            t.shutdown(utils::completion::Barrier::default(), false)
                .await
                .unwrap_err()
                .wait(),
        )
        .await
        .expect("the stopping progress must still be complete");
    }
}
