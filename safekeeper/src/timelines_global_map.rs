//! This module contains global `(tenant_id, timeline_id)` -> `Arc<Timeline>` mapping.
//! All timelines should always be present in this map, this is done by loading them
//! all from the disk on startup and keeping them in memory.

use crate::defaults::DEFAULT_EVICTION_CONCURRENCY;
use crate::rate_limit::RateLimiter;
use crate::state::TimelinePersistentState;
use crate::timeline::{get_tenant_dir, get_timeline_dir, Timeline, TimelineError};
use crate::timelines_set::TimelinesSet;
use crate::wal_storage::Storage;
use crate::{control_file, wal_storage, SafeKeeperConf};
use anyhow::{bail, Context, Result};
use camino::Utf8PathBuf;
use camino_tempfile::Utf8TempDir;
use safekeeper_api::membership::Configuration;
use safekeeper_api::ServerInfo;
use serde::Serialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::fs;
use tracing::*;
use utils::crashsafe::{durable_rename, fsync_async_opt};
use utils::id::{TenantId, TenantTimelineId, TimelineId};
use utils::lsn::Lsn;

// Timeline entry in the global map: either a ready timeline, or mark that it is
// being created.
#[derive(Clone)]
enum GlobalMapTimeline {
    CreationInProgress,
    Timeline(Arc<Timeline>),
}

struct GlobalTimelinesState {
    timelines: HashMap<TenantTimelineId, GlobalMapTimeline>,

    // A tombstone indicates this timeline used to exist has been deleted.  These are used to prevent
    // on-demand timeline creation from recreating deleted timelines.  This is only soft-enforced, as
    // this map is dropped on restart.
    tombstones: HashMap<TenantTimelineId, Instant>,

    conf: Arc<SafeKeeperConf>,
    broker_active_set: Arc<TimelinesSet>,
    global_rate_limiter: RateLimiter,
}

impl GlobalTimelinesState {
    /// Get dependencies for a timeline constructor.
    fn get_dependencies(&self) -> (Arc<SafeKeeperConf>, Arc<TimelinesSet>, RateLimiter) {
        (
            self.conf.clone(),
            self.broker_active_set.clone(),
            self.global_rate_limiter.clone(),
        )
    }

    /// Get timeline from the map. Returns error if timeline doesn't exist or
    /// creation is in progress.
    fn get(&self, ttid: &TenantTimelineId) -> Result<Arc<Timeline>, TimelineError> {
        match self.timelines.get(ttid).cloned() {
            Some(GlobalMapTimeline::Timeline(tli)) => Ok(tli),
            Some(GlobalMapTimeline::CreationInProgress) => {
                Err(TimelineError::CreationInProgress(*ttid))
            }
            None => Err(TimelineError::NotFound(*ttid)),
        }
    }

    fn delete(&mut self, ttid: TenantTimelineId) {
        self.timelines.remove(&ttid);
        self.tombstones.insert(ttid, Instant::now());
    }
}

/// A struct used to manage access to the global timelines map.
pub struct GlobalTimelines {
    state: Mutex<GlobalTimelinesState>,
}

impl GlobalTimelines {
    /// Create a new instance of the global timelines map.
    pub fn new(conf: Arc<SafeKeeperConf>) -> Self {
        Self {
            state: Mutex::new(GlobalTimelinesState {
                timelines: HashMap::new(),
                tombstones: HashMap::new(),
                conf,
                broker_active_set: Arc::new(TimelinesSet::default()),
                global_rate_limiter: RateLimiter::new(1, 1),
            }),
        }
    }

    /// Inject dependencies needed for the timeline constructors and load all timelines to memory.
    pub async fn init(&self) -> Result<()> {
        // clippy isn't smart enough to understand that drop(state) releases the
        // lock, so use explicit block
        let tenants_dir = {
            let mut state = self.state.lock().unwrap();
            state.global_rate_limiter = RateLimiter::new(
                state.conf.partial_backup_concurrency,
                DEFAULT_EVICTION_CONCURRENCY,
            );

            // Iterate through all directories and load tenants for all directories
            // named as a valid tenant_id.
            state.conf.workdir.clone()
        };
        let mut tenant_count = 0;
        for tenants_dir_entry in std::fs::read_dir(&tenants_dir)
            .with_context(|| format!("failed to list tenants dir {}", tenants_dir))?
        {
            match &tenants_dir_entry {
                Ok(tenants_dir_entry) => {
                    if let Ok(tenant_id) =
                        TenantId::from_str(tenants_dir_entry.file_name().to_str().unwrap_or(""))
                    {
                        tenant_count += 1;
                        self.load_tenant_timelines(tenant_id).await?;
                    }
                }
                Err(e) => error!(
                    "failed to list tenants dir entry {:?} in directory {}, reason: {:?}",
                    tenants_dir_entry, tenants_dir, e
                ),
            }
        }

        info!(
            "found {} tenants directories, successfully loaded {} timelines",
            tenant_count,
            self.state.lock().unwrap().timelines.len()
        );
        Ok(())
    }

    /// Loads all timelines for the given tenant to memory. Returns fs::read_dir
    /// errors if any.
    ///
    /// It is async, but self.state lock is sync and there is no important
    /// reason to make it async (it is always held for a short while), so we
    /// just lock and unlock it for each timeline -- this function is called
    /// during init when nothing else is running, so this is fine.
    async fn load_tenant_timelines(&self, tenant_id: TenantId) -> Result<()> {
        let (conf, broker_active_set, partial_backup_rate_limiter) = {
            let state = self.state.lock().unwrap();
            state.get_dependencies()
        };

        let timelines_dir = get_tenant_dir(&conf, &tenant_id);
        for timelines_dir_entry in std::fs::read_dir(&timelines_dir)
            .with_context(|| format!("failed to list timelines dir {}", timelines_dir))?
        {
            match &timelines_dir_entry {
                Ok(timeline_dir_entry) => {
                    if let Ok(timeline_id) =
                        TimelineId::from_str(timeline_dir_entry.file_name().to_str().unwrap_or(""))
                    {
                        let ttid = TenantTimelineId::new(tenant_id, timeline_id);
                        match Timeline::load_timeline(conf.clone(), ttid) {
                            Ok(tli) => {
                                let mut shared_state = tli.write_shared_state().await;
                                self.state
                                    .lock()
                                    .unwrap()
                                    .timelines
                                    .insert(ttid, GlobalMapTimeline::Timeline(tli.clone()));
                                tli.bootstrap(
                                    &mut shared_state,
                                    &conf,
                                    broker_active_set.clone(),
                                    partial_backup_rate_limiter.clone(),
                                );
                            }
                            // If we can't load a timeline, it's most likely because of a corrupted
                            // directory. We will log an error and won't allow to delete/recreate
                            // this timeline. The only way to fix this timeline is to repair manually
                            // and restart the safekeeper.
                            Err(e) => error!(
                                "failed to load timeline {} for tenant {}, reason: {:?}",
                                timeline_id, tenant_id, e
                            ),
                        }
                    }
                }
                Err(e) => error!(
                    "failed to list timelines dir entry {:?} in directory {}, reason: {:?}",
                    timelines_dir_entry, timelines_dir, e
                ),
            }
        }

        Ok(())
    }

    /// Get the number of timelines in the map.
    pub fn timelines_count(&self) -> usize {
        self.state.lock().unwrap().timelines.len()
    }

    /// Get the global safekeeper config.
    pub fn get_global_config(&self) -> Arc<SafeKeeperConf> {
        self.state.lock().unwrap().conf.clone()
    }

    pub fn get_global_broker_active_set(&self) -> Arc<TimelinesSet> {
        self.state.lock().unwrap().broker_active_set.clone()
    }

    /// Create a new timeline with the given id. If the timeline already exists, returns
    /// an existing timeline.
    pub(crate) async fn create(
        &self,
        ttid: TenantTimelineId,
        mconf: Configuration,
        server_info: ServerInfo,
        start_lsn: Lsn,
        commit_lsn: Lsn,
    ) -> Result<Arc<Timeline>> {
        let (conf, _, _) = {
            let state = self.state.lock().unwrap();
            if let Ok(timeline) = state.get(&ttid) {
                // Timeline already exists, return it.
                return Ok(timeline);
            }

            if state.tombstones.contains_key(&ttid) {
                anyhow::bail!("Timeline {ttid} is deleted, refusing to recreate");
            }

            state.get_dependencies()
        };

        info!("creating new timeline {}", ttid);

        // Do on disk initialization in tmp dir.
        let (_tmp_dir, tmp_dir_path) = create_temp_timeline_dir(&conf, ttid).await?;

        // TODO: currently we create only cfile. It would be reasonable to
        // immediately initialize first WAL segment as well.
        let state = TimelinePersistentState::new(&ttid, mconf, server_info, start_lsn, commit_lsn)?;
        control_file::FileStorage::create_new(&tmp_dir_path, state, conf.no_sync).await?;
        let timeline = self.load_temp_timeline(ttid, &tmp_dir_path, true).await?;
        Ok(timeline)
    }

    /// Move timeline from a temp directory to the main storage, and load it to
    /// the global map. Creating timeline in this way ensures atomicity: rename
    /// is atomic, so either move of the whole datadir succeeds or it doesn't,
    /// but corrupted data dir shouldn't be possible.
    ///
    /// We'd like to avoid holding map lock while doing IO, so it's a 3 step
    /// process:
    /// 1) check the global map that timeline doesn't exist and mark that we're
    ///    creating it;
    /// 2) move the directory and load the timeline
    /// 3) take lock again and insert the timeline into the global map.
    pub async fn load_temp_timeline(
        &self,
        ttid: TenantTimelineId,
        tmp_path: &Utf8PathBuf,
        check_tombstone: bool,
    ) -> Result<Arc<Timeline>> {
        // Check for existence and mark that we're creating it.
        let (conf, broker_active_set, partial_backup_rate_limiter) = {
            let mut state = self.state.lock().unwrap();
            match state.timelines.get(&ttid) {
                Some(GlobalMapTimeline::CreationInProgress) => {
                    bail!(TimelineError::CreationInProgress(ttid));
                }
                Some(GlobalMapTimeline::Timeline(_)) => {
                    bail!(TimelineError::AlreadyExists(ttid));
                }
                _ => {}
            }
            if check_tombstone {
                if state.tombstones.contains_key(&ttid) {
                    anyhow::bail!("timeline {ttid} is deleted, refusing to recreate");
                }
            } else {
                // We may be have been asked to load a timeline that was previously deleted (e.g. from `pull_timeline.rs`).  We trust
                // that the human doing this manual intervention knows what they are doing, and remove its tombstone.
                if state.tombstones.remove(&ttid).is_some() {
                    warn!("un-deleted timeline {ttid}");
                }
            }
            state
                .timelines
                .insert(ttid, GlobalMapTimeline::CreationInProgress);
            state.get_dependencies()
        };

        // Do the actual move and reflect the result in the map.
        match GlobalTimelines::install_temp_timeline(ttid, tmp_path, conf.clone()).await {
            Ok(timeline) => {
                let mut timeline_shared_state = timeline.write_shared_state().await;
                let mut state = self.state.lock().unwrap();
                assert!(matches!(
                    state.timelines.get(&ttid),
                    Some(GlobalMapTimeline::CreationInProgress)
                ));

                state
                    .timelines
                    .insert(ttid, GlobalMapTimeline::Timeline(timeline.clone()));
                drop(state);
                timeline.bootstrap(
                    &mut timeline_shared_state,
                    &conf,
                    broker_active_set,
                    partial_backup_rate_limiter,
                );
                drop(timeline_shared_state);
                Ok(timeline)
            }
            Err(e) => {
                // Init failed, remove the marker from the map
                let mut state = self.state.lock().unwrap();
                assert!(matches!(
                    state.timelines.get(&ttid),
                    Some(GlobalMapTimeline::CreationInProgress)
                ));
                state.timelines.remove(&ttid);
                Err(e)
            }
        }
    }

    /// Main part of load_temp_timeline: do the move and load.
    async fn install_temp_timeline(
        ttid: TenantTimelineId,
        tmp_path: &Utf8PathBuf,
        conf: Arc<SafeKeeperConf>,
    ) -> Result<Arc<Timeline>> {
        let tenant_path = get_tenant_dir(conf.as_ref(), &ttid.tenant_id);
        let timeline_path = get_timeline_dir(conf.as_ref(), &ttid);

        // We must have already checked that timeline doesn't exist in the map,
        // but there might be existing datadir: if timeline is corrupted it is
        // not loaded. We don't want to overwrite such a dir, so check for its
        // existence.
        match fs::metadata(&timeline_path).await {
            Ok(_) => {
                // Timeline directory exists on disk, we should leave state unchanged
                // and return error.
                bail!(TimelineError::Invalid(ttid));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e.into());
            }
        }

        info!(
            "moving timeline {} from {} to {}",
            ttid, tmp_path, timeline_path
        );

        // Now it is safe to move the timeline directory to the correct
        // location. First, create tenant directory. Ignore error if it already
        // exists.
        if let Err(e) = tokio::fs::create_dir(&tenant_path).await {
            if e.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(e.into());
            }
        }
        // fsync it
        fsync_async_opt(&tenant_path, !conf.no_sync).await?;
        // and its creation
        fsync_async_opt(&conf.workdir, !conf.no_sync).await?;

        // Do the move.
        durable_rename(tmp_path, &timeline_path, !conf.no_sync).await?;

        Timeline::load_timeline(conf, ttid)
    }

    /// Get a timeline from the global map. If it's not present, it doesn't exist on disk,
    /// or was corrupted and couldn't be loaded on startup. Returned timeline is always valid,
    /// i.e. loaded in memory and not cancelled.
    pub(crate) fn get(&self, ttid: TenantTimelineId) -> Result<Arc<Timeline>, TimelineError> {
        let tli_res = {
            let state = self.state.lock().unwrap();
            state.get(&ttid)
        };
        match tli_res {
            Ok(tli) => {
                if tli.is_cancelled() {
                    return Err(TimelineError::Cancelled(ttid));
                }
                Ok(tli)
            }
            _ => tli_res,
        }
    }

    /// Returns all timelines. This is used for background timeline processes.
    pub fn get_all(&self) -> Vec<Arc<Timeline>> {
        let global_lock = self.state.lock().unwrap();
        global_lock
            .timelines
            .values()
            .filter_map(|t| match t {
                GlobalMapTimeline::Timeline(t) => {
                    if t.is_cancelled() {
                        None
                    } else {
                        Some(t.clone())
                    }
                }
                _ => None,
            })
            .collect()
    }

    /// Returns all timelines belonging to a given tenant. Used for deleting all timelines of a tenant,
    /// and that's why it can return cancelled timelines, to retry deleting them.
    fn get_all_for_tenant(&self, tenant_id: TenantId) -> Vec<Arc<Timeline>> {
        let global_lock = self.state.lock().unwrap();
        global_lock
            .timelines
            .values()
            .filter_map(|t| match t {
                GlobalMapTimeline::Timeline(t) => Some(t.clone()),
                _ => None,
            })
            .filter(|t| t.ttid.tenant_id == tenant_id)
            .collect()
    }

    /// Cancels timeline, then deletes the corresponding data directory.
    /// If only_local, doesn't remove WAL segments in remote storage.
    pub(crate) async fn delete(
        &self,
        ttid: &TenantTimelineId,
        only_local: bool,
    ) -> Result<TimelineDeleteForceResult> {
        let tli_res = {
            let state = self.state.lock().unwrap();

            if state.tombstones.contains_key(ttid) {
                // Presence of a tombstone guarantees that a previous deletion has completed and there is no work to do.
                info!("Timeline {ttid} was already deleted");
                return Ok(TimelineDeleteForceResult {
                    dir_existed: false,
                    was_active: false,
                });
            }

            state.get(ttid)
        };

        let result = match tli_res {
            Ok(timeline) => {
                let was_active = timeline.broker_active.load(Ordering::Relaxed);

                info!("deleting timeline {}, only_local={}", ttid, only_local);
                timeline.shutdown().await;

                // Take a lock and finish the deletion holding this mutex.
                let mut shared_state = timeline.write_shared_state().await;

                let dir_existed = timeline.delete(&mut shared_state, only_local).await?;

                Ok(TimelineDeleteForceResult {
                    dir_existed,
                    was_active, // TODO: we probably should remove this field
                })
            }
            Err(_) => {
                // Timeline is not memory, but it may still exist on disk in broken state.
                let dir_path = get_timeline_dir(self.state.lock().unwrap().conf.as_ref(), ttid);
                let dir_existed = delete_dir(dir_path)?;

                Ok(TimelineDeleteForceResult {
                    dir_existed,
                    was_active: false,
                })
            }
        };

        // Finalize deletion, by dropping Timeline objects and storing smaller tombstones.  The tombstones
        // are used to prevent still-running computes from re-creating the same timeline when they send data,
        // and to speed up repeated deletion calls by avoiding re-listing objects.
        self.state.lock().unwrap().delete(*ttid);

        result
    }

    /// Deactivates and deletes all timelines for the tenant. Returns map of all timelines which
    /// the tenant had, `true` if a timeline was active. There may be a race if new timelines are
    /// created simultaneously. In that case the function will return error and the caller should
    /// retry tenant deletion again later.
    ///
    /// If only_local, doesn't remove WAL segments in remote storage.
    pub async fn delete_force_all_for_tenant(
        &self,
        tenant_id: &TenantId,
        only_local: bool,
    ) -> Result<HashMap<TenantTimelineId, TimelineDeleteForceResult>> {
        info!("deleting all timelines for tenant {}", tenant_id);
        let to_delete = self.get_all_for_tenant(*tenant_id);

        let mut err = None;

        let mut deleted = HashMap::new();
        for tli in &to_delete {
            match self.delete(&tli.ttid, only_local).await {
                Ok(result) => {
                    deleted.insert(tli.ttid, result);
                }
                Err(e) => {
                    error!("failed to delete timeline {}: {}", tli.ttid, e);
                    // Save error to return later.
                    err = Some(e);
                }
            }
        }

        // If there was an error, return it.
        if let Some(e) = err {
            return Err(e);
        }

        // There may be broken timelines on disk, so delete the whole tenant dir as well.
        // Note that we could concurrently create new timelines while we were deleting them,
        // so the directory may be not empty. In this case timelines will have bad state
        // and timeline background jobs can panic.
        delete_dir(get_tenant_dir(
            self.state.lock().unwrap().conf.as_ref(),
            tenant_id,
        ))?;

        Ok(deleted)
    }

    pub fn housekeeping(&self, tombstone_ttl: &Duration) {
        let mut state = self.state.lock().unwrap();

        // We keep tombstones long enough to have a good chance of preventing rogue computes from re-creating deleted
        // timelines.  If a compute kept running for longer than this TTL (or across a safekeeper restart) then they
        // may recreate a deleted timeline.
        let now = Instant::now();
        state
            .tombstones
            .retain(|_, v| now.duration_since(*v) < *tombstone_ttl);
    }
}

#[derive(Clone, Copy, Serialize)]
pub struct TimelineDeleteForceResult {
    pub dir_existed: bool,
    pub was_active: bool,
}

/// Deletes directory and it's contents. Returns false if directory does not exist.
fn delete_dir(path: Utf8PathBuf) -> Result<bool> {
    match std::fs::remove_dir_all(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Create temp directory for a new timeline. It needs to be located on the same
/// filesystem as the rest of the timelines. It will be automatically deleted when
/// Utf8TempDir goes out of scope.
pub async fn create_temp_timeline_dir(
    conf: &SafeKeeperConf,
    ttid: TenantTimelineId,
) -> Result<(Utf8TempDir, Utf8PathBuf)> {
    let temp_base = conf.workdir.join("tmp");

    tokio::fs::create_dir_all(&temp_base).await?;

    let tli_dir = camino_tempfile::Builder::new()
        .suffix("_temptli")
        .prefix(&format!("{}_{}_", ttid.tenant_id, ttid.timeline_id))
        .tempdir_in(temp_base)?;

    let tli_dir_path = tli_dir.path().to_path_buf();

    Ok((tli_dir, tli_dir_path))
}

/// Do basic validation of a temp timeline, before moving it to the global map.
pub async fn validate_temp_timeline(
    conf: &SafeKeeperConf,
    ttid: TenantTimelineId,
    path: &Utf8PathBuf,
) -> Result<(Lsn, Lsn)> {
    let control_path = path.join("safekeeper.control");

    let control_store = control_file::FileStorage::load_control_file(control_path)?;
    if control_store.server.wal_seg_size == 0 {
        bail!("wal_seg_size is not set");
    }

    let wal_store = wal_storage::PhysicalStorage::new(&ttid, path, &control_store, conf.no_sync)?;

    let commit_lsn = control_store.commit_lsn;
    let flush_lsn = wal_store.flush_lsn();

    Ok((commit_lsn, flush_lsn))
}
