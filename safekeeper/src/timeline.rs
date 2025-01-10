//! This module implements Timeline lifecycle management and has all necessary code
//! to glue together SafeKeeper and all other background services.

use anyhow::{anyhow, bail, Result};
use camino::{Utf8Path, Utf8PathBuf};
use remote_storage::RemotePath;
use safekeeper_api::models::{PeerInfo, TimelineTermBumpResponse};
use safekeeper_api::Term;
use tokio::fs::{self};
use tokio_util::sync::CancellationToken;
use utils::id::TenantId;
use utils::sync::gate::Gate;

use std::cmp::max;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::{sync::watch, time::Instant};
use tracing::*;
use utils::http::error::ApiError;
use utils::{
    id::{NodeId, TenantTimelineId},
    lsn::Lsn,
};

use storage_broker::proto::SafekeeperTimelineInfo;
use storage_broker::proto::TenantTimelineId as ProtoTenantTimelineId;

use crate::control_file;
use crate::rate_limit::RateLimiter;
use crate::receive_wal::WalReceivers;
use crate::safekeeper::{AcceptorProposerMessage, ProposerAcceptorMessage, SafeKeeper, TermLsn};
use crate::send_wal::WalSenders;
use crate::state::{EvictionState, TimelineMemState, TimelinePersistentState, TimelineState};
use crate::timeline_guard::ResidenceGuard;
use crate::timeline_manager::{AtomicStatus, ManagerCtl};
use crate::timelines_set::TimelinesSet;
use crate::wal_backup::{self, remote_timeline_path};
use crate::wal_backup_partial::PartialRemoteSegment;

use crate::metrics::{FullTimelineInfo, WalStorageMetrics, MISC_OPERATION_SECONDS};
use crate::wal_storage::{Storage as wal_storage_iface, WalReader};
use crate::SafeKeeperConf;
use crate::{debug_dump, timeline_manager, wal_storage};

fn peer_info_from_sk_info(sk_info: &SafekeeperTimelineInfo, ts: Instant) -> PeerInfo {
    PeerInfo {
        sk_id: NodeId(sk_info.safekeeper_id),
        term: sk_info.term,
        last_log_term: sk_info.last_log_term,
        flush_lsn: Lsn(sk_info.flush_lsn),
        commit_lsn: Lsn(sk_info.commit_lsn),
        local_start_lsn: Lsn(sk_info.local_start_lsn),
        pg_connstr: sk_info.safekeeper_connstr.clone(),
        http_connstr: sk_info.http_connstr.clone(),
        ts,
    }
}

// vector-based node id -> peer state map with very limited functionality we
// need.
#[derive(Debug, Clone, Default)]
pub struct PeersInfo(pub Vec<PeerInfo>);

impl PeersInfo {
    fn get(&mut self, id: NodeId) -> Option<&mut PeerInfo> {
        self.0.iter_mut().find(|p| p.sk_id == id)
    }

    fn upsert(&mut self, p: &PeerInfo) {
        match self.get(p.sk_id) {
            Some(rp) => *rp = p.clone(),
            None => self.0.push(p.clone()),
        }
    }
}

pub type ReadGuardSharedState<'a> = RwLockReadGuard<'a, SharedState>;

/// WriteGuardSharedState is a wrapper around `RwLockWriteGuard<SharedState>` that
/// automatically updates `watch::Sender` channels with state on drop.
pub struct WriteGuardSharedState<'a> {
    tli: Arc<Timeline>,
    guard: RwLockWriteGuard<'a, SharedState>,
}

impl<'a> WriteGuardSharedState<'a> {
    fn new(tli: Arc<Timeline>, guard: RwLockWriteGuard<'a, SharedState>) -> Self {
        WriteGuardSharedState { tli, guard }
    }
}

impl Deref for WriteGuardSharedState<'_> {
    type Target = SharedState;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for WriteGuardSharedState<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for WriteGuardSharedState<'_> {
    fn drop(&mut self) {
        let term_flush_lsn =
            TermLsn::from((self.guard.sk.last_log_term(), self.guard.sk.flush_lsn()));
        let commit_lsn = self.guard.sk.state().inmem.commit_lsn;

        let _ = self.tli.term_flush_lsn_watch_tx.send_if_modified(|old| {
            if *old != term_flush_lsn {
                *old = term_flush_lsn;
                true
            } else {
                false
            }
        });

        let _ = self.tli.commit_lsn_watch_tx.send_if_modified(|old| {
            if *old != commit_lsn {
                *old = commit_lsn;
                true
            } else {
                false
            }
        });

        // send notification about shared state update
        self.tli.shared_state_version_tx.send_modify(|old| {
            *old += 1;
        });
    }
}

/// This structure is stored in shared state and represents the state of the timeline.
///
/// Usually it holds SafeKeeper, but it also supports offloaded timeline state. In this
/// case, SafeKeeper is not available (because WAL is not present on disk) and all
/// operations can be done only with control file.
pub enum StateSK {
    Loaded(SafeKeeper<control_file::FileStorage, wal_storage::PhysicalStorage>),
    Offloaded(Box<TimelineState<control_file::FileStorage>>),
    // Not used, required for moving between states.
    Empty,
}

impl StateSK {
    pub fn flush_lsn(&self) -> Lsn {
        match self {
            StateSK::Loaded(sk) => sk.wal_store.flush_lsn(),
            StateSK::Offloaded(state) => match state.eviction_state {
                EvictionState::Offloaded(flush_lsn) => flush_lsn,
                _ => panic!("StateSK::Offloaded mismatches with eviction_state from control_file"),
            },
            StateSK::Empty => unreachable!(),
        }
    }

    /// Get a reference to the control file's timeline state.
    pub fn state(&self) -> &TimelineState<control_file::FileStorage> {
        match self {
            StateSK::Loaded(sk) => &sk.state,
            StateSK::Offloaded(ref s) => s,
            StateSK::Empty => unreachable!(),
        }
    }

    pub fn state_mut(&mut self) -> &mut TimelineState<control_file::FileStorage> {
        match self {
            StateSK::Loaded(sk) => &mut sk.state,
            StateSK::Offloaded(ref mut s) => s,
            StateSK::Empty => unreachable!(),
        }
    }

    pub fn last_log_term(&self) -> Term {
        self.state()
            .acceptor_state
            .get_last_log_term(self.flush_lsn())
    }

    pub async fn term_bump(&mut self, to: Option<Term>) -> Result<TimelineTermBumpResponse> {
        self.state_mut().term_bump(to).await
    }

    /// Close open WAL files to release FDs.
    fn close_wal_store(&mut self) {
        if let StateSK::Loaded(sk) = self {
            sk.wal_store.close();
        }
    }

    /// Update timeline state with peer safekeeper data.
    pub async fn record_safekeeper_info(&mut self, sk_info: &SafekeeperTimelineInfo) -> Result<()> {
        // update commit_lsn if safekeeper is loaded
        match self {
            StateSK::Loaded(sk) => sk.record_safekeeper_info(sk_info).await?,
            StateSK::Offloaded(_) => {}
            StateSK::Empty => unreachable!(),
        }

        // update everything else, including remote_consistent_lsn and backup_lsn
        let mut sync_control_file = false;
        let state = self.state_mut();
        let wal_seg_size = state.server.wal_seg_size as u64;

        state.inmem.backup_lsn = max(Lsn(sk_info.backup_lsn), state.inmem.backup_lsn);
        sync_control_file |= state.backup_lsn + wal_seg_size < state.inmem.backup_lsn;

        state.inmem.remote_consistent_lsn = max(
            Lsn(sk_info.remote_consistent_lsn),
            state.inmem.remote_consistent_lsn,
        );
        sync_control_file |=
            state.remote_consistent_lsn + wal_seg_size < state.inmem.remote_consistent_lsn;

        state.inmem.peer_horizon_lsn =
            max(Lsn(sk_info.peer_horizon_lsn), state.inmem.peer_horizon_lsn);
        sync_control_file |= state.peer_horizon_lsn + wal_seg_size < state.inmem.peer_horizon_lsn;

        if sync_control_file {
            state.flush().await?;
        }
        Ok(())
    }

    /// Previously known as epoch_start_lsn. Needed only for reference in some APIs.
    pub fn term_start_lsn(&self) -> Lsn {
        match self {
            StateSK::Loaded(sk) => sk.term_start_lsn,
            StateSK::Offloaded(_) => Lsn(0),
            StateSK::Empty => unreachable!(),
        }
    }

    /// Used for metrics only.
    pub fn wal_storage_metrics(&self) -> WalStorageMetrics {
        match self {
            StateSK::Loaded(sk) => sk.wal_store.get_metrics(),
            StateSK::Offloaded(_) => WalStorageMetrics::default(),
            StateSK::Empty => unreachable!(),
        }
    }

    /// Returns WAL storage internal LSNs for debug dump.
    pub fn wal_storage_internal_state(&self) -> (Lsn, Lsn, Lsn, bool) {
        match self {
            StateSK::Loaded(sk) => sk.wal_store.internal_state(),
            StateSK::Offloaded(_) => {
                let flush_lsn = self.flush_lsn();
                (flush_lsn, flush_lsn, flush_lsn, false)
            }
            StateSK::Empty => unreachable!(),
        }
    }

    /// Access to SafeKeeper object. Panics if offloaded, should be good to use from WalResidentTimeline.
    pub fn safekeeper(
        &mut self,
    ) -> &mut SafeKeeper<control_file::FileStorage, wal_storage::PhysicalStorage> {
        match self {
            StateSK::Loaded(sk) => sk,
            StateSK::Offloaded(_) => {
                panic!("safekeeper is offloaded, cannot be used")
            }
            StateSK::Empty => unreachable!(),
        }
    }

    /// Moves control file's state structure out of the enum. Used to switch states.
    fn take_state(self) -> TimelineState<control_file::FileStorage> {
        match self {
            StateSK::Loaded(sk) => sk.state,
            StateSK::Offloaded(state) => *state,
            StateSK::Empty => unreachable!(),
        }
    }
}

/// Shared state associated with database instance
pub struct SharedState {
    /// Safekeeper object
    pub(crate) sk: StateSK,
    /// In memory list containing state of peers sent in latest messages from them.
    pub(crate) peers_info: PeersInfo,
    // True value hinders old WAL removal; this is used by snapshotting. We
    // could make it a counter, but there is no need to.
    pub(crate) wal_removal_on_hold: bool,
}

impl SharedState {
    /// Creates a new SharedState.
    pub fn new(sk: StateSK) -> Self {
        Self {
            sk,
            peers_info: PeersInfo(vec![]),
            wal_removal_on_hold: false,
        }
    }

    /// Restore SharedState from control file. If file doesn't exist, bails out.
    pub fn restore(conf: &SafeKeeperConf, ttid: &TenantTimelineId) -> Result<Self> {
        let timeline_dir = get_timeline_dir(conf, ttid);
        let control_store = control_file::FileStorage::restore_new(&timeline_dir, conf.no_sync)?;
        if control_store.server.wal_seg_size == 0 {
            bail!(TimelineError::UninitializedWalSegSize(*ttid));
        }

        let sk = match control_store.eviction_state {
            EvictionState::Present => {
                let wal_store = wal_storage::PhysicalStorage::new(
                    ttid,
                    &timeline_dir,
                    &control_store,
                    conf.no_sync,
                )?;
                StateSK::Loaded(SafeKeeper::new(
                    TimelineState::new(control_store),
                    wal_store,
                    conf.my_id,
                )?)
            }
            EvictionState::Offloaded(_) => {
                StateSK::Offloaded(Box::new(TimelineState::new(control_store)))
            }
        };

        Ok(Self::new(sk))
    }

    pub(crate) fn get_wal_seg_size(&self) -> usize {
        self.sk.state().server.wal_seg_size as usize
    }

    fn get_safekeeper_info(
        &self,
        ttid: &TenantTimelineId,
        conf: &SafeKeeperConf,
        standby_apply_lsn: Lsn,
    ) -> SafekeeperTimelineInfo {
        SafekeeperTimelineInfo {
            safekeeper_id: conf.my_id.0,
            tenant_timeline_id: Some(ProtoTenantTimelineId {
                tenant_id: ttid.tenant_id.as_ref().to_owned(),
                timeline_id: ttid.timeline_id.as_ref().to_owned(),
            }),
            term: self.sk.state().acceptor_state.term,
            last_log_term: self.sk.last_log_term(),
            flush_lsn: self.sk.flush_lsn().0,
            // note: this value is not flushed to control file yet and can be lost
            commit_lsn: self.sk.state().inmem.commit_lsn.0,
            remote_consistent_lsn: self.sk.state().inmem.remote_consistent_lsn.0,
            peer_horizon_lsn: self.sk.state().inmem.peer_horizon_lsn.0,
            safekeeper_connstr: conf
                .advertise_pg_addr
                .to_owned()
                .unwrap_or(conf.listen_pg_addr.clone()),
            http_connstr: conf.listen_http_addr.to_owned(),
            backup_lsn: self.sk.state().inmem.backup_lsn.0,
            local_start_lsn: self.sk.state().local_start_lsn.0,
            availability_zone: conf.availability_zone.clone(),
            standby_horizon: standby_apply_lsn.0,
        }
    }

    /// Get our latest view of alive peers status on the timeline.
    /// We pass our own info through the broker as well, so when we don't have connection
    /// to the broker returned vec is empty.
    pub(crate) fn get_peers(&self, heartbeat_timeout: Duration) -> Vec<PeerInfo> {
        let now = Instant::now();
        self.peers_info
            .0
            .iter()
            // Regard peer as absent if we haven't heard from it within heartbeat_timeout.
            .filter(|p| now.duration_since(p.ts) <= heartbeat_timeout)
            .cloned()
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TimelineError {
    #[error("Timeline {0} was cancelled and cannot be used anymore")]
    Cancelled(TenantTimelineId),
    #[error("Timeline {0} was not found in global map")]
    NotFound(TenantTimelineId),
    #[error("Timeline {0} creation is in progress")]
    CreationInProgress(TenantTimelineId),
    #[error("Timeline {0} exists on disk, but wasn't loaded on startup")]
    Invalid(TenantTimelineId),
    #[error("Timeline {0} is already exists")]
    AlreadyExists(TenantTimelineId),
    #[error("Timeline {0} is not initialized, wal_seg_size is zero")]
    UninitializedWalSegSize(TenantTimelineId),
    #[error("Timeline {0} is not initialized, pg_version is unknown")]
    UninitialinzedPgVersion(TenantTimelineId),
}

// Convert to HTTP API error.
impl From<TimelineError> for ApiError {
    fn from(te: TimelineError) -> ApiError {
        match te {
            TimelineError::NotFound(ttid) => {
                ApiError::NotFound(anyhow!("timeline {} not found", ttid).into())
            }
            _ => ApiError::InternalServerError(anyhow!("{}", te)),
        }
    }
}

/// Timeline struct manages lifecycle (creation, deletion, restore) of a safekeeper timeline.
/// It also holds SharedState and provides mutually exclusive access to it.
pub struct Timeline {
    pub ttid: TenantTimelineId,
    pub remote_path: RemotePath,

    /// Used to broadcast commit_lsn updates to all background jobs.
    commit_lsn_watch_tx: watch::Sender<Lsn>,
    commit_lsn_watch_rx: watch::Receiver<Lsn>,

    /// Broadcasts (current term, flush_lsn) updates, walsender is interested in
    /// them when sending in recovery mode (to walproposer or peers). Note: this
    /// is just a notification, WAL reading should always done with lock held as
    /// term can change otherwise.
    term_flush_lsn_watch_tx: watch::Sender<TermLsn>,
    term_flush_lsn_watch_rx: watch::Receiver<TermLsn>,

    /// Broadcasts shared state updates.
    shared_state_version_tx: watch::Sender<usize>,
    shared_state_version_rx: watch::Receiver<usize>,

    /// Safekeeper and other state, that should remain consistent and
    /// synchronized with the disk. This is tokio mutex as we write WAL to disk
    /// while holding it, ensuring that consensus checks are in order.
    mutex: RwLock<SharedState>,
    walsenders: Arc<WalSenders>,
    walreceivers: Arc<WalReceivers>,
    timeline_dir: Utf8PathBuf,
    manager_ctl: ManagerCtl,
    conf: Arc<SafeKeeperConf>,

    /// Hold this gate from code that depends on the Timeline's non-shut-down state.  While holding
    /// this gate, you must respect [`Timeline::cancel`]
    pub(crate) gate: Gate,

    /// Delete/cancel will trigger this, background tasks should drop out as soon as it fires
    pub(crate) cancel: CancellationToken,

    // timeline_manager controlled state
    pub(crate) broker_active: AtomicBool,
    pub(crate) wal_backup_active: AtomicBool,
    pub(crate) last_removed_segno: AtomicU64,
    pub(crate) mgr_status: AtomicStatus,
}

impl Timeline {
    /// Constructs a new timeline.
    pub fn new(
        ttid: TenantTimelineId,
        timeline_dir: &Utf8Path,
        remote_path: &RemotePath,
        shared_state: SharedState,
        conf: Arc<SafeKeeperConf>,
    ) -> Arc<Self> {
        let (commit_lsn_watch_tx, commit_lsn_watch_rx) =
            watch::channel(shared_state.sk.state().commit_lsn);
        let (term_flush_lsn_watch_tx, term_flush_lsn_watch_rx) = watch::channel(TermLsn::from((
            shared_state.sk.last_log_term(),
            shared_state.sk.flush_lsn(),
        )));
        let (shared_state_version_tx, shared_state_version_rx) = watch::channel(0);

        let walreceivers = WalReceivers::new();

        Arc::new(Self {
            ttid,
            remote_path: remote_path.to_owned(),
            timeline_dir: timeline_dir.to_owned(),
            commit_lsn_watch_tx,
            commit_lsn_watch_rx,
            term_flush_lsn_watch_tx,
            term_flush_lsn_watch_rx,
            shared_state_version_tx,
            shared_state_version_rx,
            mutex: RwLock::new(shared_state),
            walsenders: WalSenders::new(walreceivers.clone()),
            walreceivers,
            gate: Default::default(),
            cancel: CancellationToken::default(),
            manager_ctl: ManagerCtl::new(),
            conf,
            broker_active: AtomicBool::new(false),
            wal_backup_active: AtomicBool::new(false),
            last_removed_segno: AtomicU64::new(0),
            mgr_status: AtomicStatus::new(),
        })
    }

    /// Load existing timeline from disk.
    pub fn load_timeline(
        conf: Arc<SafeKeeperConf>,
        ttid: TenantTimelineId,
    ) -> Result<Arc<Timeline>> {
        let _enter = info_span!("load_timeline", timeline = %ttid.timeline_id).entered();

        let shared_state = SharedState::restore(conf.as_ref(), &ttid)?;
        let timeline_dir = get_timeline_dir(conf.as_ref(), &ttid);
        let remote_path = remote_timeline_path(&ttid)?;

        Ok(Timeline::new(
            ttid,
            &timeline_dir,
            &remote_path,
            shared_state,
            conf,
        ))
    }

    /// Bootstrap new or existing timeline starting background tasks.
    pub fn bootstrap(
        self: &Arc<Timeline>,
        _shared_state: &mut WriteGuardSharedState<'_>,
        conf: &SafeKeeperConf,
        broker_active_set: Arc<TimelinesSet>,
        partial_backup_rate_limiter: RateLimiter,
    ) {
        let (tx, rx) = self.manager_ctl.bootstrap_manager();

        let Ok(gate_guard) = self.gate.enter() else {
            // Init raced with shutdown
            return;
        };

        // Start manager task which will monitor timeline state and update
        // background tasks.
        tokio::spawn({
            let this = self.clone();
            let conf = conf.clone();
            async move {
                let _gate_guard = gate_guard;
                timeline_manager::main_task(
                    ManagerTimeline { tli: this },
                    conf,
                    broker_active_set,
                    tx,
                    rx,
                    partial_backup_rate_limiter,
                )
                .await
            }
        });
    }

    /// Background timeline activities (which hold Timeline::gate) will no
    /// longer run once this function completes.
    pub async fn shutdown(&self) {
        info!("timeline {} shutting down", self.ttid);
        self.cancel.cancel();

        // Wait for any concurrent tasks to stop using this timeline, to avoid e.g. attempts
        // to read deleted files.
        self.gate.close().await;
    }

    /// Delete timeline from disk completely, by removing timeline directory.
    ///
    /// Also deletes WAL in s3. Might fail if e.g. s3 is unavailable, but
    /// deletion API endpoint is retriable.
    ///
    /// Timeline must be in shut-down state (i.e. call [`Self::shutdown`] first)
    pub async fn delete(
        &self,
        shared_state: &mut WriteGuardSharedState<'_>,
        only_local: bool,
    ) -> Result<bool> {
        // Assert that [`Self::shutdown`] was already called
        assert!(self.cancel.is_cancelled());
        assert!(self.gate.close_complete());

        // Close associated FDs. Nobody will be able to touch timeline data once
        // it is cancelled, so WAL storage won't be opened again.
        shared_state.sk.close_wal_store();

        if !only_local && self.conf.is_wal_backup_enabled() {
            // Note: we concurrently delete remote storage data from multiple
            // safekeepers. That's ok, s3 replies 200 if object doesn't exist and we
            // do some retries anyway.
            wal_backup::delete_timeline(&self.ttid).await?;
        }
        let dir_existed = delete_dir(&self.timeline_dir).await?;
        Ok(dir_existed)
    }

    /// Returns if timeline is cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Take a writing mutual exclusive lock on timeline shared_state.
    pub async fn write_shared_state<'a>(self: &'a Arc<Self>) -> WriteGuardSharedState<'a> {
        WriteGuardSharedState::new(self.clone(), self.mutex.write().await)
    }

    pub async fn read_shared_state(&self) -> ReadGuardSharedState {
        self.mutex.read().await
    }

    /// Returns commit_lsn watch channel.
    pub fn get_commit_lsn_watch_rx(&self) -> watch::Receiver<Lsn> {
        self.commit_lsn_watch_rx.clone()
    }

    /// Returns term_flush_lsn watch channel.
    pub fn get_term_flush_lsn_watch_rx(&self) -> watch::Receiver<TermLsn> {
        self.term_flush_lsn_watch_rx.clone()
    }

    /// Returns watch channel for SharedState update version.
    pub fn get_state_version_rx(&self) -> watch::Receiver<usize> {
        self.shared_state_version_rx.clone()
    }

    /// Returns wal_seg_size.
    pub async fn get_wal_seg_size(&self) -> usize {
        self.read_shared_state().await.get_wal_seg_size()
    }

    /// Returns state of the timeline.
    pub async fn get_state(&self) -> (TimelineMemState, TimelinePersistentState) {
        let state = self.read_shared_state().await;
        (
            state.sk.state().inmem.clone(),
            TimelinePersistentState::clone(state.sk.state()),
        )
    }

    /// Returns latest backup_lsn.
    pub async fn get_wal_backup_lsn(&self) -> Lsn {
        self.read_shared_state().await.sk.state().inmem.backup_lsn
    }

    /// Sets backup_lsn to the given value.
    pub async fn set_wal_backup_lsn(self: &Arc<Self>, backup_lsn: Lsn) -> Result<()> {
        if self.is_cancelled() {
            bail!(TimelineError::Cancelled(self.ttid));
        }

        let mut state = self.write_shared_state().await;
        state.sk.state_mut().inmem.backup_lsn = max(state.sk.state().inmem.backup_lsn, backup_lsn);
        // we should check whether to shut down offloader, but this will be done
        // soon by peer communication anyway.
        Ok(())
    }

    /// Get safekeeper info for broadcasting to broker and other peers.
    pub async fn get_safekeeper_info(&self, conf: &SafeKeeperConf) -> SafekeeperTimelineInfo {
        let standby_apply_lsn = self.walsenders.get_hotstandby().reply.apply_lsn;
        let shared_state = self.read_shared_state().await;
        shared_state.get_safekeeper_info(&self.ttid, conf, standby_apply_lsn)
    }

    /// Update timeline state with peer safekeeper data.
    pub async fn record_safekeeper_info(
        self: &Arc<Self>,
        sk_info: SafekeeperTimelineInfo,
    ) -> Result<()> {
        {
            let mut shared_state = self.write_shared_state().await;
            shared_state.sk.record_safekeeper_info(&sk_info).await?;
            let peer_info = peer_info_from_sk_info(&sk_info, Instant::now());
            shared_state.peers_info.upsert(&peer_info);
        }
        Ok(())
    }

    pub async fn get_peers(&self, conf: &SafeKeeperConf) -> Vec<PeerInfo> {
        let shared_state = self.read_shared_state().await;
        shared_state.get_peers(conf.heartbeat_timeout)
    }

    pub fn get_walsenders(&self) -> &Arc<WalSenders> {
        &self.walsenders
    }

    pub fn get_walreceivers(&self) -> &Arc<WalReceivers> {
        &self.walreceivers
    }

    /// Returns flush_lsn.
    pub async fn get_flush_lsn(&self) -> Lsn {
        self.read_shared_state().await.sk.flush_lsn()
    }

    /// Gather timeline data for metrics.
    pub async fn info_for_metrics(&self) -> Option<FullTimelineInfo> {
        if self.is_cancelled() {
            return None;
        }

        let (ps_feedback_count, last_ps_feedback) = self.walsenders.get_ps_feedback_stats();
        let state = self.read_shared_state().await;
        Some(FullTimelineInfo {
            ttid: self.ttid,
            ps_feedback_count,
            last_ps_feedback,
            wal_backup_active: self.wal_backup_active.load(Ordering::Relaxed),
            timeline_is_active: self.broker_active.load(Ordering::Relaxed),
            num_computes: self.walreceivers.get_num() as u32,
            last_removed_segno: self.last_removed_segno.load(Ordering::Relaxed),
            epoch_start_lsn: state.sk.term_start_lsn(),
            mem_state: state.sk.state().inmem.clone(),
            persisted_state: TimelinePersistentState::clone(state.sk.state()),
            flush_lsn: state.sk.flush_lsn(),
            wal_storage: state.sk.wal_storage_metrics(),
        })
    }

    /// Returns in-memory timeline state to build a full debug dump.
    pub async fn memory_dump(&self) -> debug_dump::Memory {
        let state = self.read_shared_state().await;

        let (write_lsn, write_record_lsn, flush_lsn, file_open) =
            state.sk.wal_storage_internal_state();

        debug_dump::Memory {
            is_cancelled: self.is_cancelled(),
            peers_info_len: state.peers_info.0.len(),
            walsenders: self.walsenders.get_all(),
            wal_backup_active: self.wal_backup_active.load(Ordering::Relaxed),
            active: self.broker_active.load(Ordering::Relaxed),
            num_computes: self.walreceivers.get_num() as u32,
            last_removed_segno: self.last_removed_segno.load(Ordering::Relaxed),
            epoch_start_lsn: state.sk.term_start_lsn(),
            mem_state: state.sk.state().inmem.clone(),
            mgr_status: self.mgr_status.get(),
            write_lsn,
            write_record_lsn,
            flush_lsn,
            file_open,
        }
    }

    /// Apply a function to the control file state and persist it.
    pub async fn map_control_file<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut TimelinePersistentState) -> Result<T>,
    ) -> Result<T> {
        let mut state = self.write_shared_state().await;
        let mut persistent_state = state.sk.state_mut().start_change();
        // If f returns error, we abort the change and don't persist anything.
        let res = f(&mut persistent_state)?;
        // If persisting fails, we abort the change and return error.
        state
            .sk
            .state_mut()
            .finish_change(&persistent_state)
            .await?;
        Ok(res)
    }

    pub async fn term_bump(self: &Arc<Self>, to: Option<Term>) -> Result<TimelineTermBumpResponse> {
        let mut state = self.write_shared_state().await;
        state.sk.term_bump(to).await
    }

    /// Guts of [`Self::wal_residence_guard`] and [`Self::try_wal_residence_guard`]
    async fn do_wal_residence_guard(
        self: &Arc<Self>,
        block: bool,
    ) -> Result<Option<WalResidentTimeline>> {
        let op_label = if block {
            "wal_residence_guard"
        } else {
            "try_wal_residence_guard"
        };

        if self.is_cancelled() {
            bail!(TimelineError::Cancelled(self.ttid));
        }

        debug!("requesting WalResidentTimeline guard");
        let started_at = Instant::now();
        let status_before = self.mgr_status.get();

        // Wait 30 seconds for the guard to be acquired. It can time out if someone is
        // holding the lock (e.g. during `SafeKeeper::process_msg()`) or manager task
        // is stuck.
        let res = tokio::time::timeout_at(started_at + Duration::from_secs(30), async {
            if block {
                self.manager_ctl.wal_residence_guard().await.map(Some)
            } else {
                self.manager_ctl.try_wal_residence_guard().await
            }
        })
        .await;

        let guard = match res {
            Ok(Ok(guard)) => {
                let finished_at = Instant::now();
                let elapsed = finished_at - started_at;
                MISC_OPERATION_SECONDS
                    .with_label_values(&[op_label])
                    .observe(elapsed.as_secs_f64());

                guard
            }
            Ok(Err(e)) => {
                warn!(
                    "error acquiring in {op_label}, statuses {:?} => {:?}",
                    status_before,
                    self.mgr_status.get()
                );
                return Err(e);
            }
            Err(_) => {
                warn!(
                    "timeout acquiring in {op_label} guard, statuses {:?} => {:?}",
                    status_before,
                    self.mgr_status.get()
                );
                anyhow::bail!("timeout while acquiring WalResidentTimeline guard");
            }
        };

        Ok(guard.map(|g| WalResidentTimeline::new(self.clone(), g)))
    }

    /// Get the timeline guard for reading/writing WAL files.
    /// If WAL files are not present on disk (evicted), they will be automatically
    /// downloaded from remote storage. This is done in the manager task, which is
    /// responsible for issuing all guards.
    ///
    /// NB: don't use this function from timeline_manager, it will deadlock.
    /// NB: don't use this function while holding shared_state lock.
    pub async fn wal_residence_guard(self: &Arc<Self>) -> Result<WalResidentTimeline> {
        self.do_wal_residence_guard(true)
            .await
            .map(|m| m.expect("Always get Some in block=true mode"))
    }

    /// Get the timeline guard for reading/writing WAL files if the timeline is resident,
    /// else return None
    pub(crate) async fn try_wal_residence_guard(
        self: &Arc<Self>,
    ) -> Result<Option<WalResidentTimeline>> {
        self.do_wal_residence_guard(false).await
    }

    pub async fn backup_partial_reset(self: &Arc<Self>) -> Result<Vec<String>> {
        self.manager_ctl.backup_partial_reset().await
    }
}

/// This is a guard that allows to read/write disk timeline state.
/// All tasks that are trying to read/write WAL from disk should use this guard.
pub struct WalResidentTimeline {
    pub tli: Arc<Timeline>,
    _guard: ResidenceGuard,
}

impl WalResidentTimeline {
    pub fn new(tli: Arc<Timeline>, _guard: ResidenceGuard) -> Self {
        WalResidentTimeline { tli, _guard }
    }
}

impl Deref for WalResidentTimeline {
    type Target = Arc<Timeline>;

    fn deref(&self) -> &Self::Target {
        &self.tli
    }
}

impl WalResidentTimeline {
    /// Returns true if walsender should stop sending WAL to pageserver. We
    /// terminate it if remote_consistent_lsn reached commit_lsn and there is no
    /// computes. While there might be nothing to stream already, we learn about
    /// remote_consistent_lsn update through replication feedback, and we want
    /// to stop pushing to the broker if pageserver is fully caughtup.
    pub async fn should_walsender_stop(&self, reported_remote_consistent_lsn: Lsn) -> bool {
        if self.is_cancelled() {
            return true;
        }
        let shared_state = self.read_shared_state().await;
        if self.walreceivers.get_num() == 0 {
            return shared_state.sk.state().inmem.commit_lsn == Lsn(0) || // no data at all yet
            reported_remote_consistent_lsn >= shared_state.sk.state().inmem.commit_lsn;
        }
        false
    }

    /// Ensure that current term is t, erroring otherwise, and lock the state.
    pub async fn acquire_term(&self, t: Term) -> Result<ReadGuardSharedState> {
        let ss = self.read_shared_state().await;
        if ss.sk.state().acceptor_state.term != t {
            bail!(
                "failed to acquire term {}, current term {}",
                t,
                ss.sk.state().acceptor_state.term
            );
        }
        Ok(ss)
    }

    /// Pass arrived message to the safekeeper.
    pub async fn process_msg(
        &self,
        msg: &ProposerAcceptorMessage,
    ) -> Result<Option<AcceptorProposerMessage>> {
        if self.is_cancelled() {
            bail!(TimelineError::Cancelled(self.ttid));
        }

        let mut rmsg: Option<AcceptorProposerMessage>;
        {
            let mut shared_state = self.write_shared_state().await;
            rmsg = shared_state.sk.safekeeper().process_msg(msg).await?;

            // if this is AppendResponse, fill in proper hot standby feedback.
            if let Some(AcceptorProposerMessage::AppendResponse(ref mut resp)) = rmsg {
                resp.hs_feedback = self.walsenders.get_hotstandby().hs_feedback;
            }
        }
        Ok(rmsg)
    }

    pub async fn get_walreader(&self, start_lsn: Lsn) -> Result<WalReader> {
        let (_, persisted_state) = self.get_state().await;
        let enable_remote_read = self.conf.is_wal_backup_enabled();

        WalReader::new(
            &self.ttid,
            self.timeline_dir.clone(),
            &persisted_state,
            start_lsn,
            enable_remote_read,
        )
    }

    pub fn get_timeline_dir(&self) -> Utf8PathBuf {
        self.timeline_dir.clone()
    }

    /// Update in memory remote consistent lsn.
    pub async fn update_remote_consistent_lsn(&self, candidate: Lsn) {
        let mut shared_state = self.write_shared_state().await;
        shared_state.sk.state_mut().inmem.remote_consistent_lsn = max(
            shared_state.sk.state().inmem.remote_consistent_lsn,
            candidate,
        );
    }
}

/// This struct contains methods that are used by timeline manager task.
pub(crate) struct ManagerTimeline {
    pub(crate) tli: Arc<Timeline>,
}

impl Deref for ManagerTimeline {
    type Target = Arc<Timeline>;

    fn deref(&self) -> &Self::Target {
        &self.tli
    }
}

impl ManagerTimeline {
    pub(crate) fn timeline_dir(&self) -> &Utf8PathBuf {
        &self.tli.timeline_dir
    }

    /// Manager requests this state on startup.
    pub(crate) async fn bootstrap_mgr(&self) -> (bool, Option<PartialRemoteSegment>) {
        let shared_state = self.read_shared_state().await;
        let is_offloaded = matches!(
            shared_state.sk.state().eviction_state,
            EvictionState::Offloaded(_)
        );
        let partial_backup_uploaded = shared_state.sk.state().partial_backup.uploaded_segment();

        (is_offloaded, partial_backup_uploaded)
    }

    /// Try to switch state Present->Offloaded.
    pub(crate) async fn switch_to_offloaded(
        &self,
        partial: &PartialRemoteSegment,
    ) -> anyhow::Result<()> {
        let mut shared = self.write_shared_state().await;

        // updating control file
        let mut pstate = shared.sk.state_mut().start_change();

        if !matches!(pstate.eviction_state, EvictionState::Present) {
            bail!(
                "cannot switch to offloaded state, current state is {:?}",
                pstate.eviction_state
            );
        }

        if partial.flush_lsn != shared.sk.flush_lsn() {
            bail!(
                "flush_lsn mismatch in partial backup, expected {}, got {}",
                shared.sk.flush_lsn(),
                partial.flush_lsn
            );
        }

        if partial.commit_lsn != pstate.commit_lsn {
            bail!(
                "commit_lsn mismatch in partial backup, expected {}, got {}",
                pstate.commit_lsn,
                partial.commit_lsn
            );
        }

        if partial.term != shared.sk.last_log_term() {
            bail!(
                "term mismatch in partial backup, expected {}, got {}",
                shared.sk.last_log_term(),
                partial.term
            );
        }

        pstate.eviction_state = EvictionState::Offloaded(shared.sk.flush_lsn());
        shared.sk.state_mut().finish_change(&pstate).await?;
        // control file is now switched to Offloaded state

        // now we can switch shared.sk to Offloaded, shouldn't fail
        let prev_sk = std::mem::replace(&mut shared.sk, StateSK::Empty);
        let cfile_state = prev_sk.take_state();
        shared.sk = StateSK::Offloaded(Box::new(cfile_state));

        Ok(())
    }

    /// Try to switch state Offloaded->Present.
    pub(crate) async fn switch_to_present(&self) -> anyhow::Result<()> {
        let mut shared = self.write_shared_state().await;

        // trying to restore WAL storage
        let wal_store = wal_storage::PhysicalStorage::new(
            &self.ttid,
            &self.timeline_dir,
            shared.sk.state(),
            self.conf.no_sync,
        )?;

        // updating control file
        let mut pstate = shared.sk.state_mut().start_change();

        if !matches!(pstate.eviction_state, EvictionState::Offloaded(_)) {
            bail!(
                "cannot switch to present state, current state is {:?}",
                pstate.eviction_state
            );
        }

        if wal_store.flush_lsn() != shared.sk.flush_lsn() {
            bail!(
                "flush_lsn mismatch in restored WAL, expected {}, got {}",
                shared.sk.flush_lsn(),
                wal_store.flush_lsn()
            );
        }

        pstate.eviction_state = EvictionState::Present;
        shared.sk.state_mut().finish_change(&pstate).await?;

        // now we can switch shared.sk to Present, shouldn't fail
        let prev_sk = std::mem::replace(&mut shared.sk, StateSK::Empty);
        let cfile_state = prev_sk.take_state();
        shared.sk = StateSK::Loaded(SafeKeeper::new(cfile_state, wal_store, self.conf.my_id)?);

        Ok(())
    }

    /// Update current manager state, useful for debugging manager deadlocks.
    pub(crate) fn set_status(&self, status: timeline_manager::Status) {
        self.mgr_status.store(status, Ordering::Relaxed);
    }
}

/// Deletes directory and it's contents. Returns false if directory does not exist.
async fn delete_dir(path: &Utf8PathBuf) -> Result<bool> {
    match fs::remove_dir_all(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Get a path to the tenant directory. If you just need to get a timeline directory,
/// use WalResidentTimeline::get_timeline_dir instead.
pub fn get_tenant_dir(conf: &SafeKeeperConf, tenant_id: &TenantId) -> Utf8PathBuf {
    conf.workdir.join(tenant_id.to_string())
}

/// Get a path to the timeline directory. If you need to read WAL files from disk,
/// use WalResidentTimeline::get_timeline_dir instead. This function does not check
/// timeline eviction status and WAL files might not be present on disk.
pub fn get_timeline_dir(conf: &SafeKeeperConf, ttid: &TenantTimelineId) -> Utf8PathBuf {
    get_tenant_dir(conf, &ttid.tenant_id).join(ttid.timeline_id.to_string())
}
