use std::sync::Arc;

use crate::rate_limit::RateLimiter;
use crate::safekeeper::{ProposerAcceptorMessage, ProposerElected, SafeKeeper, TermHistory};
use crate::state::{TimelinePersistentState, TimelineState};
use crate::timeline::{get_timeline_dir, SharedState, StateSK, Timeline};
use crate::timelines_set::TimelinesSet;
use crate::wal_backup::{remote_timeline_path, WalBackup};
use crate::{control_file, wal_storage, SafeKeeperConf};
use camino_tempfile::Utf8TempDir;
use tokio::fs::create_dir_all;
use utils::id::{NodeId, TenantTimelineId};
use utils::lsn::Lsn;

/// A Safekeeper testing or benchmarking environment. Uses a tempdir for storage, removed on drop.
pub struct Env {
    /// Whether to enable fsync.
    pub fsync: bool,
    /// Benchmark directory. Deleted when dropped.
    pub tempdir: Utf8TempDir,
}

impl Env {
    /// Creates a new test or benchmarking environment in a temporary directory. fsync controls whether to
    /// enable fsyncing.
    pub fn new(fsync: bool) -> anyhow::Result<Self> {
        let tempdir = camino_tempfile::tempdir()?;
        Ok(Self { fsync, tempdir })
    }

    /// Constructs a Safekeeper config for the given node ID.
    fn make_conf(&self, node_id: NodeId) -> SafeKeeperConf {
        let mut conf = SafeKeeperConf::dummy();
        conf.my_id = node_id;
        conf.no_sync = !self.fsync;
        conf.workdir = self.tempdir.path().join(format!("safekeeper-{node_id}"));
        conf
    }

    /// Constructs a Safekeeper with the given node and tenant/timeline ID.
    ///
    /// TODO: we should support using in-memory storage, to measure non-IO costs. This would be
    /// easier if SafeKeeper used trait objects for storage rather than generics. It's also not
    /// currently possible to construct a timeline using non-file storage since StateSK only accepts
    /// SafeKeeper<control_file::FileStorage, wal_storage::PhysicalStorage>.
    pub async fn make_safekeeper(
        &self,
        node_id: NodeId,
        ttid: TenantTimelineId,
        start_lsn: Lsn,
    ) -> anyhow::Result<SafeKeeper<control_file::FileStorage, wal_storage::PhysicalStorage>> {
        let conf = self.make_conf(node_id);

        let timeline_dir = get_timeline_dir(&conf, &ttid);
        create_dir_all(&timeline_dir).await?;

        let mut pstate = TimelinePersistentState::empty();
        pstate.tenant_id = ttid.tenant_id;
        pstate.timeline_id = ttid.timeline_id;

        let wal = wal_storage::PhysicalStorage::new(&ttid, &timeline_dir, &pstate, conf.no_sync)?;
        let ctrl =
            control_file::FileStorage::create_new(&timeline_dir, pstate, conf.no_sync).await?;
        let state = TimelineState::new(ctrl);
        let mut safekeeper = SafeKeeper::new(state, wal, conf.my_id)?;

        // Emulate an initial election.
        safekeeper
            .process_msg(&ProposerAcceptorMessage::Elected(ProposerElected {
                term: 1,
                start_streaming_at: start_lsn,
                term_history: TermHistory(vec![(1, start_lsn).into()]),
                timeline_start_lsn: start_lsn,
            }))
            .await?;

        Ok(safekeeper)
    }

    /// Constructs a timeline, including a new Safekeeper with the given node ID, and spawns its
    /// manager task.
    pub async fn make_timeline(
        &self,
        node_id: NodeId,
        ttid: TenantTimelineId,
        start_lsn: Lsn,
    ) -> anyhow::Result<Arc<Timeline>> {
        let conf = Arc::new(self.make_conf(node_id));
        let timeline_dir = get_timeline_dir(&conf, &ttid);
        let remote_path = remote_timeline_path(&ttid)?;

        let safekeeper = self.make_safekeeper(node_id, ttid, start_lsn).await?;
        let shared_state = SharedState::new(StateSK::Loaded(safekeeper));

        let wal_backup = Arc::new(WalBackup::new(&conf).await?);

        let timeline = Timeline::new(
            ttid,
            &timeline_dir,
            &remote_path,
            shared_state,
            conf.clone(),
            wal_backup.clone(),
        );
        timeline.bootstrap(
            &mut timeline.write_shared_state().await,
            &conf,
            Arc::new(TimelinesSet::default()), // ignored for now
            RateLimiter::new(0, 0),
            wal_backup,
        );
        Ok(timeline)
    }
}
