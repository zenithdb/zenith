//! Defines per timeline data stored persistently (SafeKeeperPersistentState)
//! and its wrapper with in memory layer (SafekeeperState).

use std::{cmp::max, ops::Deref, time::SystemTime};

use anyhow::{bail, Result};
use postgres_ffi::WAL_SEGMENT_SIZE;
use safekeeper_api::{
    membership::Configuration,
    models::{TimelineMembershipSwitchResponse, TimelineTermBumpResponse},
    ServerInfo, Term, INITIAL_TERM,
};
use serde::{Deserialize, Serialize};
use tracing::info;
use utils::{
    id::{TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
};

use crate::{
    control_file,
    safekeeper::{AcceptorState, PgUuid, TermHistory, TermLsn, UNKNOWN_SERVER_VERSION},
    timeline::TimelineError,
    wal_backup_partial::{self},
};

/// Persistent information stored on safekeeper node about timeline.
/// On disk data is prefixed by magic and format version and followed by checksum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimelinePersistentState {
    #[serde(with = "hex")]
    pub tenant_id: TenantId,
    #[serde(with = "hex")]
    pub timeline_id: TimelineId,
    /// Membership configuration.
    pub mconf: Configuration,
    /// persistent acceptor state
    pub acceptor_state: AcceptorState,
    /// information about server
    pub server: ServerInfo,
    /// Unique id of the last *elected* proposer we dealt with. Not needed
    /// for correctness, exists for monitoring purposes.
    #[serde(with = "hex")]
    pub proposer_uuid: PgUuid,
    /// Since which LSN this timeline generally starts. Safekeeper might have
    /// joined later.
    pub timeline_start_lsn: Lsn,
    /// Since which LSN safekeeper has (had) WAL for this timeline.
    /// All WAL segments next to one containing local_start_lsn are
    /// filled with data from the beginning.
    pub local_start_lsn: Lsn,
    /// Part of WAL acknowledged by quorum *and available locally*. Always points
    /// to record boundary.
    pub commit_lsn: Lsn,
    /// LSN that points to the end of the last backed up segment. Useful to
    /// persist to avoid finding out offloading progress on boot.
    pub backup_lsn: Lsn,
    /// Minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone). Persisting it helps skipping
    /// recovery in walproposer, generally we compute it from peers. In
    /// walproposer proto called 'truncate_lsn'. Updates are currently drived
    /// only by walproposer.
    pub peer_horizon_lsn: Lsn,
    /// LSN of the oldest known checkpoint made by pageserver and successfully
    /// pushed to s3. We don't remove WAL beyond it. Persisted only for
    /// informational purposes, we receive it from pageserver (or broker).
    pub remote_consistent_lsn: Lsn,
    /// Holds names of partial segments uploaded to remote storage. Used to
    /// clean up old objects without leaving garbage in remote storage.
    pub partial_backup: wal_backup_partial::State,
    /// Eviction state of the timeline. If it's Offloaded, we should download
    /// WAL files from remote storage to serve the timeline.
    pub eviction_state: EvictionState,
    pub creation_ts: SystemTime,
}

/// State of the local WAL files. Used to track current timeline state,
/// that can be either WAL files are present on disk or last partial segment
/// is offloaded to remote storage.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum EvictionState {
    /// WAL files are present on disk.
    Present,
    /// Last partial segment is offloaded to remote storage.
    /// Contains flush_lsn of the last offloaded segment.
    Offloaded(Lsn),
}

impl TimelinePersistentState {
    /// commit_lsn is the same as start_lsn in the normal creaiton; see
    /// `TimelineCreateRequest` comments.`
    pub fn new(
        ttid: &TenantTimelineId,
        mconf: Configuration,
        server_info: ServerInfo,
        start_lsn: Lsn,
        commit_lsn: Lsn,
    ) -> anyhow::Result<TimelinePersistentState> {
        if server_info.wal_seg_size == 0 {
            bail!(TimelineError::UninitializedWalSegSize(*ttid));
        }

        if server_info.pg_version == UNKNOWN_SERVER_VERSION {
            bail!(TimelineError::UninitialinzedPgVersion(*ttid));
        }

        if commit_lsn < start_lsn {
            bail!(
                "commit_lsn {} is smaller than start_lsn {}",
                commit_lsn,
                start_lsn
            );
        }

        // If we are given with init LSN, initialize term history with it. It
        // ensures that walproposer always must be able to find a common point
        // in histories; if it can't something is corrupted. Not having LSN here
        // is so far left for legacy case where timeline is created by compute
        // and LSN during creation is not known yet.
        let term_history = if commit_lsn != Lsn::INVALID {
            TermHistory(vec![TermLsn {
                term: INITIAL_TERM,
                lsn: start_lsn,
            }])
        } else {
            TermHistory::empty()
        };

        Ok(TimelinePersistentState {
            tenant_id: ttid.tenant_id,
            timeline_id: ttid.timeline_id,
            mconf,
            acceptor_state: AcceptorState {
                term: INITIAL_TERM,
                term_history,
            },
            server: server_info,
            proposer_uuid: [0; 16],
            timeline_start_lsn: start_lsn,
            local_start_lsn: start_lsn,
            commit_lsn,
            backup_lsn: start_lsn,
            peer_horizon_lsn: start_lsn,
            remote_consistent_lsn: Lsn(0),
            partial_backup: wal_backup_partial::State::default(),
            eviction_state: EvictionState::Present,
            creation_ts: SystemTime::now(),
        })
    }

    pub fn empty() -> Self {
        TimelinePersistentState::new(
            &TenantTimelineId::empty(),
            Configuration::empty(),
            ServerInfo {
                pg_version: 170000, /* Postgres server version (major * 10000) */
                system_id: 0,       /* Postgres system identifier */
                wal_seg_size: WAL_SEGMENT_SIZE as u32,
            },
            Lsn::INVALID,
            Lsn::INVALID,
        )
        .unwrap()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
// In memory safekeeper state. Fields mirror ones in `SafeKeeperPersistentState`; values
// are not flushed yet.
pub struct TimelineMemState {
    pub commit_lsn: Lsn,
    pub backup_lsn: Lsn,
    pub peer_horizon_lsn: Lsn,
    pub remote_consistent_lsn: Lsn,
    #[serde(with = "hex")]
    pub proposer_uuid: PgUuid,
}

/// Safekeeper persistent state plus in memory layer.
///
/// Allows us to avoid frequent fsyncs when we update fields like commit_lsn
/// which don't need immediate persistence. Provides transactional like API
/// to atomically update the state.
///
/// Implements Deref into *persistent* part.
pub struct TimelineState<CTRL: control_file::Storage> {
    pub inmem: TimelineMemState,
    pub pers: CTRL, // persistent
}

impl<CTRL> TimelineState<CTRL>
where
    CTRL: control_file::Storage,
{
    pub fn new(state: CTRL) -> Self {
        TimelineState {
            inmem: TimelineMemState {
                commit_lsn: state.commit_lsn,
                backup_lsn: state.backup_lsn,
                peer_horizon_lsn: state.peer_horizon_lsn,
                remote_consistent_lsn: state.remote_consistent_lsn,
                proposer_uuid: state.proposer_uuid,
            },
            pers: state,
        }
    }

    /// Start atomic change. Returns SafeKeeperPersistentState with in memory
    /// values applied; the protocol is to 1) change returned struct as desired
    /// 2) atomically persist it with finish_change.
    pub fn start_change(&self) -> TimelinePersistentState {
        let mut s = self.pers.clone();
        s.commit_lsn = self.inmem.commit_lsn;
        s.backup_lsn = self.inmem.backup_lsn;
        s.peer_horizon_lsn = self.inmem.peer_horizon_lsn;
        s.remote_consistent_lsn = self.inmem.remote_consistent_lsn;
        s.proposer_uuid = self.inmem.proposer_uuid;
        s
    }

    /// Persist given state. c.f. start_change.
    pub async fn finish_change(&mut self, s: &TimelinePersistentState) -> Result<()> {
        if s.eq(&*self.pers) {
            // nothing to do if state didn't change
        } else {
            self.pers.persist(s).await?;
        }

        // keep in memory values up to date
        self.inmem.commit_lsn = s.commit_lsn;
        self.inmem.backup_lsn = s.backup_lsn;
        self.inmem.peer_horizon_lsn = s.peer_horizon_lsn;
        self.inmem.remote_consistent_lsn = s.remote_consistent_lsn;
        self.inmem.proposer_uuid = s.proposer_uuid;
        Ok(())
    }

    /// Flush in memory values.
    pub async fn flush(&mut self) -> Result<()> {
        let s = self.start_change();
        self.finish_change(&s).await
    }

    /// Make term at least as `to`. If `to` is None, increment current one. This
    /// is not in safekeeper.rs because we want to be able to do it even if
    /// timeline is offloaded.
    pub async fn term_bump(&mut self, to: Option<Term>) -> Result<TimelineTermBumpResponse> {
        let before = self.acceptor_state.term;
        let mut state = self.start_change();
        let new = match to {
            Some(to) => max(state.acceptor_state.term, to),
            None => state.acceptor_state.term + 1,
        };
        if new > state.acceptor_state.term {
            state.acceptor_state.term = new;
            self.finish_change(&state).await?;
        }
        let after = self.acceptor_state.term;
        Ok(TimelineTermBumpResponse {
            previous_term: before,
            current_term: after,
        })
    }

    /// Switch into membership configuration `to` if it is higher than the
    /// current one.
    pub async fn membership_switch(
        &mut self,
        to: Configuration,
    ) -> Result<TimelineMembershipSwitchResponse> {
        let before = self.mconf.clone();
        // Is switch allowed?
        if to.generation <= self.mconf.generation {
            info!(
                "ignoring request to switch membership conf to lower {}, current conf {}",
                to, self.mconf
            );
        } else {
            let mut state = self.start_change();
            state.mconf = to.clone();
            self.finish_change(&state).await?;
            info!("switched membership conf to {} from {}", to, before);
        }
        Ok(TimelineMembershipSwitchResponse {
            previous_conf: before,
            current_conf: self.mconf.clone(),
        })
    }
}

impl<CTRL> Deref for TimelineState<CTRL>
where
    CTRL: control_file::Storage,
{
    type Target = TimelinePersistentState;

    fn deref(&self) -> &Self::Target {
        &self.pers
    }
}
