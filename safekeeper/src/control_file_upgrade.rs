//! Code to deal with safekeeper control file upgrades
use crate::{
    safekeeper::{AcceptorState, PgUuid, TermHistory, TermLsn},
    state::{EvictionState, PersistedPeers, TimelinePersistentState},
    wal_backup_partial,
};
use anyhow::{bail, Result};
use pq_proto::SystemId;
use safekeeper_api::{ServerInfo, Term};
use serde::{Deserialize, Serialize};
use tracing::*;
use utils::{
    bin_ser::LeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

/// Persistent consensus state of the acceptor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct AcceptorStateV1 {
    /// acceptor's last term it voted for (advanced in 1 phase)
    term: Term,
    /// acceptor's epoch (advanced, i.e. bumped to 'term' when VCL is reached).
    epoch: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SafeKeeperStateV1 {
    /// persistent acceptor state
    acceptor_state: AcceptorStateV1,
    /// information about server
    server: ServerInfoV2,
    /// Unique id of the last *elected* proposer we dealt with. Not needed
    /// for correctness, exists for monitoring purposes.
    proposer_uuid: PgUuid,
    /// part of WAL acknowledged by quorum and available locally
    commit_lsn: Lsn,
    /// minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone)
    truncate_lsn: Lsn,
    // Safekeeper starts receiving WAL from this LSN, zeros before it ought to
    // be skipped during decoding.
    wal_start_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfoV2 {
    /// Postgres server version
    pub pg_version: u32,
    pub system_id: SystemId,
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub wal_seg_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SafeKeeperStateV2 {
    /// persistent acceptor state
    pub acceptor_state: AcceptorState,
    /// information about server
    pub server: ServerInfoV2,
    /// Unique id of the last *elected* proposer we dealt with. Not needed
    /// for correctness, exists for monitoring purposes.
    pub proposer_uuid: PgUuid,
    /// part of WAL acknowledged by quorum and available locally
    pub commit_lsn: Lsn,
    /// minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone)
    pub truncate_lsn: Lsn,
    // Safekeeper starts receiving WAL from this LSN, zeros before it ought to
    // be skipped during decoding.
    pub wal_start_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfoV3 {
    /// Postgres server version
    pub pg_version: u32,
    pub system_id: SystemId,
    #[serde(with = "hex")]
    pub tenant_id: TenantId,
    #[serde(with = "hex")]
    pub timeline_id: TimelineId,
    pub wal_seg_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SafeKeeperStateV3 {
    /// persistent acceptor state
    pub acceptor_state: AcceptorState,
    /// information about server
    pub server: ServerInfoV3,
    /// Unique id of the last *elected* proposer we dealt with. Not needed
    /// for correctness, exists for monitoring purposes.
    #[serde(with = "hex")]
    pub proposer_uuid: PgUuid,
    /// part of WAL acknowledged by quorum and available locally
    pub commit_lsn: Lsn,
    /// minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone)
    pub truncate_lsn: Lsn,
    // Safekeeper starts receiving WAL from this LSN, zeros before it ought to
    // be skipped during decoding.
    pub wal_start_lsn: Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SafeKeeperStateV4 {
    #[serde(with = "hex")]
    pub tenant_id: TenantId,
    #[serde(with = "hex")]
    pub timeline_id: TimelineId,
    /// persistent acceptor state
    pub acceptor_state: AcceptorState,
    /// information about server
    pub server: ServerInfo,
    /// Unique id of the last *elected* proposer we dealt with. Not needed
    /// for correctness, exists for monitoring purposes.
    #[serde(with = "hex")]
    pub proposer_uuid: PgUuid,
    /// Part of WAL acknowledged by quorum and available locally. Always points
    /// to record boundary.
    pub commit_lsn: Lsn,
    /// First LSN not yet offloaded to s3. Useful to persist to avoid finding
    /// out offloading progress on boot.
    pub s3_wal_lsn: Lsn,
    /// Minimal LSN which may be needed for recovery of some safekeeper (end_lsn
    /// of last record streamed to everyone). Persisting it helps skipping
    /// recovery in walproposer, generally we compute it from peers. In
    /// walproposer proto called 'truncate_lsn'.
    pub peer_horizon_lsn: Lsn,
    /// LSN of the oldest known checkpoint made by pageserver and successfully
    /// pushed to s3. We don't remove WAL beyond it. Persisted only for
    /// informational purposes, we receive it from pageserver (or broker).
    pub remote_consistent_lsn: Lsn,
    // Peers and their state as we remember it. Knowing peers themselves is
    // fundamental; but state is saved here only for informational purposes and
    // obviously can be stale. (Currently not saved at all, but let's provision
    // place to have less file version upgrades).
    pub peers: PersistedPeers,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SafeKeeperStateV7 {
    #[serde(with = "hex")]
    pub tenant_id: TenantId,
    #[serde(with = "hex")]
    pub timeline_id: TimelineId,
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
    // Peers and their state as we remember it. Knowing peers themselves is
    // fundamental; but state is saved here only for informational purposes and
    // obviously can be stale. (Currently not saved at all, but let's provision
    // place to have less file version upgrades).
    pub peers: PersistedPeers,
}

/// Persistent information stored on safekeeper node about timeline.
/// On disk data is prefixed by magic and format version and followed by checksum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SafeKeeperStateV8 {
    #[serde(with = "hex")]
    pub tenant_id: TenantId,
    #[serde(with = "hex")]
    pub timeline_id: TimelineId,
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
    /// Peers and their state as we remember it. Knowing peers themselves is
    /// fundamental; but state is saved here only for informational purposes and
    /// obviously can be stale. (Currently not saved at all, but let's provision
    /// place to have less file version upgrades).
    pub peers: PersistedPeers,
    /// Holds names of partial segments uploaded to remote storage. Used to
    /// clean up old objects without leaving garbage in remote storage.
    pub partial_backup: wal_backup_partial::State,
}

pub fn upgrade_control_file(buf: &[u8], version: u32) -> Result<TimelinePersistentState> {
    // migrate to storing full term history
    if version == 1 {
        info!("reading safekeeper control file version {}", version);
        let oldstate = SafeKeeperStateV1::des(&buf[..buf.len()])?;
        let ac = AcceptorState {
            term: oldstate.acceptor_state.term,
            term_history: TermHistory(vec![TermLsn {
                term: oldstate.acceptor_state.epoch,
                lsn: Lsn(0),
            }]),
        };
        return Ok(TimelinePersistentState {
            tenant_id: oldstate.server.tenant_id,
            timeline_id: oldstate.server.timeline_id,
            acceptor_state: ac,
            server: ServerInfo {
                pg_version: oldstate.server.pg_version,
                system_id: oldstate.server.system_id,
                wal_seg_size: oldstate.server.wal_seg_size,
            },
            proposer_uuid: oldstate.proposer_uuid,
            timeline_start_lsn: Lsn(0),
            local_start_lsn: Lsn(0),
            commit_lsn: oldstate.commit_lsn,
            backup_lsn: Lsn(0),
            peer_horizon_lsn: oldstate.truncate_lsn,
            remote_consistent_lsn: Lsn(0),
            peers: PersistedPeers(vec![]),
            partial_backup: wal_backup_partial::State::default(),
            eviction_state: EvictionState::Present,
        });
    // migrate to hexing some ids
    } else if version == 2 {
        info!("reading safekeeper control file version {}", version);
        let oldstate = SafeKeeperStateV2::des(&buf[..buf.len()])?;
        let server = ServerInfo {
            pg_version: oldstate.server.pg_version,
            system_id: oldstate.server.system_id,
            wal_seg_size: oldstate.server.wal_seg_size,
        };
        return Ok(TimelinePersistentState {
            tenant_id: oldstate.server.tenant_id,
            timeline_id: oldstate.server.timeline_id,
            acceptor_state: oldstate.acceptor_state,
            server,
            proposer_uuid: oldstate.proposer_uuid,
            timeline_start_lsn: Lsn(0),
            local_start_lsn: Lsn(0),
            commit_lsn: oldstate.commit_lsn,
            backup_lsn: Lsn(0),
            peer_horizon_lsn: oldstate.truncate_lsn,
            remote_consistent_lsn: Lsn(0),
            peers: PersistedPeers(vec![]),
            partial_backup: wal_backup_partial::State::default(),
            eviction_state: EvictionState::Present,
        });
    // migrate to moving tenant_id/timeline_id to the top and adding some lsns
    } else if version == 3 {
        info!("reading safekeeper control file version {version}");
        let oldstate = SafeKeeperStateV3::des(&buf[..buf.len()])?;
        let server = ServerInfo {
            pg_version: oldstate.server.pg_version,
            system_id: oldstate.server.system_id,
            wal_seg_size: oldstate.server.wal_seg_size,
        };
        return Ok(TimelinePersistentState {
            tenant_id: oldstate.server.tenant_id,
            timeline_id: oldstate.server.timeline_id,
            acceptor_state: oldstate.acceptor_state,
            server,
            proposer_uuid: oldstate.proposer_uuid,
            timeline_start_lsn: Lsn(0),
            local_start_lsn: Lsn(0),
            commit_lsn: oldstate.commit_lsn,
            backup_lsn: Lsn(0),
            peer_horizon_lsn: oldstate.truncate_lsn,
            remote_consistent_lsn: Lsn(0),
            peers: PersistedPeers(vec![]),
            partial_backup: wal_backup_partial::State::default(),
            eviction_state: EvictionState::Present,
        });
    // migrate to having timeline_start_lsn
    } else if version == 4 {
        info!("reading safekeeper control file version {}", version);
        let oldstate = SafeKeeperStateV4::des(&buf[..buf.len()])?;
        let server = ServerInfo {
            pg_version: oldstate.server.pg_version,
            system_id: oldstate.server.system_id,
            wal_seg_size: oldstate.server.wal_seg_size,
        };
        return Ok(TimelinePersistentState {
            tenant_id: oldstate.tenant_id,
            timeline_id: oldstate.timeline_id,
            acceptor_state: oldstate.acceptor_state,
            server,
            proposer_uuid: oldstate.proposer_uuid,
            timeline_start_lsn: Lsn(0),
            local_start_lsn: Lsn(0),
            commit_lsn: oldstate.commit_lsn,
            backup_lsn: Lsn::INVALID,
            peer_horizon_lsn: oldstate.peer_horizon_lsn,
            remote_consistent_lsn: Lsn(0),
            peers: PersistedPeers(vec![]),
            partial_backup: wal_backup_partial::State::default(),
            eviction_state: EvictionState::Present,
        });
    } else if version == 5 {
        info!("reading safekeeper control file version {}", version);
        let mut oldstate = TimelinePersistentState::des(&buf[..buf.len()])?;
        if oldstate.timeline_start_lsn != Lsn(0) {
            return Ok(oldstate);
        }

        // set special timeline_start_lsn because we don't know the real one
        info!("setting timeline_start_lsn and local_start_lsn to Lsn(1)");
        oldstate.timeline_start_lsn = Lsn(1);
        oldstate.local_start_lsn = Lsn(1);

        return Ok(oldstate);
    } else if version == 6 {
        info!("reading safekeeper control file version {}", version);
        let mut oldstate = TimelinePersistentState::des(&buf[..buf.len()])?;
        if oldstate.server.pg_version != 0 {
            return Ok(oldstate);
        }

        // set pg_version to the default v14
        info!("setting pg_version to 140005");
        oldstate.server.pg_version = 140005;

        return Ok(oldstate);
    } else if version == 7 {
        info!("reading safekeeper control file version {}", version);
        let oldstate = SafeKeeperStateV7::des(&buf[..buf.len()])?;

        return Ok(TimelinePersistentState {
            tenant_id: oldstate.tenant_id,
            timeline_id: oldstate.timeline_id,
            acceptor_state: oldstate.acceptor_state,
            server: oldstate.server,
            proposer_uuid: oldstate.proposer_uuid,
            timeline_start_lsn: oldstate.timeline_start_lsn,
            local_start_lsn: oldstate.local_start_lsn,
            commit_lsn: oldstate.commit_lsn,
            backup_lsn: oldstate.backup_lsn,
            peer_horizon_lsn: oldstate.peer_horizon_lsn,
            remote_consistent_lsn: oldstate.remote_consistent_lsn,
            peers: oldstate.peers,
            partial_backup: wal_backup_partial::State::default(),
            eviction_state: EvictionState::Present,
        });
    } else if version == 8 {
        let oldstate = SafeKeeperStateV8::des(&buf[..buf.len()])?;

        return Ok(TimelinePersistentState {
            tenant_id: oldstate.tenant_id,
            timeline_id: oldstate.timeline_id,
            acceptor_state: oldstate.acceptor_state,
            server: oldstate.server,
            proposer_uuid: oldstate.proposer_uuid,
            timeline_start_lsn: oldstate.timeline_start_lsn,
            local_start_lsn: oldstate.local_start_lsn,
            commit_lsn: oldstate.commit_lsn,
            backup_lsn: oldstate.backup_lsn,
            peer_horizon_lsn: oldstate.peer_horizon_lsn,
            remote_consistent_lsn: oldstate.remote_consistent_lsn,
            peers: oldstate.peers,
            partial_backup: oldstate.partial_backup,
            eviction_state: EvictionState::Present,
        });
    }

    // TODO: persist the file back to the disk after upgrade
    // TODO: think about backward compatibility and rollbacks

    bail!("unsupported safekeeper control file version {}", version)
}

pub fn downgrade_v9_to_v8(state: &TimelinePersistentState) -> SafeKeeperStateV8 {
    assert!(state.eviction_state == EvictionState::Present);
    SafeKeeperStateV8 {
        tenant_id: state.tenant_id,
        timeline_id: state.timeline_id,
        acceptor_state: state.acceptor_state.clone(),
        server: state.server.clone(),
        proposer_uuid: state.proposer_uuid,
        timeline_start_lsn: state.timeline_start_lsn,
        local_start_lsn: state.local_start_lsn,
        commit_lsn: state.commit_lsn,
        backup_lsn: state.backup_lsn,
        peer_horizon_lsn: state.peer_horizon_lsn,
        remote_consistent_lsn: state.remote_consistent_lsn,
        peers: state.peers.clone(),
        partial_backup: state.partial_backup.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use utils::{id::NodeId, Hex};

    use crate::safekeeper::PersistedPeerInfo;

    use super::*;

    #[test]
    fn roundtrip_v1() {
        let tenant_id = TenantId::from_str("cf0480929707ee75372337efaa5ecf96").unwrap();
        let timeline_id = TimelineId::from_str("112ded66422aa5e953e5440fa5427ac4").unwrap();
        let state = SafeKeeperStateV1 {
            acceptor_state: AcceptorStateV1 {
                term: 42,
                epoch: 43,
            },
            server: ServerInfoV2 {
                pg_version: 14,
                system_id: 0x1234567887654321,
                tenant_id,
                timeline_id,
                wal_seg_size: 0x12345678,
            },
            proposer_uuid: {
                let mut arr = timeline_id.as_arr();
                arr.reverse();
                arr
            },
            commit_lsn: Lsn(1234567800),
            truncate_lsn: Lsn(123456780),
            wal_start_lsn: Lsn(1234567800 - 8),
        };

        let ser = state.ser().unwrap();
        #[rustfmt::skip]
        let expected = [
            // term
            0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // epoch
            0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // pg_version
            0x0e, 0x00, 0x00, 0x00,
            // system_id
            0x21, 0x43, 0x65, 0x87, 0x78, 0x56, 0x34, 0x12,
            // tenant_id
            0xcf, 0x04, 0x80, 0x92, 0x97, 0x07, 0xee, 0x75, 0x37, 0x23, 0x37, 0xef, 0xaa, 0x5e, 0xcf, 0x96,
            // timeline_id
            0x11, 0x2d, 0xed, 0x66, 0x42, 0x2a, 0xa5, 0xe9, 0x53, 0xe5, 0x44, 0x0f, 0xa5, 0x42, 0x7a, 0xc4,
            // wal_seg_size
            0x78, 0x56, 0x34, 0x12,
            // proposer_uuid
            0xc4, 0x7a, 0x42, 0xa5, 0x0f, 0x44, 0xe5, 0x53, 0xe9, 0xa5, 0x2a, 0x42, 0x66, 0xed, 0x2d, 0x11,
            // commit_lsn
            0x78, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00,
            // truncate_lsn
            0x0c, 0xcd, 0x5b, 0x07, 0x00, 0x00, 0x00, 0x00,
            // wal_start_lsn
            0x70, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_eq!(Hex(&ser), Hex(&expected));

        let deser = SafeKeeperStateV1::des(&ser).unwrap();

        assert_eq!(state, deser);
    }

    #[test]
    fn roundtrip_v2() {
        let tenant_id = TenantId::from_str("cf0480929707ee75372337efaa5ecf96").unwrap();
        let timeline_id = TimelineId::from_str("112ded66422aa5e953e5440fa5427ac4").unwrap();
        let state = SafeKeeperStateV2 {
            acceptor_state: AcceptorState {
                term: 42,
                term_history: TermHistory(vec![TermLsn {
                    lsn: Lsn(0x1),
                    term: 41,
                }]),
            },
            server: ServerInfoV2 {
                pg_version: 14,
                system_id: 0x1234567887654321,
                tenant_id,
                timeline_id,
                wal_seg_size: 0x12345678,
            },
            proposer_uuid: {
                let mut arr = timeline_id.as_arr();
                arr.reverse();
                arr
            },
            commit_lsn: Lsn(1234567800),
            truncate_lsn: Lsn(123456780),
            wal_start_lsn: Lsn(1234567800 - 8),
        };

        let ser = state.ser().unwrap();
        let expected = [
            0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x21, 0x43, 0x65, 0x87, 0x78, 0x56,
            0x34, 0x12, 0xcf, 0x04, 0x80, 0x92, 0x97, 0x07, 0xee, 0x75, 0x37, 0x23, 0x37, 0xef,
            0xaa, 0x5e, 0xcf, 0x96, 0x11, 0x2d, 0xed, 0x66, 0x42, 0x2a, 0xa5, 0xe9, 0x53, 0xe5,
            0x44, 0x0f, 0xa5, 0x42, 0x7a, 0xc4, 0x78, 0x56, 0x34, 0x12, 0xc4, 0x7a, 0x42, 0xa5,
            0x0f, 0x44, 0xe5, 0x53, 0xe9, 0xa5, 0x2a, 0x42, 0x66, 0xed, 0x2d, 0x11, 0x78, 0x02,
            0x96, 0x49, 0x00, 0x00, 0x00, 0x00, 0x0c, 0xcd, 0x5b, 0x07, 0x00, 0x00, 0x00, 0x00,
            0x70, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_eq!(Hex(&ser), Hex(&expected));

        let deser = SafeKeeperStateV2::des(&ser).unwrap();

        assert_eq!(state, deser);
    }

    #[test]
    fn roundtrip_v3() {
        let tenant_id = TenantId::from_str("cf0480929707ee75372337efaa5ecf96").unwrap();
        let timeline_id = TimelineId::from_str("112ded66422aa5e953e5440fa5427ac4").unwrap();
        let state = SafeKeeperStateV3 {
            acceptor_state: AcceptorState {
                term: 42,
                term_history: TermHistory(vec![TermLsn {
                    lsn: Lsn(0x1),
                    term: 41,
                }]),
            },
            server: ServerInfoV3 {
                pg_version: 14,
                system_id: 0x1234567887654321,
                tenant_id,
                timeline_id,
                wal_seg_size: 0x12345678,
            },
            proposer_uuid: {
                let mut arr = timeline_id.as_arr();
                arr.reverse();
                arr
            },
            commit_lsn: Lsn(1234567800),
            truncate_lsn: Lsn(123456780),
            wal_start_lsn: Lsn(1234567800 - 8),
        };

        let ser = state.ser().unwrap();
        let expected = [
            0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x21, 0x43, 0x65, 0x87, 0x78, 0x56,
            0x34, 0x12, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63, 0x66, 0x30, 0x34,
            0x38, 0x30, 0x39, 0x32, 0x39, 0x37, 0x30, 0x37, 0x65, 0x65, 0x37, 0x35, 0x33, 0x37,
            0x32, 0x33, 0x33, 0x37, 0x65, 0x66, 0x61, 0x61, 0x35, 0x65, 0x63, 0x66, 0x39, 0x36,
            0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x31, 0x32, 0x64, 0x65, 0x64,
            0x36, 0x36, 0x34, 0x32, 0x32, 0x61, 0x61, 0x35, 0x65, 0x39, 0x35, 0x33, 0x65, 0x35,
            0x34, 0x34, 0x30, 0x66, 0x61, 0x35, 0x34, 0x32, 0x37, 0x61, 0x63, 0x34, 0x78, 0x56,
            0x34, 0x12, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63, 0x34, 0x37, 0x61,
            0x34, 0x32, 0x61, 0x35, 0x30, 0x66, 0x34, 0x34, 0x65, 0x35, 0x35, 0x33, 0x65, 0x39,
            0x61, 0x35, 0x32, 0x61, 0x34, 0x32, 0x36, 0x36, 0x65, 0x64, 0x32, 0x64, 0x31, 0x31,
            0x78, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00, 0x0c, 0xcd, 0x5b, 0x07, 0x00, 0x00,
            0x00, 0x00, 0x70, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_eq!(Hex(&ser), Hex(&expected));

        let deser = SafeKeeperStateV3::des(&ser).unwrap();

        assert_eq!(state, deser);
    }

    #[test]
    fn roundtrip_v4() {
        let tenant_id = TenantId::from_str("cf0480929707ee75372337efaa5ecf96").unwrap();
        let timeline_id = TimelineId::from_str("112ded66422aa5e953e5440fa5427ac4").unwrap();
        let state = SafeKeeperStateV4 {
            tenant_id,
            timeline_id,
            acceptor_state: AcceptorState {
                term: 42,
                term_history: TermHistory(vec![TermLsn {
                    lsn: Lsn(0x1),
                    term: 41,
                }]),
            },
            server: ServerInfo {
                pg_version: 14,
                system_id: 0x1234567887654321,
                wal_seg_size: 0x12345678,
            },
            proposer_uuid: {
                let mut arr = timeline_id.as_arr();
                arr.reverse();
                arr
            },
            peers: PersistedPeers(vec![(
                NodeId(1),
                PersistedPeerInfo {
                    backup_lsn: Lsn(1234567000),
                    term: 42,
                    flush_lsn: Lsn(1234567800 - 8),
                    commit_lsn: Lsn(1234567600),
                },
            )]),
            commit_lsn: Lsn(1234567800),
            s3_wal_lsn: Lsn(1234567300),
            peer_horizon_lsn: Lsn(9999999),
            remote_consistent_lsn: Lsn(1234560000),
        };

        let ser = state.ser().unwrap();
        let expected = [
            0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63, 0x66, 0x30, 0x34, 0x38, 0x30,
            0x39, 0x32, 0x39, 0x37, 0x30, 0x37, 0x65, 0x65, 0x37, 0x35, 0x33, 0x37, 0x32, 0x33,
            0x33, 0x37, 0x65, 0x66, 0x61, 0x61, 0x35, 0x65, 0x63, 0x66, 0x39, 0x36, 0x20, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x31, 0x32, 0x64, 0x65, 0x64, 0x36, 0x36,
            0x34, 0x32, 0x32, 0x61, 0x61, 0x35, 0x65, 0x39, 0x35, 0x33, 0x65, 0x35, 0x34, 0x34,
            0x30, 0x66, 0x61, 0x35, 0x34, 0x32, 0x37, 0x61, 0x63, 0x34, 0x2a, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x0e, 0x00, 0x00, 0x00, 0x21, 0x43, 0x65, 0x87, 0x78, 0x56, 0x34, 0x12, 0x78, 0x56,
            0x34, 0x12, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63, 0x34, 0x37, 0x61,
            0x34, 0x32, 0x61, 0x35, 0x30, 0x66, 0x34, 0x34, 0x65, 0x35, 0x35, 0x33, 0x65, 0x39,
            0x61, 0x35, 0x32, 0x61, 0x34, 0x32, 0x36, 0x36, 0x65, 0x64, 0x32, 0x64, 0x31, 0x31,
            0x78, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00, 0x84, 0x00, 0x96, 0x49, 0x00, 0x00,
            0x00, 0x00, 0x7f, 0x96, 0x98, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe4, 0x95, 0x49,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x58, 0xff, 0x95, 0x49, 0x00, 0x00, 0x00, 0x00,
            0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x70, 0x02, 0x96, 0x49, 0x00, 0x00,
            0x00, 0x00, 0xb0, 0x01, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_eq!(Hex(&ser), Hex(&expected));

        let deser = SafeKeeperStateV4::des(&ser).unwrap();

        assert_eq!(state, deser);
    }
}
