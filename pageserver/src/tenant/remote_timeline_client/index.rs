//! In-memory index to track the tenant files on the remote storage.
//!
//! Able to restore itself from the storage index parts, that are located in every timeline's remote directory and contain all data about
//! remote timeline layers and its metadata.

use std::collections::HashMap;

use chrono::NaiveDateTime;
use pageserver_api::models::AuxFilePolicy;
use serde::{Deserialize, Serialize};

use super::is_same_remote_layer_path;
use crate::tenant::metadata::TimelineMetadata;
use crate::tenant::storage_layer::LayerName;
use crate::tenant::timeline::import_pgdata;
use crate::tenant::Generation;
use pageserver_api::shard::ShardIndex;
use utils::id::TimelineId;
use utils::lsn::Lsn;

/// In-memory representation of an `index_part.json` file
///
/// Contains the data about all files in the timeline, present remotely and its metadata.
///
/// This type needs to be backwards and forwards compatible. When changing the fields,
/// remember to add a test case for the changed version.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct IndexPart {
    /// Debugging aid describing the version of this type.
    #[serde(default)]
    version: usize,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<NaiveDateTime>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<NaiveDateTime>,

    /// This field supports import-from-pgdata ("fast imports" platform feature).
    /// We don't currently use fast imports, so, this field is None for all production timelines.
    /// See <https://github.com/neondatabase/neon/pull/9218> for more information.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub import_pgdata: Option<import_pgdata::index_part_format::Root>,

    /// Layer filenames and metadata. For an index persisted in remote storage, all layers must
    /// exist in remote storage.
    pub layer_metadata: HashMap<LayerName, LayerFileMetadata>,

    /// Because of the trouble of eyeballing the legacy "metadata" field, we copied the
    /// "disk_consistent_lsn" out. After version 7 this is no longer needed, but the name cannot be
    /// reused.
    pub(super) disk_consistent_lsn: Lsn,

    // TODO: rename as "metadata" next week, keep the alias = "metadata_bytes", bump version Adding
    // the "alias = metadata" was forgotten in #7693, so we have to use "rewrite = metadata_bytes"
    // for backwards compatibility.
    #[serde(
        rename = "metadata_bytes",
        alias = "metadata",
        with = "crate::tenant::metadata::modern_serde"
    )]
    pub metadata: TimelineMetadata,

    #[serde(default)]
    pub(crate) lineage: Lineage,

    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) gc_blocking: Option<GcBlocking>,

    /// Describes the kind of aux files stored in the timeline.
    ///
    /// The value is modified during file ingestion when the latest wanted value communicated via tenant config is applied if it is acceptable.
    /// A V1 setting after V2 files have been committed is not accepted.
    ///
    /// None means no aux files have been written to the storage before the point
    /// when this flag is introduced.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) last_aux_file_policy: Option<AuxFilePolicy>,
}

impl IndexPart {
    /// When adding or modifying any parts of `IndexPart`, increment the version so that it can be
    /// used to understand later versions.
    ///
    /// Version is currently informative only.
    /// Version history
    /// - 2: added `deleted_at`
    /// - 3: no longer deserialize `timeline_layers` (serialized format is the same, but timeline_layers
    ///      is always generated from the keys of `layer_metadata`)
    /// - 4: timeline_layers is fully removed.
    /// - 5: lineage was added
    /// - 6: last_aux_file_policy is added.
    /// - 7: metadata_bytes is no longer written, but still read
    /// - 8: added `archived_at`
    /// - 9: +gc_blocking
    /// - 10: +import_pgdata
    const LATEST_VERSION: usize = 10;

    // Versions we may see when reading from a bucket.
    pub const KNOWN_VERSIONS: &'static [usize] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    pub const FILE_NAME: &'static str = "index_part.json";

    pub fn empty(metadata: TimelineMetadata) -> Self {
        IndexPart {
            version: Self::LATEST_VERSION,
            layer_metadata: Default::default(),
            disk_consistent_lsn: metadata.disk_consistent_lsn(),
            metadata,
            deleted_at: None,
            archived_at: None,
            lineage: Default::default(),
            gc_blocking: None,
            last_aux_file_policy: None,
            import_pgdata: None,
        }
    }

    pub fn version(&self) -> usize {
        self.version
    }

    /// If you want this under normal operations, read it from self.metadata:
    /// this method is just for the scrubber to use when validating an index.
    pub fn duplicated_disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn
    }

    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice::<IndexPart>(bytes)
    }

    pub fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    #[cfg(test)]
    pub(crate) fn example() -> Self {
        Self::empty(TimelineMetadata::example())
    }

    /// Returns true if the index contains a reference to the given layer (i.e. file path).
    ///
    /// TODO: there should be a variant of LayerName for the physical remote path that contains
    /// information about the shard and generation, to avoid passing in metadata.
    pub fn references(&self, name: &LayerName, metadata: &LayerFileMetadata) -> bool {
        let Some(index_metadata) = self.layer_metadata.get(name) else {
            return false;
        };
        is_same_remote_layer_path(name, metadata, name, index_metadata)
    }
}

/// Metadata gathered for each of the layer files.
///
/// Fields have to be `Option`s because remote [`IndexPart`]'s can be from different version, which
/// might have less or more metadata depending if upgrading or rolling back an upgrade.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LayerFileMetadata {
    pub file_size: u64,

    #[serde(default = "Generation::none")]
    #[serde(skip_serializing_if = "Generation::is_none")]
    pub generation: Generation,

    #[serde(default = "ShardIndex::unsharded")]
    #[serde(skip_serializing_if = "ShardIndex::is_unsharded")]
    pub shard: ShardIndex,
}

impl LayerFileMetadata {
    pub fn new(file_size: u64, generation: Generation, shard: ShardIndex) -> Self {
        LayerFileMetadata {
            file_size,
            generation,
            shard,
        }
    }
}

/// Limited history of earlier ancestors.
///
/// A timeline can have more than 1 earlier ancestor, in the rare case that it was repeatedly
/// reparented by having an later timeline be detached from it's ancestor.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default)]
pub(crate) struct Lineage {
    /// Has the `reparenting_history` been truncated to [`Lineage::REMEMBER_AT_MOST`].
    #[serde(skip_serializing_if = "is_false", default)]
    reparenting_history_truncated: bool,

    /// Earlier ancestors, truncated when [`Self::reparenting_history_truncated`]
    ///
    /// These are stored in case we want to support WAL based DR on the timeline. There can be many
    /// of these and at most one [`Self::original_ancestor`]. There cannot be more reparentings
    /// after [`Self::original_ancestor`] has been set.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    reparenting_history: Vec<TimelineId>,

    /// The ancestor from which this timeline has been detached from and when.
    ///
    /// If you are adding support for detaching from a hierarchy, consider changing the ancestry
    /// into a `Vec<(TimelineId, Lsn)>` to be a path instead.
    // FIXME: this is insufficient even for path of two timelines for future wal recovery
    // purposes:
    //
    // assuming a "old main" which has received most of the WAL, and has a branch "new main",
    // starting a bit before "old main" last_record_lsn. the current version works fine,
    // because we will know to replay wal and branch at the recorded Lsn to do wal recovery.
    //
    // then assuming "new main" would similarly receive a branch right before its last_record_lsn,
    // "new new main". the current implementation would just store ("new main", ancestor_lsn, _)
    // here. however, we cannot recover from WAL using only that information, we would need the
    // whole ancestry here:
    //
    // ```json
    // [
    //   ["old main", ancestor_lsn("new main"), _],
    //   ["new main", ancestor_lsn("new new main"), _]
    // ]
    // ```
    #[serde(skip_serializing_if = "Option::is_none", default)]
    original_ancestor: Option<(TimelineId, Lsn, NaiveDateTime)>,
}

fn is_false(b: &bool) -> bool {
    !b
}

impl Lineage {
    const REMEMBER_AT_MOST: usize = 100;

    pub(crate) fn record_previous_ancestor(&mut self, old_ancestor: &TimelineId) -> bool {
        if self.reparenting_history.last() == Some(old_ancestor) {
            // do not re-record it
            false
        } else {
            #[cfg(feature = "testing")]
            {
                let existing = self
                    .reparenting_history
                    .iter()
                    .position(|x| x == old_ancestor);
                assert_eq!(
                    existing, None,
                    "we cannot reparent onto and off and onto the same timeline twice"
                );
            }
            let drop_oldest = self.reparenting_history.len() + 1 >= Self::REMEMBER_AT_MOST;

            self.reparenting_history_truncated |= drop_oldest;
            if drop_oldest {
                self.reparenting_history.remove(0);
            }
            self.reparenting_history.push(*old_ancestor);
            true
        }
    }

    /// Returns true if anything changed.
    pub(crate) fn record_detaching(&mut self, branchpoint: &(TimelineId, Lsn)) -> bool {
        if let Some((id, lsn, _)) = self.original_ancestor {
            assert_eq!(
                &(id, lsn),
                branchpoint,
                "detaching attempt has to be for the same ancestor we are already detached from"
            );
            false
        } else {
            self.original_ancestor =
                Some((branchpoint.0, branchpoint.1, chrono::Utc::now().naive_utc()));
            true
        }
    }

    /// The queried lsn is most likely the basebackup lsn, and this answers question "is it allowed
    /// to start a read/write primary at this lsn".
    ///
    /// Returns true if the Lsn was previously our branch point.
    pub(crate) fn is_previous_ancestor_lsn(&self, lsn: Lsn) -> bool {
        self.original_ancestor
            .is_some_and(|(_, ancestor_lsn, _)| ancestor_lsn == lsn)
    }

    /// Returns true if the timeline originally had an ancestor, and no longer has one.
    pub(crate) fn is_detached_from_ancestor(&self) -> bool {
        self.original_ancestor.is_some()
    }

    /// Returns original ancestor timeline id and lsn that this timeline has been detached from.
    pub(crate) fn detached_previous_ancestor(&self) -> Option<(TimelineId, Lsn)> {
        self.original_ancestor.map(|(id, lsn, _)| (id, lsn))
    }

    pub(crate) fn is_reparented(&self) -> bool {
        !self.reparenting_history.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GcBlocking {
    pub(crate) started_at: NaiveDateTime,
    pub(crate) reasons: enumset::EnumSet<GcBlockingReason>,
}

#[derive(Debug, enumset::EnumSetType, serde::Serialize, serde::Deserialize)]
#[enumset(serialize_repr = "list")]
pub(crate) enum GcBlockingReason {
    Manual,
    DetachAncestor,
}

impl GcBlocking {
    pub(super) fn started_now_for(reason: GcBlockingReason) -> Self {
        GcBlocking {
            started_at: chrono::Utc::now().naive_utc(),
            reasons: enumset::EnumSet::only(reason),
        }
    }

    /// Returns true if the given reason is one of the reasons why the gc is blocked.
    pub(crate) fn blocked_by(&self, reason: GcBlockingReason) -> bool {
        self.reasons.contains(reason)
    }

    /// Returns a version of self with the given reason.
    pub(super) fn with_reason(&self, reason: GcBlockingReason) -> Self {
        assert!(!self.blocked_by(reason));
        let mut reasons = self.reasons;
        reasons.insert(reason);

        Self {
            started_at: self.started_at,
            reasons,
        }
    }

    /// Returns a version of self without the given reason. Assumption is that if
    /// there are no more reasons, we can unblock the gc by returning `None`.
    pub(super) fn without_reason(&self, reason: GcBlockingReason) -> Option<Self> {
        assert!(self.blocked_by(reason));

        if self.reasons.len() == 1 {
            None
        } else {
            let mut reasons = self.reasons;
            assert!(reasons.remove(reason));
            assert!(!reasons.is_empty());

            Some(Self {
                started_at: self.started_at,
                reasons,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use utils::id::TimelineId;

    #[test]
    fn v1_indexpart_is_parsed() {
        let example = r#"{
            "version":1,
            "timeline_layers":["000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9"],
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        }"#;

        let expected = IndexPart {
            // note this is not verified, could be anything, but exists for humans debugging.. could be the git version instead?
            version: 1,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), LayerFileMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), LayerFileMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: None,
            archived_at: None,
            lineage: Lineage::default(),
            gc_blocking: None,
            last_aux_file_policy: None,
            import_pgdata: None,
        };

        let part = IndexPart::from_json_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v1_indexpart_is_parsed_with_optional_missing_layers() {
        let example = r#"{
            "version":1,
            "timeline_layers":["000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9"],
            "missing_layers":["This shouldn't fail deserialization"],
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        }"#;

        let expected = IndexPart {
            // note this is not verified, could be anything, but exists for humans debugging.. could be the git version instead?
            version: 1,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), LayerFileMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), LayerFileMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: None,
            archived_at: None,
            lineage: Lineage::default(),
            gc_blocking: None,
            last_aux_file_policy: None,
            import_pgdata: None,
        };

        let part = IndexPart::from_json_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v2_indexpart_is_parsed_with_deleted_at() {
        let example = r#"{
            "version":2,
            "timeline_layers":["000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9"],
            "missing_layers":["This shouldn't fail deserialization"],
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            "deleted_at": "2023-07-31T09:00:00.123"
        }"#;

        let expected = IndexPart {
            // note this is not verified, could be anything, but exists for humans debugging.. could be the git version instead?
            version: 2,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), LayerFileMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), LayerFileMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: Some(parse_naive_datetime("2023-07-31T09:00:00.123000000")),
            archived_at: None,
            lineage: Lineage::default(),
            gc_blocking: None,
            last_aux_file_policy: None,
            import_pgdata: None,
        };

        let part = IndexPart::from_json_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn empty_layers_are_parsed() {
        let empty_layers_json = r#"{
            "version":1,
            "timeline_layers":[],
            "layer_metadata":{},
            "disk_consistent_lsn":"0/2532648",
            "metadata_bytes":[136,151,49,208,0,70,0,4,0,0,0,0,2,83,38,72,1,0,0,0,0,2,83,38,32,1,87,198,240,135,97,119,45,125,38,29,155,161,140,141,255,210,0,0,0,0,2,83,38,72,0,0,0,0,1,73,240,192,0,0,0,0,1,73,240,192,0,0,0,15,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        }"#;

        let expected = IndexPart {
            version: 1,
            layer_metadata: HashMap::new(),
            disk_consistent_lsn: "0/2532648".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[
                136, 151, 49, 208, 0, 70, 0, 4, 0, 0, 0, 0, 2, 83, 38, 72, 1, 0, 0, 0, 0, 2, 83,
                38, 32, 1, 87, 198, 240, 135, 97, 119, 45, 125, 38, 29, 155, 161, 140, 141, 255,
                210, 0, 0, 0, 0, 2, 83, 38, 72, 0, 0, 0, 0, 1, 73, 240, 192, 0, 0, 0, 0, 1, 73,
                240, 192, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0,
            ])
            .unwrap(),
            deleted_at: None,
            archived_at: None,
            lineage: Lineage::default(),
            gc_blocking: None,
            last_aux_file_policy: None,
            import_pgdata: None,
        };

        let empty_layers_parsed = IndexPart::from_json_bytes(empty_layers_json.as_bytes()).unwrap();

        assert_eq!(empty_layers_parsed, expected);
    }

    #[test]
    fn v4_indexpart_is_parsed() {
        let example = r#"{
            "version":4,
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            "deleted_at": "2023-07-31T09:00:00.123"
        }"#;

        let expected = IndexPart {
            version: 4,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), LayerFileMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), LayerFileMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: Some(parse_naive_datetime("2023-07-31T09:00:00.123000000")),
            archived_at: None,
            lineage: Lineage::default(),
            gc_blocking: None,
            last_aux_file_policy: None,
            import_pgdata: None,
        };

        let part = IndexPart::from_json_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v5_indexpart_is_parsed() {
        let example = r#"{
            "version":5,
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014EF420-00000000014EF499":{"file_size":23289856,"generation":1},
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014EF499-00000000015A7619":{"file_size":1015808,"generation":1}},
                "disk_consistent_lsn":"0/15A7618",
                "metadata_bytes":[226,88,25,241,0,46,0,4,0,0,0,0,1,90,118,24,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,78,244,32,0,0,0,0,1,78,244,32,0,0,0,16,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                "lineage":{
                    "original_ancestor":["e2bfd8c633d713d279e6fcd2bcc15b6d","0/15A7618","2024-05-07T18:52:36.322426563"],
                    "reparenting_history":["e1bfd8c633d713d279e6fcd2bcc15b6d"]
                }
        }"#;

        let expected = IndexPart {
            version: 5,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014EF420-00000000014EF499".parse().unwrap(), LayerFileMetadata {
                    file_size: 23289856,
                    generation: Generation::new(1),
                    shard: ShardIndex::unsharded(),
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014EF499-00000000015A7619".parse().unwrap(), LayerFileMetadata {
                    file_size: 1015808,
                    generation: Generation::new(1),
                    shard: ShardIndex::unsharded(),
                })
            ]),
            disk_consistent_lsn: Lsn::from_str("0/15A7618").unwrap(),
            metadata: TimelineMetadata::from_bytes(&[226,88,25,241,0,46,0,4,0,0,0,0,1,90,118,24,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,78,244,32,0,0,0,0,1,78,244,32,0,0,0,16,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: None,
            archived_at: None,
            lineage: Lineage {
                reparenting_history_truncated: false,
                reparenting_history: vec![TimelineId::from_str("e1bfd8c633d713d279e6fcd2bcc15b6d").unwrap()],
                original_ancestor: Some((TimelineId::from_str("e2bfd8c633d713d279e6fcd2bcc15b6d").unwrap(), Lsn::from_str("0/15A7618").unwrap(), parse_naive_datetime("2024-05-07T18:52:36.322426563"))),
            },
            gc_blocking: None,
            last_aux_file_policy: None,
            import_pgdata: None,
        };

        let part = IndexPart::from_json_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v6_indexpart_is_parsed() {
        let example = r#"{
            "version":6,
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            "deleted_at": "2023-07-31T09:00:00.123",
            "lineage":{
                "original_ancestor":["e2bfd8c633d713d279e6fcd2bcc15b6d","0/15A7618","2024-05-07T18:52:36.322426563"],
                "reparenting_history":["e1bfd8c633d713d279e6fcd2bcc15b6d"]
            },
            "last_aux_file_policy": "V2"
        }"#;

        let expected = IndexPart {
            version: 6,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), LayerFileMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), LayerFileMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: Some(parse_naive_datetime("2023-07-31T09:00:00.123000000")),
            archived_at: None,
            lineage: Lineage {
                reparenting_history_truncated: false,
                reparenting_history: vec![TimelineId::from_str("e1bfd8c633d713d279e6fcd2bcc15b6d").unwrap()],
                original_ancestor: Some((TimelineId::from_str("e2bfd8c633d713d279e6fcd2bcc15b6d").unwrap(), Lsn::from_str("0/15A7618").unwrap(), parse_naive_datetime("2024-05-07T18:52:36.322426563"))),
            },
            gc_blocking: None,
            last_aux_file_policy: Some(AuxFilePolicy::V2),
            import_pgdata: None,
        };

        let part = IndexPart::from_json_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v7_indexpart_is_parsed() {
        let example = r#"{
            "version": 7,
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata": {
                "disk_consistent_lsn": "0/16960E8",
                "prev_record_lsn": "0/1696070",
                "ancestor_timeline": "e45a7f37d3ee2ff17dc14bf4f4e3f52e",
                "ancestor_lsn": "0/0",
                "latest_gc_cutoff_lsn": "0/1696070",
                "initdb_lsn": "0/1696070",
                "pg_version": 14
            },
            "deleted_at": "2023-07-31T09:00:00.123"
        }"#;

        let expected = IndexPart {
            version: 7,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), LayerFileMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), LayerFileMetadata {
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::new(
                Lsn::from_str("0/16960E8").unwrap(),
                Some(Lsn::from_str("0/1696070").unwrap()),
                Some(TimelineId::from_str("e45a7f37d3ee2ff17dc14bf4f4e3f52e").unwrap()),
                Lsn::INVALID,
                Lsn::from_str("0/1696070").unwrap(),
                Lsn::from_str("0/1696070").unwrap(),
                14,
            ).with_recalculated_checksum().unwrap(),
            deleted_at: Some(parse_naive_datetime("2023-07-31T09:00:00.123000000")),
            archived_at: None,
            lineage: Default::default(),
            gc_blocking: None,
            last_aux_file_policy: Default::default(),
            import_pgdata: None,
        };

        let part = IndexPart::from_json_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v8_indexpart_is_parsed() {
        let example = r#"{
            "version": 8,
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata": {
                "disk_consistent_lsn": "0/16960E8",
                "prev_record_lsn": "0/1696070",
                "ancestor_timeline": "e45a7f37d3ee2ff17dc14bf4f4e3f52e",
                "ancestor_lsn": "0/0",
                "latest_gc_cutoff_lsn": "0/1696070",
                "initdb_lsn": "0/1696070",
                "pg_version": 14
            },
            "deleted_at": "2023-07-31T09:00:00.123",
            "archived_at": "2023-04-29T09:00:00.123"
        }"#;

        let expected = IndexPart {
            version: 8,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), LayerFileMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), LayerFileMetadata {
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::new(
                Lsn::from_str("0/16960E8").unwrap(),
                Some(Lsn::from_str("0/1696070").unwrap()),
                Some(TimelineId::from_str("e45a7f37d3ee2ff17dc14bf4f4e3f52e").unwrap()),
                Lsn::INVALID,
                Lsn::from_str("0/1696070").unwrap(),
                Lsn::from_str("0/1696070").unwrap(),
                14,
            ).with_recalculated_checksum().unwrap(),
            deleted_at: Some(parse_naive_datetime("2023-07-31T09:00:00.123000000")),
            archived_at: Some(parse_naive_datetime("2023-04-29T09:00:00.123000000")),
            lineage: Default::default(),
            gc_blocking: None,
            last_aux_file_policy: Default::default(),
            import_pgdata: None,
        };

        let part = IndexPart::from_json_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v9_indexpart_is_parsed() {
        let example = r#"{
            "version": 9,
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata": {
                "disk_consistent_lsn": "0/16960E8",
                "prev_record_lsn": "0/1696070",
                "ancestor_timeline": "e45a7f37d3ee2ff17dc14bf4f4e3f52e",
                "ancestor_lsn": "0/0",
                "latest_gc_cutoff_lsn": "0/1696070",
                "initdb_lsn": "0/1696070",
                "pg_version": 14
            },
            "gc_blocking": {
                "started_at": "2024-07-19T09:00:00.123",
                "reasons": ["DetachAncestor"]
            }
        }"#;

        let expected = IndexPart {
            version: 9,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), LayerFileMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), LayerFileMetadata {
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::new(
                Lsn::from_str("0/16960E8").unwrap(),
                Some(Lsn::from_str("0/1696070").unwrap()),
                Some(TimelineId::from_str("e45a7f37d3ee2ff17dc14bf4f4e3f52e").unwrap()),
                Lsn::INVALID,
                Lsn::from_str("0/1696070").unwrap(),
                Lsn::from_str("0/1696070").unwrap(),
                14,
            ).with_recalculated_checksum().unwrap(),
            deleted_at: None,
            lineage: Default::default(),
            gc_blocking: Some(GcBlocking {
                started_at: parse_naive_datetime("2024-07-19T09:00:00.123000000"),
                reasons: enumset::EnumSet::from_iter([GcBlockingReason::DetachAncestor]),
            }),
            last_aux_file_policy: Default::default(),
            archived_at: None,
            import_pgdata: None,
        };

        let part = IndexPart::from_json_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v10_importpgdata_is_parsed() {
        let example = r#"{
            "version": 10,
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata": {
                "disk_consistent_lsn": "0/16960E8",
                "prev_record_lsn": "0/1696070",
                "ancestor_timeline": "e45a7f37d3ee2ff17dc14bf4f4e3f52e",
                "ancestor_lsn": "0/0",
                "latest_gc_cutoff_lsn": "0/1696070",
                "initdb_lsn": "0/1696070",
                "pg_version": 14
            },
            "gc_blocking": {
                "started_at": "2024-07-19T09:00:00.123",
                "reasons": ["DetachAncestor"]
            },
            "import_pgdata": {
                "V1": {
                    "Done": {
                        "idempotency_key": "specified-by-client-218a5213-5044-4562-a28d-d024c5f057f5",
                        "started_at": "2024-11-13T09:23:42.123",
                        "finished_at": "2024-11-13T09:42:23.123"
                    }
                }
            }
        }"#;

        let expected = IndexPart {
            version: 10,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), LayerFileMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), LayerFileMetadata {
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::new(
                Lsn::from_str("0/16960E8").unwrap(),
                Some(Lsn::from_str("0/1696070").unwrap()),
                Some(TimelineId::from_str("e45a7f37d3ee2ff17dc14bf4f4e3f52e").unwrap()),
                Lsn::INVALID,
                Lsn::from_str("0/1696070").unwrap(),
                Lsn::from_str("0/1696070").unwrap(),
                14,
            ).with_recalculated_checksum().unwrap(),
            deleted_at: None,
            lineage: Default::default(),
            gc_blocking: Some(GcBlocking {
                started_at: parse_naive_datetime("2024-07-19T09:00:00.123000000"),
                reasons: enumset::EnumSet::from_iter([GcBlockingReason::DetachAncestor]),
            }),
            last_aux_file_policy: Default::default(),
            archived_at: None,
            import_pgdata: Some(import_pgdata::index_part_format::Root::V1(import_pgdata::index_part_format::V1::Done(import_pgdata::index_part_format::Done{
                started_at: parse_naive_datetime("2024-11-13T09:23:42.123000000"),
                finished_at: parse_naive_datetime("2024-11-13T09:42:23.123000000"),
                idempotency_key: import_pgdata::index_part_format::IdempotencyKey::new("specified-by-client-218a5213-5044-4562-a28d-d024c5f057f5".to_string()),
            })))
        };

        let part = IndexPart::from_json_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    fn parse_naive_datetime(s: &str) -> NaiveDateTime {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S.%f").unwrap()
    }
}
