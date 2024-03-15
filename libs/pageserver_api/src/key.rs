use anyhow::{bail, Result};
use byteorder::{ByteOrder, BE};
use postgres_ffi::relfile_utils::{FSM_FORKNUM, VISIBILITYMAP_FORKNUM};
use postgres_ffi::{Oid, TransactionId};
use serde::{Deserialize, Serialize};
use std::{fmt, ops::Range};

use crate::reltag::{BlockNumber, RelTag, SlruKind};

/// Key used in the Repository kv-store.
///
/// The Repository treats this as an opaque struct, but see the code in pgdatadir_mapping.rs
/// for what we actually store in these fields.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Key {
    pub field1: u8,
    pub field2: u32,
    pub field3: u32,
    pub field4: u32,
    pub field5: u8,
    pub field6: u32,
}

/// The storage key size.
pub const KEY_SIZE: usize = 18;

/// The metadata key size. 2B fewer than the storage key size because field2 is not fully utilized.
/// See [`Key::to_i128`] for more information on the encoding.
pub const METADATA_KEY_SIZE: usize = 16;

/// The key prefix start range for the metadata keys. All keys with the first byte >= 0x40 is a metadata key.
pub const METADATA_KEY_BEGIN_PREFIX: u8 = 0x60;
pub const METADATA_KEY_END_PREFIX: u8 = 0x7F;

/// The (reserved) key prefix of relation sizes.
pub const RELATION_SIZE_PREFIX: u8 = 0x61;

/// The key prefix of AUX file keys.
pub const AUX_KEY_PREFIX: u8 = 0x62;

/// Check if the key falls in the range of metadata keys.
pub const fn is_metadata_key_slice(key: &[u8]) -> bool {
    key[0] >= METADATA_KEY_BEGIN_PREFIX && key[0] < METADATA_KEY_END_PREFIX
}

impl Key {
    /// Check if the key falls in the range of metadata keys.
    pub const fn is_metadata_key(&self) -> bool {
        self.field1 >= METADATA_KEY_BEGIN_PREFIX && self.field1 < METADATA_KEY_END_PREFIX
    }

    /// Encode a metadata key to a storage key.
    pub fn from_metadata_key_fixed_size(key: &[u8; METADATA_KEY_SIZE]) -> Self {
        assert!(is_metadata_key_slice(key), "key not in metadata key range");
        // Metadata key space ends at 0x7F so it's fine to directly convert it to i128.
        Self::from_i128(i128::from_be_bytes(*key))
    }

    /// Encode a metadata key to a storage key.
    pub fn from_metadata_key(key: &[u8]) -> Self {
        Self::from_metadata_key_fixed_size(key.try_into().expect("expect 16 byte metadata key"))
    }

    /// Get the range of metadata keys.
    pub const fn metadata_key_range() -> Range<Self> {
        Key {
            field1: METADATA_KEY_BEGIN_PREFIX,
            field2: 0,
            field3: 0,
            field4: 0,
            field5: 0,
            field6: 0,
        }..Key {
            field1: METADATA_KEY_END_PREFIX,
            field2: 0,
            field3: 0,
            field4: 0,
            field5: 0,
            field6: 0,
        }
    }

    /// Get the range of aux keys.
    pub fn metadata_aux_key_range() -> Range<Self> {
        Key {
            field1: AUX_KEY_PREFIX,
            field2: 0,
            field3: 0,
            field4: 0,
            field5: 0,
            field6: 0,
        }..Key {
            field1: AUX_KEY_PREFIX + 1,
            field2: 0,
            field3: 0,
            field4: 0,
            field5: 0,
            field6: 0,
        }
    }

    /// 'field2' is used to store tablespaceid for relations and small enum numbers for other relish.
    /// As long as Neon does not support tablespace (because of lack of access to local file system),
    /// we can assume that only some predefined namespace OIDs are used which can fit in u16
    pub fn to_i128(&self) -> i128 {
        assert!(self.field2 <= 0xFFFF || self.field2 == 0xFFFFFFFF || self.field2 == 0x22222222);
        (((self.field1 & 0x7F) as i128) << 120)
            | (((self.field2 & 0xFFFF) as i128) << 104)
            | ((self.field3 as i128) << 72)
            | ((self.field4 as i128) << 40)
            | ((self.field5 as i128) << 32)
            | self.field6 as i128
    }

    pub const fn from_i128(x: i128) -> Self {
        Key {
            field1: ((x >> 120) & 0x7F) as u8,
            field2: ((x >> 104) & 0xFFFF) as u32,
            field3: (x >> 72) as u32,
            field4: (x >> 40) as u32,
            field5: (x >> 32) as u8,
            field6: x as u32,
        }
    }

    pub const fn next(&self) -> Key {
        self.add(1)
    }

    pub const fn add(&self, x: u32) -> Key {
        let mut key = *self;

        let r = key.field6.overflowing_add(x);
        key.field6 = r.0;
        if r.1 {
            let r = key.field5.overflowing_add(1);
            key.field5 = r.0;
            if r.1 {
                let r = key.field4.overflowing_add(1);
                key.field4 = r.0;
                if r.1 {
                    let r = key.field3.overflowing_add(1);
                    key.field3 = r.0;
                    if r.1 {
                        let r = key.field2.overflowing_add(1);
                        key.field2 = r.0;
                        if r.1 {
                            let r = key.field1.overflowing_add(1);
                            key.field1 = r.0;
                            assert!(!r.1);
                        }
                    }
                }
            }
        }
        key
    }

    /// Convert a 18B slice to a key. This function should not be used for metadata keys because field2 is handled differently.
    /// Use [`Key::from_i128`] instead if you want to handle 16B keys (i.e., metadata keys).
    pub fn from_slice(b: &[u8]) -> Self {
        Key {
            field1: b[0],
            field2: u32::from_be_bytes(b[1..5].try_into().unwrap()),
            field3: u32::from_be_bytes(b[5..9].try_into().unwrap()),
            field4: u32::from_be_bytes(b[9..13].try_into().unwrap()),
            field5: b[13],
            field6: u32::from_be_bytes(b[14..18].try_into().unwrap()),
        }
    }

    /// Convert a key to a 18B slice. This function should not be used for metadata keys because field2 is handled differently.
    /// Use [`Key::to_i128`] instead if you want to get a 16B key (i.e., metadata keys).
    pub fn write_to_byte_slice(&self, buf: &mut [u8]) {
        buf[0] = self.field1;
        BE::write_u32(&mut buf[1..5], self.field2);
        BE::write_u32(&mut buf[5..9], self.field3);
        BE::write_u32(&mut buf[9..13], self.field4);
        buf[13] = self.field5;
        BE::write_u32(&mut buf[14..18], self.field6);
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02X}{:08X}{:08X}{:08X}{:02X}{:08X}",
            self.field1, self.field2, self.field3, self.field4, self.field5, self.field6
        )
    }
}

impl Key {
    pub const MIN: Key = Key {
        field1: u8::MIN,
        field2: u32::MIN,
        field3: u32::MIN,
        field4: u32::MIN,
        field5: u8::MIN,
        field6: u32::MIN,
    };
    pub const MAX: Key = Key {
        field1: u8::MAX,
        field2: u32::MAX,
        field3: u32::MAX,
        field4: u32::MAX,
        field5: u8::MAX,
        field6: u32::MAX,
    };

    pub fn from_hex(s: &str) -> Result<Self> {
        if s.len() != 36 {
            bail!("parse error");
        }
        Ok(Key {
            field1: u8::from_str_radix(&s[0..2], 16)?,
            field2: u32::from_str_radix(&s[2..10], 16)?,
            field3: u32::from_str_radix(&s[10..18], 16)?,
            field4: u32::from_str_radix(&s[18..26], 16)?,
            field5: u8::from_str_radix(&s[26..28], 16)?,
            field6: u32::from_str_radix(&s[28..36], 16)?,
        })
    }
}

// Layout of the Key address space
//
// The Key struct, used to address the underlying key-value store, consists of
// 18 bytes, split into six fields. See 'Key' in repository.rs. We need to map
// all the data and metadata keys into those 18 bytes.
//
// Principles for the mapping:
//
// - Things that are often accessed or modified together, should be close to
//   each other in the key space. For example, if a relation is extended by one
//   block, we create a new key-value pair for the block data, and update the
//   relation size entry. Because of that, the RelSize key comes after all the
//   RelBlocks of a relation: the RelSize and the last RelBlock are always next
//   to each other.
//
// The key space is divided into four major sections, identified by the first
// byte, and the form a hierarchy:
//
// 00 Relation data and metadata
//
//   DbDir    () -> (dbnode, spcnode)
//   Filenodemap
//   RelDir   -> relnode forknum
//       RelBlocks
//       RelSize
//
// 01 SLRUs
//
//   SlruDir  kind
//   SlruSegBlocks segno
//   SlruSegSize
//
// 02 pg_twophase
//
// 03 misc
//    Controlfile
//    checkpoint
//    pg_version
//
// 04 aux files
//
// Below is a full list of the keyspace allocation:
//
// DbDir:
// 00 00000000 00000000 00000000 00   00000000
//
// Filenodemap:
// 00 SPCNODE  DBNODE   00000000 00   00000000
//
// RelDir:
// 00 SPCNODE  DBNODE   00000000 00   00000001 (Postgres never uses relfilenode 0)
//
// RelBlock:
// 00 SPCNODE  DBNODE   RELNODE  FORK BLKNUM
//
// RelSize:
// 00 SPCNODE  DBNODE   RELNODE  FORK FFFFFFFF
//
// SlruDir:
// 01 kind     00000000 00000000 00   00000000
//
// SlruSegBlock:
// 01 kind     00000001 SEGNO    00   BLKNUM
//
// SlruSegSize:
// 01 kind     00000001 SEGNO    00   FFFFFFFF
//
// TwoPhaseDir:
// 02 00000000 00000000 00000000 00   00000000
//
// TwoPhaseFile:
// 02 00000000 00000000 00000000 00   XID
//
// ControlFile:
// 03 00000000 00000000 00000000 00   00000000
//
// Checkpoint:
// 03 00000000 00000000 00000000 00   00000001
//
// AuxFiles:
// 03 00000000 00000000 00000000 00   00000002
//
// ReplOrigin:
// 03 00000000 00000000 00000000 00   00000003
//

//-- Section 01: relation data and metadata

pub const DBDIR_KEY: Key = Key {
    field1: 0x00,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 0,
};

#[inline(always)]
pub fn dbdir_key_range(spcnode: Oid, dbnode: Oid) -> Range<Key> {
    Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0,
        field5: 0,
        field6: 0,
    }..Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0xffffffff,
        field5: 0xff,
        field6: 0xffffffff,
    }
}

#[inline(always)]
pub fn relmap_file_key(spcnode: Oid, dbnode: Oid) -> Key {
    Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0,
        field5: 0,
        field6: 0,
    }
}

#[inline(always)]
pub fn rel_dir_to_key(spcnode: Oid, dbnode: Oid) -> Key {
    Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0,
        field5: 0,
        field6: 1,
    }
}

#[inline(always)]
pub fn rel_block_to_key(rel: RelTag, blknum: BlockNumber) -> Key {
    Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum,
        field6: blknum,
    }
}

#[inline(always)]
pub fn rel_size_to_key(rel: RelTag) -> Key {
    Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum,
        field6: 0xffff_ffff,
    }
}

impl Key {
    #[inline(always)]
    pub fn is_rel_size_key(&self) -> bool {
        self.field1 == 0 && self.field6 == u32::MAX
    }
}

#[inline(always)]
pub fn rel_key_range(rel: RelTag) -> Range<Key> {
    Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum,
        field6: 0,
    }..Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum + 1,
        field6: 0,
    }
}

//-- Section 02: SLRUs

#[inline(always)]
pub fn slru_dir_to_key(kind: SlruKind) -> Key {
    Key {
        field1: 0x01,
        field2: match kind {
            SlruKind::Clog => 0x00,
            SlruKind::MultiXactMembers => 0x01,
            SlruKind::MultiXactOffsets => 0x02,
        },
        field3: 0,
        field4: 0,
        field5: 0,
        field6: 0,
    }
}

#[inline(always)]
pub fn slru_dir_kind(key: &Key) -> Option<Result<SlruKind, u32>> {
    if key.field1 == 0x01
        && key.field3 == 0
        && key.field4 == 0
        && key.field5 == 0
        && key.field6 == 0
    {
        match key.field2 {
            0 => Some(Ok(SlruKind::Clog)),
            1 => Some(Ok(SlruKind::MultiXactMembers)),
            2 => Some(Ok(SlruKind::MultiXactOffsets)),
            x => Some(Err(x)),
        }
    } else {
        None
    }
}

#[inline(always)]
pub fn slru_block_to_key(kind: SlruKind, segno: u32, blknum: BlockNumber) -> Key {
    Key {
        field1: 0x01,
        field2: match kind {
            SlruKind::Clog => 0x00,
            SlruKind::MultiXactMembers => 0x01,
            SlruKind::MultiXactOffsets => 0x02,
        },
        field3: 1,
        field4: segno,
        field5: 0,
        field6: blknum,
    }
}

#[inline(always)]
pub fn slru_segment_size_to_key(kind: SlruKind, segno: u32) -> Key {
    Key {
        field1: 0x01,
        field2: match kind {
            SlruKind::Clog => 0x00,
            SlruKind::MultiXactMembers => 0x01,
            SlruKind::MultiXactOffsets => 0x02,
        },
        field3: 1,
        field4: segno,
        field5: 0,
        field6: 0xffff_ffff,
    }
}

impl Key {
    pub fn is_slru_segment_size_key(&self) -> bool {
        self.field1 == 0x01
            && self.field2 < 0x03
            && self.field3 == 0x01
            && self.field5 == 0
            && self.field6 == u32::MAX
    }
}

#[inline(always)]
pub fn slru_segment_key_range(kind: SlruKind, segno: u32) -> Range<Key> {
    let field2 = match kind {
        SlruKind::Clog => 0x00,
        SlruKind::MultiXactMembers => 0x01,
        SlruKind::MultiXactOffsets => 0x02,
    };

    Key {
        field1: 0x01,
        field2,
        field3: 1,
        field4: segno,
        field5: 0,
        field6: 0,
    }..Key {
        field1: 0x01,
        field2,
        field3: 1,
        field4: segno,
        field5: 1,
        field6: 0,
    }
}

//-- Section 03: pg_twophase

pub const TWOPHASEDIR_KEY: Key = Key {
    field1: 0x02,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 0,
};

#[inline(always)]
pub fn twophase_file_key(xid: TransactionId) -> Key {
    Key {
        field1: 0x02,
        field2: 0,
        field3: 0,
        field4: 0,
        field5: 0,
        field6: xid,
    }
}

#[inline(always)]
pub fn twophase_key_range(xid: TransactionId) -> Range<Key> {
    let (next_xid, overflowed) = xid.overflowing_add(1);

    Key {
        field1: 0x02,
        field2: 0,
        field3: 0,
        field4: 0,
        field5: 0,
        field6: xid,
    }..Key {
        field1: 0x02,
        field2: 0,
        field3: 0,
        field4: 0,
        field5: u8::from(overflowed),
        field6: next_xid,
    }
}

//-- Section 03: Control file
pub const CONTROLFILE_KEY: Key = Key {
    field1: 0x03,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 0,
};

pub const CHECKPOINT_KEY: Key = Key {
    field1: 0x03,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 1,
};

pub const AUX_FILES_KEY: Key = Key {
    field1: 0x03,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 2,
};

pub const REPL_ORIGIN_KEY: Key = Key {
    field1: 0x03,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 3,
};

// Reverse mappings for a few Keys.
// These are needed by WAL redo manager.

/// Non inherited range for vectored get.
pub const NON_INHERITED_RANGE: Range<Key> = AUX_FILES_KEY..AUX_FILES_KEY.next();
/// Sparse keyspace range for vectored get. Missing key error will be ignored for this range.
pub const NON_INHERITED_SPARSE_RANGE: Range<Key> = Key::metadata_key_range();

impl Key {
    // AUX_FILES currently stores only data for logical replication (slots etc), and
    // we don't preserve these on a branch because safekeepers can't follow timeline
    // switch (and generally it likely should be optional), so ignore these.
    #[inline(always)]
    pub fn is_inherited_key(self) -> bool {
        !NON_INHERITED_RANGE.contains(&self) && !NON_INHERITED_SPARSE_RANGE.contains(&self)
    }

    #[inline(always)]
    pub fn is_rel_fsm_block_key(self) -> bool {
        self.field1 == 0x00
            && self.field4 != 0
            && self.field5 == FSM_FORKNUM
            && self.field6 != 0xffffffff
    }

    #[inline(always)]
    pub fn is_rel_vm_block_key(self) -> bool {
        self.field1 == 0x00
            && self.field4 != 0
            && self.field5 == VISIBILITYMAP_FORKNUM
            && self.field6 != 0xffffffff
    }

    #[inline(always)]
    pub fn to_slru_block(self) -> anyhow::Result<(SlruKind, u32, BlockNumber)> {
        Ok(match self.field1 {
            0x01 => {
                let kind = match self.field2 {
                    0x00 => SlruKind::Clog,
                    0x01 => SlruKind::MultiXactMembers,
                    0x02 => SlruKind::MultiXactOffsets,
                    _ => anyhow::bail!("unrecognized slru kind 0x{:02x}", self.field2),
                };
                let segno = self.field4;
                let blknum = self.field6;

                (kind, segno, blknum)
            }
            _ => anyhow::bail!("unexpected value kind 0x{:02x}", self.field1),
        })
    }

    #[inline(always)]
    pub fn is_slru_block_key(self) -> bool {
        self.field1 == 0x01                // SLRU-related
        && self.field3 == 0x00000001   // but not SlruDir
        && self.field6 != 0xffffffff // and not SlruSegSize
    }

    #[inline(always)]
    pub fn is_rel_block_key(&self) -> bool {
        self.field1 == 0x00 && self.field4 != 0 && self.field6 != 0xffffffff
    }

    /// Guaranteed to return `Ok()` if [`Self::is_rel_block_key`] returns `true` for `key`.
    #[inline(always)]
    pub fn to_rel_block(self) -> anyhow::Result<(RelTag, BlockNumber)> {
        Ok(match self.field1 {
            0x00 => (
                RelTag {
                    spcnode: self.field2,
                    dbnode: self.field3,
                    relnode: self.field4,
                    forknum: self.field5,
                },
                self.field6,
            ),
            _ => anyhow::bail!("unexpected value kind 0x{:02x}", self.field1),
        })
    }
}

impl std::str::FromStr for Key {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::key::is_metadata_key_slice;
    use crate::key::Key;

    use rand::Rng;
    use rand::SeedableRng;

    use super::AUX_KEY_PREFIX;

    #[test]
    fn display_fromstr_bijection() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let key = Key {
            field1: rng.gen(),
            field2: rng.gen(),
            field3: rng.gen(),
            field4: rng.gen(),
            field5: rng.gen(),
            field6: rng.gen(),
        };

        assert_eq!(key, Key::from_str(&format!("{key}")).unwrap());
    }

    #[test]
    fn test_metadata_keys() {
        let mut metadata_key = vec![AUX_KEY_PREFIX];
        metadata_key.extend_from_slice(&[0xFF; 15]);
        let encoded_key = Key::from_metadata_key(&metadata_key);
        let output_key = encoded_key.to_i128().to_be_bytes();
        assert_eq!(metadata_key, output_key);
        assert!(encoded_key.is_metadata_key());
        assert!(is_metadata_key_slice(&metadata_key));
    }

    #[test]
    fn test_possible_largest_key() {
        Key::from_i128(0x7FFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF);
        // TODO: put this key into the system and see if anything breaks.
    }
}
