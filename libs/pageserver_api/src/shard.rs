//! See docs/rfcs/031-sharding-static.md for an overview of sharding.
//!
//! This module contains a variety of types used to represent the concept of sharding
//! a Neon tenant across multiple physical shards.  Since there are quite a few of these,
//! we provide an summary here.
//!
//! Types used to describe shards:
//! - [`ShardCount`] describes how many shards make up a tenant, plus the magic `unsharded` value
//!   which identifies a tenant which is not shard-aware.  This means its storage paths do not include
//!   a shard suffix.
//! - [`ShardNumber`] is simply the zero-based index of a shard within a tenant.
//! - [`ShardIndex`] is the 2-tuple of `ShardCount` and `ShardNumber`, it's just like a `TenantShardId`
//!   without the tenant ID.  This is useful for things that are implicitly scoped to a particular
//!   tenant, such as layer files.
//! - [`ShardIdentity`]` is the full description of a particular shard's parameters, in sufficient
//!   detail to convert a [`Key`] to a [`ShardNumber`] when deciding where to write/read.
//! - The [`ShardSlug`] is a terse formatter for ShardCount and ShardNumber, written as
//!   four hex digits.  An unsharded tenant is `0000`.
//! - [`TenantShardId`] is the unique ID of a particular shard within a particular tenant
//!
//! Types used to describe the parameters for data distribution in a sharded tenant:
//! - [`ShardStripeSize`] controls how long contiguous runs of [`Key`]s (stripes) are when distributed across
//!   multiple shards.  Its value is given in 8kiB pages.
//! - [`ShardLayout`] describes the data distribution scheme, and at time of writing is
//!   always zero: this is provided for future upgrades that might introduce different
//!   data distribution schemes.
//!
//! Examples:
//! - A legacy unsharded tenant has one shard with ShardCount(0), ShardNumber(0), and its slug is 0000
//! - A single sharded tenant has one shard with ShardCount(1), ShardNumber(0), and its slug is 0001
//! - In a tenant with 4 shards, each shard has ShardCount(N), ShardNumber(i) where i in 0..N-1 (inclusive),
//!   and their slugs are 0004, 0104, 0204, and 0304.

use crate::{key::Key, models::ShardParameters};
use postgres_ffi::relfile_utils::INIT_FORKNUM;
use serde::{Deserialize, Serialize};

#[doc(inline)]
pub use ::utils::shard::*;

/// The ShardIdentity contains enough information to map a [`Key`] to a [`ShardNumber`],
/// and to check whether that [`ShardNumber`] is the same as the current shard.
#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ShardIdentity {
    pub number: ShardNumber,
    pub count: ShardCount,
    pub stripe_size: ShardStripeSize,
    layout: ShardLayout,
}

/// Stripe size in number of pages
#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ShardStripeSize(pub u32);

impl Default for ShardStripeSize {
    fn default() -> Self {
        DEFAULT_STRIPE_SIZE
    }
}

/// Layout version: for future upgrades where we might change how the key->shard mapping works
#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ShardLayout(u8);

const LAYOUT_V1: ShardLayout = ShardLayout(1);
/// ShardIdentity uses a magic layout value to indicate if it is unusable
const LAYOUT_BROKEN: ShardLayout = ShardLayout(255);

/// Default stripe size in pages: 256MiB divided by 8kiB page size.
const DEFAULT_STRIPE_SIZE: ShardStripeSize = ShardStripeSize(256 * 1024 / 8);

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ShardConfigError {
    #[error("Invalid shard count")]
    InvalidCount,
    #[error("Invalid shard number")]
    InvalidNumber,
    #[error("Invalid stripe size")]
    InvalidStripeSize,
}

impl ShardIdentity {
    /// An identity with number=0 count=0 is a "none" identity, which represents legacy
    /// tenants.  Modern single-shard tenants should not use this: they should
    /// have number=0 count=1.
    pub const fn unsharded() -> Self {
        Self {
            number: ShardNumber(0),
            count: ShardCount(0),
            layout: LAYOUT_V1,
            stripe_size: DEFAULT_STRIPE_SIZE,
        }
    }

    /// A broken instance of this type is only used for `TenantState::Broken` tenants,
    /// which are constructed in code paths that don't have access to proper configuration.
    ///
    /// A ShardIdentity in this state may not be used for anything, and should not be persisted.
    /// Enforcement is via assertions, to avoid making our interface fallible for this
    /// edge case: it is the Tenant's responsibility to avoid trying to do any I/O when in a broken
    /// state, and by extension to avoid trying to do any page->shard resolution.
    pub fn broken(number: ShardNumber, count: ShardCount) -> Self {
        Self {
            number,
            count,
            layout: LAYOUT_BROKEN,
            stripe_size: DEFAULT_STRIPE_SIZE,
        }
    }

    /// The "unsharded" value is distinct from simply having a single shard: it represents
    /// a tenant which is not shard-aware at all, and whose storage paths will not include
    /// a shard suffix.
    pub fn is_unsharded(&self) -> bool {
        self.number == ShardNumber(0) && self.count == ShardCount(0)
    }

    /// Count must be nonzero, and number must be < count. To construct
    /// the legacy case (count==0), use Self::unsharded instead.
    pub fn new(
        number: ShardNumber,
        count: ShardCount,
        stripe_size: ShardStripeSize,
    ) -> Result<Self, ShardConfigError> {
        if count.0 == 0 {
            Err(ShardConfigError::InvalidCount)
        } else if number.0 > count.0 - 1 {
            Err(ShardConfigError::InvalidNumber)
        } else if stripe_size.0 == 0 {
            Err(ShardConfigError::InvalidStripeSize)
        } else {
            Ok(Self {
                number,
                count,
                layout: LAYOUT_V1,
                stripe_size,
            })
        }
    }

    /// For use when creating ShardIdentity instances for new shards, where a creation request
    /// specifies the ShardParameters that apply to all shards.
    pub fn from_params(number: ShardNumber, params: &ShardParameters) -> Self {
        Self {
            number,
            count: params.count,
            layout: LAYOUT_V1,
            stripe_size: params.stripe_size,
        }
    }

    fn is_broken(&self) -> bool {
        self.layout == LAYOUT_BROKEN
    }

    pub fn get_shard_number(&self, key: &Key) -> ShardNumber {
        assert!(!self.is_broken());
        key_to_shard_number(self.count, self.stripe_size, key)
    }

    /// Return true if the key is stored only on this shard. This does not include
    /// global keys, see is_key_global().
    ///
    /// Shards must ingest _at least_ keys which return true from this check.
    pub fn is_key_local(&self, key: &Key) -> bool {
        assert!(!self.is_broken());
        if self.count < ShardCount(2) || (key_is_shard0(key) && self.number == ShardNumber(0)) {
            true
        } else {
            key_to_shard_number(self.count, self.stripe_size, key) == self.number
        }
    }

    /// Return true if the key should be stored on all shards, not just one.
    pub fn is_key_global(&self, key: &Key) -> bool {
        if key.is_slru_block_key()
            || key.is_slru_segment_size_key()
            || key.is_aux_file_key()
            || key.is_slru_dir_key()
        {
            // Special keys that are only stored on shard 0
            false
        } else if key.is_rel_block_key() {
            // Ordinary relation blocks are distributed across shards
            false
        } else if key.is_rel_size_key() {
            // All shards maintain rel size keys (although only shard 0 is responsible for
            // keeping it strictly accurate, other shards just reflect the highest block they've ingested)
            true
        } else {
            // For everything else, we assume it must be kept everywhere, because ingest code
            // might assume this -- this covers functionality where the ingest code has
            // not (yet) been made fully shard aware.
            true
        }
    }

    /// Return true if the key should be discarded if found in this shard's
    /// data store, e.g. during compaction after a split.
    ///
    /// Shards _may_ drop keys which return false here, but are not obliged to.
    pub fn is_key_disposable(&self, key: &Key) -> bool {
        if self.count < ShardCount(2) {
            // Fast path: unsharded tenant doesn't dispose of anything
            return false;
        }

        if self.is_key_global(key) {
            false
        } else {
            !self.is_key_local(key)
        }
    }

    /// Obtains the shard number and count combined into a `ShardIndex`.
    pub fn shard_index(&self) -> ShardIndex {
        ShardIndex {
            shard_count: self.count,
            shard_number: self.number,
        }
    }

    pub fn shard_slug(&self) -> String {
        if self.count > ShardCount(0) {
            format!("-{:02x}{:02x}", self.number.0, self.count.0)
        } else {
            String::new()
        }
    }

    /// Convenience for checking if this identity is the 0th shard in a tenant,
    /// for special cases on shard 0 such as ingesting relation sizes.
    pub fn is_shard_zero(&self) -> bool {
        self.number == ShardNumber(0)
    }
}

/// Whether this key is always held on shard 0 (e.g. shard 0 holds all SLRU keys
/// in order to be able to serve basebackup requests without peer communication).
fn key_is_shard0(key: &Key) -> bool {
    // To decide what to shard out to shards >0, we apply a simple rule that only
    // relation pages are distributed to shards other than shard zero. Everything else gets
    // stored on shard 0.  This guarantees that shard 0 can independently serve basebackup
    // requests, and any request other than those for particular blocks in relations.
    //
    // The only exception to this rule is "initfork" data -- this relates to postgres's UNLOGGED table
    // type. These are special relations, usually with only 0 or 1 blocks, and we store them on shard 0
    // because they must be included in basebackups.
    let is_initfork = key.field5 == INIT_FORKNUM;

    !key.is_rel_block_key() || is_initfork
}

/// Provide the same result as the function in postgres `hashfn.h` with the same name
fn murmurhash32(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h
}

/// Provide the same result as the function in postgres `hashfn.h` with the same name
fn hash_combine(mut a: u32, mut b: u32) -> u32 {
    b = b.wrapping_add(0x9e3779b9);
    b = b.wrapping_add(a << 6);
    b = b.wrapping_add(a >> 2);

    a ^= b;
    a
}

/// Where a Key is to be distributed across shards, select the shard.  This function
/// does not account for keys that should be broadcast across shards.
///
/// The hashing in this function must exactly match what we do in postgres smgr
/// code.  The resulting distribution of pages is intended to preserve locality within
/// `stripe_size` ranges of contiguous block numbers in the same relation, while otherwise
/// distributing data pseudo-randomly.
///
/// The mapping of key to shard is not stable across changes to ShardCount: this is intentional
/// and will be handled at higher levels when shards are split.
fn key_to_shard_number(count: ShardCount, stripe_size: ShardStripeSize, key: &Key) -> ShardNumber {
    // Fast path for un-sharded tenants or broadcast keys
    if count < ShardCount(2) || key_is_shard0(key) {
        return ShardNumber(0);
    }

    // relNode
    let mut hash = murmurhash32(key.field4);
    // blockNum/stripe size
    hash = hash_combine(hash, murmurhash32(key.field6 / stripe_size.0));

    ShardNumber((hash % count.0 as u32) as u8)
}

/// For debugging, while not exposing the internals.
#[derive(Debug)]
#[allow(unused)] // used by debug formatting by pagectl
struct KeyShardingInfo {
    shard0: bool,
    shard_number: ShardNumber,
}

pub fn describe(
    key: &Key,
    shard_count: ShardCount,
    stripe_size: ShardStripeSize,
) -> impl std::fmt::Debug {
    KeyShardingInfo {
        shard0: key_is_shard0(key),
        shard_number: key_to_shard_number(shard_count, stripe_size, key),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use utils::{id::TenantId, Hex};

    use super::*;

    const EXAMPLE_TENANT_ID: &str = "1f359dd625e519a1a4e8d7509690f6fc";

    #[test]
    fn tenant_shard_id_string() -> Result<(), hex::FromHexError> {
        let example = TenantShardId {
            tenant_id: TenantId::from_str(EXAMPLE_TENANT_ID).unwrap(),
            shard_count: ShardCount(10),
            shard_number: ShardNumber(7),
        };

        let encoded = format!("{example}");

        let expected = format!("{EXAMPLE_TENANT_ID}-070a");
        assert_eq!(&encoded, &expected);

        let decoded = TenantShardId::from_str(&encoded)?;

        assert_eq!(example, decoded);

        Ok(())
    }

    #[test]
    fn tenant_shard_id_binary() -> Result<(), hex::FromHexError> {
        let example = TenantShardId {
            tenant_id: TenantId::from_str(EXAMPLE_TENANT_ID).unwrap(),
            shard_count: ShardCount(10),
            shard_number: ShardNumber(7),
        };

        let encoded = bincode::serialize(&example).unwrap();
        let expected: [u8; 18] = [
            0x1f, 0x35, 0x9d, 0xd6, 0x25, 0xe5, 0x19, 0xa1, 0xa4, 0xe8, 0xd7, 0x50, 0x96, 0x90,
            0xf6, 0xfc, 0x07, 0x0a,
        ];
        assert_eq!(Hex(&encoded), Hex(&expected));

        let decoded = bincode::deserialize(&encoded).unwrap();

        assert_eq!(example, decoded);

        Ok(())
    }

    #[test]
    fn tenant_shard_id_backward_compat() -> Result<(), hex::FromHexError> {
        // Test that TenantShardId can decode a TenantId in human
        // readable form
        let example = TenantId::from_str(EXAMPLE_TENANT_ID).unwrap();
        let encoded = format!("{example}");

        assert_eq!(&encoded, EXAMPLE_TENANT_ID);

        let decoded = TenantShardId::from_str(&encoded)?;

        assert_eq!(example, decoded.tenant_id);
        assert_eq!(decoded.shard_count, ShardCount(0));
        assert_eq!(decoded.shard_number, ShardNumber(0));

        Ok(())
    }

    #[test]
    fn tenant_shard_id_forward_compat() -> Result<(), hex::FromHexError> {
        // Test that a legacy TenantShardId encodes into a form that
        // can be decoded as TenantId
        let example_tenant_id = TenantId::from_str(EXAMPLE_TENANT_ID).unwrap();
        let example = TenantShardId::unsharded(example_tenant_id);
        let encoded = format!("{example}");

        assert_eq!(&encoded, EXAMPLE_TENANT_ID);

        let decoded = TenantId::from_str(&encoded)?;

        assert_eq!(example_tenant_id, decoded);

        Ok(())
    }

    #[test]
    fn tenant_shard_id_legacy_binary() -> Result<(), hex::FromHexError> {
        // Unlike in human readable encoding, binary encoding does not
        // do any special handling of legacy unsharded TenantIds: this test
        // is equivalent to the main test for binary encoding, just verifying
        // that the same behavior applies when we have used `unsharded()` to
        // construct a TenantShardId.
        let example = TenantShardId::unsharded(TenantId::from_str(EXAMPLE_TENANT_ID).unwrap());
        let encoded = bincode::serialize(&example).unwrap();

        let expected: [u8; 18] = [
            0x1f, 0x35, 0x9d, 0xd6, 0x25, 0xe5, 0x19, 0xa1, 0xa4, 0xe8, 0xd7, 0x50, 0x96, 0x90,
            0xf6, 0xfc, 0x00, 0x00,
        ];
        assert_eq!(Hex(&encoded), Hex(&expected));

        let decoded = bincode::deserialize::<TenantShardId>(&encoded).unwrap();
        assert_eq!(example, decoded);

        Ok(())
    }

    #[test]
    fn shard_identity_validation() -> Result<(), ShardConfigError> {
        // Happy cases
        ShardIdentity::new(ShardNumber(0), ShardCount(1), DEFAULT_STRIPE_SIZE)?;
        ShardIdentity::new(ShardNumber(0), ShardCount(1), ShardStripeSize(1))?;
        ShardIdentity::new(ShardNumber(254), ShardCount(255), ShardStripeSize(1))?;

        assert_eq!(
            ShardIdentity::new(ShardNumber(0), ShardCount(0), DEFAULT_STRIPE_SIZE),
            Err(ShardConfigError::InvalidCount)
        );
        assert_eq!(
            ShardIdentity::new(ShardNumber(10), ShardCount(10), DEFAULT_STRIPE_SIZE),
            Err(ShardConfigError::InvalidNumber)
        );
        assert_eq!(
            ShardIdentity::new(ShardNumber(11), ShardCount(10), DEFAULT_STRIPE_SIZE),
            Err(ShardConfigError::InvalidNumber)
        );
        assert_eq!(
            ShardIdentity::new(ShardNumber(255), ShardCount(255), DEFAULT_STRIPE_SIZE),
            Err(ShardConfigError::InvalidNumber)
        );
        assert_eq!(
            ShardIdentity::new(ShardNumber(0), ShardCount(1), ShardStripeSize(0)),
            Err(ShardConfigError::InvalidStripeSize)
        );

        Ok(())
    }

    #[test]
    fn shard_index_human_encoding() -> Result<(), hex::FromHexError> {
        let example = ShardIndex {
            shard_number: ShardNumber(13),
            shard_count: ShardCount(17),
        };
        let expected: String = "0d11".to_string();
        let encoded = format!("{example}");
        assert_eq!(&encoded, &expected);

        let decoded = ShardIndex::from_str(&encoded)?;
        assert_eq!(example, decoded);
        Ok(())
    }

    #[test]
    fn shard_index_binary_encoding() -> Result<(), hex::FromHexError> {
        let example = ShardIndex {
            shard_number: ShardNumber(13),
            shard_count: ShardCount(17),
        };
        let expected: [u8; 2] = [0x0d, 0x11];

        let encoded = bincode::serialize(&example).unwrap();
        assert_eq!(Hex(&encoded), Hex(&expected));
        let decoded = bincode::deserialize(&encoded).unwrap();
        assert_eq!(example, decoded);

        Ok(())
    }

    // These are only smoke tests to spot check that our implementation doesn't
    // deviate from a few examples values: not aiming to validate the overall
    // hashing algorithm.
    #[test]
    fn murmur_hash() {
        assert_eq!(murmurhash32(0), 0);

        assert_eq!(hash_combine(0xb1ff3b40, 0), 0xfb7923c9);
    }

    #[test]
    fn shard_mapping() {
        let key = Key {
            field1: 0x00,
            field2: 0x67f,
            field3: 0x5,
            field4: 0x400c,
            field5: 0x00,
            field6: 0x7d06,
        };

        let shard = key_to_shard_number(ShardCount(10), DEFAULT_STRIPE_SIZE, &key);
        assert_eq!(shard, ShardNumber(8));
    }

    #[test]
    fn shard_id_split() {
        let tenant_id = TenantId::generate();
        let parent = TenantShardId::unsharded(tenant_id);

        // Unsharded into 2
        assert_eq!(
            parent.split(ShardCount(2)),
            vec![
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(2),
                    shard_number: ShardNumber(0)
                },
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(2),
                    shard_number: ShardNumber(1)
                }
            ]
        );

        // Unsharded into 4
        assert_eq!(
            parent.split(ShardCount(4)),
            vec![
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(4),
                    shard_number: ShardNumber(0)
                },
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(4),
                    shard_number: ShardNumber(1)
                },
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(4),
                    shard_number: ShardNumber(2)
                },
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(4),
                    shard_number: ShardNumber(3)
                }
            ]
        );

        // count=1 into 2 (check this works the same as unsharded.)
        let parent = TenantShardId {
            tenant_id,
            shard_count: ShardCount(1),
            shard_number: ShardNumber(0),
        };
        assert_eq!(
            parent.split(ShardCount(2)),
            vec![
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(2),
                    shard_number: ShardNumber(0)
                },
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(2),
                    shard_number: ShardNumber(1)
                }
            ]
        );

        // count=2 into count=8
        let parent = TenantShardId {
            tenant_id,
            shard_count: ShardCount(2),
            shard_number: ShardNumber(1),
        };
        assert_eq!(
            parent.split(ShardCount(8)),
            vec![
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(8),
                    shard_number: ShardNumber(1)
                },
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(8),
                    shard_number: ShardNumber(3)
                },
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(8),
                    shard_number: ShardNumber(5)
                },
                TenantShardId {
                    tenant_id,
                    shard_count: ShardCount(8),
                    shard_number: ShardNumber(7)
                },
            ]
        );
    }
}
