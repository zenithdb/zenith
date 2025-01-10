use std::collections::BTreeMap;

use utils::id::TenantId;
use utils::shard::TenantShardId;

use crate::scheduler::{ScheduleContext, ScheduleMode};
use crate::tenant_shard::TenantShard;

/// When making scheduling decisions, it is useful to have the ScheduleContext for a whole
/// tenant while considering the individual shards within it.  This iterator is a helper
/// that gathers all the shards in a tenant and then yields them together with a ScheduleContext
/// for the tenant.
pub(super) struct TenantShardContextIterator<'a> {
    schedule_mode: ScheduleMode,
    inner: std::collections::btree_map::IterMut<'a, TenantShardId, TenantShard>,
}

impl<'a> TenantShardContextIterator<'a> {
    pub(super) fn new(
        tenants: &'a mut BTreeMap<TenantShardId, TenantShard>,
        schedule_mode: ScheduleMode,
    ) -> Self {
        Self {
            schedule_mode,
            inner: tenants.iter_mut(),
        }
    }
}

impl<'a> Iterator for TenantShardContextIterator<'a> {
    type Item = (TenantId, ScheduleContext, Vec<&'a mut TenantShard>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut tenant_shards = Vec::new();
        let mut schedule_context = ScheduleContext::new(self.schedule_mode.clone());
        loop {
            let (tenant_shard_id, shard) = self.inner.next()?;

            if tenant_shard_id.is_shard_zero() {
                // Cleared on last shard of previous tenant
                assert!(tenant_shards.is_empty());
            }

            // Accumulate the schedule context for all the shards in a tenant
            schedule_context.avoid(&shard.intent.all_pageservers());
            tenant_shards.push(shard);

            if tenant_shard_id.shard_number.0 == tenant_shard_id.shard_count.count() - 1 {
                return Some((tenant_shard_id.tenant_id, schedule_context, tenant_shards));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use pageserver_api::controller_api::PlacementPolicy;
    use utils::shard::{ShardCount, ShardNumber};

    use crate::{
        scheduler::test_utils::make_test_nodes, service::Scheduler,
        tenant_shard::tests::make_test_tenant_with_id,
    };

    use super::*;

    #[test]
    fn test_context_iterator() {
        // Hand-crafted tenant IDs to ensure they appear in the expected order when put into
        // a btreemap & iterated
        let mut t_1_shards = make_test_tenant_with_id(
            TenantId::from_str("af0480929707ee75372337efaa5ecf96").unwrap(),
            PlacementPolicy::Attached(1),
            ShardCount(1),
            None,
        );
        let t_2_shards = make_test_tenant_with_id(
            TenantId::from_str("bf0480929707ee75372337efaa5ecf96").unwrap(),
            PlacementPolicy::Attached(1),
            ShardCount(4),
            None,
        );
        let mut t_3_shards = make_test_tenant_with_id(
            TenantId::from_str("cf0480929707ee75372337efaa5ecf96").unwrap(),
            PlacementPolicy::Attached(1),
            ShardCount(1),
            None,
        );

        let t1_id = t_1_shards[0].tenant_shard_id.tenant_id;
        let t2_id = t_2_shards[0].tenant_shard_id.tenant_id;
        let t3_id = t_3_shards[0].tenant_shard_id.tenant_id;

        let mut tenants = BTreeMap::new();
        tenants.insert(t_1_shards[0].tenant_shard_id, t_1_shards.pop().unwrap());
        for shard in t_2_shards {
            tenants.insert(shard.tenant_shard_id, shard);
        }
        tenants.insert(t_3_shards[0].tenant_shard_id, t_3_shards.pop().unwrap());

        let nodes = make_test_nodes(3, &[]);
        let mut scheduler = Scheduler::new(nodes.values());
        let mut context = ScheduleContext::default();
        for shard in tenants.values_mut() {
            shard.schedule(&mut scheduler, &mut context).unwrap();
        }

        let mut iter = TenantShardContextIterator::new(&mut tenants, ScheduleMode::Speculative);
        let (tenant_id, context, shards) = iter.next().unwrap();
        assert_eq!(tenant_id, t1_id);
        assert_eq!(shards[0].tenant_shard_id.shard_number, ShardNumber(0));
        assert_eq!(shards.len(), 1);
        assert_eq!(context.location_count(), 2);

        let (tenant_id, context, shards) = iter.next().unwrap();
        assert_eq!(tenant_id, t2_id);
        assert_eq!(shards[0].tenant_shard_id.shard_number, ShardNumber(0));
        assert_eq!(shards[1].tenant_shard_id.shard_number, ShardNumber(1));
        assert_eq!(shards[2].tenant_shard_id.shard_number, ShardNumber(2));
        assert_eq!(shards[3].tenant_shard_id.shard_number, ShardNumber(3));
        assert_eq!(shards.len(), 4);
        assert_eq!(context.location_count(), 8);

        let (tenant_id, context, shards) = iter.next().unwrap();
        assert_eq!(tenant_id, t3_id);
        assert_eq!(shards[0].tenant_shard_id.shard_number, ShardNumber(0));
        assert_eq!(shards.len(), 1);
        assert_eq!(context.location_count(), 2);

        for shard in tenants.values_mut() {
            shard.intent.clear(&mut scheduler);
        }
    }
}
