use std::{ops::Range, sync::Arc};

use anyhow::bail;
use pageserver_api::{
    key::Key,
    keyspace::{KeySpace, SparseKeySpace},
};
use utils::lsn::Lsn;

use pageserver_api::value::Value;

use super::{
    merge_iterator::{MergeIterator, MergeIteratorItem},
    PersistentLayerKey,
};

/// A filter iterator over merge iterators (and can be easily extended to other types of iterators).
///
/// The iterator will skip any keys not included in the keyspace filter. In other words, the keyspace filter contains the keys
/// to be retained.
pub struct FilterIterator<'a> {
    inner: MergeIterator<'a>,
    retain_key_filters: Vec<Range<Key>>,
    current_filter_idx: usize,
}

impl<'a> FilterIterator<'a> {
    pub fn create(
        inner: MergeIterator<'a>,
        dense_keyspace: KeySpace,
        sparse_keyspace: SparseKeySpace,
    ) -> anyhow::Result<Self> {
        let mut retain_key_filters = Vec::new();
        retain_key_filters.extend(dense_keyspace.ranges);
        retain_key_filters.extend(sparse_keyspace.0.ranges);
        retain_key_filters.sort_by(|a, b| a.start.cmp(&b.start));
        // Verify key filters are non-overlapping and sorted
        for window in retain_key_filters.windows(2) {
            if window[0].end > window[1].start {
                bail!(
                    "Key filters are overlapping: {:?} and {:?}",
                    window[0],
                    window[1]
                );
            }
        }
        Ok(Self {
            inner,
            retain_key_filters,
            current_filter_idx: 0,
        })
    }

    async fn next_inner<R: MergeIteratorItem>(&mut self) -> anyhow::Result<Option<R>> {
        while let Some(item) = self.inner.next_inner::<R>().await? {
            while self.current_filter_idx < self.retain_key_filters.len()
                && item.key_lsn_value().0 >= self.retain_key_filters[self.current_filter_idx].end
            {
                // [filter region]    [filter region]     [filter region]
                //                                     ^ item
                //                    ^ current filter
                self.current_filter_idx += 1;
                // [filter region]    [filter region]     [filter region]
                //                                     ^ item
                //                                        ^ current filter
            }
            if self.current_filter_idx >= self.retain_key_filters.len() {
                // We already exhausted all filters, so we should return now
                // [filter region] [filter region] [filter region]
                //                                                    ^ item
                //                                                 ^ current filter (nothing)
                return Ok(None);
            }
            if self.retain_key_filters[self.current_filter_idx].contains(&item.key_lsn_value().0) {
                // [filter region]    [filter region]     [filter region]
                //                                              ^ item
                //                                        ^ current filter
                return Ok(Some(item));
            }
            // If the key is not contained in the key retaining filters, continue to the next item.
            // [filter region]    [filter region]     [filter region]
            //                                     ^ item
            //                                        ^ current filter
        }
        Ok(None)
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<(Key, Lsn, Value)>> {
        self.next_inner().await
    }

    pub async fn next_with_trace(
        &mut self,
    ) -> anyhow::Result<Option<((Key, Lsn, Value), Arc<PersistentLayerKey>)>> {
        self.next_inner().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use itertools::Itertools;
    use pageserver_api::key::Key;
    use utils::lsn::Lsn;

    use crate::{
        tenant::{
            harness::{TenantHarness, TIMELINE_ID},
            storage_layer::delta_layer::test::produce_delta_layer,
        },
        DEFAULT_PG_VERSION,
    };

    async fn assert_filter_iter_equal(
        filter_iter: &mut FilterIterator<'_>,
        expect: &[(Key, Lsn, Value)],
    ) {
        let mut expect_iter = expect.iter();
        loop {
            let o1 = filter_iter.next().await.unwrap();
            let o2 = expect_iter.next();
            assert_eq!(o1.is_some(), o2.is_some());
            if o1.is_none() && o2.is_none() {
                break;
            }
            let (k1, l1, v1) = o1.unwrap();
            let (k2, l2, v2) = o2.unwrap();
            assert_eq!(&k1, k2);
            assert_eq!(l1, *l2);
            assert_eq!(&v1, v2);
        }
    }

    #[tokio::test]
    async fn filter_keyspace_iterator() {
        use bytes::Bytes;
        use pageserver_api::value::Value;

        let harness = TenantHarness::create("filter_iterator_filter_keyspace_iterator")
            .await
            .unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        fn get_key(id: u32) -> Key {
            let mut key = Key::from_hex("000000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }
        const N: usize = 100;
        let test_deltas1 = (0..N)
            .map(|idx| {
                (
                    get_key(idx as u32),
                    Lsn(0x20 * ((idx as u64) % 10 + 1)),
                    Value::Image(Bytes::from(format!("img{idx:05}"))),
                )
            })
            .collect_vec();
        let resident_layer_1 = produce_delta_layer(&tenant, &tline, test_deltas1.clone(), &ctx)
            .await
            .unwrap();

        let merge_iter = MergeIterator::create(
            &[resident_layer_1.get_as_delta(&ctx).await.unwrap()],
            &[],
            &ctx,
        );

        let mut filter_iter = FilterIterator::create(
            merge_iter,
            KeySpace {
                ranges: vec![
                    get_key(5)..get_key(10),
                    get_key(20)..get_key(30),
                    get_key(90)..get_key(110),
                    get_key(1000)..get_key(2000),
                ],
            },
            SparseKeySpace(KeySpace::default()),
        )
        .unwrap();
        let mut result = Vec::new();
        result.extend(test_deltas1[5..10].iter().cloned());
        result.extend(test_deltas1[20..30].iter().cloned());
        result.extend(test_deltas1[90..100].iter().cloned());
        assert_filter_iter_equal(&mut filter_iter, &result).await;

        let merge_iter = MergeIterator::create(
            &[resident_layer_1.get_as_delta(&ctx).await.unwrap()],
            &[],
            &ctx,
        );

        let mut filter_iter = FilterIterator::create(
            merge_iter,
            KeySpace {
                ranges: vec![
                    get_key(0)..get_key(10),
                    get_key(20)..get_key(30),
                    get_key(90)..get_key(95),
                ],
            },
            SparseKeySpace(KeySpace::default()),
        )
        .unwrap();
        let mut result = Vec::new();
        result.extend(test_deltas1[0..10].iter().cloned());
        result.extend(test_deltas1[20..30].iter().cloned());
        result.extend(test_deltas1[90..95].iter().cloned());
        assert_filter_iter_equal(&mut filter_iter, &result).await;
    }
}
