//! # Memory Management
//!
//! Before Timeline shutdown we have a reference cycle
//! [`HandleInner::timeline`] => [`Timeline::handles`] => [`HandleInner`]..
//!
//! We do this so the hot path need only do one, uncontended, atomic increment,
//! the [`Cache::get`]'s [`Weak<HandleInner>::upgrade`]` call.
//!
//! The reference cycle breaks when the [`HandleInner`] gets dropped.
//! The expectation is that [`HandleInner`]s are short-lived.
//! See [`PerTimelineState::shutdown`] for more details on this expectation.

use std::collections::hash_map;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use pageserver_api::shard::ShardIdentity;
use tracing::instrument;
use tracing::trace;
use utils::id::TimelineId;
use utils::shard::ShardIndex;
use utils::shard::ShardNumber;

use crate::tenant::mgr::ShardSelector;

/// The requirement for Debug is so that #[derive(Debug)] works in some places.
pub(crate) trait Types: Sized + std::fmt::Debug {
    type TenantManagerError: Sized + std::fmt::Debug;
    type TenantManager: TenantManager<Self> + Sized;
    type Timeline: ArcTimeline<Self> + Sized;
}

/// [`crate::page_service`] uses this to repeatedly, cheaply, get [`Handle`]s to
/// the right [`Timeline`] for a given [`ShardTimelineId`].
///
/// Without this, we'd have to go through the [`crate::tenant::mgr`] for each
/// getpage request.
pub(crate) struct Cache<T: Types> {
    map: Map<T>,
}

type Map<T> = HashMap<ShardTimelineId, Weak<HandleInner<T>>>;

impl<T: Types> Default for Cache<T> {
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Hash, Clone, Copy)]
pub(crate) struct ShardTimelineId {
    pub(crate) shard_index: ShardIndex,
    pub(crate) timeline_id: TimelineId,
}

/// This struct is a reference to [`Timeline`]
/// that keeps the [`Timeline::gate`] open while it exists.
///
/// The idea is that for every getpage request, [`crate::page_service`] acquires
/// one of these through [`Cache::get`], uses it to serve that request, then drops
/// the handle.
pub(crate) struct Handle<T: Types>(Arc<HandleInner<T>>);
struct HandleInner<T: Types> {
    shut_down: AtomicBool,
    timeline: T::Timeline,
    // The timeline's gate held open.
    _gate_guard: utils::sync::gate::GateGuard,
}

/// This struct embedded into each [`Timeline`] object to keep the [`Cache`]'s
/// [`Weak<HandleInner>`] alive while the [`Timeline`] is alive.
pub struct PerTimelineState<T: Types> {
    // None = shutting down
    handles: Mutex<Option<Vec<Arc<HandleInner<T>>>>>,
}

impl<T: Types> Default for PerTimelineState<T> {
    fn default() -> Self {
        Self {
            handles: Mutex::new(Some(Vec::default())),
        }
    }
}

/// We're abstract over the [`crate::tenant::mgr`] so we can test this module.
pub(crate) trait TenantManager<T: Types> {
    /// Invoked by [`Cache::get`] to resolve a [`ShardTimelineId`] to a [`Timeline`].
    /// Errors are returned as [`GetError::TenantManager`].
    async fn resolve(
        &self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
    ) -> Result<T::Timeline, T::TenantManagerError>;
}

pub(crate) trait ArcTimeline<T: Types>: Clone {
    fn gate(&self) -> &utils::sync::gate::Gate;
    fn shard_timeline_id(&self) -> ShardTimelineId;
    fn get_shard_identity(&self) -> &ShardIdentity;
    fn per_timeline_state(&self) -> &PerTimelineState<T>;
}

/// Errors returned by [`Cache::get`].
#[derive(Debug)]
pub(crate) enum GetError<T: Types> {
    TenantManager(T::TenantManagerError),
    TimelineGateClosed,
    PerTimelineStateShutDown,
}

enum RoutingResult<T: Types> {
    FastPath(Handle<T>),
    SlowPath(ShardTimelineId),
    NeedConsultTenantManager,
}

impl<T: Types> Cache<T> {
    /// Get a [`Handle`] for the Timeline identified by `key`.
    ///
    /// The returned [`Handle`] must be short-lived so that
    /// the [`Timeline::gate`] is not held open longer than necessary.
    ///
    /// Note: this method will not fail if the timeline is
    /// [`Timeline::is_stopping`] or [`Timeline::cancel`]led,
    /// but the gate is still open.
    ///
    /// The `Timeline`'s methods, which are invoked through the
    /// returned [`Handle`], are responsible for checking these conditions
    /// and if so, return an error that causes the page service to
    /// close the connection.
    #[instrument(level = "trace", skip_all)]
    pub(crate) async fn get(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
        tenant_manager: &T::TenantManager,
    ) -> Result<Handle<T>, GetError<T>> {
        // terminates because each iteration removes an element from the map
        loop {
            let handle = self
                .get_impl(timeline_id, shard_selector, tenant_manager)
                .await?;
            if handle.0.shut_down.load(Ordering::Relaxed) {
                let removed = self
                    .map
                    .remove(&handle.0.timeline.shard_timeline_id())
                    .expect("invariant of get_impl is that the returned handle is in the map");
                assert!(
                    Weak::ptr_eq(&removed, &Arc::downgrade(&handle.0)),
                    "shard_timeline_id() incorrect?"
                );
            } else {
                return Ok(handle);
            }
        }
    }

    #[instrument(level = "trace", skip_all)]
    async fn get_impl(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
        tenant_manager: &T::TenantManager,
    ) -> Result<Handle<T>, GetError<T>> {
        let miss: ShardSelector = {
            let routing_state = self.shard_routing(timeline_id, shard_selector);
            match routing_state {
                RoutingResult::FastPath(handle) => return Ok(handle),
                RoutingResult::SlowPath(key) => match self.map.get(&key) {
                    Some(cached) => match cached.upgrade() {
                        Some(upgraded) => return Ok(Handle(upgraded)),
                        None => {
                            trace!("handle cache stale");
                            self.map.remove(&key).unwrap();
                            ShardSelector::Known(key.shard_index)
                        }
                    },
                    None => ShardSelector::Known(key.shard_index),
                },
                RoutingResult::NeedConsultTenantManager => shard_selector,
            }
        };
        self.get_miss(timeline_id, miss, tenant_manager).await
    }

    #[inline(always)]
    fn shard_routing(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
    ) -> RoutingResult<T> {
        loop {
            // terminates because when every iteration we remove an element from the map
            let Some((first_key, first_handle)) = self.map.iter().next() else {
                return RoutingResult::NeedConsultTenantManager;
            };
            let Some(first_handle) = first_handle.upgrade() else {
                // TODO: dedup with get()
                trace!("handle cache stale");
                let first_key_owned = *first_key;
                self.map.remove(&first_key_owned).unwrap();
                continue;
            };

            let first_handle_shard_identity = first_handle.timeline.get_shard_identity();
            let make_shard_index = |shard_num: ShardNumber| ShardIndex {
                shard_number: shard_num,
                shard_count: first_handle_shard_identity.count,
            };

            let need_idx = match shard_selector {
                ShardSelector::Page(key) => {
                    make_shard_index(first_handle_shard_identity.get_shard_number(&key))
                }
                ShardSelector::Zero => make_shard_index(ShardNumber(0)),
                ShardSelector::Known(shard_idx) => shard_idx,
            };
            let need_shard_timeline_id = ShardTimelineId {
                shard_index: need_idx,
                timeline_id,
            };
            let first_handle_shard_timeline_id = ShardTimelineId {
                shard_index: first_handle_shard_identity.shard_index(),
                timeline_id: first_handle.timeline.shard_timeline_id().timeline_id,
            };

            if need_shard_timeline_id == first_handle_shard_timeline_id {
                return RoutingResult::FastPath(Handle(first_handle));
            } else {
                return RoutingResult::SlowPath(need_shard_timeline_id);
            }
        }
    }

    #[instrument(level = "trace", skip_all)]
    #[inline(always)]
    async fn get_miss(
        &mut self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
        tenant_manager: &T::TenantManager,
    ) -> Result<Handle<T>, GetError<T>> {
        match tenant_manager.resolve(timeline_id, shard_selector).await {
            Ok(timeline) => {
                let key = timeline.shard_timeline_id();
                match &shard_selector {
                    ShardSelector::Zero => assert_eq!(key.shard_index.shard_number, ShardNumber(0)),
                    ShardSelector::Page(_) => (), // gotta trust tenant_manager
                    ShardSelector::Known(idx) => assert_eq!(idx, &key.shard_index),
                }

                let gate_guard = match timeline.gate().enter() {
                    Ok(guard) => guard,
                    Err(_) => {
                        return Err(GetError::TimelineGateClosed);
                    }
                };
                trace!("creating new HandleInner");
                let handle = Arc::new(
                    // TODO: global metric that keeps track of the number of live HandlerTimeline instances
                    // so we can identify reference cycle bugs.
                    HandleInner {
                        shut_down: AtomicBool::new(false),
                        _gate_guard: gate_guard,
                        timeline: timeline.clone(),
                    },
                );
                let handle = {
                    let mut lock_guard = timeline
                        .per_timeline_state()
                        .handles
                        .lock()
                        .expect("mutex poisoned");
                    match &mut *lock_guard {
                        Some(timeline_handlers_list) => {
                            for handler in timeline_handlers_list.iter() {
                                assert!(!Arc::ptr_eq(handler, &handle));
                            }
                            timeline_handlers_list.push(Arc::clone(&handle));
                            match self.map.entry(key) {
                                hash_map::Entry::Occupied(mut o) => {
                                    // This should not happen in practice because the page_service
                                    // isn't calling get() concurrently(). Yet, let's deal with
                                    // this condition here so this module is a truly generic cache.
                                    if let Some(existing) = o.get().upgrade() {
                                        // Reuse to minimize the number of handles per timeline that are alive in the system.
                                        existing
                                    } else {
                                        o.insert(Arc::downgrade(&handle));
                                        handle
                                    }
                                }
                                hash_map::Entry::Vacant(v) => {
                                    v.insert(Arc::downgrade(&handle));
                                    handle
                                }
                            }
                        }
                        None => {
                            return Err(GetError::PerTimelineStateShutDown);
                        }
                    }
                };
                Ok(Handle(handle))
            }
            Err(e) => Err(GetError::TenantManager(e)),
        }
    }
}

impl<T: Types> PerTimelineState<T> {
    /// After this method returns, [`Cache::get`] will never again return a [`Handle`]
    /// to this Timeline object, even if [`TenantManager::resolve`] would still resolve
    /// to this Timeline object.
    ///
    /// Already-alive [`Handle`]s for this Timeline will remain open
    /// and keep the `Arc<Timeline>` allocation alive.
    /// The expectation is that
    /// 1. [`Handle`]s are short-lived (single call to [`Timeline`] method) and
    /// 2. [`Timeline`] methods invoked through [`Handle`] are sensitive
    ///    to [`Timeline::cancel`] and [`Timeline::is_stopping`].
    /// Thus, the [`Arc<Timeline>`] allocation's lifetime will only be minimally
    /// extended by the already-alive [`Handle`]s.
    #[instrument(level = "trace", skip_all)]
    pub(super) fn shutdown(&self) {
        let handles = self
            .handles
            .lock()
            .expect("mutex poisoned")
            // NB: this .take() sets locked to None.
            // That's what makes future `Cache::get` misses fail.
            // Cache hits are taken care of below.
            .take();
        let Some(handles) = handles else {
            trace!("already shut down");
            return;
        };
        for handle in &handles {
            // Make hits fail.
            handle.shut_down.store(true, Ordering::Relaxed);
        }
        drop(handles);
    }
}

impl<T: Types> std::ops::Deref for Handle<T> {
    type Target = T::Timeline;
    fn deref(&self) -> &Self::Target {
        &self.0.timeline
    }
}

#[cfg(test)]
impl<T: Types> Drop for HandleInner<T> {
    fn drop(&mut self) {
        trace!("HandleInner dropped");
    }
}

// When dropping a [`Cache`], prune its handles in the [`PerTimelineState`].
impl<T: Types> Drop for Cache<T> {
    fn drop(&mut self) {
        for (_, weak) in self.map.drain() {
            if let Some(handle) = weak.upgrade() {
                // handle kept alive in PerTimelineState
                let timeline = handle.timeline.per_timeline_state();
                let mut handles = timeline.handles.lock().expect("mutex poisoned");
                if let Some(handles) = &mut *handles {
                    // handles aren't shared across Cache's
                    handles.retain(|h| !Arc::ptr_eq(h, &handle)); // TODO: use hashmap instead of vec retain
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use pageserver_api::{
        key::{rel_block_to_key, Key, DBDIR_KEY},
        models::ShardParameters,
        reltag::RelTag,
        shard::ShardStripeSize,
    };
    use utils::shard::ShardCount;

    use super::*;

    const FOREVER: std::time::Duration = std::time::Duration::from_secs(u64::MAX);

    #[derive(Debug)]
    struct TestTypes;
    impl Types for TestTypes {
        type TenantManagerError = anyhow::Error;
        type TenantManager = StubManager;
        type Timeline = Arc<StubTimeline>;
    }

    struct StubManager {
        shards: Vec<Arc<StubTimeline>>,
    }

    struct StubTimeline {
        gate: utils::sync::gate::Gate,
        id: TimelineId,
        shard: ShardIdentity,
        per_timeline_state: PerTimelineState<TestTypes>,
        myself: Weak<StubTimeline>,
    }

    impl StubTimeline {
        fn getpage(&self) {
            // do nothing
        }
    }

    impl ArcTimeline<TestTypes> for Arc<StubTimeline> {
        fn gate(&self) -> &utils::sync::gate::Gate {
            &self.gate
        }

        fn shard_timeline_id(&self) -> ShardTimelineId {
            ShardTimelineId {
                shard_index: self.shard.shard_index(),
                timeline_id: self.id,
            }
        }

        fn get_shard_identity(&self) -> &ShardIdentity {
            &self.shard
        }

        fn per_timeline_state(&self) -> &PerTimelineState<TestTypes> {
            &self.per_timeline_state
        }
    }

    impl TenantManager<TestTypes> for StubManager {
        async fn resolve(
            &self,
            timeline_id: TimelineId,
            shard_selector: ShardSelector,
        ) -> anyhow::Result<Arc<StubTimeline>> {
            for timeline in &self.shards {
                if timeline.id == timeline_id {
                    match &shard_selector {
                        ShardSelector::Zero if timeline.shard.is_shard_zero() => {
                            return Ok(Arc::clone(timeline));
                        }
                        ShardSelector::Zero => continue,
                        ShardSelector::Page(key) if timeline.shard.is_key_local(key) => {
                            return Ok(Arc::clone(timeline));
                        }
                        ShardSelector::Page(_) => continue,
                        ShardSelector::Known(idx) if idx == &timeline.shard.shard_index() => {
                            return Ok(Arc::clone(timeline));
                        }
                        ShardSelector::Known(_) => continue,
                    }
                }
            }
            anyhow::bail!("not found")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_timeline_shutdown() {
        crate::tenant::harness::setup_logging();

        let timeline_id = TimelineId::generate();
        let shard0 = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::unsharded(),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let mgr = StubManager {
            shards: vec![shard0.clone()],
        };
        let key = DBDIR_KEY;

        let mut cache = Cache::<TestTypes>::default();

        //
        // fill the cache
        //
        assert_eq!(
            (Arc::strong_count(&shard0), Arc::weak_count(&shard0)),
            (2, 1),
            "strong: shard0, mgr; weak: myself"
        );

        let handle: Handle<_> = cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have the timeline");
        let handle_inner_weak = Arc::downgrade(&handle.0);
        assert!(Weak::ptr_eq(&handle.myself, &shard0.myself));
        assert_eq!(
            (
                Weak::strong_count(&handle_inner_weak),
                Weak::weak_count(&handle_inner_weak)
            ),
            (2, 2),
            "strong: handle, per_timeline_state, weak: handle_inner_weak, cache"
        );
        assert_eq!(cache.map.len(), 1);

        assert_eq!(
            (Arc::strong_count(&shard0), Arc::weak_count(&shard0)),
            (3, 1),
            "strong: handleinner(per_timeline_state), shard0, mgr; weak: myself"
        );
        drop(handle);
        assert_eq!(
            (Arc::strong_count(&shard0), Arc::weak_count(&shard0)),
            (3, 1),
            "strong: handleinner(per_timeline_state), shard0, mgr; weak: myself"
        );

        //
        // demonstrate that Handle holds up gate closure
        // but shutdown prevents new handles from being handed out
        //

        tokio::select! {
            _ = shard0.gate.close() => {
                panic!("cache and per-timeline handler state keep cache open");
            }
            _ = tokio::time::sleep(FOREVER) => {
                // NB: first poll of close() makes it enter closing state
            }
        }

        let handle = cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have the timeline");
        assert!(Weak::ptr_eq(&handle.myself, &shard0.myself));

        // SHUTDOWN
        shard0.per_timeline_state.shutdown(); // keeping handle alive across shutdown

        assert_eq!(
            1,
            Weak::strong_count(&handle_inner_weak),
            "through local var handle"
        );
        assert_eq!(
            cache.map.len(),
            1,
            "this is an implementation detail but worth pointing out: we can't clear the cache from shutdown(), it's cleared on first access after"
        );
        assert_eq!(
            (Arc::strong_count(&shard0), Arc::weak_count(&shard0)),
            (3, 1),
            "strong: handleinner(via handle), shard0, mgr; weak: myself"
        );

        // this handle is perfectly usable
        handle.getpage();

        cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .err()
            .expect("documented behavior: can't get new handle after shutdown, even if there is an alive Handle");
        assert_eq!(
            cache.map.len(),
            0,
            "first access after shutdown cleans up the Weak's from the cache"
        );

        tokio::select! {
            _ = shard0.gate.close() => {
                panic!("handle is keeping gate open");
            }
            _ = tokio::time::sleep(FOREVER) => { }
        }

        drop(handle);
        assert_eq!(
            0,
            Weak::strong_count(&handle_inner_weak),
            "the HandleInner destructor already ran"
        );
        assert_eq!(
            (Arc::strong_count(&shard0), Arc::weak_count(&shard0)),
            (2, 1),
            "strong: shard0, mgr; weak: myself"
        );

        // closing gate succeeds after dropping handle
        tokio::select! {
            _ = shard0.gate.close() => { }
            _ = tokio::time::sleep(FOREVER) => {
                panic!("handle is dropped, no other gate holders exist")
            }
        }

        // map gets cleaned on next lookup
        cache
            .get(timeline_id, ShardSelector::Page(key), &mgr)
            .await
            .err()
            .expect("documented behavior: can't get new handle after shutdown");
        assert_eq!(cache.map.len(), 0);

        // ensure all refs to shard0 are gone and we're not leaking anything
        let myself = Weak::clone(&shard0.myself);
        drop(shard0);
        drop(mgr);
        assert_eq!(Weak::strong_count(&myself), 0);
    }

    #[tokio::test]
    async fn test_multiple_timelines_and_deletion() {
        crate::tenant::harness::setup_logging();

        let timeline_a = TimelineId::generate();
        let timeline_b = TimelineId::generate();
        assert_ne!(timeline_a, timeline_b);
        let timeline_a = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_a,
            shard: ShardIdentity::unsharded(),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let timeline_b = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_b,
            shard: ShardIdentity::unsharded(),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let mut mgr = StubManager {
            shards: vec![timeline_a.clone(), timeline_b.clone()],
        };
        let key = DBDIR_KEY;

        let mut cache = Cache::<TestTypes>::default();

        cache
            .get(timeline_a.id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have it");
        cache
            .get(timeline_b.id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we have it");
        assert_eq!(cache.map.len(), 2);

        // delete timeline A
        timeline_a.per_timeline_state.shutdown();
        mgr.shards.retain(|t| t.id != timeline_a.id);
        assert!(
            mgr.resolve(timeline_a.id, ShardSelector::Page(key))
                .await
                .is_err(),
            "broken StubManager implementation"
        );

        assert_eq!(
            cache.map.len(),
            2,
            "cache still has a Weak handle to Timeline A"
        );
        cache
            .get(timeline_a.id, ShardSelector::Page(key), &mgr)
            .await
            .err()
            .expect("documented behavior: can't get new handle after shutdown");
        assert_eq!(cache.map.len(), 1, "next access cleans up the cache");

        cache
            .get(timeline_b.id, ShardSelector::Page(key), &mgr)
            .await
            .expect("we still have it");
    }

    fn make_relation_key_for_shard(shard: ShardNumber, params: &ShardParameters) -> Key {
        rel_block_to_key(
            RelTag {
                spcnode: 1663,
                dbnode: 208101,
                relnode: 2620,
                forknum: 0,
            },
            shard.0 as u32 * params.stripe_size.0,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn test_shard_split() {
        crate::tenant::harness::setup_logging();
        let timeline_id = TimelineId::generate();
        let parent = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::unsharded(),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let child_params = ShardParameters {
            count: ShardCount(2),
            stripe_size: ShardStripeSize::default(),
        };
        let child0 = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::from_params(ShardNumber(0), &child_params),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let child1 = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::from_params(ShardNumber(1), &child_params),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let child_shards_by_shard_number = [child0.clone(), child1.clone()];

        let mut cache = Cache::<TestTypes>::default();

        // fill the cache with the parent
        for i in 0..2 {
            let handle = cache
                .get(
                    timeline_id,
                    ShardSelector::Page(make_relation_key_for_shard(ShardNumber(i), &child_params)),
                    &StubManager {
                        shards: vec![parent.clone()],
                    },
                )
                .await
                .expect("we have it");
            assert!(
                Weak::ptr_eq(&handle.myself, &parent.myself),
                "mgr returns parent first"
            );
            drop(handle);
        }

        //
        // SHARD SPLIT: tenant manager changes, but the cache isn't informed
        //

        // while we haven't shut down the parent, the cache will return the cached parent, even
        // if the tenant manager returns the child
        for i in 0..2 {
            let handle = cache
                .get(
                    timeline_id,
                    ShardSelector::Page(make_relation_key_for_shard(ShardNumber(i), &child_params)),
                    &StubManager {
                        shards: vec![], // doesn't matter what's in here, the cache is fully loaded
                    },
                )
                .await
                .expect("we have it");
            assert!(
                Weak::ptr_eq(&handle.myself, &parent.myself),
                "mgr returns parent"
            );
            drop(handle);
        }

        let parent_handle = cache
            .get(
                timeline_id,
                ShardSelector::Page(make_relation_key_for_shard(ShardNumber(0), &child_params)),
                &StubManager {
                    shards: vec![parent.clone()],
                },
            )
            .await
            .expect("we have it");
        assert!(Weak::ptr_eq(&parent_handle.myself, &parent.myself));

        // invalidate the cache
        parent.per_timeline_state.shutdown();

        // the cache will now return the child, even though the parent handle still exists
        for i in 0..2 {
            let handle = cache
                .get(
                    timeline_id,
                    ShardSelector::Page(make_relation_key_for_shard(ShardNumber(i), &child_params)),
                    &StubManager {
                        shards: vec![child0.clone(), child1.clone()], // <====== this changed compared to previous loop
                    },
                )
                .await
                .expect("we have it");
            assert!(
                Weak::ptr_eq(
                    &handle.myself,
                    &child_shards_by_shard_number[i as usize].myself
                ),
                "mgr returns child"
            );
            drop(handle);
        }

        // all the while the parent handle kept the parent gate open
        tokio::select! {
            _ = parent_handle.gate.close() => {
                panic!("parent handle is keeping gate open");
            }
            _ = tokio::time::sleep(FOREVER) => { }
        }
        drop(parent_handle);
        tokio::select! {
            _ = parent.gate.close() => { }
            _ = tokio::time::sleep(FOREVER) => {
                panic!("parent handle is dropped, no other gate holders exist")
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_connection_handler_exit() {
        crate::tenant::harness::setup_logging();
        let timeline_id = TimelineId::generate();
        let shard0 = Arc::new_cyclic(|myself| StubTimeline {
            gate: Default::default(),
            id: timeline_id,
            shard: ShardIdentity::unsharded(),
            per_timeline_state: PerTimelineState::default(),
            myself: myself.clone(),
        });
        let mgr = StubManager {
            shards: vec![shard0.clone()],
        };
        let key = DBDIR_KEY;

        // Simulate 10 connections that's opened, used, and closed
        let mut used_handles = vec![];
        for _ in 0..10 {
            let mut cache = Cache::<TestTypes>::default();
            let handle = {
                let handle = cache
                    .get(timeline_id, ShardSelector::Page(key), &mgr)
                    .await
                    .expect("we have the timeline");
                assert!(Weak::ptr_eq(&handle.myself, &shard0.myself));
                handle
            };
            handle.getpage();
            used_handles.push(Arc::downgrade(&handle.0));
        }

        // No handles exist, thus gates are closed and don't require shutdown
        assert!(used_handles
            .iter()
            .all(|weak| Weak::strong_count(weak) == 0));

        // ... thus the gate should close immediately, even without shutdown
        tokio::select! {
            _ = shard0.gate.close() => { }
            _ = tokio::time::sleep(FOREVER) => {
                panic!("handle is dropped, no other gate holders exist")
            }
        }
    }
}
