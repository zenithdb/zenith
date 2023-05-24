//! The per-timeline layer eviction task, which evicts data which has not been accessed for more
//! than a given threshold.
//!
//! Data includes all kinds of caches, namely:
//! - (in-memory layers)
//! - on-demand downloaded layer files on disk
//! - (cached layer file pages)
//! - derived data from layer file contents, namely:
//!     - initial logical size
//!     - partitioning
//!     - (other currently missing unknowns)
//!
//! Items with parentheses are not (yet) touched by this task.
//!
//! See write-up on restart on-demand download spike: <https://gist.github.com/problame/2265bf7b8dc398be834abfead36c76b5>
use std::{
    collections::HashMap,
    ops::ControlFlow,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, info_span, instrument, warn, Instrument};

use crate::{
    context::{DownloadBehavior, RequestContext},
    task_mgr::{self, TaskKind, BACKGROUND_RUNTIME},
    tenant::{
        config::{EvictionPolicy, EvictionPolicyLayerAccessThreshold},
        storage_layer::PersistentLayer,
        Tenant,
    },
};

use super::Timeline;

#[derive(Default)]
pub struct EvictionTaskTimelineState {
    last_layer_access_imitation: Option<tokio::time::Instant>,
}

#[derive(Default)]
pub struct EvictionTaskTenantState {
    last_layer_access_imitation: Option<Instant>,
}

impl Timeline {
    pub(super) fn launch_eviction_task(self: &Arc<Self>) {
        let self_clone = Arc::clone(self);
        task_mgr::spawn(
            BACKGROUND_RUNTIME.handle(),
            TaskKind::Eviction,
            Some(self.tenant_id),
            Some(self.timeline_id),
            &format!("layer eviction for {}/{}", self.tenant_id, self.timeline_id),
            false,
            async move {
                self_clone.eviction_task(task_mgr::shutdown_token()).await;
                info!("eviction task finishing");
                Ok(())
            },
        );
    }

    #[instrument(skip_all, fields(tenant_id = %self.tenant_id, timeline_id = %self.timeline_id))]
    async fn eviction_task(self: Arc<Self>, cancel: CancellationToken) {
        use crate::tenant::tasks::random_init_delay;
        {
            let policy = self.get_eviction_policy();
            let period = match policy {
                EvictionPolicy::LayerAccessThreshold(lat) => lat.period,
                EvictionPolicy::NoEviction => Duration::from_secs(10),
            };
            if random_init_delay(period, &cancel).await.is_err() {
                info!("shutting down");
                return;
            }
        }

        let ctx = RequestContext::new(TaskKind::Eviction, DownloadBehavior::Warn);
        loop {
            let policy = self.get_eviction_policy();
            let cf = self.eviction_iteration(&policy, &cancel, &ctx).await;

            match cf {
                ControlFlow::Break(()) => break,
                ControlFlow::Continue(sleep_until) => {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            info!("shutting down");
                            break;
                        }
                        _ = tokio::time::sleep_until(sleep_until) => { }
                    }
                }
            }
        }
    }

    #[instrument(skip_all, fields(policy_kind = policy.discriminant_str()))]
    async fn eviction_iteration(
        self: &Arc<Self>,
        policy: &EvictionPolicy,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> ControlFlow<(), Instant> {
        debug!("eviction iteration: {policy:?}");
        match policy {
            EvictionPolicy::NoEviction => {
                // check again in 10 seconds; XXX config watch mechanism
                ControlFlow::Continue(Instant::now() + Duration::from_secs(10))
            }
            EvictionPolicy::LayerAccessThreshold(p) => {
                let start = Instant::now();
                match self.eviction_iteration_threshold(p, cancel, ctx).await {
                    ControlFlow::Break(()) => return ControlFlow::Break(()),
                    ControlFlow::Continue(()) => (),
                }
                let elapsed = start.elapsed();
                crate::tenant::tasks::warn_when_period_overrun(elapsed, p.period, "eviction");
                crate::metrics::EVICTION_ITERATION_DURATION
                    .get_metric_with_label_values(&[
                        &format!("{}", p.period.as_secs()),
                        &format!("{}", p.threshold.as_secs()),
                    ])
                    .unwrap()
                    .observe(elapsed.as_secs_f64());
                ControlFlow::Continue(start + p.period)
            }
        }
    }

    async fn eviction_iteration_threshold(
        self: &Arc<Self>,
        p: &EvictionPolicyLayerAccessThreshold,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> ControlFlow<()> {
        let now = SystemTime::now();

        // If we evict layers but keep cached values derived from those layers, then
        // we face a storm of on-demand downloads after pageserver restart.
        // The reason is that the restart empties the caches, and so, the values
        // need to be re-computed by accessing layers, which we evicted while the
        // caches were filled.
        //
        // Solutions here would be one of the following:
        // 1. Have a persistent cache.
        // 2. Count every access to a cached value to the access stats of all layers
        //    that were accessed to compute the value in the first place.
        // 3. Invalidate the caches at a period of < p.threshold/2, so that the values
        //    get re-computed from layers, thereby counting towards layer access stats.
        // 4. Make the eviction task imitate the layer accesses that typically hit caches.
        //
        // We follow approach (4) here because in Neon prod deployment:
        // - page cache is quite small => high churn => low hit rate
        //   => eviction gets correct access stats
        // - value-level caches such as logical size & repatition have a high hit rate,
        //   especially for inactive tenants
        //   => eviction sees zero accesses for these
        //   => they cause the on-demand download storm on pageserver restart
        //
        // We should probably move to persistent caches in the future, or avoid
        // having inactive tenants attached to pageserver in the first place.
        match self.imitate_layer_accesses(p, cancel, ctx).await {
            ControlFlow::Break(()) => return ControlFlow::Break(()),
            ControlFlow::Continue(()) => (),
        }

        #[allow(dead_code)]
        #[derive(Debug, Default)]
        struct EvictionStats {
            candidates: usize,
            evicted: usize,
            errors: usize,
            not_evictable: usize,
            skipped_for_shutdown: usize,
        }

        let mut stats = EvictionStats::default();
        // Gather layers for eviction.
        // NB: all the checks can be invalidated as soon as we release the layer map lock.
        // We don't want to hold the layer map lock during eviction.
        // So, we just need to deal with this.
        let candidates: Vec<Arc<dyn PersistentLayer>> = {
            let layers = self.layers.read().unwrap();
            let mut candidates = Vec::new();
            for hist_layer in layers.iter_historic_layers() {
                if hist_layer.is_remote_layer() {
                    continue;
                }

                let last_activity_ts = hist_layer.access_stats().latest_activity().unwrap_or_else(|| {
                    // We only use this fallback if there's an implementation error.
                    // `latest_activity` already does rate-limited warn!() log.
                    debug!(layer=%hist_layer.filename().file_name(), "last_activity returns None, using SystemTime::now");
                    SystemTime::now()
                });

                let no_activity_for = match now.duration_since(last_activity_ts) {
                    Ok(d) => d,
                    Err(_e) => {
                        // We reach here if `now` < `last_activity_ts`, which can legitimately
                        // happen if there is an access between us getting `now`, and us getting
                        // the access stats from the layer.
                        //
                        // The other reason why it can happen is system clock skew because
                        // SystemTime::now() is not monotonic, so, even if there is no access
                        // to the layer after we get `now` at the beginning of this function,
                        // it could be that `now`  < `last_activity_ts`.
                        //
                        // To distinguish the cases, we would need to record `Instant`s in the
                        // access stats (i.e., monotonic timestamps), but then, the timestamps
                        // values in the access stats would need to be `Instant`'s, and hence
                        // they would be meaningless outside of the pageserver process.
                        // At the time of writing, the trade-off is that access stats are more
                        // valuable than detecting clock skew.
                        continue;
                    }
                };
                if no_activity_for > p.threshold {
                    candidates.push(hist_layer)
                }
            }
            candidates
        };
        stats.candidates = candidates.len();

        let remote_client = match self.remote_client.as_ref() {
            None => {
                error!(
                    num_candidates = candidates.len(),
                    "no remote storage configured, cannot evict layers"
                );
                return ControlFlow::Continue(());
            }
            Some(c) => c,
        };

        let results = match self
            .evict_layer_batch(remote_client, &candidates[..], cancel.clone())
            .await
        {
            Err(pre_err) => {
                stats.errors += candidates.len();
                error!("could not do any evictions: {pre_err:#}");
                return ControlFlow::Continue(());
            }
            Ok(results) => results,
        };
        assert_eq!(results.len(), candidates.len());
        for (l, result) in candidates.iter().zip(results) {
            match result {
                None => {
                    stats.skipped_for_shutdown += 1;
                }
                Some(Ok(true)) => {
                    debug!("evicted layer {l:?}");
                    stats.evicted += 1;
                }
                Some(Ok(false)) => {
                    debug!("layer is not evictable: {l:?}");
                    stats.not_evictable += 1;
                }
                Some(Err(e)) => {
                    // This variant is the case where an unexpected error happened during eviction.
                    // Expected errors that result in non-eviction are `Some(Ok(false))`.
                    // So, dump Debug here to gather as much info as possible in this rare case.
                    warn!("failed to evict layer {l:?}: {e:?}");
                    stats.errors += 1;
                }
            }
        }
        if stats.candidates == stats.not_evictable {
            debug!(stats=?stats, "eviction iteration complete");
        } else if stats.errors > 0 || stats.not_evictable > 0 {
            warn!(stats=?stats, "eviction iteration complete");
        } else {
            info!(stats=?stats, "eviction iteration complete");
        }
        ControlFlow::Continue(())
    }

    #[instrument(skip_all)]
    async fn imitate_layer_accesses(
        &self,
        p: &EvictionPolicyLayerAccessThreshold,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> ControlFlow<()> {
        let mut state = self.eviction_task_timeline_state.lock().await;
        match state.last_layer_access_imitation {
            Some(ts) if ts.elapsed() < p.threshold => { /* no need to run */ }
            _ => {
                self.imitate_timeline_cached_layer_accesses(ctx).await;
                state.last_layer_access_imitation = Some(tokio::time::Instant::now())
            }
        }
        drop(state);

        // This task is timeline-scoped, but the synthetic size calculation is tenant-scoped.
        // Make one of the tenant's timelines draw the short straw and run the calculation.
        // The others wait until the calculation is done so that they take into account the
        // imitated accesses that the winner made.
        let Ok(tenant) = crate::tenant::mgr::get_tenant(self.tenant_id, true).await else {
            // likely, we're shutting down
            return ControlFlow::Break(());
        };
        let mut state = tenant.eviction_task_tenant_state.lock().await;
        match state.last_layer_access_imitation {
            Some(ts) if ts.elapsed() < p.threshold => { /* no need to run */ }
            _ => {
                self.imitate_synthetic_size_calculation_worker(&tenant, ctx, cancel)
                    .await;
                state.last_layer_access_imitation = Some(tokio::time::Instant::now());
            }
        }
        drop(state);

        if cancel.is_cancelled() {
            return ControlFlow::Break(());
        }

        ControlFlow::Continue(())
    }

    /// Recompute the values which would cause on-demand downloads during restart.
    #[instrument(skip_all)]
    async fn imitate_timeline_cached_layer_accesses(&self, ctx: &RequestContext) {
        let lsn = self.get_last_record_lsn();
        // imitiate repartiting on first compactation
        if let Err(e) = self
            .collect_keyspace(lsn, ctx)
            .instrument(info_span!("collect_keyspace"))
            .await
        {
            // if this failed, we probably failed logical size because these use the same keys
            warn!("failed to collect keyspace but succeeded in calculating logical size: {e:#}");
        }
    }

    // Imitate the synthetic size calculation done by the consumption_metrics module.
    #[instrument(skip_all)]
    async fn imitate_synthetic_size_calculation_worker(
        &self,
        tenant: &Arc<Tenant>,
        ctx: &RequestContext,
        cancel: &CancellationToken,
    ) {
        if self.conf.metric_collection_endpoint.is_none() {
            // We don't start the consumption metrics task if this is not set in the config.
            // So, no need to imitate the accesses in that case.
            return;
        }

        // The consumption metrics are collected on a per-tenant basis, by a single
        // global background loop.
        // It limits the number of synthetic size calculations using the global
        // `concurrent_tenant_size_logical_size_queries` semaphore to not overload
        // the pageserver. (size calculation is somewhat expensive in terms of CPU and IOs).
        //
        // If we used that same semaphore here, then we'd compete for the
        // same permits, which may impact timeliness of consumption metrics.
        // That is a no-go, as consumption metrics are much more important
        // than what we do here.
        //
        // So, we have a separate semaphore, initialized to the same
        // number of permits as the `concurrent_tenant_size_logical_size_queries`.
        // In the worst, we would have twice the amount of concurrenct size calculations.
        // But in practice, the `p.threshold` >> `consumption metric interval`, and
        // we spread out the eviction task using `random_init_delay`.
        // So, the chance of the worst case is quite low in practice.
        // It runs as a per-tenant task, but the eviction_task.rs is per-timeline.
        // So, we must coordinate with other with other eviction tasks of this tenant.
        let mut throwaway_cache = HashMap::new();
        let gather = crate::tenant::size::gather_inputs(tenant, None, &mut throwaway_cache, ctx)
            .instrument(info_span!("gather_inputs"));

        tokio::select! {
            _ = cancel.cancelled() => {}
            gather_result = gather => {
                match gather_result {
                    Ok(_) => {},
                    Err(e) => {
                        // We don't care about the result, but, if it failed, we should log it,
                        // since consumption metric might be hitting the cached value and
                        // thus not encountering this error.
                        warn!("failed to imitate synthetic size calculation accesses: {e:#}")
                    }
                }
           }
        }
    }
}
