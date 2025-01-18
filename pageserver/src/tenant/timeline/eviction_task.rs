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

use pageserver_api::models::{EvictionPolicy, EvictionPolicyLayerAccessThreshold};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, info_span, instrument, warn, Instrument};

use crate::{
    context::{DownloadBehavior, RequestContext},
    pgdatadir_mapping::CollectKeySpaceError,
    task_mgr::{self, TaskKind, BACKGROUND_RUNTIME},
    tenant::{
        size::CalculateSyntheticSizeError, storage_layer::LayerVisibilityHint,
        tasks::BackgroundLoopKind, timeline::EvictionError, LogicalSizeCalculationCause,
        TenantShard,
    },
};

use utils::{completion, sync::gate::GateGuard};

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
    pub(super) fn launch_eviction_task(
        self: &Arc<Self>,
        parent: Arc<TenantShard>,
        background_tasks_can_start: Option<&completion::Barrier>,
    ) {
        let self_clone = Arc::clone(self);
        let background_tasks_can_start = background_tasks_can_start.cloned();
        task_mgr::spawn(
            BACKGROUND_RUNTIME.handle(),
            TaskKind::Eviction,
            self.tenant_shard_id,
            Some(self.timeline_id),
            &format!(
                "layer eviction for {}/{}",
                self.tenant_shard_id, self.timeline_id
            ),
            async move {
                tokio::select! {
                    _ = self_clone.cancel.cancelled() => { return Ok(()); }
                    _ = completion::Barrier::maybe_wait(background_tasks_can_start) => {}
                };

                self_clone.eviction_task(parent).await;
                Ok(())
            },
        );
    }

    #[instrument(skip_all, fields(tenant_id = %self.tenant_shard_id.tenant_id, shard_id = %self.tenant_shard_id.shard_slug(), timeline_id = %self.timeline_id))]
    async fn eviction_task(self: Arc<Self>, tenant: Arc<TenantShard>) {
        use crate::tenant::tasks::random_init_delay;

        // acquire the gate guard only once within a useful span
        let Ok(guard) = self.gate.enter() else {
            return;
        };

        {
            let policy = self.get_eviction_policy();
            let period = match policy {
                EvictionPolicy::LayerAccessThreshold(lat) => lat.period,
                EvictionPolicy::OnlyImitiate(lat) => lat.period,
                EvictionPolicy::NoEviction => Duration::from_secs(10),
            };
            if random_init_delay(period, &self.cancel).await.is_err() {
                return;
            }
        }

        let ctx = RequestContext::new(TaskKind::Eviction, DownloadBehavior::Warn);
        loop {
            let policy = self.get_eviction_policy();
            let cf = self
                .eviction_iteration(&tenant, &policy, &self.cancel, &guard, &ctx)
                .await;

            match cf {
                ControlFlow::Break(()) => break,
                ControlFlow::Continue(sleep_until) => {
                    if tokio::time::timeout_at(sleep_until, self.cancel.cancelled())
                        .await
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        }
    }

    #[instrument(skip_all, fields(policy_kind = policy.discriminant_str()))]
    async fn eviction_iteration(
        self: &Arc<Self>,
        tenant: &TenantShard,
        policy: &EvictionPolicy,
        cancel: &CancellationToken,
        gate: &GateGuard,
        ctx: &RequestContext,
    ) -> ControlFlow<(), Instant> {
        debug!("eviction iteration: {policy:?}");
        let start = Instant::now();
        let (period, threshold) = match policy {
            EvictionPolicy::NoEviction => {
                // check again in 10 seconds; XXX config watch mechanism
                return ControlFlow::Continue(Instant::now() + Duration::from_secs(10));
            }
            EvictionPolicy::LayerAccessThreshold(p) => {
                match self
                    .eviction_iteration_threshold(tenant, p, cancel, gate, ctx)
                    .await
                {
                    ControlFlow::Break(()) => return ControlFlow::Break(()),
                    ControlFlow::Continue(()) => (),
                }
                (p.period, p.threshold)
            }
            EvictionPolicy::OnlyImitiate(p) => {
                if self
                    .imitiate_only(tenant, p, cancel, gate, ctx)
                    .await
                    .is_break()
                {
                    return ControlFlow::Break(());
                }
                (p.period, p.threshold)
            }
        };

        let elapsed = start.elapsed();
        crate::tenant::tasks::warn_when_period_overrun(
            elapsed,
            period,
            BackgroundLoopKind::Eviction,
        );
        // FIXME: if we were to mix policies on a pageserver, we would have no way to sense this. I
        // don't think that is a relevant fear however, and regardless the imitation should be the
        // most costly part.
        crate::metrics::EVICTION_ITERATION_DURATION
            .get_metric_with_label_values(&[
                &format!("{}", period.as_secs()),
                &format!("{}", threshold.as_secs()),
            ])
            .unwrap()
            .observe(elapsed.as_secs_f64());

        ControlFlow::Continue(start + period)
    }

    async fn eviction_iteration_threshold(
        self: &Arc<Self>,
        tenant: &TenantShard,
        p: &EvictionPolicyLayerAccessThreshold,
        cancel: &CancellationToken,
        gate: &GateGuard,
        ctx: &RequestContext,
    ) -> ControlFlow<()> {
        let now = SystemTime::now();

        let permit = self.acquire_imitation_permit(cancel, ctx).await?;

        self.imitate_layer_accesses(tenant, p, cancel, gate, permit, ctx)
            .await?;

        #[derive(Debug, Default)]
        struct EvictionStats {
            candidates: usize,
            evicted: usize,
            errors: usize,
            not_evictable: usize,
            timeouts: usize,
            #[allow(dead_code)]
            skipped_for_shutdown: usize,
        }

        let mut stats = EvictionStats::default();
        // Gather layers for eviction.
        // NB: all the checks can be invalidated as soon as we release the layer map lock.
        // We don't want to hold the layer map lock during eviction.

        // So, we just need to deal with this.

        let mut js = tokio::task::JoinSet::new();
        {
            let guard = self.layers.read().await;

            guard
                .likely_resident_layers()
                .filter(|layer| {
                    let last_activity_ts = layer.latest_activity();

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
                            return false;
                        }
                    };

                    match layer.visibility() {
                        LayerVisibilityHint::Visible => {
                            // Usual case: a visible layer might be read any time, and we will keep it
                            // resident until it hits our configured TTL threshold.
                            no_activity_for > p.threshold
                        }
                        LayerVisibilityHint::Covered => {
                            // Covered layers: this is probably a layer that was recently covered by
                            // an image layer during compaction.  We don't evict it immediately, but
                            // it doesn't stay resident for the full `threshold`: we just keep it
                            // for a shorter time in case
                            // - it is used for Timestamp->LSN lookups
                            // - a new branch is created in recent history which will read this layer
                            no_activity_for > p.period
                        }
                    }
                })
                .cloned()
                .for_each(|layer| {
                    js.spawn(async move {
                        layer
                            .evict_and_wait(std::time::Duration::from_secs(5))
                            .await
                    });
                    stats.candidates += 1;
                });
        };

        let join_all = async move {
            while let Some(next) = js.join_next().await {
                match next {
                    Ok(Ok(())) => stats.evicted += 1,
                    Ok(Err(EvictionError::NotFound | EvictionError::Downloaded)) => {
                        stats.not_evictable += 1;
                    }
                    Ok(Err(EvictionError::Timeout)) => {
                        stats.timeouts += 1;
                    }
                    Err(je) if je.is_cancelled() => unreachable!("not used"),
                    Err(je) if je.is_panic() => {
                        /* already logged */
                        stats.errors += 1;
                    }
                    Err(je) => tracing::error!("unknown JoinError: {je:?}"),
                }
            }
            stats
        };

        tokio::select! {
            stats = join_all => {
                if stats.candidates == stats.not_evictable {
                    debug!(stats=?stats, "eviction iteration complete");
                } else if stats.errors > 0 || stats.not_evictable > 0 || stats.timeouts > 0 {
                    // reminder: timeouts are not eviction cancellations
                    warn!(stats=?stats, "eviction iteration complete");
                } else {
                    info!(stats=?stats, "eviction iteration complete");
                }
            }
            _ = cancel.cancelled() => {
                // just drop the joinset to "abort"
            }
        }

        ControlFlow::Continue(())
    }

    /// Like `eviction_iteration_threshold`, but without any eviction. Eviction will be done by
    /// disk usage based eviction task.
    async fn imitiate_only(
        self: &Arc<Self>,
        tenant: &TenantShard,
        p: &EvictionPolicyLayerAccessThreshold,
        cancel: &CancellationToken,
        gate: &GateGuard,
        ctx: &RequestContext,
    ) -> ControlFlow<()> {
        let permit = self.acquire_imitation_permit(cancel, ctx).await?;

        self.imitate_layer_accesses(tenant, p, cancel, gate, permit, ctx)
            .await
    }

    async fn acquire_imitation_permit(
        &self,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> ControlFlow<(), tokio::sync::SemaphorePermit<'static>> {
        let acquire_permit = crate::tenant::tasks::concurrent_background_tasks_rate_limit_permit(
            BackgroundLoopKind::Eviction,
            ctx,
        );

        tokio::select! {
            permit = acquire_permit => ControlFlow::Continue(permit),
            _ = cancel.cancelled() => ControlFlow::Break(()),
            _ = self.cancel.cancelled() => ControlFlow::Break(()),
        }
    }

    /// If we evict layers but keep cached values derived from those layers, then
    /// we face a storm of on-demand downloads after pageserver restart.
    /// The reason is that the restart empties the caches, and so, the values
    /// need to be re-computed by accessing layers, which we evicted while the
    /// caches were filled.
    ///
    /// Solutions here would be one of the following:
    /// 1. Have a persistent cache.
    /// 2. Count every access to a cached value to the access stats of all layers
    ///    that were accessed to compute the value in the first place.
    /// 3. Invalidate the caches at a period of < p.threshold/2, so that the values
    ///    get re-computed from layers, thereby counting towards layer access stats.
    /// 4. Make the eviction task imitate the layer accesses that typically hit caches.
    ///
    /// We follow approach (4) here because in Neon prod deployment:
    /// - page cache is quite small => high churn => low hit rate
    ///   => eviction gets correct access stats
    /// - value-level caches such as logical size & repatition have a high hit rate,
    ///   especially for inactive tenants
    ///   => eviction sees zero accesses for these
    ///   => they cause the on-demand download storm on pageserver restart
    ///
    /// We should probably move to persistent caches in the future, or avoid
    /// having inactive tenants attached to pageserver in the first place.
    #[instrument(skip_all)]
    async fn imitate_layer_accesses(
        &self,
        tenant: &TenantShard,
        p: &EvictionPolicyLayerAccessThreshold,
        cancel: &CancellationToken,
        gate: &GateGuard,
        permit: tokio::sync::SemaphorePermit<'static>,
        ctx: &RequestContext,
    ) -> ControlFlow<()> {
        if !self.tenant_shard_id.is_shard_zero() {
            // Shards !=0 do not maintain accurate relation sizes, and do not need to calculate logical size
            // for consumption metrics (consumption metrics are only sent from shard 0).  We may therefore
            // skip imitating logical size accesses for eviction purposes.
            return ControlFlow::Continue(());
        }

        let mut state = self.eviction_task_timeline_state.lock().await;

        // Only do the imitate_layer accesses approximately as often as the threshold.  A little
        // more frequently, to avoid this period racing with the threshold/period-th eviction iteration.
        let inter_imitate_period = p.threshold.checked_sub(p.period).unwrap_or(p.threshold);

        match state.last_layer_access_imitation {
            Some(ts) if ts.elapsed() < inter_imitate_period => { /* no need to run */ }
            _ => {
                self.imitate_timeline_cached_layer_accesses(gate, ctx).await;
                state.last_layer_access_imitation = Some(tokio::time::Instant::now())
            }
        }
        drop(state);

        if cancel.is_cancelled() {
            return ControlFlow::Break(());
        }

        // This task is timeline-scoped, but the synthetic size calculation is tenant-scoped.
        // Make one of the tenant's timelines draw the short straw and run the calculation.
        // The others wait until the calculation is done so that they take into account the
        // imitated accesses that the winner made.
        let (mut state, _permit) = {
            if let Ok(locked) = tenant.eviction_task_tenant_state.try_lock() {
                (locked, permit)
            } else {
                // we might need to wait for a long time here in case of pathological synthetic
                // size calculation performance
                drop(permit);
                let locked = tokio::select! {
                    locked = tenant.eviction_task_tenant_state.lock() => locked,
                    _ = self.cancel.cancelled() => {
                        return ControlFlow::Break(())
                    },
                    _ = cancel.cancelled() => {
                        return ControlFlow::Break(())
                    }
                };
                // then reacquire -- this will be bad if there is a lot of traffic, but because we
                // released the permit, the overall latency will be much better.
                let permit = self.acquire_imitation_permit(cancel, ctx).await?;
                (locked, permit)
            }
        };
        match state.last_layer_access_imitation {
            Some(ts) if ts.elapsed() < inter_imitate_period => { /* no need to run */ }
            _ => {
                self.imitate_synthetic_size_calculation_worker(tenant, cancel, ctx)
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
    async fn imitate_timeline_cached_layer_accesses(
        &self,
        guard: &GateGuard,
        ctx: &RequestContext,
    ) {
        let lsn = self.get_last_record_lsn();

        // imitiate on-restart initial logical size
        let size = self
            .calculate_logical_size(
                lsn,
                LogicalSizeCalculationCause::EvictionTaskImitation,
                guard,
                ctx,
            )
            .instrument(info_span!("calculate_logical_size"))
            .await;

        match &size {
            Ok(_size) => {
                // good, don't log it to avoid confusion
            }
            Err(_) => {
                // we have known issues for which we already log this on consumption metrics,
                // gc, and compaction. leave logging out for now.
                //
                // https://github.com/neondatabase/neon/issues/2539
            }
        }

        // imitiate repartiting on first compactation
        if let Err(e) = self
            .collect_keyspace(lsn, ctx)
            .instrument(info_span!("collect_keyspace"))
            .await
        {
            // if this failed, we probably failed logical size because these use the same keys
            if size.is_err() {
                // ignore, see above comment
            } else {
                match e {
                    CollectKeySpaceError::Cancelled => {
                        // Shutting down, ignore
                    }
                    err => {
                        warn!(
                            "failed to collect keyspace but succeeded in calculating logical size: {err:#}"
                        );
                    }
                }
            }
        }
    }

    // Imitate the synthetic size calculation done by the consumption_metrics module.
    #[instrument(skip_all)]
    async fn imitate_synthetic_size_calculation_worker(
        &self,
        tenant: &TenantShard,
        cancel: &CancellationToken,
        ctx: &RequestContext,
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
        let limit = self
            .conf
            .eviction_task_immitated_concurrent_logical_size_queries
            .inner();

        let mut throwaway_cache = HashMap::new();
        let gather = crate::tenant::size::gather_inputs(
            tenant,
            limit,
            None,
            &mut throwaway_cache,
            LogicalSizeCalculationCause::EvictionTaskImitation,
            cancel,
            ctx,
        )
        .instrument(info_span!("gather_inputs"));

        tokio::select! {
            _ = cancel.cancelled() => {}
            gather_result = gather => {
                match gather_result {
                    Ok(_) => {},
                    // It can happen sometimes that we hit this instead of the cancellation token firing above
                    Err(CalculateSyntheticSizeError::Cancelled) => {}
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
