//! This module contains functions to serve per-tenant background processes,
//! such as compaction and GC

use std::ops::ControlFlow;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::context::{DownloadBehavior, RequestContext};
use crate::metrics::TENANT_TASK_EVENTS;
use crate::task_mgr;
use crate::task_mgr::{TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::throttle::Stats;
use crate::tenant::timeline::CompactionError;
use crate::tenant::{TenantShard, TenantState};
use rand::Rng;
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::{backoff, completion, pausable_failpoint};

static CONCURRENT_BACKGROUND_TASKS: once_cell::sync::Lazy<tokio::sync::Semaphore> =
    once_cell::sync::Lazy::new(|| {
        let total_threads = task_mgr::TOKIO_WORKER_THREADS.get();
        let permits = usize::max(
            1,
            // while a lot of the work is done on spawn_blocking, we still do
            // repartitioning in the async context. this should give leave us some workers
            // unblocked to be blocked on other work, hopefully easing any outside visible
            // effects of restarts.
            //
            // 6/8 is a guess; previously we ran with unlimited 8 and more from
            // spawn_blocking.
            (total_threads * 3).checked_div(4).unwrap_or(0),
        );
        assert_ne!(permits, 0, "we will not be adding in permits later");
        assert!(
            permits < total_threads,
            "need threads avail for shorter work"
        );
        tokio::sync::Semaphore::new(permits)
    });

#[derive(Debug, PartialEq, Eq, Clone, Copy, strum_macros::IntoStaticStr, enum_map::Enum)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum BackgroundLoopKind {
    Compaction,
    Gc,
    Eviction,
    IngestHouseKeeping,
    ConsumptionMetricsCollectMetrics,
    ConsumptionMetricsSyntheticSizeWorker,
    InitialLogicalSizeCalculation,
    HeatmapUpload,
    SecondaryDownload,
}

impl BackgroundLoopKind {
    fn as_static_str(&self) -> &'static str {
        self.into()
    }
}

/// Cancellation safe.
pub(crate) async fn concurrent_background_tasks_rate_limit_permit(
    loop_kind: BackgroundLoopKind,
    _ctx: &RequestContext,
) -> tokio::sync::SemaphorePermit<'static> {
    let _guard = crate::metrics::BACKGROUND_LOOP_SEMAPHORE.measure_acquisition(loop_kind);

    pausable_failpoint!(
        "initial-size-calculation-permit-pause",
        loop_kind == BackgroundLoopKind::InitialLogicalSizeCalculation
    );

    // TODO: assert that we run on BACKGROUND_RUNTIME; requires tokio_unstable Handle::id();
    match CONCURRENT_BACKGROUND_TASKS.acquire().await {
        Ok(permit) => permit,
        Err(_closed) => unreachable!("we never close the semaphore"),
    }
}

/// Start per tenant background loops: compaction and gc.
pub fn start_background_loops(
    tenant: &Arc<TenantShard>,
    background_jobs_can_start: Option<&completion::Barrier>,
) {
    let tenant_shard_id = tenant.tenant_shard_id;
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::Compaction,
        tenant_shard_id,
        None,
        &format!("compactor for tenant {tenant_shard_id}"),
        {
            let tenant = Arc::clone(tenant);
            let background_jobs_can_start = background_jobs_can_start.cloned();
            async move {
                let cancel = task_mgr::shutdown_token();
                tokio::select! {
                    _ = cancel.cancelled() => { return Ok(()) },
                    _ = completion::Barrier::maybe_wait(background_jobs_can_start) => {}
                };
                compaction_loop(tenant, cancel)
                    // If you rename this span, change the RUST_LOG env variable in test_runner/performance/test_branch_creation.py
                    .instrument(info_span!("compaction_loop", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug()))
                    .await;
                Ok(())
            }
        },
    );
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::GarbageCollector,
        tenant_shard_id,
        None,
        &format!("garbage collector for tenant {tenant_shard_id}"),
        {
            let tenant = Arc::clone(tenant);
            let background_jobs_can_start = background_jobs_can_start.cloned();
            async move {
                let cancel = task_mgr::shutdown_token();
                tokio::select! {
                    _ = cancel.cancelled() => { return Ok(()) },
                    _ = completion::Barrier::maybe_wait(background_jobs_can_start) => {}
                };
                gc_loop(tenant, cancel)
                    .instrument(info_span!("gc_loop", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug()))
                    .await;
                Ok(())
            }
        },
    );

    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::IngestHousekeeping,
        tenant_shard_id,
        None,
        &format!("ingest housekeeping for tenant {tenant_shard_id}"),
        {
            let tenant = Arc::clone(tenant);
            let background_jobs_can_start = background_jobs_can_start.cloned();
            async move {
                let cancel = task_mgr::shutdown_token();
                tokio::select! {
                    _ = cancel.cancelled() => { return Ok(()) },
                    _ = completion::Barrier::maybe_wait(background_jobs_can_start) => {}
                };
                ingest_housekeeping_loop(tenant, cancel)
                    .instrument(info_span!("ingest_housekeeping_loop", tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug()))
                    .await;
                Ok(())
            }
        },
    );
}

///
/// Compaction task's main loop
///
async fn compaction_loop(tenant: Arc<TenantShard>, cancel: CancellationToken) {
    const MAX_BACKOFF_SECS: f64 = 300.0;
    // How many errors we have seen consequtively
    let mut error_run_count = 0;

    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
        let ctx = RequestContext::todo_child(TaskKind::Compaction, DownloadBehavior::Download);
        let mut first = true;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    return;
                },
                tenant_wait_result = wait_for_active_tenant(&tenant) => match tenant_wait_result {
                    ControlFlow::Break(()) => return,
                    ControlFlow::Continue(()) => (),
                },
            }

            let period = tenant.get_compaction_period();

            // TODO: we shouldn't need to await to find tenant and this could be moved outside of
            // loop, #3501. There are also additional "allowed_errors" in tests.
            if first {
                first = false;
                if random_init_delay(period, &cancel).await.is_err() {
                    break;
                }
            }

            let sleep_duration;
            if period == Duration::ZERO {
                #[cfg(not(feature = "testing"))]
                info!("automatic compaction is disabled");
                // check again in 10 seconds, in case it's been enabled again.
                sleep_duration = Duration::from_secs(10)
            } else {
                let iteration = Iteration {
                    started_at: Instant::now(),
                    period,
                    kind: BackgroundLoopKind::Compaction,
                };

                // Run compaction
                let IterationResult { output, elapsed } = iteration
                    .run(tenant.compaction_iteration(&cancel, &ctx))
                    .await;
                match output {
                    Ok(has_pending_task) => {
                        error_run_count = 0;
                        // schedule the next compaction immediately in case there is a pending compaction task
                        sleep_duration = if has_pending_task {
                            Duration::ZERO
                        } else {
                            period
                        };
                    }
                    Err(e) => {
                        let wait_duration = backoff::exponential_backoff_duration_seconds(
                            error_run_count + 1,
                            1.0,
                            MAX_BACKOFF_SECS,
                        );
                        error_run_count += 1;
                        let wait_duration = Duration::from_secs_f64(wait_duration);
                        log_compaction_error(
                            &e,
                            error_run_count,
                            &wait_duration,
                            cancel.is_cancelled(),
                        );
                        sleep_duration = wait_duration;
                    }
                }

                // the duration is recorded by performance tests by enabling debug in this function
                tracing::debug!(
                    elapsed_ms = elapsed.as_millis(),
                    "compaction iteration complete"
                );
            };

            // Perhaps we did no work and the walredo process has been idle for some time:
            // give it a chance to shut down to avoid leaving walredo process running indefinitely.
            // TODO: move this to a separate task (housekeeping loop) that isn't affected by the back-off,
            // so we get some upper bound guarantee on when walredo quiesce / this throttling reporting here happens.
            if let Some(walredo_mgr) = &tenant.walredo_mgr {
                walredo_mgr.maybe_quiesce(period * 10);
            }

            // Sleep
            if tokio::time::timeout(sleep_duration, cancel.cancelled())
                .await
                .is_ok()
            {
                break;
            }
        }
    }
    .await;
    TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc();
}

fn log_compaction_error(
    e: &CompactionError,
    error_run_count: u32,
    sleep_duration: &std::time::Duration,
    task_cancelled: bool,
) {
    use crate::tenant::upload_queue::NotInitialized;
    use crate::tenant::PageReconstructError;
    use CompactionError::*;

    enum LooksLike {
        Info,
        Error,
    }

    let decision = match e {
        ShuttingDown => None,
        Offload(_) => Some(LooksLike::Error),
        _ if task_cancelled => Some(LooksLike::Info),
        Other(e) => {
            let root_cause = e.root_cause();

            let is_stopping = {
                let upload_queue = root_cause
                    .downcast_ref::<NotInitialized>()
                    .is_some_and(|e| e.is_stopping());

                let timeline = root_cause
                    .downcast_ref::<PageReconstructError>()
                    .is_some_and(|e| e.is_stopping());

                upload_queue || timeline
            };

            if is_stopping {
                Some(LooksLike::Info)
            } else {
                Some(LooksLike::Error)
            }
        }
    };

    match decision {
        Some(LooksLike::Info) => info!(
            "Compaction failed {error_run_count} times, retrying in {sleep_duration:?}: {e:#}",
        ),
        Some(LooksLike::Error) => error!(
            "Compaction failed {error_run_count} times, retrying in {sleep_duration:?}: {e:?}",
        ),
        None => {}
    }
}

///
/// GC task's main loop
///
async fn gc_loop(tenant: Arc<TenantShard>, cancel: CancellationToken) {
    const MAX_BACKOFF_SECS: f64 = 300.0;
    // How many errors we have seen consequtively
    let mut error_run_count = 0;

    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
        // GC might require downloading, to find the cutoff LSN that corresponds to the
        // cutoff specified as time.
        let ctx =
            RequestContext::todo_child(TaskKind::GarbageCollector, DownloadBehavior::Download);

        let mut first = true;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    return;
                },
                tenant_wait_result = wait_for_active_tenant(&tenant) => match tenant_wait_result {
                    ControlFlow::Break(()) => return,
                    ControlFlow::Continue(()) => (),
                },
            }

            let period = tenant.get_gc_period();

            if first {
                first = false;

                let delays = async {
                    random_init_delay(period, &cancel).await?;
                    Ok::<_, Cancelled>(())
                };

                if delays.await.is_err() {
                    break;
                }
            }

            let gc_horizon = tenant.get_gc_horizon();
            let sleep_duration;
            if period == Duration::ZERO || gc_horizon == 0 {
                #[cfg(not(feature = "testing"))]
                info!("automatic GC is disabled");
                // check again in 10 seconds, in case it's been enabled again.
                sleep_duration = Duration::from_secs(10);
            } else {
                let iteration = Iteration {
                    started_at: Instant::now(),
                    period,
                    kind: BackgroundLoopKind::Gc,
                };
                // Run gc
                let IterationResult { output, elapsed: _ } =
                    iteration.run(tenant.gc_iteration(None, gc_horizon, tenant.get_pitr_interval(), &cancel, &ctx))
                    .await;
                match output {
                    Ok(_) => {
                        error_run_count = 0;
                        sleep_duration = period;
                    }
                    Err(crate::tenant::GcError::TenantCancelled) => {
                        return;
                    }
                    Err(e) => {
                        let wait_duration = backoff::exponential_backoff_duration_seconds(
                            error_run_count + 1,
                            1.0,
                            MAX_BACKOFF_SECS,
                        );
                        error_run_count += 1;
                        let wait_duration = Duration::from_secs_f64(wait_duration);

                        if matches!(e, crate::tenant::GcError::TimelineCancelled) {
                            // Timeline was cancelled during gc. We might either be in an event
                            // that affects the entire tenant (tenant deletion, pageserver shutdown),
                            // or in one that affects the timeline only (timeline deletion).
                            // Therefore, don't exit the loop.
                            info!("Gc failed {error_run_count} times, retrying in {wait_duration:?}: {e:?}");
                        } else {
                            error!("Gc failed {error_run_count} times, retrying in {wait_duration:?}: {e:?}");
                        }

                        sleep_duration = wait_duration;
                    }
                }
            };

            if tokio::time::timeout(sleep_duration, cancel.cancelled())
                .await
                .is_ok()
            {
                break;
            }
        }
    }
    .await;
    TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc();
}

async fn ingest_housekeeping_loop(tenant: Arc<TenantShard>, cancel: CancellationToken) {
    TENANT_TASK_EVENTS.with_label_values(&["start"]).inc();
    async {
    let mut last_throttle_flag_reset_at = Instant::now();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    return;
                },
                tenant_wait_result = wait_for_active_tenant(&tenant) => match tenant_wait_result {
                    ControlFlow::Break(()) => return,
                    ControlFlow::Continue(()) => (),
                },
            }

            // We run ingest housekeeping with the same frequency as compaction: it is not worth
            // having a distinct setting.  But we don't run it in the same task, because compaction
            // blocks on acquiring the background job semaphore.
            let period = tenant.get_compaction_period();

            // If compaction period is set to zero (to disable it), then we will use a reasonable default
            let period = if period == Duration::ZERO {
                humantime::Duration::from_str(
                    pageserver_api::config::tenant_conf_defaults::DEFAULT_COMPACTION_PERIOD,
                )
                .unwrap()
                .into()
            } else {
                period
            };

            // Jitter the period by +/- 5%
            let period =
                rand::thread_rng().gen_range((period * (95)) / 100..(period * (105)) / 100);

            // Always sleep first: we do not need to do ingest housekeeping early in the lifetime of
            // a tenant, since it won't have started writing any ephemeral files yet.
            if tokio::time::timeout(period, cancel.cancelled())
                .await
                .is_ok()
            {
                break;
            }

            let iteration = Iteration {
                started_at: Instant::now(),
                period,
                kind: BackgroundLoopKind::IngestHouseKeeping,
            };
            iteration.run(tenant.ingest_housekeeping()).await;

            // TODO: rename the background loop kind to something more generic, like, tenant housekeeping.
            // Or just spawn another background loop for this throttle, it's not like it's super costly.
            info_span!(parent: None, "pagestream_throttle", tenant_id=%tenant.tenant_shard_id, shard_id=%tenant.tenant_shard_id.shard_slug()).in_scope(|| {
                let now = Instant::now();
                let prev = std::mem::replace(&mut last_throttle_flag_reset_at, now);
                let Stats { count_accounted_start, count_accounted_finish, count_throttled, sum_throttled_usecs} = tenant.pagestream_throttle.reset_stats();
                if count_throttled == 0 {
                    return;
                }
                let allowed_rps = tenant.pagestream_throttle.steady_rps();
                let delta = now - prev;
                info!(
                    n_seconds=%format_args!("{:.3}", delta.as_secs_f64()),
                    count_accounted = count_accounted_finish,  // don't break existing log scraping
                    count_throttled,
                    sum_throttled_usecs,
                    count_accounted_start, // log after pre-existing fields to not break existing log scraping
                    allowed_rps=%format_args!("{allowed_rps:.0}"),
                    "shard was throttled in the last n_seconds"
                );
            });
        }
    }
    .await;
    TENANT_TASK_EVENTS.with_label_values(&["stop"]).inc();
}

async fn wait_for_active_tenant(tenant: &Arc<TenantShard>) -> ControlFlow<()> {
    // if the tenant has a proper status already, no need to wait for anything
    if tenant.current_state() == TenantState::Active {
        ControlFlow::Continue(())
    } else {
        let mut tenant_state_updates = tenant.subscribe_for_state_updates();
        loop {
            match tenant_state_updates.changed().await {
                Ok(()) => {
                    let new_state = &*tenant_state_updates.borrow();
                    match new_state {
                        TenantState::Active => {
                            debug!("Tenant state changed to active, continuing the task loop");
                            return ControlFlow::Continue(());
                        }
                        state => {
                            debug!("Not running the task loop, tenant is not active: {state:?}");
                            continue;
                        }
                    }
                }
                Err(_sender_dropped_error) => {
                    return ControlFlow::Break(());
                }
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("cancelled")]
pub(crate) struct Cancelled;

/// Provide a random delay for background task initialization.
///
/// This delay prevents a thundering herd of background tasks and will likely keep them running on
/// different periods for more stable load.
pub(crate) async fn random_init_delay(
    period: Duration,
    cancel: &CancellationToken,
) -> Result<(), Cancelled> {
    if period == Duration::ZERO {
        return Ok(());
    }

    let d = {
        let mut rng = rand::thread_rng();
        rng.gen_range(Duration::ZERO..=period)
    };
    match tokio::time::timeout(d, cancel.cancelled()).await {
        Ok(_) => Err(Cancelled),
        Err(_) => Ok(()),
    }
}

struct Iteration {
    started_at: Instant,
    period: Duration,
    kind: BackgroundLoopKind,
}

struct IterationResult<O> {
    output: O,
    elapsed: Duration,
}

impl Iteration {
    #[instrument(skip_all)]
    pub(crate) async fn run<Fut, O>(self, fut: Fut) -> IterationResult<O>
    where
        Fut: std::future::Future<Output = O>,
    {
        let Self {
            started_at,
            period,
            kind,
        } = self;

        let mut fut = std::pin::pin!(fut);

        // Wrap `fut` into a future that logs a message every `period` so that we get a
        // very obvious breadcrumb in the logs _while_ a slow iteration is happening.
        let liveness_logger = async move {
            loop {
                match tokio::time::timeout(period, &mut fut).await {
                    Ok(x) => return x,
                    Err(_) => {
                        // info level as per the same rationale why warn_when_period_overrun is info
                        // =>  https://github.com/neondatabase/neon/pull/5724
                        info!("still running");
                    }
                }
            }
        };

        let output = liveness_logger.await;

        let elapsed = started_at.elapsed();
        warn_when_period_overrun(elapsed, period, kind);

        IterationResult { output, elapsed }
    }
}
/// Attention: the `task` and `period` beocme labels of a pageserver-wide prometheus metric.
pub(crate) fn warn_when_period_overrun(
    elapsed: Duration,
    period: Duration,
    task: BackgroundLoopKind,
) {
    // Duration::ZERO will happen because it's the "disable [bgtask]" value.
    if elapsed >= period && period != Duration::ZERO {
        // humantime does no significant digits clamping whereas Duration's debug is a bit more
        // intelligent. however it makes sense to keep the "configuration format" for period, even
        // though there's no way to output the actual config value.
        info!(
            ?elapsed,
            period = %humantime::format_duration(period),
            ?task,
            "task iteration took longer than the configured period"
        );
        crate::metrics::BACKGROUND_LOOP_PERIOD_OVERRUN_COUNT
            .with_label_values(&[task.as_static_str(), &format!("{}", period.as_secs())])
            .inc();
    }
}
