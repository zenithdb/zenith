use futures::Future;
use rand::Rng;
use std::{
    collections::HashMap,
    marker::PhantomData,
    pin::Pin,
    time::{Duration, Instant},
};

use pageserver_api::shard::TenantShardId;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use utils::{completion::Barrier, yielding_loop::yielding_loop};

use super::{CommandRequest, CommandResponse, SecondaryTenantError};

/// Scheduling interval is the time between calls to JobGenerator::schedule.
/// When we schedule jobs, the job generator may provide a hint of its preferred
/// interval, which we will respect within these intervals.
const MAX_SCHEDULING_INTERVAL: Duration = Duration::from_secs(10);
const MIN_SCHEDULING_INTERVAL: Duration = Duration::from_secs(1);

/// Jitter a Duration by an integer percentage.  Returned values are uniform
/// in the range 100-pct..100+pct (i.e. a 5% jitter is 5% either way: a ~10% range)
pub(super) fn period_jitter(d: Duration, pct: u32) -> Duration {
    if d == Duration::ZERO {
        d
    } else {
        rand::thread_rng().gen_range((d * (100 - pct)) / 100..(d * (100 + pct)) / 100)
    }
}

/// When a periodic task first starts, it should wait for some time in the range 0..period, so
/// that starting many such tasks at the same time spreads them across the time range.
pub(super) fn period_warmup(period: Duration) -> Duration {
    if period == Duration::ZERO {
        period
    } else {
        rand::thread_rng().gen_range(Duration::ZERO..period)
    }
}

/// Scheduling helper for background work across many tenants.
///
/// Systems that need to run background work across many tenants may use this type
/// to schedule jobs within a concurrency limit, along with their own [`JobGenerator`]
/// implementation to provide the work to execute.  This is a simple scheduler that just
/// polls the generator for outstanding work, replacing its queue of pending work with
/// what the generator yields on each call: the job generator can change its mind about
/// the order of jobs between calls.  The job generator is notified when jobs complete,
/// and additionally may expose a command hook to generate jobs on-demand (e.g. to implement
/// admin APIs).
///
/// For an example see [`crate::tenant::secondary::heatmap_uploader`]
///
/// G: A JobGenerator that this scheduler will poll to find pending jobs
/// PJ: 'Pending Job': type for job descriptors that are ready to run
/// RJ: 'Running Job' type' for jobs that have been spawned
/// C : 'Completion' type that spawned jobs will send when they finish
/// CMD: 'Command' type that the job generator will accept to create jobs on-demand
pub(super) struct TenantBackgroundJobs<G, PJ, RJ, C, CMD>
where
    G: JobGenerator<PJ, RJ, C, CMD>,
    C: Completion,
    PJ: PendingJob,
    RJ: RunningJob,
{
    generator: G,

    /// Ready to run.  Will progress to `running` once concurrent limit is satisfied, or
    /// be removed on next scheduling pass.
    pending: std::collections::VecDeque<PJ>,

    /// Tasks currently running in Self::tasks for these tenants.  Check this map
    /// before pushing more work into pending for the same tenant.
    running: HashMap<TenantShardId, RJ>,

    tasks: JoinSet<C>,

    concurrency: usize,

    /// How often we would like schedule_interval to be called.
    pub(super) scheduling_interval: Duration,

    _phantom: PhantomData<(PJ, RJ, C, CMD)>,
}

pub(crate) trait JobGenerator<PJ, RJ, C, CMD>
where
    C: Completion,
    PJ: PendingJob,
    RJ: RunningJob,
{
    /// Called at each scheduling interval.  Return a list of jobs to run, most urgent first.
    ///
    /// This function may be expensive (e.g. walk all tenants), but should not do any I/O.
    /// Implementations should take care to yield the executor periodically if running
    /// very long loops.
    ///
    /// Yielding a job here does _not_ guarantee that it will run: if the queue of pending
    /// jobs is not drained by the next scheduling interval, pending jobs will be cleared
    /// and re-generated.
    async fn schedule(&mut self) -> SchedulingResult<PJ>;

    /// Called when a pending job is ready to be run.
    ///
    /// The job generation provides a future, and a RJ (Running Job) descriptor that tracks it.
    fn spawn(&mut self, pending_job: PJ) -> (RJ, Pin<Box<dyn Future<Output = C> + Send>>);

    /// Called when a job previously spawned with spawn() transmits its completion
    fn on_completion(&mut self, completion: C);

    /// Called when a command is received.  A job will be spawned immediately if the return
    /// value is Some, ignoring concurrency limits and the pending queue.
    fn on_command(&mut self, cmd: CMD) -> Result<PJ, SecondaryTenantError>;
}

/// [`JobGenerator`] returns this to provide pending jobs, and hints about scheduling
pub(super) struct SchedulingResult<PJ> {
    pub(super) jobs: Vec<PJ>,
    /// The job generator would like to be called again this soon
    pub(super) want_interval: Option<Duration>,
}

/// See [`TenantBackgroundJobs`].
pub(super) trait PendingJob {
    fn get_tenant_shard_id(&self) -> &TenantShardId;
}

/// See [`TenantBackgroundJobs`].
pub(super) trait Completion: Send + 'static {
    fn get_tenant_shard_id(&self) -> &TenantShardId;
}

/// See [`TenantBackgroundJobs`].
pub(super) trait RunningJob {
    fn get_barrier(&self) -> Barrier;
}

impl<G, PJ, RJ, C, CMD> TenantBackgroundJobs<G, PJ, RJ, C, CMD>
where
    C: Completion,
    PJ: PendingJob,
    RJ: RunningJob,
    G: JobGenerator<PJ, RJ, C, CMD>,
{
    pub(super) fn new(generator: G, concurrency: usize) -> Self {
        Self {
            generator,
            pending: std::collections::VecDeque::new(),
            running: HashMap::new(),
            tasks: JoinSet::new(),
            concurrency,
            scheduling_interval: MAX_SCHEDULING_INTERVAL,
            _phantom: PhantomData,
        }
    }

    pub(super) async fn run(
        &mut self,
        mut command_queue: tokio::sync::mpsc::Receiver<CommandRequest<CMD>>,
        background_jobs_can_start: Barrier,
        cancel: CancellationToken,
    ) {
        tracing::info!("Waiting for background_jobs_can start...");
        background_jobs_can_start.wait().await;
        tracing::info!("background_jobs_can is ready, proceeding.");

        while !cancel.is_cancelled() {
            // Look for new work: this is relatively expensive because we have to go acquire the lock on
            // the tenant manager to retrieve tenants, and then iterate over them to figure out which ones
            // require an upload.
            self.schedule_iteration(&cancel).await;

            if cancel.is_cancelled() {
                return;
            }

            // Schedule some work, if concurrency limit permits it
            self.spawn_pending();

            // This message is printed every scheduling iteration as proof of liveness when looking at logs
            tracing::info!(
                "Status: {} tasks running, {} pending",
                self.running.len(),
                self.pending.len()
            );

            // Between scheduling iterations, we will:
            //  - Drain any complete tasks and spawn pending tasks
            //  - Handle incoming administrative commands
            //  - Check our cancellation token
            let next_scheduling_iteration = Instant::now()
                .checked_add(self.scheduling_interval)
                .unwrap_or_else(|| {
                    tracing::warn!(
                        "Scheduling interval invalid ({}s)",
                        self.scheduling_interval.as_secs_f64()
                    );
                    // unwrap(): this constant is small, cannot fail to add to time unless
                    // we are close to the end of the universe.
                    Instant::now().checked_add(MIN_SCHEDULING_INTERVAL).unwrap()
                });
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::info!("joining tasks");
                        // We do not simply drop the JoinSet, in order to have an orderly shutdown without cancellation.
                        // It is the callers responsibility to make sure that the tasks they scheduled
                        // respect an appropriate cancellation token, to shut down promptly.  It is only
                        // safe to wait on joining these tasks because we can see the cancellation token
                        // has been set.
                        while let Some(_r) = self.tasks.join_next().await {}
                        tracing::info!("terminating on cancellation token.");

                        break;
                    },
                    _ = tokio::time::sleep(next_scheduling_iteration.duration_since(Instant::now())) => {
                        tracing::debug!("woke for scheduling interval");
                        break;},
                    cmd = command_queue.recv() => {
                        tracing::debug!("woke for command queue");
                        let cmd = match cmd {
                            Some(c) =>c,
                            None => {
                                // SecondaryController was destroyed, and this has raced with
                                // our CancellationToken
                                tracing::info!("terminating on command queue destruction");
                                cancel.cancel();
                                break;
                            }
                        };

                        let CommandRequest{
                            response_tx,
                            payload
                        } = cmd;
                        self.handle_command(payload, response_tx);
                    },
                    _ = async {
                        let completion = self.process_next_completion().await;
                        match completion {
                            Some(c) => {
                                self.generator.on_completion(c);
                                if !cancel.is_cancelled() {
                                    self.spawn_pending();
                                }
                            },
                            None => {
                                // Nothing is running, so just wait: expect that this future
                                // will be dropped when something in the outer select! fires.
                                cancel.cancelled().await;
                            }
                        }

                     } => {}
                }
            }
        }
    }

    fn do_spawn(&mut self, job: PJ) {
        let tenant_shard_id = *job.get_tenant_shard_id();
        let (in_progress, fut) = self.generator.spawn(job);

        self.tasks.spawn(fut);

        let replaced = self.running.insert(tenant_shard_id, in_progress);
        debug_assert!(replaced.is_none());
        if replaced.is_some() {
            tracing::warn!(%tenant_shard_id, "Unexpectedly spawned a task when one was already running")
        }
    }

    /// For all pending tenants that are elegible for execution, spawn their task.
    ///
    /// Caller provides the spawn operation, we track the resulting execution.
    fn spawn_pending(&mut self) {
        while !self.pending.is_empty() && self.running.len() < self.concurrency {
            // unwrap: loop condition includes !is_empty()
            let pending = self.pending.pop_front().unwrap();
            if !self.running.contains_key(pending.get_tenant_shard_id()) {
                self.do_spawn(pending);
            }
        }
    }

    /// For administrative commands: skip the pending queue, ignore concurrency limits
    fn spawn_now(&mut self, job: PJ) -> &RJ {
        let tenant_shard_id = *job.get_tenant_shard_id();
        self.do_spawn(job);
        self.running
            .get(&tenant_shard_id)
            .expect("We just inserted this")
    }

    /// Wait until the next task completes, and handle its completion
    ///
    /// Cancellation: this method is cancel-safe.
    async fn process_next_completion(&mut self) -> Option<C> {
        match self.tasks.join_next().await {
            Some(r) => {
                // We use a channel to drive completions, but also
                // need to drain the JoinSet to avoid completed tasks
                // accumulating.  These calls are 1:1 because every task
                // we spawn into this joinset submits is result to the channel.
                let completion = r.expect("Panic in background task");

                self.running.remove(completion.get_tenant_shard_id());
                Some(completion)
            }
            None => {
                // Nothing is running, so we have nothing to wait for.  We may drop out: the
                // main even loop will call us again after the next time it has run something.
                None
            }
        }
    }

    /// Convert the command into a pending job, spawn it, and when the spawned
    /// job completes, send the result down `response_tx`.
    fn handle_command(
        &mut self,
        cmd: CMD,
        response_tx: tokio::sync::oneshot::Sender<CommandResponse>,
    ) {
        let job = match self.generator.on_command(cmd) {
            Ok(j) => j,
            Err(e) => {
                response_tx.send(CommandResponse { result: Err(e) }).ok();
                return;
            }
        };

        let tenant_shard_id = job.get_tenant_shard_id();
        let barrier = if let Some(barrier) = self.get_running(tenant_shard_id) {
            tracing::info!(
                tenant_id=%tenant_shard_id.tenant_id,
                shard_id=%tenant_shard_id.shard_slug(),
                "Command already running, waiting for it"
            );
            barrier
        } else {
            let running = self.spawn_now(job);
            running.get_barrier().clone()
        };

        // This task does no I/O: it only listens for a barrier's completion and then
        // sends to the command response channel.  It is therefore safe to spawn this without
        // any gates/task_mgr hooks.
        tokio::task::spawn(async move {
            barrier.wait().await;

            response_tx.send(CommandResponse { result: Ok(()) }).ok();
        });
    }

    fn get_running(&self, tenant_shard_id: &TenantShardId) -> Option<Barrier> {
        self.running.get(tenant_shard_id).map(|r| r.get_barrier())
    }

    /// Periodic execution phase: inspect all attached tenants and schedule any work they require.
    ///
    /// The type in `tenants` should be a tenant-like structure, e.g. [`crate::tenant::TenantShard`] or [`crate::tenant::secondary::SecondaryTenant`]
    ///
    /// This function resets the pending list: it is assumed that the caller may change their mind about
    /// which tenants need work between calls to schedule_iteration.
    async fn schedule_iteration(&mut self, cancel: &CancellationToken) {
        let SchedulingResult {
            jobs,
            want_interval,
        } = self.generator.schedule().await;

        // Adjust interval based on feedback from the job generator
        if let Some(want_interval) = want_interval {
            // Calculation uses second granularity: this scheduler is not intended for high frequency tasks
            self.scheduling_interval = Duration::from_secs(std::cmp::min(
                std::cmp::max(MIN_SCHEDULING_INTERVAL.as_secs(), want_interval.as_secs()),
                MAX_SCHEDULING_INTERVAL.as_secs(),
            ));
        }

        // The priority order of previously scheduled work may be invalidated by current state: drop
        // all pending work (it will be re-scheduled if still needed)
        self.pending.clear();

        // While iterating over the potentially-long list of tenants, we will periodically yield
        // to avoid blocking executor.
        yielding_loop(1000, cancel, jobs.into_iter(), |job| {
            // Skip tenants that already have a write in flight
            if !self.running.contains_key(job.get_tenant_shard_id()) {
                self.pending.push_back(job);
            }
        })
        .await
        .ok();
    }
}
