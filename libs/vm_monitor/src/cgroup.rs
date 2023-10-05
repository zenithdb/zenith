use std::{
    fmt::{Debug, Display},
    fs,
    pin::pin,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{anyhow, bail, Context};
use cgroups_rs::{
    freezer::FreezerController,
    hierarchies::{self, is_cgroup2_unified_mode, UNIFIED_MOUNTPOINT},
    memory::MemController,
    MaxValue,
    Subsystem::{Freezer, Mem},
};
use inotify::{EventStream, Inotify, WatchMask};
use tokio::sync::mpsc::{self, error::TryRecvError};
use tokio::time::{Duration, Instant};
use tokio_stream::{Stream, StreamExt};
use tracing::{info, warn};

use crate::protocol::Resources;
use crate::MiB;

/// Monotonically increasing counter of the number of memory.high events
/// the cgroup has experienced.
///
/// We use this to determine if a modification to the `memory.events` file actually
/// changed the `high` field. If not, we don't care about the change. When we
/// read the file, we check the `high` field in the file against `MEMORY_EVENT_COUNT`
/// to see if it changed since last time.
pub static MEMORY_EVENT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Monotonically increasing counter that gives each cgroup event a unique id.
///
/// This allows us to answer questions like "did this upscale arrive before this
/// memory.high?". This static is also used by the `Sequenced` type to "tag" values
/// with a sequence number. As such, prefer to used the `Sequenced` type rather
/// than this static directly.
static EVENT_SEQUENCE_NUMBER: AtomicU64 = AtomicU64::new(0);

/// A memory event type reported in memory.events.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum MemoryEvent {
    Low,
    High,
    Max,
    Oom,
    OomKill,
    OomGroupKill,
}

impl MemoryEvent {
    fn as_str(&self) -> &str {
        match self {
            MemoryEvent::Low => "low",
            MemoryEvent::High => "high",
            MemoryEvent::Max => "max",
            MemoryEvent::Oom => "oom",
            MemoryEvent::OomKill => "oom_kill",
            MemoryEvent::OomGroupKill => "oom_group_kill",
        }
    }
}

impl Display for MemoryEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Configuration for a `CgroupWatcher`
#[derive(Debug, Clone)]
pub struct Config {
    // The target difference between the total memory reserved for the cgroup
    // and the value of the cgroup's memory.high.
    //
    // In other words, memory.high + oom_buffer_bytes will equal the total memory that the cgroup may
    // use (equal to system memory, minus whatever's taken out for the file cache).
    oom_buffer_bytes: u64,

    // The amount of memory, in bytes, below a proposed new value for
    // memory.high that the cgroup's memory usage must be for us to downscale
    //
    // In other words, we can downscale only when:
    //
    //   memory.current + memory_high_buffer_bytes < (proposed) memory.high
    //
    // TODO: there's some minor issues with this approach -- in particular, that we might have
    // memory in use by the kernel's page cache that we're actually ok with getting rid of.
    pub(crate) memory_high_buffer_bytes: u64,

    // The maximum duration, in milliseconds, that we're allowed to pause
    // the cgroup for while waiting for the autoscaler-agent to upscale us
    max_upscale_wait: Duration,

    // The required minimum time, in milliseconds, that we must wait before re-freezing
    // the cgroup while waiting for the autoscaler-agent to upscale us.
    do_not_freeze_more_often_than: Duration,

    // The amount of memory, in bytes, that we should periodically increase memory.high
    // by while waiting for the autoscaler-agent to upscale us.
    //
    // This exists to avoid the excessive throttling that happens when a cgroup is above its
    // memory.high for too long. See more here:
    // https://github.com/neondatabase/autoscaling/issues/44#issuecomment-1522487217
    memory_high_increase_by_bytes: u64,

    // The period, in milliseconds, at which we should repeatedly increase the value
    // of the cgroup's memory.high while we're waiting on upscaling and memory.high
    // is still being hit.
    //
    // Technically speaking, this actually serves as a rate limit to moderate responding to
    // memory.high events, but these are roughly equivalent if the process is still allocating
    // memory.
    memory_high_increase_every: Duration,
}

impl Config {
    /// Calculate the new value for the cgroups memory.high based on system memory
    pub fn calculate_memory_high_value(&self, total_system_mem: u64) -> u64 {
        total_system_mem.saturating_sub(self.oom_buffer_bytes)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            oom_buffer_bytes: 100 * MiB,
            memory_high_buffer_bytes: 100 * MiB,
            // while waiting for upscale, don't freeze for more than 20ms every 1s
            max_upscale_wait: Duration::from_millis(20),
            do_not_freeze_more_often_than: Duration::from_millis(1000),
            // while waiting for upscale, increase memory.high by 10MiB every 25ms
            memory_high_increase_by_bytes: 10 * MiB,
            memory_high_increase_every: Duration::from_millis(25),
        }
    }
}

/// Used to represent data that is associated with a certain point in time, such
/// as an upscale request or memory.high event.
///
/// Internally, creating a `Sequenced` uses a static atomic counter to obtain
/// a unique sequence number. Sequence numbers are monotonically increasing,
/// allowing us to answer questions like "did this upscale happen after this
/// memory.high event?" by comparing the sequence numbers of the two events.
#[derive(Debug, Clone)]
pub struct Sequenced<T> {
    seqnum: u64,
    data: T,
}

impl<T> Sequenced<T> {
    pub fn new(data: T) -> Self {
        Self {
            seqnum: EVENT_SEQUENCE_NUMBER.fetch_add(1, Ordering::AcqRel),
            data,
        }
    }
}

/// Responds to `MonitorEvents` to manage the cgroup: preventing it from being
/// OOM killed or throttling.
///
/// The `CgroupWatcher` primarily achieves this by reading from a stream of
/// `MonitorEvent`s. See `main_signals_loop` for details on how to keep the
/// cgroup happy.
#[derive(Debug)]
pub struct CgroupWatcher {
    pub config: Config,

    /// The sequence number of the last upscale.
    ///
    /// If we receive a memory.high event that has a _lower_ sequence number than
    /// `last_upscale_seqnum`, then we know it occurred before the upscale, and we
    /// can safely ignore it.
    ///
    /// Note: Like the `events` field, this doesn't _need_ interior mutability but we
    /// use it anyways so that methods take `&self`, not `&mut self`.
    last_upscale_seqnum: AtomicU64,

    /// A channel on which we send messages to request upscale from the dispatcher.
    upscale_requester: mpsc::Sender<()>,

    /// The actual cgroup we are watching and managing.
    cgroup: cgroups_rs::Cgroup,
}

/// Read memory.events for the desired event type.
///
/// `path` specifies the path to the desired `memory.events` file.
/// For more info, see the `memory.events` section of the [kernel docs]
/// <https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files>
fn get_event_count(path: &str, event: MemoryEvent) -> anyhow::Result<u64> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read memory.events from {path}"))?;

    // Then contents of the file look like:
    // low 42
    // high 101
    // ...
    contents
        .lines()
        .filter_map(|s| s.split_once(' '))
        .find(|(e, _)| *e == event.as_str())
        .ok_or_else(|| anyhow!("failed to find entry for memory.{event} events in {path}"))
        .and_then(|(_, count)| {
            count
                .parse::<u64>()
                .with_context(|| format!("failed to parse memory.{event} as u64"))
        })
}

/// Create an event stream that produces events whenever the file at the provided
/// path is modified.
fn create_file_watcher(path: &str) -> anyhow::Result<EventStream<[u8; 1024]>> {
    info!("creating file watcher for {path}");
    let inotify = Inotify::init().context("failed to initialize file watcher")?;
    inotify
        .watches()
        .add(path, WatchMask::MODIFY)
        .with_context(|| format!("failed to start watching {path}"))?;
    inotify
        // The inotify docs use [0u8; 1024] so we'll just copy them. We only need
        // to store one event at a time - if the event gets written over, that's
        // ok. We still see that there is an event. For more information, see:
        // https://man7.org/linux/man-pages/man7/inotify.7.html
        .into_event_stream([0u8; 1024])
        .context("failed to start inotify event stream")
}

impl CgroupWatcher {
    /// Create a new `CgroupWatcher`.
    #[tracing::instrument(skip_all, fields(%name))]
    pub fn new(
        name: String,
        // A channel on which to send upscale requests
        upscale_requester: mpsc::Sender<()>,
    ) -> anyhow::Result<(Self, impl Stream<Item = Sequenced<u64>>)> {
        // TODO: clarify exactly why we need v2
        // Make sure cgroups v2 (aka unified) are supported
        if !is_cgroup2_unified_mode() {
            anyhow::bail!("cgroups v2 not supported");
        }
        let cgroup = cgroups_rs::Cgroup::load(hierarchies::auto(), &name);

        // Start monitoring the cgroup for memory events. In general, for
        // cgroups v2 (aka unified), metrics are reported in files like
        // > `/sys/fs/cgroup/{name}/{metric}`
        // We are looking for `memory.high` events, which are stored in the
        // file `memory.events`. For more info, see the `memory.events` section
        // of https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files
        let path = format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name);
        let memory_events = create_file_watcher(&path)
            .with_context(|| format!("failed to create event watcher for {path}"))?
            // This would be nice with with .inspect_err followed by .ok
            .filter_map(move |_| match get_event_count(&path, MemoryEvent::High) {
                Ok(high) => Some(high),
                Err(error) => {
                    // TODO: Might want to just panic here
                    warn!(?error, "failed to read high events count from {}", &path);
                    None
                }
            })
            // Only report the event if the memory.high count increased
            .filter_map(|high| {
                if MEMORY_EVENT_COUNT.fetch_max(high, Ordering::AcqRel) < high {
                    Some(high)
                } else {
                    None
                }
            })
            .map(Sequenced::new);

        let initial_count = get_event_count(
            &format!("{}/{}/memory.events", UNIFIED_MOUNTPOINT, &name),
            MemoryEvent::High,
        )?;

        info!(initial_count, "initial memory.high event count");

        // Hard update `MEMORY_EVENT_COUNT` since there could have been processes
        // running in the cgroup before that caused it to be non-zero.
        MEMORY_EVENT_COUNT.fetch_max(initial_count, Ordering::AcqRel);

        Ok((
            Self {
                cgroup,
                upscale_requester,
                last_upscale_seqnum: AtomicU64::new(0),
                config: Default::default(),
            },
            memory_events,
        ))
    }

    /// The entrypoint for the `CgroupWatcher`.
    #[tracing::instrument(skip_all)]
    pub async fn watch<E>(
        &self,
        // These are ~dependency injected~ (fancy, I know) because this function
        // should never return.
        // -> therefore: when we tokio::spawn it, we don't await the JoinHandle.
        // -> therefore: if we want to stick it in an Arc so many threads can access
        //    it, methods can never take mutable access.
        //     - note: we use the Arc strategy so that a) we can call this function
        //             right here and b) the runner can call the set/get_memory methods
        // -> since calling recv() on a tokio::sync::mpsc::Receiver takes &mut self,
        //    we just pass them in here instead of holding them in fields, as that
        //    would require this method to take &mut self.
        mut upscales: mpsc::Receiver<Sequenced<Resources>>,
        events: E,
    ) -> anyhow::Result<()>
    where
        E: Stream<Item = Sequenced<u64>>,
    {
        let mut wait_to_freeze = pin!(tokio::time::sleep(Duration::ZERO));
        let mut last_memory_high_increase_at: Option<Instant> = None;
        let mut events = pin!(events);

        // Are we waiting to be upscaled? Could be true if we request upscale due
        // to a memory.high event and it does not arrive in time.
        let mut waiting_on_upscale = false;

        loop {
            tokio::select! {
                upscale = upscales.recv() => {
                    let Sequenced { seqnum, data } = upscale
                        .context("failed to listen on upscale notification channel")?;
                    waiting_on_upscale = false;
                    last_memory_high_increase_at = None;
                    self.last_upscale_seqnum.store(seqnum, Ordering::Release);
                    info!(cpu = data.cpu, mem_bytes = data.mem, "received upscale");
                }
                event = events.next() => {
                    let Some(Sequenced { seqnum, .. }) = event else {
                        bail!("failed to listen for memory.high events")
                    };
                    // The memory.high came before our last upscale, so we consider
                    // it resolved
                    if self.last_upscale_seqnum.fetch_max(seqnum, Ordering::AcqRel) > seqnum {
                        info!(
                            "received memory.high event, but it came before our last upscale -> ignoring it"
                        );
                        continue;
                    }

                    // The memory.high came after our latest upscale. We don't
                    // want to do anything yet, so peek the next event in hopes
                    // that it's an upscale.
                    if let Some(upscale_num) = self
                        .upscaled(&mut upscales)
                        .context("failed to check if we were upscaled")?
                    {
                        if upscale_num > seqnum {
                            info!(
                                "received memory.high event, but it came before our last upscale -> ignoring it"
                            );
                            continue;
                        }
                    }

                    // If it's been long enough since we last froze, freeze the
                    // cgroup and request upscale
                    if wait_to_freeze.is_elapsed() {
                        info!("received memory.high event -> requesting upscale");
                        waiting_on_upscale = self
                            .handle_memory_high_event(&mut upscales)
                            .await
                            .context("failed to handle upscale")?;
                        wait_to_freeze
                            .as_mut()
                            .reset(Instant::now() + self.config.do_not_freeze_more_often_than);
                        continue;
                    }

                    // Ok, we can't freeze, just request upscale
                    if !waiting_on_upscale {
                        info!("received memory.high event, but too soon to refreeze -> requesting upscale");

                        // Make check to make sure we haven't been upscaled in the
                        // meantine (can happen if the agent independently decides
                        // to upscale us again)
                        if self
                            .upscaled(&mut upscales)
                            .context("failed to check if we were upscaled")?
                            .is_some()
                        {
                            info!("no need to request upscaling because we got upscaled");
                            continue;
                        }
                        self.upscale_requester
                            .send(())
                            .await
                            .context("failed to request upscale")?;
                        waiting_on_upscale = true;
                        continue;
                    }

                    // Shoot, we can't freeze or and we're still waiting on upscale,
                    // increase memory.high to reduce throttling
                    let can_increase_memory_high = match last_memory_high_increase_at {
                        None => true,
                        Some(t) => t.elapsed() > self.config.memory_high_increase_every,
                    };
                    if can_increase_memory_high {
                        info!(
                            "received memory.high event, \
                            but too soon to refreeze and already requested upscale \
                            -> increasing memory.high"
                        );

                        // Make check to make sure we haven't been upscaled in the
                        // meantine (can happen if the agent independently decides
                        // to upscale us again)
                        if self
                            .upscaled(&mut upscales)
                            .context("failed to check if we were upscaled")?
                            .is_some()
                        {
                            info!("no need to increase memory.high because got upscaled");
                            continue;
                        }

                        // Request upscale anyways (the agent will handle deduplicating
                        // requests)
                        self.upscale_requester
                            .send(())
                            .await
                            .context("failed to request upscale")?;

                        let memory_high =
                            self.get_memory_high_bytes().context("failed to get memory.high")?;
                        let new_high = memory_high + self.config.memory_high_increase_by_bytes;
                        info!(
                            current_high_bytes = memory_high,
                            new_high_bytes = new_high,
                            "updating memory.high"
                        );
                        self.set_memory_high_bytes(new_high)
                            .context("failed to set memory.high")?;
                        last_memory_high_increase_at = Some(Instant::now());
                        continue;
                    }

                    info!("received memory.high event, but can't do anything");
                }
            };
        }
    }

    /// Handle a `memory.high`, returning whether we are still waiting on upscale
    /// by the time the function returns.
    ///
    /// The general plan for handling a `memory.high` event is as follows:
    /// 1. Freeze the cgroup
    /// 2. Start a timer for `self.config.max_upscale_wait`
    /// 3. Request upscale
    /// 4. After the timer elapses or we receive upscale, thaw the cgroup.
    /// 5. Return whether or not we are still waiting for upscale. If we are,
    ///    we'll increase the cgroups memory.high to avoid getting oom killed
    #[tracing::instrument(skip_all)]
    async fn handle_memory_high_event(
        &self,
        upscales: &mut mpsc::Receiver<Sequenced<Resources>>,
    ) -> anyhow::Result<bool> {
        // Immediately freeze the cgroup before doing anything else.
        info!("received memory.high event -> freezing cgroup");
        self.freeze().context("failed to freeze cgroup")?;

        // We'll use this for logging durations
        let start_time = Instant::now();

        // Await the upscale until we have to unfreeze
        let timed =
            tokio::time::timeout(self.config.max_upscale_wait, self.await_upscale(upscales));

        // Request the upscale
        info!(
            wait = ?self.config.max_upscale_wait,
            "sending request for immediate upscaling",
        );
        self.upscale_requester
            .send(())
            .await
            .context("failed to request upscale")?;

        let waiting_on_upscale = match timed.await {
            Ok(Ok(())) => {
                info!(elapsed = ?start_time.elapsed(), "received upscale in time");
                false
            }
            // **important**: unfreeze the cgroup before ?-reporting the error
            Ok(Err(e)) => {
                info!("error waiting for upscale -> thawing cgroup");
                self.thaw()
                    .context("failed to thaw cgroup after errored waiting for upscale")?;
                Err(e.context("failed to await upscale"))?
            }
            Err(_) => {
                info!(elapsed = ?self.config.max_upscale_wait, "timed out waiting for upscale");
                true
            }
        };

        info!("thawing cgroup");
        self.thaw().context("failed to thaw cgroup")?;

        Ok(waiting_on_upscale)
    }

    /// Checks whether we were just upscaled, returning the upscale's sequence
    /// number if so.
    #[tracing::instrument(skip_all)]
    fn upscaled(
        &self,
        upscales: &mut mpsc::Receiver<Sequenced<Resources>>,
    ) -> anyhow::Result<Option<u64>> {
        let Sequenced { seqnum, data } = match upscales.try_recv() {
            Ok(upscale) => upscale,
            Err(TryRecvError::Empty) => return Ok(None),
            Err(TryRecvError::Disconnected) => {
                bail!("upscale notification channel was disconnected")
            }
        };

        // Make sure to update the last upscale sequence number
        self.last_upscale_seqnum.store(seqnum, Ordering::Release);
        info!(cpu = data.cpu, mem_bytes = data.mem, "received upscale");
        Ok(Some(seqnum))
    }

    /// Await an upscale event, discarding any `memory.high` events received in
    /// the process.
    ///
    /// This is used in `handle_memory_high_event`, where we need to listen
    /// for upscales in particular so we know if we can thaw the cgroup early.
    #[tracing::instrument(skip_all)]
    async fn await_upscale(
        &self,
        upscales: &mut mpsc::Receiver<Sequenced<Resources>>,
    ) -> anyhow::Result<()> {
        let Sequenced { seqnum, .. } = upscales
            .recv()
            .await
            .context("error listening for upscales")?;

        self.last_upscale_seqnum.store(seqnum, Ordering::Release);
        Ok(())
    }

    /// Get the cgroup's name.
    pub fn path(&self) -> &str {
        self.cgroup.path()
    }
}

// Methods for manipulating the actual cgroup
impl CgroupWatcher {
    /// Get a handle on the freezer subsystem.
    fn freezer(&self) -> anyhow::Result<&FreezerController> {
        if let Some(Freezer(freezer)) = self
            .cgroup
            .subsystems()
            .iter()
            .find(|sub| matches!(sub, Freezer(_)))
        {
            Ok(freezer)
        } else {
            anyhow::bail!("could not find freezer subsystem")
        }
    }

    /// Attempt to freeze the cgroup.
    pub fn freeze(&self) -> anyhow::Result<()> {
        self.freezer()
            .context("failed to get freezer subsystem")?
            .freeze()
            .context("failed to freeze")
    }

    /// Attempt to thaw the cgroup.
    pub fn thaw(&self) -> anyhow::Result<()> {
        self.freezer()
            .context("failed to get freezer subsystem")?
            .thaw()
            .context("failed to thaw")
    }

    /// Get a handle on the memory subsystem.
    ///
    /// Note: this method does not require `self.memory_update_lock` because
    /// getting a handle to the subsystem does not access any of the files we
    /// care about, such as memory.high and memory.events
    fn memory(&self) -> anyhow::Result<&MemController> {
        if let Some(Mem(memory)) = self
            .cgroup
            .subsystems()
            .iter()
            .find(|sub| matches!(sub, Mem(_)))
        {
            Ok(memory)
        } else {
            anyhow::bail!("could not find memory subsystem")
        }
    }

    /// Get cgroup current memory usage.
    pub fn current_memory_usage(&self) -> anyhow::Result<u64> {
        Ok(self
            .memory()
            .context("failed to get memory subsystem")?
            .memory_stat()
            .usage_in_bytes)
    }

    /// Set cgroup memory.high threshold.
    pub fn set_memory_high_bytes(&self, bytes: u64) -> anyhow::Result<()> {
        self.set_memory_high_internal(MaxValue::Value(u64::min(bytes, i64::MAX as u64) as i64))
    }

    /// Set the cgroup's memory.high to 'max', disabling it.
    pub fn unset_memory_high(&self) -> anyhow::Result<()> {
        self.set_memory_high_internal(MaxValue::Max)
    }

    fn set_memory_high_internal(&self, value: MaxValue) -> anyhow::Result<()> {
        self.memory()
            .context("failed to get memory subsystem")?
            .set_mem(cgroups_rs::memory::SetMemory {
                low: None,
                high: Some(value),
                min: None,
                max: None,
            })
            .map_err(anyhow::Error::from)
    }

    /// Get memory.high threshold.
    pub fn get_memory_high_bytes(&self) -> anyhow::Result<u64> {
        let high = self
            .memory()
            .context("failed to get memory subsystem while getting memory statistics")?
            .get_mem()
            .map(|mem| mem.high)
            .context("failed to get memory statistics from subsystem")?;
        match high {
            Some(MaxValue::Max) => Ok(i64::MAX as u64),
            Some(MaxValue::Value(high)) => Ok(high as u64),
            None => anyhow::bail!("failed to read memory.high from memory subsystem"),
        }
    }
}
