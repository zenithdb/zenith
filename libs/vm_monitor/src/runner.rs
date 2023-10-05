//! Exposes the `Runner`, which handles messages received from agent and
//! sends upscale requests.
//!
//! This is the "Monitor" part of the monitor binary and is the main entrypoint for
//! all functionality.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use axum::extract::ws::{Message, WebSocket};
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::cgroup::{CgroupWatcher, Sequenced};
use crate::dispatcher::Dispatcher;
use crate::filecache::{FileCacheConfig, FileCacheState};
use crate::protocol::{InboundMsg, InboundMsgKind, OutboundMsg, OutboundMsgKind, Resources};
use crate::{bytes_to_mebibytes, get_total_system_memory, spawn_with_cancel, Args, MiB};

/// Central struct that interacts with agent, dispatcher, and cgroup to handle
/// signals from the agent.
#[derive(Debug)]
pub struct Runner {
    config: Config,
    filecache: Option<FileCacheState>,
    cgroup: Option<Arc<CgroupWatcher>>,
    dispatcher: Dispatcher,

    /// We "mint" new message ids by incrementing this counter and taking the value.
    ///
    /// **Note**: This counter is always odd, so that we avoid collisions between the IDs generated
    /// by us vs the autoscaler-agent.
    counter: usize,

    last_upscale_request_at: Option<Instant>,

    /// A signal to kill the main thread produced by `self.run()`. This is triggered
    /// when the server receives a new connection. When the thread receives the
    /// signal off this channel, it will gracefully shutdown.
    kill: broadcast::Receiver<()>,
}

/// Configuration for a `Runner`
#[derive(Debug)]
pub struct Config {
    /// `sys_buffer_bytes` gives the estimated amount of memory, in bytes, that the kernel uses before
    /// handing out the rest to userspace. This value is the estimated difference between the
    /// *actual* physical memory and the amount reported by `grep MemTotal /proc/meminfo`.
    ///
    /// For more information, refer to `man 5 proc`, which defines MemTotal as "Total usable RAM
    /// (i.e., physical RAM minus a few reserved bits and the kernel binary code)".
    ///
    /// We only use `sys_buffer_bytes` when calculating the system memory from the *external* memory
    /// size, rather than the self-reported memory size, according to the kernel.
    ///
    /// TODO: this field is only necessary while we still have to trust the autoscaler-agent's
    /// upscale resource amounts (because we might not *actually* have been upscaled yet). This field
    /// should be removed once we have a better solution there.
    sys_buffer_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sys_buffer_bytes: 100 * MiB,
        }
    }
}

impl Runner {
    /// Create a new monitor.
    #[tracing::instrument(skip_all, fields(?config, ?args))]
    pub async fn new(
        config: Config,
        args: &Args,
        ws: WebSocket,
        kill: broadcast::Receiver<()>,
        token: CancellationToken,
    ) -> anyhow::Result<Runner> {
        anyhow::ensure!(
            config.sys_buffer_bytes != 0,
            "invalid monitor Config: sys_buffer_bytes cannot be 0"
        );

        // *NOTE*: the dispatcher and cgroup manager talk through these channels
        // so make sure they each get the correct half, nothing is dropped, etc.
        let (notified_send, notified_recv) = mpsc::channel(1);
        let (requesting_send, requesting_recv) = mpsc::channel(1);

        let dispatcher = Dispatcher::new(ws, notified_send, requesting_recv)
            .await
            .context("error creating new dispatcher")?;

        let mut state = Runner {
            config,
            filecache: None,
            cgroup: None,
            dispatcher,
            counter: 1, // NB: must be odd, see the comment about the field for more.
            last_upscale_request_at: None,
            kill,
        };

        // If we have both the cgroup and file cache integrations enabled, it's possible for
        // temporary failures to result in cgroup throttling (from memory.high), that in turn makes
        // it near-impossible to connect to the file cache (because it times out). Unfortunately,
        // we *do* still want to determine the file cache size before setting the cgroup's
        // memory.high, so it's not as simple as just swapping the order.
        //
        // Instead, the resolution here is that on vm-monitor startup (note: happens on each
        // connection from autoscaler-agent, possibly multiple times per compute_ctl lifecycle), we
        // temporarily unset memory.high, to allow any existing throttling to dissipate. It's a bit
        // of a hacky solution, but helps with reliability.
        if let Some(name) = &args.cgroup {
            // Best not to set up cgroup stuff more than once, so we'll initialize cgroup state
            // now, and then set limits later.
            info!("initializing cgroup");

            let (cgroup, cgroup_event_stream) = CgroupWatcher::new(name.clone(), requesting_send)
                .context("failed to create cgroup manager")?;

            info!("temporarily unsetting memory.high");

            // Temporarily un-set cgroup memory.high; see above.
            cgroup
                .unset_memory_high()
                .context("failed to unset memory.high")?;

            let cgroup = Arc::new(cgroup);

            let cgroup_clone = Arc::clone(&cgroup);
            spawn_with_cancel(
                token.clone(),
                |_| error!("cgroup watcher terminated"),
                async move { cgroup_clone.watch(notified_recv, cgroup_event_stream).await },
            );

            state.cgroup = Some(cgroup);
        }

        let mut file_cache_reserved_bytes = 0;
        let mem = get_total_system_memory();

        // We need to process file cache initialization before cgroup initialization, so that the memory
        // allocated to the file cache is appropriately taken into account when we decide the cgroup's
        // memory limits.
        if let Some(connstr) = &args.pgconnstr {
            info!("initializing file cache");
            let config = match args.file_cache_on_disk {
                true => FileCacheConfig::default_on_disk(),
                false => FileCacheConfig::default_in_memory(),
            };

            let mut file_cache = FileCacheState::new(connstr, config, token)
                .await
                .context("failed to create file cache")?;

            let size = file_cache
                .get_file_cache_size()
                .await
                .context("error getting file cache size")?;

            let new_size = file_cache.config.calculate_cache_size(mem);
            info!(
                initial = bytes_to_mebibytes(size),
                new = bytes_to_mebibytes(new_size),
                "setting initial file cache size",
            );

            // note: even if size == new_size, we want to explicitly set it, just
            // to make sure that we have the permissions to do so
            let actual_size = file_cache
                .set_file_cache_size(new_size)
                .await
                .context("failed to set file cache size, possibly due to inadequate permissions")?;
            if actual_size != new_size {
                info!("file cache size actually got set to {actual_size}")
            }
            // Mark the resources given to the file cache as reserved, but only if it's in memory.
            if !args.file_cache_on_disk {
                file_cache_reserved_bytes = actual_size;
            }

            state.filecache = Some(file_cache);
        }

        if let Some(cgroup) = &state.cgroup {
            let available = mem - file_cache_reserved_bytes;
            let value = cgroup.config.calculate_memory_high_value(available);

            info!(value, "setting memory.high");

            cgroup
                .set_memory_high_bytes(value)
                .context("failed to set cgroup memory.high")?;
        }

        Ok(state)
    }

    /// Attempt to downscale filecache + cgroup
    #[tracing::instrument(skip_all, fields(?target))]
    pub async fn try_downscale(&mut self, target: Resources) -> anyhow::Result<(bool, String)> {
        // Nothing to adjust
        if self.cgroup.is_none() && self.filecache.is_none() {
            info!("no action needed for downscale (no cgroup or file cache enabled)");
            return Ok((
                true,
                "monitor is not managing cgroup or file cache".to_string(),
            ));
        }

        let requested_mem = target.mem;
        let usable_system_memory = requested_mem.saturating_sub(self.config.sys_buffer_bytes);
        let expected_file_cache_mem_usage = self
            .filecache
            .as_ref()
            .map(|file_cache| file_cache.config.calculate_cache_size(usable_system_memory))
            .unwrap_or(0);
        let mut new_cgroup_mem_high = 0;
        if let Some(cgroup) = &self.cgroup {
            new_cgroup_mem_high = cgroup
                .config
                .calculate_memory_high_value(usable_system_memory - expected_file_cache_mem_usage);

            let current = cgroup
                .current_memory_usage()
                .context("failed to fetch cgroup memory")?;

            if new_cgroup_mem_high < current + cgroup.config.memory_high_buffer_bytes {
                let status = format!(
                    "{}: {} MiB (new high) < {} (current usage) + {} (buffer)",
                    "calculated memory.high too low",
                    bytes_to_mebibytes(new_cgroup_mem_high),
                    bytes_to_mebibytes(current),
                    bytes_to_mebibytes(cgroup.config.memory_high_buffer_bytes)
                );

                info!(status, "discontinuing downscale");

                return Ok((false, status));
            }
        }

        // The downscaling has been approved. Downscale the file cache, then the cgroup.
        let mut status = vec![];
        let mut file_cache_mem_usage = 0;
        if let Some(file_cache) = &mut self.filecache {
            let actual_usage = file_cache
                .set_file_cache_size(expected_file_cache_mem_usage)
                .await
                .context("failed to set file cache size")?;
            if file_cache.config.in_memory {
                file_cache_mem_usage = actual_usage;
            }
            let message = format!(
                "set file cache size to {} MiB (in memory = {})",
                bytes_to_mebibytes(actual_usage),
                file_cache.config.in_memory,
            );
            info!("downscale: {message}");
            status.push(message);
        }

        if let Some(cgroup) = &self.cgroup {
            let available_memory = usable_system_memory - file_cache_mem_usage;

            if file_cache_mem_usage != expected_file_cache_mem_usage {
                new_cgroup_mem_high = cgroup.config.calculate_memory_high_value(available_memory);
            }

            // new_cgroup_mem_high is initialized to 0 but it is guaranteed to not be here
            // since it is properly initialized in the previous cgroup if let block
            cgroup
                .set_memory_high_bytes(new_cgroup_mem_high)
                .context("failed to set cgroup memory.high")?;

            let message = format!(
                "set cgroup memory.high to {} MiB, of new max {} MiB",
                bytes_to_mebibytes(new_cgroup_mem_high),
                bytes_to_mebibytes(available_memory)
            );
            info!("downscale: {message}");
            status.push(message);
        }

        // TODO: make this status thing less jank
        let status = status.join("; ");
        Ok((true, status))
    }

    /// Handle new resources
    #[tracing::instrument(skip_all, fields(?resources))]
    pub async fn handle_upscale(&mut self, resources: Resources) -> anyhow::Result<()> {
        if self.filecache.is_none() && self.cgroup.is_none() {
            info!("no action needed for upscale (no cgroup or file cache enabled)");
            return Ok(());
        }

        let new_mem = resources.mem;
        let usable_system_memory = new_mem.saturating_sub(self.config.sys_buffer_bytes);

        // Get the file cache's expected contribution to the memory usage
        let mut file_cache_mem_usage = 0;
        if let Some(file_cache) = &mut self.filecache {
            let expected_usage = file_cache.config.calculate_cache_size(usable_system_memory);
            info!(
                target = bytes_to_mebibytes(expected_usage),
                total = bytes_to_mebibytes(new_mem),
                "updating file cache size",
            );

            let actual_usage = file_cache
                .set_file_cache_size(expected_usage)
                .await
                .context("failed to set file cache size")?;
            if file_cache.config.in_memory {
                file_cache_mem_usage = actual_usage;
            }

            if actual_usage != expected_usage {
                warn!(
                    "file cache was set to a different size that we wanted: target = {} Mib, actual= {} Mib",
                    bytes_to_mebibytes(expected_usage),
                    bytes_to_mebibytes(actual_usage)
                )
            }
        }

        if let Some(cgroup) = &self.cgroup {
            let available_memory = usable_system_memory - file_cache_mem_usage;
            let new_cgroup_mem_high = cgroup.config.calculate_memory_high_value(available_memory);
            info!(
                target = bytes_to_mebibytes(new_cgroup_mem_high),
                total = bytes_to_mebibytes(new_mem),
                name = cgroup.path(),
                "updating cgroup memory.high",
            );
            cgroup
                .set_memory_high_bytes(new_cgroup_mem_high)
                .context("failed to set cgroup memory.high")?;
        }

        Ok(())
    }

    /// Take in a message and perform some action, such as downscaling or upscaling,
    /// and return a message to be send back.
    #[tracing::instrument(skip_all, fields(%id, message = ?inner))]
    pub async fn process_message(
        &mut self,
        InboundMsg { inner, id }: InboundMsg,
    ) -> anyhow::Result<Option<OutboundMsg>> {
        match inner {
            InboundMsgKind::UpscaleNotification { granted } => {
                self.handle_upscale(granted)
                    .await
                    .context("failed to handle upscale")?;
                self.dispatcher
                    .notify_upscale(Sequenced::new(granted))
                    .await
                    .context("failed to notify notify cgroup of upscale")?;
                Ok(Some(OutboundMsg::new(
                    OutboundMsgKind::UpscaleConfirmation {},
                    id,
                )))
            }
            InboundMsgKind::DownscaleRequest { target } => self
                .try_downscale(target)
                .await
                .context("failed to downscale")
                .map(|(ok, status)| {
                    Some(OutboundMsg::new(
                        OutboundMsgKind::DownscaleResult { ok, status },
                        id,
                    ))
                }),
            InboundMsgKind::InvalidMessage { error } => {
                warn!(
                    %error, id, "received notification of an invalid message we sent"
                );
                Ok(None)
            }
            InboundMsgKind::InternalError { error } => {
                warn!(error, id, "agent experienced an internal error");
                Ok(None)
            }
            InboundMsgKind::HealthCheck {} => {
                Ok(Some(OutboundMsg::new(OutboundMsgKind::HealthCheck {}, id)))
            }
        }
    }

    // TODO: don't propagate errors, probably just warn!?
    #[tracing::instrument(skip_all)]
    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("starting dispatcher");
        loop {
            tokio::select! {
                signal = self.kill.recv() => {
                    match signal {
                        Ok(()) => return Ok(()),
                        Err(e) => bail!("failed to receive kill signal: {e}")
                    }
                }
                // we need to propagate an upscale request
                request = self.dispatcher.request_upscale_events.recv(), if self.cgroup.is_some() => {
                    if request.is_none() {
                        bail!("failed to listen for upscale event from cgroup")
                    }

                    // If it's been less than 1 second since the last time we requested upscaling,
                    // ignore the event, to avoid spamming the agent (otherwise, this can happen
                    // ~1k times per second).
                    if let Some(t) = self.last_upscale_request_at {
                        let elapsed = t.elapsed();
                        if elapsed < Duration::from_secs(1) {
                            info!(elapsed_millis = elapsed.as_millis(), "cgroup asked for upscale but too soon to forward the request, ignoring");
                            continue;
                        }
                    }

                    self.last_upscale_request_at = Some(Instant::now());

                    info!("cgroup asking for upscale; forwarding request");
                    self.counter += 2; // Increment, preserving parity (i.e. keep the
                                       // counter odd). See the field comment for more.
                    self.dispatcher
                        .send(OutboundMsg::new(OutboundMsgKind::UpscaleRequest {}, self.counter))
                        .await
                        .context("failed to send message")?;
                }
                // there is a message from the agent
                msg = self.dispatcher.source.next() => {
                    if let Some(msg) = msg {
                        // Don't use 'message' as a key as the string also uses
                        // that for its key
                        info!(?msg, "received message");
                        match msg {
                            Ok(msg) => {
                                let message: InboundMsg = match msg {
                                    Message::Text(text) => {
                                        serde_json::from_str(&text).context("failed to deserialize text message")?
                                    }
                                    other => {
                                        warn!(
                                            // Don't use 'message' as a key as the
                                            // string also uses that for its key
                                            msg = ?other,
                                            "agent should only send text messages but received different type"
                                        );
                                        continue
                                    },
                                };

                                let out = match self.process_message(message.clone()).await {
                                    Ok(Some(out)) => out,
                                    Ok(None) => continue,
                                    Err(e) => {
                                        let error = e.to_string();
                                        warn!(?error, "error handling message");
                                        OutboundMsg::new(
                                            OutboundMsgKind::InternalError {
                                                error
                                            },
                                            message.id
                                        )
                                    }
                                };

                                self.dispatcher
                                    .send(out)
                                    .await
                                    .context("failed to send message")?;
                            }
                            Err(e) => warn!("{e}"),
                        }
                    } else {
                        anyhow::bail!("dispatcher connection closed")
                    }
                }
            }
        }
    }
}
