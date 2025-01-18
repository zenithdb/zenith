//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use camino::{Utf8DirEntry, Utf8Path, Utf8PathBuf};
use futures::StreamExt;
use itertools::Itertools;
use pageserver_api::key::Key;
use pageserver_api::models::LocationConfigMode;
use pageserver_api::shard::{
    ShardCount, ShardIdentity, ShardIndex, ShardNumber, ShardStripeSize, TenantShardId,
};
use pageserver_api::upcall_api::ReAttachResponseTenant;
use rand::{distributions::Alphanumeric, Rng};
use remote_storage::TimeoutOrCancel;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::SystemExt;
use tokio::fs;

use anyhow::Context;
use once_cell::sync::Lazy;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::*;

use utils::{backoff, completion, crashsafe};

use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::controller_upcall_client::{
    ControlPlaneGenerationsApi, ControllerUpcallClient, RetryForeverError,
};
use crate::deletion_queue::DeletionQueueClient;
use crate::http::routes::ACTIVE_TENANT_TIMEOUT;
use crate::metrics::{TENANT, TENANT_MANAGER as METRICS};
use crate::task_mgr::{TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::config::{
    AttachedLocationConfig, AttachmentMode, LocationConf, LocationMode, SecondaryLocationConfig,
};
use crate::tenant::span::debug_assert_current_span_has_tenant_id;
use crate::tenant::storage_layer::inmemory_layer;
use crate::tenant::timeline::ShutdownMode;
use crate::tenant::{
    AttachedTenantConf, GcError, LoadConfigError, SpawnMode, TenantShard, TenantState,
};
use crate::virtual_file::MaybeFatalIo;
use crate::{InitializationOrder, TEMP_FILE_SUFFIX};

use utils::crashsafe::path_with_suffix_extension;
use utils::fs_ext::PathExt;
use utils::generation::Generation;
use utils::id::{TenantId, TimelineId};

use super::remote_timeline_client::remote_tenant_path;
use super::secondary::SecondaryTenant;
use super::timeline::detach_ancestor::{self, PreparedTimelineDetach};
use super::{GlobalShutDown, TenantSharedResources};

/// For a tenant that appears in TenantsMap, it may either be
/// - `Attached`: has a full Tenant object, is elegible to service
///    reads and ingest WAL.
/// - `Secondary`: is only keeping a local cache warm.
///
/// Secondary is a totally distinct state rather than being a mode of a `Tenant`, because
/// that way we avoid having to carefully switch a tenant's ingestion etc on and off during
/// its lifetime, and we can preserve some important safety invariants like `Tenant` always
/// having a properly acquired generation (Secondary doesn't need a generation)
#[derive(Clone)]
pub(crate) enum TenantSlot {
    Attached(Arc<TenantShard>),
    Secondary(Arc<SecondaryTenant>),
    /// In this state, other administrative operations acting on the TenantId should
    /// block, or return a retry indicator equivalent to HTTP 503.
    InProgress(utils::completion::Barrier),
}

impl std::fmt::Debug for TenantSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Attached(tenant) => write!(f, "Attached({})", tenant.current_state()),
            Self::Secondary(_) => write!(f, "Secondary"),
            Self::InProgress(_) => write!(f, "InProgress"),
        }
    }
}

impl TenantSlot {
    /// Return the `Tenant` in this slot if attached, else None
    fn get_attached(&self) -> Option<&Arc<TenantShard>> {
        match self {
            Self::Attached(t) => Some(t),
            Self::Secondary(_) => None,
            Self::InProgress(_) => None,
        }
    }
}

/// The tenants known to the pageserver.
/// The enum variants are used to distinguish the different states that the pageserver can be in.
pub(crate) enum TenantShardMap {
    /// [`init_tenant_mgr`] is not done yet.
    Initializing,
    /// [`init_tenant_mgr`] is done, all on-disk tenants have been loaded.
    /// New tenants can be added using [`tenant_map_acquire_slot`].
    Open(BTreeMap<TenantShardId, TenantSlot>),
    /// The pageserver has entered shutdown mode via [`TenantShardManager::shutdown`].
    /// Existing tenants are still accessible, but no new tenants can be created.
    ShuttingDown(BTreeMap<TenantShardId, TenantSlot>),
}

/// When resolving a TenantId to a shard, we may be looking for the 0th
/// shard, or we might be looking for whichever shard holds a particular page.
#[derive(Copy, Clone)]
pub(crate) enum ShardSelector {
    /// Only return the 0th shard, if it is present.  If a non-0th shard is present,
    /// ignore it.
    Zero,
    /// Pick the shard that holds this key
    Page(Key),
    /// The shard ID is known: pick the given shard
    Known(ShardIndex),
}

/// A convenience for use with the re_attach ControllerUpcallClient function: rather
/// than the serializable struct, we build this enum that encapsulates
/// the invariant that attached tenants always have generations.
///
/// This represents the subset of a LocationConfig that we receive during re-attach.
pub(crate) enum TenantStartupMode {
    Attached((AttachmentMode, Generation)),
    Secondary,
}

impl TenantStartupMode {
    /// Return the generation & mode that should be used when starting
    /// this tenant.
    ///
    /// If this returns None, the re-attach struct is in an invalid state and
    /// should be ignored in the response.
    fn from_reattach_tenant(rart: ReAttachResponseTenant) -> Option<Self> {
        match (rart.mode, rart.gen) {
            (LocationConfigMode::Detached, _) => None,
            (LocationConfigMode::Secondary, _) => Some(Self::Secondary),
            (LocationConfigMode::AttachedMulti, Some(g)) => {
                Some(Self::Attached((AttachmentMode::Multi, Generation::new(g))))
            }
            (LocationConfigMode::AttachedSingle, Some(g)) => {
                Some(Self::Attached((AttachmentMode::Single, Generation::new(g))))
            }
            (LocationConfigMode::AttachedStale, Some(g)) => {
                Some(Self::Attached((AttachmentMode::Stale, Generation::new(g))))
            }
            _ => {
                tracing::warn!(
                    "Received invalid re-attach state for tenant {}: {rart:?}",
                    rart.id
                );
                None
            }
        }
    }
}

/// Result type for looking up a TenantId to a specific shard
pub(crate) enum ShardResolveResult {
    NotFound,
    Found(Arc<TenantShard>),
    // Wait for this barrrier, then query again
    InProgress(utils::completion::Barrier),
}

impl TenantShardMap {
    /// Convenience function for typical usage, where we want to get a `Tenant` object, for
    /// working with attached tenants.  If the TenantId is in the map but in Secondary state,
    /// None is returned.
    pub(crate) fn get(&self, tenant_shard_id: &TenantShardId) -> Option<&Arc<TenantShard>> {
        match self {
            TenantShardMap::Initializing => None,
            TenantShardMap::Open(m) | TenantShardMap::ShuttingDown(m) => {
                m.get(tenant_shard_id).and_then(|slot| slot.get_attached())
            }
        }
    }

    #[cfg(all(debug_assertions, not(test)))]
    pub(crate) fn len(&self) -> usize {
        match self {
            TenantShardMap::Initializing => 0,
            TenantShardMap::Open(m) | TenantShardMap::ShuttingDown(m) => m.len(),
        }
    }
}

/// Precursor to deletion of a tenant dir: we do a fast rename to a tmp path, and then
/// the slower actual deletion in the background.
///
/// This is "safe" in that that it won't leave behind a partially deleted directory
/// at the original path, because we rename with TEMP_FILE_SUFFIX before starting deleting
/// the contents.
///
/// This is pageserver-specific, as it relies on future processes after a crash to check
/// for TEMP_FILE_SUFFIX when loading things.
async fn safe_rename_tenant_dir(path: impl AsRef<Utf8Path>) -> std::io::Result<Utf8PathBuf> {
    let parent = path
        .as_ref()
        .parent()
        // It is invalid to call this function with a relative path.  Tenant directories
        // should always have a parent.
        .ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Path must be absolute",
        ))?;
    let rand_suffix = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect::<String>()
        + TEMP_FILE_SUFFIX;
    let tmp_path = path_with_suffix_extension(&path, &rand_suffix);
    fs::rename(path.as_ref(), &tmp_path).await?;
    fs::File::open(parent)
        .await?
        .sync_all()
        .await
        .maybe_fatal_err("safe_rename_tenant_dir")?;
    Ok(tmp_path)
}

/// See [`Self::spawn`].
#[derive(Clone, Default)]
pub struct BackgroundPurges(tokio_util::task::TaskTracker);

impl BackgroundPurges {
    /// When we have moved a tenant's content to a temporary directory, we may delete it lazily in
    /// the background, and thereby avoid blocking any API requests on this deletion completing.
    ///
    /// Although we are cleaning up the tenant, this task is not meant to be bound by the lifetime of the tenant in memory.
    /// Thus the [`BackgroundPurges`] type to keep track of these tasks.
    pub fn spawn(&self, tmp_path: Utf8PathBuf) {
        // because on shutdown we close and wait, we are misusing TaskTracker a bit.
        //
        // so first acquire a token, then check if the tracker has been closed. the tracker might get closed
        // right after, but at least the shutdown will wait for what we are spawning next.
        let token = self.0.token();

        if self.0.is_closed() {
            warn!(
                %tmp_path,
                "trying to spawn background purge during shutdown, ignoring"
            );
            return;
        }

        let span = info_span!(parent: None, "background_purge", %tmp_path);

        let task = move || {
            let _token = token;
            let _entered = span.entered();
            if let Err(error) = std::fs::remove_dir_all(tmp_path.as_path()) {
                // should we fatal_io_error here?
                warn!(%error, "failed to purge tenant directory");
            }
        };

        BACKGROUND_RUNTIME.spawn_blocking(task);
    }

    /// When this future completes, all background purges have completed.
    /// The first poll of the future will already lock out new background purges spawned via [`Self::spawn`].
    ///
    /// Concurrent calls will coalesce.
    ///
    /// # Cancellation-Safety
    ///
    /// If this future is dropped before polled to completion, concurrent and subsequent
    /// instances of this future will continue to be correct.
    #[instrument(skip_all)]
    pub async fn shutdown(&self) {
        // forbid new tasks (can be called many times)
        self.0.close();
        self.0.wait().await;
    }
}

static TENANTS: Lazy<std::sync::RwLock<TenantShardMap>> =
    Lazy::new(|| std::sync::RwLock::new(TenantShardMap::Initializing));

/// Responsible for storing and mutating the collection of all tenants
/// that this pageserver has state for.
///
/// Every TenantShard and SecondaryTenant instance lives inside the TenantShardManager.
///
/// The most important role of the TenantShardManager is to prevent conflicts: e.g. trying to attach
/// the same tenant twice concurrently, or trying to configure the same tenant into secondary
/// and attached modes concurrently.
pub struct TenantShardManager {
    conf: &'static PageServerConf,
    // TODO: currently this is a &'static pointing to TENANTs.  When we finish refactoring
    // out of that static variable, the TenantManager can own this.
    // See https://github.com/neondatabase/neon/issues/5796
    tenants: &'static std::sync::RwLock<TenantShardMap>,
    resources: TenantSharedResources,

    // Long-running operations that happen outside of a [`Tenant`] lifetime should respect this token.
    // This is for edge cases like tenant deletion.  In normal cases (within a Tenant lifetime),
    // tenants have their own cancellation tokens, which we fire individually in [`Self::shutdown`], or
    // when the tenant detaches.
    cancel: CancellationToken,

    background_purges: BackgroundPurges,
}

fn emergency_generations(
    tenant_confs: &HashMap<TenantShardId, Result<LocationConf, LoadConfigError>>,
) -> HashMap<TenantShardId, TenantStartupMode> {
    tenant_confs
        .iter()
        .filter_map(|(tid, lc)| {
            let lc = match lc {
                Ok(lc) => lc,
                Err(_) => return None,
            };
            Some((
                *tid,
                match &lc.mode {
                    LocationMode::Attached(alc) => {
                        TenantStartupMode::Attached((alc.attach_mode, alc.generation))
                    }
                    LocationMode::Secondary(_) => TenantStartupMode::Secondary,
                },
            ))
        })
        .collect()
}

async fn init_load_generations(
    conf: &'static PageServerConf,
    tenant_confs: &HashMap<TenantShardId, Result<LocationConf, LoadConfigError>>,
    resources: &TenantSharedResources,
    cancel: &CancellationToken,
) -> anyhow::Result<Option<HashMap<TenantShardId, TenantStartupMode>>> {
    let generations = if conf.control_plane_emergency_mode {
        error!(
            "Emergency mode!  Tenants will be attached unsafely using their last known generation"
        );
        emergency_generations(tenant_confs)
    } else if let Some(client) = ControllerUpcallClient::new(conf, cancel) {
        info!("Calling {} API to re-attach tenants", client.base_url());
        // If we are configured to use the control plane API, then it is the source of truth for what tenants to load.
        match client.re_attach(conf).await {
            Ok(tenants) => tenants
                .into_iter()
                .flat_map(|(id, rart)| {
                    TenantStartupMode::from_reattach_tenant(rart).map(|tsm| (id, tsm))
                })
                .collect(),
            Err(RetryForeverError::ShuttingDown) => {
                anyhow::bail!("Shut down while waiting for control plane re-attach response")
            }
        }
    } else {
        info!("Control plane API not configured, tenant generations are disabled");
        return Ok(None);
    };

    // The deletion queue needs to know about the startup attachment state to decide which (if any) stored
    // deletion list entries may still be valid.  We provide that by pushing a recovery operation into
    // the queue. Sequential processing of te queue ensures that recovery is done before any new tenant deletions
    // are processed, even though we don't block on recovery completing here.
    let attached_tenants = generations
        .iter()
        .flat_map(|(id, start_mode)| {
            match start_mode {
                TenantStartupMode::Attached((_mode, generation)) => Some(generation),
                TenantStartupMode::Secondary => None,
            }
            .map(|gen| (*id, *gen))
        })
        .collect();
    resources.deletion_queue_client.recover(attached_tenants)?;

    Ok(Some(generations))
}

/// Given a directory discovered in the pageserver's tenants/ directory, attempt
/// to load a tenant config from it.
///
/// If we cleaned up something expected (like an empty dir or a temp dir), return None.
fn load_tenant_config(
    conf: &'static PageServerConf,
    tenant_shard_id: TenantShardId,
    dentry: Utf8DirEntry,
) -> Option<Result<LocationConf, LoadConfigError>> {
    let tenant_dir_path = dentry.path().to_path_buf();
    if crate::is_temporary(&tenant_dir_path) {
        info!("Found temporary tenant directory, removing: {tenant_dir_path}");
        // No need to use safe_remove_tenant_dir_all because this is already
        // a temporary path
        std::fs::remove_dir_all(&tenant_dir_path).fatal_err("delete temporary tenant dir");
        return None;
    }

    // This case happens if we crash during attachment before writing a config into the dir
    let is_empty = tenant_dir_path
        .is_empty_dir()
        .fatal_err("Checking for empty tenant dir");
    if is_empty {
        info!("removing empty tenant directory {tenant_dir_path:?}");
        std::fs::remove_dir(&tenant_dir_path).fatal_err("delete empty tenant dir");
        return None;
    }

    Some(TenantShard::load_tenant_config(conf, &tenant_shard_id))
}

/// Initial stage of load: walk the local tenants directory, clean up any temp files,
/// and load configurations for the tenants we found.
///
/// Do this in parallel, because we expect 10k+ tenants, so serial execution can take
/// seconds even on reasonably fast drives.
async fn init_load_tenant_configs(
    conf: &'static PageServerConf,
) -> HashMap<TenantShardId, Result<LocationConf, LoadConfigError>> {
    let tenants_dir = conf.tenants_path();

    let dentries = tokio::task::spawn_blocking(move || -> Vec<Utf8DirEntry> {
        let context = format!("read tenants dir {tenants_dir}");
        let dir_entries = tenants_dir.read_dir_utf8().fatal_err(&context);

        dir_entries
            .collect::<Result<Vec<_>, std::io::Error>>()
            .fatal_err(&context)
    })
    .await
    .expect("Config load task panicked");

    let mut configs = HashMap::new();

    let mut join_set = JoinSet::new();
    for dentry in dentries {
        let tenant_shard_id = match dentry.file_name().parse::<TenantShardId>() {
            Ok(id) => id,
            Err(_) => {
                warn!(
                    "Invalid tenant path (garbage in our repo directory?): '{}'",
                    dentry.file_name()
                );
                continue;
            }
        };

        join_set.spawn_blocking(move || {
            (
                tenant_shard_id,
                load_tenant_config(conf, tenant_shard_id, dentry),
            )
        });
    }

    while let Some(r) = join_set.join_next().await {
        let (tenant_shard_id, tenant_config) = r.expect("Panic in config load task");
        if let Some(tenant_config) = tenant_config {
            configs.insert(tenant_shard_id, tenant_config);
        }
    }

    configs
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeleteTenantError {
    #[error("Tenant map slot error {0}")]
    SlotError(#[from] TenantSlotError),

    #[error("Cancelled")]
    Cancelled,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Initialize repositories with locally available timelines.
/// Timelines that are only partially available locally (remote storage has more data than this pageserver)
/// are scheduled for download and added to the tenant once download is completed.
#[instrument(skip_all)]
pub async fn init_tenant_mgr(
    conf: &'static PageServerConf,
    background_purges: BackgroundPurges,
    resources: TenantSharedResources,
    init_order: InitializationOrder,
    cancel: CancellationToken,
) -> anyhow::Result<TenantShardManager> {
    let mut tenants = BTreeMap::new();

    let ctx = RequestContext::todo_child(TaskKind::Startup, DownloadBehavior::Warn);

    // Initialize dynamic limits that depend on system resources
    let system_memory =
        sysinfo::System::new_with_specifics(sysinfo::RefreshKind::new().with_memory())
            .total_memory();
    let max_ephemeral_layer_bytes =
        conf.ephemeral_bytes_per_memory_kb as u64 * (system_memory / 1024);
    tracing::info!("Initialized ephemeral layer size limit to {max_ephemeral_layer_bytes}, for {system_memory} bytes of memory");
    inmemory_layer::GLOBAL_RESOURCES.max_dirty_bytes.store(
        max_ephemeral_layer_bytes,
        std::sync::atomic::Ordering::Relaxed,
    );

    // Scan local filesystem for attached tenants
    let tenant_configs = init_load_tenant_configs(conf).await;

    // Determine which tenants are to be secondary or attached, and in which generation
    let tenant_modes = init_load_generations(conf, &tenant_configs, &resources, &cancel).await?;

    tracing::info!(
        "Attaching {} tenants at startup, warming up {} at a time",
        tenant_configs.len(),
        conf.concurrent_tenant_warmup.initial_permits()
    );
    TENANT.startup_scheduled.inc_by(tenant_configs.len() as u64);

    // Accumulate futures for writing tenant configs, so that we can execute in parallel
    let mut config_write_futs = Vec::new();

    // Update the location configs according to the re-attach response and persist them to disk
    tracing::info!("Updating {} location configs", tenant_configs.len());
    for (tenant_shard_id, location_conf) in tenant_configs {
        let tenant_dir_path = conf.tenant_path(&tenant_shard_id);

        let mut location_conf = match location_conf {
            Ok(l) => l,
            Err(e) => {
                // This should only happen in the case of a serialization bug or critical local I/O error: we cannot load this tenant
                error!(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), "Failed to load tenant config, failed to {e:#}");
                continue;
            }
        };

        // FIXME: if we were attached, and get demoted to secondary on re-attach, we
        // don't have a place to get a config.
        // (https://github.com/neondatabase/neon/issues/5377)
        const DEFAULT_SECONDARY_CONF: SecondaryLocationConfig =
            SecondaryLocationConfig { warm: true };

        if let Some(tenant_modes) = &tenant_modes {
            // We have a generation map: treat it as the authority for whether
            // this tenant is really attached.
            match tenant_modes.get(&tenant_shard_id) {
                None => {
                    info!(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), "Detaching tenant, control plane omitted it in re-attach response");

                    match safe_rename_tenant_dir(&tenant_dir_path).await {
                        Ok(tmp_path) => {
                            background_purges.spawn(tmp_path);
                        }
                        Err(e) => {
                            error!(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(),
                            "Failed to move detached tenant directory '{tenant_dir_path}': {e:?}");
                        }
                    };

                    // We deleted local content: move on to next tenant, don't try and spawn this one.
                    continue;
                }
                Some(TenantStartupMode::Secondary) => {
                    if !matches!(location_conf.mode, LocationMode::Secondary(_)) {
                        location_conf.mode = LocationMode::Secondary(DEFAULT_SECONDARY_CONF);
                    }
                }
                Some(TenantStartupMode::Attached((attach_mode, generation))) => {
                    let old_gen_higher = match &location_conf.mode {
                        LocationMode::Attached(AttachedLocationConfig {
                            generation: old_generation,
                            attach_mode: _attach_mode,
                        }) => {
                            if old_generation > generation {
                                Some(old_generation)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    if let Some(old_generation) = old_gen_higher {
                        tracing::error!(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(),
                            "Control plane gave decreasing generation ({generation:?}) in re-attach response for tenant that was attached in generation {:?}, demoting to secondary",
                            old_generation
                        );

                        // We cannot safely attach this tenant given a bogus generation number, but let's avoid throwing away
                        // local disk content: demote to secondary rather than detaching.
                        location_conf.mode = LocationMode::Secondary(DEFAULT_SECONDARY_CONF);
                    } else {
                        location_conf.attach_in_generation(*attach_mode, *generation);
                    }
                }
            }
        } else {
            // Legacy mode: no generation information, any tenant present
            // on local disk may activate
            info!(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), "Starting tenant in legacy mode, no generation",);
        };

        // Presence of a generation number implies attachment: attach the tenant
        // if it wasn't already, and apply the generation number.
        config_write_futs.push(async move {
            let r =
                TenantShard::persist_tenant_config(conf, &tenant_shard_id, &location_conf).await;
            (tenant_shard_id, location_conf, r)
        });
    }

    // Execute config writes with concurrency, to avoid bottlenecking on local FS write latency
    tracing::info!(
        "Writing {} location config files...",
        config_write_futs.len()
    );
    let config_write_results = futures::stream::iter(config_write_futs)
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;

    tracing::info!(
        "Spawning {} tenant shard locations...",
        config_write_results.len()
    );
    // For those shards that have live configurations, construct `Tenant` or `SecondaryTenant` objects and start them running
    for (tenant_shard_id, location_conf, config_write_result) in config_write_results {
        // Writing a config to local disk is foundational to startup up tenants: panic if we can't.
        config_write_result.fatal_err("write tenant shard config file");

        let tenant_dir_path = conf.tenant_path(&tenant_shard_id);
        let shard_identity = location_conf.shard;
        let slot = match location_conf.mode {
            LocationMode::Attached(attached_conf) => TenantSlot::Attached(
                tenant_spawn(
                    conf,
                    tenant_shard_id,
                    &tenant_dir_path,
                    resources.clone(),
                    AttachedTenantConf::new(location_conf.tenant_conf, attached_conf),
                    shard_identity,
                    Some(init_order.clone()),
                    SpawnMode::Lazy,
                    &ctx,
                )
                .expect("global shutdown during init_tenant_mgr cannot happen"),
            ),
            LocationMode::Secondary(secondary_conf) => {
                info!(
                    tenant_id = %tenant_shard_id.tenant_id,
                    shard_id = %tenant_shard_id.shard_slug(),
                    "Starting secondary tenant"
                );
                TenantSlot::Secondary(SecondaryTenant::new(
                    tenant_shard_id,
                    shard_identity,
                    location_conf.tenant_conf,
                    &secondary_conf,
                ))
            }
        };

        METRICS.slot_inserted(&slot);
        tenants.insert(tenant_shard_id, slot);
    }

    info!("Processed {} local tenants at startup", tenants.len());

    let mut tenants_map = TENANTS.write().unwrap();
    assert!(matches!(&*tenants_map, &TenantShardMap::Initializing));

    *tenants_map = TenantShardMap::Open(tenants);

    Ok(TenantShardManager {
        conf,
        tenants: &TENANTS,
        resources,
        cancel: CancellationToken::new(),
        background_purges,
    })
}

/// Wrapper for TenantShard::spawn that checks invariants before running
#[allow(clippy::too_many_arguments)]
fn tenant_spawn(
    conf: &'static PageServerConf,
    tenant_shard_id: TenantShardId,
    tenant_path: &Utf8Path,
    resources: TenantSharedResources,
    location_conf: AttachedTenantConf,
    shard_identity: ShardIdentity,
    init_order: Option<InitializationOrder>,
    mode: SpawnMode,
    ctx: &RequestContext,
) -> Result<Arc<TenantShard>, GlobalShutDown> {
    // All these conditions should have been satisfied by our caller: the tenant dir exists, is a well formed
    // path, and contains a configuration file.  Assertions that do synchronous I/O are limited to debug mode
    // to avoid impacting prod runtime performance.
    assert!(!crate::is_temporary(tenant_path));
    debug_assert!(tenant_path.is_dir());
    debug_assert!(conf
        .tenant_location_config_path(&tenant_shard_id)
        .try_exists()
        .unwrap());

    TenantShard::spawn(
        conf,
        tenant_shard_id,
        resources,
        location_conf,
        shard_identity,
        init_order,
        mode,
        ctx,
    )
}

async fn shutdown_all_tenants0(tenants: &std::sync::RwLock<TenantShardMap>) {
    let mut join_set = JoinSet::new();

    #[cfg(all(debug_assertions, not(test)))]
    {
        // Check that our metrics properly tracked the size of the tenants map.  This is a convenient location to check,
        // as it happens implicitly at the end of tests etc.
        let m = tenants.read().unwrap();
        debug_assert_eq!(METRICS.slots_total(), m.len() as u64);
    }

    // Atomically, 1. create the shutdown tasks and 2. prevent creation of new tenants.
    let (total_in_progress, total_attached) = {
        let mut m = tenants.write().unwrap();
        match &mut *m {
            TenantShardMap::Initializing => {
                *m = TenantShardMap::ShuttingDown(BTreeMap::default());
                info!("tenants map is empty");
                return;
            }
            TenantShardMap::Open(tenants) => {
                let mut shutdown_state = BTreeMap::new();
                let mut total_in_progress = 0;
                let mut total_attached = 0;

                for (tenant_shard_id, v) in std::mem::take(tenants).into_iter() {
                    match v {
                        TenantSlot::Attached(t) => {
                            shutdown_state.insert(tenant_shard_id, TenantSlot::Attached(t.clone()));
                            join_set.spawn(
                                async move {
                                    let res = {
                                        let (_guard, shutdown_progress) = completion::channel();
                                        t.shutdown(shutdown_progress, ShutdownMode::FreezeAndFlush).await
                                    };

                                    if let Err(other_progress) = res {
                                        // join the another shutdown in progress
                                        other_progress.wait().await;
                                    }

                                    // we cannot afford per tenant logging here, because if s3 is degraded, we are
                                    // going to log too many lines
                                    debug!("tenant successfully stopped");
                                }
                                .instrument(info_span!("shutdown", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug())),
                            );

                            total_attached += 1;
                        }
                        TenantSlot::Secondary(state) => {
                            // We don't need to wait for this individually per-tenant: the
                            // downloader task will be waited on eventually, this cancel
                            // is just to encourage it to drop out if it is doing work
                            // for this tenant right now.
                            state.cancel.cancel();

                            shutdown_state.insert(tenant_shard_id, TenantSlot::Secondary(state));
                        }
                        TenantSlot::InProgress(notify) => {
                            // InProgress tenants are not visible in TenantsMap::ShuttingDown: we will
                            // wait for their notifications to fire in this function.
                            join_set.spawn(async move {
                                notify.wait().await;
                            });

                            total_in_progress += 1;
                        }
                    }
                }
                *m = TenantShardMap::ShuttingDown(shutdown_state);
                (total_in_progress, total_attached)
            }
            TenantShardMap::ShuttingDown(_) => {
                error!("already shutting down, this function isn't supposed to be called more than once");
                return;
            }
        }
    };

    let started_at = std::time::Instant::now();

    info!(
        "Waiting for {} InProgress tenants and {} Attached tenants to shut down",
        total_in_progress, total_attached
    );

    let total = join_set.len();
    let mut panicked = 0;
    let mut buffering = true;
    const BUFFER_FOR: std::time::Duration = std::time::Duration::from_millis(500);
    let mut buffered = std::pin::pin!(tokio::time::sleep(BUFFER_FOR));

    while !join_set.is_empty() {
        tokio::select! {
            Some(joined) = join_set.join_next() => {
                match joined {
                    Ok(()) => {},
                    Err(join_error) if join_error.is_cancelled() => {
                        unreachable!("we are not cancelling any of the tasks");
                    }
                    Err(join_error) if join_error.is_panic() => {
                        // cannot really do anything, as this panic is likely a bug
                        panicked += 1;
                    }
                    Err(join_error) => {
                        warn!("unknown kind of JoinError: {join_error}");
                    }
                }
                if !buffering {
                    // buffer so that every 500ms since the first update (or starting) we'll log
                    // how far away we are; this is because we will get SIGKILL'd at 10s, and we
                    // are not able to log *then*.
                    buffering = true;
                    buffered.as_mut().reset(tokio::time::Instant::now() + BUFFER_FOR);
                }
            },
            _ = &mut buffered, if buffering => {
                buffering = false;
                info!(remaining = join_set.len(), total, elapsed_ms = started_at.elapsed().as_millis(), "waiting for tenants to shutdown");
            }
        }
    }

    if panicked > 0 {
        warn!(
            panicked,
            total, "observed panicks while shutting down tenants"
        );
    }

    // caller will log how long we took
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum UpsertLocationError {
    #[error("Bad config request: {0}")]
    BadRequest(anyhow::Error),

    #[error("Cannot change config in this state: {0}")]
    Unavailable(#[from] TenantMapError),

    #[error("Tenant is already being modified")]
    InProgress,

    #[error("Failed to flush: {0}")]
    Flush(anyhow::Error),

    /// This error variant is for unexpected situations (soft assertions) where the system is in an unexpected state.
    #[error("Internal error: {0}")]
    InternalError(anyhow::Error),
}

impl TenantShardManager {
    /// Convenience function so that anyone with a TenantManager can get at the global configuration, without
    /// having to pass it around everywhere as a separate object.
    pub(crate) fn get_conf(&self) -> &'static PageServerConf {
        self.conf
    }

    /// Gets the attached tenant from the in-memory data, erroring if it's absent, in secondary mode, or currently
    /// undergoing a state change (i.e. slot is InProgress).
    ///
    /// The return Tenant is not guaranteed to be active: check its status after obtaing it, or
    /// use [`TenantShard::wait_to_become_active`] before using it if you will do I/O on it.
    pub(crate) fn get_attached_tenant_shard(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<Arc<TenantShard>, GetTenantError> {
        let locked = self.tenants.read().unwrap();

        let peek_slot = tenant_map_peek_slot(&locked, &tenant_shard_id, TenantSlotPeekMode::Read)?;

        match peek_slot {
            Some(TenantSlot::Attached(tenant)) => Ok(Arc::clone(tenant)),
            Some(TenantSlot::InProgress(_)) => Err(GetTenantError::NotActive(tenant_shard_id)),
            None | Some(TenantSlot::Secondary(_)) => {
                Err(GetTenantError::ShardNotFound(tenant_shard_id))
            }
        }
    }

    pub(crate) fn get_secondary_tenant_shard(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Option<Arc<SecondaryTenant>> {
        let locked = self.tenants.read().unwrap();

        let peek_slot = tenant_map_peek_slot(&locked, &tenant_shard_id, TenantSlotPeekMode::Read)
            .ok()
            .flatten();

        match peek_slot {
            Some(TenantSlot::Secondary(s)) => Some(s.clone()),
            _ => None,
        }
    }

    /// Whether the `TenantManager` is responsible for the tenant shard
    pub(crate) fn manages_tenant_shard(&self, tenant_shard_id: TenantShardId) -> bool {
        let locked = self.tenants.read().unwrap();

        let peek_slot = tenant_map_peek_slot(&locked, &tenant_shard_id, TenantSlotPeekMode::Read)
            .ok()
            .flatten();

        peek_slot.is_some()
    }

    #[instrument(skip_all, fields(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug()))]
    pub(crate) async fn upsert_location(
        &self,
        tenant_shard_id: TenantShardId,
        new_location_config: LocationConf,
        flush: Option<Duration>,
        mut spawn_mode: SpawnMode,
        ctx: &RequestContext,
    ) -> Result<Option<Arc<TenantShard>>, UpsertLocationError> {
        debug_assert_current_span_has_tenant_id();
        info!("configuring tenant location to state {new_location_config:?}");

        enum FastPathModified {
            Attached(Arc<TenantShard>),
            Secondary(Arc<SecondaryTenant>),
        }

        // Special case fast-path for updates to existing slots: if our upsert is only updating configuration,
        // then we do not need to set the slot to InProgress, we can just call into the
        // existng tenant.
        let fast_path_taken = {
            let locked = self.tenants.read().unwrap();
            let peek_slot =
                tenant_map_peek_slot(&locked, &tenant_shard_id, TenantSlotPeekMode::Write)?;
            match (&new_location_config.mode, peek_slot) {
                (LocationMode::Attached(attach_conf), Some(TenantSlot::Attached(tenant))) => {
                    match attach_conf.generation.cmp(&tenant.generation) {
                        Ordering::Equal => {
                            // A transition from Attached to Attached in the same generation, we may
                            // take our fast path and just provide the updated configuration
                            // to the tenant.
                            tenant.set_new_location_config(
                                AttachedTenantConf::try_from(new_location_config.clone())
                                    .map_err(UpsertLocationError::BadRequest)?,
                            );

                            Some(FastPathModified::Attached(tenant.clone()))
                        }
                        Ordering::Less => {
                            return Err(UpsertLocationError::BadRequest(anyhow::anyhow!(
                                "Generation {:?} is less than existing {:?}",
                                attach_conf.generation,
                                tenant.generation
                            )));
                        }
                        Ordering::Greater => {
                            // Generation advanced, fall through to general case of replacing `Tenant` object
                            None
                        }
                    }
                }
                (
                    LocationMode::Secondary(secondary_conf),
                    Some(TenantSlot::Secondary(secondary_tenant)),
                ) => {
                    secondary_tenant.set_config(secondary_conf);
                    secondary_tenant.set_tenant_conf(&new_location_config.tenant_conf);
                    Some(FastPathModified::Secondary(secondary_tenant.clone()))
                }
                _ => {
                    // Not an Attached->Attached transition, fall through to general case
                    None
                }
            }
        };

        // Fast-path continued: having dropped out of the self.tenants lock, do the async
        // phase of writing config and/or waiting for flush, before returning.
        match fast_path_taken {
            Some(FastPathModified::Attached(tenant)) => {
                TenantShard::persist_tenant_config(
                    self.conf,
                    &tenant_shard_id,
                    &new_location_config,
                )
                .await
                .fatal_err("write tenant shard config");

                // Transition to AttachedStale means we may well hold a valid generation
                // still, and have been requested to go stale as part of a migration.  If
                // the caller set `flush`, then flush to remote storage.
                if let LocationMode::Attached(AttachedLocationConfig {
                    generation: _,
                    attach_mode: AttachmentMode::Stale,
                }) = &new_location_config.mode
                {
                    if let Some(flush_timeout) = flush {
                        match tokio::time::timeout(flush_timeout, tenant.flush_remote()).await {
                            Ok(Err(e)) => {
                                return Err(UpsertLocationError::Flush(e));
                            }
                            Ok(Ok(_)) => return Ok(Some(tenant)),
                            Err(_) => {
                                tracing::warn!(
                                timeout_ms = flush_timeout.as_millis(),
                                "Timed out waiting for flush to remote storage, proceeding anyway."
                            )
                            }
                        }
                    }
                }

                return Ok(Some(tenant));
            }
            Some(FastPathModified::Secondary(_secondary_tenant)) => {
                TenantShard::persist_tenant_config(
                    self.conf,
                    &tenant_shard_id,
                    &new_location_config,
                )
                .await
                .fatal_err("write tenant shard config");

                return Ok(None);
            }
            None => {
                // Proceed with the general case procedure, where we will shutdown & remove any existing
                // slot contents and replace with a fresh one
            }
        };

        // General case for upserts to TenantsMap, excluding the case above: we will substitute an
        // InProgress value to the slot while we make whatever changes are required.  The state for
        // the tenant is inaccessible to the outside world while we are doing this, but that is sensible:
        // the state is ill-defined while we're in transition.  Transitions are async, but fast: we do
        // not do significant I/O, and shutdowns should be prompt via cancellation tokens.
        let mut slot_guard = tenant_map_acquire_slot(&tenant_shard_id, TenantSlotAcquireMode::Any)
            .map_err(|e| match e {
                TenantSlotError::NotFound(_) => {
                    unreachable!("Called with mode Any")
                }
                TenantSlotError::InProgress => UpsertLocationError::InProgress,
                TenantSlotError::MapState(s) => UpsertLocationError::Unavailable(s),
            })?;

        match slot_guard.get_old_value() {
            Some(TenantSlot::Attached(tenant)) => {
                // The case where we keep a Tenant alive was covered above in the special case
                // for Attached->Attached transitions in the same generation.  By this point,
                // if we see an attached tenant we know it will be discarded and should be
                // shut down.
                let (_guard, progress) = utils::completion::channel();

                match tenant.get_attach_mode() {
                    AttachmentMode::Single | AttachmentMode::Multi => {
                        // Before we leave our state as the presumed holder of the latest generation,
                        // flush any outstanding deletions to reduce the risk of leaking objects.
                        self.resources.deletion_queue_client.flush_advisory()
                    }
                    AttachmentMode::Stale => {
                        // If we're stale there's not point trying to flush deletions
                    }
                };

                info!("Shutting down attached tenant");
                match tenant.shutdown(progress, ShutdownMode::Hard).await {
                    Ok(()) => {}
                    Err(barrier) => {
                        info!("Shutdown already in progress, waiting for it to complete");
                        barrier.wait().await;
                    }
                }
                slot_guard.drop_old_value().expect("We just shut it down");

                // Edge case: if we were called with SpawnMode::Create, but a Tenant already existed, then
                // the caller thinks they're creating but the tenant already existed.  We must switch to
                // Eager mode so that when starting this Tenant we properly probe remote storage for timelines,
                // rather than assuming it to be empty.
                spawn_mode = SpawnMode::Eager;
            }
            Some(TenantSlot::Secondary(state)) => {
                info!("Shutting down secondary tenant");
                state.shutdown().await;
            }
            Some(TenantSlot::InProgress(_)) => {
                // This should never happen: acquire_slot should error out
                // if the contents of a slot were InProgress.
                return Err(UpsertLocationError::InternalError(anyhow::anyhow!(
                    "Acquired an InProgress slot, this is a bug."
                )));
            }
            None => {
                // Slot was vacant, nothing needs shutting down.
            }
        }

        let tenant_path = self.conf.tenant_path(&tenant_shard_id);
        let timelines_path = self.conf.timelines_path(&tenant_shard_id);

        // Directory structure is the same for attached and secondary modes:
        // create it if it doesn't exist.  Timeline load/creation expects the
        // timelines/ subdir to already exist.
        //
        // Does not need to be fsync'd because local storage is just a cache.
        tokio::fs::create_dir_all(&timelines_path)
            .await
            .fatal_err("create timelines/ dir");

        // Before activating either secondary or attached mode, persist the
        // configuration, so that on restart we will re-attach (or re-start
        // secondary) on the tenant.
        TenantShard::persist_tenant_config(self.conf, &tenant_shard_id, &new_location_config)
            .await
            .fatal_err("write tenant shard config");

        let new_slot = match &new_location_config.mode {
            LocationMode::Secondary(secondary_config) => {
                let shard_identity = new_location_config.shard;
                TenantSlot::Secondary(SecondaryTenant::new(
                    tenant_shard_id,
                    shard_identity,
                    new_location_config.tenant_conf,
                    secondary_config,
                ))
            }
            LocationMode::Attached(_attach_config) => {
                let shard_identity = new_location_config.shard;

                // Testing hack: if we are configured with no control plane, then drop the generation
                // from upserts.  This enables creating generation-less tenants even though neon_local
                // always uses generations when calling the location conf API.
                let attached_conf = if cfg!(feature = "testing") {
                    let mut conf = AttachedTenantConf::try_from(new_location_config)
                        .map_err(UpsertLocationError::BadRequest)?;
                    if self.conf.control_plane_api.is_none() {
                        conf.location.generation = Generation::none();
                    }
                    conf
                } else {
                    AttachedTenantConf::try_from(new_location_config)
                        .map_err(UpsertLocationError::BadRequest)?
                };

                let tenant = tenant_spawn(
                    self.conf,
                    tenant_shard_id,
                    &tenant_path,
                    self.resources.clone(),
                    attached_conf,
                    shard_identity,
                    None,
                    spawn_mode,
                    ctx,
                )
                .map_err(|_: GlobalShutDown| {
                    UpsertLocationError::Unavailable(TenantMapError::ShuttingDown)
                })?;

                TenantSlot::Attached(tenant)
            }
        };

        let attached_tenant = if let TenantSlot::Attached(tenant) = &new_slot {
            Some(tenant.clone())
        } else {
            None
        };

        match slot_guard.upsert(new_slot) {
            Err(TenantSlotUpsertError::InternalError(e)) => {
                Err(UpsertLocationError::InternalError(anyhow::anyhow!(e)))
            }
            Err(TenantSlotUpsertError::MapState(e)) => Err(UpsertLocationError::Unavailable(e)),
            Err(TenantSlotUpsertError::ShuttingDown((new_slot, _completion))) => {
                // If we just called tenant_spawn() on a new tenant, and can't insert it into our map, then
                // we must not leak it: this would violate the invariant that after shutdown_all_tenants, all tenants
                // are shutdown.
                //
                // We must shut it down inline here.
                match new_slot {
                    TenantSlot::InProgress(_) => {
                        // Unreachable because we never insert an InProgress
                        unreachable!()
                    }
                    TenantSlot::Attached(tenant) => {
                        let (_guard, progress) = utils::completion::channel();
                        info!("Shutting down just-spawned tenant, because tenant manager is shut down");
                        match tenant.shutdown(progress, ShutdownMode::Hard).await {
                            Ok(()) => {
                                info!("Finished shutting down just-spawned tenant");
                            }
                            Err(barrier) => {
                                info!("Shutdown already in progress, waiting for it to complete");
                                barrier.wait().await;
                            }
                        }
                    }
                    TenantSlot::Secondary(secondary_tenant) => {
                        secondary_tenant.shutdown().await;
                    }
                }

                Err(UpsertLocationError::Unavailable(
                    TenantMapError::ShuttingDown,
                ))
            }
            Ok(()) => Ok(attached_tenant),
        }
    }

    /// Resetting a tenant is equivalent to detaching it, then attaching it again with the same
    /// LocationConf that was last used to attach it.  Optionally, the local file cache may be
    /// dropped before re-attaching.
    ///
    /// This is not part of a tenant's normal lifecycle: it is used for debug/support, in situations
    /// where an issue is identified that would go away with a restart of the tenant.
    ///
    /// This does not have any special "force" shutdown of a tenant: it relies on the tenant's tasks
    /// to respect the cancellation tokens used in normal shutdown().
    #[instrument(skip_all, fields(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), %drop_cache))]
    pub(crate) async fn reset_tenant(
        &self,
        tenant_shard_id: TenantShardId,
        drop_cache: bool,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let mut slot_guard = tenant_map_acquire_slot(&tenant_shard_id, TenantSlotAcquireMode::Any)?;
        let Some(old_slot) = slot_guard.get_old_value() else {
            anyhow::bail!("Tenant not found when trying to reset");
        };

        let Some(tenant) = old_slot.get_attached() else {
            slot_guard.revert();
            anyhow::bail!("Tenant is not in attached state");
        };

        let (_guard, progress) = utils::completion::channel();
        match tenant.shutdown(progress, ShutdownMode::Hard).await {
            Ok(()) => {
                slot_guard.drop_old_value()?;
            }
            Err(_barrier) => {
                slot_guard.revert();
                anyhow::bail!("Cannot reset Tenant, already shutting down");
            }
        }

        let tenant_path = self.conf.tenant_path(&tenant_shard_id);
        let timelines_path = self.conf.timelines_path(&tenant_shard_id);
        let config = TenantShard::load_tenant_config(self.conf, &tenant_shard_id)?;

        if drop_cache {
            tracing::info!("Dropping local file cache");

            match tokio::fs::read_dir(&timelines_path).await {
                Err(e) => {
                    tracing::warn!("Failed to list timelines while dropping cache: {}", e);
                }
                Ok(mut entries) => {
                    while let Some(entry) = entries.next_entry().await? {
                        tokio::fs::remove_dir_all(entry.path()).await?;
                    }
                }
            }
        }

        let shard_identity = config.shard;
        let tenant = tenant_spawn(
            self.conf,
            tenant_shard_id,
            &tenant_path,
            self.resources.clone(),
            AttachedTenantConf::try_from(config)?,
            shard_identity,
            None,
            SpawnMode::Eager,
            ctx,
        )?;

        slot_guard.upsert(TenantSlot::Attached(tenant))?;

        Ok(())
    }

    pub(crate) fn get_attached_active_tenant_shards(&self) -> Vec<Arc<TenantShard>> {
        let locked = self.tenants.read().unwrap();
        match &*locked {
            TenantShardMap::Initializing => Vec::new(),
            TenantShardMap::Open(map) | TenantShardMap::ShuttingDown(map) => map
                .values()
                .filter_map(|slot| {
                    slot.get_attached()
                        .and_then(|t| if t.is_active() { Some(t.clone()) } else { None })
                })
                .collect(),
        }
    }
    // Do some synchronous work for all tenant slots in Secondary state.  The provided
    // callback should be small and fast, as it will be called inside the global
    // TenantsMap lock.
    pub(crate) fn foreach_secondary_tenants<F>(&self, mut func: F)
    where
        // TODO: let the callback return a hint to drop out of the loop early
        F: FnMut(&TenantShardId, &Arc<SecondaryTenant>),
    {
        let locked = self.tenants.read().unwrap();

        let map = match &*locked {
            TenantShardMap::Initializing | TenantShardMap::ShuttingDown(_) => return,
            TenantShardMap::Open(m) => m,
        };

        for (tenant_id, slot) in map {
            if let TenantSlot::Secondary(state) = slot {
                // Only expose secondary tenants that are not currently shutting down
                if !state.cancel.is_cancelled() {
                    func(tenant_id, state)
                }
            }
        }
    }

    /// Total list of all tenant slots: this includes attached, secondary, and InProgress.
    pub(crate) fn list(&self) -> Vec<(TenantShardId, TenantSlot)> {
        let locked = self.tenants.read().unwrap();
        match &*locked {
            TenantShardMap::Initializing => Vec::new(),
            TenantShardMap::Open(map) | TenantShardMap::ShuttingDown(map) => {
                map.iter().map(|(k, v)| (*k, v.clone())).collect()
            }
        }
    }

    pub(crate) fn get(&self, tenant_shard_id: TenantShardId) -> Option<TenantSlot> {
        let locked = self.tenants.read().unwrap();
        match &*locked {
            TenantShardMap::Initializing => None,
            TenantShardMap::Open(map) | TenantShardMap::ShuttingDown(map) => {
                map.get(&tenant_shard_id).cloned()
            }
        }
    }

    /// If a tenant is attached, detach it.  Then remove its data from remote storage.
    ///
    /// A tenant is considered deleted once it is gone from remote storage.  It is the caller's
    /// responsibility to avoid trying to attach the tenant again or use it any way once deletion
    /// has started: this operation is not atomic, and must be retried until it succeeds.
    ///
    /// As a special case, if an unsharded tenant ID is given for a sharded tenant, it will remove
    /// all tenant shards in remote storage (removing all paths with the tenant prefix). The storage
    /// controller uses this to purge all remote tenant data, including any stale parent shards that
    /// may remain after splits. Ideally, this special case would be handled elsewhere. See:
    /// <https://github.com/neondatabase/neon/pull/9394>.
    pub(crate) async fn delete_tenant(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<(), DeleteTenantError> {
        super::span::debug_assert_current_span_has_tenant_id();

        async fn delete_local(
            conf: &PageServerConf,
            background_purges: &BackgroundPurges,
            tenant_shard_id: &TenantShardId,
        ) -> anyhow::Result<()> {
            let local_tenant_directory = conf.tenant_path(tenant_shard_id);
            let tmp_dir = safe_rename_tenant_dir(&local_tenant_directory)
                .await
                .with_context(|| {
                    format!("local tenant directory {local_tenant_directory:?} rename")
                })?;
            background_purges.spawn(tmp_dir);
            Ok(())
        }

        let slot_guard = tenant_map_acquire_slot(&tenant_shard_id, TenantSlotAcquireMode::Any)?;
        match &slot_guard.old_value {
            Some(TenantSlot::Attached(tenant)) => {
                // Legacy deletion flow: the tenant remains attached, goes to Stopping state, and
                // deletion will be resumed across restarts.
                let tenant = tenant.clone();
                let (_guard, progress) = utils::completion::channel();
                match tenant.shutdown(progress, ShutdownMode::Hard).await {
                    Ok(()) => {}
                    Err(barrier) => {
                        info!("Shutdown already in progress, waiting for it to complete");
                        barrier.wait().await;
                    }
                }
                delete_local(self.conf, &self.background_purges, &tenant_shard_id).await?;
            }
            Some(TenantSlot::Secondary(secondary_tenant)) => {
                secondary_tenant.shutdown().await;

                delete_local(self.conf, &self.background_purges, &tenant_shard_id).await?;
            }
            Some(TenantSlot::InProgress(_)) => unreachable!(),
            None => {}
        };

        // Fall through: local state for this tenant is no longer present, proceed with remote delete.
        // - We use a retry wrapper here so that common transient S3 errors (e.g. 503, 429) do not result
        //   in 500 responses to delete requests.
        // - We keep the `SlotGuard` during this I/O, so that if a concurrent delete request comes in, it will
        //   503/retry, rather than kicking off a wasteful concurrent deletion.
        // NB: this also deletes partial prefixes, i.e. a <tenant_id> path will delete all
        // <tenant_id>_<shard_id>/* objects. See method comment for why.
        backoff::retry(
            || async move {
                self.resources
                    .remote_storage
                    .delete_prefix(&remote_tenant_path(&tenant_shard_id), &self.cancel)
                    .await
            },
            |_| false, // backoff::retry handles cancellation
            1,
            3,
            &format!("delete_tenant[tenant_shard_id={tenant_shard_id}]"),
            &self.cancel,
        )
        .await
        .unwrap_or(Err(TimeoutOrCancel::Cancel.into()))
        .map_err(|err| {
            if TimeoutOrCancel::caused_by_cancel(&err) {
                return DeleteTenantError::Cancelled;
            }
            DeleteTenantError::Other(err)
        })
    }

    #[instrument(skip_all, fields(tenant_id=%tenant.get_tenant_shard_id().tenant_id, shard_id=%tenant.get_tenant_shard_id().shard_slug(), new_shard_count=%new_shard_count.literal()))]
    pub(crate) async fn shard_split(
        &self,
        tenant: Arc<TenantShard>,
        new_shard_count: ShardCount,
        new_stripe_size: Option<ShardStripeSize>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<TenantShardId>> {
        let tenant_shard_id = *tenant.get_tenant_shard_id();
        let r = self
            .do_shard_split(tenant, new_shard_count, new_stripe_size, ctx)
            .await;
        if r.is_err() {
            // Shard splitting might have left the original shard in a partially shut down state (it
            // stops the shard's remote timeline client).  Reset it to ensure we leave things in
            // a working state.
            if self.get(tenant_shard_id).is_some() {
                tracing::warn!("Resetting after shard split failure");
                if let Err(e) = self.reset_tenant(tenant_shard_id, false, ctx).await {
                    // Log this error because our return value will still be the original error, not this one.  This is
                    // a severe error: if this happens, we might be leaving behind a tenant that is not fully functional
                    // (e.g. has uploads disabled).  We can't do anything else: if reset fails then shutting the tenant down or
                    // setting it broken probably won't help either.
                    tracing::error!("Failed to reset: {e}");
                }
            }
        }

        r
    }

    pub(crate) async fn do_shard_split(
        &self,
        tenant: Arc<TenantShard>,
        new_shard_count: ShardCount,
        new_stripe_size: Option<ShardStripeSize>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<TenantShardId>> {
        let tenant_shard_id = *tenant.get_tenant_shard_id();

        // Validate the incoming request
        if new_shard_count.count() <= tenant_shard_id.shard_count.count() {
            anyhow::bail!("Requested shard count is not an increase");
        }
        let expansion_factor = new_shard_count.count() / tenant_shard_id.shard_count.count();
        if !expansion_factor.is_power_of_two() {
            anyhow::bail!("Requested split is not a power of two");
        }

        if let Some(new_stripe_size) = new_stripe_size {
            if tenant.get_shard_stripe_size() != new_stripe_size
                && tenant_shard_id.shard_count.count() > 1
            {
                // This tenant already has multiple shards, it is illegal to try and change its stripe size
                anyhow::bail!(
                    "Shard stripe size may not be modified once tenant has multiple shards"
                );
            }
        }

        // Plan: identify what the new child shards will be
        let child_shards = tenant_shard_id.split(new_shard_count);
        tracing::info!(
            "Shard {} splits into: {}",
            tenant_shard_id.to_index(),
            child_shards
                .iter()
                .map(|id| format!("{}", id.to_index()))
                .join(",")
        );

        fail::fail_point!("shard-split-pre-prepare", |_| Err(anyhow::anyhow!(
            "failpoint"
        )));

        let parent_shard_identity = tenant.shard_identity;
        let parent_tenant_conf = tenant.get_tenant_conf();
        let parent_generation = tenant.generation;

        // Phase 1: Write out child shards' remote index files, in the parent tenant's current generation
        if let Err(e) = tenant.split_prepare(&child_shards).await {
            // If [`TenantShard::split_prepare`] fails, we must reload the tenant, because it might
            // have been left in a partially-shut-down state.
            tracing::warn!("Failed to prepare for split: {e}, reloading Tenant before returning");
            return Err(e);
        }

        fail::fail_point!("shard-split-post-prepare", |_| Err(anyhow::anyhow!(
            "failpoint"
        )));

        self.resources.deletion_queue_client.flush_advisory();

        // Phase 2: Put the parent shard to InProgress and grab a reference to the parent Tenant
        drop(tenant);
        let mut parent_slot_guard =
            tenant_map_acquire_slot(&tenant_shard_id, TenantSlotAcquireMode::Any)?;
        let parent = match parent_slot_guard.get_old_value() {
            Some(TenantSlot::Attached(t)) => t,
            Some(TenantSlot::Secondary(_)) => anyhow::bail!("Tenant location in secondary mode"),
            Some(TenantSlot::InProgress(_)) => {
                // tenant_map_acquire_slot never returns InProgress, if a slot was InProgress
                // it would return an error.
                unreachable!()
            }
            None => {
                // We don't actually need the parent shard to still be attached to do our work, but it's
                // a weird enough situation that the caller probably didn't want us to continue working
                // if they had detached the tenant they requested the split on.
                anyhow::bail!("Detached parent shard in the middle of split!")
            }
        };
        fail::fail_point!("shard-split-pre-hardlink", |_| Err(anyhow::anyhow!(
            "failpoint"
        )));
        // Optimization: hardlink layers from the parent into the children, so that they don't have to
        // re-download & duplicate the data referenced in their initial IndexPart
        self.shard_split_hardlink(parent, child_shards.clone())
            .await?;
        fail::fail_point!("shard-split-post-hardlink", |_| Err(anyhow::anyhow!(
            "failpoint"
        )));

        // Take a snapshot of where the parent's WAL ingest had got to: we will wait for
        // child shards to reach this point.
        let mut target_lsns = HashMap::new();
        for timeline in parent.timelines.lock().unwrap().clone().values() {
            target_lsns.insert(timeline.timeline_id, timeline.get_last_record_lsn());
        }

        // TODO: we should have the parent shard stop its WAL ingest here, it's a waste of resources
        // and could slow down the children trying to catch up.

        // Phase 3: Spawn the child shards
        for child_shard in &child_shards {
            let mut child_shard_identity = parent_shard_identity;
            if let Some(new_stripe_size) = new_stripe_size {
                child_shard_identity.stripe_size = new_stripe_size;
            }
            child_shard_identity.count = child_shard.shard_count;
            child_shard_identity.number = child_shard.shard_number;

            let child_location_conf = LocationConf {
                mode: LocationMode::Attached(AttachedLocationConfig {
                    generation: parent_generation,
                    attach_mode: AttachmentMode::Single,
                }),
                shard: child_shard_identity,
                tenant_conf: parent_tenant_conf.clone(),
            };

            self.upsert_location(
                *child_shard,
                child_location_conf,
                None,
                SpawnMode::Eager,
                ctx,
            )
            .await?;
        }

        fail::fail_point!("shard-split-post-child-conf", |_| Err(anyhow::anyhow!(
            "failpoint"
        )));

        // Phase 4: wait for child chards WAL ingest to catch up to target LSN
        for child_shard_id in &child_shards {
            let child_shard_id = *child_shard_id;
            let child_shard = {
                let locked = self.tenants.read().unwrap();
                let peek_slot =
                    tenant_map_peek_slot(&locked, &child_shard_id, TenantSlotPeekMode::Read)?;
                peek_slot.and_then(|s| s.get_attached()).cloned()
            };
            if let Some(t) = child_shard {
                // Wait for the child shard to become active: this should be very quick because it only
                // has to download the index_part that we just uploaded when creating it.
                if let Err(e) = t.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await {
                    // This is not fatal: we have durably created the child shard.  It just makes the
                    // split operation less seamless for clients, as we will may detach the parent
                    // shard before the child shards are fully ready to serve requests.
                    tracing::warn!("Failed to wait for shard {child_shard_id} to activate: {e}");
                    continue;
                }

                let timelines = t.timelines.lock().unwrap().clone();
                for timeline in timelines.values() {
                    let Some(target_lsn) = target_lsns.get(&timeline.timeline_id) else {
                        continue;
                    };

                    tracing::info!(
                        "Waiting for child shard {}/{} to reach target lsn {}...",
                        child_shard_id,
                        timeline.timeline_id,
                        target_lsn
                    );

                    fail::fail_point!("shard-split-lsn-wait", |_| Err(anyhow::anyhow!(
                        "failpoint"
                    )));
                    if let Err(e) = timeline
                        .wait_lsn(
                            *target_lsn,
                            crate::tenant::timeline::WaitLsnWaiter::Tenant,
                            ctx,
                        )
                        .await
                    {
                        // Failure here might mean shutdown, in any case this part is an optimization
                        // and we shouldn't hold up the split operation.
                        tracing::warn!(
                            "Failed to wait for timeline {} to reach lsn {target_lsn}: {e}",
                            timeline.timeline_id
                        );
                    } else {
                        tracing::info!(
                            "Child shard {}/{} reached target lsn {}",
                            child_shard_id,
                            timeline.timeline_id,
                            target_lsn
                        );
                    }
                }
            }
        }

        // Phase 5: Shut down the parent shard, and erase it from disk
        let (_guard, progress) = completion::channel();
        match parent.shutdown(progress, ShutdownMode::Hard).await {
            Ok(()) => {}
            Err(other) => {
                other.wait().await;
            }
        }
        let local_tenant_directory = self.conf.tenant_path(&tenant_shard_id);
        let tmp_path = safe_rename_tenant_dir(&local_tenant_directory)
            .await
            .with_context(|| format!("local tenant directory {local_tenant_directory:?} rename"))?;
        self.background_purges.spawn(tmp_path);

        fail::fail_point!("shard-split-pre-finish", |_| Err(anyhow::anyhow!(
            "failpoint"
        )));

        parent_slot_guard.drop_old_value()?;

        // Phase 6: Release the InProgress on the parent shard
        drop(parent_slot_guard);

        Ok(child_shards)
    }

    /// Part of [`Self::shard_split`]: hard link parent shard layers into child shards, as an optimization
    /// to avoid the children downloading them again.
    ///
    /// For each resident layer in the parent shard, we will hard link it into all of the child shards.
    async fn shard_split_hardlink(
        &self,
        parent_shard: &TenantShard,
        child_shards: Vec<TenantShardId>,
    ) -> anyhow::Result<()> {
        debug_assert_current_span_has_tenant_id();

        let parent_path = self.conf.tenant_path(parent_shard.get_tenant_shard_id());
        let (parent_timelines, parent_layers) = {
            let mut parent_layers = Vec::new();
            let timelines = parent_shard.timelines.lock().unwrap().clone();
            let parent_timelines = timelines.keys().cloned().collect::<Vec<_>>();
            for timeline in timelines.values() {
                tracing::info!(timeline_id=%timeline.timeline_id, "Loading list of layers to hardlink");
                let layers = timeline.layers.read().await;

                for layer in layers.likely_resident_layers() {
                    let relative_path = layer
                        .local_path()
                        .strip_prefix(&parent_path)
                        .context("Removing prefix from parent layer path")?;
                    parent_layers.push(relative_path.to_owned());
                }
            }

            if parent_layers.is_empty() {
                tracing::info!("Ancestor shard has no resident layer to hard link");
            }

            (parent_timelines, parent_layers)
        };

        let mut child_prefixes = Vec::new();
        let mut create_dirs = Vec::new();

        for child in child_shards {
            let child_prefix = self.conf.tenant_path(&child);
            create_dirs.push(child_prefix.clone());
            create_dirs.extend(
                parent_timelines
                    .iter()
                    .map(|t| self.conf.timeline_path(&child, t)),
            );

            child_prefixes.push(child_prefix);
        }

        // Since we will do a large number of small filesystem metadata operations, batch them into
        // spawn_blocking calls rather than doing each one as a tokio::fs round-trip.
        let span = tracing::Span::current();
        let jh = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
            // Run this synchronous code in the same log context as the outer function that spawned it.
            let _span = span.enter();

            tracing::info!("Creating {} directories", create_dirs.len());
            for dir in &create_dirs {
                if let Err(e) = std::fs::create_dir_all(dir) {
                    // Ignore AlreadyExists errors, drop out on all other errors
                    match e.kind() {
                        std::io::ErrorKind::AlreadyExists => {}
                        _ => {
                            return Err(anyhow::anyhow!(e).context(format!("Creating {dir}")));
                        }
                    }
                }
            }

            for child_prefix in child_prefixes {
                tracing::info!(
                    "Hard-linking {} parent layers into child path {}",
                    parent_layers.len(),
                    child_prefix
                );
                for relative_layer in &parent_layers {
                    let parent_path = parent_path.join(relative_layer);
                    let child_path = child_prefix.join(relative_layer);
                    if let Err(e) = std::fs::hard_link(&parent_path, &child_path) {
                        match e.kind() {
                            std::io::ErrorKind::AlreadyExists => {}
                            std::io::ErrorKind::NotFound => {
                                tracing::info!(
                                    "Layer {} not found during hard-linking, evicted during split?",
                                    relative_layer
                                );
                            }
                            _ => {
                                return Err(anyhow::anyhow!(e).context(format!(
                                    "Hard linking {relative_layer} into {child_prefix}"
                                )))
                            }
                        }
                    }
                }
            }

            // Durability is not required for correctness, but if we crashed during split and
            // then came restarted with empty timeline dirs, it would be very inefficient to
            // re-populate from remote storage.
            tracing::info!("fsyncing {} directories", create_dirs.len());
            for dir in create_dirs {
                if let Err(e) = crashsafe::fsync(&dir) {
                    // Something removed a newly created timeline dir out from underneath us?  Extremely
                    // unexpected, but not worth panic'ing over as this whole function is just an
                    // optimization.
                    tracing::warn!("Failed to fsync directory {dir}: {e}")
                }
            }

            Ok(parent_layers.len())
        });

        match jh.await {
            Ok(Ok(layer_count)) => {
                tracing::info!(count = layer_count, "Hard linked layers into child shards");
            }
            Ok(Err(e)) => {
                // This is an optimization, so we tolerate failure.
                tracing::warn!("Error hard-linking layers, proceeding anyway: {e}")
            }
            Err(e) => {
                // This is something totally unexpected like a panic, so bail out.
                anyhow::bail!("Error joining hard linking task: {e}");
            }
        }

        Ok(())
    }

    ///
    /// Shut down all tenants. This runs as part of pageserver shutdown.
    ///
    /// NB: We leave the tenants in the map, so that they remain accessible through
    /// the management API until we shut it down. If we removed the shut-down tenants
    /// from the tenants map, the management API would return 404 for these tenants,
    /// because TenantsMap::get() now returns `None`.
    /// That could be easily misinterpreted by control plane, the consumer of the
    /// management API. For example, it could attach the tenant on a different pageserver.
    /// We would then be in split-brain once this pageserver restarts.
    #[instrument(skip_all)]
    pub(crate) async fn shutdown(&self) {
        self.cancel.cancel();

        shutdown_all_tenants0(self.tenants).await
    }

    pub(crate) async fn detach_tenant(
        &self,
        conf: &'static PageServerConf,
        tenant_shard_id: TenantShardId,
        deletion_queue_client: &DeletionQueueClient,
    ) -> Result<(), TenantStateError> {
        let tmp_path = self
            .detach_tenant0(conf, tenant_shard_id, deletion_queue_client)
            .await?;
        self.background_purges.spawn(tmp_path);

        Ok(())
    }

    async fn detach_tenant0(
        &self,
        conf: &'static PageServerConf,
        tenant_shard_id: TenantShardId,
        deletion_queue_client: &DeletionQueueClient,
    ) -> Result<Utf8PathBuf, TenantStateError> {
        let tenant_dir_rename_operation = |tenant_id_to_clean: TenantShardId| async move {
            let local_tenant_directory = conf.tenant_path(&tenant_id_to_clean);
            safe_rename_tenant_dir(&local_tenant_directory)
                .await
                .with_context(|| {
                    format!("local tenant directory {local_tenant_directory:?} rename")
                })
        };

        let removal_result = remove_tenant_from_memory(
            self.tenants,
            tenant_shard_id,
            tenant_dir_rename_operation(tenant_shard_id),
        )
        .await;

        // Flush pending deletions, so that they have a good chance of passing validation
        // before this tenant is potentially re-attached elsewhere.
        deletion_queue_client.flush_advisory();

        removal_result
    }

    pub(crate) fn list_tenants(
        &self,
    ) -> Result<Vec<(TenantShardId, TenantState, Generation)>, TenantMapListError> {
        let tenants = self.tenants.read().unwrap();
        let m = match &*tenants {
            TenantShardMap::Initializing => return Err(TenantMapListError::Initializing),
            TenantShardMap::Open(m) | TenantShardMap::ShuttingDown(m) => m,
        };
        Ok(m.iter()
            .filter_map(|(id, tenant)| match tenant {
                TenantSlot::Attached(tenant) => {
                    Some((*id, tenant.current_state(), tenant.generation()))
                }
                TenantSlot::Secondary(_) => None,
                TenantSlot::InProgress(_) => None,
            })
            .collect())
    }

    /// Completes an earlier prepared timeline detach ancestor.
    pub(crate) async fn complete_detaching_timeline_ancestor(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        prepared: PreparedTimelineDetach,
        mut attempt: detach_ancestor::Attempt,
        ctx: &RequestContext,
    ) -> Result<HashSet<TimelineId>, detach_ancestor::Error> {
        use detach_ancestor::Error;

        let slot_guard =
            tenant_map_acquire_slot(&tenant_shard_id, TenantSlotAcquireMode::MustExist).map_err(
                |e| {
                    use TenantSlotError::*;

                    match e {
                        MapState(TenantMapError::ShuttingDown) => Error::ShuttingDown,
                        NotFound(_) | InProgress | MapState(_) => Error::DetachReparent(e.into()),
                    }
                },
            )?;

        let tenant = {
            let old_slot = slot_guard
                .get_old_value()
                .as_ref()
                .expect("requested MustExist");

            let Some(tenant) = old_slot.get_attached() else {
                return Err(Error::DetachReparent(anyhow::anyhow!(
                    "Tenant is not in attached state"
                )));
            };

            if !tenant.is_active() {
                return Err(Error::DetachReparent(anyhow::anyhow!(
                    "Tenant is not active"
                )));
            }

            tenant.clone()
        };

        let timeline = tenant
            .get_timeline(timeline_id, true)
            .map_err(Error::NotFound)?;

        let resp = timeline
            .detach_from_ancestor_and_reparent(&tenant, prepared, ctx)
            .await?;

        let mut slot_guard = slot_guard;

        let tenant = if resp.reset_tenant_required() {
            attempt.before_reset_tenant();

            let (_guard, progress) = utils::completion::channel();
            match tenant.shutdown(progress, ShutdownMode::Reload).await {
                Ok(()) => {
                    slot_guard.drop_old_value().expect("it was just shutdown");
                }
                Err(_barrier) => {
                    slot_guard.revert();
                    // this really should not happen, at all, unless a shutdown without acquiring
                    // tenant slot was already going? regardless, on restart the attempt tracking
                    // will reset to retryable.
                    return Err(Error::ShuttingDown);
                }
            }

            let tenant_path = self.conf.tenant_path(&tenant_shard_id);
            let config = TenantShard::load_tenant_config(self.conf, &tenant_shard_id)
                .map_err(|e| Error::DetachReparent(e.into()))?;

            let shard_identity = config.shard;
            let tenant = tenant_spawn(
                self.conf,
                tenant_shard_id,
                &tenant_path,
                self.resources.clone(),
                AttachedTenantConf::try_from(config).map_err(Error::DetachReparent)?,
                shard_identity,
                None,
                SpawnMode::Eager,
                ctx,
            )
            .map_err(|_| Error::ShuttingDown)?;

            {
                let mut g = tenant.ongoing_timeline_detach.lock().unwrap();
                assert!(
                    g.is_none(),
                    "there cannot be any new timeline detach ancestor on newly created tenant"
                );
                *g = Some((attempt.timeline_id, attempt.new_barrier()));
            }

            // if we bail out here, we will not allow a new attempt, which should be fine.
            // pageserver should be shutting down regardless? tenant_reset would help, unless it
            // runs into the same problem.
            slot_guard
                .upsert(TenantSlot::Attached(tenant.clone()))
                .map_err(|e| match e {
                    TenantSlotUpsertError::ShuttingDown(_) => Error::ShuttingDown,
                    other => Error::DetachReparent(other.into()),
                })?;
            tenant
        } else {
            tracing::info!("skipping tenant_reset as no changes made required it");
            tenant
        };

        if let Some(reparented) = resp.completed() {
            // finally ask the restarted tenant to complete the detach
            //
            // rationale for 9999s: we don't really have a timetable here; if retried, the caller
            // will get an 503.
            tenant
                .wait_to_become_active(std::time::Duration::from_secs(9999))
                .await
                .map_err(|e| {
                    use pageserver_api::models::TenantState;
                    use GetActiveTenantError::{Cancelled, WillNotBecomeActive};
                    match e {
                        Cancelled | WillNotBecomeActive(TenantState::Stopping { .. }) => {
                            Error::ShuttingDown
                        }
                        other => Error::Complete(other.into()),
                    }
                })?;

            utils::pausable_failpoint!(
                "timeline-detach-ancestor::after_activating_before_finding-pausable"
            );

            let timeline = tenant
                .get_timeline(attempt.timeline_id, true)
                .map_err(Error::NotFound)?;

            timeline
                .complete_detaching_timeline_ancestor(&tenant, attempt, ctx)
                .await
                .map(|()| reparented)
        } else {
            // at least the latest versions have now been downloaded and refreshed; be ready to
            // retry another time.
            Err(Error::FailedToReparentAll)
        }
    }

    /// A page service client sends a TenantId, and to look up the correct Tenant we must
    /// resolve this to a fully qualified TenantShardId.
    ///
    /// During shard splits: we shall see parent shards in InProgress state and skip them, and
    /// instead match on child shards which should appear in Attached state.  Very early in a shard
    /// split, or in other cases where a shard is InProgress, we will return our own InProgress result
    /// to instruct the caller to wait for that to finish before querying again.
    pub(crate) fn resolve_attached_shard(
        &self,
        tenant_id: &TenantId,
        selector: ShardSelector,
    ) -> ShardResolveResult {
        let tenants = self.tenants.read().unwrap();
        let mut want_shard = None;
        let mut any_in_progress = None;

        match &*tenants {
            TenantShardMap::Initializing => ShardResolveResult::NotFound,
            TenantShardMap::Open(m) | TenantShardMap::ShuttingDown(m) => {
                for slot in m.range(TenantShardId::tenant_range(*tenant_id)) {
                    // Ignore all slots that don't contain an attached tenant
                    let tenant = match &slot.1 {
                        TenantSlot::Attached(t) => t,
                        TenantSlot::InProgress(barrier) => {
                            // We might still find a usable shard, but in case we don't, remember that
                            // we saw at least one InProgress slot, so that we can distinguish this case
                            // from a simple NotFound in our return value.
                            any_in_progress = Some(barrier.clone());
                            continue;
                        }
                        _ => continue,
                    };

                    match selector {
                        ShardSelector::Zero if slot.0.shard_number == ShardNumber(0) => {
                            return ShardResolveResult::Found(tenant.clone())
                        }
                        ShardSelector::Page(key) => {
                            // First slot we see for this tenant, calculate the expected shard number
                            // for the key: we will use this for checking if this and subsequent
                            // slots contain the key, rather than recalculating the hash each time.
                            if want_shard.is_none() {
                                want_shard = Some(tenant.shard_identity.get_shard_number(&key));
                            }

                            if Some(tenant.shard_identity.number) == want_shard {
                                return ShardResolveResult::Found(tenant.clone());
                            }
                        }
                        ShardSelector::Known(shard)
                            if tenant.shard_identity.shard_index() == shard =>
                        {
                            return ShardResolveResult::Found(tenant.clone());
                        }
                        _ => continue,
                    }
                }

                // Fall through: we didn't find a slot that was in Attached state & matched our selector.  If
                // we found one or more InProgress slot, indicate to caller that they should retry later.  Otherwise
                // this requested shard simply isn't found.
                if let Some(barrier) = any_in_progress {
                    ShardResolveResult::InProgress(barrier)
                } else {
                    ShardResolveResult::NotFound
                }
            }
        }
    }

    /// Calculate the tenant shards' contributions to this pageserver's utilization metrics.  The
    /// returned values are:
    ///  - the number of bytes of local disk space this pageserver's shards are requesting, i.e.
    ///    how much space they would use if not impacted by disk usage eviction.
    ///  - the number of tenant shards currently on this pageserver, including attached
    ///    and secondary.
    ///
    /// This function is quite expensive: callers are expected to cache the result and
    /// limit how often they call it.
    pub(crate) fn calculate_utilization(&self) -> Result<(u64, u32), TenantMapListError> {
        let tenants = self.tenants.read().unwrap();
        let m = match &*tenants {
            TenantShardMap::Initializing => return Err(TenantMapListError::Initializing),
            TenantShardMap::Open(m) | TenantShardMap::ShuttingDown(m) => m,
        };
        let shard_count = m.len();
        let mut wanted_bytes = 0;

        for tenant_slot in m.values() {
            match tenant_slot {
                TenantSlot::InProgress(_barrier) => {
                    // While a slot is being changed, we can't know how much storage it wants.  This
                    // means this function's output can fluctuate if a lot of changes are going on
                    // (such as transitions from secondary to attached).
                    //
                    // We could wait for the barrier and retry, but it's important that the utilization
                    // API is responsive, and the data quality impact is not very significant.
                    continue;
                }
                TenantSlot::Attached(tenant) => {
                    wanted_bytes += tenant.local_storage_wanted();
                }
                TenantSlot::Secondary(secondary) => {
                    let progress = secondary.progress.lock().unwrap();
                    wanted_bytes += if progress.heatmap_mtime.is_some() {
                        // If we have heatmap info, then we will 'want' the sum
                        // of the size of layers in the heatmap: this is how much space
                        // we would use if not doing any eviction.
                        progress.bytes_total
                    } else {
                        // In the absence of heatmap info, assume that the secondary location simply
                        // needs as much space as it is currently using.
                        secondary.resident_size_metric.get()
                    }
                }
            }
        }

        Ok((wanted_bytes, shard_count as u32))
    }

    #[instrument(skip_all, fields(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), %timeline_id))]
    pub(crate) async fn immediate_gc(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        gc_req: TimelineGcRequest,
        cancel: CancellationToken,
        ctx: &RequestContext,
    ) -> Result<GcResult, ApiError> {
        let tenant = {
            let guard = self.tenants.read().unwrap();
            guard
                .get(&tenant_shard_id)
                .cloned()
                .with_context(|| format!("tenant {tenant_shard_id}"))
                .map_err(|e| ApiError::NotFound(e.into()))?
        };

        let gc_horizon = gc_req.gc_horizon.unwrap_or_else(|| tenant.get_gc_horizon());
        // Use tenant's pitr setting
        let pitr = tenant.get_pitr_interval();

        tenant.wait_to_become_active(ACTIVE_TENANT_TIMEOUT).await?;

        // Run in task_mgr to avoid race with tenant_detach operation
        let ctx: RequestContext =
            ctx.detached_child(TaskKind::GarbageCollector, DownloadBehavior::Download);

        let _gate_guard = tenant.gate.enter().map_err(|_| ApiError::ShuttingDown)?;

        fail::fail_point!("immediate_gc_task_pre");

        #[allow(unused_mut)]
        let mut result = tenant
            .gc_iteration(Some(timeline_id), gc_horizon, pitr, &cancel, &ctx)
            .await;
        // FIXME: `gc_iteration` can return an error for multiple reasons; we should handle it
        // better once the types support it.

        #[cfg(feature = "testing")]
        {
            // we need to synchronize with drop completion for python tests without polling for
            // log messages
            if let Ok(result) = result.as_mut() {
                let mut js = tokio::task::JoinSet::new();
                for layer in std::mem::take(&mut result.doomed_layers) {
                    js.spawn(layer.wait_drop());
                }
                tracing::info!(
                    total = js.len(),
                    "starting to wait for the gc'd layers to be dropped"
                );
                while let Some(res) = js.join_next().await {
                    res.expect("wait_drop should not panic");
                }
            }

            let timeline = tenant.get_timeline(timeline_id, false).ok();
            let rtc = timeline.as_ref().map(|x| &x.remote_client);

            if let Some(rtc) = rtc {
                // layer drops schedule actions on remote timeline client to actually do the
                // deletions; don't care about the shutdown error, just exit fast
                drop(rtc.wait_completion().await);
            }
        }

        result.map_err(|e| match e {
            GcError::TenantCancelled | GcError::TimelineCancelled => ApiError::ShuttingDown,
            GcError::TimelineNotFound => {
                ApiError::NotFound(anyhow::anyhow!("Timeline not found").into())
            }
            other => ApiError::InternalServerError(anyhow::anyhow!(other)),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GetTenantError {
    /// NotFound is a TenantId rather than TenantShardId, because this error type is used from
    /// getters that use a TenantId and a ShardSelector, not just getters that target a specific shard.
    #[error("Tenant {0} not found")]
    NotFound(TenantId),

    #[error("Tenant {0} not found")]
    ShardNotFound(TenantShardId),

    #[error("Tenant {0} is not active")]
    NotActive(TenantShardId),

    // Initializing or shutting down: cannot authoritatively say whether we have this tenant
    #[error("Tenant map is not available: {0}")]
    MapState(#[from] TenantMapError),
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum GetActiveTenantError {
    /// We may time out either while TenantSlot is InProgress, or while the Tenant
    /// is in a non-Active state
    #[error(
        "Timed out waiting {wait_time:?} for tenant active state. Latest state: {latest_state:?}"
    )]
    WaitForActiveTimeout {
        latest_state: Option<TenantState>,
        wait_time: Duration,
    },

    /// The TenantSlot is absent, or in secondary mode
    #[error(transparent)]
    NotFound(#[from] GetTenantError),

    /// Cancellation token fired while we were waiting
    #[error("cancelled")]
    Cancelled,

    /// Tenant exists, but is in a state that cannot become active (e.g. Stopping, Broken)
    #[error("will not become active.  Current state: {0}")]
    WillNotBecomeActive(TenantState),

    /// Broken is logically a subset of WillNotBecomeActive, but a distinct error is useful as
    /// WillNotBecomeActive is a permitted error under some circumstances, whereas broken should
    /// never happen.
    #[error("Tenant is broken: {0}")]
    Broken(String),

    #[error("reconnect to switch tenant id")]
    SwitchedTenant,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeleteTimelineError {
    #[error("Tenant {0}")]
    Tenant(#[from] GetTenantError),

    #[error("Timeline {0}")]
    Timeline(#[from] crate::tenant::DeleteTimelineError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TenantStateError {
    #[error("Tenant {0} is stopping")]
    IsStopping(TenantShardId),
    #[error(transparent)]
    SlotError(#[from] TenantSlotError),
    #[error(transparent)]
    SlotUpsertError(#[from] TenantSlotUpsertError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TenantMapListError {
    #[error("tenant map is still initiailizing")]
    Initializing,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TenantMapInsertError {
    #[error(transparent)]
    SlotError(#[from] TenantSlotError),
    #[error(transparent)]
    SlotUpsertError(#[from] TenantSlotUpsertError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Superset of TenantMapError: issues that can occur when acquiring a slot
/// for a particular tenant ID.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TenantSlotError {
    /// When acquiring a slot with the expectation that the tenant already exists.
    #[error("Tenant {0} not found")]
    NotFound(TenantShardId),

    // Tried to read a slot that is currently being mutated by another administrative
    // operation.
    #[error("tenant has a state change in progress, try again later")]
    InProgress,

    #[error(transparent)]
    MapState(#[from] TenantMapError),
}

/// Superset of TenantMapError: issues that can occur when using a SlotGuard
/// to insert a new value.
#[derive(thiserror::Error)]
pub(crate) enum TenantSlotUpsertError {
    /// An error where the slot is in an unexpected state, indicating a code bug
    #[error("Internal error updating Tenant")]
    InternalError(Cow<'static, str>),

    #[error(transparent)]
    MapState(TenantMapError),

    // If we encounter TenantManager shutdown during upsert, we must carry the Completion
    // from the SlotGuard, so that the caller can hold it while they clean up: otherwise
    // TenantManager shutdown might race ahead before we're done cleaning up any Tenant that
    // was protected by the SlotGuard.
    #[error("Shutting down")]
    ShuttingDown((TenantSlot, utils::completion::Completion)),
}

impl std::fmt::Debug for TenantSlotUpsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::InternalError(reason) => write!(f, "Internal Error {reason}"),
            Self::MapState(map_error) => write!(f, "Tenant map state: {map_error:?}"),
            Self::ShuttingDown(_completion) => write!(f, "Tenant map shutting down"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum TenantSlotDropError {
    /// It is only legal to drop a TenantSlot if its contents are fully shut down
    #[error("Tenant was not shut down")]
    NotShutdown,
}

/// Errors that can happen any time we are walking the tenant map to try and acquire
/// the TenantSlot for a particular tenant.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TenantMapError {
    // Tried to read while initializing
    #[error("tenant map is still initializing")]
    StillInitializing,

    // Tried to read while shutting down
    #[error("tenant map is shutting down")]
    ShuttingDown,
}

/// Guards a particular tenant_id's content in the TenantsMap.
///
/// While this structure exists, the TenantsMap will contain a [`TenantSlot::InProgress`]
/// for this tenant, which acts as a marker for any operations targeting
/// this tenant to retry later, or wait for the InProgress state to end.
///
/// This structure enforces the important invariant that we do not have overlapping
/// tasks that will try use local storage for a the same tenant ID: we enforce that
/// the previous contents of a slot have been shut down before the slot can be
/// left empty or used for something else
///
/// Holders of a SlotGuard should explicitly dispose of it, using either `upsert`
/// to provide a new value, or `revert` to put the slot back into its initial
/// state.  If the SlotGuard is dropped without calling either of these, then
/// we will leave the slot empty if our `old_value` is already shut down, else
/// we will replace the slot with `old_value` (equivalent to doing a revert).
///
/// The `old_value` may be dropped before the SlotGuard is dropped, by calling
/// `drop_old_value`.  It is an error to call this without shutting down
/// the conents of `old_value`.
pub(crate) struct SlotGuard {
    tenant_shard_id: TenantShardId,
    old_value: Option<TenantSlot>,
    upserted: bool,

    /// [`TenantSlot::InProgress`] carries the corresponding Barrier: it will
    /// release any waiters as soon as this SlotGuard is dropped.
    completion: utils::completion::Completion,
}

impl SlotGuard {
    fn new(
        tenant_shard_id: TenantShardId,
        old_value: Option<TenantSlot>,
        completion: utils::completion::Completion,
    ) -> Self {
        Self {
            tenant_shard_id,
            old_value,
            upserted: false,
            completion,
        }
    }

    /// Get any value that was present in the slot before we acquired ownership
    /// of it: in state transitions, this will be the old state.
    ///
    // FIXME: get_ prefix
    // FIXME: this should be .as_ref() -- unsure why no clippy
    fn get_old_value(&self) -> &Option<TenantSlot> {
        &self.old_value
    }

    /// Emplace a new value in the slot.  This consumes the guard, and after
    /// returning, the slot is no longer protected from concurrent changes.
    fn upsert(mut self, new_value: TenantSlot) -> Result<(), TenantSlotUpsertError> {
        if !self.old_value_is_shutdown() {
            // This is a bug: callers should never try to drop an old value without
            // shutting it down
            return Err(TenantSlotUpsertError::InternalError(
                "Old TenantSlot value not shut down".into(),
            ));
        }

        let replaced = {
            let mut locked = TENANTS.write().unwrap();

            if let TenantSlot::InProgress(_) = new_value {
                // It is never expected to try and upsert InProgress via this path: it should
                // only be written via the tenant_map_acquire_slot path.  If we hit this it's a bug.
                return Err(TenantSlotUpsertError::InternalError(
                    "Attempt to upsert an InProgress state".into(),
                ));
            }

            let m = match &mut *locked {
                TenantShardMap::Initializing => {
                    return Err(TenantSlotUpsertError::MapState(
                        TenantMapError::StillInitializing,
                    ))
                }
                TenantShardMap::ShuttingDown(_) => {
                    return Err(TenantSlotUpsertError::ShuttingDown((
                        new_value,
                        self.completion.clone(),
                    )));
                }
                TenantShardMap::Open(m) => m,
            };

            METRICS.slot_inserted(&new_value);

            let replaced = m.insert(self.tenant_shard_id, new_value);
            self.upserted = true;
            if let Some(replaced) = replaced.as_ref() {
                METRICS.slot_removed(replaced);
            }

            replaced
        };

        // Sanity check: on an upsert we should always be replacing an InProgress marker
        match replaced {
            Some(TenantSlot::InProgress(_)) => {
                // Expected case: we find our InProgress in the map: nothing should have
                // replaced it because the code that acquires slots will not grant another
                // one for the same TenantId.
                Ok(())
            }
            None => {
                METRICS.unexpected_errors.inc();
                error!(
                    tenant_shard_id = %self.tenant_shard_id,
                    "Missing InProgress marker during tenant upsert, this is a bug."
                );
                Err(TenantSlotUpsertError::InternalError(
                    "Missing InProgress marker during tenant upsert".into(),
                ))
            }
            Some(slot) => {
                METRICS.unexpected_errors.inc();
                error!(tenant_shard_id=%self.tenant_shard_id, "Unexpected contents of TenantSlot during upsert, this is a bug.  Contents: {:?}", slot);
                Err(TenantSlotUpsertError::InternalError(
                    "Unexpected contents of TenantSlot".into(),
                ))
            }
        }
    }

    /// Replace the InProgress slot with whatever was in the guard when we started
    fn revert(mut self) {
        if let Some(value) = self.old_value.take() {
            match self.upsert(value) {
                Err(TenantSlotUpsertError::InternalError(_)) => {
                    // We already logged the error, nothing else we can do.
                }
                Err(
                    TenantSlotUpsertError::MapState(_) | TenantSlotUpsertError::ShuttingDown(_),
                ) => {
                    // If the map is shutting down, we need not replace anything
                }
                Ok(()) => {}
            }
        }
    }

    /// We may never drop our old value until it is cleanly shut down: otherwise we might leave
    /// rogue background tasks that would write to the local tenant directory that this guard
    /// is responsible for protecting
    fn old_value_is_shutdown(&self) -> bool {
        match self.old_value.as_ref() {
            Some(TenantSlot::Attached(tenant)) => tenant.gate.close_complete(),
            Some(TenantSlot::Secondary(secondary_tenant)) => secondary_tenant.gate.close_complete(),
            Some(TenantSlot::InProgress(_)) => {
                // A SlotGuard cannot be constructed for a slot that was already InProgress
                unreachable!()
            }
            None => true,
        }
    }

    /// The guard holder is done with the old value of the slot: they are obliged to already
    /// shut it down before we reach this point.
    fn drop_old_value(&mut self) -> Result<(), TenantSlotDropError> {
        if !self.old_value_is_shutdown() {
            Err(TenantSlotDropError::NotShutdown)
        } else {
            self.old_value.take();
            Ok(())
        }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if self.upserted {
            return;
        }
        // Our old value is already shutdown, or it never existed: it is safe
        // for us to fully release the TenantSlot back into an empty state

        let mut locked = TENANTS.write().unwrap();

        let m = match &mut *locked {
            TenantShardMap::Initializing => {
                // There is no map, this should never happen.
                return;
            }
            TenantShardMap::ShuttingDown(_) => {
                // When we transition to shutdown, InProgress elements are removed
                // from the map, so we do not need to clean up our Inprogress marker.
                // See [`shutdown_all_tenants0`]
                return;
            }
            TenantShardMap::Open(m) => m,
        };

        use std::collections::btree_map::Entry;
        match m.entry(self.tenant_shard_id) {
            Entry::Occupied(mut entry) => {
                if !matches!(entry.get(), TenantSlot::InProgress(_)) {
                    METRICS.unexpected_errors.inc();
                    error!(tenant_shard_id=%self.tenant_shard_id, "Unexpected contents of TenantSlot during drop, this is a bug.  Contents: {:?}", entry.get());
                }

                if self.old_value_is_shutdown() {
                    METRICS.slot_removed(entry.get());
                    entry.remove();
                } else {
                    let inserting = self.old_value.take().unwrap();
                    METRICS.slot_inserted(&inserting);
                    let replaced = entry.insert(inserting);
                    METRICS.slot_removed(&replaced);
                }
            }
            Entry::Vacant(_) => {
                METRICS.unexpected_errors.inc();
                error!(
                    tenant_shard_id = %self.tenant_shard_id,
                    "Missing InProgress marker during SlotGuard drop, this is a bug."
                );
            }
        }
    }
}

enum TenantSlotPeekMode {
    /// In Read mode, peek will be permitted to see the slots even if the pageserver is shutting down
    Read,
    /// In Write mode, trying to peek at a slot while the pageserver is shutting down is an error
    Write,
}

fn tenant_map_peek_slot<'a>(
    tenants: &'a std::sync::RwLockReadGuard<'a, TenantShardMap>,
    tenant_shard_id: &TenantShardId,
    mode: TenantSlotPeekMode,
) -> Result<Option<&'a TenantSlot>, TenantMapError> {
    match tenants.deref() {
        TenantShardMap::Initializing => Err(TenantMapError::StillInitializing),
        TenantShardMap::ShuttingDown(m) => match mode {
            TenantSlotPeekMode::Read => Ok(Some(
                // When reading in ShuttingDown state, we must translate None results
                // into a ShuttingDown error, because absence of a tenant shard ID in the map
                // isn't a reliable indicator of the tenant being gone: it might have been
                // InProgress when shutdown started, and cleaned up from that state such
                // that it's now no longer in the map.  Callers will have to wait until
                // we next start up to get a proper answer.  This avoids incorrect 404 API responses.
                m.get(tenant_shard_id).ok_or(TenantMapError::ShuttingDown)?,
            )),
            TenantSlotPeekMode::Write => Err(TenantMapError::ShuttingDown),
        },
        TenantShardMap::Open(m) => Ok(m.get(tenant_shard_id)),
    }
}

enum TenantSlotAcquireMode {
    /// Acquire the slot irrespective of current state, or whether it already exists
    Any,
    /// Return an error if trying to acquire a slot and it doesn't already exist
    MustExist,
}

fn tenant_map_acquire_slot(
    tenant_shard_id: &TenantShardId,
    mode: TenantSlotAcquireMode,
) -> Result<SlotGuard, TenantSlotError> {
    tenant_map_acquire_slot_impl(tenant_shard_id, &TENANTS, mode)
}

fn tenant_map_acquire_slot_impl(
    tenant_shard_id: &TenantShardId,
    tenants: &std::sync::RwLock<TenantShardMap>,
    mode: TenantSlotAcquireMode,
) -> Result<SlotGuard, TenantSlotError> {
    use TenantSlotAcquireMode::*;
    METRICS.tenant_slot_writes.inc();

    let mut locked = tenants.write().unwrap();
    let span = tracing::info_span!("acquire_slot", tenant_id=%tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug());
    let _guard = span.enter();

    let m = match &mut *locked {
        TenantShardMap::Initializing => return Err(TenantMapError::StillInitializing.into()),
        TenantShardMap::ShuttingDown(_) => return Err(TenantMapError::ShuttingDown.into()),
        TenantShardMap::Open(m) => m,
    };

    use std::collections::btree_map::Entry;

    let entry = m.entry(*tenant_shard_id);

    match entry {
        Entry::Vacant(v) => match mode {
            MustExist => {
                tracing::debug!("Vacant && MustExist: return NotFound");
                Err(TenantSlotError::NotFound(*tenant_shard_id))
            }
            _ => {
                let (completion, barrier) = utils::completion::channel();
                let inserting = TenantSlot::InProgress(barrier);
                METRICS.slot_inserted(&inserting);
                v.insert(inserting);
                tracing::debug!("Vacant, inserted InProgress");
                Ok(SlotGuard::new(*tenant_shard_id, None, completion))
            }
        },
        Entry::Occupied(mut o) => {
            // Apply mode-driven checks
            match (o.get(), mode) {
                (TenantSlot::InProgress(_), _) => {
                    tracing::debug!("Occupied, failing for InProgress");
                    Err(TenantSlotError::InProgress)
                }
                _ => {
                    // Happy case: the slot was not in any state that violated our mode
                    let (completion, barrier) = utils::completion::channel();
                    let in_progress = TenantSlot::InProgress(barrier);
                    METRICS.slot_inserted(&in_progress);
                    let old_value = o.insert(in_progress);
                    METRICS.slot_removed(&old_value);
                    tracing::debug!("Occupied, replaced with InProgress");
                    Ok(SlotGuard::new(
                        *tenant_shard_id,
                        Some(old_value),
                        completion,
                    ))
                }
            }
        }
    }
}

/// Stops and removes the tenant from memory, if it's not [`TenantState::Stopping`] already, bails otherwise.
/// Allows to remove other tenant resources manually, via `tenant_cleanup`.
/// If the cleanup fails, tenant will stay in memory in [`TenantState::Broken`] state, and another removal
/// operation would be needed to remove it.
async fn remove_tenant_from_memory<V, F>(
    tenants: &std::sync::RwLock<TenantShardMap>,
    tenant_shard_id: TenantShardId,
    tenant_cleanup: F,
) -> Result<V, TenantStateError>
where
    F: std::future::Future<Output = anyhow::Result<V>>,
{
    let mut slot_guard =
        tenant_map_acquire_slot_impl(&tenant_shard_id, tenants, TenantSlotAcquireMode::MustExist)?;

    // allow pageserver shutdown to await for our completion
    let (_guard, progress) = completion::channel();

    // The SlotGuard allows us to manipulate the Tenant object without fear of some
    // concurrent API request doing something else for the same tenant ID.
    let attached_tenant = match slot_guard.get_old_value() {
        Some(TenantSlot::Attached(tenant)) => {
            // whenever we remove a tenant from memory, we don't want to flush and wait for upload
            let shutdown_mode = ShutdownMode::Hard;

            // shutdown is sure to transition tenant to stopping, and wait for all tasks to complete, so
            // that we can continue safely to cleanup.
            match tenant.shutdown(progress, shutdown_mode).await {
                Ok(()) => {}
                Err(_other) => {
                    // if pageserver shutdown or other detach/ignore is already ongoing, we don't want to
                    // wait for it but return an error right away because these are distinct requests.
                    slot_guard.revert();
                    return Err(TenantStateError::IsStopping(tenant_shard_id));
                }
            }
            Some(tenant)
        }
        Some(TenantSlot::Secondary(secondary_state)) => {
            tracing::info!("Shutting down in secondary mode");
            secondary_state.shutdown().await;
            None
        }
        Some(TenantSlot::InProgress(_)) => {
            // Acquiring a slot guarantees its old value was not InProgress
            unreachable!();
        }
        None => None,
    };

    match tenant_cleanup
        .await
        .with_context(|| format!("Failed to run cleanup for tenant {tenant_shard_id}"))
    {
        Ok(hook_value) => {
            // Success: drop the old TenantSlot::Attached.
            slot_guard
                .drop_old_value()
                .expect("We just called shutdown");

            Ok(hook_value)
        }
        Err(e) => {
            // If we had a Tenant, set it to Broken and put it back in the TenantsMap
            if let Some(attached_tenant) = attached_tenant {
                attached_tenant.set_broken(e.to_string()).await;
            }
            // Leave the broken tenant in the map
            slot_guard.revert();

            Err(TenantStateError::Other(e))
        }
    }
}

use {
    crate::tenant::gc_result::GcResult, pageserver_api::models::TimelineGcRequest,
    utils::http::error::ApiError,
};

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tracing::Instrument;

    use crate::tenant::mgr::TenantSlot;

    use super::{super::harness::TenantShardHarness, TenantShardMap};

    #[tokio::test(start_paused = true)]
    async fn shutdown_awaits_in_progress_tenant() {
        // Test that if an InProgress tenant is in the map during shutdown, the shutdown will gracefully
        // wait for it to complete before proceeding.

        let h = TenantShardHarness::create("shutdown_awaits_in_progress_tenant")
            .await
            .unwrap();
        let (t, _ctx) = h.load().await;

        // harness loads it to active, which is forced and nothing is running on the tenant

        let id = t.tenant_shard_id();

        // tenant harness configures the logging and we cannot escape it
        let span = h.span();
        let _e = span.enter();

        let tenants = BTreeMap::from([(id, TenantSlot::Attached(t.clone()))]);
        let tenants = Arc::new(std::sync::RwLock::new(TenantShardMap::Open(tenants)));

        // Invoke remove_tenant_from_memory with a cleanup hook that blocks until we manually
        // permit it to proceed: that will stick the tenant in InProgress

        let (until_cleanup_completed, can_complete_cleanup) = utils::completion::channel();
        let (until_cleanup_started, cleanup_started) = utils::completion::channel();
        let mut remove_tenant_from_memory_task = {
            let jh = tokio::spawn({
                let tenants = tenants.clone();
                async move {
                    let cleanup = async move {
                        drop(until_cleanup_started);
                        can_complete_cleanup.wait().await;
                        anyhow::Ok(())
                    };
                    super::remove_tenant_from_memory(&tenants, id, cleanup).await
                }
                .instrument(h.span())
            });

            // now the long cleanup should be in place, with the stopping state
            cleanup_started.wait().await;
            jh
        };

        let mut shutdown_task = {
            let (until_shutdown_started, shutdown_started) = utils::completion::channel();

            let shutdown_task = tokio::spawn(async move {
                drop(until_shutdown_started);
                super::shutdown_all_tenants0(&tenants).await;
            });

            shutdown_started.wait().await;
            shutdown_task
        };

        let long_time = std::time::Duration::from_secs(15);
        tokio::select! {
            _ = &mut shutdown_task => unreachable!("shutdown should block on remove_tenant_from_memory completing"),
            _ = &mut remove_tenant_from_memory_task => unreachable!("remove_tenant_from_memory_task should not complete until explicitly unblocked"),
            _ = tokio::time::sleep(long_time) => {},
        }

        drop(until_cleanup_completed);

        // Now that we allow it to proceed, shutdown should complete immediately
        remove_tenant_from_memory_task.await.unwrap().unwrap();
        shutdown_task.await.unwrap();
    }
}
