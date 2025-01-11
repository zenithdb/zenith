use std::borrow::Cow;
use std::error::Error as _;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use control_plane::endpoint::{ComputeControlPlane, EndpointStatus};
use control_plane::local_env::LocalEnv;
use futures::StreamExt;
use hyper::StatusCode;
use pageserver_api::controller_api::AvailabilityZone;
use pageserver_api::shard::{ShardCount, ShardNumber, ShardStripeSize, TenantShardId};
use postgres_connection::parse_host_port;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{info_span, Instrument};
use utils::{
    backoff::{self},
    id::{NodeId, TenantId},
};

use crate::service::Config;

const SLOWDOWN_DELAY: Duration = Duration::from_secs(5);

const NOTIFY_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) const API_CONCURRENCY: usize = 32;

struct UnshardedComputeHookTenant {
    // Which node is this tenant attached to
    node_id: NodeId,

    // The tenant's preferred AZ, so that we may pass this on to the control plane
    preferred_az: Option<AvailabilityZone>,

    // Must hold this lock to send a notification.
    send_lock: Arc<tokio::sync::Mutex<Option<ComputeRemoteState>>>,
}
struct ShardedComputeHookTenant {
    stripe_size: ShardStripeSize,
    shard_count: ShardCount,
    shards: Vec<(ShardNumber, NodeId)>,

    // The tenant's preferred AZ, so that we may pass this on to the control plane
    preferred_az: Option<AvailabilityZone>,

    // Must hold this lock to send a notification.  The contents represent
    // the last successfully sent notification, and are used to coalesce multiple
    // updates by only sending when there is a chance since our last successful send.
    send_lock: Arc<tokio::sync::Mutex<Option<ComputeRemoteState>>>,
}

/// Represents our knowledge of the compute's state: we can update this when we get a
/// response from a notify API call, which tells us what has been applied.
///
/// Should be wrapped in an Option<>, as we cannot always know the remote state.
#[derive(PartialEq, Eq, Debug)]
struct ComputeRemoteState {
    // The request body which was acked by the compute
    request: ComputeHookNotifyRequest,

    // Whether the cplane indicated that the state was applied to running computes, or just
    // persisted.  In the Neon control plane, this is the difference between a 423 response (meaning
    // persisted but not applied), and a 2xx response (both persisted and applied)
    applied: bool,
}

enum ComputeHookTenant {
    Unsharded(UnshardedComputeHookTenant),
    Sharded(ShardedComputeHookTenant),
}

impl ComputeHookTenant {
    /// Construct with at least one shard's information
    fn new(
        tenant_shard_id: TenantShardId,
        stripe_size: ShardStripeSize,
        preferred_az: Option<AvailabilityZone>,
        node_id: NodeId,
    ) -> Self {
        if tenant_shard_id.shard_count.count() > 1 {
            Self::Sharded(ShardedComputeHookTenant {
                shards: vec![(tenant_shard_id.shard_number, node_id)],
                stripe_size,
                shard_count: tenant_shard_id.shard_count,
                preferred_az,
                send_lock: Arc::default(),
            })
        } else {
            Self::Unsharded(UnshardedComputeHookTenant {
                node_id,
                preferred_az,
                send_lock: Arc::default(),
            })
        }
    }

    fn get_send_lock(&self) -> &Arc<tokio::sync::Mutex<Option<ComputeRemoteState>>> {
        match self {
            Self::Unsharded(unsharded_tenant) => &unsharded_tenant.send_lock,
            Self::Sharded(sharded_tenant) => &sharded_tenant.send_lock,
        }
    }

    fn is_sharded(&self) -> bool {
        matches!(self, ComputeHookTenant::Sharded(_))
    }

    /// Clear compute hook state for the specified shard.
    /// Only valid for [`ComputeHookTenant::Sharded`] instances.
    fn remove_shard(&mut self, tenant_shard_id: TenantShardId, stripe_size: ShardStripeSize) {
        match self {
            ComputeHookTenant::Sharded(sharded) => {
                if sharded.stripe_size != stripe_size
                    || sharded.shard_count != tenant_shard_id.shard_count
                {
                    tracing::warn!("Shard split detected while handling detach")
                }

                let shard_idx = sharded.shards.iter().position(|(shard_number, _node_id)| {
                    *shard_number == tenant_shard_id.shard_number
                });

                if let Some(shard_idx) = shard_idx {
                    sharded.shards.remove(shard_idx);
                } else {
                    // This is a valid but niche case, where the tenant was previously attached
                    // as a Secondary location and then detached, so has no previously notified
                    // state.
                    tracing::info!("Shard not found while handling detach")
                }
            }
            ComputeHookTenant::Unsharded(_) => {
                unreachable!("Detach of unsharded tenants is handled externally");
            }
        }
    }

    /// Set one shard's location.  If stripe size or shard count have changed, Self is reset
    /// and drops existing content.
    fn update(&mut self, shard_update: ShardUpdate) {
        let tenant_shard_id = shard_update.tenant_shard_id;
        let node_id = shard_update.node_id;
        let stripe_size = shard_update.stripe_size;
        let preferred_az = shard_update.preferred_az;

        match self {
            Self::Unsharded(unsharded_tenant) if tenant_shard_id.shard_count.count() == 1 => {
                unsharded_tenant.node_id = node_id;
                if unsharded_tenant.preferred_az.as_ref()
                    != preferred_az.as_ref().map(|az| az.as_ref())
                {
                    unsharded_tenant.preferred_az = preferred_az.map(|az| az.as_ref().clone());
                }
            }
            Self::Sharded(sharded_tenant)
                if sharded_tenant.stripe_size == stripe_size
                    && sharded_tenant.shard_count == tenant_shard_id.shard_count =>
            {
                if let Some(existing) = sharded_tenant
                    .shards
                    .iter()
                    .position(|s| s.0 == tenant_shard_id.shard_number)
                {
                    sharded_tenant.shards.get_mut(existing).unwrap().1 = node_id;
                } else {
                    sharded_tenant
                        .shards
                        .push((tenant_shard_id.shard_number, node_id));
                    sharded_tenant.shards.sort_by_key(|s| s.0)
                }

                if sharded_tenant.preferred_az.as_ref()
                    != preferred_az.as_ref().map(|az| az.as_ref())
                {
                    sharded_tenant.preferred_az = preferred_az.map(|az| az.as_ref().clone());
                }
            }
            _ => {
                // Shard count changed: reset struct.
                *self = Self::new(
                    tenant_shard_id,
                    stripe_size,
                    preferred_az.map(|az| az.into_owned()),
                    node_id,
                );
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct ComputeHookNotifyRequestShard {
    node_id: NodeId,
    shard_number: ShardNumber,
}

/// Request body that we send to the control plane to notify it of where a tenant is attached
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
struct ComputeHookNotifyRequest {
    tenant_id: TenantId,
    preferred_az: Option<String>,
    stripe_size: Option<ShardStripeSize>,
    shards: Vec<ComputeHookNotifyRequestShard>,
}

/// Error type for attempts to call into the control plane compute notification hook
#[derive(thiserror::Error, Debug)]
pub(crate) enum NotifyError {
    // Request was not send successfully, e.g. transport error
    #[error("Sending request: {0}{}", .0.source().map(|e| format!(": {e}")).unwrap_or_default())]
    Request(#[from] reqwest::Error),
    // Request could not be serviced right now due to ongoing Operation in control plane, but should be possible soon.
    #[error("Control plane tenant busy")]
    Busy,
    // Explicit 429 response asking us to retry less frequently
    #[error("Control plane overloaded")]
    SlowDown,
    // A 503 response indicates the control plane can't handle the request right now
    #[error("Control plane unavailable (status {0})")]
    Unavailable(StatusCode),
    // API returned unexpected non-success status.  We will retry, but log a warning.
    #[error("Control plane returned unexpected status {0}")]
    Unexpected(StatusCode),
    // We shutdown while sending
    #[error("Shutting down")]
    ShuttingDown,
    // A response indicates we will never succeed, such as 400 or 404
    #[error("Non-retryable error {0}")]
    Fatal(StatusCode),

    #[error("neon_local error: {0}")]
    NeonLocal(anyhow::Error),
}

enum MaybeSendResult {
    // Please send this request while holding the lock, and if you succeed then write
    // the request into the lock.
    Transmit(
        (
            ComputeHookNotifyRequest,
            tokio::sync::OwnedMutexGuard<Option<ComputeRemoteState>>,
        ),
    ),
    // Something requires sending, but you must wait for a current sender then call again
    AwaitLock(Arc<tokio::sync::Mutex<Option<ComputeRemoteState>>>),
    // Nothing requires sending
    Noop,
}

impl ComputeHookTenant {
    fn maybe_send(
        &self,
        tenant_id: TenantId,
        lock: Option<tokio::sync::OwnedMutexGuard<Option<ComputeRemoteState>>>,
    ) -> MaybeSendResult {
        let locked = match lock {
            Some(already_locked) => already_locked,
            None => {
                // Lock order: this _must_ be only a try_lock, because we are called inside of the [`ComputeHook::state`] lock.
                let Ok(locked) = self.get_send_lock().clone().try_lock_owned() else {
                    return MaybeSendResult::AwaitLock(self.get_send_lock().clone());
                };
                locked
            }
        };

        let request = match self {
            Self::Unsharded(unsharded_tenant) => Some(ComputeHookNotifyRequest {
                tenant_id,
                shards: vec![ComputeHookNotifyRequestShard {
                    shard_number: ShardNumber(0),
                    node_id: unsharded_tenant.node_id,
                }],
                stripe_size: None,
                preferred_az: unsharded_tenant
                    .preferred_az
                    .as_ref()
                    .map(|az| az.0.clone()),
            }),
            Self::Sharded(sharded_tenant)
                if sharded_tenant.shards.len() == sharded_tenant.shard_count.count() as usize =>
            {
                Some(ComputeHookNotifyRequest {
                    tenant_id,
                    shards: sharded_tenant
                        .shards
                        .iter()
                        .map(|(shard_number, node_id)| ComputeHookNotifyRequestShard {
                            shard_number: *shard_number,
                            node_id: *node_id,
                        })
                        .collect(),
                    stripe_size: Some(sharded_tenant.stripe_size),
                    preferred_az: sharded_tenant.preferred_az.as_ref().map(|az| az.0.clone()),
                })
            }
            Self::Sharded(sharded_tenant) => {
                // Sharded tenant doesn't yet have information for all its shards

                tracing::info!(
                    "ComputeHookTenant::maybe_send: not enough shards ({}/{})",
                    sharded_tenant.shards.len(),
                    sharded_tenant.shard_count.count()
                );
                None
            }
        };

        match request {
            None => {
                // Not yet ready to emit a notification
                tracing::info!("Tenant isn't yet ready to emit a notification");
                MaybeSendResult::Noop
            }
            Some(request)
                if Some(&request) == locked.as_ref().map(|s| &s.request)
                    && locked.as_ref().map(|s| s.applied).unwrap_or(false) =>
            {
                tracing::info!(
                    "Skipping notification because remote state already matches ({:?})",
                    &request
                );
                // No change from the last value successfully sent, and our state indicates that the last
                // value sent was fully applied on the control plane side.
                MaybeSendResult::Noop
            }
            Some(request) => {
                // Our request differs from the last one sent, or the last one sent was not fully applied on the compute side
                MaybeSendResult::Transmit((request, locked))
            }
        }
    }
}

/// The compute hook is a destination for notifications about changes to tenant:pageserver
/// mapping.  It aggregates updates for the shards in a tenant, and when appropriate reconfigures
/// the compute connection string.
pub(super) struct ComputeHook {
    config: Config,
    state: std::sync::Mutex<HashMap<TenantId, ComputeHookTenant>>,
    authorization_header: Option<String>,

    // Concurrency limiter, so that we do not overload the cloud control plane when updating
    // large numbers of tenants (e.g. when failing over after a node failure)
    api_concurrency: tokio::sync::Semaphore,

    // This lock is only used in testing enviroments, to serialize calls into neon_lock
    neon_local_lock: tokio::sync::Mutex<()>,

    // We share a client across all notifications to enable connection re-use etc when
    // sending large numbers of notifications
    client: reqwest::Client,
}

/// Callers may give us a list of these when asking us to send a bulk batch
/// of notifications in the background.  This is a 'notification' in the sense of
/// other code notifying us of a shard's status, rather than being the final notification
/// that we send upwards to the control plane for the whole tenant.
pub(crate) struct ShardUpdate<'a> {
    pub(crate) tenant_shard_id: TenantShardId,
    pub(crate) node_id: NodeId,
    pub(crate) stripe_size: ShardStripeSize,
    pub(crate) preferred_az: Option<Cow<'a, AvailabilityZone>>,
}

impl ComputeHook {
    pub(super) fn new(config: Config) -> Self {
        let authorization_header = config
            .control_plane_jwt_token
            .clone()
            .map(|jwt| format!("Bearer {}", jwt));

        let client = reqwest::ClientBuilder::new()
            .timeout(NOTIFY_REQUEST_TIMEOUT)
            .build()
            .expect("Failed to construct HTTP client");

        Self {
            state: Default::default(),
            config,
            authorization_header,
            neon_local_lock: Default::default(),
            api_concurrency: tokio::sync::Semaphore::new(API_CONCURRENCY),
            client,
        }
    }

    /// For test environments: use neon_local's LocalEnv to update compute
    async fn do_notify_local(
        &self,
        reconfigure_request: &ComputeHookNotifyRequest,
    ) -> Result<(), NotifyError> {
        // neon_local updates are not safe to call concurrently, use a lock to serialize
        // all calls to this function
        let _locked = self.neon_local_lock.lock().await;

        let Some(repo_dir) = self.config.neon_local_repo_dir.as_deref() else {
            tracing::warn!(
                "neon_local_repo_dir not set, likely a bug in neon_local; skipping compute update"
            );
            return Ok(());
        };
        let env = match LocalEnv::load_config(repo_dir) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("Couldn't load neon_local config, skipping compute update ({e})");
                return Ok(());
            }
        };
        let cplane =
            ComputeControlPlane::load(env.clone()).expect("Error loading compute control plane");
        let ComputeHookNotifyRequest {
            tenant_id,
            shards,
            stripe_size,
            preferred_az: _preferred_az,
        } = reconfigure_request;

        let compute_pageservers = shards
            .iter()
            .map(|shard| {
                let ps_conf = env
                    .get_pageserver_conf(shard.node_id)
                    .expect("Unknown pageserver");
                let (pg_host, pg_port) = parse_host_port(&ps_conf.listen_pg_addr)
                    .expect("Unable to parse listen_pg_addr");
                (pg_host, pg_port.unwrap_or(5432))
            })
            .collect::<Vec<_>>();

        for (endpoint_name, endpoint) in &cplane.endpoints {
            if endpoint.tenant_id == *tenant_id && endpoint.status() == EndpointStatus::Running {
                tracing::info!("Reconfiguring endpoint {}", endpoint_name,);
                endpoint
                    .reconfigure(compute_pageservers.clone(), *stripe_size, None)
                    .await
                    .map_err(NotifyError::NeonLocal)?;
            }
        }

        Ok(())
    }

    async fn do_notify_iteration(
        &self,
        url: &String,
        reconfigure_request: &ComputeHookNotifyRequest,
        cancel: &CancellationToken,
    ) -> Result<(), NotifyError> {
        let req = self.client.request(reqwest::Method::PUT, url);
        let req = if let Some(value) = &self.authorization_header {
            req.header(reqwest::header::AUTHORIZATION, value)
        } else {
            req
        };

        tracing::info!(
            "Sending notify request to {} ({:?})",
            url,
            reconfigure_request
        );
        let send_result = req.json(&reconfigure_request).send().await;
        let response = match send_result {
            Ok(r) => r,
            Err(e) => return Err(e.into()),
        };

        // Treat all 2xx responses as success
        if response.status() >= reqwest::StatusCode::OK
            && response.status() < reqwest::StatusCode::MULTIPLE_CHOICES
        {
            if response.status() != reqwest::StatusCode::OK {
                // Non-200 2xx response: it doesn't make sense to retry, but this is unexpected, so
                // log a warning.
                tracing::warn!(
                    "Unexpected 2xx response code {} from control plane",
                    response.status()
                );
            }

            return Ok(());
        }

        // Error response codes
        match response.status() {
            reqwest::StatusCode::TOO_MANY_REQUESTS => {
                // TODO: 429 handling should be global: set some state visible to other requests
                // so that they will delay before starting, rather than all notifications trying
                // once before backing off.
                tokio::time::timeout(SLOWDOWN_DELAY, cancel.cancelled())
                    .await
                    .ok();
                Err(NotifyError::SlowDown)
            }
            reqwest::StatusCode::LOCKED => {
                // We consider this fatal, because it's possible that the operation blocking the control one is
                // also the one that is waiting for this reconcile.  We should let the reconciler calling
                // this hook fail, to give control plane a chance to un-lock.
                tracing::info!("Control plane reports tenant is locked, dropping out of notify");
                Err(NotifyError::Busy)
            }
            reqwest::StatusCode::SERVICE_UNAVAILABLE => {
                Err(NotifyError::Unavailable(StatusCode::SERVICE_UNAVAILABLE))
            }
            reqwest::StatusCode::GATEWAY_TIMEOUT => {
                Err(NotifyError::Unavailable(StatusCode::GATEWAY_TIMEOUT))
            }
            reqwest::StatusCode::BAD_GATEWAY => {
                Err(NotifyError::Unavailable(StatusCode::BAD_GATEWAY))
            }

            reqwest::StatusCode::BAD_REQUEST => Err(NotifyError::Fatal(StatusCode::BAD_REQUEST)),
            reqwest::StatusCode::UNAUTHORIZED => Err(NotifyError::Fatal(StatusCode::UNAUTHORIZED)),
            reqwest::StatusCode::FORBIDDEN => Err(NotifyError::Fatal(StatusCode::FORBIDDEN)),
            status => Err(NotifyError::Unexpected(
                hyper::StatusCode::from_u16(status.as_u16())
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            )),
        }
    }

    async fn do_notify(
        &self,
        url: &String,
        reconfigure_request: &ComputeHookNotifyRequest,
        cancel: &CancellationToken,
    ) -> Result<(), NotifyError> {
        // We hold these semaphore units across all retries, rather than only across each
        // HTTP request: this is to preserve fairness and avoid a situation where a retry might
        // time out waiting for a semaphore.
        let _units = self
            .api_concurrency
            .acquire()
            .await
            // Interpret closed semaphore as shutdown
            .map_err(|_| NotifyError::ShuttingDown)?;

        backoff::retry(
            || self.do_notify_iteration(url, reconfigure_request, cancel),
            |e| {
                matches!(
                    e,
                    NotifyError::Fatal(_) | NotifyError::Unexpected(_) | NotifyError::Busy
                )
            },
            3,
            10,
            "Send compute notification",
            cancel,
        )
        .await
        .ok_or_else(|| NotifyError::ShuttingDown)
        .and_then(|x| x)
    }

    /// Synchronous phase: update the per-tenant state for the next intended notification
    fn notify_prepare(&self, shard_update: ShardUpdate) -> MaybeSendResult {
        let mut state_locked = self.state.lock().unwrap();

        use std::collections::hash_map::Entry;
        let tenant_shard_id = shard_update.tenant_shard_id;

        let tenant = match state_locked.entry(tenant_shard_id.tenant_id) {
            Entry::Vacant(e) => {
                let ShardUpdate {
                    tenant_shard_id,
                    node_id,
                    stripe_size,
                    preferred_az,
                } = shard_update;
                e.insert(ComputeHookTenant::new(
                    tenant_shard_id,
                    stripe_size,
                    preferred_az.map(|az| az.into_owned()),
                    node_id,
                ))
            }
            Entry::Occupied(e) => {
                let tenant = e.into_mut();
                tenant.update(shard_update);
                tenant
            }
        };
        tenant.maybe_send(tenant_shard_id.tenant_id, None)
    }

    async fn notify_execute(
        &self,
        maybe_send_result: MaybeSendResult,
        tenant_shard_id: TenantShardId,
        cancel: &CancellationToken,
    ) -> Result<(), NotifyError> {
        // Process result: we may get an update to send, or we may have to wait for a lock
        // before trying again.
        let (request, mut send_lock_guard) = match maybe_send_result {
            MaybeSendResult::Noop => {
                return Ok(());
            }
            MaybeSendResult::AwaitLock(send_lock) => {
                let send_locked = tokio::select! {
                    guard = send_lock.lock_owned() => {guard},
                    _ = cancel.cancelled() => {
                        return Err(NotifyError::ShuttingDown)
                    }
                };

                // Lock order: maybe_send is called within the `[Self::state]` lock, and takes the send lock, but here
                // we have acquired the send lock and take `[Self::state]` lock.  This is safe because maybe_send only uses
                // try_lock.
                let state_locked = self.state.lock().unwrap();
                let Some(tenant) = state_locked.get(&tenant_shard_id.tenant_id) else {
                    return Ok(());
                };
                match tenant.maybe_send(tenant_shard_id.tenant_id, Some(send_locked)) {
                    MaybeSendResult::AwaitLock(_) => {
                        unreachable!("We supplied lock guard")
                    }
                    MaybeSendResult::Noop => {
                        return Ok(());
                    }
                    MaybeSendResult::Transmit((request, lock)) => (request, lock),
                }
            }
            MaybeSendResult::Transmit((request, lock)) => (request, lock),
        };

        let result = if let Some(notify_url) = &self.config.compute_hook_url {
            self.do_notify(notify_url, &request, cancel).await
        } else {
            self.do_notify_local(&request).await.map_err(|e| {
                // This path is for testing only, so munge the error into our prod-style error type.
                tracing::error!("neon_local notification hook failed: {e}");
                NotifyError::Fatal(StatusCode::INTERNAL_SERVER_ERROR)
            })
        };

        match result {
            Ok(_) => {
                // Before dropping the send lock, stash the request we just sent so that
                // subsequent callers can avoid redundantly re-sending the same thing.
                *send_lock_guard = Some(ComputeRemoteState {
                    request,
                    applied: true,
                });
            }
            Err(NotifyError::Busy) => {
                // Busy result means that the server responded and has stored the new configuration,
                // but was not able to fully apply it to the compute
                *send_lock_guard = Some(ComputeRemoteState {
                    request,
                    applied: false,
                });
            }
            Err(_) => {
                // General error case: we can no longer know the remote state, so clear it.  This will result in
                // the logic in maybe_send recognizing that we should call the hook again.
                *send_lock_guard = None;
            }
        }
        result
    }

    /// Infallible synchronous fire-and-forget version of notify(), that sends its results to
    /// a channel.  Something should consume the channel and arrange to try notifying again
    /// if something failed.
    pub(super) fn notify_background(
        self: &Arc<Self>,
        notifications: Vec<ShardUpdate>,
        result_tx: tokio::sync::mpsc::Sender<Result<(), (TenantShardId, NotifyError)>>,
        cancel: &CancellationToken,
    ) {
        let mut maybe_sends = Vec::new();
        for shard_update in notifications {
            let tenant_shard_id = shard_update.tenant_shard_id;
            let maybe_send_result = self.notify_prepare(shard_update);
            maybe_sends.push((tenant_shard_id, maybe_send_result))
        }

        let this = self.clone();
        let cancel = cancel.clone();

        tokio::task::spawn(async move {
            // Construct an async stream of futures to invoke the compute notify function: we do this
            // in order to subsequently use .buffered() on the stream to execute with bounded parallelism.  The
            // ComputeHook semaphore already limits concurrency, but this way we avoid constructing+polling lots of futures which
            // would mostly just be waiting on that semaphore.
            let mut stream = futures::stream::iter(maybe_sends)
                .map(|(tenant_shard_id, maybe_send_result)| {
                    let this = this.clone();
                    let cancel = cancel.clone();

                    async move {
                        this
                            .notify_execute(maybe_send_result, tenant_shard_id, &cancel)
                            .await.map_err(|e| (tenant_shard_id, e))
                    }.instrument(info_span!(
                        "notify_background", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug()
                    ))
                })
                .buffered(API_CONCURRENCY);

            loop {
                tokio::select! {
                    next = stream.next() => {
                        match next {
                            Some(r) => {
                                result_tx.send(r).await.ok();
                            },
                            None => {
                                tracing::info!("Finished sending background compute notifications");
                                break;
                            }
                        }
                    },
                    _ = cancel.cancelled() => {
                        tracing::info!("Shutdown while running background compute notifications");
                        break;
                    }
                };
            }
        });
    }

    /// Call this to notify the compute (postgres) tier of new pageservers to use
    /// for a tenant.  notify() is called by each shard individually, and this function
    /// will decide whether an update to the tenant is sent.  An update is sent on the
    /// condition that:
    /// - We know a pageserver for every shard.
    /// - All the shards have the same shard_count (i.e. we are not mid-split)
    ///
    /// Cancellation token enables callers to drop out, e.g. if calling from a Reconciler
    /// that is cancelled.
    ///
    /// This function is fallible, including in the case that the control plane is transiently
    /// unavailable.  A limited number of retries are done internally to efficiently hide short unavailability
    /// periods, but we don't retry forever.  The **caller** is responsible for handling failures and
    /// ensuring that they eventually call again to ensure that the compute is eventually notified of
    /// the proper pageserver nodes for a tenant.
    #[tracing::instrument(skip_all, fields(tenant_id=%shard_update.tenant_shard_id.tenant_id, shard_id=%shard_update.tenant_shard_id.shard_slug(), node_id))]
    pub(super) async fn notify<'a>(
        &self,
        shard_update: ShardUpdate<'a>,
        cancel: &CancellationToken,
    ) -> Result<(), NotifyError> {
        let tenant_shard_id = shard_update.tenant_shard_id;
        let maybe_send_result = self.notify_prepare(shard_update);
        self.notify_execute(maybe_send_result, tenant_shard_id, cancel)
            .await
    }

    /// Reflect a detach for a particular shard in the compute hook state.
    ///
    /// The goal is to avoid sending compute notifications with stale information (i.e.
    /// including detach pageservers).
    #[tracing::instrument(skip_all, fields(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug()))]
    pub(super) fn handle_detach(
        &self,
        tenant_shard_id: TenantShardId,
        stripe_size: ShardStripeSize,
    ) {
        use std::collections::hash_map::Entry;

        let mut state_locked = self.state.lock().unwrap();
        match state_locked.entry(tenant_shard_id.tenant_id) {
            Entry::Vacant(_) => {
                // This is a valid but niche case, where the tenant was previously attached
                // as a Secondary location and then detached, so has no previously notified
                // state.
                tracing::info!("Compute hook tenant not found for detach");
            }
            Entry::Occupied(mut e) => {
                let sharded = e.get().is_sharded();
                if !sharded {
                    e.remove();
                } else {
                    e.get_mut().remove_shard(tenant_shard_id, stripe_size);
                }

                tracing::debug!("Compute hook handled shard detach");
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use pageserver_api::shard::{ShardCount, ShardNumber};
    use utils::id::TenantId;

    use super::*;

    #[test]
    fn tenant_updates() -> anyhow::Result<()> {
        let tenant_id = TenantId::generate();
        let mut tenant_state = ComputeHookTenant::new(
            TenantShardId {
                tenant_id,
                shard_count: ShardCount::new(0),
                shard_number: ShardNumber(0),
            },
            ShardStripeSize(12345),
            None,
            NodeId(1),
        );

        // An unsharded tenant is always ready to emit a notification, but won't
        // send the same one twice
        let send_result = tenant_state.maybe_send(tenant_id, None);
        let MaybeSendResult::Transmit((request, mut guard)) = send_result else {
            anyhow::bail!("Wrong send result");
        };
        assert_eq!(request.shards.len(), 1);
        assert!(request.stripe_size.is_none());

        // Simulate successful send
        *guard = Some(ComputeRemoteState {
            request,
            applied: true,
        });
        drop(guard);

        // Try asking again: this should be a no-op
        let send_result = tenant_state.maybe_send(tenant_id, None);
        assert!(matches!(send_result, MaybeSendResult::Noop));

        // Writing the first shard of a multi-sharded situation (i.e. in a split)
        // resets the tenant state and puts it in an non-notifying state (need to
        // see all shards)
        tenant_state.update(ShardUpdate {
            tenant_shard_id: TenantShardId {
                tenant_id,
                shard_count: ShardCount::new(2),
                shard_number: ShardNumber(1),
            },
            stripe_size: ShardStripeSize(32768),
            preferred_az: None,
            node_id: NodeId(1),
        });
        assert!(matches!(
            tenant_state.maybe_send(tenant_id, None),
            MaybeSendResult::Noop
        ));

        // Writing the second shard makes it ready to notify
        tenant_state.update(ShardUpdate {
            tenant_shard_id: TenantShardId {
                tenant_id,
                shard_count: ShardCount::new(2),
                shard_number: ShardNumber(0),
            },
            stripe_size: ShardStripeSize(32768),
            preferred_az: None,
            node_id: NodeId(1),
        });

        let send_result = tenant_state.maybe_send(tenant_id, None);
        let MaybeSendResult::Transmit((request, mut guard)) = send_result else {
            anyhow::bail!("Wrong send result");
        };
        assert_eq!(request.shards.len(), 2);
        assert_eq!(request.stripe_size, Some(ShardStripeSize(32768)));

        // Simulate successful send
        *guard = Some(ComputeRemoteState {
            request,
            applied: true,
        });
        drop(guard);

        Ok(())
    }
}
