use async_trait::async_trait;
use pq_proto::StartupMessageParams;
use tokio::time;
use tracing::{debug, info, warn};

use super::retry::ShouldRetryWakeCompute;
use crate::auth::backend::{ComputeCredentialKeys, ComputeUserInfo};
use crate::compute::{self, PostgresConnection, COULD_NOT_CONNECT};
use crate::config::{ComputeConfig, RetryConfig};
use crate::context::RequestContext;
use crate::control_plane::errors::WakeComputeError;
use crate::control_plane::locks::ApiLocks;
use crate::control_plane::{self, CachedNodeInfo, NodeInfo};
use crate::error::ReportableError;
use crate::metrics::{
    ConnectOutcome, ConnectionFailureKind, Metrics, RetriesMetricGroup, RetryType,
};
use crate::proxy::retry::{retry_after, should_retry, CouldRetry};
use crate::proxy::wake_compute::wake_compute;
use crate::types::Host;

/// If we couldn't connect, a cached connection info might be to blame
/// (e.g. the compute node's address might've changed at the wrong time).
/// Invalidate the cache entry (if any) to prevent subsequent errors.
#[tracing::instrument(name = "invalidate_cache", skip_all)]
pub(crate) fn invalidate_cache(node_info: control_plane::CachedNodeInfo) -> NodeInfo {
    let is_cached = node_info.cached();
    if is_cached {
        warn!("invalidating stalled compute node info cache entry");
    }
    let label = if is_cached {
        ConnectionFailureKind::ComputeCached
    } else {
        ConnectionFailureKind::ComputeUncached
    };
    Metrics::get().proxy.connection_failures_total.inc(label);

    node_info.invalidate()
}

#[async_trait]
pub(crate) trait ConnectMechanism {
    type Connection;
    type ConnectError: ReportableError;
    type Error: From<Self::ConnectError>;
    async fn connect_once(
        &self,
        ctx: &RequestContext,
        node_info: &control_plane::CachedNodeInfo,
        config: &ComputeConfig,
    ) -> Result<Self::Connection, Self::ConnectError>;

    fn update_connect_config(&self, conf: &mut compute::ConnCfg);
}

#[async_trait]
pub(crate) trait ComputeConnectBackend {
    async fn wake_compute(
        &self,
        ctx: &RequestContext,
    ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError>;

    fn get_keys(&self) -> &ComputeCredentialKeys;
}

pub(crate) struct TcpMechanism<'a> {
    pub(crate) params_compat: bool,

    /// KV-dictionary with PostgreSQL connection params.
    pub(crate) params: &'a StartupMessageParams,

    /// connect_to_compute concurrency lock
    pub(crate) locks: &'static ApiLocks<Host>,

    pub(crate) user_info: ComputeUserInfo,
}

#[async_trait]
impl ConnectMechanism for TcpMechanism<'_> {
    type Connection = PostgresConnection;
    type ConnectError = compute::ConnectionError;
    type Error = compute::ConnectionError;

    #[tracing::instrument(fields(pid = tracing::field::Empty), skip_all)]
    async fn connect_once(
        &self,
        ctx: &RequestContext,
        node_info: &control_plane::CachedNodeInfo,
        config: &ComputeConfig,
    ) -> Result<PostgresConnection, Self::Error> {
        let host = node_info.config.get_host();
        let permit = self.locks.get_permit(&host).await?;
        permit.release_result(node_info.connect(ctx, config, self.user_info.clone()).await)
    }

    fn update_connect_config(&self, config: &mut compute::ConnCfg) {
        config.set_startup_params(self.params, self.params_compat);
    }
}

/// Try to connect to the compute node, retrying if necessary.
#[tracing::instrument(skip_all)]
pub(crate) async fn connect_to_compute<M: ConnectMechanism, B: ComputeConnectBackend>(
    ctx: &RequestContext,
    mechanism: &M,
    user_info: &B,
    wake_compute_retry_config: RetryConfig,
    compute: &ComputeConfig,
) -> Result<M::Connection, M::Error>
where
    M::ConnectError: CouldRetry + ShouldRetryWakeCompute + std::fmt::Debug,
    M::Error: From<WakeComputeError>,
{
    let mut num_retries = 0;
    let mut node_info =
        wake_compute(&mut num_retries, ctx, user_info, wake_compute_retry_config).await?;

    node_info.set_keys(user_info.get_keys());
    mechanism.update_connect_config(&mut node_info.config);

    // try once
    let err = match mechanism.connect_once(ctx, &node_info, compute).await {
        Ok(res) => {
            ctx.success();
            Metrics::get().proxy.retries_metric.observe(
                RetriesMetricGroup {
                    outcome: ConnectOutcome::Success,
                    retry_type: RetryType::ConnectToCompute,
                },
                num_retries.into(),
            );
            return Ok(res);
        }
        Err(e) => e,
    };

    debug!(error = ?err, COULD_NOT_CONNECT);

    let node_info = if !node_info.cached() || !err.should_retry_wake_compute() {
        // If we just recieved this from cplane and didn't get it from cache, we shouldn't retry.
        // Do not need to retrieve a new node_info, just return the old one.
        if should_retry(&err, num_retries, compute.retry) {
            Metrics::get().proxy.retries_metric.observe(
                RetriesMetricGroup {
                    outcome: ConnectOutcome::Failed,
                    retry_type: RetryType::ConnectToCompute,
                },
                num_retries.into(),
            );
            return Err(err.into());
        }
        node_info
    } else {
        // if we failed to connect, it's likely that the compute node was suspended, wake a new compute node
        debug!("compute node's state has likely changed; requesting a wake-up");
        let old_node_info = invalidate_cache(node_info);
        // TODO: increment num_retries?
        let mut node_info =
            wake_compute(&mut num_retries, ctx, user_info, wake_compute_retry_config).await?;
        node_info.reuse_settings(old_node_info);

        mechanism.update_connect_config(&mut node_info.config);
        node_info
    };

    // now that we have a new node, try connect to it repeatedly.
    // this can error for a few reasons, for instance:
    // * DNS connection settings haven't quite propagated yet
    debug!("wake_compute success. attempting to connect");
    num_retries = 1;
    loop {
        match mechanism.connect_once(ctx, &node_info, compute).await {
            Ok(res) => {
                ctx.success();
                Metrics::get().proxy.retries_metric.observe(
                    RetriesMetricGroup {
                        outcome: ConnectOutcome::Success,
                        retry_type: RetryType::ConnectToCompute,
                    },
                    num_retries.into(),
                );
                // TODO: is this necessary? We have a metric.
                info!(?num_retries, "connected to compute node after");
                return Ok(res);
            }
            Err(e) => {
                if !should_retry(&e, num_retries, compute.retry) {
                    // Don't log an error here, caller will print the error
                    Metrics::get().proxy.retries_metric.observe(
                        RetriesMetricGroup {
                            outcome: ConnectOutcome::Failed,
                            retry_type: RetryType::ConnectToCompute,
                        },
                        num_retries.into(),
                    );
                    return Err(e.into());
                }

                warn!(error = ?e, num_retries, retriable = true, COULD_NOT_CONNECT);
            }
        };

        let wait_duration = retry_after(num_retries, compute.retry);
        num_retries += 1;

        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::RetryTimeout);
        time::sleep(wait_duration).await;
        drop(pause);
    }
}
