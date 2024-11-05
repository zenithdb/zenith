use async_trait::async_trait;
use pq_proto::StartupMessageParams;
use tokio::time;
use tracing::{debug, info, warn};

use super::retry::ShouldRetryWakeCompute;
use crate::auth::backend::ComputeCredentialKeys;
use crate::compute::{self, PostgresConnection, COULD_NOT_CONNECT};
use crate::config::RetryConfig;
use crate::context::RequestMonitoring;
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

const CONNECT_TIMEOUT: time::Duration = time::Duration::from_secs(2);

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
        ctx: &RequestMonitoring,
        node_info: &control_plane::CachedNodeInfo,
        timeout: time::Duration,
    ) -> Result<Self::Connection, Self::ConnectError>;

    fn update_connect_config(&self, conf: &mut compute::ConnCfg);
}

#[async_trait]
pub(crate) trait ComputeConnectBackend: Send + Sync + 'static {
    async fn wake_compute(
        &self,
        ctx: &RequestMonitoring,
    ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError>;

    fn get_keys(&self) -> &ComputeCredentialKeys;
}

pub(crate) struct TcpMechanism<'a> {
    /// KV-dictionary with PostgreSQL connection params.
    pub(crate) params: &'a StartupMessageParams,

    /// connect_to_compute concurrency lock
    pub(crate) locks: &'static ApiLocks<Host>,
}

#[async_trait]
impl ConnectMechanism for TcpMechanism<'_> {
    type Connection = PostgresConnection;
    type ConnectError = compute::ConnectionError;
    type Error = compute::ConnectionError;

    #[tracing::instrument(fields(pid = tracing::field::Empty), skip_all)]
    async fn connect_once(
        &self,
        ctx: &RequestMonitoring,
        node_info: &control_plane::CachedNodeInfo,
        timeout: time::Duration,
    ) -> Result<PostgresConnection, Self::Error> {
        let host = node_info.config.get_host()?;
        let permit = self.locks.get_permit(&host).await?;
        permit.release_result(node_info.connect(ctx, timeout).await)
    }

    fn update_connect_config(&self, config: &mut compute::ConnCfg) {
        config.set_startup_params(self.params);
    }
}

/// Try to connect to the compute node, retrying if necessary.
#[tracing::instrument(skip_all)]
pub(crate) async fn connect_to_compute<M: ConnectMechanism>(
    ctx: &RequestMonitoring,
    mechanism: &M,
    user_info: &dyn ComputeConnectBackend,
    allow_self_signed_compute: bool,
    wake_compute_retry_config: RetryConfig,
    connect_to_compute_retry_config: RetryConfig,
) -> Result<M::Connection, M::Error>
where
    M::ConnectError: CouldRetry + ShouldRetryWakeCompute + std::fmt::Debug,
    M::Error: From<WakeComputeError>,
{
    let mut num_retries = 0;
    let mut node_info =
        wake_compute(&mut num_retries, ctx, user_info, wake_compute_retry_config).await?;

    node_info.set_keys(user_info.get_keys());
    node_info.allow_self_signed_compute = allow_self_signed_compute;
    mechanism.update_connect_config(&mut node_info.config);
    let retry_type = RetryType::ConnectToCompute;

    // try once
    let err = match mechanism
        .connect_once(ctx, &node_info, CONNECT_TIMEOUT)
        .await
    {
        Ok(res) => {
            ctx.success();
            Metrics::get().proxy.retries_metric.observe(
                RetriesMetricGroup {
                    outcome: ConnectOutcome::Success,
                    retry_type,
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
        if should_retry(&err, num_retries, connect_to_compute_retry_config) {
            Metrics::get().proxy.retries_metric.observe(
                RetriesMetricGroup {
                    outcome: ConnectOutcome::Failed,
                    retry_type,
                },
                num_retries.into(),
            );
            return Err(err.into());
        }
        node_info
    } else {
        // if we failed to connect, it's likely that the compute node was suspended, wake a new compute node
        info!("compute node's state has likely changed; requesting a wake-up");
        let old_node_info = invalidate_cache(node_info);
        let mut node_info =
            wake_compute(&mut num_retries, ctx, user_info, wake_compute_retry_config).await?;
        node_info.reuse_settings(old_node_info);

        mechanism.update_connect_config(&mut node_info.config);
        node_info
    };

    // now that we have a new node, try connect to it repeatedly.
    // this can error for a few reasons, for instance:
    // * DNS connection settings haven't quite propagated yet
    info!("wake_compute success. attempting to connect");
    num_retries = 1;
    loop {
        match mechanism
            .connect_once(ctx, &node_info, CONNECT_TIMEOUT)
            .await
        {
            Ok(res) => {
                ctx.success();
                Metrics::get().proxy.retries_metric.observe(
                    RetriesMetricGroup {
                        outcome: ConnectOutcome::Success,
                        retry_type,
                    },
                    num_retries.into(),
                );
                info!(?num_retries, "connected to compute node after");
                return Ok(res);
            }
            Err(e) => {
                if !should_retry(&e, num_retries, connect_to_compute_retry_config) {
                    // Don't log an error here, caller will print the error
                    Metrics::get().proxy.retries_metric.observe(
                        RetriesMetricGroup {
                            outcome: ConnectOutcome::Failed,
                            retry_type,
                        },
                        num_retries.into(),
                    );
                    return Err(e.into());
                }

                warn!(error = ?e, num_retries, retriable = true, COULD_NOT_CONNECT);
            }
        };

        let wait_duration = retry_after(num_retries, connect_to_compute_retry_config);
        num_retries += 1;

        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::RetryTimeout);
        time::sleep(wait_duration).await;
        drop(pause);
    }
}
