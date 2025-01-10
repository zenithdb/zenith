use tokio::io::{AsyncRead, AsyncWrite};
use tracing::debug;
use utils::measured_stream::MeasuredStream;

use super::copy_bidirectional::ErrorSource;
use crate::cancellation;
use crate::compute::PostgresConnection;
use crate::config::ComputeConfig;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::metrics::{Direction, Metrics, NumClientConnectionsGuard, NumConnectionRequestsGuard};
use crate::stream::Stream;
use crate::usage_metrics::{Ids, MetricCounterRecorder, USAGE_METRICS};

/// Forward bytes in both directions (client <-> compute).
#[tracing::instrument(skip_all)]
pub(crate) async fn proxy_pass(
    client: impl AsyncRead + AsyncWrite + Unpin,
    compute: impl AsyncRead + AsyncWrite + Unpin,
    aux: MetricsAuxInfo,
) -> Result<(), ErrorSource> {
    let usage = USAGE_METRICS.register(Ids {
        endpoint_id: aux.endpoint_id,
        branch_id: aux.branch_id,
    });

    let metrics = &Metrics::get().proxy.io_bytes;
    let m_sent = metrics.with_labels(Direction::Tx);
    let mut client = MeasuredStream::new(
        client,
        |_| {},
        |cnt| {
            // Number of bytes we sent to the client (outbound).
            metrics.get_metric(m_sent).inc_by(cnt as u64);
            usage.record_egress(cnt as u64);
        },
    );

    let m_recv = metrics.with_labels(Direction::Rx);
    let mut compute = MeasuredStream::new(
        compute,
        |_| {},
        |cnt| {
            // Number of bytes the client sent to the compute node (inbound).
            metrics.get_metric(m_recv).inc_by(cnt as u64);
        },
    );

    // Starting from here we only proxy the client's traffic.
    debug!("performing the proxy pass...");
    let _ = crate::proxy::copy_bidirectional::copy_bidirectional_client_compute(
        &mut client,
        &mut compute,
    )
    .await?;

    Ok(())
}

pub(crate) struct ProxyPassthrough<P, S> {
    pub(crate) client: Stream<S>,
    pub(crate) compute: PostgresConnection,
    pub(crate) aux: MetricsAuxInfo,
    pub(crate) session_id: uuid::Uuid,

    pub(crate) _req: NumConnectionRequestsGuard<'static>,
    pub(crate) _conn: NumClientConnectionsGuard<'static>,
    pub(crate) _cancel: cancellation::Session<P>,
}

impl<P, S: AsyncRead + AsyncWrite + Unpin> ProxyPassthrough<P, S> {
    pub(crate) async fn proxy_pass(
        self,
        compute_config: &ComputeConfig,
    ) -> Result<(), ErrorSource> {
        let res = proxy_pass(self.client, self.compute.stream, self.aux).await;
        if let Err(err) = self
            .compute
            .cancel_closure
            .try_cancel_query(compute_config)
            .await
        {
            tracing::warn!(session_id = ?self.session_id, ?err, "could not cancel the query in the database");
        }
        res
    }
}
