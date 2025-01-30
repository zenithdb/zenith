use crate::metrics::PageserverRequestLabelGroup;
use safekeeper_api::models::{TimelineCreateRequest, TimelineStatus};
use safekeeper_client::mgmt_api::{Client, Result};
use utils::{
    id::{NodeId, TenantId, TimelineId},
    logging::SecretString,
};

/// Thin wrapper around [`safekeeper_client::mgmt_api::Client`]. It allows the storage
/// controller to collect metrics in a non-intrusive manner.
///
/// Analogous to [`crate::pageserver_client::PageserverClient`].
#[derive(Debug, Clone)]
pub(crate) struct SafekeeperClient {
    inner: Client,
    node_id_label: String,
}

macro_rules! measured_request {
    ($name:literal, $method:expr, $node_id: expr, $invoke:expr) => {{
        let labels = PageserverRequestLabelGroup {
            pageserver_id: $node_id,
            path: $name,
            method: $method,
        };

        let latency = &crate::metrics::METRICS_REGISTRY
            .metrics_group
            .storage_controller_safekeeper_request_latency;
        let _timer_guard = latency.start_timer(labels.clone());

        let res = $invoke;

        if res.is_err() {
            let error_counters = &crate::metrics::METRICS_REGISTRY
                .metrics_group
                .storage_controller_pageserver_request_error;
            error_counters.inc(labels)
        }

        res
    }};
}

impl SafekeeperClient {
    pub(crate) fn new(
        node_id: NodeId,
        mgmt_api_endpoint: String,
        jwt: Option<SecretString>,
    ) -> Self {
        Self {
            inner: Client::from_client(reqwest::Client::new(), mgmt_api_endpoint, jwt),
            node_id_label: node_id.0.to_string(),
        }
    }

    #[allow(unused)]
    pub(crate) fn from_client(
        node_id: NodeId,
        raw_client: reqwest::Client,
        mgmt_api_endpoint: String,
        jwt: Option<SecretString>,
    ) -> Self {
        Self {
            inner: Client::from_client(raw_client, mgmt_api_endpoint, jwt),
            node_id_label: node_id.0.to_string(),
        }
    }

    pub(crate) async fn create_timeline(
        &self,
        req: &TimelineCreateRequest,
    ) -> Result<TimelineStatus> {
        measured_request!(
            "create_timeline",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner.create_timeline(req).await
        )
    }

    pub(crate) async fn delete_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<TimelineStatus> {
        measured_request!(
            "delete_timeline",
            crate::metrics::Method::Delete,
            &self.node_id_label,
            self.inner.delete_timeline(tenant_id, timeline_id).await
        )
    }
}
