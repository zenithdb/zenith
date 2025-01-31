use pageserver_api::{
    models::{
        detach_ancestor::AncestorDetached, LocationConfig, LocationConfigListResponse,
        PageserverUtilization, SecondaryProgress, TenantScanRemoteStorageResponse,
        TenantShardSplitRequest, TenantShardSplitResponse, TenantWaitLsnRequest,
        TimelineArchivalConfigRequest, TimelineCreateRequest, TimelineInfo, TopTenantShardsRequest,
        TopTenantShardsResponse,
    },
    shard::TenantShardId,
};
use pageserver_client::{
    mgmt_api::{Client, Result},
    BlockUnblock,
};
use reqwest::StatusCode;
use utils::id::{NodeId, TenantId, TimelineId};

/// Thin wrapper around [`pageserver_client::mgmt_api::Client`]. It allows the storage
/// controller to collect metrics in a non-intrusive manner.
#[derive(Debug, Clone)]
pub(crate) struct PageserverClient {
    inner: Client,
    node_id_label: String,
}

macro_rules! measured_request {
    ($name:literal, $method:expr, $node_id: expr, $invoke:expr) => {{
        let labels = crate::metrics::PageserverRequestLabelGroup {
            pageserver_id: $node_id,
            path: $name,
            method: $method,
        };

        let latency = &crate::metrics::METRICS_REGISTRY
            .metrics_group
            .storage_controller_pageserver_request_latency;
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

impl PageserverClient {
    pub(crate) fn new(node_id: NodeId, mgmt_api_endpoint: String, jwt: Option<&str>) -> Self {
        Self {
            inner: Client::from_client(reqwest::Client::new(), mgmt_api_endpoint, jwt),
            node_id_label: node_id.0.to_string(),
        }
    }

    pub(crate) fn from_client(
        node_id: NodeId,
        raw_client: reqwest::Client,
        mgmt_api_endpoint: String,
        jwt: Option<&str>,
    ) -> Self {
        Self {
            inner: Client::from_client(raw_client, mgmt_api_endpoint, jwt),
            node_id_label: node_id.0.to_string(),
        }
    }

    pub(crate) async fn tenant_delete(&self, tenant_shard_id: TenantShardId) -> Result<StatusCode> {
        measured_request!(
            "tenant",
            crate::metrics::Method::Delete,
            &self.node_id_label,
            self.inner.tenant_delete(tenant_shard_id).await
        )
    }

    pub(crate) async fn tenant_time_travel_remote_storage(
        &self,
        tenant_shard_id: TenantShardId,
        timestamp: &str,
        done_if_after: &str,
    ) -> Result<()> {
        measured_request!(
            "tenant_time_travel_remote_storage",
            crate::metrics::Method::Put,
            &self.node_id_label,
            self.inner
                .tenant_time_travel_remote_storage(tenant_shard_id, timestamp, done_if_after)
                .await
        )
    }

    pub(crate) async fn tenant_scan_remote_storage(
        &self,
        tenant_id: TenantId,
    ) -> Result<TenantScanRemoteStorageResponse> {
        measured_request!(
            "tenant_scan_remote_storage",
            crate::metrics::Method::Get,
            &self.node_id_label,
            self.inner.tenant_scan_remote_storage(tenant_id).await
        )
    }

    pub(crate) async fn tenant_secondary_download(
        &self,
        tenant_id: TenantShardId,
        wait: Option<std::time::Duration>,
    ) -> Result<(StatusCode, SecondaryProgress)> {
        measured_request!(
            "tenant_secondary_download",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner.tenant_secondary_download(tenant_id, wait).await
        )
    }

    pub(crate) async fn tenant_secondary_status(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<SecondaryProgress> {
        measured_request!(
            "tenant_secondary_status",
            crate::metrics::Method::Get,
            &self.node_id_label,
            self.inner.tenant_secondary_status(tenant_shard_id).await
        )
    }

    pub(crate) async fn tenant_heatmap_upload(&self, tenant_id: TenantShardId) -> Result<()> {
        measured_request!(
            "tenant_heatmap_upload",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner.tenant_heatmap_upload(tenant_id).await
        )
    }

    pub(crate) async fn location_config(
        &self,
        tenant_shard_id: TenantShardId,
        config: LocationConfig,
        flush_ms: Option<std::time::Duration>,
        lazy: bool,
    ) -> Result<()> {
        measured_request!(
            "location_config",
            crate::metrics::Method::Put,
            &self.node_id_label,
            self.inner
                .location_config(tenant_shard_id, config, flush_ms, lazy)
                .await
        )
    }

    pub(crate) async fn list_location_config(&self) -> Result<LocationConfigListResponse> {
        measured_request!(
            "location_configs",
            crate::metrics::Method::Get,
            &self.node_id_label,
            self.inner.list_location_config().await
        )
    }

    pub(crate) async fn get_location_config(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<Option<LocationConfig>> {
        measured_request!(
            "location_config",
            crate::metrics::Method::Get,
            &self.node_id_label,
            self.inner.get_location_config(tenant_shard_id).await
        )
    }

    pub(crate) async fn timeline_create(
        &self,
        tenant_shard_id: TenantShardId,
        req: &TimelineCreateRequest,
    ) -> Result<TimelineInfo> {
        measured_request!(
            "timeline",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner.timeline_create(tenant_shard_id, req).await
        )
    }

    pub(crate) async fn timeline_delete(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> Result<StatusCode> {
        measured_request!(
            "timeline",
            crate::metrics::Method::Delete,
            &self.node_id_label,
            self.inner
                .timeline_delete(tenant_shard_id, timeline_id)
                .await
        )
    }

    pub(crate) async fn tenant_shard_split(
        &self,
        tenant_shard_id: TenantShardId,
        req: TenantShardSplitRequest,
    ) -> Result<TenantShardSplitResponse> {
        measured_request!(
            "tenant_shard_split",
            crate::metrics::Method::Put,
            &self.node_id_label,
            self.inner.tenant_shard_split(tenant_shard_id, req).await
        )
    }

    pub(crate) async fn timeline_list(
        &self,
        tenant_shard_id: &TenantShardId,
    ) -> Result<Vec<TimelineInfo>> {
        measured_request!(
            "timelines",
            crate::metrics::Method::Get,
            &self.node_id_label,
            self.inner.timeline_list(tenant_shard_id).await
        )
    }

    pub(crate) async fn timeline_archival_config(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        req: &TimelineArchivalConfigRequest,
    ) -> Result<()> {
        measured_request!(
            "timeline_archival_config",
            crate::metrics::Method::Put,
            &self.node_id_label,
            self.inner
                .timeline_archival_config(tenant_shard_id, timeline_id, req)
                .await
        )
    }

    pub(crate) async fn timeline_detach_ancestor(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> Result<AncestorDetached> {
        measured_request!(
            "timeline_detach_ancestor",
            crate::metrics::Method::Put,
            &self.node_id_label,
            self.inner
                .timeline_detach_ancestor(tenant_shard_id, timeline_id)
                .await
        )
    }

    pub(crate) async fn timeline_block_unblock_gc(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        dir: BlockUnblock,
    ) -> Result<()> {
        // measuring these makes no sense because we synchronize with the gc loop and remote
        // storage on block_gc so there should be huge outliers
        measured_request!(
            "timeline_block_unblock_gc",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner
                .timeline_block_unblock_gc(tenant_shard_id, timeline_id, dir)
                .await
        )
    }

    pub(crate) async fn get_utilization(&self) -> Result<PageserverUtilization> {
        measured_request!(
            "utilization",
            crate::metrics::Method::Get,
            &self.node_id_label,
            self.inner.get_utilization().await
        )
    }

    pub(crate) async fn top_tenant_shards(
        &self,
        request: TopTenantShardsRequest,
    ) -> Result<TopTenantShardsResponse> {
        measured_request!(
            "top_tenants",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner.top_tenant_shards(request).await
        )
    }

    pub(crate) async fn wait_lsn(
        &self,
        tenant_shard_id: TenantShardId,
        request: TenantWaitLsnRequest,
    ) -> Result<StatusCode> {
        measured_request!(
            "wait_lsn",
            crate::metrics::Method::Post,
            &self.node_id_label,
            self.inner.wait_lsn(tenant_shard_id, request).await
        )
    }
}
