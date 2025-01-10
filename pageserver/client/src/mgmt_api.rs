use std::{collections::HashMap, error::Error as _};

use bytes::Bytes;
use detach_ancestor::AncestorDetached;
use pageserver_api::{models::*, shard::TenantShardId};
use reqwest::{IntoUrl, Method, StatusCode};
use utils::{
    http::error::HttpErrorBody,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

pub use reqwest::Body as ReqwestBody;

use crate::BlockUnblock;

pub mod util;

#[derive(Debug, Clone)]
pub struct Client {
    mgmt_api_endpoint: String,
    authorization_header: Option<String>,
    client: reqwest::Client,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("send request: {0}{}", .0.source().map(|e| format!(": {e}")).unwrap_or_default())]
    SendRequest(reqwest::Error),

    #[error("receive body: {0}{}", .0.source().map(|e| format!(": {e}")).unwrap_or_default())]
    ReceiveBody(reqwest::Error),

    #[error("receive error body: {0}")]
    ReceiveErrorBody(String),

    #[error("pageserver API: {1}")]
    ApiError(StatusCode, String),

    #[error("Cancelled")]
    Cancelled,
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait ResponseErrorMessageExt: Sized {
    fn error_from_body(self) -> impl std::future::Future<Output = Result<Self>> + Send;
}

impl ResponseErrorMessageExt for reqwest::Response {
    async fn error_from_body(self) -> Result<Self> {
        let status = self.status();
        if !(status.is_client_error() || status.is_server_error()) {
            return Ok(self);
        }

        let url = self.url().to_owned();
        Err(match self.json::<HttpErrorBody>().await {
            Ok(HttpErrorBody { msg }) => Error::ApiError(status, msg),
            Err(_) => {
                Error::ReceiveErrorBody(format!("Http error ({}) at {}.", status.as_u16(), url))
            }
        })
    }
}

pub enum ForceAwaitLogicalSize {
    Yes,
    No,
}

impl Client {
    pub fn new(mgmt_api_endpoint: String, jwt: Option<&str>) -> Self {
        Self::from_client(reqwest::Client::new(), mgmt_api_endpoint, jwt)
    }

    pub fn from_client(
        client: reqwest::Client,
        mgmt_api_endpoint: String,
        jwt: Option<&str>,
    ) -> Self {
        Self {
            mgmt_api_endpoint,
            authorization_header: jwt.map(|jwt| format!("Bearer {jwt}")),
            client,
        }
    }

    pub async fn list_tenants(&self) -> Result<Vec<pageserver_api::models::TenantInfo>> {
        let uri = format!("{}/v1/tenant", self.mgmt_api_endpoint);
        let resp = self.get(&uri).await?;
        resp.json().await.map_err(Error::ReceiveBody)
    }

    /// Get an arbitrary path and returning a streaming Response.  This function is suitable
    /// for pass-through/proxy use cases where we don't care what the response content looks
    /// like.
    ///
    /// Use/add one of the properly typed methods below if you know aren't proxying, and
    /// know what kind of response you expect.
    pub async fn get_raw(&self, path: String) -> Result<reqwest::Response> {
        debug_assert!(path.starts_with('/'));
        let uri = format!("{}{}", self.mgmt_api_endpoint, path);

        let req = self.client.request(Method::GET, uri);
        let req = if let Some(value) = &self.authorization_header {
            req.header(reqwest::header::AUTHORIZATION, value)
        } else {
            req
        };
        req.send().await.map_err(Error::ReceiveBody)
    }

    pub async fn tenant_details(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<pageserver_api::models::TenantDetails> {
        let uri = format!("{}/v1/tenant/{tenant_shard_id}", self.mgmt_api_endpoint);
        self.get(uri)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn list_timelines(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<Vec<pageserver_api::models::TimelineInfo>> {
        let uri = format!(
            "{}/v1/tenant/{tenant_shard_id}/timeline",
            self.mgmt_api_endpoint
        );
        self.get(&uri)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn timeline_info(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        force_await_logical_size: ForceAwaitLogicalSize,
    ) -> Result<pageserver_api::models::TimelineInfo> {
        let uri = format!(
            "{}/v1/tenant/{tenant_shard_id}/timeline/{timeline_id}",
            self.mgmt_api_endpoint
        );

        let uri = match force_await_logical_size {
            ForceAwaitLogicalSize::Yes => format!("{}?force-await-logical-size={}", uri, true),
            ForceAwaitLogicalSize::No => uri,
        };

        self.get(&uri)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn keyspace(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> Result<pageserver_api::models::partitioning::Partitioning> {
        let uri = format!(
            "{}/v1/tenant/{tenant_shard_id}/timeline/{timeline_id}/keyspace",
            self.mgmt_api_endpoint
        );
        self.get(&uri)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    async fn get<U: IntoUrl>(&self, uri: U) -> Result<reqwest::Response> {
        self.request(Method::GET, uri, ()).await
    }

    fn start_request<U: reqwest::IntoUrl>(
        &self,
        method: Method,
        uri: U,
    ) -> reqwest::RequestBuilder {
        let req = self.client.request(method, uri);
        if let Some(value) = &self.authorization_header {
            req.header(reqwest::header::AUTHORIZATION, value)
        } else {
            req
        }
    }

    async fn request_noerror<B: serde::Serialize, U: reqwest::IntoUrl>(
        &self,
        method: Method,
        uri: U,
        body: B,
    ) -> Result<reqwest::Response> {
        self.start_request(method, uri)
            .json(&body)
            .send()
            .await
            .map_err(Error::ReceiveBody)
    }

    async fn request<B: serde::Serialize, U: reqwest::IntoUrl>(
        &self,
        method: Method,
        uri: U,
        body: B,
    ) -> Result<reqwest::Response> {
        let res = self.request_noerror(method, uri, body).await?;
        let response = res.error_from_body().await?;
        Ok(response)
    }

    pub async fn status(&self) -> Result<()> {
        let uri = format!("{}/v1/status", self.mgmt_api_endpoint);
        self.get(&uri).await?;
        Ok(())
    }

    /// The tenant deletion API can return 202 if deletion is incomplete, or
    /// 404 if it is complete.  Callers are responsible for checking the status
    /// code and retrying.  Error codes other than 404 will return Err().
    pub async fn tenant_delete(&self, tenant_shard_id: TenantShardId) -> Result<StatusCode> {
        let uri = format!("{}/v1/tenant/{tenant_shard_id}", self.mgmt_api_endpoint);

        match self.request(Method::DELETE, &uri, ()).await {
            Err(Error::ApiError(status_code, msg)) => {
                if status_code == StatusCode::NOT_FOUND {
                    Ok(StatusCode::NOT_FOUND)
                } else {
                    Err(Error::ApiError(status_code, msg))
                }
            }
            Err(e) => Err(e),
            Ok(response) => Ok(response.status()),
        }
    }

    pub async fn tenant_time_travel_remote_storage(
        &self,
        tenant_shard_id: TenantShardId,
        timestamp: &str,
        done_if_after: &str,
    ) -> Result<()> {
        let uri = format!(
            "{}/v1/tenant/{tenant_shard_id}/time_travel_remote_storage?travel_to={timestamp}&done_if_after={done_if_after}",
            self.mgmt_api_endpoint
        );
        self.request(Method::PUT, &uri, ()).await?;
        Ok(())
    }

    pub async fn tenant_scan_remote_storage(
        &self,
        tenant_id: TenantId,
    ) -> Result<TenantScanRemoteStorageResponse> {
        let uri = format!(
            "{}/v1/tenant/{tenant_id}/scan_remote_storage",
            self.mgmt_api_endpoint
        );
        let response = self.request(Method::GET, &uri, ()).await?;
        let body = response.json().await.map_err(Error::ReceiveBody)?;
        Ok(body)
    }

    pub async fn set_tenant_config(&self, req: &TenantConfigRequest) -> Result<()> {
        let uri = format!("{}/v1/tenant/config", self.mgmt_api_endpoint);
        self.request(Method::PUT, &uri, req).await?;
        Ok(())
    }

    pub async fn patch_tenant_config(&self, req: &TenantConfigPatchRequest) -> Result<()> {
        let uri = format!("{}/v1/tenant/config", self.mgmt_api_endpoint);
        self.request(Method::PATCH, &uri, req).await?;
        Ok(())
    }

    pub async fn tenant_secondary_download(
        &self,
        tenant_id: TenantShardId,
        wait: Option<std::time::Duration>,
    ) -> Result<(StatusCode, SecondaryProgress)> {
        let mut path = reqwest::Url::parse(&format!(
            "{}/v1/tenant/{}/secondary/download",
            self.mgmt_api_endpoint, tenant_id
        ))
        .expect("Cannot build URL");

        if let Some(wait) = wait {
            path.query_pairs_mut()
                .append_pair("wait_ms", &format!("{}", wait.as_millis()));
        }

        let response = self.request(Method::POST, path, ()).await?;
        let status = response.status();
        let progress: SecondaryProgress = response.json().await.map_err(Error::ReceiveBody)?;
        Ok((status, progress))
    }

    pub async fn tenant_secondary_status(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<SecondaryProgress> {
        let path = reqwest::Url::parse(&format!(
            "{}/v1/tenant/{}/secondary/status",
            self.mgmt_api_endpoint, tenant_shard_id
        ))
        .expect("Cannot build URL");

        self.request(Method::GET, path, ())
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn tenant_heatmap_upload(&self, tenant_id: TenantShardId) -> Result<()> {
        let path = reqwest::Url::parse(&format!(
            "{}/v1/tenant/{}/heatmap_upload",
            self.mgmt_api_endpoint, tenant_id
        ))
        .expect("Cannot build URL");

        self.request(Method::POST, path, ()).await?;
        Ok(())
    }

    pub async fn location_config(
        &self,
        tenant_shard_id: TenantShardId,
        config: LocationConfig,
        flush_ms: Option<std::time::Duration>,
        lazy: bool,
    ) -> Result<()> {
        let req_body = TenantLocationConfigRequest { config };

        let mut path = reqwest::Url::parse(&format!(
            "{}/v1/tenant/{}/location_config",
            self.mgmt_api_endpoint, tenant_shard_id
        ))
        // Should always work: mgmt_api_endpoint is configuration, not user input.
        .expect("Cannot build URL");

        if lazy {
            path.query_pairs_mut().append_pair("lazy", "true");
        }

        if let Some(flush_ms) = flush_ms {
            path.query_pairs_mut()
                .append_pair("flush_ms", &format!("{}", flush_ms.as_millis()));
        }

        self.request(Method::PUT, path, &req_body).await?;
        Ok(())
    }

    pub async fn list_location_config(&self) -> Result<LocationConfigListResponse> {
        let path = format!("{}/v1/location_config", self.mgmt_api_endpoint);
        self.request(Method::GET, &path, ())
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn get_location_config(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<Option<LocationConfig>> {
        let path = format!(
            "{}/v1/location_config/{tenant_shard_id}",
            self.mgmt_api_endpoint
        );
        self.request(Method::GET, &path, ())
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn timeline_create(
        &self,
        tenant_shard_id: TenantShardId,
        req: &TimelineCreateRequest,
    ) -> Result<TimelineInfo> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline",
            self.mgmt_api_endpoint, tenant_shard_id
        );
        self.request(Method::POST, &uri, req)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    /// The timeline deletion API can return 201 if deletion is incomplete, or
    /// 403 if it is complete.  Callers are responsible for checking the status
    /// code and retrying.  Error codes other than 403 will return Err().
    pub async fn timeline_delete(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> Result<StatusCode> {
        let uri = format!(
            "{}/v1/tenant/{tenant_shard_id}/timeline/{timeline_id}",
            self.mgmt_api_endpoint
        );

        match self.request(Method::DELETE, &uri, ()).await {
            Err(Error::ApiError(status_code, msg)) => {
                if status_code == StatusCode::NOT_FOUND {
                    Ok(StatusCode::NOT_FOUND)
                } else {
                    Err(Error::ApiError(status_code, msg))
                }
            }
            Err(e) => Err(e),
            Ok(response) => Ok(response.status()),
        }
    }

    pub async fn timeline_archival_config(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        req: &TimelineArchivalConfigRequest,
    ) -> Result<()> {
        let uri = format!(
            "{}/v1/tenant/{tenant_shard_id}/timeline/{timeline_id}/archival_config",
            self.mgmt_api_endpoint
        );

        self.request(Method::PUT, &uri, req)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn timeline_detach_ancestor(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> Result<AncestorDetached> {
        let uri = format!(
            "{}/v1/tenant/{tenant_shard_id}/timeline/{timeline_id}/detach_ancestor",
            self.mgmt_api_endpoint
        );

        self.request(Method::PUT, &uri, ())
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn timeline_block_unblock_gc(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        dir: BlockUnblock,
    ) -> Result<()> {
        let uri = format!(
            "{}/v1/tenant/{tenant_shard_id}/timeline/{timeline_id}/{dir}_gc",
            self.mgmt_api_endpoint,
        );

        self.request(Method::POST, &uri, ()).await.map(|_| ())
    }

    pub async fn tenant_reset(&self, tenant_shard_id: TenantShardId) -> Result<()> {
        let uri = format!(
            "{}/v1/tenant/{}/reset",
            self.mgmt_api_endpoint, tenant_shard_id
        );
        self.request(Method::POST, &uri, ())
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn tenant_shard_split(
        &self,
        tenant_shard_id: TenantShardId,
        req: TenantShardSplitRequest,
    ) -> Result<TenantShardSplitResponse> {
        let uri = format!(
            "{}/v1/tenant/{}/shard_split",
            self.mgmt_api_endpoint, tenant_shard_id
        );
        self.request(Method::PUT, &uri, req)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn timeline_list(
        &self,
        tenant_shard_id: &TenantShardId,
    ) -> Result<Vec<TimelineInfo>> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline",
            self.mgmt_api_endpoint, tenant_shard_id
        );
        self.get(&uri)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn tenant_synthetic_size(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<TenantHistorySize> {
        let uri = format!(
            "{}/v1/tenant/{}/synthetic_size",
            self.mgmt_api_endpoint, tenant_shard_id
        );
        self.get(&uri)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn put_io_engine(
        &self,
        engine: &pageserver_api::models::virtual_file::IoEngineKind,
    ) -> Result<()> {
        let uri = format!("{}/v1/io_engine", self.mgmt_api_endpoint);
        self.request(Method::PUT, uri, engine)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    /// Configs io mode at runtime.
    pub async fn put_io_mode(
        &self,
        mode: &pageserver_api::models::virtual_file::IoMode,
    ) -> Result<()> {
        let uri = format!("{}/v1/io_mode", self.mgmt_api_endpoint);
        self.request(Method::PUT, uri, mode)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn get_utilization(&self) -> Result<PageserverUtilization> {
        let uri = format!("{}/v1/utilization", self.mgmt_api_endpoint);
        self.get(uri)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn top_tenant_shards(
        &self,
        request: TopTenantShardsRequest,
    ) -> Result<TopTenantShardsResponse> {
        let uri = format!("{}/v1/top_tenants", self.mgmt_api_endpoint);
        self.request(Method::POST, uri, request)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn layer_map_info(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
    ) -> Result<LayerMapInfo> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}/layer",
            self.mgmt_api_endpoint, tenant_shard_id, timeline_id,
        );
        self.get(&uri)
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn layer_evict(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        layer_file_name: &str,
    ) -> Result<bool> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}/layer/{}",
            self.mgmt_api_endpoint, tenant_shard_id, timeline_id, layer_file_name
        );
        let resp = self.request_noerror(Method::DELETE, &uri, ()).await?;
        match resp.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_MODIFIED => Ok(false),
            // TODO: dedupe this pattern / introduce separate error variant?
            status => Err(match resp.json::<HttpErrorBody>().await {
                Ok(HttpErrorBody { msg }) => Error::ApiError(status, msg),
                Err(_) => {
                    Error::ReceiveErrorBody(format!("Http error ({}) at {}.", status.as_u16(), uri))
                }
            }),
        }
    }

    pub async fn layer_ondemand_download(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        layer_file_name: &str,
    ) -> Result<bool> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}/layer/{}",
            self.mgmt_api_endpoint, tenant_shard_id, timeline_id, layer_file_name
        );
        let resp = self.request_noerror(Method::GET, &uri, ()).await?;
        match resp.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_MODIFIED => Ok(false),
            // TODO: dedupe this pattern / introduce separate error variant?
            status => Err(match resp.json::<HttpErrorBody>().await {
                Ok(HttpErrorBody { msg }) => Error::ApiError(status, msg),
                Err(_) => {
                    Error::ReceiveErrorBody(format!("Http error ({}) at {}.", status.as_u16(), uri))
                }
            }),
        }
    }

    pub async fn ingest_aux_files(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        aux_files: HashMap<String, String>,
    ) -> Result<bool> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}/ingest_aux_files",
            self.mgmt_api_endpoint, tenant_shard_id, timeline_id
        );
        let resp = self
            .request_noerror(Method::POST, &uri, IngestAuxFilesRequest { aux_files })
            .await?;
        match resp.status() {
            StatusCode::OK => Ok(true),
            status => Err(match resp.json::<HttpErrorBody>().await {
                Ok(HttpErrorBody { msg }) => Error::ApiError(status, msg),
                Err(_) => {
                    Error::ReceiveErrorBody(format!("Http error ({}) at {}.", status.as_u16(), uri))
                }
            }),
        }
    }

    pub async fn list_aux_files(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Result<HashMap<String, Bytes>> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}/list_aux_files",
            self.mgmt_api_endpoint, tenant_shard_id, timeline_id
        );
        let resp = self
            .request_noerror(Method::POST, &uri, ListAuxFilesRequest { lsn })
            .await?;
        match resp.status() {
            StatusCode::OK => {
                let resp: HashMap<String, Bytes> = resp.json().await.map_err(|e| {
                    Error::ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))
                })?;
                Ok(resp)
            }
            status => Err(match resp.json::<HttpErrorBody>().await {
                Ok(HttpErrorBody { msg }) => Error::ApiError(status, msg),
                Err(_) => {
                    Error::ReceiveErrorBody(format!("Http error ({}) at {}.", status.as_u16(), uri))
                }
            }),
        }
    }

    pub async fn import_basebackup(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        base_lsn: Lsn,
        end_lsn: Lsn,
        pg_version: u32,
        basebackup_tarball: ReqwestBody,
    ) -> Result<()> {
        let uri = format!(
            "{}/v1/tenant/{tenant_id}/timeline/{timeline_id}/import_basebackup?base_lsn={base_lsn}&end_lsn={end_lsn}&pg_version={pg_version}",
            self.mgmt_api_endpoint,
        );
        self.start_request(Method::PUT, uri)
            .body(basebackup_tarball)
            .send()
            .await
            .map_err(Error::SendRequest)?
            .error_from_body()
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn import_wal(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        start_lsn: Lsn,
        end_lsn: Lsn,
        wal_tarball: ReqwestBody,
    ) -> Result<()> {
        let uri = format!(
            "{}/v1/tenant/{tenant_id}/timeline/{timeline_id}/import_wal?start_lsn={start_lsn}&end_lsn={end_lsn}",
            self.mgmt_api_endpoint,
        );
        self.start_request(Method::PUT, uri)
            .body(wal_tarball)
            .send()
            .await
            .map_err(Error::SendRequest)?
            .error_from_body()
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }

    pub async fn timeline_init_lsn_lease(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Result<LsnLease> {
        let uri = format!(
            "{}/v1/tenant/{tenant_shard_id}/timeline/{timeline_id}/lsn_lease",
            self.mgmt_api_endpoint,
        );

        self.request(Method::POST, &uri, LsnLeaseRequest { lsn })
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)
    }
}
