use std::time::Duration;

use reqwest::StatusCode;
use safekeeper_client::mgmt_api;
use tokio_util::sync::CancellationToken;
use utils::{backoff, id::NodeId, logging::SecretString};

use crate::{persistence::SafekeeperPersistence, safekeeper_client::SafekeeperClient};

pub struct Safekeeper {
    #[expect(dead_code)]
    skp: SafekeeperPersistence,
    cancel: CancellationToken,
    listen_http_addr: String,
    listen_http_port: u16,
    id: NodeId,
}

impl Safekeeper {
    pub(crate) fn base_url(&self) -> String {
        format!("http://{}:{}", self.listen_http_addr, self.listen_http_port)
    }

    pub(crate) fn get_id(&self) -> NodeId {
        self.id
    }
    pub(crate) async fn with_client_retries<T, O, F>(
        &self,
        mut op: O,
        jwt: &Option<SecretString>,
        warn_threshold: u32,
        max_retries: u32,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Option<mgmt_api::Result<T>>
    where
        O: FnMut(SafekeeperClient) -> F,
        F: std::future::Future<Output = mgmt_api::Result<T>>,
    {
        fn is_fatal(e: &mgmt_api::Error) -> bool {
            use mgmt_api::Error::*;
            match e {
                ReceiveBody(_) | ReceiveErrorBody(_) => false,
                ApiError(StatusCode::SERVICE_UNAVAILABLE, _)
                | ApiError(StatusCode::GATEWAY_TIMEOUT, _)
                | ApiError(StatusCode::REQUEST_TIMEOUT, _) => false,
                ApiError(_, _) => true,
                Cancelled => true,
            }
        }

        backoff::retry(
            || {
                let http_client = reqwest::ClientBuilder::new()
                    .timeout(timeout)
                    .build()
                    .expect("Failed to construct HTTP client");

                let client = SafekeeperClient::from_client(
                    self.get_id(),
                    http_client,
                    self.base_url(),
                    jwt.clone(),
                );

                let node_cancel_fut = self.cancel.cancelled();

                let op_fut = op(client);

                async {
                    tokio::select! {
                        r = op_fut=> {r},
                        _ = node_cancel_fut => {
                        Err(mgmt_api::Error::Cancelled)
                    }}
                }
            },
            is_fatal,
            warn_threshold,
            max_retries,
            &format!(
                "Call to node {} ({}:{}) management API",
                self.id, self.listen_http_addr, self.listen_http_port
            ),
            cancel,
        )
        .await
    }
}
