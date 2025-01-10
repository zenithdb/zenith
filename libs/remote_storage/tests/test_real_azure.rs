use std::env;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use std::{collections::HashSet, time::Duration};

use anyhow::Context;
use remote_storage::{
    AzureConfig, GenericRemoteStorage, RemotePath, RemoteStorageConfig, RemoteStorageKind,
};
use test_context::AsyncTestContext;
use tracing::info;

mod common;

#[path = "common/tests.rs"]
mod tests_azure;

use common::{cleanup, ensure_logging_ready, upload_remote_data, upload_simple_remote_data};

const ENABLE_REAL_AZURE_REMOTE_STORAGE_ENV_VAR_NAME: &str = "ENABLE_REAL_AZURE_REMOTE_STORAGE";

const BASE_PREFIX: &str = "test";

struct EnabledAzure {
    client: Arc<GenericRemoteStorage>,
    base_prefix: &'static str,
}

impl EnabledAzure {
    async fn setup(max_keys_in_list_response: Option<i32>) -> Self {
        let client = create_azure_client(max_keys_in_list_response)
            .await
            .context("Azure client creation")
            .expect("Azure client creation failed");

        EnabledAzure {
            client,
            base_prefix: BASE_PREFIX,
        }
    }

    #[allow(unused)] // this will be needed when moving the timeout integration tests back
    fn configure_request_timeout(&mut self, timeout: Duration) {
        match Arc::get_mut(&mut self.client).expect("outer Arc::get_mut") {
            GenericRemoteStorage::AzureBlob(azure) => {
                let azure = Arc::get_mut(azure).expect("inner Arc::get_mut");
                azure.timeout = timeout;
            }
            _ => unreachable!(),
        }
    }
}

enum MaybeEnabledStorage {
    Enabled(EnabledAzure),
    Disabled,
}

impl AsyncTestContext for MaybeEnabledStorage {
    async fn setup() -> Self {
        ensure_logging_ready();

        if env::var(ENABLE_REAL_AZURE_REMOTE_STORAGE_ENV_VAR_NAME).is_err() {
            info!(
                "`{}` env variable is not set, skipping the test",
                ENABLE_REAL_AZURE_REMOTE_STORAGE_ENV_VAR_NAME
            );
            return Self::Disabled;
        }

        Self::Enabled(EnabledAzure::setup(None).await)
    }
}

enum MaybeEnabledStorageWithTestBlobs {
    Enabled(AzureWithTestBlobs),
    Disabled,
    UploadsFailed(anyhow::Error, AzureWithTestBlobs),
}

struct AzureWithTestBlobs {
    enabled: EnabledAzure,
    remote_prefixes: HashSet<RemotePath>,
    remote_blobs: HashSet<RemotePath>,
}

impl AsyncTestContext for MaybeEnabledStorageWithTestBlobs {
    async fn setup() -> Self {
        ensure_logging_ready();
        if env::var(ENABLE_REAL_AZURE_REMOTE_STORAGE_ENV_VAR_NAME).is_err() {
            info!(
                "`{}` env variable is not set, skipping the test",
                ENABLE_REAL_AZURE_REMOTE_STORAGE_ENV_VAR_NAME
            );
            return Self::Disabled;
        }

        let max_keys_in_list_response = 10;
        let upload_tasks_count = 1 + (2 * usize::try_from(max_keys_in_list_response).unwrap());

        let enabled = EnabledAzure::setup(Some(max_keys_in_list_response)).await;

        match upload_remote_data(&enabled.client, enabled.base_prefix, upload_tasks_count).await {
            ControlFlow::Continue(uploads) => {
                info!("Remote objects created successfully");

                Self::Enabled(AzureWithTestBlobs {
                    enabled,
                    remote_prefixes: uploads.prefixes,
                    remote_blobs: uploads.blobs,
                })
            }
            ControlFlow::Break(uploads) => Self::UploadsFailed(
                anyhow::anyhow!("One or multiple blobs failed to upload to Azure"),
                AzureWithTestBlobs {
                    enabled,
                    remote_prefixes: uploads.prefixes,
                    remote_blobs: uploads.blobs,
                },
            ),
        }
    }

    async fn teardown(self) {
        match self {
            Self::Disabled => {}
            Self::Enabled(ctx) | Self::UploadsFailed(_, ctx) => {
                cleanup(&ctx.enabled.client, ctx.remote_blobs).await;
            }
        }
    }
}

enum MaybeEnabledStorageWithSimpleTestBlobs {
    Enabled(AzureWithSimpleTestBlobs),
    Disabled,
    UploadsFailed(anyhow::Error, AzureWithSimpleTestBlobs),
}
struct AzureWithSimpleTestBlobs {
    enabled: EnabledAzure,
    remote_blobs: HashSet<RemotePath>,
}

impl AsyncTestContext for MaybeEnabledStorageWithSimpleTestBlobs {
    async fn setup() -> Self {
        ensure_logging_ready();
        if env::var(ENABLE_REAL_AZURE_REMOTE_STORAGE_ENV_VAR_NAME).is_err() {
            info!(
                "`{}` env variable is not set, skipping the test",
                ENABLE_REAL_AZURE_REMOTE_STORAGE_ENV_VAR_NAME
            );
            return Self::Disabled;
        }

        let max_keys_in_list_response = 10;
        let upload_tasks_count = 1 + (2 * usize::try_from(max_keys_in_list_response).unwrap());

        let enabled = EnabledAzure::setup(Some(max_keys_in_list_response)).await;

        match upload_simple_remote_data(&enabled.client, upload_tasks_count).await {
            ControlFlow::Continue(uploads) => {
                info!("Remote objects created successfully");

                Self::Enabled(AzureWithSimpleTestBlobs {
                    enabled,
                    remote_blobs: uploads,
                })
            }
            ControlFlow::Break(uploads) => Self::UploadsFailed(
                anyhow::anyhow!("One or multiple blobs failed to upload to Azure"),
                AzureWithSimpleTestBlobs {
                    enabled,
                    remote_blobs: uploads,
                },
            ),
        }
    }

    async fn teardown(self) {
        match self {
            Self::Disabled => {}
            Self::Enabled(ctx) | Self::UploadsFailed(_, ctx) => {
                cleanup(&ctx.enabled.client, ctx.remote_blobs).await;
            }
        }
    }
}

async fn create_azure_client(
    max_keys_per_list_response: Option<i32>,
) -> anyhow::Result<Arc<GenericRemoteStorage>> {
    use rand::Rng;

    let remote_storage_azure_container = env::var("REMOTE_STORAGE_AZURE_CONTAINER").context(
        "`REMOTE_STORAGE_AZURE_CONTAINER` env var is not set, but real Azure tests are enabled",
    )?;
    let remote_storage_azure_region = env::var("REMOTE_STORAGE_AZURE_REGION").context(
        "`REMOTE_STORAGE_AZURE_REGION` env var is not set, but real Azure tests are enabled",
    )?;

    // due to how time works, we've had test runners use the same nanos as bucket prefixes.
    // millis is just a debugging aid for easier finding the prefix later.
    let millis = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("random Azure test prefix part calculation")?
        .as_millis();

    // because nanos can be the same for two threads so can millis, add randomness
    let random = rand::thread_rng().gen::<u32>();

    let remote_storage_config = RemoteStorageConfig {
        storage: RemoteStorageKind::AzureContainer(AzureConfig {
            container_name: remote_storage_azure_container,
            storage_account: None,
            container_region: remote_storage_azure_region,
            prefix_in_container: Some(format!("test_{millis}_{random:08x}/")),
            concurrency_limit: NonZeroUsize::new(100).unwrap(),
            max_keys_per_list_response,
            conn_pool_size: 8,
        }),
        timeout: RemoteStorageConfig::DEFAULT_TIMEOUT,
        small_timeout: RemoteStorageConfig::DEFAULT_SMALL_TIMEOUT,
    };
    Ok(Arc::new(
        GenericRemoteStorage::from_config(&remote_storage_config)
            .await
            .context("remote storage init")?,
    ))
}
