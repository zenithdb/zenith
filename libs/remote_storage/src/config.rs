use std::{fmt::Debug, num::NonZeroUsize, str::FromStr, time::Duration};

use aws_sdk_s3::types::StorageClass;
use camino::Utf8PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    DEFAULT_MAX_KEYS_PER_LIST_RESPONSE, DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT,
    DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT,
};

/// External backup storage configuration, enough for creating a client for that storage.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RemoteStorageConfig {
    /// The storage connection configuration.
    #[serde(flatten)]
    pub storage: RemoteStorageKind,
    /// A common timeout enforced for all requests after concurrency limiter permit has been
    /// acquired.
    #[serde(
        with = "humantime_serde",
        default = "default_timeout",
        skip_serializing_if = "is_default_timeout"
    )]
    pub timeout: Duration,
    /// Alternative timeout used for metadata objects which are expected to be small
    #[serde(
        with = "humantime_serde",
        default = "default_small_timeout",
        skip_serializing_if = "is_default_small_timeout"
    )]
    pub small_timeout: Duration,
}

impl RemoteStorageKind {
    pub fn bucket_name(&self) -> Option<&str> {
        match self {
            RemoteStorageKind::LocalFs { .. } => None,
            RemoteStorageKind::AwsS3(config) => Some(&config.bucket_name),
            RemoteStorageKind::AzureContainer(config) => Some(&config.container_name),
        }
    }
}

fn default_timeout() -> Duration {
    RemoteStorageConfig::DEFAULT_TIMEOUT
}

fn default_small_timeout() -> Duration {
    RemoteStorageConfig::DEFAULT_SMALL_TIMEOUT
}

fn is_default_timeout(d: &Duration) -> bool {
    *d == RemoteStorageConfig::DEFAULT_TIMEOUT
}

fn is_default_small_timeout(d: &Duration) -> bool {
    *d == RemoteStorageConfig::DEFAULT_SMALL_TIMEOUT
}

/// A kind of a remote storage to connect to, with its connection configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RemoteStorageKind {
    /// Storage based on local file system.
    /// Specify a root folder to place all stored files into.
    LocalFs { local_path: Utf8PathBuf },
    /// AWS S3 based storage, storing all files in the S3 bucket
    /// specified by the config
    AwsS3(S3Config),
    /// Azure Blob based storage, storing all files in the container
    /// specified by the config
    AzureContainer(AzureConfig),
}

/// AWS S3 bucket coordinates and access credentials to manage the bucket contents (read and write).
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct S3Config {
    /// Name of the bucket to connect to.
    pub bucket_name: String,
    /// The region where the bucket is located at.
    pub bucket_region: String,
    /// A "subfolder" in the bucket, to use the same bucket separately by multiple remote storage users at once.
    pub prefix_in_bucket: Option<String>,
    /// A base URL to send S3 requests to.
    /// By default, the endpoint is derived from a region name, assuming it's
    /// an AWS S3 region name, erroring on wrong region name.
    /// Endpoint provides a way to support other S3 flavors and their regions.
    ///
    /// Example: `http://127.0.0.1:5000`
    pub endpoint: Option<String>,
    /// AWS S3 has various limits on its API calls, we need not to exceed those.
    /// See [`DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT`] for more details.
    #[serde(default = "default_remote_storage_s3_concurrency_limit")]
    pub concurrency_limit: NonZeroUsize,
    #[serde(default = "default_max_keys_per_list_response")]
    pub max_keys_per_list_response: Option<i32>,
    #[serde(
        deserialize_with = "deserialize_storage_class",
        serialize_with = "serialize_storage_class",
        default
    )]
    pub upload_storage_class: Option<StorageClass>,
}

fn default_remote_storage_s3_concurrency_limit() -> NonZeroUsize {
    DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT
        .try_into()
        .unwrap()
}

fn default_max_keys_per_list_response() -> Option<i32> {
    DEFAULT_MAX_KEYS_PER_LIST_RESPONSE
}

fn default_azure_conn_pool_size() -> usize {
    // By default, the Azure SDK does no connection pooling, due to historic reports of hard-to-reproduce issues
    // (https://github.com/hyperium/hyper/issues/2312)
    //
    // However, using connection pooling is important to avoid exhausting client ports when
    // doing huge numbers of requests (https://github.com/neondatabase/cloud/issues/20971)
    //
    // We therefore enable a modest pool size by default: this may be configured to zero if
    // issues like the alleged upstream hyper issue appear.
    8
}

impl Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket_name", &self.bucket_name)
            .field("bucket_region", &self.bucket_region)
            .field("prefix_in_bucket", &self.prefix_in_bucket)
            .field("concurrency_limit", &self.concurrency_limit)
            .field(
                "max_keys_per_list_response",
                &self.max_keys_per_list_response,
            )
            .finish()
    }
}

/// Azure  bucket coordinates and access credentials to manage the bucket contents (read and write).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AzureConfig {
    /// Name of the container to connect to.
    pub container_name: String,
    /// Name of the storage account the container is inside of
    pub storage_account: Option<String>,
    /// The region where the bucket is located at.
    pub container_region: String,
    /// A "subfolder" in the container, to use the same container separately by multiple remote storage users at once.
    pub prefix_in_container: Option<String>,
    /// Azure has various limits on its API calls, we need not to exceed those.
    /// See [`DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT`] for more details.
    #[serde(default = "default_remote_storage_azure_concurrency_limit")]
    pub concurrency_limit: NonZeroUsize,
    #[serde(default = "default_max_keys_per_list_response")]
    pub max_keys_per_list_response: Option<i32>,
    #[serde(default = "default_azure_conn_pool_size")]
    pub conn_pool_size: usize,
}

fn default_remote_storage_azure_concurrency_limit() -> NonZeroUsize {
    NonZeroUsize::new(DEFAULT_REMOTE_STORAGE_AZURE_CONCURRENCY_LIMIT).unwrap()
}

impl Debug for AzureConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureConfig")
            .field("bucket_name", &self.container_name)
            .field("storage_account", &self.storage_account)
            .field("bucket_region", &self.container_region)
            .field("prefix_in_container", &self.prefix_in_container)
            .field("concurrency_limit", &self.concurrency_limit)
            .field(
                "max_keys_per_list_response",
                &self.max_keys_per_list_response,
            )
            .finish()
    }
}

fn deserialize_storage_class<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<StorageClass>, D::Error> {
    Option::<String>::deserialize(deserializer).and_then(|s| {
        if let Some(s) = s {
            use serde::de::Error;
            let storage_class = StorageClass::from_str(&s).expect("infallible");
            #[allow(deprecated)]
            if matches!(storage_class, StorageClass::Unknown(_)) {
                return Err(D::Error::custom(format!(
                    "Specified storage class unknown to SDK: '{s}'. Allowed values: {:?}",
                    StorageClass::values()
                )));
            }
            Ok(Some(storage_class))
        } else {
            Ok(None)
        }
    })
}

fn serialize_storage_class<S: serde::Serializer>(
    val: &Option<StorageClass>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let val = val.as_ref().map(StorageClass::as_str);
    Option::<&str>::serialize(&val, serializer)
}

impl RemoteStorageConfig {
    pub const DEFAULT_TIMEOUT: Duration = std::time::Duration::from_secs(120);
    pub const DEFAULT_SMALL_TIMEOUT: Duration = std::time::Duration::from_secs(30);

    pub fn from_toml(toml: &toml_edit::Item) -> anyhow::Result<RemoteStorageConfig> {
        Ok(utils::toml_edit_ext::deserialize_item(toml)?)
    }

    pub fn from_toml_str(input: &str) -> anyhow::Result<RemoteStorageConfig> {
        let toml_document = toml_edit::DocumentMut::from_str(input)?;
        if let Some(item) = toml_document.get("remote_storage") {
            return Self::from_toml(item);
        }
        Self::from_toml(toml_document.as_item())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> anyhow::Result<RemoteStorageConfig> {
        RemoteStorageConfig::from_toml_str(input)
    }

    #[test]
    fn parse_localfs_config_with_timeout() {
        let input = "local_path = '.'
timeout = '5s'";

        let config = parse(input).unwrap();

        assert_eq!(
            config,
            RemoteStorageConfig {
                storage: RemoteStorageKind::LocalFs {
                    local_path: Utf8PathBuf::from(".")
                },
                timeout: Duration::from_secs(5),
                small_timeout: RemoteStorageConfig::DEFAULT_SMALL_TIMEOUT
            }
        );
    }

    #[test]
    fn test_s3_parsing() {
        let toml = "\
    bucket_name = 'foo-bar'
    bucket_region = 'eu-central-1'
    upload_storage_class = 'INTELLIGENT_TIERING'
    timeout = '7s'
    ";

        let config = parse(toml).unwrap();

        assert_eq!(
            config,
            RemoteStorageConfig {
                storage: RemoteStorageKind::AwsS3(S3Config {
                    bucket_name: "foo-bar".into(),
                    bucket_region: "eu-central-1".into(),
                    prefix_in_bucket: None,
                    endpoint: None,
                    concurrency_limit: default_remote_storage_s3_concurrency_limit(),
                    max_keys_per_list_response: DEFAULT_MAX_KEYS_PER_LIST_RESPONSE,
                    upload_storage_class: Some(StorageClass::IntelligentTiering),
                }),
                timeout: Duration::from_secs(7),
                small_timeout: RemoteStorageConfig::DEFAULT_SMALL_TIMEOUT
            }
        );
    }

    #[test]
    fn test_storage_class_serde_roundtrip() {
        let classes = [
            None,
            Some(StorageClass::Standard),
            Some(StorageClass::IntelligentTiering),
        ];
        for class in classes {
            #[derive(Serialize, Deserialize)]
            struct Wrapper {
                #[serde(
                    deserialize_with = "deserialize_storage_class",
                    serialize_with = "serialize_storage_class"
                )]
                class: Option<StorageClass>,
            }
            let wrapped = Wrapper {
                class: class.clone(),
            };
            let serialized = serde_json::to_string(&wrapped).unwrap();
            let deserialized: Wrapper = serde_json::from_str(&serialized).unwrap();
            assert_eq!(class, deserialized.class);
        }
    }

    #[test]
    fn test_azure_parsing() {
        let toml = "\
    container_name = 'foo-bar'
    container_region = 'westeurope'
    upload_storage_class = 'INTELLIGENT_TIERING'
    timeout = '7s'
    conn_pool_size = 8
    ";

        let config = parse(toml).unwrap();

        assert_eq!(
            config,
            RemoteStorageConfig {
                storage: RemoteStorageKind::AzureContainer(AzureConfig {
                    container_name: "foo-bar".into(),
                    storage_account: None,
                    container_region: "westeurope".into(),
                    prefix_in_container: None,
                    concurrency_limit: default_remote_storage_azure_concurrency_limit(),
                    max_keys_per_list_response: DEFAULT_MAX_KEYS_PER_LIST_RESPONSE,
                    conn_pool_size: 8,
                }),
                timeout: Duration::from_secs(7),
                small_timeout: RemoteStorageConfig::DEFAULT_SMALL_TIMEOUT
            }
        );
    }
}
