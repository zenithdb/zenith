//! Azure Blob Storage wrapper

use std::borrow::Cow;

use super::REMOTE_STORAGE_PREFIX_SEPARATOR;
use anyhow::Result;
use azure_core::request_options::Metadata;
use azure_storage_blobs::{
    blob,
    prelude::{BlobClient, ContainerClient},
};
use futures_util::StreamExt;
use http_types::StatusCode;
use tokio::io::AsyncRead;

use crate::{Download, DownloadError, RemotePath, RemoteStorage, StorageMetadata};

pub struct AzureBlobStorage {
    client: ContainerClient,
    prefix_in_container: Option<String>,
}

impl AzureBlobStorage {
    pub fn new() -> Result<Self> {
        todo!()
    }

    pub fn relative_path_to_name(&self, path: &RemotePath) -> String {
        assert_eq!(std::path::MAIN_SEPARATOR, REMOTE_STORAGE_PREFIX_SEPARATOR);
        let path_string = path
            .get_path()
            .as_str()
            .trim_end_matches(REMOTE_STORAGE_PREFIX_SEPARATOR);
        match &self.prefix_in_container {
            Some(prefix) => prefix.clone() + "/" + path_string,
            None => path_string.to_string(),
        }
    }

    fn name_to_relative_path(&self, key: &str) -> RemotePath {
        let relative_path =
            match key.strip_prefix(self.prefix_in_container.as_deref().unwrap_or_default()) {
                Some(stripped) => stripped,
                // we rely on Azure to return properly prefixed paths
                // for requests with a certain prefix
                None => panic!(
                    "Key {key} does not start with container prefix {:?}",
                    self.prefix_in_container
                ),
            };
        RemotePath(
            relative_path
                .split(REMOTE_STORAGE_PREFIX_SEPARATOR)
                .collect(),
        )
    }
}

fn to_azure_metadata(metadata: StorageMetadata) -> Metadata {
    let mut res = Metadata::new();
    for (k, v) in metadata.0.into_iter() {
        res.insert(k, v);
    }
    res
}

#[async_trait::async_trait]
impl RemoteStorage for AzureBlobStorage {
    async fn list_files(&self, folder: Option<&RemotePath>) -> anyhow::Result<Vec<RemotePath>> {
        self.list_prefixes(folder).await.map_err(|err| match err {
            DownloadError::NotFound => anyhow::anyhow!("not found"), // TODO maybe return empty list?
            DownloadError::BadInput(e) | DownloadError::Other(e) => e,
        })
    }

    async fn list_prefixes(
        &self,
        prefix: Option<&RemotePath>,
    ) -> Result<Vec<RemotePath>, DownloadError> {
        let prefix = prefix.map(|p| Cow::from(p.to_string()));
        let mut builder = self.client.list_blobs();
        if let Some(prefix) = prefix {
            builder = builder.prefix(prefix);
        }

        let mut response = builder.into_stream();
        let mut res = Vec::new();
        while let Some(l) = response.next().await {
            let entry = match l {
                Ok(l) => l,
                Err(e) => {
                    return Err(if let Some(htttp_err) = e.as_http_error() {
                        match htttp_err.status() {
                            StatusCode::NotFound => DownloadError::NotFound,
                            StatusCode::BadRequest => {
                                DownloadError::BadInput(anyhow::Error::new(e))
                            }
                            _ => DownloadError::Other(anyhow::Error::new(e)),
                        }
                    } else {
                        DownloadError::Other(e.into())
                    });
                }
            };
            res.extend(
                entry
                    .blobs
                    .blobs()
                    .map(|bl| self.name_to_relative_path(&bl.name)),
            );
        }
        Ok(res)
    }

    async fn upload(
        &self,
        mut from: impl AsyncRead + Unpin + Send + Sync + 'static,
        data_size_bytes: usize,
        to: &RemotePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let blob_client = self.client.blob_client(self.relative_path_to_name(to));

        // TODO FIX THIS UGLY HACK and don't buffer the entire object
        // into RAM here, but use the streaming interface. For that,
        // we'd have to change the interface though...
        let mut buf = Vec::with_capacity(data_size_bytes);
        tokio::io::copy(&mut from, &mut buf).await?;
        let body = azure_core::Body::Bytes(buf.into());

        let mut builder = blob_client.put_block_blob(body);

        if let Some(metadata) = metadata {
            builder = builder.metadata(to_azure_metadata(metadata));
        }

        let _response = builder.into_future().await?;

        Ok(())
    }

    async fn download(&self, from: &RemotePath) -> Result<Download, DownloadError> {
        todo!()
    }

    async fn download_byte_range(
        &self,
        from: &RemotePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
    ) -> Result<Download, DownloadError> {
        todo!()
    }

    async fn delete(&self, path: &RemotePath) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_objects<'a>(&self, paths: &'a [RemotePath]) -> anyhow::Result<()> {
        todo!()
    }
}
