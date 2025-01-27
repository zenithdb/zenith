use std::collections::HashSet;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use camino::Utf8Path;
use futures::stream::Stream;
use once_cell::sync::OnceCell;
use remote_storage::{Download, GenericRemoteStorage, RemotePath};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

static LOGGING_DONE: OnceCell<()> = OnceCell::new();

pub(crate) fn upload_stream(
    content: std::borrow::Cow<'static, [u8]>,
) -> (
    impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
    usize,
) {
    use std::borrow::Cow;

    let content = match content {
        Cow::Borrowed(x) => Bytes::from_static(x),
        Cow::Owned(vec) => Bytes::from(vec),
    };
    wrap_stream(content)
}

pub(crate) fn wrap_stream(
    content: bytes::Bytes,
) -> (
    impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static,
    usize,
) {
    let len = content.len();
    let content = futures::future::ready(Ok(content));

    (futures::stream::once(content), len)
}

pub(crate) async fn download_to_vec(dl: Download) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    tokio::io::copy_buf(
        &mut tokio_util::io::StreamReader::new(dl.download_stream),
        &mut buf,
    )
    .await?;
    Ok(buf)
}

// Uploads files `folder{j}/blob{i}.txt`. See test description for more details.
pub(crate) async fn upload_simple_remote_data(
    client: &Arc<GenericRemoteStorage>,
    upload_tasks_count: usize,
) -> ControlFlow<HashSet<RemotePath>, HashSet<RemotePath>> {
    info!("Creating {upload_tasks_count} remote files");
    let mut upload_tasks = JoinSet::new();
    let cancel = CancellationToken::new();

    for i in 1..upload_tasks_count + 1 {
        let task_client = Arc::clone(client);
        let cancel = cancel.clone();

        upload_tasks.spawn(async move {
            let blob_path = PathBuf::from(format!("folder{}/blob_{}.txt", i / 7, i));
            let blob_path = RemotePath::new(
                Utf8Path::from_path(blob_path.as_path()).expect("must be valid blob path"),
            )
            .with_context(|| format!("{blob_path:?} to RemotePath conversion"))?;
            debug!("Creating remote item {i} at path {blob_path:?}");

            let (data, len) = upload_stream(format!("remote blob data {i}").into_bytes().into());
            task_client
                .upload(data, len, &blob_path, None, &cancel)
                .await?;

            Ok::<_, anyhow::Error>(blob_path)
        });
    }

    let mut upload_tasks_failed = false;
    let mut uploaded_blobs = HashSet::with_capacity(upload_tasks_count);
    while let Some(task_run_result) = upload_tasks.join_next().await {
        match task_run_result
            .context("task join failed")
            .and_then(|task_result| task_result.context("upload task failed"))
        {
            Ok(upload_path) => {
                uploaded_blobs.insert(upload_path);
            }
            Err(e) => {
                error!("Upload task failed: {e:?}");
                upload_tasks_failed = true;
            }
        }
    }

    if upload_tasks_failed {
        ControlFlow::Break(uploaded_blobs)
    } else {
        ControlFlow::Continue(uploaded_blobs)
    }
}

pub(crate) async fn cleanup(
    client: &Arc<GenericRemoteStorage>,
    objects_to_delete: HashSet<RemotePath>,
) {
    info!(
        "Removing {} objects from the remote storage during cleanup",
        objects_to_delete.len()
    );
    let cancel = CancellationToken::new();
    let mut delete_tasks = JoinSet::new();
    for object_to_delete in objects_to_delete {
        let task_client = Arc::clone(client);
        let cancel = cancel.clone();
        delete_tasks.spawn(async move {
            debug!("Deleting remote item at path {object_to_delete:?}");
            task_client
                .delete(&object_to_delete, &cancel)
                .await
                .with_context(|| format!("{object_to_delete:?} removal"))
        });
    }

    while let Some(task_run_result) = delete_tasks.join_next().await {
        match task_run_result {
            Ok(task_result) => match task_result {
                Ok(()) => {}
                Err(e) => error!("Delete task failed: {e:?}"),
            },
            Err(join_err) => error!("Delete task did not finish correctly: {join_err}"),
        }
    }
}
pub(crate) struct Uploads {
    pub(crate) prefixes: HashSet<RemotePath>,
    pub(crate) blobs: HashSet<RemotePath>,
}

pub(crate) async fn upload_remote_data(
    client: &Arc<GenericRemoteStorage>,
    base_prefix_str: &'static str,
    upload_tasks_count: usize,
) -> ControlFlow<Uploads, Uploads> {
    info!("Creating {upload_tasks_count} remote files");
    let mut upload_tasks = JoinSet::new();
    let cancel = CancellationToken::new();

    for i in 1..=upload_tasks_count {
        let task_client = Arc::clone(client);
        let cancel = cancel.clone();

        upload_tasks.spawn(async move {
            let prefix = format!("{base_prefix_str}/sub_prefix_{i}/");
            let blob_prefix = RemotePath::new(Utf8Path::new(&prefix))
                .with_context(|| format!("{prefix:?} to RemotePath conversion"))?;
            let blob_path = blob_prefix.join(Utf8Path::new(&format!("blob_{i}")));
            debug!("Creating remote item {i} at path {blob_path:?}");

            let (data, data_len) =
                upload_stream(format!("remote blob data {i}").into_bytes().into());
            task_client
                .upload(data, data_len, &blob_path, None, &cancel)
                .await?;

            Ok::<_, anyhow::Error>((blob_prefix, blob_path))
        });
    }

    let mut upload_tasks_failed = false;
    let mut uploaded_prefixes = HashSet::with_capacity(upload_tasks_count);
    let mut uploaded_blobs = HashSet::with_capacity(upload_tasks_count);
    while let Some(task_run_result) = upload_tasks.join_next().await {
        match task_run_result
            .context("task join failed")
            .and_then(|task_result| task_result.context("upload task failed"))
        {
            Ok((upload_prefix, upload_path)) => {
                uploaded_prefixes.insert(upload_prefix);
                uploaded_blobs.insert(upload_path);
            }
            Err(e) => {
                error!("Upload task failed: {e:?}");
                upload_tasks_failed = true;
            }
        }
    }

    let uploads = Uploads {
        prefixes: uploaded_prefixes,
        blobs: uploaded_blobs,
    };
    if upload_tasks_failed {
        ControlFlow::Break(uploads)
    } else {
        ControlFlow::Continue(uploads)
    }
}

pub(crate) fn ensure_logging_ready() {
    LOGGING_DONE.get_or_init(|| {
        utils::logging::init(
            utils::logging::LogFormat::Test,
            utils::logging::TracingErrorLayerEnablement::Disabled,
            utils::logging::OtelEnablement::Disabled,
            utils::logging::Output::Stdout,
        )
        .expect("logging init failed");
    });
}
