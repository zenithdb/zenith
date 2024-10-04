use anyhow::Context;
use camino::Utf8Path;
use futures::StreamExt;
use remote_storage::{DownloadError, DownloadOpts, ListingMode, RemotePath};
use std::sync::Arc;
use std::{collections::HashSet, num::NonZeroU32};
use test_context::test_context;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::common::{download_to_vec, upload_stream, wrap_stream};

use super::{
    MaybeEnabledStorage, MaybeEnabledStorageWithSimpleTestBlobs, MaybeEnabledStorageWithTestBlobs,
};

/// Tests that S3 client can list all prefixes, even if the response come paginated and requires multiple S3 queries.
/// Uses real S3 and requires [`ENABLE_REAL_S3_REMOTE_STORAGE_ENV_VAR_NAME`] and related S3 cred env vars specified.
/// See the client creation in [`create_s3_client`] for details on the required env vars.
/// If real S3 tests are disabled, the test passes, skipping any real test run: currently, there's no way to mark the test ignored in runtime with the
/// deafult test framework, see https://github.com/rust-lang/rust/issues/68007 for details.
///
/// First, the test creates a set of S3 objects with keys `/${random_prefix_part}/${base_prefix_str}/sub_prefix_${i}/blob_${i}` in [`upload_remote_data`]
/// where
/// * `random_prefix_part` is set for the entire S3 client during the S3 client creation in [`create_s3_client`], to avoid multiple test runs interference
/// * `base_prefix_str` is a common prefix to use in the client requests: we would want to ensure that the client is able to list nested prefixes inside the bucket
///
/// Then, verifies that the client does return correct prefixes when queried:
/// * with no prefix, it lists everything after its `${random_prefix_part}/` — that should be `${base_prefix_str}` value only
/// * with `${base_prefix_str}/` prefix, it lists every `sub_prefix_${i}`
///
/// In the `MaybeEnabledStorageWithTestBlobs::setup`, we set the `max_keys_in_list_response` param to limit the keys in a single response.
/// This way, we are able to test the pagination, by ensuring all results are returned from the remote storage and avoid uploading too many blobs to S3,
/// as the current default AWS S3 pagination limit is 1000.
/// (see <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_RequestSyntax>).
///
/// Lastly, the test attempts to clean up and remove all uploaded S3 files.
/// If any errors appear during the clean up, they get logged, but the test is not failed or stopped until clean up is finished.
#[test_context(MaybeEnabledStorageWithTestBlobs)]
#[tokio::test]
async fn pagination_should_work(ctx: &mut MaybeEnabledStorageWithTestBlobs) -> anyhow::Result<()> {
    let ctx = match ctx {
        MaybeEnabledStorageWithTestBlobs::Enabled(ctx) => ctx,
        MaybeEnabledStorageWithTestBlobs::Disabled => return Ok(()),
        MaybeEnabledStorageWithTestBlobs::UploadsFailed(e, _) => {
            anyhow::bail!("S3 init failed: {e:?}")
        }
    };

    let cancel = CancellationToken::new();

    let test_client = Arc::clone(&ctx.enabled.client);
    let expected_remote_prefixes = ctx.remote_prefixes.clone();

    let base_prefix = RemotePath::new(Utf8Path::new(ctx.enabled.base_prefix))
        .context("common_prefix construction")?;
    let root_remote_prefixes = test_client
        .list(None, ListingMode::WithDelimiter, None, &cancel)
        .await?
        .prefixes
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(
        root_remote_prefixes, HashSet::from([base_prefix.clone()]),
        "remote storage root prefixes list mismatches with the uploads. Returned prefixes: {root_remote_prefixes:?}"
    );

    let nested_remote_prefixes = test_client
        .list(
            Some(&base_prefix.add_trailing_slash()),
            ListingMode::WithDelimiter,
            None,
            &cancel,
        )
        .await?
        .prefixes
        .into_iter()
        .collect::<HashSet<_>>();
    let remote_only_prefixes = nested_remote_prefixes
        .difference(&expected_remote_prefixes)
        .collect::<HashSet<_>>();
    let missing_uploaded_prefixes = expected_remote_prefixes
        .difference(&nested_remote_prefixes)
        .collect::<HashSet<_>>();
    assert_eq!(
        remote_only_prefixes.len() + missing_uploaded_prefixes.len(), 0,
        "remote storage nested prefixes list mismatches with the uploads. Remote only prefixes: {remote_only_prefixes:?}, missing uploaded prefixes: {missing_uploaded_prefixes:?}",
    );

    // list_streaming

    let prefix_with_slash = base_prefix.add_trailing_slash();
    let mut nested_remote_prefixes_st = test_client.list_streaming(
        Some(&prefix_with_slash),
        ListingMode::WithDelimiter,
        None,
        &cancel,
    );
    let mut nested_remote_prefixes_combined = HashSet::new();
    let mut segments = 0;
    let mut segment_max_size = 0;
    while let Some(st) = nested_remote_prefixes_st.next().await {
        let st = st?;
        segment_max_size = segment_max_size.max(st.prefixes.len());
        nested_remote_prefixes_combined.extend(st.prefixes.into_iter());
        segments += 1;
    }
    assert!(segments > 1, "less than 2 segments: {segments}");
    assert!(
        segment_max_size * 2 <= nested_remote_prefixes_combined.len(),
        "double of segment_max_size={segment_max_size} larger number of remote prefixes of {}",
        nested_remote_prefixes_combined.len()
    );
    let remote_only_prefixes = nested_remote_prefixes_combined
        .difference(&expected_remote_prefixes)
        .collect::<HashSet<_>>();
    let missing_uploaded_prefixes = expected_remote_prefixes
        .difference(&nested_remote_prefixes_combined)
        .collect::<HashSet<_>>();
    assert_eq!(
        remote_only_prefixes.len() + missing_uploaded_prefixes.len(), 0,
        "remote storage nested prefixes list mismatches with the uploads. Remote only prefixes: {remote_only_prefixes:?}, missing uploaded prefixes: {missing_uploaded_prefixes:?}",
    );

    Ok(())
}

/// Tests that S3 client can list all files in a folder, even if the response comes paginated and requirees multiple S3 queries.
/// Uses real S3 and requires [`ENABLE_REAL_S3_REMOTE_STORAGE_ENV_VAR_NAME`] and related S3 cred env vars specified. Test will skip real code and pass if env vars not set.
/// See `s3_pagination_should_work` for more information.
///
/// First, create a set of S3 objects with keys `random_prefix/folder{j}/blob_{i}.txt` in [`upload_remote_data`]
/// Then performs the following queries:
///    1. `list(None)`. This should return all files `random_prefix/folder{j}/blob_{i}.txt`
///    2. `list("folder1")`.  This  should return all files `random_prefix/folder1/blob_{i}.txt`
#[test_context(MaybeEnabledStorageWithSimpleTestBlobs)]
#[tokio::test]
async fn list_no_delimiter_works(
    ctx: &mut MaybeEnabledStorageWithSimpleTestBlobs,
) -> anyhow::Result<()> {
    let ctx = match ctx {
        MaybeEnabledStorageWithSimpleTestBlobs::Enabled(ctx) => ctx,
        MaybeEnabledStorageWithSimpleTestBlobs::Disabled => return Ok(()),
        MaybeEnabledStorageWithSimpleTestBlobs::UploadsFailed(e, _) => {
            anyhow::bail!("S3 init failed: {e:?}")
        }
    };
    let cancel = CancellationToken::new();
    let test_client = Arc::clone(&ctx.enabled.client);
    let base_prefix =
        RemotePath::new(Utf8Path::new("folder1")).context("common_prefix construction")?;
    let root_files = test_client
        .list(None, ListingMode::NoDelimiter, None, &cancel)
        .await
        .context("client list root files failure")?
        .keys
        .into_iter()
        .map(|o| o.key)
        .collect::<HashSet<_>>();
    assert_eq!(
        root_files,
        ctx.remote_blobs.clone(),
        "remote storage list on root mismatches with the uploads."
    );

    // Test that max_keys limit works. In total there are about 21 files (see
    // upload_simple_remote_data call in test_real_s3.rs).
    let limited_root_files = test_client
        .list(
            None,
            ListingMode::NoDelimiter,
            Some(NonZeroU32::new(2).unwrap()),
            &cancel,
        )
        .await
        .context("client list root files failure")?;
    assert_eq!(limited_root_files.keys.len(), 2);

    let nested_remote_files = test_client
        .list(Some(&base_prefix), ListingMode::NoDelimiter, None, &cancel)
        .await
        .context("client list nested files failure")?
        .keys
        .into_iter()
        .map(|o| o.key)
        .collect::<HashSet<_>>();
    let trim_remote_blobs: HashSet<_> = ctx
        .remote_blobs
        .iter()
        .map(|x| x.get_path())
        .filter(|x| x.starts_with("folder1"))
        .map(|x| RemotePath::new(x).expect("must be valid path"))
        .collect();
    assert_eq!(
        nested_remote_files, trim_remote_blobs,
        "remote storage list on subdirrectory mismatches with the uploads."
    );
    Ok(())
}

#[test_context(MaybeEnabledStorage)]
#[tokio::test]
async fn delete_non_exising_works(ctx: &mut MaybeEnabledStorage) -> anyhow::Result<()> {
    let ctx = match ctx {
        MaybeEnabledStorage::Enabled(ctx) => ctx,
        MaybeEnabledStorage::Disabled => return Ok(()),
    };

    let cancel = CancellationToken::new();

    let path = RemotePath::new(Utf8Path::new(
        format!("{}/for_sure_there_is_nothing_there_really", ctx.base_prefix).as_str(),
    ))
    .with_context(|| "RemotePath conversion")?;

    ctx.client
        .delete(&path, &cancel)
        .await
        .expect("should succeed");

    Ok(())
}

#[test_context(MaybeEnabledStorage)]
#[tokio::test]
async fn delete_objects_works(ctx: &mut MaybeEnabledStorage) -> anyhow::Result<()> {
    let ctx = match ctx {
        MaybeEnabledStorage::Enabled(ctx) => ctx,
        MaybeEnabledStorage::Disabled => return Ok(()),
    };

    let cancel = CancellationToken::new();

    let path1 = RemotePath::new(Utf8Path::new(format!("{}/path1", ctx.base_prefix).as_str()))
        .with_context(|| "RemotePath conversion")?;

    let path2 = RemotePath::new(Utf8Path::new(format!("{}/path2", ctx.base_prefix).as_str()))
        .with_context(|| "RemotePath conversion")?;

    let path3 = RemotePath::new(Utf8Path::new(format!("{}/path3", ctx.base_prefix).as_str()))
        .with_context(|| "RemotePath conversion")?;

    let (data, len) = upload_stream("remote blob data1".as_bytes().into());
    ctx.client.upload(data, len, &path1, None, &cancel).await?;

    let (data, len) = upload_stream("remote blob data2".as_bytes().into());
    ctx.client.upload(data, len, &path2, None, &cancel).await?;

    let (data, len) = upload_stream("remote blob data3".as_bytes().into());
    ctx.client.upload(data, len, &path3, None, &cancel).await?;

    ctx.client.delete_objects(&[path1, path2], &cancel).await?;

    let prefixes = ctx
        .client
        .list(None, ListingMode::WithDelimiter, None, &cancel)
        .await?
        .prefixes;

    assert_eq!(prefixes.len(), 1);

    ctx.client.delete_objects(&[path3], &cancel).await?;

    Ok(())
}

#[test_context(MaybeEnabledStorage)]
#[tokio::test]
async fn upload_download_works(ctx: &mut MaybeEnabledStorage) -> anyhow::Result<()> {
    let MaybeEnabledStorage::Enabled(ctx) = ctx else {
        return Ok(());
    };

    let cancel = CancellationToken::new();

    let path = RemotePath::new(Utf8Path::new(format!("{}/file", ctx.base_prefix).as_str()))
        .with_context(|| "RemotePath conversion")?;

    let orig = bytes::Bytes::from_static("remote blob data here".as_bytes());

    let (data, len) = wrap_stream(orig.clone());

    ctx.client.upload(data, len, &path, None, &cancel).await?;

    // Normal download request
    let dl = ctx
        .client
        .download(&path, &DownloadOpts::default(), &cancel)
        .await?;
    let buf = download_to_vec(dl).await?;
    assert_eq!(&buf, &orig);

    // Full range (end specified)
    let dl = ctx
        .client
        .download_byte_range(&path, 0, Some(len as u64), &cancel)
        .await?;
    let buf = download_to_vec(dl).await?;
    assert_eq!(&buf, &orig);

    // partial range (end specified)
    let dl = ctx
        .client
        .download_byte_range(&path, 4, Some(10), &cancel)
        .await?;
    let buf = download_to_vec(dl).await?;
    assert_eq!(&buf, &orig[4..10]);

    // partial range (end beyond real end)
    let dl = ctx
        .client
        .download_byte_range(&path, 8, Some(len as u64 * 100), &cancel)
        .await?;
    let buf = download_to_vec(dl).await?;
    assert_eq!(&buf, &orig[8..]);

    // Partial range (end unspecified)
    let dl = ctx
        .client
        .download_byte_range(&path, 4, None, &cancel)
        .await?;
    let buf = download_to_vec(dl).await?;
    assert_eq!(&buf, &orig[4..]);

    // Full range (end unspecified)
    let dl = ctx
        .client
        .download_byte_range(&path, 0, None, &cancel)
        .await?;
    let buf = download_to_vec(dl).await?;
    assert_eq!(&buf, &orig);

    debug!("Cleanup: deleting file at path {path:?}");
    ctx.client
        .delete(&path, &cancel)
        .await
        .with_context(|| format!("{path:?} removal"))?;

    Ok(())
}

/// Tests that conditional downloads work properly, by returning
/// DownloadError::Unmodified when the object ETag matches the given ETag.
#[test_context(MaybeEnabledStorage)]
#[tokio::test]
async fn download_conditional(ctx: &mut MaybeEnabledStorage) -> anyhow::Result<()> {
    let MaybeEnabledStorage::Enabled(ctx) = ctx else {
        return Ok(());
    };
    let cancel = CancellationToken::new();

    // Create a file.
    let path = RemotePath::new(Utf8Path::new(format!("{}/file", ctx.base_prefix).as_str()))?;
    let data = bytes::Bytes::from_static("foo".as_bytes());
    let (stream, len) = wrap_stream(data.clone());
    ctx.client.upload(stream, len, &path, None, &cancel).await?;

    // Download it to obtain its etag.
    let mut opts = DownloadOpts::default();
    let download = ctx.client.download(&path, &opts, &cancel).await?;

    // Download with the etag yields DownloadError::Unmodified.
    opts.etag = Some(download.etag);
    let result = ctx.client.download(&path, &opts, &cancel).await;
    assert!(
        matches!(result, Err(DownloadError::Unmodified)),
        "expected DownloadError::Unmodified, got {result:?}"
    );

    // Replace the file contents.
    let data = bytes::Bytes::from_static("bar".as_bytes());
    let (stream, len) = wrap_stream(data.clone());
    ctx.client.upload(stream, len, &path, None, &cancel).await?;

    // A download with the old etag should yield the new file.
    let download = ctx.client.download(&path, &opts, &cancel).await?;
    assert_ne!(download.etag, opts.etag.unwrap(), "ETag did not change");

    // A download with the new etag should yield Unmodified again.
    opts.etag = Some(download.etag);
    let result = ctx.client.download(&path, &opts, &cancel).await;
    assert!(
        matches!(result, Err(DownloadError::Unmodified)),
        "expected DownloadError::Unmodified, got {result:?}"
    );

    Ok(())
}

#[test_context(MaybeEnabledStorage)]
#[tokio::test]
async fn copy_works(ctx: &mut MaybeEnabledStorage) -> anyhow::Result<()> {
    let MaybeEnabledStorage::Enabled(ctx) = ctx else {
        return Ok(());
    };

    let cancel = CancellationToken::new();

    let path = RemotePath::new(Utf8Path::new(
        format!("{}/file_to_copy", ctx.base_prefix).as_str(),
    ))
    .with_context(|| "RemotePath conversion")?;
    let path_dest = RemotePath::new(Utf8Path::new(
        format!("{}/file_dest", ctx.base_prefix).as_str(),
    ))
    .with_context(|| "RemotePath conversion")?;

    let orig = bytes::Bytes::from_static("remote blob data content".as_bytes());

    let (data, len) = wrap_stream(orig.clone());

    ctx.client.upload(data, len, &path, None, &cancel).await?;

    // Normal download request
    ctx.client.copy_object(&path, &path_dest, &cancel).await?;

    let dl = ctx
        .client
        .download(&path_dest, &DownloadOpts::default(), &cancel)
        .await?;
    let buf = download_to_vec(dl).await?;
    assert_eq!(&buf, &orig);

    debug!("Cleanup: deleting file at path {path:?}");
    ctx.client
        .delete_objects(&[path.clone(), path_dest.clone()], &cancel)
        .await
        .with_context(|| format!("{path:?} removal"))?;

    Ok(())
}
