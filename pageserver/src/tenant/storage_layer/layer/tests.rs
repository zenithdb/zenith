use std::time::UNIX_EPOCH;

use pageserver_api::key::CONTROLFILE_KEY;
use tokio::task::JoinSet;
use utils::{
    completion::{self, Completion},
    id::TimelineId,
};

use super::failpoints::{Failpoint, FailpointKind};
use super::*;
use crate::{context::DownloadBehavior, tenant::storage_layer::LayerVisibilityHint};
use crate::{task_mgr::TaskKind, tenant::harness::TenantHarness};

/// Used in tests to advance a future to wanted await point, and not futher.
const ADVANCE: std::time::Duration = std::time::Duration::from_secs(3600);

/// Used in tests to indicate forever long timeout; has to be longer than the amount of ADVANCE
/// timeout uses to advance futures.
const FOREVER: std::time::Duration = std::time::Duration::from_secs(ADVANCE.as_secs() * 24 * 7);

/// Demonstrate the API and resident -> evicted -> resident -> deleted transitions.
#[tokio::test]
async fn smoke_test() {
    let handle = tokio::runtime::Handle::current();

    let h = TenantHarness::create("smoke_test").await.unwrap();
    let span = h.span();
    let download_span = span.in_scope(|| tracing::info_span!("downloading", timeline_id = 1));
    let (tenant, _) = h.load().await;

    let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Download);

    let timeline = tenant
        .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
        .await
        .unwrap();

    let layer = {
        let mut layers = {
            let layers = timeline.layers.read().await;
            layers.likely_resident_layers().cloned().collect::<Vec<_>>()
        };

        assert_eq!(layers.len(), 1);

        layers.swap_remove(0)
    };

    // all layers created at pageserver are like `layer`, initialized with strong
    // Arc<DownloadedLayer>.

    let controlfile_keyspace = KeySpace {
        ranges: vec![CONTROLFILE_KEY..CONTROLFILE_KEY.next()],
    };

    let img_before = {
        let mut data = ValuesReconstructState::default();
        layer
            .get_values_reconstruct_data(
                controlfile_keyspace.clone(),
                Lsn(0x10)..Lsn(0x11),
                &mut data,
                &ctx,
            )
            .await
            .unwrap();
        data.keys
            .remove(&CONTROLFILE_KEY)
            .expect("must be present")
            .expect("should not error")
            .img
            .take()
            .expect("tenant harness writes the control file")
    };

    // important part is evicting the layer, which can be done when there are no more ResidentLayer
    // instances -- there currently are none, only two `Layer` values, one in the layermap and on
    // in scope.
    layer.evict_and_wait(FOREVER).await.unwrap();

    // double-evict returns an error, which is valid if both eviction_task and disk usage based
    // eviction would both evict the same layer at the same time.

    let e = layer.evict_and_wait(FOREVER).await.unwrap_err();
    assert!(matches!(e, EvictionError::NotFound));

    // on accesses when the layer is evicted, it will automatically be downloaded.
    let img_after = {
        let mut data = ValuesReconstructState::default();
        layer
            .get_values_reconstruct_data(
                controlfile_keyspace.clone(),
                Lsn(0x10)..Lsn(0x11),
                &mut data,
                &ctx,
            )
            .instrument(download_span.clone())
            .await
            .unwrap();
        data.keys
            .remove(&CONTROLFILE_KEY)
            .expect("must be present")
            .expect("should not error")
            .img
            .take()
            .expect("tenant harness writes the control file")
    };

    let img_before = (img_before.0, img_before.1.await.unwrap().unwrap());
    let img_after = (img_after.0, img_after.1.await.unwrap().unwrap());
    assert_eq!(img_before, img_after);

    // evict_and_wait can timeout, but it doesn't cancel the evicting itself
    //
    // ZERO for timeout does not work reliably, so first take up all spawn_blocking slots to
    // artificially slow it down.
    let helper = SpawnBlockingPoolHelper::consume_all_spawn_blocking_threads(&handle).await;

    match layer
        .evict_and_wait(std::time::Duration::ZERO)
        .await
        .unwrap_err()
    {
        EvictionError::Timeout => {
            // expected, but note that the eviction is "still ongoing"
            helper.release().await;
            // exhaust spawn_blocking pool to ensure it is now complete
            SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads(&handle)
                .await;
        }
        other => unreachable!("{other:?}"),
    }

    // only way to query if a layer is resident is to acquire a ResidentLayer instance.
    // Layer::keep_resident never downloads, but it might initialize if the layer file is found
    // downloaded locally.
    let none = layer.keep_resident().await;
    assert!(
        none.is_none(),
        "Expected none, because eviction removed the local file, found: {none:?}"
    );

    // plain downloading is rarely needed
    layer
        .download_and_keep_resident()
        .instrument(download_span)
        .await
        .unwrap();

    // last important part is deletion on drop: gc and compaction use it for compacted L0 layers
    // or fully garbage collected layers. deletion means deleting the local file, and scheduling a
    // deletion of the already unlinked from index_part.json remote file.
    //
    // marking a layer to be deleted on drop is irreversible; there is no technical reason against
    // reversiblity, but currently it is not needed so it is not provided.
    layer.delete_on_drop();

    let path = layer.local_path().to_owned();

    // wait_drop produces an unconnected to Layer future which will resolve when the
    // LayerInner::drop has completed.
    let mut wait_drop = std::pin::pin!(layer.wait_drop());

    // paused time doesn't really work well with timeouts and evict_and_wait, so delay pausing
    // until here
    tokio::time::pause();
    tokio::time::timeout(ADVANCE, &mut wait_drop)
        .await
        .expect_err("should had timed out because two strong references exist");

    tokio::fs::metadata(&path)
        .await
        .expect("the local layer file still exists");

    let rtc = &timeline.remote_client;

    {
        let layers = &[layer];
        let mut g = timeline.layers.write().await;
        g.open_mut().unwrap().finish_gc_timeline(layers);
        // this just updates the remote_physical_size for demonstration purposes
        rtc.schedule_gc_update(layers).unwrap();
    }

    // when strong references are dropped, the file is deleted and remote deletion is scheduled
    wait_drop.await;

    let e = tokio::fs::metadata(&path)
        .await
        .expect_err("the local file is deleted");
    assert_eq!(e.kind(), std::io::ErrorKind::NotFound);

    rtc.wait_completion().await.unwrap();

    assert_eq!(rtc.get_remote_physical_size(), 0);
    assert_eq!(0, LAYER_IMPL_METRICS.inits_cancelled.get())
}

/// This test demonstrates a previous hang when a eviction and deletion were requested at the same
/// time. Now both of them complete per Arc drop semantics.
#[tokio::test(start_paused = true)]
async fn evict_and_wait_on_wanted_deleted() {
    // this is the runtime on which Layer spawns the blocking tasks on
    let handle = tokio::runtime::Handle::current();

    let h = TenantHarness::create("evict_and_wait_on_wanted_deleted")
        .await
        .unwrap();
    utils::logging::replace_panic_hook_with_tracing_panic_hook().forget();
    let (tenant, ctx) = h.load().await;

    let timeline = tenant
        .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
        .await
        .unwrap();

    let layer = {
        let mut layers = {
            let layers = timeline.layers.read().await;
            layers.likely_resident_layers().cloned().collect::<Vec<_>>()
        };

        assert_eq!(layers.len(), 1);

        layers.swap_remove(0)
    };

    // setup done

    let resident = layer.keep_resident().await.unwrap();

    {
        let mut evict_and_wait = std::pin::pin!(layer.evict_and_wait(FOREVER));

        // drive the future to await on the status channel
        tokio::time::timeout(ADVANCE, &mut evict_and_wait)
            .await
            .expect_err("should had been a timeout since we are holding the layer resident");

        layer.delete_on_drop();

        drop(resident);

        // make sure the eviction task gets to run
        SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads(&handle).await;

        let resident = layer.keep_resident().await;
        assert!(
            resident.is_none(),
            "keep_resident should not have re-initialized: {resident:?}"
        );

        evict_and_wait
            .await
            .expect("evict_and_wait should had succeeded");

        // works as intended
    }

    // assert that once we remove the `layer` from the layer map and drop our reference,
    // the deletion of the layer in remote_storage happens.
    {
        let mut layers = timeline.layers.write().await;
        layers.open_mut().unwrap().finish_gc_timeline(&[layer]);
    }

    SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads(&handle).await;

    assert_eq!(1, LAYER_IMPL_METRICS.started_deletes.get());
    assert_eq!(1, LAYER_IMPL_METRICS.completed_deletes.get());
    assert_eq!(1, LAYER_IMPL_METRICS.started_evictions.get());
    assert_eq!(1, LAYER_IMPL_METRICS.completed_evictions.get());
    assert_eq!(0, LAYER_IMPL_METRICS.inits_cancelled.get())
}

/// This test ensures we are able to read the layer while the layer eviction has been
/// started but not completed.
#[test]
fn read_wins_pending_eviction() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();

    rt.block_on(async move {
        // this is the runtime on which Layer spawns the blocking tasks on
        let handle = tokio::runtime::Handle::current();
        let h = TenantHarness::create("read_wins_pending_eviction")
            .await
            .unwrap();
        let (tenant, ctx) = h.load().await;
        let span = h.span();
        let download_span = span.in_scope(|| tracing::info_span!("downloading", timeline_id = 1));

        let timeline = tenant
            .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
            .await
            .unwrap();

        let layer = {
            let mut layers = {
                let layers = timeline.layers.read().await;
                layers.likely_resident_layers().cloned().collect::<Vec<_>>()
            };

            assert_eq!(layers.len(), 1);

            layers.swap_remove(0)
        };

        // setup done

        let resident = layer.keep_resident().await.unwrap();

        let mut evict_and_wait = std::pin::pin!(layer.evict_and_wait(FOREVER));

        // drive the future to await on the status channel
        tokio::time::timeout(ADVANCE, &mut evict_and_wait)
            .await
            .expect_err("should had been a timeout since we are holding the layer resident");
        assert_eq!(1, LAYER_IMPL_METRICS.started_evictions.get());

        let (completion, barrier) = utils::completion::channel();
        let (arrival, arrived_at_barrier) = utils::completion::channel();
        layer.enable_failpoint(Failpoint::WaitBeforeStartingEvicting(
            Some(arrival),
            barrier,
        ));

        // now the eviction cannot proceed because the threads are consumed while completion exists
        drop(resident);
        arrived_at_barrier.wait().await;
        assert!(!layer.is_likely_resident());

        // because no actual eviction happened, we get to just reinitialize the DownloadedLayer
        layer
            .0
            .get_or_maybe_download(false, None)
            .instrument(download_span)
            .await
            .expect("should had reinitialized without downloading");

        assert!(layer.is_likely_resident());

        // reinitialization notifies of new resident status, which should error out all evict_and_wait
        let e = tokio::time::timeout(ADVANCE, &mut evict_and_wait)
            .await
            .expect("no timeout, because get_or_maybe_download re-initialized")
            .expect_err("eviction should not have succeeded because re-initialized");

        // works as intended: evictions lose to "downloads"
        assert!(matches!(e, EvictionError::Downloaded), "{e:?}");
        assert_eq!(0, LAYER_IMPL_METRICS.completed_evictions.get());

        // this is not wrong: the eviction is technically still "on the way" as it's still queued
        // because of a failpoint
        assert_eq!(
            0,
            LAYER_IMPL_METRICS
                .cancelled_evictions
                .values()
                .map(|ctr| ctr.get())
                .sum::<u64>()
        );

        drop(completion);

        tokio::time::sleep(ADVANCE).await;
        SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads0(&handle, 1)
            .await;

        assert_eq!(0, LAYER_IMPL_METRICS.completed_evictions.get());

        // now we finally can observe the original eviction failing
        // it would had been possible to observe it earlier, but here it is guaranteed to have
        // happened.
        assert_eq!(
            1,
            LAYER_IMPL_METRICS
                .cancelled_evictions
                .values()
                .map(|ctr| ctr.get())
                .sum::<u64>()
        );

        assert_eq!(
            1,
            LAYER_IMPL_METRICS.cancelled_evictions[EvictionCancelled::AlreadyReinitialized].get()
        );

        assert_eq!(0, LAYER_IMPL_METRICS.inits_cancelled.get())
    });
}

/// Use failpoint to delay an eviction starting to get a VersionCheckFailed.
#[test]
fn multiple_pending_evictions_in_order() {
    let name = "multiple_pending_evictions_in_order";
    let in_order = true;
    multiple_pending_evictions_scenario(name, in_order);
}

/// Use failpoint to reorder later eviction before first to get a UnexpectedEvictedState.
#[test]
fn multiple_pending_evictions_out_of_order() {
    let name = "multiple_pending_evictions_out_of_order";
    let in_order = false;
    multiple_pending_evictions_scenario(name, in_order);
}

fn multiple_pending_evictions_scenario(name: &'static str, in_order: bool) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();

    rt.block_on(async move {
        // this is the runtime on which Layer spawns the blocking tasks on
        let handle = tokio::runtime::Handle::current();
        let h = TenantHarness::create(name).await.unwrap();
        let (tenant, ctx) = h.load().await;
        let span = h.span();
        let download_span = span.in_scope(|| tracing::info_span!("downloading", timeline_id = 1));

        let timeline = tenant
            .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
            .await
            .unwrap();

        let layer = {
            let mut layers = {
                let layers = timeline.layers.read().await;
                layers.likely_resident_layers().cloned().collect::<Vec<_>>()
            };

            assert_eq!(layers.len(), 1);

            layers.swap_remove(0)
        };

        // setup done

        let resident = layer.keep_resident().await.unwrap();

        let mut evict_and_wait = std::pin::pin!(layer.evict_and_wait(FOREVER));

        // drive the future to await on the status channel
        tokio::time::timeout(ADVANCE, &mut evict_and_wait)
            .await
            .expect_err("should had been a timeout since we are holding the layer resident");
        assert_eq!(1, LAYER_IMPL_METRICS.started_evictions.get());

        let (completion1, barrier) = utils::completion::channel();
        let mut completion1 = Some(completion1);
        let (arrival, arrived_at_barrier) = utils::completion::channel();
        layer.enable_failpoint(Failpoint::WaitBeforeStartingEvicting(
            Some(arrival),
            barrier,
        ));

        // now the eviction cannot proceed because we are simulating arbitrary long delay for the
        // eviction task start.
        drop(resident);
        assert!(!layer.is_likely_resident());

        arrived_at_barrier.wait().await;

        // because no actual eviction happened, we get to just reinitialize the DownloadedLayer
        layer
            .0
            .get_or_maybe_download(false, None)
            .instrument(download_span)
            .await
            .expect("should had reinitialized without downloading");

        assert!(layer.is_likely_resident());

        // reinitialization notifies of new resident status, which should error out all evict_and_wait
        let e = tokio::time::timeout(ADVANCE, &mut evict_and_wait)
            .await
            .expect("no timeout, because get_or_maybe_download re-initialized")
            .expect_err("eviction should not have succeeded because re-initialized");

        // works as intended: evictions lose to "downloads"
        assert!(matches!(e, EvictionError::Downloaded), "{e:?}");
        assert_eq!(0, LAYER_IMPL_METRICS.completed_evictions.get());

        // this is not wrong: the eviction is technically still "on the way" as it's still queued
        // because of a failpoint
        assert_eq!(
            0,
            LAYER_IMPL_METRICS
                .cancelled_evictions
                .values()
                .map(|ctr| ctr.get())
                .sum::<u64>()
        );

        assert_eq!(0, LAYER_IMPL_METRICS.completed_evictions.get());

        // configure another failpoint for the second eviction -- evictions are per initialization,
        // so now that we've reinitialized the inner, we get to run two of them at the same time.
        let (completion2, barrier) = utils::completion::channel();
        let (arrival, arrived_at_barrier) = utils::completion::channel();
        layer.enable_failpoint(Failpoint::WaitBeforeStartingEvicting(
            Some(arrival),
            barrier,
        ));

        let mut second_eviction = std::pin::pin!(layer.evict_and_wait(FOREVER));

        // advance to the wait on the queue
        tokio::time::timeout(ADVANCE, &mut second_eviction)
            .await
            .expect_err("timeout because failpoint is blocking");

        arrived_at_barrier.wait().await;

        assert_eq!(2, LAYER_IMPL_METRICS.started_evictions.get());

        let mut release_earlier_eviction = |expected_reason| {
            assert_eq!(
                0,
                LAYER_IMPL_METRICS.cancelled_evictions[expected_reason].get(),
            );

            drop(completion1.take().unwrap());

            let handle = &handle;

            async move {
                tokio::time::sleep(ADVANCE).await;
                SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads0(
                    handle, 1,
                )
                .await;

                assert_eq!(
                    1,
                    LAYER_IMPL_METRICS.cancelled_evictions[expected_reason].get(),
                );
            }
        };

        if in_order {
            release_earlier_eviction(EvictionCancelled::VersionCheckFailed).await;
        }

        // release the later eviction which is for the current version
        drop(completion2);
        tokio::time::sleep(ADVANCE).await;
        SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads0(&handle, 1)
            .await;

        if !in_order {
            release_earlier_eviction(EvictionCancelled::UnexpectedEvictedState).await;
        }

        tokio::time::timeout(ADVANCE, &mut second_eviction)
            .await
            .expect("eviction goes through now that spawn_blocking is unclogged")
            .expect("eviction should succeed, because version matches");

        assert_eq!(1, LAYER_IMPL_METRICS.completed_evictions.get());

        // ensure the cancelled are unchanged
        assert_eq!(
            1,
            LAYER_IMPL_METRICS
                .cancelled_evictions
                .values()
                .map(|ctr| ctr.get())
                .sum::<u64>()
        );

        assert_eq!(0, LAYER_IMPL_METRICS.inits_cancelled.get())
    });
}

/// The test ensures with a failpoint that a pending eviction is not cancelled by what is currently
/// a `Layer::keep_resident` call.
///
/// This matters because cancelling the eviction would leave us in a state where the file is on
/// disk but the layer internal state says it has not been initialized. Futhermore, it allows us to
/// have non-repairing `Layer::is_likely_resident`.
#[tokio::test(start_paused = true)]
async fn cancelled_get_or_maybe_download_does_not_cancel_eviction() {
    let handle = tokio::runtime::Handle::current();
    let h = TenantHarness::create("cancelled_get_or_maybe_download_does_not_cancel_eviction")
        .await
        .unwrap();
    let (tenant, ctx) = h.load().await;

    let timeline = tenant
        .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
        .await
        .unwrap();

    let layer = {
        let mut layers = {
            let layers = timeline.layers.read().await;
            layers.likely_resident_layers().cloned().collect::<Vec<_>>()
        };

        assert_eq!(layers.len(), 1);

        layers.swap_remove(0)
    };

    // this failpoint will simulate the `get_or_maybe_download` becoming cancelled (by returning an
    // Err) at the right time as in "during" the `LayerInner::needs_download`.
    layer.enable_failpoint(Failpoint::AfterDeterminingLayerNeedsNoDownload);

    let (completion, barrier) = utils::completion::channel();
    let (arrival, arrived_at_barrier) = utils::completion::channel();

    layer.enable_failpoint(Failpoint::WaitBeforeStartingEvicting(
        Some(arrival),
        barrier,
    ));

    tokio::time::timeout(ADVANCE, layer.evict_and_wait(FOREVER))
        .await
        .expect_err("should had advanced to waiting on channel");

    arrived_at_barrier.wait().await;

    // simulate a cancelled read which is cancelled before it gets to re-initialize
    let e = layer
        .0
        .get_or_maybe_download(false, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            e,
            DownloadError::Failpoint(FailpointKind::AfterDeterminingLayerNeedsNoDownload)
        ),
        "{e:?}"
    );

    assert!(
        layer.0.needs_download().await.unwrap().is_none(),
        "file is still on disk"
    );

    // release the eviction task
    drop(completion);
    tokio::time::sleep(ADVANCE).await;
    SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads(&handle).await;

    // failpoint is still enabled, but it is not hit
    let e = layer
        .0
        .get_or_maybe_download(false, None)
        .await
        .unwrap_err();
    assert!(matches!(e, DownloadError::DownloadRequired), "{e:?}");

    // failpoint is not counted as cancellation either
    assert_eq!(0, LAYER_IMPL_METRICS.inits_cancelled.get())
}

#[tokio::test(start_paused = true)]
async fn evict_and_wait_does_not_wait_for_download() {
    // let handle = tokio::runtime::Handle::current();
    let h = TenantHarness::create("evict_and_wait_does_not_wait_for_download")
        .await
        .unwrap();
    let (tenant, ctx) = h.load().await;
    let span = h.span();
    let download_span = span.in_scope(|| tracing::info_span!("downloading", timeline_id = 1));

    let timeline = tenant
        .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
        .await
        .unwrap();

    let layer = {
        let mut layers = {
            let layers = timeline.layers.read().await;
            layers.likely_resident_layers().cloned().collect::<Vec<_>>()
        };

        assert_eq!(layers.len(), 1);

        layers.swap_remove(0)
    };

    // kind of forced setup: start an eviction but do not allow it progress until we are
    // downloading
    let (eviction_can_continue, barrier) = utils::completion::channel();
    let (arrival, eviction_arrived) = utils::completion::channel();
    layer.enable_failpoint(Failpoint::WaitBeforeStartingEvicting(
        Some(arrival),
        barrier,
    ));

    let mut evict_and_wait = std::pin::pin!(layer.evict_and_wait(FOREVER));

    // use this once-awaited other_evict to synchronize with the eviction
    let other_evict = layer.evict_and_wait(FOREVER);

    tokio::time::timeout(ADVANCE, &mut evict_and_wait)
        .await
        .expect_err("should had advanced");
    eviction_arrived.wait().await;
    drop(eviction_can_continue);
    other_evict.await.unwrap();

    // now the layer is evicted, and the "evict_and_wait" is waiting on the receiver
    assert!(!layer.is_likely_resident());

    // following new evict_and_wait will fail until we've completed the download
    let e = layer.evict_and_wait(FOREVER).await.unwrap_err();
    assert!(matches!(e, EvictionError::NotFound), "{e:?}");

    let (download_can_continue, barrier) = utils::completion::channel();
    let (arrival, _download_arrived) = utils::completion::channel();
    layer.enable_failpoint(Failpoint::WaitBeforeDownloading(Some(arrival), barrier));

    let mut download = std::pin::pin!(layer
        .0
        .get_or_maybe_download(true, None)
        .instrument(download_span));

    assert!(
        !layer.is_likely_resident(),
        "during download layer is evicted"
    );

    tokio::time::timeout(ADVANCE, &mut download)
        .await
        .expect_err("should had timed out because of failpoint");

    // now we finally get to continue, and because the latest state is downloading, we deduce that
    // original eviction succeeded
    evict_and_wait.await.unwrap();

    // however a new evict_and_wait will fail
    let e = layer.evict_and_wait(FOREVER).await.unwrap_err();
    assert!(matches!(e, EvictionError::NotFound), "{e:?}");

    assert!(!layer.is_likely_resident());

    drop(download_can_continue);
    download.await.expect("download should had succeeded");
    assert!(layer.is_likely_resident());

    // only now can we evict
    layer.evict_and_wait(FOREVER).await.unwrap();
}

/// Asserts that there is no miscalculation when Layer is dropped while it is being kept resident,
/// which is the last value.
///
/// Also checks that the same does not happen on a non-evicted layer (regression test).
#[tokio::test(start_paused = true)]
async fn eviction_cancellation_on_drop() {
    use crate::repository::Value;
    use bytes::Bytes;

    // this is the runtime on which Layer spawns the blocking tasks on
    let handle = tokio::runtime::Handle::current();

    let h = TenantHarness::create("eviction_cancellation_on_drop")
        .await
        .unwrap();
    utils::logging::replace_panic_hook_with_tracing_panic_hook().forget();
    let (tenant, ctx) = h.load().await;

    let timeline = tenant
        .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
        .await
        .unwrap();

    {
        // create_test_timeline wrote us one layer, write another
        let mut writer = timeline.writer().await;
        writer
            .put(
                crate::repository::Key::from_i128(5),
                Lsn(0x20),
                &Value::Image(Bytes::from_static(b"this does not matter either")),
                &ctx,
            )
            .await
            .unwrap();

        writer.finish_write(Lsn(0x20));
    }

    timeline.freeze_and_flush().await.unwrap();

    // wait for the upload to complete so our Arc::strong_count assertion holds
    timeline.remote_client.wait_completion().await.unwrap();

    let (evicted_layer, not_evicted) = {
        let mut layers = {
            let mut guard = timeline.layers.write().await;
            let layers = guard.likely_resident_layers().cloned().collect::<Vec<_>>();
            // remove the layers from layermap
            guard.open_mut().unwrap().finish_gc_timeline(&layers);

            layers
        };

        assert_eq!(layers.len(), 2);

        (layers.pop().unwrap(), layers.pop().unwrap())
    };

    let victims = [(evicted_layer, true), (not_evicted, false)];

    for (victim, evict) in victims {
        let resident = victim.keep_resident().await.unwrap();
        drop(victim);

        assert_eq!(Arc::strong_count(&resident.owner.0), 1);

        if evict {
            let evict_and_wait = resident.owner.evict_and_wait(FOREVER);

            // drive the future to await on the status channel, and then drop it
            tokio::time::timeout(ADVANCE, evict_and_wait)
                .await
                .expect_err("should had been a timeout since we are holding the layer resident");
        }

        // 1 == we only evict one of the layers
        assert_eq!(1, LAYER_IMPL_METRICS.started_evictions.get());

        drop(resident);

        // run any spawned
        tokio::time::sleep(ADVANCE).await;

        SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads(&handle).await;

        assert_eq!(
            1,
            LAYER_IMPL_METRICS.cancelled_evictions[EvictionCancelled::LayerGone].get()
        );
    }
}

/// A test case to remind you the cost of these structures. You can bump the size limit
/// below if it is really necessary to add more fields to the structures.
#[test]
#[cfg(target_arch = "x86_64")]
fn layer_size() {
    assert_eq!(size_of::<LayerAccessStats>(), 8);
    assert_eq!(size_of::<PersistentLayerDesc>(), 104);
    assert_eq!(size_of::<LayerInner>(), 296);
    // it also has the utf8 path
}

struct SpawnBlockingPoolHelper {
    awaited_by_spawn_blocking_tasks: Completion,
    blocking_tasks: JoinSet<()>,
}

impl SpawnBlockingPoolHelper {
    /// All `crate::task_mgr::BACKGROUND_RUNTIME` spawn_blocking threads will be consumed until
    /// release is called.
    ///
    /// In the tests this can be used to ensure something cannot be started on the target runtimes
    /// spawn_blocking pool.
    ///
    /// This should be no issue nowdays, because nextest runs each test in it's own process.
    async fn consume_all_spawn_blocking_threads(handle: &tokio::runtime::Handle) -> Self {
        let default_max_blocking_threads = 512;

        Self::consume_all_spawn_blocking_threads0(handle, default_max_blocking_threads).await
    }

    async fn consume_all_spawn_blocking_threads0(
        handle: &tokio::runtime::Handle,
        threads: usize,
    ) -> Self {
        assert_ne!(threads, 0);

        let (completion, barrier) = completion::channel();
        let (started, starts_completed) = completion::channel();

        let mut blocking_tasks = JoinSet::new();

        for _ in 0..threads {
            let barrier = barrier.clone();
            let started = started.clone();
            blocking_tasks.spawn_blocking_on(
                move || {
                    drop(started);
                    tokio::runtime::Handle::current().block_on(barrier.wait());
                },
                handle,
            );
        }

        drop(started);

        starts_completed.wait().await;

        drop(barrier);

        tracing::trace!("consumed all threads");

        SpawnBlockingPoolHelper {
            awaited_by_spawn_blocking_tasks: completion,
            blocking_tasks,
        }
    }

    /// Release all previously blocked spawn_blocking threads
    async fn release(self) {
        let SpawnBlockingPoolHelper {
            awaited_by_spawn_blocking_tasks,
            mut blocking_tasks,
        } = self;

        drop(awaited_by_spawn_blocking_tasks);

        while let Some(res) = blocking_tasks.join_next().await {
            res.expect("none of the tasks should had panicked");
        }

        tracing::trace!("released all threads");
    }

    /// In the tests it is used as an easy way of making sure something scheduled on the target
    /// runtimes `spawn_blocking` has completed, because it must've been scheduled and completed
    /// before our tasks have a chance to schedule and complete.
    async fn consume_and_release_all_of_spawn_blocking_threads(handle: &tokio::runtime::Handle) {
        Self::consume_and_release_all_of_spawn_blocking_threads0(handle, 512).await
    }

    async fn consume_and_release_all_of_spawn_blocking_threads0(
        handle: &tokio::runtime::Handle,
        threads: usize,
    ) {
        Self::consume_all_spawn_blocking_threads0(handle, threads)
            .await
            .release()
            .await
    }
}

#[test]
fn spawn_blocking_pool_helper_actually_works() {
    // create a custom runtime for which we know and control how many blocking threads it has
    //
    // because the amount is not configurable for our helper, expect the same amount as
    // BACKGROUND_RUNTIME using the tokio defaults would have.
    let rt = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let handle = rt.handle();

    rt.block_on(async move {
        // this will not return until all threads are spun up and actually executing the code
        // waiting on `consumed` to be `SpawnBlockingPoolHelper::release`'d.
        let consumed =
            SpawnBlockingPoolHelper::consume_all_spawn_blocking_threads0(handle, 1).await;

        println!("consumed");

        let mut jh = std::pin::pin!(tokio::task::spawn_blocking(move || {
            // this will not get to run before we release
        }));

        println!("spawned");

        tokio::time::timeout(std::time::Duration::from_secs(1), &mut jh)
            .await
            .expect_err("the task should not have gotten to run yet");

        println!("tried to join");

        consumed.release().await;

        println!("released");

        tokio::time::timeout(std::time::Duration::from_secs(1), jh)
            .await
            .expect("no timeout")
            .expect("no join error");

        println!("joined");
    });
}

/// Drop the low bits from a time, to emulate the precision loss in LayerAccessStats
fn lowres_time(hires: SystemTime) -> SystemTime {
    let ts = hires.duration_since(UNIX_EPOCH).unwrap().as_secs();
    UNIX_EPOCH + Duration::from_secs(ts)
}

#[test]
fn access_stats() {
    let access_stats = LayerAccessStats::default();
    // Default is visible
    assert_eq!(access_stats.visibility(), LayerVisibilityHint::Visible);

    access_stats.set_visibility(LayerVisibilityHint::Covered);
    assert_eq!(access_stats.visibility(), LayerVisibilityHint::Covered);
    access_stats.set_visibility(LayerVisibilityHint::Visible);
    assert_eq!(access_stats.visibility(), LayerVisibilityHint::Visible);

    let rtime = UNIX_EPOCH + Duration::from_secs(2000000000);
    access_stats.record_residence_event_at(rtime);
    assert_eq!(access_stats.latest_activity(), lowres_time(rtime));

    let atime = UNIX_EPOCH + Duration::from_secs(2100000000);
    access_stats.record_access_at(atime);
    assert_eq!(access_stats.latest_activity(), lowres_time(atime));

    // Setting visibility doesn't clobber access time
    access_stats.set_visibility(LayerVisibilityHint::Covered);
    assert_eq!(access_stats.latest_activity(), lowres_time(atime));
    access_stats.set_visibility(LayerVisibilityHint::Visible);
    assert_eq!(access_stats.latest_activity(), lowres_time(atime));
}

#[test]
fn access_stats_2038() {
    // The access stats structure uses a timestamp representation that will run out
    // of bits in 2038.  One year before that, this unit test will start failing.

    let one_year_from_now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
        + Duration::from_secs(3600 * 24 * 365);

    assert!(one_year_from_now.as_secs() < (2 << 31));
}
