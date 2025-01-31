use std::sync::Arc;

use anyhow::{bail, Context};
use remote_storage::RemotePath;
use tokio_util::sync::CancellationToken;
use tracing::{info, info_span, Instrument};
use utils::lsn::Lsn;

use crate::{context::RequestContext, tenant::metadata::TimelineMetadata};

use super::Timeline;

mod flow;
mod importbucket_client;
mod importbucket_format;
pub(crate) mod index_part_format;
pub(crate) mod upcall_api;

pub async fn doit(
    timeline: &Arc<Timeline>,
    index_part: index_part_format::Root,
    ctx: &RequestContext,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let index_part_format::Root::V1(v1) = index_part;
    let index_part_format::InProgress {
        location,
        idempotency_key,
        started_at,
    } = match v1 {
        index_part_format::V1::Done(_) => return Ok(()),
        index_part_format::V1::InProgress(in_progress) => in_progress,
    };

    let storage = importbucket_client::new(timeline.conf, &location, cancel.clone()).await?;

    info!("get spec early so we know we'll be able to upcall when done");
    let Some(spec) = storage.get_spec().await? else {
        bail!("spec not found")
    };

    let upcall_client =
        upcall_api::Client::new(timeline.conf, cancel.clone()).context("create upcall client")?;

    //
    // send an early progress update to clean up k8s job early and generate potentially useful logs
    //
    info!("send early progress update");
    upcall_client
        .send_progress_until_success(&spec)
        .instrument(info_span!("early_progress_update"))
        .await?;

    let status_prefix = RemotePath::from_string("status").unwrap();

    //
    // See if shard is done.
    // TODO: incorporate generations into status key for split brain safety. Figure out together with checkpointing.
    //
    let shard_status_key =
        status_prefix.join(format!("shard-{}", timeline.tenant_shard_id.shard_slug()));
    let shard_status: Option<importbucket_format::ShardStatus> =
        storage.get_json(&shard_status_key).await?;
    info!(?shard_status, "peeking shard status");
    if shard_status.map(|st| st.done).unwrap_or(false) {
        info!("shard status indicates that the shard is done, skipping import");
    } else {
        // TODO: checkpoint the progress into the IndexPart instead of restarting
        // from the beginning.

        //
        // Wipe the slate clean - the flow does not allow resuming.
        // We can implement resuming in the future by checkpointing the progress into the IndexPart.
        //
        info!("wipe the slate clean");
        {
            // TODO: do we need to hold GC lock for this?
            let mut guard = timeline.layers.write().await;
            assert!(
                guard.layer_map()?.open_layer.is_none(),
                "while importing, there should be no in-memory layer" // this just seems like a good place to assert it
            );
            let all_layers_keys = guard.all_persistent_layers();
            let all_layers: Vec<_> = all_layers_keys
                .iter()
                .map(|key| guard.get_from_key(key))
                .collect();
            let open = guard.open_mut().context("open_mut")?;

            timeline.remote_client.schedule_gc_update(&all_layers)?;
            open.finish_gc_timeline(&all_layers);
        }

        //
        // Wait for pgdata to finish uploading
        //
        info!("wait for pgdata to reach status 'done'");
        let pgdata_status_key = status_prefix.join("pgdata");
        loop {
            let res = async {
                let pgdata_status: Option<importbucket_format::PgdataStatus> = storage
                    .get_json(&pgdata_status_key)
                    .await
                    .context("get pgdata status")?;
                info!(?pgdata_status, "peeking pgdata status");
                if pgdata_status.map(|st| st.done).unwrap_or(false) {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("pgdata not done yet"))
                }
            }
            .await;
            match res {
                Ok(_) => break,
                Err(err) => {
                    info!(?err, "indefinitely waiting for pgdata to finish");
                    if tokio::time::timeout(std::time::Duration::from_secs(10), cancel.cancelled())
                        .await
                        .is_ok()
                    {
                        bail!("cancelled while waiting for pgdata");
                    }
                }
            }
        }

        //
        // Do the import
        //
        info!("do the import");
        let control_file = storage.get_control_file().await?;
        let base_lsn = control_file.base_lsn();

        info!("update TimelineMetadata based on LSNs from control file");
        {
            let pg_version = control_file.pg_version();
            let _ctx: &RequestContext = ctx;
            async move {
                // FIXME: The 'disk_consistent_lsn' should be the LSN at the *end* of the
                // checkpoint record, and prev_record_lsn should point to its beginning.
                // We should read the real end of the record from the WAL, but here we
                // just fake it.
                let disk_consistent_lsn = Lsn(base_lsn.0 + 8);
                let prev_record_lsn = base_lsn;
                let metadata = TimelineMetadata::new(
                    disk_consistent_lsn,
                    Some(prev_record_lsn),
                    None,     // no ancestor
                    Lsn(0),   // no ancestor lsn
                    base_lsn, // latest_gc_cutoff_lsn
                    base_lsn, // initdb_lsn
                    pg_version,
                );

                let _start_lsn = disk_consistent_lsn + 1;

                timeline
                    .remote_client
                    .schedule_index_upload_for_full_metadata_update(&metadata)?;

                timeline.remote_client.wait_completion().await?;

                anyhow::Ok(())
            }
        }
        .await?;

        flow::run(
            timeline.clone(),
            base_lsn,
            control_file,
            storage.clone(),
            ctx,
        )
        .await?;

        //
        // Communicate that shard is done.
        //
        storage
            .put_json(
                &shard_status_key,
                &importbucket_format::ShardStatus { done: true },
            )
            .await
            .context("put shard status")?;
    }

    //
    // Ensure at-least-once deliver of the upcall to cplane
    // before we mark the task as done and never come here again.
    //
    info!("send final progress update");
    upcall_client
        .send_progress_until_success(&spec)
        .instrument(info_span!("final_progress_update"))
        .await?;

    //
    // Mark as done in index_part.
    // This makes subsequent timeline loads enter the normal load code path
    // instead of spawning the import task and calling this here function.
    //
    info!("mark import as complete in index part");
    timeline
        .remote_client
        .schedule_index_upload_for_import_pgdata_state_update(Some(index_part_format::Root::V1(
            index_part_format::V1::Done(index_part_format::Done {
                idempotency_key,
                started_at,
                finished_at: chrono::Utc::now().naive_utc(),
            }),
        )))?;

    timeline.remote_client.wait_completion().await?;

    Ok(())
}
