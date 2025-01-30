use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::{
    failpoint_support,
    id::{TenantId, TimelineId},
};

use crate::{
    id_lock_map::trace_shared_lock,
    persistence::SafekeeperPersistence,
    service::{TenantOperations, TimelineStatusCreating, TimelineStatusKind},
};

use super::{Service, TimelinePersistence};

pub struct SafekeeperReconciler {
    service: Arc<Service>,
    duration: Duration,
}

impl SafekeeperReconciler {
    pub fn new(service: Arc<Service>, duration: Duration) -> Self {
        SafekeeperReconciler { service, duration }
    }
    pub async fn run(&self, cancel: CancellationToken) {
        while !cancel.is_cancelled() {
            tokio::select! {
                _ = tokio::time::sleep(self.duration) => (),
                _ = cancel.cancelled() => break,
            }
            match self.reconcile_iteration(&cancel).await {
                Ok(()) => (),
                Err(e) => {
                    tracing::warn!("Error during safekeeper reconciliation: {e:?}");
                }
            }
        }
    }
    async fn reconcile_iteration(&self, cancel: &CancellationToken) -> Result<(), anyhow::Error> {
        let work_list = self
            .service
            .persistence
            .timelines_to_be_reconciled()
            .await?;
        if work_list.is_empty() {
            return Ok(());
        }
        let sk_persistences = self
            .service
            .persistence
            .list_safekeepers()
            .await?
            .into_iter()
            .map(|p| (p.id, p))
            .collect::<HashMap<_, _>>();
        for tl in work_list {
            let reconcile_fut =
                self.reconcile_timeline(&tl, &sk_persistences)
                    .instrument(tracing::info_span!(
                        "safekeeper_reconcile_timeline",
                        timeline_id = tl.timeline_id,
                        tenant_id = tl.tenant_id
                    ));

            tokio::select! {
                r = reconcile_fut => r?,
                _ = cancel.cancelled() => break,
            }
        }
        Ok(())
    }
    async fn reconcile_timeline(
        &self,
        tl: &TimelinePersistence,
        sk_persistences: &HashMap<i64, SafekeeperPersistence>,
    ) -> Result<(), anyhow::Error> {
        tracing::info!("Reconciling timeline on safekeepers");
        let tenant_id = TenantId::from_slice(tl.tenant_id.as_bytes())?;
        let timeline_id = TimelineId::from_slice(tl.timeline_id.as_bytes())?;

        let _tenant_lock = trace_shared_lock(
            &self.service.tenant_op_locks,
            tenant_id,
            TenantOperations::TimelineReconcile,
        )
        .await;

        failpoint_support::sleep_millis_async!("safekeeper-reconcile-timeline-shared-lock");
        // Load the timeline again from the db: unless we hold the tenant lock, the timeline can change under our noses.
        let tl = self
            .service
            .persistence
            .get_timeline(tenant_id, timeline_id)
            .await?;
        let Some(tl) = tl else {
            // This can happen but is a bit unlikely, so print it on the warn level instead of info
            tracing::warn!("timeline row in database disappeared");
            return Ok(());
        };
        let status = TimelineStatusKind::from_str(&tl.status)?;
        match status {
            TimelineStatusKind::Created | TimelineStatusKind::Deleted => (),
            TimelineStatusKind::Creating => {
                let status_creating: TimelineStatusCreating = serde_json::from_str(&tl.status)?;
                self.service
                    .tenant_timeline_create_safekeepers_reconcile(
                        tenant_id,
                        timeline_id,
                        &tl,
                        &status_creating,
                        sk_persistences,
                    )
                    .await?;
            }
            TimelineStatusKind::Deleting => {
                self.service
                    .tenant_timeline_delete_safekeepers_reconcile(
                        tenant_id,
                        timeline_id,
                        &tl,
                        sk_persistences,
                    )
                    .await?;
            }
        }
        Ok(())
    }
}
