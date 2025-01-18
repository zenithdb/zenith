use crate::tenant::mgr::TenantShardManager;
use crate::{context::RequestContext, tenant::timeline::logical_size::CurrentLogicalSize};
use chrono::{DateTime, Utc};
use consumption_metrics::EventType;
use futures::stream::StreamExt;
use std::{sync::Arc, time::SystemTime};
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use super::{Cache, NewRawMetric};

/// Name of the metric, used by `MetricsKey` factory methods and `deserialize_cached_events`
/// instead of static str.
// Do not rename any of these without first consulting with data team and partner
// management.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(super) enum Name {
    /// Timeline last_record_lsn, absolute
    #[serde(rename = "written_size")]
    WrittenSize,
    /// Timeline last_record_lsn, incremental
    #[serde(rename = "written_data_bytes_delta")]
    WrittenSizeDelta,
    /// Timeline logical size
    #[serde(rename = "timeline_logical_size")]
    LogicalSize,
    /// Tenant remote size
    #[serde(rename = "remote_storage_size")]
    RemoteSize,
    /// Tenant resident size
    #[serde(rename = "resident_size")]
    ResidentSize,
    /// Tenant synthetic size
    #[serde(rename = "synthetic_storage_size")]
    SyntheticSize,
}

/// Key that uniquely identifies the object this metric describes.
///
/// This is a denormalization done at the MetricsKey const methods; these should not be constructed
/// elsewhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(crate) struct MetricsKey {
    pub(super) tenant_id: TenantId,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) timeline_id: Option<TimelineId>,

    pub(super) metric: Name,
}

impl MetricsKey {
    const fn absolute_values(self) -> AbsoluteValueFactory {
        AbsoluteValueFactory(self)
    }
    const fn incremental_values(self) -> IncrementalValueFactory {
        IncrementalValueFactory(self)
    }
}

/// Helper type which each individual metric kind can return to produce only absolute values.
struct AbsoluteValueFactory(MetricsKey);

impl AbsoluteValueFactory {
    #[cfg(test)]
    const fn at_old_format(self, time: DateTime<Utc>, val: u64) -> super::RawMetric {
        let key = self.0;
        (key, (EventType::Absolute { time }, val))
    }

    const fn at(self, time: DateTime<Utc>, val: u64) -> NewRawMetric {
        let key = self.0;
        NewRawMetric {
            key,
            kind: EventType::Absolute { time },
            value: val,
        }
    }

    fn key(&self) -> &MetricsKey {
        &self.0
    }
}

/// Helper type which each individual metric kind can return to produce only incremental values.
struct IncrementalValueFactory(MetricsKey);

impl IncrementalValueFactory {
    #[allow(clippy::wrong_self_convention)]
    const fn from_until(
        self,
        prev_end: DateTime<Utc>,
        up_to: DateTime<Utc>,
        val: u64,
    ) -> NewRawMetric {
        let key = self.0;
        // cannot assert prev_end < up_to because these are realtime clock based
        let when = EventType::Incremental {
            start_time: prev_end,
            stop_time: up_to,
        };
        NewRawMetric {
            key,
            kind: when,
            value: val,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    #[cfg(test)]
    const fn from_until_old_format(
        self,
        prev_end: DateTime<Utc>,
        up_to: DateTime<Utc>,
        val: u64,
    ) -> super::RawMetric {
        let key = self.0;
        // cannot assert prev_end < up_to because these are realtime clock based
        let when = EventType::Incremental {
            start_time: prev_end,
            stop_time: up_to,
        };
        (key, (when, val))
    }

    fn key(&self) -> &MetricsKey {
        &self.0
    }
}

// the static part of a MetricsKey
impl MetricsKey {
    /// Absolute value of [`Timeline::get_last_record_lsn`].
    ///
    /// [`Timeline::get_last_record_lsn`]: crate::tenant::Timeline::get_last_record_lsn
    const fn written_size(tenant_id: TenantId, timeline_id: TimelineId) -> AbsoluteValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: Name::WrittenSize,
        }
        .absolute_values()
    }

    /// Values will be the difference of the latest [`MetricsKey::written_size`] to what we
    /// previously sent, starting from the previously sent incremental time range ending at the
    /// latest absolute measurement.
    const fn written_size_delta(
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> IncrementalValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: Name::WrittenSizeDelta,
        }
        .incremental_values()
    }

    /// Exact [`Timeline::get_current_logical_size`].
    ///
    /// [`Timeline::get_current_logical_size`]: crate::tenant::Timeline::get_current_logical_size
    const fn timeline_logical_size(
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> AbsoluteValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: Some(timeline_id),
            metric: Name::LogicalSize,
        }
        .absolute_values()
    }

    /// [`TenantShard::remote_size`]
    ///
    /// [`TenantShard::remote_size`]: crate::tenant::TenantShard::remote_size
    const fn remote_storage_size(tenant_id: TenantId) -> AbsoluteValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: None,
            metric: Name::RemoteSize,
        }
        .absolute_values()
    }

    /// Sum of [`Timeline::resident_physical_size`] for each `Tenant`.
    ///
    /// [`Timeline::resident_physical_size`]: crate::tenant::Timeline::resident_physical_size
    const fn resident_size(tenant_id: TenantId) -> AbsoluteValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: None,
            metric: Name::ResidentSize,
        }
        .absolute_values()
    }

    /// [`TenantShard::cached_synthetic_size`] as refreshed by [`calculate_synthetic_size_worker`].
    ///
    /// [`TenantShard::cached_synthetic_size`]: crate::tenant::TenantShard::cached_synthetic_size
    /// [`calculate_synthetic_size_worker`]: super::calculate_synthetic_size_worker
    const fn synthetic_size(tenant_id: TenantId) -> AbsoluteValueFactory {
        MetricsKey {
            tenant_id,
            timeline_id: None,
            metric: Name::SyntheticSize,
        }
        .absolute_values()
    }
}

pub(super) async fn collect_all_metrics(
    tenant_manager: &Arc<TenantShardManager>,
    cached_metrics: &Cache,
    ctx: &RequestContext,
) -> Vec<NewRawMetric> {
    use pageserver_api::models::TenantState;

    let started_at = std::time::Instant::now();

    let tenants = match tenant_manager.list_tenants() {
        Ok(tenants) => tenants,
        Err(err) => {
            tracing::error!("failed to list tenants: {:?}", err);
            return vec![];
        }
    };

    let tenants = futures::stream::iter(tenants).filter_map(|(id, state, _)| async move {
        if state != TenantState::Active || !id.is_shard_zero() {
            None
        } else {
            tenant_manager
                .get_attached_tenant_shard(id)
                .ok()
                .map(|tenant| (id.tenant_id, tenant))
        }
    });

    let res = collect(tenants, cached_metrics, ctx).await;

    tracing::info!(
        elapsed_ms = started_at.elapsed().as_millis(),
        total = res.len(),
        "collected metrics"
    );

    res
}

async fn collect<S>(tenants: S, cache: &Cache, ctx: &RequestContext) -> Vec<NewRawMetric>
where
    S: futures::stream::Stream<Item = (TenantId, Arc<crate::tenant::TenantShard>)>,
{
    let mut current_metrics: Vec<NewRawMetric> = Vec::new();

    let mut tenants = std::pin::pin!(tenants);

    while let Some((tenant_id, tenant)) = tenants.next().await {
        let mut tenant_resident_size = 0;

        for timeline in tenant.list_timelines() {
            let timeline_id = timeline.timeline_id;

            match TimelineSnapshot::collect(&timeline, ctx) {
                Ok(Some(snap)) => {
                    snap.to_metrics(
                        tenant_id,
                        timeline_id,
                        Utc::now(),
                        &mut current_metrics,
                        cache,
                    );
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(
                        "failed to get metrics values for tenant {tenant_id} timeline {}: {e:#?}",
                        timeline.timeline_id
                    );
                    continue;
                }
            }

            tenant_resident_size += timeline.resident_physical_size();
        }

        let snap = TenantSnapshot::collect(&tenant, tenant_resident_size);
        snap.to_metrics(tenant_id, Utc::now(), cache, &mut current_metrics);
    }

    current_metrics
}

/// In-between abstraction to allow testing metrics without actual Tenants.
struct TenantSnapshot {
    resident_size: u64,
    remote_size: u64,
    synthetic_size: u64,
}

impl TenantSnapshot {
    /// Collect tenant status to have metrics created out of it.
    ///
    /// `resident_size` is calculated of the timelines we had access to for other metrics, so we
    /// cannot just list timelines here.
    fn collect(t: &Arc<crate::tenant::TenantShard>, resident_size: u64) -> Self {
        TenantSnapshot {
            resident_size,
            remote_size: t.remote_size(),
            // Note that this metric is calculated in a separate bgworker
            // Here we only use cached value, which may lag behind the real latest one
            synthetic_size: t.cached_synthetic_size(),
        }
    }

    fn to_metrics(
        &self,
        tenant_id: TenantId,
        now: DateTime<Utc>,
        cached: &Cache,
        metrics: &mut Vec<NewRawMetric>,
    ) {
        let remote_size = MetricsKey::remote_storage_size(tenant_id).at(now, self.remote_size);

        let resident_size = MetricsKey::resident_size(tenant_id).at(now, self.resident_size);

        let synthetic_size = {
            let factory = MetricsKey::synthetic_size(tenant_id);
            let mut synthetic_size = self.synthetic_size;

            if synthetic_size == 0 {
                if let Some(item) = cached.get(factory.key()) {
                    // use the latest value from previous session, TODO: check generation number
                    synthetic_size = item.value;
                }
            }

            if synthetic_size != 0 {
                // only send non-zeroes because otherwise these show up as errors in logs
                Some(factory.at(now, synthetic_size))
            } else {
                None
            }
        };

        metrics.extend(
            [Some(remote_size), Some(resident_size), synthetic_size]
                .into_iter()
                .flatten(),
        );
    }
}

/// Internal type to make timeline metric production testable.
///
/// As this value type contains all of the information needed from a timeline to produce the
/// metrics, it can easily be created with different values in test.
struct TimelineSnapshot {
    loaded_at: (Lsn, SystemTime),
    last_record_lsn: Lsn,
    current_exact_logical_size: Option<u64>,
}

impl TimelineSnapshot {
    /// Collect the metrics from an actual timeline.
    ///
    /// Fails currently only when [`Timeline::get_current_logical_size`] fails.
    ///
    /// [`Timeline::get_current_logical_size`]: crate::tenant::Timeline::get_current_logical_size
    fn collect(
        t: &Arc<crate::tenant::Timeline>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Option<Self>> {
        if !t.is_active() {
            // no collection for broken or stopping needed, we will still keep the cached values
            // though at the caller.
            Ok(None)
        } else {
            let loaded_at = t.loaded_at;
            let last_record_lsn = t.get_last_record_lsn();

            let current_exact_logical_size = {
                let span = tracing::info_span!("collect_metrics_iteration", tenant_id = %t.tenant_shard_id.tenant_id, timeline_id = %t.timeline_id);
                let size = span.in_scope(|| {
                    t.get_current_logical_size(
                        crate::tenant::timeline::GetLogicalSizePriority::Background,
                        ctx,
                    )
                });
                match size {
                    // Only send timeline logical size when it is fully calculated.
                    CurrentLogicalSize::Exact(ref size) => Some(size.into()),
                    CurrentLogicalSize::Approximate(_) => None,
                }
            };

            Ok(Some(TimelineSnapshot {
                loaded_at,
                last_record_lsn,
                current_exact_logical_size,
            }))
        }
    }

    /// Produce the timeline consumption metrics into the `metrics` argument.
    fn to_metrics(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        now: DateTime<Utc>,
        metrics: &mut Vec<NewRawMetric>,
        cache: &Cache,
    ) {
        let timeline_written_size = u64::from(self.last_record_lsn);

        let written_size_delta_key = MetricsKey::written_size_delta(tenant_id, timeline_id);

        let last_stop_time = cache.get(written_size_delta_key.key()).map(|item| {
            item.kind
                .incremental_timerange()
                .expect("never create EventType::Absolute for written_size_delta")
                .end
        });

        let written_size_now =
            MetricsKey::written_size(tenant_id, timeline_id).at(now, timeline_written_size);

        // by default, use the last sent written_size as the basis for
        // calculating the delta. if we don't yet have one, use the load time value.
        let prev: (DateTime<Utc>, u64) = cache
            .get(&written_size_now.key)
            .map(|item| {
                // use the prev time from our last incremental update, or default to latest
                // absolute update on the first round.
                let prev_at = item
                    .kind
                    .absolute_time()
                    .expect("never create EventType::Incremental for written_size");
                let prev_at = last_stop_time.unwrap_or(prev_at);
                (*prev_at, item.value)
            })
            .unwrap_or_else(|| {
                // if we don't have a previous point of comparison, compare to the load time
                // lsn.
                let (disk_consistent_lsn, loaded_at) = &self.loaded_at;
                (DateTime::from(*loaded_at), disk_consistent_lsn.0)
            });

        let up_to = now;

        if let Some(delta) = written_size_now.value.checked_sub(prev.1) {
            let key_value = written_size_delta_key.from_until(prev.0, up_to, delta);
            // written_size_delta
            metrics.push(key_value);
            // written_size
            metrics.push(written_size_now);
        } else {
            // the cached value was ahead of us, report zero until we've caught up
            metrics.push(written_size_delta_key.from_until(prev.0, up_to, 0));
            // the cached value was ahead of us, report the same until we've caught up
            metrics.push(NewRawMetric {
                key: written_size_now.key,
                kind: written_size_now.kind,
                value: prev.1,
            });
        }

        {
            let factory = MetricsKey::timeline_logical_size(tenant_id, timeline_id);
            let current_or_previous = self
                .current_exact_logical_size
                .or_else(|| cache.get(factory.key()).map(|item| item.value));

            if let Some(size) = current_or_previous {
                metrics.push(factory.at(now, size));
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use tests::{metric_examples, metric_examples_old};
