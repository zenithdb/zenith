//! Periodically collect proxy consumption metrics
//! and push them to a HTTP endpoint.
use crate::{
    config::{MetricBackupCollectionConfig, MetricCollectionConfig},
    context::parquet::{FAILED_UPLOAD_MAX_RETRIES, FAILED_UPLOAD_WARN_THRESHOLD},
    dns::Dns,
    http,
    intern::{BranchIdInt, EndpointIdInt},
};
use anyhow::Context;
use async_compression::tokio::write::GzipEncoder;
use bytes::Bytes;
use chrono::{DateTime, Datelike, Timelike, Utc};
use consumption_metrics::{idempotency_key, Event, EventChunk, EventType, CHUNK_SIZE};
use dashmap::{mapref::entry::Entry, DashMap};
use futures::future::select;
use once_cell::sync::Lazy;
use remote_storage::{GenericRemoteStorage, RemotePath, TimeoutOrCancel};
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    pin::pin,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, trace};
use utils::backoff;
use uuid::{NoContext, Timestamp};

const PROXY_IO_BYTES_PER_CLIENT: &str = "proxy_io_bytes_per_client";

const DEFAULT_HTTP_REPORTING_TIMEOUT: Duration = Duration::from_secs(60);

/// Key that uniquely identifies the object, this metric describes.
/// Currently, endpoint_id is enough, but this may change later,
/// so keep it in a named struct.
///
/// Both the proxy and the ingestion endpoint will live in the same region (or cell)
/// so while the project-id is unique across regions the whole pipeline will work correctly
/// because we enrich the event with project_id in the control-plane endpoint.
#[derive(Eq, Hash, PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct Ids {
    pub endpoint_id: EndpointIdInt,
    pub branch_id: BranchIdInt,
}

pub trait MetricCounterRecorder {
    /// Record that some bytes were sent from the proxy to the client
    fn record_egress(&self, bytes: u64);
    /// Record that some connections were opened
    fn record_connection(&self, count: usize);
}

trait MetricCounterReporter {
    fn get_metrics(&mut self) -> (u64, usize);
    fn move_metrics(&self) -> (u64, usize);
}

#[derive(Debug)]
struct MetricBackupCounter {
    transmitted: AtomicU64,
    opened_connections: AtomicUsize,
}

impl MetricCounterRecorder for MetricBackupCounter {
    fn record_egress(&self, bytes: u64) {
        self.transmitted.fetch_add(bytes, Ordering::AcqRel);
    }

    fn record_connection(&self, count: usize) {
        self.opened_connections.fetch_add(count, Ordering::AcqRel);
    }
}

impl MetricCounterReporter for MetricBackupCounter {
    fn get_metrics(&mut self) -> (u64, usize) {
        (
            *self.transmitted.get_mut(),
            *self.opened_connections.get_mut(),
        )
    }
    fn move_metrics(&self) -> (u64, usize) {
        (
            self.transmitted.swap(0, Ordering::AcqRel),
            self.opened_connections.swap(0, Ordering::AcqRel),
        )
    }
}

#[derive(Debug)]
pub struct MetricCounter {
    transmitted: AtomicU64,
    opened_connections: AtomicUsize,
    backup: Arc<MetricBackupCounter>,
}

impl MetricCounterRecorder for MetricCounter {
    /// Record that some bytes were sent from the proxy to the client
    fn record_egress(&self, bytes: u64) {
        self.transmitted.fetch_add(bytes, Ordering::AcqRel);
        self.backup.record_egress(bytes);
    }

    /// Record that some connections were opened
    fn record_connection(&self, count: usize) {
        self.opened_connections.fetch_add(count, Ordering::AcqRel);
        self.backup.record_connection(count);
    }
}

impl MetricCounterReporter for MetricCounter {
    fn get_metrics(&mut self) -> (u64, usize) {
        (
            *self.transmitted.get_mut(),
            *self.opened_connections.get_mut(),
        )
    }
    fn move_metrics(&self) -> (u64, usize) {
        (
            self.transmitted.swap(0, Ordering::AcqRel),
            self.opened_connections.swap(0, Ordering::AcqRel),
        )
    }
}

trait Clearable {
    /// extract the value that should be reported
    fn should_report(self: &Arc<Self>) -> Option<u64>;
    /// Determine whether the counter should be cleared from the global map.
    fn should_clear(self: &mut Arc<Self>) -> bool;
}

impl<C: MetricCounterReporter> Clearable for C {
    fn should_report(self: &Arc<Self>) -> Option<u64> {
        // heuristic to see if the branch is still open
        // if a clone happens while we are observing, the heuristic will be incorrect.
        //
        // Worst case is that we won't report an event for this endpoint.
        // However, for the strong count to be 1 it must have occured that at one instant
        // all the endpoints were closed, so missing a report because the endpoints are closed is valid.
        let is_open = Arc::strong_count(self) > 1;

        // update cached metrics eagerly, even if they can't get sent
        // (to avoid sending the same metrics twice)
        // see the relevant discussion on why to do so even if the status is not success:
        // https://github.com/neondatabase/neon/pull/4563#discussion_r1246710956
        let (value, opened) = self.move_metrics();

        // Our only requirement is that we report in every interval if there was an open connection
        // if there were no opened connections since, then we don't need to report
        if value == 0 && !is_open && opened == 0 {
            None
        } else {
            Some(value)
        }
    }
    fn should_clear(self: &mut Arc<Self>) -> bool {
        // we can't clear this entry if it's acquired elsewhere
        let Some(counter) = Arc::get_mut(self) else {
            return false;
        };
        let (opened, value) = counter.get_metrics();
        // clear if there's no data to report
        value == 0 && opened == 0
    }
}

// endpoint and branch IDs are not user generated so we don't run the risk of hash-dos
type FastHasher = std::hash::BuildHasherDefault<rustc_hash::FxHasher>;

#[derive(Default)]
pub struct Metrics {
    endpoints: DashMap<Ids, Arc<MetricCounter>, FastHasher>,
    backup_endpoints: DashMap<Ids, Arc<MetricBackupCounter>, FastHasher>,
}

impl Metrics {
    /// Register a new byte metrics counter for this endpoint
    pub fn register(&self, ids: Ids) -> Arc<MetricCounter> {
        let backup = if let Some(entry) = self.backup_endpoints.get(&ids) {
            entry.clone()
        } else {
            self.backup_endpoints
                .entry(ids.clone())
                .or_insert_with(|| {
                    Arc::new(MetricBackupCounter {
                        transmitted: AtomicU64::new(0),
                        opened_connections: AtomicUsize::new(0),
                    })
                })
                .clone()
        };

        let entry = if let Some(entry) = self.endpoints.get(&ids) {
            entry.clone()
        } else {
            self.endpoints
                .entry(ids)
                .or_insert_with(|| {
                    Arc::new(MetricCounter {
                        transmitted: AtomicU64::new(0),
                        opened_connections: AtomicUsize::new(0),
                        backup: backup.clone(),
                    })
                })
                .clone()
        };

        entry.record_connection(1);
        entry
    }
}

pub static USAGE_METRICS: Lazy<Metrics> = Lazy::new(Metrics::default);

pub async fn task_main(dns: Dns, config: &MetricCollectionConfig) -> anyhow::Result<Infallible> {
    info!("metrics collector config: {config:?}");
    scopeguard::defer! {
        info!("metrics collector has shut down");
    }

    let http_client = http::new_client_with_timeout(dns, DEFAULT_HTTP_REPORTING_TIMEOUT);
    let hostname = hostname::get()?.as_os_str().to_string_lossy().into_owned();

    let mut prev = Utc::now();
    let mut ticker = tokio::time::interval(config.interval);
    loop {
        ticker.tick().await;

        let now = Utc::now();
        collect_metrics_iteration(
            &USAGE_METRICS.endpoints,
            &http_client,
            &config.endpoint,
            &hostname,
            prev,
            now,
        )
        .await;
        prev = now;
    }
}

fn collect_and_clear_metrics<C: Clearable>(
    endpoints: &DashMap<Ids, Arc<C>, FastHasher>,
) -> Vec<(Ids, u64)> {
    let mut metrics_to_clear = Vec::new();

    let metrics_to_send: Vec<(Ids, u64)> = endpoints
        .iter()
        .filter_map(|counter| {
            let key = counter.key().clone();
            let Some(value) = counter.should_report() else {
                metrics_to_clear.push(key);
                return None;
            };
            Some((key, value))
        })
        .collect();

    for metric in metrics_to_clear {
        match endpoints.entry(metric) {
            Entry::Occupied(mut counter) => {
                if counter.get_mut().should_clear() {
                    counter.remove_entry();
                }
            }
            Entry::Vacant(_) => {}
        }
    }
    metrics_to_send
}

fn create_event_chunks<'a>(
    metrics_to_send: &'a [(Ids, u64)],
    hostname: &'a str,
    prev: DateTime<Utc>,
    now: DateTime<Utc>,
    chunk_size: usize,
) -> impl Iterator<Item = EventChunk<'a, Event<Ids, &'static str>>> + 'a {
    // Split into chunks of 1000 metrics to avoid exceeding the max request size
    metrics_to_send
        .chunks(chunk_size)
        .map(move |chunk| EventChunk {
            events: chunk
                .iter()
                .map(|(ids, value)| Event {
                    kind: EventType::Incremental {
                        start_time: prev,
                        stop_time: now,
                    },
                    metric: PROXY_IO_BYTES_PER_CLIENT,
                    idempotency_key: idempotency_key(hostname),
                    value: *value,
                    extra: ids.clone(),
                })
                .collect(),
        })
}

#[instrument(skip_all)]
async fn collect_metrics_iteration(
    endpoints: &DashMap<Ids, Arc<MetricCounter>, FastHasher>,
    client: &http::ClientWithMiddleware,
    metric_collection_endpoint: &reqwest::Url,
    hostname: &str,
    prev: DateTime<Utc>,
    now: DateTime<Utc>,
) {
    info!(
        "starting collect_metrics_iteration. metric_collection_endpoint: {}",
        metric_collection_endpoint
    );

    let metrics_to_send = collect_and_clear_metrics(endpoints);

    if metrics_to_send.is_empty() {
        trace!("no new metrics to send");
    }

    // Send metrics.
    for chunk in create_event_chunks(&metrics_to_send, hostname, prev, now, CHUNK_SIZE) {
        let res = client
            .post(metric_collection_endpoint.clone())
            .json(&chunk)
            .send()
            .await;

        let res = match res {
            Ok(x) => x,
            Err(err) => {
                error!("failed to send metrics: {:?}", err);
                continue;
            }
        };

        if !res.status().is_success() {
            error!("metrics endpoint refused the sent metrics: {:?}", res);
            for metric in chunk.events.iter().filter(|e| e.value > (1u64 << 40)) {
                // Report if the metric value is suspiciously large
                error!("potentially abnormal metric value: {:?}", metric);
            }
        }
    }
}

pub async fn task_backup(
    backup_config: &MetricBackupCollectionConfig,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    info!("metrics backup config: {backup_config:?}");
    scopeguard::defer! {
        info!("metrics backup has shut down");
    }
    // Even if the remote storage is not configured, we still want to clear the metrics.
    let storage = backup_config
        .remote_storage_config
        .as_ref()
        .map(|config| GenericRemoteStorage::from_config(config).context("remote storage init"))
        .transpose()?;
    let mut ticker = tokio::time::interval(backup_config.interval);
    let mut prev = Utc::now();
    let hostname = hostname::get()?.as_os_str().to_string_lossy().into_owned();
    loop {
        select(pin!(ticker.tick()), pin!(cancellation_token.cancelled())).await;
        let now = Utc::now();
        collect_metrics_backup_iteration(
            &USAGE_METRICS.backup_endpoints,
            &storage,
            &hostname,
            prev,
            now,
            backup_config.chunk_size,
        )
        .await;

        prev = now;
        if cancellation_token.is_cancelled() {
            info!("metrics backup has been cancelled");
            break;
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn collect_metrics_backup_iteration(
    endpoints: &DashMap<Ids, Arc<MetricBackupCounter>, FastHasher>,
    storage: &Option<GenericRemoteStorage>,
    hostname: &str,
    prev: DateTime<Utc>,
    now: DateTime<Utc>,
    chunk_size: usize,
) {
    let year = now.year();
    let month = now.month();
    let day = now.day();
    let hour = now.hour();
    let minute = now.minute();
    let second = now.second();
    let cancel = CancellationToken::new();

    info!("starting collect_metrics_backup_iteration");

    let metrics_to_send = collect_and_clear_metrics(endpoints);

    if metrics_to_send.is_empty() {
        trace!("no new metrics to send");
    }

    // Send metrics.
    for chunk in create_event_chunks(&metrics_to_send, hostname, prev, now, chunk_size) {
        let real_now = Utc::now();
        let id = uuid::Uuid::new_v7(Timestamp::from_unix(
            NoContext,
            real_now.second().into(),
            real_now.nanosecond(),
        ));
        let path = format!("year={year:04}/month={month:02}/day={day:02}/{hour:02}:{minute:02}:{second:02}Z_{id}.json.gz");
        let remote_path = match RemotePath::from_string(&path) {
            Ok(remote_path) => remote_path,
            Err(e) => {
                error!("failed to create remote path from str {path}: {:?}", e);
                continue;
            }
        };

        let res = upload_events_chunk(storage, chunk, &remote_path, &cancel).await;

        if let Err(e) = res {
            error!(
                "failed to upload consumption events to remote storage: {:?}",
                e
            );
        }
    }
}

async fn upload_events_chunk(
    storage: &Option<GenericRemoteStorage>,
    chunk: EventChunk<'_, Event<Ids, &'static str>>,
    remote_path: &RemotePath,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let storage = match storage {
        Some(storage) => storage,
        None => {
            error!("no remote storage configured");
            return Ok(());
        }
    };
    let data = serde_json::to_vec(&chunk).context("serialize metrics")?;
    let mut encoder = GzipEncoder::new(Vec::new());
    encoder.write_all(&data).await.context("compress metrics")?;
    encoder.shutdown().await.context("compress metrics")?;
    let compressed_data: Bytes = encoder.get_ref().clone().into();
    backoff::retry(
        || async {
            let stream = futures::stream::once(futures::future::ready(Ok(compressed_data.clone())));
            storage
                .upload(stream, compressed_data.len(), remote_path, None, cancel)
                .await
        },
        TimeoutOrCancel::caused_by_cancel,
        FAILED_UPLOAD_WARN_THRESHOLD,
        FAILED_UPLOAD_MAX_RETRIES,
        "request_data_upload",
        cancel,
    )
    .await
    .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
    .and_then(|x| x)
    .context("request_data_upload")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        net::TcpListener,
        sync::{Arc, Mutex},
    };

    use anyhow::Error;
    use chrono::Utc;
    use consumption_metrics::{Event, EventChunk};
    use hyper::{
        service::{make_service_fn, service_fn},
        Body, Response,
    };
    use url::Url;

    use super::*;
    use crate::{dns::Dns, http, BranchId, EndpointId};

    #[tokio::test]
    async fn metrics() {
        let listener = TcpListener::bind("0.0.0.0:0").unwrap();

        let reports = Arc::new(Mutex::new(vec![]));
        let reports2 = reports.clone();

        let server = hyper::server::Server::from_tcp(listener)
            .unwrap()
            .serve(make_service_fn(move |_| {
                let reports = reports.clone();
                async move {
                    Ok::<_, Error>(service_fn(move |req| {
                        let reports = reports.clone();
                        async move {
                            let bytes = hyper::body::to_bytes(req.into_body()).await?;
                            let events: EventChunk<'static, Event<Ids, String>> =
                                serde_json::from_slice(&bytes)?;
                            reports.lock().unwrap().push(events);
                            Ok::<_, Error>(Response::new(Body::from(vec![])))
                        }
                    }))
                }
            }));
        let addr = server.local_addr();
        tokio::spawn(server);

        let metrics = Metrics::default();
        let client = http::new_client(Dns::new());
        let endpoint = Url::parse(&format!("http://{addr}")).unwrap();
        let now = Utc::now();

        // no counters have been registered
        collect_metrics_iteration(&metrics.endpoints, &client, &endpoint, "foo", now, now).await;
        let r = std::mem::take(&mut *reports2.lock().unwrap());
        assert!(r.is_empty());

        // register a new counter

        let counter = metrics.register(Ids {
            endpoint_id: (&EndpointId::from("e1")).into(),
            branch_id: (&BranchId::from("b1")).into(),
        });

        // the counter should be observed despite 0 egress
        collect_metrics_iteration(&metrics.endpoints, &client, &endpoint, "foo", now, now).await;
        let r = std::mem::take(&mut *reports2.lock().unwrap());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].events.len(), 1);
        assert_eq!(r[0].events[0].value, 0);

        // record egress
        counter.record_egress(1);

        // egress should be observered
        collect_metrics_iteration(&metrics.endpoints, &client, &endpoint, "foo", now, now).await;
        let r = std::mem::take(&mut *reports2.lock().unwrap());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].events.len(), 1);
        assert_eq!(r[0].events[0].value, 1);

        // release counter
        drop(counter);

        // we do not observe the counter
        collect_metrics_iteration(&metrics.endpoints, &client, &endpoint, "foo", now, now).await;
        let r = std::mem::take(&mut *reports2.lock().unwrap());
        assert!(r.is_empty());

        // counter is unregistered
        assert!(metrics.endpoints.is_empty());

        collect_metrics_backup_iteration(&metrics.backup_endpoints, &None, "foo", now, now, 1000)
            .await;
        assert!(!metrics.backup_endpoints.is_empty());
        collect_metrics_backup_iteration(&metrics.backup_endpoints, &None, "foo", now, now, 1000)
            .await;
        // backup counter is unregistered after the second iteration
        assert!(metrics.backup_endpoints.is_empty());
    }
}
