//! Communication with the broker, providing safekeeper peers and pageserver coordination.

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;

use anyhow::Error;
use anyhow::Result;

use storage_broker::parse_proto_ttid;

use storage_broker::proto::subscribe_safekeeper_info_request::SubscriptionKey as ProtoSubscriptionKey;
use storage_broker::proto::FilterTenantTimelineId;
use storage_broker::proto::MessageType;
use storage_broker::proto::SafekeeperDiscoveryResponse;
use storage_broker::proto::SubscribeByFilterRequest;
use storage_broker::proto::SubscribeSafekeeperInfoRequest;
use storage_broker::proto::TypeSubscription;
use storage_broker::proto::TypedMessage;
use storage_broker::Request;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::UNIX_EPOCH;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::*;

use crate::metrics::BROKER_ITERATION_TIMELINES;
use crate::metrics::BROKER_PULLED_UPDATES;
use crate::metrics::BROKER_PUSHED_UPDATES;
use crate::metrics::BROKER_PUSH_ALL_UPDATES_SECONDS;
use crate::GlobalTimelines;
use crate::SafeKeeperConf;

const RETRY_INTERVAL_MSEC: u64 = 1000;
const PUSH_INTERVAL_MSEC: u64 = 1000;

/// Push once in a while data about all active timelines to the broker.
async fn push_loop(
    conf: Arc<SafeKeeperConf>,
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    if conf.disable_periodic_broker_push {
        info!("broker push_loop is disabled, doing nothing...");
        futures::future::pending::<()>().await; // sleep forever
        return Ok(());
    }

    let active_timelines_set = global_timelines.get_global_broker_active_set();

    let mut client =
        storage_broker::connect(conf.broker_endpoint.clone(), conf.broker_keepalive_interval)?;
    let push_interval = Duration::from_millis(PUSH_INTERVAL_MSEC);

    let outbound = async_stream::stream! {
        loop {
            // Note: we lock runtime here and in timeline methods as GlobalTimelines
            // is under plain mutex. That's ok, all this code is not performance
            // sensitive and there is no risk of deadlock as we don't await while
            // lock is held.
            let now = Instant::now();
            let all_tlis = active_timelines_set.get_all();
            let mut n_pushed_tlis = 0;
            for tli in &all_tlis {
                let sk_info = tli.get_safekeeper_info(&conf).await;
                yield sk_info;
                BROKER_PUSHED_UPDATES.inc();
                n_pushed_tlis += 1;
            }
            let elapsed = now.elapsed();

            BROKER_PUSH_ALL_UPDATES_SECONDS.observe(elapsed.as_secs_f64());
            BROKER_ITERATION_TIMELINES.observe(n_pushed_tlis as f64);

            if elapsed > push_interval / 2 {
                info!("broker push is too long, pushed {} timeline updates to broker in {:?}", n_pushed_tlis, elapsed);
            }

            sleep(push_interval).await;
        }
    };
    client
        .publish_safekeeper_info(Request::new(outbound))
        .await?;
    Ok(())
}

/// Subscribe and fetch all the interesting data from the broker.
#[instrument(name = "broker_pull", skip_all)]
async fn pull_loop(
    conf: Arc<SafeKeeperConf>,
    global_timelines: Arc<GlobalTimelines>,
    stats: Arc<BrokerStats>,
) -> Result<()> {
    let mut client =
        storage_broker::connect(conf.broker_endpoint.clone(), conf.broker_keepalive_interval)?;

    // TODO: subscribe only to local timelines instead of all
    let request = SubscribeSafekeeperInfoRequest {
        subscription_key: Some(ProtoSubscriptionKey::All(())),
    };

    let mut stream = client
        .subscribe_safekeeper_info(request)
        .await
        .context("subscribe_safekeper_info request failed")?
        .into_inner();

    let ok_counter = BROKER_PULLED_UPDATES.with_label_values(&["ok"]);
    let not_found = BROKER_PULLED_UPDATES.with_label_values(&["not_found"]);
    let err_counter = BROKER_PULLED_UPDATES.with_label_values(&["error"]);

    while let Some(msg) = stream.message().await? {
        stats.update_pulled();

        let proto_ttid = msg
            .tenant_timeline_id
            .as_ref()
            .ok_or_else(|| anyhow!("missing tenant_timeline_id"))?;
        let ttid = parse_proto_ttid(proto_ttid)?;
        if let Ok(tli) = global_timelines.get(ttid) {
            // Note that we also receive *our own* info. That's
            // important, as it is used as an indication of live
            // connection to the broker.

            // note: there are blocking operations below, but it's considered fine for now
            let res = tli.record_safekeeper_info(msg).await;
            if res.is_ok() {
                ok_counter.inc();
            } else {
                err_counter.inc();
            }
            res?;
        } else {
            not_found.inc();
        }
    }
    bail!("end of stream");
}

/// Process incoming discover requests. This is done in a separate task to avoid
/// interfering with the normal pull/push loops.
async fn discover_loop(
    conf: Arc<SafeKeeperConf>,
    global_timelines: Arc<GlobalTimelines>,
    stats: Arc<BrokerStats>,
) -> Result<()> {
    let mut client =
        storage_broker::connect(conf.broker_endpoint.clone(), conf.broker_keepalive_interval)?;

    let request = SubscribeByFilterRequest {
        types: vec![TypeSubscription {
            r#type: MessageType::SafekeeperDiscoveryRequest as i32,
        }],
        tenant_timeline_id: Some(FilterTenantTimelineId {
            enabled: false,
            tenant_timeline_id: None,
        }),
    };

    let mut stream = client
        .subscribe_by_filter(request)
        .await
        .context("subscribe_by_filter request failed")?
        .into_inner();

    let discover_counter = BROKER_PULLED_UPDATES.with_label_values(&["discover"]);

    while let Some(typed_msg) = stream.message().await? {
        stats.update_pulled();

        match typed_msg.r#type() {
            MessageType::SafekeeperDiscoveryRequest => {
                let msg = typed_msg
                    .safekeeper_discovery_request
                    .expect("proto type mismatch from broker message");

                let proto_ttid = msg
                    .tenant_timeline_id
                    .as_ref()
                    .ok_or_else(|| anyhow!("missing tenant_timeline_id"))?;
                let ttid = parse_proto_ttid(proto_ttid)?;
                if let Ok(tli) = global_timelines.get(ttid) {
                    // we received a discovery request for a timeline we know about
                    discover_counter.inc();

                    // create and reply with discovery response
                    let sk_info = tli.get_safekeeper_info(&conf).await;
                    let response = SafekeeperDiscoveryResponse {
                        safekeeper_id: sk_info.safekeeper_id,
                        tenant_timeline_id: sk_info.tenant_timeline_id,
                        commit_lsn: sk_info.commit_lsn,
                        safekeeper_connstr: sk_info.safekeeper_connstr,
                        availability_zone: sk_info.availability_zone,
                        standby_horizon: 0,
                    };

                    // note this is a blocking call
                    client
                        .publish_one(TypedMessage {
                            r#type: MessageType::SafekeeperDiscoveryResponse as i32,
                            safekeeper_timeline_info: None,
                            safekeeper_discovery_request: None,
                            safekeeper_discovery_response: Some(response),
                        })
                        .await?;
                }
            }

            _ => {
                warn!(
                    "unexpected message type i32 {}, {:?}",
                    typed_msg.r#type,
                    typed_msg.r#type()
                );
            }
        }
    }
    bail!("end of stream");
}

pub async fn task_main(
    conf: Arc<SafeKeeperConf>,
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    info!("started, broker endpoint {:?}", conf.broker_endpoint);

    let mut ticker = tokio::time::interval(Duration::from_millis(RETRY_INTERVAL_MSEC));
    let mut push_handle: Option<JoinHandle<Result<(), Error>>> = None;
    let mut pull_handle: Option<JoinHandle<Result<(), Error>>> = None;
    let mut discover_handle: Option<JoinHandle<Result<(), Error>>> = None;

    let stats = Arc::new(BrokerStats::new());
    let stats_task = task_stats(stats.clone());
    tokio::pin!(stats_task);

    // Selecting on JoinHandles requires some squats; is there a better way to
    // reap tasks individually?

    // Handling failures in task itself won't catch panic and in Tokio, task's
    // panic doesn't kill the whole executor, so it is better to do reaping
    // here.
    loop {
        tokio::select! {
                res = async { push_handle.as_mut().unwrap().await }, if push_handle.is_some() => {
                    // was it panic or normal error?
                    let err = match res {
                        Ok(res_internal) => res_internal.unwrap_err(),
                        Err(err_outer) => err_outer.into(),
                    };
                    warn!("push task failed: {:?}", err);
                    push_handle = None;
                },
                res = async { pull_handle.as_mut().unwrap().await }, if pull_handle.is_some() => {
                    // was it panic or normal error?
                    match res {
                        Ok(res_internal) => if let Err(err_inner) = res_internal {
                            warn!("pull task failed: {:?}", err_inner);
                        }
                        Err(err_outer) => { warn!("pull task panicked: {:?}", err_outer) }
                    };
                    pull_handle = None;
                },
                res = async { discover_handle.as_mut().unwrap().await }, if discover_handle.is_some() => {
                    // was it panic or normal error?
                    match res {
                        Ok(res_internal) => if let Err(err_inner) = res_internal {
                            warn!("discover task failed: {:?}", err_inner);
                        }
                        Err(err_outer) => { warn!("discover task panicked: {:?}", err_outer) }
                    };
                    discover_handle = None;
                },
                _ = ticker.tick() => {
                    if push_handle.is_none() {
                        push_handle = Some(tokio::spawn(push_loop(conf.clone(), global_timelines.clone())));
                    }
                    if pull_handle.is_none() {
                        pull_handle = Some(tokio::spawn(pull_loop(conf.clone(), global_timelines.clone(), stats.clone())));
                    }
                    if discover_handle.is_none() {
                        discover_handle = Some(tokio::spawn(discover_loop(conf.clone(), global_timelines.clone(), stats.clone())));
                    }
                },
                _ = &mut stats_task => {}
        }
    }
}

struct BrokerStats {
    /// Timestamp of the last received message from the broker.
    last_pulled_ts: AtomicU64,
}

impl BrokerStats {
    fn new() -> Self {
        BrokerStats {
            last_pulled_ts: AtomicU64::new(0),
        }
    }

    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is before epoch")
            .as_millis() as u64
    }

    /// Update last_pulled timestamp to current time.
    fn update_pulled(&self) {
        self.last_pulled_ts
            .store(Self::now_millis(), std::sync::atomic::Ordering::Relaxed);
    }
}

/// Periodically write to logs if there are issues with receiving data from the broker.
async fn task_stats(stats: Arc<BrokerStats>) {
    let warn_duration = Duration::from_secs(10);
    let mut ticker = tokio::time::interval(warn_duration);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let last_pulled = stats.last_pulled_ts.load(std::sync::atomic::Ordering::SeqCst);
                if last_pulled == 0 {
                    // no broker updates yet
                    continue;
                }

                let now = BrokerStats::now_millis();
                if now > last_pulled && now - last_pulled > warn_duration.as_millis() as u64 {
                    let ts = chrono::DateTime::from_timestamp_millis(last_pulled as i64).expect("invalid timestamp");
                    info!("no broker updates for some time, last update: {:?}", ts);
                }
            }
        }
    }
}
