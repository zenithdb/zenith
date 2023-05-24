//! Actual Postgres connection handler to stream WAL to the server.

use std::{
    error::Error,
    pin::pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{bail, ensure, Context};
use bytes::BytesMut;
use chrono::{NaiveDateTime, Utc};
use fail::fail_point;
use futures::StreamExt;
use postgres::{error::SqlState, SimpleQueryMessage, SimpleQueryRow};
use postgres_ffi::v14::xlog_utils::normalize_lsn;
use postgres_ffi::WAL_SEGMENT_SIZE;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use tokio::{select, sync::watch, time};
use tokio_postgres::{replication::ReplicationStream, Client};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use super::TaskStateUpdate;
use crate::metrics::LIVE_CONNECTIONS_COUNT;
use crate::{context::RequestContext, metrics::WALRECEIVER_STARTED_CONNECTIONS};
use crate::{
    task_mgr,
    task_mgr::TaskKind,
    task_mgr::WALRECEIVER_RUNTIME,
    tenant::{Timeline, WalReceiverInfo},
    walingest::WalIngest,
    walrecord::DecodedWALRecord,
};
use postgres_backend::is_expected_io_error;
use postgres_connection::PgConnectionConfig;
use postgres_ffi::waldecoder::WalStreamDecoder;
use utils::pageserver_feedback::PageserverFeedback;
use utils::{id::NodeId, lsn::Lsn};

/// Status of the connection.
#[derive(Debug, Clone, Copy)]
pub(super) struct WalConnectionStatus {
    /// If we were able to initiate a postgres connection, this means that safekeeper process is at least running.
    pub is_connected: bool,
    /// Defines a healthy connection as one on which pageserver received WAL from safekeeper
    /// and is able to process it in walingest without errors.
    pub has_processed_wal: bool,
    /// Connection establishment time or the timestamp of a latest connection message received.
    pub latest_connection_update: NaiveDateTime,
    /// Time of the latest WAL message received.
    pub latest_wal_update: NaiveDateTime,
    /// Latest WAL update contained WAL up to this LSN. Next WAL message with start from that LSN.
    pub streaming_lsn: Option<Lsn>,
    /// Latest commit_lsn received from the safekeeper. Can be zero if no message has been received yet.
    pub commit_lsn: Option<Lsn>,
    /// The node it is connected to
    pub node: NodeId,
}

/// Open a connection to the given safekeeper and receive WAL, sending back progress
/// messages as we go.
pub(super) async fn handle_walreceiver_connection(
    timeline: Arc<Timeline>,
    wal_source_connconf: PgConnectionConfig,
    events_sender: watch::Sender<TaskStateUpdate<WalConnectionStatus>>,
    cancellation: CancellationToken,
    connect_timeout: Duration,
    ctx: RequestContext,
    node: NodeId,
) -> anyhow::Result<()> {
    WALRECEIVER_STARTED_CONNECTIONS.inc();

    // Connect to the database in replication mode.
    info!("connecting to {wal_source_connconf:?}");

    let (mut replication_client, connection) = {
        let mut config = wal_source_connconf.to_tokio_postgres_config();
        config.application_name("pageserver");
        config.replication_mode(tokio_postgres::config::ReplicationMode::Physical);
        match time::timeout(connect_timeout, config.connect(postgres::NoTls)).await {
            Ok(Ok(client_and_conn)) => client_and_conn,
            Ok(Err(conn_err)) => {
                let expected_error = ignore_expected_errors(conn_err)?;
                info!("DB connection stream finished: {expected_error}");
                return Ok(());
            }
            Err(_) => {
                // Timing out to connect to a safekeeper node could happen long time, due to
                // many reasons that pageserver cannot control.
                // Do not produce an error, but make it visible, that timeouts happen by logging the `event.
                info!("Timed out while waiting {connect_timeout:?} for walreceiver connection to open");
                return Ok(());
            }
        }
    };

    info!("connected!");
    let mut connection_status = WalConnectionStatus {
        is_connected: true,
        has_processed_wal: false,
        latest_connection_update: Utc::now().naive_utc(),
        latest_wal_update: Utc::now().naive_utc(),
        streaming_lsn: None,
        commit_lsn: None,
        node,
    };
    if let Err(e) = events_sender.send(TaskStateUpdate::Progress(connection_status)) {
        warn!("Wal connection event listener dropped right after connection init, aborting the connection: {e}");
        return Ok(());
    }

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    let _connection_ctx = ctx.detached_child(
        TaskKind::WalReceiverConnectionPoller,
        ctx.download_behavior(),
    );
    let connection_cancellation = cancellation.clone();
    task_mgr::spawn(
        WALRECEIVER_RUNTIME.handle(),
        TaskKind::WalReceiverConnectionPoller,
        Some(timeline.tenant_id),
        Some(timeline.timeline_id),
        "walreceiver connection",
        false,
        async move {
            select! {
                connection_result = connection => match connection_result {
                    Ok(()) => info!("Walreceiver db connection closed"),
                    Err(connection_error) => {
                        if let Err(e) = ignore_expected_errors(connection_error) {
                            warn!("Connection aborted: {e:#}")
                        }
                    }
                },
                // Future: replace connection_cancellation with connection_ctx cancellation
                _ = connection_cancellation.cancelled() => info!("Connection cancelled"),
            }
            Ok(())
        },
    );

    // Immediately increment the gauge, then create a job to decrement it on task exit.
    // One of the pros of `defer!` is that this will *most probably*
    // get called, even in presence of panics.
    let gauge = LIVE_CONNECTIONS_COUNT.with_label_values(&["wal_receiver"]);
    gauge.inc();
    scopeguard::defer! {
        gauge.dec();
    }

    let identify = identify_system(&mut replication_client).await?;
    info!("{identify:?}");

    let end_of_wal = Lsn::from(u64::from(identify.xlogpos));
    let mut caught_up = false;

    connection_status.latest_connection_update = Utc::now().naive_utc();
    connection_status.latest_wal_update = Utc::now().naive_utc();
    connection_status.commit_lsn = Some(end_of_wal);
    if let Err(e) = events_sender.send(TaskStateUpdate::Progress(connection_status)) {
        warn!("Wal connection event listener dropped after IDENTIFY_SYSTEM, aborting the connection: {e}");
        return Ok(());
    }

    //
    // Start streaming the WAL, from where we left off previously.
    //
    // If we had previously received WAL up to some point in the middle of a WAL record, we
    // better start from the end of last full WAL record, not in the middle of one.
    let mut last_rec_lsn = timeline.get_last_record_lsn();
    let mut startpoint = last_rec_lsn;

    if startpoint == Lsn(0) {
        bail!("No previous WAL position");
    }

    // There might be some padding after the last full record, skip it.
    startpoint += startpoint.calc_padding(8u32);

    // If the starting point is at a WAL page boundary, skip past the page header. We don't need the page headers
    // for anything, and in some corner cases, the compute node might have never generated the WAL for page headers
    //. That happens if you create a branch at page boundary: the start point of the branch is at the page boundary,
    // but when the compute node first starts on the branch, we normalize the first REDO position to just after the page
    // header (see generate_pg_control()), so the WAL for the page header is never streamed from the compute node
    //  to the safekeepers.
    startpoint = normalize_lsn(startpoint, WAL_SEGMENT_SIZE);

    info!("last_record_lsn {last_rec_lsn} starting replication from {startpoint}, safekeeper is at {end_of_wal}...");

    let query = format!("START_REPLICATION PHYSICAL {startpoint}");

    let copy_stream = replication_client.copy_both_simple(&query).await?;
    let mut physical_stream = pin!(ReplicationStream::new(copy_stream));

    let mut waldecoder = WalStreamDecoder::new(startpoint, timeline.pg_version);

    let mut walingest = WalIngest::new(timeline.as_ref(), startpoint, &ctx).await?;

    while let Some(replication_message) = {
        select! {
            _ = cancellation.cancelled() => {
                info!("walreceiver interrupted");
                None
            }
            replication_message = physical_stream.next() => replication_message,
        }
    } {
        let replication_message = match replication_message {
            Ok(message) => message,
            Err(replication_error) => {
                let expected_error = ignore_expected_errors(replication_error)?;
                info!("Replication stream finished: {expected_error}");
                return Ok(());
            }
        };

        let now = Utc::now().naive_utc();
        let last_rec_lsn_before_msg = last_rec_lsn;

        // Update the connection status before processing the message. If the message processing
        // fails (e.g. in walingest), we still want to know latests LSNs from the safekeeper.
        match &replication_message {
            ReplicationMessage::XLogData(xlog_data) => {
                connection_status.latest_connection_update = now;
                connection_status.commit_lsn = Some(Lsn::from(xlog_data.wal_end()));
                connection_status.streaming_lsn = Some(Lsn::from(
                    xlog_data.wal_start() + xlog_data.data().len() as u64,
                ));
                if !xlog_data.data().is_empty() {
                    connection_status.latest_wal_update = now;
                }
            }
            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                connection_status.latest_connection_update = now;
                connection_status.commit_lsn = Some(Lsn::from(keepalive.wal_end()));
            }
            &_ => {}
        };
        if let Err(e) = events_sender.send(TaskStateUpdate::Progress(connection_status)) {
            warn!("Wal connection event listener dropped, aborting the connection: {e}");
            return Ok(());
        }

        let status_update = match replication_message {
            ReplicationMessage::XLogData(xlog_data) => {
                // Pass the WAL data to the decoder, and see if we can decode
                // more records as a result.
                let data = xlog_data.data();
                let startlsn = Lsn::from(xlog_data.wal_start());
                let endlsn = startlsn + data.len() as u64;

                trace!("received XLogData between {startlsn} and {endlsn}");

                waldecoder.feed_bytes(data);

                {
                    let mut decoded = DecodedWALRecord::default();
                    let mut modification = timeline.begin_modification(endlsn);
                    while let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                        // let _enter = info_span!("processing record", lsn = %lsn).entered();

                        // It is important to deal with the aligned records as lsn in getPage@LSN is
                        // aligned and can be several bytes bigger. Without this alignment we are
                        // at risk of hitting a deadlock.
                        ensure!(lsn.is_aligned());

                        walingest
                            .ingest_record(recdata, lsn, &mut modification, &mut decoded, &ctx)
                            .await
                            .with_context(|| format!("could not ingest record at {lsn}"))?;

                        fail_point!("walreceiver-after-ingest");

                        last_rec_lsn = lsn;
                    }
                }

                if !caught_up && endlsn >= end_of_wal {
                    info!("caught up at LSN {endlsn}");
                    caught_up = true;
                }

                Some(endlsn)
            }

            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let wal_end = keepalive.wal_end();
                let timestamp = keepalive.timestamp();
                let reply_requested = keepalive.reply() != 0;

                trace!("received PrimaryKeepAlive(wal_end: {wal_end}, timestamp: {timestamp:?} reply: {reply_requested})");

                if reply_requested {
                    Some(last_rec_lsn)
                } else {
                    None
                }
            }

            _ => None,
        };

        if !connection_status.has_processed_wal && last_rec_lsn > last_rec_lsn_before_msg {
            // We have successfully processed at least one WAL record.
            connection_status.has_processed_wal = true;
            if let Err(e) = events_sender.send(TaskStateUpdate::Progress(connection_status)) {
                warn!("Wal connection event listener dropped, aborting the connection: {e}");
                return Ok(());
            }
        }

        timeline.check_checkpoint_distance().with_context(|| {
            format!(
                "Failed to check checkpoint distance for timeline {}",
                timeline.timeline_id
            )
        })?;

        if let Some(last_lsn) = status_update {
            let timeline_remote_consistent_lsn =
                timeline.get_remote_consistent_lsn().unwrap_or(Lsn(0));

            // The last LSN we processed. It is not guaranteed to survive pageserver crash.
            let last_received_lsn = last_lsn;
            // `disk_consistent_lsn` is the LSN at which page server guarantees local persistence of all received data
            let disk_consistent_lsn = timeline.get_disk_consistent_lsn();
            // The last LSN that is synced to remote storage and is guaranteed to survive pageserver crash
            // Used by safekeepers to remove WAL preceding `remote_consistent_lsn`.
            let remote_consistent_lsn = timeline_remote_consistent_lsn;
            let ts = SystemTime::now();

            // Update the status about what we just received. This is shown in the mgmt API.
            let last_received_wal = WalReceiverInfo {
                wal_source_connconf: wal_source_connconf.clone(),
                last_received_msg_lsn: last_lsn,
                last_received_msg_ts: ts
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Received message time should be before UNIX EPOCH!")
                    .as_micros(),
            };
            *timeline.last_received_wal.lock().unwrap() = Some(last_received_wal);

            // Send the replication feedback message.
            // Regular standby_status_update fields are put into this message.
            let timeline_logical_size = timeline.get_current_logical_size();
            let status_update = PageserverFeedback {
                current_timeline_size: timeline_logical_size,
                last_received_lsn,
                disk_consistent_lsn,
                remote_consistent_lsn,
                replytime: ts,
            };

            debug!("neon_status_update {status_update:?}");

            let mut data = BytesMut::new();
            status_update.serialize(&mut data);
            physical_stream
                .as_mut()
                .zenith_status_update(data.len() as u64, &data)
                .await?;
        }
    }

    Ok(())
}

/// Data returned from the postgres `IDENTIFY_SYSTEM` command
///
/// See the [postgres docs] for more details.
///
/// [postgres docs]: https://www.postgresql.org/docs/current/protocol-replication.html
#[derive(Debug)]
// As of nightly 2021-09-11, fields that are only read by the type's `Debug` impl still count as
// unused. Relevant issue: https://github.com/rust-lang/rust/issues/88900
#[allow(dead_code)]
struct IdentifySystem {
    systemid: u64,
    timeline: u32,
    xlogpos: PgLsn,
    dbname: Option<String>,
}

/// There was a problem parsing the response to
/// a postgres IDENTIFY_SYSTEM command.
#[derive(Debug, thiserror::Error)]
#[error("IDENTIFY_SYSTEM parse error")]
struct IdentifyError;

/// Run the postgres `IDENTIFY_SYSTEM` command
async fn identify_system(client: &mut Client) -> anyhow::Result<IdentifySystem> {
    let query_str = "IDENTIFY_SYSTEM";
    let response = client.simple_query(query_str).await?;

    // get(N) from row, then parse it as some destination type.
    fn get_parse<T>(row: &SimpleQueryRow, idx: usize) -> Result<T, IdentifyError>
    where
        T: FromStr,
    {
        let val = row.get(idx).ok_or(IdentifyError)?;
        val.parse::<T>().or(Err(IdentifyError))
    }

    // extract the row contents into an IdentifySystem struct.
    // written as a closure so I can use ? for Option here.
    if let Some(SimpleQueryMessage::Row(first_row)) = response.get(0) {
        Ok(IdentifySystem {
            systemid: get_parse(first_row, 0)?,
            timeline: get_parse(first_row, 1)?,
            xlogpos: get_parse(first_row, 2)?,
            dbname: get_parse(first_row, 3).ok(),
        })
    } else {
        Err(IdentifyError.into())
    }
}

/// We don't want to report connectivity problems as real errors towards connection manager because
/// 1. they happen frequently enough to make server logs hard to read and
/// 2. the connection manager can retry other safekeeper.
///
/// If this function returns `Ok(pg_error)`, it's such an error.
/// The caller should log it at info level and then report to connection manager that we're done handling this connection.
/// Connection manager will then handle reconnections.
///
/// If this function returns an `Err()`, the caller can bubble it up using `?`.
/// The connection manager will log the error at ERROR level.
fn ignore_expected_errors(pg_error: postgres::Error) -> anyhow::Result<postgres::Error> {
    if pg_error.is_closed()
        || pg_error
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .map(is_expected_io_error)
            .unwrap_or(false)
    {
        return Ok(pg_error);
    } else if let Some(db_error) = pg_error.as_db_error() {
        if db_error.code() == &SqlState::SUCCESSFUL_COMPLETION
            && db_error.message().contains("ending streaming")
        {
            return Ok(pg_error);
        }
    }
    Err(pg_error).context("connection error")
}
