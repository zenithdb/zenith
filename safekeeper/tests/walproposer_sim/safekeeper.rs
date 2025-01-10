//! Safekeeper communication endpoint to WAL proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{bail, Result};
use bytes::{Bytes, BytesMut};
use camino::Utf8PathBuf;
use desim::{
    executor::{self, PollSome},
    network::TCP,
    node_os::NodeOs,
    proto::{AnyMessage, NetEvent, NodeEvent},
};
use http::Uri;
use safekeeper::{
    safekeeper::{ProposerAcceptorMessage, SafeKeeper, UNKNOWN_SERVER_VERSION},
    state::{TimelinePersistentState, TimelineState},
    timeline::TimelineError,
    wal_storage::Storage,
    SafeKeeperConf,
};
use safekeeper_api::ServerInfo;
use tracing::{debug, info_span, warn};
use utils::{
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
};

use super::safekeeper_disk::{DiskStateStorage, DiskWALStorage, SafekeeperDisk, TimelineDisk};

struct SharedState {
    sk: SafeKeeper<DiskStateStorage, DiskWALStorage>,
    disk: Arc<TimelineDisk>,
}

struct GlobalMap {
    timelines: HashMap<TenantTimelineId, SharedState>,
    conf: SafeKeeperConf,
    disk: Arc<SafekeeperDisk>,
}

impl GlobalMap {
    /// Restores global state from disk.
    fn new(disk: Arc<SafekeeperDisk>, conf: SafeKeeperConf) -> Result<Self> {
        let mut timelines = HashMap::new();

        for (&ttid, disk) in disk.timelines.lock().iter() {
            debug!("loading timeline {}", ttid);
            let state = disk.state.lock().clone();

            if state.server.wal_seg_size == 0 {
                bail!(TimelineError::UninitializedWalSegSize(ttid));
            }

            if state.server.pg_version == UNKNOWN_SERVER_VERSION {
                bail!(TimelineError::UninitialinzedPgVersion(ttid));
            }

            if state.commit_lsn < state.local_start_lsn {
                bail!(
                    "commit_lsn {} is smaller than local_start_lsn {}",
                    state.commit_lsn,
                    state.local_start_lsn
                );
            }

            let control_store = DiskStateStorage::new(disk.clone());
            let wal_store = DiskWALStorage::new(disk.clone(), &control_store)?;

            let sk = SafeKeeper::new(TimelineState::new(control_store), wal_store, conf.my_id)?;
            timelines.insert(
                ttid,
                SharedState {
                    sk,
                    disk: disk.clone(),
                },
            );
        }

        Ok(Self {
            timelines,
            conf,
            disk,
        })
    }

    fn create(&mut self, ttid: TenantTimelineId, server_info: ServerInfo) -> Result<()> {
        if self.timelines.contains_key(&ttid) {
            bail!("timeline {} already exists", ttid);
        }

        debug!("creating new timeline {}", ttid);

        let commit_lsn = Lsn::INVALID;
        let local_start_lsn = Lsn::INVALID;

        let state =
            TimelinePersistentState::new(&ttid, server_info, vec![], commit_lsn, local_start_lsn)?;

        let disk_timeline = self.disk.put_state(&ttid, state);
        let control_store = DiskStateStorage::new(disk_timeline.clone());
        let wal_store = DiskWALStorage::new(disk_timeline.clone(), &control_store)?;

        let sk = SafeKeeper::new(
            TimelineState::new(control_store),
            wal_store,
            self.conf.my_id,
        )?;

        self.timelines.insert(
            ttid,
            SharedState {
                sk,
                disk: disk_timeline,
            },
        );
        Ok(())
    }

    fn get(&mut self, ttid: &TenantTimelineId) -> &mut SharedState {
        self.timelines.get_mut(ttid).expect("timeline must exist")
    }

    fn has_tli(&self, ttid: &TenantTimelineId) -> bool {
        self.timelines.contains_key(ttid)
    }
}

/// State of a single connection to walproposer.
struct ConnState {
    tcp: TCP,

    greeting: bool,
    ttid: TenantTimelineId,
    flush_pending: bool,

    runtime: tokio::runtime::Runtime,
}

pub fn run_server(os: NodeOs, disk: Arc<SafekeeperDisk>) -> Result<()> {
    let _enter = info_span!("safekeeper", id = os.id()).entered();
    debug!("started server");
    os.log_event("started;safekeeper".to_owned());
    let conf = SafeKeeperConf {
        workdir: Utf8PathBuf::from("."),
        my_id: NodeId(os.id() as u64),
        listen_pg_addr: String::new(),
        listen_http_addr: String::new(),
        no_sync: false,
        broker_endpoint: "/".parse::<Uri>().unwrap(),
        broker_keepalive_interval: Duration::from_secs(0),
        heartbeat_timeout: Duration::from_secs(0),
        remote_storage: None,
        max_offloader_lag_bytes: 0,
        wal_backup_enabled: false,
        listen_pg_addr_tenant_only: None,
        advertise_pg_addr: None,
        availability_zone: None,
        peer_recovery_enabled: false,
        backup_parallel_jobs: 0,
        pg_auth: None,
        pg_tenant_only_auth: None,
        http_auth: None,
        sk_auth_token: None,
        current_thread_runtime: false,
        walsenders_keep_horizon: false,
        partial_backup_timeout: Duration::from_secs(0),
        disable_periodic_broker_push: false,
        enable_offload: false,
        delete_offloaded_wal: false,
        control_file_save_interval: Duration::from_secs(1),
        partial_backup_concurrency: 1,
        eviction_min_resident: Duration::ZERO,
    };

    let mut global = GlobalMap::new(disk, conf.clone())?;
    let mut conns: HashMap<usize, ConnState> = HashMap::new();

    for (&_ttid, shared_state) in global.timelines.iter_mut() {
        let flush_lsn = shared_state.sk.wal_store.flush_lsn();
        let commit_lsn = shared_state.sk.state.commit_lsn;
        os.log_event(format!("tli_loaded;{};{}", flush_lsn.0, commit_lsn.0));
    }

    let node_events = os.node_events();
    let mut epoll_vec: Vec<Box<dyn PollSome>> = vec![];
    let mut epoll_idx: Vec<usize> = vec![];

    // TODO: batch events processing (multiple events per tick)
    loop {
        epoll_vec.clear();
        epoll_idx.clear();

        // node events channel
        epoll_vec.push(Box::new(node_events.clone()));
        epoll_idx.push(0);

        // tcp connections
        for conn in conns.values() {
            epoll_vec.push(Box::new(conn.tcp.recv_chan()));
            epoll_idx.push(conn.tcp.connection_id());
        }

        // waiting for the next message
        let index = executor::epoll_chans(&epoll_vec, -1).unwrap();

        if index == 0 {
            // got a new connection
            match node_events.must_recv() {
                NodeEvent::Accept(tcp) => {
                    conns.insert(
                        tcp.connection_id(),
                        ConnState {
                            tcp,
                            greeting: false,
                            ttid: TenantTimelineId::empty(),
                            flush_pending: false,
                            runtime: tokio::runtime::Builder::new_current_thread().build()?,
                        },
                    );
                }
                NodeEvent::Internal(_) => unreachable!(),
            }
            continue;
        }

        let connection_id = epoll_idx[index];
        let conn = conns.get_mut(&connection_id).unwrap();
        let mut next_event = Some(conn.tcp.recv_chan().must_recv());

        loop {
            let event = match next_event {
                Some(event) => event,
                None => break,
            };

            match event {
                NetEvent::Message(msg) => {
                    let res = conn.process_any(msg, &mut global);
                    if res.is_err() {
                        let e = res.unwrap_err();
                        let estr = e.to_string();
                        if !estr.contains("finished processing START_REPLICATION") {
                            warn!("conn {:?} error: {:?}", connection_id, e);
                            panic!("unexpected error at safekeeper: {:#}", e);
                        }
                        conns.remove(&connection_id);
                        break;
                    }
                }
                NetEvent::Closed => {
                    // TODO: remove from conns?
                }
            }

            next_event = conn.tcp.recv_chan().try_recv();
        }

        conns.retain(|_, conn| {
            let res = conn.flush(&mut global);
            if res.is_err() {
                debug!("conn {:?} error: {:?}", conn.tcp, res);
            }
            res.is_ok()
        });
    }
}

impl ConnState {
    /// Process a message from the network. It can be START_REPLICATION request or a valid ProposerAcceptorMessage message.
    fn process_any(&mut self, any: AnyMessage, global: &mut GlobalMap) -> Result<()> {
        if let AnyMessage::Bytes(copy_data) = any {
            let repl_prefix = b"START_REPLICATION ";
            if !self.greeting && copy_data.starts_with(repl_prefix) {
                self.process_start_replication(copy_data.slice(repl_prefix.len()..), global)?;
                bail!("finished processing START_REPLICATION")
            }

            let msg = ProposerAcceptorMessage::parse(copy_data)?;
            debug!("got msg: {:?}", msg);
            self.process(msg, global)
        } else {
            bail!("unexpected message, expected AnyMessage::Bytes");
        }
    }

    /// Process START_REPLICATION request.
    fn process_start_replication(
        &mut self,
        copy_data: Bytes,
        global: &mut GlobalMap,
    ) -> Result<()> {
        // format is "<tenant_id> <timeline_id> <start_lsn> <end_lsn>"
        let str = String::from_utf8(copy_data.to_vec())?;

        let mut parts = str.split(' ');
        let tenant_id = parts.next().unwrap().parse::<TenantId>()?;
        let timeline_id = parts.next().unwrap().parse::<TimelineId>()?;
        let start_lsn = parts.next().unwrap().parse::<u64>()?;
        let end_lsn = parts.next().unwrap().parse::<u64>()?;

        let ttid = TenantTimelineId::new(tenant_id, timeline_id);
        let shared_state = global.get(&ttid);

        // read bytes from start_lsn to end_lsn
        let mut buf = vec![0; (end_lsn - start_lsn) as usize];
        shared_state.disk.wal.lock().read(start_lsn, &mut buf);

        // send bytes to the client
        self.tcp.send(AnyMessage::Bytes(Bytes::from(buf)));
        Ok(())
    }

    /// Get or create a timeline.
    fn init_timeline(
        &mut self,
        ttid: TenantTimelineId,
        server_info: ServerInfo,
        global: &mut GlobalMap,
    ) -> Result<()> {
        self.ttid = ttid;
        if global.has_tli(&ttid) {
            return Ok(());
        }

        global.create(ttid, server_info)
    }

    /// Process a ProposerAcceptorMessage.
    fn process(&mut self, msg: ProposerAcceptorMessage, global: &mut GlobalMap) -> Result<()> {
        if !self.greeting {
            self.greeting = true;

            match msg {
                ProposerAcceptorMessage::Greeting(ref greeting) => {
                    tracing::info!(
                        "start handshake with walproposer {:?} {:?}",
                        self.tcp,
                        greeting
                    );
                    let server_info = ServerInfo {
                        pg_version: greeting.pg_version,
                        system_id: greeting.system_id,
                        wal_seg_size: greeting.wal_seg_size,
                    };
                    let ttid = TenantTimelineId::new(greeting.tenant_id, greeting.timeline_id);
                    self.init_timeline(ttid, server_info, global)?
                }
                _ => {
                    bail!("unexpected message {msg:?} instead of greeting");
                }
            }
        }

        let tli = global.get(&self.ttid);

        match msg {
            ProposerAcceptorMessage::AppendRequest(append_request) => {
                self.flush_pending = true;
                self.process_sk_msg(
                    tli,
                    &ProposerAcceptorMessage::NoFlushAppendRequest(append_request),
                )?;
            }
            other => {
                self.process_sk_msg(tli, &other)?;
            }
        }

        Ok(())
    }

    /// Process FlushWAL if needed.
    fn flush(&mut self, global: &mut GlobalMap) -> Result<()> {
        // TODO: try to add extra flushes in simulation, to verify that extra flushes don't break anything
        if !self.flush_pending {
            return Ok(());
        }
        self.flush_pending = false;
        let shared_state = global.get(&self.ttid);
        self.process_sk_msg(shared_state, &ProposerAcceptorMessage::FlushWAL)
    }

    /// Make safekeeper process a message and send a reply to the TCP
    fn process_sk_msg(
        &mut self,
        shared_state: &mut SharedState,
        msg: &ProposerAcceptorMessage,
    ) -> Result<()> {
        let mut reply = self.runtime.block_on(shared_state.sk.process_msg(msg))?;
        if let Some(reply) = &mut reply {
            // TODO: if this is AppendResponse, fill in proper hot standby feedback and disk consistent lsn

            let mut buf = BytesMut::with_capacity(128);
            reply.serialize(&mut buf)?;

            self.tcp.send(AnyMessage::Bytes(buf.into()));
        }
        Ok(())
    }
}

impl Drop for ConnState {
    fn drop(&mut self) {
        debug!("dropping conn: {:?}", self.tcp);
        if !std::thread::panicking() {
            self.tcp.close();
        }
        // TODO: clean up non-fsynced WAL
    }
}
