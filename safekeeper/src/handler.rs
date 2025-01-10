//! Part of Safekeeper pretending to be Postgres, i.e. handling Postgres
//! protocol commands.

use anyhow::Context;
use pageserver_api::models::ShardParameters;
use pageserver_api::shard::{ShardIdentity, ShardStripeSize};
use safekeeper_api::models::ConnectionId;
use safekeeper_api::Term;
use std::future::Future;
use std::str::{self, FromStr};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info, info_span, Instrument};
use utils::postgres_client::PostgresClientProtocol;
use utils::shard::{ShardCount, ShardNumber};

use crate::auth::check_permission;
use crate::json_ctrl::{handle_json_ctrl, AppendLogicalMessage};

use crate::metrics::{TrafficMetrics, PG_QUERIES_GAUGE};
use crate::timeline::TimelineError;
use crate::{GlobalTimelines, SafeKeeperConf};
use postgres_backend::PostgresBackend;
use postgres_backend::QueryError;
use postgres_ffi::PG_TLI;
use pq_proto::{BeMessage, FeStartupPacket, RowDescriptor, INT4_OID, TEXT_OID};
use regex::Regex;
use utils::auth::{Claims, JwtAuth, Scope};
use utils::{
    id::{TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
};

/// Safekeeper handler of postgres commands
pub struct SafekeeperPostgresHandler {
    pub conf: Arc<SafeKeeperConf>,
    /// assigned application name
    pub appname: Option<String>,
    pub tenant_id: Option<TenantId>,
    pub timeline_id: Option<TimelineId>,
    pub ttid: TenantTimelineId,
    pub shard: Option<ShardIdentity>,
    pub protocol: Option<PostgresClientProtocol>,
    /// Unique connection id is logged in spans for observability.
    pub conn_id: ConnectionId,
    pub global_timelines: Arc<GlobalTimelines>,
    /// Auth scope allowed on the connections and public key used to check auth tokens. None if auth is not configured.
    auth: Option<(Scope, Arc<JwtAuth>)>,
    claims: Option<Claims>,
    io_metrics: Option<TrafficMetrics>,
}

/// Parsed Postgres command.
enum SafekeeperPostgresCommand {
    StartWalPush,
    StartReplication { start_lsn: Lsn, term: Option<Term> },
    IdentifySystem,
    TimelineStatus,
    JSONCtrl { cmd: AppendLogicalMessage },
}

fn parse_cmd(cmd: &str) -> anyhow::Result<SafekeeperPostgresCommand> {
    if cmd.starts_with("START_WAL_PUSH") {
        Ok(SafekeeperPostgresCommand::StartWalPush)
    } else if cmd.starts_with("START_REPLICATION") {
        let re = Regex::new(
            // We follow postgres START_REPLICATION LOGICAL options to pass term.
            r"START_REPLICATION(?: SLOT [^ ]+)?(?: PHYSICAL)? ([[:xdigit:]]+/[[:xdigit:]]+)(?: \(term='(\d+)'\))?",
        )
        .unwrap();
        let caps = re
            .captures(cmd)
            .context(format!("failed to parse START_REPLICATION command {}", cmd))?;
        let start_lsn =
            Lsn::from_str(&caps[1]).context("parse start LSN from START_REPLICATION command")?;
        let term = if let Some(m) = caps.get(2) {
            Some(m.as_str().parse::<u64>().context("invalid term")?)
        } else {
            None
        };
        Ok(SafekeeperPostgresCommand::StartReplication { start_lsn, term })
    } else if cmd.starts_with("IDENTIFY_SYSTEM") {
        Ok(SafekeeperPostgresCommand::IdentifySystem)
    } else if cmd.starts_with("TIMELINE_STATUS") {
        Ok(SafekeeperPostgresCommand::TimelineStatus)
    } else if cmd.starts_with("JSON_CTRL") {
        let cmd = cmd.strip_prefix("JSON_CTRL").context("invalid prefix")?;
        Ok(SafekeeperPostgresCommand::JSONCtrl {
            cmd: serde_json::from_str(cmd)?,
        })
    } else {
        anyhow::bail!("unsupported command {cmd}");
    }
}

fn cmd_to_string(cmd: &SafekeeperPostgresCommand) -> &str {
    match cmd {
        SafekeeperPostgresCommand::StartWalPush => "START_WAL_PUSH",
        SafekeeperPostgresCommand::StartReplication { .. } => "START_REPLICATION",
        SafekeeperPostgresCommand::TimelineStatus => "TIMELINE_STATUS",
        SafekeeperPostgresCommand::IdentifySystem => "IDENTIFY_SYSTEM",
        SafekeeperPostgresCommand::JSONCtrl { .. } => "JSON_CTRL",
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin + Send> postgres_backend::Handler<IO>
    for SafekeeperPostgresHandler
{
    // tenant_id and timeline_id are passed in connection string params
    fn startup(
        &mut self,
        _pgb: &mut PostgresBackend<IO>,
        sm: &FeStartupPacket,
    ) -> Result<(), QueryError> {
        if let FeStartupPacket::StartupMessage { params, .. } = sm {
            if let Some(options) = params.options_raw() {
                let mut shard_count: Option<u8> = None;
                let mut shard_number: Option<u8> = None;
                let mut shard_stripe_size: Option<u32> = None;

                for opt in options {
                    // FIXME `ztenantid` and `ztimelineid` left for compatibility during deploy,
                    // remove these after the PR gets deployed:
                    // https://github.com/neondatabase/neon/pull/2433#discussion_r970005064
                    match opt.split_once('=') {
                        Some(("protocol", value)) => {
                            self.protocol =
                                Some(serde_json::from_str(value).with_context(|| {
                                    format!("Failed to parse {value} as protocol")
                                })?);
                        }
                        Some(("ztenantid", value)) | Some(("tenant_id", value)) => {
                            self.tenant_id = Some(value.parse().with_context(|| {
                                format!("Failed to parse {value} as tenant id")
                            })?);
                        }
                        Some(("ztimelineid", value)) | Some(("timeline_id", value)) => {
                            self.timeline_id = Some(value.parse().with_context(|| {
                                format!("Failed to parse {value} as timeline id")
                            })?);
                        }
                        Some(("availability_zone", client_az)) => {
                            if let Some(metrics) = self.io_metrics.as_ref() {
                                metrics.set_client_az(client_az)
                            }
                        }
                        Some(("shard_count", value)) => {
                            shard_count = Some(value.parse::<u8>().with_context(|| {
                                format!("Failed to parse {value} as shard count")
                            })?);
                        }
                        Some(("shard_number", value)) => {
                            shard_number = Some(value.parse::<u8>().with_context(|| {
                                format!("Failed to parse {value} as shard number")
                            })?);
                        }
                        Some(("shard_stripe_size", value)) => {
                            shard_stripe_size = Some(value.parse::<u32>().with_context(|| {
                                format!("Failed to parse {value} as shard stripe size")
                            })?);
                        }
                        _ => continue,
                    }
                }

                match self.protocol() {
                    PostgresClientProtocol::Vanilla => {
                        if shard_count.is_some()
                            || shard_number.is_some()
                            || shard_stripe_size.is_some()
                        {
                            return Err(QueryError::Other(anyhow::anyhow!(
                                "Shard params specified for vanilla protocol"
                            )));
                        }
                    }
                    PostgresClientProtocol::Interpreted { .. } => {
                        match (shard_count, shard_number, shard_stripe_size) {
                            (Some(count), Some(number), Some(stripe_size)) => {
                                let params = ShardParameters {
                                    count: ShardCount(count),
                                    stripe_size: ShardStripeSize(stripe_size),
                                };
                                self.shard =
                                    Some(ShardIdentity::from_params(ShardNumber(number), &params));
                            }
                            _ => {
                                return Err(QueryError::Other(anyhow::anyhow!(
                                    "Shard params were not specified"
                                )));
                            }
                        }
                    }
                }
            }

            if let Some(app_name) = params.get("application_name") {
                self.appname = Some(app_name.to_owned());
                if let Some(metrics) = self.io_metrics.as_ref() {
                    metrics.set_app_name(app_name)
                }
            }

            let ttid = TenantTimelineId::new(
                self.tenant_id.unwrap_or(TenantId::from([0u8; 16])),
                self.timeline_id.unwrap_or(TimelineId::from([0u8; 16])),
            );
            tracing::Span::current()
                .record("ttid", tracing::field::display(ttid))
                .record(
                    "application_name",
                    tracing::field::debug(self.appname.clone()),
                );

            if let Some(shard) = self.shard.as_ref() {
                if let Some(slug) = shard.shard_slug().strip_prefix("-") {
                    tracing::Span::current().record("shard", tracing::field::display(slug));
                }
            }

            Ok(())
        } else {
            Err(QueryError::Other(anyhow::anyhow!(
                "Safekeeper received unexpected initial message: {sm:?}"
            )))
        }
    }

    fn check_auth_jwt(
        &mut self,
        _pgb: &mut PostgresBackend<IO>,
        jwt_response: &[u8],
    ) -> Result<(), QueryError> {
        // this unwrap is never triggered, because check_auth_jwt only called when auth_type is NeonJWT
        // which requires auth to be present
        let (allowed_auth_scope, auth) = self
            .auth
            .as_ref()
            .expect("auth_type is configured but .auth of handler is missing");
        let data = auth
            .decode(str::from_utf8(jwt_response).context("jwt response is not UTF-8")?)
            .map_err(|e| QueryError::Unauthorized(e.0))?;

        // The handler might be configured to allow only tenant scope tokens.
        if matches!(allowed_auth_scope, Scope::Tenant)
            && !matches!(data.claims.scope, Scope::Tenant)
        {
            return Err(QueryError::Unauthorized(
                "passed JWT token is for full access, but only tenant scope is allowed".into(),
            ));
        }

        if matches!(data.claims.scope, Scope::Tenant) && data.claims.tenant_id.is_none() {
            return Err(QueryError::Unauthorized(
                "jwt token scope is Tenant, but tenant id is missing".into(),
            ));
        }

        debug!(
            "jwt scope check succeeded for scope: {:#?} by tenant id: {:?}",
            data.claims.scope, data.claims.tenant_id,
        );

        self.claims = Some(data.claims);
        Ok(())
    }

    fn process_query(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        query_string: &str,
    ) -> impl Future<Output = Result<(), QueryError>> {
        Box::pin(async move {
            if query_string
                .to_ascii_lowercase()
                .starts_with("set datestyle to ")
            {
                // important for debug because psycopg2 executes "SET datestyle TO 'ISO'" on connect
                pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
                return Ok(());
            }

            let cmd = parse_cmd(query_string)?;
            let cmd_str = cmd_to_string(&cmd);

            let _guard = PG_QUERIES_GAUGE.with_label_values(&[cmd_str]).guard();

            info!("got query {:?}", query_string);

            let tenant_id = self.tenant_id.context("tenantid is required")?;
            let timeline_id = self.timeline_id.context("timelineid is required")?;
            self.check_permission(Some(tenant_id))?;
            self.ttid = TenantTimelineId::new(tenant_id, timeline_id);

            match cmd {
                SafekeeperPostgresCommand::StartWalPush => {
                    self.handle_start_wal_push(pgb)
                        .instrument(info_span!("WAL receiver"))
                        .await
                }
                SafekeeperPostgresCommand::StartReplication { start_lsn, term } => {
                    self.handle_start_replication(pgb, start_lsn, term)
                        .instrument(info_span!("WAL sender"))
                        .await
                }
                SafekeeperPostgresCommand::IdentifySystem => self.handle_identify_system(pgb).await,
                SafekeeperPostgresCommand::TimelineStatus => self.handle_timeline_status(pgb).await,
                SafekeeperPostgresCommand::JSONCtrl { ref cmd } => {
                    handle_json_ctrl(self, pgb, cmd).await
                }
            }
        })
    }
}

impl SafekeeperPostgresHandler {
    pub fn new(
        conf: Arc<SafeKeeperConf>,
        conn_id: u32,
        io_metrics: Option<TrafficMetrics>,
        auth: Option<(Scope, Arc<JwtAuth>)>,
        global_timelines: Arc<GlobalTimelines>,
    ) -> Self {
        SafekeeperPostgresHandler {
            conf,
            appname: None,
            tenant_id: None,
            timeline_id: None,
            ttid: TenantTimelineId::empty(),
            shard: None,
            protocol: None,
            conn_id,
            claims: None,
            auth,
            io_metrics,
            global_timelines,
        }
    }

    pub fn protocol(&self) -> PostgresClientProtocol {
        self.protocol.unwrap_or(PostgresClientProtocol::Vanilla)
    }

    // when accessing management api supply None as an argument
    // when using to authorize tenant pass corresponding tenant id
    fn check_permission(&self, tenant_id: Option<TenantId>) -> Result<(), QueryError> {
        if self.auth.is_none() {
            // auth is set to Trust, nothing to check so just return ok
            return Ok(());
        }
        // auth is some, just checked above, when auth is some
        // then claims are always present because of checks during connection init
        // so this expect won't trigger
        let claims = self
            .claims
            .as_ref()
            .expect("claims presence already checked");
        check_permission(claims, tenant_id).map_err(|e| QueryError::Unauthorized(e.0))
    }

    async fn handle_timeline_status<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
    ) -> Result<(), QueryError> {
        // Get timeline, handling "not found" error
        let tli = match self.global_timelines.get(self.ttid) {
            Ok(tli) => Ok(Some(tli)),
            Err(TimelineError::NotFound(_)) => Ok(None),
            Err(e) => Err(QueryError::Other(e.into())),
        }?;

        // Write row description
        pgb.write_message_noflush(&BeMessage::RowDescription(&[
            RowDescriptor::text_col(b"flush_lsn"),
            RowDescriptor::text_col(b"commit_lsn"),
        ]))?;

        // Write row if timeline exists
        if let Some(tli) = tli {
            let (inmem, _state) = tli.get_state().await;
            let flush_lsn = tli.get_flush_lsn().await;
            let commit_lsn = inmem.commit_lsn;
            pgb.write_message_noflush(&BeMessage::DataRow(&[
                Some(flush_lsn.to_string().as_bytes()),
                Some(commit_lsn.to_string().as_bytes()),
            ]))?;
        }

        pgb.write_message_noflush(&BeMessage::CommandComplete(b"TIMELINE_STATUS"))?;
        Ok(())
    }

    ///
    /// Handle IDENTIFY_SYSTEM replication command
    ///
    async fn handle_identify_system<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
    ) -> Result<(), QueryError> {
        let tli = self
            .global_timelines
            .get(self.ttid)
            .map_err(|e| QueryError::Other(e.into()))?;

        let lsn = if self.is_walproposer_recovery() {
            // walproposer should get all local WAL until flush_lsn
            tli.get_flush_lsn().await
        } else {
            // other clients shouldn't get any uncommitted WAL
            tli.get_state().await.0.commit_lsn
        }
        .to_string();

        let sysid = tli.get_state().await.1.server.system_id.to_string();
        let lsn_bytes = lsn.as_bytes();
        let tli = PG_TLI.to_string();
        let tli_bytes = tli.as_bytes();
        let sysid_bytes = sysid.as_bytes();

        pgb.write_message_noflush(&BeMessage::RowDescription(&[
            RowDescriptor {
                name: b"systemid",
                typoid: TEXT_OID,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"timeline",
                typoid: INT4_OID,
                typlen: 4,
                ..Default::default()
            },
            RowDescriptor {
                name: b"xlogpos",
                typoid: TEXT_OID,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"dbname",
                typoid: TEXT_OID,
                typlen: -1,
                ..Default::default()
            },
        ]))?
        .write_message_noflush(&BeMessage::DataRow(&[
            Some(sysid_bytes),
            Some(tli_bytes),
            Some(lsn_bytes),
            None,
        ]))?
        .write_message_noflush(&BeMessage::CommandComplete(b"IDENTIFY_SYSTEM"))?;
        Ok(())
    }

    /// Returns true if current connection is a replication connection, originating
    /// from a walproposer recovery function. This connection gets a special handling:
    /// safekeeper must stream all local WAL till the flush_lsn, whether committed or not.
    pub fn is_walproposer_recovery(&self) -> bool {
        match &self.appname {
            None => false,
            Some(appname) => {
                appname == "wal_proposer_recovery" ||
                // set by safekeeper peer recovery
                appname.starts_with("safekeeper")
            }
        }
    }
}
