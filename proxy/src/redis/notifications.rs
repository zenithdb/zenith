use std::convert::Infallible;
use std::sync::Arc;

use futures::StreamExt;
use pq_proto::CancelKeyData;
use redis::aio::PubSub;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use uuid::Uuid;

use super::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use crate::cache::project_info::ProjectInfoCache;
use crate::cancellation::{CancelMap, CancellationHandler};
use crate::config::ProxyConfig;
use crate::intern::{ProjectIdInt, RoleNameInt};
use crate::metrics::{Metrics, RedisErrors, RedisEventsCount};

const CPLANE_CHANNEL_NAME: &str = "neondb-proxy-ws-updates";
pub(crate) const PROXY_CHANNEL_NAME: &str = "neondb-proxy-to-proxy-updates";
const RECONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const INVALIDATION_LAG: std::time::Duration = std::time::Duration::from_secs(20);

async fn try_connect(client: &ConnectionWithCredentialsProvider) -> anyhow::Result<PubSub> {
    let mut conn = client.get_async_pubsub().await?;
    tracing::info!("subscribing to a channel `{CPLANE_CHANNEL_NAME}`");
    conn.subscribe(CPLANE_CHANNEL_NAME).await?;
    tracing::info!("subscribing to a channel `{PROXY_CHANNEL_NAME}`");
    conn.subscribe(PROXY_CHANNEL_NAME).await?;
    Ok(conn)
}

#[derive(Debug, Deserialize)]
struct NotificationHeader<'a> {
    topic: &'a str,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "topic", content = "data")]
pub(crate) enum Notification {
    #[serde(
        rename = "/allowed_ips_updated",
        deserialize_with = "deserialize_json_string"
    )]
    AllowedIpsUpdate {
        allowed_ips_update: AllowedIpsUpdate,
    },
    #[serde(
        rename = "/block_public_or_vpc_access_updated",
        deserialize_with = "deserialize_json_string"
    )]
    BlockPublicOrVpcAccessUpdated {
        block_public_or_vpc_access_updated: BlockPublicOrVpcAccessUpdated,
    },
    #[serde(
        rename = "/allowed_vpc_endpoints_updated_for_org",
        deserialize_with = "deserialize_json_string"
    )]
    AllowedVpcEndpointsUpdatedForOrg {
        allowed_vpc_endpoints_updated_for_org: AllowedVpcEndpointsUpdatedForOrg,
    },
    #[serde(
        rename = "/allowed_vpc_endpoints_updated_for_projects",
        deserialize_with = "deserialize_json_string"
    )]
    AllowedVpcEndpointsUpdatedForProjects {
        allowed_vpc_endpoints_updated_for_projects: AllowedVpcEndpointsUpdatedForProjects,
    },
    #[serde(
        rename = "/password_updated",
        deserialize_with = "deserialize_json_string"
    )]
    PasswordUpdate { password_update: PasswordUpdate },
    #[serde(rename = "/cancel_session")]
    Cancel(CancelSession),

    #[serde(
        other,
        deserialize_with = "deserialize_unknown_topic",
        skip_serializing
    )]
    UnknownTopic,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct AllowedIpsUpdate {
    project_id: ProjectIdInt,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct BlockPublicOrVpcAccessUpdated {
    project_id: ProjectIdInt,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct AllowedVpcEndpointsUpdatedForOrg {
    // TODO: change type once the implementation is more fully fledged.
    // See e.g. https://github.com/neondatabase/neon/pull/10073.
    account_id: ProjectIdInt,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct AllowedVpcEndpointsUpdatedForProjects {
    project_ids: Vec<ProjectIdInt>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct PasswordUpdate {
    project_id: ProjectIdInt,
    role_name: RoleNameInt,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct CancelSession {
    pub(crate) region_id: Option<String>,
    pub(crate) cancel_key_data: CancelKeyData,
    pub(crate) session_id: Uuid,
    pub(crate) peer_addr: Option<std::net::IpAddr>,
}

fn deserialize_json_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: for<'de2> serde::Deserialize<'de2>,
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(<D::Error as serde::de::Error>::custom)
}

// https://github.com/serde-rs/serde/issues/1714
fn deserialize_unknown_topic<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_any(serde::de::IgnoredAny)?;
    Ok(())
}

struct MessageHandler<C: ProjectInfoCache + Send + Sync + 'static> {
    cache: Arc<C>,
    cancellation_handler: Arc<CancellationHandler<()>>,
    region_id: String,
}

impl<C: ProjectInfoCache + Send + Sync + 'static> Clone for MessageHandler<C> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            cancellation_handler: self.cancellation_handler.clone(),
            region_id: self.region_id.clone(),
        }
    }
}

impl<C: ProjectInfoCache + Send + Sync + 'static> MessageHandler<C> {
    pub(crate) fn new(
        cache: Arc<C>,
        cancellation_handler: Arc<CancellationHandler<()>>,
        region_id: String,
    ) -> Self {
        Self {
            cache,
            cancellation_handler,
            region_id,
        }
    }

    pub(crate) async fn increment_active_listeners(&self) {
        self.cache.increment_active_listeners().await;
    }

    pub(crate) async fn decrement_active_listeners(&self) {
        self.cache.decrement_active_listeners().await;
    }

    #[tracing::instrument(skip(self, msg), fields(session_id = tracing::field::Empty))]
    async fn handle_message(&self, msg: redis::Msg) -> anyhow::Result<()> {
        let payload: String = msg.get_payload()?;
        tracing::debug!(?payload, "received a message payload");

        let msg: Notification = match serde_json::from_str(&payload) {
            Ok(Notification::UnknownTopic) => {
                match serde_json::from_str::<NotificationHeader>(&payload) {
                    // don't update the metric for redis errors if it's just a topic we don't know about.
                    Ok(header) => tracing::warn!(topic = header.topic, "unknown topic"),
                    Err(e) => {
                        Metrics::get().proxy.redis_errors_total.inc(RedisErrors {
                            channel: msg.get_channel_name(),
                        });
                        tracing::error!("broken message: {e}");
                    }
                };
                return Ok(());
            }
            Ok(msg) => msg,
            Err(e) => {
                Metrics::get().proxy.redis_errors_total.inc(RedisErrors {
                    channel: msg.get_channel_name(),
                });
                match serde_json::from_str::<NotificationHeader>(&payload) {
                    Ok(header) => tracing::error!(topic = header.topic, "broken message: {e}"),
                    Err(_) => tracing::error!("broken message: {e}"),
                };
                return Ok(());
            }
        };

        tracing::debug!(?msg, "received a message");
        match msg {
            Notification::Cancel(cancel_session) => {
                tracing::Span::current().record(
                    "session_id",
                    tracing::field::display(cancel_session.session_id),
                );
                Metrics::get()
                    .proxy
                    .redis_events_count
                    .inc(RedisEventsCount::CancelSession);
                if let Some(cancel_region) = cancel_session.region_id {
                    // If the message is not for this region, ignore it.
                    if cancel_region != self.region_id {
                        return Ok(());
                    }
                }

                // TODO: Remove unspecified peer_addr after the complete migration to the new format
                let peer_addr = cancel_session
                    .peer_addr
                    .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
                let cancel_span = tracing::span!(parent: None, tracing::Level::INFO, "cancel_session", session_id = ?cancel_session.session_id);
                cancel_span.follows_from(tracing::Span::current());
                // This instance of cancellation_handler doesn't have a RedisPublisherClient so it can't publish the message.
                match self
                    .cancellation_handler
                    .cancel_session(
                        cancel_session.cancel_key_data,
                        uuid::Uuid::nil(),
                        peer_addr,
                        cancel_session.peer_addr.is_some(),
                    )
                    .instrument(cancel_span)
                    .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        tracing::warn!("failed to cancel session: {e}");
                    }
                }
            }
            Notification::AllowedIpsUpdate { .. }
            | Notification::PasswordUpdate { .. }
            | Notification::BlockPublicOrVpcAccessUpdated { .. }
            | Notification::AllowedVpcEndpointsUpdatedForOrg { .. }
            | Notification::AllowedVpcEndpointsUpdatedForProjects { .. } => {
                invalidate_cache(self.cache.clone(), msg.clone());
                if matches!(msg, Notification::AllowedIpsUpdate { .. }) {
                    Metrics::get()
                        .proxy
                        .redis_events_count
                        .inc(RedisEventsCount::AllowedIpsUpdate);
                } else if matches!(msg, Notification::PasswordUpdate { .. }) {
                    Metrics::get()
                        .proxy
                        .redis_events_count
                        .inc(RedisEventsCount::PasswordUpdate);
                }
                // TODO: add additional metrics for the other event types.

                // It might happen that the invalid entry is on the way to be cached.
                // To make sure that the entry is invalidated, let's repeat the invalidation in INVALIDATION_LAG seconds.
                // TODO: include the version (or the timestamp) in the message and invalidate only if the entry is cached before the message.
                let cache = self.cache.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(INVALIDATION_LAG).await;
                    invalidate_cache(cache, msg);
                });
            }

            Notification::UnknownTopic => unreachable!(),
        }

        Ok(())
    }
}

fn invalidate_cache<C: ProjectInfoCache>(cache: Arc<C>, msg: Notification) {
    match msg {
        Notification::AllowedIpsUpdate { allowed_ips_update } => {
            cache.invalidate_allowed_ips_for_project(allowed_ips_update.project_id);
        }
        Notification::PasswordUpdate { password_update } => cache
            .invalidate_role_secret_for_project(
                password_update.project_id,
                password_update.role_name,
            ),
        Notification::Cancel(_) => unreachable!("cancel message should be handled separately"),
        Notification::BlockPublicOrVpcAccessUpdated { .. } => {
            // https://github.com/neondatabase/neon/pull/10073
        }
        Notification::AllowedVpcEndpointsUpdatedForOrg { .. } => {
            // https://github.com/neondatabase/neon/pull/10073
        }
        Notification::AllowedVpcEndpointsUpdatedForProjects { .. } => {
            // https://github.com/neondatabase/neon/pull/10073
        }
        Notification::UnknownTopic => unreachable!(),
    }
}

async fn handle_messages<C: ProjectInfoCache + Send + Sync + 'static>(
    handler: MessageHandler<C>,
    redis: ConnectionWithCredentialsProvider,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        if cancellation_token.is_cancelled() {
            return Ok(());
        }
        let mut conn = match try_connect(&redis).await {
            Ok(conn) => {
                handler.increment_active_listeners().await;
                conn
            }
            Err(e) => {
                tracing::error!(
            "failed to connect to redis: {e}, will try to reconnect in {RECONNECT_TIMEOUT:#?}"
        );
                tokio::time::sleep(RECONNECT_TIMEOUT).await;
                continue;
            }
        };
        let mut stream = conn.on_message();
        while let Some(msg) = stream.next().await {
            match handler.handle_message(msg).await {
                Ok(()) => {}
                Err(e) => {
                    tracing::error!("failed to handle message: {e}, will try to reconnect");
                    break;
                }
            }
            if cancellation_token.is_cancelled() {
                handler.decrement_active_listeners().await;
                return Ok(());
            }
        }
        handler.decrement_active_listeners().await;
    }
}

/// Handle console's invalidation messages.
#[tracing::instrument(name = "redis_notifications", skip_all)]
pub async fn task_main<C>(
    config: &'static ProxyConfig,
    redis: ConnectionWithCredentialsProvider,
    cache: Arc<C>,
    cancel_map: CancelMap,
    region_id: String,
) -> anyhow::Result<Infallible>
where
    C: ProjectInfoCache + Send + Sync + 'static,
{
    let cancellation_handler = Arc::new(CancellationHandler::<()>::new(
        &config.connect_to_compute,
        cancel_map,
        crate::metrics::CancellationSource::FromRedis,
    ));
    let handler = MessageHandler::new(cache, cancellation_handler, region_id);
    // 6h - 1m.
    // There will be 1 minute overlap between two tasks. But at least we can be sure that no message is lost.
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(6 * 60 * 60 - 60));
    loop {
        let cancellation_token = CancellationToken::new();
        interval.tick().await;

        tokio::spawn(handle_messages(
            handler.clone(),
            redis.clone(),
            cancellation_token.clone(),
        ));
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(6 * 60 * 60)).await; // 6h.
            cancellation_token.cancel();
        });
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::types::{ProjectId, RoleName};

    #[test]
    fn parse_allowed_ips() -> anyhow::Result<()> {
        let project_id: ProjectId = "new_project".into();
        let data = format!("{{\"project_id\": \"{project_id}\"}}");
        let text = json!({
            "type": "message",
            "topic": "/allowed_ips_updated",
            "data": data,
            "extre_fields": "something"
        })
        .to_string();

        let result: Notification = serde_json::from_str(&text)?;
        assert_eq!(
            result,
            Notification::AllowedIpsUpdate {
                allowed_ips_update: AllowedIpsUpdate {
                    project_id: (&project_id).into()
                }
            }
        );

        Ok(())
    }

    #[test]
    fn parse_password_updated() -> anyhow::Result<()> {
        let project_id: ProjectId = "new_project".into();
        let role_name: RoleName = "new_role".into();
        let data = format!("{{\"project_id\": \"{project_id}\", \"role_name\": \"{role_name}\"}}");
        let text = json!({
            "type": "message",
            "topic": "/password_updated",
            "data": data,
            "extre_fields": "something"
        })
        .to_string();

        let result: Notification = serde_json::from_str(&text)?;
        assert_eq!(
            result,
            Notification::PasswordUpdate {
                password_update: PasswordUpdate {
                    project_id: (&project_id).into(),
                    role_name: (&role_name).into(),
                }
            }
        );

        Ok(())
    }
    #[test]
    fn parse_cancel_session() -> anyhow::Result<()> {
        let cancel_key_data = CancelKeyData {
            backend_pid: 42,
            cancel_key: 41,
        };
        let uuid = uuid::Uuid::new_v4();
        let msg = Notification::Cancel(CancelSession {
            cancel_key_data,
            region_id: None,
            session_id: uuid,
            peer_addr: None,
        });
        let text = serde_json::to_string(&msg)?;
        let result: Notification = serde_json::from_str(&text)?;
        assert_eq!(msg, result);

        let msg = Notification::Cancel(CancelSession {
            cancel_key_data,
            region_id: Some("region".to_string()),
            session_id: uuid,
            peer_addr: None,
        });
        let text = serde_json::to_string(&msg)?;
        let result: Notification = serde_json::from_str(&text)?;
        assert_eq!(msg, result,);

        Ok(())
    }

    #[test]
    fn parse_unknown_topic() -> anyhow::Result<()> {
        let with_data = json!({
            "type": "message",
            "topic": "/doesnotexist",
            "data": {
                "payload": "ignored"
            },
            "extra_fields": "something"
        })
        .to_string();
        let result: Notification = serde_json::from_str(&with_data)?;
        assert_eq!(result, Notification::UnknownTopic);

        let without_data = json!({
            "type": "message",
            "topic": "/doesnotexist",
            "extra_fields": "something"
        })
        .to_string();
        let result: Notification = serde_json::from_str(&without_data)?;
        assert_eq!(result, Notification::UnknownTopic);

        Ok(())
    }
}
