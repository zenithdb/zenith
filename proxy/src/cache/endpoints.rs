use std::convert::Infallible;
use std::future::pending;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashSet;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, FromRedisValue, Value};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::EndpointCacheConfig;
use crate::context::RequestContext;
use crate::ext::LockExt;
use crate::intern::{BranchIdInt, EndpointIdInt, ProjectIdInt};
use crate::metrics::{Metrics, RedisErrors, RedisEventsCount};
use crate::rate_limiter::GlobalRateLimiter;
use crate::redis::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use crate::types::EndpointId;

// TODO: this could be an enum, but events in Redis need to be fixed first.
// ProjectCreated was sent with type:branch_created. So we ignore type.
#[derive(Deserialize, Debug, Clone, PartialEq)]
struct ControlPlaneEvent {
    endpoint_created: Option<EndpointCreated>,
    branch_created: Option<BranchCreated>,
    project_created: Option<ProjectCreated>,
    #[serde(rename = "type")]
    _type: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct EndpointCreated {
    endpoint_id: EndpointIdInt,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct BranchCreated {
    branch_id: BranchIdInt,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct ProjectCreated {
    project_id: ProjectIdInt,
}

impl TryFrom<&Value> for ControlPlaneEvent {
    type Error = anyhow::Error;
    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let json = String::from_redis_value(value)?;
        Ok(serde_json::from_str(&json)?)
    }
}

pub struct EndpointsCache {
    config: EndpointCacheConfig,
    endpoints: DashSet<EndpointIdInt>,
    branches: DashSet<BranchIdInt>,
    projects: DashSet<ProjectIdInt>,
    ready: AtomicBool,
    limiter: Arc<Mutex<GlobalRateLimiter>>,
}

impl EndpointsCache {
    pub(crate) fn new(config: EndpointCacheConfig) -> Self {
        Self {
            limiter: Arc::new(Mutex::new(GlobalRateLimiter::new(
                config.limiter_info.clone(),
            ))),
            config,
            endpoints: DashSet::new(),
            branches: DashSet::new(),
            projects: DashSet::new(),
            ready: AtomicBool::new(false),
        }
    }

    pub(crate) fn is_valid(&self, ctx: &RequestContext, endpoint: &EndpointId) -> bool {
        if !self.ready.load(Ordering::Acquire) {
            // the endpoint cache is not yet fully initialised.
            return true;
        }

        if !self.should_reject(endpoint) {
            ctx.set_rejected(false);
            return true;
        }

        // report that we might want to reject this endpoint
        ctx.set_rejected(true);

        // If cache is disabled, just collect the metrics and return.
        if self.config.disable_cache {
            return true;
        }

        // If the limiter allows, we can pretend like it's valid
        // (incase it is, due to redis channel lag).
        if self.limiter.lock_propagate_poison().check() {
            return true;
        }

        // endpoint not found, and there's too much load.
        false
    }

    fn should_reject(&self, endpoint: &EndpointId) -> bool {
        if endpoint.is_endpoint() {
            let Some(endpoint) = EndpointIdInt::get(endpoint) else {
                // if we haven't interned this endpoint, it's not in the cache.
                return true;
            };
            !self.endpoints.contains(&endpoint)
        } else if endpoint.is_branch() {
            let Some(branch) = BranchIdInt::get(endpoint) else {
                // if we haven't interned this branch, it's not in the cache.
                return true;
            };
            !self.branches.contains(&branch)
        } else {
            let Some(project) = ProjectIdInt::get(endpoint) else {
                // if we haven't interned this project, it's not in the cache.
                return true;
            };
            !self.projects.contains(&project)
        }
    }

    fn insert_event(&self, event: ControlPlaneEvent) {
        if let Some(endpoint_created) = event.endpoint_created {
            self.endpoints.insert(endpoint_created.endpoint_id);
            Metrics::get()
                .proxy
                .redis_events_count
                .inc(RedisEventsCount::EndpointCreated);
        } else if let Some(branch_created) = event.branch_created {
            self.branches.insert(branch_created.branch_id);
            Metrics::get()
                .proxy
                .redis_events_count
                .inc(RedisEventsCount::BranchCreated);
        } else if let Some(project_created) = event.project_created {
            self.projects.insert(project_created.project_id);
            Metrics::get()
                .proxy
                .redis_events_count
                .inc(RedisEventsCount::ProjectCreated);
        }
    }

    pub async fn do_read(
        &self,
        mut con: ConnectionWithCredentialsProvider,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<Infallible> {
        let mut last_id = "0-0".to_string();
        loop {
            if let Err(e) = con.connect().await {
                tracing::error!("error connecting to redis: {:?}", e);
                self.ready.store(false, Ordering::Release);
            }
            if let Err(e) = self.read_from_stream(&mut con, &mut last_id).await {
                tracing::error!("error reading from redis: {:?}", e);
                self.ready.store(false, Ordering::Release);
            }
            if cancellation_token.is_cancelled() {
                info!("cancellation token is cancelled, exiting");
                // Maintenance tasks run forever. Sleep forever when canceled.
                pending::<()>().await;
            }
            tokio::time::sleep(self.config.retry_interval).await;
        }
    }

    async fn read_from_stream(
        &self,
        con: &mut ConnectionWithCredentialsProvider,
        last_id: &mut String,
    ) -> anyhow::Result<()> {
        tracing::info!("reading endpoints/branches/projects from redis");
        self.batch_read(
            con,
            StreamReadOptions::default().count(self.config.initial_batch_size),
            last_id,
            true,
        )
        .await?;
        tracing::info!("ready to filter user requests");
        self.ready.store(true, Ordering::Release);
        self.batch_read(
            con,
            StreamReadOptions::default()
                .count(self.config.default_batch_size)
                .block(self.config.xread_timeout.as_millis() as usize),
            last_id,
            false,
        )
        .await
    }

    async fn batch_read(
        &self,
        conn: &mut ConnectionWithCredentialsProvider,
        opts: StreamReadOptions,
        last_id: &mut String,
        return_when_finish: bool,
    ) -> anyhow::Result<()> {
        let mut total: usize = 0;
        loop {
            let mut res: StreamReadReply = conn
                .xread_options(&[&self.config.stream_name], &[last_id.as_str()], &opts)
                .await?;

            if res.keys.is_empty() {
                if return_when_finish {
                    if total != 0 {
                        break;
                    }
                    anyhow::bail!(
                        "Redis stream {} is empty, cannot be used to filter endpoints",
                        self.config.stream_name
                    );
                }
                // If we are not returning when finish, we should wait for more data.
                continue;
            }
            if res.keys.len() != 1 {
                anyhow::bail!("Cannot read from redis stream {}", self.config.stream_name);
            }

            let key = res.keys.pop().expect("Checked length above");
            let len = key.ids.len();
            for stream_id in key.ids {
                total += 1;
                for value in stream_id.map.values() {
                    match value.try_into() {
                        Ok(event) => self.insert_event(event),
                        Err(err) => {
                            Metrics::get().proxy.redis_errors_total.inc(RedisErrors {
                                channel: &self.config.stream_name,
                            });
                            tracing::error!("error parsing value {value:?}: {err:?}");
                        }
                    };
                }
                if total.is_power_of_two() {
                    tracing::debug!("endpoints read {}", total);
                }
                *last_id = stream_id.id;
            }
            if return_when_finish && len <= self.config.default_batch_size {
                break;
            }
        }
        tracing::info!("read {} endpoints/branches/projects from redis", total);
        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_control_plane_event() {
        let s = r#"{"branch_created":null,"endpoint_created":{"endpoint_id":"ep-rapid-thunder-w0qqw2q9"},"project_created":null,"type":"endpoint_created"}"#;

        let endpoint_id: EndpointId = "ep-rapid-thunder-w0qqw2q9".into();

        assert_eq!(
            serde_json::from_str::<ControlPlaneEvent>(s).unwrap(),
            ControlPlaneEvent {
                endpoint_created: Some(EndpointCreated {
                    endpoint_id: endpoint_id.into(),
                }),
                branch_created: None,
                project_created: None,
                _type: Some("endpoint_created".into()),
            }
        );
    }
}
