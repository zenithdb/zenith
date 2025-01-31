use redis::aio::ConnectionLike;
use redis::{Cmd, FromRedisValue, Pipeline, RedisResult};

use super::connection_with_credentials_provider::ConnectionWithCredentialsProvider;
use crate::rate_limiter::{GlobalRateLimiter, RateBucketInfo};

pub struct RedisKVClient {
    client: ConnectionWithCredentialsProvider,
    limiter: GlobalRateLimiter,
}

#[allow(async_fn_in_trait)]
pub trait Queryable {
    async fn query<T: FromRedisValue>(&self, conn: &mut impl ConnectionLike) -> RedisResult<T>;
}

impl Queryable for Pipeline {
    async fn query<T: FromRedisValue>(&self, conn: &mut impl ConnectionLike) -> RedisResult<T> {
        self.query_async(conn).await
    }
}

impl Queryable for Cmd {
    async fn query<T: FromRedisValue>(&self, conn: &mut impl ConnectionLike) -> RedisResult<T> {
        self.query_async(conn).await
    }
}

impl RedisKVClient {
    pub fn new(client: ConnectionWithCredentialsProvider, info: &'static [RateBucketInfo]) -> Self {
        Self {
            client,
            limiter: GlobalRateLimiter::new(info.into()),
        }
    }

    pub async fn try_connect(&mut self) -> anyhow::Result<()> {
        match self.client.connect().await {
            Ok(()) => {}
            Err(e) => {
                tracing::error!("failed to connect to redis: {e}");
                return Err(e);
            }
        }
        Ok(())
    }

    pub(crate) async fn query<T: FromRedisValue>(
        &mut self,
        q: impl Queryable,
    ) -> anyhow::Result<T> {
        if !self.limiter.check() {
            tracing::info!("Rate limit exceeded. Skipping hset");
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        match q.query(&mut self.client).await {
            Ok(t) => return Ok(t),
            Err(e) => {
                tracing::error!("failed to set a key-value pair: {e}");
            }
        }

        tracing::info!("Redis client is disconnected. Reconnecting...");
        self.try_connect().await?;
        Ok(q.query(&mut self.client).await?)
    }
}
