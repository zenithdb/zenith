use redis::aio::ConnectionLike;
use redis::{Cmd, FromRedisValue, Pipeline, RedisResult, ToRedisArgs};

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

    pub(crate) async fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        self.query(Cmd::hset(key, field, value)).await
    }

    #[allow(dead_code)]
    pub(crate) async fn hset_multiple<K, V>(
        &mut self,
        key: &str,
        items: &[(K, V)],
    ) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        self.query(Cmd::hset_multiple(key, items)).await
    }

    #[allow(dead_code)]
    pub(crate) async fn expire<K>(&mut self, key: K, seconds: i64) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
    {
        self.query(Cmd::expire(key, seconds)).await
    }

    #[allow(dead_code)]
    pub(crate) async fn hget<K, F, V>(&mut self, key: K, field: F) -> anyhow::Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: redis::FromRedisValue,
    {
        self.query(Cmd::hget(key, field)).await
    }

    pub(crate) async fn hget_all<K, V>(&mut self, key: K) -> anyhow::Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        V: redis::FromRedisValue,
    {
        self.query(Cmd::hgetall(key)).await
    }

    pub(crate) async fn hdel<K, F>(&mut self, key: K, field: F) -> anyhow::Result<()>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
    {
        self.query(Cmd::hdel(key, field)).await
    }
}
